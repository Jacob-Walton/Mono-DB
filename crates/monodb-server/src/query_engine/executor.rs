#![allow(dead_code)]

//! Query Executor
//!
//! Volcano-style iterator model for executing physical plans.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use monodb_common::{MonoError, Result, Value};

use crate::query_engine::ast::{BinaryOp, ParamRef, SortDirection, UnaryOp};
use crate::query_engine::logical_plan::{AggregateFunction, JoinType, ScalarExpr};
use crate::query_engine::physical_plan::*;
use crate::query_engine::storage::{QueryStorage, Row, ScanConfig};

#[cfg(feature = "plugins")]
use monodb_plugin::{PluginHost, PluginPermissions};

// Execution Context

/// Context for query execution.
pub struct ExecutionContext {
    /// Parameter values for prepared statements
    pub params: Vec<Value>,
    /// Named parameters
    pub named_params: HashMap<String, Value>,
    /// Current transaction ID (if any)
    pub tx_id: Option<u64>,
    /// Row limit for safety
    pub max_rows: usize,
    /// Optional plugin host for user-defined functions
    #[cfg(feature = "plugins")]
    pub plugin_host: Option<Arc<PluginHost>>,
    /// User permissions for plugin execution
    #[cfg(feature = "plugins")]
    pub user_permissions: Option<PluginPermissions>,
    /// Current namespace for table name resolution
    pub current_namespace: String,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            params: Vec::new(),
            named_params: HashMap::new(),
            tx_id: None,
            max_rows: 100_000,
            #[cfg(feature = "plugins")]
            plugin_host: None,
            #[cfg(feature = "plugins")]
            user_permissions: None,
            current_namespace: "default".to_string(),
        }
    }

    pub fn with_params(mut self, params: Vec<Value>) -> Self {
        self.params = params;
        self
    }

    pub fn with_tx(mut self, tx_id: u64) -> Self {
        self.tx_id = Some(tx_id);
        self
    }

    /// Set the current namespace for table name resolution
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.current_namespace = namespace.into();
        self
    }

    /// Set the plugin host for user-defined function execution
    #[cfg(feature = "plugins")]
    pub fn with_plugin_host(mut self, host: Arc<PluginHost>) -> Self {
        self.plugin_host = Some(host);
        self
    }

    /// Set user permissions for plugin execution
    #[cfg(feature = "plugins")]
    pub fn with_user_permissions(mut self, perms: PluginPermissions) -> Self {
        self.user_permissions = Some(perms);
        self
    }

    /// Qualify a table name with the current namespace if not already qualified.
    pub fn qualify_table(&self, name: &str) -> String {
        if name.contains('.') {
            name.to_string()
        } else {
            format!("{}.{}", self.current_namespace, name)
        }
    }

    pub fn get_param(&self, param: &ParamRef) -> Result<&Value> {
        match param {
            ParamRef::Positional(idx) => self.params.get(*idx as usize).ok_or_else(|| {
                MonoError::InvalidOperation(format!("param ${} not found", idx + 1))
            }),
            ParamRef::Named(name) => self
                .named_params
                .get(name.as_str())
                .ok_or_else(|| MonoError::InvalidOperation(format!("param :{} not found", name))),
        }
    }

    /// Try to call a plugin function if available
    #[cfg(feature = "plugins")]
    pub fn call_plugin_function(&self, name: &str, args: Vec<Value>) -> Option<Result<Value>> {
        let host = self.plugin_host.as_ref()?;
        
        // Check if function exists
        if !host.has_function(name) {
            return None;
        }

        // Get user permissions (default to read-only if not set)
        let perms = self.user_permissions.clone()
            .unwrap_or_else(PluginPermissions::read_only);

        // Call the plugin function
        Some(host.call_function(name, args, perms).map_err(|e| {
            MonoError::InvalidOperation(format!("Plugin function error: {}", e))
        }))
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

// Query Result

/// Result of query execution.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub rows_affected: u64,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            rows: Vec::new(),
            rows_affected: 0,
        }
    }

    pub fn from_rows(rows: Vec<Row>) -> Self {
        Self {
            rows,
            rows_affected: 0,
        }
    }

    pub fn affected(count: u64) -> Self {
        Self {
            rows: Vec::new(),
            rows_affected: count,
        }
    }
}

// Executor

/// Executes physical plans against storage.
pub struct Executor<S: QueryStorage> {
    storage: Arc<S>,
}

impl<S: QueryStorage> Executor<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Execute a physical plan.
    pub fn execute(&self, plan: &PhysicalPlan, ctx: &ExecutionContext) -> Result<QueryResult> {
        let tx_id = ctx
            .tx_id
            .ok_or_else(|| MonoError::Transaction("No active transaction".into()))?;

        match plan {
            PhysicalPlan::SeqScan(op) => self.exec_seq_scan(op, tx_id, ctx),
            PhysicalPlan::Filter(op) => self.exec_filter(op, tx_id, ctx),
            PhysicalPlan::Project(op) => self.exec_project(op, tx_id, ctx),
            PhysicalPlan::Sort(op) => self.exec_sort(op, tx_id, ctx),
            PhysicalPlan::Limit(op) => self.exec_limit(op, tx_id, ctx),
            PhysicalPlan::Offset(op) => self.exec_offset(op, tx_id, ctx),
            PhysicalPlan::HashAggregate(op) => self.exec_hash_aggregate(op, tx_id, ctx),
            PhysicalPlan::NestedLoopJoin(op) => self.exec_nested_loop_join(op, tx_id, ctx),
            PhysicalPlan::HashJoin(op) => self.exec_hash_join(op, tx_id, ctx),
            PhysicalPlan::Values(op) => self.exec_values(op),
            PhysicalPlan::Empty => Ok(QueryResult::empty()),
            PhysicalPlan::Insert(op) => self.exec_insert(op, tx_id, ctx),
            PhysicalPlan::Update(op) => self.exec_update(op, tx_id, ctx),
            PhysicalPlan::Delete(op) => self.exec_delete(op, tx_id, ctx),
            PhysicalPlan::PrimaryKeyLookup(op) => self.exec_pk_lookup(op, tx_id, ctx),
            PhysicalPlan::IndexScan(op) => self.exec_index_scan(op, tx_id, ctx),
            PhysicalPlan::StreamAggregate(op) => self.exec_stream_aggregate(op, tx_id, ctx),
            PhysicalPlan::MergeJoin(op) => self.exec_merge_join(op, tx_id, ctx),
        }
    }

    fn exec_seq_scan(
        &self,
        op: &SeqScanOp,
        tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let config = ScanConfig {
            columns: op
                .columns
                .as_ref()
                .map(|cols| cols.iter().map(|c| c.as_str().to_string()).collect()),
            limit: None,
            offset: None,
        };

        let table = ctx.qualify_table(op.table.as_str());
        let mut rows = self.storage.scan(tx_id, &table, &config)?;

        // Apply filter if present
        if let Some(ref pred) = op.filter {
            rows.retain(|row| {
                self.eval_expr(pred, row, ctx)
                    .map(|v| v.to_bool())
                    .unwrap_or(false)
            });
        }

        Ok(QueryResult::from_rows(rows))
    }

    fn exec_pk_lookup(
        &self,
        op: &PkLookupOp,
        tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let key = self.eval_expr(&op.key, &Row::new(), ctx)?;
        let table = ctx.qualify_table(op.table.as_str());
        match self.storage.read(tx_id, &table, &key)? {
            Some(row) => Ok(QueryResult::from_rows(vec![row])),
            None => Ok(QueryResult::empty()),
        }
    }

    fn exec_index_scan(
        &self,
        op: &IndexScanOp,
        tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        use crate::query_engine::physical_plan::IndexScanType;

        let table = ctx.qualify_table(op.table.as_str());
        let index_name = op.index_name.as_str();

        let mut rows = match op.scan_type {
            IndexScanType::ExactMatch => {
                // Point lookup using secondary index
                if let Some(ref lookup_expr) = op.lookup_key {
                    // Evaluate the lookup key value
                    let lookup_value = self.eval_expr(lookup_expr, &Row::new(), ctx)?;
                    
                    // Find primary keys via index lookup
                    let pks = self.storage.find_by_index(&table, index_name, &[lookup_value])?;
                    
                    // Fetch actual rows by primary key
                    let config = ScanConfig::new();
                    let all_rows = self.storage.scan(tx_id, &table, &config)?;
                    
                    // Filter to just the matching rows
                    // FIXME: Direct PK lookup would be more efficient
                    all_rows.into_iter()
                        .filter(|row| {
                            // Get the primary key value from the row and check if it's in pks
                            if let Some(pk_val) = row.get("_pk").or_else(|| row.get("id")).cloned() {
                                let pk_bytes = pk_val.to_bytes();
                                pks.contains(&pk_bytes)
                            } else {
                                false
                            }
                        })
                        .collect()
                } else {
                    // No lookup key, fall back to scan
                    let config = ScanConfig::new();
                    self.storage.scan(tx_id, &table, &config)?
                }
            }
            IndexScanType::RangeScan | IndexScanType::PrefixScan => {
                // Range/prefix scans, not yet fully implemented, fall back to filtered scan
                let config = ScanConfig::new();
                self.storage.scan(tx_id, &table, &config)?
            }
            IndexScanType::OrderedScan { ascending: _ } => {
                // Ordered scan for ORDER BY, not yet fully implemented
                let config = ScanConfig::new();
                self.storage.scan(tx_id, &table, &config)?
            }
        };

        // Apply residual filter (for any predicates not fully covered by index)
        if let Some(ref filter) = op.residual_filter {
            rows.retain(|row| {
                self.eval_expr(filter, row, ctx)
                    .map(|v| v.to_bool())
                    .unwrap_or(false)
            });
        }

        Ok(QueryResult::from_rows(rows))
    }

    fn exec_filter(
        &self,
        op: &FilterOp,
        _tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let input = self.execute(&op.input, ctx)?;
        let mut rows = input.rows;

        rows.retain(|row| {
            self.eval_expr(&op.predicate, row, ctx)
                .map(|v| v.to_bool())
                .unwrap_or(false)
        });

        Ok(QueryResult::from_rows(rows))
    }

    fn exec_project(
        &self,
        op: &ProjectOp,
        _tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let input = self.execute(&op.input, ctx)?;

        let rows = input
            .rows
            .into_iter()
            .map(|row| {
                let mut new_row = Row::new();
                for (name, expr) in &op.expressions {
                    if let Ok(val) = self.eval_expr(expr, &row, ctx) {
                        new_row.insert(name.as_str(), val);
                    }
                }
                new_row
            })
            .collect();

        Ok(QueryResult::from_rows(rows))
    }

    fn exec_sort(&self, op: &SortOp, _tx_id: u64, ctx: &ExecutionContext) -> Result<QueryResult> {
        let input = self.execute(&op.input, ctx)?;
        let mut rows = input.rows;

        rows.sort_by(|a, b| {
            for key in &op.order_by {
                let va = self.eval_expr(&key.expr, a, ctx).unwrap_or(Value::Null);
                let vb = self.eval_expr(&key.expr, b, ctx).unwrap_or(Value::Null);
                let cmp = compare_values(&va, &vb);
                let cmp = match key.direction {
                    SortDirection::Asc => cmp,
                    SortDirection::Desc => cmp.reverse(),
                };
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        if let Some(limit) = op.limit {
            rows.truncate(limit);
        }

        Ok(QueryResult::from_rows(rows))
    }

    fn exec_limit(&self, op: &LimitOp, _tx_id: u64, ctx: &ExecutionContext) -> Result<QueryResult> {
        let input = self.execute(&op.input, ctx)?;
        let mut rows = input.rows;
        rows.truncate(op.count);
        Ok(QueryResult::from_rows(rows))
    }

    fn exec_offset(
        &self,
        op: &OffsetOp,
        _tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let input = self.execute(&op.input, ctx)?;
        let rows = input.rows.into_iter().skip(op.count).collect();
        Ok(QueryResult::from_rows(rows))
    }

    fn exec_hash_aggregate(
        &self,
        op: &HashAggregateOp,
        _tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let input = self.execute(&op.input, ctx)?;

        // Group rows by group-by keys
        let mut groups: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();

        for row in input.rows {
            let mut key_bytes = Vec::new();
            for expr in &op.group_by {
                let val = self.eval_expr(expr, &row, ctx)?;
                key_bytes.extend(format!("{:?}|", val).as_bytes());
            }
            groups.entry(key_bytes).or_default().push(row);
        }

        // Compute aggregates for each group
        let mut result_rows = Vec::new();

        for (_, group_rows) in groups {
            let mut result_row = Row::new();

            // Add group-by values from first row
            if let Some(first) = group_rows.first() {
                for expr in &op.group_by {
                    if let ScalarExpr::Column(col) = expr
                        && let Some(val) = first.get(col.column.as_str())
                    {
                        result_row.insert(col.column.as_str(), val.clone());
                    }
                }
            }

            // Compute aggregates
            for agg in &op.aggregates {
                let val = self.compute_aggregate(&agg.function, &agg.args, &group_rows, ctx)?;
                result_row.insert(agg.output_name.as_str(), val);
            }

            result_rows.push(result_row);
        }

        Ok(QueryResult::from_rows(result_rows))
    }

    fn exec_stream_aggregate(
        &self,
        op: &StreamAggregateOp,
        tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        // Stream aggregate requires sorted input, delegate to hash aggregate
        self.exec_hash_aggregate(
            &HashAggregateOp {
                input: op.input.clone(),
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                cost: op.cost.clone(),
            },
            tx_id,
            ctx,
        )
    }

    fn compute_aggregate(
        &self,
        func: &AggregateFunction,
        args: &[ScalarExpr],
        rows: &[Row],
        ctx: &ExecutionContext,
    ) -> Result<Value> {
        match func {
            AggregateFunction::Count => Ok(Value::Int64(rows.len() as i64)),

            AggregateFunction::Sum => {
                let mut sum = 0.0f64;
                for row in rows {
                    if let Some(expr) = args.first()
                        && let Ok(val) = self.eval_expr(expr, row, ctx)
                    {
                        sum += val.to_f64();
                    }
                }
                Ok(Value::Float64(sum))
            }

            AggregateFunction::Avg => {
                if rows.is_empty() {
                    return Ok(Value::Null);
                }
                let mut sum = 0.0f64;
                let mut count = 0;
                for row in rows {
                    if let Some(expr) = args.first()
                        && let Ok(val) = self.eval_expr(expr, row, ctx)
                        && !matches!(val, Value::Null)
                    {
                        sum += val.to_f64();
                        count += 1;
                    }
                }
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float64(sum / count as f64))
                }
            }

            AggregateFunction::Min => {
                let mut min: Option<Value> = None;
                for row in rows {
                    if let Some(expr) = args.first()
                        && let Ok(val) = self.eval_expr(expr, row, ctx)
                    {
                        min = Some(match min {
                            None => val,
                            Some(m) => {
                                if compare_values(&val, &m) == Ordering::Less {
                                    val
                                } else {
                                    m
                                }
                            }
                        });
                    }
                }
                Ok(min.unwrap_or(Value::Null))
            }

            AggregateFunction::Max => {
                let mut max: Option<Value> = None;
                for row in rows {
                    if let Some(expr) = args.first()
                        && let Ok(val) = self.eval_expr(expr, row, ctx)
                    {
                        max = Some(match max {
                            None => val,
                            Some(m) => {
                                if compare_values(&val, &m) == Ordering::Greater {
                                    val
                                } else {
                                    m
                                }
                            }
                        });
                    }
                }
                Ok(max.unwrap_or(Value::Null))
            }

            AggregateFunction::First => {
                if let Some(row) = rows.first()
                    && let Some(expr) = args.first()
                {
                    return self.eval_expr(expr, row, ctx);
                }
                Ok(Value::Null)
            }

            AggregateFunction::Last => {
                if let Some(row) = rows.last()
                    && let Some(expr) = args.first()
                {
                    return self.eval_expr(expr, row, ctx);
                }
                Ok(Value::Null)
            }
        }
    }

    fn exec_nested_loop_join(
        &self,
        op: &NestedLoopJoinOp,
        _tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let outer = self.execute(&op.outer, ctx)?;
        let inner = self.execute(&op.inner, ctx)?;

        let mut result = Vec::new();

        for outer_row in &outer.rows {
            let mut matched = false;

            for inner_row in &inner.rows {
                // Merge rows
                let mut combined = outer_row.clone();
                for (k, v) in inner_row.iter() {
                    combined.insert(k.clone(), v.clone());
                }

                // Check condition
                let matches = match &op.condition {
                    Some(cond) => self
                        .eval_expr(cond, &combined, ctx)
                        .map(|v| v.to_bool())
                        .unwrap_or(false),
                    None => true,
                };

                if matches {
                    result.push(combined);
                    matched = true;
                }
            }

            // Handle LEFT/FULL outer join
            if !matched && matches!(op.join_type, JoinType::Left | JoinType::Full) {
                let combined = outer_row.clone();
                // Add nulls for inner columns
                result.push(combined);
            }
        }

        // Handle RIGHT/FULL outer join
        if matches!(op.join_type, JoinType::Right | JoinType::Full) {
            for inner_row in &inner.rows {
                let mut has_match = false;
                for outer_row in &outer.rows {
                    let mut combined = outer_row.clone();
                    for (k, v) in inner_row.iter() {
                        combined.insert(k.clone(), v.clone());
                    }
                    if let Some(cond) = &op.condition
                        && self
                            .eval_expr(cond, &combined, ctx)
                            .map(|v| v.to_bool())
                            .unwrap_or(false)
                    {
                        has_match = true;
                        break;
                    }
                }
                if !has_match {
                    result.push(inner_row.clone());
                }
            }
        }

        Ok(QueryResult::from_rows(result))
    }

    fn exec_hash_join(
        &self,
        op: &HashJoinOp,
        _tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let build = self.execute(&op.build, ctx)?;
        let probe = self.execute(&op.probe, ctx)?;

        // Build hash table
        let mut hash_table: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();
        for row in build.rows {
            let mut key = Vec::new();
            for expr in &op.build_keys {
                let val = self.eval_expr(expr, &row, ctx)?;
                key.extend(format!("{:?}|", val).as_bytes());
            }
            hash_table.entry(key).or_default().push(row);
        }

        // Probe
        let mut result = Vec::new();
        for probe_row in probe.rows {
            let mut key = Vec::new();
            for expr in &op.probe_keys {
                let val = self.eval_expr(expr, &probe_row, ctx)?;
                key.extend(format!("{:?}|", val).as_bytes());
            }

            if let Some(matches) = hash_table.get(&key) {
                for build_row in matches {
                    let mut combined = probe_row.clone();
                    for (k, v) in build_row.iter() {
                        combined.insert(k.clone(), v.clone());
                    }
                    result.push(combined);
                }
            } else if matches!(op.join_type, JoinType::Left | JoinType::Full) {
                result.push(probe_row.clone());
            }
        }

        Ok(QueryResult::from_rows(result))
    }

    fn exec_merge_join(
        &self,
        op: &MergeJoinOp,
        tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        // Simplified, delegate to nested loop for now
        self.exec_nested_loop_join(
            &NestedLoopJoinOp {
                outer: op.left.clone(),
                inner: op.right.clone(),
                condition: None,
                join_type: op.join_type,
                cost: op.cost.clone(),
            },
            tx_id,
            ctx,
        )
    }

    fn exec_values(&self, op: &ValuesOp) -> Result<QueryResult> {
        let rows = op
            .rows
            .iter()
            .map(|vals| {
                let mut row = Row::new();
                for (i, val) in vals.iter().enumerate() {
                    let col_name = op
                        .columns
                        .get(i)
                        .map(|c| c.as_str().to_string())
                        .unwrap_or_else(|| format!("col{}", i));
                    row.insert(col_name, val.clone());
                }
                row
            })
            .collect();

        Ok(QueryResult::from_rows(rows))
    }

    fn exec_insert(
        &self,
        op: &InsertOp,
        tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let source = self.execute(&op.source, ctx)?;
        let mut count = 0u64;
        let table = ctx.qualify_table(op.table.as_str());

        for row in source.rows {
            self.storage.insert(tx_id, &table, row)?;
            count += 1;
        }

        Ok(QueryResult::affected(count))
    }

    fn exec_update(
        &self,
        op: &UpdateOp,
        tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let scan = self.execute(&op.scan, ctx)?;
        let mut count = 0u64;
        let table = ctx.qualify_table(op.table.as_str());

        for row in scan.rows {
            // Extract key
            let key = row
                .get("_id")
                .or_else(|| row.get("id"))
                .cloned()
                .ok_or_else(|| MonoError::InvalidOperation("no key for update".into()))?;

            // Compute updates
            let mut updates = HashMap::new();
            for (col, expr) in &op.assignments {
                let val = self.eval_expr(expr, &row, ctx)?;
                updates.insert(col.as_str().to_string(), val);
            }

            if self.storage.update(tx_id, &table, &key, updates)? {
                count += 1;
            }
        }

        Ok(QueryResult::affected(count))
    }

    fn exec_delete(
        &self,
        op: &DeleteOp,
        tx_id: u64,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let scan = self.execute(&op.scan, ctx)?;
        let mut count = 0u64;
        let table = ctx.qualify_table(op.table.as_str());

        for row in scan.rows {
            let key = row
                .get("_id")
                .or_else(|| row.get("id"))
                .cloned()
                .ok_or_else(|| MonoError::InvalidOperation("no key for delete".into()))?;

            if self.storage.delete(tx_id, &table, &key)? {
                count += 1;
            }
        }

        Ok(QueryResult::affected(count))
    }

    fn eval_expr(&self, expr: &ScalarExpr, row: &Row, ctx: &ExecutionContext) -> Result<Value> {
        match expr {
            ScalarExpr::Constant(val) => Ok(val.clone()),

            ScalarExpr::Column(col) => {
                let path: Vec<&str> = std::iter::once(col.column.as_str())
                    .chain(col.path.iter().map(|i| i.as_str()))
                    .collect();

                if path.len() == 1 {
                    Ok(row.get(path[0]).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(row.get_nested(&path).cloned().unwrap_or(Value::Null))
                }
            }

            ScalarExpr::Param(p) => ctx.get_param(p).cloned(),

            ScalarExpr::BinaryOp { left, op, right } => {
                let l = self.eval_expr(left, row, ctx)?;
                let r = self.eval_expr(right, row, ctx)?;
                eval_binary_op(&l, *op, &r)
            }

            ScalarExpr::UnaryOp { op, operand } => {
                let val = self.eval_expr(operand, row, ctx)?;
                eval_unary_op(*op, &val)
            }

            ScalarExpr::FunctionCall { name, args } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.eval_expr(a, row, ctx))
                    .collect::<Result<_>>()?;
                
                // Try built-in functions first
                match eval_function(name.as_str(), &arg_vals) {
                    Ok(val) => Ok(val),
                    Err(_) => {
                        // If not a built-in, try plugin functions
                        #[cfg(feature = "plugins")]
                        if let Some(result) = ctx.call_plugin_function(name.as_str(), arg_vals) {
                            return result;
                        }
                        
                        // No matching function found
                        Err(MonoError::InvalidOperation(format!(
                            "unknown function: {}",
                            name.as_str()
                        )))
                    }
                }
            }

            ScalarExpr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let op_val = operand
                    .as_ref()
                    .map(|e| self.eval_expr(e, row, ctx))
                    .transpose()?;

                for (cond, result) in when_clauses {
                    let cond_val = self.eval_expr(cond, row, ctx)?;
                    let matches = match &op_val {
                        Some(v) => cond_val == *v,
                        None => cond_val.to_bool(),
                    };
                    if matches {
                        return self.eval_expr(result, row, ctx);
                    }
                }

                match else_result {
                    Some(e) => self.eval_expr(e, row, ctx),
                    None => Ok(Value::Null),
                }
            }

            ScalarExpr::Array(items) => {
                let vals: Vec<Value> = items
                    .iter()
                    .map(|i| self.eval_expr(i, row, ctx))
                    .collect::<Result<_>>()?;
                Ok(Value::Array(vals))
            }

            ScalarExpr::Object(fields) => {
                let mut obj = std::collections::BTreeMap::new();
                for (name, expr) in fields {
                    let val = self.eval_expr(expr, row, ctx)?;
                    obj.insert(name.as_str().to_string(), val);
                }
                Ok(Value::Object(obj))
            }

            ScalarExpr::AggregateRef { index: _ } => {
                // Aggregate refs are resolved during aggregation, not here
                Ok(Value::Null)
            }
        }
    }
}

// Operator Implementations

fn eval_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    match op {
        BinaryOp::Eq => Ok(Value::Bool(left == right)),
        BinaryOp::NotEq => Ok(Value::Bool(left != right)),
        BinaryOp::Lt => Ok(Value::Bool(compare_values(left, right) == Ordering::Less)),
        BinaryOp::Gt => Ok(Value::Bool(
            compare_values(left, right) == Ordering::Greater,
        )),
        BinaryOp::LtEq => Ok(Value::Bool(
            compare_values(left, right) != Ordering::Greater,
        )),
        BinaryOp::GtEq => Ok(Value::Bool(compare_values(left, right) != Ordering::Less)),

        BinaryOp::And => Ok(Value::Bool(left.to_bool() && right.to_bool())),
        BinaryOp::Or => Ok(Value::Bool(left.to_bool() || right.to_bool())),

        BinaryOp::Add => Ok(Value::Float64(left.to_f64() + right.to_f64())),
        BinaryOp::Sub => Ok(Value::Float64(left.to_f64() - right.to_f64())),
        BinaryOp::Mul => Ok(Value::Float64(left.to_f64() * right.to_f64())),
        BinaryOp::Div => {
            let r = right.to_f64();
            if r == 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float64(left.to_f64() / r))
            }
        }
        BinaryOp::Mod => Ok(Value::Float64(left.to_f64() % right.to_f64())),

        BinaryOp::Contains => match (left, right) {
            (Value::String(s), Value::String(sub)) => Ok(Value::Bool(s.contains(sub.as_str()))),
            (Value::Array(arr), val) => Ok(Value::Bool(arr.contains(val))),
            _ => Ok(Value::Bool(false)),
        },

        BinaryOp::Like => match (left, right) {
            (Value::String(s), Value::String(pattern)) => Ok(Value::Bool(like_match(s, pattern))),
            _ => Ok(Value::Bool(false)),
        },

        BinaryOp::In => match right {
            Value::Array(arr) => Ok(Value::Bool(arr.contains(left))),
            _ => Ok(Value::Bool(false)),
        },

        BinaryOp::PlusEq | BinaryOp::MinusEq => {
            // These are assignment operators, shouldn't appear in expressions
            Err(MonoError::InvalidOperation(
                "assignment in expression".into(),
            ))
        }
    }
}

fn eval_unary_op(op: UnaryOp, val: &Value) -> Result<Value> {
    match op {
        UnaryOp::Not => Ok(Value::Bool(!val.to_bool())),
        UnaryOp::Neg => Ok(Value::Float64(-val.to_f64())),
        UnaryOp::IsNull => Ok(Value::Bool(matches!(val, Value::Null))),
        UnaryOp::IsNotNull => Ok(Value::Bool(!matches!(val, Value::Null))),
    }
}

fn eval_function(name: &str, args: &[Value]) -> Result<Value> {
    match name.to_lowercase().as_str() {
        "upper" | "uppercase" => match args.first() {
            Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
            _ => Ok(Value::Null),
        },
        "lower" | "lowercase" => match args.first() {
            Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
            _ => Ok(Value::Null),
        },
        "length" | "len" => match args.first() {
            Some(Value::String(s)) => Ok(Value::Int64(s.len() as i64)),
            Some(Value::Array(a)) => Ok(Value::Int64(a.len() as i64)),
            _ => Ok(Value::Null),
        },
        "abs" => match args.first() {
            Some(v) => Ok(Value::Float64(v.to_f64().abs())),
            None => Ok(Value::Null),
        },
        "floor" => match args.first() {
            Some(v) => Ok(Value::Float64(v.to_f64().floor())),
            None => Ok(Value::Null),
        },
        "ceil" | "ceiling" => match args.first() {
            Some(v) => Ok(Value::Float64(v.to_f64().ceil())),
            None => Ok(Value::Null),
        },
        "round" => match args.first() {
            Some(v) => Ok(Value::Float64(v.to_f64().round())),
            None => Ok(Value::Null),
        },
        "coalesce" => {
            for arg in args {
                if !matches!(arg, Value::Null) {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "nullif" => match (args.first(), args.get(1)) {
            (Some(a), Some(b)) if a == b => Ok(Value::Null),
            (Some(a), _) => Ok(a.clone()),
            _ => Ok(Value::Null),
        },
        "concat" => {
            let mut result = String::new();
            for arg in args {
                if let Value::String(s) = arg {
                    result.push_str(s);
                }
            }
            Ok(Value::String(result))
        }
        "substring" | "substr" => match (args.first(), args.get(1), args.get(2)) {
            (Some(Value::String(s)), Some(start), len) => {
                let start = start.to_i64().max(0) as usize;
                let len = len.map(|l| l.to_i64().max(0) as usize).unwrap_or(s.len());
                Ok(Value::String(s.chars().skip(start).take(len).collect()))
            }
            _ => Ok(Value::Null),
        },
        "now" => Ok(Value::DateTime(chrono::Utc::now().fixed_offset())),
        "typeof" | "type" => match args.first() {
            Some(v) => Ok(Value::String(v.type_name().to_string())),
            None => Ok(Value::Null),
        },
        _ => Err(MonoError::InvalidOperation(format!(
            "unknown function: {}",
            name
        ))),
    }
}

/// Simple LIKE pattern matching without regex.
/// Supports % (any sequence) and _ (any single char).
fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();

    fn match_impl(s: &[char], p: &[char]) -> bool {
        match (s.first(), p.first()) {
            (_, Some('%')) => {
                // % matches zero or more characters
                match_impl(s, &p[1..]) || (!s.is_empty() && match_impl(&s[1..], p))
            }
            (Some(_), Some('_')) => {
                // _ matches exactly one character
                match_impl(&s[1..], &p[1..])
            }
            (Some(sc), Some(pc)) if sc.eq_ignore_ascii_case(pc) => match_impl(&s[1..], &p[1..]),
            (None, None) => true,
            _ => false,
        }
    }

    match_impl(&s_chars, &p_chars)
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        // Cross-type numeric comparison
        (a, b) => a
            .to_f64()
            .partial_cmp(&b.to_f64())
            .unwrap_or(Ordering::Equal),
    }
}

// Value Extensions

trait ValueExt {
    fn to_bool(&self) -> bool;
    fn to_i64(&self) -> i64;
    fn to_f64(&self) -> f64;
}

impl ValueExt for Value {
    fn to_bool(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Null => false,
            Value::Int32(n) => *n != 0,
            Value::Int64(n) => *n != 0,
            Value::Float32(n) => *n != 0.0,
            Value::Float64(n) => *n != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            _ => true,
        }
    }

    fn to_i64(&self) -> i64 {
        match self {
            Value::Int32(n) => *n as i64,
            Value::Int64(n) => *n,
            Value::Float32(n) => *n as i64,
            Value::Float64(n) => *n as i64,
            Value::Bool(b) => {
                if *b {
                    1
                } else {
                    0
                }
            }
            Value::String(s) => s.parse().unwrap_or(0),
            _ => 0,
        }
    }

    fn to_f64(&self) -> f64 {
        match self {
            Value::Int32(n) => *n as f64,
            Value::Int64(n) => *n as f64,
            Value::Float32(n) => *n as f64,
            Value::Float64(n) => *n,
            Value::Bool(b) => {
                if *b {
                    1.0
                } else {
                    0.0
                }
            }
            Value::String(s) => s.parse().unwrap_or(0.0),
            _ => 0.0,
        }
    }
}

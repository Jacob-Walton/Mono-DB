#![allow(dead_code)]

//! Query Planner
//!
//! Transforms AST into optimized physical execution plans through the following pipeline:
//! AST -> Logical Plan -> Optimization -> Physical Plan.
//!
//! Applies rule-based optimizations (predicate/projection pushdown, filter merge, etc.)
//! and cost-based optimization for join ordering and access method selection.

use crate::query_engine::ast::{
    self, BinaryOp, Expr, GetQuery, Ident, JoinType as AstJoinType, LiteralValue, QueryStatement,
    Statement, UnaryOp,
};
use crate::query_engine::logical_plan::{
    AggregateFunction, AggregateNode, DeleteNode, FilterNode, InsertNode, JoinNode, JoinType,
    LimitNode, LogicalPlan, OffsetNode, ProjectNode, ScalarExpr, ScanNode, SortKey, SortNode,
    UpdateNode, ValuesNode,
};
use crate::query_engine::physical_plan::{
    DeleteOp, FilterOp, HashAggregateOp, IndexScanOp, IndexScanType, InsertOp, LimitOp,
    NestedLoopJoinOp, OffsetOp, PhysicalPlan, PlanCost, ProjectOp, SeqScanOp, SortOp, UpdateOp,
};
use monodb_common::{MonoError, Result, Value};
use std::collections::HashSet;
use std::sync::Arc;

// Catalog Trait
/// Catalog interface for accessing table metadata and statistics.
pub trait Catalog: Send + Sync {
    /// Get the schema for a table
    fn get_table_schema(&self, name: &str) -> Option<Arc<TableSchema>>;

    /// Get available indexes for a table
    fn get_indexes(&self, table: &str) -> Vec<IndexInfo>;

    /// Get statistics for a table (for cost estimation)
    fn get_table_stats(&self, table: &str) -> Option<TableStats>;

    /// Check if a table exists
    fn table_exists(&self, name: &str) -> bool {
        self.get_table_schema(name).is_some()
    }
}

/// Schema information for a table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub table_type: ast::TableType,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
}

/// Schema information for a column.
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ast::DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}

/// Index information.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
}

/// Type of index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    Hash,
}

/// Table statistics for cost estimation.
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    pub row_count: u64,
    pub avg_row_size: usize,
    pub page_count: u64,
}

// Query Planner
pub struct QueryPlanner<C: Catalog> {
    catalog: Arc<C>,
    optimization_rules: Vec<Box<dyn OptimizationRule>>,
}

impl<C: Catalog> QueryPlanner<C> {
    /// Create a new query planner with default optimization rules.
    pub fn new(catalog: Arc<C>) -> Self {
        Self {
            catalog,
            optimization_rules: default_optimization_rules(),
        }
    }

    /// Create a planner with custom optimization rules.
    pub fn with_rules(catalog: Arc<C>, rules: Vec<Box<dyn OptimizationRule>>) -> Self {
        Self {
            catalog,
            optimization_rules: rules,
        }
    }

    /// Full planning pipeline: AST -> optimized physical plan.
    pub fn plan(&self, stmt: &Statement) -> Result<PhysicalPlan> {
        // Phase 1: AST -> Logical Plan
        let logical = self.ast_to_logical(stmt)?;

        // Phase 2: Optimize logical plan
        let optimized = self.optimize_logical(logical);

        // Phase 3: Logical -> Physical Plan
        self.logical_to_physical(&optimized)
    }

    /// Convert AST statement to logical plan.
    pub fn ast_to_logical(&self, stmt: &Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(q) => self.query_to_logical(q),
            Statement::Mutation(m) => self.mutation_to_logical(m),
            Statement::Ddl(_) => Err(MonoError::InvalidOperation(
                "DDL statements don't produce query plans".into(),
            )),
            Statement::Transaction(_) => Err(MonoError::InvalidOperation(
                "Transaction statements don't produce query plans".into(),
            )),
            Statement::Control(_) => Err(MonoError::InvalidOperation(
                "Control statements require special handling".into(),
            )),
        }
    }

    /// Apply optimization rules to a logical plan.
    pub fn optimize_logical(&self, mut plan: LogicalPlan) -> LogicalPlan {
        // Apply each rule until no more changes
        let max_iterations = 10;
        for _ in 0..max_iterations {
            let mut changed = false;
            for rule in &self.optimization_rules {
                let new_plan = rule.apply(plan.clone(), &*self.catalog);
                if !plans_equal(&new_plan, &plan) {
                    changed = true;
                    plan = new_plan;
                }
            }
            if !changed {
                break;
            }
        }
        plan
    }

    /// Convert logical plan to physical plan.
    pub fn logical_to_physical(&self, logical: &LogicalPlan) -> Result<PhysicalPlan> {
        match logical {
            LogicalPlan::Scan(scan) => self.plan_scan(scan),
            LogicalPlan::Filter(filter) => self.plan_filter(filter),
            LogicalPlan::Project(project) => self.plan_project(project),
            LogicalPlan::Sort(sort) => self.plan_sort(sort),
            LogicalPlan::Limit(limit) => self.plan_limit(limit),
            LogicalPlan::Offset(offset) => self.plan_offset(offset),
            LogicalPlan::Empty => Ok(PhysicalPlan::Empty),
            LogicalPlan::Values(values) => self.plan_values(values),
            LogicalPlan::Insert(insert) => self.plan_insert(insert),
            LogicalPlan::Update(update) => self.plan_update(update),
            LogicalPlan::Delete(delete) => self.plan_delete(delete),
            LogicalPlan::Aggregate(agg) => self.plan_aggregate(agg),
            LogicalPlan::Join(join) => self.plan_join(join),
        }
    }

    // AST -> Logical Plan Helpers

    fn query_to_logical(&self, query: &QueryStatement) -> Result<LogicalPlan> {
        match query {
            QueryStatement::Get(get) => self.get_to_logical(get),
            QueryStatement::Count(count) => {
                // COUNT is a scan with aggregation
                let mut plan = LogicalPlan::Scan(ScanNode::new(count.table.node.clone()));
                if let Some(ref filter) = count.filter {
                    plan = plan.filter(self.expr_to_scalar(&filter.node)?);
                }
                // Add count aggregation
                Ok(LogicalPlan::Aggregate(
                    crate::query_engine::logical_plan::AggregateNode {
                        input: Box::new(plan),
                        group_by: vec![],
                        aggregates: vec![crate::query_engine::logical_plan::AggregateExpr {
                            function: AggregateFunction::Count,
                            args: vec![],
                            distinct: false,
                            alias: Ident::new("count"),
                        }],
                    },
                ))
            }
            QueryStatement::Describe(_) => Err(MonoError::InvalidOperation(
                "DESCRIBE is handled specially".into(),
            )),
        }
    }

    fn get_to_logical(&self, get: &GetQuery) -> Result<LogicalPlan> {
        let base_alias = get
            .source_alias
            .as_ref()
            .map(|a| a.node.clone())
            .unwrap_or_else(|| Self::default_alias_for_table(&get.source.node));
        let use_aliases = !get.joins.is_empty() || get.source_alias.is_some();

        let mut aliases = HashSet::new();
        if use_aliases {
            aliases.insert(base_alias.clone());
        }

        let base_scan = if use_aliases {
            ScanNode::new(get.source.node.clone()).with_alias(base_alias.clone())
        } else {
            ScanNode::new(get.source.node.clone())
        };
        let mut plan = LogicalPlan::Scan(base_scan);

        for join in &get.joins {
            let join_alias = join
                .alias
                .as_ref()
                .map(|a| a.node.clone())
                .unwrap_or_else(|| Self::default_alias_for_table(&join.table.node));

            if use_aliases {
                aliases.insert(join_alias.clone());
            }

            let right_scan = if use_aliases {
                ScanNode::new(join.table.node.clone()).with_alias(join_alias.clone())
            } else {
                ScanNode::new(join.table.node.clone())
            };

            let condition = if let Some(ref cond) = join.condition {
                let expr = self.expr_to_scalar(&cond.node)?;
                Some(self.qualify_scalar_expr(expr, &aliases, &base_alias))
            } else {
                None
            };

            plan = LogicalPlan::Join(JoinNode {
                left: Box::new(plan),
                right: Box::new(LogicalPlan::Scan(right_scan)),
                join_type: Self::map_join_type(join.join_type),
                condition,
            });
        }

        // Add filter (WHERE clause) after joins
        if let Some(ref filter) = get.filter {
            let predicate = self.expr_to_scalar(&filter.node)?;
            let predicate = if use_aliases {
                self.qualify_scalar_expr(predicate, &aliases, &base_alias)
            } else {
                predicate
            };
            plan = plan.filter(predicate);
        }

        // Add projection (SELECT specific columns)
        if let Some(ref cols) = get.projection {
            let expressions: Vec<_> = cols
                .iter()
                .map(|c| {
                    let output = Ident::new(c.node.full_path());
                    let expr = ScalarExpr::Column(c.node.clone());
                    let expr = if use_aliases {
                        self.qualify_scalar_expr(expr, &aliases, &base_alias)
                    } else {
                        expr
                    };
                    (output, expr)
                })
                .collect();
            plan = plan.project(expressions);
        }

        // Add ordering (ORDER BY)
        if let Some(ref order_by) = get.order_by {
            let keys: Vec<_> = order_by
                .iter()
                .map(|o| {
                    let expr = ScalarExpr::column(o.field.node.clone());
                    let expr = if use_aliases {
                        self.qualify_scalar_expr(expr, &aliases, &base_alias)
                    } else {
                        expr
                    };
                    SortKey {
                        expr,
                        direction: o.direction,
                        nulls_first: o.direction == ast::SortDirection::Desc,
                    }
                })
                .collect();
            plan = plan.sort(keys);
        }

        // Add offset (SKIP)
        if let Some(ref offset) = get.offset {
            let count = match offset {
                ast::LimitValue::Literal(n) => *n,
                ast::LimitValue::Param(_) => 0, // Will be resolved at execution time
            };
            plan = plan.offset(count);
        }

        // Add limit (TAKE)
        if let Some(ref limit) = get.limit {
            let count = match limit {
                ast::LimitValue::Literal(n) => *n,
                ast::LimitValue::Param(_) => u64::MAX, // Will be resolved at execution time
            };
            plan = plan.limit(count);
        }

        Ok(plan)
    }

    fn default_alias_for_table(table: &Ident) -> Ident {
        let name = table.as_str();
        let alias = name.rsplit('.').next().unwrap_or(name);
        Ident::new(alias)
    }

    fn map_join_type(join_type: AstJoinType) -> JoinType {
        match join_type {
            AstJoinType::Inner => JoinType::Inner,
            AstJoinType::Left => JoinType::Left,
            AstJoinType::Right => JoinType::Right,
            AstJoinType::Full => JoinType::Full,
            AstJoinType::Cross => JoinType::Cross,
        }
    }

    fn qualify_scalar_expr(
        &self,
        expr: ScalarExpr,
        aliases: &HashSet<Ident>,
        base_alias: &Ident,
    ) -> ScalarExpr {
        match expr {
            ScalarExpr::Column(col) => {
                if col.table.is_some() || aliases.contains(&col.column) {
                    ScalarExpr::Column(col)
                } else {
                    let mut new_col =
                        crate::query_engine::ast::ColumnRef::simple(base_alias.clone());
                    new_col.path.push(col.column);
                    new_col.path.extend(col.path);
                    ScalarExpr::Column(new_col)
                }
            }
            ScalarExpr::BinaryOp { left, op, right } => ScalarExpr::BinaryOp {
                left: Box::new(self.qualify_scalar_expr(*left, aliases, base_alias)),
                op,
                right: Box::new(self.qualify_scalar_expr(*right, aliases, base_alias)),
            },
            ScalarExpr::UnaryOp { op, operand } => ScalarExpr::UnaryOp {
                op,
                operand: Box::new(self.qualify_scalar_expr(*operand, aliases, base_alias)),
            },
            ScalarExpr::FunctionCall { name, args } => ScalarExpr::FunctionCall {
                name,
                args: args
                    .into_iter()
                    .map(|a| self.qualify_scalar_expr(a, aliases, base_alias))
                    .collect(),
            },
            ScalarExpr::Case {
                operand,
                when_clauses,
                else_result,
            } => ScalarExpr::Case {
                operand: operand
                    .map(|o| Box::new(self.qualify_scalar_expr(*o, aliases, base_alias))),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|(c, r)| {
                        (
                            self.qualify_scalar_expr(c, aliases, base_alias),
                            self.qualify_scalar_expr(r, aliases, base_alias),
                        )
                    })
                    .collect(),
                else_result: else_result
                    .map(|e| Box::new(self.qualify_scalar_expr(*e, aliases, base_alias))),
            },
            ScalarExpr::Array(items) => ScalarExpr::Array(
                items
                    .into_iter()
                    .map(|i| self.qualify_scalar_expr(i, aliases, base_alias))
                    .collect(),
            ),
            ScalarExpr::Object(fields) => ScalarExpr::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, self.qualify_scalar_expr(v, aliases, base_alias)))
                    .collect(),
            ),
            other => other,
        }
    }

    fn mutation_to_logical(&self, mutation: &ast::MutationStatement) -> Result<LogicalPlan> {
        match mutation {
            ast::MutationStatement::Put(put) => {
                // Build values node from assignments
                let columns: Vec<_> = put
                    .assignments
                    .iter()
                    .map(|a| a.field.node.clone())
                    .collect();
                let values: Result<Vec<_>> = put
                    .assignments
                    .iter()
                    .map(|a| self.expr_to_scalar(&a.value.node))
                    .collect();

                Ok(LogicalPlan::Insert(InsertNode {
                    table: put.target.node.clone(),
                    columns: columns.clone(),
                    source: Box::new(LogicalPlan::Values(ValuesNode {
                        columns, // Pass column names to Values node
                        rows: vec![values?],
                    })),
                }))
            }
            ast::MutationStatement::Change(change) => {
                let assignments: Result<Vec<_>> = change
                    .assignments
                    .iter()
                    .map(|a| Ok((a.field.node.clone(), self.expr_to_scalar(&a.value.node)?)))
                    .collect();

                let filter = if let Some(ref f) = change.filter {
                    Some(Box::new(
                        LogicalPlan::Scan(ScanNode::new(change.target.node.clone()))
                            .filter(self.expr_to_scalar(&f.node)?),
                    ))
                } else {
                    None
                };

                Ok(LogicalPlan::Update(UpdateNode {
                    table: change.target.node.clone(),
                    assignments: assignments?,
                    filter,
                }))
            }
            ast::MutationStatement::Remove(remove) => {
                let predicate = self.expr_to_scalar(&remove.filter.node)?;
                let filter =
                    LogicalPlan::Scan(ScanNode::new(remove.target.node.clone())).filter(predicate);

                Ok(LogicalPlan::Delete(DeleteNode {
                    table: remove.target.node.clone(),
                    filter: Some(Box::new(filter)),
                }))
            }
        }
    }

    /// Convert AST expression to scalar expression.
    fn expr_to_scalar(&self, expr: &Expr) -> Result<ScalarExpr> {
        match expr {
            Expr::Literal(slot) => Ok(ScalarExpr::Constant(literal_to_value(&slot.value))),
            Expr::Column(col) => Ok(ScalarExpr::Column(col.clone())),
            Expr::Param(p) => Ok(ScalarExpr::Param(p.clone())),
            Expr::Binary { left, op, right } => {
                let l = self.expr_to_scalar(&left.node)?;
                let r = self.expr_to_scalar(&right.node)?;
                Ok(ScalarExpr::BinaryOp {
                    left: Box::new(l),
                    op: *op,
                    right: Box::new(r),
                })
            }
            Expr::Unary { op, operand } => {
                let o = self.expr_to_scalar(&operand.node)?;
                Ok(ScalarExpr::UnaryOp {
                    op: *op,
                    operand: Box::new(o),
                })
            }
            Expr::FunctionCall { name, args } => {
                let func_name = name.node.as_str();
                let converted_args: Result<Vec<_>> =
                    args.iter().map(|a| self.expr_to_scalar(&a.node)).collect();
                let converted_args = converted_args?;

                // Validate against built-in functions
                if let Some(builtin) = crate::query_engine::builtins::lookup_builtin(func_name) {
                    // Check arity
                    builtin
                        .check_arity(converted_args.len())
                        .map_err(MonoError::InvalidOperation)?;

                    // Note: Full type checking would require a TypeContext with column types.
                    // For now, we defer detailed type checking to execution time for columns.
                    // We can still validate constant arguments here.
                    for (i, arg) in converted_args.iter().enumerate() {
                        if let ScalarExpr::Constant(val) = arg {
                            let arg_type = val.data_type();
                            builtin
                                .check_arg_type(i, &arg_type)
                                .map_err(MonoError::InvalidOperation)?;
                        }
                    }
                }
                // If not a builtin, it might be a plugin function - validated at execution time

                Ok(ScalarExpr::FunctionCall {
                    name: name.node.clone(),
                    args: converted_args,
                })
            }
            Expr::Array(items) => {
                let converted: Result<Vec<_>> =
                    items.iter().map(|i| self.expr_to_scalar(&i.node)).collect();
                Ok(ScalarExpr::Array(converted?))
            }
            Expr::Object(fields) => {
                let converted: Result<Vec<_>> = fields
                    .iter()
                    .map(|(k, v)| Ok((k.node.clone(), self.expr_to_scalar(&v.node)?)))
                    .collect();
                Ok(ScalarExpr::Object(converted?))
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let op = if let Some(o) = operand {
                    Some(Box::new(self.expr_to_scalar(&o.node)?))
                } else {
                    None
                };
                let whens: Result<Vec<_>> = when_clauses
                    .iter()
                    .map(|w| {
                        Ok((
                            self.expr_to_scalar(&w.condition.node)?,
                            self.expr_to_scalar(&w.result.node)?,
                        ))
                    })
                    .collect();
                let else_r = if let Some(e) = else_result {
                    Some(Box::new(self.expr_to_scalar(&e.node)?))
                } else {
                    None
                };
                Ok(ScalarExpr::Case {
                    operand: op,
                    when_clauses: whens?,
                    else_result: else_r,
                })
            }
        }
    }

    // Logical -> Physical Plan Helpers

    fn plan_scan(&self, scan: &ScanNode) -> Result<PhysicalPlan> {
        let table_name = scan.table.as_str();
        let stats = self.catalog.get_table_stats(table_name).unwrap_or_default();

        // Check if we can use an index for any pushdown predicates
        if !scan.pushdown_predicates.is_empty()
            && let Some(index_plan) = self.try_index_scan(scan, &stats)
        {
            return Ok(index_plan);
        }

        // Default to sequential scan
        Ok(PhysicalPlan::SeqScan(SeqScanOp {
            table: scan.table.clone(),
            alias: scan.alias.clone(),
            filter: scan.pushdown_predicates.first().cloned(),
            columns: scan.required_columns.clone(),
            cost: PlanCost {
                io_cost: stats.page_count as f64,
                cpu_cost: stats.row_count as f64 * 0.01,
                memory_bytes: 0,
                estimated_rows: stats.row_count as f64,
            },
        }))
    }

    fn try_index_scan(&self, scan: &ScanNode, _stats: &TableStats) -> Option<PhysicalPlan> {
        let indexes = self.catalog.get_indexes(scan.table.as_str());

        // Simple heuristic: look for equality predicates on indexed columns
        for predicate in &scan.pushdown_predicates {
            if let ScalarExpr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } = predicate
            {
                // Check if left is a column and we have an index on it
                if let ScalarExpr::Column(col) = left.as_ref() {
                    for index in &indexes {
                        if index.columns.first().map(|c| c.as_str()) == Some(col.column.as_str()) {
                            return Some(PhysicalPlan::IndexScan(Box::new(IndexScanOp {
                                table: scan.table.clone(),
                                alias: scan.alias.clone(),
                                index_name: Ident::new(&index.name),
                                scan_type: IndexScanType::ExactMatch,
                                lookup_key: Some(*right.clone()),
                                range_start: None,
                                range_end: None,
                                columns: scan.required_columns.clone(),
                                residual_filter: None,
                                cost: PlanCost {
                                    io_cost: 2.0, // Index + data page
                                    cpu_cost: 1.0,
                                    memory_bytes: 0,
                                    estimated_rows: 1.0,
                                },
                            })));
                        }
                    }
                }
            }
        }

        None
    }

    fn plan_filter(&self, filter: &FilterNode) -> Result<PhysicalPlan> {
        let input = self.logical_to_physical(&filter.input)?;
        let input_cost = input.estimated_cost();

        // Estimate selectivity (default 10%)
        let selectivity = 0.1;

        Ok(PhysicalPlan::Filter(FilterOp {
            input: Box::new(input),
            predicate: filter.predicate.clone(),
            cost: PlanCost {
                io_cost: input_cost.io_cost,
                cpu_cost: input_cost.cpu_cost + input_cost.estimated_rows * 0.001,
                memory_bytes: 0,
                estimated_rows: input_cost.estimated_rows * selectivity,
            },
        }))
    }

    fn plan_project(&self, project: &ProjectNode) -> Result<PhysicalPlan> {
        let input = self.logical_to_physical(&project.input)?;
        let input_cost = input.estimated_cost();

        Ok(PhysicalPlan::Project(ProjectOp {
            input: Box::new(input),
            expressions: project.expressions.clone(),
            cost: PlanCost {
                io_cost: input_cost.io_cost,
                cpu_cost: input_cost.cpu_cost
                    + input_cost.estimated_rows * project.expressions.len() as f64 * 0.0001,
                memory_bytes: 0,
                estimated_rows: input_cost.estimated_rows,
            },
        }))
    }

    fn plan_sort(&self, sort: &SortNode) -> Result<PhysicalPlan> {
        let input = self.logical_to_physical(&sort.input)?;
        let input_cost = input.estimated_cost();

        // n log n cost for sorting
        let n = input_cost.estimated_rows;
        let sort_cost = if n > 0.0 { n * n.log2() * 0.01 } else { 0.0 };

        Ok(PhysicalPlan::Sort(SortOp {
            input: Box::new(input),
            order_by: sort.order_by.clone(),
            limit: None,
            external: input_cost.estimated_rows > 100_000.0,
            cost: PlanCost {
                io_cost: input_cost.io_cost,
                cpu_cost: input_cost.cpu_cost + sort_cost,
                memory_bytes: (input_cost.estimated_rows * 100.0) as usize, // Rough estimate
                estimated_rows: input_cost.estimated_rows,
            },
        }))
    }

    fn plan_limit(&self, limit: &LimitNode) -> Result<PhysicalPlan> {
        let input = self.logical_to_physical(&limit.input)?;
        let input_cost = input.estimated_cost();

        let count = match &limit.count {
            ScalarExpr::Constant(Value::Int64(n)) => *n as usize,
            _ => usize::MAX,
        };

        Ok(PhysicalPlan::Limit(LimitOp {
            input: Box::new(input),
            count,
            cost: PlanCost {
                io_cost: input_cost.io_cost,
                cpu_cost: input_cost.cpu_cost,
                memory_bytes: 0,
                estimated_rows: (count as f64).min(input_cost.estimated_rows),
            },
        }))
    }

    fn plan_offset(&self, offset: &OffsetNode) -> Result<PhysicalPlan> {
        let input = self.logical_to_physical(&offset.input)?;
        let input_cost = input.estimated_cost();

        let count = match &offset.count {
            ScalarExpr::Constant(Value::Int64(n)) => *n as usize,
            _ => 0,
        };

        Ok(PhysicalPlan::Offset(OffsetOp {
            input: Box::new(input),
            count,
            cost: PlanCost {
                io_cost: input_cost.io_cost,
                cpu_cost: input_cost.cpu_cost,
                memory_bytes: 0,
                estimated_rows: (input_cost.estimated_rows - count as f64).max(0.0),
            },
        }))
    }

    fn plan_values(&self, values: &ValuesNode) -> Result<PhysicalPlan> {
        use crate::query_engine::physical_plan::ValuesOp;

        // Pass through ScalarExpr rows directly, they will be evaluated at execution time
        // This allows parameterized queries ($1, $2, etc.) to work with INSERT/VALUES
        Ok(PhysicalPlan::Values(ValuesOp {
            columns: values.columns.clone(),
            rows: values.rows.clone(),
        }))
    }

    fn plan_insert(&self, insert: &InsertNode) -> Result<PhysicalPlan> {
        let source = self.logical_to_physical(&insert.source)?;
        let source_cost = source.estimated_cost();

        Ok(PhysicalPlan::Insert(InsertOp {
            table: insert.table.clone(),
            columns: insert.columns.clone(),
            source: Box::new(source),
            cost: PlanCost {
                io_cost: source_cost.estimated_rows * 10.0, // Write cost
                cpu_cost: source_cost.cpu_cost + source_cost.estimated_rows,
                memory_bytes: 0,
                estimated_rows: source_cost.estimated_rows,
            },
        }))
    }

    fn plan_update(&self, update: &UpdateNode) -> Result<PhysicalPlan> {
        let scan = if let Some(ref filter) = update.filter {
            self.logical_to_physical(filter)?
        } else {
            // Full table scan if no filter
            self.plan_scan(&ScanNode::new(update.table.clone()))?
        };
        let scan_cost = scan.estimated_cost();

        Ok(PhysicalPlan::Update(UpdateOp {
            table: update.table.clone(),
            scan: Box::new(scan),
            assignments: update.assignments.clone(),
            cost: PlanCost {
                io_cost: scan_cost.io_cost + scan_cost.estimated_rows * 10.0,
                cpu_cost: scan_cost.cpu_cost + scan_cost.estimated_rows,
                memory_bytes: 0,
                estimated_rows: scan_cost.estimated_rows,
            },
        }))
    }

    fn plan_delete(&self, delete: &DeleteNode) -> Result<PhysicalPlan> {
        let scan = if let Some(ref filter) = delete.filter {
            self.logical_to_physical(filter)?
        } else {
            // Full table scan if no filter
            self.plan_scan(&ScanNode::new(delete.table.clone()))?
        };
        let scan_cost = scan.estimated_cost();

        Ok(PhysicalPlan::Delete(DeleteOp {
            table: delete.table.clone(),
            scan: Box::new(scan),
            cost: PlanCost {
                io_cost: scan_cost.io_cost + scan_cost.estimated_rows * 5.0,
                cpu_cost: scan_cost.cpu_cost,
                memory_bytes: 0,
                estimated_rows: scan_cost.estimated_rows,
            },
        }))
    }

    fn plan_aggregate(&self, agg: &AggregateNode) -> Result<PhysicalPlan> {
        use crate::query_engine::physical_plan::PhysicalAggregate;

        let input = self.logical_to_physical(&agg.input)?;
        let input_cost = input.estimated_cost();

        // Convert logical AggregateExpr to PhysicalAggregate
        let aggregates: Vec<PhysicalAggregate> = agg
            .aggregates
            .iter()
            .map(|a| PhysicalAggregate {
                function: a.function,
                args: a.args.clone(),
                distinct: a.distinct,
                output_name: a.alias.clone(),
            })
            .collect();

        Ok(PhysicalPlan::HashAggregate(HashAggregateOp {
            input: Box::new(input),
            group_by: agg.group_by.clone(),
            aggregates,
            cost: PlanCost {
                io_cost: input_cost.io_cost,
                cpu_cost: input_cost.cpu_cost + input_cost.estimated_rows * 2.0,
                memory_bytes: (input_cost.estimated_rows * 100.0) as usize,
                estimated_rows: if agg.group_by.is_empty() {
                    1.0
                } else {
                    input_cost.estimated_rows * 0.1 // Rough estimate
                },
            },
        }))
    }

    fn plan_join(&self, join: &JoinNode) -> Result<PhysicalPlan> {
        let left = self.logical_to_physical(&join.left)?;
        let right = self.logical_to_physical(&join.right)?;
        let left_cost = left.estimated_cost();
        let right_cost = right.estimated_cost();

        // Use nested loop join, outer is the left, inner is the right
        Ok(PhysicalPlan::NestedLoopJoin(NestedLoopJoinOp {
            outer: Box::new(left),
            inner: Box::new(right),
            condition: join.condition.clone(),
            join_type: join.join_type,
            cost: PlanCost {
                io_cost: left_cost.io_cost + right_cost.io_cost,
                cpu_cost: left_cost.estimated_rows * right_cost.estimated_rows,
                memory_bytes: (right_cost.estimated_rows * 100.0) as usize,
                estimated_rows: left_cost.estimated_rows * right_cost.estimated_rows * 0.1,
            },
        }))
    }
}

// Optimization Rules
pub trait OptimizationRule: Send + Sync {
    /// Name of this rule (for debugging/logging)
    fn name(&self) -> &'static str;

    /// Apply the rule to a plan, returning a potentially transformed plan.
    fn apply(&self, plan: LogicalPlan, catalog: &dyn Catalog) -> LogicalPlan;
}

/// Get the default set of optimization rules.
pub fn default_optimization_rules() -> Vec<Box<dyn OptimizationRule>> {
    vec![
        Box::new(ConstantFoldingRule),
        Box::new(PredicatePushdownRule),
        Box::new(FilterMergeRule),
        Box::new(LimitPushdownRule),
    ]
}

/// Constant folding: evaluate constant expressions at plan time.
struct ConstantFoldingRule;

impl ConstantFoldingRule {
    /// Recursively fold constants in an expression.
    fn fold_expr(&self, expr: ScalarExpr) -> ScalarExpr {
        match expr {
            ScalarExpr::BinaryOp { left, op, right } => {
                let left = self.fold_expr(*left);
                let right = self.fold_expr(*right);

                // Try to evaluate if both sides are constants
                if let (ScalarExpr::Constant(l), ScalarExpr::Constant(r)) = (&left, &right)
                    && let Some(result) = self.eval_binary(l, &op, r)
                {
                    return ScalarExpr::Constant(result);
                }

                // Logical simplifications
                match op {
                    BinaryOp::And => {
                        // false AND x -> false
                        if matches!(&left, ScalarExpr::Constant(Value::Bool(false))) {
                            return ScalarExpr::Constant(Value::Bool(false));
                        }
                        if matches!(&right, ScalarExpr::Constant(Value::Bool(false))) {
                            return ScalarExpr::Constant(Value::Bool(false));
                        }
                        // true AND x -> x
                        if matches!(&left, ScalarExpr::Constant(Value::Bool(true))) {
                            return right;
                        }
                        if matches!(&right, ScalarExpr::Constant(Value::Bool(true))) {
                            return left;
                        }
                    }
                    BinaryOp::Or => {
                        // true OR x -> true
                        if matches!(&left, ScalarExpr::Constant(Value::Bool(true))) {
                            return ScalarExpr::Constant(Value::Bool(true));
                        }
                        if matches!(&right, ScalarExpr::Constant(Value::Bool(true))) {
                            return ScalarExpr::Constant(Value::Bool(true));
                        }
                        // false OR x -> x
                        if matches!(&left, ScalarExpr::Constant(Value::Bool(false))) {
                            return right;
                        }
                        if matches!(&right, ScalarExpr::Constant(Value::Bool(false))) {
                            return left;
                        }
                    }
                    _ => {}
                }

                ScalarExpr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }
            }
            ScalarExpr::UnaryOp { op, operand } => {
                let operand = self.fold_expr(*operand);
                if let ScalarExpr::Constant(v) = &operand
                    && let Some(result) = self.eval_unary(&op, v)
                {
                    return ScalarExpr::Constant(result);
                }
                ScalarExpr::UnaryOp {
                    op,
                    operand: Box::new(operand),
                }
            }
            ScalarExpr::FunctionCall { name, args } => ScalarExpr::FunctionCall {
                name,
                args: args.into_iter().map(|a| self.fold_expr(a)).collect(),
            },
            ScalarExpr::Case {
                operand,
                when_clauses,
                else_result,
            } => ScalarExpr::Case {
                operand: operand.map(|o| Box::new(self.fold_expr(*o))),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|(w, t)| (self.fold_expr(w), self.fold_expr(t)))
                    .collect(),
                else_result: else_result.map(|e| Box::new(self.fold_expr(*e))),
            },
            ScalarExpr::Array(items) => {
                ScalarExpr::Array(items.into_iter().map(|i| self.fold_expr(i)).collect())
            }
            ScalarExpr::Object(fields) => ScalarExpr::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, self.fold_expr(v)))
                    .collect(),
            ),
            other => other,
        }
    }

    /// Evaluate a binary operation on constants.
    fn eval_binary(&self, left: &Value, op: &BinaryOp, right: &Value) -> Option<Value> {
        match op {
            // Arithmetic
            BinaryOp::Add => match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a + b)),
                (Value::Float64(a), Value::Float64(b)) => Some(Value::Float64(a + b)),
                (Value::Int64(a), Value::Float64(b)) => Some(Value::Float64(*a as f64 + b)),
                (Value::Float64(a), Value::Int64(b)) => Some(Value::Float64(a + *b as f64)),
                (Value::String(a), Value::String(b)) => Some(Value::String(format!("{}{}", a, b))),
                _ => None,
            },
            BinaryOp::Sub => match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a - b)),
                (Value::Float64(a), Value::Float64(b)) => Some(Value::Float64(a - b)),
                _ => None,
            },
            BinaryOp::Mul => match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a * b)),
                (Value::Float64(a), Value::Float64(b)) => Some(Value::Float64(a * b)),
                _ => None,
            },
            BinaryOp::Div => match (left, right) {
                (Value::Int64(a), Value::Int64(b)) if *b != 0 => Some(Value::Int64(a / b)),
                (Value::Float64(a), Value::Float64(b)) if *b != 0.0 => Some(Value::Float64(a / b)),
                _ => None,
            },
            BinaryOp::Mod => match (left, right) {
                (Value::Int64(a), Value::Int64(b)) if *b != 0 => Some(Value::Int64(a % b)),
                _ => None,
            },
            // Comparison
            BinaryOp::Eq => Some(Value::Bool(left == right)),
            BinaryOp::NotEq => Some(Value::Bool(left != right)),
            BinaryOp::Lt => self
                .compare_values(left, right)
                .map(|o| Value::Bool(o.is_lt())),
            BinaryOp::Gt => self
                .compare_values(left, right)
                .map(|o| Value::Bool(o.is_gt())),
            BinaryOp::LtEq => self
                .compare_values(left, right)
                .map(|o| Value::Bool(o.is_le())),
            BinaryOp::GtEq => self
                .compare_values(left, right)
                .map(|o| Value::Bool(o.is_ge())),
            // Logical
            BinaryOp::And => match (left, right) {
                (Value::Bool(a), Value::Bool(b)) => Some(Value::Bool(*a && *b)),
                _ => None,
            },
            BinaryOp::Or => match (left, right) {
                (Value::Bool(a), Value::Bool(b)) => Some(Value::Bool(*a || *b)),
                _ => None,
            },
            _ => None,
        }
    }

    /// Compare two values.
    fn compare_values(&self, left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
        match (left, right) {
            (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Evaluate a unary operation on a constant.
    fn eval_unary(&self, op: &UnaryOp, value: &Value) -> Option<Value> {
        match op {
            UnaryOp::Not => match value {
                Value::Bool(b) => Some(Value::Bool(!b)),
                _ => None,
            },
            UnaryOp::Neg => match value {
                Value::Int64(n) => Some(Value::Int64(-n)),
                Value::Float64(f) => Some(Value::Float64(-f)),
                _ => None,
            },
            _ => None,
        }
    }

    /// Apply constant folding to all expressions in a plan node.
    fn fold_plan(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter(FilterNode { input, predicate }) => {
                let folded_pred = self.fold_expr(predicate);
                // If predicate folds to true, eliminate filter
                if matches!(&folded_pred, ScalarExpr::Constant(Value::Bool(true))) {
                    return self.fold_plan(*input);
                }
                // If predicate folds to false, return empty
                if matches!(&folded_pred, ScalarExpr::Constant(Value::Bool(false))) {
                    return LogicalPlan::Empty;
                }
                LogicalPlan::Filter(FilterNode {
                    input: Box::new(self.fold_plan(*input)),
                    predicate: folded_pred,
                })
            }
            LogicalPlan::Project(ProjectNode { input, expressions }) => {
                LogicalPlan::Project(ProjectNode {
                    input: Box::new(self.fold_plan(*input)),
                    expressions: expressions
                        .into_iter()
                        .map(|(name, expr)| (name, self.fold_expr(expr)))
                        .collect(),
                })
            }
            LogicalPlan::Sort(SortNode { input, order_by }) => LogicalPlan::Sort(SortNode {
                input: Box::new(self.fold_plan(*input)),
                order_by: order_by
                    .into_iter()
                    .map(|sk| SortKey {
                        expr: self.fold_expr(sk.expr),
                        ..sk
                    })
                    .collect(),
            }),
            LogicalPlan::Limit(LimitNode { input, count }) => LogicalPlan::Limit(LimitNode {
                input: Box::new(self.fold_plan(*input)),
                count,
            }),
            LogicalPlan::Offset(OffsetNode { input, count }) => LogicalPlan::Offset(OffsetNode {
                input: Box::new(self.fold_plan(*input)),
                count,
            }),
            LogicalPlan::Aggregate(AggregateNode {
                input,
                group_by,
                aggregates,
            }) => LogicalPlan::Aggregate(AggregateNode {
                input: Box::new(self.fold_plan(*input)),
                group_by: group_by.into_iter().map(|e| self.fold_expr(e)).collect(),
                aggregates,
            }),
            other => other,
        }
    }
}

impl OptimizationRule for ConstantFoldingRule {
    fn name(&self) -> &'static str {
        "constant_folding"
    }

    fn apply(&self, plan: LogicalPlan, _catalog: &dyn Catalog) -> LogicalPlan {
        self.fold_plan(plan)
    }
}

/// Predicate pushdown: move filters closer to data sources.
struct PredicatePushdownRule;

impl OptimizationRule for PredicatePushdownRule {
    fn name(&self) -> &'static str {
        "predicate_pushdown"
    }

    fn apply(&self, plan: LogicalPlan, _catalog: &dyn Catalog) -> LogicalPlan {
        match plan {
            // Push filter into scan
            LogicalPlan::Filter(FilterNode { input, predicate }) => {
                if let LogicalPlan::Scan(mut scan) = *input {
                    scan.pushdown_predicates.push(predicate);
                    LogicalPlan::Scan(scan)
                } else {
                    // Recursively apply to input
                    let optimized_input = self.apply(*input, _catalog);
                    LogicalPlan::Filter(FilterNode {
                        input: Box::new(optimized_input),
                        predicate,
                    })
                }
            }
            // Recurse into other nodes
            LogicalPlan::Project(ProjectNode { input, expressions }) => {
                LogicalPlan::Project(ProjectNode {
                    input: Box::new(self.apply(*input, _catalog)),
                    expressions,
                })
            }
            LogicalPlan::Sort(SortNode { input, order_by }) => LogicalPlan::Sort(SortNode {
                input: Box::new(self.apply(*input, _catalog)),
                order_by,
            }),
            LogicalPlan::Limit(LimitNode { input, count }) => LogicalPlan::Limit(LimitNode {
                input: Box::new(self.apply(*input, _catalog)),
                count,
            }),
            LogicalPlan::Offset(OffsetNode { input, count }) => LogicalPlan::Offset(OffsetNode {
                input: Box::new(self.apply(*input, _catalog)),
                count,
            }),
            other => other,
        }
    }
}

/// Filter merge: combine adjacent filters.
struct FilterMergeRule;

impl OptimizationRule for FilterMergeRule {
    fn name(&self) -> &'static str {
        "filter_merge"
    }

    fn apply(&self, plan: LogicalPlan, _catalog: &dyn Catalog) -> LogicalPlan {
        match plan {
            // Filter(Filter(x, a), b) → Filter(x, a AND b)
            LogicalPlan::Filter(FilterNode {
                input,
                predicate: outer_pred,
            }) => {
                if let LogicalPlan::Filter(FilterNode {
                    input: inner_input,
                    predicate: inner_pred,
                }) = *input
                {
                    LogicalPlan::Filter(FilterNode {
                        input: inner_input,
                        predicate: inner_pred.and(outer_pred),
                    })
                } else {
                    LogicalPlan::Filter(FilterNode {
                        input: Box::new(self.apply(*input, _catalog)),
                        predicate: outer_pred,
                    })
                }
            }
            other => other,
        }
    }
}

/// Limit pushdown: optimize top-N queries.
struct LimitPushdownRule;

impl LimitPushdownRule {
    /// Try to extract a constant limit value from a ScalarExpr.
    fn get_const_limit(expr: &ScalarExpr) -> Option<i64> {
        match expr {
            ScalarExpr::Constant(Value::Int64(n)) => Some(*n),
            ScalarExpr::Constant(Value::Int32(n)) => Some(*n as i64),
            _ => None,
        }
    }

    /// Recursively apply limit pushdown.
    fn push_limit(&self, plan: LogicalPlan, limit: Option<ScalarExpr>) -> LogicalPlan {
        match plan {
            // Limit(Limit(x, a), b) -> Limit(x, min(a, b)) if both are constants
            LogicalPlan::Limit(LimitNode { input, count }) => {
                let new_limit = match (&limit, Self::get_const_limit(&count)) {
                    (Some(outer), Some(inner)) => {
                        if let Some(outer_val) = Self::get_const_limit(outer) {
                            Some(ScalarExpr::Constant(Value::Int64(outer_val.min(inner))))
                        } else {
                            Some(count) // Keep inner if outer isn't constant
                        }
                    }
                    (None, _) => Some(count),
                    (Some(_), None) => limit, // Keep outer if inner isn't constant
                };
                self.push_limit(*input, new_limit)
            }
            // Limit can be pushed through Project (doesn't change row count)
            LogicalPlan::Project(ProjectNode { input, expressions }) => {
                let pushed = self.push_limit(*input, limit);
                LogicalPlan::Project(ProjectNode {
                    input: Box::new(pushed),
                    expressions,
                })
            }
            // Limit(Sort(x)), keep limit above sort for top-N optimization
            LogicalPlan::Sort(SortNode { input, order_by }) => {
                let pushed = self.push_limit(*input, None);
                let sorted = LogicalPlan::Sort(SortNode {
                    input: Box::new(pushed),
                    order_by,
                });
                if let Some(l) = limit {
                    LogicalPlan::Limit(LimitNode {
                        input: Box::new(sorted),
                        count: l,
                    })
                } else {
                    sorted
                }
            }
            // For scan, apply limit at this level
            LogicalPlan::Scan(scan) => {
                let result = LogicalPlan::Scan(scan);
                if let Some(l) = limit {
                    LogicalPlan::Limit(LimitNode {
                        input: Box::new(result),
                        count: l,
                    })
                } else {
                    result
                }
            }
            // Filter, don't push limit through
            LogicalPlan::Filter(FilterNode { input, predicate }) => {
                let pushed = self.push_limit(*input, None);
                let filtered = LogicalPlan::Filter(FilterNode {
                    input: Box::new(pushed),
                    predicate,
                });
                if let Some(l) = limit {
                    LogicalPlan::Limit(LimitNode {
                        input: Box::new(filtered),
                        count: l,
                    })
                } else {
                    filtered
                }
            }
            // For other nodes, apply limit at current level
            other => {
                if let Some(l) = limit {
                    LogicalPlan::Limit(LimitNode {
                        input: Box::new(other),
                        count: l,
                    })
                } else {
                    other
                }
            }
        }
    }
}

impl OptimizationRule for LimitPushdownRule {
    fn name(&self) -> &'static str {
        "limit_pushdown"
    }

    fn apply(&self, plan: LogicalPlan, _catalog: &dyn Catalog) -> LogicalPlan {
        self.push_limit(plan, None)
    }
}

// Helper Functions
fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Null => Value::Null,
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Int64(n) => Value::Int64(*n),
        LiteralValue::Float64(f) => Value::Float64(*f),
        LiteralValue::String(s) => Value::String(s.to_string()),
        LiteralValue::Bytes(b) => Value::Binary(b.to_vec()),
    }
}

/// Structural equality check for logical plans.
fn plans_equal(a: &LogicalPlan, b: &LogicalPlan) -> bool {
    match (a, b) {
        (LogicalPlan::Scan(a), LogicalPlan::Scan(b)) => {
            a.table == b.table
                && a.alias == b.alias
                && a.required_columns == b.required_columns
                && a.pushdown_predicates.len() == b.pushdown_predicates.len()
                && a.pushdown_predicates
                    .iter()
                    .zip(&b.pushdown_predicates)
                    .all(|(x, y)| exprs_equal(x, y))
        }
        (LogicalPlan::Filter(a), LogicalPlan::Filter(b)) => {
            plans_equal(&a.input, &b.input) && exprs_equal(&a.predicate, &b.predicate)
        }
        (LogicalPlan::Project(a), LogicalPlan::Project(b)) => {
            plans_equal(&a.input, &b.input)
                && a.expressions.len() == b.expressions.len()
                && a.expressions
                    .iter()
                    .zip(&b.expressions)
                    .all(|((n1, e1), (n2, e2))| n1 == n2 && exprs_equal(e1, e2))
        }
        (LogicalPlan::Sort(a), LogicalPlan::Sort(b)) => {
            plans_equal(&a.input, &b.input)
                && a.order_by.len() == b.order_by.len()
                && a.order_by.iter().zip(&b.order_by).all(|(x, y)| {
                    exprs_equal(&x.expr, &y.expr)
                        && x.direction == y.direction
                        && x.nulls_first == y.nulls_first
                })
        }
        (LogicalPlan::Limit(a), LogicalPlan::Limit(b)) => {
            exprs_equal(&a.count, &b.count) && plans_equal(&a.input, &b.input)
        }
        (LogicalPlan::Offset(a), LogicalPlan::Offset(b)) => {
            exprs_equal(&a.count, &b.count) && plans_equal(&a.input, &b.input)
        }
        (LogicalPlan::Aggregate(a), LogicalPlan::Aggregate(b)) => {
            plans_equal(&a.input, &b.input)
                && a.group_by.len() == b.group_by.len()
                && a.group_by
                    .iter()
                    .zip(&b.group_by)
                    .all(|(x, y)| exprs_equal(x, y))
                && a.aggregates.len() == b.aggregates.len()
        }
        (LogicalPlan::Join(a), LogicalPlan::Join(b)) => {
            plans_equal(&a.left, &b.left)
                && plans_equal(&a.right, &b.right)
                && a.join_type == b.join_type
                && match (&a.condition, &b.condition) {
                    (Some(x), Some(y)) => exprs_equal(x, y),
                    (None, None) => true,
                    _ => false,
                }
        }
        (LogicalPlan::Values(a), LogicalPlan::Values(b)) => {
            a.columns == b.columns
                && a.rows.len() == b.rows.len()
                && a.rows.iter().zip(&b.rows).all(|(r1, r2)| {
                    r1.len() == r2.len() && r1.iter().zip(r2).all(|(e1, e2)| exprs_equal(e1, e2))
                })
        }
        (LogicalPlan::Empty, LogicalPlan::Empty) => true,
        (LogicalPlan::Insert(a), LogicalPlan::Insert(b)) => {
            a.table == b.table && a.columns == b.columns && plans_equal(&a.source, &b.source)
        }
        (LogicalPlan::Update(a), LogicalPlan::Update(b)) => {
            a.table == b.table
                && a.assignments.len() == b.assignments.len()
                && a.assignments
                    .iter()
                    .zip(&b.assignments)
                    .all(|((n1, e1), (n2, e2))| n1 == n2 && exprs_equal(e1, e2))
        }
        (LogicalPlan::Delete(a), LogicalPlan::Delete(b)) => a.table == b.table,
        _ => false,
    }
}

/// Check if two scalar expressions are structurally equal.
fn exprs_equal(a: &ScalarExpr, b: &ScalarExpr) -> bool {
    match (a, b) {
        (ScalarExpr::Column(a), ScalarExpr::Column(b)) => a == b,
        (ScalarExpr::Constant(a), ScalarExpr::Constant(b)) => a == b,
        (ScalarExpr::Param(a), ScalarExpr::Param(b)) => a == b,
        (
            ScalarExpr::BinaryOp {
                left: l1,
                op: o1,
                right: r1,
            },
            ScalarExpr::BinaryOp {
                left: l2,
                op: o2,
                right: r2,
            },
        ) => o1 == o2 && exprs_equal(l1, l2) && exprs_equal(r1, r2),
        (
            ScalarExpr::UnaryOp {
                op: o1,
                operand: e1,
            },
            ScalarExpr::UnaryOp {
                op: o2,
                operand: e2,
            },
        ) => o1 == o2 && exprs_equal(e1, e2),
        (
            ScalarExpr::FunctionCall { name: n1, args: a1 },
            ScalarExpr::FunctionCall { name: n2, args: a2 },
        ) => n1 == n2 && a1.len() == a2.len() && a1.iter().zip(a2).all(|(x, y)| exprs_equal(x, y)),
        (ScalarExpr::Array(a), ScalarExpr::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| exprs_equal(x, y))
        }
        (ScalarExpr::Object(a), ScalarExpr::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && exprs_equal(v1, v2))
        }
        (ScalarExpr::AggregateRef { index: a }, ScalarExpr::AggregateRef { index: b }) => a == b,
        _ => false,
    }
}

// Empty Catalog (for testing)
pub struct EmptyCatalog;

impl Catalog for EmptyCatalog {
    fn get_table_schema(&self, _name: &str) -> Option<Arc<TableSchema>> {
        None
    }

    fn get_indexes(&self, _table: &str) -> Vec<IndexInfo> {
        Vec::new()
    }

    fn get_table_stats(&self, _table: &str) -> Option<TableStats> {
        None
    }
}

/// Storage-backed catalog that reads from the SchemaCatalog.
///
/// This is what connects the query planner to the actual
/// persisted table schemas.
pub struct StorageCatalog {
    schema_catalog: Arc<crate::storage::SchemaCatalog>,
    engine: Option<Arc<crate::storage::StorageEngine>>,
}

impl StorageCatalog {
    /// Create a new storage catalog backed by the given schema catalog.
    pub fn new(schema_catalog: Arc<crate::storage::SchemaCatalog>) -> Self {
        Self {
            schema_catalog,
            engine: None,
        }
    }

    /// Create a storage catalog with access to the storage engine for statistics.
    pub fn with_engine(
        schema_catalog: Arc<crate::storage::SchemaCatalog>,
        engine: Arc<crate::storage::StorageEngine>,
    ) -> Self {
        Self {
            schema_catalog,
            engine: Some(engine),
        }
    }
}

impl Catalog for StorageCatalog {
    fn get_table_schema(&self, name: &str) -> Option<Arc<TableSchema>> {
        let stored = self.schema_catalog.get(name)?;

        // Convert StoredTableSchema to TableSchema
        let columns = stored
            .columns
            .iter()
            .map(|col| ColumnSchema {
                name: col.name.clone(),
                data_type: col.data_type.to_ast_type(),
                nullable: col.nullable,
                default: col.default.clone(),
            })
            .collect();

        Some(Arc::new(TableSchema {
            name: stored.name.clone(),
            table_type: stored.table_type.into(),
            columns,
            primary_key: stored.primary_key.clone(),
        }))
    }

    fn get_indexes(&self, table: &str) -> Vec<IndexInfo> {
        let Some(stored) = self.schema_catalog.get(table) else {
            return Vec::new();
        };

        stored
            .indexes
            .iter()
            .map(|idx| IndexInfo {
                name: idx.name.clone(),
                columns: idx.columns.clone(),
                unique: idx.unique,
                index_type: match idx.index_type {
                    crate::storage::schema::StoredIndexType::BTree => IndexType::BTree,
                    crate::storage::schema::StoredIndexType::Hash => IndexType::Hash,
                    _ => IndexType::BTree, // Default for FullText/Spatial
                },
            })
            .collect()
    }

    fn get_table_stats(&self, table: &str) -> Option<TableStats> {
        let stored = self.schema_catalog.get(table)?;

        // Estimate row size from column types
        let avg_row_size: usize = stored
            .columns
            .iter()
            .map(|col| col.data_type.estimated_size())
            .sum();

        // Get actual row count from engine if available
        let row_count = self
            .engine
            .as_ref()
            .and_then(|e| e.get_table_row_count(table))
            .unwrap_or(0);

        // Estimate page count from row count and row size
        const PAGE_SIZE: usize = 16384;
        let rows_per_page = if avg_row_size > 0 {
            PAGE_SIZE / avg_row_size
        } else {
            100
        };
        let page_count = if rows_per_page > 0 {
            (row_count as usize).div_ceil(rows_per_page)
        } else {
            0
        };

        Some(TableStats {
            row_count,
            avg_row_size,
            page_count: page_count as u64,
        })
    }
}

// Tests

#[cfg(test)]
mod tests {
    use crate::query_engine::ast::Spanned;

    use super::*;

    #[test]
    fn test_simple_scan_plan() {
        let catalog = Arc::new(EmptyCatalog);
        let planner = QueryPlanner::new(catalog);

        // Create a simple GET query AST
        let get = GetQuery {
            source: Spanned::dummy(Ident::new("users")),
            source_alias: None,
            joins: Vec::new(),
            filter: None,
            projection: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let logical = planner.get_to_logical(&get).unwrap();
        assert!(matches!(logical, LogicalPlan::Scan(_)));

        let physical = planner.logical_to_physical(&logical).unwrap();
        assert!(matches!(physical, PhysicalPlan::SeqScan(_)));
    }
}

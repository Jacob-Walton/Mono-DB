#![allow(dead_code)]

//! Query Planner
//!
//! Transforms AST into optimized physical execution plans through the following pipeline:
//! AST -> Logical Plan -> Optimization -> Physical Plan.
//!
//! Applies rule-based optimizations (predicate/projection pushdown, filter merge, etc.)
//! and cost-based optimization for join ordering and access method selection.

use crate::query_engine::ast::{
    self, BinaryOp, Expr, GetQuery, Ident, LiteralValue, QueryStatement, Statement,
};
use crate::query_engine::logical_plan::{
    AggregateFunction, AggregateNode, DeleteNode, FilterNode, InsertNode, JoinNode, LimitNode,
    LogicalPlan, OffsetNode, ProjectNode, ScalarExpr, ScanNode, SortKey, SortNode, UpdateNode,
    ValuesNode,
};
use crate::query_engine::physical_plan::{
    DeleteOp, FilterOp, HashAggregateOp, IndexScanOp, IndexScanType, InsertOp, LimitOp,
    NestedLoopJoinOp, OffsetOp, PhysicalPlan, PlanCost, ProjectOp, SeqScanOp, SortOp, UpdateOp,
};
use monodb_common::{MonoError, Result, Value};
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
        // Start with table scan
        let mut plan = LogicalPlan::Scan(ScanNode::new(get.source.node.clone()));

        // Add filter (WHERE clause)
        if let Some(ref filter) = get.filter {
            let predicate = self.expr_to_scalar(&filter.node)?;
            plan = plan.filter(predicate);
        }

        // Add projection (SELECT specific columns)
        if let Some(ref cols) = get.projection {
            let expressions: Vec<_> = cols
                .iter()
                .map(|c| (c.node.clone(), ScalarExpr::column(c.node.clone())))
                .collect();
            plan = plan.project(expressions);
        }

        // Add ordering (ORDER BY)
        if let Some(ref order_by) = get.order_by {
            let keys: Vec<_> = order_by
                .iter()
                .map(|o| SortKey {
                    expr: ScalarExpr::column(o.field.node.clone()),
                    direction: o.direction,
                    nulls_first: o.direction == ast::SortDirection::Desc,
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

        // Convert ScalarExpr rows to Value rows by evaluating constants
        let rows: Result<Vec<Vec<Value>>> = values
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| match expr {
                        ScalarExpr::Constant(v) => Ok(v.clone()),
                        _ => Err(MonoError::InvalidOperation(
                            "Non-constant values not supported in VALUES".into(),
                        )),
                    })
                    .collect()
            })
            .collect();

        Ok(PhysicalPlan::Values(ValuesOp {
            columns: values.columns.clone(),
            rows: rows?,
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

impl OptimizationRule for ConstantFoldingRule {
    fn name(&self) -> &'static str {
        "constant_folding"
    }

    fn apply(&self, plan: LogicalPlan, _catalog: &dyn Catalog) -> LogicalPlan {
        // TODO: Implement constant folding
        // Examples:
        // - 1 + 2 → 3
        // - true AND x → x
        // - false AND x → false
        // - x = x → true (for non-null x)
        plan
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

impl OptimizationRule for LimitPushdownRule {
    fn name(&self) -> &'static str {
        "limit_pushdown"
    }

    fn apply(&self, plan: LogicalPlan, _catalog: &dyn Catalog) -> LogicalPlan {
        // TODO: Push limit through compatible operators
        // Limit(Sort(x)) can be optimized to keep only top-N during sort
        plan
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

/// Simple plan equality check (for optimization convergence).
fn plans_equal(a: &LogicalPlan, b: &LogicalPlan) -> bool {
    // This is a simplified check
    // TODO: Structural equality check
    std::mem::discriminant(a) == std::mem::discriminant(b)
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
}

impl StorageCatalog {
    /// Create a new storage catalog backed by the given schema catalog.
    pub fn new(schema_catalog: Arc<crate::storage::SchemaCatalog>) -> Self {
        Self { schema_catalog }
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

    fn get_indexes(&self, _table: &str) -> Vec<IndexInfo> {
        // TODO: Implement index metadata storage
        Vec::new()
    }

    fn get_table_stats(&self, _table: &str) -> Option<TableStats> {
        // TODO: Implement statistics collection
        None
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

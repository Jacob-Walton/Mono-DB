#![allow(dead_code)]

//! Logical Query Plan
//!
//! Database-agnostic representation of a query describing what operations to perform,
//! but not how. Sits between AST and physical plan, enabling optimization without
//! storage implementation details.

use crate::query_engine::ast::{BinaryOp, ColumnRef, Ident, ParamRef, SortDirection, UnaryOp};
use monodb_common::Value;

// Logical Plan Nodes

/// A node in the logical query plan tree.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan a table (produces rows)
    Scan(ScanNode),
    /// Filter rows based on a predicate
    Filter(FilterNode),
    /// Project (select) specific columns/expressions
    Project(ProjectNode),
    /// Sort rows by one or more keys
    Sort(SortNode),
    /// Limit the number of result rows
    Limit(LimitNode),
    /// Skip a number of rows (offset)
    Offset(OffsetNode),
    /// Aggregate rows (GROUP BY, COUNT, SUM, etc.)
    Aggregate(AggregateNode),
    /// Join two relations
    Join(JoinNode),
    /// Return a constant set of values
    Values(ValuesNode),
    /// Empty result (for failed conditions, etc.)
    Empty,
    /// Insert rows into a table
    Insert(InsertNode),
    /// Update rows in a table
    Update(UpdateNode),
    /// Delete rows from a table
    Delete(DeleteNode),
}

impl LogicalPlan {
    /// Get a human-readable name for this plan node type
    pub fn node_type(&self) -> &'static str {
        match self {
            LogicalPlan::Scan(_) => "Scan",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Project(_) => "Project",
            LogicalPlan::Sort(_) => "Sort",
            LogicalPlan::Limit(_) => "Limit",
            LogicalPlan::Offset(_) => "Offset",
            LogicalPlan::Aggregate(_) => "Aggregate",
            LogicalPlan::Join(_) => "Join",
            LogicalPlan::Values(_) => "Values",
            LogicalPlan::Empty => "Empty",
            LogicalPlan::Insert(_) => "Insert",
            LogicalPlan::Update(_) => "Update",
            LogicalPlan::Delete(_) => "Delete",
        }
    }

    /// Check if this is a read-only query plan
    pub fn is_read_only(&self) -> bool {
        match self {
            LogicalPlan::Insert(_) | LogicalPlan::Update(_) | LogicalPlan::Delete(_) => false,
            LogicalPlan::Filter(f) => f.input.is_read_only(),
            LogicalPlan::Project(p) => p.input.is_read_only(),
            LogicalPlan::Sort(s) => s.input.is_read_only(),
            LogicalPlan::Limit(l) => l.input.is_read_only(),
            LogicalPlan::Offset(o) => o.input.is_read_only(),
            LogicalPlan::Aggregate(a) => a.input.is_read_only(),
            LogicalPlan::Join(j) => j.left.is_read_only() && j.right.is_read_only(),
            _ => true,
        }
    }
}

// Query Operator Nodes

/// Scan a table, optionally with pushed-down predicates.
#[derive(Debug, Clone)]
pub struct ScanNode {
    /// Table to scan
    pub table: Ident,
    /// Alias for the table (for self-joins, etc.)
    pub alias: Option<Ident>,
    /// Predicates that can be pushed down to storage layer
    pub pushdown_predicates: Vec<ScalarExpr>,
    /// Columns required (for column pruning optimization)
    pub required_columns: Option<Vec<Ident>>,
}

impl ScanNode {
    pub fn new(table: impl Into<Ident>) -> Self {
        Self {
            table: table.into(),
            alias: None,
            pushdown_predicates: Vec::new(),
            required_columns: None,
        }
    }

    pub fn with_alias(mut self, alias: impl Into<Ident>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn with_columns(mut self, columns: Vec<Ident>) -> Self {
        self.required_columns = Some(columns);
        self
    }
}

/// Filter (WHERE clause), keeps rows matching the predicate.
#[derive(Debug, Clone)]
pub struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicate: ScalarExpr,
}

/// Project, select specific columns or compute expressions.
#[derive(Debug, Clone)]
pub struct ProjectNode {
    pub input: Box<LogicalPlan>,
    /// List of (output_name, expression) pairs
    pub expressions: Vec<(Ident, ScalarExpr)>,
}

/// Sort rows by one or more keys.
#[derive(Debug, Clone)]
pub struct SortNode {
    pub input: Box<LogicalPlan>,
    pub order_by: Vec<SortKey>,
}

/// A single sort key with direction and null handling.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: ScalarExpr,
    pub direction: SortDirection,
    pub nulls_first: bool,
}

impl SortKey {
    pub fn asc(expr: ScalarExpr) -> Self {
        Self {
            expr,
            direction: SortDirection::Asc,
            nulls_first: false,
        }
    }

    pub fn desc(expr: ScalarExpr) -> Self {
        Self {
            expr,
            direction: SortDirection::Desc,
            nulls_first: true,
        }
    }
}

/// Limit the number of result rows.
#[derive(Debug, Clone)]
pub struct LimitNode {
    pub input: Box<LogicalPlan>,
    /// Limit can be a literal or a parameter
    pub count: ScalarExpr,
}

/// Skip a number of rows (OFFSET).
#[derive(Debug, Clone)]
pub struct OffsetNode {
    pub input: Box<LogicalPlan>,
    /// Offset can be a literal or a parameter
    pub count: ScalarExpr,
}

/// Aggregate rows with grouping and aggregate functions.
#[derive(Debug, Clone)]
pub struct AggregateNode {
    pub input: Box<LogicalPlan>,
    /// GROUP BY expressions
    pub group_by: Vec<ScalarExpr>,
    /// Aggregate expressions
    pub aggregates: Vec<AggregateExpr>,
}

/// An aggregate expression (COUNT, SUM, etc.)
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub args: Vec<ScalarExpr>,
    pub distinct: bool,
    pub alias: Ident,
}

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
}

impl AggregateFunction {
    pub fn name(&self) -> &'static str {
        match self {
            AggregateFunction::Count => "count",
            AggregateFunction::Sum => "sum",
            AggregateFunction::Avg => "avg",
            AggregateFunction::Min => "min",
            AggregateFunction::Max => "max",
            AggregateFunction::First => "first",
            AggregateFunction::Last => "last",
        }
    }
}

/// Join two relations.
#[derive(Debug, Clone)]
pub struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    /// Join condition (ON clause)
    pub condition: Option<ScalarExpr>,
}

/// Type of join operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl JoinType {
    pub fn name(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL",
            JoinType::Cross => "CROSS",
        }
    }
}

/// A constant set of values (for INSERT or UNION of literals).
#[derive(Debug, Clone)]
pub struct ValuesNode {
    /// Column names for the values
    pub columns: Vec<Ident>,
    /// Rows of values
    pub rows: Vec<Vec<ScalarExpr>>,
}

// Mutation Operator Nodes

/// Insert rows into a table.
#[derive(Debug, Clone)]
pub struct InsertNode {
    pub table: Ident,
    pub columns: Vec<Ident>,
    /// Source of data to insert
    pub source: Box<LogicalPlan>,
}

/// Update rows in a table.
#[derive(Debug, Clone)]
pub struct UpdateNode {
    pub table: Ident,
    /// Column assignments: (column, new_value_expression)
    pub assignments: Vec<(Ident, ScalarExpr)>,
    /// Optional filter to select rows to update
    pub filter: Option<Box<LogicalPlan>>,
}

/// Delete rows from a table.
#[derive(Debug, Clone)]
pub struct DeleteNode {
    pub table: Ident,
    /// Filter to select rows to delete
    pub filter: Option<Box<LogicalPlan>>,
}

// Scalar Expressions

/// A scalar expression evaluating to a single value per row.
///
/// Simplified compared to AST `Expr` for easier optimization and evaluation.
#[derive(Debug, Clone)]
pub enum ScalarExpr {
    /// Column reference
    Column(ColumnRef),
    /// Constant value (evaluated from literal or parameter)
    Constant(Value),
    /// Parameter reference (for prepared statements)
    Param(ParamRef),
    /// Binary operation
    BinaryOp {
        left: Box<ScalarExpr>,
        op: BinaryOp,
        right: Box<ScalarExpr>,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOp,
        operand: Box<ScalarExpr>,
    },
    /// Function call
    FunctionCall { name: Ident, args: Vec<ScalarExpr> },
    /// CASE expression
    Case {
        operand: Option<Box<ScalarExpr>>,
        when_clauses: Vec<(ScalarExpr, ScalarExpr)>,
        else_result: Option<Box<ScalarExpr>>,
    },
    /// Array constructor
    Array(Vec<ScalarExpr>),
    /// Object/Map constructor
    Object(Vec<(Ident, ScalarExpr)>),
    /// Aggregate function placeholder (resolved during planning)
    AggregateRef { index: usize },
}

impl ScalarExpr {
    /// Create a column reference
    pub fn column(name: impl Into<Ident>) -> Self {
        ScalarExpr::Column(ColumnRef::simple(name))
    }

    /// Create a constant value
    pub fn constant(value: impl Into<Value>) -> Self {
        ScalarExpr::Constant(value.into())
    }

    /// Create a null constant
    pub fn null() -> Self {
        ScalarExpr::Constant(Value::Null)
    }

    /// Create an integer constant
    pub fn int(n: i64) -> Self {
        ScalarExpr::Constant(Value::Int64(n))
    }

    /// Create a string constant
    pub fn string(s: impl Into<String>) -> Self {
        ScalarExpr::Constant(Value::String(s.into()))
    }

    /// Create a boolean constant
    pub fn bool(b: bool) -> Self {
        ScalarExpr::Constant(Value::Bool(b))
    }

    /// Create an AND expression
    pub fn and(self, other: ScalarExpr) -> Self {
        ScalarExpr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        }
    }

    /// Create an OR expression
    pub fn or(self, other: ScalarExpr) -> Self {
        ScalarExpr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other),
        }
    }

    /// Create an equality comparison
    pub fn eq(self, other: ScalarExpr) -> Self {
        ScalarExpr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        }
    }

    /// Create a NOT expression
    pub fn not(self) -> Self {
        ScalarExpr::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(self),
        }
    }

    /// Check if this expression is a constant
    pub fn is_constant(&self) -> bool {
        matches!(self, ScalarExpr::Constant(_))
    }

    /// Check if this expression references any columns
    pub fn references_columns(&self) -> bool {
        match self {
            ScalarExpr::Column(_) => true,
            ScalarExpr::Constant(_) | ScalarExpr::Param(_) | ScalarExpr::AggregateRef { .. } => {
                false
            }
            ScalarExpr::BinaryOp { left, right, .. } => {
                left.references_columns() || right.references_columns()
            }
            ScalarExpr::UnaryOp { operand, .. } => operand.references_columns(),
            ScalarExpr::FunctionCall { args, .. } => args.iter().any(|a| a.references_columns()),
            ScalarExpr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand
                    .as_ref()
                    .map(|o| o.references_columns())
                    .unwrap_or(false)
                    || when_clauses
                        .iter()
                        .any(|(c, r)| c.references_columns() || r.references_columns())
                    || else_result
                        .as_ref()
                        .map(|e| e.references_columns())
                        .unwrap_or(false)
            }
            ScalarExpr::Array(items) => items.iter().any(|i| i.references_columns()),
            ScalarExpr::Object(fields) => fields.iter().any(|(_, v)| v.references_columns()),
        }
    }

    /// Collect all column references in this expression
    pub fn collect_columns(&self, columns: &mut Vec<ColumnRef>) {
        match self {
            ScalarExpr::Column(c) => columns.push(c.clone()),
            ScalarExpr::Constant(_) | ScalarExpr::Param(_) | ScalarExpr::AggregateRef { .. } => {}
            ScalarExpr::BinaryOp { left, right, .. } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            ScalarExpr::UnaryOp { operand, .. } => operand.collect_columns(columns),
            ScalarExpr::FunctionCall { args, .. } => {
                for arg in args {
                    arg.collect_columns(columns);
                }
            }
            ScalarExpr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(o) = operand {
                    o.collect_columns(columns);
                }
                for (c, r) in when_clauses {
                    c.collect_columns(columns);
                    r.collect_columns(columns);
                }
                if let Some(e) = else_result {
                    e.collect_columns(columns);
                }
            }
            ScalarExpr::Array(items) => {
                for item in items {
                    item.collect_columns(columns);
                }
            }
            ScalarExpr::Object(fields) => {
                for (_, v) in fields {
                    v.collect_columns(columns);
                }
            }
        }
    }
}

// Plan Builder Helpers

impl LogicalPlan {
    /// Wrap this plan in a Filter node
    pub fn filter(self, predicate: ScalarExpr) -> Self {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(self),
            predicate,
        })
    }

    /// Wrap this plan in a Limit node
    pub fn limit(self, count: u64) -> Self {
        LogicalPlan::Limit(LimitNode {
            input: Box::new(self),
            count: ScalarExpr::int(count as i64),
        })
    }

    /// Wrap this plan in an Offset node
    pub fn offset(self, count: u64) -> Self {
        LogicalPlan::Offset(OffsetNode {
            input: Box::new(self),
            count: ScalarExpr::int(count as i64),
        })
    }

    /// Wrap this plan in a Sort node
    pub fn sort(self, order_by: Vec<SortKey>) -> Self {
        LogicalPlan::Sort(SortNode {
            input: Box::new(self),
            order_by,
        })
    }

    /// Wrap this plan in a Project node
    pub fn project(self, expressions: Vec<(Ident, ScalarExpr)>) -> Self {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(self),
            expressions,
        })
    }
}

// Display implementations for debugging

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_indent(
            plan: &LogicalPlan,
            f: &mut std::fmt::Formatter<'_>,
            indent: usize,
        ) -> std::fmt::Result {
            let pad = "  ".repeat(indent);
            match plan {
                LogicalPlan::Scan(s) => {
                    writeln!(f, "{pad}Scan: {}", s.table)?;
                    if !s.pushdown_predicates.is_empty() {
                        writeln!(f, "{pad}  pushdown: {:?}", s.pushdown_predicates)?;
                    }
                }
                LogicalPlan::Filter(n) => {
                    writeln!(f, "{pad}Filter: {:?}", n.predicate)?;
                    fmt_indent(&n.input, f, indent + 1)?;
                }
                LogicalPlan::Project(n) => {
                    let cols: Vec<_> = n
                        .expressions
                        .iter()
                        .map(|(name, _)| name.as_str())
                        .collect();
                    writeln!(f, "{pad}Project: {}", cols.join(", "))?;
                    fmt_indent(&n.input, f, indent + 1)?;
                }
                LogicalPlan::Sort(n) => {
                    writeln!(f, "{pad}Sort: {} keys", n.order_by.len())?;
                    fmt_indent(&n.input, f, indent + 1)?;
                }
                LogicalPlan::Limit(n) => {
                    writeln!(f, "{pad}Limit: {:?}", n.count)?;
                    fmt_indent(&n.input, f, indent + 1)?;
                }
                LogicalPlan::Offset(n) => {
                    writeln!(f, "{pad}Offset: {:?}", n.count)?;
                    fmt_indent(&n.input, f, indent + 1)?;
                }
                LogicalPlan::Aggregate(n) => {
                    writeln!(f, "{pad}Aggregate: {} aggs", n.aggregates.len())?;
                    fmt_indent(&n.input, f, indent + 1)?;
                }
                LogicalPlan::Join(n) => {
                    writeln!(f, "{pad}Join: {:?}", n.join_type)?;
                    fmt_indent(&n.left, f, indent + 1)?;
                    fmt_indent(&n.right, f, indent + 1)?;
                }
                LogicalPlan::Values(n) => {
                    writeln!(f, "{pad}Values: {} rows", n.rows.len())?;
                }
                LogicalPlan::Empty => {
                    writeln!(f, "{pad}Empty")?;
                }
                LogicalPlan::Insert(n) => {
                    writeln!(f, "{pad}Insert: {}", n.table)?;
                    fmt_indent(&n.source, f, indent + 1)?;
                }
                LogicalPlan::Update(n) => {
                    writeln!(f, "{pad}Update: {}", n.table)?;
                    if let Some(ref filter) = n.filter {
                        fmt_indent(filter, f, indent + 1)?;
                    }
                }
                LogicalPlan::Delete(n) => {
                    writeln!(f, "{pad}Delete: {}", n.table)?;
                    if let Some(ref filter) = n.filter {
                        fmt_indent(filter, f, indent + 1)?;
                    }
                }
            }
            Ok(())
        }

        writeln!(f, "LogicalPlan:")?;
        fmt_indent(self, f, 1)
    }
}

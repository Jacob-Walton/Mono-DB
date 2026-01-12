#![allow(dead_code)]

//! Physical Query Plan
//!
//! Specifies HOW to execute a query with concrete algorithms and access methods.
//! Uses Volcano-style execution with iterator interface for each operator.
//! Includes cost estimates (I/O, CPU, memory) for choosing optimal execution strategies.

use crate::query_engine::ast::Ident;
use crate::query_engine::logical_plan::{AggregateFunction, JoinType, ScalarExpr, SortKey};
use monodb_common::Value;

// Physical Plan Nodes
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Sequential scan of all rows in a table
    SeqScan(SeqScanOp),
    /// Index scan using a B-tree index (boxed due to size)
    IndexScan(Box<IndexScanOp>),
    /// Primary key point lookup
    PrimaryKeyLookup(PkLookupOp),
    /// Filter rows based on a predicate
    Filter(FilterOp),
    /// Project specific columns/expressions
    Project(ProjectOp),
    /// Sort rows (in-memory or external)
    Sort(SortOp),
    /// Limit number of result rows
    Limit(LimitOp),
    /// Skip rows (offset)
    Offset(OffsetOp),
    /// Hash-based aggregation
    HashAggregate(HashAggregateOp),
    /// Stream aggregation (requires sorted input)
    StreamAggregate(StreamAggregateOp),
    /// Nested loop join (simple, works for any condition)
    NestedLoopJoin(NestedLoopJoinOp),
    /// Hash join (for equi-joins)
    HashJoin(HashJoinOp),
    /// Merge join (for sorted inputs)
    MergeJoin(MergeJoinOp),
    /// Constant values (for INSERT)
    Values(ValuesOp),
    /// Empty result
    Empty,
    /// Insert rows into a table
    Insert(InsertOp),
    /// Update rows in a table
    Update(UpdateOp),
    /// Delete rows from a table
    Delete(DeleteOp),
}

impl PhysicalPlan {
    /// Get a human-readable name for this operator
    pub fn operator_name(&self) -> &'static str {
        match self {
            PhysicalPlan::SeqScan(_) => "SeqScan",
            PhysicalPlan::IndexScan(_) => "IndexScan",
            PhysicalPlan::PrimaryKeyLookup(_) => "PkLookup",
            PhysicalPlan::Filter(_) => "Filter",
            PhysicalPlan::Project(_) => "Project",
            PhysicalPlan::Sort(_) => "Sort",
            PhysicalPlan::Limit(_) => "Limit",
            PhysicalPlan::Offset(_) => "Offset",
            PhysicalPlan::HashAggregate(_) => "HashAggregate",
            PhysicalPlan::StreamAggregate(_) => "StreamAggregate",
            PhysicalPlan::NestedLoopJoin(_) => "NestedLoopJoin",
            PhysicalPlan::HashJoin(_) => "HashJoin",
            PhysicalPlan::MergeJoin(_) => "MergeJoin",
            PhysicalPlan::Values(_) => "Values",
            PhysicalPlan::Empty => "Empty",
            PhysicalPlan::Insert(_) => "Insert",
            PhysicalPlan::Update(_) => "Update",
            PhysicalPlan::Delete(_) => "Delete",
        }
    }

    /// Get the estimated cost of executing this plan
    pub fn estimated_cost(&self) -> PlanCost {
        match self {
            PhysicalPlan::SeqScan(op) => op.cost.clone(),
            PhysicalPlan::IndexScan(op) => op.cost.clone(),
            PhysicalPlan::PrimaryKeyLookup(op) => op.cost.clone(),
            PhysicalPlan::Filter(op) => op.cost.clone(),
            PhysicalPlan::Project(op) => op.cost.clone(),
            PhysicalPlan::Sort(op) => op.cost.clone(),
            PhysicalPlan::Limit(op) => op.cost.clone(),
            PhysicalPlan::Offset(op) => op.cost.clone(),
            PhysicalPlan::HashAggregate(op) => op.cost.clone(),
            PhysicalPlan::StreamAggregate(op) => op.cost.clone(),
            PhysicalPlan::NestedLoopJoin(op) => op.cost.clone(),
            PhysicalPlan::HashJoin(op) => op.cost.clone(),
            PhysicalPlan::MergeJoin(op) => op.cost.clone(),
            PhysicalPlan::Values(_) | PhysicalPlan::Empty => PlanCost::zero(),
            PhysicalPlan::Insert(op) => op.cost.clone(),
            PhysicalPlan::Update(op) => op.cost.clone(),
            PhysicalPlan::Delete(op) => op.cost.clone(),
        }
    }
}

// Cost Model
#[derive(Debug, Clone, Default)]
pub struct PlanCost {
    /// Estimated number of page I/O operations
    pub io_cost: f64,
    /// Estimated CPU cost (arbitrary units)
    pub cpu_cost: f64,
    /// Estimated memory usage in bytes
    pub memory_bytes: usize,
    /// Estimated number of output rows
    pub estimated_rows: f64,
}

impl PlanCost {
    pub fn zero() -> Self {
        Self::default()
    }

    /// Total cost (weighted sum of components)
    pub fn total(&self) -> f64 {
        // I/O is typically 100x more expensive than CPU
        self.io_cost * 100.0 + self.cpu_cost
    }

    /// Add costs together
    pub fn add(&self, other: &PlanCost) -> PlanCost {
        PlanCost {
            io_cost: self.io_cost + other.io_cost,
            cpu_cost: self.cpu_cost + other.cpu_cost,
            memory_bytes: self.memory_bytes + other.memory_bytes,
            estimated_rows: self.estimated_rows, // Keep self's estimate
        }
    }
}

// Scan Operators
#[derive(Debug, Clone)]
pub struct SeqScanOp {
    pub table: Ident,
    /// Optional filter predicate to evaluate during scan
    pub filter: Option<ScalarExpr>,
    /// Columns to output (None = all columns)
    pub columns: Option<Vec<Ident>>,
    /// Estimated cost
    pub cost: PlanCost,
}

/// Index scan
#[derive(Debug, Clone)]
pub struct IndexScanOp {
    pub table: Ident,
    pub index_name: Ident,
    pub scan_type: IndexScanType,
    /// For point lookups
    pub lookup_key: Option<ScalarExpr>,
    /// For range scans: (start_value, inclusive)
    pub range_start: Option<(ScalarExpr, bool)>,
    /// For range scans: (end_value, inclusive)
    pub range_end: Option<(ScalarExpr, bool)>,
    /// Columns to output
    pub columns: Option<Vec<Ident>>,
    /// Additional filter after index lookup
    pub residual_filter: Option<ScalarExpr>,
    /// Estimated cost
    pub cost: PlanCost,
}

/// Type of index scan operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexScanType {
    /// Exact match lookup
    ExactMatch,
    /// Range scan
    RangeScan,
    /// Ordered scan (for ORDER BY)
    OrderedScan { ascending: bool },
    /// Prefix scan (for LIKE 'foo%')
    PrefixScan,
}

/// Primary key point lookup, direct access by primary key.
#[derive(Debug, Clone)]
pub struct PkLookupOp {
    pub table: Ident,
    /// The primary key value to look up
    pub key: ScalarExpr,
    /// Columns to output
    pub columns: Option<Vec<Ident>>,
    /// Estimated cost
    pub cost: PlanCost,
}

// Relational Operators
#[derive(Debug, Clone)]
pub struct FilterOp {
    pub input: Box<PhysicalPlan>,
    pub predicate: ScalarExpr,
    pub cost: PlanCost,
}

/// Project operator, computes output expressions for each row.
#[derive(Debug, Clone)]
pub struct ProjectOp {
    pub input: Box<PhysicalPlan>,
    /// (output_column_name, expression)
    pub expressions: Vec<(Ident, ScalarExpr)>,
    pub cost: PlanCost,
}

/// Sort operator, sorts input rows.
#[derive(Debug, Clone)]
pub struct SortOp {
    pub input: Box<PhysicalPlan>,
    pub order_by: Vec<SortKey>,
    /// For top-N optimization: only keep this many rows
    pub limit: Option<usize>,
    /// Whether to use external sort (for large datasets)
    pub external: bool,
    pub cost: PlanCost,
}

/// Limit operator. returns at most N rows.
#[derive(Debug, Clone)]
pub struct LimitOp {
    pub input: Box<PhysicalPlan>,
    /// The limit count (resolved to concrete value)
    pub count: usize,
    pub cost: PlanCost,
}

/// Offset operator, skips N rows.
#[derive(Debug, Clone)]
pub struct OffsetOp {
    pub input: Box<PhysicalPlan>,
    /// The offset count (resolved to concrete value)
    pub count: usize,
    pub cost: PlanCost,
}

// Aggregation Operators
#[derive(Debug, Clone)]
pub struct HashAggregateOp {
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<ScalarExpr>,
    pub aggregates: Vec<PhysicalAggregate>,
    pub cost: PlanCost,
}

/// Stream aggregation (requires sorted input by group keys).
#[derive(Debug, Clone)]
pub struct StreamAggregateOp {
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<ScalarExpr>,
    pub aggregates: Vec<PhysicalAggregate>,
    pub cost: PlanCost,
}

/// A single aggregate computation.
#[derive(Debug, Clone)]
pub struct PhysicalAggregate {
    pub function: AggregateFunction,
    pub args: Vec<ScalarExpr>,
    pub distinct: bool,
    pub output_name: Ident,
}

// Join Operators
#[derive(Debug, Clone)]
pub struct NestedLoopJoinOp {
    /// Outer relation (iterated in outer loop)
    pub outer: Box<PhysicalPlan>,
    /// Inner relation (iterated in inner loop, rescanned for each outer row)
    pub inner: Box<PhysicalPlan>,
    /// Join condition
    pub condition: Option<ScalarExpr>,
    pub join_type: JoinType,
    pub cost: PlanCost,
}

/// Hash join: O(n+m) for equi-joins.
#[derive(Debug, Clone)]
pub struct HashJoinOp {
    /// Build side (smaller, used to build hash table)
    pub build: Box<PhysicalPlan>,
    /// Probe side (larger, probes into hash table)
    pub probe: Box<PhysicalPlan>,
    /// Keys from build side for hash table
    pub build_keys: Vec<ScalarExpr>,
    /// Keys from probe side for lookup
    pub probe_keys: Vec<ScalarExpr>,
    pub join_type: JoinType,
    pub cost: PlanCost,
}

/// Merge join: O(n+m) for sorted inputs.
#[derive(Debug, Clone)]
pub struct MergeJoinOp {
    /// Left input (must be sorted by left_keys)
    pub left: Box<PhysicalPlan>,
    /// Right input (must be sorted by right_keys)
    pub right: Box<PhysicalPlan>,
    pub left_keys: Vec<ScalarExpr>,
    pub right_keys: Vec<ScalarExpr>,
    pub join_type: JoinType,
    pub cost: PlanCost,
}

// Data Source Operators
#[derive(Debug, Clone)]
pub struct ValuesOp {
    pub columns: Vec<Ident>,
    pub rows: Vec<Vec<Value>>,
}

// Mutation Operators

/// Insert operator, inserts rows from input into table.
#[derive(Debug, Clone)]
pub struct InsertOp {
    pub table: Ident,
    pub columns: Vec<Ident>,
    pub source: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

/// Update operator, updates rows matching the scan.
#[derive(Debug, Clone)]
pub struct UpdateOp {
    pub table: Ident,
    /// Scan to find rows to update
    pub scan: Box<PhysicalPlan>,
    /// Assignments: (column, new_value_expr)
    pub assignments: Vec<(Ident, ScalarExpr)>,
    pub cost: PlanCost,
}

/// Delete operator, deletes rows matching the scan.
#[derive(Debug, Clone)]
pub struct DeleteOp {
    pub table: Ident,
    /// Scan to find rows to delete
    pub scan: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

// Plan Properties
#[derive(Debug, Clone, Default)]
pub struct PlanProperties {
    /// Output ordering (if any)
    pub ordering: Option<Vec<SortKey>>,
    /// Partitioning (for parallel execution)
    pub partitioning: Partitioning,
    /// Output schema
    pub output_columns: Vec<Ident>,
}

/// How data is partitioned across workers.
#[derive(Debug, Clone, Default)]
pub enum Partitioning {
    #[default]
    Single,
    Hash {
        columns: Vec<Ident>,
        num_partitions: usize,
    },
    RoundRobin {
        num_partitions: usize,
    },
}

// Display Implementations

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_indent(
            plan: &PhysicalPlan,
            f: &mut std::fmt::Formatter<'_>,
            indent: usize,
        ) -> std::fmt::Result {
            let pad = "  ".repeat(indent);
            let cost = plan.estimated_cost();

            match plan {
                PhysicalPlan::SeqScan(op) => {
                    writeln!(
                        f,
                        "{pad}SeqScan: {} (rows: {:.0}, cost: {:.1})",
                        op.table,
                        cost.estimated_rows,
                        cost.total()
                    )?;
                }
                PhysicalPlan::IndexScan(op) => {
                    writeln!(
                        f,
                        "{pad}IndexScan: {}.{} {:?} (rows: {:.0})",
                        op.table, op.index_name, op.scan_type, cost.estimated_rows
                    )?;
                }
                PhysicalPlan::PrimaryKeyLookup(op) => {
                    writeln!(f, "{pad}PkLookup: {} (rows: 1)", op.table)?;
                }
                PhysicalPlan::Filter(op) => {
                    writeln!(f, "{pad}Filter: (rows: {:.0})", cost.estimated_rows)?;
                    fmt_indent(&op.input, f, indent + 1)?;
                }
                PhysicalPlan::Project(op) => {
                    let cols: Vec<_> = op.expressions.iter().map(|(n, _)| n.as_str()).collect();
                    writeln!(f, "{pad}Project: {}", cols.join(", "))?;
                    fmt_indent(&op.input, f, indent + 1)?;
                }
                PhysicalPlan::Sort(op) => {
                    writeln!(f, "{pad}Sort: {} keys", op.order_by.len())?;
                    fmt_indent(&op.input, f, indent + 1)?;
                }
                PhysicalPlan::Limit(op) => {
                    writeln!(f, "{pad}Limit: {}", op.count)?;
                    fmt_indent(&op.input, f, indent + 1)?;
                }
                PhysicalPlan::Offset(op) => {
                    writeln!(f, "{pad}Offset: {}", op.count)?;
                    fmt_indent(&op.input, f, indent + 1)?;
                }
                PhysicalPlan::HashAggregate(op) => {
                    writeln!(f, "{pad}HashAggregate: {} aggs", op.aggregates.len())?;
                    fmt_indent(&op.input, f, indent + 1)?;
                }
                PhysicalPlan::StreamAggregate(op) => {
                    writeln!(f, "{pad}StreamAggregate: {} aggs", op.aggregates.len())?;
                    fmt_indent(&op.input, f, indent + 1)?;
                }
                PhysicalPlan::NestedLoopJoin(op) => {
                    writeln!(f, "{pad}NestedLoopJoin: {:?}", op.join_type)?;
                    fmt_indent(&op.outer, f, indent + 1)?;
                    fmt_indent(&op.inner, f, indent + 1)?;
                }
                PhysicalPlan::HashJoin(op) => {
                    writeln!(f, "{pad}HashJoin: {:?}", op.join_type)?;
                    writeln!(f, "{pad}  build:")?;
                    fmt_indent(&op.build, f, indent + 2)?;
                    writeln!(f, "{pad}  probe:")?;
                    fmt_indent(&op.probe, f, indent + 2)?;
                }
                PhysicalPlan::MergeJoin(op) => {
                    writeln!(f, "{pad}MergeJoin: {:?}", op.join_type)?;
                    fmt_indent(&op.left, f, indent + 1)?;
                    fmt_indent(&op.right, f, indent + 1)?;
                }
                PhysicalPlan::Values(op) => {
                    writeln!(f, "{pad}Values: {} rows", op.rows.len())?;
                }
                PhysicalPlan::Empty => {
                    writeln!(f, "{pad}Empty")?;
                }
                PhysicalPlan::Insert(op) => {
                    writeln!(f, "{pad}Insert: {}", op.table)?;
                    fmt_indent(&op.source, f, indent + 1)?;
                }
                PhysicalPlan::Update(op) => {
                    writeln!(f, "{pad}Update: {}", op.table)?;
                    fmt_indent(&op.scan, f, indent + 1)?;
                }
                PhysicalPlan::Delete(op) => {
                    writeln!(f, "{pad}Delete: {}", op.table)?;
                    fmt_indent(&op.scan, f, indent + 1)?;
                }
            }
            Ok(())
        }

        writeln!(f, "PhysicalPlan:")?;
        fmt_indent(self, f, 1)
    }
}

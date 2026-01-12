//! Storage models for MonoDB server.

use monodb_common::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum Data<'a> {
    KeyValue { key: &'a [u8], value: &'a [u8] },
    Document(Value),
    Row(HashMap<String, Value>),
}

#[derive(Debug, Default, Clone)]
pub struct Query {
    pub filter: Option<Filter>,
    pub projection: Option<Vec<String>>,
    pub sort: Option<Vec<Sort>>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum Filter {
    Eq(String, Value),
    Neq(String, Value),
    Gt(String, Value),
    Lt(String, Value),
    Gte(String, Value),
    Lte(String, Value),
    Contains(String, Value),
    And(Vec<Filter>),
    Or(Vec<Filter>),
}

#[derive(Debug, Clone)]
pub struct Sort {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Query execution plan that uses an index
#[derive(Debug, Clone)]
pub struct IndexPlan {
    /// Name of the index to use
    pub index_name: String,
    /// Columns covered by this index
    pub columns: Vec<String>,
    /// Type of index scan to perform
    pub scan_type: IndexScanType,
    /// For equality lookups: the exact value to search for
    pub lookup_value: Option<Value>,
}

/// Type of index scan operation
#[derive(Debug, Clone)]
pub enum IndexScanType {
    /// Exact match lookup on secondary index (WHERE col = value)
    ExactMatch,
    /// Point lookup by primary key (WHERE pk = value)
    PrimaryKeyLookup,
    /// Forward scan for ORDER BY ASC
    OrderedScanAsc,
    /// Backward scan for ORDER BY DESC
    OrderedScanDesc,
    /// Direct primary key scan
    PrimaryKeyScanAsc,
    PrimaryKeyScanDesc,
}

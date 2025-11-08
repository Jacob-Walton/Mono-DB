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

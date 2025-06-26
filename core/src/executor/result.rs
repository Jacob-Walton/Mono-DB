use crate::executor::ColumnInfo;

use super::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Row {
    pub data: HashMap<String, Value>,
}

impl Row {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, column: String, value: Value) {
        self.data.insert(column, value);
    }

    pub fn get(&self, column: &str) -> Option<&Value> {
        self.data.get(column)
    }
}

#[derive(Debug)]
pub enum QueryResult {
    Rows(Vec<Row>),
    RowsAffected(usize),
    Created,
    Columns(Vec<ColumnInfo>),
}

impl QueryResult {
    pub fn rows(rows: Vec<Row>) -> Self {
        Self::Rows(rows)
    }

    pub fn affected(count: usize) -> Self {
        Self::RowsAffected(count)
    }

    pub fn created() -> Self {
        Self::Created
    }

    pub fn columns(columns: Vec<ColumnInfo>) -> Self {
        Self::Columns(columns)
    }
}

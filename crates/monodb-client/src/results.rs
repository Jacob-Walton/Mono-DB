//! Ergonomic result types for query execution
//!
//! Provides structured result types that make working with query results
//! much more convenient than matching on raw protocol responses.

use std::collections::BTreeMap;

use indexmap::IndexMap;
use monodb_common::{
    protocol::{ExecutionResult, Response},
    MonoError, Result, Value,
};

/// Result of a query execution with convenient access methods
#[derive(Debug, Clone)]
pub struct QueryResult {
    results: Vec<ExecutionResult>,
}

impl QueryResult {
    /// Create a new QueryResult from a protocol Response
    pub(crate) fn from_response(response: Response) -> Result<Self> {
        match response {
            Response::Success { result } => Ok(Self { results: result }),
            Response::Error { code: _, message } => Err(MonoError::Execution(message)),
            other => Err(MonoError::Execution(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    /// Get all results
    pub fn results(&self) -> &[ExecutionResult] {
        &self.results
    }

    /// Get the first result, if any
    pub fn first(&self) -> Option<&ExecutionResult> {
        self.results.first()
    }

    /// Get all rows from all results
    ///
    /// Flattens all data from all ExecutionResult::Ok variants into a single vec.
    /// Ignores Created/Modified results.
    pub fn rows(&self) -> Vec<Row> {
        self.results
            .iter()
            .filter_map(|r| match r {
                ExecutionResult::Ok { data, .. } => Some(data.iter().map(Row::from_value)),
                _ => None,
            })
            .flatten()
            .collect()
    }

    /// Get rows from the first result only
    pub fn rows_from_first(&self) -> Vec<Row> {
        self.first()
            .and_then(|r| match r {
                ExecutionResult::Ok { data, .. } => Some(data.iter().map(Row::from_value).collect()),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Get a single row (convenience for queries that return one row)
    pub fn one(&self) -> Result<Row> {
        let rows = self.rows_from_first();
        match rows.len() {
            0 => Err(MonoError::NotFound("No rows returned".into())),
            1 => Ok(rows.into_iter().next().unwrap()),
            n => Err(MonoError::InvalidOperation(format!(
                "Expected 1 row, got {}",
                n
            ))),
        }
    }

    /// Get an optional single row (None if no rows, error if multiple)
    pub fn optional(&self) -> Result<Option<Row>> {
        let rows = self.rows_from_first();
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.into_iter().next().unwrap())),
            n => Err(MonoError::InvalidOperation(format!(
                "Expected 0 or 1 row, got {}",
                n
            ))),
        }
    }

    /// Get the number of rows affected (for INSERT/UPDATE/DELETE)
    pub fn rows_affected(&self) -> u64 {
        self.results
            .iter()
            .filter_map(|r| match r {
                ExecutionResult::Modified { rows_affected, .. } => *rows_affected,
                _ => None,
            })
            .sum()
    }

    /// Check if any result is a Created result
    pub fn is_created(&self) -> bool {
        self.results
            .iter()
            .any(|r| matches!(r, ExecutionResult::Created { .. }))
    }

    /// Check if any result is a Modified result
    pub fn is_modified(&self) -> bool {
        self.results
            .iter()
            .any(|r| matches!(r, ExecutionResult::Modified { .. }))
    }

    /// Get the commit timestamp if available
    pub fn commit_timestamp(&self) -> Option<u64> {
        self.first().and_then(|r| match r {
            ExecutionResult::Ok {
                commit_timestamp, ..
            } => *commit_timestamp,
            ExecutionResult::Created {
                commit_timestamp, ..
            } => *commit_timestamp,
            ExecutionResult::Modified {
                commit_timestamp, ..
            } => *commit_timestamp,
        })
    }
}

/// A single row from a query result
#[derive(Debug, Clone)]
pub struct Row {
    value: Value,
}

impl Row {
    fn from_value(value: &Value) -> Self {
        Self {
            value: value.clone(),
        }
    }

    /// Get the underlying Value
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Get a field by name (for Row values)
    pub fn get(&self, key: &str) -> Option<&Value> {
        match &self.value {
            Value::Row(map) => map.get(key),
            Value::Object(map) => map.get(key),
            _ => None,
        }
    }

    /// Try to extract a typed value
    pub fn get_typed<T: FromValue>(&self, key: &str) -> Result<T> {
        self.get(key)
            .ok_or_else(|| MonoError::NotFound(format!("Field '{}' not found", key)))
            .and_then(T::from_value)
    }

    /// Convert to a Row map (if the value is Value::Row)
    pub fn as_row(&self) -> Option<&IndexMap<String, Value>> {
        match &self.value {
            Value::Row(map) => Some(map),
            _ => None,
        }
    }

    /// Convert to an Object map (if the value is Value::Object)
    pub fn as_object(&self) -> Option<&BTreeMap<String, Value>> {
        match &self.value {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    /// Get all field names
    pub fn fields(&self) -> Vec<&str> {
        match &self.value {
            Value::Row(map) => map.keys().map(|s| s.as_str()).collect(),
            Value::Object(map) => map.keys().map(|s| s.as_str()).collect(),
            _ => vec![],
        }
    }

    /// Convert to the inner Value, consuming self
    pub fn into_value(self) -> Value {
        self.value
    }
}

/// Trait for extracting typed values from Value
pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Result<Self>;
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        value
            .as_string()
            .cloned()
            .ok_or_else(|| MonoError::TypeError {
                expected: "string".into(),
                actual: value.type_name().into(),
            })
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_i32().ok_or_else(|| MonoError::TypeError {
            expected: "i32".into(),
            actual: value.type_name().into(),
        })
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Int64(v) => Ok(*v),
            Value::Int32(v) => Ok(*v as i64),
            _ => Err(MonoError::TypeError {
                expected: "i64".into(),
                actual: value.type_name().into(),
            }),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Float64(v) => Ok(*v),
            Value::Float32(v) => Ok(*v as f64),
            _ => Err(MonoError::TypeError {
                expected: "f64".into(),
                actual: value.type_name().into(),
            }),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_bool().ok_or_else(|| MonoError::TypeError {
            expected: "bool".into(),
            actual: value.type_name().into(),
        })
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            _ => T::from_value(value).map(Some),
        }
    }
}

//! Query result types for MonoDB client

use monodb_common::{
    MonoError, Result, Value,
    permissions::PermissionSet,
    protocol::{QueryOutcome, Response, TableInfo},
};

/// Result of a query execution.
#[derive(Debug, Clone)]
pub struct QueryResult {
    outcome: Outcome,
    elapsed_ms: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum Outcome {
    Rows {
        data: Vec<Value>,
        columns: Option<Vec<String>>,
    },
    Inserted {
        count: u64,
        ids: Option<Vec<Value>>,
    },
    Updated {
        count: u64,
    },
    Deleted {
        count: u64,
    },
    Created {
        object_type: String,
        name: String,
    },
    Dropped {
        object_type: String,
        name: String,
    },
    Executed,
}

impl QueryResult {
    /// Create a QueryResult from a protocol Response.
    pub(crate) fn from_response(response: Response) -> Result<Self> {
        match response {
            Response::QueryResult { result, elapsed_ms } => Ok(Self {
                outcome: Self::outcome_from_protocol(result),
                elapsed_ms,
            }),
            Response::Ok => Ok(Self {
                outcome: Outcome::Executed,
                elapsed_ms: 0,
            }),
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            other => Err(MonoError::Execution(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    fn outcome_from_protocol(outcome: QueryOutcome) -> Outcome {
        match outcome {
            QueryOutcome::Rows { data, columns, .. } => Outcome::Rows { data, columns },
            QueryOutcome::Inserted {
                rows_inserted,
                generated_ids,
            } => Outcome::Inserted {
                count: rows_inserted,
                ids: generated_ids,
            },
            QueryOutcome::Updated { rows_updated } => Outcome::Updated {
                count: rows_updated,
            },
            QueryOutcome::Deleted { rows_deleted } => Outcome::Deleted {
                count: rows_deleted,
            },
            QueryOutcome::Created {
                object_type,
                object_name,
            } => Outcome::Created {
                object_type,
                name: object_name,
            },
            QueryOutcome::Dropped {
                object_type,
                object_name,
            } => Outcome::Dropped {
                object_type,
                name: object_name,
            },
            QueryOutcome::Executed => Outcome::Executed,
        }
    }

    /// Get all rows as Row objects.
    pub fn rows(&self) -> Vec<Row> {
        match &self.outcome {
            Outcome::Rows { data, .. } => data.iter().map(Row::from_value).collect(),
            _ => vec![],
        }
    }

    /// Get a single row (error if not exactly one).
    pub fn one(&self) -> Result<Row> {
        let rows = self.rows();
        match rows.len() {
            0 => Err(MonoError::NotFound("No rows returned".into())),
            1 => Ok(rows.into_iter().next().unwrap()),
            n => Err(MonoError::InvalidOperation(format!(
                "Expected 1 row, got {n}"
            ))),
        }
    }

    /// Get an optional single row (None if no rows, error if multiple).
    pub fn optional(&self) -> Result<Option<Row>> {
        let rows = self.rows();
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.into_iter().next().unwrap())),
            n => Err(MonoError::InvalidOperation(format!(
                "Expected 0 or 1 row, got {n}"
            ))),
        }
    }

    /// Get the number of rows affected (for mutations).
    pub fn rows_affected(&self) -> u64 {
        match &self.outcome {
            Outcome::Inserted { count, .. } => *count,
            Outcome::Updated { count } => *count,
            Outcome::Deleted { count } => *count,
            _ => 0,
        }
    }

    /// Check if this was an insert operation.
    pub fn is_created(&self) -> bool {
        matches!(&self.outcome, Outcome::Inserted { .. })
    }

    /// Check if this was an update/delete operation.
    pub fn is_modified(&self) -> bool {
        matches!(
            &self.outcome,
            Outcome::Updated { .. } | Outcome::Deleted { .. }
        )
    }

    /// Get elapsed time in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.elapsed_ms
    }
}

/// A single row from query results.
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

    /// Get the underlying value.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Get a field by name.
    pub fn get(&self, key: &str) -> Option<&Value> {
        match &self.value {
            Value::Row(map) => map.get(key),
            Value::Object(map) => map.get(key),
            _ => None,
        }
    }

    /// Get a typed field value.
    pub fn get_typed<T: FromValue>(&self, key: &str) -> Result<T> {
        self.get(key)
            .ok_or_else(|| MonoError::NotFound(format!("Field '{key}' not found")))
            .and_then(T::from_value)
    }

    /// Get all field names.
    pub fn fields(&self) -> Vec<&str> {
        match &self.value {
            Value::Row(map) => map.keys().map(String::as_str).collect(),
            Value::Object(map) => map.keys().map(String::as_str).collect(),
            _ => vec![],
        }
    }

    /// Consume and return the inner value.
    pub fn into_value(self) -> Value {
        self.value
    }
}

/// Trait for extracting typed values from Value.
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

/// Result of listing tables.
#[derive(Debug, Clone)]
pub struct TableListResult {
    /// Tables with their info
    pub tables: Vec<TableInfo>,
    /// All namespaces (including empty ones)
    pub namespaces: Vec<String>,
}

/// Result of an authentication request.
#[derive(Debug, Clone)]
pub struct AuthResult {
    /// Session ID assigned by the server
    pub session_id: u64,
    /// User ID of the authenticated user
    pub user_id: String,
    /// Permissions granted to this session
    pub permissions: PermissionSet,
    /// When the session/token expires (Unix timestamp)
    pub expires_at: Option<u64>,
    /// Session token for resumption (only present on password auth)
    pub token: Option<String>,
}

impl AuthResult {
    /// Create an AuthResult from a protocol Response.
    pub(crate) fn from_response(response: Response) -> Result<Self> {
        match response {
            Response::AuthSuccess {
                session_id,
                user_id,
                permissions,
                expires_at,
                token,
            } => Ok(Self {
                session_id,
                user_id,
                permissions,
                expires_at,
                token,
            }),
            Response::AuthFailed { reason, .. } => Err(MonoError::AuthenticationFailed(reason)),
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            other => Err(MonoError::Execution(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    /// Check if authentication was successful.
    pub fn is_authenticated(&self) -> bool {
        true // If we have an AuthResult, we're authenticated
    }

    /// Get the session token for later resumption.
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }
}

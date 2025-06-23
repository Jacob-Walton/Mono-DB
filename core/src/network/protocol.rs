use crate::executor::{ExecutionContext, QueryResult, Value};
use crate::nsql::ast::Statement;
use crate::nsql::interner::StringInterner;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Constants
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB max message size

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    Execute {
        sql: String,
        database: Option<String>,
    },
    Ping,
    Heartbeat,
    ListDatabases,
    ListTables {
        database: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    ExecuteResult {
        results: Vec<ExecutionResultData>,
        rows_affected: Option<usize>,
        execution_time_ms: u64,
    },
    Pong,
    HeartbeatAck,
    DatabaseList {
        databases: Vec<String>,
    },
    TableList {
        tables: Vec<TableInfo>,
    },
    Error {
        code: ErrorCode,
        message: String,
        details: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResultData {
    pub result_type: ResultType,
    pub columns: Vec<ColumnMetadata>,
    pub rows: Vec<Vec<SerializableValue>>,
    pub rows_affected: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResultType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub row_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
    Date(String),     // ISO format
    DateTime(String), // ISO format
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorCode {
    ParseError,
    ExecutionError,
    TableNotFound,
    ColumnNotFound,
    TypeMismatch,
    ConstraintViolation,
    InternalError,
    NotImplemented,
}

#[derive(Debug)]
pub struct ProtocolHandler {
    pub interner: Arc<RwLock<StringInterner>>,
}

impl ProtocolHandler {
    pub fn new() -> Self {
        Self {
            interner: Arc::new(RwLock::new(StringInterner::new())),
        }
    }

    pub fn handle_message(
        &mut self,
        message: ClientMessage,
        ctx: &mut ExecutionContext,
    ) -> ServerMessage {
        match message {
            ClientMessage::Execute { sql, database: _ } => self.handle_execute(sql, ctx),
            ClientMessage::Ping => ServerMessage::Pong,
            ClientMessage::Heartbeat => ServerMessage::HeartbeatAck,
            ClientMessage::ListDatabases => ServerMessage::DatabaseList {
                databases: vec!["default".to_string()],
            },
            ClientMessage::ListTables { database: _ } => self.handle_list_tables(ctx),
        }
    }

    fn handle_execute(&mut self, sql: String, ctx: &mut ExecutionContext) -> ServerMessage {
        let start_time = std::time::Instant::now();

        // Parse SQL
        let program = {
            let mut interner = self.interner.write();
            match crate::nsql::parser::parse(&sql, &mut interner) {
                Ok(program) => program,
                Err(e) => {
                    return ServerMessage::Error {
                        code: ErrorCode::ParseError,
                        message: "Failed to parse SQL".to_string(),
                        details: Some(e.to_string()),
                    };
                }
            }
        };

        // Update context
        let interner_clone = Arc::clone(&self.interner);
        let interner_ref = interner_clone.read();
        ctx.interner = (*interner_ref).clone();
        drop(interner_ref);

        // Execute statements
        let executor = crate::executor::Executor::new();
        let results = match executor.execute_program(&program, ctx) {
            Ok(results) => results,
            Err(e) => {
                return ServerMessage::Error {
                    code: self.execution_error_to_code(&e),
                    message: "Execution failed".to_string(),
                    details: Some(e.to_string()),
                };
            }
        };

        let execution_time = start_time.elapsed().as_millis() as u64;

        // Convert results to protocol format
        let mut execution_results = Vec::new();
        let mut total_rows_affected = 0;

        for (i, result) in results.iter().enumerate() {
            let result_data = self.convert_query_result(result, program.statements.get(i), ctx);

            if let Some(rows_affected) = result_data.rows_affected {
                total_rows_affected += rows_affected;
            }

            execution_results.push(result_data);
        }

        ServerMessage::ExecuteResult {
            results: execution_results,
            rows_affected: if total_rows_affected > 0 {
                Some(total_rows_affected)
            } else {
                None
            },
            execution_time_ms: execution_time,
        }
    }

    fn handle_list_tables(&self, _ctx: &ExecutionContext) -> ServerMessage {
        // TODO: Use catalog to get actual table information
        ServerMessage::TableList { tables: Vec::new() }
    }

    fn convert_query_result(
        &self,
        result: &QueryResult,
        statement: Option<&Statement>,
        #[allow(unused)]
        ctx: &ExecutionContext,
    ) -> ExecutionResultData {
        match result {
            QueryResult::Rows(rows) => {
                let result_type = if matches!(statement, Some(Statement::Query(_))) {
                    ResultType::Select
                } else {
                    ResultType::Other
                };

                let (columns, converted_rows) = if rows.is_empty() {
                    (Vec::new(), Vec::new())
                } else {
                    let columns = self.infer_columns_from_rows(rows);
                    let converted_rows = rows
                        .iter()
                        .map(|row| self.convert_row_to_values(row, &columns))
                        .collect();
                    (columns, converted_rows)
                };

                ExecutionResultData {
                    result_type,
                    columns,
                    rows: converted_rows,
                    rows_affected: None,
                }
            }
            QueryResult::RowsAffected(count) => {
                let result_type = match statement {
                    Some(Statement::Insert(_)) => ResultType::Insert,
                    Some(Statement::Update(_)) => ResultType::Update,
                    Some(Statement::Delete(_)) => ResultType::Delete,
                    _ => ResultType::Other,
                };

                ExecutionResultData {
                    result_type,
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: Some(*count),
                }
            }
            QueryResult::Created => {
                let result_type = match statement {
                    Some(Statement::CreateTable(_)) => ResultType::CreateTable,
                    Some(Statement::DropTable(_)) => ResultType::DropTable,
                    _ => ResultType::Other,
                };

                ExecutionResultData {
                    result_type,
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: Some(1),
                }
            }
        }
    }

    fn infer_columns_from_rows(&self, rows: &[crate::executor::Row]) -> Vec<ColumnMetadata> {
        if let Some(first_row) = rows.first() {
            first_row
                .data
                .keys()
                .map(|name| ColumnMetadata {
                    name: name.clone(),
                    data_type: self.infer_data_type_from_value(first_row.data.get(name).unwrap()),
                    nullable: true, // Default to nullable for inferred columns
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    fn infer_data_type_from_value(&self, value: &Value) -> String {
        match value {
            Value::Integer(_) => "INTEGER".to_string(),
            Value::Float(_) => "FLOAT".to_string(),
            Value::String(_) => "TEXT".to_string(),
            Value::Boolean(_) => "BOOLEAN".to_string(),
            Value::Null => "NULL".to_string(),
            Value::Date(_) => "DATE".to_string(),
            Value::DateTime(_) => "DATETIME".to_string(),
        }
    }

    fn convert_row_to_values(
        &self,
        row: &crate::executor::Row,
        columns: &[ColumnMetadata],
    ) -> Vec<SerializableValue> {
        columns
            .iter()
            .map(|col| {
                row.data
                    .get(&col.name)
                    .map(|value| self.convert_value_to_serializable(value))
                    .unwrap_or(SerializableValue::Null)
            })
            .collect()
    }

    fn convert_value_to_serializable(&self, value: &Value) -> SerializableValue {
        match value {
            Value::Integer(i) => SerializableValue::Integer(*i),
            Value::Float(f) => SerializableValue::Float(*f),
            Value::String(s) => SerializableValue::String(s.clone()),
            Value::Boolean(b) => SerializableValue::Boolean(*b),
            Value::Null => SerializableValue::Null,
            Value::Date(d) => SerializableValue::Date(d.to_string()),
            Value::DateTime(dt) => SerializableValue::DateTime(dt.to_string()),
        }
    }

    fn execution_error_to_code(&self, error: &crate::executor::ExecutionError) -> ErrorCode {
        match error {
            crate::executor::ExecutionError::TableNotFound(_) => ErrorCode::TableNotFound,
            crate::executor::ExecutionError::ColumnNotFound(_) => ErrorCode::ColumnNotFound,
            crate::executor::ExecutionError::TypeMismatch { .. } => ErrorCode::TypeMismatch,
            crate::executor::ExecutionError::InvalidExpression(_) => ErrorCode::ExecutionError,
            crate::executor::ExecutionError::StorageError(_) => ErrorCode::InternalError,
            crate::executor::ExecutionError::NotImplemented(_) => ErrorCode::NotImplemented,
        }
    }
}

// Helper functions for working with the protocol

impl From<Value> for SerializableValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(i) => SerializableValue::Integer(i),
            Value::Float(f) => SerializableValue::Float(f),
            Value::String(s) => SerializableValue::String(s),
            Value::Boolean(b) => SerializableValue::Boolean(b),
            Value::Null => SerializableValue::Null,
            Value::Date(d) => SerializableValue::Date(d.to_string()),
            Value::DateTime(dt) => SerializableValue::DateTime(dt.to_string()),
        }
    }
}

impl SerializableValue {
    pub fn to_value(&self) -> Result<Value, String> {
        match self {
            SerializableValue::Integer(i) => Ok(Value::Integer(*i)),
            SerializableValue::Float(f) => Ok(Value::Float(*f)),
            SerializableValue::String(s) => Ok(Value::String(s.clone())),
            SerializableValue::Boolean(b) => Ok(Value::Boolean(*b)),
            SerializableValue::Null => Ok(Value::Null),
            SerializableValue::Date(s) => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(Value::Date)
                .map_err(|e| format!("Invalid date format: {}", e)),
            SerializableValue::DateTime(s) => {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .map(Value::DateTime)
                    .map_err(|e| format!("Invalid datetime format: {}", e))
            }
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            SerializableValue::Integer(_) => "INTEGER",
            SerializableValue::Float(_) => "FLOAT",
            SerializableValue::String(_) => "TEXT",
            SerializableValue::Boolean(_) => "BOOLEAN",
            SerializableValue::Null => "NULL",
            SerializableValue::Date(_) => "DATE",
            SerializableValue::DateTime(_) => "DATETIME",
        }
    }
}

// Additional utility functions for protocol handling

pub fn format_execution_result(result: &ExecutionResultData) -> String {
    match result.result_type {
        ResultType::Select => {
            if result.rows.is_empty() {
                "Empty result set".to_string()
            } else {
                format!("{} rows returned", result.rows.len())
            }
        }
        ResultType::Insert => {
            format!("{} rows inserted", result.rows_affected.unwrap_or(0))
        }
        ResultType::Update => {
            format!("{} rows updated", result.rows_affected.unwrap_or(0))
        }
        ResultType::Delete => {
            format!("{} rows deleted", result.rows_affected.unwrap_or(0))
        }
        ResultType::CreateTable => "Table created".to_string(),
        ResultType::DropTable => "Table dropped".to_string(),
        ResultType::Other => "Command executed".to_string(),
    }
}

pub fn format_table_for_display(result: &ExecutionResultData) -> String {
    if result.rows.is_empty() {
        return "Empty result set".to_string();
    }

    let mut output = String::new();

    // Header
    let headers: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
    output.push_str(&format!("{}\n", headers.join(" | ")));

    // Separator
    let separator: Vec<String> = result
        .columns
        .iter()
        .map(|c| "-".repeat(c.name.len()))
        .collect();
    output.push_str(&format!("{}\n", separator.join("-+-")));

    // Rows
    for row in &result.rows {
        let row_strings: Vec<String> = row.iter().map(|v| format_value_for_display(v)).collect();
        output.push_str(&format!("{}\n", row_strings.join(" | ")));
    }

    output
}

fn format_value_for_display(value: &SerializableValue) -> String {
    match value {
        SerializableValue::Integer(i) => i.to_string(),
        SerializableValue::Float(f) => f.to_string(),
        SerializableValue::String(s) => s.clone(),
        SerializableValue::Boolean(b) => b.to_string(),
        SerializableValue::Null => "NULL".to_string(),
        SerializableValue::Date(d) => d.clone(),
        SerializableValue::DateTime(dt) => dt.clone(),
    }
}

// Legacy support functions for backward compatibility
impl ClientMessage {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|e| e.to_string())
    }
}

impl ServerMessage {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serializable_value_conversion() {
        let value = Value::Integer(42);
        let serializable: SerializableValue = value.into();
        let converted_back = serializable.to_value().unwrap();

        match converted_back {
            Value::Integer(i) => assert_eq!(i, 42),
            _ => panic!("Conversion failed"),
        }
    }

    #[test]
    fn test_protocol_handler_creation() {
        let _handler = ProtocolHandler::new();
        // Just test that it creates without panic
        assert!(true);
    }

    #[test]
    fn test_message_serialization() {
        let message = ClientMessage::Execute {
            sql: "SELECT * FROM users".to_string(),
            database: None,
        };

        let bytes = message.to_bytes().unwrap();
        let parsed = ClientMessage::from_bytes(&bytes).unwrap();

        match parsed {
            ClientMessage::Execute { sql, .. } => {
                assert_eq!(sql, "SELECT * FROM users");
            }
            _ => panic!("Message type mismatch"),
        }
    }
}

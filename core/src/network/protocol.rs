use crate::executor::{ExecutionContext, QueryResult, Value};
use crate::nsql::ast::Statement;
use crate::nsql::interner::StringInterner;
use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
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
		// tables: Vec<TableInfo>,
		tables: Vec<String>,
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
	Describe,
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

	fn handle_list_tables(&self, ctx: &ExecutionContext) -> ServerMessage {
		let tables = ctx.storage.get_tables("");
		ServerMessage::TableList { tables }
	}

	fn convert_query_result(
		&self,
		result: &QueryResult,
		statement: Option<&Statement>,
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
					// Try to get schema-based column ordering first
					let columns =
						if let Some(schema) = self.get_table_schema_for_result(statement, ctx) {
							self.create_columns_from_schema(&schema, rows)
						} else {
							self.infer_columns_from_rows(rows)
						};

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
			QueryResult::Columns(column_info) => {
				let result_type = ResultType::Describe;

				let mut columns: Vec<ColumnMetadata> = vec![];

				for column in column_info {
					columns.push(ColumnMetadata {
						name: column.name.clone(),
						data_type: column.data_type.to_string(),
						nullable: column.nullable,
					})
				}

				ExecutionResultData {
					result_type,
					columns: columns,
					rows: Vec::new(),
					rows_affected: None,
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

	fn get_table_schema_for_result(
		&self,
		statement: Option<&Statement>,
		ctx: &ExecutionContext,
	) -> Option<crate::storage::TableSchema> {
		// Try to extract table name from the statement
		if let Some(Statement::Query(query)) = statement {
			if let Some(table_name) = self.extract_primary_table_name(query, ctx) {
				// Access the persistent storage engine if available
				if let Some(storage_engine) = ctx.get_storage_engine() {
					if let Ok(engine) = storage_engine.read() {
						return engine.get_table_schema(&table_name).ok().flatten();
					}
				}
			}
		}
		None
	}

	fn extract_primary_table_name(
		&self,
		query: &crate::nsql::ast::Query,
		ctx: &ExecutionContext,
	) -> Option<String> {
		// Extract table name from the FROM clause of the query
		if let Some(from) = &query.from {
			// from is a QualifiedName, get the first part as table name
			if let Some(first_part) = from.parts.first() {
				return Some(
					ctx.interner
						.resolve(*first_part)
						.unwrap_or("unknown")
						.to_string(),
				);
			}
		}
		None
	}

	fn create_columns_from_schema(
		&self,
		schema: &crate::storage::TableSchema,
		rows: &[crate::executor::Row],
	) -> Vec<ColumnMetadata> {
		// Use schema column order but only include columns that are present in the result
		let available_columns: std::collections::HashSet<String> =
			if let Some(first_row) = rows.first() {
				first_row.data.keys().cloned().collect()
			} else {
				std::collections::HashSet::new()
			};

		schema
			.columns
			.iter()
			.filter(|col| available_columns.contains(&col.name))
			.map(|col| ColumnMetadata {
				name: col.name.clone(),
				data_type: self.storage_type_to_string(&col.data_type),
				nullable: !col
					.constraints
					.iter()
					.any(|c| matches!(c, crate::storage::ColumnConstraint::NotNull)),
			})
			.collect()
	}

	fn storage_type_to_string(&self, data_type: &crate::storage::DataType) -> String {
		match data_type {
			crate::storage::DataType::Boolean => "BOOLEAN".to_string(),
			crate::storage::DataType::TinyInt => "TINYINT".to_string(),
			crate::storage::DataType::SmallInt => "SMALLINT".to_string(),
			crate::storage::DataType::Integer => "INTEGER".to_string(),
			crate::storage::DataType::BigInt => "BIGINT".to_string(),
			crate::storage::DataType::Float => "FLOAT".to_string(),
			crate::storage::DataType::Double => "DOUBLE".to_string(),
			crate::storage::DataType::Decimal { precision, scale } => {
				format!("DECIMAL({},{})", precision, scale)
			}
			crate::storage::DataType::Char(len) => format!("CHAR({})", len),
			crate::storage::DataType::Varchar(len) => format!("VARCHAR({})", len),
			crate::storage::DataType::Text => "TEXT".to_string(),
			crate::storage::DataType::Binary(len) => format!("BINARY({})", len),
			crate::storage::DataType::Varbinary(len) => format!("VARBINARY({})", len),
			crate::storage::DataType::Blob => "BLOB".to_string(),
			crate::storage::DataType::Date => "DATE".to_string(),
			crate::storage::DataType::Time => "TIME".to_string(),
			crate::storage::DataType::Timestamp => "TIMESTAMP".to_string(),
			crate::storage::DataType::Json => "JSON".to_string(),
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

pub fn format_table_info_list(tables: &[TableInfo]) -> String {
	if tables.is_empty() {
		return "No tables found".to_string();
	}

	let mut table = Table::new();

	table
		.set_content_arrangement(ContentArrangement::Dynamic)
		.set_width(80);

	// Headers
	table.set_header(vec![
		Cell::new("Table Name")
			.add_attribute(Attribute::Bold)
			.fg(Color::Cyan),
		Cell::new("Columns")
			.add_attribute(Attribute::Bold)
			.fg(Color::Cyan),
		Cell::new("Row Count")
			.add_attribute(Attribute::Bold)
			.fg(Color::Cyan),
	]);

	// Add table info rows
	for table_info in tables {
		table.add_row(vec![
			Cell::new(&table_info.name),
			Cell::new(table_info.columns.len().to_string()),
			Cell::new(
				table_info
					.row_count
					.map(|c| c.to_string())
					.unwrap_or_else(|| "Unknown".to_string()),
			),
		]);
	}

	let count_text = format!(
		"({} table{})",
		tables.len(),
		if tables.len() == 1 { "" } else { "s" }
	);
	format!("{}\n\n{}", table.to_string(), count_text)
}

pub fn format_table_for_display_with_schema(
	result: &ExecutionResultData,
	schema: Option<&crate::storage::TableSchema>,
) -> String {
	if result.rows.is_empty() {
		return "Empty result set".to_string();
	}

	// Use schema column order if available, otherwise use result column order
	let ordered_columns = if let Some(schema) = schema {
		// Create a map for quick lookup of result columns
		let result_column_map: std::collections::HashMap<&str, &ColumnMetadata> = result
			.columns
			.iter()
			.map(|c| (c.name.as_str(), c))
			.collect();

		// Use schema order for columns that exist in the result
		schema
			.columns
			.iter()
			.filter_map(|schema_col| result_column_map.get(schema_col.name.as_str()))
			.copied()
			.collect::<Vec<_>>()
	} else {
		result.columns.iter().collect()
	};

	if ordered_columns.is_empty() {
		return "Empty result set".to_string();
	}

	// Create comfy table
	let mut table = Table::new();

	// Set table style
	table
		.set_content_arrangement(ContentArrangement::Dynamic)
		.set_width(80);

	// Add header row
	let headers: Vec<Cell> = ordered_columns
		.iter()
		.map(|col| {
			Cell::new(&col.name)
				.add_attribute(Attribute::Bold)
				.fg(Color::Cyan)
		})
		.collect();
	table.set_header(headers);

	// Add data rows with proper column ordering
	for row in &result.rows {
		let row_cells: Vec<Cell> = ordered_columns
			.iter()
			.map(|col| {
				// Find the index of this column in the original result
				if let Some(original_index) = result.columns.iter().position(|c| c.name == col.name)
				{
					let value = row
						.get(original_index)
						.map(|v| format_value_for_display(v))
						.unwrap_or_else(|| "NULL".to_string());

					// Color NULL values differently
					if value == "NULL" {
						Cell::new(value).fg(Color::DarkGrey)
					} else {
						Cell::new(value)
					}
				} else {
					Cell::new("NULL").fg(Color::DarkGrey)
				}
			})
			.collect();
		table.add_row(row_cells);
	}

	// Add row count footer
	let row_count_text = format!(
		"({} row{})",
		result.rows.len(),
		if result.rows.len() == 1 { "" } else { "s" }
	);

	format!("{}\n\n{}", table.to_string(), row_count_text)
}

// Keep the original function for backward compatibility
pub fn format_table_for_display(result: &ExecutionResultData) -> String {
	format_table_for_display_with_schema(result, None)
}

fn format_value_for_display(value: &SerializableValue) -> String {
	match value {
		SerializableValue::Integer(i) => i.to_string(),
		SerializableValue::Float(f) => {
			// Format floats with reasonable precision
			if f.fract() == 0.0 {
				format!("{:.0}", f)
			} else {
				format!("{:.6}", f)
					.trim_end_matches('0')
					.trim_end_matches('.')
					.to_string()
			}
		}
		SerializableValue::String(s) => s.clone(),
		SerializableValue::Boolean(b) => {
			if *b {
				"true".to_string()
			} else {
				"false".to_string()
			}
		}
		SerializableValue::Null => "NULL".to_string(),
		SerializableValue::Date(d) => d.clone(),
		SerializableValue::DateTime(dt) => dt.clone(),
	}
}

/// Format execution result for simple status messages
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
		ResultType::Describe => {
			if result.columns.is_empty() {
				"No columns found".to_string()
			} else {
				let column_info: Vec<String> = result
					.columns
					.iter()
					.map(|col| format!("{}: {}", col.name, col.data_type))
					.collect();
				format!("Columns: {}", column_info.join(", "))
			}
		}
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

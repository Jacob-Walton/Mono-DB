use super::*;
use crate::nsql::ast::DataType;
use crate::storage::{
	Column as StorageColumn, DataType as StorageDataType, Engine as StorageEngineImpl,
	IsolationLevel, TxnId,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ColumnInfo {
	pub name: String,
	pub data_type: DataType,
	pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
	Integer(i64),
	Float(f64),
	String(String),
	Boolean(bool),
	Null,
	Date(chrono::NaiveDate),
	DateTime(chrono::NaiveDateTime),
}

pub trait StorageEngine: Send + Sync {
	fn create_table(&mut self, name: &str, columns: &[ColumnInfo]) -> ExecutionResult<()>;
	fn drop_table(&mut self, name: &str) -> ExecutionResult<()>;
	fn insert_row(&mut self, table: &str, row: HashMap<String, Value>) -> ExecutionResult<()>;
	fn scan_table(&self, table: &str) -> ExecutionResult<Vec<HashMap<String, Value>>>;
	fn get_table_schema(&self, table: &str) -> ExecutionResult<Vec<ColumnInfo>>;
}

pub struct MemoryStorage {
	tables: HashMap<String, Table>,
	schemas: HashMap<String, Vec<ColumnInfo>>,
}

struct Table {
	#[allow(unused)]
	schema: Vec<ColumnInfo>,
	rows: Vec<HashMap<String, Value>>,
}

impl MemoryStorage {
	pub fn new() -> Self {
		Self {
			tables: HashMap::new(),
			schemas: HashMap::new(),
		}
	}
}

impl StorageEngine for MemoryStorage {
	fn create_table(&mut self, name: &str, columns: &[ColumnInfo]) -> ExecutionResult<()> {
		let table = Table {
			schema: columns.to_vec(),
			rows: Vec::new(),
		};

		self.tables.insert(name.to_string(), table);
		self.schemas.insert(name.to_string(), columns.to_vec());
		Ok(())
	}

	fn drop_table(&mut self, name: &str) -> ExecutionResult<()> {
		if self.tables.remove(name).is_some() {
			self.schemas.remove(name);
			Ok(())
		} else {
			Err(ExecutionError::TableNotFound(name.to_string()))
		}
	}

	fn insert_row(&mut self, table: &str, row: HashMap<String, Value>) -> ExecutionResult<()> {
		let table = self
			.tables
			.get_mut(table)
			.ok_or_else(|| ExecutionError::TableNotFound(table.to_string()))?;

		table.rows.push(row);
		Ok(())
	}

	fn scan_table(&self, table: &str) -> ExecutionResult<Vec<HashMap<String, Value>>> {
		let table = self
			.tables
			.get(table)
			.ok_or_else(|| ExecutionError::TableNotFound(table.to_string()))?;

		Ok(table.rows.clone())
	}

	fn get_table_schema(&self, table: &str) -> ExecutionResult<Vec<ColumnInfo>> {
		self.schemas
			.get(table)
			.cloned()
			.ok_or_else(|| ExecutionError::TableNotFound(table.to_string()))
	}
}

/// Persistent storage engine implementation
pub struct PersistentStorage {
	engine: Arc<std::sync::RwLock<StorageEngineImpl>>,
	current_transaction: Option<TxnId>,
}

impl PersistentStorage {
	pub fn new(engine: Arc<std::sync::RwLock<StorageEngineImpl>>) -> Self {
		Self {
			engine,
			current_transaction: None,
		}
	}

	fn get_current_transaction(&mut self) -> ExecutionResult<TxnId> {
		if let Some(txn_id) = self.current_transaction {
			Ok(txn_id)
		} else {
			self.begin_transaction()
		}
	}

	pub fn begin_transaction(&mut self) -> ExecutionResult<TxnId> {
		let engine = self.engine.read().unwrap();
		let txn = engine
			.begin_transaction(IsolationLevel::ReadCommitted)
			.map_err(|e| ExecutionError::StorageError(e.to_string()))?;
		let txn_id = txn.id;
		self.current_transaction = Some(txn_id);
		Ok(txn_id)
	}

	pub fn commit_transaction(&mut self) -> ExecutionResult<()> {
		if let Some(txn_id) = self.current_transaction.take() {
			let engine = self.engine.read().unwrap();
			engine
				.commit_transaction(txn_id)
				.map_err(|e| ExecutionError::StorageError(e.to_string()))?;
		}
		Ok(())
	}

	pub fn abort_transaction(&mut self) -> ExecutionResult<()> {
		if let Some(txn_id) = self.current_transaction.take() {
			let engine = self.engine.read().unwrap();
			engine
				.abort_transaction(txn_id)
				.map_err(|e| ExecutionError::StorageError(e.to_string()))?;
		}
		Ok(())
	}

	// Auto-commit for now to ensure data is persisted
	fn auto_commit_if_needed(&mut self) -> ExecutionResult<()> {
		if self.current_transaction.is_some() {
			self.commit_transaction()?;
		}
		Ok(())
	}

	fn convert_column_info_to_storage_column(column_info: &ColumnInfo) -> StorageColumn {
		let mut column = StorageColumn::new(
			column_info.name.clone(),
			convert_data_type(&column_info.data_type),
		);

		if !column_info.nullable {
			column = column.not_null();
		}

		column
	}
}

impl StorageEngine for PersistentStorage {
	fn create_table(&mut self, name: &str, columns: &[ColumnInfo]) -> ExecutionResult<()> {
		let storage_columns: Vec<StorageColumn> = columns
			.iter()
			.map(Self::convert_column_info_to_storage_column)
			.collect();

		let engine = self.engine.write().unwrap();
		engine
			.create_table(name.to_string(), storage_columns)
			.map_err(|e| ExecutionError::StorageError(e.to_string()))?;

		Ok(())
	}

	fn drop_table(&mut self, name: &str) -> ExecutionResult<()> {
		let engine = self.engine.write().unwrap();

		// Check if table exists in catalog first
		if !engine.table_exists(name) {
			return Err(ExecutionError::TableNotFound(name.to_string()));
		}

		engine
			.drop_table(name)
			.map_err(|e| ExecutionError::StorageError(e.to_string()))?;

		Ok(())
	}

	fn insert_row(&mut self, table: &str, row: HashMap<String, Value>) -> ExecutionResult<()> {
		let txn_id = self.get_current_transaction()?;

		// Serialize the row data
		let tuple_data = serialize_row(&row)?;

		let engine = self.engine.read().unwrap();
		engine
			.insert(table, &tuple_data, txn_id)
			.map_err(|e| ExecutionError::StorageError(e.to_string()))?;

		// Auto-commit to ensure data is persisted
		drop(engine);
		self.auto_commit_if_needed()?;

		Ok(())
	}

	fn get_table_schema(&self, table: &str) -> ExecutionResult<Vec<ColumnInfo>> {
		let engine = match self.engine.read() {
			Ok(engine) => engine,
			Err(_) => {
				return Err(ExecutionError::StorageError(
					"Failed to acquire engine lock".to_string(),
				));
			}
		};

		// Get schema from catalog
		if let Some(schema) = engine
			.get_table_schema(table)
			.map_err(|e| ExecutionError::StorageError(e.to_string()))?
		{
			let column_infos = schema
				.columns
				.iter()
				.map(|col| ColumnInfo {
					name: col.name.clone(),
					data_type: convert_storage_data_type_to_ast(&col.data_type),
					nullable: !col
						.constraints
						.contains(&crate::storage::ColumnConstraint::NotNull),
				})
				.collect();

			Ok(column_infos)
		} else {
			Err(ExecutionError::TableNotFound(table.to_string()))
		}
	}

	fn scan_table(&self, table: &str) -> ExecutionResult<Vec<HashMap<String, Value>>> {
		let engine = match self.engine.read() {
			Ok(engine) => engine,
			Err(_) => {
				return Err(ExecutionError::StorageError(
					"Failed to acquire engine lock".to_string(),
				));
			}
		};

		// Verify table exists in catalog first
		if !engine.table_exists(table) {
			return Err(ExecutionError::TableNotFound(table.to_string()));
		}

		// Create a read transaction
		let txn = match engine.begin_transaction(IsolationLevel::ReadCommitted) {
			Ok(txn) => txn,
			Err(e) => {
				return Err(ExecutionError::StorageError(format!(
					"Failed to begin transaction: {}",
					e
				)));
			}
		};

		let mut rows = Vec::new();

		// Use scan_table with a callback
		let scan_result = engine.scan_table(table, txn.id, |_page_id, _tuple_id, tuple_data| {
			match deserialize_row(tuple_data) {
				Ok(row) => {
					rows.push(row);
					Ok(true) // Continue scanning
				}
				Err(_) => Ok(true), // Skip malformed rows but continue
			}
		});

		// Always commit the transaction
		let _ = engine.commit_transaction(txn.id);

		// Handle scan result
		match scan_result {
			Ok(_) => Ok(rows),
			Err(e) => Err(ExecutionError::StorageError(format!("Scan failed: {}", e))),
		}
	}
}

fn convert_data_type(data_type: &crate::nsql::ast::DataType) -> StorageDataType {
	match data_type {
		crate::nsql::ast::DataType::Integer => StorageDataType::Integer,
		crate::nsql::ast::DataType::Text => StorageDataType::Text,
		crate::nsql::ast::DataType::Boolean => StorageDataType::Boolean,
		crate::nsql::ast::DataType::Varchar(size) => {
			StorageDataType::Varchar(size.unwrap_or(255) as u16)
		}
		crate::nsql::ast::DataType::Char(size) => StorageDataType::Char(size.unwrap_or(1) as u16),
		crate::nsql::ast::DataType::Decimal(precision_scale) => {
			if let Some((precision, scale)) = precision_scale {
				StorageDataType::Decimal {
					precision: *precision,
					scale: *scale,
				}
			} else {
				StorageDataType::Decimal {
					precision: 10,
					scale: 2,
				}
			}
		}
	}
}

fn serialize_row(row: &HashMap<String, Value>) -> ExecutionResult<Vec<u8>> {
	bincode::serialize(row)
		.map_err(|e| ExecutionError::StorageError(format!("Serialization error: {}", e)))
}

fn deserialize_row(data: &[u8]) -> ExecutionResult<HashMap<String, Value>> {
	bincode::deserialize(data)
		.map_err(|e| ExecutionError::StorageError(format!("Deserialization error: {}", e)))
}

fn convert_storage_data_type_to_ast(storage_type: &StorageDataType) -> crate::nsql::ast::DataType {
	match storage_type {
		StorageDataType::Integer => crate::nsql::ast::DataType::Integer,
		StorageDataType::Text => crate::nsql::ast::DataType::Text,
		StorageDataType::Boolean => crate::nsql::ast::DataType::Boolean,
		StorageDataType::Varchar(size) => crate::nsql::ast::DataType::Varchar(Some(*size as u32)),
		StorageDataType::Char(size) => crate::nsql::ast::DataType::Char(Some(*size as u32)),
		StorageDataType::Decimal { precision, scale } => {
			crate::nsql::ast::DataType::Decimal(Some((*precision as u8, *scale as u8)))
		}
		_ => crate::nsql::ast::DataType::Text, // Default fallback
	}
}

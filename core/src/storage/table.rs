//! Table schema and management

use crate::error::{MonoError, MonoResult};
use crate::storage::{BufferPool, PageId, PageType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Table identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u32);

/// Column data types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
	Boolean,
	TinyInt,
	SmallInt,
	Integer,
	BigInt,
	Float,
	Double,
	Decimal { precision: u8, scale: u8 },
	Char(u16),
	Varchar(u16),
	Text,
	Binary(u16),
	Varbinary(u16),
	Blob,
	Date,
	Time,
	Timestamp,
	Json,
}

/// Column constraints
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnConstraint {
	NotNull,
	PrimaryKey,
	Unique,
	Default(Vec<u8>),
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
	pub name: String,
	pub data_type: DataType,
	pub constraints: Vec<ColumnConstraint>,
}

impl Column {
	/// Create a new column
	pub fn new(name: String, data_type: DataType) -> Self {
		Self {
			name,
			data_type,
			constraints: Vec::new(),
		}
	}

	/// Add NOT NULL constraint
	pub fn not_null(mut self) -> Self {
		self.constraints.push(ColumnConstraint::NotNull);
		self
	}

	/// Add PRIMARY KEY constraint
	pub fn primary_key(mut self) -> Self {
		self.constraints.push(ColumnConstraint::PrimaryKey);
		self.constraints.push(ColumnConstraint::NotNull);
		self
	}

	/// Add UNIQUE constraint
	pub fn unique(mut self) -> Self {
		self.constraints.push(ColumnConstraint::Unique);
		self
	}
}

/// Table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
	pub id: TableId,
	pub name: String,
	pub columns: Vec<Column>,
}

/// Table storage
pub struct Table {
	pub schema: TableSchema,
	pub first_page_id: Option<PageId>,
	buffer_pool: Arc<BufferPool>,
}

impl Table {
	/// Create a new table
	pub fn new(schema: TableSchema, buffer_pool: Arc<BufferPool>) -> Self {
		Self {
			schema,
			first_page_id: None,
			buffer_pool,
		}
	}

	/// Insert a tuple
	pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> MonoResult<(PageId, u16)> {
		// Find or create a page with space
		let (page_id, frame) = if let Some(first_page) = self.first_page_id {
			// Try first page
			let frame = self.buffer_pool.fetch_page(&self.schema.name, first_page)?;

			// Check if it has space
			let has_space = {
				let page_guard = frame.page.read();
				if let Some(page) = page_guard.as_ref() {
					page.free_space() >= tuple_data.len() + 4
				} else {
					false
				}
			};

			if has_space {
				(first_page, frame)
			} else {
				// Create new page
				let (new_page_id, new_frame) = self
					.buffer_pool
					.new_page(&self.schema.name, PageType::Table)?;
				(new_page_id, new_frame)
			}
		} else {
			// Create first page
			let (new_page_id, new_frame) = self
				.buffer_pool
				.new_page(&self.schema.name, PageType::Table)?;
			self.first_page_id = Some(new_page_id);
			(new_page_id, new_frame)
		};

		// Insert tuple
		let tuple_id = {
			let mut page_guard = frame.page.write();
			if let Some(page) = page_guard.as_mut() {
				page.add_tuple(tuple_data)?
			} else {
				return Err(MonoError::Storage("Page not loaded".into()));
			}
		};

		// Mark frame as dirty
		frame
			.is_dirty
			.store(true, std::sync::atomic::Ordering::Relaxed);

		// Unpin
		self.buffer_pool
			.unpin_page(&self.schema.name, page_id, true)?;

		Ok((page_id, tuple_id))
	}

	/// Read a tuple with proper column ordering
	pub fn read_tuple(&self, page_id: PageId, tuple_id: u16) -> MonoResult<Vec<u8>> {
		let frame = self.buffer_pool.fetch_page_with_schema(
			&self.schema.name,
			page_id,
			Some(self.schema.clone()),
		)?;

		let tuple_data = {
			let page_guard = frame.page.read();
			if let Some(page) = page_guard.as_ref() {
				page.get_tuple(tuple_id)?
			} else {
				return Err(MonoError::Storage("Page not loaded".into()));
			}
		};

		self.buffer_pool
			.unpin_page(&self.schema.name, page_id, false)?;

		Ok(tuple_data)
	}

	/// Scan all tuples with schema-aware ordering
	pub fn scan_tuples<F>(&self, mut callback: F) -> MonoResult<()>
	where
		F: FnMut(PageId, u16, &[u8]) -> MonoResult<bool>,
	{
		if let Some(first_page) = self.first_page_id {
			let mut current_page = first_page;

			loop {
				let frame = self.buffer_pool.fetch_page_with_schema(
					&self.schema.name,
					current_page,
					Some(self.schema.clone()),
				)?;

				let (tuple_count, next_page) = {
					let page_guard = frame.page.read();
					if let Some(page) = page_guard.as_ref() {
						(page.header.tuple_count, PageId(page.header.next_page_id))
					} else {
						return Err(MonoError::Storage("Page not loaded".into()));
					}
				};

				// Scan tuples in this page with proper schema context
				for tuple_id in 0..tuple_count {
					let page_guard = frame.page.read();
					if let Some(page) = page_guard.as_ref() {
						if let Ok(tuple_data) = page.get_tuple(tuple_id) {
							if !callback(current_page, tuple_id, &tuple_data)? {
								self.buffer_pool.unpin_page(
									&self.schema.name,
									current_page,
									false,
								)?;
								return Ok(());
							}
						}
					}
				}

				self.buffer_pool
					.unpin_page(&self.schema.name, current_page, false)?;

				// Move to next page
				if next_page.0 == 0 {
					break;
				}
				current_page = next_page;
			}
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::DiskManager;
	use tempfile::tempdir;

	#[test]
	fn test_table_operations() {
		let temp_dir = tempdir().unwrap();
		let disk_manager = Arc::new(DiskManager::new(temp_dir.path()).unwrap());
		let buffer_pool = Arc::new(BufferPool::new(10, disk_manager));

		let schema = TableSchema {
			id: TableId(1),
			name: "test_table".to_string(),
			columns: vec![
				Column::new("id".to_string(), DataType::Integer).primary_key(),
				Column::new("name".to_string(), DataType::Varchar(255)),
			],
		};

		let mut table = Table::new(schema, buffer_pool);

		// Insert tuples
		let (page_id1, tuple_id1) = table.insert_tuple(b"tuple1").unwrap();
		let (page_id2, tuple_id2) = table.insert_tuple(b"tuple2").unwrap();

		// Read tuples
		assert_eq!(table.read_tuple(page_id1, tuple_id1).unwrap(), b"tuple1");
		assert_eq!(table.read_tuple(page_id2, tuple_id2).unwrap(), b"tuple2");

		// Scan tuples
		let mut scanned = Vec::new();
		table
			.scan_tuples(|_, _, data| {
				scanned.push(data.to_vec());
				Ok(true)
			})
			.unwrap();

		assert_eq!(scanned.len(), 2);
	}
}

/// Simple table catalog for managing table metadata
pub struct TableCatalog {
	tables: HashMap<String, TableSchema>,
	next_table_id: u32,
}

impl TableCatalog {
	/// Create a new table catalog
	pub fn new() -> Self {
		Self {
			tables: HashMap::new(),
			next_table_id: 1,
		}
	}

	/// Create a table
	pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> MonoResult<TableSchema> {
		if self.tables.contains_key(&name) {
			return Err(MonoError::Storage(format!(
				"Table '{}' already exists",
				name
			)));
		}

		let schema = TableSchema {
			id: TableId(self.next_table_id),
			name: name.clone(),
			columns,
		};

		self.next_table_id += 1;
		self.tables.insert(name, schema.clone());

		Ok(schema)
	}

	/// Get table schema
	pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
		self.tables.get(name)
	}

	/// Drop a table
	pub fn drop_table(&mut self, name: &str) -> MonoResult<()> {
		if self.tables.remove(name).is_some() {
			Ok(())
		} else {
			Err(MonoError::Storage(format!("Table '{}' not found", name)))
		}
	}

	/// List all tables
	pub fn list_tables(&self) -> Vec<String> {
		self.tables.keys().cloned().collect()
	}
}

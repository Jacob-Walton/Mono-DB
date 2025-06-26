//! Persistent table catalog

use crate::error::{MonoError, MonoResult};
use crate::storage::TableSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogEntry {
	pub schema: TableSchema,
	pub created_at: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentCatalog {
	pub tables: HashMap<String, CatalogEntry>,
	pub next_table_id: u32,
}

impl PersistentCatalog {
	pub fn new() -> Self {
		Self {
			tables: HashMap::new(),
			next_table_id: 1,
		}
	}

	/// Load catalog from disk
	pub fn load<P: AsRef<Path>>(path: P) -> MonoResult<Self> {
		match File::open(path) {
			Ok(mut file) => {
				let mut contents = String::new();
				file.read_to_string(&mut contents)?;

				serde_json::from_str(&contents)
					.map_err(|e| MonoError::Storage(format!("Failed to parse catalog: {}", e)))
			}
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new()),
			Err(e) => Err(MonoError::Io(e)),
		}
	}

	/// Save catalog to disk
	pub fn save<P: AsRef<Path>>(&self, path: P) -> MonoResult<()> {
		let json = serde_json::to_string_pretty(self)
			.map_err(|e| MonoError::Storage(format!("Failed to serialize catalog: {}", e)))?;

		let mut file = OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(path)?;

		file.write_all(json.as_bytes())?;
		file.sync_all()?;

		Ok(())
	}

	/// Add a table to the catalog
	pub fn add_table(&mut self, schema: TableSchema) -> MonoResult<()> {
		let entry = CatalogEntry {
			schema: schema.clone(),
			created_at: std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_secs(),
		};

		self.tables.insert(schema.name.clone(), entry);
		Ok(())
	}

	/// Remove a table from the catalog
	pub fn remove_table(&mut self, name: &str) -> MonoResult<()> {
		self.tables.remove(name);
		Ok(())
	}

	/// Get table schema
	pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
		self.tables.get(name).map(|entry| &entry.schema)
	}

	/// List all tables
	pub fn list_tables(&self) -> Vec<String> {
		self.tables.keys().cloned().collect()
	}
}

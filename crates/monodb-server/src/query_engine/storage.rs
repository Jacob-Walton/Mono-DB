#![allow(dead_code)]

//! Storage Interface for Query Engine
//!
//! Bridges the query engine to the underlying storage system.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use monodb_common::{MonoError, Result, Value};

use crate::storage::engine::{StorageEngine, TableInfo};

// Row Types

/// A single row of data with named columns.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Row {
    pub columns: IndexMap<String, Value>,
}

impl Row {
    pub fn new() -> Self {
        Self {
            columns: IndexMap::new(),
        }
    }

    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns.get(column)
    }

    pub fn insert(&mut self, column: impl Into<String>, value: Value) {
        self.columns.insert(column.into(), value);
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.columns.iter()
    }

    pub fn into_value(self) -> Value {
        Value::Row(self.columns)
    }

    pub fn project<'a>(&self, cols: impl IntoIterator<Item = &'a str>) -> Row {
        let mut result = Row::new();
        for col in cols {
            if let Some(v) = self.columns.get(col) {
                result.insert(col, v.clone());
            }
        }
        result
    }

    pub fn get_nested(&self, path: &[&str]) -> Option<&Value> {
        if path.is_empty() {
            return None;
        }
        let mut current = self.columns.get(path[0])?;
        for segment in &path[1..] {
            match current {
                Value::Object(obj) => current = obj.get(*segment)?,
                Value::Row(row) => current = row.get(*segment)?,
                _ => return None,
            }
        }
        Some(current)
    }
}

impl IntoIterator for Row {
    type Item = (String, Value);
    type IntoIter = indexmap::map::IntoIter<String, Value>;
    fn into_iter(self) -> Self::IntoIter {
        self.columns.into_iter()
    }
}

/// Batch of rows.
#[derive(Debug, Clone, Default)]
pub struct RowBatch {
    pub rows: Vec<Row>,
}

impl RowBatch {
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }
    pub fn push(&mut self, row: Row) {
        self.rows.push(row);
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }
}

impl IntoIterator for RowBatch {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

// Scan Config

#[derive(Debug, Clone, Default)]
pub struct ScanConfig {
    pub columns: Option<Vec<String>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl ScanConfig {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }
    pub fn with_offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }
}

// Query Storage Trait

/// Abstract storage interface for the query engine.
pub trait QueryStorage: Send + Sync {
    fn table_exists(&self, table: &str) -> bool;
    fn get_table_info(&self, table: &str) -> Option<TableInfo>;
    fn begin_transaction(&self) -> Result<u64>;
    fn begin_read_only(&self) -> Result<u64>;
    fn commit(&self, tx_id: u64) -> Result<()>;
    fn rollback(&self, tx_id: u64) -> Result<()>;

    fn scan(&self, tx_id: u64, table: &str, config: &ScanConfig) -> Result<Vec<Row>>;
    fn read(&self, tx_id: u64, table: &str, key: &Value) -> Result<Option<Row>>;
    fn insert(&self, tx_id: u64, table: &str, row: Row) -> Result<()>;
    fn update(
        &self,
        tx_id: u64,
        table: &str,
        key: &Value,
        updates: HashMap<String, Value>,
    ) -> Result<bool>;
    fn delete(&self, tx_id: u64, table: &str, key: &Value) -> Result<bool>;

    fn create_table(&self, name: &str) -> Result<()>;
    fn drop_table(&self, name: &str) -> Result<()>;
}

// Storage Adapter

/// Bridges QueryStorage to the actual StorageEngine.
pub struct StorageAdapter {
    engine: Arc<StorageEngine>,
}

impl StorageAdapter {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Encode a key value to bytes using the native binary format.
    fn encode_key(value: &Value) -> Vec<u8> {
        value.to_bytes()
    }

    /// Encode a row to bytes using the native binary format.
    fn encode_row(row: &Row) -> Vec<u8> {
        row.clone().into_value().to_bytes()
    }

    /// Decode bytes to a row using the native binary format.
    fn decode_row(bytes: &[u8]) -> Result<Row> {
        let (value, _) = Value::from_bytes(bytes)?;
        match value {
            Value::Row(m) => Ok(Row { columns: m }),
            Value::Object(o) => Ok(Row {
                columns: o.into_iter().collect(),
            }),
            _ => Err(MonoError::Parse("expected row or object".into())),
        }
    }

    fn extract_key(row: &Row) -> Result<Value> {
        row.get("_id")
            .or_else(|| row.get("id"))
            .or_else(|| row.columns.values().next())
            .cloned()
            .ok_or_else(|| MonoError::InvalidOperation("no key".into()))
    }
}

impl QueryStorage for StorageAdapter {
    fn table_exists(&self, table: &str) -> bool {
        self.engine.get_table_info(table).is_some()
    }

    fn get_table_info(&self, table: &str) -> Option<TableInfo> {
        self.engine.get_table_info(table)
    }

    fn begin_transaction(&self) -> Result<u64> {
        self.engine.begin_transaction()
    }

    fn begin_read_only(&self) -> Result<u64> {
        self.engine.begin_read_only()
    }

    fn commit(&self, tx_id: u64) -> Result<()> {
        self.engine.commit(tx_id)
    }

    fn rollback(&self, tx_id: u64) -> Result<()> {
        self.engine.rollback(tx_id)
    }

    fn scan(&self, tx_id: u64, table: &str, config: &ScanConfig) -> Result<Vec<Row>> {
        let raw = self.engine.mvcc_scan(tx_id, table)?;
        let offset = config.offset.unwrap_or(0);
        let limit = config.limit.unwrap_or(usize::MAX);

        raw.into_iter()
            .skip(offset)
            .take(limit)
            .map(|(_, bytes)| Self::decode_row(&bytes))
            .collect()
    }

    fn read(&self, tx_id: u64, table: &str, key: &Value) -> Result<Option<Row>> {
        let key_bytes = Self::encode_key(key);
        match self.engine.mvcc_read(tx_id, table, &key_bytes)? {
            Some(bytes) => Ok(Some(Self::decode_row(&bytes)?)),
            None => Ok(None),
        }
    }

    fn insert(&self, tx_id: u64, table: &str, row: Row) -> Result<()> {
        let key = Self::extract_key(&row)?;
        let key_bytes = Self::encode_key(&key);
        let value_bytes = Self::encode_row(&row);
        self.engine.mvcc_write(tx_id, table, key_bytes, value_bytes)
    }

    fn update(
        &self,
        tx_id: u64,
        table: &str,
        key: &Value,
        updates: HashMap<String, Value>,
    ) -> Result<bool> {
        let key_bytes = Self::encode_key(key);
        let Some(bytes) = self.engine.mvcc_read(tx_id, table, &key_bytes)? else {
            return Ok(false);
        };
        let mut row = Self::decode_row(&bytes)?;
        for (col, val) in updates {
            row.insert(col, val);
        }
        self.engine
            .mvcc_write(tx_id, table, key_bytes, Self::encode_row(&row))?;
        Ok(true)
    }

    fn delete(&self, tx_id: u64, table: &str, key: &Value) -> Result<bool> {
        let key_bytes = Self::encode_key(key);
        self.engine.mvcc_delete(tx_id, table, &key_bytes)
    }

    fn create_table(&self, name: &str) -> Result<()> {
        self.engine.create_relational_table(name)
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        self.engine.drop_table(name)
    }
}

// In-Memory Storage (for testing)

/// Type alias for in-memory table storage.
type MemoryTableMap = HashMap<String, HashMap<Vec<u8>, Vec<u8>>>;

/// Simple in-memory storage for tests.
pub struct MemoryStorage {
    tables: parking_lot::RwLock<MemoryTableMap>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            tables: parking_lot::RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryStorage for MemoryStorage {
    fn table_exists(&self, table: &str) -> bool {
        self.tables.read().contains_key(table)
    }

    fn get_table_info(&self, table: &str) -> Option<TableInfo> {
        if self.table_exists(table) {
            Some(TableInfo {
                name: table.to_string(),
                storage_type: crate::storage::engine::StorageType::Relational,
                path: std::path::PathBuf::new(),
            })
        } else {
            None
        }
    }

    fn begin_transaction(&self) -> Result<u64> {
        Ok(1)
    }
    fn begin_read_only(&self) -> Result<u64> {
        Ok(1)
    }
    fn commit(&self, _: u64) -> Result<()> {
        Ok(())
    }
    fn rollback(&self, _: u64) -> Result<()> {
        Ok(())
    }

    fn scan(&self, _: u64, table: &str, config: &ScanConfig) -> Result<Vec<Row>> {
        let tables = self.tables.read();
        let tbl = tables
            .get(table)
            .ok_or_else(|| MonoError::NotFound(table.into()))?;
        tbl.values()
            .skip(config.offset.unwrap_or(0))
            .take(config.limit.unwrap_or(usize::MAX))
            .map(|b| StorageAdapter::decode_row(b))
            .collect()
    }

    fn read(&self, _: u64, table: &str, key: &Value) -> Result<Option<Row>> {
        let tables = self.tables.read();
        let tbl = tables
            .get(table)
            .ok_or_else(|| MonoError::NotFound(table.into()))?;
        match tbl.get(&StorageAdapter::encode_key(key)) {
            Some(b) => Ok(Some(StorageAdapter::decode_row(b)?)),
            None => Ok(None),
        }
    }

    fn insert(&self, _: u64, table: &str, row: Row) -> Result<()> {
        let key = StorageAdapter::extract_key(&row)?;
        let mut tables = self.tables.write();
        let tbl = tables
            .get_mut(table)
            .ok_or_else(|| MonoError::NotFound(table.into()))?;
        tbl.insert(
            StorageAdapter::encode_key(&key),
            StorageAdapter::encode_row(&row),
        );
        Ok(())
    }

    fn update(
        &self,
        _: u64,
        table: &str,
        key: &Value,
        updates: HashMap<String, Value>,
    ) -> Result<bool> {
        let key_bytes = StorageAdapter::encode_key(key);
        let mut tables = self.tables.write();
        let tbl = tables
            .get_mut(table)
            .ok_or_else(|| MonoError::NotFound(table.into()))?;
        let Some(bytes) = tbl.get(&key_bytes).cloned() else {
            return Ok(false);
        };
        let mut row = StorageAdapter::decode_row(&bytes)?;
        for (c, v) in updates {
            row.insert(c, v);
        }
        tbl.insert(key_bytes, StorageAdapter::encode_row(&row));
        Ok(true)
    }

    fn delete(&self, _: u64, table: &str, key: &Value) -> Result<bool> {
        let mut tables = self.tables.write();
        let tbl = tables
            .get_mut(table)
            .ok_or_else(|| MonoError::NotFound(table.into()))?;
        Ok(tbl.remove(&StorageAdapter::encode_key(key)).is_some())
    }

    fn create_table(&self, name: &str) -> Result<()> {
        self.tables.write().insert(name.to_string(), HashMap::new());
        Ok(())
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        self.tables.write().remove(name);
        Ok(())
    }
}

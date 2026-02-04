#![allow(dead_code)]

//! Storage Interface for Query Engine
//!
//! Bridges the query engine to the underlying storage system.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use monodb_common::{MonoError, Result, Value};

#[cfg(feature = "plugins")]
use crate::query_engine::ast::{Expr, Spanned};
use crate::storage::{
    StorageType,
    engine::{StorageEngine, TableInfo},
};

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

    fn create_table(
        &self,
        name: &str,
        table_type: crate::query_engine::ast::TableType,
    ) -> Result<()>;
    fn drop_table(&self, name: &str) -> Result<()>;

    /// Find rows by secondary index lookup. Returns primary keys as bytes.
    fn find_by_index(
        &self,
        table: &str,
        index_name: &str,
        lookup_values: &[Value],
    ) -> Result<Vec<Vec<u8>>>;
}

// Storage Adapter

/// Bridges QueryStorage to the actual StorageEngine.
pub struct StorageAdapter {
    engine: Arc<StorageEngine>,
    #[cfg(feature = "plugins")]
    plugin_host: parking_lot::RwLock<Option<Arc<monodb_plugin::PluginHost>>>,
}

impl StorageAdapter {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self {
            engine,
            #[cfg(feature = "plugins")]
            plugin_host: parking_lot::RwLock::new(None),
        }
    }

    /// Set the plugin host reference
    #[cfg(feature = "plugins")]
    pub fn set_plugin_host(&self, host: Arc<monodb_plugin::PluginHost>) {
        *self.plugin_host.write() = Some(host);
    }

    /// Flush all data to disk.
    pub fn flush(&self) -> Result<()> {
        self.engine.flush()
    }

    /// Run a checkpoint (flush + WAL marker).
    pub fn checkpoint(&self) -> Result<()> {
        self.engine.checkpoint()
    }

    /// Encode a key value to bytes.
    fn encode_key(value: &Value) -> Vec<u8> {
        value.to_bytes()
    }

    /// Encode a row to bytes.
    fn encode_row(row: &Row) -> Vec<u8> {
        row.clone().into_value().to_bytes()
    }

    /// Decode bytes to a row.
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

    // Marker strings for dynamic defaults (evaluated at insert time)
    const DEFAULT_UUID_MARKER: &'static str = "__default_fn:uuid";
    const DEFAULT_NOW_MARKER: &'static str = "__default_fn:now";
    const DEFAULT_PLUGIN_PREFIX: &'static str = "__default_fn:plugin:";

    /// Evaluate a stored default value, generating fresh values for dynamic defaults.
    #[cfg(feature = "plugins")]
    fn evaluate_default(&self, default: &Value) -> Value {
        // Check if this is a dynamic default marker
        if let Value::String(s) = default {
            match s.as_str() {
                Self::DEFAULT_UUID_MARKER => {
                    return Value::Uuid(uuid::Uuid::new_v4());
                }
                Self::DEFAULT_NOW_MARKER => {
                    let now = chrono::Utc::now();
                    return Value::DateTime(
                        now.with_timezone(&chrono::FixedOffset::east_opt(0).unwrap()),
                    );
                }
                _ if s.starts_with(Self::DEFAULT_PLUGIN_PREFIX) => {
                    // Extract function name and call plugin
                    let func_name = &s[Self::DEFAULT_PLUGIN_PREFIX.len()..];
                    if let Some(host) = self.plugin_host.read().as_ref() {
                        let perms = monodb_plugin::PluginPermissions::read_only();
                        if let Ok(value) = host.call_function(func_name, vec![], perms) {
                            return value;
                        }
                    }
                    // Fall back to null if plugin call fails
                    return Value::Null;
                }
                _ => {}
            }
        }
        // Not a marker, return as-is
        default.clone()
    }

    /// Evaluate a stored default value, generating fresh values for dynamic defaults.
    #[cfg(not(feature = "plugins"))]
    fn evaluate_default(&self, default: &Value) -> Value {
        // Check if this is a dynamic default marker
        if let Value::String(s) = default {
            match s.as_str() {
                Self::DEFAULT_UUID_MARKER => {
                    return Value::Uuid(uuid::Uuid::new_v4());
                }
                Self::DEFAULT_NOW_MARKER => {
                    let now = chrono::Utc::now();
                    return Value::DateTime(
                        now.with_timezone(&chrono::FixedOffset::east_opt(0).unwrap()),
                    );
                }
                _ => {}
            }
        }
        // Not a marker, return as-is
        default.clone()
    }

    /// Evaluate a constant expression to a Value (for default values).
    fn eval_constant_expr(expr: &crate::query_engine::ast::Expr) -> Option<Value> {
        use crate::query_engine::ast::{Expr, LiteralValue};

        match expr {
            Expr::Literal(lit) => match &lit.value {
                LiteralValue::Null => Some(Value::Null),
                LiteralValue::Bool(b) => Some(Value::Bool(*b)),
                LiteralValue::Int64(i) => Some(Value::Int64(*i)),
                LiteralValue::Float64(f) => Some(Value::Float64(*f)),
                LiteralValue::String(s) => Some(Value::String(s.to_string())),
                LiteralValue::Bytes(b) => Some(Value::Binary(b.to_vec())),
            },
            Expr::FunctionCall { name, args: _ } => {
                let func_name = name.node.as_str().to_lowercase();
                match func_name.as_str() {
                    // Store markers for dynamic defaults
                    "now" | "current_timestamp" => {
                        Some(Value::String(Self::DEFAULT_NOW_MARKER.to_string()))
                    }
                    "uuid" | "gen_random_uuid" => {
                        Some(Value::String(Self::DEFAULT_UUID_MARKER.to_string()))
                    }
                    // For other functions, check if it's a plugin function
                    other => Self::check_plugin_function(other),
                }
            }
            Expr::Column(col_ref) => {
                // Handle identifiers that might be boolean constants or special values
                let name = col_ref.column.as_str().to_lowercase();
                match name.as_str() {
                    "true" => Some(Value::Bool(true)),
                    "false" => Some(Value::Bool(false)),
                    "null" => Some(Value::Null),
                    _ => None,
                }
            }
            Expr::Array(items) => {
                let values: Option<Vec<Value>> = items
                    .iter()
                    .map(|item| Self::eval_constant_expr(&item.node))
                    .collect();
                values.map(Value::Array)
            }
            Expr::Unary { op, operand } => {
                use crate::query_engine::ast::UnaryOp;
                let val = Self::eval_constant_expr(&operand.node)?;
                match op {
                    UnaryOp::Neg => match val {
                        Value::Int64(i) => Some(Value::Int64(-i)),
                        Value::Float64(f) => Some(Value::Float64(-f)),
                        _ => None,
                    },
                    UnaryOp::Not => match val {
                        Value::Bool(b) => Some(Value::Bool(!b)),
                        _ => None,
                    },
                    UnaryOp::IsNull => Some(Value::Bool(val == Value::Null)),
                    UnaryOp::IsNotNull => Some(Value::Bool(val != Value::Null)),
                }
            }
            _ => None,
        }
    }

    /// Check if a function name is a registered plugin function
    #[cfg(feature = "plugins")]
    fn check_plugin_function(name: &str) -> Option<Value> {
        Some(Value::String(format!(
            "{}{}",
            Self::DEFAULT_PLUGIN_PREFIX,
            name
        )))
    }

    #[cfg(not(feature = "plugins"))]
    fn check_plugin_function(_name: &str) -> Option<Value> {
        None
    }

    fn extract_key(row: &Row) -> Result<Value> {
        row.get("_id")
            .or_else(|| row.get("id"))
            .or_else(|| row.columns.values().next())
            .cloned()
            .ok_or_else(|| MonoError::InvalidOperation("no key".into()))
    }

    /// List all tables as (name, type) tuples
    pub fn list_tables(&self) -> Vec<(String, String)> {
        self.engine
            .list_tables()
            .into_iter()
            .map(|t| {
                let table_type = match t.storage_type {
                    crate::storage::engine::StorageType::Relational => "relational",
                    crate::storage::engine::StorageType::Document => "document",
                    crate::storage::engine::StorageType::Keyspace => "keyspace",
                };
                (t.name, table_type.to_string())
            })
            .collect()
    }

    /// List all namespace names
    pub fn list_namespaces(&self) -> Vec<String> {
        self.engine.namespace_manager().names()
    }

    /// Describe a table's schema
    pub fn describe_table(&self, table: &str) -> Result<Value> {
        use std::collections::BTreeMap;

        let schema = self
            .engine
            .schema_catalog()
            .get(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        let mut result: BTreeMap<String, Value> = BTreeMap::new();
        result.insert("name".to_string(), Value::String(schema.name.clone()));

        match schema.table_type {
            crate::storage::schema::StoredTableType::Relation => {
                result.insert("type".to_string(), Value::String("relational".to_string()));

                // Columns as array of objects with is_primary and is_unique
                let cols: Vec<Value> = schema
                    .columns
                    .iter()
                    .map(|col| {
                        let mut col_obj = BTreeMap::new();
                        col_obj.insert("name".to_string(), Value::String(col.name.clone()));
                        col_obj.insert(
                            "data_type".to_string(),
                            Value::String(format!("{:?}", col.data_type).to_lowercase()),
                        );
                        col_obj.insert("nullable".to_string(), Value::Bool(col.nullable));
                        let is_pk = schema.primary_key.contains(&col.name);
                        let has_unique_index = schema.indexes.iter().any(|idx| {
                            idx.unique && idx.columns.len() == 1 && idx.columns[0] == col.name
                        });
                        col_obj.insert("is_primary".to_string(), Value::Bool(is_pk));
                        col_obj.insert(
                            "is_unique".to_string(),
                            Value::Bool(is_pk || has_unique_index),
                        );
                        if let Some(ref default) = col.default {
                            // Format default value for display
                            let default_str = match default {
                                Value::String(s) => {
                                    // Check for dynamic default markers
                                    if s == Self::DEFAULT_UUID_MARKER {
                                        "uuid()".to_string()
                                    } else if s == Self::DEFAULT_NOW_MARKER {
                                        "now()".to_string()
                                    } else {
                                        format!("\"{}\"", s)
                                    }
                                }
                                Value::Null => "null".to_string(),
                                Value::Bool(b) => b.to_string(),
                                Value::Int32(i) => i.to_string(),
                                Value::Int64(i) => i.to_string(),
                                Value::Float32(f) => f.to_string(),
                                Value::Float64(f) => f.to_string(),
                                Value::DateTime(dt) => {
                                    format!("now() [{}]", dt.format("%Y-%m-%d %H:%M:%S"))
                                }
                                Value::Uuid(u) => format!("uuid() [{}]", u),
                                other => format!("{:?}", other),
                            };
                            col_obj.insert("default_value".to_string(), Value::String(default_str));
                        }
                        Value::Object(col_obj)
                    })
                    .collect();
                result.insert("columns".to_string(), Value::Array(cols));

                // Primary key as array of columns
                let pk: Vec<Value> = schema
                    .primary_key
                    .iter()
                    .map(|k| Value::String(k.clone()))
                    .collect();
                result.insert("primary_key".to_string(), Value::Array(pk));

                // Indexes from schema
                let indexes: Vec<Value> = schema
                    .indexes
                    .iter()
                    .map(|idx| {
                        let mut idx_obj = BTreeMap::new();
                        idx_obj.insert("name".to_string(), Value::String(idx.name.clone()));
                        idx_obj.insert(
                            "columns".to_string(),
                            Value::Array(
                                idx.columns
                                    .iter()
                                    .map(|c| Value::String(c.clone()))
                                    .collect(),
                            ),
                        );
                        idx_obj.insert("unique".to_string(), Value::Bool(idx.unique));
                        idx_obj.insert(
                            "type".to_string(),
                            Value::String(format!("{:?}", idx.index_type).to_lowercase()),
                        );
                        Value::Object(idx_obj)
                    })
                    .collect();
                result.insert("indexes".to_string(), Value::Array(indexes));
            }
            crate::storage::schema::StoredTableType::Document => {
                result.insert("type".to_string(), Value::String("document".to_string()));
                // Document collections can have indexes too
                let indexes: Vec<Value> = schema
                    .indexes
                    .iter()
                    .map(|idx| {
                        let mut idx_obj = BTreeMap::new();
                        idx_obj.insert("name".to_string(), Value::String(idx.name.clone()));
                        idx_obj.insert(
                            "columns".to_string(),
                            Value::Array(
                                idx.columns
                                    .iter()
                                    .map(|c| Value::String(c.clone()))
                                    .collect(),
                            ),
                        );
                        idx_obj.insert("unique".to_string(), Value::Bool(idx.unique));
                        idx_obj.insert(
                            "type".to_string(),
                            Value::String(format!("{:?}", idx.index_type).to_lowercase()),
                        );
                        Value::Object(idx_obj)
                    })
                    .collect();
                result.insert("indexes".to_string(), Value::Array(indexes));
            }
            crate::storage::schema::StoredTableType::Keyspace => {
                result.insert("type".to_string(), Value::String("keyspace".to_string()));
            }
        }

        Ok(Value::Object(result))
    }

    /// Create a new namespace
    pub fn create_namespace(&self, name: &str) -> Result<()> {
        self.engine.namespace_manager().create(name, None)?;
        self.engine.wal_log_namespace_create(name)?;
        Ok(())
    }

    /// Drop a namespace and all its tables
    pub fn drop_namespace(&self, name: &str, force: bool) -> Result<()> {
        let tables = self.engine.list_tables_in_namespace(name);
        self.engine.drop_namespace(name, force)?;
        for table in tables {
            self.engine.wal_log_schema_drop(&table.name)?;
        }
        self.engine.wal_log_namespace_drop(name, force)?;
        Ok(())
    }

    /// Add columns to an existing table
    pub fn add_columns(
        &self,
        table: &str,
        columns: &[crate::query_engine::ast::ColumnDef],
    ) -> Result<()> {
        use crate::storage::schema::{StoredColumnSchema, StoredDataType};

        let stored_columns: Vec<StoredColumnSchema> = columns
            .iter()
            .map(|col| {
                let data_type = match &col.data_type {
                    crate::query_engine::ast::DataType::Int32 => StoredDataType::Int32,
                    crate::query_engine::ast::DataType::Int64 => StoredDataType::Int64,
                    crate::query_engine::ast::DataType::Float32 => StoredDataType::Float32,
                    crate::query_engine::ast::DataType::Float64 => StoredDataType::Float64,
                    crate::query_engine::ast::DataType::String => StoredDataType::String,
                    crate::query_engine::ast::DataType::Bool => StoredDataType::Bool,
                    crate::query_engine::ast::DataType::DateTime => StoredDataType::DateTime,
                    crate::query_engine::ast::DataType::Bytes => StoredDataType::Bytes,
                    crate::query_engine::ast::DataType::Uuid => StoredDataType::Uuid,
                    crate::query_engine::ast::DataType::Json => StoredDataType::Json,
                    _ => StoredDataType::String,
                };

                let nullable = !col
                    .constraints
                    .iter()
                    .any(|c| matches!(c, crate::query_engine::ast::ColumnConstraint::NotNull));

                // Extract default value from constraints
                let default = col.constraints.iter().find_map(|c| {
                    if let crate::query_engine::ast::ColumnConstraint::Default(expr) = c {
                        // Evaluate the default expression to a Value
                        Self::eval_constant_expr(&expr.node)
                    } else {
                        None
                    }
                });

                StoredColumnSchema {
                    name: col.name.node.to_string(),
                    data_type,
                    nullable,
                    default,
                }
            })
            .collect();

        // Backfill existing rows with default values
        self.backfill_new_columns(table, &stored_columns)?;

        // Update schema catalog
        self.engine
            .schema_catalog()
            .add_columns(table, &stored_columns)?;
        self.wal_log_schema_upsert(table)
    }

    /// Backfill existing rows with default values for new columns.
    fn backfill_new_columns(
        &self,
        table: &str,
        columns: &[crate::storage::schema::StoredColumnSchema],
    ) -> Result<()> {
        // Collect columns that have default values
        let defaults: Vec<(String, Value)> = columns
            .iter()
            .filter_map(|col| col.default.as_ref().map(|d| (col.name.clone(), d.clone())))
            .collect();

        if defaults.is_empty() {
            return Ok(()); // Nothing to backfill
        }

        // Start a transaction for the backfill
        let tx_id = self.begin_transaction()?;

        // Scan all existing rows
        let config = ScanConfig::new();
        let rows = match self.scan(tx_id, table, &config) {
            Ok(rows) => rows,
            Err(e) => {
                let _ = self.rollback(tx_id);
                return Err(e);
            }
        };

        // Update each row with the default values
        for row in rows {
            let key = match Self::extract_key(&row) {
                Ok(k) => k,
                Err(_) => continue,
            };

            let mut updates = HashMap::new();
            for (col_name, default_val) in &defaults {
                // Only add if the column doesn't already exist in the row
                if row.get(col_name).is_none() {
                    updates.insert(col_name.clone(), default_val.clone());
                }
            }

            if !updates.is_empty()
                && let Err(e) = self.update(tx_id, table, &key, updates)
            {
                let _ = self.rollback(tx_id);
                return Err(e);
            }
        }

        self.commit(tx_id)?;
        Ok(())
    }

    /// Drop columns from an existing table
    pub fn drop_columns(&self, table: &str, columns: &[String]) -> Result<()> {
        // First get the schema to find the primary key
        let schema = self
            .engine
            .schema_catalog()
            .get(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        // Only relational tables support column drops
        if schema.table_type != crate::storage::schema::StoredTableType::Relation {
            return Err(MonoError::InvalidOperation(
                "Can only drop columns from relational tables".into(),
            ));
        }

        // Update the schema first
        self.engine.schema_catalog().drop_columns(table, columns)?;
        self.wal_log_schema_upsert(table)?;

        // Now update existing rows to remove the dropped columns
        let tx_id = self.engine.begin_transaction()?;
        match self.engine.mvcc_scan(tx_id, table) {
            Ok(rows) => {
                for (key_bytes, value_bytes) in rows {
                    // Decode the row
                    if let Ok(mut row) = Self::decode_row(&value_bytes) {
                        // Remove the dropped columns
                        let mut modified = false;
                        for col in columns {
                            if row.columns.shift_remove(col).is_some() {
                                modified = true;
                            }
                        }

                        // If modified, write back the row
                        if modified {
                            let new_value = Self::encode_row(&row);
                            if let Err(e) =
                                self.engine.mvcc_write(tx_id, table, key_bytes, new_value)
                            {
                                let _ = self.engine.rollback(tx_id);
                                return Err(e);
                            }
                        }
                    }
                }
                self.engine.commit(tx_id)?;
            }
            Err(e) => {
                let _ = self.engine.rollback(tx_id);
                return Err(e);
            }
        }

        Ok(())
    }

    /// Rename columns in an existing table
    pub fn rename_columns(&self, table: &str, renames: &[(String, String)]) -> Result<()> {
        // First get the schema to verify it's a relational table
        let schema = self
            .engine
            .schema_catalog()
            .get(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        // Only relational tables support column renames
        if schema.table_type != crate::storage::schema::StoredTableType::Relation {
            return Err(MonoError::InvalidOperation(
                "Can only rename columns in relational tables".into(),
            ));
        }

        // Update the schema first
        self.engine
            .schema_catalog()
            .rename_columns(table, renames)?;
        self.wal_log_schema_upsert(table)?;

        // Now update existing rows to rename the columns
        let tx_id = self.engine.begin_transaction()?;
        match self.engine.mvcc_scan(tx_id, table) {
            Ok(rows) => {
                for (key_bytes, value_bytes) in rows {
                    // Decode the row
                    if let Ok(mut row) = Self::decode_row(&value_bytes) {
                        // Rename the columns
                        let mut modified = false;
                        for (old_name, new_name) in renames {
                            if let Some(value) = row.columns.shift_remove(old_name) {
                                row.columns.insert(new_name.clone(), value);
                                modified = true;
                            }
                        }

                        // If modified, write back the row
                        if modified {
                            let new_value = Self::encode_row(&row);
                            if let Err(e) =
                                self.engine.mvcc_write(tx_id, table, key_bytes, new_value)
                            {
                                let _ = self.engine.rollback(tx_id);
                                return Err(e);
                            }
                        }
                    }
                }
                self.engine.commit(tx_id)?;
            }
            Err(e) => {
                let _ = self.engine.rollback(tx_id);
                return Err(e);
            }
        }

        Ok(())
    }

    /// Rename a table
    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        self.engine.rename_table(old_name, new_name)?;
        self.engine.wal_log_schema_drop(old_name)?;
        self.wal_log_schema_upsert(new_name)?;
        Ok(())
    }

    /// Alter a column's properties (default, nullability, type)
    pub fn alter_column(
        &self,
        table: &str,
        column: &str,
        new_default: Option<Option<Value>>,
        new_nullable: Option<bool>,
        new_type: Option<crate::storage::schema::StoredDataType>,
    ) -> Result<()> {
        self.engine.schema_catalog().alter_column(
            table,
            column,
            new_default,
            new_nullable,
            new_type,
        )?;
        self.wal_log_schema_upsert(table)
    }

    /// Evaluate a default expression to a Value
    #[cfg(feature = "plugins")]
    pub fn eval_default_expr(&self, expr: &Spanned<Expr>) -> Result<Value> {
        if let Some(value) = Self::eval_constant_expr(&expr.node) {
            // If it's a plugin function marker, verify the function exists
            if let Value::String(s) = &value
                && let Some(func_name) = s.strip_prefix(Self::DEFAULT_PLUGIN_PREFIX)
            {
                if let Some(host) = self.plugin_host.read().as_ref()
                    && host.has_function(func_name)
                {
                    return Ok(value);
                }
                return Err(MonoError::InvalidOperation(format!(
                    "Unknown function '{}' (no plugin provides this function)",
                    func_name
                )));
            }
            Ok(value)
        } else {
            Err(MonoError::InvalidOperation(
                "Cannot evaluate expression as constant default value".into(),
            ))
        }
    }

    /// Evaluate a default expression to a Value
    #[cfg(not(feature = "plugins"))]
    pub fn eval_default_expr(
        &self,
        expr: &crate::query_engine::ast::Spanned<crate::query_engine::ast::Expr>,
    ) -> Result<Value> {
        Self::eval_constant_expr(&expr.node).ok_or_else(|| {
            MonoError::InvalidOperation(
                "Cannot evaluate expression as constant default value".into(),
            )
        })
    }

    /// Create a table with full schema definition
    pub fn create_table_with_schema(
        &self,
        name: &str,
        table_type: crate::query_engine::ast::TableType,
        columns: &[crate::query_engine::ast::ColumnDef],
        constraints: &[crate::query_engine::ast::TableConstraint],
    ) -> Result<()> {
        use crate::query_engine::ast::TableType;
        use crate::storage::schema::{
            StoredColumnSchema, StoredDataType, StoredTableSchema, StoredTableType,
        };

        match table_type {
            TableType::Relational => {
                // Convert columns
                let stored_columns: Result<Vec<StoredColumnSchema>> = columns
                    .iter()
                    .map(|col| {
                        // Check constraints
                        let is_primary = col.constraints.iter().any(|c| {
                            matches!(c, crate::query_engine::ast::ColumnConstraint::PrimaryKey)
                        });
                        let is_not_null = col.constraints.iter().any(|c| {
                            matches!(c, crate::query_engine::ast::ColumnConstraint::NotNull)
                        });
                        let default_val = col.constraints.iter().find_map(|c| {
                            if let crate::query_engine::ast::ColumnConstraint::Default(expr) = c {
                                Some(expr)
                            } else {
                                None
                            }
                        });

                        // Evaluate default expression if present
                        let default = if let Some(expr) = default_val {
                            Some(self.eval_default_expr(expr)?)
                        } else {
                            None
                        };

                        Ok(StoredColumnSchema {
                            name: col.name.node.as_str().to_string(),
                            data_type: StoredDataType::from(&col.data_type),
                            nullable: !is_primary && !is_not_null,
                            default,
                        })
                    })
                    .collect();

                let stored_columns = stored_columns?;

                // Extract primary key columns
                let mut primary_keys = Vec::new();
                for col in columns {
                    if col.constraints.iter().any(|c| {
                        matches!(c, crate::query_engine::ast::ColumnConstraint::PrimaryKey)
                    }) {
                        primary_keys.push(col.name.node.as_str().to_string());
                    }
                }

                // Also check table-level constraints for primary key
                for constraint in constraints {
                    if let crate::query_engine::ast::TableConstraint::PrimaryKey(cols) = constraint
                    {
                        for col in cols {
                            let col_name = col.0.to_string();
                            if !primary_keys.contains(&col_name) {
                                primary_keys.push(col_name);
                            }
                        }
                    }
                }

                let mut unique_columns = Vec::new();
                for col in columns {
                    let is_unique = col
                        .constraints
                        .iter()
                        .any(|c| matches!(c, crate::query_engine::ast::ColumnConstraint::Unique));
                    if !is_unique {
                        continue;
                    }
                    let col_name = col.name.node.as_str().to_string();
                    if primary_keys.contains(&col_name) {
                        continue;
                    }
                    unique_columns.push(col_name);
                }

                // First create the physical table to get meta_page_id
                let meta_page_id = self.engine.create_relational_table(name)?;

                // Then register schema with the meta_page_id
                let schema = StoredTableSchema {
                    name: name.to_string(),
                    table_type: StoredTableType::Relation,
                    columns: stored_columns,
                    primary_key: primary_keys,
                    indexes: vec![],
                    version: 0,
                    created_at: 0,
                    meta_page_id,
                };

                self.engine.schema_catalog().register(schema)?;
                self.wal_log_schema_upsert(name)?;

                for col_name in unique_columns {
                    let index_name = format!("uniq_{}", col_name);
                    self.engine
                        .create_index(name, &index_name, vec![col_name], true)?;
                }
                self.wal_log_schema_upsert(name)?;

                Ok(())
            }
            TableType::Document => {
                // First create the physical store to get meta_page_id
                let meta_page_id = self.engine.create_document_store(name, true)?;

                // Then register schema with the meta_page_id
                let schema = StoredTableSchema {
                    name: name.to_string(),
                    table_type: StoredTableType::Document,
                    columns: vec![],
                    primary_key: vec![],
                    indexes: vec![],
                    version: 0,
                    created_at: 0,
                    meta_page_id,
                };

                self.engine.schema_catalog().register(schema)?;
                self.wal_log_schema_upsert(name)?;
                Ok(())
            }
            TableType::Keyspace => {
                // First create the physical keyspace to get meta_page_id
                let meta_page_id = self.engine.create_disk_keyspace(name)?;

                // Then register schema with the meta_page_id
                let schema = StoredTableSchema {
                    name: name.to_string(),
                    table_type: StoredTableType::Keyspace,
                    columns: vec![],
                    primary_key: vec![],
                    indexes: vec![],
                    version: 0,
                    created_at: 0,
                    meta_page_id,
                };

                self.engine.schema_catalog().register(schema)?;
                self.wal_log_schema_upsert(name)?;
                Ok(())
            }
        }
    }

    /// Create a secondary index on a table
    pub fn create_index(
        &self,
        table: &str,
        index_name: &str,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<()> {
        self.engine
            .create_index(table, index_name, columns, unique)?;
        self.wal_log_schema_upsert(table)
    }

    /// Drop a secondary index from a table
    pub fn drop_index(&self, table: &str, index_name: &str) -> Result<()> {
        self.engine.drop_index(table, index_name)?;
        self.wal_log_schema_upsert(table)
    }

    /// Find rows by secondary index lookup
    pub fn find_by_index(
        &self,
        table: &str,
        index_name: &str,
        lookup_values: &[Value],
    ) -> Result<Vec<Vec<u8>>> {
        self.engine.find_by_index(table, index_name, lookup_values)
    }

    /// Get all indexes for a table
    pub fn get_indexes(&self, table: &str) -> Vec<crate::storage::schema::StoredIndex> {
        self.engine.get_indexes(table)
    }

    fn wal_log_schema_upsert(&self, table: &str) -> Result<()> {
        if let Some(schema) = self.engine.schema_catalog().get(table) {
            self.engine.wal_log_schema_upsert(&schema)?;
        }
        Ok(())
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
        let storage_type = self
            .engine
            .get_storage_type(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        let offset = config.offset.unwrap_or(0);
        let limit = config.limit.unwrap_or(usize::MAX);

        let raw: Vec<(Vec<u8>, Vec<u8>)> = match storage_type {
            StorageType::Relational => self.engine.mvcc_scan(tx_id, table)?,
            StorageType::Document => self.engine.doc_scan(table)?,
            StorageType::Keyspace => self.engine.ks_scan(table)?,
        };

        raw.into_iter()
            .skip(offset)
            .take(limit)
            .map(|(key_bytes, value_bytes)| {
                let mut row = Self::decode_row(&value_bytes)?;
                // Inject the key as _id if not already present
                if !row.columns.contains_key("_id")
                    && !row.columns.contains_key("id")
                    && let Ok((key_value, _)) = Value::from_bytes(&key_bytes)
                {
                    row.insert("_id".to_string(), key_value);
                }

                Ok(row)
            })
            .collect()
    }

    fn read(&self, _tx_id: u64, table: &str, key: &Value) -> Result<Option<Row>> {
        use crate::storage::engine::StorageType;

        let storage_type = self
            .engine
            .get_storage_type(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        let key_bytes = Self::encode_key(key);

        let bytes_opt = match storage_type {
            StorageType::Relational => self.engine.mvcc_read(_tx_id, table, &key_bytes)?,
            StorageType::Document => self
                .engine
                .doc_get(table, &key_bytes)?
                .map(|(data, _)| data),
            StorageType::Keyspace => self.engine.ks_get(table, &key_bytes)?,
        };

        match bytes_opt {
            Some(bytes) => Ok(Some(Self::decode_row(&bytes)?)),
            None => Ok(None),
        }
    }

    fn insert(&self, _tx_id: u64, table: &str, mut row: Row) -> Result<()> {
        use crate::storage::engine::StorageType;
        use crate::storage::schema::StoredDataType;

        let storage_type = self
            .engine
            .get_storage_type(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        // For relational tables, apply schema-based transformations
        if matches!(storage_type, StorageType::Relational)
            && let Some(schema) = self.engine.schema_catalog().get(table)
        {
            // Handle primary key auto-generation
            if schema.primary_key.len() == 1 {
                let pk_name = &schema.primary_key[0];
                if !row.columns.contains_key(pk_name) {
                    // Check if PK column is numeric
                    if let Some(pk_col) = schema.columns.iter().find(|c| &c.name == pk_name)
                        && matches!(
                            pk_col.data_type,
                            StoredDataType::Int32 | StoredDataType::Int64
                        )
                    {
                        // Auto-generate primary key value, use a simple counter based on current rows
                        // Get max existing value and increment
                        let config = ScanConfig::new();
                        let existing_rows =
                            QueryStorage::scan(self, _tx_id, table, &config).unwrap_or_default();
                        let max_id = existing_rows
                            .iter()
                            .filter_map(|r| r.get(pk_name))
                            .filter_map(|v| match v {
                                Value::Int64(n) => Some(*n),
                                Value::Int32(n) => Some(*n as i64),
                                _ => None,
                            })
                            .max()
                            .unwrap_or(0);

                        let next_id = max_id + 1;
                        let pk_value = match pk_col.data_type {
                            StoredDataType::Int64 => Value::Int64(next_id),
                            StoredDataType::Int32 => Value::Int32(next_id as i32),
                            _ => unreachable!(),
                        };
                        row.insert(pk_name.clone(), pk_value);
                    }
                }
            }

            // Apply default values for missing columns
            for col in &schema.columns {
                if !row.columns.contains_key(&col.name) {
                    if let Some(default) = &col.default {
                        // Check if this is a dynamic default marker
                        let value = self.evaluate_default(default);
                        row.insert(col.name.clone(), value);
                    } else if !col.nullable {
                        // Required field missing without default
                        return Err(MonoError::InvalidOperation(format!(
                            "Required field '{}' is missing",
                            col.name
                        )));
                    }
                }
            }

            // Validate that non-nullable columns don't have NULL values
            for col in &schema.columns {
                if !col.nullable
                    && let Some(value) = row.columns.get(&col.name)
                    && matches!(value, Value::Null)
                {
                    return Err(MonoError::InvalidOperation(format!(
                        "Column '{}' cannot be NULL",
                        col.name
                    )));
                }
            }

            // Ensure all defined columns are included (even if null)
            for col in &schema.columns {
                if !row.columns.contains_key(&col.name) && col.nullable {
                    row.insert(col.name.clone(), Value::Null);
                }
            }

            // Rebuild row in schema column order
            let mut ordered_row = Row::new();
            for col in &schema.columns {
                if let Some(value) = row.columns.get(&col.name) {
                    ordered_row.insert(col.name.clone(), value.clone());
                }
            }
            row = ordered_row;
        }

        // For document and keyspace stores, auto-generate _id if not present
        if matches!(storage_type, StorageType::Document | StorageType::Keyspace)
            && !row.columns.contains_key("_id")
            && !row.columns.contains_key("id")
        {
            let oid = monodb_common::ObjectId::new()?;
            row.insert("_id".to_string(), Value::ObjectId(oid));
        }

        let key = Self::extract_key(&row)?;
        let key_bytes = Self::encode_key(&key);
        let value_bytes = Self::encode_row(&row);

        match storage_type {
            StorageType::Relational => {
                self.engine
                    .mvcc_write(_tx_id, table, key_bytes, value_bytes)
            }
            StorageType::Document => {
                self.engine.doc_insert(table, key_bytes, value_bytes)?;
                Ok(())
            }
            StorageType::Keyspace => self.engine.ks_set(table, key_bytes, value_bytes),
        }
    }

    fn update(
        &self,
        tx_id: u64,
        table: &str,
        key: &Value,
        updates: HashMap<String, Value>,
    ) -> Result<bool> {
        use crate::storage::engine::StorageType;

        let storage_type = self
            .engine
            .get_storage_type(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        let key_bytes = Self::encode_key(key);

        // Read existing row based on storage type
        let bytes_opt = match storage_type {
            StorageType::Relational => self.engine.mvcc_read(tx_id, table, &key_bytes)?,
            StorageType::Document => self
                .engine
                .doc_get(table, &key_bytes)?
                .map(|(data, _)| data),
            StorageType::Keyspace => self.engine.ks_get(table, &key_bytes)?,
        };

        let Some(bytes) = bytes_opt else {
            return Ok(false);
        };

        let mut row = Self::decode_row(&bytes)?;
        for (col, val) in updates {
            row.insert(col, val);
        }

        let new_bytes = Self::encode_row(&row);

        match storage_type {
            StorageType::Relational => {
                self.engine.mvcc_write(tx_id, table, key_bytes, new_bytes)?;
            }
            StorageType::Document => {
                self.engine.doc_upsert(table, key_bytes, new_bytes)?;
            }
            StorageType::Keyspace => {
                self.engine.ks_set(table, key_bytes, new_bytes)?;
            }
        }
        Ok(true)
    }

    fn delete(&self, tx_id: u64, table: &str, key: &Value) -> Result<bool> {
        use crate::storage::engine::StorageType;

        let storage_type = self
            .engine
            .get_storage_type(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        let key_bytes = Self::encode_key(key);

        match storage_type {
            StorageType::Relational => self.engine.mvcc_delete(tx_id, table, &key_bytes),
            StorageType::Document => {
                // Get current revision for optimistic concurrency check
                if let Some((_, revision)) = self.engine.doc_get(table, &key_bytes)? {
                    self.engine.doc_delete(table, &key_bytes, revision)
                } else {
                    Ok(false) // Document doesn't exist
                }
            }
            StorageType::Keyspace => self.engine.ks_delete(table, &key_bytes),
        }
    }

    fn create_table(
        &self,
        name: &str,
        table_type: crate::query_engine::ast::TableType,
    ) -> Result<()> {
        use crate::query_engine::ast::TableType;
        match table_type {
            TableType::Relational => {
                self.engine.create_relational_table(name)?;
                Ok(())
            }
            TableType::Document => {
                self.engine.create_document_store(name, true)?;
                Ok(())
            }
            TableType::Keyspace => {
                self.engine.create_disk_keyspace(name)?;
                Ok(())
            }
        }
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        self.engine.drop_table(name)?;
        self.engine.wal_log_schema_drop(name)?;
        Ok(())
    }

    fn find_by_index(
        &self,
        table: &str,
        index_name: &str,
        lookup_values: &[Value],
    ) -> Result<Vec<Vec<u8>>> {
        self.engine.find_by_index(table, index_name, lookup_values)
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

    fn create_table(
        &self,
        name: &str,
        _table_type: crate::query_engine::ast::TableType,
    ) -> Result<()> {
        self.tables.write().insert(name.to_string(), HashMap::new());
        Ok(())
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        self.tables.write().remove(name);
        Ok(())
    }

    fn find_by_index(
        &self,
        _table: &str,
        _index_name: &str,
        _lookup_values: &[Value],
    ) -> Result<Vec<Vec<u8>>> {
        // In-memory storage doesn't support secondary indexes
        Ok(vec![])
    }
}

// Plugin Storage Implementation

#[cfg(feature = "plugins")]
impl monodb_plugin::PluginStorage for StorageAdapter {
    fn table_exists(&self, table: &str) -> bool {
        QueryStorage::table_exists(self, table)
    }

    fn begin_transaction(&self) -> anyhow::Result<u64> {
        QueryStorage::begin_transaction(self).map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn begin_read_only(&self) -> anyhow::Result<u64> {
        QueryStorage::begin_read_only(self).map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn commit(&self, tx_id: u64) -> anyhow::Result<()> {
        QueryStorage::commit(self, tx_id).map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn rollback(&self, tx_id: u64) -> anyhow::Result<()> {
        QueryStorage::rollback(self, tx_id).map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn scan(
        &self,
        tx_id: u64,
        table: &str,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> anyhow::Result<Vec<Value>> {
        let config = ScanConfig {
            columns: None,
            limit: limit.map(|l| l as usize),
            offset: offset.map(|o| o as usize),
        };

        let rows = QueryStorage::scan(self, tx_id, table, &config)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(rows.into_iter().map(|r| r.into_value()).collect())
    }

    fn get(&self, tx_id: u64, table: &str, key: &Value) -> anyhow::Result<Option<Value>> {
        let row =
            QueryStorage::read(self, tx_id, table, key).map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(row.map(|r| r.into_value()))
    }

    fn insert(&self, tx_id: u64, table: &str, row: Value) -> anyhow::Result<Option<Value>> {
        // Convert Value to Row
        let row_data = match row {
            Value::Row(m) => Row { columns: m },
            Value::Object(o) => Row {
                columns: o.into_iter().collect(),
            },
            _ => anyhow::bail!("Expected Row or Object value for insert"),
        };

        // Extract key before insert
        let key = Self::extract_key(&row_data).ok();

        QueryStorage::insert(self, tx_id, table, row_data).map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(key)
    }

    fn update(
        &self,
        tx_id: u64,
        table: &str,
        key: &Value,
        updates: Vec<(String, Value)>,
    ) -> anyhow::Result<bool> {
        let updates_map: HashMap<String, Value> = updates.into_iter().collect();
        QueryStorage::update(self, tx_id, table, key, updates_map)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn delete(&self, tx_id: u64, table: &str, key: &Value) -> anyhow::Result<bool> {
        QueryStorage::delete(self, tx_id, table, key).map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn list_tables(&self) -> anyhow::Result<Vec<(String, String)>> {
        Ok(self.list_tables())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engine::ast::{
        ColumnConstraint, ColumnDef, DataType, Ident, Span, Spanned, TableType,
    };
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn spanned_ident(name: &str) -> Spanned<Ident> {
        Spanned::new(Ident::new(name), Span::DUMMY)
    }

    #[test]
    fn test_unique_column_creates_index_and_describe() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            buffer_pool_size: 64,
            wal_enabled: false,
            ..Default::default()
        };

        let engine = Arc::new(StorageEngine::new(config).unwrap());
        let storage = StorageAdapter::new(Arc::clone(&engine));

        let columns = vec![
            ColumnDef {
                name: spanned_ident("id"),
                data_type: DataType::Int64,
                constraints: vec![ColumnConstraint::PrimaryKey],
            },
            ColumnDef {
                name: spanned_ident("email"),
                data_type: DataType::String,
                constraints: vec![ColumnConstraint::Unique],
            },
        ];

        storage
            .create_table_with_schema("default.users", TableType::Relational, &columns, &[])
            .unwrap();

        let indexes = engine.get_indexes("default.users");
        assert!(
            indexes
                .iter()
                .any(|idx| idx.unique && idx.columns == vec!["email".to_string()])
        );

        let description = storage.describe_table("default.users").unwrap();
        let columns_val = match description {
            Value::Object(obj) => obj.get("columns").cloned().unwrap(),
            _ => panic!("expected object description"),
        };
        let columns_list = match columns_val {
            Value::Array(cols) => cols,
            _ => panic!("expected columns array"),
        };
        let email_unique = columns_list
            .iter()
            .find_map(|col| {
                let Value::Object(col_obj) = col else {
                    return None;
                };
                let Some(Value::String(name)) = col_obj.get("name") else {
                    return None;
                };
                if name != "email" {
                    return None;
                }
                match col_obj.get("is_unique") {
                    Some(Value::Bool(val)) => Some(*val),
                    _ => None,
                }
            })
            .unwrap();

        assert!(email_unique);
    }
}

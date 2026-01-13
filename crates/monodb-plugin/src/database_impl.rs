//! Database interface implementation for plugins
//! 
//! TODO: Replace with full featured database impl.

use crate::bindings::monodb::plugin::database::Host as DatabaseHost;
use crate::bindings::monodb::plugin::schema::Host as SchemaHost;
use crate::bindings::monodb::plugin::transaction::Host as TransactionHost;
use crate::bindings::monodb::plugin::types::{DbError, ErrorKind, Value as WitValue};
use crate::state::State;
use crate::value_bridge::ValueBridge;
use anyhow::{Result, bail};
use monodb_common::Value as MonoValue;

// Database interface implementation
impl DatabaseHost for State {
    fn query(
        &mut self,
        statement: String,
        _params: Vec<WitValue>,
    ) -> Result<Vec<WitValue>, DbError> {
        // Extract table name from query (basic parser)
        let table = extract_table_name(&statement).map_err(to_db_error)?;

        self.permissions
            .check_table_access(&table)
            .map_err(to_db_error)?;

        // Get or create transaction
        let tx_id = self.get_or_create_tx().map_err(to_db_error)?;

        let limit = Some(self.permissions.max_query_rows as u32);
        let results = self.storage.scan(tx_id, &table, limit, None).map_err(to_db_error)?;

        results
            .iter()
            .map(ValueBridge::to_wit)
            .collect::<Result<Vec<_>>>()
            .map_err(to_db_error)
    }

    fn get(&mut self, table: String, key: WitValue) -> Result<Option<WitValue>, DbError> {
        self.permissions
            .check_table_access(&table)
            .map_err(to_db_error)?;

        let mono_key = ValueBridge::from_wit(&key).map_err(to_db_error)?;
        let tx_id = self.get_or_create_tx().map_err(to_db_error)?;

        self.storage
            .get(tx_id, &table, &mono_key)
            .map_err(to_db_error)?
            .map(|v| ValueBridge::to_wit(&v))
            .transpose()
            .map_err(to_db_error)
    }

    fn scan(
        &mut self,
        table: String,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<WitValue>, DbError> {
        self.permissions
            .check_table_access(&table)
            .map_err(to_db_error)?;

        let tx_id = self.get_or_create_tx().map_err(to_db_error)?;
        let limit = limit.or(Some(self.permissions.max_query_rows as u32));
        let results = self.storage.scan(tx_id, &table, limit, offset).map_err(to_db_error)?;

        results
            .iter()
            .map(ValueBridge::to_wit)
            .collect::<Result<Vec<_>>>()
            .map_err(to_db_error)
    }

    fn insert(&mut self, table: String, row: WitValue) -> Result<Option<WitValue>, DbError> {
        self.permissions.check_write_access().map_err(to_db_error)?;
        self.permissions
            .check_table_access(&table)
            .map_err(to_db_error)?;

        let mono_row = ValueBridge::from_wit(&row).map_err(to_db_error)?;
        let tx_id = self.get_or_create_tx().map_err(to_db_error)?;

        let result = self.storage
            .insert(tx_id, &table, mono_row)
            .map_err(to_db_error)?;

        result
            .map(|v| ValueBridge::to_wit(&v))
            .transpose()
            .map_err(to_db_error)
    }

    fn insert_batch(
        &mut self,
        table: String,
        rows: Vec<WitValue>,
    ) -> Result<Vec<Option<WitValue>>, DbError> {
        self.permissions.check_write_access().map_err(to_db_error)?;
        self.permissions
            .check_table_access(&table)
            .map_err(to_db_error)?;

        let mut ids = Vec::new();
        for row in rows {
            let id = self.insert(table.clone(), row)?;
            ids.push(id);
        }
        Ok(ids)
    }

    fn update(
        &mut self,
        table: String,
        key: WitValue,
        updates: Vec<(String, WitValue)>,
    ) -> Result<bool, DbError> {
        self.permissions.check_write_access().map_err(to_db_error)?;
        self.permissions
            .check_table_access(&table)
            .map_err(to_db_error)?;

        let mono_key = ValueBridge::from_wit(&key).map_err(to_db_error)?;
        let tx_id = self.get_or_create_tx().map_err(to_db_error)?;

        let mono_updates: Vec<(String, MonoValue)> = updates
            .into_iter()
            .map(|(k, v)| Ok((k, ValueBridge::from_wit(&v)?)))
            .collect::<Result<Vec<_>>>()
            .map_err(to_db_error)?;

        self.storage
            .update(tx_id, &table, &mono_key, mono_updates)
            .map_err(to_db_error)
    }

    fn delete(&mut self, table: String, key: WitValue) -> Result<bool, DbError> {
        self.permissions.check_write_access().map_err(to_db_error)?;
        self.permissions
            .check_table_access(&table)
            .map_err(to_db_error)?;

        let mono_key = ValueBridge::from_wit(&key).map_err(to_db_error)?;
        let tx_id = self.get_or_create_tx().map_err(to_db_error)?;

        self.storage
            .delete(tx_id, &table, &mono_key)
            .map_err(to_db_error)
    }
}

// Transaction interface implementation
impl TransactionHost for State {
    fn begin(
        &mut self,
        _isolation: crate::bindings::monodb::plugin::transaction::IsolationLevel,
        read_only: bool,
    ) -> Result<u64, DbError> {
        let tx_id = if read_only {
            self.storage.begin_read_only()
        } else {
            self.storage.begin_transaction()
        }.map_err(to_db_error)?;

        *self.current_tx.write() = Some(tx_id);
        Ok(tx_id)
    }

    fn commit(&mut self, tx_id: u64) -> Result<(), DbError> {
        let current = self.current_tx.read();
        if current.as_ref() != Some(&tx_id) {
            return Err(DbError {
                kind: ErrorKind::TransactionError,
                message: "Transaction not active".into(),
                details: Some(format!("Expected {:?}, got {}", *current, tx_id)),
            });
        }
        drop(current);

        self.storage.commit(tx_id).map_err(to_db_error)?;
        *self.current_tx.write() = None;
        Ok(())
    }

    fn rollback(&mut self, tx_id: u64) -> Result<(), DbError> {
        let current = self.current_tx.read();
        if current.as_ref() != Some(&tx_id) {
            return Err(DbError {
                kind: ErrorKind::TransactionError,
                message: "Transaction not active".into(),
                details: None,
            });
        }
        drop(current);

        self.storage.rollback(tx_id).map_err(to_db_error)?;
        *self.current_tx.write() = None;
        Ok(())
    }

    fn current(&mut self) -> Option<u64> {
        *self.current_tx.read()
    }
}

// Schema interface implementation
impl SchemaHost for State {
    fn list_tables(
        &mut self,
    ) -> Result<Vec<crate::bindings::monodb::plugin::schema::TableInfo>, DbError> {
        let tables = self.storage.list_tables().map_err(to_db_error)?;
        Ok(tables
            .into_iter()
            .map(|(name, storage_type)| crate::bindings::monodb::plugin::schema::TableInfo {
                name,
                storage_type,
                row_count: None,
                size_bytes: None,
            })
            .collect())
    }

    fn describe(
        &mut self,
        table: String,
    ) -> Result<Vec<crate::bindings::monodb::plugin::schema::ColumnInfo>, DbError> {
        if !self.storage.table_exists(&table) {
            return Err(DbError {
                kind: ErrorKind::NotFound,
                message: format!("Table {} not found", table),
                details: None,
            });
        }

        // Return empty schema for now
        Ok(vec![])
    }

    fn table_exists(&mut self, table: String) -> bool {
        self.storage.table_exists(&table)
    }
}

impl State {
    /// Get current transaction or create a new read-only one
    fn get_or_create_tx(&mut self) -> Result<u64> {
        if let Some(tx_id) = *self.current_tx.read() {
            return Ok(tx_id);
        }

        // Auto-create a read-only transaction for queries
        let tx_id = self.storage.begin_read_only()?;
        *self.current_tx.write() = Some(tx_id);
        Ok(tx_id)
    }
}

// Helper functions

fn to_db_error(err: anyhow::Error) -> DbError {
    DbError {
        kind: ErrorKind::InvalidOperation,
        message: err.to_string(),
        details: None,
    }
}

fn extract_table_name(query: &str) -> Result<String> {
    let query = query.trim().to_lowercase();
    if let Some(from_pos) = query.find("from ") {
        let table_part = &query[from_pos + 5..];
        let table = table_part.split_whitespace().next().unwrap_or("");
        Ok(table.to_string())
    } else {
        bail!("Could not extract table name from query")
    }
}

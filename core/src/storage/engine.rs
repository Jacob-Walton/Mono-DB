//! Main storage engine coordinating all components

use crate::error::{MonoError, MonoResult};
use crate::storage::{
    BufferPool, Column, DiskManager, IsolationLevel, PageId, PersistentCatalog, Table, TableId,
    TableSchema, Transaction, TransactionManager, TxnId, WalManager, WalRecord,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Main storage engine
pub struct StorageEngine {
    wal_manager: Arc<WalManager>,
    buffer_pool: Arc<BufferPool>,
    disk_manager: Arc<DiskManager>,
    transaction_manager: Arc<TransactionManager>,
    tables: RwLock<HashMap<String, Arc<RwLock<Table>>>>,
    catalog: RwLock<PersistentCatalog>,
    data_dir: PathBuf,
}

impl StorageEngine {
    /// Create a new storage engine
    pub fn new<P: AsRef<Path>>(data_dir: P, buffer_pool_size: usize) -> MonoResult<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let wal_path = data_dir.join("wal.log");
        let catalog_path = data_dir.join("catalog.json");

        let wal_manager = Arc::new(WalManager::new(wal_path)?);
        let disk_manager = Arc::new(DiskManager::new(&data_dir)?);
        let buffer_pool = Arc::new(BufferPool::new(buffer_pool_size, Arc::clone(&disk_manager)));
        let transaction_manager = Arc::new(TransactionManager::new(Arc::clone(&wal_manager)));

        // Load catalog from disk
        let catalog = PersistentCatalog::load(&catalog_path)?;

        Ok(Self {
            wal_manager,
            buffer_pool,
            disk_manager,
            transaction_manager,
            tables: RwLock::new(HashMap::new()),
            catalog: RwLock::new(catalog),
            data_dir,
        })
    }

    /// Initialize the storage engine
    pub fn initialize(&mut self) -> MonoResult<()> {
        self.recover()?;
        self.load_tables()?;
        Ok(())
    }

    /// Load existing tables from catalog
    fn load_tables(&self) -> MonoResult<()> {
        let catalog = self.catalog.read();
        let mut tables = self.tables.write();

        for (name, entry) in &catalog.tables {
            let mut table = Table::new(entry.schema.clone(), Arc::clone(&self.buffer_pool));

            // Try to find the first page for this table by scanning existing pages
            // This reconnects the table to its existing data on disk
            if let Ok(first_page_id) = self.find_first_table_page(name) {
                table.first_page_id = Some(first_page_id);
            }

            let table_arc = Arc::new(RwLock::new(table));
            tables.insert(name.clone(), table_arc);
        }

        Ok(())
    }

    /// Find the first page of a table by scanning the disk
    fn find_first_table_page(&self, table_name: &str) -> MonoResult<PageId> {
        // Try to read page 0 for this table - if it exists, the table has data
        match self.disk_manager.read_page(table_name, PageId(0)) {
            Ok(page) => {
                if page.header.tuple_count > 0 || page.header.next_page_id != 0 {
                    Ok(PageId(0))
                } else {
                    Err(MonoError::Storage("No data found for table".into()))
                }
            }
            Err(_) => Err(MonoError::Storage("Table data file not found".into())),
        }
    }

    /// Save catalog to disk
    fn save_catalog(&self) -> MonoResult<()> {
        let catalog = self.catalog.read();
        let catalog_path = self.data_dir.join("catalog.json");
        catalog.save(catalog_path)?;
        Ok(())
    }

    /// Perform recovery
    pub fn recover(&self) -> MonoResult<()> {
        let entries = self.wal_manager.recover()?;

        // Simple recovery: just mark which transactions committed
        let mut committed_txns = std::collections::HashSet::new();

        for entry in &entries {
            match &entry.record {
                WalRecord::Commit { txn_id } => {
                    committed_txns.insert(*txn_id);
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Create a table
    pub fn create_table(&self, name: String, columns: Vec<Column>) -> MonoResult<()> {
        let mut catalog = self.catalog.write();

        let table_id = TableId(catalog.next_table_id);
        catalog.next_table_id += 1;

        let schema = TableSchema {
            id: table_id,
            name: name.clone(),
            columns,
        };

        // Add to catalog
        catalog.add_table(schema.clone())?;

        // Save catalog immediately
        drop(catalog);
        self.save_catalog()?;

        // Create table instance and add to tables map
        let table = Arc::new(RwLock::new(Table::new(
            schema,
            Arc::clone(&self.buffer_pool),
        )));
        self.tables.write().insert(name, table);

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&self, name: &str) -> MonoResult<()> {
        self.tables.write().remove(name);

        let mut catalog = self.catalog.write();
        catalog.remove_table(name)?;
        drop(catalog);

        self.save_catalog()?;
        Ok(())
    }

    /// Begin a transaction
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> MonoResult<Arc<Transaction>> {
        self.transaction_manager.begin_transaction(isolation_level)
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, txn_id: TxnId) -> MonoResult<()> {
        self.transaction_manager.commit_transaction(txn_id)
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, txn_id: TxnId) -> MonoResult<()> {
        self.transaction_manager.abort_transaction(txn_id)
    }

    /// Insert a row
    pub fn insert(
        &self,
        table_name: &str,
        tuple_data: &[u8],
        txn_id: TxnId,
    ) -> MonoResult<(PageId, u16)> {
        // Acquire lock
        self.transaction_manager
            .acquire_lock(txn_id, table_name, 0, true)?;

        // Get table reference
        let table = {
            let tables = self.tables.read();
            tables
                .get(table_name)
                .cloned()
                .ok_or_else(|| MonoError::Storage(format!("Table '{}' not found", table_name)))?
        };

        let (page_id, tuple_id) = table.write().insert_tuple(tuple_data)?;

        // Log the operation
        let table_id = self
            .catalog
            .read()
            .get_table(table_name)
            .map(|s| s.id.0)
            .unwrap_or(0);

        self.wal_manager.write(WalRecord::Insert {
            txn_id,
            table_id,
            page_id: page_id.0,
            tuple_data: tuple_data.to_vec(),
        })?;

        // Track in transaction
        if let Some(txn) = self.transaction_manager.get_transaction(txn_id) {
            txn.add_to_write_set(table_name, page_id.0);
        }

        // Force flush for persistence and save catalog to ensure consistency
        self.buffer_pool.flush_all()?;
        self.save_catalog()?;

        Ok((page_id, tuple_id))
    }

    /// Read a row
    pub fn read(
        &self,
        table_name: &str,
        page_id: PageId,
        tuple_id: u16,
        txn_id: TxnId,
    ) -> MonoResult<Vec<u8>> {
        // Acquire lock
        self.transaction_manager
            .acquire_lock(txn_id, table_name, page_id.0, false)?;

        // Get table reference
        let table = {
            let tables = self.tables.read();
            tables
                .get(table_name)
                .cloned()
                .ok_or_else(|| MonoError::Storage(format!("Table '{}' not found", table_name)))?
        };

        let data = table.read().read_tuple(page_id, tuple_id)?;

        // Track in transaction
        if let Some(txn) = self.transaction_manager.get_transaction(txn_id) {
            txn.add_to_read_set(table_name, page_id.0);
        }

        Ok(data)
    }

    /// Scan a table
    pub fn scan_table<F>(&self, table_name: &str, txn_id: TxnId, callback: F) -> MonoResult<()>
    where
        F: FnMut(PageId, u16, &[u8]) -> MonoResult<bool>,
    {
        // Acquire lock
        self.transaction_manager
            .acquire_lock(txn_id, table_name, 0, false)?;

        // Get table reference
        let table = {
            let tables = self.tables.read();
            tables
                .get(table_name)
                .cloned()
                .ok_or_else(|| MonoError::Storage(format!("Table '{}' not found", table_name)))?
        };

        // Scan tuples
        let table_guard = table.read();
        table_guard.scan_tuples(callback)
    }

    /// Update a row
    pub fn update(
        &self,
        table_name: &str,
        page_id: PageId,
        tuple_id: u16,
        new_data: &[u8],
        txn_id: TxnId,
    ) -> MonoResult<()> {
        // Acquire lock
        self.transaction_manager
            .acquire_lock(txn_id, table_name, page_id.0, true)?;

        // Get old data for undo log
        let old_data = self.read(table_name, page_id, tuple_id, txn_id)?;

        // TODO: Modify in-place if possible
        let table_id = self
            .catalog
            .read()
            .get_table(table_name)
            .map(|s| s.id.0)
            .unwrap_or(0);

        self.wal_manager.write(WalRecord::Update {
            txn_id,
            table_id,
            page_id: page_id.0,
            old_data,
            new_data: new_data.to_vec(),
        })?;

        // Track in transaction
        if let Some(txn) = self.transaction_manager.get_transaction(txn_id) {
            txn.add_to_write_set(table_name, page_id.0);
        }

        Ok(())
    }

    /// Delete a row
    pub fn delete(
        &self,
        table_name: &str,
        page_id: PageId,
        tuple_id: u16,
        txn_id: TxnId,
    ) -> MonoResult<()> {
        // Acquire lock
        self.transaction_manager
            .acquire_lock(txn_id, table_name, page_id.0, true)?;

        // Get data for undo log
        let tuple_data = self.read(table_name, page_id, tuple_id, txn_id)?;

        // Log the delete
        let table_id = self
            .catalog
            .read()
            .get_table(table_name)
            .map(|s| s.id.0)
            .unwrap_or(0);

        self.wal_manager.write(WalRecord::Delete {
            txn_id,
            table_id,
            page_id: page_id.0,
            tuple_data,
        })?;

        // Track in transaction
        if let Some(txn) = self.transaction_manager.get_transaction(txn_id) {
            txn.add_to_write_set(table_name, page_id.0);
        }

        Ok(())
    }

    /// Checkpoint
    pub fn checkpoint(&self) -> MonoResult<()> {
        self.buffer_pool.flush_all()?;
        self.disk_manager.sync_all()?;
        self.wal_manager.checkpoint()?;
        self.save_catalog()?;
        Ok(())
    }

    /// Flush pending changes
    pub fn flush(&mut self) -> MonoResult<()> {
        self.buffer_pool.flush_all()?;
        self.wal_manager.flush()?;
        self.save_catalog()?;
        Ok(())
    }

    /// Get active transaction count
    pub fn active_transaction_count(&self) -> usize {
        self.transaction_manager.active_transaction_count()
    }

    /// Get table schema from catalog
    pub fn get_table_schema(&self, table_name: &str) -> MonoResult<Option<TableSchema>> {
        let catalog = self.catalog.read();
        Ok(catalog.get_table(table_name).cloned())
    }

    /// Check if table exists in catalog
    pub fn table_exists(&self, table_name: &str) -> bool {
        let catalog = self.catalog.read();
        catalog.get_table(table_name).is_some()
    }

    /// List all tables in catalog
    pub fn list_tables(&self) -> Vec<String> {
        let catalog = self.catalog.read();
        catalog.list_tables()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DataType;
    use tempfile::tempdir;

    #[test]
    fn test_storage_engine_basic() {
        let temp_dir = tempdir().unwrap();
        let mut engine = StorageEngine::new(temp_dir.path(), 10).unwrap();
        engine.initialize().unwrap();

        // Create table
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer).primary_key(),
            Column::new("name".to_string(), DataType::Varchar(100)),
        ];
        engine.create_table("users".to_string(), columns).unwrap();

        // Begin transaction
        let txn = engine
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Insert data
        let tuple_data = b"test data";
        let (page_id, tuple_id) = engine.insert("users", tuple_data, txn.id).unwrap();

        // Read data back
        let read_data = engine.read("users", page_id, tuple_id, txn.id).unwrap();
        assert_eq!(read_data, tuple_data);

        // Commit
        engine.commit_transaction(txn.id).unwrap();
    }
}

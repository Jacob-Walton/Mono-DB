#![allow(dead_code)]

//! Storage engine facade.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use monodb_common::{MonoError, Result};
use parking_lot::RwLock;

use super::buffer::LruBufferPool;
use super::disk::DiskManager;
use super::document::{DocumentStore, HistoryEntry};
use super::keyspace::Keyspace;
use super::mvcc::{MvccTable, Snapshot, TransactionManager};
use super::traits::{IsolationLevel, VersionedDocument};
use super::wal::{Wal, WalConfig};

// Storage Configuration

/// Configuration for the storage engine.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Data directory path.
    pub data_dir: PathBuf,
    /// Buffer pool size in pages.
    pub buffer_pool_size: usize,
    /// WAL configuration.
    pub wal_enabled: bool,
    /// Sync WAL on every commit.
    pub wal_sync_on_commit: bool,
    /// Default isolation level for transactions.
    pub default_isolation: IsolationLevel,
    /// Enable memory-mapped I/O.
    pub use_mmap: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            buffer_pool_size: 1000,
            wal_enabled: true,
            wal_sync_on_commit: true,
            default_isolation: IsolationLevel::ReadCommitted,
            use_mmap: true,
        }
    }
}

// Storage Type

/// Type of storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageType {
    /// MVCC-based relational storage.
    Relational,
    /// Versioned document storage.
    Document,
    /// Simple key-value keyspace.
    Keyspace,
}

// Transaction

/// Transaction handle.
pub struct Transaction {
    /// Transaction ID.
    pub id: u64,
    /// Snapshot for reads.
    pub snapshot: Snapshot,
    /// Whether the transaction is read-only.
    pub read_only: bool,
    /// WAL LSN at start.
    #[allow(dead_code)]
    start_lsn: u64,
}

impl Transaction {
    fn new(id: u64, snapshot: Snapshot, read_only: bool, start_lsn: u64) -> Self {
        Self {
            id,
            snapshot,
            read_only,
            start_lsn,
        }
    }
}

// Table Info

/// Information about a table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name.
    pub name: String,
    /// Storage type.
    pub storage_type: StorageType,
    /// File path.
    pub path: PathBuf,
}

// Type aliases for complex generic types
type MvccTableMap = DashMap<String, Arc<MvccTable<Vec<u8>, Vec<u8>>>>;
type DocumentStoreMap = DashMap<String, Arc<DocumentStore<Vec<u8>, Vec<u8>>>>;
type KeyspaceMap = DashMap<String, Arc<Keyspace<Vec<u8>, Vec<u8>>>>;

// Storage Engine
pub struct StorageEngine {
    /// Configuration.
    config: StorageConfig,
    /// Transaction manager (shared across all MVCC tables).
    tx_manager: Arc<TransactionManager>,
    /// Write-ahead log.
    wal: Option<Arc<Wal>>,
    /// Disk managers for each file.
    disk_managers: DashMap<PathBuf, Arc<DiskManager>>,
    /// Buffer pools for each file.
    buffer_pools: DashMap<PathBuf, Arc<LruBufferPool>>,
    /// MVCC tables (relational).
    mvcc_tables: MvccTableMap,
    /// Document stores.
    document_stores: DocumentStoreMap,
    /// Keyspaces.
    keyspaces: KeyspaceMap,
    /// Table metadata.
    tables: RwLock<HashMap<String, TableInfo>>,
    /// Active transactions.
    active_txs: DashMap<u64, Transaction>,
}

impl StorageEngine {
    /// Create a new storage engine.
    pub fn new(config: StorageConfig) -> Result<Self> {
        // Ensure data directory exists
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| MonoError::Io(format!("Failed to create data directory: {}", e)))?;

        // Initialize WAL if enabled
        let wal = if config.wal_enabled {
            let wal_config = WalConfig {
                path: config.data_dir.join("wal.log"),
                sync_on_commit: config.wal_sync_on_commit,
                ..Default::default()
            };
            Some(Wal::open(wal_config)?)
        } else {
            None
        };

        // Create transaction manager
        let tx_manager = Arc::new(TransactionManager::new());

        Ok(Self {
            config,
            tx_manager,
            wal,
            disk_managers: DashMap::new(),
            buffer_pools: DashMap::new(),
            mvcc_tables: DashMap::new(),
            document_stores: DashMap::new(),
            keyspaces: DashMap::new(),
            tables: RwLock::new(HashMap::new()),
            active_txs: DashMap::new(),
        })
    }

    /// Get or create a buffer pool for a path.
    fn get_or_create_pool(&self, path: &Path) -> Result<Arc<LruBufferPool>> {
        if let Some(pool) = self.buffer_pools.get(path) {
            return Ok(pool.clone());
        }

        // Create disk manager
        let disk = Arc::new(DiskManager::open(path)?);
        self.disk_managers.insert(path.to_path_buf(), disk.clone());

        // Create buffer pool
        let pool = Arc::new(LruBufferPool::new(disk, self.config.buffer_pool_size));
        self.buffer_pools.insert(path.to_path_buf(), pool.clone());

        Ok(pool)
    }
    // Transaction API

    /// Begin a new transaction with default isolation level.
    pub fn begin_transaction(&self) -> Result<u64> {
        self.begin_transaction_with_isolation(self.config.default_isolation)
    }

    /// Begin a transaction with specific isolation level.
    pub fn begin_transaction_with_isolation(&self, isolation: IsolationLevel) -> Result<u64> {
        // Create snapshot via transaction manager (it generates tx_id internally)
        let snapshot = self.tx_manager.begin(isolation, false)?;
        let tx_id = snapshot.tx_id;

        // Log to WAL
        let start_lsn = if let Some(wal) = &self.wal {
            wal.log_tx_begin(tx_id)?
        } else {
            0
        };

        let tx = Transaction::new(tx_id, snapshot, false, start_lsn);
        self.active_txs.insert(tx_id, tx);

        Ok(tx_id)
    }

    /// Begin a read-only transaction.
    pub fn begin_read_only(&self) -> Result<u64> {
        let snapshot = self
            .tx_manager
            .begin(IsolationLevel::RepeatableRead, true)?;
        let tx_id = snapshot.tx_id;

        let tx = Transaction::new(tx_id, snapshot, true, 0);
        self.active_txs.insert(tx_id, tx);

        Ok(tx_id)
    }

    /// Commit a transaction.
    pub fn commit(&self, tx_id: u64) -> Result<()> {
        let tx = self.active_txs.remove(&tx_id);
        if tx.is_none() {
            return Err(MonoError::Transaction(format!(
                "Transaction {} not found",
                tx_id
            )));
        }

        // Commit in transaction manager
        self.tx_manager.commit(tx_id)?;

        // Log to WAL
        if let Some(wal) = &self.wal {
            wal.log_tx_commit(tx_id)?;
        }

        Ok(())
    }

    /// Rollback a transaction.
    pub fn rollback(&self, tx_id: u64) -> Result<()> {
        let tx = self.active_txs.remove(&tx_id);
        if tx.is_none() {
            return Err(MonoError::Transaction(format!(
                "Transaction {} not found",
                tx_id
            )));
        }

        // Rollback in transaction manager
        self.tx_manager.rollback(tx_id)?;

        // Log to WAL
        if let Some(wal) = &self.wal {
            wal.log_tx_rollback(tx_id)?;
        }

        Ok(())
    }

    /// Get transaction snapshot for the given transaction ID.
    fn get_snapshot(&self, tx_id: u64) -> Result<Snapshot> {
        let tx = self
            .active_txs
            .get(&tx_id)
            .ok_or_else(|| MonoError::Transaction(format!("Transaction {} not found", tx_id)))?;

        Ok(tx.snapshot.clone())
    }

    /// Check if a transaction is read-only.
    fn is_read_only(&self, tx_id: u64) -> Result<bool> {
        let tx = self
            .active_txs
            .get(&tx_id)
            .ok_or_else(|| MonoError::Transaction(format!("Transaction {} not found", tx_id)))?;

        Ok(tx.read_only)
    }
    // Table Management

    /// Create a relational (MVCC) table.
    pub fn create_relational_table(&self, name: &str) -> Result<()> {
        if self.tables.read().contains_key(name) {
            return Err(MonoError::AlreadyExists(format!(
                "Table '{}' already exists",
                name
            )));
        }

        let path = self.config.data_dir.join(format!("{}.rel", name));
        let pool = self.get_or_create_pool(&path)?;

        let table = Arc::new(MvccTable::new(pool, self.tx_manager.clone())?);
        self.mvcc_tables.insert(name.to_string(), table);

        self.tables.write().insert(
            name.to_string(),
            TableInfo {
                name: name.to_string(),
                storage_type: StorageType::Relational,
                path,
            },
        );

        Ok(())
    }

    /// Create a document store.
    pub fn create_document_store(&self, name: &str, keep_history: bool) -> Result<()> {
        if self.tables.read().contains_key(name) {
            return Err(MonoError::AlreadyExists(format!(
                "Store '{}' already exists",
                name
            )));
        }

        let path = self.config.data_dir.join(format!("{}.doc", name));
        let pool = self.get_or_create_pool(&path)?;

        let store = Arc::new(DocumentStore::new(pool, keep_history)?);
        self.document_stores.insert(name.to_string(), store);

        self.tables.write().insert(
            name.to_string(),
            TableInfo {
                name: name.to_string(),
                storage_type: StorageType::Document,
                path,
            },
        );

        Ok(())
    }

    /// Create a memory-backed keyspace.
    pub fn create_memory_keyspace(&self, name: &str) -> Result<()> {
        if self.tables.read().contains_key(name) {
            return Err(MonoError::AlreadyExists(format!(
                "Keyspace '{}' already exists",
                name
            )));
        }

        let keyspace = Arc::new(Keyspace::memory());
        self.keyspaces.insert(name.to_string(), keyspace);

        self.tables.write().insert(
            name.to_string(),
            TableInfo {
                name: name.to_string(),
                storage_type: StorageType::Keyspace,
                path: PathBuf::new(),
            },
        );

        Ok(())
    }

    /// Create a disk-backed keyspace.
    pub fn create_disk_keyspace(&self, name: &str) -> Result<()> {
        if self.tables.read().contains_key(name) {
            return Err(MonoError::AlreadyExists(format!(
                "Keyspace '{}' already exists",
                name
            )));
        }

        let path = self.config.data_dir.join(format!("{}.ks", name));
        let pool = self.get_or_create_pool(&path)?;

        let keyspace = Arc::new(Keyspace::disk(pool)?);
        self.keyspaces.insert(name.to_string(), keyspace);

        self.tables.write().insert(
            name.to_string(),
            TableInfo {
                name: name.to_string(),
                storage_type: StorageType::Keyspace,
                path,
            },
        );

        Ok(())
    }

    /// Drop a table/store/keyspace.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let info = self
            .tables
            .write()
            .remove(name)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", name)))?;

        match info.storage_type {
            StorageType::Relational => {
                self.mvcc_tables.remove(name);
            }
            StorageType::Document => {
                self.document_stores.remove(name);
            }
            StorageType::Keyspace => {
                self.keyspaces.remove(name);
            }
        }

        // Clean up file
        if !info.path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&info.path);
            self.buffer_pools.remove(&info.path);
            self.disk_managers.remove(&info.path);
        }

        Ok(())
    }

    /// List all tables.
    pub fn list_tables(&self) -> Vec<TableInfo> {
        self.tables.read().values().cloned().collect()
    }

    /// List tables in a specific namespace.
    pub fn list_tables_in_namespace(&self, namespace: &str) -> Vec<TableInfo> {
        let prefix = format!("{}.", namespace);
        self.tables
            .read()
            .values()
            .filter(|info| info.name.starts_with(&prefix))
            .cloned()
            .collect()
    }

    /// Get table info by name.
    pub fn get_table_info(&self, name: &str) -> Option<TableInfo> {
        self.tables.read().get(name).cloned()
    }
    // Relational (MVCC) Operations

    /// Write a key-value pair in an MVCC table (insert or update).
    pub fn mvcc_write(&self, tx_id: u64, table: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.is_read_only(tx_id)? {
            return Err(MonoError::InvalidOperation(
                "Cannot write in read-only transaction".to_string(),
            ));
        }

        let snapshot = self.get_snapshot(tx_id)?;
        let mvcc = self
            .mvcc_tables
            .get(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        mvcc.write(&snapshot, key.clone(), value.clone())?;

        // Log to WAL
        if let Some(wal) = &self.wal {
            wal.log_insert(tx_id, table, &key, &value)?;
        }

        Ok(())
    }

    /// Read a value from an MVCC table within a transaction.
    pub fn mvcc_read(&self, tx_id: u64, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot = self.get_snapshot(tx_id)?;
        let mvcc = self
            .mvcc_tables
            .get(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        mvcc.read(&snapshot, &key.to_vec())
    }

    /// Delete a key from an MVCC table within a transaction.
    pub fn mvcc_delete(&self, tx_id: u64, table: &str, key: &[u8]) -> Result<bool> {
        if self.is_read_only(tx_id)? {
            return Err(MonoError::InvalidOperation(
                "Cannot delete in read-only transaction".to_string(),
            ));
        }

        let snapshot = self.get_snapshot(tx_id)?;
        let mvcc = self
            .mvcc_tables
            .get(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        let deleted = mvcc.delete(&snapshot, &key.to_vec())?;

        // Log to WAL
        if deleted && let Some(wal) = &self.wal {
            wal.log_delete::<Vec<u8>>(tx_id, table, &key.to_vec())?;
        }

        Ok(deleted)
    }

    /// Scan all visible records in an MVCC table.
    pub fn mvcc_scan(&self, tx_id: u64, table: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot = self.get_snapshot(tx_id)?;
        let mvcc = self
            .mvcc_tables
            .get(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        mvcc.scan(&snapshot)
    }

    /// Scan a range of keys in an MVCC table.
    pub fn mvcc_range_scan(
        &self,
        tx_id: u64,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot = self.get_snapshot(tx_id)?;
        let mvcc = self
            .mvcc_tables
            .get(table)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table)))?;

        mvcc.range_scan(&snapshot, &start.to_vec(), &end.to_vec())
    }
    // Document Operations

    /// Insert a document. Returns the revision number.
    pub fn doc_insert(&self, store: &str, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let doc_store = self
            .document_stores
            .get(store)
            .ok_or_else(|| MonoError::NotFound(format!("Document store '{}' not found", store)))?;

        doc_store.insert(key, value)
    }

    /// Get a document. Returns the value and revision if it exists and is not deleted.
    pub fn doc_get(&self, store: &str, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        let doc_store = self
            .document_stores
            .get(store)
            .ok_or_else(|| MonoError::NotFound(format!("Document store '{}' not found", store)))?;

        match doc_store.get(&key.to_vec())? {
            Some(doc) if !doc.deleted => Ok(Some((doc.data, doc.revision))),
            _ => Ok(None),
        }
    }

    /// Get the full versioned document metadata.
    pub fn doc_get_versioned(
        &self,
        store: &str,
        key: &[u8],
    ) -> Result<Option<VersionedDocument<Vec<u8>>>> {
        let doc_store = self
            .document_stores
            .get(store)
            .ok_or_else(|| MonoError::NotFound(format!("Document store '{}' not found", store)))?;

        doc_store.get(&key.to_vec())
    }

    /// Update a document with optimistic concurrency.
    /// The expected_revision must match the current revision.
    pub fn doc_update(
        &self,
        store: &str,
        key: &[u8],
        value: Vec<u8>,
        expected_revision: u64,
    ) -> Result<u64> {
        let doc_store = self
            .document_stores
            .get(store)
            .ok_or_else(|| MonoError::NotFound(format!("Document store '{}' not found", store)))?;

        doc_store.update(&key.to_vec(), value, expected_revision)
    }

    /// Delete a document with optimistic concurrency.
    pub fn doc_delete(&self, store: &str, key: &[u8], expected_revision: u64) -> Result<bool> {
        let doc_store = self
            .document_stores
            .get(store)
            .ok_or_else(|| MonoError::NotFound(format!("Document store '{}' not found", store)))?;

        doc_store.delete(&key.to_vec(), expected_revision)
    }

    /// Upsert a document (insert or update without revision check).
    pub fn doc_upsert(&self, store: &str, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let doc_store = self
            .document_stores
            .get(store)
            .ok_or_else(|| MonoError::NotFound(format!("Document store '{}' not found", store)))?;

        doc_store.upsert(key, value)
    }

    /// Get document history (most recent first, including current version).
    pub fn doc_history(
        &self,
        store: &str,
        key: &[u8],
        limit: usize,
    ) -> Result<Vec<HistoryEntry<Vec<u8>>>> {
        let doc_store = self
            .document_stores
            .get(store)
            .ok_or_else(|| MonoError::NotFound(format!("Document store '{}' not found", store)))?;

        let key_vec = key.to_vec();
        let mut history = Vec::new();

        // Include current document as the first (most recent) entry
        if let Some(current) = doc_store.get(&key_vec)?
            && !current.deleted
        {
            history.push(HistoryEntry {
                revision: current.revision,
                timestamp: 0, // Current document doesn't have a separate timestamp
                deleted: current.deleted,
                data: current.data,
            });
        }

        // Add past versions from history
        let past_history = doc_store.get_history(&key_vec, limit.saturating_sub(history.len()))?;
        history.extend(past_history);

        Ok(history)
    }

    /// Get document at a specific revision.
    pub fn doc_get_at_revision(
        &self,
        store: &str,
        key: &[u8],
        revision: u64,
    ) -> Result<Option<Vec<u8>>> {
        let doc_store = self
            .document_stores
            .get(store)
            .ok_or_else(|| MonoError::NotFound(format!("Document store '{}' not found", store)))?;

        doc_store.get_revision(&key.to_vec(), revision)
    }
    // Keyspace Operations

    /// Get a value from a keyspace.
    pub fn ks_get(&self, name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ks = self
            .keyspaces
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Keyspace '{}' not found", name)))?;

        ks.get(&key.to_vec())
    }

    /// Set a value in a keyspace.
    pub fn ks_set(&self, name: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let ks = self
            .keyspaces
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Keyspace '{}' not found", name)))?;

        ks.set(key, value)
    }

    /// Set a value with TTL (time-to-live in milliseconds).
    pub fn ks_set_with_ttl(
        &self,
        name: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
    ) -> Result<()> {
        let ks = self
            .keyspaces
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Keyspace '{}' not found", name)))?;

        ks.set_with_ttl(key, value, ttl_ms)
    }

    /// Delete a key from a keyspace.
    pub fn ks_delete(&self, name: &str, key: &[u8]) -> Result<bool> {
        let ks = self
            .keyspaces
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Keyspace '{}' not found", name)))?;

        ks.delete(&key.to_vec())
    }

    /// Check if a key exists in a keyspace.
    pub fn ks_exists(&self, name: &str, key: &[u8]) -> Result<bool> {
        let ks = self
            .keyspaces
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Keyspace '{}' not found", name)))?;

        ks.exists(&key.to_vec())
    }

    /// Get remaining TTL for a key (in milliseconds).
    pub fn ks_ttl(&self, name: &str, key: &[u8]) -> Result<Option<u64>> {
        let ks = self
            .keyspaces
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Keyspace '{}' not found", name)))?;

        ks.ttl(&key.to_vec())
    }

    /// Clean up expired keys in a keyspace.
    pub fn ks_cleanup(&self, name: &str) -> Result<usize> {
        let ks = self
            .keyspaces
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Keyspace '{}' not found", name)))?;

        ks.cleanup()
    }
    // Maintenance Operations

    /// Flush all buffers to disk.
    pub fn flush(&self) -> Result<()> {
        for pool in self.buffer_pools.iter() {
            pool.flush_all()?;
        }

        if let Some(wal) = &self.wal {
            wal.sync()?;
        }

        Ok(())
    }

    /// Run a checkpoint (flush + WAL marker).
    pub fn checkpoint(&self) -> Result<()> {
        // Flush all data
        self.flush()?;

        // Log checkpoint
        if let Some(wal) = &self.wal {
            wal.log_checkpoint()?;
            wal.sync()?;
        }

        Ok(())
    }

    /// Get storage engine statistics.
    pub fn stats(&self) -> StorageStats {
        let mut total_pages = 0;
        let mut total_hits = 0;
        let mut total_misses = 0;

        for pool in self.buffer_pools.iter() {
            let stats = pool.stats();
            total_hits += stats.hits;
            total_misses += stats.misses;
            total_pages += pool.len();
        }

        StorageStats {
            relational_tables: self.mvcc_tables.len(),
            document_stores: self.document_stores.len(),
            keyspaces: self.keyspaces.len(),
            buffer_pool_pages: total_pages,
            cache_hits: total_hits,
            cache_misses: total_misses,
            active_transactions: self.active_txs.len(),
        }
    }

    /// Shutdown the storage engine gracefully.
    pub fn shutdown(&self) -> Result<()> {
        // Rollback all active transactions
        let active_ids: Vec<u64> = self.active_txs.iter().map(|e| *e.key()).collect();
        for tx_id in active_ids {
            let _ = self.rollback(tx_id);
        }

        // Flush all data
        self.flush()?;

        // Shutdown WAL
        if let Some(wal) = &self.wal {
            wal.shutdown()?;
        }

        // Shutdown transaction manager (stops GC thread)
        self.tx_manager.shutdown();

        Ok(())
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    /// Get the current WAL LSN.
    pub fn current_lsn(&self) -> Option<u64> {
        self.wal.as_ref().map(|w| w.current_lsn())
    }

    /// Get transaction manager for advanced usage.
    pub fn tx_manager(&self) -> &Arc<TransactionManager> {
        &self.tx_manager
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

// Statistics

/// Storage engine statistics.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Number of relational (MVCC) tables.
    pub relational_tables: usize,
    /// Number of document stores.
    pub document_stores: usize,
    /// Number of keyspaces.
    pub keyspaces: usize,
    /// Total pages in buffer pools.
    pub buffer_pool_pages: usize,
    /// Buffer cache hits.
    pub cache_hits: u64,
    /// Buffer cache misses.
    pub cache_misses: u64,
    /// Number of active transactions.
    pub active_transactions: usize,
}

impl StorageStats {
    /// Calculate cache hit ratio.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config() -> (StorageConfig, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            buffer_pool_size: 100,
            wal_enabled: false, // Disable for simpler testing
            ..Default::default()
        };
        (config, dir)
    }

    #[test]
    fn test_engine_relational_basic() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        // Create table
        engine.create_relational_table("users").unwrap();

        // Begin transaction
        let tx = engine.begin_transaction().unwrap();

        // Write
        engine
            .mvcc_write(tx, "users", b"user:1".to_vec(), b"alice".to_vec())
            .unwrap();

        // Read
        let val = engine.mvcc_read(tx, "users", b"user:1").unwrap();
        assert_eq!(val, Some(b"alice".to_vec()));

        // Commit
        engine.commit(tx).unwrap();

        // Read in new transaction
        let tx2 = engine.begin_transaction().unwrap();
        let val = engine.mvcc_read(tx2, "users", b"user:1").unwrap();
        assert_eq!(val, Some(b"alice".to_vec()));
        engine.commit(tx2).unwrap();
    }

    #[test]
    fn test_engine_mvcc_isolation() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_relational_table("test").unwrap();

        // Transaction 1 writes
        let tx1 = engine.begin_transaction().unwrap();
        engine
            .mvcc_write(tx1, "test", b"key".to_vec(), b"value1".to_vec())
            .unwrap();

        // Transaction 2 shouldn't see uncommitted write
        let tx2 = engine.begin_transaction().unwrap();
        let val = engine.mvcc_read(tx2, "test", b"key").unwrap();
        assert!(val.is_none());

        // Commit tx1
        engine.commit(tx1).unwrap();

        // tx2 still shouldn't see it (snapshot isolation)
        let val = engine.mvcc_read(tx2, "test", b"key").unwrap();
        assert!(val.is_none());

        engine.commit(tx2).unwrap();

        // New transaction should see it
        let tx3 = engine.begin_transaction().unwrap();
        let val = engine.mvcc_read(tx3, "test", b"key").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
        engine.commit(tx3).unwrap();
    }

    #[test]
    fn test_engine_document_store() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_document_store("docs", true).unwrap();

        // Insert
        let rev = engine
            .doc_insert("docs", b"doc:1".to_vec(), b"content v1".to_vec())
            .unwrap();
        assert_eq!(rev, 1);

        // Get
        let (data, rev) = engine.doc_get("docs", b"doc:1").unwrap().unwrap();
        assert_eq!(data, b"content v1".to_vec());
        assert_eq!(rev, 1);

        // Update
        let rev2 = engine
            .doc_update("docs", b"doc:1", b"content v2".to_vec(), 1)
            .unwrap();
        assert_eq!(rev2, 2);

        // Get updated
        let (data, rev) = engine.doc_get("docs", b"doc:1").unwrap().unwrap();
        assert_eq!(data, b"content v2".to_vec());
        assert_eq!(rev, 2);

        // Optimistic concurrency, wrong revision should fail
        let result = engine.doc_update("docs", b"doc:1", b"content v3".to_vec(), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_engine_document_history() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_document_store("docs", true).unwrap();

        // Create several versions
        let rev1 = engine
            .doc_insert("docs", b"doc:1".to_vec(), b"v1".to_vec())
            .unwrap();
        let rev2 = engine
            .doc_update("docs", b"doc:1", b"v2".to_vec(), rev1)
            .unwrap();
        let _rev3 = engine
            .doc_update("docs", b"doc:1", b"v3".to_vec(), rev2)
            .unwrap();

        // Get history
        let history = engine.doc_history("docs", b"doc:1", 10).unwrap();
        assert_eq!(history.len(), 3);

        // Get specific revision
        let v1 = engine.doc_get_at_revision("docs", b"doc:1", 1).unwrap();
        assert_eq!(v1, Some(b"v1".to_vec()));
    }

    #[test]
    fn test_engine_keyspace() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_memory_keyspace("cache").unwrap();

        // Set
        engine
            .ks_set("cache", b"key".to_vec(), b"value".to_vec())
            .unwrap();

        // Get
        let val = engine.ks_get("cache", b"key").unwrap();
        assert_eq!(val, Some(b"value".to_vec()));

        // Exists
        assert!(engine.ks_exists("cache", b"key").unwrap());
        assert!(!engine.ks_exists("cache", b"nonexistent").unwrap());

        // Delete
        assert!(engine.ks_delete("cache", b"key").unwrap());
        assert!(engine.ks_get("cache", b"key").unwrap().is_none());
    }

    #[test]
    fn test_engine_stats() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_relational_table("t1").unwrap();
        engine.create_document_store("d1", false).unwrap();
        engine.create_memory_keyspace("k1").unwrap();

        let stats = engine.stats();
        assert_eq!(stats.relational_tables, 1);
        assert_eq!(stats.document_stores, 1);
        assert_eq!(stats.keyspaces, 1);
    }

    #[test]
    fn test_engine_read_only_transaction() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_relational_table("test").unwrap();

        // Write some data first
        let tx = engine.begin_transaction().unwrap();
        engine
            .mvcc_write(tx, "test", b"key".to_vec(), b"value".to_vec())
            .unwrap();
        engine.commit(tx).unwrap();

        // Read-only transaction
        let ro_tx = engine.begin_read_only().unwrap();
        let val = engine.mvcc_read(ro_tx, "test", b"key").unwrap();
        assert_eq!(val, Some(b"value".to_vec()));

        // Write should fail in read-only
        let result = engine.mvcc_write(ro_tx, "test", b"key2".to_vec(), b"value2".to_vec());
        assert!(result.is_err());

        engine.rollback(ro_tx).unwrap();
    }

    #[test]
    fn test_engine_drop_table() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_memory_keyspace("temp").unwrap();
        assert!(engine.get_table_info("temp").is_some());

        engine.drop_table("temp").unwrap();
        assert!(engine.get_table_info("temp").is_none());
    }

    #[test]
    fn test_engine_list_tables() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_relational_table("users").unwrap();
        engine.create_document_store("posts", false).unwrap();
        engine.create_memory_keyspace("sessions").unwrap();

        let tables = engine.list_tables();
        assert_eq!(tables.len(), 3);

        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"posts"));
        assert!(names.contains(&"sessions"));
    }

    #[test]
    fn test_engine_mvcc_scan() {
        let (config, _dir) = test_config();
        let engine = StorageEngine::new(config).unwrap();

        engine.create_relational_table("items").unwrap();

        let tx = engine.begin_transaction().unwrap();
        engine
            .mvcc_write(tx, "items", b"a".to_vec(), b"1".to_vec())
            .unwrap();
        engine
            .mvcc_write(tx, "items", b"b".to_vec(), b"2".to_vec())
            .unwrap();
        engine
            .mvcc_write(tx, "items", b"c".to_vec(), b"3".to_vec())
            .unwrap();
        engine.commit(tx).unwrap();

        let tx2 = engine.begin_transaction().unwrap();
        let results = engine.mvcc_scan(tx2, "items").unwrap();
        assert_eq!(results.len(), 3);
        engine.commit(tx2).unwrap();
    }
}

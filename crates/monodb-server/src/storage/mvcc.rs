//! Multi-Version Concurrency Control (MVCC) for relational tables.
//!
//! Implements snapshot isolation and serializable transactions using
//! multi-versioning. Each row can have multiple versions, each visible
//! to different transactions based on their start timestamp.

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded};
use dashmap::DashMap;
use monodb_common::{MonoError, Result};
use parking_lot::{Mutex, RwLock};

use super::btree::BTree;
use super::buffer::LruBufferPool;
use super::traits::{IsolationLevel, Serializable, Timestamp, TxId};

// MVCC Record
#[derive(Debug, Clone)]
pub struct MvccRecord<V> {
    /// Transaction that created this version.
    pub xmin: TxId,
    /// Transaction that deleted this version (0 if active).
    pub xmax: TxId,
    /// Commit timestamp of xmin (0 if not yet committed).
    pub cmin: Timestamp,
    /// Commit timestamp of xmax (0 if not yet committed or still active).
    pub cmax: Timestamp,
    /// The actual data.
    pub data: V,
}

impl<V> MvccRecord<V> {
    /// Create a new record created by a transaction.
    pub fn new(tx_id: TxId, data: V) -> Self {
        Self {
            xmin: tx_id,
            xmax: 0,
            cmin: 0,
            cmax: 0,
            data,
        }
    }

    /// Check if this record is visible to a transaction at the given snapshot.
    pub fn is_visible(&self, snapshot: &Snapshot, tx_manager: &TransactionManager) -> bool {
        // Rule 1: xmin must be committed before our snapshot
        if self.xmin == snapshot.tx_id {
            // Our own insert - visible if not deleted by us
            return self.xmax == 0 || self.xmax != snapshot.tx_id;
        }

        // xmin must be committed
        if !tx_manager.is_committed(self.xmin) {
            return false;
        }

        // xmin must have committed before our snapshot started
        if self.cmin > snapshot.start_ts {
            return false;
        }

        // xmin must not be in our active list
        if snapshot.active_txs.contains(&self.xmin) {
            return false;
        }

        // Rule 2: xmax must be either 0 or not visible to us
        if self.xmax == 0 {
            return true; // Not deleted
        }

        if self.xmax == snapshot.tx_id {
            return false; // Deleted by us
        }

        // xmax must be committed to make this invisible
        if !tx_manager.is_committed(self.xmax) {
            return true; // Deleter not committed yet
        }

        // xmax must have committed before our snapshot
        if self.cmax > snapshot.start_ts {
            return true; // Deleted after our snapshot
        }

        // xmax must not be in our active list
        if snapshot.active_txs.contains(&self.xmax) {
            return true; // Deleter was active when we started
        }

        false // Deleted and visible to us
    }

    /// Mark this record as deleted by a transaction.
    pub fn mark_deleted(&mut self, tx_id: TxId) {
        self.xmax = tx_id;
    }
}

impl<V: Serializable> Serializable for MvccRecord<V> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.xmin.serialize(buf);
        self.xmax.serialize(buf);
        self.cmin.serialize(buf);
        self.cmax.serialize(buf);
        self.data.serialize(buf);
    }

    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        let mut offset = 0;

        let (xmin, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (xmax, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (cmin, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (cmax, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (data, consumed) = V::deserialize(&buf[offset..])?;
        offset += consumed;

        Ok((
            Self {
                xmin,
                xmax,
                cmin,
                cmax,
                data,
            },
            offset,
        ))
    }

    fn serialized_size(&self) -> usize {
        8 + 8 + 8 + 8 + self.data.serialized_size()
    }
}

// Transaction Snapshot
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction ID that owns this snapshot.
    pub tx_id: TxId,
    /// Timestamp when this snapshot was created.
    pub start_ts: Timestamp,
    /// Set of transaction IDs that were active when this snapshot was created.
    pub active_txs: HashSet<TxId>,
    /// Isolation level.
    pub isolation: IsolationLevel,
}

impl Snapshot {
    /// Create a new snapshot for a transaction.
    fn new(
        tx_id: TxId,
        start_ts: Timestamp,
        active_txs: HashSet<TxId>,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            tx_id,
            start_ts,
            active_txs,
            isolation,
        }
    }
}

// Transaction State
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    /// Transaction is active.
    Active,
    /// Transaction has committed.
    Committed,
    /// Transaction has been rolled back.
    Aborted,
}

/// Internal transaction metadata.
#[derive(Debug, Clone)]
struct TxInfo {
    /// Transaction ID.
    id: TxId,
    /// Current state.
    state: TxState,
    /// Start timestamp.
    start_ts: Timestamp,
    /// Commit timestamp (0 if not committed).
    commit_ts: Timestamp,
    /// Isolation level.
    isolation: IsolationLevel,
    /// Read-only flag.
    read_only: bool,
    /// Write set: keys modified by this transaction.
    write_set: HashSet<Vec<u8>>,
}

// Transaction Manager
pub struct TransactionManager {
    /// Next transaction ID.
    next_tx_id: AtomicU64,
    /// Next timestamp (for ordering).
    next_timestamp: AtomicU64,
    /// Active transactions.
    active_txs: DashMap<TxId, TxInfo>,
    /// Recently committed transactions (for visibility checks).
    committed_txs: DashMap<TxId, Timestamp>,
    /// Minimum active transaction ID (for garbage collection).
    min_active_tx: AtomicU64,
    /// Garbage collection channel.
    gc_sender: Option<Sender<GcTask>>,
    /// GC thread handle.
    gc_handle: Mutex<Option<thread::JoinHandle<()>>>,
    /// Shutdown flag.
    shutdown_flag: AtomicBool,
    /// Written versioned keys per transaction (for finalize_commit).
    tx_written_keys: DashMap<TxId, Vec<Vec<u8>>>,
}

impl TransactionManager {
    /// Create a new transaction manager.
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            next_timestamp: AtomicU64::new(1),
            active_txs: DashMap::new(),
            committed_txs: DashMap::new(),
            min_active_tx: AtomicU64::new(u64::MAX),
            gc_sender: None,
            gc_handle: Mutex::new(None),
            shutdown_flag: AtomicBool::new(false),
            tx_written_keys: DashMap::new(),
        }
    }

    /// Create a transaction manager with background GC.
    pub fn with_gc(gc_interval: Duration) -> Arc<Self> {
        let (sender, receiver) = bounded(1000);

        let manager = Arc::new(Self {
            next_tx_id: AtomicU64::new(1),
            next_timestamp: AtomicU64::new(1),
            active_txs: DashMap::new(),
            committed_txs: DashMap::new(),
            min_active_tx: AtomicU64::new(u64::MAX),
            gc_sender: Some(sender),
            gc_handle: Mutex::new(None),
            shutdown_flag: AtomicBool::new(false),
            tx_written_keys: DashMap::new(),
        });

        // Start GC thread
        let manager_clone = manager.clone();
        let handle = thread::spawn(move || {
            gc_worker(manager_clone, receiver, gc_interval);
        });

        *manager.gc_handle.lock() = Some(handle);

        manager
    }

    /// Begin a new transaction.
    pub fn begin(&self, isolation: IsolationLevel, read_only: bool) -> Result<Snapshot> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let start_ts = self.next_timestamp.fetch_add(1, Ordering::SeqCst);

        // Collect active transaction IDs for snapshot
        let active_txs: HashSet<TxId> = self.active_txs.iter().map(|e| *e.key()).collect();

        // Create transaction info
        let tx_info = TxInfo {
            id: tx_id,
            state: TxState::Active,
            start_ts,
            commit_ts: 0,
            isolation,
            read_only,
            write_set: HashSet::new(),
        };

        self.active_txs.insert(tx_id, tx_info);
        self.update_min_active();

        Ok(Snapshot::new(tx_id, start_ts, active_txs, isolation))
    }

    /// Commit a transaction.
    pub fn commit(&self, tx_id: TxId) -> Result<Timestamp> {
        let mut tx_info = self
            .active_txs
            .get_mut(&tx_id)
            .ok_or_else(|| MonoError::Transaction(format!("transaction {} not found", tx_id)))?;

        if tx_info.state != TxState::Active {
            return Err(MonoError::Transaction(format!(
                "transaction {} is not active",
                tx_id
            )));
        }

        let commit_ts = self.next_timestamp.fetch_add(1, Ordering::SeqCst);
        tx_info.state = TxState::Committed;
        tx_info.commit_ts = commit_ts;

        drop(tx_info);

        // Move to committed set
        self.committed_txs.insert(tx_id, commit_ts);
        self.active_txs.remove(&tx_id);
        self.update_min_active();

        Ok(commit_ts)
    }

    /// Rollback a transaction.
    pub fn rollback(&self, tx_id: TxId) -> Result<()> {
        let mut tx_info = self
            .active_txs
            .get_mut(&tx_id)
            .ok_or_else(|| MonoError::Transaction(format!("transaction {} not found", tx_id)))?;

        if tx_info.state != TxState::Active {
            return Err(MonoError::Transaction(format!(
                "transaction {} is not active",
                tx_id
            )));
        }

        tx_info.state = TxState::Aborted;

        drop(tx_info);

        self.active_txs.remove(&tx_id);
        self.clear_written_keys(tx_id);
        self.update_min_active();

        Ok(())
    }

    /// Check if a transaction is committed.
    pub fn is_committed(&self, tx_id: TxId) -> bool {
        self.committed_txs.contains_key(&tx_id)
    }

    /// Get the commit timestamp of a transaction.
    pub fn commit_timestamp(&self, tx_id: TxId) -> Option<Timestamp> {
        self.committed_txs.get(&tx_id).map(|e| *e.value())
    }

    /// Record a write for conflict detection.
    pub fn record_write(&self, tx_id: TxId, key: Vec<u8>) -> Result<()> {
        if let Some(mut tx_info) = self.active_txs.get_mut(&tx_id) {
            tx_info.write_set.insert(key);
        }
        Ok(())
    }

    /// Check for write-write conflicts (for serializable isolation).
    pub fn check_conflicts(&self, tx_id: TxId, key: &[u8]) -> Result<()> {
        // Check if any other active transaction has written to this key
        for entry in self.active_txs.iter() {
            if *entry.key() != tx_id && entry.value().write_set.contains(key) {
                return Err(MonoError::WriteConflict(format!(
                    "key conflict with transaction {}",
                    entry.key()
                )));
            }
        }
        Ok(())
    }

    /// Update the minimum active transaction ID.
    fn update_min_active(&self) {
        let min = self
            .active_txs
            .iter()
            .map(|e| *e.key())
            .min()
            .unwrap_or(u64::MAX);
        self.min_active_tx.store(min, Ordering::Release);
    }

    /// Get the minimum active transaction ID (for GC).
    pub fn min_active(&self) -> TxId {
        self.min_active_tx.load(Ordering::Acquire)
    }

    /// Get the number of active transactions.
    pub fn active_count(&self) -> usize {
        self.active_txs.len()
    }

    /// Restore timestamp counter (for WAL replay).
    pub fn restore_timestamp(&self, ts: Timestamp) {
        self.next_timestamp.store(ts, Ordering::SeqCst);
        self.next_tx_id.store(ts, Ordering::SeqCst);
    }

    /// Signal the GC to check for garbage.
    pub fn signal_gc(&self) {
        if let Some(ref sender) = self.gc_sender {
            let _ = sender.try_send(GcTask::Collect);
        }
    }

    /// Shutdown the transaction manager.
    /// Stops the GC thread and marks the manager as shutting down.
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);

        // Signal GC thread to stop
        if let Some(ref sender) = self.gc_sender {
            let _ = sender.try_send(GcTask::Shutdown);
        }

        // Wait for GC thread (if possible without blocking forever)
        if let Some(handle) = self.gc_handle.lock().take() {
            let _ = handle.join();
        }
    }

    /// Check if the transaction manager is shutting down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::SeqCst)
    }

    /// Record a versioned key written by a transaction.
    /// Used by [`MvccTable`] to track keys for finalize_commit.
    pub fn record_versioned_key(&self, tx_id: TxId, versioned_key: Vec<u8>) {
        self.tx_written_keys
            .entry(tx_id)
            .or_default()
            .push(versioned_key);
    }

    /// Get and remove written keys for a transaction.
    pub fn take_written_keys(&self, tx_id: TxId) -> Vec<Vec<u8>> {
        self.tx_written_keys
            .remove(&tx_id)
            .map(|(_, v)| v)
            .unwrap_or_default()
    }

    /// Clear written keys for a transaction (on rollback).
    pub fn clear_written_keys(&self, tx_id: TxId) {
        self.tx_written_keys.remove(&tx_id);
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TransactionManager {
    fn drop(&mut self) {
        // Signal GC thread to stop
        if let Some(ref sender) = self.gc_sender {
            let _ = sender.send(GcTask::Shutdown);
        }

        // Wait for GC thread
        if let Some(handle) = self.gc_handle.lock().take() {
            let _ = handle.join();
        }
    }
}

// Garbage Collection
enum GcTask {
    Collect,
    Shutdown,
}

/// GC callback for cleaning up old versions.
pub type GcCallback = Box<dyn Fn(TxId) -> Result<()> + Send + Sync>;

/// Background garbage collection worker.
fn gc_worker(manager: Arc<TransactionManager>, receiver: Receiver<GcTask>, interval: Duration) {
    let mut last_run = Instant::now();

    loop {
        // Wait for signal or timeout
        match receiver.recv_timeout(interval) {
            Ok(GcTask::Shutdown) => break,
            Ok(GcTask::Collect) => {}
            Err(_) => {} // Timeout, run periodic GC
        }

        // Rate limit GC runs
        if last_run.elapsed() < interval / 2 {
            continue;
        }
        last_run = Instant::now();

        // Perform garbage collection
        let min_active = manager.min_active();

        // Remove old committed transactions from tracking
        let to_remove: Vec<TxId> = manager
            .committed_txs
            .iter()
            .filter(|e| *e.key() < min_active.saturating_sub(1000))
            .map(|e| *e.key())
            .collect();

        for tx_id in to_remove {
            manager.committed_txs.remove(&tx_id);
        }
    }
}

// MVCC Table
/// Stores multiple versions of each row, with visibility determined by
/// transaction snapshots.
pub struct MvccTable<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    /// The underlying B+Tree storing versioned records.
    /// Key: (original_key, version_ts)
    /// Value: [`MvccRecord<V>`]
    tree: BTree<Vec<u8>, MvccRecord<V>>,
    /// Transaction manager.
    tx_manager: Arc<TransactionManager>,
    /// Phantom data for K type parameter.
    _phantom: PhantomData<K>,
}

impl<K, V> MvccTable<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    /// Create a new MVCC table.
    pub fn new(pool: Arc<LruBufferPool>, tx_manager: Arc<TransactionManager>) -> Result<Self> {
        Ok(Self {
            tree: BTree::new(pool)?,
            tx_manager,
            _phantom: PhantomData,
        })
    }

    /// Create a versioned key from a key and version timestamp.
    fn versioned_key(key: &K, version_ts: Timestamp) -> Vec<u8> {
        let mut buf = Vec::new();
        key.serialize(&mut buf);
        // Append version in descending order (newer versions first)
        (u64::MAX - version_ts).serialize(&mut buf);
        buf
    }

    /// Extract the original key from a versioned key.
    fn extract_key(versioned: &[u8]) -> Result<K> {
        let (key, _) = K::deserialize(versioned)?;
        Ok(key)
    }

    /// Read a value as seen by a transaction.
    pub fn read(&self, snapshot: &Snapshot, key: &K) -> Result<Option<V>> {
        // Scan all versions of this key, find the visible one
        let _key_prefix = {
            let mut buf = Vec::new();
            key.serialize(&mut buf);
            buf
        };

        // Range scan for all versions of this key
        let start = Self::versioned_key(key, u64::MAX);
        let end = Self::versioned_key(key, 0);

        let versions = self.tree.range(start..=end)?;

        for (versioned_key, record) in versions {
            // Verify this is the right key
            let found_key = Self::extract_key(&versioned_key)?;
            if &found_key != key {
                continue;
            }

            // Check visibility
            if record.is_visible(snapshot, &self.tx_manager) {
                return Ok(Some(record.data));
            }
        }

        Ok(None)
    }

    /// Write a value in a transaction.
    pub fn write(&self, snapshot: &Snapshot, key: K, value: V) -> Result<()> {
        // Check for conflicts (for serializable)
        if snapshot.isolation == IsolationLevel::Serializable {
            let key_bytes = {
                let mut buf = Vec::new();
                key.serialize(&mut buf);
                buf
            };
            self.tx_manager
                .check_conflicts(snapshot.tx_id, &key_bytes)?;
            self.tx_manager.record_write(snapshot.tx_id, key_bytes)?;
        }

        // Mark old version as deleted (if exists)
        self.mark_deleted(snapshot, &key)?;

        // Insert new version
        let version_ts = snapshot.start_ts;
        let versioned_key = Self::versioned_key(&key, version_ts);
        let record = MvccRecord::new(snapshot.tx_id, value);

        // Record this versioned key for finalize_commit
        self.tx_manager
            .record_versioned_key(snapshot.tx_id, versioned_key.clone());

        self.tree.insert(versioned_key, record)?;

        Ok(())
    }

    /// Delete a value in a transaction.
    pub fn delete(&self, snapshot: &Snapshot, key: &K) -> Result<bool> {
        // Check for conflicts
        if snapshot.isolation == IsolationLevel::Serializable {
            let key_bytes = {
                let mut buf = Vec::new();
                key.serialize(&mut buf);
                buf
            };
            self.tx_manager
                .check_conflicts(snapshot.tx_id, &key_bytes)?;
            self.tx_manager.record_write(snapshot.tx_id, key_bytes)?;
        }

        self.mark_deleted(snapshot, key)
    }

    /// Mark the current visible version as deleted.
    fn mark_deleted(&self, snapshot: &Snapshot, key: &K) -> Result<bool> {
        // Find current visible version
        let start = Self::versioned_key(key, u64::MAX);
        let end = Self::versioned_key(key, 0);

        let versions = self.tree.range(start..=end)?;

        for (versioned_key, mut record) in versions {
            let found_key = Self::extract_key(&versioned_key)?;
            if &found_key != key {
                continue;
            }

            if record.is_visible(snapshot, &self.tx_manager) {
                // Mark as deleted
                record.mark_deleted(snapshot.tx_id);
                self.tree.insert(versioned_key, record)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Finalize a transaction by updating commit timestamps on all written records.
    ///
    /// Updates cmin (inserts) or cmax (deletes) with commit timestamp.
    pub fn finalize_commit(&self, tx_id: TxId, commit_ts: Timestamp) -> Result<()> {
        // Get all versioned keys written by this transaction
        let written_keys = self.tx_manager.take_written_keys(tx_id);

        for versioned_key in written_keys {
            // Read the record, update its commit timestamp, and write back
            if let Ok(Some(mut record)) = self.tree.get(&versioned_key) {
                // Update cmin if this transaction created the record
                if record.xmin == tx_id && record.cmin == 0 {
                    record.cmin = commit_ts;
                }
                // Update cmax if this transaction deleted the record
                if record.xmax == tx_id && record.cmax == 0 {
                    record.cmax = commit_ts;
                }
                // Write back the updated record
                self.tree.insert(versioned_key, record)?;
            }
        }

        Ok(())
    }

    /// Range scan for visible records between start and end keys.
    pub fn range_scan(&self, snapshot: &Snapshot, start: &K, end: &K) -> Result<Vec<(K, V)>> {
        let mut results = Vec::new();
        let mut seen_keys = HashSet::new();

        // Create versioned key bounds
        let start_versioned = Self::versioned_key(start, u64::MAX);
        let end_versioned = Self::versioned_key(end, 0);

        for (versioned_key, record) in self.tree.range(start_versioned..=end_versioned)? {
            let key = Self::extract_key(&versioned_key)?;

            // Check if key is in range
            if &key < start || &key > end {
                continue;
            }

            // Skip if we've already found a visible version for this key
            let key_bytes = {
                let mut buf = Vec::new();
                key.serialize(&mut buf);
                buf
            };

            if seen_keys.contains(&key_bytes) {
                continue;
            }

            if record.is_visible(snapshot, &self.tx_manager) {
                seen_keys.insert(key_bytes);
                results.push((key, record.data));
            }
        }

        Ok(results)
    }

    /// Scan all visible records.
    pub fn scan(&self, snapshot: &Snapshot) -> Result<Vec<(K, V)>> {
        let mut results = Vec::new();
        let mut seen_keys = HashSet::new();

        for (versioned_key, record) in self.tree.iter()? {
            let key = Self::extract_key(&versioned_key)?;

            // Skip if we've already found a visible version for this key
            let key_bytes = {
                let mut buf = Vec::new();
                key.serialize(&mut buf);
                buf
            };

            if seen_keys.contains(&key_bytes) {
                continue;
            }

            if record.is_visible(snapshot, &self.tx_manager) {
                seen_keys.insert(key_bytes);
                results.push((key, record.data));
            }
        }

        Ok(results)
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let tm = TransactionManager::new();

        let snapshot = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        assert_eq!(tm.active_count(), 1);

        let commit_ts = tm.commit(snapshot.tx_id).unwrap();
        assert!(commit_ts > snapshot.start_ts);
        assert_eq!(tm.active_count(), 0);
        assert!(tm.is_committed(snapshot.tx_id));
    }

    #[test]
    fn test_rollback() {
        let tm = TransactionManager::new();

        let snapshot = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        tm.rollback(snapshot.tx_id).unwrap();

        assert_eq!(tm.active_count(), 0);
        assert!(!tm.is_committed(snapshot.tx_id));
    }

    #[test]
    fn test_snapshot_isolation() {
        let tm = TransactionManager::new();

        // Start tx1
        let snapshot1 = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();

        // Start tx2 - tx1 should be in its active set
        let snapshot2 = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        assert!(snapshot2.active_txs.contains(&snapshot1.tx_id));

        // Commit tx1
        tm.commit(snapshot1.tx_id).unwrap();

        // Start tx3 - tx1 should NOT be in its active set
        let snapshot3 = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        assert!(!snapshot3.active_txs.contains(&snapshot1.tx_id));
        assert!(snapshot3.active_txs.contains(&snapshot2.tx_id));
    }

    #[test]
    fn test_record_visibility() {
        let tm = Arc::new(TransactionManager::new());

        // Create a record
        let snapshot1 = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        let record = MvccRecord::new(snapshot1.tx_id, "value".to_string());

        // Should be visible to its own transaction
        assert!(record.is_visible(&snapshot1, &tm));

        // Should NOT be visible to other transactions before commit
        let snapshot2 = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        assert!(!record.is_visible(&snapshot2, &tm));

        // Commit tx1
        let commit_ts = tm.commit(snapshot1.tx_id).unwrap();

        // Create record with commit timestamp
        let mut record = record;
        record.cmin = commit_ts;

        // Should be visible to new transactions after commit
        let snapshot3 = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        assert!(record.is_visible(&snapshot3, &tm));
    }
}

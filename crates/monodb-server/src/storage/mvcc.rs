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

use super::btree::ShardedBTree;
use super::page::PageId;
use super::traits::{IsolationLevel, Serializable, Timestamp, TxId};

use std::path::Path;

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
        // Check visibility of INSERT (xmin)
        let insert_visible = self.is_xmin_visible(snapshot, tx_manager);
        if !insert_visible {
            return false;
        }

        // Check visibility of DELETE (xmax)
        if self.xmax == 0 {
            return true; // Not deleted
        }

        // Check if deletion is visible
        !self.is_xmax_visible(snapshot, tx_manager)
    }

    /// Check if the creating transaction (xmin) makes this record visible.
    fn is_xmin_visible(&self, snapshot: &Snapshot, tx_manager: &TransactionManager) -> bool {
        // Our own write is always visible
        if self.xmin == snapshot.tx_id {
            return true;
        }

        // Look up the creating transaction
        if let Some(creator_tx) = tx_manager.get(self.xmin) {
            match creator_tx.status {
                TxState::Committed => {
                    // Visible if committed before our snapshot started
                    if let Some(commit_ts) = creator_tx.commit_ts {
                        commit_ts <= snapshot.start_ts
                    } else {
                        false // Shouldn't happen for committed tx
                    }
                }
                TxState::Active => {
                    // Only visible if it's our own transaction (checked above)
                    false
                }
                TxState::Aborted => false,
            }
        } else {
            // Transaction not in manager, assume it's an old committed transaction
            // whose entry was cleaned up. The xmin IS the commit_ts in this case.
            // For old data, cmin stores the commit timestamp.
            if self.cmin > 0 {
                self.cmin <= snapshot.start_ts
            } else {
                // No cmin recorded, assume visible if xmin < our start_ts
                self.xmin <= snapshot.start_ts
            }
        }
    }

    /// Check if the deleting transaction (xmax) makes this record invisible.
    fn is_xmax_visible(&self, snapshot: &Snapshot, tx_manager: &TransactionManager) -> bool {
        // Our own delete is visible to us
        if self.xmax == snapshot.tx_id {
            return true;
        }

        // Look up the deleting transaction
        if let Some(deleter_tx) = tx_manager.get(self.xmax) {
            match deleter_tx.status {
                TxState::Committed => {
                    // Deletion visible if committed before our snapshot
                    if let Some(commit_ts) = deleter_tx.commit_ts {
                        commit_ts <= snapshot.start_ts
                    } else {
                        false
                    }
                }
                TxState::Active => false,  // Active delete not visible
                TxState::Aborted => false, // Aborted delete not visible
            }
        } else {
            // Old committed deletion
            if self.cmax > 0 {
                self.cmax <= snapshot.start_ts
            } else {
                self.xmax <= snapshot.start_ts
            }
        }
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

// Transaction State (matching P2's TxStatus)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    /// Transaction is active.
    Active,
    /// Transaction has committed.
    Committed,
    /// Transaction has been rolled back.
    Aborted,
}

/// Transaction metadata (matching P2's Transaction struct).
#[derive(Debug, Clone)]
pub struct TxInfo {
    /// Transaction ID.
    pub tx_id: TxId,
    /// Start timestamp.
    pub start_ts: Timestamp,
    /// Commit timestamp (None if not committed).
    pub commit_ts: Option<Timestamp>,
    /// Current status.
    pub status: TxState,
    /// Isolation level.
    pub isolation: IsolationLevel,
    /// Read-only flag.
    pub read_only: bool,
    /// Write set: keys modified by this transaction.
    pub write_set: HashSet<Vec<u8>>,
}

impl TxInfo {
    fn new(tx_id: TxId, start_ts: Timestamp, isolation: IsolationLevel, read_only: bool) -> Self {
        Self {
            tx_id,
            start_ts,
            commit_ts: None,
            status: TxState::Active,
            isolation,
            read_only,
            write_set: HashSet::new(),
        }
    }
}

// Transaction Manager (simplified like P2)
pub struct TransactionManager {
    /// Monotonically increasing timestamp generator.
    next_ts: AtomicU64,
    /// All transactions (active, committed, and recently aborted).
    transactions: DashMap<TxId, TxInfo>,
    /// Oldest active transaction timestamp (for GC).
    oldest_active_ts: AtomicU64,
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
    /// Get initial timestamp based on wall clock time.
    /// This ensures new transactions have timestamps greater than any
    /// previously committed data, even across server restarts.
    fn initial_timestamp() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        // Use milliseconds since epoch as base timestamp
        // This gives us ~300 years of headroom and survives restarts
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1)
    }

    /// Create a new transaction manager.
    pub fn new() -> Self {
        Self {
            next_ts: AtomicU64::new(Self::initial_timestamp()),
            transactions: DashMap::new(),
            oldest_active_ts: AtomicU64::new(u64::MAX),
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
            next_ts: AtomicU64::new(Self::initial_timestamp()),
            transactions: DashMap::new(),
            oldest_active_ts: AtomicU64::new(u64::MAX),
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
        let ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        let tx_id = ts; // tx_id == start_ts for simplicity

        // Update oldest active timestamp
        self.oldest_active_ts.fetch_min(ts, Ordering::SeqCst);

        // Collect active transaction IDs for snapshot
        let active_txs: HashSet<TxId> = self
            .transactions
            .iter()
            .filter(|e| e.value().status == TxState::Active)
            .map(|e| *e.key())
            .collect();

        // Create transaction info and store it
        let tx_info = TxInfo::new(tx_id, ts, isolation, read_only);
        self.transactions.insert(tx_id, tx_info);

        Ok(Snapshot::new(tx_id, ts, active_txs, isolation))
    }

    /// Commit a transaction.
    pub fn commit(&self, tx_id: TxId) -> Result<Timestamp> {
        let commit_ts = self.next_ts.fetch_add(1, Ordering::SeqCst);

        if let Some(mut tx) = self.transactions.get_mut(&tx_id) {
            if tx.status != TxState::Active {
                return Err(MonoError::Transaction(format!(
                    "Transaction {} is not active (status: {:?})",
                    tx_id, tx.status
                )));
            }
            tx.commit_ts = Some(commit_ts);
            tx.status = TxState::Committed;
        } else {
            return Err(MonoError::Transaction(format!(
                "Transaction {} not found",
                tx_id
            )));
        }

        self.update_oldest_active();
        Ok(commit_ts)
    }

    /// Rollback a transaction.
    pub fn rollback(&self, tx_id: TxId) -> Result<()> {
        if let Some(mut tx) = self.transactions.get_mut(&tx_id) {
            if tx.status != TxState::Active {
                return Err(MonoError::Transaction(format!(
                    "Transaction {} is not active (status: {:?})",
                    tx_id, tx.status
                )));
            }
            tx.status = TxState::Aborted;
        } else {
            return Err(MonoError::Transaction(format!(
                "Transaction {} not found",
                tx_id
            )));
        }

        self.clear_written_keys(tx_id);
        self.update_oldest_active();
        Ok(())
    }

    /// Get a transaction by ID.
    pub fn get(&self, tx_id: TxId) -> Option<TxInfo> {
        self.transactions.get(&tx_id).map(|tx| tx.clone())
    }

    /// Check if a transaction is committed.
    pub fn is_committed(&self, tx_id: TxId) -> bool {
        self.transactions
            .get(&tx_id)
            .map(|tx| tx.status == TxState::Committed)
            .unwrap_or(false)
    }

    /// Check if a transaction is aborted.
    pub fn is_aborted(&self, tx_id: TxId) -> bool {
        self.transactions
            .get(&tx_id)
            .map(|tx| tx.status == TxState::Aborted)
            .unwrap_or(false)
    }

    /// Check if a transaction is still active.
    pub fn is_active(&self, tx_id: TxId) -> bool {
        self.transactions
            .get(&tx_id)
            .map(|tx| tx.status == TxState::Active)
            .unwrap_or(false)
    }

    /// Get the commit timestamp of a transaction.
    pub fn commit_timestamp(&self, tx_id: TxId) -> Option<Timestamp> {
        self.transactions.get(&tx_id).and_then(|tx| tx.commit_ts)
    }

    /// Record a write for conflict detection.
    pub fn record_write(&self, tx_id: TxId, key: Vec<u8>) -> Result<()> {
        if let Some(mut tx) = self.transactions.get_mut(&tx_id) {
            tx.write_set.insert(key);
        }
        Ok(())
    }

    /// Check for write-write conflicts (for serializable isolation).
    pub fn check_conflicts(&self, tx_id: TxId, key: &[u8]) -> Result<()> {
        for entry in self.transactions.iter() {
            if *entry.key() != tx_id
                && entry.value().status == TxState::Active
                && entry.value().write_set.contains(key)
            {
                return Err(MonoError::WriteConflict(format!(
                    "key conflict with transaction {}",
                    entry.key()
                )));
            }
        }
        Ok(())
    }

    /// Recalculate the oldest active transaction timestamp.
    fn update_oldest_active(&self) {
        let mut oldest = u64::MAX;
        for entry in self.transactions.iter() {
            if entry.status == TxState::Active && entry.start_ts < oldest {
                oldest = entry.start_ts;
            }
        }
        self.oldest_active_ts.store(oldest, Ordering::SeqCst);
    }

    /// Get the oldest active transaction timestamp (for GC).
    pub fn oldest_active(&self) -> Timestamp {
        self.oldest_active_ts.load(Ordering::SeqCst)
    }

    /// Get the minimum active transaction ID (for GC), alias for oldest_active.
    pub fn min_active(&self) -> TxId {
        self.oldest_active()
    }

    /// Get the number of active transactions.
    pub fn active_count(&self) -> usize {
        self.transactions
            .iter()
            .filter(|e| e.status == TxState::Active)
            .count()
    }

    /// Restore timestamp counter (for WAL replay).
    /// Sets the counter to at least the given value.
    pub fn restore_timestamp(&self, min_ts: Timestamp) {
        self.next_ts.fetch_max(min_ts, Ordering::SeqCst);
    }

    /// Signal the GC to check for garbage.
    pub fn signal_gc(&self) {
        if let Some(ref sender) = self.gc_sender {
            let _ = sender.try_send(GcTask::Collect);
        }
    }

    /// Shutdown the transaction manager.
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);

        if let Some(ref sender) = self.gc_sender {
            let _ = sender.try_send(GcTask::Shutdown);
        }

        if let Some(handle) = self.gc_handle.lock().take() {
            let _ = handle.join();
        }
    }

    /// Check if the transaction manager is shutting down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::SeqCst)
    }

    /// Record a versioned key written by a transaction.
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

    /// Clean up old committed/aborted transactions (call periodically).
    pub fn cleanup(&self, older_than: Timestamp) {
        self.transactions
            .retain(|_, tx| tx.status == TxState::Active || tx.start_ts >= older_than);
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
        // Keep transactions newer than oldest_active - 1000
        let oldest = manager.oldest_active();
        if oldest != u64::MAX {
            manager.cleanup(oldest.saturating_sub(1000));
        }
    }
}

/// MVCC Table
/// Stores multiple versions of each row, with visibility determined by
/// transaction snapshots.
///
/// Uses a [`ShardedBTree`] for high-concurrency write performance.
pub struct MvccTable<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    /// The underlying sharded B+Tree storing versioned records.
    /// Key: (original_key, version_ts)
    /// Value: [`MvccRecord<V>`]
    tree: ShardedBTree<Vec<u8>, MvccRecord<V>>,
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
    /// Create a new MVCC table with sharded storage.
    ///
    /// Creates NUM_SHARDS separate B+Tree files at `base_path.shard0`, etc.
    pub fn new(
        base_path: impl AsRef<Path>,
        pool_size: usize,
        tx_manager: Arc<TransactionManager>,
    ) -> Result<Self> {
        Ok(Self {
            tree: ShardedBTree::new(base_path, pool_size)?,
            tx_manager,
            _phantom: PhantomData,
        })
    }

    /// Open an existing MVCC table from disk.
    pub fn open(
        base_path: impl AsRef<Path>,
        pool_size: usize,
        tx_manager: Arc<TransactionManager>,
    ) -> Result<Self> {
        Ok(Self {
            tree: ShardedBTree::open(base_path, pool_size)?,
            tx_manager,
            _phantom: PhantomData,
        })
    }

    /// Get the first shard's metadata page ID (for backwards compatibility).
    pub fn meta_page_id(&self) -> PageId {
        self.tree.meta_page_id()
    }

    /// Get the approximate number of entries in the table.
    /// Note: This counts all versions, not just visible ones.
    pub fn len(&self) -> usize {
        self.tree.len()
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

    /// Finalize a rollback by removing all records written by the aborted transaction.
    ///
    /// Physically deletes records created by this transaction from the B-tree.
    pub fn finalize_rollback(&self, tx_id: TxId) -> Result<()> {
        // Get all versioned keys written by this transaction
        let written_keys = self.tx_manager.take_written_keys(tx_id);

        for versioned_key in written_keys {
            // Check if this record was created by the aborted transaction
            if let Ok(Some(record)) = self.tree.get(&versioned_key) {
                if record.xmin == tx_id && record.cmin == 0 {
                    // This record was created by the aborted transaction, delete it
                    self.tree.delete(&versioned_key)?;
                } else if record.xmax == tx_id {
                    // This record was deleted by the aborted transaction, undelete it
                    let mut record = record;
                    record.xmax = 0;
                    record.cmax = 0;
                    self.tree.insert(versioned_key, record)?;
                }
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

    /// Flush all data to disk.
    pub fn flush(&self) -> Result<()> {
        self.tree.flush()
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

    #[test]
    fn test_rollback_visibility() {
        let tm = Arc::new(TransactionManager::new());

        // Start tx1 and create a record
        let snapshot1 = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        let record = MvccRecord::new(snapshot1.tx_id, "value".to_string());

        // Record is visible to its own transaction
        assert!(record.is_visible(&snapshot1, &tm));

        // Rollback tx1
        tm.rollback(snapshot1.tx_id).unwrap();

        // Record should NOT be visible to any transaction after rollback
        let snapshot2 = tm.begin(IsolationLevel::ReadCommitted, false).unwrap();
        assert!(
            !record.is_visible(&snapshot2, &tm),
            "Record from rolled-back tx should not be visible!"
        );

        // Also verify the transaction status
        assert!(tm.is_aborted(snapshot1.tx_id));
        assert!(!tm.is_committed(snapshot1.tx_id));
        assert!(!tm.is_active(snapshot1.tx_id));
    }
}

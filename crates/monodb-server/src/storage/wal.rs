//! Write-Ahead Logging (WAL) for durability.
//!
//! Provides crash recovery through sequential logging of all mutations.
//! Supports group commit for better throughput and MVCC-aware replay.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded};
use monodb_common::{MonoError, Result};
use parking_lot::{Mutex, RwLock};

use super::traits::Serializable;

// WAL format constants

/// WAL file magic number: "MWAL"
const WAL_MAGIC: u32 = u32::from_be_bytes(*b"MWAL");

/// WAL version.
const WAL_VERSION: u8 = 1;

/// Default sync interval in milliseconds.
const DEFAULT_SYNC_INTERVAL_MS: u64 = 100;

/// Default bytes before sync.
const DEFAULT_SYNC_EVERY_BYTES: u64 = 64 * 1024; // 64KB

/// Maximum entry size.
const MAX_ENTRY_SIZE: usize = 16 * 1024 * 1024; // 16MB

// WAL entry types

/// Type of WAL entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalEntryType {
    /// Invalid/padding entry.
    Invalid = 0,
    /// Insert operation.
    Insert = 1,
    /// Update operation.
    Update = 2,
    /// Delete operation.
    Delete = 3,
    /// Transaction begin.
    TxBegin = 4,
    /// Transaction commit.
    TxCommit = 5,
    /// Transaction rollback.
    TxRollback = 6,
    /// Checkpoint marker.
    Checkpoint = 7,
}

impl From<u8> for WalEntryType {
    fn from(value: u8) -> Self {
        match value {
            1 => WalEntryType::Insert,
            2 => WalEntryType::Update,
            3 => WalEntryType::Delete,
            4 => WalEntryType::TxBegin,
            5 => WalEntryType::TxCommit,
            6 => WalEntryType::TxRollback,
            7 => WalEntryType::Checkpoint,
            _ => WalEntryType::Invalid,
        }
    }
}

// WAL entry header and payload

/// A WAL entry header (25 bytes).
#[derive(Debug, Clone)]
pub struct WalEntryHeader {
    /// Log sequence number.
    pub lsn: u64,
    /// Transaction ID.
    pub tx_id: u64,
    /// Entry type.
    pub entry_type: WalEntryType,
    /// Payload length.
    pub length: u32,
    /// CRC32 checksum of header + payload.
    pub checksum: u32,
}

impl WalEntryHeader {
    /// Header size in bytes.
    pub const SIZE: usize = 25;

    /// Serialize header to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.lsn.to_le_bytes());
        buf[8..16].copy_from_slice(&self.tx_id.to_le_bytes());
        buf[16] = self.entry_type as u8;
        buf[17..21].copy_from_slice(&self.length.to_le_bytes());
        buf[21..25].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize header from bytes.
    pub fn from_bytes(buf: &[u8; Self::SIZE]) -> Self {
        Self {
            lsn: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            tx_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            entry_type: WalEntryType::from(buf[16]),
            length: u32::from_le_bytes(buf[17..21].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[21..25].try_into().unwrap()),
        }
    }
}

/// A complete WAL entry.
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Entry header.
    pub header: WalEntryHeader,
    /// Entry payload.
    pub payload: Vec<u8>,
}

impl WalEntry {
    /// Create a new entry.
    pub fn new(lsn: u64, tx_id: u64, entry_type: WalEntryType, payload: Vec<u8>) -> Self {
        let mut header = WalEntryHeader {
            lsn,
            tx_id,
            entry_type,
            length: payload.len() as u32,
            checksum: 0,
        };

        // Calculate checksum
        let checksum = Self::compute_checksum(&header, &payload);
        header.checksum = checksum;

        Self { header, payload }
    }

    /// Compute CRC32 checksum.
    fn compute_checksum(header: &WalEntryHeader, payload: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header.lsn.to_le_bytes());
        hasher.update(&header.tx_id.to_le_bytes());
        hasher.update(&[header.entry_type as u8]);
        hasher.update(&header.length.to_le_bytes());
        hasher.update(payload);
        hasher.finalize()
    }

    /// Verify checksum.
    pub fn verify(&self) -> bool {
        let expected = Self::compute_checksum(&self.header, &self.payload);
        expected == self.header.checksum
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(WalEntryHeader::SIZE + self.payload.len());
        buf.extend_from_slice(&self.header.to_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }
}

// WAL Configuration

/// WAL configuration.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Path to WAL file.
    pub path: PathBuf,
    /// Bytes to write before syncing.
    pub sync_every_bytes: u64,
    /// Maximum time between syncs.
    pub sync_interval_ms: u64,
    /// Whether to sync on commit.
    pub sync_on_commit: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data.wal"),
            sync_every_bytes: DEFAULT_SYNC_EVERY_BYTES,
            sync_interval_ms: DEFAULT_SYNC_INTERVAL_MS,
            sync_on_commit: true,
        }
    }
}

// WAL Writer Command

enum WriterCommand {
    /// Write an entry.
    Write(WalEntry, Option<Sender<Result<u64>>>),
    /// Force sync to disk.
    Sync(Sender<Result<()>>),
    /// Shutdown writer.
    Shutdown,
}

// WAL Writer

/// Asynchronous WAL writer thread.
struct WalWriter {
    file: BufWriter<File>,
    pending_bytes: u64,
    last_sync: Instant,
    config: WalConfig,
}

impl WalWriter {
    fn new(file: File, config: WalConfig) -> Self {
        Self {
            file: BufWriter::with_capacity(64 * 1024, file),
            pending_bytes: 0,
            last_sync: Instant::now(),
            config,
        }
    }

    fn write(&mut self, entry: &WalEntry) -> Result<u64> {
        let bytes = entry.to_bytes();
        self.file
            .write_all(&bytes)
            .map_err(|e| MonoError::Wal(format!("Write failed: {}", e)))?;

        self.pending_bytes += bytes.len() as u64;
        Ok(entry.header.lsn)
    }

    fn maybe_sync(&mut self) -> Result<()> {
        let should_sync = self.pending_bytes >= self.config.sync_every_bytes
            || self.last_sync.elapsed() >= Duration::from_millis(self.config.sync_interval_ms);

        if should_sync {
            self.sync()?;
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file
            .flush()
            .map_err(|e| MonoError::Wal(format!("Flush failed: {}", e)))?;

        self.file
            .get_ref()
            .sync_data()
            .map_err(|e| MonoError::Wal(format!("Sync failed: {}", e)))?;

        self.pending_bytes = 0;
        self.last_sync = Instant::now();
        Ok(())
    }
}

// WAL Handle

/// Write-ahead log handle for durability.
pub struct Wal {
    /// Channel to writer thread.
    sender: Sender<WriterCommand>,
    /// Writer thread handle.
    writer_handle: Mutex<Option<JoinHandle<()>>>,
    /// Current LSN counter.
    next_lsn: AtomicU64,
    /// Shutdown flag.
    shutdown: AtomicBool,
    /// WAL path.
    path: PathBuf,
    /// Configuration.
    config: WalConfig,
    /// Committed transaction IDs (for replay filtering).
    committed_txs: RwLock<std::collections::HashSet<u64>>,
}

impl Wal {
    /// Open or create a WAL.
    pub fn open(config: WalConfig) -> Result<Arc<Self>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.path)
            .map_err(|e| MonoError::Wal(format!("Failed to open WAL: {}", e)))?;

        // Determine next LSN from file
        let next_lsn = Self::find_next_lsn(&config.path)?;

        let (sender, receiver) = bounded::<WriterCommand>(10_000);

        let wal = Arc::new(Self {
            sender,
            writer_handle: Mutex::new(None),
            next_lsn: AtomicU64::new(next_lsn),
            shutdown: AtomicBool::new(false),
            path: config.path.clone(),
            config: config.clone(),
            committed_txs: RwLock::new(std::collections::HashSet::new()),
        });

        // Start writer thread
        let mut writer = WalWriter::new(file, config);
        let handle = thread::spawn(move || {
            Self::writer_loop(&mut writer, receiver);
        });

        *wal.writer_handle.lock() = Some(handle);

        Ok(wal)
    }

    /// Find the next LSN by scanning the WAL.
    fn find_next_lsn(path: &Path) -> Result<u64> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(1), // New file starts at LSN 1
        };

        let metadata = file
            .metadata()
            .map_err(|e| MonoError::Wal(format!("Failed to read metadata: {}", e)))?;

        if metadata.len() == 0 {
            return Ok(1);
        }

        // Scan to find highest LSN
        let mut reader = BufReader::new(file);
        let mut max_lsn = 0u64;
        let mut header_buf = [0u8; WalEntryHeader::SIZE];

        loop {
            match reader.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(MonoError::Wal(format!("Read error: {}", e))),
            }

            let header = WalEntryHeader::from_bytes(&header_buf);

            if header.entry_type == WalEntryType::Invalid {
                break;
            }

            max_lsn = max_lsn.max(header.lsn);

            // Skip payload
            if header.length > 0 {
                reader
                    .seek(SeekFrom::Current(header.length as i64))
                    .map_err(|e| MonoError::Wal(format!("Seek error: {}", e)))?;
            }
        }

        Ok(max_lsn + 1)
    }

    /// Writer loop.
    fn writer_loop(writer: &mut WalWriter, receiver: Receiver<WriterCommand>) {
        loop {
            match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(WriterCommand::Write(entry, reply)) => {
                    let result = writer.write(&entry).and_then(|lsn| {
                        writer.maybe_sync()?;
                        Ok(lsn)
                    });
                    if let Some(tx) = reply {
                        let _ = tx.send(result);
                    }
                }
                Ok(WriterCommand::Sync(reply)) => {
                    let result = writer.sync();
                    let _ = reply.send(result);
                }
                Ok(WriterCommand::Shutdown) => {
                    let _ = writer.sync();
                    break;
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Periodic sync
                    let _ = writer.maybe_sync();
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }

    /// Get next LSN and increment.
    fn alloc_lsn(&self) -> u64 {
        self.next_lsn.fetch_add(1, Ordering::SeqCst)
    }

    /// Append an entry to the WAL.
    pub fn append(&self, tx_id: u64, entry_type: WalEntryType, payload: Vec<u8>) -> Result<u64> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(MonoError::Wal("WAL is shutdown".to_string()));
        }

        if payload.len() > MAX_ENTRY_SIZE {
            return Err(MonoError::Wal(format!(
                "Entry too large: {} > {}",
                payload.len(),
                MAX_ENTRY_SIZE
            )));
        }

        let lsn = self.alloc_lsn();
        let entry = WalEntry::new(lsn, tx_id, entry_type, payload);

        let (tx, rx) = bounded(1);
        self.sender
            .send(WriterCommand::Write(entry, Some(tx)))
            .map_err(|_| MonoError::Wal("Writer thread died".to_string()))?;

        rx.recv()
            .map_err(|_| MonoError::Wal("Writer thread died".to_string()))?
    }

    /// Append without waiting for confirmation.
    pub fn append_async(
        &self,
        tx_id: u64,
        entry_type: WalEntryType,
        payload: Vec<u8>,
    ) -> Result<u64> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(MonoError::Wal("WAL is shutdown".to_string()));
        }

        let lsn = self.alloc_lsn();
        let entry = WalEntry::new(lsn, tx_id, entry_type, payload);

        self.sender
            .send(WriterCommand::Write(entry, None))
            .map_err(|_| MonoError::Wal("Writer thread died".to_string()))?;

        Ok(lsn)
    }

    /// Force sync to disk.
    pub fn sync(&self) -> Result<()> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(MonoError::Wal("WAL is shutdown".to_string()));
        }

        let (tx, rx) = bounded(1);
        self.sender
            .send(WriterCommand::Sync(tx))
            .map_err(|_| MonoError::Wal("Writer thread died".to_string()))?;

        rx.recv()
            .map_err(|_| MonoError::Wal("Writer thread died".to_string()))?
    }

    /// Log transaction begin.
    pub fn log_tx_begin(&self, tx_id: u64) -> Result<u64> {
        self.append(tx_id, WalEntryType::TxBegin, Vec::new())
    }

    /// Log transaction commit.
    pub fn log_tx_commit(&self, tx_id: u64) -> Result<u64> {
        let lsn = self.append(tx_id, WalEntryType::TxCommit, Vec::new())?;

        // Track committed transaction
        self.committed_txs.write().insert(tx_id);

        // Sync if configured
        if self.config.sync_on_commit {
            self.sync()?;
        }

        Ok(lsn)
    }

    /// Log transaction rollback.
    pub fn log_tx_rollback(&self, tx_id: u64) -> Result<u64> {
        self.append(tx_id, WalEntryType::TxRollback, Vec::new())
    }

    /// Log insert operation.
    pub fn log_insert<K: Serializable, V: Serializable>(
        &self,
        tx_id: u64,
        table: &str,
        key: &K,
        value: &V,
    ) -> Result<u64> {
        let payload = Self::serialize_mutation(table, key, Some(value));
        self.append(tx_id, WalEntryType::Insert, payload)
    }

    /// Log update operation.
    pub fn log_update<K: Serializable, V: Serializable>(
        &self,
        tx_id: u64,
        table: &str,
        key: &K,
        value: &V,
    ) -> Result<u64> {
        let payload = Self::serialize_mutation(table, key, Some(value));
        self.append(tx_id, WalEntryType::Update, payload)
    }

    /// Log delete operation.
    pub fn log_delete<K: Serializable>(&self, tx_id: u64, table: &str, key: &K) -> Result<u64> {
        let payload = Self::serialize_delete_mutation(table, key);
        self.append(tx_id, WalEntryType::Delete, payload)
    }

    /// Log checkpoint.
    pub fn log_checkpoint(&self) -> Result<u64> {
        self.append(0, WalEntryType::Checkpoint, Vec::new())
    }

    /// Serialize a mutation payload.
    fn serialize_mutation<K: Serializable, V: Serializable>(
        table: &str,
        key: &K,
        value: Option<&V>,
    ) -> Vec<u8> {
        let mut buf = Vec::new();

        // Table name (length-prefixed)
        (table.len() as u32).serialize(&mut buf);
        buf.extend_from_slice(table.as_bytes());

        // Key
        key.serialize(&mut buf);

        // Value (optional)
        if let Some(v) = value {
            buf.push(1);
            v.serialize(&mut buf);
        } else {
            buf.push(0);
        }

        buf
    }

    /// Serialize a delete mutation payload (no value).
    fn serialize_delete_mutation<K: Serializable>(table: &str, key: &K) -> Vec<u8> {
        let mut buf = Vec::new();

        // Table name (length-prefixed)
        (table.len() as u32).serialize(&mut buf);
        buf.extend_from_slice(table.as_bytes());

        // Key
        key.serialize(&mut buf);

        // No value
        buf.push(0);

        buf
    }

    /// Replay WAL entries, calling handler for each entry.
    pub fn replay<F>(&self, mut handler: F) -> Result<u64>
    where
        F: FnMut(&WalEntry) -> Result<()>,
    {
        let file = File::open(&self.path)
            .map_err(|e| MonoError::Wal(format!("Failed to open WAL for replay: {}", e)))?;

        let mut reader = BufReader::new(file);
        let mut header_buf = [0u8; WalEntryHeader::SIZE];
        let mut count = 0u64;
        let mut committed_txs = std::collections::HashSet::new();

        // First pass: find committed transactions
        loop {
            match reader.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(MonoError::Wal(format!("Read error: {}", e))),
            }

            let header = WalEntryHeader::from_bytes(&header_buf);

            if header.entry_type == WalEntryType::Invalid {
                break;
            }

            // Read payload
            let mut payload = vec![0u8; header.length as usize];
            if !payload.is_empty() {
                reader
                    .read_exact(&mut payload)
                    .map_err(|e| MonoError::Wal(format!("Read payload error: {}", e)))?;
            }

            if header.entry_type == WalEntryType::TxCommit {
                committed_txs.insert(header.tx_id);
            }
        }

        // Second pass: replay only committed transactions
        reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| MonoError::Wal(format!("Seek error: {}", e)))?;

        loop {
            match reader.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(MonoError::Wal(format!("Read error: {}", e))),
            }

            let header = WalEntryHeader::from_bytes(&header_buf);

            if header.entry_type == WalEntryType::Invalid {
                break;
            }

            // Read payload
            let mut payload = vec![0u8; header.length as usize];
            if !payload.is_empty() {
                reader
                    .read_exact(&mut payload)
                    .map_err(|e| MonoError::Wal(format!("Read payload error: {}", e)))?;
            }

            let entry = WalEntry { header, payload };

            // Verify checksum
            if !entry.verify() {
                return Err(MonoError::ChecksumMismatch {
                    expected: 0, // We don't have the expected value here
                    actual: entry.header.checksum,
                });
            }

            // Skip uncommitted transactions (MVCC-aware)
            let tx_id = entry.header.tx_id;
            if tx_id != 0 && !committed_txs.contains(&tx_id) {
                continue;
            }

            // Skip control entries
            match entry.header.entry_type {
                WalEntryType::TxBegin | WalEntryType::TxCommit | WalEntryType::TxRollback => {
                    continue;
                }
                _ => {}
            }

            handler(&entry)?;
            count += 1;
        }

        Ok(count)
    }

    /// Get current LSN.
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst) - 1
    }

    /// Truncate WAL after checkpoint.
    pub fn truncate(&self) -> Result<()> {
        self.sync()?;

        // Create new empty WAL file
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| MonoError::Wal(format!("Failed to truncate WAL: {}", e)))?;

        file.sync_all()
            .map_err(|e| MonoError::Wal(format!("Failed to sync truncated WAL: {}", e)))?;

        Ok(())
    }

    /// Shutdown the WAL.
    pub fn shutdown(&self) -> Result<()> {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already shutdown
        }

        let _ = self.sender.send(WriterCommand::Shutdown);

        if let Some(handle) = self.writer_handle.lock().take() {
            handle
                .join()
                .map_err(|_| MonoError::Wal("Writer thread panicked".to_string()))?;
        }

        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_basic_operations() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            path: dir.path().join("test.wal"),
            sync_on_commit: false,
            ..Default::default()
        };

        let wal = Wal::open(config).unwrap();

        // Begin transaction
        let lsn1 = wal.log_tx_begin(1).unwrap();
        assert_eq!(lsn1, 1);

        // Insert
        let lsn2 = wal
            .log_insert(1, "users", &"key1".to_string(), &"value1".to_string())
            .unwrap();
        assert_eq!(lsn2, 2);

        // Commit
        let lsn3 = wal.log_tx_commit(1).unwrap();
        assert_eq!(lsn3, 3);

        wal.shutdown().unwrap();
    }

    #[test]
    fn test_wal_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("replay.wal");
        let config = WalConfig {
            path: path.clone(),
            sync_on_commit: true,
            ..Default::default()
        };

        // Write entries
        {
            let wal = Wal::open(config.clone()).unwrap();

            wal.log_tx_begin(1).unwrap();
            wal.log_insert(1, "users", &"k1".to_string(), &"v1".to_string())
                .unwrap();
            wal.log_tx_commit(1).unwrap();

            // Uncommitted transaction (should be skipped on replay)
            wal.log_tx_begin(2).unwrap();
            wal.log_insert(2, "users", &"k2".to_string(), &"v2".to_string())
                .unwrap();
            // No commit!

            wal.shutdown().unwrap();
        }

        // Replay
        {
            let wal = Wal::open(config).unwrap();

            let mut count = 0;
            wal.replay(|_entry| {
                count += 1;
                Ok(())
            })
            .unwrap();

            // Only 1 entry (committed insert), uncommitted is skipped
            assert_eq!(count, 1);

            wal.shutdown().unwrap();
        }
    }

    #[test]
    fn test_wal_entry_checksum() {
        let entry = WalEntry::new(1, 100, WalEntryType::Insert, b"test payload".to_vec());
        assert!(entry.verify());

        // Corrupt it
        let mut corrupted = entry.clone();
        corrupted.payload[0] ^= 0xFF;
        assert!(!corrupted.verify());
    }

    #[test]
    fn test_wal_async_append() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            path: dir.path().join("async.wal"),
            ..Default::default()
        };

        let wal = Wal::open(config).unwrap();

        // Multiple async appends
        for i in 0..100 {
            wal.append_async(i, WalEntryType::Insert, vec![i as u8])
                .unwrap();
        }

        wal.sync().unwrap();
        wal.shutdown().unwrap();
    }
}

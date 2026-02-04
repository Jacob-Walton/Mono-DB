//! Write-Ahead Logging (WAL) for durability.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use ahash::{HashSet, HashSetExt};
use crossbeam_channel::{Receiver, Sender, bounded};
use monodb_common::{MonoError, Result};
use parking_lot::{Mutex, RwLock};

use super::traits::Serializable;

// WAL format constants

/// WAL file magic number
const WAL_MAGIC: u32 = u32::from_be_bytes(*b"MWAL");

/// WAL version.
const WAL_VERSION: u8 = 3;

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
    /// DDL/schema change.
    Ddl = 8,
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
            8 => WalEntryType::Ddl,
            _ => WalEntryType::Invalid,
        }
    }
}

/// DDL operation recorded in the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalDdlOp {
    /// Upsert a schema entry.
    SchemaUpsert = 1,
    /// Drop a schema entry.
    SchemaDrop = 2,
    /// Create a namespace.
    NamespaceCreate = 3,
    /// Drop a namespace.
    NamespaceDrop = 4,
}

impl WalDdlOp {
    fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(WalDdlOp::SchemaUpsert),
            2 => Some(WalDdlOp::SchemaDrop),
            3 => Some(WalDdlOp::NamespaceCreate),
            4 => Some(WalDdlOp::NamespaceDrop),
            _ => None,
        }
    }
}

/// Parsed DDL payload data.
#[derive(Debug, Clone)]
pub enum WalDdlRecord {
    SchemaUpsert(Vec<u8>),
    SchemaDrop(String),
    NamespaceCreate(String),
    NamespaceDrop { name: String, force: bool },
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
    /// Buffer size for WAL writes.
    pub buffer_size: usize,
    /// Bytes to write before syncing.
    pub sync_every_bytes: Option<u64>,
    /// Maximum time between syncs.
    pub sync_interval_ms: Option<u64>,
    /// Whether to sync on commit.
    pub sync_on_commit: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data.wal"),
            buffer_size: 64 * 1024,
            sync_every_bytes: Some(DEFAULT_SYNC_EVERY_BYTES),
            sync_interval_ms: Some(DEFAULT_SYNC_INTERVAL_MS),
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
            file: BufWriter::with_capacity(config.buffer_size, file),
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
        let mut should_sync = false;

        if let Some(threshold) = self.config.sync_every_bytes
            && self.pending_bytes >= threshold
        {
            should_sync = true;
        }

        if let Some(interval_ms) = self.config.sync_interval_ms
            && self.last_sync.elapsed() >= Duration::from_millis(interval_ms)
        {
            should_sync = true;
        }

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
    /// Committed transaction IDs.
    committed_txs: RwLock<HashSet<u64>>,
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
            committed_txs: RwLock::new(HashSet::new()),
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
    /// Payload: tx_id (u64) + start_ts (u64)
    pub fn log_tx_begin(&self, tx_id: u64, start_ts: u64) -> Result<u64> {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&tx_id.to_le_bytes());
        payload.extend_from_slice(&start_ts.to_le_bytes());
        self.append(tx_id, WalEntryType::TxBegin, payload)
    }

    /// Log transaction commit.
    /// Payload: tx_id (u64) + commit_ts (u64)
    pub fn log_tx_commit(&self, tx_id: u64, commit_ts: u64) -> Result<u64> {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&tx_id.to_le_bytes());
        payload.extend_from_slice(&commit_ts.to_le_bytes());
        let lsn = self.append(tx_id, WalEntryType::TxCommit, payload)?;

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
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&tx_id.to_le_bytes());
        self.append(tx_id, WalEntryType::TxRollback, payload)
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

    /// Log delete operation with a value payload (tombstones, metadata).
    pub fn log_delete_with_value<K: Serializable, V: Serializable>(
        &self,
        tx_id: u64,
        table: &str,
        key: &K,
        value: &V,
    ) -> Result<u64> {
        let payload = Self::serialize_mutation(table, key, Some(value));
        self.append(tx_id, WalEntryType::Delete, payload)
    }

    /// Log checkpoint.
    pub fn log_checkpoint(&self) -> Result<u64> {
        self.append(0, WalEntryType::Checkpoint, Vec::new())
    }

    /// Log schema upsert.
    pub fn log_schema_upsert(&self, schema_bytes: Vec<u8>) -> Result<u64> {
        let mut payload = Vec::new();
        payload.push(WalDdlOp::SchemaUpsert as u8);
        schema_bytes.serialize(&mut payload);
        self.append(0, WalEntryType::Ddl, payload)
    }

    /// Log schema drop.
    pub fn log_schema_drop(&self, table: &str) -> Result<u64> {
        let mut payload = Vec::new();
        payload.push(WalDdlOp::SchemaDrop as u8);
        table.to_string().serialize(&mut payload);
        self.append(0, WalEntryType::Ddl, payload)
    }

    /// Log namespace create.
    pub fn log_namespace_create(&self, name: &str) -> Result<u64> {
        let mut payload = Vec::new();
        payload.push(WalDdlOp::NamespaceCreate as u8);
        name.to_string().serialize(&mut payload);
        self.append(0, WalEntryType::Ddl, payload)
    }

    /// Log namespace drop.
    pub fn log_namespace_drop(&self, name: &str, force: bool) -> Result<u64> {
        let mut payload = Vec::new();
        payload.push(WalDdlOp::NamespaceDrop as u8);
        name.to_string().serialize(&mut payload);
        payload.push(if force { 1 } else { 0 });
        self.append(0, WalEntryType::Ddl, payload)
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

    /// Parse a mutation payload into (table, key, value_blob).
    pub fn parse_mutation_payload(payload: &[u8]) -> Result<(String, Vec<u8>, Option<Vec<u8>>)> {
        let mut offset = 0;

        let (name_len, consumed) = u32::deserialize(payload)?;
        offset += consumed;

        let name_len = name_len as usize;
        if payload.len() < offset + name_len {
            return Err(MonoError::Wal("Malformed WAL payload (table name)".into()));
        }

        let table = std::str::from_utf8(&payload[offset..offset + name_len])
            .map_err(|e| MonoError::Wal(format!("Invalid table name in WAL: {}", e)))?
            .to_string();
        offset += name_len;

        let (key, consumed) = Vec::<u8>::deserialize(&payload[offset..])?;
        offset += consumed;

        if offset >= payload.len() {
            return Ok((table, key, None));
        }

        let has_value = payload[offset];
        offset += 1;
        if has_value == 0 {
            return Ok((table, key, None));
        }

        let value_blob = payload[offset..].to_vec();
        Ok((table, key, Some(value_blob)))
    }

    /// Parse a TxBegin payload into (tx_id, start_ts).
    pub fn parse_tx_begin_payload(payload: &[u8], fallback_tx_id: u64) -> (u64, u64) {
        if payload.len() >= 16 {
            let tx_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let start_ts = u64::from_le_bytes(payload[8..16].try_into().unwrap());
            (tx_id, start_ts)
        } else {
            (fallback_tx_id, fallback_tx_id)
        }
    }

    /// Parse a TxCommit payload into (tx_id, commit_ts).
    pub fn parse_tx_commit_payload(payload: &[u8], fallback_tx_id: u64) -> (u64, u64) {
        if payload.len() >= 16 {
            let tx_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let commit_ts = u64::from_le_bytes(payload[8..16].try_into().unwrap());
            (tx_id, commit_ts)
        } else {
            (fallback_tx_id, fallback_tx_id)
        }
    }

    /// Parse a TxRollback payload into tx_id.
    pub fn parse_tx_rollback_payload(payload: &[u8], fallback_tx_id: u64) -> u64 {
        if payload.len() >= 8 {
            u64::from_le_bytes(payload[0..8].try_into().unwrap())
        } else {
            fallback_tx_id
        }
    }

    /// Parse a DDL payload into a structured record.
    pub fn parse_ddl_payload(payload: &[u8]) -> Result<WalDdlRecord> {
        if payload.is_empty() {
            return Err(MonoError::Wal("Malformed DDL payload".into()));
        }

        let op = WalDdlOp::from_byte(payload[0])
            .ok_or_else(|| MonoError::Wal("Unknown DDL opcode".into()))?;
        let mut offset = 1;

        match op {
            WalDdlOp::SchemaUpsert => {
                let (bytes, consumed) = Vec::<u8>::deserialize(&payload[offset..])?;
                offset += consumed;
                if offset != payload.len() {
                    return Err(MonoError::Wal("Malformed DDL payload".into()));
                }
                Ok(WalDdlRecord::SchemaUpsert(bytes))
            }
            WalDdlOp::SchemaDrop => {
                let (name, consumed) = String::deserialize(&payload[offset..])?;
                offset += consumed;
                if offset != payload.len() {
                    return Err(MonoError::Wal("Malformed DDL payload".into()));
                }
                Ok(WalDdlRecord::SchemaDrop(name))
            }
            WalDdlOp::NamespaceCreate => {
                let (name, consumed) = String::deserialize(&payload[offset..])?;
                offset += consumed;
                if offset != payload.len() {
                    return Err(MonoError::Wal("Malformed DDL payload".into()));
                }
                Ok(WalDdlRecord::NamespaceCreate(name))
            }
            WalDdlOp::NamespaceDrop => {
                let (name, consumed) = String::deserialize(&payload[offset..])?;
                offset += consumed;
                let force = payload.get(offset).copied().unwrap_or(0) != 0;
                Ok(WalDdlRecord::NamespaceDrop { name, force })
            }
        }
    }

    /// Replay WAL entries, calling handler for each entry.
    pub fn replay<F>(&self, mut handler: F) -> Result<u64>
    where
        F: FnMut(&WalEntry) -> Result<()>,
    {
        let entries = self.read_entries()?;
        let mut count = 0u64;
        let mut committed_txs = std::collections::HashSet::new();

        for entry in &entries {
            if entry.header.entry_type == WalEntryType::TxCommit {
                let (tx_id, _) = Self::parse_tx_commit_payload(&entry.payload, entry.header.tx_id);
                committed_txs.insert(tx_id);
            }
        }

        for entry in entries {
            // Skip uncommitted transactions
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

    /// Read all entries from the WAL file.
    pub fn read_entries(&self) -> Result<Vec<WalEntry>> {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(MonoError::Wal(format!("Failed to open WAL: {}", e))),
        };

        let mut reader = BufReader::new(file);
        let mut header_buf = [0u8; WalEntryHeader::SIZE];
        let mut entries = Vec::new();

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

            let mut payload = vec![0u8; header.length as usize];
            if !payload.is_empty() {
                match reader.read_exact(&mut payload) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => {
                        return Err(MonoError::Wal(format!("Read payload error: {}", e)));
                    }
                }
            }

            let entry = WalEntry { header, payload };
            if !entry.verify() {
                return Err(MonoError::ChecksumMismatch {
                    expected: 0,
                    actual: entry.header.checksum,
                });
            }

            entries.push(entry);
        }

        Ok(entries)
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
        let lsn1 = wal.log_tx_begin(1, 1).unwrap();
        assert_eq!(lsn1, 1);

        // Insert
        let lsn2 = wal
            .log_insert(1, "users", &"key1".to_string(), &"value1".to_string())
            .unwrap();
        assert_eq!(lsn2, 2);

        // Commit
        let lsn3 = wal.log_tx_commit(1, 2).unwrap();
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

            wal.log_tx_begin(1, 1).unwrap();
            wal.log_insert(1, "users", &"k1".to_string(), &"v1".to_string())
                .unwrap();
            wal.log_tx_commit(1, 2).unwrap();

            // Uncommitted transaction (should be skipped on replay)
            wal.log_tx_begin(2, 2).unwrap();
            wal.log_insert(2, "users", &"k2".to_string(), &"v2".to_string())
                .unwrap();
            // No commit

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

            // Only 1 entry, uncommitted is skipped
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

    #[test]
    fn test_wal_ddl_entries() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            path: dir.path().join("ddl.wal"),
            sync_on_commit: false,
            ..Default::default()
        };

        {
            let wal = Wal::open(config.clone()).unwrap();
            wal.log_schema_upsert(vec![1, 2, 3]).unwrap();
            wal.log_schema_drop("default.users").unwrap();
            wal.log_namespace_create("analytics").unwrap();
            wal.log_namespace_drop("analytics", true).unwrap();
            wal.shutdown().unwrap();
        }

        {
            let wal = Wal::open(config).unwrap();
            let entries = wal.read_entries().unwrap();
            let ddl_entries: Vec<_> = entries
                .iter()
                .filter(|e| e.header.entry_type == WalEntryType::Ddl)
                .collect();
            assert_eq!(ddl_entries.len(), 4);

            match Wal::parse_ddl_payload(&ddl_entries[0].payload).unwrap() {
                WalDdlRecord::SchemaUpsert(bytes) => assert_eq!(bytes, vec![1, 2, 3]),
                other => panic!("unexpected ddl entry: {:?}", other),
            }
            match Wal::parse_ddl_payload(&ddl_entries[1].payload).unwrap() {
                WalDdlRecord::SchemaDrop(name) => assert_eq!(name, "default.users"),
                other => panic!("unexpected ddl entry: {:?}", other),
            }
            match Wal::parse_ddl_payload(&ddl_entries[2].payload).unwrap() {
                WalDdlRecord::NamespaceCreate(name) => assert_eq!(name, "analytics"),
                other => panic!("unexpected ddl entry: {:?}", other),
            }
            match Wal::parse_ddl_payload(&ddl_entries[3].payload).unwrap() {
                WalDdlRecord::NamespaceDrop { name, force } => {
                    assert_eq!(name, "analytics");
                    assert!(force);
                }
                other => panic!("unexpected ddl entry: {:?}", other),
            }

            wal.shutdown().unwrap();
        }
    }
}

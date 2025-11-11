use monodb_common::{MonoError, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, error, info, warn};

use crate::config::WalConfig;

/// Write-Ahead Log
pub struct Wal {
    writer: BufWriter<File>,
    file_path: PathBuf,
    sequence_number: AtomicU64,
    /// Current size of the WAL file
    current_size: u64,
    /// Maximum size before rotation (64MB default)
    max_size: u64,
    /// Whether to sync after every write (default: true for durability)
    sync_on_write: bool,
    /// Last checkpoint information
    last_checkpoint: Option<CheckpointInfo>,
    /// Optional async writer
    #[allow(dead_code)]
    async_mode: bool,
    #[allow(dead_code)]
    tx: Option<std::sync::mpsc::Sender<Vec<u8>>>,
    /// Group-commit byte counter
    bytes_since_sync: u64,
    /// Last time we performed a sync
    last_sync: Instant,
    /// Optional thresholds for group commit
    sync_every_bytes: Option<u64>,
    sync_interval: Option<Duration>,
    /// Last fully synced WAL sequence (async mode)
    last_synced: Arc<AtomicU64>,
    /// Commit barrier notification (async mode)
    commit_mu: Arc<Mutex<()>>,
    commit_cv: Arc<Condvar>,
}

/// Checkpoint metadata for tracking state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    /// Sequence number of the checkpoint
    pub sequence: u64,
    /// Timestamp when checkpoint was created
    pub timestamp: u64,
    /// List of SSTable files that existed at checkpoint time
    pub sstable_files: Vec<String>,
    /// Schema version at checkpoint time
    pub schema_version: u64,
}

/// WAL record header structure
/// Format: [MAGIC][VERSION][SEQUENCE][TIMESTAMP][TYPE][KEY_LEN][VALUE_LEN][CRC32]
const WAL_MAGIC: u32 = u32::from_be_bytes(*b"WLOG");
const WAL_VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub sequence: u64,
    pub timestamp: u64,
    pub entry_type: WalEntryType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum WalEntryType {
    Insert = 1,
    Update = 2,
    Delete = 3,
    Checkpoint = 4,
}

impl From<u8> for WalEntryType {
    fn from(value: u8) -> Self {
        match value {
            1 => WalEntryType::Insert,
            2 => WalEntryType::Update,
            3 => WalEntryType::Delete,
            4 => WalEntryType::Checkpoint,
            _ => WalEntryType::Insert, // Default fallback
        }
    }
}

#[derive(Debug)]
pub enum WalError {
    Corruption(String),
    InvalidFormat(String),
    IoError(std::io::Error),
    ChecksumMismatch { expected: u32, actual: u32 },
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Corruption(msg) => write!(f, "WAL corruption: {}", msg),
            WalError::InvalidFormat(msg) => write!(f, "Invalid WAL format: {}", msg),
            WalError::IoError(e) => write!(f, "WAL I/O error: {}", e),
            WalError::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "WAL checksum mismatch: expected {}, got {}",
                    expected, actual
                )
            }
        }
    }
}

impl std::error::Error for WalError {}

impl From<std::io::Error> for WalError {
    fn from(error: std::io::Error) -> Self {
        WalError::IoError(error)
    }
}

impl Wal {
    /// Create a new WAL
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_config(path, WalConfig::default())
    }

    /// Create a new WAL with custom configuration
    pub fn with_config<P: AsRef<Path>>(path: P, config: WalConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let current_size = file.metadata()?.len();
        let writer = BufWriter::with_capacity(config.buffer_size, file);

        // Get the last sequence number from the file
        let last_sequence = Self::get_last_sequence_number(&path)?;

        // Load last checkpoint information
        let last_checkpoint = Self::load_last_checkpoint(&path)?;

        info!(
            "Opened WAL, size: {} bytes, last sequence: {}, last checkpoint: {:?}",
            current_size,
            last_sequence,
            last_checkpoint.as_ref().map(|c| c.sequence)
        );

        let mut wal = Self {
            writer,
            file_path: path,
            sequence_number: AtomicU64::new(last_sequence + 1),
            current_size,
            max_size: config.max_size,
            sync_on_write: config.sync_on_write,
            last_checkpoint,
            async_mode: false,
            tx: None,
            bytes_since_sync: 0,
            last_sync: Instant::now(),
            sync_every_bytes: config.sync_every_bytes,
            sync_interval: config.sync_interval_ms.map(Duration::from_millis),
            last_synced: Arc::new(AtomicU64::new(last_sequence)),
            commit_mu: Arc::new(Mutex::new(())),
            commit_cv: Arc::new(Condvar::new()),
        };

        if config.async_write {
            // Spawn background writer thread
            let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
            let path_bg = wal.file_path.clone();
            let buffer_size = config.buffer_size;
            let sync_on_write = config.sync_on_write;
            let max_size = config.max_size;
            let sync_every_bytes = config.sync_every_bytes;
            let sync_interval_ms = config.sync_interval_ms;
            let mut current_size_bg = wal.current_size;

            let last_synced_arc = Arc::clone(&wal.last_synced);
            let commit_mu_arc = Arc::clone(&wal.commit_mu);
            let commit_cv_arc = Arc::clone(&wal.commit_cv);
            std::thread::spawn(move || {
                let open_writer = || -> std::io::Result<BufWriter<File>> {
                    let f = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path_bg)?;
                    Ok(BufWriter::with_capacity(buffer_size, f))
                };
                let mut writer = match open_writer() {
                    Ok(w) => w,
                    Err(_) => return,
                };
                let mut pending_bytes: u64 = 0;
                let mut last_sync = Instant::now();
                let per_write_sync =
                    sync_on_write && sync_every_bytes.is_none() && sync_interval_ms.is_none();
                let mut max_seq_written: u64 = 0;
                loop {
                    // Determine timeout for recv based on sync interval
                    let timeout_opt = if sync_on_write {
                        sync_interval_ms.map(Duration::from_millis)
                    } else {
                        None
                    };
                    let recv_result = if let Some(to) = timeout_opt {
                        rx.recv_timeout(to)
                    } else {
                        rx.recv()
                            .map_err(|_| std::sync::mpsc::RecvTimeoutError::Disconnected)
                    };
                    match recv_result {
                        Ok(buf) => {
                            if current_size_bg + buf.len() as u64 > max_size {
                                // Rotate: close current, archive, open new
                                let _ = writer.flush();
                                let _ = sync_file_data(writer.get_ref());
                                let ts = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                let archive = path_bg.with_extension(format!("wal.{}", ts));
                                let _ = std::fs::rename(&path_bg, &archive);
                                // Create fresh file
                                if let Ok(f) = OpenOptions::new()
                                    .create(true)
                                    .write(true)
                                    .truncate(true)
                                    .open(&path_bg)
                                {
                                    writer = BufWriter::with_capacity(buffer_size, f);
                                    current_size_bg = 0;
                                }
                                // rotation implies a sync point; update last_synced
                                last_synced_arc.store(max_seq_written, Ordering::Release);
                                let _g = commit_mu_arc.lock().unwrap();
                                commit_cv_arc.notify_all();
                            }
                            let _ = writer.write_all(&buf);
                            // Extract sequence from buffer (offset 6..14)
                            if buf.len() >= 14 {
                                let mut seq_bytes = [0u8; 8];
                                seq_bytes.copy_from_slice(&buf[6..14]);
                                let seq = u64::from_le_bytes(seq_bytes);
                                if seq > max_seq_written {
                                    max_seq_written = seq;
                                }
                            }
                            pending_bytes += buf.len() as u64;
                            if sync_on_write {
                                let mut do_sync = false;
                                if let Some(threshold) = sync_every_bytes {
                                    if pending_bytes >= threshold {
                                        do_sync = true;
                                    }
                                }
                                if !do_sync {
                                    if let Some(iv) = sync_interval_ms.map(Duration::from_millis) {
                                        if last_sync.elapsed() >= iv {
                                            do_sync = true;
                                        }
                                    }
                                }
                                if per_write_sync || do_sync {
                                    let _ = writer.flush();
                                    let _ = sync_file_data(writer.get_ref());
                                    last_sync = Instant::now();
                                    pending_bytes = 0;
                                    // Update last_synced and notify waiters
                                    last_synced_arc.store(max_seq_written, Ordering::Release);
                                    let _g = commit_mu_arc.lock().unwrap();
                                    commit_cv_arc.notify_all();
                                }
                            }
                            current_size_bg += buf.len() as u64;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            // Timer-based sync
                            if sync_on_write && pending_bytes > 0 {
                                let _ = writer.flush();
                                let _ = sync_file_data(writer.get_ref());
                                last_sync = Instant::now();
                                pending_bytes = 0;
                                last_synced_arc.store(max_seq_written, Ordering::Release);
                                let _g = commit_mu_arc.lock().unwrap();
                                commit_cv_arc.notify_all();
                            }
                        }
                        Err(_) => break,
                    }
                }
                // Final flush on shutdown
                let _ = writer.flush();
                let _ = sync_file_data(writer.get_ref());
                last_synced_arc.store(max_seq_written, Ordering::Release);
                let _g = commit_mu_arc.lock().unwrap();
                commit_cv_arc.notify_all();
            });
            wal.async_mode = true;
            wal.tx = Some(tx);
        }

        Ok(wal)
    }

    /// Returns true if WAL is using background async writer
    pub fn is_async(&self) -> bool {
        self.async_mode
    }

    /// Append using async path without taking a mutable writer lock.
    /// Only valid when async mode is enabled.
    pub fn append_async(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        self.append_with_type_async(key, value, WalEntryType::Insert)
    }

    /// Append with explicit type using async path without mutable lock.
    /// Only valid when async mode is enabled.
    pub fn append_with_type_async(
        &self,
        key: &[u8],
        value: &[u8],
        entry_type: WalEntryType,
    ) -> Result<u64> {
        if !self.async_mode {
            return Err(MonoError::InvalidOperation(
                "WAL async mode is disabled".to_string(),
            ));
        }

        let sequence = self.sequence_number.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let record_size = HEADER_SIZE + key.len() + value.len();
        let mut buf = Vec::with_capacity(record_size);
        buf.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        buf.extend_from_slice(&WAL_VERSION.to_le_bytes());
        buf.extend_from_slice(&sequence.to_le_bytes());
        buf.extend_from_slice(&timestamp.to_le_bytes());
        buf.push(entry_type as u8);
        buf.extend_from_slice(&[0u8; 3]);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&sequence.to_le_bytes());
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&[entry_type as u8]);
        hasher.update(&(key.len() as u32).to_le_bytes());
        hasher.update(&(value.len() as u32).to_le_bytes());
        hasher.update(key);
        hasher.update(value);
        let checksum = hasher.finalize();
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);

        if let Some(tx) = &self.tx {
            let _ = tx.send(buf);
        }
        Ok(sequence)
    }

    /// Append a new entry to the WAL
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<u64> {
        self.append_with_type(key, value, WalEntryType::Insert)
    }

    /// Append an entry with specific type
    pub fn append_with_type(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: WalEntryType,
    ) -> Result<u64> {
        let sequence = self.sequence_number.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if self.async_mode {
            // Pre-serialize into a buffer and send to background thread
            let record_size = HEADER_SIZE + key.len() + value.len();
            let mut buf = Vec::with_capacity(record_size);
            buf.extend_from_slice(&WAL_MAGIC.to_le_bytes());
            buf.extend_from_slice(&WAL_VERSION.to_le_bytes());
            buf.extend_from_slice(&sequence.to_le_bytes());
            buf.extend_from_slice(&timestamp.to_le_bytes());
            buf.push(entry_type as u8);
            buf.extend_from_slice(&[0u8; 3]);
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&sequence.to_le_bytes());
            hasher.update(&timestamp.to_le_bytes());
            hasher.update(&[entry_type as u8]);
            hasher.update(&(key.len() as u32).to_le_bytes());
            hasher.update(&(value.len() as u32).to_le_bytes());
            hasher.update(key);
            hasher.update(value);
            let checksum = hasher.finalize();
            buf.extend_from_slice(&checksum.to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(value);
            if let Some(tx) = &self.tx {
                let _ = tx.send(buf);
            }
            return Ok(sequence);
        }

        // Calculate total record size
        let record_size = HEADER_SIZE + key.len() + value.len();

        // Check if we need to rotate the log
        if self.current_size + record_size as u64 > self.max_size {
            self.rotate_log()?;
        }

        // Pre-serialize into a single buffer and write once
        let mut buf = Vec::with_capacity(record_size);
        buf.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        buf.extend_from_slice(&WAL_VERSION.to_le_bytes());
        buf.extend_from_slice(&sequence.to_le_bytes());
        buf.extend_from_slice(&timestamp.to_le_bytes());
        buf.push(entry_type as u8);
        buf.extend_from_slice(&[0u8; 3]);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());

        // Calculate CRC32 for the entire payload
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&sequence.to_le_bytes());
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&[entry_type as u8]);
        hasher.update(&(key.len() as u32).to_le_bytes());
        hasher.update(&(value.len() as u32).to_le_bytes());
        hasher.update(key);
        hasher.update(value);
        let checksum = hasher.finalize();
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);

        self.writer.write_all(&buf)?;

        // Group-commit logic
        if self.sync_on_write {
            self.bytes_since_sync += buf.len() as u64;
            let mut do_sync = false;
            if let Some(threshold) = self.sync_every_bytes {
                if self.bytes_since_sync >= threshold {
                    do_sync = true;
                }
            }
            if !do_sync {
                if let Some(iv) = self.sync_interval {
                    if self.last_sync.elapsed() >= iv {
                        do_sync = true;
                    }
                }
            }
            if do_sync || (self.sync_every_bytes.is_none() && self.sync_interval.is_none()) {
                self.writer.flush()?;
                sync_file_data(self.writer.get_ref())?;
                self.last_sync = Instant::now();
                self.bytes_since_sync = 0;
                // In sync path, consider the just-appended sequence durable
                self.last_synced.store(sequence, Ordering::Release);
                let _g = self.commit_mu.lock().unwrap();
                self.commit_cv.notify_all();
            }
        }

        self.current_size += record_size as u64;

        debug!(
            "WAL: Appended sequence {} ({} bytes): {:?}",
            sequence, record_size, entry_type
        );

        Ok(sequence)
    }

    /// Force flush and sync all pending writes
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Replay all entries from the WAL file
    pub fn replay<P: AsRef<Path>>(path: P) -> Result<Vec<WalEntry>> {
        let path = path.as_ref();
        let mut entries = Vec::new();

        let mut file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("WAL file not found at {:?}, starting with empty log", path);
                return Ok(entries);
            }
            Err(e) => return Err(e.into()),
        };

        let file_size = file.metadata()?.len();
        if file_size == 0 {
            debug!("WAL file is empty");
            return Ok(entries);
        }

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut cursor = 0;
        let mut recovered_entries = 0;
        let mut corrupted_entries = 0;

        while cursor + HEADER_SIZE <= buffer.len() {
            match Self::parse_entry(&buffer, &mut cursor) {
                Ok(entry) => {
                    entries.push(entry);
                    recovered_entries += 1;
                }
                Err(WalError::Corruption(msg)) => {
                    warn!("Skipping corrupted WAL entry at offset {}: {}", cursor, msg);
                    corrupted_entries += 1;
                    // Try to find the next valid record
                    if !Self::find_next_valid_record(&buffer, &mut cursor) {
                        break;
                    }
                }
                Err(e) => {
                    error!("Fatal WAL error at offset {}: {}", cursor, e);
                    break;
                }
            }
        }

        // Sort entries by sequence number to handle out-of-order writes
        entries.sort_by_key(|entry| entry.sequence);

        info!(
            "WAL replay complete: {} entries recovered, {} corrupted, from {} bytes",
            recovered_entries, corrupted_entries, file_size
        );

        Ok(entries)
    }

    /// Create a checkpoint entry in the WAL
    pub fn checkpoint(&mut self, sstable_files: Vec<String>, schema_version: u64) -> Result<u64> {
        let sequence = self.sequence_number.load(Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let checkpoint_info = CheckpointInfo {
            sequence,
            timestamp,
            sstable_files: sstable_files.clone(),
            schema_version,
        };

        // Serialize checkpoint metadata
        let checkpoint_data = bincode::serialize(&checkpoint_info)
            .map_err(|e| MonoError::Storage(format!("Failed to serialize checkpoint: {}", e)))?;

        // Write checkpoint entry to WAL
        let checkpoint_seq =
            self.append_with_type(b"checkpoint", &checkpoint_data, WalEntryType::Checkpoint)?;

        // Save checkpoint metadata to separate file
        self.save_checkpoint_metadata(&checkpoint_info)?;

        // Update in-memory checkpoint info
        self.last_checkpoint = Some(checkpoint_info.clone());

        info!(
            "Created checkpoint at sequence {} with {} SSTable files",
            checkpoint_seq,
            sstable_files.len()
        );

        Ok(checkpoint_seq)
    }

    /// Rotate the WAL log (archive current and start new)
    fn rotate_log(&mut self) -> Result<()> {
        // Flush current log
        self.sync()?;

        // Archive current log with timestamp
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let archive_path = self.file_path.with_extension(format!("wal.{}", timestamp));

        std::fs::rename(&self.file_path, &archive_path)?;
        info!("Archived WAL to {:?}", archive_path);

        // Create new log file
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.file_path)?;

        let cap = self.writer.capacity();
        self.writer = BufWriter::with_capacity(cap, file);
        self.current_size = 0;

        info!("Created new WAL at {:?}", self.file_path);
        Ok(())
    }

    /// Parse a single WAL entry from buffer
    fn parse_entry(buffer: &[u8], cursor: &mut usize) -> std::result::Result<WalEntry, WalError> {
        let _start_pos = *cursor;

        if *cursor + HEADER_SIZE > buffer.len() {
            return Err(WalError::Corruption("Incomplete header".to_string()));
        }

        // Verify magic number
        let magic = u32::from_le_bytes([
            buffer[*cursor],
            buffer[*cursor + 1],
            buffer[*cursor + 2],
            buffer[*cursor + 3],
        ]);
        *cursor += 4;

        if magic != WAL_MAGIC {
            return Err(WalError::Corruption(format!(
                "Invalid magic number: expected {}, got {}",
                WAL_MAGIC, magic
            )));
        }

        // Read version
        let version = u16::from_le_bytes([buffer[*cursor], buffer[*cursor + 1]]);
        *cursor += 2;

        if version != WAL_VERSION {
            return Err(WalError::InvalidFormat(format!(
                "Unsupported WAL version: {}",
                version
            )));
        }

        // Read sequence number
        let sequence = u64::from_le_bytes([
            buffer[*cursor],
            buffer[*cursor + 1],
            buffer[*cursor + 2],
            buffer[*cursor + 3],
            buffer[*cursor + 4],
            buffer[*cursor + 5],
            buffer[*cursor + 6],
            buffer[*cursor + 7],
        ]);
        *cursor += 8;

        // Read timestamp
        let timestamp = u64::from_le_bytes([
            buffer[*cursor],
            buffer[*cursor + 1],
            buffer[*cursor + 2],
            buffer[*cursor + 3],
            buffer[*cursor + 4],
            buffer[*cursor + 5],
            buffer[*cursor + 6],
            buffer[*cursor + 7],
        ]);
        *cursor += 8;

        // Read entry type and skip padding
        let entry_type = WalEntryType::from(buffer[*cursor]);
        *cursor += 4; // type + 3 bytes padding

        // Read lengths
        let key_len = u32::from_le_bytes([
            buffer[*cursor],
            buffer[*cursor + 1],
            buffer[*cursor + 2],
            buffer[*cursor + 3],
        ]) as usize;
        *cursor += 4;

        let value_len = u32::from_le_bytes([
            buffer[*cursor],
            buffer[*cursor + 1],
            buffer[*cursor + 2],
            buffer[*cursor + 3],
        ]) as usize;
        *cursor += 4;

        // Read stored checksum
        let stored_checksum = u32::from_le_bytes([
            buffer[*cursor],
            buffer[*cursor + 1],
            buffer[*cursor + 2],
            buffer[*cursor + 3],
        ]);
        *cursor += 4;

        // Verify we have enough data for the payload
        if *cursor + key_len + value_len > buffer.len() {
            return Err(WalError::Corruption("Incomplete payload".to_string()));
        }

        // Read payload
        let key = buffer[*cursor..*cursor + key_len].to_vec();
        *cursor += key_len;
        let value = buffer[*cursor..*cursor + value_len].to_vec();
        *cursor += value_len;

        // Verify checksum
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&sequence.to_le_bytes());
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&[entry_type as u8]);
        hasher.update(&(key_len as u32).to_le_bytes());
        hasher.update(&(value_len as u32).to_le_bytes());
        hasher.update(&key);
        hasher.update(&value);
        let computed_checksum = hasher.finalize();

        if stored_checksum != computed_checksum {
            return Err(WalError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        Ok(WalEntry {
            sequence,
            timestamp,
            entry_type,
            key,
            value,
        })
    }

    /// Find the next valid record after corruption
    fn find_next_valid_record(buffer: &[u8], cursor: &mut usize) -> bool {
        *cursor += 1; // Skip current position

        while *cursor + 4 <= buffer.len() {
            let magic = u32::from_le_bytes([
                buffer[*cursor],
                buffer[*cursor + 1],
                buffer[*cursor + 2],
                buffer[*cursor + 3],
            ]);

            if magic == WAL_MAGIC {
                return true; // Found potential start of next record
            }
            *cursor += 1;
        }

        false // No more valid records found
    }

    /// Get the last sequence number from an existing WAL file
    fn get_last_sequence_number<P: AsRef<Path>>(path: P) -> Result<u64> {
        let entries = Self::replay(path)?;
        Ok(entries.last().map(|e| e.sequence).unwrap_or(0))
    }

    /// Clear the WAL file (called after successful compaction)
    pub fn clear<P: AsRef<Path>>(path: P) -> Result<()> {
        std::fs::write(path, b"")?;
        info!("Cleared WAL file");
        Ok(())
    }

    /// Get current WAL statistics
    pub fn stats(&self) -> WalStats {
        WalStats {
            current_size: self.current_size,
            next_sequence: self.sequence_number.load(Ordering::SeqCst),
            max_size: self.max_size,
            last_checkpoint_sequence: self.last_checkpoint.as_ref().map(|c| c.sequence),
        }
    }

    /// Load the last checkpoint metadata from disk
    fn load_last_checkpoint<P: AsRef<Path>>(wal_path: P) -> Result<Option<CheckpointInfo>> {
        let checkpoint_path = wal_path.as_ref().with_extension("checkpoint");

        match std::fs::read(&checkpoint_path) {
            Ok(data) => match bincode::deserialize::<CheckpointInfo>(&data) {
                Ok(checkpoint) => {
                    debug!(
                        "Loaded checkpoint metadata: sequence {}",
                        checkpoint.sequence
                    );
                    Ok(Some(checkpoint))
                }
                Err(e) => {
                    warn!("Failed to deserialize checkpoint metadata: {}", e);
                    Ok(None)
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("No checkpoint metadata file found");
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to read checkpoint metadata: {}", e);
                Ok(None)
            }
        }
    }

    /// Save checkpoint metadata to disk for quick access during recovery
    fn save_checkpoint_metadata(&self, checkpoint: &CheckpointInfo) -> Result<()> {
        let checkpoint_path = self.file_path.with_extension("checkpoint");
        let data = bincode::serialize(checkpoint)
            .map_err(|e| MonoError::Storage(format!("Failed to serialize checkpoint: {}", e)))?;

        std::fs::write(&checkpoint_path, data)?;
        debug!("Saved checkpoint metadata to {:?}", checkpoint_path);
        Ok(())
    }

    /// Truncate WAL to the last checkpoint, removing all entries before it
    pub fn truncate_to_checkpoint(&mut self) -> Result<()> {
        let checkpoint = match &self.last_checkpoint {
            Some(cp) => cp.clone(),
            None => {
                debug!("No checkpoint found, cannot truncate WAL");
                return Ok(());
            }
        };

        info!(
            "Truncating WAL to checkpoint at sequence {}",
            checkpoint.sequence
        );

        // Read all entries from the current WAL
        let entries = Self::replay(&self.file_path)?;

        // Filter entries to keep only those after the checkpoint
        let entries_after_checkpoint: Vec<_> = entries
            .into_iter()
            .filter(|entry| entry.sequence > checkpoint.sequence)
            .collect();

        // Create a new WAL file with only post-checkpoint entries
        let backup_path = self.file_path.with_extension("wal.truncating");

        // Flush and close current writer
        self.sync()?;

        // Rename current WAL to backup
        std::fs::rename(&self.file_path, &backup_path)?;

        // Create new WAL file
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.file_path)?;

        let cap = self.writer.capacity();
        self.writer = BufWriter::with_capacity(cap, file);
        self.current_size = 0;

        // Re-write entries after checkpoint
        for entry in entries_after_checkpoint {
            self.append_with_type(&entry.key, &entry.value, entry.entry_type)?;
        }

        // Remove backup file
        std::fs::remove_file(&backup_path)?;

        info!(
            "WAL truncation completed, new size: {} bytes",
            self.current_size
        );
        Ok(())
    }

    /// Get the last checkpoint information
    pub fn last_checkpoint(&self) -> Option<&CheckpointInfo> {
        self.last_checkpoint.as_ref()
    }

    /// Check if WAL needs truncation based on size and checkpoint age
    pub fn should_truncate(&self) -> bool {
        if let Some(checkpoint) = &self.last_checkpoint {
            let current_seq = self.sequence_number.load(Ordering::SeqCst);
            let entries_since_checkpoint = current_seq.saturating_sub(checkpoint.sequence);

            // Truncate if we have more than 1000 entries since last checkpoint
            // or if WAL is larger than 80% of max size
            entries_since_checkpoint > 1000
                || (self.current_size as f64 / self.max_size as f64) > 0.8
        } else {
            false
        }
    }
}

/// WAL statistics
#[derive(Debug)]
pub struct WalStats {
    pub current_size: u64,
    pub next_sequence: u64,
    pub max_size: u64,
    pub last_checkpoint_sequence: Option<u64>,
}

#[inline]
fn sync_file_data(f: &File) -> std::io::Result<()> {
    #[cfg(target_family = "unix")]
    {
        f.sync_data()
    }
    #[cfg(not(target_family = "unix"))]
    {
        // On non-Unix targets, fall back to sync_all
        f.sync_all()
    }
}

impl Wal {
    /// Wait until the WAL is synced (fsynced) to at least the given sequence.
    /// Only supported in async mode; returns Ok(true) if reached, Ok(false) on timeout.
    pub fn commit_barrier(&self, sequence: u64, timeout: Option<Duration>) -> Result<bool> {
        if !self.async_mode {
            return Err(MonoError::InvalidOperation(
                "commit_barrier requires async WAL mode".to_string(),
            ));
        }
        if self.last_synced.load(Ordering::Acquire) >= sequence {
            return Ok(true);
        }
        let guard = self.commit_mu.lock().unwrap();
        if let Some(to) = timeout {
            let mut g = guard;
            let start = Instant::now();
            let mut remaining = to;
            loop {
                let (gg, timeout_res) = self.commit_cv.wait_timeout(g, remaining).unwrap();
                g = gg;
                if self.last_synced.load(Ordering::Acquire) >= sequence {
                    return Ok(true);
                }
                if timeout_res.timed_out() {
                    return Ok(false);
                }
                let elapsed = start.elapsed();
                if elapsed >= to {
                    return Ok(false);
                }
                remaining = to - elapsed;
            }
        } else {
            let mut g = guard;
            while self.last_synced.load(Ordering::Acquire) < sequence {
                g = self.commit_cv.wait(g).unwrap();
            }
            Ok(true)
        }
    }
}

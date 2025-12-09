use monodb_common::{MonoError, Result};
use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{RecvTimeoutError, Sender, channel},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::config::WalConfig;

/// Magic number: 'WLOG'
const WAL_MAGIC: u32 = 0x574C4F47;
/// Current format version
const WAL_VERSION: u16 = 2;
/// Standard header size (32 bytes)
const HEADER_SIZE: usize = 32;
/// Extended header size (40 bytes)
const EXTENDED_HEADER_SIZE: usize = 40;

/// Maximum key length for standard record
const MAX_STANDARD_KEY_LEN: usize = 254;
/// Maximum value length for standard record
const MAX_STANDARD_VALUE_LEN: usize = 65_534;

// Flags

/// Value payload is LZ4 compressed
#[allow(dead_code)]
const FLAG_COMPRESSED: u16 = 0b0000_0001;
/// Last entry in a batch (safe sync point)
const FLAG_BATCH_END: u16 = 0b0000_0010;
/// Uses extended length fields
const FLAG_EXTENDED: u16 = 0b0000_0100;

// Entry types

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalEntryType {
    Insert = 0x01,
    Update = 0x02,
    Delete = 0x03,
    Checkpoint = 0x04,
    TxBegin = 0x05,
    TxCommit = 0x06,
    TxRollback = 0x07,
}

impl From<u8> for WalEntryType {
    fn from(value: u8) -> Self {
        match value {
            0x01 => WalEntryType::Insert,
            0x02 => WalEntryType::Update,
            0x03 => WalEntryType::Delete,
            0x04 => WalEntryType::Checkpoint,
            0x05 => WalEntryType::TxBegin,
            0x06 => WalEntryType::TxCommit,
            0x07 => WalEntryType::TxRollback,
            _ => panic!("Invalid WAL entry type: {}", value),
        }
    }
}

// WAL Entry

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub sequence: u64,
    #[allow(dead_code)]
    pub timestamp: u64,
    pub entry_type: WalEntryType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    #[allow(dead_code)]
    pub flags: u16,
}

// Checkpoint Info

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub sequence: u64,
    pub timestamp: u64,
    pub sstable_files: Vec<String>,
    pub schema_version: u64,
}

// WAL Error

#[derive(Debug)]
pub enum WalError {
    Corruption(String),
    InvalidFormat(String),
    IoError(io::Error),
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
                    "WAL checksum mismatch: expected {:#x}, got {:#x}",
                    expected, actual
                )
            }
        }
    }
}

impl std::error::Error for WalError {}

impl From<io::Error> for WalError {
    fn from(err: io::Error) -> Self {
        WalError::IoError(err)
    }
}

impl From<WalError> for MonoError {
    fn from(err: WalError) -> Self {
        MonoError::Storage(format!("WAL error: {}", err))
    }
}

// WAL Statistics

#[derive(Debug)]
pub struct WalStats {
    pub current_size: u64,
    pub next_sequence: u64,
    pub max_size: u64,
    pub last_checkpoint_sequence: Option<u64>,
}

// Record Encoding/Decoding

/// Encode a WAL record into bytes.
/// Returns the encoded buffer.
fn encode_record(
    sequence: u64,
    timestamp: u64,
    entry_type: WalEntryType,
    key: &[u8],
    value: &[u8],
    batch_end: bool,
) -> Vec<u8> {
    let needs_extended = key.len() > MAX_STANDARD_KEY_LEN || value.len() > MAX_STANDARD_VALUE_LEN;

    let header_size = if needs_extended {
        EXTENDED_HEADER_SIZE
    } else {
        HEADER_SIZE
    };
    let total_size = header_size + key.len() + value.len();

    let mut buf = Vec::with_capacity(total_size);

    // Build flags
    let mut flags: u16 = 0;
    if needs_extended {
        flags |= FLAG_EXTENDED;
    }
    if batch_end {
        flags |= FLAG_BATCH_END;
    }

    // Header bytes [0..28]
    // [0..4] MAGIC
    buf.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    // [4..6] Version
    buf.extend_from_slice(&WAL_VERSION.to_le_bytes());
    // [6..8] Flags
    buf.extend_from_slice(&flags.to_le_bytes());
    // [8..16] Sequence
    buf.extend_from_slice(&sequence.to_le_bytes());
    // [16..24] Timestamp
    buf.extend_from_slice(&timestamp.to_le_bytes());
    // [24] Entry Type
    buf.push(entry_type as u8);
    // [25] KEY_LEN (u8) or 0xFF for extended
    // [26..28] VALUE_LEN (u16) or 0xFFFF for extended
    if needs_extended {
        buf.push(0xFF); // KEY_LEN marker
        buf.extend_from_slice(&0xFFFFu16.to_le_bytes()); // VALUE_LEN marker
    } else {
        buf.push(key.len() as u8);
        buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
    }

    // CRC placeholder at [28..32]
    // We'll compute this after building the rest
    let crc_offset = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Extended header if needed
    if needs_extended {
        // [32..36] KEY_LEN (u32)
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        // [36..40] VALUE_LEN (u32)
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    }

    // Append key and value
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);

    // Compute CRC32
    // CRC covers: header[0..28] + extended_header? + key + value
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[0..crc_offset]); // header up to CRC
    if needs_extended {
        hasher.update(&buf[32..40]); // extended header
    }
    hasher.update(key);
    hasher.update(value);
    let crc = hasher.finalize();

    // Write CRC into buffer
    buf[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());

    buf
}

/// Parse a WAL record from a buffer starting at `cursor`.
/// Updates `cursor` to point past the parsed record on success.
fn parse_record(buffer: &[u8], cursor: &mut usize) -> std::result::Result<WalEntry, WalError> {
    let start = *cursor;

    // Need at least standard header
    if start + HEADER_SIZE > buffer.len() {
        return Err(WalError::Corruption("Incomplete header".into()));
    }

    // [0..4] MAGIC
    let magic = u32::from_le_bytes(buffer[start..start + 4].try_into().unwrap());
    if magic != WAL_MAGIC {
        return Err(WalError::Corruption(format!(
            "Invalid magic: expected {:#x}, got {:#x}",
            WAL_MAGIC, magic
        )));
    }

    // [4..6] Version
    let version = u16::from_le_bytes(buffer[start + 4..start + 6].try_into().unwrap());
    if version != WAL_VERSION {
        return Err(WalError::InvalidFormat(format!(
            "Unsupported WAL version: {}",
            version
        )));
    }

    // [6..8] Flags
    let flags = u16::from_le_bytes(buffer[start + 6..start + 8].try_into().unwrap());
    let is_extended = (flags & FLAG_EXTENDED) != 0;
    // [8..16] Sequence
    let sequence = u64::from_le_bytes(buffer[start + 8..start + 16].try_into().unwrap());
    // [16..24] Timestamp
    let timestamp = u64::from_le_bytes(buffer[start + 16..start + 24].try_into().unwrap());
    // [24] Entry Type
    let entry_type = WalEntryType::from(buffer[start + 24]);
    // [25] KEY_LEN_HDR, [26..28] VALUE_LEN_HDR
    let key_len_hdr = buffer[start + 25];
    let value_len_hdr = u16::from_le_bytes(buffer[start + 26..start + 28].try_into().unwrap());

    // [28..32] CRC
    let stored_crc = u32::from_le_bytes(buffer[start + 28..start + 32].try_into().unwrap());

    // Determine actual key/value lengths
    let (key_len, value_len, data_start) = if is_extended {
        // Need extended header
        if start + EXTENDED_HEADER_SIZE > buffer.len() {
            return Err(WalError::Corruption("Incomplete extended header".into()));
        }
        let ext_key_len =
            u32::from_le_bytes(buffer[start + 32..start + 36].try_into().unwrap()) as usize;
        let ext_value_len =
            u32::from_le_bytes(buffer[start + 36..start + 40].try_into().unwrap()) as usize;
        (ext_key_len, ext_value_len, start + EXTENDED_HEADER_SIZE)
    } else {
        (
            key_len_hdr as usize,
            value_len_hdr as usize,
            start + HEADER_SIZE,
        )
    };

    // Verify we have enough data for payload
    if data_start + key_len + value_len > buffer.len() {
        return Err(WalError::Corruption("Incomplete record payload".into()));
    }

    // Extract key and value
    let key = buffer[data_start..data_start + key_len].to_vec();
    let value = buffer[data_start + key_len..data_start + key_len + value_len].to_vec();

    // Compute CRC to verify
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buffer[start..start + 28]); // header up to CRC
    if is_extended {
        hasher.update(&buffer[start + 32..start + 40]); // extended header
    }
    hasher.update(&key);
    hasher.update(&value);
    let computed_crc = hasher.finalize();

    if stored_crc != computed_crc {
        return Err(WalError::ChecksumMismatch {
            expected: stored_crc,
            actual: computed_crc,
        });
    }

    // Update cursor
    *cursor = data_start + key_len + value_len;

    Ok(WalEntry {
        sequence,
        timestamp,
        entry_type,
        key,
        value,
        flags,
    })
}

/// Scan forward for the next magic number after corruption.
fn find_next_magic(buffer: &[u8], cursor: &mut usize) -> bool {
    *cursor += 1;
    while *cursor + 4 <= buffer.len() {
        let magic = u32::from_le_bytes(buffer[*cursor..*cursor + 4].try_into().unwrap());
        if magic == WAL_MAGIC {
            return true;
        }
        *cursor += 1;
    }
    false
}

// WAL Implementation

pub struct Wal {
    writer: BufWriter<File>,
    file_path: PathBuf,
    sequence_number: AtomicU64,
    current_size: u64,
    max_size: u64,
    sync_on_write: bool,
    last_checkpoint: Option<CheckpointInfo>,
    async_mode: bool,
    tx: Option<Sender<Vec<u8>>>,
    bytes_since_sync: u64,
    last_sync: Instant,
    sync_every_bytes: Option<u64>,
    sync_interval: Option<Duration>,
    last_synced: Arc<AtomicU64>,
    commit_mu: Arc<Mutex<()>>,
    commit_cv: Arc<Condvar>,
}

impl Wal {
    /// Create a new WAL with default configuration
    #[allow(dead_code)]
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_config(path, WalConfig::default())
    }

    /// Create a new WAL with custom configuration
    pub fn with_config<P: AsRef<Path>>(path: P, config: WalConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let current_size = file.metadata()?.len();
        let writer = BufWriter::with_capacity(config.buffer_size, file);

        let last_sequence = Self::get_last_sequence_number(&path)?;
        let last_checkpoint = Self::load_last_checkpoint(&path)?;

        #[cfg(debug_assertions)]
        tracing::debug!(
            "Opened WAL, size {} bytes, last sequence: {}, last checkpoint: {:?}",
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
            wal.spawn_async_writer(config);
        }

        Ok(wal)
    }

    /// Spawn the background async writer thread
    fn spawn_async_writer(&mut self, config: WalConfig) {
        let (tx, rx) = channel::<Vec<u8>>();
        let path_bg = self.file_path.clone();
        let buffer_size = config.buffer_size;
        let sync_on_write = config.sync_on_write;
        let max_size = config.max_size;
        let sync_every_bytes = config.sync_every_bytes;
        let sync_interval_ms = config.sync_interval_ms;
        let mut current_size_bg = self.current_size;

        let last_synced_arc = Arc::clone(&self.last_synced);
        let commit_mu_arc = Arc::clone(&self.commit_mu);
        let commit_cv_arc = Arc::clone(&self.commit_cv);

        thread::spawn(move || {
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

            let mut pending_bytes = 0u64;
            let mut last_sync = Instant::now();
            let per_write_sync =
                sync_on_write && sync_every_bytes.is_none() && sync_interval_ms.is_none();
            let mut max_seq_written = 0u64;

            loop {
                let timeout_opt = if sync_on_write {
                    sync_interval_ms.map(Duration::from_millis)
                } else {
                    None
                };

                let recv_reuslt = if let Some(to) = timeout_opt {
                    rx.recv_timeout(to)
                } else {
                    rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
                };

                match recv_reuslt {
                    Ok(buf) => {
                        // Check rotation
                        if current_size_bg + buf.len() as u64 > max_size {
                            let _ = writer.flush();
                            let _ = sync_file_data(writer.get_ref());
                            let ts = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            let archive = path_bg.with_extension(format!("wal.{}", ts));
                            let _ = std::fs::rename(&path_bg, &archive);
                            if let Ok(f) =
                                OpenOptions::new().create(true).append(true).open(&path_bg)
                            {
                                writer = BufWriter::with_capacity(buffer_size, f);
                                current_size_bg = 0;
                            }
                            last_synced_arc.store(max_seq_written, Ordering::Release);
                            let _g = commit_mu_arc.lock().unwrap();
                            commit_cv_arc.notify_all();
                        }

                        let _ = writer.write_all(&buf);

                        // Extract sequence from buffer [8..16]
                        if buf.len() >= 16 {
                            let seq = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                            if seq > max_seq_written {
                                max_seq_written = seq;
                            }
                        }

                        pending_bytes += buf.len() as u64;
                        current_size_bg += buf.len() as u64;

                        if sync_on_write {
                            let mut do_sync = per_write_sync;
                            if !do_sync
                                && let Some(threshold) = sync_every_bytes
                                && pending_bytes >= threshold
                            {
                                do_sync = true;
                            }
                            if !do_sync
                                && let Some(iv) = sync_interval_ms.map(Duration::from_millis)
                                && last_sync.elapsed() >= iv
                            {
                                do_sync = true;
                            }
                            if do_sync {
                                let _ = writer.flush();
                                let _ = sync_file_data(writer.get_ref());
                                pending_bytes = 0;
                                last_sync = Instant::now();
                                last_synced_arc.store(max_seq_written, Ordering::Release);
                                let _g = commit_mu_arc.lock().unwrap();
                                commit_cv_arc.notify_all();
                            }
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
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

            // Final flush
            let _ = writer.flush();
            let _ = sync_file_data(writer.get_ref());
            last_synced_arc.store(max_seq_written, Ordering::Release);
            let _g = commit_mu_arc.lock().unwrap();
            commit_cv_arc.notify_all();
        });

        self.async_mode = true;
        self.tx = Some(tx);
    }

    /// Returns true if WAL is using background async writer
    pub fn is_async(&self) -> bool {
        self.async_mode
    }

    /// Get current timestamp in microseconds
    #[inline(always)]
    fn now_micros() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    /// Append a new Insert entry to the WAL
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
        let timestamp = Self::now_micros();

        let buf = encode_record(sequence, timestamp, entry_type, key, value, false);

        if self.async_mode {
            if let Some(tx) = &self.tx {
                let _ = tx.send(buf);
            }
            return Ok(sequence);
        }

        // Check rotation
        if self.current_size + buf.len() as u64 > self.max_size {
            self.rotate_log()?;
        }

        self.writer.write_all(&buf)?;
        self.current_size += buf.len() as u64;

        // Group-commit logic
        if self.sync_on_write {
            self.bytes_since_sync += buf.len() as u64;
            let mut do_sync = self.sync_every_bytes.is_none() && self.sync_interval.is_none();

            if !do_sync
                && let Some(threshold) = self.sync_every_bytes
                && self.bytes_since_sync >= threshold
            {
                do_sync = true;
            }
            if !do_sync
                && let Some(iv) = self.sync_interval
                && self.last_sync.elapsed() >= iv
            {
                do_sync = true;
            }

            if do_sync {
                self.writer.flush()?;
                sync_file_data(self.writer.get_ref())?;
                self.last_sync = Instant::now();
                self.bytes_since_sync = 0;
                self.last_synced.store(sequence, Ordering::Release);
                let _g = self.commit_mu.lock().unwrap();
                self.commit_cv.notify_all();
            }
        }

        #[cfg(debug_assertions)]
        tracing::debug!(
            "WAL: Appended entry seq {} ({} bytes): {:?}",
            sequence,
            buf.len(),
            entry_type
        );

        Ok(sequence)
    }

    /// Apend using async path without mutable lock
    pub fn append_async(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        self.append_with_type_async(key, value, WalEntryType::Insert)
    }

    /// Append with explicit type using async path
    pub fn append_with_type_async(
        &self,
        key: &[u8],
        value: &[u8],
        entry_type: WalEntryType,
    ) -> Result<u64> {
        if !self.async_mode {
            return Err(MonoError::InvalidOperation(
                "WAL async mode is disabled".into(),
            ));
        }

        let sequence = self.sequence_number.fetch_add(1, Ordering::Relaxed);
        let timestamp = Self::now_micros();
        let buf = encode_record(sequence, timestamp, entry_type, key, value, false);

        if let Some(tx) = &self.tx {
            let _ = tx.send(buf);
        }

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

        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                tracing::debug!("WAL file not found for replay: {:?}", path);
                return Ok(entries);
            }
            Err(e) => return Err(e.into()),
        };

        let file_size = file.metadata()?.len();
        if file_size == 0 {
            tracing::debug!("WAL file is empty: {:?}", path);
            return Ok(entries);
        }

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut cursor = 0;
        let mut recovered_entries = 0;
        let mut corrupted_entries = 0;

        while cursor + HEADER_SIZE <= buffer.len() {
            match parse_record(&buffer, &mut cursor) {
                Ok(entry) => {
                    entries.push(entry);
                    recovered_entries += 1;
                }
                Err(WalError::Corruption(msg)) => {
                    tracing::warn!("Skipping corrupted WAL entry: {}", msg);
                    corrupted_entries += 1;
                    if !find_next_magic(&buffer, &mut cursor) {
                        break;
                    }
                }
                Err(WalError::ChecksumMismatch { expected, actual }) => {
                    tracing::warn!(
                        "Skipping WAL entry with checksum mismatch: expected {:#x}, got {:#x}",
                        expected,
                        actual
                    );
                    corrupted_entries += 1;
                    if !find_next_magic(&buffer, &mut cursor) {
                        break;
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        // Sort by sequence number to ensure correct order
        entries.sort_by_key(|e| e.sequence);

        #[cfg(debug_assertions)]
        tracing::debug!(
            "WAL Replay: Recovered {} entries, skipped {} corrupted entries",
            recovered_entries,
            corrupted_entries
        );

        Ok(entries)
    }

    /// Create a checkpoint entry in the WAL
    pub fn checkpoint(&mut self, sstable_files: Vec<String>, schema_version: u64) -> Result<u64> {
        let sequence = self.sequence_number.load(Ordering::SeqCst);
        let timestamp = Self::now_micros();

        let checkpoint_info = CheckpointInfo {
            sequence,
            timestamp,
            sstable_files: sstable_files.clone(),
            schema_version,
        };

        let mut checkpoint_data = Vec::new();
        checkpoint_data.extend_from_slice(&schema_version.to_le_bytes()); // u64
        checkpoint_data.extend_from_slice(&(sstable_files.len() as u32).to_le_bytes()); // u32
        for file in &sstable_files {
            let name_bytes = file.as_bytes();
            checkpoint_data.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            checkpoint_data.extend_from_slice(name_bytes);
        }

        let checkpoint_seq =
            self.append_with_type(b"checkpoint", &checkpoint_data, WalEntryType::Checkpoint)?;

        self.save_checkpoint_metadata(&checkpoint_info)?;
        self.last_checkpoint = Some(checkpoint_info);

        #[cfg(debug_assertions)]
        tracing::debug!(
            "WAL: Created checkpoint at seq {} with {} SSTable files",
            checkpoint_seq,
            sstable_files.len()
        );

        Ok(checkpoint_seq)
    }

    /// Rotate the WAL log
    fn rotate_log(&mut self) -> Result<()> {
        self.sync()?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let archive_path = self.file_path.with_extension(format!("wal.{}", timestamp));

        std::fs::rename(&self.file_path, &archive_path)?;
        #[cfg(debug_assertions)]
        tracing::debug!("WAL: Rotated log file to {:?}", archive_path);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)?;

        let cap = self.writer.capacity();
        self.writer = BufWriter::with_capacity(cap, file);
        self.current_size = 0;

        #[cfg(debug_assertions)]
        tracing::debug!("WAL: Created new log file {:?}", self.file_path);

        Ok(())
    }

    /// Get the last sequence number from an existing WAL file
    fn get_last_sequence_number<P: AsRef<Path>>(path: P) -> Result<u64> {
        let entries = Self::replay(path)?;
        Ok(entries.last().map(|e| e.sequence).unwrap_or(0))
    }

    /// Clear the WAL file
    pub fn clear<P: AsRef<Path>>(path: P) -> Result<()> {
        std::fs::write(path, b"")?;

        #[cfg(debug_assertions)]
        tracing::debug!("WAL: Cleared log file");

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

        let data = match std::fs::read(&checkpoint_path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                #[cfg(debug_assertions)]
                tracing::debug!(
                    "WAL: No checkpoint metadata file found at {:?}",
                    checkpoint_path
                );
                return Ok(None);
            }
            Err(e) => {
                tracing::warn!("Failed to read checkpoint metadata: {}", e);
                return Ok(None);
            }
        };

        // Minimum size: 8 (schema_ver) + 4 (file_count) + 8 (sequence) + 8 (timestamp) = 28
        if data.len() < 28 {
            tracing::warn!(
                "WAL: Checkpoint metadata file too small: {} bytes",
                data.len()
            );
            return Ok(None);
        }

        let schema_version = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let file_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

        let mut cursor = 12;
        let mut sstable_files = Vec::with_capacity(file_count);

        for _ in 0..file_count {
            if cursor + 2 > data.len() {
                tracing::warn!("Checkpoint metadata truncated while reading file entry");
                return Ok(None);
            }
            let name_len =
                u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap()) as usize;
            cursor += 2;

            if cursor + name_len > data.len() {
                tracing::warn!("Checkpoint metadata truncated while reading file name");
                return Ok(None);
            }
            let name = String::from_utf8_lossy(&data[cursor..cursor + name_len]).to_string();
            cursor += name_len;
            sstable_files.push(name);
        }

        // Read trailing sequence and timestamp
        if cursor + 16 > data.len() {
            tracing::warn!("Checkpoint metadata truncated while reading sequence/timestamp");
            return Ok(None);
        }
        let sequence = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        let timestamp = u64::from_le_bytes(data[cursor + 8..cursor + 16].try_into().unwrap());

        #[cfg(debug_assertions)]
        tracing::debug!(
            "WAL: Loaded checkpoint metadata: seq {}, schema_ver {}, {} SSTable files",
            sequence,
            schema_version,
            sstable_files.len()
        );

        Ok(Some(CheckpointInfo {
            sequence,
            timestamp,
            sstable_files,
            schema_version,
        }))
    }

    /// Save checkpoint metadata to disk
    fn save_checkpoint_metadata(&self, checkpoint: &CheckpointInfo) -> Result<()> {
        let checkpoint_path = self.file_path.with_extension("checkpoint");

        let mut data = Vec::new();

        // [0..8] Schema Version
        data.extend_from_slice(&checkpoint.schema_version.to_le_bytes());

        // [8..12] SSTable file count
        data.extend_from_slice(&(checkpoint.sstable_files.len() as u32).to_le_bytes());

        // [12..] SSTable file names
        for file in &checkpoint.sstable_files {
            let name_bytes = file.as_bytes();
            data.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            data.extend_from_slice(name_bytes);
        }

        // Append sequence and timestamp
        data.extend_from_slice(&checkpoint.sequence.to_le_bytes());
        data.extend_from_slice(&checkpoint.timestamp.to_le_bytes());

        std::fs::write(&checkpoint_path, &data)?;
        #[cfg(debug_assertions)]
        tracing::debug!("WAL: Saved checkpoint metadata to {:?}", checkpoint_path);
        Ok(())
    }

    /// Truncate WAL to the last checkpoint
    pub fn truncate_to_checkpoint(&mut self) -> Result<()> {
        let checkpoint = match &self.last_checkpoint {
            Some(chk) => chk.clone(),
            None => {
                #[cfg(debug_assertions)]
                tracing::debug!("WAL: No checkpoint to truncate to");
                return Ok(());
            }
        };

        let entries = Self::replay(&self.file_path)?;
        let entries_after_checkpoint: Vec<_> = entries
            .into_iter()
            .filter(|e| e.sequence > checkpoint.sequence)
            .collect();

        let backup_path = self.file_path.with_extension("wal.truncating");
        self.sync()?;

        std::fs::rename(&self.file_path, &backup_path)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)?;

        let cap = self.writer.capacity();
        self.writer = BufWriter::with_capacity(cap, file);
        self.current_size = 0;

        for entry in entries_after_checkpoint {
            self.append_with_type(&entry.key, &entry.value, entry.entry_type)?;
        }

        std::fs::remove_file(&backup_path)?;

        #[cfg(debug_assertions)]
        tracing::debug!(
            "WAL: Truncated to checkpoint at seq {}",
            checkpoint.sequence
        );
        Ok(())
    }

    /// Get the last checkpoint information
    #[allow(dead_code)]
    pub fn last_checkpoint(&self) -> Option<&CheckpointInfo> {
        self.last_checkpoint.as_ref()
    }

    /// Check if WAL needs truncation
    pub fn should_truncate(&self) -> bool {
        if let Some(checkpoint) = &self.last_checkpoint {
            let current_seq = self.sequence_number.load(Ordering::SeqCst);
            let entries_since_checkpoint = current_seq.saturating_sub(checkpoint.sequence);
            entries_since_checkpoint > 1000
                || (self.current_size as f64 / self.max_size as f64) > 0.8
        } else {
            false
        }
    }

    /// Wait until WAL is synced to at least the given sequence number
    pub fn commit_barrier(&self, sequence: u64, timeout: Option<Duration>) -> Result<bool> {
        if !self.async_mode {
            return Err(MonoError::InvalidOperation(
                "'commit_barrier' requires async WAL mode".into(),
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

// Helper functions

#[inline(always)]
fn sync_file_data(f: &File) -> io::Result<()> {
    if cfg!(target_family = "unix") {
        f.sync_data()
    } else {
        f.sync_all()
    }
}

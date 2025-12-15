//! SSTable file format and helpers.
//!
//! Index offsets are relative to the first entry byte (i.e., after the header).

use crate::storage::lsm::bloom_filter::BloomFilter;
use monodb_common::Result;
use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

const SST_MAGIC: u32 = 0x53535431; // "SST1"
const SST_HEADER_BYTES: u64 = 16;
const SST_FOOTER_BYTES: u64 = 16; // index_offset (u64) | entry_count (u32) | footer magic (u32)
const SST_FOOTER_MAGIC: u32 = 0x53535446; // "SSTF"
const MAX_KEY_LEN: u64 = 1 << 20; // 1 MiB upper bound for sanity
const MAX_VALUE_LEN: u64 = 1 << 28; // 256 MiB upper bound for sanity

static SSTABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct SsTable {
    pub level: usize,
    pub path: PathBuf,
    file: Arc<File>,
    /// Absolute offset where the serialized index starts (from file start).
    #[allow(dead_code)]
    pub index_offset: u64,
    /// End of data section (relative to the first entry, i.e., after the header).
    pub data_end_offset: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub file_size: usize,
    pub bloom_filter: Option<BloomFilter>,
    pub index: SsTableIndex,
    pub created_at: SystemTime,
    pub num_entries: usize,
}

#[derive(Clone)]
pub struct SsTableIndex {
    pub entries: Vec<IndexEntry>,
    #[allow(dead_code)]
    pub block_size: usize,
}

#[derive(Clone)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: u64, // relative to first-entry offset (i.e., after header)
}

pub struct SsTableIterator {
    reader: Option<BufReader<File>>,
    pos: u64, // bytes read relative to data section (after header)
    limit: u64, // total data section length (excludes index/footer)
              // iterator starts positioned at first entry after header
}

impl SsTableIterator {
    /// Create an iterator positioned at the first entry after the header.
    pub fn new(table: &SsTable) -> std::io::Result<Self> {
        Self::from_parts(table.file.clone(), table.data_end_offset)
    }

    pub fn from_parts(file: Arc<File>, data_end_offset: u64) -> std::io::Result<Self> {
        let mut file = file.as_ref().try_clone()?;
        let mut header = [0u8; SST_HEADER_BYTES as usize];
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != SST_MAGIC {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "bad SSTable magic",
            ));
        }
        // Iterator does not need level or block_size from header.

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(SST_HEADER_BYTES))?;
        Ok(Self {
            reader: Some(reader),
            pos: 0,
            limit: data_end_offset,
        })
    }
}

impl Iterator for SsTableIterator {
    type Item = std::io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;

        if self.pos >= self.limit {
            self.reader = None;
            return None;
        }

        // key length
        if self.limit - self.pos < 8 {
            self.reader = None;
            return None;
        }
        let mut len_buf = [0u8; 8];
        if let Err(e) = reader.read_exact(&mut len_buf) {
            self.reader = None;
            return Some(Err(e));
        }
        let key_len = u64::from_le_bytes(len_buf);
        if key_len > MAX_KEY_LEN {
            self.reader = None;
            return None;
        }
        self.pos += 8;
        if self.limit - self.pos < key_len {
            self.reader = None;
            return None;
        }
        let mut key = vec![0u8; key_len as usize];
        if let Err(e) = reader.read_exact(&mut key) {
            self.reader = None;
            return Some(Err(e));
        }
        self.pos += key_len;

        // value length
        if self.limit - self.pos < 8 {
            self.reader = None;
            return None;
        }
        if let Err(e) = reader.read_exact(&mut len_buf) {
            self.reader = None;
            return Some(Err(e));
        }
        let val_len = u64::from_le_bytes(len_buf);
        if val_len > MAX_VALUE_LEN {
            self.reader = None;
            return None;
        }
        self.pos += 8;
        if self.limit - self.pos < val_len {
            self.reader = None;
            return None;
        }
        let mut value = vec![0u8; val_len as usize];
        if let Err(e) = reader.read_exact(&mut value) {
            self.reader = None;
            return Some(Err(e));
        }
        self.pos += val_len;

        Some(Ok((key, value)))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct HeapEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub sstable_source_index: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.sstable_source_index.cmp(&other.sstable_source_index))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Builder for writing a new SSTable to disk.
pub struct SsTableBuilder {
    level: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    estimated_size: usize,
    bloom_filter: BloomFilter,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder targeting a specific LSM level.
    pub fn new(level: usize) -> Self {
        Self {
            level,
            entries: Vec::new(),
            estimated_size: 0,
            bloom_filter: BloomFilter::new(10_000, 0.01),
            block_size: 100, // default fanout
        }
    }

    /// Add a single key/value pair to the table.
    pub fn add(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.estimated_size += key.len() + value.len() + 16;
        self.bloom_filter.add(&key);
        self.entries.push((key, value));
        Ok(())
    }

    /// Best-effort estimated in-memory size of buffered entries.
    #[allow(dead_code)]
    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    /// Returns true if any entries have been added.
    pub fn has_data(&self) -> bool {
        !self.entries.is_empty()
    }

    /// Finalize and write the SSTable file to `data_dir`.
    pub async fn finish(self, data_dir: &std::path::Path) -> Result<SsTable> {
        use std::time::UNIX_EPOCH;

        if self.entries.is_empty() {
            return Err(monodb_common::MonoError::Storage(
                "Cannot finish empty SSTable".into(),
            ));
        }

        let min_key = self.entries.first().unwrap().0.clone();
        let max_key = self.entries.last().unwrap().0.clone();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let counter = SSTABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let filename = format!("{}_{}", timestamp, counter);

        // Filename no longer encodes level; header is source of truth.
        let output_file = data_dir.join(format!("{filename}.sstable"));

        // Build the entire file in-memory, then write once.
        // Header (little-endian): MAGIC | level | block_size | reserved
        let total_size = SST_HEADER_BYTES as usize + self.estimated_size;
        let mut file_bytes = Vec::with_capacity(total_size);
        file_bytes.extend_from_slice(&SST_MAGIC.to_le_bytes());
        file_bytes.extend_from_slice(&(self.level as u32).to_le_bytes());
        file_bytes.extend_from_slice(&(self.block_size as u32).to_le_bytes());
        file_bytes.extend_from_slice(&0u32.to_le_bytes()); // reserved

        // First-entry offset relative to after the header.
        let mut current_offset = 0u64; // offsets are relative to after header
        let mut index_entries = Vec::new();

        // Append entries and build sparse index
        for (i, (key, value)) in self.entries.iter().enumerate() {
            if i.is_multiple_of(self.block_size) {
                index_entries.push(IndexEntry {
                    key: key.clone(),
                    offset: current_offset,
                });
            }

            let klen = key.len() as u64;
            let vlen = value.len() as u64;
            file_bytes.extend_from_slice(&klen.to_le_bytes());
            file_bytes.extend_from_slice(key);
            file_bytes.extend_from_slice(&vlen.to_le_bytes());
            file_bytes.extend_from_slice(value);
            current_offset += 8 + klen + 8 + vlen;
        }

        // Serialize index right after the data section
        let index_offset = file_bytes.len() as u64;
        for entry in &index_entries {
            let key_len = entry.key.len() as u32;
            file_bytes.extend_from_slice(&key_len.to_le_bytes());
            file_bytes.extend_from_slice(&entry.key);
            file_bytes.extend_from_slice(&entry.offset.to_le_bytes());
        }

        // Append footer: index_offset (absolute), entry_count, footer magic
        file_bytes.extend_from_slice(&index_offset.to_le_bytes());
        file_bytes.extend_from_slice(&(index_entries.len() as u32).to_le_bytes());
        file_bytes.extend_from_slice(&SST_FOOTER_MAGIC.to_le_bytes());

        // Single write to disk
        tokio::fs::write(&output_file, &file_bytes).await?;
        let file_size = file_bytes.len();
        let file = Arc::new(File::open(&output_file)?);
        let data_end_offset = index_offset.saturating_sub(SST_HEADER_BYTES);

        Ok(SsTable {
            level: self.level,
            path: output_file,
            file,
            index_offset,
            data_end_offset,
            min_key,
            max_key,
            file_size,
            bloom_filter: Some(self.bloom_filter),
            index: SsTableIndex {
                entries: index_entries,
                block_size: self.block_size,
            },
            created_at: SystemTime::now(),
            num_entries: self.entries.len(),
        })
    }
}

impl SsTable {
    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key < self.min_key.as_slice() || key > self.max_key.as_slice() {
            return Ok(None);
        }

        if let Some(ref bloom) = self.bloom_filter
            && !bloom.might_contain(key)
        {
            return Ok(None);
        }

        // choose start offset via sparse index
        let block_index = self
            .index
            .entries
            .partition_point(|entry| entry.key.as_slice() <= key);

        let rel_start = if block_index == 0 {
            0
        } else {
            self.index.entries[block_index - 1].offset
        };

        let mut reader = BufReader::new(self.file.try_clone()?);
        reader.seek(SeekFrom::Start(SST_HEADER_BYTES + rel_start))?;
        let mut pos = rel_start;
        let limit = self.data_end_offset;

        while pos < limit {
            if limit - pos < 8 {
                break;
            }
            let mut len_buf = [0u8; 8];
            reader.read_exact(&mut len_buf)?;
            let key_len = u64::from_le_bytes(len_buf);
            if key_len > MAX_KEY_LEN {
                break;
            }
            pos += 8;
            if limit - pos < key_len {
                break;
            }
            let mut key_from_file = vec![0u8; key_len as usize];
            reader.read_exact(&mut key_from_file)?;
            pos += key_len;

            if key_from_file.as_slice() > key {
                break;
            }

            if limit - pos < 8 {
                break;
            }
            reader.read_exact(&mut len_buf)?;
            let val_len = u64::from_le_bytes(len_buf);
            if val_len > MAX_VALUE_LEN {
                break;
            }
            pos += 8;
            if limit - pos < val_len {
                break;
            }

            if key_from_file.as_slice() == key {
                let mut value = vec![0u8; val_len as usize];
                reader.read_exact(&mut value)?;
                return Ok(Some(value));
            } else {
                reader.seek(SeekFrom::Current(val_len as i64))?;
                pos += val_len;
            }
        }

        Ok(None)
    }

    /// Scan a bounded key range and return entries in ascending order.
    /// Uses the sparse index to seek near `start_key` and stops early when keys
    /// exceed `end_key` or `limit` is reached.
    pub fn scan_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Fast path: the range does not intersect this SSTable.
        if end_key < self.min_key.as_slice() || start_key > self.max_key.as_slice() {
            return Ok(Vec::new());
        }

        // Pick a starting offset using the sparse index.
        let block_index = self
            .index
            .entries
            .partition_point(|entry| entry.key.as_slice() < start_key);

        let rel_start = if block_index == 0 {
            0
        } else {
            self.index.entries[block_index - 1].offset
        };

        let mut reader = BufReader::new(self.file.try_clone()?);
        reader.seek(SeekFrom::Start(SST_HEADER_BYTES + rel_start))?;

        let mut results = Vec::new();
        let mut pos = rel_start;
        let limit_bytes = self.data_end_offset;

        while pos < limit_bytes {
            if limit_bytes - pos < 8 {
                break;
            }
            let mut len_buf = [0u8; 8];
            reader.read_exact(&mut len_buf)?;
            let key_len = u64::from_le_bytes(len_buf);
            if key_len > MAX_KEY_LEN {
                break;
            }
            pos += 8;
            if limit_bytes - pos < key_len {
                break;
            }
            let mut key_from_file = vec![0u8; key_len as usize];
            reader.read_exact(&mut key_from_file)?;
            pos += key_len;

            if key_from_file.as_slice() > end_key {
                break;
            }

            if limit_bytes - pos < 8 {
                break;
            }
            reader.read_exact(&mut len_buf)?;
            let val_len = u64::from_le_bytes(len_buf);
            if val_len > MAX_VALUE_LEN {
                break;
            }
            pos += 8;
            if limit_bytes - pos < val_len {
                break;
            }

            if key_from_file.as_slice() >= start_key {
                let mut value = vec![0u8; val_len as usize];
                reader.read_exact(&mut value)?;
                results.push((key_from_file, value));
                pos += val_len;

                if let Some(max) = limit
                    && results.len() >= max
                {
                    break;
                }
            } else {
                // Skip value bytes
                reader.seek(SeekFrom::Current(val_len as i64))?;
                pos += val_len;
            }
        }

        Ok(results)
    }

    /// Read a full indexed block into memory (bounded by the data section).
    pub fn read_block(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let safe_end = end_offset.min(self.data_end_offset);
        let mut reader = BufReader::new(self.file.try_clone()?);
        reader.seek(SeekFrom::Start(SST_HEADER_BYTES + start_offset))?;

        let mut entries = Vec::new();
        let mut pos = start_offset;
        loop {
            if pos >= safe_end {
                break;
            }
            // Ensure enough bytes for key length
            if SST_HEADER_BYTES + safe_end - pos < 8 {
                break;
            }
            let mut len_buf = [0u8; 8];
            reader.read_exact(&mut len_buf)?;
            let key_len = u64::from_le_bytes(len_buf);
            if key_len > MAX_KEY_LEN {
                break;
            }
            pos += 8;
            if pos >= safe_end || safe_end - pos < key_len {
                break;
            }
            let mut key = vec![0u8; key_len as usize];
            reader.read_exact(&mut key)?;
            pos += key_len;

            if SST_HEADER_BYTES + safe_end - pos < 8 {
                break;
            }
            reader.read_exact(&mut len_buf)?;
            let val_len = u64::from_le_bytes(len_buf);
            if val_len > MAX_VALUE_LEN {
                break;
            }
            pos += 8;
            if pos >= safe_end || safe_end - pos < val_len {
                break;
            }
            let mut value = vec![0u8; val_len as usize];
            reader.read_exact(&mut value)?;
            pos += val_len;
            entries.push((key, value));
        }

        Ok(entries)
    }

    /// Load SSTable metadata from disk. Reads header for level and block size.
    pub fn load_metadata(path: PathBuf) -> Result<Self> {
        let file = Arc::new(std::fs::File::open(&path)?);
        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        // read header
        let mut file_reader = file.try_clone()?;
        let mut header = [0u8; SST_HEADER_BYTES as usize];
        file_reader.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != SST_MAGIC {
            return Err(monodb_common::MonoError::Storage(
                "Invalid SSTable magic".into(),
            ));
        }

        let level = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let block_size = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        // reserved = header[12..16]

        // Attempt to read footer for index location; if missing, fall back.
        let footer = read_footer(&file)?;
        let (index_offset, entry_count) = match footer {
            Some(v) => v,
            None => (file_size as u64, 0),
        };
        let data_end_offset = index_offset.saturating_sub(SST_HEADER_BYTES);

        // Load sparse index if present
        let mut index_entries = Vec::new();
        if entry_count > 0 && index_offset + SST_FOOTER_BYTES <= file_size as u64 {
            let index_region = file_size as u64 - index_offset - SST_FOOTER_BYTES;
            let mut consumed = 0u64;
            let mut reader = BufReader::new(file.try_clone()?);
            reader.seek(SeekFrom::Start(index_offset))?;
            for _ in 0..entry_count {
                let mut len_buf = [0u8; 4];
                if consumed + 4 > index_region {
                    break;
                }
                reader.read_exact(&mut len_buf)?;
                consumed += 4;

                let key_len = u32::from_le_bytes(len_buf) as u64;
                // Guard against absurd key sizes or truncated index.
                let fixed_overhead = 8u64; // offset u64
                if key_len > index_region.saturating_sub(consumed + fixed_overhead) {
                    break;
                }

                let mut key = vec![0u8; key_len as usize];
                reader.read_exact(&mut key)?;
                consumed += key_len;

                let mut offset_buf = [0u8; 8];
                reader.read_exact(&mut offset_buf)?;
                consumed += 8;
                let offset = u64::from_le_bytes(offset_buf);
                index_entries.push(IndexEntry { key, offset });
            }
        }

        // scan entries to get min/max and count (bounded by data section)
        let mut min_key = Vec::new();
        let mut max_key = Vec::new();
        let mut num_entries = 0;

        if let Ok(mut iter) = SsTableIterator::from_parts(file.clone(), data_end_offset)
            && let Some(Ok((first_key, _))) = iter.next()
        {
            min_key = first_key.clone();
            max_key = first_key;
            num_entries = 1;

            while let Some(Ok((k, _))) = iter.next() {
                max_key = k;
                num_entries += 1;
            }
        }

        let created_at = metadata.created().unwrap_or_else(|_| SystemTime::now());

        let table = SsTable {
            level,
            path,
            file,
            index_offset,
            data_end_offset,
            min_key,
            max_key,
            file_size,
            bloom_filter: None, // could store in footer later
            index: SsTableIndex {
                entries: index_entries,
                block_size,
            },
            created_at,
            num_entries,
        };

        // Validate data section to avoid crashing later on malformed files.
        if let Err(e) = Self::validate_data(&table) {
            return Err(monodb_common::MonoError::Storage(format!(
                "Invalid SSTable {:?}: {}",
                &table.path, e
            )));
        }

        Ok(table)
    }

    fn validate_data(table: &SsTable) -> std::io::Result<()> {
        let iter = SsTableIterator::new(table)?;
        for item in iter {
            item?; // propagate read errors
        }
        Ok(())
    }
}

// ------------ entry read helpers ------------

fn read_footer(file: &Arc<File>) -> std::io::Result<Option<(u64, u32)>> {
    let metadata = file.metadata()?;
    if metadata.len() < SST_FOOTER_BYTES {
        return Ok(None);
    }

    let mut tail = [0u8; SST_FOOTER_BYTES as usize];
    let mut reader = file.as_ref().try_clone()?;
    reader.seek(SeekFrom::End(-(SST_FOOTER_BYTES as i64)))?;
    reader.read_exact(&mut tail)?;

    let magic = u32::from_le_bytes(tail[12..16].try_into().unwrap());
    if magic != SST_FOOTER_MAGIC {
        return Ok(None);
    }

    let index_offset = u64::from_le_bytes(tail[0..8].try_into().unwrap());
    let entry_count = u32::from_le_bytes(tail[8..12].try_into().unwrap());

    // Sanity checks: index must live before the footer and after the header,
    // and entry_count must fit in the remaining bytes.
    let file_len = metadata.len();
    if index_offset < SST_HEADER_BYTES || index_offset + SST_FOOTER_BYTES > file_len {
        return Ok(None);
    }
    let remaining = file_len - index_offset - SST_FOOTER_BYTES;
    // Each index entry is key_len(u32) + key bytes + offset(u64); we only
    // enforce the minimal fixed part here.
    let min_per_entry = 4u64 + 8u64;
    if (entry_count as u64) * min_per_entry > remaining {
        return Ok(None);
    }

    Ok(Some((index_offset, entry_count)))
}

#[allow(dead_code)]
pub fn read_entry_key<R: Read>(reader: &mut std::io::BufReader<R>) -> std::io::Result<Vec<u8>> {
    let mut key_len = [0u8; 8];
    reader.read_exact(&mut key_len)?;
    let key_len = u64::from_le_bytes(key_len);
    if key_len > MAX_KEY_LEN {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "key length exceeds sanity limit",
        ));
    }
    let mut key = vec![0u8; key_len as usize];
    reader.read_exact(&mut key)?;
    Ok(key)
}

#[allow(dead_code)]
pub fn read_entry_value<R: Read>(reader: &mut std::io::BufReader<R>) -> std::io::Result<Vec<u8>> {
    let mut value_len = [0u8; 8];
    reader.read_exact(&mut value_len)?;
    let value_len = u64::from_le_bytes(value_len);
    if value_len > MAX_VALUE_LEN {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "value length exceeds sanity limit",
        ));
    }
    let mut value = vec![0u8; value_len as usize];
    reader.read_exact(&mut value)?;
    Ok(value)
}

#[allow(dead_code)]
pub fn skip_entry_value<R: Read>(reader: &mut std::io::BufReader<R>) -> std::io::Result<()> {
    let mut value_len = [0u8; 8];
    reader.read_exact(&mut value_len)?;
    let value_len = u64::from_le_bytes(value_len);
    let mut buffer = vec![0u8; value_len as usize];
    reader.read_exact(&mut buffer)?;
    Ok(())
}

pub fn is_tombstone(value: &[u8]) -> bool {
    value.is_empty()
}

//! SSTable file format and helpers.
//!
//! Index offsets are relative to the first entry byte (i.e., after the header).

use crate::storage::lsm::bloom_filter::BloomFilter;
use monodb_common::Result;
use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

const SST_MAGIC: u32 = 0x53535431; // "SST1"
const SST_HEADER_BYTES: u64 = 16;

static SSTABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct SsTable {
    pub level: usize,
    pub path: PathBuf,
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
    // iterator starts positioned at first entry after header
}

impl SsTableIterator {
    /// Create an iterator positioned at the first entry after the header.
    pub fn new(path: &std::path::Path) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
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
        })
    }
}

impl Iterator for SsTableIterator {
    type Item = std::io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;

        match read_entry_key(reader) {
            Ok(key) => match read_entry_value(reader) {
                Ok(value) => Some(Ok((key, value))),
                Err(e) => {
                    self.reader = None;
                    Some(Err(e))
                }
            },
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                self.reader = None;
                None
            }
            Err(e) => {
                self.reader = None;
                Some(Err(e))
            }
        }
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

        // Single write to disk
        tokio::fs::write(&output_file, &file_bytes).await?;
        let file_size = file_bytes.len();

        Ok(SsTable {
            level: self.level,
            path: output_file,
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

        let file = std::fs::File::open(&self.path).unwrap();
        let mut reader = std::io::BufReader::new(file);
        // seek to entry area: header + relative offset
        reader.seek(SeekFrom::Start(SST_HEADER_BYTES + rel_start))?;

        loop {
            let key_from_file = match read_entry_key(&mut reader) {
                Ok(key) => key,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };

            if key_from_file.as_slice() > key {
                break;
            }

            if key_from_file.as_slice() == key {
                let value = read_entry_value(&mut reader)?;
                return Ok(Some(value));
            } else {
                skip_entry_value(&mut reader)?;
            }
        }

        Ok(None)
    }

    /// Load SSTable metadata from disk. Reads header for level and block size.
    pub fn load_metadata(path: PathBuf) -> Result<Self> {
        let metadata = std::fs::metadata(&path)?;
        let file_size = metadata.len() as usize;

        // read header
        let mut file = std::fs::File::open(&path)?;
        let mut header = [0u8; SST_HEADER_BYTES as usize];
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != SST_MAGIC {
            return Err(monodb_common::MonoError::Storage(
                "Invalid SSTable magic".into(),
            ));
        }

        let level = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let block_size = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        // reserved = header[12..16]

        // scan entries to get min/max and count
        let mut min_key = Vec::new();
        let mut max_key = Vec::new();
        let mut num_entries = 0;

        if let Ok(mut iter) = SsTableIterator::new(&path)
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

        Ok(SsTable {
            level,
            path,
            min_key,
            max_key,
            file_size,
            bloom_filter: None, // could store in footer later
            index: SsTableIndex {
                entries: Vec::new(), // not loaded for metadata-only path
                block_size,
            },
            created_at,
            num_entries,
        })
    }
}

// ------------ entry read helpers ------------

pub fn read_entry_key<R: Read>(reader: &mut std::io::BufReader<R>) -> std::io::Result<Vec<u8>> {
    let mut key_len = [0u8; 8];
    reader.read_exact(&mut key_len)?;
    let key_len = u64::from_le_bytes(key_len);
    let mut key = vec![0u8; key_len as usize];
    reader.read_exact(&mut key)?;
    Ok(key)
}

pub fn read_entry_value<R: Read>(reader: &mut std::io::BufReader<R>) -> std::io::Result<Vec<u8>> {
    let mut value_len = [0u8; 8];
    reader.read_exact(&mut value_len)?;
    let value_len = u64::from_le_bytes(value_len);
    let mut value = vec![0u8; value_len as usize];
    reader.read_exact(&mut value)?;
    Ok(value)
}

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

/// Reverse iterator for SSTable, iterates entries from end to start.
/// Uses the sparse index to load blocks in reverse order.
pub struct ReverseSsTableIterator {
    path: PathBuf,
    /// All entries loaded from the SSTable (we need to load all for reverse)
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Current position (starts at end, decrements)
    position: usize,
}

impl ReverseSsTableIterator {
    /// Create a reverse iterator for entries in the given key range.
    /// Returns entries from max_key down to min_key.
    pub fn new(
        path: &std::path::Path,
        start_key: &[u8],
        end_key: &[u8],
    ) -> std::io::Result<Self> {
        // Load all entries in range, then we'll iterate in reverse
        let mut entries = Vec::new();
        let iter = SsTableIterator::new(path)?;

        for result in iter {
            match result {
                Ok((key, value)) => {
                    if key.as_slice() >= start_key && key.as_slice() <= end_key {
                        entries.push((key, value));
                    }
                }
                Err(e) => return Err(e),
            }
        }

        let position = entries.len();
        Ok(Self {
            path: path.to_path_buf(),
            entries,
            position,
        })
    }

    /// Create a reverse iterator starting from keys <= upper_bound.
    /// This is optimized for DESC queries, seeks to end and works backwards.
    pub fn from_upper_bound(
        path: &std::path::Path,
        upper_bound: &[u8],
        index: &SsTableIndex,
    ) -> std::io::Result<Self> {
        // Find the block that might contain our upper bound
        let block_index = index
            .entries
            .partition_point(|entry| entry.key.as_slice() <= upper_bound);

        // Load entries from the relevant blocks
        let mut entries = Vec::new();

        // We need to scan from the block before our target to the end
        let start_block = if block_index > 0 { block_index - 1 } else { 0 };

        let mut file = File::open(path)?;
        let mut header = [0u8; SST_HEADER_BYTES as usize];
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != SST_MAGIC {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "bad SSTable magic",
            ));
        }

        // Seek to the start of our target block
        let start_offset = if start_block < index.entries.len() {
            index.entries[start_block].offset
        } else {
            0
        };

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(SST_HEADER_BYTES + start_offset))?;

        // Read entries until we're past upper_bound
        loop {
            match read_entry_key(&mut reader) {
                Ok(key) => {
                    let value = read_entry_value(&mut reader)?;
                    if key.as_slice() <= upper_bound {
                        entries.push((key, value));
                    } else {
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        let position = entries.len();
        Ok(Self {
            path: path.to_path_buf(),
            entries,
            position,
        })
    }
}

impl Iterator for ReverseSsTableIterator {
    type Item = std::io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == 0 {
            return None;
        }
        self.position -= 1;
        let (key, value) = self.entries[self.position].clone();
        Some(Ok((key, value)))
    }
}

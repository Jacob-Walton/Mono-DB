//! Core storage traits for extensibility.
//!
//! These traits define the interfaces for the storage engine components,
//! allowing different implementations (e.g., B+Tree vs LSM-Tree, different
//! buffer pool strategies, etc.) to be swapped in without changing the
//! higher-level code.

use std::ops::RangeBounds;

use monodb_common::Result;

use super::page::PageId;

// Serialization Traits

/// Trait for types that can be serialized to/from bytes.
///
/// Used for keys and values in B+Trees and other structures.
/// Implementations should support efficient, variable-length encoding.
pub trait Serializable: Sized + Clone {
    /// Serialize this value, appending bytes to the buffer.
    fn serialize(&self, buf: &mut Vec<u8>);

    /// Deserialize from bytes, returning (value, bytes_consumed).
    fn deserialize(buf: &[u8]) -> Result<(Self, usize)>;

    /// Get the serialized size in bytes without actually serializing.
    fn serialized_size(&self) -> usize;

    /// Optional: fixed size if known at compile time.
    /// Returns None for variable-length types.
    fn fixed_size() -> Option<usize> {
        None
    }
}

// Primitive Implementations

impl Serializable for u32 {
    #[inline]
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    #[inline]
    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 4 {
            return Err(monodb_common::MonoError::Storage(
                "insufficient bytes for u32".into(),
            ));
        }
        Ok((u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]), 4))
    }

    #[inline]
    fn serialized_size(&self) -> usize {
        4
    }

    #[inline]
    fn fixed_size() -> Option<usize> {
        Some(4)
    }
}

impl Serializable for u64 {
    #[inline]
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    #[inline]
    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 8 {
            return Err(monodb_common::MonoError::Storage(
                "insufficient bytes for u64".into(),
            ));
        }
        Ok((
            u64::from_le_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]),
            8,
        ))
    }

    #[inline]
    fn serialized_size(&self) -> usize {
        8
    }

    #[inline]
    fn fixed_size() -> Option<usize> {
        Some(8)
    }
}

impl Serializable for i64 {
    #[inline]
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    #[inline]
    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 8 {
            return Err(monodb_common::MonoError::Storage(
                "insufficient bytes for i64".into(),
            ));
        }
        Ok((
            i64::from_le_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]),
            8,
        ))
    }

    #[inline]
    fn serialized_size(&self) -> usize {
        8
    }

    #[inline]
    fn fixed_size() -> Option<usize> {
        Some(8)
    }
}

/// Variable-length byte slice serialization with length prefix.
impl Serializable for Vec<u8> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        // Length prefix as varint
        encode_varint(self.len() as u64, buf);
        buf.extend_from_slice(self);
    }

    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        let (len, varint_size) = decode_varint(buf)?;
        let len = len as usize;
        if buf.len() < varint_size + len {
            return Err(monodb_common::MonoError::Storage(
                "insufficient bytes for Vec<u8>".into(),
            ));
        }
        let data = buf[varint_size..varint_size + len].to_vec();
        Ok((data, varint_size + len))
    }

    fn serialized_size(&self) -> usize {
        varint_size(self.len() as u64) + self.len()
    }
}

impl Serializable for String {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.as_bytes().to_vec().serialize(buf);
    }

    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        let (bytes, consumed) = Vec::<u8>::deserialize(buf)?;
        let s = String::from_utf8(bytes)
            .map_err(|e| monodb_common::MonoError::Storage(format!("invalid UTF-8: {}", e)))?;
        Ok((s, consumed))
    }

    fn serialized_size(&self) -> usize {
        varint_size(self.len() as u64) + self.len()
    }
}

// Varint Encoding (LEB128-style)

/// Encode a u64 as a variable-length integer.
#[inline]
pub fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a variable-length integer, returning (value, bytes_consumed).
#[inline]
pub fn decode_varint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in buf.iter().enumerate() {
        if i >= 10 {
            return Err(monodb_common::MonoError::Storage("varint too long".into()));
        }
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(monodb_common::MonoError::Storage(
        "incomplete varint".into(),
    ))
}

/// Calculate the size of a varint without encoding it.
#[inline]
pub fn varint_size(value: u64) -> usize {
    match value {
        0..=0x7F => 1,
        0x80..=0x3FFF => 2,
        0x4000..=0x1F_FFFF => 3,
        0x20_0000..=0x0FFF_FFFF => 4,
        0x1000_0000..=0x07_FFFF_FFFF => 5,
        0x08_0000_0000..=0x03FF_FFFF_FFFF => 6,
        0x0400_0000_0000..=0x01_FFFF_FFFF_FFFF => 7,
        0x02_0000_0000_0000..=0x00FF_FFFF_FFFF_FFFF => 8,
        0x0100_0000_0000_0000..=0x7FFF_FFFF_FFFF_FFFF => 9,
        _ => 10,
    }
}

// Page Store Trait

/// Low-level page I/O abstraction.
///
/// Implementations can use memory-mapped files, standard file I/O, or
/// in-memory storage for testing.
pub trait PageStore: Send + Sync {
    /// Read a page from storage.
    fn read(&self, page_id: PageId) -> Result<Vec<u8>>;

    /// Write a page to storage.
    fn write(&self, page_id: PageId, data: &[u8]) -> Result<()>;

    /// Allocate a new page, returning its ID.
    fn allocate(&self) -> Result<PageId>;

    /// Free a page for reuse.
    fn free(&self, page_id: PageId) -> Result<()>;

    /// Sync all pages to durable storage.
    fn sync(&self) -> Result<()>;

    /// Get the total number of pages.
    fn page_count(&self) -> u64;
}

// Buffer Pool Trait

/// Page caching layer with eviction.
pub trait BufferPool: Send + Sync {
    /// Get a page, loading from disk if necessary.
    fn get(&self, page_id: PageId) -> Result<Vec<u8>>;

    /// Get a mutable reference to a page (marks as dirty).
    fn get_mut(&self, page_id: PageId) -> Result<Vec<u8>>;

    /// Pin a page to prevent eviction.
    fn pin(&self, page_id: PageId) -> Result<()>;

    /// Unpin a page, allowing eviction.
    fn unpin(&self, page_id: PageId) -> Result<()>;

    /// Mark a page as dirty.
    fn mark_dirty(&self, page_id: PageId) -> Result<()>;

    /// Write a page back to the buffer (and mark dirty).
    fn put(&self, page_id: PageId, data: Vec<u8>) -> Result<()>;

    /// Flush all dirty pages to disk.
    fn flush(&self) -> Result<()>;

    /// Flush a specific page to disk.
    fn flush_page(&self, page_id: PageId) -> Result<()>;
}

// Tree Trait

/// Generic ordered key-value tree operations.
pub trait Tree<K, V>: Send + Sync
where
    K: Ord + Serializable,
    V: Serializable,
{
    /// Insert a key-value pair. Returns the old value if the key existed.
    fn insert(&self, key: K, value: V) -> Result<Option<V>>;

    /// Get the value for a key.
    fn get(&self, key: &K) -> Result<Option<V>>;

    /// Delete a key. Returns the old value if it existed.
    fn delete(&self, key: &K) -> Result<Option<V>>;

    /// Check if a key exists.
    fn contains(&self, key: &K) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Range scan, returning all key-value pairs in the range.
    fn range<R: RangeBounds<K>>(&self, range: R) -> Result<Vec<(K, V)>>;

    /// Get the minimum key-value pair.
    fn min(&self) -> Result<Option<(K, V)>>;

    /// Get the maximum key-value pair.
    fn max(&self) -> Result<Option<(K, V)>>;

    /// Get the number of entries.
    fn len(&self) -> usize;

    /// Check if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// MVCC Store Trait (for relational tables)

/// Transaction ID type.
pub type TxId = u64;

/// Timestamp type for MVCC.
pub type Timestamp = u64;

/// MVCC visibility information for a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionInfo {
    /// Transaction that created this version.
    pub xmin: TxId,
    /// Transaction that deleted/superseded this version (0 if active).
    pub xmax: TxId,
}

impl VersionInfo {
    /// Create a new version created by the given transaction.
    pub fn new(creator: TxId) -> Self {
        Self {
            xmin: creator,
            xmax: 0,
        }
    }

    /// Mark this version as deleted by a transaction.
    pub fn mark_deleted(&mut self, deleter: TxId) {
        self.xmax = deleter;
    }

    /// Check if this version is still active (not deleted).
    pub fn is_active(&self) -> bool {
        self.xmax == 0
    }
}

/// MVCC-enabled storage for relational tables.
///
/// Provides snapshot isolation and serializable transactions.
pub trait MvccStore<K, V>: Send + Sync
where
    K: Ord + Serializable,
    V: Serializable,
{
    /// Read a value as seen by the given transaction.
    fn read(&self, tx_id: TxId, key: &K) -> Result<Option<V>>;

    /// Write a value in the context of a transaction.
    fn write(&self, tx_id: TxId, key: K, value: V) -> Result<()>;

    /// Delete a value in the context of a transaction.
    fn delete(&self, tx_id: TxId, key: &K) -> Result<bool>;

    /// Begin a new transaction with the given isolation level.
    fn begin(&self, isolation: super::IsolationLevel) -> Result<TxId>;

    /// Commit a transaction.
    fn commit(&self, tx_id: TxId) -> Result<()>;

    /// Rollback a transaction.
    fn rollback(&self, tx_id: TxId) -> Result<()>;
}

// Versioned Store Trait (for documents)

/// Revision identifier for document versioning.
pub type Revision = u64;

/// Document with revision metadata.
#[derive(Debug, Clone)]
pub struct VersionedDocument<V> {
    /// The document data.
    pub data: V,
    /// Current revision number.
    pub revision: Revision,
    /// Whether this document is deleted (tombstone).
    pub deleted: bool,
}

/// Simple versioned storage for documents (not full MVCC).
///
/// Uses optimistic concurrency with revision checking.
pub trait VersionedStore<K, V>: Send + Sync
where
    K: Ord + Serializable,
    V: Serializable,
{
    /// Get the current version of a document.
    fn get(&self, key: &K) -> Result<Option<VersionedDocument<V>>>;

    /// Insert a new document. Fails if the key already exists.
    fn insert(&self, key: K, value: V) -> Result<Revision>;

    /// Update a document. Fails if the revision doesn't match.
    fn update(&self, key: &K, value: V, expected_rev: Revision) -> Result<Revision>;

    /// Delete a document. Fails if the revision doesn't match.
    fn delete(&self, key: &K, expected_rev: Revision) -> Result<bool>;

    /// Get all versions of a document (for conflict resolution).
    fn get_history(&self, key: &K, limit: usize) -> Result<Vec<VersionedDocument<V>>>;
}

// Storage Backend Trait

/// High-level storage backend interface.
///
/// Query engine interface providing unified API over storage types
/// (relational, document, keyspace).
pub trait StorageBackend: Send + Sync {
    /// Get the name of this backend (for logging/debugging).
    fn name(&self) -> &str;

    /// Check if this backend is healthy.
    fn is_healthy(&self) -> bool;

    /// Flush all pending writes to durable storage.
    fn flush(&self) -> Result<()>;

    /// Get storage statistics.
    fn stats(&self) -> StorageStats;
}

/// Storage statistics.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total number of pages allocated.
    pub pages_allocated: u64,
    /// Number of pages in the buffer pool.
    pub pages_cached: u64,
    /// Number of dirty pages.
    pub pages_dirty: u64,
    /// Total bytes on disk.
    pub disk_bytes: u64,
    /// Number of active transactions (for MVCC).
    pub active_transactions: u64,
    /// Number of documents/rows stored.
    pub record_count: u64,
}

// Re-export IsolationLevel at module level for convenience
pub use monodb_common::protocol::IsolationLevel;

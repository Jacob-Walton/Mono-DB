//! Simple key-value keyspace storage.
//!
//! Provides a lightweight key-value store without versioning overhead.
//! Supports optional TTL (time-to-live) for ephemeral data and can be
//! backed by either disk or memory.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use monodb_common::Result;
use parking_lot::Mutex;

use super::btree::BTree;
use super::buffer::LruBufferPool;
use super::traits::Serializable;

// TTL Entry

/// Value with optional TTL metadata.
#[derive(Debug, Clone)]
pub struct TtlEntry<V> {
    /// The value.
    pub value: V,
    /// Expiration timestamp (milliseconds since epoch). 0 = no expiration.
    pub expires_at: u64,
}

impl<V> TtlEntry<V> {
    /// Create an entry without TTL.
    pub fn new(value: V) -> Self {
        Self {
            value,
            expires_at: 0,
        }
    }

    /// Create an entry with TTL.
    pub fn with_ttl(value: V, ttl_ms: u64) -> Self {
        let expires_at = current_time_ms() + ttl_ms;
        Self { value, expires_at }
    }

    /// Check if this entry has expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at > 0 && current_time_ms() > self.expires_at
    }

    /// Get remaining TTL in milliseconds, or None if no TTL.
    pub fn ttl_remaining(&self) -> Option<u64> {
        if self.expires_at == 0 {
            return None;
        }
        let now = current_time_ms();
        if now >= self.expires_at {
            Some(0)
        } else {
            Some(self.expires_at - now)
        }
    }
}

impl<V: Serializable> Serializable for TtlEntry<V> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.expires_at.serialize(buf);
        self.value.serialize(buf);
    }

    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        let mut offset = 0;

        let (expires_at, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (value, consumed) = V::deserialize(&buf[offset..])?;
        offset += consumed;

        Ok((Self { value, expires_at }, offset))
    }

    fn serialized_size(&self) -> usize {
        8 + self.value.serialized_size()
    }
}

/// Get current time in milliseconds since epoch.
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// Keyspace Backend Trait

/// Backend trait for keyspace storage.
pub trait KeyspaceBackend<K, V>: Send + Sync {
    /// Get a value by key.
    fn get(&self, key: &K) -> Result<Option<V>>;

    /// Set a key-value pair.
    fn set(&self, key: K, value: V) -> Result<()>;

    /// Set a key-value pair with TTL.
    fn set_with_ttl(&self, key: K, value: V, ttl_ms: u64) -> Result<()>;

    /// Delete a key.
    fn delete(&self, key: &K) -> Result<bool>;

    /// Check if a key exists.
    fn exists(&self, key: &K) -> Result<bool>;

    /// Get all keys (for iteration).
    fn keys(&self) -> Result<Vec<K>>;

    /// Clear all keys.
    fn clear(&self) -> Result<()>;

    /// Get the number of keys.
    fn len(&self) -> usize;

    /// Check if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// Memory Keyspace

/// In-memory keyspace using DashMap for concurrent access.
pub struct MemoryKeyspace<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    data: DashMap<K, TtlEntry<V>>,
    last_cleanup: Mutex<Instant>,
    cleanup_interval: Duration,
}

impl<K, V> MemoryKeyspace<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Create a new in-memory keyspace.
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            last_cleanup: Mutex::new(Instant::now()),
            cleanup_interval: Duration::from_secs(60),
        }
    }

    /// Create with custom cleanup interval.
    pub fn with_cleanup_interval(interval: Duration) -> Self {
        Self {
            data: DashMap::new(),
            last_cleanup: Mutex::new(Instant::now()),
            cleanup_interval: interval,
        }
    }

    /// Maybe run cleanup of expired entries.
    fn maybe_cleanup(&self) {
        let should_cleanup = {
            let last = self.last_cleanup.lock();
            last.elapsed() > self.cleanup_interval
        };

        if should_cleanup {
            self.cleanup();
        }
    }

    /// Remove all expired entries.
    pub fn cleanup(&self) -> usize {
        let mut removed = 0;
        self.data.retain(|_, entry| {
            if entry.is_expired() {
                removed += 1;
                false
            } else {
                true
            }
        });

        *self.last_cleanup.lock() = Instant::now();
        removed
    }

    /// Get remaining TTL for a key.
    pub fn ttl(&self, key: &K) -> Option<u64> {
        self.data.get(key).and_then(|entry| entry.ttl_remaining())
    }

    /// Extend TTL for a key.
    pub fn expire(&self, key: &K, ttl_ms: u64) -> Result<bool> {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.expires_at = current_time_ms() + ttl_ms;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Remove TTL from a key (make persistent).
    pub fn persist(&self, key: &K) -> Result<bool> {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.expires_at = 0;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<K, V> Default for MemoryKeyspace<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> KeyspaceBackend<K, V> for MemoryKeyspace<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    fn get(&self, key: &K) -> Result<Option<V>> {
        self.maybe_cleanup();

        if let Some(entry) = self.data.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.data.remove(key);
                return Ok(None);
            }
            return Ok(Some(entry.value.clone()));
        }
        Ok(None)
    }

    fn set(&self, key: K, value: V) -> Result<()> {
        self.maybe_cleanup();
        self.data.insert(key, TtlEntry::new(value));
        Ok(())
    }

    fn set_with_ttl(&self, key: K, value: V, ttl_ms: u64) -> Result<()> {
        self.maybe_cleanup();
        self.data.insert(key, TtlEntry::with_ttl(value, ttl_ms));
        Ok(())
    }

    fn delete(&self, key: &K) -> Result<bool> {
        Ok(self.data.remove(key).is_some())
    }

    fn exists(&self, key: &K) -> Result<bool> {
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.data.remove(key);
                return Ok(false);
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn keys(&self) -> Result<Vec<K>> {
        self.maybe_cleanup();
        Ok(self.data.iter().map(|e| e.key().clone()).collect())
    }

    fn clear(&self) -> Result<()> {
        self.data.clear();
        Ok(())
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

// Disk Keyspace

/// Disk-backed keyspace using B+Tree storage.
pub struct DiskKeyspace<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    tree: BTree<K, TtlEntry<V>>,
    count: AtomicU64,
}

impl<K, V> DiskKeyspace<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    /// Create a new disk-backed keyspace.
    pub fn new(pool: Arc<LruBufferPool>) -> Result<Self> {
        let tree = BTree::new(pool)?;
        let count = tree.len() as u64;

        Ok(Self {
            tree,
            count: AtomicU64::new(count),
        })
    }

    /// Open an existing disk-backed keyspace.
    pub fn open(pool: Arc<LruBufferPool>, meta_page_id: crate::storage::page::PageId) -> Result<Self> {
        let tree = BTree::open(pool, meta_page_id)?;
        let count = tree.len() as u64;

        Ok(Self {
            tree,
            count: AtomicU64::new(count),
        })
    }

    /// Get the metadata page ID.
    pub fn meta_page_id(&self) -> crate::storage::page::PageId {
        self.tree.meta_page_id()
    }

    /// Get remaining TTL for a key.
    pub fn ttl(&self, key: &K) -> Result<Option<u64>> {
        if let Some(entry) = self.tree.get(key)? {
            Ok(entry.ttl_remaining())
        } else {
            Ok(None)
        }
    }

    /// Extend TTL for a key.
    pub fn expire(&self, key: &K, ttl_ms: u64) -> Result<bool> {
        if let Some(mut entry) = self.tree.get(key)? {
            entry.expires_at = current_time_ms() + ttl_ms;
            self.tree.insert(key.clone(), entry)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Remove expired entries.
    pub fn cleanup(&self) -> Result<usize> {
        let mut removed = 0;
        let now = current_time_ms();

        // Collect expired keys
        let expired: Vec<K> = self
            .tree
            .iter()?
            .filter_map(|(key, entry)| {
                if entry.expires_at > 0 && now > entry.expires_at {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();

        // Delete them
        for key in expired {
            self.tree.delete(&key)?;
            removed += 1;
        }

        self.count.fetch_sub(removed as u64, Ordering::Relaxed);
        Ok(removed)
    }

    /// Flush to disk.
    pub fn flush(&self) -> Result<()> {
        self.tree.flush()
    }
}

impl<K, V> KeyspaceBackend<K, V> for DiskKeyspace<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    fn get(&self, key: &K) -> Result<Option<V>> {
        if let Some(entry) = self.tree.get(key)? {
            if entry.is_expired() {
                // Lazy deletion
                self.tree.delete(key)?;
                self.count.fetch_sub(1, Ordering::Relaxed);
                return Ok(None);
            }
            return Ok(Some(entry.value));
        }
        Ok(None)
    }

    fn set(&self, key: K, value: V) -> Result<()> {
        let is_new = self.tree.get(&key)?.is_none();
        self.tree.insert(key, TtlEntry::new(value))?;
        if is_new {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn set_with_ttl(&self, key: K, value: V, ttl_ms: u64) -> Result<()> {
        let is_new = self.tree.get(&key)?.is_none();
        self.tree.insert(key, TtlEntry::with_ttl(value, ttl_ms))?;
        if is_new {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn delete(&self, key: &K) -> Result<bool> {
        if self.tree.delete(key)?.is_some() {
            self.count.fetch_sub(1, Ordering::Relaxed);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn exists(&self, key: &K) -> Result<bool> {
        if let Some(entry) = self.tree.get(key)? {
            if entry.is_expired() {
                self.tree.delete(key)?;
                self.count.fetch_sub(1, Ordering::Relaxed);
                return Ok(false);
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn keys(&self) -> Result<Vec<K>> {
        let now = current_time_ms();
        Ok(self
            .tree
            .iter()?
            .filter_map(|(key, entry)| {
                if entry.expires_at == 0 || now <= entry.expires_at {
                    Some(key)
                } else {
                    None
                }
            })
            .collect())
    }

    fn clear(&self) -> Result<()> {
        // Delete all keys
        let keys: Vec<K> = self.tree.iter()?.map(|(k, _)| k).collect();
        for key in keys {
            self.tree.delete(&key)?;
        }
        self.count.store(0, Ordering::Relaxed);
        Ok(())
    }

    fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed) as usize
    }
}

// Unified Keyspace

/// A unified keyspace that can be either memory-backed or disk-backed.
pub enum Keyspace<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync + Eq + std::hash::Hash,
    V: Clone + Serializable + Send + Sync,
{
    Memory(MemoryKeyspace<K, V>),
    Disk(DiskKeyspace<K, V>),
}

impl<K, V> Keyspace<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync + Eq + std::hash::Hash,
    V: Clone + Serializable + Send + Sync,
{
    /// Create a memory-backed keyspace.
    pub fn memory() -> Self {
        Keyspace::Memory(MemoryKeyspace::new())
    }

    /// Create a disk-backed keyspace.
    pub fn disk(pool: Arc<LruBufferPool>) -> Result<Self> {
        Ok(Keyspace::Disk(DiskKeyspace::new(pool)?))
    }

    /// Get a value.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        match self {
            Keyspace::Memory(ks) => ks.get(key),
            Keyspace::Disk(ks) => ks.get(key),
        }
    }

    /// Set a value.
    pub fn set(&self, key: K, value: V) -> Result<()> {
        match self {
            Keyspace::Memory(ks) => ks.set(key, value),
            Keyspace::Disk(ks) => ks.set(key, value),
        }
    }

    /// Set a value with TTL.
    pub fn set_with_ttl(&self, key: K, value: V, ttl_ms: u64) -> Result<()> {
        match self {
            Keyspace::Memory(ks) => ks.set_with_ttl(key, value, ttl_ms),
            Keyspace::Disk(ks) => ks.set_with_ttl(key, value, ttl_ms),
        }
    }

    /// Delete a key.
    pub fn delete(&self, key: &K) -> Result<bool> {
        match self {
            Keyspace::Memory(ks) => ks.delete(key),
            Keyspace::Disk(ks) => ks.delete(key),
        }
    }

    /// Check if a key exists.
    pub fn exists(&self, key: &K) -> Result<bool> {
        match self {
            Keyspace::Memory(ks) => ks.exists(key),
            Keyspace::Disk(ks) => ks.exists(key),
        }
    }

    /// Get all keys.
    pub fn keys(&self) -> Result<Vec<K>> {
        match self {
            Keyspace::Memory(ks) => ks.keys(),
            Keyspace::Disk(ks) => ks.keys(),
        }
    }

    /// Clear all keys.
    pub fn clear(&self) -> Result<()> {
        match self {
            Keyspace::Memory(ks) => ks.clear(),
            Keyspace::Disk(ks) => ks.clear(),
        }
    }

    /// Get the number of keys.
    pub fn len(&self) -> usize {
        match self {
            Keyspace::Memory(ks) => ks.len(),
            Keyspace::Disk(ks) => ks.len(),
        }
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get remaining TTL for a key.
    pub fn ttl(&self, key: &K) -> Result<Option<u64>> {
        match self {
            Keyspace::Memory(ks) => Ok(ks.ttl(key)),
            Keyspace::Disk(ks) => ks.ttl(key),
        }
    }

    /// Cleanup expired entries.
    pub fn cleanup(&self) -> Result<usize> {
        match self {
            Keyspace::Memory(ks) => Ok(ks.cleanup()),
            Keyspace::Disk(ks) => ks.cleanup(),
        }
    }

    /// Get the metadata page ID (only for disk keyspaces).
    pub fn meta_page_id(&self) -> Option<crate::storage::page::PageId> {
        match self {
            Keyspace::Memory(_) => None,
            Keyspace::Disk(ks) => Some(ks.meta_page_id()),
        }
    }

    /// Open an existing disk-backed keyspace.
    pub fn open_disk(pool: Arc<LruBufferPool>, meta_page_id: crate::storage::page::PageId) -> Result<Self> {
        Ok(Keyspace::Disk(DiskKeyspace::open(pool, meta_page_id)?))
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::DiskManager;
    use tempfile::tempdir;

    #[test]
    fn test_memory_keyspace_basic() {
        let ks = MemoryKeyspace::<String, String>::new();

        ks.set("key1".to_string(), "value1".to_string()).unwrap();
        assert_eq!(
            ks.get(&"key1".to_string()).unwrap(),
            Some("value1".to_string())
        );

        ks.delete(&"key1".to_string()).unwrap();
        assert_eq!(ks.get(&"key1".to_string()).unwrap(), None);
    }

    #[test]
    fn test_memory_keyspace_ttl() {
        let ks = MemoryKeyspace::<String, String>::new();

        // Set with very short TTL
        ks.set_with_ttl("key".to_string(), "value".to_string(), 1)
            .unwrap();

        // Should exist initially
        assert!(ks.exists(&"key".to_string()).unwrap());

        // Wait for expiration
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should be expired now
        assert!(!ks.exists(&"key".to_string()).unwrap());
    }

    #[test]
    fn test_disk_keyspace_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("keyspace.db");
        let disk = Arc::new(DiskManager::open(&path).unwrap());
        let pool = Arc::new(LruBufferPool::new(disk, 1000));

        let ks = DiskKeyspace::<String, String>::new(pool).unwrap();

        ks.set("key1".to_string(), "value1".to_string()).unwrap();
        assert_eq!(
            ks.get(&"key1".to_string()).unwrap(),
            Some("value1".to_string())
        );

        ks.delete(&"key1".to_string()).unwrap();
        assert_eq!(ks.get(&"key1".to_string()).unwrap(), None);
    }

    #[test]
    fn test_unified_keyspace() {
        // Memory
        let mem_ks = Keyspace::<String, String>::memory();
        mem_ks.set("key".to_string(), "value".to_string()).unwrap();
        assert_eq!(
            mem_ks.get(&"key".to_string()).unwrap(),
            Some("value".to_string())
        );

        // Disk
        let dir = tempdir().unwrap();
        let path = dir.path().join("unified.db");
        let disk = Arc::new(DiskManager::open(&path).unwrap());
        let pool = Arc::new(LruBufferPool::new(disk, 1000));

        let disk_ks = Keyspace::<String, String>::disk(pool).unwrap();
        disk_ks.set("key".to_string(), "value".to_string()).unwrap();
        assert_eq!(
            disk_ks.get(&"key".to_string()).unwrap(),
            Some("value".to_string())
        );
    }
}

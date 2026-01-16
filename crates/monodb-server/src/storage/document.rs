//! Document store with optimistic concurrency control.
//!
//! Implements a simple versioned document store using revision numbers
//! for conflict detection. Simpler than full MVCC but suitable for
//! document.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use monodb_common::{MonoError, Result};

use super::btree::BTree;
use super::buffer::LruBufferPool;
use super::page::PageId;
use super::traits::{Revision, Serializable, VersionedDocument, VersionedStore};

// Document Metadata

/// Internal document representation with metadata.
#[derive(Debug, Clone)]
pub struct Document<V> {
    /// Current revision number.
    pub revision: Revision,
    /// Whether this document is deleted (tombstone).
    pub deleted: bool,
    /// Creation timestamp.
    pub created_at: u64,
    /// Last modified timestamp.
    pub updated_at: u64,
    /// The document data.
    pub data: V,
}

impl<V> Document<V> {
    /// Create a new document.
    fn new(data: V, revision: Revision, timestamp: u64) -> Self {
        Self {
            revision,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            data,
        }
    }

    /// Mark as deleted (tombstone).
    fn delete(mut self, revision: Revision, timestamp: u64) -> Self {
        self.revision = revision;
        self.deleted = true;
        self.updated_at = timestamp;
        self
    }

    /// Update the document.
    fn update(mut self, data: V, revision: Revision, timestamp: u64) -> Self {
        self.data = data;
        self.revision = revision;
        self.updated_at = timestamp;
        self
    }
}

impl<V: Serializable> Serializable for Document<V> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.revision.serialize(buf);
        (self.deleted as u8).serialize(buf);
        self.created_at.serialize(buf);
        self.updated_at.serialize(buf);
        self.data.serialize(buf);
    }

    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        let mut offset = 0;

        let (revision, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (deleted_byte, consumed) = u8::deserialize(&buf[offset..])?;
        offset += consumed;
        let deleted = deleted_byte != 0;

        let (created_at, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (updated_at, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (data, consumed) = V::deserialize(&buf[offset..])?;
        offset += consumed;

        Ok((
            Self {
                revision,
                deleted,
                created_at,
                updated_at,
                data,
            },
            offset,
        ))
    }

    fn serialized_size(&self) -> usize {
        8 + 1 + 8 + 8 + self.data.serialized_size()
    }
}

impl Serializable for u8 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(*self);
    }

    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(MonoError::Storage("insufficient bytes for u8".into()));
        }
        Ok((buf[0], 1))
    }

    fn serialized_size(&self) -> usize {
        1
    }

    fn fixed_size() -> Option<usize> {
        Some(1)
    }
}

// Document History Entry

/// A historical version of a document.
#[derive(Debug, Clone)]
pub struct HistoryEntry<V> {
    /// Revision number.
    pub revision: Revision,
    /// Timestamp when this version was created.
    pub timestamp: u64,
    /// Whether this was a deletion.
    pub deleted: bool,
    /// The document data at this revision.
    pub data: V,
}

// Document Store

/// A versioned document store with optimistic concurrency control.
pub struct DocumentStore<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    /// Main storage: key -> current document.
    documents: BTree<K, Document<V>>,
    /// History storage: (key, revision) -> document snapshot.
    /// Only populated if history is enabled.
    history: Option<BTree<Vec<u8>, Document<V>>>,
    /// Global revision counter.
    next_revision: AtomicU64,
    /// Whether to keep document history.
    keep_history: bool,
    /// Maximum history entries per document.
    max_history: usize,
}

impl<K, V> DocumentStore<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    /// Create a new document store.
    pub fn new(pool: Arc<LruBufferPool>, keep_history: bool) -> Result<Self> {
        let history = if keep_history {
            Some(BTree::new(pool.clone())?)
        } else {
            None
        };

        Ok(Self {
            documents: BTree::new(pool)?,
            history,
            next_revision: AtomicU64::new(1),
            keep_history,
            max_history: 100, // Default max history per document
        })
    }

    /// Open an existing document store from disk.
    pub fn open(
        pool: Arc<LruBufferPool>,
        meta_page_id: PageId,
        keep_history: bool,
    ) -> Result<Self> {
        let documents: BTree<K, Document<V>> = BTree::open(pool.clone(), meta_page_id)?;

        // Find the max revision to continue numbering
        let max_rev = documents
            .iter()?
            .map(|(_, doc)| doc.revision)
            .max()
            .unwrap_or(0);

        let history = if keep_history {
            // TODO: store history btree meta_page_id separately
            Some(BTree::new(pool)?)
        } else {
            None
        };

        Ok(Self {
            documents,
            history,
            next_revision: AtomicU64::new(max_rev + 1),
            keep_history,
            max_history: 100,
        })
    }

    /// Get the metadata page ID for persistence.
    pub fn meta_page_id(&self) -> PageId {
        self.documents.meta_page_id()
    }

    /// Create without history tracking.
    pub fn without_history(pool: Arc<LruBufferPool>) -> Result<Self> {
        Self::new(pool, false)
    }

    /// Create with history tracking.
    pub fn with_history(pool: Arc<LruBufferPool>, max_entries: usize) -> Result<Self> {
        let mut store = Self::new(pool, true)?;
        store.max_history = max_entries;
        Ok(store)
    }

    /// Get current timestamp (milliseconds since epoch).
    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Generate a history key from a document key and revision.
    fn history_key(key: &K, revision: Revision) -> Vec<u8> {
        let mut buf = Vec::new();
        key.serialize(&mut buf);
        // Use descending revision order for efficient recent history retrieval
        (u64::MAX - revision).serialize(&mut buf);
        buf
    }

    fn bump_revision(&self, revision: Revision) {
        let next = revision.saturating_add(1);
        self.next_revision.fetch_max(next, Ordering::SeqCst);
    }

    /// Prepare an insert without applying it.
    pub fn prepare_insert(&self, key: &K, value: V) -> Result<Document<V>> {
        if let Some(existing) = self.documents.get(key)?
            && !existing.deleted
        {
            return Err(MonoError::AlreadyExists(
                "document already exists".to_string(),
            ));
        }

        let revision = self.next_revision.fetch_add(1, Ordering::SeqCst);
        let timestamp = Self::now();
        Ok(Document::new(value, revision, timestamp))
    }

    /// Prepare an update without applying it. Returns (new_doc, previous_doc).
    pub fn prepare_update(
        &self,
        key: &K,
        value: V,
        expected_rev: Revision,
    ) -> Result<(Document<V>, Document<V>)> {
        let existing = self
            .documents
            .get(key)?
            .ok_or_else(|| MonoError::NotFound("document not found".into()))?;

        if existing.deleted {
            return Err(MonoError::NotFound("document was deleted".into()));
        }

        if existing.revision != expected_rev {
            return Err(MonoError::WriteConflict(format!(
                "revision mismatch: expected {}, got {}",
                expected_rev, existing.revision
            )));
        }

        let new_revision = self.next_revision.fetch_add(1, Ordering::SeqCst);
        let timestamp = Self::now();
        let updated = existing.clone().update(value, new_revision, timestamp);

        Ok((updated, existing))
    }

    /// Prepare a delete without applying it. Returns (tombstone, previous_doc).
    pub fn prepare_delete(
        &self,
        key: &K,
        expected_rev: Revision,
    ) -> Result<(Document<V>, Document<V>)> {
        let existing = self
            .documents
            .get(key)?
            .ok_or_else(|| MonoError::NotFound("document not found".into()))?;

        if existing.deleted {
            return Err(MonoError::NotFound("document was deleted".into()));
        }

        if existing.revision != expected_rev {
            return Err(MonoError::WriteConflict(format!(
                "revision mismatch: expected {}, got {}",
                expected_rev, existing.revision
            )));
        }

        let new_revision = self.next_revision.fetch_add(1, Ordering::SeqCst);
        let timestamp = Self::now();
        let deleted = existing.clone().delete(new_revision, timestamp);

        Ok((deleted, existing))
    }

    /// Apply an insert prepared by prepare_insert.
    pub fn apply_prepared_insert(&self, key: K, doc: Document<V>) -> Result<()> {
        self.bump_revision(doc.revision);
        self.documents.insert(key, doc)?;
        Ok(())
    }

    /// Apply an update/delete prepared with a previous document.
    pub fn apply_prepared_update(
        &self,
        key: K,
        doc: Document<V>,
        previous: Document<V>,
    ) -> Result<()> {
        if let Some(ref history) = self.history
            && !previous.deleted
        {
            let history_key = Self::history_key(&key, previous.revision);
            history.insert(history_key, previous)?;
        }

        self.bump_revision(doc.revision);
        self.documents.insert(key, doc)?;
        Ok(())
    }

    /// Apply a WAL document write (used by recovery).
    pub fn apply_wal_document(&self, key: K, doc: Document<V>) -> Result<()> {
        if let Some(ref history) = self.history
            && let Some(existing) = self.documents.get(&key)?
            && !existing.deleted
        {
            let history_key = Self::history_key(&key, existing.revision);
            history.insert(history_key, existing)?;
        }

        self.bump_revision(doc.revision);
        self.documents.insert(key, doc)?;
        Ok(())
    }

    /// Get a document by key.
    pub fn get(&self, key: &K) -> Result<Option<VersionedDocument<V>>> {
        match self.documents.get(key)? {
            Some(doc) if !doc.deleted => Ok(Some(VersionedDocument {
                data: doc.data,
                revision: doc.revision,
                deleted: false,
            })),
            Some(doc) if doc.deleted => Ok(Some(VersionedDocument {
                data: doc.data,
                revision: doc.revision,
                deleted: true,
            })),
            _ => Ok(None),
        }
    }

    /// Insert a new document. Fails if the key already exists.
    pub fn insert(&self, key: K, value: V) -> Result<Revision> {
        let doc = self.prepare_insert(&key, value)?;
        let revision = doc.revision;
        self.apply_prepared_insert(key, doc)?;
        Ok(revision)
    }

    /// Update a document. Fails if the revision doesn't match.
    pub fn update(&self, key: &K, value: V, expected_rev: Revision) -> Result<Revision> {
        let (updated, existing) = self.prepare_update(key, value, expected_rev)?;
        let new_revision = updated.revision;
        self.apply_prepared_update(key.clone(), updated, existing)?;
        Ok(new_revision)
    }

    /// Delete a document. Fails if the revision doesn't match.
    pub fn delete(&self, key: &K, expected_rev: Revision) -> Result<bool> {
        let (deleted, existing) = match self.prepare_delete(key, expected_rev) {
            Ok(result) => result,
            Err(MonoError::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        };

        self.apply_prepared_update(key.clone(), deleted, existing)?;
        Ok(true)
    }

    /// Get document history (most recent first).
    pub fn get_history(&self, key: &K, limit: usize) -> Result<Vec<HistoryEntry<V>>> {
        let history = match &self.history {
            Some(h) => h,
            None => return Ok(Vec::new()),
        };

        let start = Self::history_key(key, u64::MAX);
        let end = Self::history_key(key, 0);

        let entries = history.range(start..=end)?;
        let mut results = Vec::with_capacity(limit.min(entries.len()));

        for (_, doc) in entries.into_iter().take(limit) {
            results.push(HistoryEntry {
                revision: doc.revision,
                timestamp: doc.updated_at,
                deleted: doc.deleted,
                data: doc.data,
            });
        }

        Ok(results)
    }

    /// Get a specific revision of a document.
    pub fn get_revision(&self, key: &K, revision: Revision) -> Result<Option<V>> {
        // Check current document first
        if let Some(doc) = self.documents.get(key)?
            && doc.revision == revision
            && !doc.deleted
        {
            return Ok(Some(doc.data));
        }

        // Check history
        if let Some(ref history) = self.history {
            let history_key = Self::history_key(key, revision);
            if let Some(doc) = history.get(&history_key)?
                && !doc.deleted
            {
                return Ok(Some(doc.data));
            }
        }

        Ok(None)
    }

    /// Upsert: insert if not exists, update if exists.
    pub fn upsert(&self, key: K, value: V) -> Result<Revision> {
        match self.documents.get(&key)? {
            Some(existing) if !existing.deleted => self.update(&key, value, existing.revision),
            _ => self.insert(key, value),
        }
    }

    /// Get all non-deleted documents.
    pub fn scan(&self) -> Result<Vec<(K, VersionedDocument<V>)>> {
        let mut results = Vec::new();

        for (key, doc) in self.documents.iter()? {
            if !doc.deleted {
                results.push((
                    key,
                    VersionedDocument {
                        data: doc.data,
                        revision: doc.revision,
                        deleted: false,
                    },
                ));
            }
        }

        Ok(results)
    }

    /// Count non-deleted documents.
    pub fn count(&self) -> Result<usize> {
        let mut count = 0;
        for (_, doc) in self.documents.iter()? {
            if !doc.deleted {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Purge tombstones older than the given age (in milliseconds).
    pub fn purge_tombstones(&self, max_age_ms: u64) -> Result<usize> {
        let cutoff = Self::now().saturating_sub(max_age_ms);
        let mut purged = 0;

        // Collect keys to purge
        let to_purge: Vec<K> = self
            .documents
            .iter()?
            .filter_map(|(key, doc)| {
                if doc.deleted && doc.updated_at < cutoff {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();

        // Delete them
        for key in to_purge {
            self.documents.delete(&key)?;
            purged += 1;
        }

        Ok(purged)
    }

    /// Flush all data to disk.
    pub fn flush(&self) -> Result<()> {
        self.documents.flush()?;
        if let Some(history) = &self.history {
            history.flush()?;
        }
        Ok(())
    }
}

// VersionedStore Implementation

impl<K, V> VersionedStore<K, V> for DocumentStore<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    fn get(&self, key: &K) -> Result<Option<VersionedDocument<V>>> {
        DocumentStore::get(self, key)
    }

    fn insert(&self, key: K, value: V) -> Result<Revision> {
        DocumentStore::insert(self, key, value)
    }

    fn update(&self, key: &K, value: V, expected_rev: Revision) -> Result<Revision> {
        DocumentStore::update(self, key, value, expected_rev)
    }

    fn delete(&self, key: &K, expected_rev: Revision) -> Result<bool> {
        DocumentStore::delete(self, key, expected_rev)
    }

    fn get_history(&self, key: &K, limit: usize) -> Result<Vec<VersionedDocument<V>>> {
        let entries = DocumentStore::get_history(self, key, limit)?;
        Ok(entries
            .into_iter()
            .map(|e| VersionedDocument {
                data: e.data,
                revision: e.revision,
                deleted: e.deleted,
            })
            .collect())
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::DiskManager;
    use tempfile::tempdir;

    fn create_store() -> DocumentStore<String, String> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("docs.db");
        let disk = Arc::new(DiskManager::open(&path).unwrap());
        let pool = Arc::new(LruBufferPool::new(disk, 1000));
        std::mem::forget(dir);
        DocumentStore::without_history(pool).unwrap()
    }

    fn create_store_with_history() -> DocumentStore<String, String> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("docs.db");
        let disk = Arc::new(DiskManager::open(&path).unwrap());
        let pool = Arc::new(LruBufferPool::new(disk, 1000));
        std::mem::forget(dir);
        DocumentStore::with_history(pool, 10).unwrap()
    }

    #[test]
    fn test_insert_and_get() {
        let store = create_store();

        let rev = store
            .insert("doc1".to_string(), "value1".to_string())
            .unwrap();
        assert!(rev > 0);

        let doc = store.get(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(doc.data, "value1");
        assert_eq!(doc.revision, rev);
    }

    #[test]
    fn test_update() {
        let store = create_store();

        let rev1 = store.insert("doc".to_string(), "v1".to_string()).unwrap();
        let rev2 = store
            .update(&"doc".to_string(), "v2".to_string(), rev1)
            .unwrap();

        assert!(rev2 > rev1);
        assert_eq!(store.get(&"doc".to_string()).unwrap().unwrap().data, "v2");
    }

    #[test]
    fn test_conflict_detection() {
        let store = create_store();

        let rev = store.insert("doc".to_string(), "v1".to_string()).unwrap();

        // Try to update with wrong revision
        let result = store.update(&"doc".to_string(), "v2".to_string(), rev + 1);
        assert!(matches!(result, Err(MonoError::WriteConflict(_))));
    }

    #[test]
    fn test_delete() {
        let store = create_store();

        let rev = store
            .insert("doc".to_string(), "value".to_string())
            .unwrap();
        let deleted = store.delete(&"doc".to_string(), rev).unwrap();

        assert!(deleted);
        assert!(store.get(&"doc".to_string()).unwrap().unwrap().deleted);
    }

    #[test]
    fn test_history() {
        let store = create_store_with_history();

        let rev1 = store.insert("doc".to_string(), "v1".to_string()).unwrap();
        let _rev2 = store
            .update(&"doc".to_string(), "v2".to_string(), rev1)
            .unwrap();

        let history = store.get_history(&"doc".to_string(), 10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].data, "v1");
    }

    #[test]
    fn test_upsert() {
        let store = create_store();

        // Insert via upsert
        let rev1 = store.upsert("doc".to_string(), "v1".to_string()).unwrap();
        assert!(rev1 > 0);

        // Update via upsert
        let rev2 = store.upsert("doc".to_string(), "v2".to_string()).unwrap();
        assert!(rev2 > rev1);

        assert_eq!(store.get(&"doc".to_string()).unwrap().unwrap().data, "v2");
    }
}

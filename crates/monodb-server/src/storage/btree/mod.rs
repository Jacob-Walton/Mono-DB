//! B-tree index implementation for MonoDB

mod delete;
mod insert;
mod node;
mod search;

#[cfg(test)]
mod tests;

use monodb_common::Result;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::storage::{
    buffer_pool::BufferPool,
    page::PageId,
    wal::{Wal, WalEntryType},
};

/// B-tree index structure
pub struct BTree {
    root_page_id: Arc<RwLock<Option<PageId>>>,
    buffer_pool: Arc<BufferPool>,
    wal: Option<Arc<RwLock<Wal>>>,
}

impl BTree {
    /// Create a new B-tree with the given name and buffer pool
    pub fn new(_name: String, buffer_pool: Arc<BufferPool>) -> Self {
        let root_from_disk = buffer_pool.get_root_page_id();
        let root = Arc::new(RwLock::new(root_from_disk));

        Self {
            root_page_id: root,
            buffer_pool,
            wal: None,
        }
    }

    /// Attach a write-ahead log
    pub fn with_wal(mut self, wal: Arc<RwLock<Wal>>) -> Self {
        self.wal = Some(wal);
        self
    }

    /// Insert a key-value pair into the tree.
    ///
    /// If the key already exists, the value is updated. The opeartion is
    /// logged to WAL (if configured) before modifying the tree structure.
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Write-ahead logging
        if let Some(wal) = &self.wal {
            let async_mode = { wal.read().is_async() };
            if async_mode {
                wal.read()
                    .append_with_type_async(key, value, WalEntryType::Insert)?;
            } else {
                wal.write()
                    .append_with_type(key, value, WalEntryType::Insert)?;
            }
        }

        insert::insert(self, key, value).await
    }

    /// Insert without writing to WAL (used for WAL recovery)
    pub async fn insert_no_wal(&self, key: &[u8], value: &[u8]) -> Result<()> {
        insert::insert(self, key, value).await
    }

    /// Search for a key in the tree.
    ///
    /// Returns `Ok(Some(value))` if found, `Ok(None)` if not present.
    #[allow(dead_code)]
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        search::get(self, key).await
    }

    /// Range scan from `start_key` to `end_key` inclusive.
    pub async fn scan(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        search::scan(self, start_key, end_key).await
    }

    /// Delete a key from the tree.
    ///
    /// Attempts to remove the key from a leaf node. This basic implementation
    /// does not perform rebalancing/merging on underflow; it simply removes the
    /// cell if present and marks the page dirty.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        if let Some(wal) = &self.wal {
            let async_mode = { wal.read().is_async() };
            if async_mode {
                wal.read()
                    .append_with_type_async(key, &[], WalEntryType::Delete)?;
            } else {
                wal.write()
                    .append_with_type(key, &[], WalEntryType::Delete)?;
            }
        }

        delete::delete(self, key).await
    }

    /// Delete without writing to WAL (used for WAL recovery)
    pub async fn delete_no_wal(&self, key: &[u8]) -> Result<()> {
        delete::delete(self, key).await
    }

    /// Flush all pending changes to disk
    pub async fn flush(&self) -> Result<()> {
        // Flush buffered pages to disk
        self.buffer_pool.flush_all()?;
        // Ensure data and meta are synced to storage
        self.buffer_pool.sync()?;
        // Ensure WAL is durable
        if let Some(wal) = &self.wal {
            wal.write().sync()?;
        }
        Ok(())
    }

    /// Get the root page ID (for internal use)
    pub(crate) fn root_page_id(&self) -> Arc<RwLock<Option<PageId>>> {
        Arc::clone(&self.root_page_id)
    }

    /// Get the buffer pool reference (for internal use)
    pub(crate) fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }
}

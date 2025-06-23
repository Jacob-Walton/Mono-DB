//! B-tree index implementation

use crate::error::{MonoError, MonoResult};
use crate::storage::{BufferPool, PageId, PageType};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::Arc;

/// Index key type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKey {
    Integer(i64),
    String(String),
    Composite(Vec<IndexKey>),
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (IndexKey::Integer(a), IndexKey::Integer(b)) => a.cmp(b),
            (IndexKey::String(a), IndexKey::String(b)) => a.cmp(b),
            (IndexKey::Composite(a), IndexKey::Composite(b)) => a.cmp(b),
            (IndexKey::Integer(_), _) => Ordering::Less,
            (_, IndexKey::Integer(_)) => Ordering::Greater,
            (IndexKey::String(_), IndexKey::Composite(_)) => Ordering::Less,
            (IndexKey::Composite(_), IndexKey::String(_)) => Ordering::Greater,
        }
    }
}

/// Index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub key: IndexKey,
    pub page_id: PageId,
    pub tuple_id: u16,
}

/// B-tree index
pub struct Index {
    pub name: String,
    pub table_name: String,
    pub is_unique: bool,
    pub root_page_id: Option<PageId>,
    buffer_pool: Arc<BufferPool>,
}

impl Index {
    /// Create a new index
    pub fn new(
        name: String,
        table_name: String,
        is_unique: bool,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        Self {
            name,
            table_name,
            is_unique,
            root_page_id: None,
            buffer_pool,
        }
    }

    /// Insert an entry
    pub fn insert(&mut self, entry: IndexEntry) -> MonoResult<()> {
        if self.is_unique {
            if let Some(_existing) = self.search(&entry.key)? {
                return Err(MonoError::Storage("Duplicate key in unique index".into()));
            }
        }

        // For simplicity, just store in a single page
        let (page_id, frame) = if let Some(root) = self.root_page_id {
            let frame = self
                .buffer_pool
                .fetch_page(&self.index_table_name(), root)?;
            (root, frame)
        } else {
            let (new_page_id, new_frame) = self
                .buffer_pool
                .new_page(&self.index_table_name(), PageType::Index)?;
            self.root_page_id = Some(new_page_id);
            (new_page_id, new_frame)
        };

        // Serialize and insert entry
        let entry_data = bincode::serialize(&entry)
            .map_err(|e| MonoError::Storage(format!("Failed to serialize index entry: {}", e)))?;

        {
            let mut page_guard = frame.page.write();
            if let Some(page) = page_guard.as_mut() {
                page.add_tuple(&entry_data)?;
            }
        }

        frame
            .is_dirty
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.buffer_pool
            .unpin_page(&self.index_table_name(), page_id, true)?;

        Ok(())
    }

    /// Search for a key
    pub fn search(&self, key: &IndexKey) -> MonoResult<Option<IndexEntry>> {
        if let Some(root) = self.root_page_id {
            let frame = self
                .buffer_pool
                .fetch_page(&self.index_table_name(), root)?;

            let result = {
                let page_guard = frame.page.read();
                if let Some(page) = page_guard.as_ref() {
                    // Linear search through entries
                    for i in 0..page.header.tuple_count {
                        if let Ok(entry_data) = page.get_tuple(i) {
                            if let Ok(entry) = bincode::deserialize::<IndexEntry>(&entry_data) {
                                if &entry.key == key {
                                    return Ok(Some(entry));
                                }
                            }
                        }
                    }
                }
                None
            };

            self.buffer_pool
                .unpin_page(&self.index_table_name(), root, false)?;
            Ok(result)
        } else {
            Ok(None)
        }
    }

    /// Get index table name
    fn index_table_name(&self) -> String {
        format!("{}__idx_{}", self.table_name, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DiskManager;
    use tempfile::tempdir;

    #[test]
    fn test_index_operations() {
        let temp_dir = tempdir().unwrap();
        let disk_manager = Arc::new(DiskManager::new(temp_dir.path()).unwrap());
        let buffer_pool = Arc::new(BufferPool::new(10, disk_manager));

        let mut index = Index::new(
            "test_idx".to_string(),
            "test_table".to_string(),
            false,
            buffer_pool,
        );

        // Insert entries
        let entry1 = IndexEntry {
            key: IndexKey::Integer(10),
            page_id: PageId(0),
            tuple_id: 0,
        };

        let entry2 = IndexEntry {
            key: IndexKey::Integer(20),
            page_id: PageId(0),
            tuple_id: 1,
        };

        index.insert(entry1.clone()).unwrap();
        index.insert(entry2.clone()).unwrap();

        // Search
        let found = index.search(&IndexKey::Integer(10)).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().tuple_id, 0);
    }
}

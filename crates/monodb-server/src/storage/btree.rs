//! B+Tree implementation for ordered key-value storage.
//!
//! Persistent B+Tree supporting concurrent reads and serialized writes,
//! with pages managed by the [LruBufferPool](super::buffer::LruBufferPool).

use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use monodb_common::{MonoError, Result};
use parking_lot::RwLock;

use super::buffer::LruBufferPool;
use super::page::{DiskPage, PAGE_DATA_SIZE, PageId, PageType};
use super::traits::Serializable;

// Tree configuration

/// Maximum keys per node. Conservative default that works for most key sizes.
const MAX_KEYS: usize = 128;

/// Minimum keys per node (except root).
const MIN_KEYS: usize = MAX_KEYS / 2;

// Binary search utilities

/// Binary search: find first index where keys[i] >= key.
#[inline]
fn lower_bound<K: Ord>(keys: &[K], key: &K) -> usize {
    keys.partition_point(|k| k < key)
}

/// Binary search: find first index where keys[i] > key.
#[inline]
fn upper_bound<K: Ord>(keys: &[K], key: &K) -> usize {
    keys.partition_point(|k| k <= key)
}

// In-memory tree node representation

/// In-memory representation of a B+Tree node.
#[derive(Debug, Clone)]
struct TreeNode<K, V> {
    /// The page ID of this node.
    page_id: PageId,
    /// Whether this is a leaf node.
    is_leaf: bool,
    /// Keys stored in this node.
    keys: Vec<K>,
    /// Values (only for leaf nodes).
    values: Vec<V>,
    /// Child page IDs (only for interior nodes).
    children: Vec<PageId>,
    /// Next leaf page (only for leaf nodes, for range scans).
    next_leaf: PageId,
}

impl<K: Serializable, V: Serializable> TreeNode<K, V> {
    /// Create a new leaf node.
    fn new_leaf(page_id: PageId) -> Self {
        Self {
            page_id,
            is_leaf: true,
            keys: Vec::with_capacity(MAX_KEYS),
            values: Vec::with_capacity(MAX_KEYS),
            children: Vec::new(),
            next_leaf: PageId::INVALID,
        }
    }

    /// Create a new interior node.
    fn new_interior(page_id: PageId) -> Self {
        Self {
            page_id,
            is_leaf: false,
            keys: Vec::with_capacity(MAX_KEYS),
            values: Vec::new(),
            children: Vec::with_capacity(MAX_KEYS + 1),
            next_leaf: PageId::INVALID,
        }
    }

    /// Serialize the node to a page.
    fn to_page(&self) -> DiskPage {
        let mut page = if self.is_leaf {
            DiskPage::new_leaf(self.page_id)
        } else {
            DiskPage::new_interior(self.page_id)
        };

        page.header.key_count = self.keys.len() as u16;
        page.header.next_page = self.next_leaf;

        // Serialize data
        let mut buf = Vec::with_capacity(PAGE_DATA_SIZE);

        // Key count
        (self.keys.len() as u32).serialize(&mut buf);

        if self.is_leaf {
            // Leaf: serialize keys then values
            for key in &self.keys {
                key.serialize(&mut buf);
            }
            for value in &self.values {
                value.serialize(&mut buf);
            }
        } else {
            // Interior: serialize key count, child count, keys, then children
            (self.children.len() as u32).serialize(&mut buf);
            for key in &self.keys {
                key.serialize(&mut buf);
            }
            for child in &self.children {
                child.0.serialize(&mut buf);
            }
        }

        page.data = buf;
        page.header.free_space = (PAGE_DATA_SIZE - page.data.len()) as u16;

        page
    }

    /// Deserialize from a page.
    fn from_page(page: &DiskPage) -> Result<Self> {
        let is_leaf = page.header.page_type == PageType::Leaf;
        let page_id = page.header.page_id;
        let next_leaf = page.header.next_page;

        if page.data.is_empty() {
            // Empty node
            return Ok(if is_leaf {
                Self::new_leaf(page_id)
            } else {
                Self::new_interior(page_id)
            });
        }

        let buf = &page.data;
        let mut offset = 0;

        // Read key count
        let (key_count, consumed) = u32::deserialize(&buf[offset..])?;
        offset += consumed;
        let key_count = key_count as usize;

        if is_leaf {
            // Leaf: read keys then values
            let mut keys = Vec::with_capacity(key_count);
            let mut values = Vec::with_capacity(key_count);

            for _ in 0..key_count {
                let (key, consumed) = K::deserialize(&buf[offset..])?;
                offset += consumed;
                keys.push(key);
            }

            for _ in 0..key_count {
                let (value, consumed) = V::deserialize(&buf[offset..])?;
                offset += consumed;
                values.push(value);
            }

            Ok(Self {
                page_id,
                is_leaf: true,
                keys,
                values,
                children: Vec::new(),
                next_leaf,
            })
        } else {
            // Interior: read child count, keys, then children
            let (child_count, consumed) = u32::deserialize(&buf[offset..])?;
            offset += consumed;
            let child_count = child_count as usize;

            let mut keys = Vec::with_capacity(key_count);
            let mut children = Vec::with_capacity(child_count);

            for _ in 0..key_count {
                let (key, consumed) = K::deserialize(&buf[offset..])?;
                offset += consumed;
                keys.push(key);
            }

            for _ in 0..child_count {
                let (child_id, consumed) = u64::deserialize(&buf[offset..])?;
                offset += consumed;
                children.push(PageId(child_id));
            }

            Ok(Self {
                page_id,
                is_leaf: false,
                keys,
                values: Vec::new(),
                children,
                next_leaf: PageId::INVALID,
            })
        }
    }

    /// Check if the node is full.
    #[inline]
    fn is_full(&self) -> bool {
        self.keys.len() >= MAX_KEYS
    }

    /// Calculate serialized size.
    fn serialized_size(&self) -> usize {
        let mut size = 4; // key count

        if self.is_leaf {
            for key in &self.keys {
                size += key.serialized_size();
            }
            for value in &self.values {
                size += value.serialized_size();
            }
        } else {
            size += 4; // child count
            for key in &self.keys {
                size += key.serialized_size();
            }
            size += self.children.len() * 8;
        }

        size
    }

    /// Check if adding an entry would overflow the page.
    fn would_overflow(&self, key: &K, value: &V) -> bool {
        let additional = key.serialized_size() + value.serialized_size();
        self.serialized_size() + additional > PAGE_DATA_SIZE
    }
}

// Tree Metadata

/// Metadata stored in page 0 of the tree file.
#[derive(Debug, Clone)]
struct TreeMeta {
    /// Root page ID.
    root: PageId,
    /// Total number of entries.
    len: u64,
    /// Tree height.
    height: u32,
}

impl TreeMeta {
    fn new() -> Self {
        Self {
            root: PageId(1), // Root starts at page 1
            len: 0,
            height: 1,
        }
    }

    fn serialize(&self, buf: &mut Vec<u8>) {
        self.root.0.serialize(buf);
        self.len.serialize(buf);
        self.height.serialize(buf);
    }

    fn deserialize(buf: &[u8]) -> Result<Self> {
        let mut offset = 0;

        let (root, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (len, consumed) = u64::deserialize(&buf[offset..])?;
        offset += consumed;

        let (height, _) = u32::deserialize(&buf[offset..])?;

        Ok(Self {
            root: PageId(root),
            len,
            height,
        })
    }
}

// B+Tree

/// Persistent B+Tree with concurrent read access.
///
/// Each BTree instance manages its own metadata page, allowing multiple
/// BTrees to coexist in the same buffer pool (namespacing support).
pub struct BTree<K, V> {
    /// Buffer pool for page access.
    pool: Arc<LruBufferPool>,
    /// Page ID where this tree's metadata is stored.
    meta_page_id: PageId,
    /// Tree metadata (cached).
    meta: RwLock<TreeMeta>,
    /// Number of entries (cached for fast access).
    len: AtomicUsize,
    /// Phantom data for key/value types.
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> BTree<K, V>
where
    K: Ord + Clone + Serializable + Send + Sync,
    V: Clone + Serializable + Send + Sync,
{
    /// Create a new B+Tree with its own namespace (metadata page).
    ///
    /// Each call creates a fresh tree with a newly allocated metadata page.
    /// Multiple BTrees can safely coexist in the same buffer pool.
    pub fn new(pool: Arc<LruBufferPool>) -> Result<Self> {
        let (meta, meta_page_id) = Self::initialize(&pool)?;
        let len = meta.len as usize;

        Ok(Self {
            pool,
            meta_page_id,
            meta: RwLock::new(meta),
            len: AtomicUsize::new(len),
            _phantom: PhantomData,
        })
    }

    /// Open an existing B+Tree from a known metadata page.
    ///
    /// Use this to reopen a tree after restart, passing the metadata page ID
    /// that was previously returned by `meta_page_id()`.
    pub fn open(pool: Arc<LruBufferPool>, meta_page_id: PageId) -> Result<Self> {
        let meta = Self::load_meta(&pool, meta_page_id)?;
        let len = meta.len as usize;

        Ok(Self {
            pool,
            meta_page_id,
            meta: RwLock::new(meta),
            len: AtomicUsize::new(len),
            _phantom: PhantomData,
        })
    }

    /// Get the metadata page ID for this tree.
    ///
    /// Store this value to reopen the tree later with `open()`.
    pub fn meta_page_id(&self) -> PageId {
        self.meta_page_id
    }

    /// Initialize a new empty tree, returning metadata and its page ID.
    fn initialize(pool: &LruBufferPool) -> Result<(TreeMeta, PageId)> {
        // Allocate metadata page for this tree
        let meta_frame = pool.alloc_meta()?;
        let meta_page_id = meta_frame.page_id;

        // Allocate root page (leaf initially)
        let root_frame = pool.alloc_leaf()?;
        let root_id = root_frame.page_id;

        let meta = TreeMeta {
            root: root_id,
            len: 0,
            height: 1,
        };

        // Save metadata to our dedicated page
        Self::save_meta_to_pool(pool, meta_page_id, &meta)?;

        Ok((meta, meta_page_id))
    }

    /// Load metadata from a specific page.
    fn load_meta(pool: &LruBufferPool, meta_page_id: PageId) -> Result<TreeMeta> {
        let frame = pool.get_frame(meta_page_id)?;
        let page = frame.page.read();

        if page.data.is_empty() {
            return Err(MonoError::Storage("no metadata found".into()));
        }

        TreeMeta::deserialize(&page.data)
    }

    /// Save metadata to the pool.
    fn save_meta(&self) -> Result<()> {
        let meta = self.meta.read().clone();
        Self::save_meta_to_pool(&self.pool, self.meta_page_id, &meta)
    }

    fn save_meta_to_pool(
        pool: &LruBufferPool,
        meta_page_id: PageId,
        meta: &TreeMeta,
    ) -> Result<()> {
        let frame = pool.get_frame(meta_page_id)?;

        let mut buf = Vec::new();
        meta.serialize(&mut buf);

        {
            let mut page = frame.page.write();
            page.data = buf;
            page.header.page_type = PageType::Meta;
        }
        frame.mark_dirty();

        Ok(())
    }

    /// Load a node from the buffer pool.
    fn load_node(&self, page_id: PageId) -> Result<TreeNode<K, V>> {
        let frame = self.pool.get_frame(page_id)?;
        let page = frame.page.read();
        TreeNode::from_page(&page)
    }

    /// Save a node to the buffer pool.
    fn save_node(&self, node: &TreeNode<K, V>) -> Result<()> {
        let page = node.to_page();
        let frame = self.pool.get_frame(node.page_id)?;
        *frame.page.write() = page;
        frame.mark_dirty();
        Ok(())
    }

    /// Insert a key-value pair.
    pub fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        let root_id = self.meta.read().root;

        // Check if root needs splitting
        let root = self.load_node(root_id)?;
        if root.is_full() {
            // Split root
            self.split_root()?;
        }

        let old_value = self.insert_non_full(self.meta.read().root, key, value)?;

        if old_value.is_none() {
            self.len.fetch_add(1, AtomicOrdering::Relaxed);
            self.meta.write().len += 1;
        }

        self.save_meta()?;
        Ok(old_value)
    }

    /// Split the root node, creating a new root.
    fn split_root(&self) -> Result<()> {
        let old_root_id = self.meta.read().root;
        let old_root = self.load_node(old_root_id)?;

        // Create new root (interior node)
        let new_root_frame = self.pool.alloc_interior()?;
        let new_root_id = new_root_frame.page_id;

        // Split old root
        let (median_key, sibling) = self.split_node(&old_root)?;

        // Update new root
        let mut new_root = TreeNode::<K, V>::new_interior(new_root_id);
        new_root.keys.push(median_key);
        new_root.children.push(old_root_id);
        new_root.children.push(sibling.page_id);

        self.save_node(&new_root)?;
        self.save_node(&sibling)?;

        // Update metadata
        let mut meta = self.meta.write();
        meta.root = new_root_id;
        meta.height += 1;

        Ok(())
    }

    /// Split a node, returning the median key and the new sibling.
    fn split_node(&self, node: &TreeNode<K, V>) -> Result<(K, TreeNode<K, V>)> {
        let split_point = node.keys.len() / 2;

        if node.is_leaf {
            // Leaf split: copy median to parent, split keys/values
            let sibling_frame = self.pool.alloc_leaf()?;
            let mut sibling = TreeNode::new_leaf(sibling_frame.page_id);

            sibling.keys = node.keys[split_point..].to_vec();
            sibling.values = node.values[split_point..].to_vec();
            sibling.next_leaf = node.next_leaf;

            let median = sibling.keys[0].clone();

            // Update original node
            let mut updated = node.clone();
            updated.keys.truncate(split_point);
            updated.values.truncate(split_point);
            updated.next_leaf = sibling.page_id;

            self.save_node(&updated)?;

            Ok((median, sibling))
        } else {
            // Interior split: move median to parent
            let sibling_frame = self.pool.alloc_interior()?;
            let mut sibling = TreeNode::new_interior(sibling_frame.page_id);

            let median = node.keys[split_point].clone();

            sibling.keys = node.keys[split_point + 1..].to_vec();
            sibling.children = node.children[split_point + 1..].to_vec();

            // Update original node
            let mut updated = node.clone();
            updated.keys.truncate(split_point);
            updated.children.truncate(split_point + 1);

            self.save_node(&updated)?;

            Ok((median, sibling))
        }
    }

    /// Insert into a non-full node.
    fn insert_non_full(&self, page_id: PageId, key: K, value: V) -> Result<Option<V>> {
        let mut current_id = page_id;

        loop {
            let node = self.load_node(current_id)?;

            if node.is_leaf {
                return self.insert_into_leaf(current_id, key, value);
            }

            // Find child to descend into
            let idx = upper_bound(&node.keys, &key);
            let child_id = node.children[idx];

            // Check if child needs splitting
            let child = self.load_node(child_id)?;
            if child.is_full() {
                self.split_child(&node, idx)?;
                // Re-determine which child to use
                let node = self.load_node(current_id)?;
                let idx = upper_bound(&node.keys, &key);
                current_id = node.children[idx];
            } else {
                current_id = child_id;
            }
        }
    }

    /// Insert into a leaf node.
    fn insert_into_leaf(&self, page_id: PageId, key: K, value: V) -> Result<Option<V>> {
        let mut node = self.load_node(page_id)?;
        let idx = lower_bound(&node.keys, &key);

        if idx < node.keys.len() && node.keys[idx] == key {
            // Key exists, update value
            let old = std::mem::replace(&mut node.values[idx], value);
            self.save_node(&node)?;
            return Ok(Some(old));
        }

        // Insert new key-value
        node.keys.insert(idx, key);
        node.values.insert(idx, value);
        self.save_node(&node)?;

        Ok(None)
    }

    /// Split a child of an interior node.
    fn split_child(&self, parent: &TreeNode<K, V>, child_idx: usize) -> Result<()> {
        let child_id = parent.children[child_idx];
        let child = self.load_node(child_id)?;

        let (median, sibling) = self.split_node(&child)?;

        // Update parent
        let mut updated_parent = parent.clone();
        updated_parent.keys.insert(child_idx, median);
        updated_parent
            .children
            .insert(child_idx + 1, sibling.page_id);

        self.save_node(&updated_parent)?;
        self.save_node(&sibling)?;

        Ok(())
    }

    /// Get the value for a key.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let root_id = self.meta.read().root;
        let mut current_id = root_id;

        loop {
            let node = self.load_node(current_id)?;

            if node.is_leaf {
                let idx = lower_bound(&node.keys, key);
                if idx < node.keys.len() && &node.keys[idx] == key {
                    return Ok(Some(node.values[idx].clone()));
                }
                return Ok(None);
            }

            let idx = upper_bound(&node.keys, key);
            current_id = node.children[idx];
        }
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &K) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Delete a key.
    pub fn delete(&self, key: &K) -> Result<Option<V>> {
        // Simplified delete: just mark as deleted, don't rebalance
        // TODO: Rebalance tree on underflows
        let root_id = self.meta.read().root;
        let result = self.delete_from_node(root_id, key)?;

        if result.is_some() {
            self.len.fetch_sub(1, AtomicOrdering::Relaxed);
            self.meta.write().len -= 1;
            self.save_meta()?;
        }

        Ok(result)
    }

    /// Delete from a node (recursive).
    fn delete_from_node(&self, page_id: PageId, key: &K) -> Result<Option<V>> {
        let mut node = self.load_node(page_id)?;

        if node.is_leaf {
            let idx = lower_bound(&node.keys, key);
            if idx < node.keys.len() && &node.keys[idx] == key {
                node.keys.remove(idx);
                let value = node.values.remove(idx);
                self.save_node(&node)?;
                return Ok(Some(value));
            }
            return Ok(None);
        }

        // Interior node: find and recurse
        let idx = upper_bound(&node.keys, key);
        self.delete_from_node(node.children[idx], key)
    }

    /// Range scan.
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> Result<Vec<(K, V)>> {
        let mut results = Vec::new();

        // Find starting leaf
        let start_key = match range.start_bound() {
            Bound::Included(k) | Bound::Excluded(k) => Some(k.clone()),
            Bound::Unbounded => None,
        };

        let leaf_id = if let Some(ref key) = start_key {
            self.find_leaf(key)?
        } else {
            self.find_leftmost_leaf()?
        };

        let mut current_id = leaf_id;

        loop {
            let node = self.load_node(current_id)?;

            let start_idx = if let Some(ref key) = start_key {
                let idx = lower_bound(&node.keys, key);
                // Handle Excluded bound
                if matches!(range.start_bound(), Bound::Excluded(_))
                    && idx < node.keys.len()
                    && &node.keys[idx] == key
                {
                    idx + 1
                } else {
                    idx
                }
            } else {
                0
            };

            for i in start_idx..node.keys.len() {
                let key = &node.keys[i];

                // Check end bound
                let in_range = match range.end_bound() {
                    Bound::Included(end) => key <= end,
                    Bound::Excluded(end) => key < end,
                    Bound::Unbounded => true,
                };

                if !in_range {
                    return Ok(results);
                }

                results.push((key.clone(), node.values[i].clone()));
            }

            // Move to next leaf
            if node.next_leaf.is_valid() {
                current_id = node.next_leaf;
            } else {
                break;
            }
        }

        Ok(results)
    }

    /// Find the leaf containing a key.
    fn find_leaf(&self, key: &K) -> Result<PageId> {
        let root_id = self.meta.read().root;
        let mut current_id = root_id;

        loop {
            let node = self.load_node(current_id)?;

            if node.is_leaf {
                return Ok(current_id);
            }

            let idx = upper_bound(&node.keys, key);
            current_id = node.children[idx];
        }
    }

    /// Find the leftmost leaf.
    fn find_leftmost_leaf(&self) -> Result<PageId> {
        let root_id = self.meta.read().root;
        let mut current_id = root_id;

        loop {
            let node = self.load_node(current_id)?;

            if node.is_leaf {
                return Ok(current_id);
            }

            current_id = node.children[0];
        }
    }

    /// Get the minimum key-value pair.
    pub fn min(&self) -> Result<Option<(K, V)>> {
        let leaf_id = self.find_leftmost_leaf()?;
        let node = self.load_node(leaf_id)?;

        if node.keys.is_empty() {
            return Ok(None);
        }

        Ok(Some((node.keys[0].clone(), node.values[0].clone())))
    }

    /// Get the maximum key-value pair.
    pub fn max(&self) -> Result<Option<(K, V)>> {
        let root_id = self.meta.read().root;
        let mut current_id = root_id;

        loop {
            let node = self.load_node(current_id)?;

            if node.is_leaf {
                if node.keys.is_empty() {
                    return Ok(None);
                }
                let last = node.keys.len() - 1;
                return Ok(Some((node.keys[last].clone(), node.values[last].clone())));
            }

            current_id = *node.children.last().unwrap();
        }
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.len.load(AtomicOrdering::Relaxed)
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Flush all changes to disk.
    pub fn flush(&self) -> Result<()> {
        self.save_meta()?;
        self.pool.flush_all()
    }

    /// Iterate over all key-value pairs.
    pub fn iter(&self) -> Result<impl Iterator<Item = (K, V)>> {
        let all = self.range::<(Bound<K>, Bound<K>)>((Bound::Unbounded, Bound::Unbounded))?;
        Ok(all.into_iter())
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::DiskManager;
    use tempfile::tempdir;

    fn create_tree() -> BTree<String, String> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let disk = Arc::new(DiskManager::open(&path).unwrap());
        let pool = Arc::new(LruBufferPool::new(disk, 1000));
        std::mem::forget(dir);
        BTree::new(pool).unwrap()
    }

    #[test]
    fn test_insert_and_get() {
        let tree = create_tree();

        tree.insert("key1".to_string(), "value1".to_string())
            .unwrap();
        tree.insert("key2".to_string(), "value2".to_string())
            .unwrap();

        assert_eq!(
            tree.get(&"key1".to_string()).unwrap(),
            Some("value1".to_string())
        );
        assert_eq!(
            tree.get(&"key2".to_string()).unwrap(),
            Some("value2".to_string())
        );
        assert_eq!(tree.get(&"key3".to_string()).unwrap(), None);
    }

    #[test]
    fn test_update() {
        let tree = create_tree();

        tree.insert("key".to_string(), "value1".to_string())
            .unwrap();
        let old = tree
            .insert("key".to_string(), "value2".to_string())
            .unwrap();

        assert_eq!(old, Some("value1".to_string()));
        assert_eq!(
            tree.get(&"key".to_string()).unwrap(),
            Some("value2".to_string())
        );
    }

    #[test]
    fn test_delete() {
        let tree = create_tree();

        tree.insert("key".to_string(), "value".to_string()).unwrap();
        let deleted = tree.delete(&"key".to_string()).unwrap();

        assert_eq!(deleted, Some("value".to_string()));
        assert_eq!(tree.get(&"key".to_string()).unwrap(), None);
    }

    #[test]
    fn test_range() {
        let tree = create_tree();

        for i in 0..10 {
            tree.insert(format!("key{:02}", i), format!("value{}", i))
                .unwrap();
        }

        let range: Vec<_> = tree
            .range("key03".to_string().."key07".to_string())
            .unwrap();

        assert_eq!(range.len(), 4);
        assert_eq!(range[0].0, "key03");
        assert_eq!(range[3].0, "key06");
    }

    #[test]
    fn test_min_max() {
        let tree = create_tree();

        tree.insert("banana".to_string(), "yellow".to_string())
            .unwrap();
        tree.insert("apple".to_string(), "red".to_string()).unwrap();
        tree.insert("cherry".to_string(), "red".to_string())
            .unwrap();

        let min = tree.min().unwrap().unwrap();
        let max = tree.max().unwrap().unwrap();

        assert_eq!(min.0, "apple");
        assert_eq!(max.0, "cherry");
    }

    #[test]
    fn test_many_inserts() {
        let tree = create_tree();

        for i in 0..1000 {
            tree.insert(format!("{:05}", i), format!("value{}", i))
                .unwrap();
        }

        assert_eq!(tree.len(), 1000);

        // Verify some random accesses
        assert_eq!(
            tree.get(&"00500".to_string()).unwrap(),
            Some("value500".to_string())
        );
        assert_eq!(
            tree.get(&"00999".to_string()).unwrap(),
            Some("value999".to_string())
        );
    }
}

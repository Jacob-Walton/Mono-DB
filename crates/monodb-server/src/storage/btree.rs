//! Persistent B+Tree implementation.
//!
//! Uses TypedPageStore for LRU page caching and handles all the usual
//! B+Tree operations (insert, get, delete, range queries, etc.).

use std::path::Path;

use monodb_common::{MonoError, Result};
use parking_lot::Mutex;
use xxhash_rust::xxh64::xxh64;

use crate::storage::{
    buffer_pool::{BufferPoolStats, TypedPageStore},
    page::{PageId, Serializable},
};

/// Branchless binary search - finds first index where keys[i] >= key
#[inline(always)]
fn lower_bound<K: Ord>(keys: &[K], key: &K) -> usize {
    let mut len = keys.len();
    let mut base = 0usize;

    while len > 1 {
        let half = len / 2;
        let mid = base + half;
        base = if unsafe { keys.get_unchecked(mid) } < key {
            mid
        } else {
            base
        };
        len -= half;
    }

    base + (len > 0 && unsafe { keys.get_unchecked(base) } < key) as usize
}

/// Same as lower_bound but finds keys[i] > key instead of >=
#[inline(always)]
fn upper_bound<K: Ord>(keys: &[K], key: &K) -> usize {
    let mut len = keys.len();
    let mut base = 0usize;

    while len > 1 {
        let half = len / 2;
        let mid = base + half;
        base = if unsafe { keys.get_unchecked(mid) } <= key {
            mid
        } else {
            base
        };
        len -= half;
    }

    base + (len > 0 && unsafe { keys.get_unchecked(base) } <= key) as usize
}

// Max keys per node - 64 is conservative but works for any key/value size
const MAX_KEYS: usize = 64;

// Persistent B+Tree

// Page 0 is always metadata
const META_PAGE_ID: PageId = PageId(0);

// Persistent B+Tree with typed page caching
pub struct BTree<K: Ord + Clone + Serializable, V: Clone + Serializable> {
    store: TypedPageStore<K, V>,
    root: PageId,
    len: usize,
}

impl<K: Ord + Clone + Serializable, V: Clone + Serializable> BTree<K, V> {
    // Open existing tree or create a new one
    pub fn new<P: AsRef<Path>>(path: P, pool_size: usize) -> Result<Self> {
        let mut store = TypedPageStore::new(path, pool_size)?;

        let (root, len) = if store.page_count() > 0 {
            let meta = store.get(META_PAGE_ID)?;
            let root_id = if !meta.children.is_empty() {
                meta.children[0]
            } else {
                PageId(1)
            };
            let len = if meta.children.len() >= 3 {
                ((meta.children[1].0 as usize) << 32) | (meta.children[2].0 as usize)
            } else {
                0
            };
            (root_id, len)
        } else {
            let _meta_id = store.alloc_interior()?;
            let root = store.alloc_leaf()?;

            {
                let meta = store.get_mut(META_PAGE_ID)?;
                meta.children.push(root);
                meta.children.push(PageId(0));
                meta.children.push(PageId(0));
            }

            (root, 0)
        };

        Ok(Self { store, root, len })
    }
    fn save_metadata(&mut self) -> Result<()> {
        let meta = self.store.get_mut(META_PAGE_ID)?;

        while meta.children.len() < 3 {
            meta.children.push(PageId(0));
        }

        meta.children[0] = self.root;
        meta.children[1] = PageId((self.len >> 32) as u32);
        meta.children[2] = PageId(self.len as u32);

        Ok(())
    }

    #[inline(always)]
    #[allow(dead_code)]
    fn split_point(&self) -> usize {
        MAX_KEYS / 2
    }

    // Insert key-value pair, splitting root if needed
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        // Check if root is full (count-based check)
        let root_is_full = {
            let root = self.store.get(self.root)?;
            root.keys.len() >= MAX_KEYS
        };

        if root_is_full {
            let old_root = self.root;
            let new_root = self.store.alloc_interior()?;

            {
                let new_root_page = self.store.get_mut(new_root)?;
                new_root_page.children.push(old_root);
            }

            self.root = new_root;
            self.split_child(new_root, 0)?;
        }

        self.insert_non_full(self.root, key, value)?;
        self.len += 1;
        Ok(())
    }

    fn insert_non_full(&mut self, page_id: PageId, key: K, value: V) -> Result<()> {
        let mut current = page_id;

        loop {
            let (is_leaf, child_id, needs_split) = {
                let page = self.store.get(current)?;

                if page.is_leaf {
                    (true, PageId::INVALID, false)
                } else {
                    let i = upper_bound(&page.keys, &key);
                    let child_id = page.children[i];

                    let child = self.store.get(child_id)?;
                    // Count-based check: is the child full?
                    let needs_split = child.keys.len() >= MAX_KEYS;

                    (false, child_id, needs_split)
                }
            };

            if is_leaf {
                let page = self.store.get_mut(current)?;
                let pos = lower_bound(&page.keys, &key);

                if pos == page.keys.len() {
                    page.keys.push(key);
                    page.values.push(value);
                } else {
                    page.keys.insert(pos, key);
                    page.values.insert(pos, value);
                }
                return Ok(());
            }

            if needs_split {
                let i = {
                    let page = self.store.get(current)?;
                    upper_bound(&page.keys, &key)
                };

                self.split_child(current, i)?;

                let page = self.store.get(current)?;
                if key >= page.keys[i] {
                    current = page.children[i + 1];
                } else {
                    current = page.children[i];
                }
            } else {
                current = child_id;
            }
        }
    }

    fn split_child(&mut self, parent_id: PageId, child_index: usize) -> Result<()> {
        let (child_id, is_leaf, split_point) = {
            let parent = self.store.get(parent_id)?;
            let child_id = parent.children[child_index];
            let child = self.store.get(child_id)?;
            // Count-based split point: split in the middle
            let sp = child.keys.len() / 2;
            (child_id, child.is_leaf, sp.max(1)) // Ensure at least 1 key stays in child
        };

        let sibling_id = if is_leaf {
            self.store.alloc_leaf()?
        } else {
            self.store.alloc_interior()?
        };

        let (split_key, sibling_keys, sibling_values, sibling_children, child_next) = {
            let child = self.store.get_mut(child_id)?;

            if is_leaf {
                let split_key = child.keys[split_point].clone();
                let sibling_keys = child.keys.split_off(split_point);
                let sibling_values = child.values.split_off(split_point);
                let child_next = child.next_leaf;
                child.next_leaf = sibling_id;
                (
                    split_key,
                    sibling_keys,
                    sibling_values,
                    Vec::new(),
                    child_next,
                )
            } else {
                let median_key = child.keys[split_point - 1].clone();
                let sibling_keys = child.keys.split_off(split_point);
                child.keys.pop();
                let sibling_children = child.children.split_off(split_point);
                (
                    median_key,
                    sibling_keys,
                    Vec::new(),
                    sibling_children,
                    PageId::INVALID,
                )
            }
        };

        {
            let sibling = self.store.get_mut(sibling_id)?;
            sibling.keys = sibling_keys;
            if is_leaf {
                sibling.values = sibling_values;
                sibling.next_leaf = child_next;
            } else {
                sibling.children = sibling_children;
            }
        }

        {
            let parent = self.store.get_mut(parent_id)?;
            parent.keys.insert(child_index, split_key);
            parent.children.insert(child_index + 1, sibling_id);
        }

        Ok(())
    }

    // Look up value by key
    #[inline]
    pub fn get(&mut self, key: &K) -> Result<Option<V>> {
        let mut current = self.root;

        loop {
            let page = self.store.get(current)?;

            if page.is_leaf {
                let i = lower_bound(&page.keys, key);
                if i < page.keys.len() && &page.keys[i] == key {
                    return Ok(Some(page.values[i].clone()));
                }
                return Ok(None);
            }

            let i = upper_bound(&page.keys, key);
            current = page.children[i];
        }
    }

    #[inline]
    pub fn contains(&mut self, key: &K) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    // Range scan [start, end)
    pub fn range(&mut self, start: &K, end: &K) -> Result<Vec<(K, V)>> {
        let mut results = Vec::new();
        let leaf_id = self.find_leaf(start)?;
        let mut current = leaf_id;

        loop {
            let (keys, values, next) = {
                let page = self.store.get(current)?;
                (page.keys.clone(), page.values.clone(), page.next_leaf)
            };

            let start_pos = lower_bound(&keys, start);

            for i in start_pos..keys.len() {
                if &keys[i] >= end {
                    return Ok(results);
                }
                results.push((keys[i].clone(), values[i].clone()));
            }

            if next.is_valid() {
                current = next;
            } else {
                break;
            }
        }
        Ok(results)
    }

    fn find_leaf(&mut self, key: &K) -> Result<PageId> {
        let mut current = self.root;

        loop {
            let page = self.store.get(current)?;

            if page.is_leaf {
                return Ok(current);
            }

            let i = upper_bound(&page.keys, key);
            current = page.children[i];
        }
    }

    // Get min key-value pair
    pub fn min(&mut self) -> Result<Option<(K, V)>> {
        let mut current = self.root;

        loop {
            let page = self.store.get(current)?;

            if page.is_leaf {
                if page.keys.is_empty() {
                    return Ok(None);
                }
                return Ok(Some((page.keys[0].clone(), page.values[0].clone())));
            }

            current = page.children[0];
        }
    }

    // Get max key-value pair
    pub fn max(&mut self) -> Result<Option<(K, V)>> {
        let mut current = self.root;

        loop {
            let page = self.store.get(current)?;

            if page.is_leaf {
                if page.keys.is_empty() {
                    return Ok(None);
                }
                let last = page.keys.len() - 1;
                return Ok(Some((page.keys[last].clone(), page.values[last].clone())));
            }

            current = *page.children.last().unwrap();
        }
    }

    // Delete key and return old value if it existed
    pub fn delete(&mut self, key: &K) -> Result<Option<V>> {
        let result = self.delete_internal(self.root, key)?;
        if result.is_some() {
            self.len = self.len.saturating_sub(1);
        }
        Ok(result)
    }

    fn delete_internal(&mut self, page_id: PageId, key: &K) -> Result<Option<V>> {
        let page = self.store.get(page_id)?;

        if page.is_leaf {
            // Find and remove the key from the leaf
            let pos = lower_bound(&page.keys, key);
            if pos < page.keys.len() && &page.keys[pos] == key {
                let page = self.store.get_mut(page_id)?;
                page.keys.remove(pos);
                let value = page.values.remove(pos);
                return Ok(Some(value));
            }
            return Ok(None);
        }

        // Interior node - recurse into correct child
        let i = upper_bound(&page.keys, key);
        let child_id = page.children[i];

        self.delete_internal(child_id, key)
    }

    // Flush everything to disk
    pub fn sync(&mut self) -> Result<()> {
        self.save_metadata()?;
        self.store.flush_all()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn stats(&self) -> BufferPoolStats {
        self.store.stats()
    }

    pub fn page_count(&self) -> u32 {
        self.store.page_count()
    }

    // Iterator over all entries in sorted order
    pub fn iter(&mut self) -> Result<BTreeIterator<K, V>> {
        // Find the leftmost leaf
        let mut current = self.root;
        loop {
            let page = self.store.get(current)?;
            if page.is_leaf {
                break;
            }
            current = page.children[0];
        }

        // Collect all entries from this leaf
        let page = self.store.get(current)?;
        let entries: Vec<_> = page
            .keys
            .iter()
            .cloned()
            .zip(page.values.iter().cloned())
            .collect();

        Ok(BTreeIterator {
            current_leaf: current,
            current_index: 0,
            entries,
            next_leaf: page.next_leaf,
        })
    }
}

impl<K: Ord + Clone + Serializable, V: Clone + Serializable> Drop for BTree<K, V> {
    fn drop(&mut self) {
        // Save metadata before the store is dropped
        let _ = self.save_metadata();
    }
}

// Iterator state for walking through the tree
pub struct BTreeIterator<K, V> {
    current_leaf: PageId,
    current_index: usize,
    entries: Vec<(K, V)>,
    next_leaf: PageId,
}

impl<K: Clone, V: Clone> BTreeIterator<K, V> {
    // Get next entry, loading new leaf pages as needed
    pub fn next<S: Serializable>(
        &mut self,
        store: &mut TypedPageStore<K, V>,
    ) -> Result<Option<(K, V)>>
    where
        K: Serializable,
        V: Serializable,
    {
        if self.current_index < self.entries.len() {
            let entry = self.entries[self.current_index].clone();
            self.current_index += 1;
            return Ok(Some(entry));
        }

        // Need to load next leaf
        if !self.next_leaf.is_valid() {
            return Ok(None);
        }

        self.current_leaf = self.next_leaf;
        let page = store.get(self.current_leaf)?;
        self.entries = page
            .keys
            .iter()
            .cloned()
            .zip(page.values.iter().cloned())
            .collect();
        self.next_leaf = page.next_leaf;
        self.current_index = 0;

        if self.current_index < self.entries.len() {
            let entry = self.entries[self.current_index].clone();
            self.current_index += 1;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
}

// Thread-Safe Storage Tree Wrapper

// Using 8 shards for concurrency - power of 2 for fast modulo
const NUM_SHARDS: usize = 8;

// Hash key to pick which shard to use
#[inline(always)]
fn shard_for_key(key: &[u8]) -> usize {
    const SEED: u64 = 0; // deterministic hashing
    let hash = xxh64(key, SEED);
    (hash as usize) & (NUM_SHARDS - 1)
}

// Sharded B+Tree wrapper for concurrent access
pub struct StorageTree {
    shards: Vec<Mutex<BTree<Vec<u8>, Vec<u8>>>>,
    name: String,
}

impl StorageTree {
    /// Create a new sharded storage tree or open an existing one
    pub fn new<P: AsRef<Path>>(path: P, name: String, pool_size: usize) -> Result<Self> {
        let base_path = path.as_ref();
        let pool_per_shard = pool_size / NUM_SHARDS;

        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for i in 0..NUM_SHARDS {
            let shard_path = base_path.with_file_name(format!(
                "{}.shard{:02}.db",
                base_path.file_stem().unwrap_or_default().to_string_lossy(),
                i
            ));
            let tree = BTree::new(&shard_path, pool_per_shard.max(256)).map_err(|e| {
                MonoError::Storage(format!("Failed to create B+Tree shard {}: {}", i, e))
            })?;
            shards.push(Mutex::new(tree));
        }

        Ok(Self { shards, name })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    fn get_shard(&self, key: &[u8]) -> &Mutex<BTree<Vec<u8>, Vec<u8>>> {
        &self.shards[shard_for_key(key)]
    }

    // Insert without WAL (async wrapper around sync code)
    pub async fn put_no_wal(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut tree = self.get_shard(&key).lock();
        tree.insert(key, value)
            .map_err(|e| MonoError::Storage(format!("Insert failed: {}", e)))
    }

    // Batch insert - groups by shard to minimize lock contention
    pub async fn put_batch_no_wal(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Group entries by shard
        let mut by_shard: [Vec<(Vec<u8>, Vec<u8>)>; NUM_SHARDS] = Default::default();
        for (key, value) in entries {
            let shard_idx = shard_for_key(&key);
            by_shard[shard_idx].push((key, value));
        }

        // Process each shard
        for (shard_idx, shard_entries) in by_shard.into_iter().enumerate() {
            if shard_entries.is_empty() {
                continue;
            }
            let mut tree = self.shards[shard_idx].lock();
            for (key, value) in shard_entries {
                tree.insert(key, value)
                    .map_err(|e| MonoError::Storage(format!("Batch insert failed: {}", e)))?;
            }
        }
        Ok(())
    }

    // Delete without WAL
    pub async fn delete_no_wal(&self, key: Vec<u8>) -> Result<()> {
        let mut tree = self.get_shard(&key).lock();
        tree.delete(&key)
            .map_err(|e| MonoError::Storage(format!("Delete failed: {}", e)))?;
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut tree = self.get_shard(key).lock();
        tree.get(&key.to_vec())
            .map_err(|e| MonoError::Storage(format!("Get failed: {}", e)))
    }

    // Scan [start, end) across all shards and merge results
    pub async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut all_results = Vec::new();

        // Scan all shards and merge results
        for shard in &self.shards {
            let mut tree = shard.lock();
            let results = tree
                .range(&start.to_vec(), &end.to_vec())
                .map_err(|e| MonoError::Storage(format!("Scan failed: {}", e)))?;
            all_results.extend(results);
        }

        // Sort by key for consistent ordering
        all_results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(all_results)
    }

    // Scan with result limit
    pub async fn scan_with_limit(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut all_results = Vec::new();

        // Scan all shards
        for shard in &self.shards {
            let mut tree = shard.lock();
            let results = tree
                .range(&start.to_vec(), &end.to_vec())
                .map_err(|e| MonoError::Storage(format!("Scan failed: {}", e)))?;
            all_results.extend(results);
        }

        // Sort by key for consistent ordering
        all_results.sort_by(|a, b| a.0.cmp(&b.0));

        if let Some(limit) = limit {
            all_results.truncate(limit);
        }
        Ok(all_results)
    }

    // Alias for scan_with_limit
    pub async fn scan_range_with_limit(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_with_limit(start, end, Some(limit)).await
    }

    // Reverse scan with limit
    pub async fn scan_reverse_with_limit(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut all_results = Vec::new();

        for shard in &self.shards {
            let mut tree = shard.lock();
            let results = tree
                .range(&start.to_vec(), &end.to_vec())
                .map_err(|e| MonoError::Storage(format!("Scan failed: {}", e)))?;
            all_results.extend(results);
        }

        // Sort by key descending
        all_results.sort_by(|a, b| b.0.cmp(&a.0));

        if let Some(limit) = limit {
            all_results.truncate(limit);
        }
        Ok(all_results)
    }

    // Flush all shards
    pub async fn flush(&self) -> Result<()> {
        for shard in &self.shards {
            let mut tree = shard.lock();
            tree.sync()
                .map_err(|e| MonoError::Storage(format!("Flush failed: {}", e)))?;
        }
        Ok(())
    }

    // Aggregate stats across all shards
    pub fn stats(&self) -> BufferPoolStats {
        let mut combined = BufferPoolStats {
            pool_size: 0,
            used_frames: 0,
            pinned_frames: 0,
            dirty_frames: 0,
            free_frames: 0,
        };
        for shard in &self.shards {
            let stats = shard.lock().stats();
            combined.pool_size += stats.pool_size;
            combined.used_frames += stats.used_frames;
            combined.pinned_frames += stats.pinned_frames;
            combined.dirty_frames += stats.dirty_frames;
            combined.free_frames += stats.free_frames;
        }
        combined
    }

    // Total entries across all shards
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.lock().is_empty())
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;

    #[test]
    fn test_btree_sequential_inserts() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("btree_test.db");

        let mut btree: BTree<i32, String> = BTree::new(&path, 1024)?;

        for i in 0..1000 {
            btree.insert(i, format!("value{}", i))?;
        }

        for i in 0..1000 {
            let value = btree.get(&i)?;
            assert_eq!(value, Some(format!("value{}", i)));
        }

        Ok(())
    }

    #[test]
    fn test_btree_random_inserts() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("btree_test_random.db");

        let mut btree: BTree<i32, String> = BTree::new(&path, 1024)?;

        let mut keys: Vec<i32> = (0..1000).collect();
        use rand::seq::SliceRandom;
        let mut rng = rand::rng();
        keys.shuffle(&mut rng);

        for &key in &keys {
            btree.insert(key, format!("value{}", key))?;
        }

        for i in 0..1000 {
            let value = btree.get(&i)?;
            assert_eq!(value, Some(format!("value{}", i)));
        }

        Ok(())
    }

    #[test]
    fn test_point_lookup() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("btree_test_lookup.db");

        let mut btree: BTree<i32, String> = BTree::new(&path, 1024)?;

        for i in 0..500 {
            btree.insert(i, format!("value{}", i))?;
        }

        for i in 0..500 {
            let value = btree.get(&i)?;
            assert_eq!(value, Some(format!("value{}", i)));
        }

        let missing = btree.get(&1000)?;
        assert_eq!(missing, None);

        Ok(())
    }

    #[test]
    fn range_scan_correctness() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("btree_test_range.db");

        let mut btree: BTree<i32, String> = BTree::new(&path, 1024)?;

        for i in 0..1000 {
            btree.insert(i, format!("value{}", i))?;
        }

        let results = btree.range(&200, &300)?;
        assert_eq!(results.len(), 100);
        for (i, (key, value)) in results.iter().enumerate() {
            assert_eq!(*key, 200 + i as i32);
            assert_eq!(value, &format!("value{}", 200 + i as i32));
        }

        Ok(())
    }

    #[test]
    fn page_split_during_insert() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("btree_test_split.db");

        let mut btree: BTree<i32, String> = BTree::new(&path, 1024)?;

        for i in 0..200 {
            btree.insert(i, format!("value{}", i))?;
        }

        for i in 0..200 {
            let value = btree.get(&i)?;
            assert_eq!(value, Some(format!("value{}", i)));
        }

        Ok(())
    }

    #[test]
    fn delete_operations() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("btree_test_delete.db");

        let mut btree: BTree<i32, String> = BTree::new(&path, 1024)?;

        for i in 0..100 {
            btree.insert(i, format!("value{}", i))?;
        }

        for i in 0..50 {
            let deleted = btree.delete(&i)?;
            assert_eq!(deleted, Some(format!("value{}", i)));
        }

        for i in 0..50 {
            let value = btree.get(&i)?;
            assert_eq!(value, None);
        }

        for i in 50..100 {
            let value = btree.get(&i)?;
            assert_eq!(value, Some(format!("value{}", i)));
        }

        Ok(())
    }

    // Sharded storage tests
    #[tokio::test]
    async fn test_shard_distribution() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("storage_tree_test.db");

        let storage_tree = StorageTree::new(&path, "test_tree".to_string(), 2048)?;

        let mut counts = [0; NUM_SHARDS];

        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let shard_idx = shard_for_key(&key);
            counts[shard_idx] += 1;
            storage_tree
                .put_no_wal(key, format!("value{}", i).into_bytes())
                .await?;
        }

        // Ensure that each shard has received some entries
        for (i, count) in counts.iter().enumerate() {
            assert!(*count > 0, "Shard {} received no entries", i);
        }

        Ok(())
    }

    #[tokio::test]
    async fn concurrent_access_across_shards() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("storage_tree_concurrent_test.db");

        use std::sync::Arc;
        let storage_tree = Arc::new(StorageTree::new(
            &path,
            "concurrent_tree".to_string(),
            4096,
        )?);

        let mut handles = Vec::new();

        for shard_idx in 0..NUM_SHARDS {
            let tree_clone = Arc::clone(&storage_tree);
            let handle = tokio::spawn(async move {
                for i in 0..250 {
                    let key = format!("shard{}_key{}", shard_idx, i).into_bytes();
                    let value = format!("shard{}_value{}", shard_idx, i).into_bytes();
                    tree_clone.put_no_wal(key, value).await.unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify entries
        for shard_idx in 0..NUM_SHARDS {
            for i in 0..250 {
                let key = format!("shard{}_key{}", shard_idx, i).into_bytes();
                let expected_value = format!("shard{}_value{}", shard_idx, i).into_bytes();
                let value = storage_tree.get(&key).await?;
                assert_eq!(value, Some(expected_value));
            }
        }

        Ok(())
    }
}

//! Buffer pool with LRU eviction and typed page caching.

// Serialization Helpers

use std::{collections::VecDeque, path::Path};

use crate::storage::{
    disk_manager::DiskManager,
    page::{DiskPage, PAGE_DATA_SIZE, PageId, Serializable},
};
use ahash::AHashMap;
use monodb_common::Result;

fn serialize_leaf_page<K: Serializable, V: Serializable>(
    keys: &[K],
    values: &[V],
    buf: &mut Vec<u8>,
) {
    buf.clear();
    (keys.len() as u32).serialize_to(buf);
    for key in keys {
        key.serialize_to(buf);
    }
    for value in values {
        value.serialize_to(buf);
    }
}

fn serialize_interior_page<K: Serializable>(keys: &[K], children: &[PageId], buf: &mut Vec<u8>) {
    buf.clear();
    (keys.len() as u32).serialize_to(buf);
    (children.len() as u32).serialize_to(buf);
    for key in keys {
        key.serialize_to(buf);
    }
    for child in children {
        child.0.serialize_to(buf);
    }
}

fn deserialize_leaf_page<K: Serializable, V: Serializable>(buf: &[u8]) -> Result<(Vec<K>, Vec<V>)> {
    if buf.len() < 4 {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut offset = 0;
    let (count, consumed) = u32::deserialize_from(&buf[offset..])?;
    offset += consumed;
    let count = count as usize;

    let mut keys = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);

    for _ in 0..count {
        if offset >= buf.len() {
            break;
        }
        let (key, consumed) = K::deserialize_from(&buf[offset..])?;
        offset += consumed;
        keys.push(key);
    }

    for _ in 0..count {
        if offset >= buf.len() {
            break;
        }
        let (value, consumed) = V::deserialize_from(&buf[offset..])?;
        offset += consumed;
        values.push(value);
    }

    Ok((keys, values))
}

fn deserialize_interior_page<K: Serializable>(buf: &[u8]) -> Result<(Vec<K>, Vec<PageId>)> {
    if buf.len() < 8 {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut offset = 0;
    let (key_count, consumed) = u32::deserialize_from(&buf[offset..])?;
    offset += consumed;
    let (child_count, consumed) = u32::deserialize_from(&buf[offset..])?;
    offset += consumed;

    let mut keys = Vec::with_capacity(key_count as usize);
    let mut children = Vec::with_capacity(child_count as usize);

    for _ in 0..key_count {
        if offset >= buf.len() {
            break;
        }
        let (key, consumed) = K::deserialize_from(&buf[offset..])?;
        offset += consumed;
        keys.push(key);
    }

    for _ in 0..child_count {
        if offset >= buf.len() {
            break;
        }
        let (child_id, consumed) = u32::deserialize_from(&buf[offset..])?;
        offset += consumed;
        children.push(PageId(child_id));
    }

    Ok((keys, children))
}

// LRU Replacer

/// LRU eviction polciy
#[allow(dead_code)]
pub struct LruReplacer {
    lru_list: VecDeque<PageId>,
    page_set: AHashMap<PageId, usize>,
}

impl LruReplacer {
    #[allow(dead_code)]
    pub fn new(capacity: usize) -> Self {
        Self {
            lru_list: VecDeque::with_capacity(capacity),
            page_set: AHashMap::with_capacity(capacity),
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn access(&mut self, page_id: PageId) {
        if self.page_set.contains_key(&page_id) {
            self.lru_list.retain(|&p| p != page_id);
        }
        self.lru_list.push_back(page_id);
        self.page_set.insert(page_id, self.lru_list.len() - 1);
    }

    #[inline]
    #[allow(dead_code)]
    pub fn pin(&mut self, page_id: PageId) {
        if self.page_set.remove(&page_id).is_some() {
            self.lru_list.retain(|&p| p != page_id);
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn unpin(&mut self, page_id: PageId) {
        if !self.page_set.contains_key(&page_id) {
            self.lru_list.push_back(page_id);
            self.page_set.insert(page_id, self.lru_list.len() - 1);
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn victim(&mut self) -> Option<PageId> {
        let page_id = self.lru_list.pop_front()?;
        self.page_set.remove(&page_id);
        Some(page_id)
    }

    #[inline]
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.lru_list.len()
    }
}

// Buffer Pool Statistics

#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    pub pool_size: usize,
    pub used_frames: usize,
    pub pinned_frames: usize,
    pub dirty_frames: usize,
    pub free_frames: usize,
}

// Typed Page for In-Memory Caching

/// A typed page that holds deserialized data in memory
#[derive(Clone)]
pub struct TypedPage<K, V> {
    pub page_id: PageId,
    pub is_leaf: bool,
    pub keys: Vec<K>,
    pub values: Vec<V>,
    pub children: Vec<PageId>,
    pub next_leaf: PageId,
    pub dirty: bool,
}

impl<K, V> TypedPage<K, V> {
    fn new_leaf(page_id: PageId) -> Self {
        Self {
            page_id,
            is_leaf: true,
            keys: Vec::with_capacity(128),
            values: Vec::with_capacity(128),
            children: Vec::new(),
            next_leaf: PageId::INVALID,
            dirty: true,
        }
    }

    fn new_interior(page_id: PageId) -> Self {
        Self {
            page_id,
            is_leaf: false,
            keys: Vec::with_capacity(128),
            values: Vec::new(),
            children: Vec::with_capacity(129),
            next_leaf: PageId::INVALID,
            dirty: true,
        }
    }
}

impl<K: Serializable, V: Serializable> TypedPage<K, V> {
    /// Calculate the serialized size of this page's data
    fn serialized_size(&self) -> usize {
        if self.is_leaf {
            // Leaf format: count (4) + keys + values
            4 + self.keys.iter().map(|k| k.serialized_size()).sum::<usize>()
                + self
                    .values
                    .iter()
                    .map(|v| v.serialized_size())
                    .sum::<usize>()
        } else {
            // Interior format: key_count (4) + child_count (4) + keys + children (4 bytes each)
            4 + 4
                + self.keys.iter().map(|k| k.serialized_size()).sum::<usize>()
                + self.children.len() * 4
        }
    }

    /// Calculate the size if we added a new key-value entry
    fn size_with_entry(&self, key: &K, value: &V) -> usize {
        self.serialized_size() + key.serialized_size() + value.serialized_size()
    }

    /// Check if adding an entry would exceed the page capacity
    pub fn would_overflow(&self, key: &K, value: &V) -> bool {
        self.size_with_entry(key, value) > PAGE_DATA_SIZE
    }

    /// Find the split point that divides the page roughly in half by size
    pub fn find_size_split_point(&self) -> usize {
        let key_count = self.keys.len();
        if key_count < 2 {
            // Can't split with less than 2 keys
            return 1;
        }

        let total_size = self.serialized_size();
        let target_size = total_size / 2;

        let mut cumulative_size = 4; // count prefix
        for (i, (k, v)) in self.keys.iter().zip(self.values.iter()).enumerate() {
            cumulative_size += k.serialized_size() + v.serialized_size();
            if cumulative_size >= target_size && i > 0 {
                return i;
            }
        }
        // Fall back to middle, ensuring we stay in valid range [1, len-1]
        (key_count / 2).max(1).min(key_count - 1)
    }
}

fn typed_to_disk<K: Serializable, V: Serializable>(typed: &TypedPage<K, V>) -> DiskPage {
    let mut disk = if typed.is_leaf {
        DiskPage::new_leaf(typed.page_id)
    } else {
        DiskPage::new_interior(typed.page_id)
    };

    disk.key_count = typed.keys.len() as u16;
    disk.next_leaf = typed.next_leaf;

    if typed.is_leaf {
        serialize_leaf_page(&typed.keys, &typed.values, &mut disk.data);
    } else {
        serialize_interior_page(&typed.keys, &typed.children, &mut disk.data);
    }

    disk
}

fn disk_to_typed<K: Serializable, V: Serializable>(disk: &DiskPage) -> Result<TypedPage<K, V>> {
    let mut typed = if disk.is_leaf() {
        TypedPage::new_leaf(disk.page_id)
    } else {
        TypedPage::new_interior(disk.page_id)
    };

    typed.next_leaf = disk.next_leaf;
    typed.dirty = false;

    if disk.is_leaf() {
        let (keys, values) = deserialize_leaf_page(&disk.data)?;
        typed.keys = keys;
        typed.values = values;
    } else {
        let (keys, children) = deserialize_interior_page(&disk.data)?;
        typed.keys = keys;
        typed.children = children;
    }

    Ok(typed)
}

// Typed Page Store with LRU Eviction (TODO: LRU-K)

/// A page store that keeps typed pages in memory with LRU eviction to disk
pub struct TypedPageStore<K: Clone + Serializable, V: Clone + Serializable> {
    pages: AHashMap<PageId, TypedPage<K, V>>,
    lru: VecDeque<PageId>,
    disk_manager: DiskManager,
    max_pages: usize,
    next_page_id: u32,
}

impl<K: Clone + Serializable, V: Clone + Serializable> TypedPageStore<K, V> {
    pub fn new<P: AsRef<Path>>(path: P, max_pages: usize) -> Result<Self> {
        let disk_manager = DiskManager::new(path)?;
        let next_page_id = disk_manager.num_pages();

        Ok(Self {
            pages: AHashMap::with_capacity(max_pages),
            lru: VecDeque::with_capacity(max_pages),
            disk_manager,
            max_pages,
            next_page_id,
        })
    }

    #[inline]
    pub fn alloc_leaf(&mut self) -> Result<PageId> {
        let page_id = PageId(self.next_page_id);
        self.next_page_id += 1;

        let page = TypedPage::new_leaf(page_id);
        self.insert_page(page)?;

        Ok(page_id)
    }

    #[inline]
    pub fn alloc_interior(&mut self) -> Result<PageId> {
        let page_id = PageId(self.next_page_id);
        self.next_page_id += 1;

        let page = TypedPage::new_interior(page_id);
        self.insert_page(page)?;

        Ok(page_id)
    }

    #[inline]
    fn insert_page(&mut self, page: TypedPage<K, V>) -> Result<()> {
        let page_id = page.page_id;

        while self.pages.len() >= self.max_pages {
            self.evict_one()?;
        }

        self.pages.insert(page_id, page);
        self.lru.push_back(page_id);

        Ok(())
    }

    fn evict_one(&mut self) -> Result<()> {
        if let Some(victim_id) = self.lru.pop_front()
            && let Some(page) = self.pages.remove(&victim_id)
            && page.dirty
        {
            let disk_page = typed_to_disk(&page);
            self.disk_manager.write_page(&disk_page)?;
        }
        Ok(())
    }

    #[inline]
    pub fn get(&mut self, page_id: PageId) -> Result<&TypedPage<K, V>> {
        if self.pages.contains_key(&page_id) {
            return Ok(self.pages.get(&page_id).unwrap());
        }

        let disk_page = self.disk_manager.read_page(page_id)?;
        let typed = disk_to_typed(&disk_page)?;
        self.insert_page(typed)?;

        Ok(self.pages.get(&page_id).unwrap())
    }

    #[inline]
    pub fn get_mut(&mut self, page_id: PageId) -> Result<&mut TypedPage<K, V>> {
        if !self.pages.contains_key(&page_id) {
            let disk_page = self.disk_manager.read_page(page_id)?;
            let typed = disk_to_typed(&disk_page)?;
            self.insert_page(typed)?;
        }

        let page = self.pages.get_mut(&page_id).unwrap();
        page.dirty = true;
        Ok(page)
    }

    pub fn flush_all(&mut self) -> Result<()> {
        for page in self.pages.values() {
            if page.dirty {
                let disk_page = typed_to_disk(page);
                self.disk_manager.write_page(&disk_page)?;
            }
        }
        self.disk_manager.sync()?;

        for page in self.pages.values_mut() {
            page.dirty = false;
        }

        Ok(())
    }

    pub fn page_count(&self) -> u32 {
        self.next_page_id
    }

    pub fn stats(&self) -> BufferPoolStats {
        let dirty = self.pages.values().filter(|p| p.dirty).count();
        BufferPoolStats {
            pool_size: self.max_pages,
            used_frames: self.pages.len(),
            pinned_frames: 0,
            dirty_frames: dirty,
            free_frames: self.max_pages.saturating_sub(self.pages.len()),
        }
    }
}

impl<K: Clone + Serializable, V: Clone + Serializable> Drop for TypedPageStore<K, V> {
    fn drop(&mut self) {
        let _ = self.flush_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monodb_common::Result;

    /// Create auto-cleanup test directory that deletes itself when dropped
    struct TestDir {
        path: std::path::PathBuf,
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn create_test_dir(name: &str) -> TestDir {
        let test_dir = std::path::PathBuf::from(format!("test_data/{}", name));
        if test_dir.exists() {
            std::fs::remove_dir_all(&test_dir).unwrap();
        }
        std::fs::create_dir_all(&test_dir).unwrap();
        TestDir { path: test_dir }
    }

    // TypedPageStore tests

    #[test]
    fn page_allocation_and_caching() -> Result<()> {
        let test_dir = create_test_dir("page_allocation_and_caching");
        let db_path = test_dir.path.join("test_db");
        let mut store: TypedPageStore<u32, String> = TypedPageStore::new(&db_path, 2)?;

        let page_id1 = store.alloc_leaf()?;
        let page_id2 = store.alloc_interior()?;

        {
            let leaf_page = store.get_mut(page_id1)?;
            leaf_page.keys.push(1);
            leaf_page.values.push("value1".to_string());
        }

        {
            let interior_page = store.get_mut(page_id2)?;
            interior_page.keys.push(10);
            interior_page.children.push(PageId(3));
        }

        // Access pages to ensure they are cached
        let _ = store.get(page_id1)?;
        let _ = store.get(page_id2)?;

        Ok(())
    }

    #[test]
    fn lru_eviction() -> Result<()> {
        let test_dir = create_test_dir("lru_eviction");
        let db_path = test_dir.path.join("test_db");
        let mut store: TypedPageStore<u32, String> = TypedPageStore::new(&db_path, 2)?;

        let page_id1 = store.alloc_leaf()?;
        let page_id2 = store.alloc_interior()?;

        // Access both pages
        let _ = store.get(page_id1)?;
        let _ = store.get(page_id2)?;

        // Allocate a third page, should evict the least recently used (page_id1)
        let page_id3 = store.alloc_leaf()?;

        assert!(store.pages.contains_key(&page_id2));
        assert!(store.pages.contains_key(&page_id3));
        assert!(!store.pages.contains_key(&page_id1));

        Ok(())
    }

    #[test]
    fn dirty_page_flushing() -> Result<()> {
        let test_dir = create_test_dir("dirty_page_flushing");
        let db_path = test_dir.path.join("test_db");
        let mut store: TypedPageStore<u32, String> = TypedPageStore::new(&db_path, 2)?;

        let page_id = store.alloc_leaf()?;

        {
            let leaf_page = store.get_mut(page_id)?;
            leaf_page.keys.push(1);
            leaf_page.values.push("value1".to_string());
        }

        store.flush_all()?;

        let disk_page = store.disk_manager.read_page(page_id)?;
        let restored_leaf = disk_to_typed::<u32, String>(&disk_page)?;

        assert_eq!(restored_leaf.keys, vec![1]);
        assert_eq!(restored_leaf.values, vec!["value1".to_string()]);

        Ok(())
    }

    #[test]
    fn cache_hit_rate() -> Result<()> {
        let test_dir = create_test_dir("cache_hit_rate");
        let db_path = test_dir.path.join("test_db");
        let mut store: TypedPageStore<u32, String> = TypedPageStore::new(&db_path, 2)?;

        let page_id = store.alloc_leaf()?;

        // First access, should be a miss
        let _ = store.get(page_id)?;

        // Second access, should be a hit
        let _ = store.get(page_id)?;

        assert_eq!(store.stats().used_frames, 1);

        Ok(())
    }

    #[test]
    fn disk_page_round_trip() -> Result<()> {
        let mut leaf_page = TypedPage::new_leaf(PageId(1));
        leaf_page.keys = vec![1u32, 2, 3];
        leaf_page.values = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let disk_page = typed_to_disk(&leaf_page);
        let restored_leaf = disk_to_typed::<u32, String>(&disk_page)?;

        assert_eq!(leaf_page.keys, restored_leaf.keys);
        assert_eq!(leaf_page.values, restored_leaf.values);
        assert!(restored_leaf.is_leaf);

        let mut interior_page: TypedPage<u32, String> = TypedPage::new_interior(PageId(2));
        interior_page.keys = vec![10u32, 20, 30];
        interior_page.children = vec![PageId(3), PageId(4), PageId(5), PageId(6)];

        let disk_page = typed_to_disk(&interior_page);
        let restored_interior = disk_to_typed::<u32, String>(&disk_page)?;

        assert_eq!(interior_page.keys, restored_interior.keys);
        assert_eq!(interior_page.children, restored_interior.children);
        assert!(!restored_interior.is_leaf);

        Ok(())
    }

    #[test]
    fn checksum_validation() -> Result<()> {
        let mut leaf_page = TypedPage::new_leaf(PageId(1));
        leaf_page.keys = vec![1u32, 2, 3];
        leaf_page.values = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let disk_page = typed_to_disk(&leaf_page);
        let mut corrupted_disk_page = disk_page.clone();
        corrupted_disk_page.data[0] ^= 0xFF; // Corrupt the data

        let result = disk_to_typed::<u32, String>(&corrupted_disk_page);
        assert!(
            result.is_err(),
            "Checksum validation should fail for corrupted data"
        );

        Ok(())
    }

    #[test]
    fn variable_length_key_serialization() -> Result<()> {
        let mut leaf_page = TypedPage::new_leaf(PageId(1));
        leaf_page.keys = vec![
            "short".to_string(),
            "a bit longer key".to_string(),
            "the longest key of all three".to_string(),
        ];
        leaf_page.values = vec!["val1".to_string(), "val2".to_string(), "val3".to_string()];

        let disk_page = typed_to_disk(&leaf_page);
        let restored_leaf = disk_to_typed::<String, String>(&disk_page)?;

        assert_eq!(leaf_page.keys, restored_leaf.keys);
        assert_eq!(leaf_page.values, restored_leaf.values);
        assert!(restored_leaf.is_leaf);

        Ok(())
    }
}

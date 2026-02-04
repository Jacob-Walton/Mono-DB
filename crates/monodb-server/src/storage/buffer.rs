//! Buffer pool for page caching.
//!
//! Implements clock-sweep eviction policy with concurrent access support.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;
use monodb_common::{MonoError, Result};
use parking_lot::{Mutex, RwLock};

use super::disk::DiskManager;
use super::page::{DiskPage, PAGE_SIZE, PageId};
use super::traits::BufferPool as BufferPoolTrait;

// Buffer frame for holding pages

/// A frame in the buffer pool holding a single page.
pub struct BufferFrame {
    /// The page data.
    pub page: RwLock<DiskPage>,
    /// The page ID.
    pub page_id: PageId,
    /// Reference bit for clock-sweep eviction.
    pub referenced: AtomicBool,
    /// Whether the page has been modified.
    pub dirty: AtomicBool,
    /// Pin count (pinned pages cannot be evicted).
    pub pin_count: AtomicUsize,
}

impl BufferFrame {
    /// Create a new buffer frame with the given page.
    pub fn new(page: DiskPage) -> Self {
        let page_id = page.header.page_id;
        Self {
            page: RwLock::new(page),
            page_id,
            referenced: AtomicBool::new(true),
            dirty: AtomicBool::new(false),
            pin_count: AtomicUsize::new(0),
        }
    }

    /// Check if this frame can be evicted.
    #[inline]
    pub fn is_evictable(&self) -> bool {
        self.pin_count.load(Ordering::Acquire) == 0
    }

    /// Mark as referenced (accessed).
    #[inline]
    pub fn touch(&self) {
        self.referenced.store(true, Ordering::Release);
    }

    /// Mark as dirty (modified).
    #[inline]
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Clear dirty flag.
    #[inline]
    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    /// Check if dirty.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Increment pin count.
    #[inline]
    pub fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement pin count.
    #[inline]
    pub fn unpin(&self) {
        self.pin_count.fetch_sub(1, Ordering::AcqRel);
    }
}

// Buffer pool statistics

/// Statistics for the buffer pool.
#[derive(Debug, Clone, Default)]
pub struct BufferPoolStats {
    /// Total capacity in frames.
    pub capacity: usize,
    /// Number of frames in use.
    pub used: usize,
    /// Number of pinned frames.
    pub pinned: usize,
    /// Number of dirty frames.
    pub dirty: usize,
    /// Cache hits.
    pub hits: u64,
    /// Cache misses.
    pub misses: u64,
    /// Number of evictions.
    pub evictions: u64,
    /// Number of flushes.
    pub flushes: u64,
}

// Clock-sweep eviction policy

/// Clock-sweep eviction policy approximating LRU with circular frame list.
struct ClockReplacer {
    /// Frame IDs in the pool.
    frames: Vec<PageId>,
    /// Current position of the clock hand.
    hand: AtomicUsize,
    /// Capacity.
    capacity: usize,
}

impl ClockReplacer {
    fn new(capacity: usize) -> Self {
        Self {
            frames: Vec::with_capacity(capacity),
            hand: AtomicUsize::new(0),
            capacity,
        }
    }

    /// Add a frame to the replacer.
    fn add(&mut self, page_id: PageId) {
        if self.frames.len() < self.capacity {
            self.frames.push(page_id);
        }
    }

    /// Remove a frame from the replacer.
    fn remove(&mut self, page_id: PageId) {
        if let Some(pos) = self.frames.iter().position(|&id| id == page_id) {
            self.frames.remove(pos);
            // Adjust hand if needed
            let hand = self.hand.load(Ordering::Relaxed);
            if pos < hand && hand > 0 {
                self.hand.store(hand - 1, Ordering::Relaxed);
            }
        }
    }

    /// Find a victim frame for eviction using clock-sweep algorithm.
    fn find_victim<F>(&self, is_evictable: F) -> Option<PageId>
    where
        F: Fn(PageId) -> (bool, bool), // (is_evictable, is_referenced)
    {
        if self.frames.is_empty() {
            return None;
        }

        let len = self.frames.len();
        let start = self.hand.load(Ordering::Relaxed) % len;
        let mut pos = start;
        let mut rounds = 0;

        // Allow up to 2 full rotations
        while rounds < 2 {
            let page_id = self.frames[pos];
            let (evictable, referenced) = is_evictable(page_id);

            if evictable && !referenced {
                // Found a victim
                self.hand.store((pos + 1) % len, Ordering::Relaxed);
                return Some(page_id);
            }
            // Clear reference bit and continue

            pos = (pos + 1) % len;
            if pos == start {
                rounds += 1;
            }
        }

        None
    }
}

// Lru Buffer Pool

/// Thread-safe buffer pool with clock-sweep eviction.
pub struct LruBufferPool {
    /// Cached frames indexed by page ID.
    frames: DashMap<PageId, Arc<BufferFrame>>,
    /// The underlying disk manager.
    disk: Arc<DiskManager>,
    /// Clock-sweep replacer.
    replacer: Mutex<ClockReplacer>,
    /// Maximum number of frames.
    capacity: usize,
    /// Statistics.
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    flushes: AtomicU64,
}

impl LruBufferPool {
    /// Create a new buffer pool with the given capacity and disk manager.
    pub fn new(disk: Arc<DiskManager>, capacity: usize) -> Self {
        Self {
            frames: DashMap::with_capacity(capacity),
            disk,
            replacer: Mutex::new(ClockReplacer::new(capacity)),
            capacity,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
        }
    }

    /// Get the buffer pool statistics.
    pub fn stats(&self) -> BufferPoolStats {
        let mut pinned = 0;
        let mut dirty = 0;

        for entry in self.frames.iter() {
            if entry.value().pin_count.load(Ordering::Relaxed) > 0 {
                pinned += 1;
            }
            if entry.value().is_dirty() {
                dirty += 1;
            }
        }

        BufferPoolStats {
            capacity: self.capacity,
            used: self.frames.len(),
            pinned,
            dirty,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
        }
    }

    /// Evict a page if the pool is at capacity.
    fn maybe_evict(&self) -> Result<()> {
        if self.frames.len() < self.capacity {
            return Ok(());
        }

        // Find a victim using clock-sweep
        let victim = {
            let replacer = self.replacer.lock();
            replacer.find_victim(|page_id| {
                if let Some(frame) = self.frames.get(&page_id) {
                    let evictable = frame.is_evictable();
                    let referenced = frame.referenced.swap(false, Ordering::AcqRel);
                    (evictable, referenced)
                } else {
                    (false, false)
                }
            })
        };

        if let Some(victim_id) = victim {
            // Flush if dirty before evicting
            if let Some(frame) = self.frames.get(&victim_id)
                && frame.is_dirty()
            {
                self.flush_frame(&frame)?;
            }

            // Remove from pool and replacer
            self.frames.remove(&victim_id);
            self.replacer.lock().remove(victim_id);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        } else if self.frames.len() >= self.capacity {
            return Err(MonoError::BufferPoolExhausted(self.frames.len()));
        }

        Ok(())
    }

    /// Flush a single frame to disk.
    fn flush_frame(&self, frame: &BufferFrame) -> Result<()> {
        if !frame.is_dirty() {
            return Ok(());
        }

        let page_guard = frame.page.read();
        let bytes = page_guard.to_bytes();
        self.disk.write(frame.page_id, &bytes)?;
        frame.clear_dirty();
        self.flushes.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Load a page from disk into the buffer pool.
    fn load_page(&self, page_id: PageId) -> Result<Arc<BufferFrame>> {
        // Check if already loaded (race condition)
        if let Some(frame) = self.frames.get(&page_id) {
            frame.touch();
            return Ok(frame.clone());
        }

        // Ensure we have space
        self.maybe_evict()?;

        // Read from disk
        let page_bytes = self.disk.read(page_id)?;
        let page = DiskPage::from_bytes(&page_bytes)?;

        // Create frame
        let frame = Arc::new(BufferFrame::new(page));

        // Add to pool
        self.frames.insert(page_id, frame.clone());
        self.replacer.lock().add(page_id);
        self.misses.fetch_add(1, Ordering::Relaxed);

        Ok(frame)
    }

    /// Get a frame, loading from disk if necessary.
    pub fn get_frame(&self, page_id: PageId) -> Result<Arc<BufferFrame>> {
        // Check cache first
        if let Some(frame) = self.frames.get(&page_id) {
            frame.touch();
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(frame.clone());
        }

        // Load from disk
        self.load_page(page_id)
    }

    /// Create a new page in the buffer pool.
    pub fn new_page(&self, page: DiskPage) -> Result<Arc<BufferFrame>> {
        let page_id = page.header.page_id;

        // Ensure we have space
        self.maybe_evict()?;

        // Create frame
        let frame = Arc::new(BufferFrame::new(page));
        frame.mark_dirty(); // New pages are dirty

        // Add to pool
        self.frames.insert(page_id, frame.clone());
        self.replacer.lock().add(page_id);

        Ok(frame)
    }

    /// Allocate a new leaf page.
    pub fn alloc_leaf(&self) -> Result<Arc<BufferFrame>> {
        let page_id = self.disk.allocate()?;
        let page = DiskPage::new_leaf(page_id);
        self.new_page(page)
    }

    /// Allocate a new interior page.
    pub fn alloc_interior(&self) -> Result<Arc<BufferFrame>> {
        let page_id = self.disk.allocate()?;
        let page = DiskPage::new_interior(page_id);
        self.new_page(page)
    }

    /// Allocate a new metadata page.
    pub fn alloc_meta(&self) -> Result<Arc<BufferFrame>> {
        let page_id = self.disk.allocate()?;
        let page = DiskPage::new_meta(page_id);
        self.new_page(page)
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&self) -> Result<()> {
        for entry in self.frames.iter() {
            self.flush_frame(&entry)?;
        }
        self.disk.sync()?;
        Ok(())
    }

    /// Flush a specific page to disk.
    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        if let Some(frame) = self.frames.get(&page_id) {
            self.flush_frame(&frame)?;
        }
        Ok(())
    }

    /// Check if a page is in the buffer pool.
    pub fn contains(&self, page_id: &PageId) -> bool {
        self.frames.contains_key(page_id)
    }

    /// Get the number of frames in the pool.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

// BufferPool Trait Implementation

impl BufferPoolTrait for LruBufferPool {
    fn get(&self, page_id: PageId) -> Result<Vec<u8>> {
        let frame = self.get_frame(page_id)?;
        let page = frame.page.read();
        Ok(page.to_bytes().to_vec())
    }

    fn get_mut(&self, page_id: PageId) -> Result<Vec<u8>> {
        let frame = self.get_frame(page_id)?;
        frame.mark_dirty();
        let page = frame.page.read();
        Ok(page.to_bytes().to_vec())
    }

    fn pin(&self, page_id: PageId) -> Result<()> {
        if let Some(frame) = self.frames.get(&page_id) {
            frame.pin();
        }
        Ok(())
    }

    fn unpin(&self, page_id: PageId) -> Result<()> {
        if let Some(frame) = self.frames.get(&page_id) {
            frame.unpin();
        }
        Ok(())
    }

    fn mark_dirty(&self, page_id: PageId) -> Result<()> {
        if let Some(frame) = self.frames.get(&page_id) {
            frame.mark_dirty();
        }
        Ok(())
    }

    fn put(&self, page_id: PageId, data: Vec<u8>) -> Result<()> {
        let page_bytes: [u8; PAGE_SIZE] = data
            .try_into()
            .map_err(|_| MonoError::Storage("invalid page size".into()))?;
        let page = DiskPage::from_bytes(&page_bytes)?;

        if let Some(frame) = self.frames.get(&page_id) {
            *frame.page.write() = page;
            frame.mark_dirty();
        } else {
            self.new_page(page)?;
        }

        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.flush_all()
    }

    fn flush_page(&self, page_id: PageId) -> Result<()> {
        self.flush_page(page_id)
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_pool(capacity: usize) -> LruBufferPool {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let disk = Arc::new(DiskManager::open(&path).unwrap());
        // Leak the tempdir to keep the file around
        std::mem::forget(dir);
        LruBufferPool::new(disk, capacity)
    }

    #[test]
    fn test_alloc_and_get() {
        let pool = create_pool(100);

        let frame = pool.alloc_leaf().unwrap();
        let page_id = frame.page_id;

        // Should be in cache
        let frame2 = pool.get_frame(page_id).unwrap();
        assert_eq!(frame.page_id, frame2.page_id);

        let stats = pool.stats();
        assert_eq!(stats.used, 1);
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_eviction() {
        let pool = create_pool(4);

        // Fill the pool
        let mut page_ids = Vec::new();
        for _ in 0..4 {
            let frame = pool.alloc_leaf().unwrap();
            page_ids.push(frame.page_id);
        }

        assert_eq!(pool.stats().used, 4);

        // Allocate one more, should trigger eviction
        let _frame = pool.alloc_leaf().unwrap();

        // Should have evicted one
        assert_eq!(pool.stats().used, 4);
        assert_eq!(pool.stats().evictions, 1);
    }

    #[test]
    fn test_pin_prevents_eviction() {
        let pool = create_pool(2);

        // Allocate and pin first page
        let frame1 = pool.alloc_leaf().unwrap();
        frame1.pin();

        // Allocate second page
        let _frame2 = pool.alloc_leaf().unwrap();

        // Try to allocate third, should evict frame2, not frame1
        let frame3 = pool.alloc_leaf().unwrap();

        // Frame1 should still be in cache
        assert!(pool.frames.contains_key(&frame1.page_id));
        // Frame3 should be in cache
        assert!(pool.frames.contains_key(&frame3.page_id));
    }

    #[test]
    fn test_flush() {
        let pool = create_pool(100);

        let frame = pool.alloc_leaf().unwrap();
        frame.mark_dirty();

        assert!(frame.is_dirty());

        pool.flush_page(frame.page_id).unwrap();

        assert!(!frame.is_dirty());
    }
}

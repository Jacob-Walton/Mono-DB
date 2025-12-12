use monodb_common::{MonoError, Result};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::storage::{
    disk_manager::DiskManager,
    page::{Page, PageId, PageType},
};

/// Number of recent accesses to track per page.
/// K=2 is optimal for most database workloads.
const K: usize = 2;

/// Represents infinity for pages with fewer than K accesses.
/// These pages are evicted first as they're likely from sequential scans.
const INFINITY_DISTANCE: u64 = u64::MAX;

/// A frame in the buffer pool holding a single page.
struct Frame {
    /// The cached page, if any
    page: Option<Arc<RwLock<Page>>>,
    /// Whether the page has been modified since loading
    is_dirty: bool,
    /// Number of active references to this page
    pin_count: usize,
    /// Timestamps of the K most recent accesses (front = oldest, back = newest)
    access_history: VecDeque<u64>,
}

impl Frame {
    /// Create a new empty frame
    fn new() -> Self {
        Self {
            page: None,
            is_dirty: false,
            pin_count: 0,
            access_history: VecDeque::with_capacity(K),
        }
    }

    /// Check if this frame can be evicted (not pinned)
    fn is_evictable(&self) -> bool {
        self.pin_count == 0
    }

    /// Record an access at the given timestamp.
    /// Maintains only the K most recent timestamps.
    fn record_access(&mut self, timestamp: u64) {
        if self.access_history.len() >= K {
            self.access_history.pop_front(); // Remove oldest
        }
        self.access_history.push_back(timestamp); // Add newest
    }

    /// Compute the backward K-distance at the given current timestamp.
    ///
    /// Returns INFINITY_DISTNACE if fewer than K accesses have occurred,
    /// otherwise returns current_ts - Kth_most_recent_accesses
    fn backward_k_distance(&self, current_ts: u64) -> u64 {
        if self.access_history.len() < K {
            // Fewer than K accesses, treat as infinity (first to evict)
            INFINITY_DISTANCE
        } else {
            // K-distance = time since Kth most recent access
            current_ts.saturating_sub(*self.access_history.front().unwrap())
        }
    }

    /// Reset the frame to empty state
    fn reset(&mut self) {
        self.page = None;
        self.is_dirty = false;
        self.pin_count = 0;
        self.access_history.clear();
    }
}

/// LRU-K Buffer Pool Manager
///
/// Manages a fixed-size pool of page frames using the LRU-K replacement policy.
/// LRU-K tracks the K most recent accesses to each page and evicts the page
/// with the largest "backward K-distance" (time since Kth most recent access).
///
/// Benefits over simple LRU:
///  * Pages accessed once (usually from sequential scans) are evicted quickly
///  * Pages accessed repeatedly (typically working set) are kept in memory
pub struct BufferPool {
    /// Page frames managed by this pool
    frames: Vec<Arc<RwLock<Frame>>>,
    /// Maps PageId to frame index [O(1)]
    page_table: Arc<RwLock<HashMap<PageId, usize>>>,
    /// List of free frame indices
    free_list: Arc<Mutex<VecDeque<usize>>>,
    /// Disk manager for page I/O
    disk_manager: Arc<DiskManager>,
    /// Global logical timestamp counter
    current_timestamp: AtomicU64,
    /// Total number of frames in the pool
    #[allow(unused)]
    pool_size: usize,
}

impl BufferPool {
    /// Create a new LRU-K buffer pool.
    ///
    /// # Arguments
    /// * `pool_size` - Number of page frames to allocate
    /// * `disk_manager` - Disk manager for page I/O
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = VecDeque::with_capacity(pool_size);

        // Initialize all frames as empty and add to free list
        for i in 0..pool_size {
            frames.push(Arc::new(RwLock::new(Frame::new())));
            free_list.push_back(i);
        }

        #[cfg(debug_assertions)]
        tracing::debug!("LRU-{K} buffer pool initialized with {pool_size} frames");

        Self {
            frames,
            page_table: Arc::new(RwLock::new(HashMap::new())),
            free_list: Arc::new(Mutex::new(free_list)),
            disk_manager,
            current_timestamp: AtomicU64::new(0),
            pool_size,
        }
    }

    /// Get the next logical timestamp (monotonically increasing)
    fn next_timestamp(&self) -> u64 {
        self.current_timestamp.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current timestamp without incrementing
    fn get_current_timestamp(&self) -> u64 {
        self.current_timestamp.load(Ordering::SeqCst)
    }

    /// Expose root page metadata helpers via DiskManager
    pub fn get_root_page_id(&self) -> Option<PageId> {
        self.disk_manager.get_root_page_id()
    }

    pub fn set_root_page_id(&self, root: Option<PageId>) -> Result<()> {
        self.disk_manager.set_root_page_id(root)
    }

    /// Ensure all underlying files are synced to stable storage
    pub fn sync(&self) -> Result<()> {
        self.disk_manager.sync()
    }

    /// Fetch a page from the buffer pool, loading from disk if necessary.
    ///
    /// The returned page is pinned and must be unpinned when done.
    /// Each fetch records an access timestamp for LRU-K tracking.
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to fetch
    ///
    /// # Returns
    /// An Arc to the page wrapped in RwLock
    pub fn fetch_page(&self, page_id: PageId) -> Result<Arc<RwLock<Page>>> {
        let timestamp = self.next_timestamp();

        // Check if the page is already in buffer pool
        let frame_idx_opt = {
            let page_table = self.page_table.read();
            page_table.get(&page_id).copied()
        };

        if let Some(frame_idx) = frame_idx_opt {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();

            // Increment pin count and record access
            frame_guard.pin_count += 1;
            frame_guard.record_access(timestamp);

            #[cfg(debug_assertions)]
            tracing::trace!(
                "Page {} hit in frame {} (accesses: {}, pin_count: {})",
                page_id.0,
                frame_idx,
                frame_guard.access_history.len(),
                frame_guard.pin_count
            );

            if let Some(page) = &frame_guard.page {
                return Ok(Arc::clone(page));
            }
        }

        // Page not in buffer pool, load from disk
        self.load_page_from_disk(page_id, timestamp)
    }

    /// Create a new page in the buffer pool.
    ///
    /// # Arguments
    /// * `page_type` - Type of page to create (Leaf, Interior, etc.)
    ///
    /// # Returns
    /// An Arc to the newly created page
    pub fn new_page(&self, page_type: PageType) -> Result<Arc<RwLock<Page>>> {
        let timestamp = self.next_timestamp();

        // Allocate a new page ID
        let page_id = self.disk_manager.allocate_page()?;

        // Get a frame for the new page
        let frame_idx = self.get_frame()?;

        // Create the new page
        let page = Arc::new(RwLock::new(Page::new(page_type, page_id)));

        // Install page in frame
        {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();
            frame_guard.page = Some(Arc::clone(&page));
            frame_guard.is_dirty = true;
            frame_guard.pin_count = 1;
            frame_guard.access_history.clear();
            frame_guard.record_access(timestamp);
        }

        // Update page table
        self.page_table.write().insert(page_id, frame_idx);

        #[cfg(debug_assertions)]
        tracing::trace!("Created new page {} in frame {}", page_id.0, frame_idx);

        Ok(page)
    }

    /// Unpin a page, optionally marking it as dirty.
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to unpin
    /// * `is_dirty` - Whether the page was modified
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let page_table = self.page_table.read();

        if let Some(&frame_idx) = page_table.get(&page_id) {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();

            if frame_guard.pin_count == 0 {
                tracing::warn!("Attempted to unpin page {} with pin_count=0", page_id.0);
                return Ok(());
            }

            frame_guard.pin_count -= 1;

            if is_dirty {
                frame_guard.is_dirty = true;
            }

            #[cfg(debug_assertions)]
            tracing::trace!(
                "Unpinned page {} (pin_count={}, dirty={}, k_accesses={})",
                page_id.0,
                frame_guard.pin_count,
                frame_guard.is_dirty,
                frame_guard.access_history.len()
            );
        } else {
            return Err(MonoError::Storage(format!(
                "Page {} not in buffer pool",
                page_id.0
            )));
        }

        Ok(())
    }

    /// Flush a specific page to disk if dirty.
    #[allow(dead_code)]
    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let page_table = self.page_table.read();

        if let Some(&frame_idx) = page_table.get(&page_id) {
            let frame = self.frames[frame_idx].clone();
            let frame_guard = frame.write();

            if frame_guard.is_dirty && frame_guard.page.is_some() {
                let page_arc = Arc::clone(frame_guard.page.as_ref().unwrap());
                drop(frame_guard);

                let page_guard = page_arc.read();
                self.disk_manager.write_page(&page_guard)?;
                drop(page_guard);

                let mut frame_guard = frame.write();
                frame_guard.is_dirty = false;

                #[cfg(debug_assertions)]
                tracing::trace!("Flushed page {} to disk", page_id.0);
            }
        }

        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&self) -> Result<()> {
        let page_table = self.page_table.read();
        let mut flushed = 0;

        for &frame_idx in page_table.values() {
            let frame = self.frames[frame_idx].clone();
            let frame_guard = frame.write();

            if frame_guard.is_dirty && frame_guard.page.is_some() {
                let page_arc = Arc::clone(frame_guard.page.as_ref().unwrap());
                drop(frame_guard);

                let page_guard = page_arc.read();
                self.disk_manager.write_page(&page_guard)?;
                drop(page_guard);

                let mut frame_guard = frame.write();
                frame_guard.is_dirty = false;
                flushed += 1;
            }
        }

        #[cfg(debug_assertions)]
        tracing::trace!("Flushed {} dirty pages to disk", flushed);
        Ok(())
    }

    /// Delete a page from the buffer pool and disk.
    pub fn delete_page(&self, page_id: PageId) -> Result<()> {
        let mut page_table = self.page_table.write();

        if let Some(frame_idx) = page_table.remove(&page_id) {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();
            frame_guard.reset();
            self.free_list.lock().push_back(frame_idx);

            #[cfg(debug_assertions)]
            tracing::trace!("Deleted page {} from buffer pool", page_id.0);
        }

        self.disk_manager.free_page(page_id)?;
        Ok(())
    }

    /// Load a page from disk into buffer pool
    fn load_page_from_disk(&self, page_id: PageId, timestamp: u64) -> Result<Arc<RwLock<Page>>> {
        let frame_idx = self.get_frame()?;

        let page = self.disk_manager.read_page(page_id)?;
        let page_arc = Arc::new(RwLock::new(page));

        {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();
            frame_guard.page = Some(Arc::clone(&page_arc));
            frame_guard.is_dirty = false;
            frame_guard.pin_count = 1;
            frame_guard.access_history.clear();
            frame_guard.record_access(timestamp);
        }

        self.page_table.write().insert(page_id, frame_idx);

        #[cfg(debug_assertions)]
        tracing::trace!(
            "Loaded page {} from disk into frame {}",
            page_id.0,
            frame_idx
        );

        Ok(page_arc)
    }

    /// Get a frame for a new page, using free list or eviction
    fn get_frame(&self) -> Result<usize> {
        // Try to get a free frame first
        if let Some(frame_idx) = self.free_list.lock().pop_front() {
            return Ok(frame_idx);
        }

        // No free frames, need to evict using LRU-K
        self.evict_page()
    }

    /// Evict a page using the LRU-K algorithm.
    ///
    /// Selects the victim frame with the maximum backward K-distance.
    /// Pages with fewer than K accesses have infinite distance and are
    /// evicted first (they're likely from sequential scans, not the working set).
    fn evict_page(&self) -> Result<usize> {
        let current_ts = self.get_current_timestamp();
        let mut victim_idx: Option<usize> = None;
        let mut max_distance: u64 = 0;

        // Scan all frames to find the one with maximum backward K-distance
        for (idx, frame_arc) in self.frames.iter().enumerate() {
            let frame = frame_arc.read();

            // Skip pinned frames
            if !frame.is_evictable() {
                continue;
            }

            // Skip empty frames (shouldn't happen if free_list is empty, but be safe)
            if frame.page.is_none() {
                continue;
            }

            let distance = frame.backward_k_distance(current_ts);

            // Select this frame if it has greater distance
            // (INFINITY_DISTANCE will always win if present)
            if distance > max_distance || victim_idx.is_none() {
                max_distance = distance;
                victim_idx = Some(idx);

                // Early exit: can't do better than infinity
                if distance == INFINITY_DISTANCE {
                    break;
                }
            }
        }

        // If no victim found, all pages are pinned
        let frame_idx = victim_idx
            .ok_or_else(|| MonoError::Storage("buffer pool full: all pages are pinned".into()))?;

        // Evict the victim
        let frame = self.frames[frame_idx].clone();
        let mut frame_guard = frame.write();

        if let Some(page) = &frame_guard.page {
            let page_id = page.read().header.page_id;

            // Flush if dirty
            if frame_guard.is_dirty {
                let page_guard = page.read();
                self.disk_manager.write_page(&page_guard)?;
                #[cfg(debug_assertions)]
                tracing::trace!("Flushed page {} during eviction", page_id.0);
            }

            // Remove from page table
            self.page_table.write().remove(&page_id);

            #[cfg(debug_assertions)]
            tracing::trace!(
                "Evicted page {} from frame {} (k_distance={}, accesses={})",
                page_id.0,
                frame_idx,
                if max_distance == INFINITY_DISTANCE {
                    "∞".to_string()
                } else {
                    max_distance.to_string()
                },
                frame_guard.access_history.len()
            );
        }

        frame_guard.reset();
        Ok(frame_idx)
    }
}

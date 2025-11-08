use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use monodb_common::{MonoError, Result};
use parking_lot::{Mutex, RwLock};
use tracing::{debug, warn};

use crate::storage::{
    disk_manager::DiskManager,
    page::{Page, PageId, PageType},
};

struct Frame {
    page: Option<Arc<RwLock<Page>>>,
    is_dirty: bool,
    pin_count: usize,
    access_count: u64,
}

impl Frame {
    /// Create a new empty frame
    fn new() -> Self {
        Self {
            page: None,
            is_dirty: false,
            pin_count: 0,
            access_count: 0,
        }
    }

    /// Check if this frame is evictable (not pinned).
    fn is_evictable(&self) -> bool {
        self.pin_count == 0
    }

    fn reset(&mut self) {
        self.page = None;
        self.is_dirty = false;
        self.pin_count = 0;
        self.access_count = 0;
    }
}

pub struct BufferPool {
    frames: Vec<Arc<RwLock<Frame>>>,
    page_table: Arc<RwLock<HashMap<PageId, usize>>>, // PageId -> frame index
    free_list: Arc<Mutex<VecDeque<usize>>>,
    disk_manager: Arc<DiskManager>,
    clock_hand: Arc<Mutex<usize>>,
    pool_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool with the specified number of frames.
    ///
    /// # Arguments
    /// * `pool_size` - Number of page frames ot allocate
    /// * `disk_manager` - Disk manager for page I/O
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = VecDeque::with_capacity(pool_size);

        // Initialize all frames as empty and add to free list
        for i in 0..pool_size {
            frames.push(Arc::new(RwLock::new(Frame::new())));
            free_list.push_back(i);
        }

        debug!("Buffer pool initialized with {} frames", pool_size);

        Self {
            frames,
            page_table: Arc::new(RwLock::new(HashMap::new())),
            free_list: Arc::new(Mutex::new(free_list)),
            disk_manager,
            clock_hand: Arc::new(Mutex::new(0)),
            pool_size,
        }
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
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to fetch
    ///
    /// # Returns
    /// An Arc to the page wrapped in RwLock
    pub fn fetch_page(&self, page_id: PageId) -> Result<Arc<RwLock<Page>>> {
        // Check if page is already in buffer pool
        let frame_idx_opt = {
            let page_table = self.page_table.read();
            page_table.get(&page_id).copied()
        }; // page_table read lock dropped here

        if let Some(frame_idx) = frame_idx_opt {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();

            // Increment pin count and access count
            frame_guard.pin_count += 1;
            frame_guard.access_count += 1;

            if let Some(page) = &frame_guard.page {
                return Ok(Arc::clone(page));
            }
        }

        // Page not in buffer pool, load from disk
        self.load_page_from_disk(page_id)
    }

    /// Create a new page in the buffer pool.
    ///
    /// The page is allocated a new PageId and initialized with the given type.
    ///
    /// # Arguments
    /// * `page_type` - Type of page to create (Leaf, Interior, etc.)
    ///
    /// # Returns
    /// An Arc to the newly created page
    pub fn new_page(&self, page_type: PageType) -> Result<Arc<RwLock<Page>>> {
        // Allocate a new page ID
        let page_id = self.disk_manager.allocate_page()?;

        // Get a frame for the new page
        let frame_idx = self.get_frame()?;

        // Creat the new page
        let page = Arc::new(RwLock::new(Page::new(page_type, page_id)));

        // Install page in frame
        {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();

            frame_guard.page = Some(Arc::clone(&page));
            frame_guard.is_dirty = true;
            frame_guard.pin_count = 1;
            frame_guard.access_count = 1;
        }

        // Update page table
        self.page_table.write().insert(page_id, frame_idx);

        debug!("Created new page {} in frame {}", page_id.0, frame_idx);

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
                warn!("Attempted to unping page {} with pin_count=0", page_id.0);
                return Ok(());
            }

            frame_guard.pin_count -= 1;
            if is_dirty {
                frame_guard.is_dirty = true;
            }

            debug!(
                "Unpinned page {} (pin_count={}, dirty={})",
                page_id.0, frame_guard.pin_count, frame_guard.access_count,
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
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to flush
    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let page_table = self.page_table.read();

        if let Some(&frame_idx) = page_table.get(&page_id) {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();

            if frame_guard.is_dirty {
                if let Some(page) = &frame_guard.page {
                    // Clone the Arc to avoid borrow issues
                    let page_arc = Arc::clone(page);
                    // Drop frame_guard temporarily to avoid holding lock during I/O
                    drop(frame_guard);

                    // Write to disk (may take time, don't hold frame lock)
                    let page_guard = page_arc.read();
                    self.disk_manager.write_page(&page_guard)?;
                    drop(page_guard);

                    // Re-acquire frame lock and mark as clean
                    let mut frame_guard = frame.write();
                    frame_guard.is_dirty = false;

                    debug!("Flushed page {} to disk", page_id.0);
                }
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
            let mut frame_guard = frame.write();

            if frame_guard.is_dirty {
                if let Some(page) = &frame_guard.page {
                    // Clone the Arc to avoid borrow issues
                    let page_arc = Arc::clone(page);
                    // Drop frame_guard temporarily to avoid holding lock during I/O
                    drop(frame_guard);

                    // Write to disk
                    let page_guard = page_arc.read();
                    self.disk_manager.write_page(&page_guard)?;
                    drop(page_guard);

                    // Re-acquire frame lock and mark as clean
                    let mut frame_guard = frame.write();
                    frame_guard.is_dirty = false;
                    flushed += 1;
                }
            }
        }

        debug!("Flushed {} dirty pages to disk", flushed);
        Ok(())
    }

    /// Delete a page from the buffer pool and disk.
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to delete
    pub fn delete_page(&self, page_id: PageId) -> Result<()> {
        let mut page_table = self.page_table.write();

        if let Some(frame_idx) = page_table.remove(&page_id) {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();

            // Reset frame to empty state
            frame_guard.reset();

            // Add to free list
            self.free_list.lock().push_back(frame_idx);

            debug!("Deleted page {} from buffer pool", page_id.0);
        }

        // Delete from disk
        self.disk_manager.free_page(page_id)?;

        Ok(())
    }

    /// Load a page from disk into buffer pool
    fn load_page_from_disk(&self, page_id: PageId) -> Result<Arc<RwLock<Page>>> {
        // Get a frame to load the page into
        let frame_idx = self.get_frame()?;

        // Read page from disk
        let page = self.disk_manager.read_page(page_id)?;
        let page_arc = Arc::new(RwLock::new(page));

        // Install in frame
        {
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();

            frame_guard.page = Some(Arc::clone(&page_arc));
            frame_guard.is_dirty = false;
            frame_guard.pin_count = 1;
            frame_guard.access_count = 1;
        }

        // Update page table
        self.page_table.write().insert(page_id, frame_idx);

        debug!(
            "Loaded page {} from disk into frame {}",
            page_id.0, frame_idx
        );

        Ok(page_arc)
    }

    /// Get a frame for a new page, using free list or eviction
    fn get_frame(&self) -> Result<usize> {
        // Try to get a free frame first
        if let Some(frame_idx) = self.free_list.lock().pop_front() {
            return Ok(frame_idx);
        }

        // No free frames, need to evict
        self.evict_page()
    }

    /// Evict a page using the clock algorithm
    ///
    /// The clock algorithm treats the frame array as a circular buffer.
    /// It scans frames looking for an unpinned page with access_count=0.
    /// If access_count>0, it decrements and continues (second chance).
    fn evict_page(&self) -> Result<usize> {
        let mut attempts = 0;
        let max_attempts = self.pool_size * 2; // Two full scans

        loop {
            if attempts >= max_attempts {
                return Err(MonoError::Storage(
                    "buffer pool full: all pages are pinned".into(),
                ));
            }

            // Get current clock position
            let mut clock_hand = self.clock_hand.lock();
            let frame_idx = *clock_hand;

            // Advance clock hand
            *clock_hand = (*clock_hand + 1) % self.pool_size;
            drop(clock_hand);

            // Check if this frame is evictable
            let frame = self.frames[frame_idx].clone();
            let mut frame_guard = frame.write();

            if !frame_guard.is_evictable() {
                attempts += 1;
                continue;
            }

            // Give second chance to recently accessed pages
            if frame_guard.access_count > 0 {
                frame_guard.access_count = 0;
                attempts += 1;
                continue;
            }

            // Found victim frame, evict it
            if let Some(page) = &frame_guard.page {
                let page_id = page.read().header.page_id;

                // Flush if dirty
                if frame_guard.is_dirty {
                    let page_guard = page.read();
                    self.disk_manager.write_page(&page_guard)?;
                    debug!("Flushed page {} during eviction", page_id.0);
                }

                // Remove from page table
                self.page_table.write().remove(&page_id);

                debug!("Evicted page {} from frame {}", page_id.0, frame_idx);
            }

            // Reset frame
            frame_guard.reset();

            return Ok(frame_idx);
        }
    }
}

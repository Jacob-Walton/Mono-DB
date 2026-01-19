//! Disk manager for page-level I/O.
//!
//! Manages page I/O with memory-mapped file support and fallback to standard I/O.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{Mmap, MmapMut};
use monodb_common::{MonoError, Result};
use parking_lot::RwLock;

use super::page::{PAGE_SIZE, PageId};
use super::traits::PageStore;

// Free list header (stored at end of page 0): 4-byte magic + 8-byte head
const FREE_LIST_OFFSET: usize = PAGE_SIZE - 12;
const FREE_LIST_MAGIC: u32 = u32::from_be_bytes(*b"FREE");

// Disk manager configuration

/// Configuration for the disk manager.
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Initial file size in pages (default: 16 pages = 256KB).
    pub initial_pages: u64,
    /// Growth factor when extending the file (default: 2.0).
    pub growth_factor: f64,
    /// Minimum pages to add when growing (default: 16).
    pub min_growth_pages: u64,
    /// Use memory mapping (default: true).
    pub use_mmap: bool,
}

impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            initial_pages: 16,
            growth_factor: 2.0,
            min_growth_pages: 16,
            use_mmap: true,
        }
    }
}

// Disk manager for page I/O

/// Manages raw page I/O with memory-mapped file support.
pub struct DiskManager {
    /// Path to the data file.
    path: PathBuf,
    /// The underlying file handle.
    file: RwLock<File>,
    /// Memory-mapped view of the file (read-only).
    mmap: RwLock<Option<Mmap>>,
    /// Memory-mapped view for writes (optional).
    mmap_mut: RwLock<Option<MmapMut>>,
    /// Next page ID to allocate.
    next_page_id: AtomicU64,
    /// Current file size in pages.
    file_pages: AtomicU64,
    /// Configuration.
    config: DiskConfig,
    /// Head of the free page list (0 = none).
    free_list_head: AtomicU64,
}

impl DiskManager {
    /// Create or open a data file.
    pub fn new<P: AsRef<Path>>(path: P, config: DiskConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let file_len = file.metadata()?.len();
        let file_pages = if file_len == 0 {
            // New file: initialize with the configured initial size
            let initial_size = config.initial_pages * PAGE_SIZE as u64;
            file.set_len(initial_size)?;
            config.initial_pages
        } else {
            file_len / PAGE_SIZE as u64
        };

        // Determine next page ID from file contents
        // Reserve page 0 for metadata (free list head), start user allocations at page 1
        let next_page_id = if file_len == 0 { 1 } else { file_pages.max(1) };

        let manager = Self {
            path,
            file: RwLock::new(file),
            mmap: RwLock::new(None),
            mmap_mut: RwLock::new(None),
            next_page_id: AtomicU64::new(next_page_id),
            file_pages: AtomicU64::new(file_pages),
            config,
            free_list_head: AtomicU64::new(0),
        };

        // Create initial memory mapping
        manager.remap()?;

        // Load persisted free list head
        if file_len > 0 {
            let head = manager.load_free_list_head()?;
            manager.free_list_head.store(head, Ordering::Release);
        }

        Ok(manager)
    }

    /// Open with default configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::new(path, DiskConfig::default())
    }

    /// Load free list head from page 0.
    fn load_free_list_head(&self) -> Result<u64> {
        let page0 = self.read(PageId(0))?;
        let magic = u32::from_le_bytes(
            page0[FREE_LIST_OFFSET..FREE_LIST_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        if magic != FREE_LIST_MAGIC {
            return Ok(0); // Not initialized, empty list
        }
        let head = u64::from_le_bytes(
            page0[FREE_LIST_OFFSET + 4..FREE_LIST_OFFSET + 12]
                .try_into()
                .unwrap(),
        );
        Ok(head)
    }

    /// Save free list head to page 0.
    fn save_free_list_head(&self, head: u64) -> Result<()> {
        let mut page0 = self.read(PageId(0))?;
        page0[FREE_LIST_OFFSET..FREE_LIST_OFFSET + 4]
            .copy_from_slice(&FREE_LIST_MAGIC.to_le_bytes());
        page0[FREE_LIST_OFFSET + 4..FREE_LIST_OFFSET + 12].copy_from_slice(&head.to_le_bytes());
        self.write(PageId(0), &page0)
    }

    /// Re-create the memory mapping (after file growth).
    fn remap(&self) -> Result<()> {
        if !self.config.use_mmap {
            return Ok(());
        }

        let file = self.file.read();
        let file_len = file.metadata()?.len();

        if file_len == 0 {
            // Can't map empty file
            *self.mmap.write() = None;
            return Ok(());
        }

        // Create read-only mapping
        let mmap = unsafe { Mmap::map(&*file) }
            .map_err(|e| MonoError::Storage(format!("failed to create memory mapping: {}", e)))?;

        *self.mmap.write() = Some(mmap);
        Ok(())
    }

    /// Grow the file to accommodate more pages.
    fn grow(&self, min_pages: u64) -> Result<()> {
        let current_pages = self.file_pages.load(Ordering::Acquire);
        let needed_pages = min_pages.saturating_sub(current_pages);

        if needed_pages == 0 {
            return Ok(());
        }

        // Calculate new size
        let growth = ((current_pages as f64 * self.config.growth_factor) as u64)
            .saturating_sub(current_pages)
            .max(needed_pages)
            .max(self.config.min_growth_pages);

        let new_pages = current_pages + growth;
        let new_size = new_pages * PAGE_SIZE as u64;

        // Extend the file
        {
            let file = self.file.write();
            file.set_len(new_size)?;
        }

        self.file_pages.store(new_pages, Ordering::Release);

        // Remap after growth
        self.remap()?;

        Ok(())
    }

    /// Get the number of allocated pages.
    pub fn page_count(&self) -> u64 {
        self.next_page_id.load(Ordering::Acquire)
    }

    /// Get the file capacity in pages.
    pub fn capacity(&self) -> u64 {
        self.file_pages.load(Ordering::Acquire)
    }

    /// Read a page using memory mapping.
    fn read_mmap(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]> {
        let mmap_guard = self.mmap.read();
        let mmap = mmap_guard
            .as_ref()
            .ok_or_else(|| MonoError::Storage("memory mapping not available".into()))?;

        let offset = page_id.offset() as usize;
        let end = offset + PAGE_SIZE;

        if end > mmap.len() {
            return Err(MonoError::PageNotFound(page_id.0));
        }

        let mut buf = [0u8; PAGE_SIZE];
        buf.copy_from_slice(&mmap[offset..end]);
        Ok(buf)
    }

    /// Read a page using standard file I/O.
    fn read_file(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]> {
        let mut file = self.file.write();
        let offset = page_id.offset();

        file.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Write a page using standard file I/O.
    fn write_file(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        if data.len() != PAGE_SIZE {
            return Err(MonoError::Storage(format!(
                "page data must be {} bytes, got {}",
                PAGE_SIZE,
                data.len()
            )));
        }

        let mut file = self.file.write();
        let offset = page_id.offset();

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;

        Ok(())
    }

    /// Read a page.
    pub fn read(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]> {
        if !page_id.is_valid() {
            return Err(MonoError::PageNotFound(page_id.0));
        }

        let file_pages = self.file_pages.load(Ordering::Acquire);
        if page_id.0 >= file_pages {
            return Err(MonoError::PageNotFound(page_id.0));
        }

        // Try memory mapping first
        if self.config.use_mmap && self.mmap.read().is_some() {
            self.read_mmap(page_id)
        } else {
            self.read_file(page_id)
        }
    }

    /// Write a page.
    pub fn write(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        if !page_id.is_valid() {
            return Err(MonoError::Storage("cannot write to invalid page".into()));
        }

        // Ensure file is large enough
        let needed_pages = page_id.0 + 1;
        if needed_pages > self.file_pages.load(Ordering::Acquire) {
            self.grow(needed_pages)?;
        }

        self.write_file(page_id, data)
    }

    /// Allocate a new page.
    pub fn allocate(&self) -> Result<PageId> {
        // Check free list first, try to reuse a freed page
        loop {
            let free_head = self.free_list_head.load(Ordering::Acquire);
            if free_head == 0 {
                // No free pages, allocate a new one
                break;
            }

            // Read the free page to get the next pointer
            let free_page_id = PageId(free_head);
            let page_data = self.read(free_page_id)?;
            let next_free = u64::from_le_bytes(page_data[0..8].try_into().unwrap());

            // Try to update the head atomically
            if self
                .free_list_head
                .compare_exchange(free_head, next_free, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Persist the new head
                self.save_free_list_head(next_free)?;
                // Successfully popped from free list, zero out the page before returning
                let zeroed = [0u8; PAGE_SIZE];
                self.write(free_page_id, &zeroed)?;
                return Ok(free_page_id);
            }
            // CAS failed, another thread modified the list, retry
        }

        // No free pages available, allocate new
        let page_id = PageId(self.next_page_id.fetch_add(1, Ordering::AcqRel));

        // Grow file if needed
        if page_id.0 >= self.file_pages.load(Ordering::Acquire) {
            self.grow(page_id.0 + 1)?;
        }

        Ok(page_id)
    }

    /// Mark a page as free for reuse.
    pub fn free(&self, page_id: PageId) -> Result<()> {
        if !page_id.is_valid() || page_id.0 == 0 {
            // Don't free invalid pages or the metadata page
            return Ok(());
        }

        // Push onto free list
        loop {
            let current_head = self.free_list_head.load(Ordering::Acquire);

            // Write the current head as the next pointer in the freed page
            let mut page_data = [0u8; PAGE_SIZE];
            page_data[0..8].copy_from_slice(&current_head.to_le_bytes());
            self.write(page_id, &page_data)?;

            // Try to update the head to point to this freed page
            if self
                .free_list_head
                .compare_exchange(current_head, page_id.0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Persist the new head
                self.save_free_list_head(page_id.0)?;
                return Ok(());
            }
            // CAS failed, retry
        }
    }

    /// Sync all data to disk.
    pub fn sync(&self) -> Result<()> {
        let file = self.file.read();
        file.sync_all()?;
        Ok(())
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

// PageStore Implementation

impl PageStore for DiskManager {
    fn read(&self, page_id: PageId) -> Result<Vec<u8>> {
        Ok(self.read(page_id)?.to_vec())
    }

    fn write(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        // Pad or truncate to PAGE_SIZE
        let mut buf = [0u8; PAGE_SIZE];
        let len = data.len().min(PAGE_SIZE);
        buf[..len].copy_from_slice(&data[..len]);
        self.write(page_id, &buf)
    }

    fn allocate(&self) -> Result<PageId> {
        self.allocate()
    }

    fn free(&self, page_id: PageId) -> Result<()> {
        self.free(page_id)
    }

    fn sync(&self) -> Result<()> {
        self.sync()
    }

    fn page_count(&self) -> u64 {
        self.page_count()
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_new_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = DiskManager::open(&path).unwrap();
        assert!(path.exists());
        assert!(dm.page_count() >= 1);
        assert!(dm.capacity() >= 16);
    }

    #[test]
    fn test_read_write_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = DiskManager::open(&path).unwrap();

        // Allocate and write a page
        let page_id = dm.allocate().unwrap();
        let mut data = [0u8; PAGE_SIZE];
        data[0..11].copy_from_slice(b"hello world");
        dm.write(page_id, &data).unwrap();

        // Read it back
        let read_data = dm.read(page_id).unwrap();
        assert_eq!(&read_data[0..11], b"hello world");
    }

    #[test]
    fn test_file_growth() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let config = DiskConfig {
            initial_pages: 4,
            growth_factor: 2.0,
            min_growth_pages: 4,
            use_mmap: true,
        };

        let dm = DiskManager::new(&path, config).unwrap();
        assert_eq!(dm.capacity(), 4);

        // Allocate pages beyond initial capacity
        for _ in 0..10 {
            dm.allocate().unwrap();
        }

        // File should have grown
        assert!(dm.capacity() >= 10);
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write some data
        {
            let dm = DiskManager::open(&path).unwrap();
            let page_id = dm.allocate().unwrap();
            let mut data = [0u8; PAGE_SIZE];
            data[0..4].copy_from_slice(b"test");
            dm.write(page_id, &data).unwrap();
            dm.sync().unwrap();
        }

        // Reopen and verify
        {
            let dm = DiskManager::open(&path).unwrap();
            let data = dm.read(PageId(1)).unwrap();
            assert_eq!(&data[0..4], b"test");
        }
    }

    #[test]
    fn test_free_list() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = DiskManager::open(&path).unwrap();

        // Allocate some pages
        let p1 = dm.allocate().unwrap();
        let p2 = dm.allocate().unwrap();
        let p3 = dm.allocate().unwrap();

        // Free the middle page
        dm.free(p2).unwrap();

        // Next allocation should reuse the freed page
        let p4 = dm.allocate().unwrap();
        assert_eq!(p4, p2);

        // Free multiple pages
        dm.free(p1).unwrap();
        dm.free(p3).unwrap();

        // Should reuse in LIFO order
        let p5 = dm.allocate().unwrap();
        assert_eq!(p5, p3);
        let p6 = dm.allocate().unwrap();
        assert_eq!(p6, p1);
    }

    #[test]
    fn test_free_list_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let freed_page: PageId;

        // Allocate and free a page
        {
            let dm = DiskManager::open(&path).unwrap();
            let p1 = dm.allocate().unwrap();
            dm.free(p1).unwrap();
            freed_page = p1;
            dm.sync().unwrap();
        }

        // Reopen and verify free list is preserved
        {
            let dm = DiskManager::open(&path).unwrap();
            let p = dm.allocate().unwrap();
            assert_eq!(p, freed_page);
        }
    }
}

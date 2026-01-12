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
        // Page 0 is metadata, so we start allocating from page 1
        let next_page_id = if file_len == 0 { 1 } else { file_pages };

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

        Ok(manager)
    }

    /// Open with default configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::new(path, DiskConfig::default())
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
        // Check free list first
        let free_head = self.free_list_head.load(Ordering::Acquire);
        if free_head != 0 {
            // TODO: Implement free list pop
            // For now, just allocate new pages
        }

        let page_id = PageId(self.next_page_id.fetch_add(1, Ordering::AcqRel));

        // Grow file if needed
        if page_id.0 >= self.file_pages.load(Ordering::Acquire) {
            self.grow(page_id.0 + 1)?;
        }

        Ok(page_id)
    }

    /// Mark a page as free for reuse.
    pub fn free(&self, _page_id: PageId) -> Result<()> {
        // TODO: Implement free list push
        // For now, pages are not reused
        Ok(())
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
}

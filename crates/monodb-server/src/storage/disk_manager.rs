use monodb_common::Result;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};

use crate::storage::page::{DiskPage, PAGE_SIZE, PageId};

/// Manages raw file I/O for pages
pub struct DiskManager {
    file: File,
    next_page_id: AtomicU32,
}

impl DiskManager {
    /// Create or open a database file
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let next_page_id = if file_len == 0 {
            0
        } else {
            (file_len / PAGE_SIZE as u64) as u32
        };

        Ok(Self {
            file,
            next_page_id: AtomicU32::new(next_page_id),
        })
    }

    /// Allocate a new page ID
    #[allow(dead_code)]
    pub fn allocate_page(&self) -> PageId {
        PageId(self.next_page_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Write a page to disk (without sync)
    pub fn write_page(&mut self, page: &DiskPage) -> Result<()> {
        let offset = page.page_id.0 as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.to_bytes())?;
        Ok(())
    }

    /// Write a page to disk with immediate sync
    #[allow(dead_code)]
    pub fn write_page_sync(&mut self, page: &DiskPage) -> Result<()> {
        self.write_page(page)?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Read a page from disk
    pub fn read_page(&mut self, page_id: PageId) -> Result<DiskPage> {
        let offset = page_id.0 as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;

        DiskPage::from_bytes(&buf)
    }

    /// Get the number of pages in the file
    pub fn num_pages(&self) -> u32 {
        self.next_page_id.load(Ordering::SeqCst)
    }

    /// Sync all data to disk
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

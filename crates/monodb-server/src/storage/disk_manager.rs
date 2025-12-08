use crate::storage::page::{PAGE_SIZE, Page, PageId, PageType};
use monodb_common::Result;
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

pub struct DiskManager {
    data_file: Arc<RwLock<File>>,
    meta_file: Arc<RwLock<File>>,
    next_page_id: Arc<RwLock<PageId>>,
    free_pages: Arc<RwLock<Vec<PageId>>>,
    root_page_id: Arc<RwLock<Option<PageId>>>,
}

impl DiskManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data_path = path.as_ref().join("data.db");
        let meta_path = path.as_ref().join("meta.db");

        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)?;

        let meta_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&meta_path)?;

        let mut manager = Self {
            data_file: Arc::new(RwLock::new(data_file)),
            meta_file: Arc::new(RwLock::new(meta_file)),
            // Start page ids at 1 so 0 can represent "no root" in metadata
            next_page_id: Arc::new(RwLock::new(PageId(1))),
            free_pages: Arc::new(RwLock::new(Vec::new())),
            root_page_id: Arc::new(RwLock::new(None)),
        };

        manager.load_metadata()?;

        Ok(manager)
    }

    fn load_metadata(&mut self) -> Result<()> {
        let mut file = self.meta_file.write();
        let file_len = file.metadata()?.len();

        if file_len == 0 {
            // New database, write initial metadata
            // next_page_id starts at 1 (page 1 will be first real page)
            self.write_metadata(&mut file, PageId(1), None)?;
        } else {
            // Load existing metadata (fixed-size header: next_page_id [8] + root_page_id [8])
            file.seek(SeekFrom::Start(0))?;
            let mut buf = [0u8; 8];
            file.read_exact(&mut buf)?;
            let next_page = u64::from_le_bytes(buf);
            *self.next_page_id.write() = PageId(next_page);

            let mut rbuf = [0u8; 8];
            file.read_exact(&mut rbuf)?;
            let root_raw = u64::from_le_bytes(rbuf);
            *self.root_page_id.write() = if root_raw == 0 {
                None
            } else {
                Some(PageId(root_raw))
            };

            // Load free page list
            // TODO: Implement free page list properly
        }

        Ok(())
    }

    fn write_metadata(
        &self,
        file: &mut File,
        next_page_id: PageId,
        root: Option<PageId>,
    ) -> Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&next_page_id.0.to_le_bytes())?;
        let root_raw = root.map(|p| p.0).unwrap_or(0);
        file.write_all(&root_raw.to_le_bytes())?;
        file.sync_all()?;

        Ok(())
    }

    pub fn allocate_page(&self) -> Result<PageId> {
        // Try to reuse a free page first
        if let Some(page_id) = self.free_pages.write().pop() {
            return Ok(page_id);
        }

        // Allocate new page
        let (page_id, new_next_id) = {
            let mut next_id = self.next_page_id.write();
            let mut page_id = *next_id;
            // Ensure page id 0 is never handed out; start from 1
            if page_id.0 == 0 {
                page_id = PageId(1);
                next_id.0 = 2;
            } else {
                next_id.0 += 1;
            }
            (page_id, *next_id)
        };

        // Update metadata (preserve current root)
        {
            let mut meta_file = self.meta_file.write();
            let current_root = *self.root_page_id.read();
            self.write_metadata(&mut meta_file, new_next_id, current_root)?;
        }

        Ok(page_id)
    }

    pub fn free_page(&self, page_id: PageId) -> Result<()> {
        self.free_pages.write().push(page_id);
        // Overwrite on-disk page with a Free page to avoid stale/corrupted reads
        let free_page = Page::new(PageType::Free, page_id);
        self.write_page(&free_page)?;
        Ok(())
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let mut file = self.data_file.write();
        let offset = page_id.0 * PAGE_SIZE as u64;

        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buffer)?;

        Page::from_bytes(&buffer)
    }

    pub fn write_page(&self, page: &Page) -> Result<()> {
        let mut file = self.data_file.write();
        let offset = page.header.page_id.0 * PAGE_SIZE as u64;

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page.to_bytes())?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.data_file.write().sync_all()?;
        self.meta_file.write().sync_all()?;
        Ok(())
    }

    pub fn get_root_page_id(&self) -> Option<PageId> {
        *self.root_page_id.read()
    }

    pub fn set_root_page_id(&self, root: Option<PageId>) -> Result<()> {
        *self.root_page_id.write() = root;
        // Rewrite metadata with new root
        let mut file = self.meta_file.write();
        let next = *self.next_page_id.read();
        self.write_metadata(&mut file, next, root)?;
        Ok(())
    }
}

//! Disk I/O manager

use crate::error::MonoResult;
use crate::storage::{Page, PageId};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Manages disk I/O operations
pub struct DiskManager {
    data_files: Mutex<HashMap<String, File>>,
    data_dir: PathBuf,
}

impl DiskManager {
    /// Create a new disk manager
    pub fn new<P: AsRef<Path>>(data_dir: P) -> MonoResult<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        Ok(Self {
            data_files: Mutex::new(HashMap::new()),
            data_dir,
        })
    }

    /// Read a page from disk
    pub fn read_page(&self, table_name: &str, page_id: PageId) -> MonoResult<Page> {
        let mut files = self.data_files.lock().unwrap();
        let file = self.get_or_create_file(&mut files, table_name)?;

        let offset = page_id.0 as u64 * crate::storage::PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; crate::storage::PAGE_SIZE];
        file.read_exact(&mut buffer)?;

        Page::from_bytes(&buffer)
    }

    /// Write a page to disk
    pub fn write_page(&self, table_name: &str, page: &Page) -> MonoResult<()> {
        let mut files = self.data_files.lock().unwrap();
        let file = self.get_or_create_file(&mut files, table_name)?;

        let offset = page.header.page_id as u64 * crate::storage::PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;

        let bytes = page.to_bytes();
        file.write_all(&bytes)?;
        file.sync_all()?;

        Ok(())
    }

    /// Allocate a new page
    pub fn allocate_page(&self, table_name: &str) -> MonoResult<PageId> {
        let mut files = self.data_files.lock().unwrap();
        let file = self.get_or_create_file(&mut files, table_name)?;

        // Get file size to determine next page ID
        let file_size = file.metadata()?.len();
        let page_count = file_size / crate::storage::PAGE_SIZE as u64;

        let page_id = PageId(page_count as u32);

        // Extend file
        let new_size = (page_count + 1) * crate::storage::PAGE_SIZE as u64;
        file.set_len(new_size)?;

        Ok(page_id)
    }

    /// Sync all files to disk
    pub fn sync_all(&self) -> MonoResult<()> {
        let mut files = self.data_files.lock().unwrap();
        for file in files.values_mut() {
            file.sync_all()?;
        }
        Ok(())
    }

    fn get_or_create_file<'a>(
        &self,
        files: &'a mut HashMap<String, File>,
        table_name: &str,
    ) -> MonoResult<&'a mut File> {
        if !files.contains_key(table_name) {
            let path = self.data_dir.join(format!("{}.db", table_name));
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)?;
            files.insert(table_name.to_string(), file);
        }

        Ok(files.get_mut(table_name).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::PageType;

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_disk_manager() {
        let temp_dir = tempdir().unwrap();
        let disk_manager = DiskManager::new(temp_dir.path()).unwrap();

        // Allocate and write a page
        let page_id = disk_manager.allocate_page("test_table").unwrap();
        let mut page = Page::new(page_id, PageType::Table);
        page.add_tuple(b"test data").unwrap();

        disk_manager.write_page("test_table", &page).unwrap();

        // Read it back
        let read_page = disk_manager.read_page("test_table", page_id).unwrap();
        assert_eq!(read_page.header.page_id, page_id.0);
        assert_eq!(read_page.get_tuple(0).unwrap(), b"test data");
    }
}

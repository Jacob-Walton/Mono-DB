//! Page management

use crate::error::{MonoError, MonoResult};
use serde::{Deserialize, Serialize};

/// Page size in bytes (8KB)
pub const PAGE_SIZE: usize = 8192;

/// Page header size
pub const PAGE_HEADER_SIZE: usize = 64;

/// Maximum tuple data size
pub const MAX_TUPLE_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

/// Page identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PageId(pub u32);

/// Page types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Invalid = 0,
    Meta = 1,
    Table = 2,
    Index = 3,
    Free = 4,
}

/// Page header structure
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub magic: u32,             // Magic number for validation
    pub page_id: u32,           // Page ID
    pub page_type: u8,          // Page type
    pub flags: u8,              // Flags (dirty, pinned, etc.)
    pub lsn: u64,               // Last LSN that modified this page
    pub free_space_offset: u16, // Offset to free space
    pub tuple_count: u16,       // Number of tuples
    pub next_page_id: u32,      // Next page in chain (for overflow)
    pub prev_page_id: u32,      // Previous page in chain
    pub checksum: u32,          // CRC32 checksum
    pub _reserved: [u8; 28],    // Reserved for future use
}

/// Database page
pub struct Page {
    pub header: PageHeader,
    pub data: Vec<u8>,
}

impl Page {
    /// Magic number for page validation
    const MAGIC: u32 = 0x4D4F4E4F; // "MONO"

    /// Create a new empty page
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let header = PageHeader {
            magic: Self::MAGIC,
            page_id: page_id.0,
            page_type: page_type as u8,
            flags: 0,
            lsn: 0,
            free_space_offset: PAGE_HEADER_SIZE as u16,
            tuple_count: 0,
            next_page_id: 0,
            prev_page_id: 0,
            checksum: 0,
            _reserved: [0; 28],
        };

        Self {
            header,
            data: vec![0; PAGE_SIZE - PAGE_HEADER_SIZE],
        }
    }

    /// Serialize page to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(PAGE_SIZE);

        // Serialize header
        bytes.extend_from_slice(&self.header.magic.to_le_bytes());
        bytes.extend_from_slice(&self.header.page_id.to_le_bytes());
        bytes.push(self.header.page_type);
        bytes.push(self.header.flags);
        bytes.extend_from_slice(&self.header.lsn.to_le_bytes());
        bytes.extend_from_slice(&self.header.free_space_offset.to_le_bytes());
        bytes.extend_from_slice(&self.header.tuple_count.to_le_bytes());
        bytes.extend_from_slice(&self.header.next_page_id.to_le_bytes());
        bytes.extend_from_slice(&self.header.prev_page_id.to_le_bytes());

        // Calculate checksum (excluding checksum field itself)
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());

        // Add reserved bytes
        bytes.extend_from_slice(&self.header._reserved);

        // Add data
        bytes.extend_from_slice(&self.data);

        // Ensure we have exactly PAGE_SIZE bytes
        bytes.resize(PAGE_SIZE, 0);

        bytes
    }

    /// Deserialize page from bytes
    pub fn from_bytes(bytes: &[u8]) -> MonoResult<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(MonoError::Storage(format!(
                "Invalid page size: expected {}, got {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }

        let mut offset = 0;

        // Read header fields
        let magic = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        if magic != Self::MAGIC {
            return Err(MonoError::Storage("Invalid page magic number".into()));
        }

        let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let page_type = bytes[offset];
        offset += 1;

        let flags = bytes[offset];
        offset += 1;

        let lsn = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let free_space_offset = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let tuple_count = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let next_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let prev_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let stored_checksum = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Verify checksum
        let calculated_checksum = crc32fast::hash(&bytes[..offset - 4]);
        if stored_checksum != calculated_checksum {
            return Err(MonoError::Storage("Page checksum mismatch".into()));
        }

        let mut _reserved = [0u8; 28];
        _reserved.copy_from_slice(&bytes[offset..offset + 28]);
        offset += 28;

        // Read data
        let data = bytes[offset..].to_vec();

        let header = PageHeader {
            magic,
            page_id,
            page_type,
            flags,
            lsn,
            free_space_offset,
            tuple_count,
            next_page_id,
            prev_page_id,
            checksum: stored_checksum,
            _reserved,
        };

        Ok(Self { header, data })
    }

    /// Get available free space
    pub fn free_space(&self) -> usize {
        PAGE_SIZE - self.header.free_space_offset as usize
    }

    /// Add a tuple to the page
    pub fn add_tuple(&mut self, tuple_data: &[u8]) -> MonoResult<u16> {
        let tuple_size = tuple_data.len() + 4; // 4 bytes for size header

        if tuple_size > self.free_space() {
            return Err(MonoError::Storage("Not enough space in page".into()));
        }

        // Write tuple at current free space offset
        let offset = self.header.free_space_offset as usize - PAGE_HEADER_SIZE;

        // Write tuple size
        let size_bytes = (tuple_data.len() as u32).to_le_bytes();
        self.data[offset..offset + 4].copy_from_slice(&size_bytes);

        // Write tuple data
        self.data[offset + 4..offset + 4 + tuple_data.len()].copy_from_slice(tuple_data);

        // Update header
        self.header.free_space_offset += tuple_size as u16;
        let tuple_id = self.header.tuple_count;
        self.header.tuple_count += 1;

        Ok(tuple_id)
    }

    /// Read a tuple from the page
    pub fn get_tuple(&self, tuple_id: u16) -> MonoResult<Vec<u8>> {
        if tuple_id >= self.header.tuple_count {
            return Err(MonoError::Storage("Invalid tuple ID".into()));
        }

        // Simple linear scan for now
        let mut offset = 0;
        for i in 0..=tuple_id {
            if offset + 4 > self.data.len() {
                return Err(MonoError::Storage("Corrupted page data".into()));
            }

            let size =
                u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;

            if i == tuple_id {
                if offset + 4 + size > self.data.len() {
                    return Err(MonoError::Storage("Corrupted tuple data".into()));
                }
                return Ok(self.data[offset + 4..offset + 4 + size].to_vec());
            }

            offset += 4 + size;
        }

        Err(MonoError::Storage("Tuple not found".into()))
    }

    /// Mark page as dirty
    pub fn mark_dirty(&mut self) {
        self.header.flags |= 0x01;
    }

    /// Check if page is dirty
    pub fn is_dirty(&self) -> bool {
        self.header.flags & 0x01 != 0
    }

    /// Clear dirty flag
    pub fn clear_dirty(&mut self) {
        self.header.flags &= !0x01;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_serialization() {
        let mut page = Page::new(PageId(42), PageType::Table);

        // Add some tuples
        let tuple1 = b"Hello, World!";
        let tuple2 = b"Test data";

        let id1 = page.add_tuple(tuple1).unwrap();
        let id2 = page.add_tuple(tuple2).unwrap();

        // Serialize and deserialize
        let bytes = page.to_bytes();
        assert_eq!(bytes.len(), PAGE_SIZE);

        let restored = Page::from_bytes(&bytes).unwrap();

        // Verify header
        assert_eq!(restored.header.page_id, 42);
        assert_eq!(restored.header.page_type, PageType::Table as u8);
        assert_eq!(restored.header.tuple_count, 2);

        // Verify tuples
        assert_eq!(restored.get_tuple(id1).unwrap(), tuple1);
        assert_eq!(restored.get_tuple(id2).unwrap(), tuple2);
    }
}

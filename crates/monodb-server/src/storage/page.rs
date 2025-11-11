use monodb_common::{MonoError, Result};
use serde::{Deserialize, Serialize};

pub const PAGE_SIZE: usize = 16384; // 16KB pages
pub const PAGE_HEADER_SIZE: usize = 64;
pub const PAGE_DATA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageId(pub u64);

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PageType {
    Invalid = 0,
    Meta = 1,     // Metadata page
    Interior = 2, // B-tree interior node
    Leaf = 3,     // B-tree leaf node
    Overflow = 4, // Overflow data
    Free = 5,     // Free page
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub magic: u32, // Magic number for validation
    pub page_type: PageType,
    pub page_id: PageId,
    pub lsn: u64, // Log sequence number
    pub checksum: u32,
    pub free_space_start: u16,
    pub free_space_end: u16,
    pub cell_count: u16,
    pub first_free_cell: u16,
    pub parent_page: PageId,
    pub right_sibling: PageId,
    _padding: [u8; 18],
}

impl PageHeader {
    const MAGIC: u32 = u32::from_be_bytes(*b"MPAG");

    pub fn new(page_type: PageType, page_id: PageId) -> Self {
        Self {
            magic: Self::MAGIC,
            page_type,
            page_id,
            lsn: 0,
            checksum: 0,
            free_space_start: PAGE_HEADER_SIZE as u16,
            free_space_end: PAGE_SIZE as u16,
            cell_count: 0,
            first_free_cell: 0,
            parent_page: PageId(0),
            right_sibling: PageId(0),
            _padding: [0; 18],
        }
    }

    // Serialize header into a fixed 64-byte layout with checksum at bytes 60..64 (LE)
    fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
        let mut buf = [0u8; PAGE_HEADER_SIZE];
        // 0..4 magic
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        // 4 page_type (u8)
        buf[4] = self.page_type as u8;
        // 5..8 reserved
        buf[5..8].copy_from_slice(&[0u8; 3]);
        // 8..16 page_id
        buf[8..16].copy_from_slice(&self.page_id.0.to_le_bytes());
        // 16..24 lsn
        buf[16..24].copy_from_slice(&self.lsn.to_le_bytes());
        // 24..26 free_space_start
        buf[24..26].copy_from_slice(&self.free_space_start.to_le_bytes());
        // 26..28 free_space_end
        buf[26..28].copy_from_slice(&self.free_space_end.to_le_bytes());
        // 28..30 cell_count
        buf[28..30].copy_from_slice(&self.cell_count.to_le_bytes());
        // 30..32 first_free_cell
        buf[30..32].copy_from_slice(&self.first_free_cell.to_le_bytes());
        // 32..40 parent_page
        buf[32..40].copy_from_slice(&self.parent_page.0.to_le_bytes());
        // 40..48 right_sibling
        buf[40..48].copy_from_slice(&self.right_sibling.0.to_le_bytes());
        // 48..60 reserved
        // leave zeros

        // checksum over 0..60
        let checksum = crc32fast::hash(&buf[0..60]);
        buf[60..64].copy_from_slice(&checksum.to_le_bytes());
        buf
    }

    // Deserialize header from fixed 64-byte layout, verify checksum
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PAGE_HEADER_SIZE {
            return Err(MonoError::Storage("Invalid header size".into()));
        }
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != Self::MAGIC {
            return Err(MonoError::Storage("Invalid page magic number".into()));
        }
        let page_type = match bytes[4] {
            0 => PageType::Invalid,
            1 => PageType::Meta,
            2 => PageType::Interior,
            3 => PageType::Leaf,
            4 => PageType::Overflow,
            5 => PageType::Free,
            v => return Err(MonoError::Storage(format!("Invalid page type byte: {}", v))),
        };
        let page_id = PageId(u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]));
        let lsn = u64::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]);
        let free_space_start = u16::from_le_bytes([bytes[24], bytes[25]]);
        let free_space_end = u16::from_le_bytes([bytes[26], bytes[27]]);
        let cell_count = u16::from_le_bytes([bytes[28], bytes[29]]);
        let first_free_cell = u16::from_le_bytes([bytes[30], bytes[31]]);
        let parent_page = PageId(u64::from_le_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
        ]));
        let right_sibling = PageId(u64::from_le_bytes([
            bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46], bytes[47],
        ]));
        let checksum_expected = u32::from_le_bytes([bytes[60], bytes[61], bytes[62], bytes[63]]);
        let checksum_actual = crc32fast::hash(&bytes[0..60]);
        if checksum_expected != checksum_actual && checksum_expected != 0 {
            return Err(MonoError::Storage("Page checksum mismatch".into()));
        }
        Ok(Self {
            magic,
            page_type,
            page_id,
            lsn,
            checksum: checksum_expected,
            free_space_start,
            free_space_end,
            cell_count,
            first_free_cell,
            parent_page,
            right_sibling,
            _padding: [0; 18],
        })
    }
}

#[derive(Clone)]
pub struct Page {
    pub header: PageHeader,
    pub data: [u8; PAGE_DATA_SIZE],
}

impl Page {
    pub fn new(page_type: PageType, page_id: PageId) -> Self {
        Self {
            header: PageHeader::new(page_type, page_id),
            data: [0; PAGE_DATA_SIZE],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(MonoError::Storage(format!(
                "Invalid page size: expected {}, got {}",
                PAGE_SIZE,
                bytes.len(),
            )));
        }

        // Deserialize header from fixed layout
        let header = PageHeader::from_bytes(&bytes[0..PAGE_HEADER_SIZE])?;

        let mut page = Self {
            header,
            data: [0; PAGE_DATA_SIZE],
        };
        page.data.copy_from_slice(&bytes[PAGE_HEADER_SIZE..]);
        Ok(page)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; PAGE_SIZE];
        // Build header bytes with correct checksum
        let header_bytes = self.header.to_bytes();
        bytes[0..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);
        // Copy data
        bytes[PAGE_HEADER_SIZE..].copy_from_slice(&self.data);
        bytes
    }

    pub fn available_space(&self) -> usize {
        (self.header.free_space_end - self.header.free_space_start) as usize
    }

    /// Add a cell (key-value pair) to the page in sorted order
    pub fn add_cell(&mut self, key: &[u8], value: &[u8]) -> Result<u16> {
        let cell_size = 4 + key.len() + 4 + value.len(); // lengths + data
        let directory_entry_size = 2; // 2 bytes for cell offset

        if self.available_space() < cell_size + directory_entry_size {
            return Err(MonoError::Storage("Not enough space in page".into()));
        }

        // First, write the cell data at the current free space start
        let cell_offset = self.header.free_space_start;
        let mut offset = (cell_offset - PAGE_HEADER_SIZE as u16) as usize;

        // Write key length and key
        self.data[offset..offset + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
        offset += 4;
        self.data[offset..offset + key.len()].copy_from_slice(key);
        offset += key.len();

        // Write value length and value
        self.data[offset..offset + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
        offset += 4;
        self.data[offset..offset + value.len()].copy_from_slice(value);

        // Find the correct position in the sorted cell directory
        let mut insert_pos = self.header.cell_count as usize;

        // Search through existing cells to find insertion position
        for i in 0..self.header.cell_count as usize {
            // Get the cell offset for this position
            if let Ok(existing_cell_offset) = self.get_cell_offset(i as u16)
                && let Ok((existing_key, _)) = self.get_cell(existing_cell_offset)
                && key < existing_key.as_slice()
            {
                insert_pos = i;
                break;
            }
        }

        // Move entries that come after insert_pos one position to the right
        for i in (insert_pos..self.header.cell_count as usize).rev() {
            let current_dir_offset = self.data.len() - ((i + 1) * 2);
            let new_dir_offset = self.data.len() - ((i + 2) * 2);

            if current_dir_offset + 1 < self.data.len() && new_dir_offset + 1 < self.data.len() {
                // Copy the entry to its new position
                self.data[new_dir_offset] = self.data[current_dir_offset];
                self.data[new_dir_offset + 1] = self.data[current_dir_offset + 1];
            }
        }

        // Insert the new cell at the correct position
        let final_directory_offset = self.data.len() - ((insert_pos + 1) * 2);

        if final_directory_offset + 1 < self.data.len() {
            self.data[final_directory_offset..final_directory_offset + 2]
                .copy_from_slice(&cell_offset.to_le_bytes());
        }

        self.header.free_space_start += cell_size as u16;
        self.header.cell_count += 1;

        Ok(cell_offset)
    }

    pub fn get_cell(&self, offset: u16) -> Result<(Vec<u8>, Vec<u8>)> {
        if offset < PAGE_HEADER_SIZE as u16 {
            return Err(MonoError::Storage(format!(
                "Cell offset {offset} is less than PAGE_HEADER_SIZE {PAGE_HEADER_SIZE}"
            )));
        }

        let mut idx = (offset - PAGE_HEADER_SIZE as u16) as usize;

        // Bounds check
        if idx + 4 > self.data.len() {
            return Err(MonoError::Storage("Cell offset out of bounds".into()));
        }

        // Read key
        let key_len = u32::from_le_bytes([
            self.data[idx],
            self.data[idx + 1],
            self.data[idx + 2],
            self.data[idx + 3],
        ]) as usize;
        idx += 4;

        // Bounds check for key
        if idx + key_len > self.data.len() {
            return Err(MonoError::Storage(format!(
                "Key length {key_len} would exceed data bounds at idx {idx}"
            )));
        }

        let key = self.data[idx..idx + key_len].to_vec();
        idx += key_len;

        // Bounds check for value length
        if idx + 4 > self.data.len() {
            return Err(MonoError::Storage(
                "Value length offset out of bounds".into(),
            ));
        }

        // Read value
        let value_len = u32::from_le_bytes([
            self.data[idx],
            self.data[idx + 1],
            self.data[idx + 2],
            self.data[idx + 3],
        ]) as usize;
        idx += 4;

        // Bounds check for value
        if idx + value_len > self.data.len() {
            return Err(MonoError::Storage(format!(
                "Value length {value_len} would exceed data bounds at idx {idx}"
            )));
        }

        let value = self.data[idx..idx + value_len].to_vec();

        Ok((key, value))
    }

    /// Get the number of cells in this page
    pub fn cell_count(&self) -> u16 {
        self.header.cell_count
    }

    /// Get the offset of a cell by its index in the cell directory
    pub fn get_cell_offset(&self, index: u16) -> Result<u16> {
        if index >= self.header.cell_count {
            return Err(MonoError::Storage(format!(
                "Cell index {} out of bounds (count: {})",
                index, self.header.cell_count
            )));
        }

        // Cell directory starts at the end of the page and grows backward
        // Index 0 should be the first entry (highest offset)
        let directory_offset = self.data.len() - ((index as usize + 1) * 2);

        if directory_offset + 1 < self.data.len() {
            let cell_offset =
                u16::from_le_bytes([self.data[directory_offset], self.data[directory_offset + 1]]);
            Ok(cell_offset)
        } else {
            Err(MonoError::Storage(
                "Cell directory offset out of bounds".into(),
            ))
        }
    }

    /// Update a cell in place, only works if new value fits in same space
    pub fn update_cell(&mut self, offset: u16, key: &[u8], value: &[u8]) -> Result<()> {
        if offset < PAGE_HEADER_SIZE as u16 {
            return Err(MonoError::Storage("Invalid cell offset".into()));
        }

        let mut idx = (offset - PAGE_HEADER_SIZE as u16) as usize;

        // Read existing lengths to verify space
        if idx + 4 > self.data.len() {
            return Err(MonoError::Storage("Cell offset out of bounds".into()));
        }

        let existing_key_len = u32::from_le_bytes([
            self.data[idx],
            self.data[idx + 1],
            self.data[idx + 2],
            self.data[idx + 3],
        ]) as usize;
        idx += 4 + existing_key_len;

        if idx + 4 > self.data.len() {
            return Err(MonoError::Storage("Cell key length invalid".into()));
        }

        let existing_value_len = u32::from_le_bytes([
            self.data[idx],
            self.data[idx + 1],
            self.data[idx + 2],
            self.data[idx + 3],
        ]) as usize;

        // Check if new data fits in existing space
        if key.len() != existing_key_len || value.len() > existing_value_len {
            return Err(MonoError::Storage(
                "New data doesn't fit in existing cell space".into(),
            ));
        }

        // Update the cell data in place
        let mut offset_idx = (offset - PAGE_HEADER_SIZE as u16) as usize;

        // Write key length and key
        self.data[offset_idx..offset_idx + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
        offset_idx += 4;
        self.data[offset_idx..offset_idx + key.len()].copy_from_slice(key);
        offset_idx += key.len();

        // Write value length and value
        self.data[offset_idx..offset_idx + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
        offset_idx += 4;
        self.data[offset_idx..offset_idx + value.len()].copy_from_slice(value);

        // If new value is shorter, pad with zeros
        if value.len() < existing_value_len {
            let padding_start = offset_idx + value.len();
            let padding_end = offset_idx + existing_value_len;
            self.data[padding_start..padding_end].fill(0);
        }

        Ok(())
    }

    /// Remove a cell by its index in the cell directory
    pub fn remove_cell(&mut self, index: u16) -> Result<()> {
        if index >= self.header.cell_count {
            return Err(MonoError::Storage(format!(
                "Cell index {} out of bounds (count: {})",
                index, self.header.cell_count
            )));
        }

        // Get the cell offset to determine space freed
        let cell_offset = self.get_cell_offset(index)?;
        let (key, value) = self.get_cell(cell_offset)?;
        let freed_space = 4 + key.len() + 4 + value.len();

        // Mark the cell space as free by zeroing it out
        // (In a more sophisticated implementation, we'd compact the page)
        let start_idx = (cell_offset - PAGE_HEADER_SIZE as u16) as usize;
        let end_idx = start_idx + freed_space;
        if end_idx <= self.data.len() {
            self.data[start_idx..end_idx].fill(0);
        }

        // Remove the cell directory entry by shifting all subsequent entries
        let _dir_start = self.data.len() - (self.header.cell_count as usize * 2);
        let _remove_dir_offset = self.data.len() - ((index as usize + 1) * 2);

        // Shift directory entries to fill the gap
        for i in (index + 1)..self.header.cell_count {
            let src_offset = self.data.len() - ((i as usize + 1) * 2);
            let dst_offset = self.data.len() - (i as usize * 2);

            if src_offset + 1 < self.data.len() && dst_offset + 1 < self.data.len() {
                self.data[dst_offset] = self.data[src_offset];
                self.data[dst_offset + 1] = self.data[src_offset + 1];
            }
        }

        // Clear the last directory entry
        let last_dir_offset = self.data.len() - (self.header.cell_count as usize * 2);
        if last_dir_offset + 1 < self.data.len() {
            self.data[last_dir_offset] = 0;
            self.data[last_dir_offset + 1] = 0;
        }

        self.header.cell_count -= 1;
        // Note: We don't update free_space_start here - in a more sophisticated system,
        // we'd need to compact the page or use a proper free space management scheme

        Ok(())
    }

    /// Compact the page to reclaim fragmented space from deleted cells
    pub fn compact(&mut self) -> Result<()> {
        if self.header.cell_count == 0 {
            // No cells to compact
            self.header.free_space_start = PAGE_HEADER_SIZE as u16;
            return Ok(());
        }

        // Collect all valid cells
        let mut valid_cells = Vec::new();
        for i in 0..self.header.cell_count {
            if let Ok(offset) = self.get_cell_offset(i) {
                if let Ok((key, value)) = self.get_cell(offset) {
                    valid_cells.push((key, value));
                }
            }
        }

        // Clear the page data
        self.data.fill(0);
        self.header.cell_count = 0;
        self.header.free_space_start = PAGE_HEADER_SIZE as u16;

        // Re-add all valid cells
        for (key, value) in valid_cells {
            if let Err(e) = self.add_cell(&key, &value) {
                // If we can't re-add a cell, we have a serious problem
                return Err(e);
            }
        }

        Ok(())
    }

    /// Get the fragmentation ratio (0.0 = no fragmentation, 1.0 = completely fragmented)
    pub fn fragmentation_ratio(&self) -> f64 {
        if self.header.cell_count == 0 {
            return 0.0;
        }

        let _total_page_space = PAGE_DATA_SIZE as u16;
        let used_space = self.header.free_space_start - PAGE_HEADER_SIZE as u16;
        let directory_space = self.header.cell_count * 2;
        let actual_used_space = used_space + directory_space;

        // Calculate what the used space would be if compacted
        let mut compacted_size = 0u16;
        for i in 0..self.header.cell_count {
            if let Ok(offset) = self.get_cell_offset(i) {
                if let Ok((key, value)) = self.get_cell(offset) {
                    compacted_size += 4 + key.len() as u16 + 4 + value.len() as u16; // lengths + data
                }
            }
        }
        compacted_size += self.header.cell_count * 2; // directory entries

        if actual_used_space == 0 {
            return 0.0;
        }

        let fragmentation = (actual_used_space - compacted_size) as f64 / actual_used_space as f64;
        fragmentation.max(0.0).min(1.0)
    }
}

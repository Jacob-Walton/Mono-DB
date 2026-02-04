//! Page structures for disk-based storage.
//!
//! Pages are 16KB units containing a header and data section.

use monodb_common::{MonoError, Result};

// Page format constants

/// Page size in bytes (16 KB).
pub const PAGE_SIZE: usize = 16 * 1024;

/// Page header size in bytes (64 bytes).
pub const PAGE_HEADER_SIZE: usize = 64;

/// Usable data area in a page.
pub const PAGE_DATA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

/// Magic number for page validation.
pub const PAGE_MAGIC: u32 = u32::from_be_bytes(*b"MONO");

/// Current page format version.
pub const PAGE_VERSION: u8 = 3;

// Page identifiers

/// Unique identifier for a page (page 0 is reserved for metadata).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
#[repr(transparent)]
pub struct PageId(pub u64);

impl PageId {
    /// Invalid page ID sentinel value.
    pub const INVALID: PageId = PageId(u64::MAX);

    /// Metadata page (always page 0).
    pub const META: PageId = PageId(0);

    /// Check if this is a valid page ID.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.0 != u64::MAX
    }

    /// Get the byte offset of this page in a file.
    #[inline]
    pub fn offset(&self) -> u64 {
        self.0 * PAGE_SIZE as u64
    }
}

impl From<u64> for PageId {
    #[inline]
    fn from(id: u64) -> Self {
        PageId(id)
    }
}

impl From<PageId> for u64 {
    #[inline]
    fn from(id: PageId) -> Self {
        id.0
    }
}

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if *self == PageId::INVALID {
            write!(f, "INVALID")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

// Page Type

/// Type of page, stored in the header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Invalid/uninitialized page.
    Invalid = 0,
    /// B+Tree leaf node (contains key-value pairs).
    Leaf = 1,
    /// B+Tree interior node (contains keys and child pointers).
    Interior = 2,
    /// Free page (available for reuse).
    Free = 3,
    /// Overflow page (for large values that don't fit in a single page).
    Overflow = 4,
    /// Metadata page (file header, schema info).
    Meta = 5,
}

impl TryFrom<u8> for PageType {
    type Error = MonoError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(PageType::Invalid),
            1 => Ok(PageType::Leaf),
            2 => Ok(PageType::Interior),
            3 => Ok(PageType::Free),
            4 => Ok(PageType::Overflow),
            5 => Ok(PageType::Meta),
            _ => Err(MonoError::Storage(format!("invalid page type: {}", value))),
        }
    }
}

// Page Header

/// Page header layout (64 bytes):
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub magic: u32,
    pub version: u8,
    pub page_type: PageType,
    pub flags: u16,
    pub page_id: PageId,
    pub key_count: u16,
    pub free_space: u16,
    pub data_offset: u32,
    pub next_page: PageId,
    pub prev_page: PageId,
    pub parent: PageId,
    pub lsn: u64,
    pub checksum: u32,
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            magic: PAGE_MAGIC,
            version: PAGE_VERSION,
            page_type: PageType::Invalid,
            flags: 0,
            page_id: PageId::INVALID,
            key_count: 0,
            free_space: PAGE_DATA_SIZE as u16,
            data_offset: PAGE_HEADER_SIZE as u32,
            next_page: PageId::INVALID,
            prev_page: PageId::INVALID,
            parent: PageId::INVALID,
            lsn: 0,
            checksum: 0,
        }
    }
}

impl PageHeader {
    /// Serialize the header to bytes (excluding checksum).
    fn to_bytes_without_checksum(self) -> [u8; PAGE_HEADER_SIZE] {
        let mut buf = [0u8; PAGE_HEADER_SIZE];

        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4] = self.version;
        buf[5] = self.page_type as u8;
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.page_id.0.to_le_bytes());
        buf[16..18].copy_from_slice(&self.key_count.to_le_bytes());
        buf[18..20].copy_from_slice(&self.free_space.to_le_bytes());
        buf[20..24].copy_from_slice(&self.data_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.next_page.0.to_le_bytes());
        buf[32..40].copy_from_slice(&self.prev_page.0.to_le_bytes());
        buf[40..48].copy_from_slice(&self.parent.0.to_le_bytes());
        buf[48..56].copy_from_slice(&self.lsn.to_le_bytes());
        // checksum at 56..60, reserved at 60..64

        buf
    }

    /// Deserialize header from bytes.
    pub fn from_bytes(buf: &[u8; PAGE_HEADER_SIZE]) -> Result<Self> {
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != PAGE_MAGIC {
            return Err(MonoError::Corruption {
                location: "page header".into(),
                details: format!(
                    "invalid magic: expected {:#x}, got {:#x}",
                    PAGE_MAGIC, magic
                ),
            });
        }

        let version = buf[4];
        if version != PAGE_VERSION {
            return Err(MonoError::Corruption {
                location: "page header".into(),
                details: format!(
                    "unsupported version: expected {}, got {}",
                    PAGE_VERSION, version
                ),
            });
        }

        let page_type = PageType::try_from(buf[5])?;
        let flags = u16::from_le_bytes([buf[6], buf[7]]);
        let page_id = PageId(u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]));
        let key_count = u16::from_le_bytes([buf[16], buf[17]]);
        let free_space = u16::from_le_bytes([buf[18], buf[19]]);
        let data_offset = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);
        let next_page = PageId(u64::from_le_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]));
        let prev_page = PageId(u64::from_le_bytes([
            buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
        ]));
        let parent = PageId(u64::from_le_bytes([
            buf[40], buf[41], buf[42], buf[43], buf[44], buf[45], buf[46], buf[47],
        ]));
        let lsn = u64::from_le_bytes([
            buf[48], buf[49], buf[50], buf[51], buf[52], buf[53], buf[54], buf[55],
        ]);
        let checksum = u32::from_le_bytes([buf[56], buf[57], buf[58], buf[59]]);

        Ok(Self {
            magic,
            version,
            page_type,
            flags,
            page_id,
            key_count,
            free_space,
            data_offset,
            next_page,
            prev_page,
            parent,
            lsn,
            checksum,
        })
    }
}

// Disk Page

/// Complete page with header and data.
///
/// In-memory page representation. The `data` field contains variable-length
/// content after the fixed header.
#[derive(Clone)]
pub struct DiskPage {
    pub header: PageHeader,
    pub data: Vec<u8>,
}

impl DiskPage {
    /// Create a new leaf page.
    pub fn new_leaf(page_id: PageId) -> Self {
        Self {
            header: PageHeader {
                page_type: PageType::Leaf,
                page_id,
                ..Default::default()
            },
            data: Vec::with_capacity(PAGE_DATA_SIZE),
        }
    }

    /// Create a new interior page.
    pub fn new_interior(page_id: PageId) -> Self {
        Self {
            header: PageHeader {
                page_type: PageType::Interior,
                page_id,
                ..Default::default()
            },
            data: Vec::with_capacity(PAGE_DATA_SIZE),
        }
    }

    /// Create a new metadata page.
    pub fn new_meta(page_id: PageId) -> Self {
        Self {
            header: PageHeader {
                page_type: PageType::Meta,
                page_id,
                ..Default::default()
            },
            data: Vec::with_capacity(PAGE_DATA_SIZE),
        }
    }

    /// Create a free page.
    pub fn new_free(page_id: PageId) -> Self {
        Self {
            header: PageHeader {
                page_type: PageType::Free,
                page_id,
                ..Default::default()
            },
            data: Vec::new(),
        }
    }

    /// Check if this is a leaf page.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.header.page_type == PageType::Leaf
    }

    /// Check if this is an interior page.
    #[inline]
    pub fn is_interior(&self) -> bool {
        self.header.page_type == PageType::Interior
    }

    /// Check if this page is free.
    #[inline]
    pub fn is_free(&self) -> bool {
        self.header.page_type == PageType::Free
    }

    /// Serialize the page to a fixed-size byte array.
    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];

        // Serialize header (without checksum first)
        let header_bytes = self.header.to_bytes_without_checksum();
        buf[..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);

        // Copy data
        let data_len = self.data.len().min(PAGE_DATA_SIZE);
        buf[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_len].copy_from_slice(&self.data[..data_len]);

        // Compute and store checksum over the entire page (excluding checksum field)
        let checksum = crc32fast::hash(&buf[..56]);
        let checksum2 = crc32fast::hash(&buf[60..]);
        let final_checksum = checksum ^ checksum2;
        buf[56..60].copy_from_slice(&final_checksum.to_le_bytes());

        buf
    }

    /// Deserialize a page from bytes.
    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<Self> {
        // Verify checksum first
        let stored_checksum = u32::from_le_bytes([buf[56], buf[57], buf[58], buf[59]]);
        if stored_checksum != 0 {
            let checksum1 = crc32fast::hash(&buf[..56]);
            let checksum2 = crc32fast::hash(&buf[60..]);
            let computed_checksum = checksum1 ^ checksum2;

            if stored_checksum != computed_checksum {
                return Err(MonoError::ChecksumMismatch {
                    expected: stored_checksum,
                    actual: computed_checksum,
                });
            }
        }

        // Parse header
        let header_bytes: [u8; PAGE_HEADER_SIZE] = buf[..PAGE_HEADER_SIZE]
            .try_into()
            .map_err(|_| MonoError::Storage("header size mismatch".into()))?;
        let header = PageHeader::from_bytes(&header_bytes)?;

        // Copy data
        let data_len = PAGE_DATA_SIZE.saturating_sub(header.free_space as usize);
        let data = buf[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_len].to_vec();

        Ok(Self { header, data })
    }

    /// Get remaining free space in the data section.
    #[inline]
    pub fn free_space(&self) -> usize {
        PAGE_DATA_SIZE.saturating_sub(self.data.len())
    }

    /// Check if the page can accommodate `size` more bytes.
    #[inline]
    pub fn can_fit(&self, size: usize) -> bool {
        self.free_space() >= size
    }
}

impl std::fmt::Debug for DiskPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskPage")
            .field("page_id", &self.header.page_id)
            .field("page_type", &self.header.page_type)
            .field("key_count", &self.header.key_count)
            .field("data_len", &self.data.len())
            .field("free_space", &self.free_space())
            .finish()
    }
}

// Slot Directory (for variable-length records)

/// A slot in the slot directory, pointing to a record in the page.
#[derive(Debug, Clone, Copy)]
pub struct Slot {
    /// Offset from the start of the data area.
    pub offset: u16,
    /// Length of the record.
    pub length: u16,
}

impl Slot {
    pub const SIZE: usize = 4;

    pub fn to_bytes(self) -> [u8; 4] {
        let mut buf = [0u8; 4];
        buf[0..2].copy_from_slice(&self.offset.to_le_bytes());
        buf[2..4].copy_from_slice(&self.length.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 4]) -> Self {
        Self {
            offset: u16::from_le_bytes([buf[0], buf[1]]),
            length: u16::from_le_bytes([buf[2], buf[3]]),
        }
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_id_offset() {
        assert_eq!(PageId(0).offset(), 0);
        assert_eq!(PageId(1).offset(), PAGE_SIZE as u64);
        assert_eq!(PageId(10).offset(), 10 * PAGE_SIZE as u64);
    }

    #[test]
    fn test_page_roundtrip() {
        let mut page = DiskPage::new_leaf(PageId(42));
        page.data.extend_from_slice(b"hello world");
        page.header.key_count = 1;

        let bytes = page.to_bytes();
        let restored = DiskPage::from_bytes(&bytes).unwrap();

        assert_eq!(restored.header.page_id, PageId(42));
        assert_eq!(restored.header.page_type, PageType::Leaf);
        assert_eq!(restored.header.key_count, 1);
        assert!(restored.data.starts_with(b"hello world"));
    }

    #[test]
    fn test_checksum_validation() {
        let page = DiskPage::new_leaf(PageId(1));
        let mut bytes = page.to_bytes();

        // Corrupt the data
        bytes[PAGE_HEADER_SIZE + 10] = 0xFF;

        // Should fail checksum
        let result = DiskPage::from_bytes(&bytes);
        assert!(matches!(result, Err(MonoError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_slot_roundtrip() {
        let slot = Slot {
            offset: 100,
            length: 50,
        };
        let bytes = slot.to_bytes();
        let restored = Slot::from_bytes(&bytes);
        assert_eq!(restored.offset, 100);
        assert_eq!(restored.length, 50);
    }
}

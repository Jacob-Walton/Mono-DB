use monodb_common::{MonoError, Result};

/// Page size in bytes
pub const PAGE_SIZE: usize = 16 * 1024; // 16 KB

/// Page header size
pub const PAGE_HEADER_SIZE: usize = 64; // 64 bytes

/// Usable date area in a page
pub const PAGE_DATA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE; // 16 KB - 64 bytes

/// Invalid page ID sentinel
pub const INVALID_PAGE_ID: u32 = u32::MAX;

// Page types

/// Page identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct PageId(pub u32);

impl PageId {
    pub const INVALID: PageId = PageId(INVALID_PAGE_ID);

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        self.0 != INVALID_PAGE_ID
    }
}

/// Page type discriminator
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Invalid = 0,
    Leaf = 1,
    Interior = 2,
    Free = 3,
}

// Disk page serialization

const DISK_PAGE_MAGIC: u32 = 0x42545245; // "BTRE"

/// A disk page with fixed-size raw byte layout
/// Layout:
///     - Header (64 bytes):
///         - magic: u32 (4 bytes)
///         - page_type: u8 (1 byte)
///         - _reserved: [u8; 3] (3 bytes)
///         - page_id: u32 (4 bytes)
///         - key_count: u16 (2 bytes)
///         - next_leaf: u32 (4 bytes)
///         - parent: u32 (4 bytes)
///         - lsn: u64 (8 bytes)
///         - _reserved2: [u8; 30] (30 bytes)
///         - checksum: u32 (4 bytes)
///     - Data (PAGE_SIZE - 64 bytes):
///         - Variable length encoded keys, values, children
#[derive(Clone)]
pub struct DiskPage {
    pub page_type: PageType,
    pub page_id: PageId,
    pub key_count: u16,
    pub next_leaf: PageId,
    pub parent: PageId,
    pub lsn: u64,
    /// Raw data area
    pub data: Vec<u8>,
}

impl DiskPage {
    pub fn new_leaf(page_id: PageId) -> Self {
        Self {
            page_type: PageType::Leaf,
            page_id,
            key_count: 0,
            next_leaf: PageId::INVALID,
            parent: PageId::INVALID,
            lsn: 0,
            data: Vec::with_capacity(PAGE_DATA_SIZE),
        }
    }

    pub fn new_interior(page_id: PageId) -> Self {
        Self {
            page_type: PageType::Interior,
            page_id,
            key_count: 0,
            next_leaf: PageId::INVALID,
            parent: PageId::INVALID,
            lsn: 0,
            data: Vec::with_capacity(PAGE_DATA_SIZE),
        }
    }

    /// Serialize to raw bytes (PAGE_SIZE bytes)
    #[inline]
    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];

        // Safety check: ensure data fits in page
        if self.data.len() > PAGE_DATA_SIZE {
            panic!(
                "Page {} data overflow: {} bytes exceeds {} byte limit. This indicates a bug in max_keys calculation.",
                self.page_id.0,
                self.data.len(),
                PAGE_DATA_SIZE
            );
        }

        // Header
        buf[0..4].copy_from_slice(&DISK_PAGE_MAGIC.to_le_bytes());
        buf[4] = self.page_type as u8;
        // 5..8 reserved
        buf[8..12].copy_from_slice(&self.page_id.0.to_le_bytes());
        buf[12..14].copy_from_slice(&self.key_count.to_le_bytes());
        buf[14..18].copy_from_slice(&self.next_leaf.0.to_le_bytes());
        buf[18..22].copy_from_slice(&self.parent.0.to_le_bytes());
        buf[22..30].copy_from_slice(&self.lsn.to_le_bytes());
        // 30..60 reserved

        // Checksum over header[0..60]
        let checksum = crc32fast::hash(&buf[0..60]);
        buf[60..64].copy_from_slice(&checksum.to_le_bytes());

        // Data - we've already verified it fits
        let data_len = self.data.len();
        buf[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_len].copy_from_slice(&self.data[..data_len]);

        buf
    }

    /// Deserialize from raw bytes
    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<Self> {
        // Verify magic
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != DISK_PAGE_MAGIC {
            return Err(MonoError::Storage("Invalid page magic number".into()));
        }

        // Verify checksum
        let stored_checksum = u32::from_le_bytes([buf[60], buf[61], buf[62], buf[63]]);
        let computed_checksum = crc32fast::hash(&buf[0..60]);
        if stored_checksum != computed_checksum && stored_checksum != 0 {
            return Err(MonoError::Storage("Checksum mismatch".into()));
        }

        let page_type = match buf[4] {
            0 => PageType::Invalid,
            1 => PageType::Leaf,
            2 => PageType::Interior,
            3 => PageType::Free,
            _ => return Err(MonoError::Storage("Invalid page type".into())),
        };

        let page_id = PageId(u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]));
        let key_count = u16::from_le_bytes([buf[12], buf[13]]);
        let next_leaf = PageId(u32::from_le_bytes([buf[14], buf[15], buf[16], buf[17]]));
        let parent = PageId(u32::from_le_bytes([buf[18], buf[19], buf[20], buf[21]]));
        let lsn = u64::from_le_bytes([
            buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29],
        ]);

        let data = buf[PAGE_HEADER_SIZE..].to_vec();

        Ok(Self {
            page_type,
            page_id,
            key_count,
            next_leaf,
            parent,
            lsn,
            data,
        })
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.page_type == PageType::Leaf
    }
}

// Key-Value Serialization Traits

/// Trait for types that can be serialized to/from raw bytes
pub trait Serializable: Sized {
    /// Serialize to bytes, appending to the buffer
    fn serialize_to(&self, buf: &mut Vec<u8>);

    /// Deserialize from bytes, returing (value, bytes_consumed)
    fn deserialize_from(buf: &[u8]) -> Result<(Self, usize)>;

    /// Fixed size if known
    fn fixed_size() -> Option<usize> {
        None
    }

    /// Get the serialized size of this value in bytes
    fn serialized_size(&self) -> usize;
}

impl Serializable for i32 {
    fn serialize_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    fn deserialize_from(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 4 {
            return Err(MonoError::Storage("Not enough bytes for i32".into()));
        }
        Ok((i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]), 4))
    }

    fn fixed_size() -> Option<usize> {
        Some(4)
    }

    fn serialized_size(&self) -> usize {
        4
    }
}

impl Serializable for i64 {
    fn serialize_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    fn deserialize_from(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 8 {
            return Err(MonoError::Storage("Not enough bytes for i64".into()));
        }
        Ok((
            i64::from_le_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]),
            8,
        ))
    }

    fn fixed_size() -> Option<usize> {
        Some(8)
    }

    fn serialized_size(&self) -> usize {
        8
    }
}

impl Serializable for u32 {
    fn serialize_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    fn deserialize_from(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 4 {
            return Err(MonoError::Storage("Not enough bytes for u32".into()));
        }
        Ok((u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]), 4))
    }

    fn fixed_size() -> Option<usize> {
        Some(4)
    }

    fn serialized_size(&self) -> usize {
        4
    }
}

impl Serializable for u64 {
    fn serialize_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    fn deserialize_from(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 8 {
            return Err(MonoError::Storage("Not enough bytes for u64".into()));
        }
        Ok((
            u64::from_le_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]),
            8,
        ))
    }

    fn fixed_size() -> Option<usize> {
        Some(8)
    }

    fn serialized_size(&self) -> usize {
        8
    }
}

impl Serializable for String {
    fn serialize_to(&self, buf: &mut Vec<u8>) {
        let bytes = self.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    }

    fn deserialize_from(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 4 {
            return Err(MonoError::Storage(
                "Not enough bytes for String length".into(),
            ));
        }
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + len {
            return Err(MonoError::Storage(
                "Not enough bytes for String data".into(),
            ));
        }
        let s = String::from_utf8(buf[4..4 + len].to_vec())
            .map_err(|e| MonoError::Storage(e.to_string()))?;
        Ok((s, 4 + len))
    }

    fn serialized_size(&self) -> usize {
        4 + self.len()
    }
}

impl Serializable for Vec<u8> {
    fn serialize_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.len() as u32).to_le_bytes());
        buf.extend_from_slice(self);
    }

    fn deserialize_from(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 4 {
            return Err(MonoError::Storage("Not enough bytes for Vec length".into()));
        }
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + len {
            return Err(MonoError::Storage("Not enough bytes for Vec data".into()));
        }
        Ok((buf[4..4 + len].to_vec(), 4 + len))
    }

    fn serialized_size(&self) -> usize {
        4 + self.len()
    }
}

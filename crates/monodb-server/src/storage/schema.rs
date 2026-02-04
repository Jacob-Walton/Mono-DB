//! Schema persistence and catalog

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use monodb_common::{MonoError, Result, Value};
use parking_lot::RwLock;

use crate::query_engine::ast::{DataType, TableType};

// Binary format constants
const SCHEMA_MAGIC: u32 = u32::from_be_bytes(*b"SCHM");
const SCHEMA_FOOTER_MAGIC: u32 = u32::from_be_bytes(*b"SCMF");
const SCHEMA_VERSION: u16 = 1;

/// Schema version for MVCC-style versioning
pub type SchemaVersion = u64;

/// Persistent schema information for a table.
#[derive(Debug, Clone)]
pub struct StoredTableSchema {
    /// Table name
    pub name: String,
    /// Table type (Relation, Document, Keyspace)
    pub table_type: StoredTableType,
    /// Column definitions (empty for Document type)
    pub columns: Vec<StoredColumnSchema>,
    /// Primary key column names
    pub primary_key: Vec<String>,
    /// Secondary indexes
    pub indexes: Vec<StoredIndex>,
    /// Schema version
    pub version: SchemaVersion,
    /// Timestamp when this version was created
    pub created_at: i64,
    /// Metadata page ID for B+Tree storage
    pub meta_page_id: u64,
}

/// Serializable table type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredTableType {
    Relation,
    Document,
    Keyspace,
}

impl From<TableType> for StoredTableType {
    fn from(tt: TableType) -> Self {
        match tt {
            TableType::Relational => StoredTableType::Relation,
            TableType::Document => StoredTableType::Document,
            TableType::Keyspace => StoredTableType::Keyspace,
        }
    }
}

impl From<StoredTableType> for TableType {
    fn from(st: StoredTableType) -> Self {
        match st {
            StoredTableType::Relation => TableType::Relational,
            StoredTableType::Document => TableType::Document,
            StoredTableType::Keyspace => TableType::Keyspace,
        }
    }
}

/// Persistent column schema.
#[derive(Debug, Clone)]
pub struct StoredColumnSchema {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: StoredDataType,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Default value (if any)
    pub default: Option<Value>,
}

/// Persistent index definition.
#[derive(Debug, Clone)]
pub struct StoredIndex {
    /// Index name
    pub name: String,
    /// Columns covered by this index
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub unique: bool,
    /// Type of index (BTree, Hash, etc.)
    pub index_type: StoredIndexType,
}

/// Index type for storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredIndexType {
    BTree,
    Hash,
    FullText,
    Spatial,
}

impl StoredIndexType {
    /// Convert to type tag byte for binary serialization
    pub fn to_type_tag(self) -> u8 {
        match self {
            StoredIndexType::BTree => 0x01,
            StoredIndexType::Hash => 0x02,
            StoredIndexType::FullText => 0x03,
            StoredIndexType::Spatial => 0x04,
        }
    }

    /// Create from type tag byte
    pub fn from_type_tag(tag: u8) -> Self {
        match tag {
            0x01 => StoredIndexType::BTree,
            0x02 => StoredIndexType::Hash,
            0x03 => StoredIndexType::FullText,
            0x04 => StoredIndexType::Spatial,
            _ => StoredIndexType::BTree, // Default to BTree
        }
    }
}

/// Serializable data type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredDataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bool,
    Bytes,
    DateTime,
    Date,
    Time,
    Uuid,
    ObjectId,
    Json,
    Array(Box<StoredDataType>),
    Map(Box<StoredDataType>, Box<StoredDataType>),
    Reference(String),
    /// For document columns where type is determined at runtime
    Any,
}

impl From<&DataType> for StoredDataType {
    fn from(dt: &DataType) -> Self {
        match dt {
            DataType::Int32 => StoredDataType::Int32,
            DataType::Int64 => StoredDataType::Int64,
            DataType::Float32 => StoredDataType::Float32,
            DataType::Float64 => StoredDataType::Float64,
            DataType::String => StoredDataType::String,
            DataType::Bool => StoredDataType::Bool,
            DataType::Bytes => StoredDataType::Bytes,
            DataType::DateTime => StoredDataType::DateTime,
            DataType::Date => StoredDataType::Date,
            DataType::Time => StoredDataType::Time,
            DataType::Uuid => StoredDataType::Uuid,
            DataType::ObjectId => StoredDataType::ObjectId,
            DataType::Json => StoredDataType::Json,
            DataType::Array(inner) => StoredDataType::Array(Box::new(inner.as_ref().into())),
            DataType::Map(k, v) => {
                StoredDataType::Map(Box::new(k.as_ref().into()), Box::new(v.as_ref().into()))
            }
            DataType::Reference(name) => StoredDataType::Reference(name.as_str().to_string()),
        }
    }
}

impl StoredDataType {
    /// Estimate the size in bytes for this data type.
    pub fn estimated_size(&self) -> usize {
        match self {
            StoredDataType::Int32 => 4,
            StoredDataType::Int64 => 8,
            StoredDataType::Float32 => 4,
            StoredDataType::Float64 => 8,
            StoredDataType::Bool => 1,
            StoredDataType::String => 32, // Average string size estimate
            StoredDataType::Bytes => 64,  // Average bytes size estimate
            StoredDataType::DateTime => 8,
            StoredDataType::Date => 4,
            StoredDataType::Time => 8,
            StoredDataType::Uuid => 16,
            StoredDataType::ObjectId => 12,
            StoredDataType::Json => 128, // Average JSON size estimate
            StoredDataType::Array(inner) => 8 + inner.estimated_size() * 4, // Assume avg 4 elements
            StoredDataType::Map(k, v) => 8 + (k.estimated_size() + v.estimated_size()) * 4,
            StoredDataType::Reference(_) => 24, // Collection name + id
            StoredDataType::Any => 64,
        }
    }

    /// Convert back to AST DataType
    pub fn to_ast_type(&self) -> DataType {
        use crate::query_engine::ast::Ident;
        match self {
            StoredDataType::Int32 => DataType::Int32,
            StoredDataType::Int64 => DataType::Int64,
            StoredDataType::Float32 => DataType::Float32,
            StoredDataType::Float64 => DataType::Float64,
            StoredDataType::String => DataType::String,
            StoredDataType::Bool => DataType::Bool,
            StoredDataType::Bytes => DataType::Bytes,
            StoredDataType::DateTime => DataType::DateTime,
            StoredDataType::Date => DataType::Date,
            StoredDataType::Time => DataType::Time,
            StoredDataType::Uuid => DataType::Uuid,
            StoredDataType::ObjectId => DataType::ObjectId,
            StoredDataType::Json => DataType::Json,
            StoredDataType::Array(inner) => DataType::Array(Box::new(inner.to_ast_type())),
            StoredDataType::Map(k, v) => {
                DataType::Map(Box::new(k.to_ast_type()), Box::new(v.to_ast_type()))
            }
            StoredDataType::Reference(name) => DataType::Reference(Ident::new(name.clone())),
            // Any maps to Json for AST compatibility
            StoredDataType::Any => DataType::Json,
        }
    }

    /// Convert to type tag byte for binary serialization
    pub fn to_type_tag(&self) -> u8 {
        match self {
            StoredDataType::Int32 => 0x01,
            StoredDataType::Int64 => 0x02,
            StoredDataType::Float32 => 0x03,
            StoredDataType::Float64 => 0x04,
            StoredDataType::String => 0x05,
            StoredDataType::Bool => 0x06,
            StoredDataType::Bytes => 0x07,
            StoredDataType::DateTime => 0x08,
            StoredDataType::Date => 0x09,
            StoredDataType::Time => 0x0A,
            StoredDataType::Uuid => 0x0B,
            StoredDataType::ObjectId => 0x0C,
            StoredDataType::Json => 0x0D,
            StoredDataType::Array(_) => 0x0E,
            StoredDataType::Map(_, _) => 0x0F,
            StoredDataType::Reference(_) => 0x10,
            StoredDataType::Any => 0x11,
        }
    }

    /// Create from type tag byte
    pub fn from_type_tag(tag: u8) -> Self {
        match tag {
            0x01 => StoredDataType::Int32,
            0x02 => StoredDataType::Int64,
            0x03 => StoredDataType::Float32,
            0x04 => StoredDataType::Float64,
            0x05 => StoredDataType::String,
            0x06 => StoredDataType::Bool,
            0x07 => StoredDataType::Bytes,
            0x08 => StoredDataType::DateTime,
            0x09 => StoredDataType::Date,
            0x0A => StoredDataType::Time,
            0x0B => StoredDataType::Uuid,
            0x0C => StoredDataType::ObjectId,
            0x0D => StoredDataType::Json,
            0x0E => StoredDataType::Array(Box::new(StoredDataType::Any)), // TODO: nested types
            0x0F => {
                StoredDataType::Map(Box::new(StoredDataType::Any), Box::new(StoredDataType::Any))
            }
            0x10 => StoredDataType::Reference(String::new()), // TODO: read ref name
            _ => StoredDataType::Any,
        }
    }
}

/// Schema catalog, manages all table schemas with versioning.
pub struct SchemaCatalog {
    /// Current schemas by table name
    schemas: RwLock<HashMap<String, Arc<StoredTableSchema>>>,
    /// Historical schema versions for MVCC (table_name -> version -> schema)
    history: RwLock<HashMap<String, HashMap<SchemaVersion, Arc<StoredTableSchema>>>>,
    /// Next schema version counter
    next_version: RwLock<SchemaVersion>,
    /// Path to schema file
    schema_file: std::path::PathBuf,
}

impl SchemaCatalog {
    /// Create a new schema catalog, loading from disk if the file exists.
    pub fn new(data_dir: &Path) -> Result<Self> {
        let schema_file = data_dir.join("schemas.bin");

        let (schemas, next_version) = if schema_file.exists() {
            Self::load_from_file(&schema_file)?
        } else {
            (HashMap::new(), 1)
        };

        Ok(Self {
            schemas: RwLock::new(schemas),
            history: RwLock::new(HashMap::new()),
            next_version: RwLock::new(next_version),
            schema_file,
        })
    }

    /// Load schemas from binary file
    fn load_from_file(
        path: &Path,
    ) -> Result<(HashMap<String, Arc<StoredTableSchema>>, SchemaVersion)> {
        let data = std::fs::read(path)
            .map_err(|e| MonoError::Io(format!("Failed to read schema file: {}", e)))?;

        if data.len() < 24 {
            return Err(MonoError::Parse("Schema file too short".into()));
        }

        let mut offset = 0;

        // Header (16 bytes)
        let magic = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if magic != SCHEMA_MAGIC {
            return Err(MonoError::Parse(format!(
                "Invalid schema magic: expected 0x{:08X}, got 0x{:08X}",
                SCHEMA_MAGIC, magic
            )));
        }

        let version = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if version != SCHEMA_VERSION {
            return Err(MonoError::Parse(format!(
                "Unsupported schema version: {}",
                version
            )));
        }

        let _flags = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let schema_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let _reserved = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Footer verification
        let footer_start = data.len() - 8;
        let stored_crc =
            u32::from_le_bytes(data[footer_start..footer_start + 4].try_into().unwrap());
        let footer_magic =
            u32::from_le_bytes(data[footer_start + 4..footer_start + 8].try_into().unwrap());

        if footer_magic != SCHEMA_FOOTER_MAGIC {
            return Err(MonoError::Parse(format!(
                "Invalid schema footer magic: expected 0x{:08X}, got 0x{:08X}",
                SCHEMA_FOOTER_MAGIC, footer_magic
            )));
        }

        let computed_crc = crc32fast::hash(&data[..footer_start]);
        if computed_crc != stored_crc {
            return Err(MonoError::Parse(format!(
                "Schema CRC mismatch: expected 0x{:08X}, got 0x{:08X}",
                stored_crc, computed_crc
            )));
        }

        // Schema Entries
        let mut schemas = HashMap::new();
        let mut max_version: SchemaVersion = 0;

        for _ in 0..schema_count {
            let (schema, used) = Self::deserialize_schema_entry(&data[offset..footer_start])?;
            offset += used;
            if schema.version > max_version {
                max_version = schema.version;
            }
            schemas.insert(schema.name.clone(), Arc::new(schema));
        }

        Ok((schemas, max_version + 1))
    }

    /// Persist current schemas to disk in binary format
    fn persist(&self) -> Result<()> {
        let schemas = self.schemas.read();
        let schema_list: Vec<&StoredTableSchema> = schemas.values().map(|s| s.as_ref()).collect();

        let mut bytes = Vec::new();

        // Header (16 bytes)
        bytes.extend(&SCHEMA_MAGIC.to_le_bytes());
        bytes.extend(&SCHEMA_VERSION.to_le_bytes());
        bytes.extend(&0u16.to_le_bytes()); // FLAGS
        bytes.extend(&(schema_list.len() as u32).to_le_bytes());
        bytes.extend(&0u32.to_le_bytes()); // RESERVED

        // Schema Entries
        for schema in &schema_list {
            Self::serialize_schema_entry(&mut bytes, schema)?;
        }

        // Footer (8 bytes)
        let crc = crc32fast::hash(&bytes);
        bytes.extend(&crc.to_le_bytes());
        bytes.extend(&SCHEMA_FOOTER_MAGIC.to_le_bytes());

        let temp_path = self.schema_file.with_extension("bin.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| MonoError::Io(format!("Failed to write schema file: {}", e)))?;
        file.write_all(&bytes)
            .map_err(|e| MonoError::Io(format!("Failed to write schema file: {}", e)))?;
        file.sync_all()
            .map_err(|e| MonoError::Io(format!("Failed to sync schema file: {}", e)))?;
        std::fs::rename(&temp_path, &self.schema_file)
            .map_err(|e| MonoError::Io(format!("Failed to rename schema file: {}", e)))?;
        Self::sync_parent_dir(&self.schema_file);

        Ok(())
    }

    fn sync_parent_dir(path: &Path) {
        if let Some(parent) = path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
        {
            let _ = dir.sync_all();
        }
    }

    // Binary serialization helpers

    fn write_string(bytes: &mut Vec<u8>, s: &str) {
        bytes.extend(&(s.len() as u32).to_le_bytes());
        bytes.extend(s.as_bytes());
    }

    fn read_string(data: &[u8]) -> Result<(String, usize)> {
        if data.len() < 4 {
            return Err(MonoError::Parse("String too short".into()));
        }
        let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if data.len() < 4 + len {
            return Err(MonoError::Parse("String data truncated".into()));
        }
        let s = String::from_utf8(data[4..4 + len].to_vec())
            .map_err(|e| MonoError::Parse(format!("Invalid UTF-8: {}", e)))?;
        Ok((s, 4 + len))
    }

    /// Serialize a schema entry
    fn serialize_schema_entry(bytes: &mut Vec<u8>, schema: &StoredTableSchema) -> Result<()> {
        // schema_type: u8
        let type_byte = match schema.table_type {
            StoredTableType::Relation => 0u8,
            StoredTableType::Document => 1u8,
            StoredTableType::Keyspace => 2u8,
        };
        bytes.push(type_byte);

        // name: String
        Self::write_string(bytes, &schema.name);

        // version: u64
        bytes.extend(&schema.version.to_le_bytes());

        // created_at: i64
        bytes.extend(&schema.created_at.to_le_bytes());

        // meta_page_id: u64
        bytes.extend(&schema.meta_page_id.to_le_bytes());

        match schema.table_type {
            StoredTableType::Relation => {
                // Table Schema Payload
                bytes.extend(&(schema.columns.len() as u32).to_le_bytes());
                for col in &schema.columns {
                    Self::serialize_column(bytes, col)?;
                }
                bytes.extend(&(schema.primary_key.len() as u32).to_le_bytes());
                for pk in &schema.primary_key {
                    Self::write_string(bytes, pk);
                }
                // index_count and indexes
                bytes.extend(&(schema.indexes.len() as u32).to_le_bytes());
                for idx in &schema.indexes {
                    Self::serialize_index(bytes, idx);
                }
            }
            StoredTableType::Document => {
                // Collection Schema Payload
                bytes.push(0u8); // has_validation = false
                // index_count and indexes
                bytes.extend(&(schema.indexes.len() as u32).to_le_bytes());
                for idx in &schema.indexes {
                    Self::serialize_index(bytes, idx);
                }
            }
            StoredTableType::Keyspace => {
                // KeySpace Schema Payload
                bytes.push(0u8); // flags
                bytes.push(0u8); // has_max_size = false
            }
        }

        Ok(())
    }

    /// Serialize a column
    fn serialize_column(bytes: &mut Vec<u8>, col: &StoredColumnSchema) -> Result<()> {
        Self::write_string(bytes, &col.name);
        bytes.push(col.data_type.to_type_tag());

        // flags: bit 0: nullable
        let flags = if col.nullable { 0b0000_0001u8 } else { 0u8 };
        bytes.push(flags);

        // has_default: u8
        if let Some(default) = &col.default {
            bytes.push(1);
            let default_bytes = default.to_bytes();
            bytes.extend(&(default_bytes.len() as u32).to_le_bytes());
            bytes.extend(&default_bytes);
        } else {
            bytes.push(0);
        }

        Ok(())
    }

    /// Serialize an index
    fn serialize_index(bytes: &mut Vec<u8>, idx: &StoredIndex) {
        // name: String
        Self::write_string(bytes, &idx.name);
        // column_count: u32 + columns
        bytes.extend(&(idx.columns.len() as u32).to_le_bytes());
        for col in &idx.columns {
            Self::write_string(bytes, col);
        }
        // flags: bit 0 = unique
        let flags = if idx.unique { 0b0000_0001u8 } else { 0u8 };
        bytes.push(flags);
        // index_type: u8
        bytes.push(idx.index_type.to_type_tag());
    }

    /// Deserialize an index
    fn deserialize_index(data: &[u8]) -> Result<(StoredIndex, usize)> {
        let mut offset = 0;

        // name
        let (name, used) = Self::read_string(&data[offset..])?;
        offset += used;

        // columns
        let col_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let (col, used) = Self::read_string(&data[offset..])?;
            offset += used;
            columns.push(col);
        }

        // flags
        let flags = data[offset];
        offset += 1;
        let unique = (flags & 0b0000_0001) != 0;

        // index_type
        let type_tag = data[offset];
        offset += 1;

        let idx = StoredIndex {
            name,
            columns,
            unique,
            index_type: StoredIndexType::from_type_tag(type_tag),
        };

        Ok((idx, offset))
    }

    /// Deserialize a schema entry
    fn deserialize_schema_entry(data: &[u8]) -> Result<(StoredTableSchema, usize)> {
        if data.is_empty() {
            return Err(MonoError::Parse("Empty schema entry".into()));
        }

        let mut offset = 0;
        let schema_type = data[offset];
        offset += 1;

        let (name, used) = Self::read_string(&data[offset..])?;
        offset += used;

        // version: u64
        let version = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // created_at: i64
        let created_at = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // meta_page_id: u64
        let meta_page_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let (table_type, columns, primary_key, indexes) = match schema_type {
            0 => {
                // Table
                let column_count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut columns = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    let (col, used) = Self::deserialize_column(&data[offset..])?;
                    offset += used;
                    columns.push(col);
                }

                let pk_count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut primary_key = Vec::with_capacity(pk_count);
                for _ in 0..pk_count {
                    let (pk, used) = Self::read_string(&data[offset..])?;
                    offset += used;
                    primary_key.push(pk);
                }

                // Read indexes
                let index_count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut indexes = Vec::with_capacity(index_count);
                for _ in 0..index_count {
                    let (idx, used) = Self::deserialize_index(&data[offset..])?;
                    offset += used;
                    indexes.push(idx);
                }

                (StoredTableType::Relation, columns, primary_key, indexes)
            }
            1 => {
                // Collection
                let _has_validation = data[offset];
                offset += 1;
                // Skip validation rule if present

                // Read indexes
                let index_count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut indexes = Vec::with_capacity(index_count);
                for _ in 0..index_count {
                    let (idx, used) = Self::deserialize_index(&data[offset..])?;
                    offset += used;
                    indexes.push(idx);
                }

                (StoredTableType::Document, vec![], vec![], indexes)
            }
            2 => {
                // KeySpace
                let _flags = data[offset];
                offset += 1;
                let has_max_size = data[offset];
                offset += 1;
                if has_max_size == 1 {
                    offset += 8; // skip max_size
                }

                (StoredTableType::Keyspace, vec![], vec![], vec![])
            }
            _ => {
                return Err(MonoError::Parse(format!(
                    "Unknown schema type: {}",
                    schema_type
                )));
            }
        };

        let schema = StoredTableSchema {
            name,
            table_type,
            columns,
            primary_key,
            indexes,
            version,
            created_at,
            meta_page_id,
        };

        Ok((schema, offset))
    }

    /// Encode a schema entry for WAL or snapshot storage.
    pub(crate) fn encode_schema_entry(schema: &StoredTableSchema) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        Self::serialize_schema_entry(&mut buf, schema)?;
        Ok(buf)
    }

    /// Decode a schema entry.
    pub(crate) fn decode_schema_entry(data: &[u8]) -> Result<StoredTableSchema> {
        let (schema, used) = Self::deserialize_schema_entry(data)?;
        if used != data.len() {
            return Err(MonoError::Parse("Extra bytes in schema entry".into()));
        }
        Ok(schema)
    }

    /// Deserialize a column
    fn deserialize_column(data: &[u8]) -> Result<(StoredColumnSchema, usize)> {
        let mut offset = 0;

        let (name, used) = Self::read_string(&data[offset..])?;
        offset += used;

        let type_tag = data[offset];
        offset += 1;

        let flags = data[offset];
        offset += 1;
        let nullable = (flags & 0b0000_0001) != 0;

        let has_default = data[offset];
        offset += 1;

        let default = if has_default == 1 {
            let default_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let (value, _) = Value::from_bytes(&data[offset..offset + default_len])?;
            offset += default_len;
            Some(value)
        } else {
            None
        };

        let col = StoredColumnSchema {
            name,
            data_type: StoredDataType::from_type_tag(type_tag),
            nullable,
            default,
        };

        Ok((col, offset))
    }

    /// Get the current schema for a table
    pub fn get(&self, table: &str) -> Option<Arc<StoredTableSchema>> {
        self.schemas.read().get(table).cloned()
    }

    /// Get a specific version of a schema
    pub fn get_version(
        &self,
        table: &str,
        version: SchemaVersion,
    ) -> Option<Arc<StoredTableSchema>> {
        // First check current
        let current = self.schemas.read().get(table).cloned();
        if let Some(schema) = &current
            && schema.version == version
        {
            return current;
        }

        // Check history
        self.history
            .read()
            .get(table)
            .and_then(|versions| versions.get(&version).cloned())
    }

    /// Get the schema version that was current at a given transaction start
    pub fn get_for_tx(
        &self,
        table: &str,
        tx_start_version: SchemaVersion,
    ) -> Option<Arc<StoredTableSchema>> {
        let schemas = self.schemas.read();
        let current = schemas.get(table)?;

        // If current version is <= tx_start_version, use it
        if current.version <= tx_start_version {
            return Some(current.clone());
        }

        // Otherwise, find the highest version <= tx_start_version in history
        drop(schemas);
        let history = self.history.read();
        let versions = history.get(table)?;

        versions
            .iter()
            .filter(|(v, _)| **v <= tx_start_version)
            .max_by_key(|(v, _)| *v)
            .map(|(_, schema)| schema.clone())
    }

    /// Register a new table schema
    pub fn register(&self, schema: StoredTableSchema) -> Result<()> {
        let mut schemas = self.schemas.write();

        if schemas.contains_key(&schema.name) {
            return Err(MonoError::AlreadyExists(format!(
                "Table '{}' already exists",
                schema.name
            )));
        }

        let mut version_lock = self.next_version.write();
        let version = *version_lock;
        *version_lock = version + 1;

        let mut versioned_schema = schema;
        versioned_schema.version = version;
        versioned_schema.created_at = chrono::Utc::now().timestamp();

        schemas.insert(versioned_schema.name.clone(), Arc::new(versioned_schema));

        drop(schemas);
        drop(version_lock);
        self.persist()?;

        Ok(())
    }

    /// Update a table schema (ALTER TABLE)
    pub fn update(&self, name: &str, new_schema: StoredTableSchema) -> Result<()> {
        let mut schemas = self.schemas.write();
        let mut history = self.history.write();

        let old_schema = schemas
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", name)))?
            .clone();

        // Store old version in history for MVCC
        history
            .entry(name.to_string())
            .or_default()
            .insert(old_schema.version, old_schema);

        // Create new version
        let mut version_lock = self.next_version.write();
        let version = *version_lock;
        *version_lock = version + 1;

        let mut versioned_schema = new_schema;
        versioned_schema.version = version;
        versioned_schema.created_at = chrono::Utc::now().timestamp();

        schemas.insert(name.to_string(), Arc::new(versioned_schema));

        drop(schemas);
        drop(history);
        drop(version_lock);
        self.persist()?;

        Ok(())
    }

    /// Add columns to an existing table schema
    pub fn add_columns(&self, name: &str, columns: &[StoredColumnSchema]) -> Result<()> {
        let current = self
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", name)))?;

        if current.table_type != StoredTableType::Relation {
            return Err(MonoError::InvalidOperation(
                "Can only add columns to relational tables".into(),
            ));
        }

        // Check for duplicate column names
        for new_col in columns {
            if current.columns.iter().any(|c| c.name == new_col.name) {
                return Err(MonoError::AlreadyExists(format!(
                    "Column '{}' already exists in table '{}'",
                    new_col.name, name
                )));
            }
        }

        let mut new_schema = (*current).clone();
        new_schema.columns.extend(columns.iter().cloned());

        self.update(name, new_schema)
    }

    /// Drop columns from an existing table schema
    pub fn drop_columns(&self, name: &str, columns: &[String]) -> Result<()> {
        let current = self
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", name)))?;

        if current.table_type != StoredTableType::Relation {
            return Err(MonoError::InvalidOperation(
                "Can only drop columns from relational tables".into(),
            ));
        }

        // Check columns exist and aren't primary keys
        for col_name in columns {
            if !current.columns.iter().any(|c| &c.name == col_name) {
                return Err(MonoError::NotFound(format!(
                    "Column '{}' not found in table '{}'",
                    col_name, name
                )));
            }
            if current.primary_key.contains(col_name) {
                return Err(MonoError::InvalidOperation(format!(
                    "Cannot drop primary key column '{}'",
                    col_name
                )));
            }
        }

        let mut new_schema = (*current).clone();
        new_schema.columns.retain(|c| !columns.contains(&c.name));

        self.update(name, new_schema)
    }

    /// Rename columns in an existing table schema
    pub fn rename_columns(&self, name: &str, renames: &[(String, String)]) -> Result<()> {
        let current = self
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", name)))?;

        if current.table_type != StoredTableType::Relation {
            return Err(MonoError::InvalidOperation(
                "Can only rename columns in relational tables".into(),
            ));
        }

        // Validate renames
        for (old_name, new_name) in renames {
            if !current.columns.iter().any(|c| &c.name == old_name) {
                return Err(MonoError::NotFound(format!(
                    "Column '{}' not found in table '{}'",
                    old_name, name
                )));
            }
            if current.columns.iter().any(|c| &c.name == new_name) {
                return Err(MonoError::AlreadyExists(format!(
                    "Column '{}' already exists in table '{}'",
                    new_name, name
                )));
            }
        }

        let mut new_schema = (*current).clone();

        // Apply renames to columns
        for col in &mut new_schema.columns {
            for (old_name, new_name) in renames {
                if &col.name == old_name {
                    col.name = new_name.clone();
                    break;
                }
            }
        }

        // Apply renames to primary key
        for pk in &mut new_schema.primary_key {
            for (old_name, new_name) in renames {
                if pk == old_name {
                    *pk = new_name.clone();
                    break;
                }
            }
        }

        self.update(name, new_schema)
    }

    /// Alter a column's properties (default, nullability, type)
    pub fn alter_column(
        &self,
        table_name: &str,
        column_name: &str,
        new_default: Option<Option<Value>>, // Some(None) = remove default, Some(Some(v)) = set default
        new_nullable: Option<bool>,
        new_type: Option<StoredDataType>,
    ) -> Result<()> {
        let current = self
            .get(table_name)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", table_name)))?;

        if current.table_type != StoredTableType::Relation {
            return Err(MonoError::InvalidOperation(
                "Can only alter columns in relational tables".into(),
            ));
        }

        // Find the column
        let col_idx = current
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| {
                MonoError::NotFound(format!(
                    "Column '{}' not found in table '{}'",
                    column_name, table_name
                ))
            })?;

        let mut new_schema = (*current).clone();
        let col = &mut new_schema.columns[col_idx];

        // Apply changes
        if let Some(default_change) = new_default {
            col.default = default_change;
        }

        if let Some(nullable) = new_nullable {
            col.nullable = nullable;
        }

        if let Some(data_type) = new_type {
            col.data_type = data_type;
        }

        self.update(table_name, new_schema)
    }

    /// Rename a table in the schema catalog
    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        let current = self
            .get(old_name)
            .ok_or_else(|| MonoError::NotFound(format!("Table '{}' not found", old_name)))?;

        if self.exists(new_name) {
            return Err(MonoError::AlreadyExists(format!(
                "Table '{}' already exists",
                new_name
            )));
        }

        let mut new_schema = (*current).clone();
        new_schema.name = new_name.to_string();

        // Remove old and add new
        let mut schemas = self.schemas.write();
        let mut history = self.history.write();

        // Store old version in history
        if let Some(old_schema) = schemas.remove(old_name) {
            history
                .entry(old_name.to_string())
                .or_default()
                .insert(old_schema.version, old_schema);
        }

        // Create new version
        let mut version_lock = self.next_version.write();
        let version = *version_lock;
        *version_lock = version + 1;

        new_schema.version = version;
        new_schema.created_at = chrono::Utc::now().timestamp();

        schemas.insert(new_name.to_string(), Arc::new(new_schema));

        drop(schemas);
        drop(history);
        drop(version_lock);
        self.persist()?;

        Ok(())
    }

    /// Remove a table schema
    /// Returns Ok(()) if schema doesn't exist.
    pub fn remove(&self, name: &str) -> Result<()> {
        let mut schemas = self.schemas.write();
        let mut history = self.history.write();

        let old_schema = match schemas.remove(name) {
            Some(schema) => schema,
            None => return Ok(()), // Schema doesn't exist, nothing to remove
        };

        // Store in history for any in-flight transactions
        history
            .entry(name.to_string())
            .or_default()
            .insert(old_schema.version, old_schema);

        drop(schemas);
        drop(history);
        self.persist()?;

        Ok(())
    }

    /// List all current table schemas
    pub fn list(&self) -> Vec<Arc<StoredTableSchema>> {
        self.schemas.read().values().cloned().collect()
    }

    /// Get the current schema version counter
    pub fn current_version(&self) -> SchemaVersion {
        *self.next_version.read() - 1
    }

    /// Clean up old schema versions that are no longer needed
    pub fn cleanup_old_versions(&self, min_active_version: SchemaVersion) {
        let mut history = self.history.write();

        for versions in history.values_mut() {
            versions.retain(|version, _| *version >= min_active_version);
        }

        // Remove empty tables from history
        history.retain(|_, versions| !versions.is_empty());
    }

    /// Check if a table exists
    pub fn exists(&self, name: &str) -> bool {
        self.schemas.read().contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_schema_catalog_basic() {
        let dir = tempdir().unwrap();
        let catalog = SchemaCatalog::new(dir.path()).unwrap();

        // Register a table
        let schema = StoredTableSchema {
            name: "users".to_string(),
            table_type: StoredTableType::Relation,
            columns: vec![
                StoredColumnSchema {
                    name: "id".to_string(),
                    data_type: StoredDataType::Int64,
                    nullable: false,
                    default: None,
                },
                StoredColumnSchema {
                    name: "name".to_string(),
                    data_type: StoredDataType::String,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            version: 0,
            created_at: 0,
            meta_page_id: 0,
        };

        catalog.register(schema).unwrap();

        // Verify it exists
        assert!(catalog.exists("users"));
        let retrieved = catalog.get("users").unwrap();
        assert_eq!(retrieved.columns.len(), 2);
    }

    #[test]
    fn test_schema_persistence() {
        let dir = tempdir().unwrap();

        // Create and register
        {
            let catalog = SchemaCatalog::new(dir.path()).unwrap();
            let schema = StoredTableSchema {
                name: "test".to_string(),
                table_type: StoredTableType::Document,
                columns: vec![],
                primary_key: vec![],
                indexes: vec![],
                version: 0,
                created_at: 0,
                meta_page_id: 0,
            };
            catalog.register(schema).unwrap();
        }

        // Reload and verify
        {
            let catalog = SchemaCatalog::new(dir.path()).unwrap();
            assert!(catalog.exists("test"));
            let schema = catalog.get("test").unwrap();
            assert_eq!(schema.table_type, StoredTableType::Document);
        }
    }
}

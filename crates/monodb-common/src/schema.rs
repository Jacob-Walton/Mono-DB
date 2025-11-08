use crate::{Value, ValueType};
use serde::{Deserialize, Serialize};

/// Schema definitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Schema {
    Table {
        name: String,
        columns: Vec<TableColumn>,
        primary_key: Vec<String>,
        indexes: Vec<Index>,
    },
    Collection {
        name: String,
        validation: Option<ValidationRule>,
        indexes: Vec<Index>,
    },
    KeySpace {
        name: String,
        ttl_enabled: bool,
        max_size: Option<usize>,
        persistence: KeySpacePersistence,
    },
}

/// Keyspace persistence modes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeySpacePersistence {
    /// Persist to disk
    Persistent,
    /// Keep in memory only, data lost on restart
    Memory,
}

/// Table column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumn {
    pub name: String,
    pub data_type: ValueType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub is_primary: bool,
    pub is_unique: bool,
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
}

/// Index types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    FullText,
    Spatial,
}

/// Validation rule for collections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationRule {
    JsonSchema(String),
    Custom(String),
}

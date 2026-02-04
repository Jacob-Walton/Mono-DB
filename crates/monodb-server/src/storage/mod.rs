#![allow(unused_imports)]
#![allow(dead_code)]

//! Storage engine for MonoDB.

mod btree;
mod buffer;
mod disk;
mod document;
pub mod engine;
mod keyspace;
mod models;
mod mvcc;
mod page;
pub mod schema;
mod traits;
mod wal;

// Re-exports
pub use btree::BTree;
pub use buffer::{BufferFrame, BufferPoolStats, LruBufferPool};
pub use disk::{DiskConfig, DiskManager};
pub use document::{Document, DocumentStore};
pub use engine::{StorageConfig, StorageEngine, StorageStats, StorageType, TableInfo, Transaction};
pub use keyspace::{DiskKeyspace, Keyspace, KeyspaceBackend, MemoryKeyspace, TtlEntry};
pub use mvcc::{MvccRecord, MvccTable, Snapshot, TransactionManager, TxState};
pub use page::{DiskPage, PAGE_SIZE, PageHeader, PageId, PageType, Slot};
pub use schema::{
    SchemaCatalog, SchemaVersion, StoredColumnSchema, StoredDataType, StoredTableSchema,
    StoredTableType,
};
pub use traits::{
    BufferPool as BufferPoolTrait, IsolationLevel, MvccStore, PageStore, Serializable, Tree,
    VersionedStore,
};
pub use wal::{Wal, WalConfig, WalEntry, WalEntryHeader, WalEntryType};

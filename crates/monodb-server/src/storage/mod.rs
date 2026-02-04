#![allow(unused_imports)]
#![allow(dead_code)]

//! Storage engine for MonoDB.

mod models;
mod buffer;
mod disk;
mod page;
mod traits;
mod btree;
mod document;
mod keyspace;
mod mvcc;
mod wal;
pub mod schema;
pub mod engine;

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

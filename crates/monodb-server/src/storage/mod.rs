#![allow(unused_imports)]
#![allow(dead_code)]

//! Storage engine for MonoDB.

mod models;

// Core infrastructure
mod buffer;
mod disk;
mod page;
mod traits;

// Data structures
mod btree;

// Storage backends
mod document;
mod keyspace;
mod mvcc;

// Durability
mod wal;

// Unified interface
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
pub use traits::{
    BufferPool as BufferPoolTrait, IsolationLevel, MvccStore, PageStore, Serializable, Tree,
    VersionedStore,
};
pub use wal::{Wal, WalConfig, WalEntry, WalEntryHeader, WalEntryType};

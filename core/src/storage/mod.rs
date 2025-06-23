//! Storage engine module providing ACID-compliant data persistence

pub mod buffer_pool;
pub mod catalog;
pub mod disk;
pub mod engine;
pub mod index;
pub mod page;
pub mod table;
pub mod transaction;
pub mod wal;

// Re-export core types
pub use buffer_pool::{BufferPool, FrameId};
pub use catalog::{CatalogEntry, PersistentCatalog};
pub use disk::DiskManager;
pub use engine::StorageEngine;
pub use index::{Index, IndexKey};
pub use page::{PAGE_SIZE, Page, PageId, PageType};
pub use table::{Column, ColumnConstraint, DataType, Table, TableCatalog, TableId, TableSchema};
pub use transaction::{IsolationLevel, Transaction, TransactionManager};
pub use wal::{Lsn, TxnId, WalManager, WalRecord};

// Create alias for compatibility
pub use engine::StorageEngine as Engine;

/// Result type for storage operations
pub type StorageResult<T> = crate::error::MonoResult<T>;

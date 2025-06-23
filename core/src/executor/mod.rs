mod engine;
mod error;
mod result;
mod storage;

pub use engine::Executor;
pub use error::{ExecutionError, ExecutionResult};
pub use result::{QueryResult, Row};
pub use storage::{ColumnInfo, MemoryStorage, PersistentStorage, StorageEngine, Value};

use crate::nsql::interner::StringInterner;

pub struct ExecutionContext {
    pub interner: StringInterner,
    pub storage: Box<dyn StorageEngine>,
}

impl ExecutionContext {
    pub fn new_with_memory_storage() -> Self {
        Self {
            interner: StringInterner::new(),
            storage: Box::new(MemoryStorage::new()),
        }
    }

    pub fn new_with_persistent_storage(
        storage_engine: std::sync::Arc<std::sync::RwLock<crate::storage::Engine>>,
    ) -> Self {
        Self {
            interner: StringInterner::new(),
            storage: Box::new(PersistentStorage::new(storage_engine)),
        }
    }
}

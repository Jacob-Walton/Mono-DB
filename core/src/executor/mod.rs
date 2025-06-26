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
	storage_engine: Option<std::sync::Arc<std::sync::RwLock<crate::storage::Engine>>>,
}

impl ExecutionContext {
	pub fn new_with_memory_storage() -> Self {
		Self {
			interner: StringInterner::new(),
			storage: Box::new(MemoryStorage::new()),
			storage_engine: None,
		}
	}

	pub fn new_with_persistent_storage(
		storage_engine: std::sync::Arc<std::sync::RwLock<crate::storage::Engine>>,
	) -> Self {
		Self {
			interner: StringInterner::new(),
			storage: Box::new(PersistentStorage::new(storage_engine.clone())),
			storage_engine: Some(storage_engine),
		}
	}

	/// Get access to the persistent storage engine if available
	pub fn get_storage_engine(
		&self,
	) -> Option<&std::sync::Arc<std::sync::RwLock<crate::storage::Engine>>> {
		self.storage_engine.as_ref()
	}
}

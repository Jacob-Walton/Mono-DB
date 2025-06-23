//! MonoDB - database engine

pub mod error;
pub mod executor;
pub mod memory;
pub mod network;
pub mod nsql;
pub mod server;
pub mod storage;

pub use error::{MonoError, MonoResult};
pub use executor::{ExecutionContext, Executor, MemoryStorage, PersistentStorage, StorageEngine};
pub use nsql::{Program, Result, StringInterner};

// TODO: Implement database configuration in database binary and config file.

/// Database configuration
#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: String,
    pub port: u16,
    pub max_connections: usize,
    pub buffer_pool_size: usize,
    pub wal_enabled: bool,
    pub checkpoint_interval_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            port: 3282,
            max_connections: 100,
            buffer_pool_size: 1024, // 1024 pages
            wal_enabled: true,
            checkpoint_interval_secs: 300, // 5 minutes
        }
    }
}

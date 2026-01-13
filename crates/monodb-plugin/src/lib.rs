//! MonoDB Plugin System
//!
//! Provides WebAssembly-based plugin support for extending the database
//! with user-defined functions, custom types, and more.

mod bindings;
mod component_cache;
mod config;
mod database_impl;
mod function_registry;
mod host;
mod output_store;
mod registry;
mod state;
mod tasks;
mod value_bridge;
mod watcher;

// Re-exports
pub use config::{
    CacheConfig, EpochConfig, FuelConfig, LimitsConfig, PluginHostConfig, SizeLimitsConfig,
};
pub use function_registry::{
    FunctionInfoHost, FunctionRegistry, ParamInfoHost, RegisteredFunction,
};
pub use host::PluginHost;
pub use registry::{PluginInfo, PluginRegistry};
pub use state::PluginPermissions;
pub use tasks::{PluginResult, TaskStatus};
pub use value_bridge::ValueBridge;
pub use watcher::PluginWatcher;

/// Trait for storage backends that plugins can use.
/// This is implemented by StorageAdapter in monodb-server.
pub trait PluginStorage: Send + Sync {
    fn table_exists(&self, table: &str) -> bool;
    fn begin_transaction(&self) -> anyhow::Result<u64>;
    fn begin_read_only(&self) -> anyhow::Result<u64>;
    fn commit(&self, tx_id: u64) -> anyhow::Result<()>;
    fn rollback(&self, tx_id: u64) -> anyhow::Result<()>;

    fn scan(
        &self,
        tx_id: u64,
        table: &str,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> anyhow::Result<Vec<monodb_common::Value>>;

    fn get(
        &self,
        tx_id: u64,
        table: &str,
        key: &monodb_common::Value,
    ) -> anyhow::Result<Option<monodb_common::Value>>;

    fn insert(
        &self,
        tx_id: u64,
        table: &str,
        row: monodb_common::Value,
    ) -> anyhow::Result<Option<monodb_common::Value>>;

    fn update(
        &self,
        tx_id: u64,
        table: &str,
        key: &monodb_common::Value,
        updates: Vec<(String, monodb_common::Value)>,
    ) -> anyhow::Result<bool>;

    fn delete(&self, tx_id: u64, table: &str, key: &monodb_common::Value) -> anyhow::Result<bool>;

    /// List tables as (name, type) tuples
    fn list_tables(&self) -> anyhow::Result<Vec<(String, String)>>;
}

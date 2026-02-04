//! Configuration for the plugin host

use std::path::PathBuf;

/// Configuration for resource limits
#[derive(Clone, Debug)]
pub struct LimitsConfig {
    pub max_memory_size: usize,
    pub max_tables: usize,
    pub max_table_elements: usize,
    pub max_instances: usize,
    pub max_memories: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_memory_size: 128 * 1024 * 1024, // 128 MiB
            max_tables: 10,
            max_table_elements: 10_000,
            max_instances: 10,
            max_memories: 1,
        }
    }
}

/// Configuration for WASM plugin size limits
#[derive(Clone, Debug)]
pub struct SizeLimitsConfig {
    /// Soft limit: warn when plugin exceeds this size (bytes)
    pub soft_limit: u64,
    /// Hard limit: reject plugins exceeding this size (bytes)
    pub hard_limit: u64,
}

impl Default for SizeLimitsConfig {
    fn default() -> Self {
        Self {
            soft_limit: 5 * 1024 * 1024,  // 5 MiB
            hard_limit: 50 * 1024 * 1024, // 50 MiB
        }
    }
}

/// Configuration for epoch-based interruption
#[derive(Clone, Debug)]
pub struct EpochConfig {
    pub enabled: bool,
    pub default_deadline: u64,
    pub max_deadline: u64,
    pub long_running_deadline: u64,
    pub increment_interval_secs: u64,
}

impl Default for EpochConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_deadline: 4,
            max_deadline: 16,
            long_running_deadline: 3600, // 1 hour
            increment_interval_secs: 1,
        }
    }
}

/// Configuration for fuel-based execution limiting
#[derive(Clone, Debug)]
pub struct FuelConfig {
    pub enabled: bool,
    pub default_fuel: u64,
    pub max_fuel: u64,
}

impl Default for FuelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_fuel: 100_000_000,
            max_fuel: 1_000_000_000,
        }
    }
}

/// Configuration for component caching
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Whether to persist compiled components to disk
    pub persist: bool,
    /// Custom cache directory (None = use temp dir)
    pub cache_dir: Option<PathBuf>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            persist: true,
            cache_dir: None,
        }
    }
}

/// Main configuration for the plugin host
#[derive(Clone, Debug, Default)]
pub struct PluginHostConfig {
    pub limits: LimitsConfig,
    pub size_limits: SizeLimitsConfig,
    pub cache: CacheConfig,
    pub epoch: EpochConfig,
    pub fuel: FuelConfig,
    /// Directory to scan for plugins
    pub plugins_dir: PathBuf,
    /// Enable hot-reload via filesystem watcher
    pub hot_reload: bool,
}

impl PluginHostConfig {
    pub fn new(plugins_dir: impl Into<PathBuf>) -> Self {
        Self {
            plugins_dir: plugins_dir.into(),
            hot_reload: true,
            ..Default::default()
        }
    }
}

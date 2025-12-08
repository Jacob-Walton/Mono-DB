use crate::daemon::config::ServerConfig;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{env, fs};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsmConfig {
    pub memtable_size: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub level0_size: usize,
    pub level_multiplier: usize,
    pub compression: bool,
    pub max_level: usize,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_size: 512 * 1024 * 1024,
            level0_file_num_compaction_trigger: 8,
            level0_size: 512 * 1024 * 1024,
            level_multiplier: 10,
            compression: true,
            max_level: 7,
        }
    }
}

/// WAL configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Buffer size for writes (default: 64KB)
    pub buffer_size: usize,
    /// Maximum WAL size before rotation (default: 64MB)
    pub max_size: u64,
    /// Whether to sync after every write (default: true)
    pub sync_on_write: bool,
    /// Enable background WAL writer (non-blocking append)
    #[serde(default)]
    pub async_write: bool,
    /// Group-commit: sync after this many bytes (None = disabled)
    #[serde(default)]
    pub sync_every_bytes: Option<u64>,
    /// Group-commit: sync at this interval in milliseconds (None = disabled)
    #[serde(default)]
    pub sync_interval_ms: Option<u64>,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            buffer_size: 64 * 1024,     // 64KB
            max_size: 64 * 1024 * 1024, // 64MB
            sync_on_write: true,        // Prioritize durability
            async_write: false,
            sync_every_bytes: None,
            sync_interval_ms: None,
        }
    }
}

/// Configuration for the storage engine
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory path for storing database files
    pub data_dir: String,
    /// Buffer pool size in number of pages
    pub buffer_pool_size: usize,
    /// Use LSM-tree storage engine instead of B-tree
    pub use_lsm: bool,
    /// Page size in bytes (for B-tree engine)
    pub page_size: usize,
    /// LSM-tree specific configuration
    pub lsm: LsmConfig,
    /// Write-ahead log configuration
    pub wal: WalConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".into(),
            buffer_pool_size: 1000, // 1000 pages = ~16MB (depending on page_size)
            use_lsm: true,
            page_size: 16384, // 16KB pages
            lsm: LsmConfig::default(),
            wal: WalConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
}

impl Config {
    /// Load config from a TOML file. If the file does not exist, returns Default.
    /// After loading from file, environment variables (MONODB_*) are applied to override values.
    pub fn load_from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = match fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No config file: use defaults
                let mut cfg = Config::default();
                cfg.apply_env_overrides();
                return Ok(cfg);
            }
            Err(e) => return Err(e.into()),
        };

        let mut cfg: Config = toml::from_str(&content)?;
        cfg.apply_env_overrides();
        Ok(cfg)
    }

    /// Apply a small set of environment variable overrides.
    /// Supported variables:
    /// MONODB_HOST, MONODB_PORT, MONODB_MAX_CONNECTIONS,
    /// MONODB_DATA_DIR, MONODB_BUFFER_POOL_SIZE, MONODB_USE_LSM, MONODB_PAGE_SIZE,
    /// MONODB_MEMTABLE_SIZE
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = env::var("MONODB_HOST") {
            self.server.host = v;
        }
        if let Ok(v) = env::var("MONODB_PORT")
            && let Ok(p) = v.parse::<u16>()
        {
            self.server.port = p;
        }
        // if let Ok(v) = env::var("MONODB_MAX_CONNECTIONS") {
        //     if let Ok(n) = v.parse::<usize>() {
        //         self.server.max_connections = n;
        //     }
        // }
        if let Ok(v) = env::var("MONODB_DATA_DIR") {
            self.storage.data_dir = v;
        }
        if let Ok(v) = env::var("MONODB_BUFFER_POOL_SIZE")
            && let Ok(n) = v.parse::<usize>()
        {
            self.storage.buffer_pool_size = n;
        }
        if let Ok(v) = env::var("MONODB_USE_LSM")
            && let Ok(b) = v.parse::<bool>()
        {
            self.storage.use_lsm = b;
        }
        if let Ok(v) = env::var("MONODB_PAGE_SIZE")
            && let Ok(n) = v.parse::<usize>()
        {
            self.storage.page_size = n;
        }
        if let Ok(v) = env::var("MONODB_MEMTABLE_SIZE")
            && let Ok(n) = v.parse::<usize>()
        {
            self.storage.lsm.memtable_size = n;
        }
        // Additional overrides will be added
    }
}

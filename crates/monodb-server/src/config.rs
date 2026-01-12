use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub tls: Option<TlsConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6432,
            tls: None,
        }
    }
}

/// Write-Ahead Log configuration settings.
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

/// Storage engine configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory path for storing database files
    pub data_dir: String,
    /// Buffer pool size in number of pages
    pub buffer_pool_size: usize,
    /// Page size in bytes (for B-tree engine)
    pub page_size: usize,
    /// Write-ahead log configuration
    pub wal: WalConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".into(),
            buffer_pool_size: 1000, // 1000 pages = ~16MB (depending on page_size)
            page_size: 16384,       // 16KB pages
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
    /// Load config from TOML file, with environment variable overrides.
    /// Falls back to defaults if file is not found. MDB_CONFIG env var overrides the path.
    pub fn load_from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        ConfigLoader::new().load(path)
    }
}

/// Resolves configuration from file, CLI args, and environment variables.
struct ConfigLoader {
    args: Vec<String>,
}

impl ConfigLoader {
    fn new() -> Self {
        Self {
            args: env::args().collect(),
        }
    }

    fn load<P: AsRef<Path>>(&self, default_path: P) -> anyhow::Result<Config> {
        let mut cfg_path = self.resolve_config_path(default_path);

        // Allow MDB_CONFIG to fully override any arg/default
        if let Ok(env_path) = env::var("MDB_CONFIG").or_else(|_| env::var("MONODB_CONFIG")) {
            cfg_path = PathBuf::from(env_path);
        }

        match fs::read_to_string(&cfg_path) {
            Ok(s) => {
                let mut cfg: Config = toml::from_str(&s)?;
                Self::apply_env_overrides(&mut cfg);
                Ok(cfg)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut cfg = Config::default();
                Self::apply_env_overrides(&mut cfg);
                Ok(cfg)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Resolve config path from CLI args, env vars, or default.
    fn resolve_config_path<P: AsRef<Path>>(&self, default_path: P) -> PathBuf {
        if let Some(p) = Self::find_config_arg(&self.args) {
            p
        } else {
            default_path.as_ref().to_path_buf()
        }
    }

    /// Find --config or -c flag in arguments.
    fn find_config_arg(args: &[String]) -> Option<PathBuf> {
        let mut iter = args.iter().peekable();
        while let Some(a) = iter.next() {
            if a.starts_with("--config=") || a.starts_with("-c=") {
                if let Some((_, val)) = a.split_once('=') {
                    return Some(PathBuf::from(val));
                }
            } else if (a == "--config" || a == "-c")
                && let Some(next) = iter.peek()
            {
                return Some(PathBuf::from((*next).clone()));
            }
        }
        None
    }

    /// Apply MONODB_*/MDB_* environment variable overrides.
    fn apply_env_overrides(cfg: &mut Config) {
        if let Ok(v) = env::var("MONODB_HOST").or_else(|_| env::var("MDB_HOST")) {
            cfg.server.host = v;
        }

        if let Ok(v) = env::var("MONODB_PORT").or_else(|_| env::var("MDB_PORT"))
            && let Ok(p) = v.parse::<u16>()
        {
            cfg.server.port = p;
        }

        if let Ok(v) = env::var("MONODB_DATA_DIR").or_else(|_| env::var("MDB_DATA_DIR")) {
            cfg.storage.data_dir = v;
        }
    }
}

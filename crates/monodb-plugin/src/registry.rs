//! Plugin registry

use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Metadata about a registered plugin
#[derive(Debug, Clone)]
pub struct PluginInfo {
    /// Unique identifier (the plugin's self-reported name)
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Version string
    pub version: String,
    /// Whether this plugin is long-running
    pub long_running: bool,
    /// Path to the WASM file
    pub path: PathBuf,
    /// Size of the WASM file in bytes
    pub size_bytes: u64,
}

/// Registry of available plugins
pub struct PluginRegistry {
    plugins: RwLock<BTreeMap<String, PluginInfo>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(BTreeMap::new()),
        }
    }

    /// Register a plugin
    pub fn register(&self, info: PluginInfo) {
        self.plugins.write().insert(info.name.clone(), info);
    }

    /// Get a plugin by name
    pub fn get(&self, name: &str) -> Option<PluginInfo> {
        self.plugins.read().get(name).cloned()
    }

    /// List all registered plugins
    pub fn list(&self) -> Vec<PluginInfo> {
        self.plugins.read().values().cloned().collect()
    }

    /// Check if a plugin exists
    pub fn exists(&self, name: &str) -> bool {
        self.plugins.read().contains_key(name)
    }

    /// Remove a plugin by name
    pub fn unregister(&self, name: &str) -> Option<PluginInfo> {
        self.plugins.write().remove(name)
    }

    /// Remove plugins whose paths no longer exist on disk
    /// Returns the removed plugin infos
    pub fn prune_missing(&self) -> Vec<PluginInfo> {
        let mut plugins = self.plugins.write();
        let to_remove: Vec<String> = plugins
            .iter()
            .filter(|(_, info)| !info.path.exists())
            .map(|(name, _)| name.clone())
            .collect();

        let mut removed = Vec::new();
        for name in to_remove {
            if let Some(info) = plugins.remove(&name) {
                removed.push(info);
            }
        }

        removed
    }

    /// Get plugin count
    pub fn count(&self) -> usize {
        self.plugins.read().len()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

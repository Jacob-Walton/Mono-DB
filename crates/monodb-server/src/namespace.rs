#![allow(dead_code)]

//! Namespace management for MonoDB.
//!
//! Namespaces provide logical isolation between different databases/tenants.
//! Each namespace has its own set of tables, indexes, and configurations.
//! Each namespace gets its own subdirectory under the data directory.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use monodb_common::{MonoError, Result};

/// Namespace metadata
#[derive(Debug, Clone)]
pub struct Namespace {
    /// Unique name of the namespace
    pub name: String,
    /// Optional description
    pub description: Option<String>,
    /// When the namespace was created (Unix timestamp)
    pub created_at: i64,
    /// Storage quota in bytes (None = unlimited)
    pub quota_bytes: Option<u64>,
    /// Current storage usage in bytes
    pub usage_bytes: u64,
    /// Whether the namespace is active
    pub active: bool,
}

impl Namespace {
    /// Create a new namespace.
    pub fn new(name: impl Into<String>) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        Self {
            name: name.into(),
            description: None,
            created_at,
            quota_bytes: None,
            usage_bytes: 0,
            active: true,
        }
    }

    /// Create a namespace with a description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set a storage quota.
    pub fn with_quota(mut self, bytes: u64) -> Self {
        self.quota_bytes = Some(bytes);
        self
    }

    /// Check if adding bytes would exceed the quota.
    pub fn would_exceed_quota(&self, bytes: u64) -> bool {
        if let Some(quota) = self.quota_bytes {
            self.usage_bytes + bytes > quota
        } else {
            false
        }
    }
}

/// Binary format constants for persistence
const NAMESPACE_MAGIC: u32 = u32::from_be_bytes(*b"NMSP");
const NAMESPACE_VERSION: u16 = 1;

/// Manages namespaces in the system.
pub struct NamespaceManager {
    /// Namespace metadata by name
    namespaces: RwLock<HashMap<String, Namespace>>,
    /// Base data directory
    data_dir: PathBuf,
    /// Default namespace name
    default_namespace: String,
}

impl NamespaceManager {
    /// Create a new namespace manager.
    /// Loads existing namespaces from disk or creates default ones.
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Ensure data directory exists
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| MonoError::Io(format!("Failed to create data directory: {}", e)))?;

        let manager = Self {
            namespaces: RwLock::new(HashMap::new()),
            data_dir,
            default_namespace: "default".to_string(),
        };

        // Try to load existing namespaces
        if manager.namespace_file().exists() {
            manager.load()?;
        } else {
            // Initialize with default namespaces
            manager.init_defaults()?;
        }

        Ok(manager)
    }

    /// Initialize default namespaces
    fn init_defaults(&self) -> Result<()> {
        let mut namespaces = self.namespaces.write();

        // Always create the default namespace
        let default =
            Namespace::new("default").with_description("Default namespace for all operations");
        namespaces.insert("default".to_string(), default);

        // System namespace for internal tables
        let system =
            Namespace::new("system").with_description("System namespace for internal metadata");
        namespaces.insert("system".to_string(), system);

        drop(namespaces);

        // Create directories for default namespaces
        self.ensure_namespace_dir("default")?;
        self.ensure_namespace_dir("system")?;

        // Persist
        self.persist()?;

        Ok(())
    }

    /// Get the namespace metadata file path
    fn namespace_file(&self) -> PathBuf {
        self.data_dir.join("namespaces.bin")
    }

    /// Get the directory for a namespace
    pub fn namespace_dir(&self, namespace: &str) -> PathBuf {
        self.data_dir.join(namespace)
    }

    /// Ensure namespace directory exists
    fn ensure_namespace_dir(&self, namespace: &str) -> Result<()> {
        let dir = self.namespace_dir(namespace);
        std::fs::create_dir_all(&dir)
            .map_err(|e| MonoError::Io(format!("Failed to create namespace directory: {}", e)))?;
        Ok(())
    }

    /// Get a table file path within a namespace
    pub fn table_path(&self, namespace: &str, table: &str, extension: &str) -> PathBuf {
        self.namespace_dir(namespace)
            .join(format!("{}.{}", table, extension))
    }

    /// Get the default namespace name
    pub fn default_namespace(&self) -> &str {
        &self.default_namespace
    }

    /// Create a new namespace
    pub fn create(&self, name: &str, description: Option<&str>) -> Result<()> {
        // Validate namespace name
        if !Self::is_valid_name(name) {
            return Err(MonoError::InvalidOperation(format!(
                "invalid namespace name '{}': must be alphanumeric with underscores, 1-64 chars",
                name
            )));
        }

        let mut namespaces = self.namespaces.write();

        if namespaces.contains_key(name) {
            return Err(MonoError::AlreadyExists(format!(
                "namespace '{}' already exists",
                name
            )));
        }

        let mut ns = Namespace::new(name);
        if let Some(desc) = description {
            ns = ns.with_description(desc);
        }

        namespaces.insert(name.to_string(), ns);
        drop(namespaces);

        // Create namespace directory
        self.ensure_namespace_dir(name)?;

        // Persist changes
        self.persist()?;

        Ok(())
    }

    /// Drop a namespace
    /// If force is true, removes the directory even if it contains files.
    pub fn drop(&self, name: &str, force: bool) -> Result<()> {
        // Cannot drop system namespaces
        if name == "default" || name == "system" {
            return Err(MonoError::InvalidOperation(format!(
                "cannot drop system namespace '{}'",
                name
            )));
        }

        let mut namespaces = self.namespaces.write();

        if !namespaces.contains_key(name) {
            return Err(MonoError::NotFound(format!(
                "namespace '{}' not found",
                name
            )));
        }

        // Check if namespace directory has files (unless force is true)
        let ns_dir = self.namespace_dir(name);
        if ns_dir.exists() && !force {
            let has_files = std::fs::read_dir(&ns_dir)
                .map(|mut entries| entries.next().is_some())
                .unwrap_or(false);
            if has_files {
                return Err(MonoError::InvalidOperation(format!(
                    "namespace '{}' is not empty, use FORCE to drop anyway",
                    name
                )));
            }
        }

        namespaces.remove(name);
        drop(namespaces);

        // Remove namespace directory
        if ns_dir.exists() {
            if force {
                std::fs::remove_dir_all(&ns_dir).map_err(|e| {
                    MonoError::Io(format!("Failed to remove namespace directory: {}", e))
                })?;
            } else {
                std::fs::remove_dir(&ns_dir).map_err(|e| {
                    MonoError::Io(format!("Failed to remove namespace directory: {}", e))
                })?;
            }
        }

        // Persist changes
        self.persist()?;

        Ok(())
    }

    /// Get namespace by name
    pub fn get(&self, name: &str) -> Option<Namespace> {
        self.namespaces.read().get(name).cloned()
    }

    /// Check if a namespace exists
    pub fn exists(&self, name: &str) -> bool {
        self.namespaces.read().contains_key(name)
    }

    /// List all namespaces
    pub fn list(&self) -> Vec<Namespace> {
        self.namespaces.read().values().cloned().collect()
    }

    /// List namespace names
    pub fn names(&self) -> Vec<String> {
        self.namespaces.read().keys().cloned().collect()
    }

    /// Update namespace usage statistics
    pub fn update_usage(&self, name: &str, bytes: u64) {
        if let Some(ns) = self.namespaces.write().get_mut(name) {
            ns.usage_bytes = bytes;
        }
    }

    /// Add to namespace usage
    pub fn add_usage(&self, name: &str, bytes: u64) -> Result<()> {
        let mut namespaces = self.namespaces.write();

        if let Some(ns) = namespaces.get_mut(name) {
            if ns.would_exceed_quota(bytes) {
                return Err(MonoError::QuotaExceeded {
                    namespace: name.to_string(),
                    limit: ns.quota_bytes.unwrap_or(0),
                    used: ns.usage_bytes,
                    requested: bytes,
                });
            }
            ns.usage_bytes += bytes;
            Ok(())
        } else {
            Err(MonoError::NotFound(format!(
                "namespace '{}' not found",
                name
            )))
        }
    }

    /// Validate a namespace name
    pub fn is_valid_name(name: &str) -> bool {
        if name.is_empty() || name.len() > 64 {
            return false;
        }

        let mut chars = name.chars();

        // First char must be letter or underscore
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }

        // Rest must be alphanumeric or underscore
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// Qualify a table name with namespace if not already qualified
    pub fn qualify_table(table: &str, current_namespace: &str) -> String {
        if table.contains('.') {
            // Already qualified
            table.to_string()
        } else {
            format!("{}.{}", current_namespace, table)
        }
    }

    /// Parse a qualified table name into (namespace, table)
    pub fn parse_qualified(name: &str) -> (&str, &str) {
        if let Some(idx) = name.find('.') {
            (&name[..idx], &name[idx + 1..])
        } else {
            ("default", name)
        }
    }

    /// Persist namespaces to disk
    fn persist(&self) -> Result<()> {
        let namespaces = self.namespaces.read();
        let mut buf = Vec::new();

        // Header: magic (4) + version (2) + count (4)
        buf.extend_from_slice(&NAMESPACE_MAGIC.to_le_bytes());
        buf.extend_from_slice(&NAMESPACE_VERSION.to_le_bytes());
        buf.extend_from_slice(&(namespaces.len() as u32).to_le_bytes());

        // Entries
        for ns in namespaces.values() {
            // Name length (2) + name
            let name_bytes = ns.name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(name_bytes);

            // Description: present (1) + length (2) + data
            if let Some(desc) = &ns.description {
                buf.push(1);
                let desc_bytes = desc.as_bytes();
                buf.extend_from_slice(&(desc_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(desc_bytes);
            } else {
                buf.push(0);
            }

            // created_at (8)
            buf.extend_from_slice(&ns.created_at.to_le_bytes());

            // quota_bytes: present (1) + value (8)
            if let Some(quota) = ns.quota_bytes {
                buf.push(1);
                buf.extend_from_slice(&quota.to_le_bytes());
            } else {
                buf.push(0);
            }

            // usage_bytes (8)
            buf.extend_from_slice(&ns.usage_bytes.to_le_bytes());

            // active (1)
            buf.push(if ns.active { 1 } else { 0 });
        }

        // Write to file atomically
        let path = self.namespace_file();
        let temp_path = path.with_extension("bin.tmp");
        std::fs::write(&temp_path, &buf)
            .map_err(|e| MonoError::Io(format!("Failed to write namespace file: {}", e)))?;
        std::fs::rename(&temp_path, &path)
            .map_err(|e| MonoError::Io(format!("Failed to rename namespace file: {}", e)))?;

        Ok(())
    }

    /// Load namespaces from disk
    fn load(&self) -> Result<()> {
        let path = self.namespace_file();
        let data = std::fs::read(&path)
            .map_err(|e| MonoError::Io(format!("Failed to read namespace file: {}", e)))?;

        let mut offset = 0;

        // Header
        if data.len() < 10 {
            return Err(MonoError::Corruption {
                location: "namespaces.bin".into(),
                details: "File too short".into(),
            });
        }

        let magic = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if magic != NAMESPACE_MAGIC {
            return Err(MonoError::Corruption {
                location: "namespaces.bin".into(),
                details: "Invalid magic number".into(),
            });
        }

        let version = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if version != NAMESPACE_VERSION {
            return Err(MonoError::Corruption {
                location: "namespaces.bin".into(),
                details: format!("Unsupported version: {}", version),
            });
        }

        let count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut namespaces = HashMap::new();

        for _ in 0..count {
            // Name
            let name_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
            offset += name_len;

            // Description
            let has_desc = data[offset] != 0;
            offset += 1;
            let description = if has_desc {
                let desc_len =
                    u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;
                let desc = String::from_utf8_lossy(&data[offset..offset + desc_len]).to_string();
                offset += desc_len;
                Some(desc)
            } else {
                None
            };

            // created_at
            let created_at = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            // quota_bytes
            let has_quota = data[offset] != 0;
            offset += 1;
            let quota_bytes = if has_quota {
                let quota = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Some(quota)
            } else {
                None
            };

            // usage_bytes
            let usage_bytes = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            // active
            let active = data[offset] != 0;
            offset += 1;

            let ns = Namespace {
                name: name.clone(),
                description,
                created_at,
                quota_bytes,
                usage_bytes,
                active,
            };
            namespaces.insert(name, ns);
        }

        *self.namespaces.write() = namespaces;

        // Ensure directories exist for all namespaces
        for name in self.names() {
            self.ensure_namespace_dir(&name)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_valid_namespace_names() {
        assert!(NamespaceManager::is_valid_name("default"));
        assert!(NamespaceManager::is_valid_name("my_namespace"));
        assert!(NamespaceManager::is_valid_name("_private"));
        assert!(NamespaceManager::is_valid_name("test123"));
        assert!(NamespaceManager::is_valid_name("A"));

        assert!(!NamespaceManager::is_valid_name("")); // empty
        assert!(!NamespaceManager::is_valid_name("123abc")); // starts with number
        assert!(!NamespaceManager::is_valid_name("my-namespace")); // hyphen
        assert!(!NamespaceManager::is_valid_name("my namespace")); // space
        assert!(!NamespaceManager::is_valid_name(&"a".repeat(65))); // too long
    }

    #[test]
    fn test_parse_qualified() {
        assert_eq!(
            NamespaceManager::parse_qualified("default.users"),
            ("default", "users")
        );
        assert_eq!(
            NamespaceManager::parse_qualified("system.meta"),
            ("system", "meta")
        );
        assert_eq!(
            NamespaceManager::parse_qualified("users"),
            ("default", "users")
        );
    }

    #[test]
    fn test_namespace_manager_basics() {
        let dir = tempdir().unwrap();
        let manager = NamespaceManager::new(dir.path()).unwrap();

        // Default namespaces should exist
        assert!(manager.exists("default"));
        assert!(manager.exists("system"));

        // Directories should be created
        assert!(dir.path().join("default").exists());
        assert!(dir.path().join("system").exists());
    }

    #[test]
    fn test_create_namespace() {
        let dir = tempdir().unwrap();
        let manager = NamespaceManager::new(dir.path()).unwrap();

        // Create a new namespace
        manager.create("myapp", Some("My application")).unwrap();
        assert!(manager.exists("myapp"));
        assert!(dir.path().join("myapp").exists());

        // Check metadata
        let ns = manager.get("myapp").unwrap();
        assert_eq!(ns.name, "myapp");
        assert_eq!(ns.description, Some("My application".to_string()));
    }

    #[test]
    fn test_drop_namespace() {
        let dir = tempdir().unwrap();
        let manager = NamespaceManager::new(dir.path()).unwrap();

        manager.create("temp", None).unwrap();
        assert!(manager.exists("temp"));

        manager.drop("temp", false).unwrap();
        assert!(!manager.exists("temp"));
        assert!(!dir.path().join("temp").exists());
    }

    #[test]
    fn test_cannot_drop_system_namespaces() {
        let dir = tempdir().unwrap();
        let manager = NamespaceManager::new(dir.path()).unwrap();

        assert!(manager.drop("default", false).is_err());
        assert!(manager.drop("system", false).is_err());
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();

        // Create and add namespace
        {
            let manager = NamespaceManager::new(dir.path()).unwrap();
            manager
                .create("persistent", Some("Test persistence"))
                .unwrap();
        }

        // Reload and verify
        {
            let manager = NamespaceManager::new(dir.path()).unwrap();
            assert!(manager.exists("persistent"));
            let ns = manager.get("persistent").unwrap();
            assert_eq!(ns.description, Some("Test persistence".to_string()));
        }
    }

    #[test]
    fn test_table_path() {
        let dir = tempdir().unwrap();
        let manager = NamespaceManager::new(dir.path()).unwrap();

        let path = manager.table_path("default", "users", "rel");
        assert_eq!(path, dir.path().join("default").join("users.rel"));

        let path = manager.table_path("myapp", "orders", "doc");
        assert_eq!(path, dir.path().join("myapp").join("orders.doc"));
    }
}

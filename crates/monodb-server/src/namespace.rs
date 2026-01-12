#![allow(dead_code)]

//! Namespace management for MonoDB.
//!
//! Namespaces provide logical isolation between different databases/tenants.
//! Each namespace has its own set of tables, indexes, and configurations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::storage::engine::StorageEngine;
use monodb_common::{MonoError, Result};

/// Namespace metadata
#[derive(Debug, Clone)]
pub struct Namespace {
    /// Unique name of the namespace
    pub name: String,
    /// Optional description
    pub description: Option<String>,
    /// When the namespace was created
    pub created_at: Instant,
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
        Self {
            name: name.into(),
            description: None,
            created_at: Instant::now(),
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

/// Manages namespaces in the system.
pub struct NamespaceManager {
    /// Namespace metadata by name
    namespaces: RwLock<HashMap<String, Namespace>>,
    /// Storage engine reference
    storage: Arc<StorageEngine>,
    /// Default namespace
    default_namespace: String,
}

impl NamespaceManager {
    /// Create a namespace manager.
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        let mut namespaces = HashMap::new();

        // Always create the default namespace
        let default =
            Namespace::new("default").with_description("Default namespace for all operations");
        namespaces.insert("default".to_string(), default);

        // System namespace for internal tables
        let system =
            Namespace::new("system").with_description("System namespace for internal metadata");
        namespaces.insert("system".to_string(), system);

        Self {
            namespaces: RwLock::new(namespaces),
            storage,
            default_namespace: "default".to_string(),
        }
    }

    /// Get the default namespace name
    pub fn default_namespace(&self) -> &str {
        &self.default_namespace
    }

    /// Create a new namespace
    pub fn create(&self, name: &str, description: Option<&str>) -> Result<()> {
        let mut namespaces = self.namespaces.write();

        if namespaces.contains_key(name) {
            return Err(MonoError::AlreadyExists(format!(
                "namespace '{}' already exists",
                name
            )));
        }

        // Validate namespace name
        if !Self::is_valid_name(name) {
            return Err(MonoError::InvalidOperation(format!(
                "invalid namespace name '{}': must be alphanumeric with underscores, 1-64 chars",
                name
            )));
        }

        let mut ns = Namespace::new(name);
        if let Some(desc) = description {
            ns = ns.with_description(desc);
        }

        namespaces.insert(name.to_string(), ns);

        // TODO: Persist namespace to storage

        Ok(())
    }

    /// Drop a namespace (must be empty)
    pub fn drop(&self, name: &str, force: bool) -> Result<()> {
        // Cannot drop system namespaces
        if name == "default" || name == "system" {
            return Err(MonoError::InvalidOperation(format!(
                "cannot drop system namespace '{}'",
                name
            )));
        }

        let mut namespaces = self.namespaces.write();

        let _ns = namespaces
            .get(name)
            .ok_or_else(|| MonoError::NotFound(format!("namespace '{}' not found", name)))?;

        // Check if namespace has tables (unless force is true)
        if !force {
            let tables = self.storage.list_tables_in_namespace(name);
            if !tables.is_empty() {
                return Err(MonoError::InvalidOperation(format!(
                    "namespace '{}' is not empty, use force=true to drop anyway",
                    name
                )));
            }
        }

        namespaces.remove(name);

        // TODO: Clean up storage for this namespace

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
    fn is_valid_name(name: &str) -> bool {
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
    pub fn qualify_table(&self, table: &str, current_namespace: &str) -> String {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_manager() -> NamespaceManager {
        // Create a minimal storage engine for testing
        // For now, we'll use a simple stub
        todo!("Need test storage engine")
    }

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
}

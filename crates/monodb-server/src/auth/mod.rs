//! Authentication and authorization module for MonoDB.

#![allow(dead_code)]

use std::sync::Arc;

use monodb_common::{
    MonoError, Result,
    permissions::{BuiltinRole, PermissionSet},
};

pub mod bootstrap;
pub mod token;
pub mod user;

pub use bootstrap::bootstrap;
pub use token::{DEFAULT_TOKEN_EXPIRY_SECS, SessionToken, TokenStore};
pub use user::{User, UserStore, generate_dummy_hash, verify_password};

use crate::query_engine::storage::StorageAdapter;

/// Result of a successful authentication.
#[derive(Debug, Clone)]
pub struct AuthResult {
    /// The authenticated user
    pub user: User,
    /// Session token for resumption (if password auth)
    pub token: Option<SessionToken>,
    /// Effective permissions for this session
    pub permissions: PermissionSet,
}

/// Authentication manager for the database server.
pub struct AuthManager {
    user_store: UserStore,
    token_store: TokenStore,
    /// Whether authentication is required
    auth_enabled: bool,
    /// Token expiry time in seconds
    token_expiry_secs: u64,
    /// Pre-computed dummy hash for timing attack protection
    dummy_hash: String,
}

impl AuthManager {
    /// Create a new AuthManager.
    pub fn new(storage: Arc<StorageAdapter>, auth_enabled: bool, token_expiry_secs: u64) -> Self {
        Self {
            user_store: UserStore::new(Arc::clone(&storage)),
            token_store: TokenStore::new(storage),
            auth_enabled,
            token_expiry_secs,
            dummy_hash: generate_dummy_hash(),
        }
    }

    /// Check if authentication is required for this server.
    pub fn is_auth_required(&self) -> bool {
        self.auth_enabled
    }

    /// Authenticate with username and password.
    ///
    /// On success, returns the user and a session token for future reconnection.
    /// Uses constant-time comparison to prevent timing attacks.
    pub fn authenticate_password(&self, username: &str, password: &str) -> Result<AuthResult> {
        // Look up user (or use dummy hash if not found)
        let (hash, user) = match self.user_store.get_by_username(username)? {
            Some(u) => (u.password_hash.clone(), Some(u)),
            None => (self.dummy_hash.clone(), None),
        };

        // Always verify to prevent timing attacks
        let valid = verify_password(password, &hash);

        match (user, valid) {
            (Some(u), true) if !u.disabled => {
                // Generate session token
                let token = SessionToken::new(&u.id, self.token_expiry_secs);
                self.token_store.create(&token)?;

                // Build permissions from roles
                let permissions = self.build_permissions(&u.roles);

                Ok(AuthResult {
                    user: u,
                    token: Some(token),
                    permissions,
                })
            }
            _ => Err(MonoError::Network("Invalid username or password".into())),
        }
    }

    /// Authenticate with a session token.
    ///
    /// Validates the token and returns the associated user if valid.
    pub fn authenticate_token(&self, token_str: &str) -> Result<AuthResult> {
        // Look up token
        let token = self
            .token_store
            .get(token_str)?
            .ok_or_else(|| MonoError::Network("Invalid or expired token".into()))?;

        // Check expiry
        if token.is_expired() {
            // Clean up expired token
            let _ = self.token_store.delete(token_str);
            return Err(MonoError::Network("Token has expired".into()));
        }

        // Look up associated user
        let user = self
            .user_store
            .get_by_id(&token.user_id)?
            .ok_or_else(|| MonoError::Network("User not found".into()))?;

        // Check if user is disabled
        if user.disabled {
            return Err(MonoError::Network("User account is disabled".into()));
        }

        // Update last_used timestamp
        self.token_store.touch(token_str)?;

        // Build permissions from roles
        let permissions = self.build_permissions(&user.roles);

        Ok(AuthResult {
            user,
            token: None, // Don't issue new token for token auth
            permissions,
        })
    }

    /// Invalidate a session token (logout).
    pub fn invalidate_token(&self, token: &str) -> Result<()> {
        self.token_store.delete(token)?;
        Ok(())
    }

    /// Invalidate all tokens for a user.
    pub fn invalidate_user_tokens(&self, user_id: &str) -> Result<u64> {
        self.token_store.delete_user_tokens(user_id)
    }

    /// Clean up expired tokens.
    pub fn cleanup_expired_tokens(&self) -> Result<u64> {
        self.token_store.cleanup_expired()
    }

    /// Get the default permissions when auth is disabled.
    pub fn default_permissions(&self) -> PermissionSet {
        // When auth is disabled, grant root permissions
        BuiltinRole::Root.permissions(None)
    }

    /// Get access to the user store.
    pub fn user_store(&self) -> &UserStore {
        &self.user_store
    }

    /// Get access to the token store.
    pub fn token_store(&self) -> &TokenStore {
        &self.token_store
    }

    /// Build a PermissionSet from a list of role names.
    fn build_permissions(&self, roles: &[String]) -> PermissionSet {
        let mut permissions = PermissionSet::new();

        for role_name in roles {
            // Try to parse as builtin role
            if let Ok(builtin) = role_name.parse::<BuiltinRole>() {
                permissions.merge(&builtin.permissions(None));
            }
            // TODO: Load custom roles from system.roles table
        }

        permissions
    }
}

/// Configuration for the authentication system.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Whether authentication is enabled
    pub enabled: bool,
    /// Token expiry time in seconds
    pub token_expiry_secs: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default
            token_expiry_secs: DEFAULT_TOKEN_EXPIRY_SECS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::{StorageConfig, StorageEngine};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_storage() -> (TempDir, Arc<StorageAdapter>) {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            buffer_pool_size: 64,
            wal_enabled: false,
            ..Default::default()
        };
        let engine = Arc::new(StorageEngine::new(config).unwrap());
        let storage = Arc::new(StorageAdapter::new(engine));
        (dir, storage)
    }

    #[test]
    fn test_auth_config_defaults() {
        let config = AuthConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.token_expiry_secs, 86400);
    }

    #[test]
    fn test_bootstrap_creates_system_tables() {
        let (_dir, storage) = create_test_storage();

        // Bootstrap should succeed
        let result = bootstrap(&storage, true);
        assert!(result.is_ok());

        let bootstrap_result = result.unwrap();
        assert!(bootstrap_result.fresh_install);
        assert!(bootstrap_result.root_password.is_some());

        // System tables should exist
        use crate::query_engine::QueryStorage;
        assert!(storage.table_exists("system.users"));
        assert!(storage.table_exists("system.tokens"));
        assert!(storage.table_exists("system.roles"));
    }

    #[test]
    fn test_bootstrap_idempotent() {
        let (_dir, storage) = create_test_storage();

        // First bootstrap
        let result1 = bootstrap(&storage, true).unwrap();
        assert!(result1.fresh_install);

        // Second bootstrap should not create new root password
        let result2 = bootstrap(&storage, true).unwrap();
        assert!(!result2.fresh_install);
        assert!(result2.root_password.is_none());
    }

    #[test]
    fn test_auth_manager_password_auth() {
        let (_dir, storage) = create_test_storage();

        // Bootstrap to create root user
        let bootstrap_result = bootstrap(&storage, true).unwrap();
        let root_password = bootstrap_result.root_password.unwrap();

        // Create AuthManager
        let auth_manager = AuthManager::new(Arc::clone(&storage), true, 86400);

        // Authenticate with correct password
        let result = auth_manager.authenticate_password("root", &root_password);
        assert!(result.is_ok());

        let auth_result = result.unwrap();
        assert_eq!(auth_result.user.username, "root");
        assert!(auth_result.token.is_some());
        // Root user should have permissions
        assert!(!auth_result.permissions.permissions().is_empty());
    }

    #[test]
    fn test_auth_manager_invalid_password() {
        let (_dir, storage) = create_test_storage();

        // Bootstrap
        bootstrap(&storage, true).unwrap();

        // Create AuthManager
        let auth_manager = AuthManager::new(Arc::clone(&storage), true, 86400);

        // Authenticate with wrong password
        let result = auth_manager.authenticate_password("root", "wrong_password");
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_manager_token_auth() {
        let (_dir, storage) = create_test_storage();

        // Bootstrap
        let bootstrap_result = bootstrap(&storage, true).unwrap();
        let root_password = bootstrap_result.root_password.unwrap();

        // Create AuthManager
        let auth_manager = AuthManager::new(Arc::clone(&storage), true, 86400);

        // First authenticate with password to get a token
        let password_result = auth_manager
            .authenticate_password("root", &root_password)
            .unwrap();
        let token = password_result.token.unwrap();

        // Now authenticate with the token
        let token_result = auth_manager.authenticate_token(&token.token);
        assert!(token_result.is_ok());

        let auth_result = token_result.unwrap();
        assert_eq!(auth_result.user.username, "root");
        assert!(auth_result.token.is_none()); // No new token for token auth
    }

    #[test]
    fn test_auth_manager_invalid_token() {
        let (_dir, storage) = create_test_storage();

        // Bootstrap
        bootstrap(&storage, true).unwrap();

        // Create AuthManager
        let auth_manager = AuthManager::new(Arc::clone(&storage), true, 86400);

        // Authenticate with invalid token
        let result = auth_manager.authenticate_token("invalid-token-12345");
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_manager_auth_disabled() {
        let (_dir, storage) = create_test_storage();

        // Bootstrap
        bootstrap(&storage, true).unwrap();

        // Create AuthManager with auth disabled
        let auth_manager = AuthManager::new(Arc::clone(&storage), false, 86400);

        assert!(!auth_manager.is_auth_required());

        // Default permissions should include all permissions (root-level)
        let perms = auth_manager.default_permissions();
        assert!(!perms.permissions().is_empty());
    }

    #[test]
    fn test_user_store_crud() {
        let (_dir, storage) = create_test_storage();

        // Bootstrap to create system tables
        bootstrap(&storage, true).unwrap();

        // Create UserStore
        let user_store = UserStore::new(Arc::clone(&storage));

        // Create a new user
        let user = User::new("testuser", "password123", vec!["read".to_string()]).unwrap();
        let user_id = user.id.clone();

        // Store the user
        user_store.create(&user).unwrap();

        // Retrieve by ID
        let retrieved = user_store.get_by_id(&user_id).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().username, "testuser");

        // Retrieve by username
        let retrieved = user_store.get_by_username("testuser").unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, user_id);

        // Check has_users
        assert!(user_store.has_users().unwrap());
    }

    #[test]
    fn test_token_store_crud() {
        let (_dir, storage) = create_test_storage();

        // Bootstrap to create system tables
        bootstrap(&storage, true).unwrap();

        // Create TokenStore
        let token_store = TokenStore::new(Arc::clone(&storage));

        // Create a new token
        let token = SessionToken::new("user123", 86400);
        let token_str = token.token.clone();

        // Store the token
        token_store.create(&token).unwrap();

        // Retrieve
        let retrieved = token_store.get(&token_str).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.user_id, "user123");
        assert!(!retrieved.is_expired());

        // Delete
        let deleted = token_store.delete(&token_str).unwrap();
        assert!(deleted);

        // Should not exist anymore
        let retrieved = token_store.get(&token_str).unwrap();
        assert!(retrieved.is_none());
    }
}

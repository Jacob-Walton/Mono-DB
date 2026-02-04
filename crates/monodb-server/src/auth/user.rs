//! User management and password hashing for MonoDB authentication.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use monodb_common::{MonoError, Result, Value};

use crate::query_engine::storage::{QueryStorage, Row, ScanConfig, StorageAdapter};

/// System table name for users
pub const USERS_TABLE: &str = "system.users";

/// A user account in the authentication system.
#[derive(Debug, Clone)]
pub struct User {
    /// Unique user identifier (UUID)
    pub id: String,
    /// Username for authentication
    pub username: String,
    /// Argon2id password hash in PHC format
    pub password_hash: String,
    /// List of role names assigned to this user
    pub roles: Vec<String>,
    /// Unix timestamp when user was created
    pub created_at: i64,
    /// Unix timestamp when user was last updated
    pub updated_at: i64,
    /// Whether the user account is disabled
    pub disabled: bool,
}

impl User {
    /// Create a new user with the given username and password.
    /// Password will be hashed with Argon2id.
    pub fn new(username: impl Into<String>, password: &str, roles: Vec<String>) -> Result<Self> {
        let password_hash = hash_password(password)?;
        let now = current_timestamp();

        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            username: username.into(),
            password_hash,
            roles,
            created_at: now,
            updated_at: now,
            disabled: false,
        })
    }

    /// Verify the given password against this user's stored hash.
    pub fn verify_password(&self, password: &str) -> bool {
        verify_password(password, &self.password_hash)
    }

    /// Update the user's password.
    pub fn set_password(&mut self, password: &str) -> Result<()> {
        self.password_hash = hash_password(password)?;
        self.updated_at = current_timestamp();
        Ok(())
    }

    /// Convert user to a storage row.
    pub fn to_row(&self) -> Row {
        let mut row = Row::new();
        row.insert("id", Value::String(self.id.clone()));
        row.insert("username", Value::String(self.username.clone()));
        row.insert("password_hash", Value::String(self.password_hash.clone()));
        row.insert(
            "roles",
            Value::String(serde_json::to_string(&self.roles).unwrap_or_default()),
        );
        row.insert("created_at", Value::Int64(self.created_at));
        row.insert("updated_at", Value::Int64(self.updated_at));
        row.insert("disabled", Value::Bool(self.disabled));
        row
    }

    /// Create user from a storage row.
    pub fn from_row(row: &Row) -> Result<Self> {
        let id = match row.get("id") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(MonoError::Parse("Missing or invalid 'id' field".into())),
        };

        let username = match row.get("username") {
            Some(Value::String(s)) => s.clone(),
            _ => {
                return Err(MonoError::Parse(
                    "Missing or invalid 'username' field".into(),
                ));
            }
        };

        let password_hash = match row.get("password_hash") {
            Some(Value::String(s)) => s.clone(),
            _ => {
                return Err(MonoError::Parse(
                    "Missing or invalid 'password_hash' field".into(),
                ));
            }
        };

        let roles = match row.get("roles") {
            Some(Value::String(s)) => serde_json::from_str(s).unwrap_or_default(),
            _ => Vec::new(),
        };

        let created_at = match row.get("created_at") {
            Some(Value::Int64(n)) => *n,
            _ => 0,
        };

        let updated_at = match row.get("updated_at") {
            Some(Value::Int64(n)) => *n,
            _ => 0,
        };

        let disabled = match row.get("disabled") {
            Some(Value::Bool(b)) => *b,
            _ => false,
        };

        Ok(Self {
            id,
            username,
            password_hash,
            roles,
            created_at,
            updated_at,
            disabled,
        })
    }
}

/// Hash a password using Argon2id.
pub fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| MonoError::InvalidOperation(format!("Failed to hash password: {}", e)))
}

/// Verify a password against an Argon2id hash.
/// Returns true if the password matches, false otherwise.
/// This function performs constant-time comparison to prevent timing attacks.
pub fn verify_password(password: &str, hash: &str) -> bool {
    match PasswordHash::new(hash) {
        Ok(parsed_hash) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok(),
        Err(_) => false,
    }
}

/// Generate a pre-computed dummy hash for timing attack protection.
/// This is used when a user lookup fails to ensure consistent timing.
pub fn generate_dummy_hash() -> String {
    // Hash a random password to create a valid hash structure
    hash_password("__dummy_password_for_timing_protection__").unwrap_or_else(|_| {
        // Fallback to a static hash if generation fails
        "$argon2id$v=19$m=19456,t=2,p=1$dW5rbm93bg$X0X0X0X0X0X0X0X0X0X0X0".to_string()
    })
}

/// Get current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Store for user accounts backed by the system.users table.
pub struct UserStore {
    storage: Arc<StorageAdapter>,
}

impl UserStore {
    /// Create a new UserStore.
    pub fn new(storage: Arc<StorageAdapter>) -> Self {
        Self { storage }
    }

    /// Get a user by their unique ID.
    pub fn get_by_id(&self, user_id: &str) -> Result<Option<User>> {
        let tx_id = self.storage.begin_read_only()?;
        let result = self
            .storage
            .read(tx_id, USERS_TABLE, &Value::String(user_id.to_string()))?;
        self.storage.commit(tx_id)?;

        match result {
            Some(row) => Ok(Some(User::from_row(&row)?)),
            None => Ok(None),
        }
    }

    /// Get a user by their username.
    pub fn get_by_username(&self, username: &str) -> Result<Option<User>> {
        let tx_id = self.storage.begin_read_only()?;
        let rows = self.storage.scan(tx_id, USERS_TABLE, &ScanConfig::new())?;
        self.storage.commit(tx_id)?;

        for row in rows {
            if let Some(Value::String(stored_username)) = row.get("username")
                && stored_username == username
            {
                return Ok(Some(User::from_row(&row)?));
            }
        }

        Ok(None)
    }

    /// Create a new user in the store.
    pub fn create(&self, user: &User) -> Result<()> {
        // Check if username already exists
        if self.get_by_username(&user.username)?.is_some() {
            return Err(MonoError::AlreadyExists(format!(
                "User '{}' already exists",
                user.username
            )));
        }

        let tx_id = self.storage.begin_transaction()?;
        match self.storage.insert(tx_id, USERS_TABLE, user.to_row()) {
            Ok(()) => {
                self.storage.commit(tx_id)?;
                Ok(())
            }
            Err(e) => {
                let _ = self.storage.rollback(tx_id);
                Err(e)
            }
        }
    }

    /// Update an existing user.
    pub fn update(&self, user: &User) -> Result<()> {
        let tx_id = self.storage.begin_transaction()?;

        let updates: HashMap<String, Value> = [
            ("username".to_string(), Value::String(user.username.clone())),
            (
                "password_hash".to_string(),
                Value::String(user.password_hash.clone()),
            ),
            (
                "roles".to_string(),
                Value::String(serde_json::to_string(&user.roles).unwrap_or_default()),
            ),
            ("updated_at".to_string(), Value::Int64(user.updated_at)),
            ("disabled".to_string(), Value::Bool(user.disabled)),
        ]
        .into_iter()
        .collect();

        match self
            .storage
            .update(tx_id, USERS_TABLE, &Value::String(user.id.clone()), updates)
        {
            Ok(_) => {
                self.storage.commit(tx_id)?;
                Ok(())
            }
            Err(e) => {
                let _ = self.storage.rollback(tx_id);
                Err(e)
            }
        }
    }

    /// Delete a user by ID.
    pub fn delete(&self, user_id: &str) -> Result<bool> {
        let tx_id = self.storage.begin_transaction()?;

        match self
            .storage
            .delete(tx_id, USERS_TABLE, &Value::String(user_id.to_string()))
        {
            Ok(deleted) => {
                self.storage.commit(tx_id)?;
                Ok(deleted)
            }
            Err(e) => {
                let _ = self.storage.rollback(tx_id);
                Err(e)
            }
        }
    }

    /// Check if any users exist in the system.
    pub fn has_users(&self) -> Result<bool> {
        let tx_id = self.storage.begin_read_only()?;
        let config = ScanConfig::new().with_limit(1);
        let rows = self.storage.scan(tx_id, USERS_TABLE, &config)?;
        self.storage.commit(tx_id)?;
        Ok(!rows.is_empty())
    }

    /// List all users.
    pub fn list(&self) -> Result<Vec<User>> {
        let tx_id = self.storage.begin_read_only()?;
        let rows = self.storage.scan(tx_id, USERS_TABLE, &ScanConfig::new())?;
        self.storage.commit(tx_id)?;

        rows.iter().map(User::from_row).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hash_and_verify() {
        let password = "test_password_123";
        let hash = hash_password(password).unwrap();

        // Hash should be in PHC format
        assert!(hash.starts_with("$argon2"));

        // Verification should work
        assert!(verify_password(password, &hash));

        // Wrong password should fail
        assert!(!verify_password("wrong_password", &hash));
    }

    #[test]
    fn test_user_creation() {
        let user = User::new("testuser", "password123", vec!["read".to_string()]).unwrap();

        assert!(!user.id.is_empty());
        assert_eq!(user.username, "testuser");
        assert!(user.verify_password("password123"));
        assert!(!user.verify_password("wrong"));
        assert_eq!(user.roles, vec!["read".to_string()]);
        assert!(!user.disabled);
    }

    #[test]
    fn test_user_row_roundtrip() {
        let user = User::new("testuser", "password123", vec!["admin".to_string()]).unwrap();
        let row = user.to_row();
        let restored = User::from_row(&row).unwrap();

        assert_eq!(user.id, restored.id);
        assert_eq!(user.username, restored.username);
        assert_eq!(user.password_hash, restored.password_hash);
        assert_eq!(user.roles, restored.roles);
        assert_eq!(user.disabled, restored.disabled);
    }

    #[test]
    fn test_dummy_hash_generation() {
        let hash = generate_dummy_hash();
        assert!(hash.starts_with("$argon2"));
    }
}

//! Session token management for MonoDB authentication.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use monodb_common::{MonoError, Result, Value};

use crate::query_engine::storage::{QueryStorage, Row, ScanConfig, StorageAdapter};

/// System table name for tokens
pub const TOKENS_TABLE: &str = "system.tokens";

/// Default token expiry time in seconds (24 hours)
pub const DEFAULT_TOKEN_EXPIRY_SECS: u64 = 86400;

/// A session token for authentication resumption.
#[derive(Debug, Clone)]
pub struct SessionToken {
    /// The token string (UUID v4)
    pub token: String,
    /// User ID this token belongs to
    pub user_id: String,
    /// Unix timestamp when token was created
    pub created_at: i64,
    /// Unix timestamp when token expires
    pub expires_at: i64,
    /// Unix timestamp when token was last used
    pub last_used: i64,
}

impl SessionToken {
    /// Create a new session token for a user.
    pub fn new(user_id: impl Into<String>, expiry_secs: u64) -> Self {
        let now = current_timestamp();
        Self {
            token: uuid::Uuid::new_v4().to_string(),
            user_id: user_id.into(),
            created_at: now,
            expires_at: now + expiry_secs as i64,
            last_used: now,
        }
    }

    /// Check if the token has expired.
    pub fn is_expired(&self) -> bool {
        current_timestamp() > self.expires_at
    }

    /// Update the last_used timestamp.
    pub fn touch(&mut self) {
        self.last_used = current_timestamp();
    }

    /// Convert token to a storage row.
    pub fn to_row(&self) -> Row {
        let mut row = Row::new();
        row.insert("token", Value::String(self.token.clone()));
        row.insert("user_id", Value::String(self.user_id.clone()));
        row.insert("created_at", Value::Int64(self.created_at));
        row.insert("expires_at", Value::Int64(self.expires_at));
        row.insert("last_used", Value::Int64(self.last_used));
        row
    }

    /// Create token from a storage row.
    pub fn from_row(row: &Row) -> Result<Self> {
        let token = match row.get("token") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(MonoError::Parse("Missing or invalid 'token' field".into())),
        };

        let user_id = match row.get("user_id") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(MonoError::Parse("Missing or invalid 'user_id' field".into())),
        };

        let created_at = match row.get("created_at") {
            Some(Value::Int64(n)) => *n,
            _ => 0,
        };

        let expires_at = match row.get("expires_at") {
            Some(Value::Int64(n)) => *n,
            _ => 0,
        };

        let last_used = match row.get("last_used") {
            Some(Value::Int64(n)) => *n,
            _ => 0,
        };

        Ok(Self {
            token,
            user_id,
            created_at,
            expires_at,
            last_used,
        })
    }
}

/// Get current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Store for session tokens backed by the system.tokens table.
pub struct TokenStore {
    storage: Arc<StorageAdapter>,
}

impl TokenStore {
    /// Create a new TokenStore.
    pub fn new(storage: Arc<StorageAdapter>) -> Self {
        Self { storage }
    }

    /// Get a token by its string value.
    pub fn get(&self, token: &str) -> Result<Option<SessionToken>> {
        let tx_id = self.storage.begin_read_only()?;
        let result = self
            .storage
            .read(tx_id, TOKENS_TABLE, &Value::String(token.to_string()))?;
        self.storage.commit(tx_id)?;

        match result {
            Some(row) => Ok(Some(SessionToken::from_row(&row)?)),
            None => Ok(None),
        }
    }

    /// Create a new token in the store.
    pub fn create(&self, token: &SessionToken) -> Result<()> {
        let tx_id = self.storage.begin_transaction()?;
        match self.storage.insert(tx_id, TOKENS_TABLE, token.to_row()) {
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

    /// Update a token's last_used timestamp.
    pub fn touch(&self, token: &str) -> Result<()> {
        let tx_id = self.storage.begin_transaction()?;

        let updates: HashMap<String, Value> =
            [("last_used".to_string(), Value::Int64(current_timestamp()))]
                .into_iter()
                .collect();

        match self
            .storage
            .update(tx_id, TOKENS_TABLE, &Value::String(token.to_string()), updates)
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

    /// Delete a token.
    pub fn delete(&self, token: &str) -> Result<bool> {
        let tx_id = self.storage.begin_transaction()?;

        match self
            .storage
            .delete(tx_id, TOKENS_TABLE, &Value::String(token.to_string()))
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

    /// Delete all tokens for a user.
    pub fn delete_user_tokens(&self, user_id: &str) -> Result<u64> {
        let tx_id = self.storage.begin_transaction()?;
        let rows = match self.storage.scan(tx_id, TOKENS_TABLE, &ScanConfig::new()) {
            Ok(rows) => rows,
            Err(e) => {
                let _ = self.storage.rollback(tx_id);
                return Err(e);
            }
        };

        let mut deleted = 0u64;
        for row in rows {
            if let Some(Value::String(stored_user_id)) = row.get("user_id") {
                if stored_user_id == user_id {
                    if let Some(Value::String(token)) = row.get("token") {
                        if self
                            .storage
                            .delete(tx_id, TOKENS_TABLE, &Value::String(token.clone()))
                            .is_ok()
                        {
                            deleted += 1;
                        }
                    }
                }
            }
        }

        self.storage.commit(tx_id)?;
        Ok(deleted)
    }

    /// Clean up expired tokens.
    pub fn cleanup_expired(&self) -> Result<u64> {
        let now = current_timestamp();
        let tx_id = self.storage.begin_transaction()?;
        let rows = match self.storage.scan(tx_id, TOKENS_TABLE, &ScanConfig::new()) {
            Ok(rows) => rows,
            Err(e) => {
                let _ = self.storage.rollback(tx_id);
                return Err(e);
            }
        };

        let mut deleted = 0u64;
        for row in rows {
            let expired = match row.get("expires_at") {
                Some(Value::Int64(expires_at)) => now > *expires_at,
                _ => false,
            };

            if expired {
                if let Some(Value::String(token)) = row.get("token") {
                    if self
                        .storage
                        .delete(tx_id, TOKENS_TABLE, &Value::String(token.clone()))
                        .is_ok()
                    {
                        deleted += 1;
                    }
                }
            }
        }

        self.storage.commit(tx_id)?;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_creation() {
        let token = SessionToken::new("user123", 3600);

        assert!(!token.token.is_empty());
        assert_eq!(token.user_id, "user123");
        assert!(!token.is_expired());
        assert!(token.expires_at > token.created_at);
    }

    #[test]
    fn test_token_expiry() {
        let mut token = SessionToken::new("user123", 0);
        // Token with 0 expiry should be immediately expired
        token.expires_at = token.created_at - 1;
        assert!(token.is_expired());
    }

    #[test]
    fn test_token_row_roundtrip() {
        let token = SessionToken::new("user123", 3600);
        let row = token.to_row();
        let restored = SessionToken::from_row(&row).unwrap();

        assert_eq!(token.token, restored.token);
        assert_eq!(token.user_id, restored.user_id);
        assert_eq!(token.created_at, restored.created_at);
        assert_eq!(token.expires_at, restored.expires_at);
        assert_eq!(token.last_used, restored.last_used);
    }
}

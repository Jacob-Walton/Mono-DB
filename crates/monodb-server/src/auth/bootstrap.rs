//! First-run bootstrap for MonoDB authentication system.
//!
//! Creates system tables and the initial root user on first server start.

#![allow(dead_code)]

use std::sync::Arc;

use monodb_common::{permissions::PermissionSet, Result, Value};
use rand::Rng;

use crate::query_engine::{
    ast::{ColumnConstraint, ColumnDef, DataType, Ident, Span, Spanned, TableType},
    storage::StorageAdapter,
    QueryStorage,
};

use super::{
    token::TOKENS_TABLE,
    user::{User, UserStore, USERS_TABLE},
};

/// System table name for roles
pub const ROLES_TABLE: &str = "system.roles";

/// Bootstrap result containing any generated credentials.
#[derive(Debug)]
pub struct BootstrapResult {
    /// Whether this was a fresh bootstrap (first run)
    pub fresh_install: bool,
    /// Generated root password (only set on fresh install)
    pub root_password: Option<String>,
}

/// Bootstrap the authentication system.
///
/// This function:
/// 1. Ensures the system namespace and tables exist
/// 2. Creates the root user if auth is enabled and no users exist
/// 3. Creates builtin roles
pub fn bootstrap(storage: &Arc<StorageAdapter>, auth_enabled: bool) -> Result<BootstrapResult> {
    // Create system namespace if it doesn't exist
    ensure_system_namespace(storage)?;

    // Create system tables
    ensure_users_table(storage)?;
    ensure_tokens_table(storage)?;
    ensure_roles_table(storage)?;

    // Check if we need to create root user (only when auth is enabled)
    let user_store = UserStore::new(Arc::clone(storage));
    let has_users = user_store.has_users()?;

    let root_password = if auth_enabled && !has_users {
        // Generate a secure random password
        let password = generate_secure_password(32);

        // Create root user
        let root_user = User::new("root", &password, vec!["root".to_string()])?;
        user_store.create(&root_user)?;

        // Create builtin roles
        create_builtin_roles(storage)?;

        Some(password)
    } else if !has_users {
        // Auth disabled, no users - just create builtin roles for when auth is enabled later
        create_builtin_roles(storage)?;
        None
    } else {
        None
    };

    Ok(BootstrapResult {
        fresh_install: !has_users,
        root_password,
    })
}

/// Ensure the system namespace exists.
fn ensure_system_namespace(storage: &Arc<StorageAdapter>) -> Result<()> {
    let namespaces = storage.list_namespaces();
    if !namespaces.contains(&"system".to_string()) {
        storage.create_namespace("system")?;
        tracing::info!("Created 'system' namespace");
    }
    Ok(())
}

/// Helper to create a Spanned<Ident>.
fn spanned_ident(name: &str) -> Spanned<Ident> {
    Spanned::new(Ident::new(name), Span::DUMMY)
}

/// Ensure the system.users table exists.
fn ensure_users_table(storage: &Arc<StorageAdapter>) -> Result<()> {
    if storage.table_exists(USERS_TABLE) {
        return Ok(());
    }

    let columns = vec![
        ColumnDef {
            name: spanned_ident("id"),
            data_type: DataType::String,
            constraints: vec![ColumnConstraint::PrimaryKey],
        },
        ColumnDef {
            name: spanned_ident("username"),
            data_type: DataType::String,
            constraints: vec![ColumnConstraint::NotNull, ColumnConstraint::Unique],
        },
        ColumnDef {
            name: spanned_ident("password_hash"),
            data_type: DataType::String,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("roles"),
            data_type: DataType::String,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("created_at"),
            data_type: DataType::Int64,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("updated_at"),
            data_type: DataType::Int64,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("disabled"),
            data_type: DataType::Bool,
            constraints: vec![ColumnConstraint::NotNull],
        },
    ];

    storage.create_table_with_schema(USERS_TABLE, TableType::Relational, &columns, &[])?;
    tracing::info!("Created system.users table");
    Ok(())
}

/// Ensure the system.tokens table exists.
fn ensure_tokens_table(storage: &Arc<StorageAdapter>) -> Result<()> {
    if storage.table_exists(TOKENS_TABLE) {
        return Ok(());
    }

    let columns = vec![
        ColumnDef {
            name: spanned_ident("token"),
            data_type: DataType::String,
            constraints: vec![ColumnConstraint::PrimaryKey],
        },
        ColumnDef {
            name: spanned_ident("user_id"),
            data_type: DataType::String,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("created_at"),
            data_type: DataType::Int64,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("expires_at"),
            data_type: DataType::Int64,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("last_used"),
            data_type: DataType::Int64,
            constraints: vec![ColumnConstraint::NotNull],
        },
    ];

    storage.create_table_with_schema(TOKENS_TABLE, TableType::Relational, &columns, &[])?;
    tracing::info!("Created system.tokens table");
    Ok(())
}

/// Ensure the system.roles table exists.
fn ensure_roles_table(storage: &Arc<StorageAdapter>) -> Result<()> {
    if storage.table_exists(ROLES_TABLE) {
        return Ok(());
    }

    let columns = vec![
        ColumnDef {
            name: spanned_ident("name"),
            data_type: DataType::String,
            constraints: vec![ColumnConstraint::PrimaryKey],
        },
        ColumnDef {
            name: spanned_ident("permissions"),
            data_type: DataType::String,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("builtin"),
            data_type: DataType::Bool,
            constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDef {
            name: spanned_ident("created_at"),
            data_type: DataType::Int64,
            constraints: vec![ColumnConstraint::NotNull],
        },
    ];

    storage.create_table_with_schema(ROLES_TABLE, TableType::Relational, &columns, &[])?;
    tracing::info!("Created system.roles table");
    Ok(())
}

/// Create the builtin roles in the system.roles table.
fn create_builtin_roles(storage: &Arc<StorageAdapter>) -> Result<()> {
    use monodb_common::permissions::BuiltinRole;
    use crate::query_engine::storage::{QueryStorage, Row};

    let builtin_roles = [
        ("root", BuiltinRole::Root),
        ("clusterAdmin", BuiltinRole::ClusterAdmin),
        ("clusterMonitor", BuiltinRole::ClusterMonitor),
        ("dbOwner", BuiltinRole::DbOwner),
        ("dbAdmin", BuiltinRole::DbAdmin),
        ("userAdmin", BuiltinRole::UserAdmin),
        ("readWrite", BuiltinRole::ReadWrite),
        ("read", BuiltinRole::Read),
    ];

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let tx_id = storage.begin_transaction()?;

    for (name, builtin) in builtin_roles {
        let permissions = builtin.permissions(None);
        let permissions_json = serialize_permissions(&permissions);

        let mut row = Row::new();
        row.insert("name", Value::String(name.to_string()));
        row.insert("permissions", Value::String(permissions_json));
        row.insert("builtin", Value::Bool(true));
        row.insert("created_at", Value::Int64(now));

        if let Err(e) = storage.insert(tx_id, ROLES_TABLE, row) {
            let _ = storage.rollback(tx_id);
            return Err(e);
        }
    }

    storage.commit(tx_id)?;
    tracing::info!("Created builtin roles");
    Ok(())
}

/// Serialize a PermissionSet to JSON for storage.
fn serialize_permissions(permissions: &PermissionSet) -> String {
    permissions.to_string_array().join(",")
}

/// Generate a cryptographically secure random password.
fn generate_secure_password(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*";
    let mut rng = rand::rng();

    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Check if system tables exist (for quick auth availability check).
pub fn system_tables_exist(storage: &Arc<StorageAdapter>) -> bool {
    storage.table_exists(USERS_TABLE) && storage.table_exists(TOKENS_TABLE)
}

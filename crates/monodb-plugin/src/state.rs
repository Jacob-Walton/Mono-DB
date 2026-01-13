//! Plugin execution state

use crate::PluginStorage;
use anyhow::Result;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use wasmtime::StoreLimits;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::p2::pipe::MemoryOutputPipe;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

/// Plugin execution state held in the Wasmtime Store
pub struct State {
    // WASI resources
    pub table: ResourceTable,
    pub wasi: WasiCtx,
    pub limits: StoreLimits,
    pub stdout: MemoryOutputPipe,
    pub stderr: MemoryOutputPipe,

    // Database resources
    pub storage: Arc<dyn PluginStorage>,
    pub permissions: PluginPermissions,
    pub current_tx: RwLock<Option<u64>>,
}

impl State {
    pub fn new(
        stdout: MemoryOutputPipe,
        storage: Arc<dyn PluginStorage>,
        permissions: PluginPermissions,
    ) -> Result<Self> {
        let table = ResourceTable::new();
        let stderr = MemoryOutputPipe::new(4096);

        let wasi = wasmtime_wasi::WasiCtxBuilder::new()
            .stdout(stdout.clone())
            .stderr(stderr.clone())
            .build();

        Ok(Self {
            table,
            wasi,
            limits: StoreLimits::default(),
            stdout,
            stderr,
            storage,
            permissions,
            current_tx: RwLock::new(None),
        })
    }
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// Plugin permissions, derived from the calling user's permissions.
/// 
/// This ensures plugins cannot escalate privileges beyond what the
/// calling user is allowed to do. When a user invokes a plugin function,
/// the plugin inherits that user's access rights.
#[derive(Debug, Clone)]
pub struct PluginPermissions {
    /// Whether the plugin can only read data (derived from user lacking write perms)
    pub read_only: bool,
    /// Tables the user (and thus plugin) is allowed to access
    pub allowed_tables: Option<HashSet<String>>,
    /// Maximum rows a plugin can return from queries
    pub max_query_rows: usize,
    /// The current transaction ID to use (inherits user's transaction)
    pub inherited_tx_id: Option<u64>,
}

impl PluginPermissions {
    /// Create permissions that allow full access (for system/admin use only)
    pub fn full_access() -> Self {
        Self {
            read_only: false,
            allowed_tables: None,
            max_query_rows: 100_000,
            inherited_tx_id: None,
        }
    }

    /// Create read-only permissions
    pub fn read_only() -> Self {
        Self {
            read_only: true,
            allowed_tables: None,
            max_query_rows: 10_000,
            inherited_tx_id: None,
        }
    }

    /// Create permissions from a user context
    /// 
    /// This is the primary constructor, it derives plugin permissions
    /// from the calling user's session, ensuring the plugin can only
    /// do what the user is allowed to do.
    pub fn from_user_context(
        can_write: bool,
        allowed_tables: Option<HashSet<String>>,
        tx_id: Option<u64>,
    ) -> Self {
        Self {
            read_only: !can_write,
            allowed_tables,
            max_query_rows: 10_000,
            inherited_tx_id: tx_id,
        }
    }

    /// Set the inherited transaction ID
    pub fn with_tx(mut self, tx_id: u64) -> Self {
        self.inherited_tx_id = Some(tx_id);
        self
    }

    pub fn check_table_access(&self, table: &str) -> Result<()> {
        if let Some(allowed) = &self.allowed_tables {
            if !allowed.contains(table) {
                anyhow::bail!("Access denied to table: {}", table);
            }
        }
        Ok(())
    }

    pub fn check_write_access(&self) -> Result<()> {
        if self.read_only {
            anyhow::bail!("Write access denied - user is read-only");
        }
        Ok(())
    }
}

impl Default for PluginPermissions {
    fn default() -> Self {
        // Default to read-only for safety
        Self::read_only()
    }
}

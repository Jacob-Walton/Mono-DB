#![allow(dead_code)]

use std::{sync::atomic::AtomicU64, time::Instant};

use monodb_common::permissions::PermissionSet;

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A client session representing an active connection.
pub struct Session {
    /// Unique session identifier
    pub id: u64,
    /// Current transaction ID if in a transaction
    pub tx_id: Option<u64>,
    /// User identifier (empty if not authenticated)
    pub user_id: String,
    /// Permission set for this session
    pub permissions: PermissionSet,
    /// Current namespace context
    pub current_namespace: String,
    /// When the session was created
    pub created_at: Instant,
    /// Last activity timestamp for timeout tracking
    pub last_activity: Instant,
    /// Whether the session is authenticated
    pub authenticated: bool,
}

impl Session {
    /// Create a new session with the given user ID and authentication state.
    pub fn new(user_id: String, authenticated: bool) -> Self {
        let id = SESSION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Session {
            id,
            tx_id: None,
            user_id,
            permissions: PermissionSet::new(),
            current_namespace: "default".to_string(),
            created_at: Instant::now(),
            last_activity: Instant::now(),
            authenticated,
        }
    }

    /// Create an unauthenticated session (for initial connection).
    pub fn unauthenticated() -> Self {
        Self::new(String::new(), false)
    }

    /// Update the last activity timestamp.
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Switch to a different namespace.
    pub fn use_namespace(&mut self, namespace: String) {
        self.current_namespace = namespace;
        self.touch();
    }

    /// Begin a transaction with the given ID.
    pub fn begin_transaction(&mut self, tx_id: u64) {
        self.tx_id = Some(tx_id);
        self.touch();
    }

    /// End the current transaction.
    pub fn end_transaction(&mut self) {
        self.tx_id = None;
        self.touch();
    }

    /// Check if there's an active transaction.
    pub fn in_transaction(&self) -> bool {
        self.tx_id.is_some()
    }

    /// Get the current transaction ID.
    pub fn transaction_id(&self) -> Option<u64> {
        self.tx_id
    }

    /// Check how long since the last activity.
    pub fn idle_duration(&self) -> std::time::Duration {
        self.last_activity.elapsed()
    }

    /// Check if the session has timed out.
    pub fn is_timed_out(&self, timeout: std::time::Duration) -> bool {
        self.idle_duration() > timeout
    }

    /// Qualify a table name with the current namespace if not already qualified.
    pub fn qualify_table(&self, table: &str) -> String {
        if table.contains('.') {
            table.to_string()
        } else {
            format!("{}.{}", self.current_namespace, table)
        }
    }
}

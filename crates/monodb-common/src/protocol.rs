use crate::value::Value;
use serde::{Deserialize, Serialize};

/// Current protocol version
const VERSION: u8 = 3;

/// Wire protocol messages
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    // Connection Lifecycle
    /// Initial handshake, without authentication
    Hello {
        client_name: String,
        #[serde(default)]
        capabilities: Vec<String>, // e.g. ["transactions", "streaming"]
    },

    /// Authenticate the connection
    Authenticate {
        method: AuthMethod,
    },

    /// Graceful disconnect
    Disconnect,

    /// Health check
    Ping,

    // Query Execution
    /// Single query execution
    Query {
        statement: String,
        #[serde(default)]
        params: Vec<Value>,
    },

    /// Batch multiple queries (to be executed sequentially)
    Batch {
        statements: Vec<Statement>,
    },

    // Transaction Management
    TxBegin {
        isolation: IsolationLevel,
        #[serde(default)]
        read_only: bool,
    },

    TxCommit {
        tx_id: u64,
    },

    TxRollback {
        tx_id: u64,
    },

    // Metadata & Admin
    Describe {
        target: String,
    },

    List,

    Stats {
        #[serde(default)]
        detailed: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Statement {
    pub query: String,
    #[serde(default)]
    pub params: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum AuthMethod {
    Password { username: String, password: String },
    Token { token: String },
    Certificate, // For mTLS (if implemented)
}

/// Wire protocol response messages
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    // Connection Lifecycle
    Welcome {
        server_version: String,
        server_capabilities: Vec<String>,
        server_timestamp: u64,
    },

    AuthSuccess {
        session_id: u64,
        user_id: String,
        permissions: Vec<String>,
        expires_at: Option<u64>,
    },

    AuthFailed {
        reason: String,
        #[serde(default)]
        retry_after: Option<u64>, // Rate limiting
    },

    Pong {
        timestamp: u64,
    },

    // Query Results
    QueryResult {
        result: QueryOutcome,
        elapsed_ms: u64,
    },

    BatchResults {
        results: Vec<QueryOutcome>,
        elapsed_ms: u64,
    },

    KvDeleteResult {
        keys_deleted: u64,
    },

    // Transaction Results
    TxStarted {
        tx_id: u64,
        read_timestamp: u64,
    },

    TxCommitted {
        tx_id: u64,
        commit_timestamp: u64,
    },

    TxRolledBack {
        tx_id: u64,
    },

    // Metadata & Admin Results
    TableList {
        tables: Vec<TableInfo>,
    },

    TableDescription {
        schema: TableSchema,
    },

    StatsResult {
        stats: ServerStats,
    },

    // Universal Responses
    Ok, // Generic success acknowledgment

    Error {
        code: u16,
        message: String,
        #[serde(default)]
        details: Option<Value>,
    },
}

/// Query Outcomes
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryOutcome {
    /// get
    Rows {
        data: Vec<Value>, // Can be rows, objects, etc.
        row_count: u64,
        #[serde(default)]
        columns: Option<Vec<String>>, // For relational results
        #[serde(default)]
        has_more: bool, // For pagination/streaming
    },

    /// put
    Inserted {
        rows_inserted: u64,
        #[serde(default)]
        generated_ids: Option<Vec<Value>>,
    },

    /// change
    Updated { rows_updated: u64 },

    /// remove
    Deleted { rows_deleted: u64 },

    /// drop table, drop index, etc.
    Dropped {
        object_type: String,
        object_name: String,
    },

    /// DDL operations that don't fit above
    Executed,
}

// Supporting Types

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: Option<String>,
    pub row_count: Option<u64>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(default)]
    pub default_value: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerStats {
    pub uptime_seconds: u64,
    pub active_connections: u64,
    pub total_queries: u64,
    pub cache_hit_rate: f64,
    #[serde(default)]
    pub storage: Option<StorageStats>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageStats {
    pub total_size_bytes: u64,
    pub document_count: u64,
    pub table_count: u64,
    pub keyspace_entries: u64,
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

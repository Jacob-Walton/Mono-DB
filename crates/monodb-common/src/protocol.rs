//! Protocol definitions and codec for MonoDB
//!
//! This module defines the request and response messages used in the MonoDB protocol,
//! as well as stateful encoder and decoder structs for binary serialization.

use std::sync::atomic::{AtomicU32, Ordering};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{MonoError, Result, permissions::PermissionSet, value::Value};

/// Current protocol version
pub const VERSION: u8 = 3;

// Request Types

/// Wire protocol request messages
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    /// Initial handshake (no authentication)
    Hello {
        client_name: String,
        capabilities: Vec<String>,
    },

    /// Authenticate the connection
    Authenticate { method: AuthMethod },

    /// Graceful disconnect
    Disconnect,

    /// Health check / keepalive
    Ping,

    /// Execute a single query
    Query {
        statement: String,
        params: Vec<Value>,
    },

    /// Execute multiple queries sequentially
    Batch { statements: Vec<Statement> },

    /// Begin a transaction
    TxBegin {
        isolation: IsolationLevel,
        read_only: bool,
    },

    /// Commit a transaction
    TxCommit { tx_id: u64 },

    /// Rollback a transaction
    TxRollback { tx_id: u64 },

    /// Get schema for a table/collection
    Describe { target: String },

    /// List all tables/collections
    List,

    /// Get server statistics
    Stats { detailed: bool },

    // Namespace operations
    /// Switch to a different namespace
    UseNamespace { namespace: String },

    /// Create a new namespace
    CreateNamespace {
        name: String,
        description: Option<String>,
    },

    /// Drop a namespace (must be empty unless force=true)
    DropNamespace { name: String, force: bool },

    /// List all namespaces
    ListNamespaces,
}

/// A statement with parameters for batch execution
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    pub query: String,
    pub params: Vec<Value>,
}

/// Authentication methods
#[derive(Debug, Clone, PartialEq)]
pub enum AuthMethod {
    Password { username: String, password: String },
    Token { token: String },
    Certificate,
}

/// Transaction isolation levels
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted = 0x01,
    ReadCommitted = 0x02,
    RepeatableRead = 0x03,
    Serializable = 0x04,
}

/// Error codes for protocol error responses.
///
/// Error codes are grouped by category:
/// - 1xxx: Client/parse errors
/// - 2xxx: Execution errors
/// - 3xxx: Data errors (not found, already exists, etc.)
/// - 4xxx: Transaction errors
/// - 5xxx: Authentication/authorization errors
/// - 6xxx: Configuration errors
/// - 9xxx: Internal/unknown errors
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    // Client/Parse errors (1xxx)
    /// Failed to parse query or request
    ParseError = 1001,
    /// Invalid operation requested
    InvalidOperation = 1002,
    /// Type mismatch error
    TypeError = 1003,
    /// Schema validation failed
    SchemaMismatch = 1004,

    // Execution errors (2xxx)
    /// Query execution failed
    ExecutionError = 2001,
    /// I/O error during operation
    IoError = 2002,
    /// Storage engine error
    StorageError = 2003,
    /// Network communication error
    NetworkError = 2004,
    /// Data corruption detected
    CorruptionError = 2005,
    /// WAL error
    WalError = 2006,
    /// Checksum verification failed
    ChecksumError = 2007,
    /// Page not found in storage
    PageNotFound = 2008,
    /// Page is full
    PageFull = 2009,
    /// Buffer pool exhausted
    BufferPoolExhausted = 2010,
    /// Index structure corrupted
    IndexCorrupted = 2011,
    /// Disk is full
    DiskFull = 2012,
    /// Namespace quota exceeded
    QuotaExceeded = 2013,

    // Data errors (3xxx)
    /// Requested resource not found
    NotFound = 3001,
    /// Resource already exists
    AlreadyExists = 3002,

    // Transaction errors (4xxx)
    /// Transaction failed
    TransactionError = 4001,
    /// Write conflict detected
    WriteConflict = 4002,
    /// Lock acquisition timeout
    LockTimeout = 4003,
    /// Deadlock detected
    Deadlock = 4004,

    // Auth/Config errors (5xxx)
    /// Authentication failed (invalid credentials)
    AuthenticationFailed = 5001,
    /// User is not authorized for this operation
    Unauthorized = 5002,
    /// Session has expired
    SessionExpired = 5003,
    /// Permission denied for the requested resource/action
    PermissionDenied = 5004,
    /// Configuration error
    ConfigError = 5005,
    /// TLS/certificate error
    TlsError = 5006,

    // Internal errors (9xxx)
    /// Unknown or internal server error
    InternalError = 9999,
}

impl ErrorCode {
    /// Convert error code to its numeric value
    #[inline]
    pub fn to_u16(self) -> u16 {
        self as u16
    }

    /// Get a short string identifier for the error code
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::ParseError => "parse_error",
            ErrorCode::InvalidOperation => "invalid_operation",
            ErrorCode::TypeError => "type_error",
            ErrorCode::ExecutionError => "execution_error",
            ErrorCode::IoError => "io_error",
            ErrorCode::StorageError => "storage_error",
            ErrorCode::NetworkError => "network_error",
            ErrorCode::CorruptionError => "corruption_error",
            ErrorCode::WalError => "wal_error",
            ErrorCode::ChecksumError => "checksum_error",
            ErrorCode::PageNotFound => "page_not_found",
            ErrorCode::PageFull => "page_full",
            ErrorCode::BufferPoolExhausted => "buffer_pool_exhausted",
            ErrorCode::IndexCorrupted => "index_corrupted",
            ErrorCode::DiskFull => "disk_full",
            ErrorCode::QuotaExceeded => "quota_exceeded",
            ErrorCode::SchemaMismatch => "schema_mismatch",
            ErrorCode::NotFound => "not_found",
            ErrorCode::AlreadyExists => "already_exists",
            ErrorCode::TransactionError => "transaction_error",
            ErrorCode::WriteConflict => "write_conflict",
            ErrorCode::LockTimeout => "lock_timeout",
            ErrorCode::Deadlock => "deadlock",
            ErrorCode::AuthenticationFailed => "authentication_failed",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::SessionExpired => "session_expired",
            ErrorCode::PermissionDenied => "permission_denied",
            ErrorCode::ConfigError => "config_error",
            ErrorCode::TlsError => "tls_error",
            ErrorCode::InternalError => "internal_error",
        }
    }
}

impl From<u16> for ErrorCode {
    fn from(code: u16) -> Self {
        match code {
            1001 => ErrorCode::ParseError,
            1002 => ErrorCode::InvalidOperation,
            1003 => ErrorCode::TypeError,
            1004 => ErrorCode::SchemaMismatch,
            2001 => ErrorCode::ExecutionError,
            2002 => ErrorCode::IoError,
            2003 => ErrorCode::StorageError,
            2004 => ErrorCode::NetworkError,
            2005 => ErrorCode::CorruptionError,
            2006 => ErrorCode::WalError,
            2007 => ErrorCode::ChecksumError,
            2008 => ErrorCode::PageNotFound,
            2009 => ErrorCode::PageFull,
            2010 => ErrorCode::BufferPoolExhausted,
            2011 => ErrorCode::IndexCorrupted,
            2012 => ErrorCode::DiskFull,
            2013 => ErrorCode::QuotaExceeded,
            3001 => ErrorCode::NotFound,
            3002 => ErrorCode::AlreadyExists,
            4001 => ErrorCode::TransactionError,
            4002 => ErrorCode::WriteConflict,
            4003 => ErrorCode::LockTimeout,
            4004 => ErrorCode::Deadlock,
            5001 => ErrorCode::AuthenticationFailed,
            5002 => ErrorCode::Unauthorized,
            5003 => ErrorCode::SessionExpired,
            5004 => ErrorCode::PermissionDenied,
            5005 => ErrorCode::ConfigError,
            5006 => ErrorCode::TlsError,
            _ => ErrorCode::InternalError,
        }
    }
}

impl From<&crate::MonoError> for ErrorCode {
    fn from(err: &crate::MonoError) -> Self {
        match err {
            crate::MonoError::Parse(_) => ErrorCode::ParseError,
            crate::MonoError::InvalidOperation(_) => ErrorCode::InvalidOperation,
            crate::MonoError::TypeError { .. } => ErrorCode::TypeError,
            crate::MonoError::Execution(_) => ErrorCode::ExecutionError,
            crate::MonoError::Io(_) => ErrorCode::IoError,
            crate::MonoError::Storage(_) => ErrorCode::StorageError,
            crate::MonoError::Network(_) => ErrorCode::NetworkError,
            crate::MonoError::Corruption { .. } => ErrorCode::CorruptionError,
            crate::MonoError::Wal(_) => ErrorCode::WalError,
            crate::MonoError::ChecksumMismatch { .. } => ErrorCode::ChecksumError,
            crate::MonoError::PageNotFound(_) => ErrorCode::PageNotFound,
            crate::MonoError::PageFull => ErrorCode::PageFull,
            crate::MonoError::BufferPoolExhausted(_) => ErrorCode::BufferPoolExhausted,
            crate::MonoError::IndexCorrupted(_) => ErrorCode::IndexCorrupted,
            crate::MonoError::DiskFull { .. } => ErrorCode::DiskFull,
            crate::MonoError::QuotaExceeded { .. } => ErrorCode::QuotaExceeded,
            crate::MonoError::SchemaMismatch { .. } => ErrorCode::SchemaMismatch,
            crate::MonoError::LockTimeout(_) => ErrorCode::LockTimeout,
            crate::MonoError::Deadlock(_) => ErrorCode::Deadlock,
            crate::MonoError::NotFound(_) => ErrorCode::NotFound,
            crate::MonoError::AlreadyExists(_) => ErrorCode::AlreadyExists,
            crate::MonoError::Transaction(_) => ErrorCode::TransactionError,
            crate::MonoError::WriteConflict(_) => ErrorCode::WriteConflict,
            crate::MonoError::Config(_) => ErrorCode::ConfigError,
            crate::MonoError::AuthenticationFailed(_) => ErrorCode::Unauthorized,
            #[cfg(feature = "tls")]
            crate::MonoError::TlsCertLoad { .. }
            | crate::MonoError::TlsKeyLoad { .. }
            | crate::MonoError::TlsConfig(_) => ErrorCode::TlsError,
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.as_str(), *self as u16)
    }
}

// Response Types

/// Wire protocol response messages
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// Handshake acknowledgment
    Welcome {
        server_version: String,
        server_capabilities: Vec<String>,
        server_timestamp: u64,
    },

    /// Authentication succeeded
    AuthSuccess {
        session_id: u64,
        user_id: String,
        permissions: PermissionSet,
        expires_at: Option<u64>,
        /// Session token for resumption (only returned on password auth)
        token: Option<String>,
    },

    /// Authentication failed
    AuthFailed {
        reason: String,
        retry_after: Option<u64>,
    },

    /// Ping response
    Pong { timestamp: u64 },

    /// Single query result
    QueryResult {
        result: QueryOutcome,
        elapsed_ms: u64,
    },

    /// Batch query results
    BatchResults {
        results: Vec<QueryOutcome>,
        elapsed_ms: u64,
    },

    /// Transaction started
    TxStarted { tx_id: u64, read_timestamp: u64 },

    /// Transaction committed
    TxCommitted { tx_id: u64, commit_timestamp: u64 },

    /// Transaction rolled back
    TxRolledBack { tx_id: u64 },

    /// List of tables
    TableList {
        tables: Vec<TableInfo>,
        namespaces: Vec<String>,
    },

    /// Table schema description
    TableDescription { schema: TableSchema },

    /// Server statistics
    StatsResult { stats: ServerStats },

    /// List of namespaces
    NamespaceList { namespaces: Vec<NamespaceInfo> },

    /// Namespace switched successfully
    NamespaceSwitched { namespace: String },

    /// Namespace created successfully
    NamespaceCreated { namespace: String },

    /// Namespace dropped successfully
    NamespaceDropped { namespace: String },

    /// Generic success acknowledgment
    Ok,

    /// Error response
    Error {
        code: u16,
        message: String,
        details: Option<Value>,
    },
}

/// Query execution outcomes
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOutcome {
    /// Query returned rows/documents
    Rows {
        data: Vec<Value>,
        row_count: u64,
        columns: Option<Vec<String>>,
        has_more: bool,
    },

    /// Insert operation result
    Inserted {
        rows_inserted: u64,
        generated_ids: Option<Vec<Value>>,
    },

    /// Update operation result
    Updated { rows_updated: u64 },

    /// Delete operation result
    Deleted { rows_deleted: u64 },

    /// Create operation result
    Created {
        object_type: String,
        object_name: String,
    },

    /// Drop operation result
    Dropped {
        object_type: String,
        object_name: String,
    },

    /// DDL operation executed
    Executed,
}

// Supporting Types

#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    pub name: String,
    pub schema: Option<String>,
    pub row_count: Option<u64>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerStats {
    pub uptime_seconds: u64,
    pub active_connections: u64,
    pub total_queries: u64,
    pub cache_hit_rate: f64,
    pub storage: Option<StorageStats>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StorageStats {
    pub total_size_bytes: u64,
    pub document_count: u64,
    pub table_count: u64,
    pub keyspace_entries: u64,
}

/// Information about a namespace
#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceInfo {
    /// Namespace name
    pub name: String,
    /// Optional description
    pub description: Option<String>,
    /// Number of tables in this namespace
    pub table_count: u64,
    /// Storage usage in bytes
    pub size_bytes: Option<u64>,
}

// Command Codes

mod cmd {
    // Request commands
    pub const REQ_HELLO: u8 = 0x01;
    pub const REQ_AUTHENTICATE: u8 = 0x02;
    pub const REQ_DISCONNECT: u8 = 0x03;
    pub const REQ_PING: u8 = 0x04;
    pub const REQ_QUERY: u8 = 0x05;
    pub const REQ_BATCH: u8 = 0x06;
    pub const REQ_TX_BEGIN: u8 = 0x07;
    pub const REQ_TX_COMMIT: u8 = 0x08;
    pub const REQ_TX_ROLLBACK: u8 = 0x09;
    pub const REQ_DESCRIBE: u8 = 0x0A;
    pub const REQ_LIST: u8 = 0x0B;
    pub const REQ_STATS: u8 = 0x0C;
    pub const REQ_USE_NAMESPACE: u8 = 0x0D;
    pub const REQ_CREATE_NAMESPACE: u8 = 0x0E;
    pub const REQ_DROP_NAMESPACE: u8 = 0x0F;
    pub const REQ_LIST_NAMESPACES: u8 = 0x10;

    // Response commands
    pub const RSP_WELCOME: u8 = 0x01;
    pub const RSP_AUTH_SUCCESS: u8 = 0x02;
    pub const RSP_AUTH_FAILED: u8 = 0x03;
    pub const RSP_PONG: u8 = 0x04;
    pub const RSP_QUERY_RESULT: u8 = 0x05;
    pub const RSP_BATCH_RESULTS: u8 = 0x06;
    pub const RSP_TX_STARTED: u8 = 0x07;
    pub const RSP_TX_COMMITTED: u8 = 0x08;
    pub const RSP_TX_ROLLED_BACK: u8 = 0x09;
    pub const RSP_TABLE_LIST: u8 = 0x0A;
    pub const RSP_TABLE_DESCRIPTION: u8 = 0x0B;
    pub const RSP_STATS_RESULT: u8 = 0x0C;
    pub const RSP_OK: u8 = 0x0D;
    pub const RSP_ERROR: u8 = 0x0E;
    pub const RSP_NAMESPACE_LIST: u8 = 0x0F;
    pub const RSP_NAMESPACE_SWITCHED: u8 = 0x10;
    pub const RSP_NAMESPACE_CREATED: u8 = 0x11;
    pub const RSP_NAMESPACE_DROPPED: u8 = 0x12;

    // Auth method tags
    pub const AUTH_PASSWORD: u8 = 0x01;
    pub const AUTH_TOKEN: u8 = 0x02;
    pub const AUTH_CERTIFICATE: u8 = 0x03;

    // QueryOutcome tags
    pub const OUTCOME_ROWS: u8 = 0x01;
    pub const OUTCOME_INSERTED: u8 = 0x02;
    pub const OUTCOME_UPDATED: u8 = 0x03;
    pub const OUTCOME_DELETED: u8 = 0x04;
    pub const OUTCOME_DROPPED: u8 = 0x05;
    pub const OUTCOME_EXECUTED: u8 = 0x06;
    pub const OUTCOME_CREATED: u8 = 0x07;

    // Message kinds
    pub const KIND_REQUEST: u8 = 0x00;
    pub const KIND_RESPONSE: u8 = 0x01;
}

// Protocol Encoder

/// Stateful protocol encoder for requests
pub struct ProtocolEncoder {
    next_correlation_id: AtomicU32,
}

impl Default for ProtocolEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolEncoder {
    pub fn new() -> Self {
        Self {
            next_correlation_id: AtomicU32::new(1),
        }
    }

    /// Get the next correlation ID
    pub fn next_correlation_id(&self) -> u32 {
        self.next_correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Encode a request into bytes
    pub fn encode_request(&self, req: &Request) -> Result<Bytes> {
        self.encode_request_with_correlation(req, self.next_correlation_id())
    }

    /// Encode a request with a specific correlation ID
    pub fn encode_request_with_correlation(
        &self,
        req: &Request,
        correlation_id: u32,
    ) -> Result<Bytes> {
        let mut header = BytesMut::with_capacity(8);
        let mut body = BytesMut::new();

        // Write header prefix
        header.put_u8(VERSION);
        header.put_u8(cmd::KIND_REQUEST);

        match req {
            Request::Hello {
                client_name,
                capabilities,
            } => {
                header.put_u8(cmd::REQ_HELLO);
                put_string(&mut body, client_name);
                put_string_array(&mut body, capabilities);
            }
            Request::Authenticate { method } => {
                header.put_u8(cmd::REQ_AUTHENTICATE);
                match method {
                    AuthMethod::Password { username, password } => {
                        body.put_u8(cmd::AUTH_PASSWORD);
                        put_string(&mut body, username);
                        put_string(&mut body, password);
                    }
                    AuthMethod::Token { token } => {
                        body.put_u8(cmd::AUTH_TOKEN);
                        put_string(&mut body, token);
                    }
                    AuthMethod::Certificate => {
                        body.put_u8(cmd::AUTH_CERTIFICATE);
                    }
                }
            }
            Request::Disconnect => {
                header.put_u8(cmd::REQ_DISCONNECT);
            }
            Request::Ping => {
                header.put_u8(cmd::REQ_PING);
            }
            Request::Query { statement, params } => {
                header.put_u8(cmd::REQ_QUERY);
                put_string(&mut body, statement);
                put_value_array(&mut body, params);
            }
            Request::Batch { statements } => {
                header.put_u8(cmd::REQ_BATCH);
                body.put_u32_le(statements.len() as u32);
                for stmt in statements {
                    put_string(&mut body, &stmt.query);
                    put_value_array(&mut body, &stmt.params);
                }
            }
            Request::TxBegin {
                isolation,
                read_only,
            } => {
                header.put_u8(cmd::REQ_TX_BEGIN);
                body.put_u8(*isolation as u8);
                body.put_u8(if *read_only { 1 } else { 0 });
            }
            Request::TxCommit { tx_id } => {
                header.put_u8(cmd::REQ_TX_COMMIT);
                body.put_u64_le(*tx_id);
            }
            Request::TxRollback { tx_id } => {
                header.put_u8(cmd::REQ_TX_ROLLBACK);
                body.put_u64_le(*tx_id);
            }
            Request::Describe { target } => {
                header.put_u8(cmd::REQ_DESCRIBE);
                put_string(&mut body, target);
            }
            Request::List => {
                header.put_u8(cmd::REQ_LIST);
            }
            Request::Stats { detailed } => {
                header.put_u8(cmd::REQ_STATS);
                body.put_u8(if *detailed { 1 } else { 0 });
            }
            Request::UseNamespace { namespace } => {
                header.put_u8(cmd::REQ_USE_NAMESPACE);
                put_string(&mut body, namespace);
            }
            Request::CreateNamespace { name, description } => {
                header.put_u8(cmd::REQ_CREATE_NAMESPACE);
                put_string(&mut body, name);
                put_opt_string(&mut body, description);
            }
            Request::DropNamespace { name, force } => {
                header.put_u8(cmd::REQ_DROP_NAMESPACE);
                put_string(&mut body, name);
                body.put_u8(if *force { 1 } else { 0 });
            }
            Request::ListNamespaces => {
                header.put_u8(cmd::REQ_LIST_NAMESPACES);
            }
        }

        // Complete header
        header.put_u8(0); // flags
        header.put_u32_le(correlation_id);

        // Build final frame
        let frame_len = header.len() + body.len();
        let mut buf = BytesMut::with_capacity(4 + frame_len);
        buf.put_u32_le(frame_len as u32);
        buf.put_slice(&header);
        buf.put_slice(&body);

        Ok(buf.freeze())
    }

    /// Encode a response into bytes
    pub fn encode_response(&self, resp: &Response, correlation_id: u32) -> Result<Bytes> {
        let mut header = BytesMut::with_capacity(8);
        let mut body = BytesMut::new();

        // Write header prefix
        header.put_u8(VERSION);
        header.put_u8(cmd::KIND_RESPONSE);

        match resp {
            Response::Welcome {
                server_version,
                server_capabilities,
                server_timestamp,
            } => {
                header.put_u8(cmd::RSP_WELCOME);
                put_string(&mut body, server_version);
                put_string_array(&mut body, server_capabilities);
                body.put_u64_le(*server_timestamp);
            }
            Response::AuthSuccess {
                session_id,
                user_id,
                permissions,
                expires_at,
                token,
            } => {
                header.put_u8(cmd::RSP_AUTH_SUCCESS);
                body.put_u64_le(*session_id);
                put_string(&mut body, user_id);
                permissions.encode(&mut body);
                put_opt_u64(&mut body, expires_at);
                put_opt_string(&mut body, token);
            }
            Response::AuthFailed {
                reason,
                retry_after,
            } => {
                header.put_u8(cmd::RSP_AUTH_FAILED);
                put_string(&mut body, reason);
                put_opt_u64(&mut body, retry_after);
            }
            Response::Pong { timestamp } => {
                header.put_u8(cmd::RSP_PONG);
                body.put_u64_le(*timestamp);
            }
            Response::QueryResult { result, elapsed_ms } => {
                header.put_u8(cmd::RSP_QUERY_RESULT);
                encode_query_outcome(&mut body, result);
                body.put_u64_le(*elapsed_ms);
            }
            Response::BatchResults {
                results,
                elapsed_ms,
            } => {
                header.put_u8(cmd::RSP_BATCH_RESULTS);
                body.put_u32_le(results.len() as u32);
                for result in results {
                    encode_query_outcome(&mut body, result);
                }
                body.put_u64_le(*elapsed_ms);
            }
            Response::TxStarted {
                tx_id,
                read_timestamp,
            } => {
                header.put_u8(cmd::RSP_TX_STARTED);
                body.put_u64_le(*tx_id);
                body.put_u64_le(*read_timestamp);
            }
            Response::TxCommitted {
                tx_id,
                commit_timestamp,
            } => {
                header.put_u8(cmd::RSP_TX_COMMITTED);
                body.put_u64_le(*tx_id);
                body.put_u64_le(*commit_timestamp);
            }
            Response::TxRolledBack { tx_id } => {
                header.put_u8(cmd::RSP_TX_ROLLED_BACK);
                body.put_u64_le(*tx_id);
            }
            Response::TableList { tables, namespaces } => {
                header.put_u8(cmd::RSP_TABLE_LIST);
                body.put_u32_le(tables.len() as u32);
                for table in tables {
                    put_string(&mut body, &table.name);
                    put_opt_string(&mut body, &table.schema);
                    put_opt_u64(&mut body, &table.row_count);
                    put_opt_u64(&mut body, &table.size_bytes);
                }
                // Include all namespaces (even if empty)
                body.put_u32_le(namespaces.len() as u32);
                for ns in namespaces {
                    put_string(&mut body, ns);
                }
            }
            Response::TableDescription { schema } => {
                header.put_u8(cmd::RSP_TABLE_DESCRIPTION);
                put_string(&mut body, &schema.name);
                body.put_u32_le(schema.columns.len() as u32);
                for col in &schema.columns {
                    put_string(&mut body, &col.name);
                    put_string(&mut body, &col.data_type);
                    body.put_u8(if col.nullable { 1 } else { 0 });
                    put_opt_value(&mut body, &col.default_value);
                }
                body.put_u32_le(schema.indexes.len() as u32);
                for idx in &schema.indexes {
                    put_string(&mut body, &idx.name);
                    put_string_array(&mut body, &idx.columns);
                    body.put_u8(if idx.unique { 1 } else { 0 });
                }
            }
            Response::StatsResult { stats } => {
                header.put_u8(cmd::RSP_STATS_RESULT);
                body.put_u64_le(stats.uptime_seconds);
                body.put_u64_le(stats.active_connections);
                body.put_u64_le(stats.total_queries);
                body.put_u64_le(stats.cache_hit_rate.to_bits());
                if let Some(storage) = &stats.storage {
                    body.put_u8(1);
                    body.put_u64_le(storage.total_size_bytes);
                    body.put_u64_le(storage.document_count);
                    body.put_u64_le(storage.table_count);
                    body.put_u64_le(storage.keyspace_entries);
                } else {
                    body.put_u8(0);
                }
            }
            Response::NamespaceList { namespaces } => {
                header.put_u8(cmd::RSP_NAMESPACE_LIST);
                body.put_u32_le(namespaces.len() as u32);
                for ns in namespaces {
                    put_string(&mut body, &ns.name);
                    put_opt_string(&mut body, &ns.description);
                    body.put_u64_le(ns.table_count);
                    put_opt_u64(&mut body, &ns.size_bytes);
                }
            }
            Response::NamespaceSwitched { namespace } => {
                header.put_u8(cmd::RSP_NAMESPACE_SWITCHED);
                put_string(&mut body, namespace);
            }
            Response::NamespaceCreated { namespace } => {
                header.put_u8(cmd::RSP_NAMESPACE_CREATED);
                put_string(&mut body, namespace);
            }
            Response::NamespaceDropped { namespace } => {
                header.put_u8(cmd::RSP_NAMESPACE_DROPPED);
                put_string(&mut body, namespace);
            }
            Response::Ok => {
                header.put_u8(cmd::RSP_OK);
            }
            Response::Error {
                code,
                message,
                details,
            } => {
                header.put_u8(cmd::RSP_ERROR);
                body.put_u16_le(*code);
                put_string(&mut body, message);
                put_opt_value(&mut body, details);
            }
        }

        // Complete header
        header.put_u8(0); // flags
        header.put_u32_le(correlation_id);

        // Build final frame
        let frame_len = header.len() + body.len();
        let mut buf = BytesMut::with_capacity(4 + frame_len);
        buf.put_u32_le(frame_len as u32);
        buf.put_slice(&header);
        buf.put_slice(&body);

        Ok(buf.freeze())
    }
}

// Protocol Decoder

/// Stateful protocol decoder
pub struct ProtocolDecoder {
    // Reserved for future state (partial frames, etc.)
}

impl Default for ProtocolDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolDecoder {
    pub fn new() -> Self {
        Self {}
    }

    /// Decode a request from bytes
    /// Returns Ok(None) if not enough data is available
    pub fn decode_request(&self, buf: &mut BytesMut) -> Result<Option<(Request, u32)>> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let frame_len = (&buf[..4]).get_u32_le() as usize;
        if buf.len() < 4 + frame_len {
            return Ok(None);
        }

        buf.advance(4);
        let data = buf.split_to(frame_len);
        let mut cursor = &data[..];

        // Parse header
        let version = get_u8(&mut cursor)?;
        let kind = get_u8(&mut cursor)?;
        let command = get_u8(&mut cursor)?;
        let _flags = get_u8(&mut cursor)?;
        let correlation_id = get_u32_le(&mut cursor)?;

        if version != VERSION {
            return Err(MonoError::Network(format!(
                "Unsupported protocol version: {version}"
            )));
        }
        if kind != cmd::KIND_REQUEST {
            return Err(MonoError::Network(format!(
                "Expected request, got kind: {kind}"
            )));
        }

        let request = match command {
            cmd::REQ_HELLO => {
                let client_name = get_string(&mut cursor)?;
                let capabilities = get_string_array(&mut cursor)?;
                Request::Hello {
                    client_name,
                    capabilities,
                }
            }
            cmd::REQ_AUTHENTICATE => {
                let method_tag = get_u8(&mut cursor)?;
                let method = match method_tag {
                    cmd::AUTH_PASSWORD => {
                        let username = get_string(&mut cursor)?;
                        let password = get_string(&mut cursor)?;
                        AuthMethod::Password { username, password }
                    }
                    cmd::AUTH_TOKEN => {
                        let token = get_string(&mut cursor)?;
                        AuthMethod::Token { token }
                    }
                    cmd::AUTH_CERTIFICATE => AuthMethod::Certificate,
                    _ => {
                        return Err(MonoError::Network(format!(
                            "Unknown auth method: {method_tag}"
                        )));
                    }
                };
                Request::Authenticate { method }
            }
            cmd::REQ_DISCONNECT => Request::Disconnect,
            cmd::REQ_PING => Request::Ping,
            cmd::REQ_QUERY => {
                let statement = get_string(&mut cursor)?;
                let params = get_value_array(&mut cursor)?;
                Request::Query { statement, params }
            }
            cmd::REQ_BATCH => {
                let count = get_u32_le(&mut cursor)? as usize;
                let mut statements = Vec::with_capacity(count);
                for _ in 0..count {
                    let query = get_string(&mut cursor)?;
                    let params = get_value_array(&mut cursor)?;
                    statements.push(Statement { query, params });
                }
                Request::Batch { statements }
            }
            cmd::REQ_TX_BEGIN => {
                let isolation_byte = get_u8(&mut cursor)?;
                let isolation = match isolation_byte {
                    0x01 => IsolationLevel::ReadUncommitted,
                    0x02 => IsolationLevel::ReadCommitted,
                    0x03 => IsolationLevel::RepeatableRead,
                    0x04 => IsolationLevel::Serializable,
                    _ => {
                        return Err(MonoError::Network(format!(
                            "Unknown isolation level: {isolation_byte}"
                        )));
                    }
                };
                let read_only = get_u8(&mut cursor)? != 0;
                Request::TxBegin {
                    isolation,
                    read_only,
                }
            }
            cmd::REQ_TX_COMMIT => {
                let tx_id = get_u64_le(&mut cursor)?;
                Request::TxCommit { tx_id }
            }
            cmd::REQ_TX_ROLLBACK => {
                let tx_id = get_u64_le(&mut cursor)?;
                Request::TxRollback { tx_id }
            }
            cmd::REQ_DESCRIBE => {
                let target = get_string(&mut cursor)?;
                Request::Describe { target }
            }
            cmd::REQ_LIST => Request::List,
            cmd::REQ_STATS => {
                let detailed = get_u8(&mut cursor)? != 0;
                Request::Stats { detailed }
            }
            cmd::REQ_USE_NAMESPACE => {
                let namespace = get_string(&mut cursor)?;
                Request::UseNamespace { namespace }
            }
            cmd::REQ_CREATE_NAMESPACE => {
                let name = get_string(&mut cursor)?;
                let description = get_opt_string(&mut cursor)?;
                Request::CreateNamespace { name, description }
            }
            cmd::REQ_DROP_NAMESPACE => {
                let name = get_string(&mut cursor)?;
                let force = get_u8(&mut cursor)? != 0;
                Request::DropNamespace { name, force }
            }
            cmd::REQ_LIST_NAMESPACES => Request::ListNamespaces,
            _ => {
                return Err(MonoError::Network(format!(
                    "Unknown request command: {command}"
                )));
            }
        };

        Ok(Some((request, correlation_id)))
    }

    /// Decode a response from bytes
    /// Returns Ok(None) if not enough data is available
    pub fn decode_response(&self, buf: &mut BytesMut) -> Result<Option<(Response, u32)>> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let frame_len = (&buf[..4]).get_u32_le() as usize;
        if buf.len() < 4 + frame_len {
            return Ok(None);
        }

        buf.advance(4);
        let data = buf.split_to(frame_len);
        let mut cursor = &data[..];

        // Parse header
        let version = get_u8(&mut cursor)?;
        let kind = get_u8(&mut cursor)?;
        let command = get_u8(&mut cursor)?;
        let _flags = get_u8(&mut cursor)?;
        let correlation_id = get_u32_le(&mut cursor)?;

        if version != VERSION {
            return Err(MonoError::Network(format!(
                "Unsupported protocol version: {version}"
            )));
        }
        if kind != cmd::KIND_RESPONSE {
            return Err(MonoError::Network(format!(
                "Expected response, got kind: {kind}"
            )));
        }

        let response = match command {
            cmd::RSP_WELCOME => {
                let server_version = get_string(&mut cursor)?;
                let server_capabilities = get_string_array(&mut cursor)?;
                let server_timestamp = get_u64_le(&mut cursor)?;
                Response::Welcome {
                    server_version,
                    server_capabilities,
                    server_timestamp,
                }
            }
            cmd::RSP_AUTH_SUCCESS => {
                let session_id = get_u64_le(&mut cursor)?;
                let user_id = get_string(&mut cursor)?;
                let permissions = PermissionSet::decode(&mut cursor)?;
                let expires_at = get_opt_u64(&mut cursor)?;
                // Token is optional for backwards compatibility
                let token = if cursor.has_remaining() {
                    get_opt_string(&mut cursor)?
                } else {
                    None
                };
                Response::AuthSuccess {
                    session_id,
                    user_id,
                    permissions,
                    expires_at,
                    token,
                }
            }
            cmd::RSP_AUTH_FAILED => {
                let reason = get_string(&mut cursor)?;
                let retry_after = get_opt_u64(&mut cursor)?;
                Response::AuthFailed {
                    reason,
                    retry_after,
                }
            }
            cmd::RSP_PONG => {
                let timestamp = get_u64_le(&mut cursor)?;
                Response::Pong { timestamp }
            }
            cmd::RSP_QUERY_RESULT => {
                let result = decode_query_outcome(&mut cursor)?;
                let elapsed_ms = get_u64_le(&mut cursor)?;
                Response::QueryResult { result, elapsed_ms }
            }
            cmd::RSP_BATCH_RESULTS => {
                let count = get_u32_le(&mut cursor)? as usize;
                let mut results = Vec::with_capacity(count);
                for _ in 0..count {
                    results.push(decode_query_outcome(&mut cursor)?);
                }
                let elapsed_ms = get_u64_le(&mut cursor)?;
                Response::BatchResults {
                    results,
                    elapsed_ms,
                }
            }
            cmd::RSP_TX_STARTED => {
                let tx_id = get_u64_le(&mut cursor)?;
                let read_timestamp = get_u64_le(&mut cursor)?;
                Response::TxStarted {
                    tx_id,
                    read_timestamp,
                }
            }
            cmd::RSP_TX_COMMITTED => {
                let tx_id = get_u64_le(&mut cursor)?;
                let commit_timestamp = get_u64_le(&mut cursor)?;
                Response::TxCommitted {
                    tx_id,
                    commit_timestamp,
                }
            }
            cmd::RSP_TX_ROLLED_BACK => {
                let tx_id = get_u64_le(&mut cursor)?;
                Response::TxRolledBack { tx_id }
            }
            cmd::RSP_TABLE_LIST => {
                let count = get_u32_le(&mut cursor)? as usize;
                let mut tables = Vec::with_capacity(count);
                for _ in 0..count {
                    let name = get_string(&mut cursor)?;
                    let schema = get_opt_string(&mut cursor)?;
                    let row_count = get_opt_u64(&mut cursor)?;
                    let size_bytes = get_opt_u64(&mut cursor)?;
                    tables.push(TableInfo {
                        name,
                        schema,
                        row_count,
                        size_bytes,
                    });
                }
                // Read namespaces if present
                let namespaces = if cursor.has_remaining() {
                    let ns_count = get_u32_le(&mut cursor)? as usize;
                    let mut ns = Vec::with_capacity(ns_count);
                    for _ in 0..ns_count {
                        ns.push(get_string(&mut cursor)?);
                    }
                    ns
                } else {
                    Vec::new()
                };
                Response::TableList { tables, namespaces }
            }
            cmd::RSP_TABLE_DESCRIPTION => {
                let name = get_string(&mut cursor)?;
                let col_count = get_u32_le(&mut cursor)? as usize;
                let mut columns = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    let col_name = get_string(&mut cursor)?;
                    let data_type = get_string(&mut cursor)?;
                    let nullable = get_u8(&mut cursor)? != 0;
                    let default_value = get_opt_value(&mut cursor)?;
                    columns.push(ColumnInfo {
                        name: col_name,
                        data_type,
                        nullable,
                        default_value,
                    });
                }
                let idx_count = get_u32_le(&mut cursor)? as usize;
                let mut indexes = Vec::with_capacity(idx_count);
                for _ in 0..idx_count {
                    let idx_name = get_string(&mut cursor)?;
                    let idx_columns = get_string_array(&mut cursor)?;
                    let unique = get_u8(&mut cursor)? != 0;
                    indexes.push(IndexInfo {
                        name: idx_name,
                        columns: idx_columns,
                        unique,
                    });
                }
                Response::TableDescription {
                    schema: TableSchema {
                        name,
                        columns,
                        indexes,
                    },
                }
            }
            cmd::RSP_STATS_RESULT => {
                let uptime_seconds = get_u64_le(&mut cursor)?;
                let active_connections = get_u64_le(&mut cursor)?;
                let total_queries = get_u64_le(&mut cursor)?;
                let cache_hit_rate = f64::from_bits(get_u64_le(&mut cursor)?);
                let storage = if get_u8(&mut cursor)? != 0 {
                    Some(StorageStats {
                        total_size_bytes: get_u64_le(&mut cursor)?,
                        document_count: get_u64_le(&mut cursor)?,
                        table_count: get_u64_le(&mut cursor)?,
                        keyspace_entries: get_u64_le(&mut cursor)?,
                    })
                } else {
                    None
                };
                Response::StatsResult {
                    stats: ServerStats {
                        uptime_seconds,
                        active_connections,
                        total_queries,
                        cache_hit_rate,
                        storage,
                    },
                }
            }
            cmd::RSP_OK => Response::Ok,
            cmd::RSP_NAMESPACE_LIST => {
                let count = get_u32_le(&mut cursor)? as usize;
                let mut namespaces = Vec::with_capacity(count);
                for _ in 0..count {
                    let name = get_string(&mut cursor)?;
                    let description = get_opt_string(&mut cursor)?;
                    let table_count = get_u64_le(&mut cursor)?;
                    let size_bytes = get_opt_u64(&mut cursor)?;
                    namespaces.push(NamespaceInfo {
                        name,
                        description,
                        table_count,
                        size_bytes,
                    });
                }
                Response::NamespaceList { namespaces }
            }
            cmd::RSP_NAMESPACE_SWITCHED => {
                let namespace = get_string(&mut cursor)?;
                Response::NamespaceSwitched { namespace }
            }
            cmd::RSP_NAMESPACE_CREATED => {
                let namespace = get_string(&mut cursor)?;
                Response::NamespaceCreated { namespace }
            }
            cmd::RSP_NAMESPACE_DROPPED => {
                let namespace = get_string(&mut cursor)?;
                Response::NamespaceDropped { namespace }
            }
            cmd::RSP_ERROR => {
                let code = get_u16_le(&mut cursor)?;
                let message = get_string(&mut cursor)?;
                let details = get_opt_value(&mut cursor)?;
                Response::Error {
                    code,
                    message,
                    details,
                }
            }
            _ => {
                return Err(MonoError::Network(format!(
                    "Unknown response command: {command}"
                )));
            }
        };

        Ok(Some((response, correlation_id)))
    }
}

// QueryOutcome Encoding/Decoding

fn encode_query_outcome(buf: &mut BytesMut, outcome: &QueryOutcome) {
    match outcome {
        QueryOutcome::Rows {
            data,
            row_count,
            columns,
            has_more,
        } => {
            buf.put_u8(cmd::OUTCOME_ROWS);
            buf.put_u64_le(*row_count);
            put_value_array(buf, data);
            if let Some(cols) = columns {
                buf.put_u8(1);
                put_string_array(buf, cols);
            } else {
                buf.put_u8(0);
            }
            buf.put_u8(if *has_more { 1 } else { 0 });
        }
        QueryOutcome::Inserted {
            rows_inserted,
            generated_ids,
        } => {
            buf.put_u8(cmd::OUTCOME_INSERTED);
            buf.put_u64_le(*rows_inserted);
            if let Some(ids) = generated_ids {
                buf.put_u8(1);
                put_value_array(buf, ids);
            } else {
                buf.put_u8(0);
            }
        }
        QueryOutcome::Updated { rows_updated } => {
            buf.put_u8(cmd::OUTCOME_UPDATED);
            buf.put_u64_le(*rows_updated);
        }
        QueryOutcome::Deleted { rows_deleted } => {
            buf.put_u8(cmd::OUTCOME_DELETED);
            buf.put_u64_le(*rows_deleted);
        }
        QueryOutcome::Created {
            object_type,
            object_name,
        } => {
            buf.put_u8(cmd::OUTCOME_CREATED);
            put_string(buf, object_type);
            put_string(buf, object_name);
        }
        QueryOutcome::Dropped {
            object_type,
            object_name,
        } => {
            buf.put_u8(cmd::OUTCOME_DROPPED);
            put_string(buf, object_type);
            put_string(buf, object_name);
        }
        QueryOutcome::Executed => {
            buf.put_u8(cmd::OUTCOME_EXECUTED);
        }
    }
}

fn decode_query_outcome(cursor: &mut &[u8]) -> Result<QueryOutcome> {
    let tag = get_u8(cursor)?;
    match tag {
        cmd::OUTCOME_ROWS => {
            let row_count = get_u64_le(cursor)?;
            let data = get_value_array(cursor)?;
            let columns = if get_u8(cursor)? != 0 {
                Some(get_string_array(cursor)?)
            } else {
                None
            };
            let has_more = get_u8(cursor)? != 0;
            Ok(QueryOutcome::Rows {
                data,
                row_count,
                columns,
                has_more,
            })
        }
        cmd::OUTCOME_INSERTED => {
            let rows_inserted = get_u64_le(cursor)?;
            let generated_ids = if get_u8(cursor)? != 0 {
                Some(get_value_array(cursor)?)
            } else {
                None
            };
            Ok(QueryOutcome::Inserted {
                rows_inserted,
                generated_ids,
            })
        }
        cmd::OUTCOME_UPDATED => {
            let rows_updated = get_u64_le(cursor)?;
            Ok(QueryOutcome::Updated { rows_updated })
        }
        cmd::OUTCOME_DELETED => {
            let rows_deleted = get_u64_le(cursor)?;
            Ok(QueryOutcome::Deleted { rows_deleted })
        }
        cmd::OUTCOME_DROPPED => {
            let object_type = get_string(cursor)?;
            let object_name = get_string(cursor)?;
            Ok(QueryOutcome::Dropped {
                object_type,
                object_name,
            })
        }
        cmd::OUTCOME_CREATED => {
            let object_type = get_string(cursor)?;
            let object_name = get_string(cursor)?;
            Ok(QueryOutcome::Created {
                object_type,
                object_name,
            })
        }
        cmd::OUTCOME_EXECUTED => Ok(QueryOutcome::Executed),
        _ => Err(MonoError::Network(format!(
            "Unknown query outcome tag: {tag}"
        ))),
    }
}

// Primitive Encoding Helpers

#[inline]
pub(crate) fn put_string(buf: &mut BytesMut, s: &str) {
    buf.put_u32_le(s.len() as u32);
    buf.put_slice(s.as_bytes());
}

#[inline]
pub(crate) fn put_string_array(buf: &mut BytesMut, arr: &[String]) {
    buf.put_u32_le(arr.len() as u32);
    for s in arr {
        put_string(buf, s);
    }
}

#[inline]
pub(crate) fn put_opt_u64(buf: &mut BytesMut, val: &Option<u64>) {
    match val {
        Some(v) => {
            buf.put_u8(1);
            buf.put_u64_le(*v);
        }
        None => buf.put_u8(0),
    }
}

#[inline]
pub(crate) fn put_opt_string(buf: &mut BytesMut, val: &Option<String>) {
    match val {
        Some(s) => {
            buf.put_u8(1);
            put_string(buf, s);
        }
        None => buf.put_u8(0),
    }
}

#[inline]
pub(crate) fn put_opt_value(buf: &mut BytesMut, val: &Option<Value>) {
    match val {
        Some(v) => {
            buf.put_u8(1);
            let bytes = v.to_bytes();
            buf.put_u32_le(bytes.len() as u32);
            buf.put_slice(&bytes);
        }
        None => buf.put_u8(0),
    }
}

#[inline]
pub(crate) fn put_value_array(buf: &mut BytesMut, arr: &[Value]) {
    buf.put_u32_le(arr.len() as u32);
    for v in arr {
        let bytes = v.to_bytes();
        buf.put_u32_le(bytes.len() as u32);
        buf.put_slice(&bytes);
    }
}

// Primitive Decoding Helpers

#[inline]
pub(crate) fn get_u8(cursor: &mut &[u8]) -> Result<u8> {
    if cursor.is_empty() {
        return Err(MonoError::Network("Unexpected EOF".into()));
    }
    let v = cursor[0];
    *cursor = &cursor[1..];
    Ok(v)
}

#[inline]
pub(crate) fn get_u16_le(cursor: &mut &[u8]) -> Result<u16> {
    if cursor.len() < 2 {
        return Err(MonoError::Network("Unexpected EOF".into()));
    }
    let v = u16::from_le_bytes([cursor[0], cursor[1]]);
    *cursor = &cursor[2..];
    Ok(v)
}

#[inline]
pub(crate) fn get_u32_le(cursor: &mut &[u8]) -> Result<u32> {
    if cursor.len() < 4 {
        return Err(MonoError::Network("Unexpected EOF".into()));
    }
    let v = u32::from_le_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
    *cursor = &cursor[4..];
    Ok(v)
}

#[inline]
pub(crate) fn get_u64_le(cursor: &mut &[u8]) -> Result<u64> {
    if cursor.len() < 8 {
        return Err(MonoError::Network("Unexpected EOF".into()));
    }
    let v = u64::from_le_bytes([
        cursor[0], cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6], cursor[7],
    ]);
    *cursor = &cursor[8..];
    Ok(v)
}

#[inline]
pub(crate) fn get_string(cursor: &mut &[u8]) -> Result<String> {
    let len = get_u32_le(cursor)? as usize;
    if cursor.len() < len {
        return Err(MonoError::Network("Truncated string".into()));
    }
    let s = std::str::from_utf8(&cursor[..len])
        .map_err(|e| MonoError::Network(format!("Invalid UTF-8: {e}")))?
        .to_owned();
    *cursor = &cursor[len..];
    Ok(s)
}

#[inline]
pub(crate) fn get_string_array(cursor: &mut &[u8]) -> Result<Vec<String>> {
    let count = get_u32_le(cursor)? as usize;
    let mut arr = Vec::with_capacity(count);
    for _ in 0..count {
        arr.push(get_string(cursor)?);
    }
    Ok(arr)
}

#[inline]
pub(crate) fn get_opt_u64(cursor: &mut &[u8]) -> Result<Option<u64>> {
    if get_u8(cursor)? != 0 {
        Ok(Some(get_u64_le(cursor)?))
    } else {
        Ok(None)
    }
}

#[inline]
pub(crate) fn get_opt_string(cursor: &mut &[u8]) -> Result<Option<String>> {
    if get_u8(cursor)? != 0 {
        Ok(Some(get_string(cursor)?))
    } else {
        Ok(None)
    }
}

#[inline]
pub(crate) fn get_opt_value(cursor: &mut &[u8]) -> Result<Option<Value>> {
    if get_u8(cursor)? != 0 {
        let len = get_u32_le(cursor)? as usize;
        if cursor.len() < len {
            return Err(MonoError::Network("Truncated value".into()));
        }
        let (val, _) = Value::from_bytes(&cursor[..len])?;
        *cursor = &cursor[len..];
        Ok(Some(val))
    } else {
        Ok(None)
    }
}

#[inline]
pub(crate) fn get_value_array(cursor: &mut &[u8]) -> Result<Vec<Value>> {
    let count = get_u32_le(cursor)? as usize;
    let mut arr = Vec::with_capacity(count);
    for _ in 0..count {
        let len = get_u32_le(cursor)? as usize;
        if cursor.len() < len {
            return Err(MonoError::Network("Truncated value in array".into()));
        }
        let (val, _) = Value::from_bytes(&cursor[..len])?;
        *cursor = &cursor[len..];
        arr.push(val);
    }
    Ok(arr)
}

// Tests

#[cfg(test)]
mod tests {
    use crate::permissions::{Action, Permission, Resource};

    use super::*;

    #[test]
    fn test_request_roundtrip_hello() {
        let encoder = ProtocolEncoder::new();
        let decoder = ProtocolDecoder::new();

        let req = Request::Hello {
            client_name: "test-client".into(),
            capabilities: vec!["transactions".into(), "streaming".into()],
        };

        let encoded = encoder.encode_request(&req).unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let (decoded, _corr_id) = decoder.decode_request(&mut buf).unwrap().unwrap();

        assert_eq!(req, decoded);
    }

    #[test]
    fn test_request_roundtrip_query() {
        let encoder = ProtocolEncoder::new();
        let decoder = ProtocolDecoder::new();

        let req = Request::Query {
            statement: "SELECT * FROM users WHERE id = ?".into(),
            params: vec![Value::Int32(42)],
        };

        let encoded = encoder.encode_request(&req).unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let (decoded, _corr_id) = decoder.decode_request(&mut buf).unwrap().unwrap();

        assert_eq!(req, decoded);
    }

    #[test]
    fn test_response_roundtrip_query_result() {
        let encoder = ProtocolEncoder::new();
        let decoder = ProtocolDecoder::new();

        let resp = Response::QueryResult {
            result: QueryOutcome::Rows {
                data: vec![Value::String("hello".into()), Value::Int64(123)],
                row_count: 2,
                columns: Some(vec!["name".into(), "id".into()]),
                has_more: false,
            },
            elapsed_ms: 42,
        };

        let encoded = encoder.encode_response(&resp, 1).unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let (decoded, corr_id) = decoder.decode_response(&mut buf).unwrap().unwrap();

        assert_eq!(resp, decoded);
        assert_eq!(corr_id, 1);
    }

    #[test]
    fn test_response_roundtrip_error() {
        let encoder = ProtocolEncoder::new();
        let decoder = ProtocolDecoder::new();

        let resp = Response::Error {
            code: 1001,
            message: "Parse error".into(),
            details: Some(Value::String("unexpected token".into())),
        };

        let encoded = encoder.encode_response(&resp, 42).unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let (decoded, corr_id) = decoder.decode_response(&mut buf).unwrap().unwrap();

        assert_eq!(resp, decoded);
        assert_eq!(corr_id, 42);
    }

    #[test]
    fn test_incomplete_frame() {
        let decoder = ProtocolDecoder::new();
        let encoder = ProtocolEncoder::new();

        let req = Request::Ping;
        let encoded = encoder.encode_request(&req).unwrap();

        // Only provide partial data
        let mut buf = BytesMut::from(&encoded[..5]);
        assert!(decoder.decode_request(&mut buf).unwrap().is_none());
    }

    fn roundtrip_request(req: Request) {
        let encoder = ProtocolEncoder::new();
        let decoder = ProtocolDecoder::new();
        let encoded = encoder.encode_request_with_correlation(&req, 99).unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let (decoded, corr_id) = decoder.decode_request(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, req);
        assert_eq!(corr_id, 99);
    }

    fn roundtrip_response(resp: Response) {
        let encoder = ProtocolEncoder::new();
        let decoder = ProtocolDecoder::new();
        let encoded = encoder.encode_response(&resp, 7).unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let (decoded, corr_id) = decoder.decode_response(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, resp);
        assert_eq!(corr_id, 7);
    }

    #[test]
    fn test_request_roundtrip_variants() {
        roundtrip_request(Request::Authenticate {
            method: AuthMethod::Password {
                username: "user".into(),
                password: "pass".into(),
            },
        });
        roundtrip_request(Request::Authenticate {
            method: AuthMethod::Token {
                token: "token".into(),
            },
        });
        roundtrip_request(Request::Authenticate {
            method: AuthMethod::Certificate,
        });
        roundtrip_request(Request::Disconnect);
        roundtrip_request(Request::Ping);
        roundtrip_request(Request::Batch {
            statements: vec![
                Statement {
                    query: "get from users".into(),
                    params: vec![Value::Int32(1)],
                },
                Statement {
                    query: "get from posts".into(),
                    params: vec![],
                },
            ],
        });
        roundtrip_request(Request::TxBegin {
            isolation: IsolationLevel::Serializable,
            read_only: true,
        });
        roundtrip_request(Request::TxCommit { tx_id: 42 });
        roundtrip_request(Request::TxRollback { tx_id: 7 });
        roundtrip_request(Request::Describe {
            target: "users".into(),
        });
        roundtrip_request(Request::List);
        roundtrip_request(Request::Stats { detailed: true });
        roundtrip_request(Request::UseNamespace {
            namespace: "default".into(),
        });
        roundtrip_request(Request::CreateNamespace {
            name: "analytics".into(),
            description: Some("desc".into()),
        });
        roundtrip_request(Request::CreateNamespace {
            name: "empty".into(),
            description: None,
        });
        roundtrip_request(Request::DropNamespace {
            name: "analytics".into(),
            force: true,
        });
        roundtrip_request(Request::ListNamespaces);
    }

    #[test]
    fn test_encoder_correlation_id() {
        let encoder = ProtocolEncoder::new();
        let first = encoder.next_correlation_id();
        let second = encoder.next_correlation_id();
        assert_eq!(second, first + 1);

        let req = Request::Ping;
        let encoded = encoder.encode_request(&req).unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let (_decoded, corr_id) = ProtocolDecoder::new()
            .decode_request(&mut buf)
            .unwrap()
            .unwrap();
        assert_eq!(corr_id, second + 1);
    }

    #[test]
    fn test_response_roundtrip_variants() {
        let mut permissions = PermissionSet::new();
        permissions.add(Permission::new(Resource::Cluster, Action::All));

        roundtrip_response(Response::Welcome {
            server_version: "0.1.0".into(),
            server_capabilities: vec!["transactions".into()],
            server_timestamp: 123,
        });
        roundtrip_response(Response::AuthSuccess {
            session_id: 1,
            user_id: "user".into(),
            permissions: permissions.clone(),
            expires_at: Some(999),
            token: Some("test-token-123".into()),
        });
        roundtrip_response(Response::AuthFailed {
            reason: "bad creds".into(),
            retry_after: Some(3),
        });
        roundtrip_response(Response::Pong { timestamp: 10 });
        roundtrip_response(Response::BatchResults {
            results: vec![
                QueryOutcome::Executed,
                QueryOutcome::Inserted {
                    rows_inserted: 1,
                    generated_ids: None,
                },
            ],
            elapsed_ms: 5,
        });
        roundtrip_response(Response::TxStarted {
            tx_id: 1,
            read_timestamp: 2,
        });
        roundtrip_response(Response::TxCommitted {
            tx_id: 1,
            commit_timestamp: 2,
        });
        roundtrip_response(Response::TxRolledBack { tx_id: 9 });
        roundtrip_response(Response::TableList {
            tables: vec![TableInfo {
                name: "users".into(),
                schema: Some("relational".into()),
                row_count: Some(10),
                size_bytes: Some(2048),
            }],
            namespaces: vec!["default".into()],
        });
        roundtrip_response(Response::TableDescription {
            schema: TableSchema {
                name: "users".into(),
                columns: vec![ColumnInfo {
                    name: "id".into(),
                    data_type: "int64".into(),
                    nullable: false,
                    default_value: Some(Value::Int32(1)),
                }],
                indexes: vec![IndexInfo {
                    name: "idx_users_id".into(),
                    columns: vec!["id".into()],
                    unique: true,
                }],
            },
        });
        roundtrip_response(Response::StatsResult {
            stats: ServerStats {
                uptime_seconds: 1,
                active_connections: 2,
                total_queries: 3,
                cache_hit_rate: 0.5,
                storage: Some(StorageStats {
                    total_size_bytes: 10,
                    document_count: 2,
                    table_count: 1,
                    keyspace_entries: 4,
                }),
            },
        });
        roundtrip_response(Response::StatsResult {
            stats: ServerStats {
                uptime_seconds: 1,
                active_connections: 2,
                total_queries: 3,
                cache_hit_rate: 0.5,
                storage: None,
            },
        });
        roundtrip_response(Response::Ok);
        roundtrip_response(Response::NamespaceList {
            namespaces: vec![NamespaceInfo {
                name: "default".into(),
                description: Some("desc".into()),
                table_count: 2,
                size_bytes: Some(100),
            }],
        });
        roundtrip_response(Response::NamespaceSwitched {
            namespace: "analytics".into(),
        });
        roundtrip_response(Response::NamespaceCreated {
            namespace: "analytics".into(),
        });
        roundtrip_response(Response::NamespaceDropped {
            namespace: "analytics".into(),
        });

        roundtrip_response(Response::Error {
            code: 1234,
            message: "oops".into(),
            details: None,
        });
    }

    #[test]
    fn test_query_outcome_roundtrip() {
        let outcomes = vec![
            QueryOutcome::Rows {
                data: vec![Value::Int32(1)],
                row_count: 1,
                columns: Some(vec!["id".into()]),
                has_more: true,
            },
            QueryOutcome::Inserted {
                rows_inserted: 2,
                generated_ids: Some(vec![Value::Int32(1), Value::Int32(2)]),
            },
            QueryOutcome::Updated { rows_updated: 3 },
            QueryOutcome::Deleted { rows_deleted: 4 },
            QueryOutcome::Created {
                object_type: "table".into(),
                object_name: "users".into(),
            },
            QueryOutcome::Dropped {
                object_type: "table".into(),
                object_name: "users".into(),
            },
            QueryOutcome::Executed,
        ];

        for outcome in outcomes {
            let mut buf = BytesMut::new();
            encode_query_outcome(&mut buf, &outcome);
            let mut cursor = &buf[..];
            let decoded = decode_query_outcome(&mut cursor).unwrap();
            assert_eq!(decoded, outcome);
        }
    }

    #[test]
    fn test_error_code_mappings() {
        assert_eq!(ErrorCode::ParseError.to_u16(), 1001);
        assert_eq!(ErrorCode::ParseError.as_str(), "parse_error");
        assert_eq!(ErrorCode::from(1001u16), ErrorCode::ParseError);
        assert_eq!(ErrorCode::from(9998u16), ErrorCode::InternalError);
        assert_eq!(
            ErrorCode::from(&MonoError::NotFound("x".into())),
            ErrorCode::NotFound
        );
        assert_eq!(
            ErrorCode::from(&MonoError::ChecksumMismatch {
                expected: 1,
                actual: 2
            }),
            ErrorCode::ChecksumError
        );
        assert_eq!(
            ErrorCode::from(&MonoError::WriteConflict("x".into())),
            ErrorCode::WriteConflict
        );
        assert_eq!(format!("{}", ErrorCode::ParseError), "parse_error (1001)");
    }

    #[test]
    fn test_primitive_helpers_roundtrip() {
        let mut buf = BytesMut::new();
        put_string(&mut buf, "hello");
        let mut cursor = &buf[..];
        assert_eq!(get_string(&mut cursor).unwrap(), "hello");

        let mut buf = BytesMut::new();
        put_opt_string(&mut buf, &Some("a".into()));
        let mut cursor = &buf[..];
        assert_eq!(get_opt_string(&mut cursor).unwrap(), Some("a".into()));

        let mut buf = BytesMut::new();
        put_opt_string(&mut buf, &None);
        let mut cursor = &buf[..];
        assert_eq!(get_opt_string(&mut cursor).unwrap(), None);

        let mut buf = BytesMut::new();
        put_opt_u64(&mut buf, &Some(42));
        let mut cursor = &buf[..];
        assert_eq!(get_opt_u64(&mut cursor).unwrap(), Some(42));

        let mut buf = BytesMut::new();
        put_opt_u64(&mut buf, &None);
        let mut cursor = &buf[..];
        assert_eq!(get_opt_u64(&mut cursor).unwrap(), None);

        let values = vec![Value::Int32(1), Value::String("x".into())];
        let mut buf = BytesMut::new();
        put_value_array(&mut buf, &values);
        let mut cursor = &buf[..];
        assert_eq!(get_value_array(&mut cursor).unwrap(), values);

        let mut buf = BytesMut::new();
        put_opt_value(&mut buf, &Some(Value::Int32(7)));
        let mut cursor = &buf[..];
        assert_eq!(get_opt_value(&mut cursor).unwrap(), Some(Value::Int32(7)));

        let mut buf = BytesMut::new();
        put_opt_value(&mut buf, &None);
        let mut cursor = &buf[..];
        assert_eq!(get_opt_value(&mut cursor).unwrap(), None);

        let mut buf = BytesMut::new();
        put_string_array(&mut buf, &["a".to_string(), "b".to_string()]);
        let mut cursor = &buf[..];
        assert_eq!(get_string_array(&mut cursor).unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn test_primitive_helpers_errors() {
        let mut cursor: &[u8] = &[];
        let err = get_u8(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Network(_)));

        let mut cursor: &[u8] = &[1, 0, 0];
        let err = get_u32_le(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Network(_)));

        let mut cursor: &[u8] = &[1, 0, 0, 0];
        let err = get_string(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Network(_)));

        let mut cursor: &[u8] = &[2, 0, 0, 0, 0xff, 0xff];
        let err = get_string(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Network(_)));

        let mut cursor: &[u8] = &[1, 4, 0, 0, 0, 1, 2];
        let err = get_opt_value(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Network(_)));

        let mut cursor: &[u8] = &[1, 0, 0, 0, 5, 0, 0, 0];
        let err = get_value_array(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Network(_)));

        let mut cursor: &[u8] = &[2, 0, 0, 0, 0xff, 0xff];
        let err = get_string(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Network(_)));

        let mut cursor: &[u8] = &[1, 1, 0, 0, 0, 0xff];
        let err = get_opt_value(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));
    }

    #[test]
    fn test_decoder_error_paths() {
        let encoder = ProtocolEncoder::new();
        let req = Request::Ping;
        let mut bytes = encoder
            .encode_request_with_correlation(&req, 1)
            .unwrap()
            .to_vec();
        bytes[4] = VERSION + 1;
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            ProtocolDecoder::new().decode_request(&mut buf).unwrap_err(),
            MonoError::Network(_)
        ));

        let mut bytes = encoder
            .encode_request_with_correlation(&req, 1)
            .unwrap()
            .to_vec();
        bytes[5] = cmd::KIND_RESPONSE;
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            ProtocolDecoder::new().decode_request(&mut buf).unwrap_err(),
            MonoError::Network(_)
        ));

        let mut bytes = encoder
            .encode_request_with_correlation(&req, 1)
            .unwrap()
            .to_vec();
        bytes[6] = 0xFF;
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            ProtocolDecoder::new().decode_request(&mut buf).unwrap_err(),
            MonoError::Network(_)
        ));

        let req = Request::Authenticate {
            method: AuthMethod::Certificate,
        };
        let mut bytes = encoder
            .encode_request_with_correlation(&req, 1)
            .unwrap()
            .to_vec();
        bytes[12] = 0xFF;
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            ProtocolDecoder::new().decode_request(&mut buf).unwrap_err(),
            MonoError::Network(_)
        ));

        let req = Request::TxBegin {
            isolation: IsolationLevel::ReadCommitted,
            read_only: false,
        };
        let mut bytes = encoder
            .encode_request_with_correlation(&req, 1)
            .unwrap()
            .to_vec();
        bytes[12] = 0xFF;
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            ProtocolDecoder::new().decode_request(&mut buf).unwrap_err(),
            MonoError::Network(_)
        ));

        let resp = Response::Ok;
        let mut bytes = encoder.encode_response(&resp, 1).unwrap().to_vec();
        bytes[4] = VERSION + 1;
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            ProtocolDecoder::new()
                .decode_response(&mut buf)
                .unwrap_err(),
            MonoError::Network(_)
        ));

        let mut bytes = encoder.encode_response(&resp, 1).unwrap().to_vec();
        bytes[5] = cmd::KIND_REQUEST;
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            ProtocolDecoder::new()
                .decode_response(&mut buf)
                .unwrap_err(),
            MonoError::Network(_)
        ));

        let mut bytes = encoder.encode_response(&resp, 1).unwrap().to_vec();
        bytes[6] = 0xFF;
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            ProtocolDecoder::new()
                .decode_response(&mut buf)
                .unwrap_err(),
            MonoError::Network(_)
        ));
    }

    #[test]
    fn test_incomplete_response_frame() {
        let encoder = ProtocolEncoder::new();
        let resp = Response::Ok;
        let encoded = encoder.encode_response(&resp, 1).unwrap();
        let mut buf = BytesMut::from(&encoded[..6]);
        assert!(
            ProtocolDecoder::new()
                .decode_response(&mut buf)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_incomplete_request_header() {
        let mut buf = BytesMut::from(&[1u8, 2, 3][..]);
        assert!(
            ProtocolDecoder::new()
                .decode_request(&mut buf)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_error_code_more_mappings() {
        assert_eq!(
            ErrorCode::from(&MonoError::Config("cfg".into())),
            ErrorCode::ConfigError
        );
        assert_eq!(
            ErrorCode::from(&MonoError::Transaction("tx".into())),
            ErrorCode::TransactionError
        );
        assert_eq!(
            ErrorCode::from(&MonoError::AlreadyExists("x".into())),
            ErrorCode::AlreadyExists
        );
    }

    #[test]
    fn test_decode_query_outcome_unknown_tag() {
        let mut cursor: &[u8] = &[0xFF];
        let err = decode_query_outcome(&mut cursor).unwrap_err();
        assert!(matches!(err, MonoError::Network(_)));
    }

    #[test]
    fn test_table_list_legacy_without_namespaces() {
        let mut body = BytesMut::new();
        body.put_u32_le(1);
        put_string(&mut body, "users");
        put_opt_string(&mut body, &Some("relational".into()));
        put_opt_u64(&mut body, &Some(10));
        put_opt_u64(&mut body, &Some(64));

        let mut header = BytesMut::with_capacity(8);
        header.put_u8(VERSION);
        header.put_u8(cmd::KIND_RESPONSE);
        header.put_u8(cmd::RSP_TABLE_LIST);
        header.put_u8(0);
        header.put_u32_le(5);

        let frame_len = header.len() + body.len();
        let mut buf = BytesMut::new();
        buf.put_u32_le(frame_len as u32);
        buf.put_slice(&header);
        buf.put_slice(&body);

        let mut buf = BytesMut::from(&buf[..]);
        let (resp, _corr_id) = ProtocolDecoder::new()
            .decode_response(&mut buf)
            .unwrap()
            .unwrap();
        match resp {
            Response::TableList { tables, namespaces } => {
                assert_eq!(tables.len(), 1);
                assert!(namespaces.is_empty());
            }
            other => panic!("unexpected response: {:?}", other),
        }
    }
}

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
    TableList { tables: Vec<TableInfo> },

    /// Table schema description
    TableDescription { schema: TableSchema },

    /// Server statistics
    StatsResult { stats: ServerStats },

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
            } => {
                header.put_u8(cmd::RSP_AUTH_SUCCESS);
                body.put_u64_le(*session_id);
                put_string(&mut body, user_id);
                permissions.encode(&mut body);
                put_opt_u64(&mut body, expires_at);
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
            Response::TableList { tables } => {
                header.put_u8(cmd::RSP_TABLE_LIST);
                body.put_u32_le(tables.len() as u32);
                for table in tables {
                    put_string(&mut body, &table.name);
                    put_opt_string(&mut body, &table.schema);
                    put_opt_u64(&mut body, &table.row_count);
                    put_opt_u64(&mut body, &table.size_bytes);
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
                Response::AuthSuccess {
                    session_id,
                    user_id,
                    permissions,
                    expires_at,
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
                Response::TableList { tables }
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
}

//! Protocol definitions and codec for MonoDB
//!
//! This module defines the request and response messages used in the MonoDB protocol, as well as
//! a codec for encoding and decoding these messages for transmission over a network.

use std::fmt;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::{MonoError, Value};

const VERSION: u8 = 1;

/// Wire protocol messages
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Request {
    /// Connect to server
    Connect {
        protocol_version: u8,
        #[serde(default)]
        auth_token: Option<String>,
    },

    /// Execute a query
    Execute {
        query: String,
        params: Vec<Value>,
        #[serde(default)]
        snapshot_timestamp: Option<u64>,
        #[serde(default)]
        user_id: Option<String>,
    },

    /// List tables
    List {},

    /// Begin a transaction
    BeginTx {
        isolation: IsolationLevel,
        #[serde(default)]
        user_id: Option<String>,
        #[serde(default)]
        read_timestamp: Option<u64>,
    },

    /// Commit transaction
    CommitTx { tx_id: u64 },

    /// Rollback transaction
    RollbackTx { tx_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// Connection acknowledgment
    ConnectAck {
        protocol_version: u8,
        #[serde(default)]
        server_timestamp: Option<u64>,
        #[serde(default)]
        user_permissions: Option<Vec<String>>,
    },

    /// Successful response
    Success {
        result: Vec<ExecutionResult>,
    },

    /// Error response with code and message
    Error {
        code: ErrorCode,
        message: String,
    },

    // Stream response (TODO)
    Stream,

    // Acknowledgment response
    Ack,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionResult {
    Ok {
        data: Value,
        time: u64,
        #[serde(default)]
        commit_timestamp: Option<u64>,
        #[serde(default)]
        time_elapsed: Option<u64>,
        #[serde(default)]
        row_count: Option<u64>,
    },
    Created {
        time: u64,
        #[serde(default)]
        commit_timestamp: Option<u64>,
    },
    Modified {
        time: u64,
        #[serde(default)]
        commit_timestamp: Option<u64>,
        #[serde(default)]
        rows_affected: Option<u64>,
    },
}

impl fmt::Display for ExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionResult::Ok {
                data,
                time,
                commit_timestamp,
                time_elapsed,
                row_count,
            } => {
                write!(
                    f,
                    "OK | data: {:?}, time: {}, commit_timestamp: {:?}, time_elapsed: {:?}, row_count: {:?}",
                    data, time, commit_timestamp, time_elapsed, row_count
                )
            }
            ExecutionResult::Created {
                time,
                commit_timestamp,
            } => {
                write!(
                    f,
                    "CREATED | time: {}, commit_timestamp: {:?}",
                    time, commit_timestamp
                )
            }
            ExecutionResult::Modified {
                time,
                commit_timestamp,
                rows_affected,
            } => {
                write!(
                    f,
                    "MODIFIED | time: {}, commit_timestamp: {:?}, rows_affected: {:?}",
                    time, commit_timestamp, rows_affected
                )
            }
        }
    }
}

/// Error codes for responses
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ErrorCode {
    ParseError = 1001,
    ExecutionError = 1002,
    NotFound = 1003,
    AlreadyExists = 1004,
    TypeError = 1005,
    TransactionError = 1006,
    NetworkError = 1007,
    InternalError = 9999,
}

/// Transaction isolation levels
#[derive(Debug, Clone, PartialEq, Copy, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

pub struct ProtocolCodec;

impl ProtocolCodec {
    /// Encode a request into bytes
    ///
    /// Format: [length (4 bytes)][bincode serialized request]
    ///
    /// # Arguments
    /// * `req` - The request to encode
    ///
    /// # Returns
    /// A `Result` containing the encoded bytes or an error
    ///
    /// # Example
    /// ```rust
    /// use monodb_common::protocol::{ProtocolCodec, Request};
    ///
    /// let req = Request::Execute {
    ///     query: "select users".into(),
    ///     params: vec![],
    ///     snapshot_timestamp: None,
    ///     user_id: None,
    /// };
    /// let encoded = ProtocolCodec::encode_request(&req).unwrap();
    /// ```
    pub fn encode_request(req: &Request) -> crate::Result<Bytes> {
        let mut header = BytesMut::new();
        let mut body = BytesMut::new();
        put_u8(&mut header, VERSION);
        put_u8(&mut header, 0x00); // 0 = Request

        match req {
            Request::Connect { protocol_version, auth_token } => {
                put_u8(&mut header, 0x01);
                put_u8(&mut body, *protocol_version); // protocol_version
                put_opt_string(&mut body, auth_token); // auth_token
            },
            Request::Execute { query, params, snapshot_timestamp, user_id } => {
                put_u8(&mut header, 0x02);
                put_string(&mut body, &query);
                put_u32(&mut body, params.len() as u32);
                for param in params {
                    put_bytes(&mut body, &param.to_bytes().as_slice());
                }
                put_opt_u64(&mut body, snapshot_timestamp);
                put_opt_string(&mut body, user_id);
            },
            Request::List { } => {
                put_u8(&mut header, 0x03);
            },
            Request::BeginTx { isolation, user_id, read_timestamp } => {
                put_u8(&mut header, 0x04);
                match isolation {
                    IsolationLevel::ReadUncommitted => put_u8(&mut body, 0x01),
                    IsolationLevel::ReadCommitted => put_u8(&mut body, 0x02),
                    IsolationLevel::RepeatableRead => put_u8(&mut body, 0x03),
                    IsolationLevel::Serializable => put_u8(&mut body, 0x04),
                }
                put_opt_string(&mut body, user_id);
                put_opt_u64(&mut body, read_timestamp);
            },
            Request::CommitTx { tx_id } => {
                put_u8(&mut header, 0x05);
                put_u64(&mut body, *tx_id);
            },
            Request::RollbackTx { tx_id } => {
                put_u8(&mut header, 0x06);
                put_u64(&mut body, *tx_id);
            }
        }

        put_u8(&mut header, 0); // Empty flags for now
        put_u32(&mut header, 0); // 0 for correlation id for now

        let mut buf = BytesMut::with_capacity(4 + header.len() + body.len());
        buf.put_u32((header.len() + body.len()) as u32);
        buf.put_slice(&header);
        buf.put_slice(&body);

        Ok(buf.freeze())
    }

    /// Decode a request from bytes
    ///
    /// Format: [length (4 bytes)][bincode serialized request]
    ///
    /// # Arguments
    /// * `buf` - The buffer containing the bytes to decode
    ///
    /// # Returns
    /// A `Result` containing an Option with the decoded request or None if not enough data is available
    /// or an error if decoding fails
    pub fn decode_request(buf: &mut BytesMut) -> crate::Result<Option<Request>> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let len = (&buf[..4]).get_u32() as usize;

        if buf.len() < 4 + len {
            return Ok(None);
        }

        buf.advance(4);
        let data = buf.split_to(len);

        let req = bincode::deserialize(&data)
            .map_err(|e| MonoError::Network(format!("Decode error: {e}")))?;

        Ok(Some(req))
    }

    /// Encode a response into bytes
    ///
    /// Format: [length (4 bytes)][bincode serialized response]
    ///
    /// # Arguments
    /// * `resp` - The response to encode
    ///
    /// # Returns
    /// A `Result` containing the encoded bytes or an error
    pub fn encode_response(resp: &Response) -> crate::Result<Bytes> {
        let data = bincode::serialize(resp)
            .map_err(|e| MonoError::Network(format!("Encode error: {e}")))?;

        let mut buf = BytesMut::with_capacity(4 + data.len());
        buf.put_u32(data.len() as u32);
        buf.put_slice(&data);

        Ok(buf.freeze())
    }

    /// Decode a response from bytes
    ///
    /// Format: [length (4 bytes)][bincode serialized response]
    ///
    /// # Arguments
    /// * `buf` - The buffer containing the bytes to decode
    ///
    /// # Returns
    /// A `Result` containing an Option with the decoded response or None if not enough data is available
    /// or an error if decoding fails
    pub fn decode_response(buf: &mut BytesMut) -> crate::Result<Option<Response>> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let len = (&buf[..4]).get_u32() as usize;

        if buf.len() < 4 + len {
            return Ok(None);
        }

        buf.advance(4);
        let data = buf.split_to(len);

        let resp = bincode::deserialize(&data)
            .map_err(|e| MonoError::Network(format!("Decode error: {e}")))?;

        Ok(Some(resp))
    }
}

// Primitive helpers

fn put_u8(buf: &mut BytesMut, v: u8) { buf.put_u8(v); }
fn put_u16(buf: &mut BytesMut, v: u16) { buf.put_u16_le(v); }
fn put_u32(buf: &mut BytesMut, v: u32) { buf.put_u32_le(v); }
fn put_u64(buf: &mut BytesMut, v: u64) { buf.put_u64_le(v); }
fn put_i32(buf: &mut BytesMut, v: i32) { buf.put_i32_le(v); }
fn put_i64(buf: &mut BytesMut, v: i64) { buf.put_i64_le(v); }

fn put_string(buf: &mut BytesMut, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.put_slice(s.as_bytes());
}

fn put_bytes(buf: &mut BytesMut, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.put_slice(b);
}

fn put_opt_u64(buf: &mut BytesMut, v: &Option<u64>) {
    match v {
        None => put_u8(buf, 0),
        Some(x) => { put_u8(buf, 1); put_u64(buf, *x); }
    }
}

fn put_opt_string(buf: &mut BytesMut, v: &Option<String>) {
    match v {
        None => put_u8(buf, 0),
        Some(s) => { put_u8(buf, 1); put_string(buf, s); }
    }
}

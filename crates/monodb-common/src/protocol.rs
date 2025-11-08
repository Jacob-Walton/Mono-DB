//! Protocol definitions and codec for MonoDB
//!
//! This module defines the request and response messages used in the MonoDB protocol, as well as
//! a codec for encoding and decoding these messages for transmission over a network.

use std::fmt;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::{MonoError, Value};

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
        let data = bincode::serialize(req)
            .map_err(|e| MonoError::Network(format!("Encode error: {e}")))?;

        let mut buf = BytesMut::with_capacity(4 + data.len());
        buf.put_u32(data.len() as u32);
        buf.put_slice(&data);

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

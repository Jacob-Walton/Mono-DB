//! Protocol definitions and codec for MonoDB
//!
//! This module defines the request and response messages used in the MonoDB protocol, as well as
//! a codec for encoding and decoding these messages for transmission over a network.
//!
//! TODO: Split encoder and decoder into separate structs and allow them to become instances with states
//! for things like correlation ids.

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
        data: Vec<Value>, // Either Value::Row, Value::Object, Value::Array, or Value::Null
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
#[repr(u16)]
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

impl From<u16> for ErrorCode {
    fn from(code: u16) -> Self {
        match code {
            1001 => ErrorCode::ParseError,
            1002 => ErrorCode::ExecutionError,
            1003 => ErrorCode::NotFound,
            1004 => ErrorCode::AlreadyExists,
            1005 => ErrorCode::TypeError,
            1006 => ErrorCode::TransactionError,
            1007 => ErrorCode::NetworkError,
            _ => ErrorCode::InternalError,
        }
    }
}

/// Transaction isolation levels
#[repr(u8)]
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
            Request::Connect {
                protocol_version,
                auth_token,
            } => {
                put_u8(&mut header, 0x01);
                put_u8(&mut body, *protocol_version); // protocol_version
                put_opt_string(&mut body, auth_token); // auth_token
            }
            Request::Execute {
                query,
                params,
                snapshot_timestamp,
                user_id,
            } => {
                put_u8(&mut header, 0x02);
                put_string(&mut body, query);
                put_u32(&mut body, params.len() as u32);
                for param in params {
                    put_bytes(&mut body, param.to_bytes().as_slice());
                }
                put_opt_u64(&mut body, snapshot_timestamp);
                put_opt_string(&mut body, user_id);
            }
            Request::List {} => {
                put_u8(&mut header, 0x03);
            }
            Request::BeginTx {
                isolation,
                user_id,
                read_timestamp,
            } => {
                put_u8(&mut header, 0x04);
                match isolation {
                    IsolationLevel::ReadUncommitted => put_u8(&mut body, 0x01),
                    IsolationLevel::ReadCommitted => put_u8(&mut body, 0x02),
                    IsolationLevel::RepeatableRead => put_u8(&mut body, 0x03),
                    IsolationLevel::Serializable => put_u8(&mut body, 0x04),
                }
                put_opt_string(&mut body, user_id);
                put_opt_u64(&mut body, read_timestamp);
            }
            Request::CommitTx { tx_id } => {
                put_u8(&mut header, 0x05);
                put_u64(&mut body, *tx_id);
            }
            Request::RollbackTx { tx_id } => {
                put_u8(&mut header, 0x06);
                put_u64(&mut body, *tx_id);
            }
        }

        put_u8(&mut header, 0); // Empty flags for now
        put_u32(&mut header, 0); // 0 for correlation id for now

        let mut buf = BytesMut::with_capacity(4 + header.len() + body.len());
        buf.put_u32_le((header.len() + body.len()) as u32); // Make sure this is _le too
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

        // Get header
        let len = (&buf[..4]).get_u32_le() as usize;

        #[cfg(debug_assertions)]
        tracing::debug!("decode_request: frame_len={}, buf.len()={}", len, buf.len());

        if buf.len() < 4 + len {
            return Ok(None);
        }
        buf.advance(4);
        let data = buf.split_to(len);
        let mut data_buf = BytesMut::from(&data[..]);

        // Parse header - CORRECTED ORDER
        let version = data_buf.get_u8();
        let kind = data_buf.get_u8();
        let command = data_buf.get_u8();
        let _flags = data_buf.get_u8();
        let _correlation_id = data_buf.get_u32_le();

        #[cfg(debug_assertions)]
        tracing::debug!(
            "decode_request: version={}, kind={}, command={}, remaining={}",
            version,
            kind,
            command,
            data_buf.len()
        );

        if version != VERSION {
            return Err(MonoError::Network(format!(
                "Unsupported protocol version: {version}"
            )));
        }
        if kind != 0x00 {
            return Err(MonoError::Network(format!(
                "Invalid message type for request: {kind}"
            )));
        }

        let req = match command {
            // Connect
            0x01 => {
                let protocol_version = data_buf.get_u8();
                let auth_token = if data_buf.get_u8() == 1 {
                    let str_len = data_buf.get_u32_le() as usize;
                    let s = String::from_utf8(data_buf.split_to(str_len).to_vec())
                        .map_err(|e| MonoError::Network(format!("Decode error: {e}")))?;
                    Some(s)
                } else {
                    None
                };
                Request::Connect {
                    protocol_version,
                    auth_token,
                }
            }
            // Execute
            0x02 => {
                let query_len = data_buf.get_u32_le() as usize;
                let query = String::from_utf8(data_buf.split_to(query_len).to_vec())
                    .map_err(|e| MonoError::Network(format!("Decode error: {e}")))?;
                let params_len = data_buf.get_u32_le() as usize;
                let mut params = Vec::with_capacity(params_len);
                for _ in 0..params_len {
                    let param_len = data_buf.get_u32_le() as usize;
                    let param_bytes = data_buf.split_to(param_len).to_vec();
                    let param = Value::from_bytes(&param_bytes)
                        .map_err(|e| MonoError::Network(format!("Decode error: {e}")))?;
                    params.push(param.0);
                }
                let snapshot_timestamp = if data_buf.get_u8() == 1 {
                    Some(data_buf.get_u64_le())
                } else {
                    None
                };
                let user_id = if data_buf.get_u8() == 1 {
                    let str_len = data_buf.get_u32_le() as usize;
                    let s = String::from_utf8(data_buf.split_to(str_len).to_vec())
                        .map_err(|e| MonoError::Network(format!("Decode error: {e}")))?;
                    Some(s)
                } else {
                    None
                };
                Request::Execute {
                    query,
                    params,
                    snapshot_timestamp,
                    user_id,
                }
            }
            // List
            0x03 => Request::List {},
            // BeginTx
            0x04 => {
                let isolation_byte = data_buf.get_u8();
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
                let user_id = get_opt_string(&mut data_buf)?;
                let read_timestamp = get_opt_u64(&mut data_buf);

                Request::BeginTx {
                    isolation,
                    user_id,
                    read_timestamp,
                }
            }
            // CommitTx
            0x05 => Request::CommitTx {
                tx_id: data_buf.get_u64_le(),
            },
            // RollbackTx
            0x06 => Request::RollbackTx {
                tx_id: data_buf.get_u64_le(),
            },
            _ => return Err(MonoError::Network(format!("Unknown command: {command}"))),
        };

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
        let mut header = BytesMut::new();
        let mut body = BytesMut::new();
        put_u8(&mut header, VERSION);
        put_u8(&mut header, 0x01); // 1 = Response

        match resp {
            Response::ConnectAck {
                protocol_version,
                server_timestamp,
                user_permissions,
            } => {
                header.put_u8(0x01);
                body.put_u8(*protocol_version);
                put_opt_u64(&mut body, server_timestamp);

                if user_permissions.is_some() {
                    body.put_u8(1);
                    let unwrapped = user_permissions.as_ref().unwrap();
                    body.put_u32_le(unwrapped.len() as u32);
                    for item in unwrapped {
                        put_string(&mut body, item);
                    }
                } else {
                    body.put_u8(0);
                }
            }
            Response::Success { result } => {
                header.put_u8(0x02);

                body.put_u32_le(result.len() as u32);
                for res in result {
                    match res {
                        ExecutionResult::Ok {
                            data,
                            time,
                            time_elapsed,
                            commit_timestamp,
                            row_count,
                        } => {
                            const VALID_TYPES: [&str; 3] = ["array", "object", "row"];

                            body.put_u8(0); // Tag
                            body.put_u32_le(data.len() as u32);
                            for value in data {
                                if !VALID_TYPES.contains(&value.type_name()) {
                                    return Err(MonoError::Network(format!(
                                        "Invalid value type in ExecutionResult::Ok: {}",
                                        value.type_name()
                                    )));
                                }
                                let value_bytes = value.to_bytes();
                                body.put_u32_le(value_bytes.len() as u32);
                                body.put_slice(&value_bytes);
                            }
                            body.put_u64_le(*time);
                            put_opt_u64(&mut body, commit_timestamp);
                            put_opt_u64(&mut body, time_elapsed);
                            put_opt_u64(&mut body, row_count);
                        }
                        ExecutionResult::Created {
                            time,
                            commit_timestamp,
                        } => {
                            body.put_u8(1); // Tag
                            body.put_u64_le(*time);
                            // commit_timestamp is Option<u64>, not u64
                            put_opt_u64(&mut body, commit_timestamp);
                        }
                        ExecutionResult::Modified {
                            time,
                            commit_timestamp,
                            rows_affected,
                        } => {
                            body.put_u8(2); // Tag
                            body.put_u64_le(*time);
                            put_opt_u64(&mut body, commit_timestamp);
                            put_opt_u64(&mut body, rows_affected);
                        }
                    }
                }
            }
            Response::Error { code, message } => {
                header.put_u8(0x03);

                body.put_u16_le((*code) as u16);
                put_string(&mut body, message);
            }
            Response::Stream => {
                header.put_u8(0x04);
            }
            Response::Ack => {
                header.put_u8(0x05);
            }
        }

        put_u8(&mut header, 0); // Empty flags for now
        put_u32(&mut header, 0); // 0 for correlation id for now

        let mut buf = BytesMut::with_capacity(4 + header.len() + body.len());
        buf.put_u32_le((header.len() + body.len()) as u32); // Make sure this is _le too
        buf.put_slice(&header);
        buf.put_slice(&body);

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

        // Get header
        let len = (&buf[..4]).get_u32_le() as usize;
        if buf.len() < 4 + len {
            return Ok(None);
        }
        buf.advance(4);
        let data = buf.split_to(len);
        let mut data_buf = BytesMut::from(&data[..]);

        // Parse header - CORRECTED ORDER
        let version = data_buf.get_u8();
        if version != VERSION {
            return Err(MonoError::Network(format!(
                "Unsupported protocol version: {version}"
            )));
        }
        let kind = data_buf.get_u8(); // Read kind (was missing!)
        if kind != 0x01 {
            return Err(MonoError::Network(format!(
                "Invalid message type for response: {kind}"
            )));
        }
        let command = data_buf.get_u8(); // This is msg_type
        let _flags = data_buf.get_u8(); // Skip flags
        let _correlation_id = data_buf.get_u32_le(); // Skip correlation id

        let resp = match command {
            // ConnectAck
            0x01 => {
                let protocol_version = data_buf.get_u8();
                let server_timestamp = get_opt_u64(&mut data_buf);
                let permissions_present = data_buf.get_u8() == 1;
                let mut user_permissions: Option<Vec<String>> = None;
                if permissions_present {
                    user_permissions = Some(Vec::new());

                    let len = data_buf.get_u32_le() as usize;

                    for _ in 0..len {
                        let string = get_string(&mut data_buf)?;
                        user_permissions.as_mut().unwrap().push(string);
                    }
                }

                Response::ConnectAck {
                    protocol_version,
                    server_timestamp,
                    user_permissions,
                }
            }
            // Sucess
            0x02 => {
                let result_len = data_buf.get_u32_le() as usize;
                let mut result = Vec::with_capacity(result_len);
                for _ in 0..result_len {
                    let tag = data_buf.get_u8();
                    match tag {
                        0 => {
                            let len = data_buf.get_u32_le();
                            let mut data = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                let value_len = data_buf.get_u32_le() as usize;
                                let value_bytes = data_buf.split_to(value_len).to_vec();
                                let value = Value::from_bytes(&value_bytes).map_err(|e| {
                                    MonoError::Network(format!("Decode error: {e}"))
                                })?;
                                data.push(value.0);
                            }
                            let time = data_buf.get_u64_le();
                            let commit_timestamp = get_opt_u64(&mut data_buf);
                            let time_elapsed = get_opt_u64(&mut data_buf);
                            let row_count = get_opt_u64(&mut data_buf);

                            result.push(ExecutionResult::Ok {
                                data,
                                time,
                                commit_timestamp,
                                time_elapsed,
                                row_count,
                            });
                        }
                        1 => {
                            let time = data_buf.get_u64_le();
                            let commit_timestamp = get_opt_u64(&mut data_buf);

                            result.push(ExecutionResult::Created {
                                time,
                                commit_timestamp,
                            });
                        }
                        2 => {
                            let time = data_buf.get_u64_le();
                            let commit_timestamp = get_opt_u64(&mut data_buf);
                            let rows_affected = get_opt_u64(&mut data_buf);

                            result.push(ExecutionResult::Modified {
                                time,
                                commit_timestamp,
                                rows_affected,
                            });
                        }
                        _ => {
                            return Err(MonoError::Network(format!(
                                "Unknown execution result tag: {tag}"
                            )));
                        }
                    }
                }
                Response::Success { result }
            }
            // Error
            0x03 => {
                let code: ErrorCode = data_buf.get_u16_le().into();
                let message = get_string(&mut data_buf)?;

                Response::Error { code, message }
            }
            // Stream
            0x04 => Response::Stream {},
            // Ack
            0x05 => Response::Ack {},
            _ => return Err(MonoError::Network(format!("Unknown command: {command}"))),
        };

        Ok(Some(resp))
    }
}

// Primitive helpers

#[inline]
#[allow(unused)]
fn put_u8(buf: &mut BytesMut, v: u8) {
    buf.put_u8(v);
}
#[inline]
#[allow(unused)]
fn put_u16(buf: &mut BytesMut, v: u16) {
    buf.put_u16_le(v);
}
#[inline]
#[allow(unused)]
fn put_u32(buf: &mut BytesMut, v: u32) {
    buf.put_u32_le(v);
}
#[inline]
#[allow(unused)]
fn put_u64(buf: &mut BytesMut, v: u64) {
    buf.put_u64_le(v);
}
#[inline]
#[allow(unused)]
fn put_i32(buf: &mut BytesMut, v: i32) {
    buf.put_i32_le(v);
}
#[inline]
#[allow(unused)]
fn put_i64(buf: &mut BytesMut, v: i64) {
    buf.put_i64_le(v);
}

#[inline]
#[allow(unused)]
fn put_string(buf: &mut BytesMut, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.put_slice(s.as_bytes());
}

#[inline]
#[allow(unused)]
fn put_bytes(buf: &mut BytesMut, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.put_slice(b);
}

#[inline]
#[allow(unused)]
fn put_opt_u64(buf: &mut BytesMut, v: &Option<u64>) {
    match v {
        None => put_u8(buf, 0),
        Some(x) => {
            put_u8(buf, 1);
            put_u64(buf, *x);
        }
    }
}

#[inline]
#[allow(unused)]
fn put_opt_string(buf: &mut BytesMut, v: &Option<String>) {
    match v {
        None => put_u8(buf, 0),
        Some(s) => {
            put_u8(buf, 1);
            put_string(buf, s);
        }
    }
}

#[inline]
#[allow(unused)]
fn get_string(buf: &mut BytesMut) -> crate::Result<String> {
    let str_len = buf.get_u32_le() as usize;
    let s = String::from_utf8(buf.split_to(str_len).to_vec())
        .map_err(|e| MonoError::Network(format!("Decode error: {e}")))?;
    Ok(s)
}

#[inline]
#[allow(unused)]
fn get_opt_string(buf: &mut BytesMut) -> crate::Result<Option<String>> {
    let flag = buf.get_u8();
    if flag == 0 {
        Ok(None)
    } else {
        let str_len = buf.get_u32_le() as usize;
        let s = String::from_utf8(buf.split_to(str_len).to_vec())
            .map_err(|e| MonoError::Network(format!("Decode error: {e}")))?;
        Ok(Some(s))
    }
}

#[inline]
#[allow(unused)]
fn get_opt_u64(buf: &mut BytesMut) -> Option<u64> {
    let flag = buf.get_u8();
    if flag == 0 {
        None
    } else {
        Some(buf.get_u64_le())
    }
}

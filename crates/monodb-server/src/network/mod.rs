use std::{sync::Arc, time::UNIX_EPOCH};

use bytes::BytesMut;
use dashmap::DashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast,
    time::Instant,
};

use monodb_common::{
    MonoError, Result, Value,
    protocol::{ErrorCode, ExecutionResult, ProtocolCodec, Request, Response},
    schema::Schema,
};

use crate::{
    network::session::Session,
    query_engine::{executor::QueryExecutor, parser::parse},
    storage::engine::StorageEngine,
};

pub mod session;

static SERVER_PROTOCOL_VERSION: u8 = 1;

pub async fn handle_connection(
    mut stream: TcpStream,
    storage_engine: Arc<StorageEngine>,
    sessions: Arc<DashMap<u64, Session>>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    // Create a new session
    let session = Session::new();
    let session_id = session.id;
    sessions.insert(session_id, session);

    // Create executor once per session to persist transaction state
    let mut executor = QueryExecutor::new(storage_engine.clone());

    let mut buffer = BytesMut::new();

    loop {
        tokio::select! {
            read_result = stream.read_buf(&mut buffer) => {
                match read_result {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        tracing::debug!("Read {} bytes from connection", buffer.len());
                        // Try to decode requests from the buffer
                        while let Some(request) = ProtocolCodec::decode_request(&mut buffer)? {
                            #[cfg(debug_assertions)]
                            tracing::debug!("Received request: {:?}", request);

                            match handle_request(&mut executor, request, session_id).await {
                                Ok(response) => {
                                    let response_bytes = ProtocolCodec::encode_response(&response)?;
                                    if let Err(e) = stream.write_all(&response_bytes).await {
                                        tracing::error!("Failed to write response: {e}");
                                        return Err(MonoError::Network(format!("Write error: {e}")));
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Request handling error: {e}");
                                    let error_response = Response::Error {
                                        code: error_to_code(&e),
                                        message: e.to_string(),
                                    };
                                    let response_bytes = ProtocolCodec::encode_response(&error_response)?;
                                    if let Err(e) = stream.write_all(&response_bytes).await {
                                        tracing::error!("Failed to write error response: {e}");
                                        return Err(MonoError::Network(format!("Write error: {e}")));
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Connection read error: {e}");
                        break;
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("Server shutdown requested, notifying client and closing connection");

                // Send shutdown notification to client
                let shutdown_response = Response::Error {
                    code: ErrorCode::InternalError,
                    message: "Server is shutting down".to_string(),
                };

                if let Ok(response_bytes) = ProtocolCodec::encode_response(&shutdown_response) {
                    let _ = stream.write_all(&response_bytes).await;
                }

                break;
            }
        }
    }

    tracing::info!("Session {session_id} ended");
    sessions.remove(&session_id);

    Ok(())
}

async fn handle_request(
    executor: &mut QueryExecutor,
    request: Request,
    _session_id: u64,
) -> Result<Response> {
    let start = Instant::now();

    match request {
        Request::Connect {
            protocol_version, ..
        } => {
            if protocol_version != SERVER_PROTOCOL_VERSION {
                return Err(MonoError::Network(format!(
                    "Protocol version mismatch: client {}, server {}",
                    protocol_version, SERVER_PROTOCOL_VERSION
                )));
            }

            let response = Response::ConnectAck {
                protocol_version: SERVER_PROTOCOL_VERSION,
                server_timestamp: Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ),
                user_permissions: None,
            };

            #[cfg(debug_assertions)]
            tracing::debug!(
                "Handled Connect request in {} ns",
                start.elapsed().as_nanos()
            );

            Ok(response)
        }
        Request::Execute { query, .. } => {
            let _start = std::time::Instant::now();

            let parse_result = parse(query.to_string());

            match parse_result {
                Ok(statements) => {
                    let mut results = Vec::new();

                    for stmt in statements {
                        let exec_start = std::time::Instant::now();

                        match executor.execute(stmt).await {
                            Ok(exec_result) => {
                                // attach timing info if relevant
                                let current_time = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis()
                                    as u64;

                                let updated = match exec_result {
                                    ExecutionResult::Ok {
                                        data,
                                        commit_timestamp,
                                        row_count,
                                        ..
                                    } => ExecutionResult::Ok {
                                        data,
                                        time: current_time,
                                        commit_timestamp,
                                        time_elapsed: Some(exec_start.elapsed().as_millis() as u64),
                                        row_count,
                                    },
                                    ExecutionResult::Created {
                                        commit_timestamp, ..
                                    } => ExecutionResult::Created {
                                        time: current_time,
                                        commit_timestamp,
                                    },
                                    ExecutionResult::Modified {
                                        commit_timestamp,
                                        rows_affected,
                                        ..
                                    } => ExecutionResult::Modified {
                                        time: current_time,
                                        commit_timestamp,
                                        rows_affected,
                                    },
                                };

                                results.push(updated);
                            }
                            Err(err) => {
                                return Ok(Response::Error {
                                    code: ErrorCode::ExecutionError,
                                    message: err.to_string(),
                                });
                            }
                        }
                    }

                    Ok(Response::Success { result: results })
                }

                Err(errors) => {
                    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
                    Ok(Response::Error {
                        code: ErrorCode::ParseError,
                        message: messages.join("; "),
                    })
                }
            }
        }
        Request::BeginTx { .. } => todo!(),
        Request::CommitTx { .. } => todo!(),
        Request::RollbackTx { .. } => todo!(),
        Request::List { .. } => {
            let schemas = executor.storage().schemas();

            let tables: Vec<Value> = schemas
                .iter()
                .map(|schema| match &**schema {
                    Schema::Table { name, .. } => Value::Array(vec![
                        Value::String("table".to_string()),
                        Value::String(name.clone()),
                    ]),
                    Schema::Collection { name, .. } => Value::Array(vec![
                        Value::String("collection".to_string()),
                        Value::String(name.clone()),
                    ]),
                    Schema::KeySpace { name, .. } => Value::Array(vec![
                        Value::String("keyspace".to_string()),
                        Value::String(name.clone()),
                    ]),
                })
                .collect();

            let time = std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let row_count = Some(tables.len() as u64);

            Ok(Response::Success {
                result: vec![ExecutionResult::Ok {
                    data: vec![Value::Array(tables)],
                    time,
                    time_elapsed: None,
                    row_count,
                    commit_timestamp: None,
                }],
            })
        }
    }
}

fn error_to_code(error: &MonoError) -> ErrorCode {
    match error {
        MonoError::Parse(_) => ErrorCode::ParseError,
        MonoError::NotFound(_) => ErrorCode::NotFound,
        MonoError::AlreadyExists(_) => ErrorCode::AlreadyExists,
        MonoError::TypeError { .. } => ErrorCode::TypeError,
        MonoError::Transaction(_) => ErrorCode::TransactionError,
        MonoError::Network(_) => ErrorCode::NetworkError,
        MonoError::Storage(_) => ErrorCode::InternalError,
        MonoError::Io(_) => ErrorCode::InternalError,
        MonoError::Config(_) => ErrorCode::InternalError,
        MonoError::Execution(_) => ErrorCode::ExecutionError,
        MonoError::InvalidOperation(_) => ErrorCode::ExecutionError,
        MonoError::WriteConflict(_) => ErrorCode::TransactionError,
    }
}

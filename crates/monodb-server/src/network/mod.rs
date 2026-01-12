//! Network layer for MonoDB server.
//!
//! Handles client connections, protocol encoding/decoding, and request dispatch.

use bytes::BytesMut;
use monodb_common::{
    Result, Value,
    permissions::BuiltinRole,
    protocol::{ErrorCode, ProtocolDecoder, ProtocolEncoder, QueryOutcome, Request, Response},
};

use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::broadcast,
};

use crate::query_engine::ast::{ControlStatement, DdlStatement, MutationStatement, QueryStatement};
use crate::query_engine::planner::{EmptyCatalog, QueryPlanner};
use crate::query_engine::storage::StorageAdapter;
use crate::query_engine::{ExecutionContext, Executor, Statement, parse};
use crate::{network::session::Session, query_engine::QueryStorage};

pub mod session;

/// Handle a client connection.
pub async fn handle_connection<S>(
    mut stream: S,
    sessions: Arc<DashMap<u64, Session>>,
    storage: Arc<StorageAdapter>,
    mut shutdown_rx: broadcast::Receiver<()>,
    encoder: Arc<ProtocolEncoder>,
    decoder: Arc<ProtocolDecoder>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let session = Session::new("guest".to_string(), false);
    let session_id = session.id;
    sessions.insert(session_id, session);

    let mut buffer = BytesMut::new();

    loop {
        tokio::select! {
            read_result = stream.read_buf(&mut buffer) => {
                match read_result {
                    Ok(0) => {
                        // Client closed connection
                        break;
                    }
                    Ok(n) => {
                        #[cfg(debug_assertions)]
                        tracing::debug!("Received {n} bytes");

                        while let Some((request, correlation_id)) = decoder.decode_request(&mut buffer)? {
                            #[cfg(debug_assertions)]
                            tracing::debug!("Decoded request: {:?}", request);

                            let response = handle_request(
                                &sessions,
                                session_id,
                                &storage,
                                request
                            ).await?;

                            let encoded_response = encoder.encode_response(&response, correlation_id)?;

                            #[cfg(debug_assertions)]
                            tracing::debug!("Sending response: {:?}", response);

                            stream.write_all(&encoded_response).await?;
                            stream.flush().await?;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Read error: {e}");
                        break;
                    }
                }
            }

            _ = shutdown_rx.recv() => {
                tracing::info!("Shutdown requested, closing connection");
                let _ = stream.write_all(b"SERVER SHUTDOWN\n").await;
                break;
            }
        }
    }

    sessions.remove(&session_id);
    Ok(())
}

/// Handle a single request.
async fn handle_request(
    sessions: &Arc<DashMap<u64, Session>>,
    session_id: u64,
    storage: &Arc<StorageAdapter>,
    request: Request,
) -> Result<Response> {
    let start = Instant::now();

    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match request {
        Request::Hello {
            client_name,
            capabilities,
        } => {
            tracing::debug!("Hello from {client_name:?}, capabilities: {capabilities:?}");

            Ok(Response::Welcome {
                server_version: env!("CARGO_PKG_VERSION").to_string(),
                server_capabilities: vec!["transactions".into(), "queries".into()],
                server_timestamp: current_time,
            })
        }

        Request::Authenticate { method } => {
            tracing::debug!("Authenticate with method: {method:?}");

            let user_id = uuid::Uuid::new_v4().to_string();

            if let Some(mut sess) = sessions.get_mut(&session_id) {
                sess.authenticated = true;
                sess.user_id = user_id.clone();
            }

            let permissions = BuiltinRole::Root.permissions(Some("default"));
            Ok(Response::AuthSuccess {
                session_id,
                user_id,
                expires_at: None,
                permissions,
            })
        }

        Request::Ping => Ok(Response::Pong {
            timestamp: current_time,
        }),

        Request::Disconnect => {
            sessions.remove(&session_id);
            Ok(Response::Ok)
        }

        Request::Query { statement, params } => {
            handle_query(sessions, session_id, storage, &statement, params, start).await
        }

        Request::TxBegin {
            isolation: _,
            read_only,
        } => {
            let tx_id = if read_only {
                storage.begin_read_only()?
            } else {
                storage.begin_transaction()?
            };

            if let Some(mut sess) = sessions.get_mut(&session_id) {
                sess.begin_transaction(tx_id);
            }

            Ok(Response::TxStarted {
                tx_id,
                read_timestamp: current_time,
            })
        }

        Request::TxCommit { tx_id } => {
            storage.commit(tx_id)?;

            if let Some(mut sess) = sessions.get_mut(&session_id) {
                sess.end_transaction();
            }

            Ok(Response::TxCommitted {
                tx_id,
                commit_timestamp: current_time,
            })
        }

        Request::TxRollback { tx_id } => {
            storage.rollback(tx_id)?;

            if let Some(mut sess) = sessions.get_mut(&session_id) {
                sess.end_transaction();
            }

            Ok(Response::TxRolledBack { tx_id })
        }

        Request::UseNamespace { namespace } => {
            if let Some(mut sess) = sessions.get_mut(&session_id) {
                sess.use_namespace(namespace.clone());
            }

            Ok(Response::NamespaceSwitched { namespace })
        }

        Request::List => {
            // List tables, for now return empty. FIXME
            Ok(Response::TableList { tables: vec![] })
        }

        _ => {
            tracing::debug!("Unhandled request type");
            Ok(Response::Error {
                code: ErrorCode::InvalidOperation.to_u16(),
                message: "Request type not yet implemented".into(),
                details: None,
            })
        }
    }
}

/// Handle a query request.
async fn handle_query(
    sessions: &Arc<DashMap<u64, Session>>,
    session_id: u64,
    storage: &Arc<StorageAdapter>,
    query: &str,
    params: Vec<Value>,
    start: Instant,
) -> Result<Response> {
    // Parse the query
    let statements = match parse(query) {
        Ok(stmts) => stmts,
        Err(errors) => {
            let msg = errors.join("; ");
            return Ok(Response::Error {
                code: ErrorCode::ParseError.to_u16(),
                message: format!("Parse error: {}", msg),
                details: None,
            });
        }
    };

    if statements.is_empty() {
        return Ok(Response::QueryResult {
            result: QueryOutcome::Executed,
            elapsed_ms: start.elapsed().as_millis() as u64,
        });
    }

    // Get or create transaction
    let tx_id = {
        let sess = sessions.get(&session_id);
        match sess.as_ref().and_then(|s| s.transaction_id()) {
            Some(id) => id,
            None => {
                // Auto-transaction for single query
                storage.begin_transaction()?
            }
        }
    };

    let auto_tx = sessions
        .get(&session_id)
        .map(|s| s.transaction_id().is_none())
        .unwrap_or(true);

    // Execute each statement
    let executor = Executor::new(storage.clone());
    let ctx = ExecutionContext::new().with_params(params).with_tx(tx_id);

    let mut last_result = QueryOutcome::Executed;

    for stmt in statements {
        match execute_statement(&executor, &ctx, storage, stmt).await {
            Ok(outcome) => last_result = outcome,
            Err(e) => {
                if auto_tx {
                    let _ = storage.rollback(tx_id);
                }
                return Ok(Response::Error {
                    code: ErrorCode::ExecutionError.to_u16(),
                    message: e.to_string(),
                    details: None,
                });
            }
        }
    }

    // Auto-commit if we started the transaction
    if auto_tx {
        storage.commit(tx_id)?;
    }

    Ok(Response::QueryResult {
        result: last_result,
        elapsed_ms: start.elapsed().as_millis() as u64,
    })
}

/// Execute a single statement.
async fn execute_statement(
    executor: &Executor<StorageAdapter>,
    ctx: &ExecutionContext,
    storage: &Arc<StorageAdapter>,
    stmt: Statement,
) -> Result<QueryOutcome> {
    let catalog = Arc::new(EmptyCatalog);
    let planner = QueryPlanner::new(catalog);

    match &stmt {
        Statement::Query(query) => {
            // Plan and execute the query
            let plan = planner.plan(&stmt)?;
            let result = executor.execute(&plan, ctx)?;

            let data: Vec<Value> = result
                .rows
                .into_iter()
                .map(|row| row.into_value())
                .collect();

            // Check if it's a COUNT query
            let (row_count, columns) = match query {
                QueryStatement::Count(_) => (1, Some(vec!["count".into()])),
                _ => (data.len() as u64, None),
            };

            Ok(QueryOutcome::Rows {
                row_count,
                data,
                columns,
                has_more: false,
            })
        }

        Statement::Mutation(mutation) => {
            let plan = planner.plan(&stmt)?;
            let result = executor.execute(&plan, ctx)?;

            match mutation {
                MutationStatement::Put(_) => Ok(QueryOutcome::Inserted {
                    rows_inserted: result.rows_affected.max(1),
                    generated_ids: None,
                }),
                MutationStatement::Change(_) => Ok(QueryOutcome::Updated {
                    rows_updated: result.rows_affected,
                }),
                MutationStatement::Remove(_) => Ok(QueryOutcome::Deleted {
                    rows_deleted: result.rows_affected,
                }),
            }
        }

        Statement::Ddl(ddl) => match ddl {
            DdlStatement::CreateTable(create) => {
                storage.create_table(create.name.node.as_str())?;
                Ok(QueryOutcome::Executed)
            }
            DdlStatement::DropTable(drop) => {
                storage.drop_table(drop.name.node.as_str())?;
                Ok(QueryOutcome::Dropped {
                    object_type: "table".into(),
                    object_name: drop.name.node.to_string(),
                })
            }
            _ => Ok(QueryOutcome::Executed),
        },

        Statement::Transaction(_) => {
            // Transaction statements are handled at the request level
            Ok(QueryOutcome::Executed)
        }

        Statement::Control(ctrl) => {
            match ctrl {
                ControlStatement::Use(_use_stmt) => {
                    // Namespace switching is handled at request level
                    Ok(QueryOutcome::Executed)
                }
                _ => Ok(QueryOutcome::Executed),
            }
        }
    }
}

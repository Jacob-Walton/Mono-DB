//! Connection management for the database server
//!
//! Handles incoming client connections and manages connection state.

use crate::executor::ExecutionContext;
use crate::network::NetworkConnection;
use crate::network::protocol::{ClientMessage, ProtocolHandler, ServerMessage};
use crate::storage::Engine;
use crate::{MonoError, MonoResult};
use std::sync::Arc;
use std::sync::RwLock;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Connection manager
pub struct ConnectionManager {
    max_connections: Arc<Semaphore>,
}

impl ConnectionManager {
    /// Create a new connection manager
    pub fn new(max_connections: usize) -> Self {
        Self {
            max_connections: Arc::new(Semaphore::new(max_connections)),
        }
    }

    /// Handle a new client connection
    pub async fn handle_connection(
        &self,
        socket: TcpStream,
        engine: Arc<RwLock<Engine>>,
    ) -> MonoResult<()> {
        // Acquire a connection permit
        let _permit = self
            .max_connections
            .acquire()
            .await
            .map_err(|_| MonoError::ConnectionLimit)?;

        let connection = Connection::new(socket, engine).await?;
        connection.run().await
    }
}

/// Represents a single client connection
pub struct Connection {
    id: Uuid,
    network_conn: NetworkConnection,
    #[allow(unused)]
    engine: Arc<RwLock<Engine>>,
    protocol_handler: ProtocolHandler,
    execution_context: ExecutionContext,
    connection_timeout: Duration,
    shutdown_token: CancellationToken,
}

impl Connection {
    /// Create a new connection
    pub async fn new(socket: TcpStream, engine: Arc<RwLock<Engine>>) -> MonoResult<Self> {
        let network_conn = NetworkConnection::new(socket);
        let protocol_handler = ProtocolHandler::new();

        // Create execution context
        let _interner = {
            let interner_ref = protocol_handler.interner.read();
            (*interner_ref).clone()
        };

        // Use persistent storage
        let execution_context = ExecutionContext::new_with_persistent_storage(Arc::clone(&engine));

        Ok(Self {
            id: Uuid::new_v4(),
            network_conn,
            engine,
            protocol_handler,
            execution_context,
            connection_timeout: Duration::from_secs(300), // 5 minutes
            shutdown_token: CancellationToken::new(),
        })
    }

    /// Run the connection loop
    pub async fn run(mut self) -> MonoResult<()> {
        println!("Connection {} started", self.id);

        // Start heartbeat monitor
        let shutdown_token = self.shutdown_token.clone();
        let _connection_timeout = self.connection_timeout;
        let connection_id = self.id;

        tokio::spawn(async move {
            let mut heartbeat_interval = interval(Duration::from_secs(60)); // Check every minute

            loop {
                tokio::select! {
                    _ = heartbeat_interval.tick() => {
                        // TODO: Give access to the network connection to send a heartbeat
                    }
                    _ = shutdown_token.cancelled() => {
                        break;
                    }
                }
            }

            println!("Heartbeat monitor for connection {} stopped", connection_id);
        });

        loop {
            tokio::select! {
                message_result = self.network_conn.read_message() => {
                    match message_result {
                        Ok(Some(message)) => {
                            let response = self.handle_message(message).await;
                            if let Err(e) = self.network_conn.send_response(response).await {
                                eprintln!("Error sending response: {}", e);
                                break;
                            }
                        }
                        Ok(None) => {
                            // Client disconnected
                            break;
                        }
                        Err(e) => {
                            eprintln!("Error reading message: {}", e);
                            let error_response = ServerMessage::Error {
                                code: crate::network::protocol::ErrorCode::InternalError,
                                message: format!("Connection error: {}", e),
                                details: None,
                            };
                            let _ = self.network_conn.send_response(error_response).await;
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(self.connection_timeout) => {
                    if self.network_conn.is_idle(self.connection_timeout) {
                        println!("Connection {} timed out due to inactivity", self.id);
                        let timeout_response = ServerMessage::Error {
                            code: crate::network::protocol::ErrorCode::InternalError,
                            message: "Connection timed out due to inactivity".to_string(),
                            details: None,
                        };
                        let _ = self.network_conn.send_response(timeout_response).await;
                        break;
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    println!("Connection {} shutdown requested", self.id);
                    break;
                }
            }
        }

        self.shutdown_token.cancel();
        println!("Connection {} closed", self.id);
        Ok(())
    }

    /// Handle a client message
    async fn handle_message(&mut self, message: ClientMessage) -> ServerMessage {
        self.protocol_handler
            .handle_message(message, &mut self.execution_context)
    }
}

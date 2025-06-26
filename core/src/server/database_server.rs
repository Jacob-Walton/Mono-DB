//! Database server implementation
//!
//! The main database server that handles client connections and manages
//! the overall database state.

use crate::server::ConnectionManager;
use crate::storage::Engine;
use crate::{Config, MonoError, MonoResult};
use std::sync::Arc;
use std::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// Main database server
pub struct DatabaseServer {
	config: Config,
	engine: Arc<RwLock<Engine>>,
	connection_manager: Arc<ConnectionManager>,
	shutdown_token: CancellationToken,
}

impl DatabaseServer {
	/// Create a new database server with the given configuration
	pub fn new(config: Config) -> MonoResult<Self> {
		let engine = Arc::new(RwLock::new(Engine::new(
			&config.data_dir,
			config.buffer_pool_size,
		)?));
		let connection_manager = Arc::new(ConnectionManager::new(config.max_connections));

		Ok(Self {
			config,
			engine,
			connection_manager,
			shutdown_token: CancellationToken::new(),
		})
	}

	/// Start the database server
	pub async fn start(&self) -> MonoResult<()> {
		println!("Starting MonoDB server on port {}", self.config.port);

		// Initialize the storage engine
		{
			let mut engine = self.engine.write().unwrap();
			engine.initialize()?;
		}

		// Start accepting connections
		self.accept_connections().await
	}

	/// Accept incoming connections
	async fn accept_connections(&self) -> MonoResult<()> {
		use tokio::net::TcpListener;

		let listener = TcpListener::bind(format!("0.0.0.0:{}", self.config.port))
			.await
			.map_err(|e| MonoError::Io(e))?;

		println!("MonoDB server listening on port {}", self.config.port);

		loop {
			tokio::select! {
				// Handle new connections
				result = listener.accept() => {
					match result {
						Ok((socket, addr)) => {
							println!("New connection from: {}", addr);

							let connection_manager = Arc::clone(&self.connection_manager);
							let engine = Arc::clone(&self.engine);

							tokio::spawn(async move {
								if let Err(e) = connection_manager.handle_connection(socket, engine).await {
									eprintln!("Error handling connection: {}", e);
								}
							});
						}
						Err(e) => {
							eprintln!("Failed to accept connection: {}", e);
						}
					}
				}
				// Handle shutdown signal
				_ = self.shutdown_token.cancelled() => {
					println!("Shutdown signal received, stopping connection acceptance");
					break;
				}
			}
		}

		Ok(())
	}

	/// Gracefully shutdown the server
	pub async fn shutdown(&self) -> MonoResult<()> {
		println!("Shutting down MonoDB server...");

		// Signal shutdown to break the connection loop
		self.shutdown_token.cancel();

		// Checkpoint to ensure all data is persisted
		{
			let engine = self.engine.read().unwrap();
			engine.checkpoint()?;
		}

		// Flush any pending changes
		{
			let mut engine = self.engine.write().unwrap();
			engine.flush()?;
		}

		Ok(())
	}

	/// Get shutdown token for external shutdown coordination
	pub fn shutdown_token(&self) -> CancellationToken {
		self.shutdown_token.clone()
	}
}

use std::sync::Arc;

use dashmap::DashMap;
use monodb_common::Result;
use tokio::{net::TcpListener, sync::broadcast};

use crate::{
    config::StorageConfig, daemon::config::ServerConfig, network::session,
    storage::engine::StorageEngine,
};

pub mod config;

pub struct Server {
    config: ServerConfig,
    storage: Arc<StorageEngine>,
    sessions: Arc<DashMap<u64, session::Session>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl Server {
    pub async fn new(server_config: ServerConfig, storage_config: StorageConfig) -> Result<Self> {
        let (shutdown_tx, _) = broadcast::channel(1);

        let storage_engine = StorageEngine::new(storage_config).await?;

        Ok(Self {
            config: server_config,
            storage: storage_engine,
            sessions: Arc::new(DashMap::new()),
            shutdown_tx,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.address()).await?;
        tracing::info!("Server listening on {}", self.address());

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                // Accept new connections
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    #[cfg(debug_assertions)]
                    tracing::debug!("New connection from {addr}");

                    let storage = Arc::clone(&self.storage);
                    let sessions = Arc::clone(&self.sessions);
                    let connection_shutdown = self.shutdown_tx.subscribe();

                    tokio::spawn(async move {
                        if let Err(e) = crate::network::handle_connection(
                            stream,
                            storage,
                            sessions,
                            connection_shutdown
                        ).await {
                            tracing::error!("Connection error: {e}");
                        }
                    });
                }

                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    tracing::info!("Shutdown signal received, stopping server...");
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!("Starting shutdown...");

        // Signal all connections to close
        if self.shutdown_tx.send(()).is_err() {
            tracing::warn!("No active connections to signal for shutdown");
        }

        // Give connections time to finish current requests
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        tracing::info!("Shutdown completed");
        Ok(())
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }

    pub fn shutdown_signal(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }
}

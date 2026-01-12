#![allow(dead_code)]

use std::sync::Arc;

use monodb_common::{
    Result,
    protocol::{ProtocolDecoder, ProtocolEncoder},
};

use dashmap::DashMap;
use tokio::{net::TcpListener, sync::broadcast};
use tokio_rustls::TlsAcceptor;

use crate::{
    config::{ServerConfig, StorageConfig},
    daemon::tls::load_tls_acceptor,
    network::session,
    query_engine::storage::StorageAdapter,
    storage::engine::{StorageConfig as EngineStorageConfig, StorageEngine},
};

mod tls;

pub struct Server {
    address: String,
    storage: Arc<StorageAdapter>,
    sessions: Arc<DashMap<u64, session::Session>>,
    shutdown_tx: broadcast::Sender<()>,
    tls_acceptor: Option<TlsAcceptor>,
}

impl Server {
    pub async fn new(server_config: ServerConfig, storage_config: StorageConfig) -> Result<Self> {
        let (shutdown_tx, _) = broadcast::channel(1);

        // Set up storage engine
        let engine_config = EngineStorageConfig {
            data_dir: storage_config.data_dir.into(),
            buffer_pool_size: storage_config.buffer_pool_size,
            wal_enabled: true,
            wal_sync_on_commit: storage_config.wal.sync_on_write,
            ..Default::default()
        };
        let engine = Arc::new(StorageEngine::new(engine_config)?);
        let storage = Arc::new(StorageAdapter::new(engine));

        let address = format!("{}:{}", server_config.host, server_config.port);
        let tls_acceptor = if let Some(ref tls_cfg) = server_config.tls {
            Some(load_tls_acceptor(tls_cfg)?)
        } else {
            None
        };

        Ok(Self {
            address,
            storage,
            sessions: Arc::new(DashMap::new()),
            shutdown_tx,
            tls_acceptor,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.address).await?;
        tracing::info!("Server listening on {}", self.address);

        if self.tls_acceptor.is_some() {
            tracing::info!("TLS is enabled for client connections");
        } else {
            tracing::info!("TLS is not enabled; connections are unencrypted");
        }

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                // Accept new connections
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    #[cfg(debug_assertions)]
                    tracing::debug!("New connection from {addr}");

                    let encoder = Arc::new(ProtocolEncoder::new());
                    let decoder = Arc::new(ProtocolDecoder::new());
                    let storage = Arc::clone(&self.storage);
                    let sessions = Arc::clone(&self.sessions);
                    let connection_shutdown = self.shutdown_tx.subscribe();

                    if let Some(tls) = self.tls_acceptor.clone() {
                        // Handle TLS connection
                        tokio::spawn(async move {
                            match tls.accept(stream).await {
                                Ok(tls_stream) => {
                                    if let Err(e) = crate::network::handle_connection(
                                        tls_stream, sessions, storage, connection_shutdown, encoder, decoder
                                    ).await {
                                        tracing::error!("TLS connection error: {e}");
                                    }
                                }
                                Err(e) => tracing::warn!("TLS handshake failed from {addr}: {e}"),
                            }
                        });
                    } else {
                        // Handle plain TCP connection
                        tokio::spawn(async move {
                            if let Err(e) = crate::network::handle_connection(
                                stream, sessions, storage, connection_shutdown, encoder, decoder
                            ).await {
                                tracing::error!("Connection error: {e}");
                            }
                        });
                    }
                }

                // Shutdown signal received
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

        // Notify active connections
        if self.shutdown_tx.send(()).is_err() {
            tracing::warn!("No active connections");
        }

        // Allow graceful close of active requests
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        tracing::info!("Shutdown completed");
        Ok(())
    }

    pub fn address(&self) -> String {
        self.address.clone()
    }

    pub fn shutdown_signal(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }
}

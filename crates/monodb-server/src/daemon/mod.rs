#![allow(dead_code)]

#[cfg(feature = "plugins")]
use std::path::Path;
use std::sync::Arc;

use monodb_common::{
    Result,
    protocol::{ProtocolDecoder, ProtocolEncoder},
};
#[cfg(feature = "plugins")]
use monodb_plugin::{PluginHost, PluginHostConfig};

use dashmap::DashMap;
use tokio::{net::TcpListener, sync::broadcast};
use tokio_rustls::TlsAcceptor;

use crate::{
    config::{PluginConfig, ServerConfig, StorageConfig},
    daemon::tls::load_tls_acceptor,
    network::session,
    query_engine::storage::StorageAdapter,
    storage::engine::{StorageConfig as EngineStorageConfig, StorageEngine},
};

mod tls;

pub struct Server {
    address: String,
    storage: Arc<StorageAdapter>,
    #[cfg(feature = "plugins")]
    plugin_host: Option<Arc<PluginHost>>,
    sessions: Arc<DashMap<u64, session::Session>>,
    shutdown_tx: broadcast::Sender<()>,
    tls_acceptor: Option<TlsAcceptor>,
}

impl Server {
    pub async fn new(
        server_config: ServerConfig,
        storage_config: StorageConfig,
        #[cfg_attr(not(feature = "plugins"), allow(unused_variables))] plugin_config: PluginConfig,
    ) -> Result<Self> {
        let (shutdown_tx, _) = broadcast::channel(1);

        // Set up storage engine
        let engine_config = EngineStorageConfig {
            data_dir: storage_config.data_dir.clone().into(),
            buffer_pool_size: storage_config.buffer_pool_size,
            wal_enabled: true,
            wal_sync_on_commit: storage_config.wal.sync_on_write,
            ..Default::default()
        };
        let engine = Arc::new(StorageEngine::new(engine_config)?);
        let storage = Arc::new(StorageAdapter::new(engine));

        // Set up plugin host if enabled
        #[cfg(feature = "plugins")]
        let plugin_host = if plugin_config.enabled {
            let plugins_dir = Path::new(&plugin_config.plugins_dir);

            // Ensure plugins directory exists
            if !plugins_dir.exists() {
                std::fs::create_dir_all(plugins_dir)?;
                tracing::info!(path = %plugins_dir.display(), "Created plugins directory");
            }

            let host_config = PluginHostConfig {
                plugins_dir: plugins_dir.to_path_buf(),
                hot_reload: plugin_config.hot_reload,
                limits: monodb_plugin::LimitsConfig {
                    max_memory_size: plugin_config.max_memory,
                    ..Default::default()
                },
                fuel: monodb_plugin::FuelConfig {
                    enabled: true,
                    default_fuel: plugin_config.default_fuel,
                    max_fuel: plugin_config.max_fuel,
                },
                epoch: monodb_plugin::EpochConfig {
                    enabled: true,
                    default_deadline: plugin_config.default_deadline,
                    max_deadline: plugin_config.max_deadline,
                    ..Default::default()
                },
                ..Default::default()
            };

            match PluginHost::new(host_config, storage.clone()) {
                Ok(host) => {
                    let host = Arc::new(host);
                    host.start_epoch_thread();

                    // Scan for plugins
                    if plugins_dir.is_dir() {
                        match host.scan_plugins_dir(plugins_dir) {
                            Ok((added, removed)) => {
                                if !removed.is_empty() {
                                    tracing::info!(count = removed.len(), "Removed stale plugins");
                                }
                                tracing::info!(
                                    count = added,
                                    path = %plugins_dir.display(),
                                    "Loaded plugins"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to scan plugins directory");
                            }
                        }
                    }

                    Some(host)
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to initialize plugin host");
                    None
                }
            }
        } else {
            tracing::info!("Plugin system is disabled");
            None
        };

        #[cfg(not(feature = "plugins"))]
        tracing::info!("Plugin system is not available (compiled without plugins feature)");

        let address = format!("{}:{}", server_config.host, server_config.port);
        let tls_acceptor = if let Some(ref tls_cfg) = server_config.tls {
            Some(load_tls_acceptor(tls_cfg)?)
        } else {
            None
        };

        Ok(Self {
            address,
            storage,
            #[cfg(feature = "plugins")]
            plugin_host,
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

        // Start hot-reload polling task if plugin host is enabled
        #[cfg(feature = "plugins")]
        {
            let plugin_host = self.plugin_host.clone();
            if plugin_host.is_some() {
                let host = plugin_host.clone().unwrap();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
                    loop {
                        interval.tick().await;
                        // Run in blocking thread since hot-reload involves filesystem operations
                        let host_clone = host.clone();
                        if let Err(e) =
                            tokio::task::spawn_blocking(move || host_clone.process_hot_reload())
                                .await
                        {
                            tracing::warn!(error = %e, "Hot-reload task error");
                        }
                    }
                });
            }
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

    /// Get the plugin host, if plugins are enabled
    #[cfg(feature = "plugins")]
    pub fn plugin_host(&self) -> Option<&Arc<PluginHost>> {
        self.plugin_host.as_ref()
    }

    /// Get the storage adapter
    pub fn storage(&self) -> &Arc<StorageAdapter> {
        &self.storage
    }
}

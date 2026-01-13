
//! Connection pooling for MonoDB client

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Semaphore;

use monodb_common::{MonoError, Result};

use crate::connection::Connection;

/// Connection pool configuration.
#[derive(Clone)]
pub struct PoolConfig {
    /// Maximum number of connections.
    pub max_connections: usize,
    /// Connection timeout.
    pub connection_timeout: Duration,
    /// Path to CA certificate for TLS. If None, use plain TCP.
    pub cert_path: Option<PathBuf>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            connection_timeout: Duration::from_secs(5),
            cert_path: None,
        }
    }
}

/// A pool of database connections.
#[derive(Clone)]
pub struct ConnectionPool {
    addr: String,
    connections: Arc<Mutex<Vec<Connection>>>,
    semaphore: Arc<Semaphore>,
    config: PoolConfig,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(addr: String, config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));

        Self {
            addr,
            connections: Arc::new(Mutex::new(Vec::new())),
            semaphore,
            config,
        }
    }

    /// Get a connection from the pool.
    pub async fn get(&self) -> Result<Connection> {
        // Try to reuse an existing connection
        if let Some(conn) = self.connections.lock().pop() {
            return Ok(conn);
        }

        // Acquire a permit for a new connection
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MonoError::Network("Pool closed".into()))?;

        let mut conn = if let Some(ref cert_path) = self.config.cert_path {
            Connection::connect_tls(&self.addr, cert_path).await?
        } else {
            Connection::connect(&self.addr).await?
        };
        conn.permit = Some(permit);
        Ok(conn)
    }

    /// Return a connection to the pool.
    pub fn return_connection(&self, conn: Connection) {
        let mut connections = self.connections.lock();
        if connections.len() < self.config.max_connections {
            connections.push(conn);
        }
        // Otherwise the connection is dropped
    }

    /// Close all pooled connections.
    pub async fn close(&self) -> Result<()> {
        self.connections.lock().clear();
        Ok(())
    }

    /// Test that we can establish a connection.
    pub async fn test_connection(&self) -> Result<()> {
        let conn = self.get().await?;
        self.return_connection(conn);
        Ok(())
    }
}

use crate::connection::Connection;
use monodb_common::{MonoError, Result};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct PoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub connection_timeout: Duration,
    pub idle_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 32,
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Some(Duration::from_secs(300)),
        }
    }
}

#[derive(Clone)]
pub struct ConnectionPool {
    addr: String,
    connections: Arc<Mutex<Vec<Connection>>>,
    semaphore: Arc<Semaphore>,
    config: PoolConfig,
}

impl ConnectionPool {
    pub fn new(addr: String, config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));

        Self {
            addr,
            connections: Arc::new(Mutex::new(Vec::new())),
            semaphore,
            config,
        }
    }

    pub async fn get(&self) -> Result<Connection> {
        // Try to get an existing connection
        if let Some(conn) = self.connections.lock().pop() {
            // TODO: Check if connection is still alive
            return Ok(conn);
        }

        // Create new connection
        // Acquire an owned permit which we will store inside the Connection
        // so that the permit is held for the lifetime of the TCP connection
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MonoError::Network("Connection pool closed".into()))?;

        let mut conn = Connection::connect(&self.addr).await?;
        // Attach the permit so the pool slot remains reserved while the
        // connection exists (idle or checked out).
        conn.permit = Some(permit);
        Ok(conn)
    }

    pub fn return_connection(&self, conn: Connection) {
        let mut connections = self.connections.lock();
        if connections.len() < self.config.max_connections {
            connections.push(conn);
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.connections.lock().clear();
        Ok(())
    }

    pub async fn test_connection(&self) -> Result<()> {
        let conn = self.get().await?;
        self.return_connection(conn);
        Ok(())
    }
}

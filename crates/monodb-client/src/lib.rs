use monodb_common::Result;

use crate::pool::ConnectionPool;

mod connection;
mod pool;

#[derive(Clone)]
pub struct Client {
    pool: ConnectionPool,
}

impl Client {
    /// Create a new client with default settings
    pub async fn connect(addr: &str) -> Result<Self> {
        let pool = ConnectionPool::new(addr.to_string(), Default::default());
        Ok(Self { pool })
    }

    /// Get the pool
    pub async fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    /// Close all connections
    pub async fn close(&self) -> Result<()> {
        self.pool.close().await
    }
}

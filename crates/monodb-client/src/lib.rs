//! MonoDB Client Library
//!
//! A simple, ergonomic client for interacting with MonoDB.
//! Supports both plain TCP and TLS connections.
//!
//! # Example
//!
//! ```no_run
//! use monodb_client::Client;
//! use std::path::Path;
//!
//! #[tokio::main]
//! async fn main() -> monodb_common::Result<()> {
//!     // Plain connection
//!     let client = Client::connect("localhost:6432").await?;
//!
//!     // With TLS (provide path to CA certificate)
//!     let client_tls = Client::connect_with_cert("localhost:6432", Path::new("ca.pem")).await?;
//!
//!     let result = client.query("get from users").await?;
//!     for row in result.rows() {
//!         println!("{:?}", row);
//!     }
//!     Ok(())
//! }
//! ```

use std::path::Path;

use monodb_common::Result;

pub use connection::Connection;
pub use pool::{ConnectionPool, PoolConfig};
pub use results::{FromValue, QueryResult, Row};

mod connection;
mod pool;
mod results;

/// MonoDB client with connection pooling.
#[derive(Clone)]
pub struct Client {
    pool: ConnectionPool,
}

impl Client {
    /// Connect to a MonoDB server using plain TCP.
    ///
    /// # Example
    /// ```no_run
    /// # use monodb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:6432").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(addr: &str) -> Result<Self> {
        let pool = ConnectionPool::new(addr.to_string(), PoolConfig::default());
        pool.test_connection().await?;
        Ok(Self { pool })
    }

    /// Connect to a MonoDB server using TLS with a custom CA certificate.
    ///
    /// # Example
    /// ```no_run
    /// # use monodb_client::Client;
    /// # use std::path::Path;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect_with_cert("localhost:6432", Path::new("ca.pem")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_cert(addr: &str, cert_path: &Path) -> Result<Self> {
        let config = PoolConfig {
            cert_path: Some(cert_path.to_path_buf()),
            ..PoolConfig::default()
        };
        let pool = ConnectionPool::new(addr.to_string(), config);
        pool.test_connection().await?;
        Ok(Self { pool })
    }

    /// Connect to a MonoDB server with custom pool configuration.
    pub async fn connect_with_config(addr: &str, config: PoolConfig) -> Result<Self> {
        let pool = ConnectionPool::new(addr.to_string(), config);
        pool.test_connection().await?;
        Ok(Self { pool })
    }

    /// Execute a query and return all results.
    pub async fn query(&self, query: impl Into<String>) -> Result<QueryResult> {
        let mut conn = self.pool.get().await?;
        let result = conn.execute(query.into()).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Execute a query with parameters.
    pub async fn query_with_params(
        &self,
        query: impl Into<String>,
        params: Vec<monodb_common::Value>,
    ) -> Result<QueryResult> {
        let mut conn = self.pool.get().await?;
        let result = conn.execute_with_params(query.into(), params).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Execute a query that returns a single row.
    pub async fn query_one(&self, query: impl Into<String>) -> Result<Row> {
        self.query(query).await?.one()
    }

    /// Execute a query that returns an optional single row.
    pub async fn query_optional(&self, query: impl Into<String>) -> Result<Option<Row>> {
        self.query(query).await?.optional()
    }

    /// List all tables.
    pub async fn list_tables(&self) -> Result<QueryResult> {
        let mut conn = self.pool.get().await?;
        let result = conn.list_tables().await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Close all connections.
    pub async fn close(&self) -> Result<()> {
        self.pool.close().await
    }
}

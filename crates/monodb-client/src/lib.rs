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

use std::path::{Path, PathBuf};

use monodb_common::Result;

pub use connection::Connection;
pub use pool::{ConnectionPool, Credentials, PoolConfig};
pub use results::{AuthResult, FromValue, QueryResult, Row, TableListResult};

mod connection;
mod pool;
mod results;

/// Builder for configuring and creating a MonoDB client.
///
/// # Example
///
/// ```no_run
/// use monodb_client::ClientBuilder;
///
/// #[tokio::main]
/// async fn main() -> monodb_common::Result<()> {
///     let client = ClientBuilder::new("localhost:6432")
///         .with_credentials("root", "password")
///         .build()
///         .await?;
///     Ok(())
/// }
/// ```
pub struct ClientBuilder {
    addr: String,
    config: PoolConfig,
}

impl ClientBuilder {
    /// Create a new client builder for the given address.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            config: PoolConfig::default(),
        }
    }

    /// Set username and password credentials.
    pub fn with_credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.credentials = Some(Credentials::Password {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Set a session token for authentication.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.config.credentials = Some(Credentials::Token(token.into()));
        self
    }

    /// Set the TLS certificate path.
    pub fn with_tls(mut self, cert_path: impl Into<PathBuf>) -> Self {
        self.config.cert_path = Some(cert_path.into());
        self
    }

    /// Set the maximum number of pooled connections.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.config.max_connections = max;
        self
    }

    /// Build and connect the client.
    pub async fn build(self) -> Result<Client> {
        let pool = ConnectionPool::new(self.addr, self.config);
        pool.test_connection().await?;
        Ok(Client { pool })
    }
}

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
    pub async fn list_tables(&self) -> Result<TableListResult> {
        let mut conn = self.pool.get().await?;
        let result = conn.list_tables().await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Close all connections.
    pub async fn close(&self) -> Result<()> {
        self.pool.close().await
    }

    /// Ping the server.
    pub async fn ping(&self) -> Result<u64> {
        let mut conn = self.pool.get().await?;
        let result = conn.ping().await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Describe a table's schema.
    pub async fn describe_table(
        &self,
        table: impl Into<String>,
    ) -> Result<monodb_common::protocol::TableSchema> {
        let mut conn = self.pool.get().await?;
        let result = conn.describe_table(table).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Get server statistics.
    pub async fn stats(&self, detailed: bool) -> Result<monodb_common::protocol::ServerStats> {
        let mut conn = self.pool.get().await?;
        let result = conn.stats(detailed).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// List all namespaces.
    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        let mut conn = self.pool.get().await?;
        let result = conn.list_namespaces().await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Create a new namespace.
    pub async fn create_namespace(
        &self,
        name: impl Into<String>,
        description: Option<String>,
    ) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let result = conn.create_namespace(name, description).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Drop a namespace.
    pub async fn drop_namespace(&self, name: impl Into<String>, force: bool) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let result = conn.drop_namespace(name, force).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Switch the default namespace for queries.
    pub async fn use_namespace(&self, namespace: impl Into<String>) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let result = conn.use_namespace(namespace).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Execute a batch of queries.
    pub async fn batch(&self, statements: Vec<String>) -> Result<Vec<QueryResult>> {
        let mut conn = self.pool.get().await?;
        let result = conn.batch(statements).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Begin a transaction and return a transaction handle.
    pub async fn begin_transaction(&self, read_only: bool) -> Result<Transaction> {
        let mut conn = self.pool.get().await?;
        let tx_id = conn.begin_transaction(read_only).await?;
        Ok(Transaction {
            conn,
            tx_id,
            committed: false,
        })
    }
}

/// A database transaction.
///
/// The transaction is automatically rolled back when dropped unless committed.
pub struct Transaction {
    conn: Connection,
    tx_id: u64,
    committed: bool,
}

impl Transaction {
    /// Execute a query within this transaction.
    pub async fn execute(&mut self, query: impl Into<String>) -> Result<QueryResult> {
        self.conn.execute(query.into()).await
    }

    /// Execute a query with parameters within this transaction.
    pub async fn execute_with_params(
        &mut self,
        query: impl Into<String>,
        params: Vec<monodb_common::Value>,
    ) -> Result<QueryResult> {
        self.conn.execute_with_params(query.into(), params).await
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<()> {
        self.conn.commit(self.tx_id).await?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction.
    pub async fn rollback(mut self) -> Result<()> {
        self.conn.rollback(self.tx_id).await?;
        self.committed = true; // Mark as handled
        Ok(())
    }

    /// Get the transaction ID.
    pub fn tx_id(&self) -> u64 {
        self.tx_id
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            // Transaction was not committed, it will be rolled back by the server
            // when the connection is reused or closed
            eprintln!(
                "Warning: Transaction {} dropped without commit/rollback",
                self.tx_id
            );
        }
    }
}

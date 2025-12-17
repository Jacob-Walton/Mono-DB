use monodb_common::Result;

pub use crate::pool::ConnectionPool;
pub use crate::results::{FromValue, QueryResult, Row};

pub mod connection;
pub mod pool;
pub mod results;

#[derive(Clone)]
pub struct Client {
    pool: ConnectionPool,
}

impl Client {
    /// Create a new client with default settings
    pub async fn connect(addr: &str) -> Result<Self> {
        let pool = ConnectionPool::new(addr.to_string(), Default::default());

        // Test the connection
        pool.test_connection().await?;

        Ok(Self { pool })
    }

    /// Execute a query and return all rows
    ///
    /// # Example
    /// ```no_run
    /// # use monodb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:5532").await?;
    /// let result = client.query("select users").await?;
    /// for row in result.rows() {
    ///     println!("{:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query(&self, query: impl Into<String>) -> Result<QueryResult> {
        let mut conn = self.pool.get().await?;
        let result = conn.execute(query.into()).await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Execute a query with parameters and return all rows
    ///
    /// Supports both numbered ($1, $2) and named (:name) parameters.
    ///
    /// # Example with numbered parameters
    /// ```no_run
    /// # use monodb_client::Client;
    /// # use monodb_common::Value;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:5532").await?;
    /// let result = client.query_with_params(
    ///     "get from users where id = $1",
    ///     vec![Value::Int32(42)]
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Example with named parameters
    /// ```no_run
    /// # use monodb_client::Client;
    /// # use monodb_common::Value;
    /// # use std::collections::BTreeMap;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:5532").await?;
    /// let mut params = BTreeMap::new();
    /// params.insert("name".to_string(), Value::String("Alice".to_string()));
    /// params.insert("age".to_string(), Value::Int32(30));
    /// let result = client.query_with_params(
    ///     "get from users where first_name = :name and age = :age",
    ///     vec![Value::Object(params)]
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Execute a query that returns a single row
    ///
    /// Returns an error if no rows or multiple rows are returned.
    ///
    /// # Example
    /// ```no_run
    /// # use monodb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:5532").await?;
    /// let row = client.query_one("select users where id = 1").await?;
    /// let name: String = row.get_typed("name")?;
    /// println!("Name: {}", name);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_one(&self, query: impl Into<String>) -> Result<crate::results::Row> {
        let result = self.query(query).await?;
        result.one()
    }

    /// Execute a query that returns an optional single row
    ///
    /// Returns None if no rows, error if multiple rows.
    ///
    /// # Example
    /// ```no_run
    /// # use monodb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:5532").await?;
    /// if let Some(row) = client.query_optional("select users where id = 999").await? {
    ///     println!("Found user: {:?}", row);
    /// } else {
    ///     println!("User not found");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_optional(
        &self,
        query: impl Into<String>,
    ) -> Result<Option<crate::results::Row>> {
        let result = self.query(query).await?;
        result.optional()
    }

    /// Execute a statement (INSERT, UPDATE, DELETE, CREATE, etc.)
    ///
    /// Returns the result which can be used to get rows_affected, etc.
    ///
    /// # Example
    /// ```no_run
    /// # use monodb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:5532").await?;
    /// let result = client.execute("insert users (name, age) values ('Alice', 30)").await?;
    /// println!("Rows affected: {}", result.rows_affected());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(&self, query: impl Into<String>) -> Result<QueryResult> {
        self.query(query).await
    }

    /// Execute a statement with parameters
    ///
    /// # Example
    /// ```no_run
    /// # use monodb_client::Client;
    /// # use monodb_common::Value;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:5532").await?;
    /// let result = client.execute_with_params(
    ///     "put to users (name, age) values ($1, $2)",
    ///     vec![Value::String("Alice".to_string()), Value::Int32(30)]
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_with_params(
        &self,
        query: impl Into<String>,
        params: Vec<monodb_common::Value>,
    ) -> Result<QueryResult> {
        self.query_with_params(query, params).await
    }

    /// List all tables
    ///
    /// # Example
    /// ```no_run
    /// # use monodb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> monodb_common::Result<()> {
    /// let client = Client::connect("localhost:5532").await?;
    /// let result = client.list_tables().await?;
    /// for row in result.rows() {
    ///     println!("Table: {:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_tables(&self) -> Result<QueryResult> {
        let mut conn = self.pool.get().await?;
        let result = conn.list_tables().await?;
        self.pool.return_connection(conn);
        Ok(result)
    }

    /// Get a connection from the pool for advanced usage
    ///
    /// The connection should be returned to the pool using `return_connection`.
    pub async fn get_connection(&self) -> Result<crate::connection::Connection> {
        self.pool.get().await
    }

    /// Return a connection to the pool
    pub fn return_connection(&self, conn: crate::connection::Connection) {
        self.pool.return_connection(conn);
    }

    /// Get the pool
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    /// Close all connections
    pub async fn close(&self) -> Result<()> {
        self.pool.close().await
    }
}

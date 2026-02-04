//! Connection management for MonoDB client
//!
//! Handles establishing and maintaining connections to the server.
//! Supports both plain TCP and TLS connections.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use monodb_common::{
    MonoError, Result, Value,
    protocol::{
        AuthMethod, IsolationLevel, ProtocolDecoder, ProtocolEncoder, Request, Response,
        ServerStats, Statement, TableSchema,
    },
};
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::results::{AuthResult, QueryResult, TableListResult};

/// Stream type that can be either plain TCP or TLS-wrapped.
enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Stream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.write_all(buf).await,
            Stream::Tls(s) => s.write_all(buf).await,
        }
    }

    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read_buf(buf).await,
            Stream::Tls(s) => s.read_buf(buf).await,
        }
    }
}

/// A connection to a MonoDB server.
pub struct Connection {
    stream: Stream,
    buffer: BytesMut,
    encoder: ProtocolEncoder,
    decoder: ProtocolDecoder,
    /// Owned permit from connection pool (keeps slot reserved).
    pub(crate) permit: Option<OwnedSemaphorePermit>,
}

impl Connection {
    /// Establish a new plain TCP connection to the server.
    pub async fn connect(addr: &str) -> Result<Self> {
        Self::connect_inner(addr, None).await
    }

    /// Establish a new TLS connection to the server with a custom CA certificate.
    pub async fn connect_tls(addr: &str, cert_path: &Path) -> Result<Self> {
        Self::connect_inner(addr, Some(cert_path)).await
    }

    async fn connect_inner(addr: &str, cert_path: Option<&Path>) -> Result<Self> {
        let tcp_stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
            .await
            .map_err(|_| MonoError::Network("Connection timeout".into()))?
            .map_err(|e| MonoError::Network(format!("Failed to connect: {e}")))?;

        tcp_stream.set_nodelay(true)?;

        let stream = if let Some(cert_path) = cert_path {
            let tls_stream = Self::wrap_tls(tcp_stream, addr, cert_path).await?;
            Stream::Tls(Box::new(tls_stream))
        } else {
            Stream::Plain(tcp_stream)
        };

        let mut conn = Self {
            stream,
            buffer: BytesMut::with_capacity(8192),
            encoder: ProtocolEncoder::new(),
            decoder: ProtocolDecoder::new(),
            permit: None,
        };

        // Perform handshake
        conn.handshake().await?;

        Ok(conn)
    }

    async fn wrap_tls(
        tcp: TcpStream,
        addr: &str,
        cert_path: &Path,
    ) -> Result<TlsStream<TcpStream>> {
        // Extract hostname (strip port if present)
        let hostname = addr.split(':').next().unwrap_or("localhost");

        let server_name = ServerName::try_from(hostname.to_string())
            .map_err(|_| MonoError::Network(format!("Invalid server name: {hostname}")))?;

        // Load CA certificate from file
        let cert_pem = std::fs::read(cert_path)
            .map_err(|e| MonoError::Network(format!("Failed to read certificate: {e}")))?;

        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
            .filter_map(|r| r.ok())
            .collect();

        if certs.is_empty() {
            return Err(MonoError::Network(
                "No valid certificates found in file".into(),
            ));
        }

        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| MonoError::Network(format!("Failed to add certificate: {e}")))?;
        }

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));

        connector
            .connect(server_name, tcp)
            .await
            .map_err(|e| MonoError::Network(format!("TLS handshake failed: {e}")))
    }

    async fn handshake(&mut self) -> Result<()> {
        let capabilities = vec![];

        let request = Request::Hello {
            client_name: format!("monodb-client/{}", env!("CARGO_PKG_VERSION")),
            capabilities,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::Welcome { .. } => Ok(()),
            Response::Error { message, .. } => {
                Err(MonoError::Network(format!("Handshake failed: {message}")))
            }
            _ => Err(MonoError::Network("Unexpected handshake response".into())),
        }
    }

    async fn send_request(&mut self, request: Request) -> Result<Response> {
        // Encode and send
        let bytes = self.encoder.encode_request(&request)?;
        self.stream
            .write_all(&bytes)
            .await
            .map_err(|e| MonoError::Network(format!("Write failed: {e}")))?;

        // Read response
        loop {
            if let Some((response, _correlation_id)) =
                self.decoder.decode_response(&mut self.buffer)?
            {
                return Ok(response);
            }

            let n = self
                .stream
                .read_buf(&mut self.buffer)
                .await
                .map_err(|e| MonoError::Network(format!("Read failed: {e}")))?;

            if n == 0 {
                return Err(MonoError::Network("Connection closed".into()));
            }
        }
    }

    /// Execute a query.
    pub async fn execute(&mut self, query: String) -> Result<QueryResult> {
        self.execute_with_params(query, Vec::new()).await
    }

    /// Execute a query with parameters.
    pub async fn execute_with_params(
        &mut self,
        statement: String,
        params: Vec<Value>,
    ) -> Result<QueryResult> {
        let request = Request::Query { statement, params };
        let response = self.send_request(request).await?;
        QueryResult::from_response(response)
    }

    /// List all tables.
    pub async fn list_tables(&mut self) -> Result<TableListResult> {
        let request = Request::List;
        let response = self.send_request(request).await?;

        // Convert TableList response to TableListResult
        match response {
            Response::TableList { tables, namespaces } => {
                Ok(TableListResult { tables, namespaces })
            }
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Ping the server.
    pub async fn ping(&mut self) -> Result<u64> {
        let request = Request::Ping;
        let response = self.send_request(request).await?;

        match response {
            Response::Pong { timestamp } => Ok(timestamp),
            Response::Error { message, .. } => Err(MonoError::Network(message)),
            _ => Err(MonoError::Network("Unexpected ping response".into())),
        }
    }

    /// Authenticate with username and password.
    ///
    /// On success, returns an `AuthResult` containing the session token
    /// which can be used for session resumption via `authenticate_token`.
    pub async fn authenticate_password(
        &mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<AuthResult> {
        let request = Request::Authenticate {
            method: AuthMethod::Password {
                username: username.into(),
                password: password.into(),
            },
        };

        let response = self.send_request(request).await?;
        AuthResult::from_response(response)
    }

    /// Authenticate with a session token.
    ///
    /// Use this to resume a previous session without re-entering credentials.
    /// Tokens are obtained from a successful `authenticate_password` call.
    pub async fn authenticate_token(&mut self, token: impl Into<String>) -> Result<AuthResult> {
        let request = Request::Authenticate {
            method: AuthMethod::Token {
                token: token.into(),
            },
        };

        let response = self.send_request(request).await?;
        AuthResult::from_response(response)
    }

    /// Begin a transaction.
    pub async fn begin_transaction(&mut self, read_only: bool) -> Result<u64> {
        let request = Request::TxBegin {
            isolation: IsolationLevel::ReadCommitted,
            read_only,
        };
        let response = self.send_request(request).await?;

        match response {
            Response::TxStarted { tx_id, .. } => Ok(tx_id),
            Response::Error { message, .. } => Err(MonoError::Transaction(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Commit a transaction.
    pub async fn commit(&mut self, tx_id: u64) -> Result<()> {
        let request = Request::TxCommit { tx_id };
        let response = self.send_request(request).await?;

        match response {
            Response::TxCommitted { .. } | Response::Ok => Ok(()),
            Response::Error { message, .. } => Err(MonoError::Transaction(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Rollback a transaction.
    pub async fn rollback(&mut self, tx_id: u64) -> Result<()> {
        let request = Request::TxRollback { tx_id };
        let response = self.send_request(request).await?;

        match response {
            Response::TxRolledBack { .. } | Response::Ok => Ok(()),
            Response::Error { message, .. } => Err(MonoError::Transaction(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Describe a table's schema.
    pub async fn describe_table(&mut self, table: impl Into<String>) -> Result<TableSchema> {
        let request = Request::Describe {
            target: table.into(),
        };
        let response = self.send_request(request).await?;

        match response {
            Response::TableDescription { schema } => Ok(schema),
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Get server statistics.
    pub async fn stats(&mut self, detailed: bool) -> Result<ServerStats> {
        let request = Request::Stats { detailed };
        let response = self.send_request(request).await?;

        match response {
            Response::StatsResult { stats } => Ok(stats),
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Switch to a namespace.
    pub async fn use_namespace(&mut self, namespace: impl Into<String>) -> Result<()> {
        let request = Request::UseNamespace {
            namespace: namespace.into(),
        };
        let response = self.send_request(request).await?;

        match response {
            Response::NamespaceSwitched { .. } | Response::Ok => Ok(()),
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Create a new namespace.
    pub async fn create_namespace(
        &mut self,
        name: impl Into<String>,
        description: Option<String>,
    ) -> Result<()> {
        let request = Request::CreateNamespace {
            name: name.into(),
            description,
        };
        let response = self.send_request(request).await?;

        match response {
            Response::NamespaceCreated { .. } | Response::Ok => Ok(()),
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Drop a namespace.
    pub async fn drop_namespace(&mut self, name: impl Into<String>, force: bool) -> Result<()> {
        let request = Request::DropNamespace {
            name: name.into(),
            force,
        };
        let response = self.send_request(request).await?;

        match response {
            Response::NamespaceDropped { .. } | Response::Ok => Ok(()),
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// List all namespaces.
    pub async fn list_namespaces(&mut self) -> Result<Vec<String>> {
        let request = Request::ListNamespaces;
        let response = self.send_request(request).await?;

        match response {
            Response::NamespaceList { namespaces } => {
                Ok(namespaces.into_iter().map(|ns| ns.name).collect())
            }
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }

    /// Execute a batch of queries.
    pub async fn batch(&mut self, queries: Vec<String>) -> Result<Vec<QueryResult>> {
        let statements = queries
            .into_iter()
            .map(|q| Statement {
                query: q,
                params: vec![],
            })
            .collect();
        let request = Request::Batch { statements };
        let response = self.send_request(request).await?;

        match response {
            Response::BatchResults { results, .. } => {
                // Convert each QueryOutcome to QueryResult
                results
                    .into_iter()
                    .map(|outcome| {
                        QueryResult::from_response(Response::QueryResult {
                            result: outcome,
                            elapsed_ms: 0,
                        })
                    })
                    .collect()
            }
            Response::Error { message, .. } => Err(MonoError::Execution(message)),
            _ => Err(MonoError::Network("Unexpected response".into())),
        }
    }
}

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
        ProtocolDecoder, ProtocolEncoder, Request, Response,
    },
};
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::results::QueryResult;

/// Stream type that can be either plain TCP or TLS-wrapped.
enum Stream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
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
        let tcp_stream = tokio::time::timeout(
            Duration::from_secs(5),
            TcpStream::connect(addr),
        )
        .await
        .map_err(|_| MonoError::Network("Connection timeout".into()))?
        .map_err(|e| MonoError::Network(format!("Failed to connect: {e}")))?;

        tcp_stream.set_nodelay(true)?;

        let stream = if let Some(cert_path) = cert_path {
            let tls_stream = Self::wrap_tls(tcp_stream, addr, cert_path).await?;
            Stream::Tls(tls_stream)
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

    async fn wrap_tls(tcp: TcpStream, addr: &str, cert_path: &Path) -> Result<TlsStream<TcpStream>> {
        // Extract hostname for SNI (strip port if present)
        let hostname = addr
            .split(':')
            .next()
            .unwrap_or("localhost");

        let server_name = ServerName::try_from(hostname.to_string())
            .map_err(|_| MonoError::Network(format!("Invalid server name: {hostname}")))?;

        // Load CA certificate from file
        let cert_pem = std::fs::read(cert_path)
            .map_err(|e| MonoError::Network(format!("Failed to read certificate: {e}")))?;
        
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
            .filter_map(|r| r.ok())
            .collect();
        
        if certs.is_empty() {
            return Err(MonoError::Network("No valid certificates found in file".into()));
        }

        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs {
            root_store.add(cert)
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
            if let Some((response, _correlation_id)) = self.decoder.decode_response(&mut self.buffer)? {
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
    pub async fn list_tables(&mut self) -> Result<QueryResult> {
        let request = Request::List;
        let response = self.send_request(request).await?;

        // Convert TableList response to QueryResult format
        match response {
            Response::TableList { tables } => Ok(QueryResult::from_tables(tables)),
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
}

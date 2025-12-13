//! Connection management for MonoDB client
//!
//! Handles establishing and maintaining connections to the server.

use std::time::Duration;

use bytes::BytesMut;
use monodb_common::{
    MonoError, Result,
    protocol::{ProtocolCodec, Request, Response},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio::sync::OwnedSemaphorePermit;

/// Represents a connection to the server.
///
/// Handles sending requests and receiving responses.
pub struct Connection {
    /// Underlying TCP stream
    stream: TcpStream,
    /// Buffer for reading data from the server
    buffer: BytesMut,
    /// Request ID counter for matching requests and responses
    #[allow(dead_code)]
    request_id: u64,
    /// Owned permit from the connection pool semaphore. Keeping this
    /// permit alive for the lifetime of the connection ensures the pool
    /// counts active connections correctly.
    pub(crate) permit: Option<OwnedSemaphorePermit>,
}

impl Connection {
    /// Esablishes a new connection to the specified address.
    ///
    /// # Arguments
    /// * `addr` - The address of the server (e.e., "localhost:5532")
    ///
    /// # Returns
    /// A `Result` containing the established `Connection` or an error if the connection fails
    pub async fn connect(addr: &str) -> Result<Self> {
        let connect_future = TcpStream::connect(addr);
        let stream = tokio::time::timeout(Duration::from_secs(3), connect_future)
            .await
            .map_err(|_| MonoError::Network("Connection timeout after 3 seconds".into()))?
            .map_err(|e| MonoError::Network(format!("Failed to connect: {e}")))?;
        stream.set_nodelay(true)?;

        let mut conn = Self {
            stream,
            buffer: BytesMut::new(),
            request_id: 0,
            permit: None,
        };

        // Send Connect request
        conn.send_connect_request().await?;

        Ok(conn)
    }

    async fn send_connect_request(&mut self) -> Result<()> {
        let request = Request::Connect {
            protocol_version: 1,
            auth_token: None,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::ConnectAck {
                protocol_version: _,
                ..
            } => Ok(()),
            Response::Error { code: _, message } => {
                Err(MonoError::Network(format!("Connection failed: {message}")))
            }
            _ => Err(MonoError::Network(
                "Unexpected response to Connect request".into(),
            )),
        }
    }

    async fn send_request(&mut self, request: Request) -> Result<Response> {
        // Encode and send request
        let request_bytes = ProtocolCodec::encode_request(&request)?;
        self.stream
            .write_all(&request_bytes)
            .await
            .map_err(|e| MonoError::Network(format!("Failed to send request: {e}")))?;

        // Read response
        loop {
            // Try to decode a response from the buffer
            if let Some(response) = ProtocolCodec::decode_response(&mut self.buffer)? {
                return Ok(response);
            }

            // Read more data
            let n = self
                .stream
                .read_buf(&mut self.buffer)
                .await
                .map_err(|e| MonoError::Network(format!("Failed to read response: {e}")))?;

            if n == 0 {
                return Err(MonoError::Network("Connection closed by server".into()));
            }
        }
    }

    pub async fn execute(&mut self, query: String) -> Result<Response> {
        let request = Request::Execute {
            query,
            params: Vec::new(),
            snapshot_timestamp: None,
            user_id: None,
        };

        let response = self
            .send_request(request)
            .await
            .map_err(|e| MonoError::Execution(e.to_string()))?;

        match response {
            Response::Success { .. } => Ok(response),
            Response::Error { code: _, message } => Err(MonoError::Execution(message)),
            other => Err(MonoError::Execution(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }

    pub async fn list_tables(&mut self) -> Result<Response> {
        let request = Request::List {};

        let response = self
            .send_request(request)
            .await
            .map_err(|e| MonoError::Execution(e.to_string()))?;

        match response {
            Response::Success { .. } => Ok(response),
            Response::Error { code: _, message } => Err(MonoError::Execution(message)),
            other => Err(MonoError::Execution(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }
}

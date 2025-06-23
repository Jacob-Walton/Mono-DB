//! Network connection handling
//!
//! Provides utilities for managing network connections and communication.

use crate::network::protocol::{ClientMessage, MAX_MESSAGE_SIZE, ServerMessage};
use crate::{MonoError, MonoResult};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Represents a network connection
pub struct NetworkConnection {
    stream: TcpStream,
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    last_activity: Instant,
}

impl NetworkConnection {
    /// Create a new network connection
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buffer: vec![0; 8192],
            write_buffer: Vec::with_capacity(8192),
            last_activity: Instant::now(),
        }
    }

    /// Read a message from the connection
    pub async fn read_message(&mut self) -> MonoResult<Option<ClientMessage>> {
        // Read message length (4 bytes)
        let mut len_bytes = [0u8; 4];
        match tokio::time::timeout(
            Duration::from_secs(60),
            self.stream.read_exact(&mut len_bytes),
        )
        .await
        {
            Ok(Ok(_)) => {
                self.last_activity = Instant::now();
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None); // Connection closed
            }
            Ok(Err(e)) => return Err(MonoError::Io(e)),
            Err(_) => return Err(MonoError::Timeout),
        }

        let message_len = u32::from_be_bytes(len_bytes) as usize;

        // Validate message size
        if message_len > MAX_MESSAGE_SIZE {
            return Err(MonoError::Protocol(format!(
                "Message too large: {} bytes",
                message_len
            )));
        }

        // Ensure buffer is large enough
        if self.read_buffer.len() < message_len {
            self.read_buffer.resize(message_len, 0);
        }

        // Read message data
        self.stream
            .read_exact(&mut self.read_buffer[..message_len])
            .await
            .map_err(MonoError::Io)?;

        self.last_activity = Instant::now();

        // Deserialize message
        let message = ClientMessage::from_bytes(&self.read_buffer[..message_len])
            .map_err(|e| MonoError::Protocol(e))?;

        Ok(Some(message))
    }

    /// Send a response through the connection
    pub async fn send_response(&mut self, response: ServerMessage) -> MonoResult<()> {
        // Serialize response
        let response_bytes = response.to_bytes().map_err(|e| MonoError::Protocol(e))?;

        // Check size limit
        if response_bytes.len() > MAX_MESSAGE_SIZE {
            return Err(MonoError::Protocol(format!(
                "Response too large: {} bytes",
                response_bytes.len()
            )));
        }

        // Clear write buffer and reserve space
        self.write_buffer.clear();
        self.write_buffer.reserve(4 + response_bytes.len());

        // Write length prefix
        let len = response_bytes.len() as u32;
        self.write_buffer.extend_from_slice(&len.to_be_bytes());

        // Write response data
        self.write_buffer.extend_from_slice(&response_bytes);

        // Send all data
        self.stream
            .write_all(&self.write_buffer)
            .await
            .map_err(MonoError::Io)?;

        self.stream.flush().await.map_err(MonoError::Io)?;

        self.last_activity = Instant::now();

        Ok(())
    }

    /// Send a message through the connection (for client use)
    pub async fn send_message(&mut self, message: ClientMessage) -> MonoResult<()> {
        // Serialize message
        let message_bytes = message.to_bytes().map_err(|e| MonoError::Protocol(e))?;

        // Check size limit
        if message_bytes.len() > MAX_MESSAGE_SIZE {
            return Err(MonoError::Protocol(format!(
                "Message too large: {} bytes",
                message_bytes.len()
            )));
        }

        // Clear write buffer and reserve space
        self.write_buffer.clear();
        self.write_buffer.reserve(4 + message_bytes.len());

        // Write length prefix
        let len = message_bytes.len() as u32;
        self.write_buffer.extend_from_slice(&len.to_be_bytes());

        // Write message data
        self.write_buffer.extend_from_slice(&message_bytes);

        // Send all data
        self.stream
            .write_all(&self.write_buffer)
            .await
            .map_err(MonoError::Io)?;

        self.stream.flush().await.map_err(MonoError::Io)?;

        self.last_activity = Instant::now();

        Ok(())
    }

    /// Read a response from the connection (for client use)
    pub async fn read_response(&mut self) -> MonoResult<Option<ServerMessage>> {
        // Read response length (4 bytes)
        let mut len_bytes = [0u8; 4];
        match tokio::time::timeout(
            Duration::from_secs(60),
            self.stream.read_exact(&mut len_bytes),
        )
        .await
        {
            Ok(Ok(_)) => {
                self.last_activity = Instant::now();
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None); // Connection closed
            }
            Ok(Err(e)) => return Err(MonoError::Io(e)),
            Err(_) => return Err(MonoError::Timeout),
        }

        let response_len = u32::from_be_bytes(len_bytes) as usize;

        // Validate response size
        if response_len > MAX_MESSAGE_SIZE {
            return Err(MonoError::Protocol(format!(
                "Response too large: {} bytes",
                response_len
            )));
        }

        // Ensure buffer is large enough
        if self.read_buffer.len() < response_len {
            self.read_buffer.resize(response_len, 0);
        }

        // Read response data
        self.stream
            .read_exact(&mut self.read_buffer[..response_len])
            .await
            .map_err(MonoError::Io)?;

        self.last_activity = Instant::now();

        // Deserialize response
        let response = ServerMessage::from_bytes(&self.read_buffer[..response_len])
            .map_err(|e| MonoError::Protocol(e))?;

        Ok(Some(response))
    }

    /// Check if the connection has been idle for too long
    pub fn is_idle(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }

    /// Get the last activity time
    pub fn last_activity(&self) -> Instant {
        self.last_activity
    }

    /// Close the connection gracefully
    pub async fn close(&mut self) -> MonoResult<()> {
        self.stream.shutdown().await.map_err(MonoError::Io)?;
        Ok(())
    }
}

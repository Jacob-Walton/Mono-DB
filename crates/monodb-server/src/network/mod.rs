use bytes::BytesMut;
use monodb_common::Result;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::broadcast,
};

use crate::network::session::Session;

pub mod session;

const SERVER_PROTOCOL_VERSION: u8 = 3;

pub async fn handle_connection<S>(
    mut stream: S,
    _sessions: Arc<DashMap<u64, Session>>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut buffer = BytesMut::new();

    loop {
        tokio::select! {
            read_result = stream.read_buf(&mut buffer) => {
                match read_result {
                    Ok(0) => {
                        // Client closed connection
                        break;
                    }
                    Ok(n) => {
                        tracing::debug!("Received {n} bytes");

                        // Clear buffer after acknowledging
                        buffer.clear();

                        // Simple ACK response
                        if let Err(e) = stream.write_all(b"OK\n").await {
                            tracing::error!("Write error: {e}");
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Read error: {e}");
                        break;
                    }
                }
            }

            _ = shutdown_rx.recv() => {
                tracing::info!("Shutdown requested, closing connection");

                let _ = stream.write_all(b"SERVER SHUTDOWN\n").await;
                break;
            }
        }
    }

    Ok(())
}

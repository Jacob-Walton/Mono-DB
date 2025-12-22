use bytes::BytesMut;
use monodb_common::{
    Result,
    permissions::BuiltinRole,
    protocol::{ProtocolDecoder, ProtocolEncoder, Request, Response},
};

use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Instant,
};

use dashmap::DashMap;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::broadcast,
};

use crate::network::session::Session;

// Atomic counter
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub mod session;

pub async fn handle_connection<S>(
    mut stream: S,
    _sessions: Arc<DashMap<u64, Session>>,
    mut shutdown_rx: broadcast::Receiver<()>,
    encoder: Arc<ProtocolEncoder>,
    decoder: Arc<ProtocolDecoder>,
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
                        #[cfg(debug_assertions)]
                        tracing::debug!("Received {n} bytes");

                        while let Some((request, correlation_id)) = decoder.decode_request(&mut buffer)? {
                            #[cfg(debug_assertions)]
                            tracing::debug!("Decoded request: {:?}", request);

                            let response = handle_request(request).await?;

                            let encoded_response = encoder.encode_response(&response, correlation_id)?;

                            #[cfg(debug_assertions)]
                            tracing::debug!("Sending response: {:?}", response);

                            #[cfg(debug_assertions)]
                            tracing::debug!("Encoded response size: {}", encoded_response.len());

                            stream.write_all(&encoded_response).await?;
                            stream.flush().await?;
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

async fn handle_request(request: Request) -> Result<Response> {
    let start = Instant::now();

    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match request {
        Request::Hello {
            client_name,
            capabilities,
        } => {
            #[cfg(debug_assertions)]
            tracing::debug!(
                "Handling Hello request from {client_name:?} with capabilities: {capabilities:?}"
            );

            let response = Response::Welcome {
                server_version: env!("CARGO_PKG_VERSION").to_string(),
                server_capabilities: vec!["transactions".into()],
                server_timestamp: current_time,
            };

            #[cfg(debug_assertions)]
            tracing::debug!("Hello request handled in {:?}", start.elapsed());

            Ok(response)
        }
        Request::Authenticate { method } => {
            #[cfg(debug_assertions)]
            tracing::debug!("Handling Authenticate request with method: {method:?}");

            let session_id = SESSION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dummy_permissions = BuiltinRole::Root.permissions(Some("default"));
            let response = Response::AuthSuccess {
                session_id,
                user_id: uuid::Uuid::new_v4().to_string(),
                expires_at: None,
                permissions: dummy_permissions,
            };

            #[cfg(debug_assertions)]
            tracing::debug!("Authenticate request handled in {:?}", start.elapsed());

            Ok(response)
        }
        _ => {
            #[cfg(debug_assertions)]
            tracing::debug!("Received unhandled request: {:?}", request);

            let response = Response::Error {
                code: 400,
                details: None,
                message: "Unhandled request type".to_string(),
            };

            #[cfg(debug_assertions)]
            tracing::debug!("Unhandled request processed in {:?}", start.elapsed());

            Ok(response)
        }
    }
}

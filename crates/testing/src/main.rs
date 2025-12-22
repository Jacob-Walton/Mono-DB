use bytes::BytesMut;
use monodb_common::protocol::{AuthMethod, ProtocolDecoder, ProtocolEncoder, Request, Response};
use std::{path::PathBuf, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig as RustlsConfig, RootCertStore},
};

#[derive(Debug)]
struct ClientConfig {
    host: String,
    port: u16,
    tls: bool,
    ca_cert: Option<PathBuf>,
}

async fn run_protocol<S>(
    decoder: &ProtocolDecoder,
    encoder: &ProtocolEncoder,
    mut stream: S,
    client_name: &str,
) -> anyhow::Result<()>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // Send Hello
    let hello_request = encoder.encode_request(&Request::Hello {
        client_name: client_name.into(),
        capabilities: vec!["transactions".into()],
    })?;
    println!("Sending request of size {}", hello_request.len());
    stream.write_all(&hello_request).await?;

    let mut buffer = BytesMut::new();
    loop {
        let read_result = stream.read_buf(&mut buffer).await?;
        println!("Received {} bytes", read_result);
        if let Some((response, _)) = decoder.decode_response(&mut buffer)? {
            match response {
                Response::Welcome {
                    server_version,
                    server_capabilities,
                    server_timestamp,
                } => {
                    println!(
                        "Welcome from server version {}: capabilities={:?}, timestamp={}",
                        server_version, server_capabilities, server_timestamp
                    );
                    break;
                }
                _ => {
                    println!("Received unexpected response: {:?}", response);
                    return Ok(());
                }
            }
        } else if read_result == 0 {
            println!("Failed to decode hello response: incomplete or invalid response");
            return Ok(());
        }
    }

    // Send Authenticate
    let auth_request = encoder.encode_request(&Request::Authenticate {
        method: AuthMethod::Password {
            username: "test_user".into(),
            password: "test_password".into(),
        },
    })?;
    buffer.clear();
    println!("Sending auth request of size {}", auth_request.len());
    stream.write_all(&auth_request).await?;

    loop {
        let read_result = stream.read_buf(&mut buffer).await?;
        println!("Received {} bytes", read_result);
        if let Some((auth_response, _)) = decoder.decode_response(&mut buffer)? {
            match auth_response {
                Response::AuthSuccess {
                    session_id,
                    user_id,
                    permissions,
                    expires_at,
                } => {
                    println!(
                        "Authenticated successfully: session_id={}, user_id={}, permissions={:?}, expires_at={:?}",
                        session_id,
                        user_id,
                        permissions.to_string_array(),
                        expires_at
                    );
                }
                _ => {
                    println!("Received unexpected auth response: {:?}", auth_response);
                }
            }
            break;
        } else if read_result == 0 {
            println!("Failed to decode auth response: incomplete or invalid response");
            break;
        }
    }

    stream.shutdown().await?;
    Ok(())
}

async fn run_tls(
    decoder: &ProtocolDecoder,
    encoder: &ProtocolEncoder,
    tcp: TcpStream,
    cfg: &ClientConfig,
) -> anyhow::Result<()> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};

    let mut roots = RootCertStore::empty();

    if let Some(ca) = &cfg.ca_cert {
        let cert_bytes = std::fs::read(ca)?;
        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(&cert_bytes).collect::<Result<_, _>>()?;

        roots.add_parsable_certificates(certs);
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let tls_cfg = RustlsConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(tls_cfg));
    let server_name = cfg.host.clone().try_into()?;

    let mut stream = connector.connect(server_name, tcp).await?;
    println!("TLS handshake completed");
    run_protocol(decoder, encoder, &mut stream, "MonoDB TLS Test Client").await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let use_tls = args.iter().any(|arg| arg == "--tls");

    let cfg = ClientConfig {
        host: "127.0.0.1".into(),
        port: 6432,
        tls: use_tls,
        ca_cert: Some("certs/cert.pem".into()),
    };

    println!("Connecting to {}:{} (tls={})", cfg.host, cfg.port, cfg.tls);

    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;

    let encoder = ProtocolEncoder::new();
    let decoder = ProtocolDecoder::new();

    if cfg.tls {
        run_tls(&decoder, &encoder, tcp, &cfg).await?;
    } else {
        run_protocol(&decoder, &encoder, tcp, "MonoDB Test Client").await?;
    }

    Ok(())
}

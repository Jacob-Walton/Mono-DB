use std::{path::PathBuf, sync::Arc};
use monodb_common::protocol::{ProtocolDecoder, ProtocolEncoder, Request};
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

async fn run_plain(mut decoder: &ProtocolDecoder, mut encoder: &ProtocolEncoder, mut stream: TcpStream) -> anyhow::Result<()> {
    let hello_request = encoder.encode_request(
        &Request::Hello {
            client_name: "MonoDB Test Client".into(),
            capabilities: vec!["transactions".into()],
        }
    ).expect("Failed to encode request");

    println!("Sending request of size {}", hello_request.len());

    stream.write_all(&hello_request).await?;
    println!("Sent plaintext");

    let mut buf = [0u8; 128];
    let n = stream.read(&mut buf).await?;

    println!("Received: {}", String::from_utf8_lossy(&buf[..n]));
    Ok(())
}

async fn run_tls(mut decoder: &ProtocolDecoder, mut encoder: &ProtocolEncoder, tcp: TcpStream, cfg: &ClientConfig) -> anyhow::Result<()> {
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

    let hello_request = encoder.encode_request(
        &Request::Hello {
            client_name: "MonoDB TLS Test Client".into(),
            capabilities: vec!["transactions".into()],
        }
    ).expect("Failed to encode request");
    println!("Sending request of size {}", hello_request.len());
    stream.write_all(&hello_request).await?;

    let mut buf = [0u8; 128];
    let n = stream.read(&mut buf).await?;

    println!("Received: {}", String::from_utf8_lossy(&buf[..n]));

    // Gracefully shutdown TLS session
    stream.shutdown().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = ClientConfig {
        host: "127.0.0.1".into(),
        port: 6432,
        tls: true,
        ca_cert: Some("certs/ca.crt".into()),
    };

    println!("Connecting to {}:{} (tls={})", cfg.host, cfg.port, cfg.tls);

    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;

    let encoder = ProtocolEncoder::new();
    let decoder = ProtocolDecoder::new();

    if cfg.tls {
        run_tls(&decoder, &encoder, tcp, &cfg).await?;
    } else {
        run_plain(&decoder, &encoder, tcp).await?;
    }

    Ok(())
}

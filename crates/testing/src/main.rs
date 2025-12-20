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

async fn run_plain(mut stream: TcpStream) -> anyhow::Result<()> {
    stream.write_all(b"hello server").await?;
    println!("Sent plaintext");

    let mut buf = [0u8; 128];
    let n = stream.read(&mut buf).await?;

    println!("Received: {}", String::from_utf8_lossy(&buf[..n]));
    Ok(())
}

async fn run_tls(tcp: TcpStream, cfg: &ClientConfig) -> anyhow::Result<()> {
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

    stream.write_all(b"hello tls").await?;

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
        ca_cert: Some("certs/server.crt".into()),
    };

    println!("Connecting to {}:{} (tls={})", cfg.host, cfg.port, cfg.tls);

    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;

    if cfg.tls {
        run_tls(tcp, &cfg).await?;
    } else {
        run_plain(tcp).await?;
    }

    Ok(())
}

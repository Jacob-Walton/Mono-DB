use monodb_common::Result;

use std::sync::Arc;

use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use tokio_rustls::TlsAcceptor;

use monodb_common::MonoError;

use crate::config::TlsConfig;

pub fn load_tls_acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor> {
    // Load certificates from path
    let cert_iter =
        CertificateDer::pem_file_iter(&cfg.cert_path).map_err(|e| MonoError::Io(e.to_string()))?;

    let certs = cert_iter
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| MonoError::Io(e.to_string()))?;

    // Load private key (RSA/PKCS8/EC, etc.) from path
    let key: PrivateKeyDer<'static> =
        PrivateKeyDer::from_pem_file(&cfg.key_path).map_err(|e| MonoError::Io(e.to_string()))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| MonoError::Io(e.to_string()))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

use monodb_common::MonoError;
use monodb_common::Result;

use std::sync::Arc;

use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use tokio_rustls::TlsAcceptor;

use crate::config::TlsConfig;

pub fn load_tls_acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor> {
    // Load certificates
    let cert_iter =
        CertificateDer::pem_file_iter(&cfg.cert_path).map_err(|e| MonoError::TlsCertLoad {
            path: cfg.cert_path.clone(),
            source: e,
        })?;

    let certs = cert_iter
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| MonoError::TlsCertLoad {
            path: cfg.cert_path.clone(),
            source: e,
        })?;

    // Load private key
    let key = PrivateKeyDer::from_pem_file(&cfg.key_path).map_err(|e| MonoError::TlsKeyLoad {
        path: cfg.key_path.clone(),
        source: e,
    })?;

    // Build TLS config
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(MonoError::TlsConfig)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

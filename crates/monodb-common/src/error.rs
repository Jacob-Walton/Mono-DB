//! Error definitions for MonoDB

#[cfg(feature = "tls")]
use std::path::PathBuf;

use thiserror::Error;

/// Represents errors that can occur during MonoDB operations.
///
/// # Exmaple
/// ```rust
/// use monodb_common::MonoError;
///
/// fn example() -> monodb_common::Result<()> {
///     Err(MonoError::NotFound("Item not found".into()))
/// }
///
/// match example() {
///     Ok(_) => println!("Success"),
///     Err(e) => println!("Error occrred: {e}"),
/// }
/// ```
#[derive(Error, Debug)]
pub enum MonoError {
    #[error("IO error: {0}")]
    Io(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Type error: expected {expected}, got {actual}")]
    TypeError { expected: String, actual: String },

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Write conflict: {0}")]
    WriteConflict(String),

    // Storage-specific errors
    #[error("Data corruption in {location}: {details}")]
    Corruption { location: String, details: String },

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Page {0} not found")]
    PageNotFound(u64),

    #[error("Page full, requires split")]
    PageFull,

    #[error("Buffer pool exhausted: {0} pages in use")]
    BufferPoolExhausted(usize),

    #[error("Index corrupted: {0}")]
    IndexCorrupted(String),

    #[error("Disk full: cannot allocate {needed} bytes")]
    DiskFull { needed: u64 },

    #[error(
        "Quota exceeded for namespace '{namespace}': limit {limit}, used {used}, requested {requested}"
    )]
    QuotaExceeded {
        namespace: String,
        limit: u64,
        used: u64,
        requested: u64,
    },

    #[error("Schema mismatch: expected {expected}, found {actual}")]
    SchemaMismatch { expected: String, actual: String },

    // Concurrency errors
    #[error("Lock timeout on resource: {0}")]
    LockTimeout(String),

    #[error("Deadlock detected between transactions: {0:?}")]
    Deadlock(Vec<u64>),

    // TLS
    #[cfg(feature = "tls")]
    #[error("Failed to load TLS certificate from {path}: {source}")]
    TlsCertLoad {
        path: PathBuf,
        source: rustls::pki_types::pem::Error,
    },

    #[cfg(feature = "tls")]
    #[error("Failed to load TLS private key from {path}: {source}")]
    TlsKeyLoad {
        path: PathBuf,
        source: rustls::pki_types::pem::Error,
    },

    #[cfg(feature = "tls")]
    #[error("Invalid TLS configuration: {0}")]
    TlsConfig(rustls::Error),
}

pub type Result<T> = std::result::Result<T, MonoError>;

impl MonoError {
    /// Get the inner message without the type prefix.
    /// Useful when re-wrapping errors to avoid "Invalid operation: Invalid operation: ..."
    pub fn message(&self) -> String {
        match self {
            MonoError::Io(msg) => msg.clone(),
            MonoError::Storage(msg) => msg.clone(),
            MonoError::Parse(msg) => msg.clone(),
            MonoError::Execution(msg) => msg.clone(),
            MonoError::Network(msg) => msg.clone(),
            MonoError::TypeError {
                expected,
                actual: _,
            } => expected.clone(), // Partial, but acceptable
            MonoError::NotFound(msg) => msg.clone(),
            MonoError::AlreadyExists(msg) => msg.clone(),
            MonoError::InvalidOperation(msg) => msg.clone(),
            MonoError::Transaction(msg) => msg.clone(),
            MonoError::Config(msg) => msg.clone(),
            MonoError::WriteConflict(msg) => msg.clone(),
            MonoError::Corruption { details, .. } => details.clone(),
            MonoError::Wal(msg) => msg.clone(),
            MonoError::ChecksumMismatch { expected, actual } => {
                format!("expected {expected:#010x}, got {actual:#010x}")
            }
            MonoError::PageNotFound(id) => format!("page {id}"),
            MonoError::PageFull => "page full".to_string(),
            MonoError::BufferPoolExhausted(count) => format!("{count} pages in use"),
            MonoError::IndexCorrupted(msg) => msg.clone(),
            MonoError::DiskFull { needed } => format!("{needed} bytes needed"),
            MonoError::QuotaExceeded { namespace, .. } => namespace.clone(),
            MonoError::SchemaMismatch { expected, .. } => expected.clone(),
            MonoError::LockTimeout(msg) => msg.clone(),
            MonoError::Deadlock(txs) => format!("{txs:?}"),
            #[cfg(feature = "tls")]
            MonoError::TlsCertLoad { source, .. } => source.to_string(),
            #[cfg(feature = "tls")]
            MonoError::TlsKeyLoad { source, .. } => source.to_string(),
            #[cfg(feature = "tls")]
            MonoError::TlsConfig(err) => err.to_string(),
        }
    }

    /// Get a short error kind name
    pub fn kind(&self) -> &'static str {
        match self {
            MonoError::Io(_) => "io_error",
            MonoError::Storage(_) => "storage_error",
            MonoError::Parse(_) => "parse_error",
            MonoError::Execution(_) => "execution_error",
            MonoError::Network(_) => "network_error",
            MonoError::TypeError { .. } => "type_error",
            MonoError::NotFound(_) => "not_found",
            MonoError::AlreadyExists(_) => "already_exists",
            MonoError::InvalidOperation(_) => "invalid_operation",
            MonoError::Transaction(_) => "transaction_error",
            MonoError::Config(_) => "config_error",
            MonoError::WriteConflict(_) => "write_conflict",
            MonoError::Corruption { .. } => "corruption",
            MonoError::Wal(_) => "wal_error",
            MonoError::ChecksumMismatch { .. } => "checksum_mismatch",
            MonoError::PageNotFound(_) => "page_not_found",
            MonoError::PageFull => "page_full",
            MonoError::BufferPoolExhausted(_) => "buffer_pool_exhausted",
            MonoError::IndexCorrupted(_) => "index_corrupted",
            MonoError::DiskFull { .. } => "disk_full",
            MonoError::QuotaExceeded { .. } => "quota_exceeded",
            MonoError::SchemaMismatch { .. } => "schema_mismatch",
            MonoError::LockTimeout(_) => "lock_timeout",
            MonoError::Deadlock(_) => "deadlock",
            #[cfg(feature = "tls")]
            MonoError::TlsCertLoad { .. } => "tls_cert_load_error",
            #[cfg(feature = "tls")]
            MonoError::TlsKeyLoad { .. } => "tls_key_load_error",
            #[cfg(feature = "tls")]
            MonoError::TlsConfig(_) => "tls_config_error",
        }
    }
}

/// Convert std::io::Error to MonoError
///
/// Shortcut as it's a common error we need
/// to convert from.
impl From<std::io::Error> for MonoError {
    fn from(err: std::io::Error) -> Self {
        MonoError::Io(err.to_string())
    }
}

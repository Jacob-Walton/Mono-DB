//! Error definitions for MonoDB

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
#[derive(Error, Debug, Clone)]
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
}

pub type Result<T> = std::result::Result<T, MonoError>;

impl MonoError {
    /// Get the inner message without the type prefix.
    /// Useful when re-wrapping errors to avoid "Invalid operation: Invalid operation: ..."
    pub fn message(&self) -> &str {
        match self {
            MonoError::Io(msg) => msg,
            MonoError::Storage(msg) => msg,
            MonoError::Parse(msg) => msg,
            MonoError::Execution(msg) => msg,
            MonoError::Network(msg) => msg,
            MonoError::TypeError {
                expected,
                actual: _,
            } => expected, // Partial, but acceptable
            MonoError::NotFound(msg) => msg,
            MonoError::AlreadyExists(msg) => msg,
            MonoError::InvalidOperation(msg) => msg,
            MonoError::Transaction(msg) => msg,
            MonoError::Config(msg) => msg,
            MonoError::WriteConflict(msg) => msg,
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

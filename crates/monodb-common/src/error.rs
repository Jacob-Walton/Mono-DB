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
}

pub type Result<T> = std::result::Result<T, MonoError>;

/// Convert std::io::Error to MonoError
///
/// Shortcut as it's a common error we need
/// to convert from.
impl From<std::io::Error> for MonoError {
    fn from(err: std::io::Error) -> Self {
        MonoError::Io(err.to_string())
    }
}

use thiserror::Error;

pub type MonoResult<T> = Result<T, MonoError>;

#[derive(Error, Debug)]
pub enum MonoError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Parser error: {0}")]
    Parser(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Connection limit reached")]
    ConnectionLimit,

    #[error("Operation timed out")]
    Timeout,

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Out of memory")]
    OutOfMemory,
}

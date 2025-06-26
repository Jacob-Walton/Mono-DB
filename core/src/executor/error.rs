use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutionError {
	#[error("Table '{0}' not found")]
	TableNotFound(String),

	#[error("Column '{0}' not found")]
	ColumnNotFound(String),

	#[error("Type mismatch: expected {expected}, got {actual}")]
	TypeMismatch { expected: String, actual: String },

	#[error("Invalid expression: {0}")]
	InvalidExpression(String),

	#[error("Storage error: {0}")]
	StorageError(String),

	#[error("Not implemented: {0}")]
	NotImplemented(String),
}

pub type ExecutionResult<T> = Result<T, ExecutionError>;

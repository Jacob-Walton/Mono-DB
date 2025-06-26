use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("Parse error at position {pos}: {msg}")]
	ParseError { pos: usize, msg: String },

	#[error("Invalid syntax: {0}")]
	SyntaxError(String),

	#[error("Compilation error: {0}")]
	CompilationError(String),
}

pub type Result<T> = std::result::Result<T, Error>;

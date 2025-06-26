use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
	#[error("Unexpected character '{0}' at position {1}")]
	UnexpectedChar(char, usize),

	#[error("Unterminated string starting at position {0}")]
	UnterminatedString(usize),

	#[error("Invalid escape sequence at position {0}")]
	InvalidEscape(usize),

	#[error("Invalid number at position {0}")]
	InvalidNumber(usize),

	#[error("Expected {expected}, found {found} at position {position}")]
	UnexpectedToken {
		expected: String,
		found: String,
		position: usize,
	},

	#[error("Unexpected end of input")]
	UnexpectedEof,

	#[error("{0}")]
	SyntaxError(String),
}

pub type ParseResult<T> = Result<T, ParseError>;

mod error;
mod lexer;
mod parser;

pub use error::{ParseError, ParseResult};
pub use parser::Parser;

use crate::nsql::interner::StringInterner;
use crate::nsql::{Result, ast::Program};

pub fn parse(input: &str, interner: &mut StringInterner) -> Result<Program> {
	let parser = Parser::new(input, interner).map_err(|e| crate::nsql::Error::ParseError {
		pos: 0,
		msg: e.to_string(),
	})?;

	parser.parse().map_err(|e| crate::nsql::Error::ParseError {
		pos: 0,
		msg: e.to_string(),
	})
}

use crate::nsql::{
	interner::{Intern, InternerId, StringInterner},
	parser::{ParseError, ParseResult},
};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Basic SQL Keywords
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Delete,
    Create,
    Drop,
    Table,
    Set,
    Describe,

	// Data types
	Integer,
	Text,
	Boolean,
	Varchar,
	Char,
	Decimal,

	// Constraints
	Primary,
	Key,
	Not,
	Null,

	// Operators
	And,
	Or,

	// Literals
	IntLiteral(i64),
	StringLiteral(InternerId),
	True,
	False,

	// Identifiers
	Identifier(InternerId),

	// Symbols
	LeftParen,    // (
	RightParen,   // )
	Comma,        // ,
	Semicolon,    // ;
	Dot,          // .
	Star,         // *
	Equal,        // =
	NotEqual,     // !=
	Less,         // <
	Greater,      // >
	LessEqual,    // <=
	GreaterEqual, // >=

	// Special
	Eof,
}

pub struct Lexer<'a> {
	input: &'a str,
	position: usize,
	current_char: Option<char>,
	interner: &'a mut StringInterner,
}

impl<'a> Lexer<'a> {
	pub fn new(input: &'a str, interner: &'a mut StringInterner) -> Self {
		let mut lexer = Self {
			input,
			position: 0,
			current_char: None,
			interner,
		};
		lexer.current_char = lexer.input.chars().next();
		lexer
	}

	pub fn next(&mut self) -> ParseResult<Token> {
		self.skip_whitespace();

		match self.current_char {
			None => Ok(Token::Eof),
			Some(ch) => match ch {
				'(' => {
					self.advance();
					Ok(Token::LeftParen)
				}
				')' => {
					self.advance();
					Ok(Token::RightParen)
				}
				',' => {
					self.advance();
					Ok(Token::Comma)
				}
				';' => {
					self.advance();
					Ok(Token::Semicolon)
				}
				'.' => {
					self.advance();
					Ok(Token::Dot)
				}
				'*' => {
					self.advance();
					Ok(Token::Star)
				}
				'=' => {
					self.advance();
					Ok(Token::Equal)
				}
				'!' => {
					self.advance();
					if self.current_char == Some('=') {
						self.advance();
						Ok(Token::NotEqual)
					} else {
						Err(ParseError::UnexpectedChar('!', self.position))
					}
				}
				'<' => {
					self.advance();
					if self.current_char == Some('=') {
						self.advance();
						Ok(Token::LessEqual)
					} else {
						Ok(Token::Less)
					}
				}
				'>' => {
					self.advance();
					if self.current_char == Some('=') {
						self.advance();
						Ok(Token::GreaterEqual)
					} else {
						Ok(Token::Greater)
					}
				}
				'\'' | '"' => self.read_string(ch),
				'0'..='9' => self.read_number(),
				'a'..='z' | 'A'..='Z' | '_' => self.read_identifier(),
				_ => Err(ParseError::UnexpectedChar(ch, self.position)),
			},
		}
	}

	fn advance(&mut self) {
		self.position += 1;
		self.current_char = self.input.chars().nth(self.position);
	}

	fn skip_whitespace(&mut self) {
		while let Some(ch) = self.current_char {
			if ch.is_whitespace() {
				self.advance();
			} else {
				break;
			}
		}
	}

	fn read_string(&mut self, quote: char) -> ParseResult<Token> {
		let start = self.position;
		self.advance(); // skip opening quote

		let mut value = String::new();
		while self.current_char.is_some() && self.current_char != Some(quote) {
			if self.current_char == Some('\\') {
				self.advance();
				match self.current_char {
					Some('n') => value.push('\n'),
					Some('t') => value.push('\t'),
					Some('\\') => value.push('\\'),
					Some(q) if q == quote => value.push(q),
					_ => return Err(ParseError::InvalidEscape(self.position)),
				}
			} else {
				value.push(self.current_char.unwrap());
			}
			self.advance();
		}

		if self.current_char != Some(quote) {
			return Err(ParseError::UnterminatedString(start));
		}

		self.advance(); // skip closing quote
		let id = value.intern(self.interner);
		Ok(Token::StringLiteral(id))
	}

	fn read_number(&mut self) -> ParseResult<Token> {
		let start = self.position;

		while let Some(ch) = self.current_char {
			if ch.is_ascii_digit() {
				self.advance();
			} else {
				break;
			}
		}

		let text = &self.input[start..self.position];
		match text.parse::<i64>() {
			Ok(n) => Ok(Token::IntLiteral(n)),
			Err(_) => Err(ParseError::InvalidNumber(start)),
		}
	}

	fn read_identifier(&mut self) -> ParseResult<Token> {
		let start = self.position;

		while let Some(ch) = self.current_char {
			if ch.is_alphanumeric() || ch == '_' {
				self.advance();
			} else {
				break;
			}
		}

		let text = &self.input[start..self.position];
		let token = self.classify_identifier(text);
		Ok(token)
	}

    fn classify_identifier(&mut self, text: &str) -> Token {
        let upper = text.to_uppercase();
        match upper.as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "UPDATE" => Token::Update,
            "DELETE" => Token::Delete,
            "CREATE" => Token::Create,
            "DROP" => Token::Drop,
            "TABLE" => Token::Table,
            "SET" => Token::Set,
            "DESCRIBE" => Token::Describe,
            "INTEGER" | "INT" => Token::Integer,
            "TEXT" => Token::Text,
            "BOOLEAN" | "BOOL" => Token::Boolean,
            "VARCHAR" => Token::Varchar,
            "CHAR" => Token::Char,
            "DECIMAL" => Token::Decimal,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "NOT" => Token::Not,
            "NULL" => Token::Null,
            "AND" => Token::And,
            "OR" => Token::Or,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            _ => {
                let id = text.intern(self.interner);
                Token::Identifier(id)
            }
        }
    }
}

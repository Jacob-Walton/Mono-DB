use memchr::memchr;
use smallvec::SmallVec;
use std::{cell::RefCell, ops::Range};

include!(concat!(env!("OUT_DIR"), "/keywords.rs"));

thread_local! {
    static LEXER: RefCell<Lexer> = RefCell::new(Lexer::new());
}

/// Token kinds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TokenKind {
    // Literals and identifiers
    Keyword = 0,
    Identifier = 1,
    Variable = 2, // $variable_name
    StringLiteral = 3,
    Number = 4,

    // Delimiters
    LeftParen = 5,
    RightParen = 6,
    LeftBracket = 7,
    RightBracket = 8,
    LeftBrace = 9,
    RightBrace = 10,

    // Operators
    Comma = 11,
    Equals = 12,
    Dot = 13,
    Colon = 14,
    PlusEquals = 15,
    MinusEquals = 16,
    LessThan = 17,
    GreaterThan = 18,
    LessEqual = 19,
    GreaterEqual = 20,
    NotEqual = 21,
    Plus = 22,
    Minus = 23,
    Star = 24,
    Slash = 25,

    // Pipeline and Control Flow
    Pipe = 26,             // |
    Semicolon = 27,        // ;
    Question = 28,         // ?
    QuestionQuestion = 29, // ??
    PipePipe = 30,         // ||
    Dollar = 31,           // $

    // Special
    Newline = 32,
    Indent = 33,
    Dedent = 34,
    Eof = 35,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: u32,  // Byte offset
    pub end: u32,    // Byte offset
    pub line: u32,   // Line number (1-based)
    pub column: u16, // Column number (1-based)
}

impl Span {
    #[inline]
    pub fn new(start: u32, end: u32, line: u32, column: u16) -> Self {
        Self {
            start,
            end,
            line,
            column,
        }
    }

    #[inline]
    pub fn as_range(&self) -> Range<usize> {
        self.start as usize..self.end as usize
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

pub struct Lexer {
    source: Vec<u8>,
    tokens: Vec<Token>,
    pos: usize,
    line: u32,
    column: u16,
    indent_stack: SmallVec<[u16; 16]>,
    pending_dedents: u8,
    paren_depth: usize,
}

impl Default for Lexer {
    fn default() -> Self {
        Self::new()
    }
}

impl Lexer {
    pub fn new() -> Self {
        let mut lexer = Self {
            source: Vec::with_capacity(4096),
            tokens: Vec::with_capacity(4096),
            pos: 0,
            line: 1,
            column: 1,
            indent_stack: SmallVec::new(),
            pending_dedents: 0,
            paren_depth: 0,
        };

        lexer.indent_stack.push(0); // Initial indent level
        lexer
    }

    pub fn lex(&mut self, source: &[u8]) -> Vec<Token> {
        self.source.clear();
        self.source.extend_from_slice(source);
        self.tokens.clear();

        self.pos = 0;
        self.line = 1;
        self.column = 1;
        self.indent_stack.clear();
        self.indent_stack.push(0);
        self.pending_dedents = 0;

        while self.pos < self.source.len() {
            while self.pending_dedents > 0 {
                self.tokens.push(Token {
                    kind: TokenKind::Dedent,
                    span: self.current_span(0),
                });
                self.pending_dedents -= 1;
            }

            let byte = self.source[self.pos];
            match byte {
                b'a'..=b'z' | b'A'..=b'Z' | b'_' => self.scan_identifier_or_keyword(),
                b'$' => self.scan_variable(),
                b'0'..=b'9' => self.scan_number(),
                b'"' => self.scan_string(),
                b'\n' => self.handle_newline(),
                b' ' | b'\t' | b'\r' => self.skip_whitespace(),
                // treat '//' (and thus '///' and '//!') as line comments
                b'/' => {
                    if self.pos + 1 < self.source.len() && self.source[self.pos + 1] == b'/' {
                        self.skip_comment();
                    } else {
                        self.scan_operator_or_delimiter();
                    }
                }
                _ => self.scan_operator_or_delimiter(),
            }
        }

        // Emit any pending dedents that weren't emitted in the main loop
        while self.pending_dedents > 0 {
            self.tokens.push(Token {
                kind: TokenKind::Dedent,
                span: self.current_span(0),
            });
            self.pending_dedents -= 1;
        }

        // Emit a final newline if last token wasn't a newline and we have indentation
        let needs_newline = !self.tokens.is_empty()
            && self.tokens.last().unwrap().kind != TokenKind::Newline
            && self.indent_stack.len() > 1
            && self.paren_depth == 0;

        if needs_newline {
            self.tokens.push(Token {
                kind: TokenKind::Newline,
                span: self.current_span(0),
            });
        }

        // Emit remaining dedents at EOF
        while self.indent_stack.len() > 1 {
            self.indent_stack.pop();
            self.tokens.push(Token {
                kind: TokenKind::Dedent,
                span: self.current_span(0),
            });
        }

        // Add EOF token
        self.tokens.push(Token {
            kind: TokenKind::Eof,
            span: self.current_span(0),
        });

        self.tokens.clone()
    }

    #[inline]
    fn current_span(&self, len: usize) -> Span {
        Span::new(
            self.pos as u32,
            (self.pos + len) as u32,
            self.line,
            self.column,
        )
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.source.len() {
            match self.source[self.pos] {
                b' ' => {
                    self.pos += 1;
                    self.column += 1;
                }
                b'\t' => {
                    self.pos += 1;
                    self.column += 4;
                }
                b'\r' => {
                    self.pos += 1;
                }
                _ => break,
            }
        }
    }

    fn skip_comment(&mut self) {
        // Skip from the current position up to (but not including) the next newline.
        if self.pos >= self.source.len() {
            return;
        }

        // If somehow called on a single '/', and not a comment, don't advance.
        if self.source[self.pos] == b'/'
            && (self.pos + 1 >= self.source.len() || self.source[self.pos + 1] != b'/')
        {
            return;
        }
        // Fall through and search for newline from current pos

        if let Some(len) = memchr(b'\n', &self.source[self.pos..]) {
            self.pos += len;
        } else {
            self.pos = self.source.len();
        }
    }

    fn handle_newline(&mut self) {
        if self.paren_depth > 0 {
            self.pos += 1;
            self.line += 1;
            self.column = 1;
            return;
        }

        let span = self.current_span(1);
        self.tokens.push(Token {
            kind: TokenKind::Newline,
            span,
        });

        self.pos += 1;
        self.line += 1;
        self.column = 1;

        // Count indentation
        let mut indent = 0u16;
        while self.pos < self.source.len() {
            match self.source[self.pos] {
                b' ' => {
                    self.pos += 1;
                    indent += 1;
                }
                b'\t' => {
                    self.pos += 1;
                    indent += 4;
                }
                b'\r' => {
                    self.pos += 1;
                }
                _ => break,
            }
        }
        self.column = indent + 1;

        // Skip blank lines and lines with only comments
        while self.pos < self.source.len() {
            if self.source[self.pos] == b'\n' {
                // Another blank line, consume it
                self.pos += 1;
                self.line += 1;

                // Reset indent count for the new line
                indent = 0;
                while self.pos < self.source.len() {
                    match self.source[self.pos] {
                        b' ' => {
                            self.pos += 1;
                            indent += 1;
                        }
                        b'\t' => {
                            self.pos += 1;
                            indent += 4;
                        }
                        b'\r' => {
                            self.pos += 1;
                        }
                        _ => break,
                    }
                }
                self.column = indent + 1;
            } else if self.pos + 1 < self.source.len()
                && self.source[self.pos] == b'/'
                && self.source[self.pos + 1] == b'/'
            {
                // Line with only a comment
                self.skip_comment();
                if self.pos < self.source.len() && self.source[self.pos] == b'\n' {
                    continue; // Will process the newline in next iteration
                } else {
                    break; // Comment goes to EOF
                }
            } else {
                break;
            }
        }

        // Handle indent/dedent
        let current_indent = *self.indent_stack.last().unwrap();
        if indent > current_indent {
            self.indent_stack.push(indent);
            self.tokens.push(Token {
                kind: TokenKind::Indent,
                span: self.current_span(0),
            });
        } else if indent < current_indent {
            while self.indent_stack.len() > 1 && *self.indent_stack.last().unwrap() > indent {
                self.indent_stack.pop();
                self.pending_dedents += 1;
            }
        }
    }

    fn scan_identifier_or_keyword(&mut self) {
        let start = self.pos;
        let start_column = self.column;

        let len = self.source[self.pos..]
            .iter()
            .take_while(|&&c| c.is_ascii_alphanumeric() || c == b'_')
            .count();
        self.pos += len;
        self.column += len as u16;

        let text = &self.source[start..self.pos];
        let kind = KEYWORDS.get(text).cloned().unwrap_or(TokenKind::Identifier);

        self.tokens.push(Token {
            kind,
            span: Span::new(start as u32, self.pos as u32, self.line, start_column),
        });
    }

    fn scan_variable(&mut self) {
        let start = self.pos;
        let start_column = self.column;

        // Skip the '$' symbol
        self.pos += 1;
        self.column += 1;

        // Scan the variable name (must start with letter or underscore)
        if self.pos < self.source.len()
            && (self.source[self.pos].is_ascii_alphabetic() || self.source[self.pos] == b'_')
        {
            let len = self.source[self.pos..]
                .iter()
                .take_while(|&&c| c.is_ascii_alphanumeric() || c == b'_')
                .count();
            self.pos += len;
            self.column += len as u16;
        }

        self.tokens.push(Token {
            kind: TokenKind::Variable,
            span: Span::new(start as u32, self.pos as u32, self.line, start_column),
        });
    }

    fn scan_number(&mut self) {
        let start = self.pos;
        let start_column = self.column;

        let len = self.source[self.pos..]
            .iter()
            .take_while(|&&c| c.is_ascii_digit())
            .count();
        self.pos += len;
        self.column += len as u16;

        if self.pos + 1 < self.source.len()
            && self.source[self.pos] == b'.'
            && self.source[self.pos + 1].is_ascii_digit()
        {
            self.pos += 1;
            self.column += 1;
            let len = self.source[self.pos..]
                .iter()
                .take_while(|&&c| c.is_ascii_digit())
                .count();
            self.pos += len;
            self.column += len as u16;
        }

        self.tokens.push(Token {
            kind: TokenKind::Number,
            span: Span::new(start as u32, self.pos as u32, self.line, start_column),
        });
    }

    fn scan_string(&mut self) {
        let start = self.pos;
        let start_column = self.column;
        self.pos += 1; // Skip opening quote
        self.column += 1;

        while self.pos < self.source.len() {
            if let Some(len) = memchr(b'"', &self.source[self.pos..]) {
                let mut i = self.pos + len;
                let mut backslashes = 0;
                while i > self.pos && self.source[i - 1] == b'\\' {
                    backslashes += 1;
                    i -= 1;
                }
                if backslashes % 2 == 0 {
                    self.pos += len + 1;
                    self.column += (len + 1) as u16;
                    break;
                }
                self.pos += len + 1;
                self.column += (len + 1) as u16;
            } else {
                self.pos = self.source.len();
                break;
            }
        }

        self.tokens.push(Token {
            kind: TokenKind::StringLiteral,
            span: Span::new(start as u32, self.pos as u32, self.line, start_column),
        });
    }

    fn scan_operator_or_delimiter(&mut self) {
        let start_column = self.column;
        let byte = self.source[self.pos];

        let (kind, len, adjust_paren) = match byte {
            b'(' => (TokenKind::LeftParen, 1, 1isize),
            b')' => (TokenKind::RightParen, 1, -1isize),
            b'[' => (TokenKind::LeftBracket, 1, 1isize),
            b']' => (TokenKind::RightBracket, 1, -1isize),
            b'{' => (TokenKind::LeftBrace, 1, 1isize),
            b'}' => (TokenKind::RightBrace, 1, -1isize),
            b',' => (TokenKind::Comma, 1, 0isize),
            b'.' => (TokenKind::Dot, 1, 0isize),
            b':' => (TokenKind::Colon, 1, 0isize),
            b'+' => {
                if self.pos + 1 < self.source.len() && self.source[self.pos + 1] == b'=' {
                    (TokenKind::PlusEquals, 2, 0isize)
                } else {
                    (TokenKind::Plus, 1, 0isize)
                }
            }
            b'-' => {
                if self.pos + 1 < self.source.len() && self.source[self.pos + 1] == b'=' {
                    (TokenKind::MinusEquals, 2, 0isize)
                } else {
                    (TokenKind::Minus, 1, 0isize)
                }
            }
            b'*' => (TokenKind::Star, 1, 0isize),
            b'/' => (TokenKind::Slash, 1, 0isize),
            b'=' => (TokenKind::Equals, 1, 0isize),
            b'<' => {
                if self.pos + 1 < self.source.len() && self.source[self.pos + 1] == b'=' {
                    (TokenKind::LessEqual, 2, 0isize)
                } else {
                    (TokenKind::LessThan, 1, 0isize)
                }
            }
            b'>' => {
                if self.pos + 1 < self.source.len() && self.source[self.pos + 1] == b'=' {
                    (TokenKind::GreaterEqual, 2, 0isize)
                } else {
                    (TokenKind::GreaterThan, 1, 0isize)
                }
            }
            b'!' => {
                if self.pos + 1 < self.source.len() && self.source[self.pos + 1] == b'=' {
                    (TokenKind::NotEqual, 2, 0isize)
                } else {
                    self.pos += 1;
                    self.column += 1;
                    return;
                }
            }
            b'|' => {
                if self.pos + 1 < self.source.len() && self.source[self.pos + 1] == b'|' {
                    (TokenKind::PipePipe, 2, 0isize)
                } else {
                    (TokenKind::Pipe, 1, 0isize)
                }
            }
            b';' => (TokenKind::Semicolon, 1, 0isize),
            b'?' => {
                if self.pos + 1 < self.source.len() && self.source[self.pos + 1] == b'?' {
                    (TokenKind::QuestionQuestion, 2, 0isize)
                } else {
                    (TokenKind::Question, 1, 0isize)
                }
            }
            _ => {
                self.pos += 1;
                self.column += 1;
                return;
            }
        };

        if adjust_paren > 0 {
            self.paren_depth = self.paren_depth.saturating_add(adjust_paren as usize);
        }

        let span = Span::new(
            self.pos as u32,
            (self.pos + len) as u32,
            self.line,
            start_column,
        );
        self.tokens.push(Token { kind, span });
        self.pos += len;
        self.column += len as u16;

        if adjust_paren < 0 {
            self.paren_depth = self.paren_depth.saturating_sub((-adjust_paren) as usize);
        }
    }
}

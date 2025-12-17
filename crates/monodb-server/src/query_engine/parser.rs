use std::collections::{BTreeMap, HashSet};

use annotate_snippets::renderer::DecorStyle;
use monodb_common::{Value, ValueType};

use crate::query_engine::{
    ast::{
        Assignment, BinaryOp, Expr, Extension, FieldDef, OrderByField, SortDirection, Statement,
        TableProperty, TableType,
    },
    lexer::{Lexer, Span, Token, TokenKind},
};

/// Severity level for diagnostics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
    Note,
}

/// A suggestion for fixing an issue
#[derive(Debug, Clone)]
pub struct Suggestion {
    pub message: String,
    pub replacement: String,
}

/// A diagnostic message
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub severity: Severity,
    pub message: String,
    pub span: Span,
    pub notes: Vec<String>,
    pub suggestions: Vec<Suggestion>,
}

impl Diagnostic {
    pub fn error(message: impl Into<String>, span: Span) -> Self {
        Self {
            severity: Severity::Error,
            message: message.into(),
            span,
            notes: Vec::new(),
            suggestions: Vec::new(),
        }
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    pub fn with_suggestion(
        mut self,
        msg: impl Into<String>,
        replacement: impl Into<String>,
    ) -> Self {
        self.suggestions.push(Suggestion {
            message: msg.into(),
            replacement: replacement.into(),
        });
        self
    }

    pub fn render(&self, source: &str) -> String {
        use annotate_snippets::*;

        let err_start = self.span.start as usize;
        let err_end = self.span.end as usize;

        // Compute byte ranges per line
        let mut lines = Vec::new();
        let mut cur = 0;
        for l in source.split_inclusive('\n') {
            let end = cur + l.len();
            lines.push((cur, end));
            cur = end;
        }
        if !source.ends_with('\n') {
            lines.push((cur, source.len()));
        }

        // Pick context window around the error line
        let line_idx = self.span.line.saturating_sub(1) as usize;
        let before = 2;
        let after = 2;
        let from = line_idx.saturating_sub(before);
        let to = (line_idx + after).min(lines.len().saturating_sub(1));

        let vis_start = lines[from].0;
        let vis_end = lines[to].1;

        let snippet = Snippet::source(source)
            .line_start(1)
            .annotation(
                AnnotationKind::Primary
                    .span(err_start..err_end)
                    .label(self.message.clone()),
            )
            .annotation(AnnotationKind::Visible.span(vis_start..vis_end));

        let report = &[Level::ERROR
            .primary_title("failed to parse statement")
            .element(snippet)];

        let renderer = Renderer::styled().decor_style(DecorStyle::Ascii);

        renderer.render(report)
    }
}

pub type ParseResult<T> = Result<T, Diagnostic>;

/// Recursive descent parser
pub struct Parser<'a> {
    tokens: &'a [Token],
    source: &'a [u8],
    pos: usize,
    errors: Vec<Diagnostic>,
}

impl<'a> Parser<'a> {
    /// Create new parser with given tokens
    pub fn new(tokens: &'a [Token], source: &'a [u8]) -> Self {
        Self {
            tokens,
            source,
            pos: 0,
            errors: Vec::new(),
        }
    }

    // Token management methods

    #[inline]
    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    #[inline]
    fn peek(&self) -> Option<TokenKind> {
        self.current().map(|t| t.kind)
    }

    #[inline]
    fn advance(&mut self) -> Option<&Token> {
        if self.pos < self.tokens.len() {
            let token = &self.tokens[self.pos];
            self.pos += 1;
            Some(token)
        } else {
            None
        }
    }

    #[inline]
    fn is_at_end(&self) -> bool {
        matches!(self.peek(), Some(TokenKind::Eof) | None)
    }

    #[inline]
    fn check(&self, kind: TokenKind) -> bool {
        self.peek() == Some(kind)
    }

    #[inline]
    fn check_keyword(&self, keyword: &str) -> bool {
        if let Some(token) = self.current()
            && token.kind == TokenKind::Keyword
        {
            let text = self.token_text(token);
            return text == keyword.as_bytes();
        }
        false
    }

    #[inline]
    fn token_text(&self, token: &Token) -> &'a [u8] {
        &self.source[token.span.as_range()]
    }

    #[inline]
    fn token_string(&self, token: &Token) -> String {
        String::from_utf8_lossy(self.token_text(token)).into_owned()
    }

    fn skip_newlines(&mut self) {
        while self.check(TokenKind::Newline) {
            self.advance();
        }
    }

    /// Shortcut to create a new Diagnostic from Parser
    fn error(&self, message: impl Into<String>) -> Diagnostic {
        let span = self.current().map(|t| t.span).unwrap_or(Span::new(
            self.source.len() as u32,
            self.source.len() as u32,
            1,
            1,
        ));
        Diagnostic::error(message, span)
    }

    fn expect(&mut self, kind: TokenKind) -> ParseResult<&Token> {
        match self.current() {
            Some(token) if token.kind == kind => Ok(self.advance().unwrap()),
            Some(token) => Err(self.error(format!(
                "expected {}, found {}",
                self.describe_kind(kind),
                self.describe_token(token)
            ))),
            None => Err(self.error(format!(
                "expected {}, but reached end of input",
                self.describe_kind(kind)
            ))),
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> ParseResult<()> {
        if self.check_keyword(keyword) {
            self.advance();
            Ok(())
        } else {
            match self.current() {
                Some(token) => Err(self.error(format!(
                    "expected keyword '{}', found {}",
                    keyword,
                    self.describe_token(token)
                ))),
                None => Err(self.error(format!(
                    "expected keyword '{keyword}', but reached end of input"
                ))),
            }
        }
    }

    fn expect_identifier(&mut self) -> ParseResult<String> {
        match self.current() {
            Some(token) if token.kind == TokenKind::Identifier => {
                let name = self.token_string(token);
                self.advance();
                Ok(name)
            }
            Some(token) if token.kind == TokenKind::Keyword => {
                let text = self.token_string(token);

                // Some keywords may be identifiers
                if matches!(text.as_str(), "key" | "value" | "data" | "ttl") {
                    self.advance();
                    Ok(text)
                } else {
                    Err(self
                        .error(format!("cannot use keyword '{text}' as identifier"))
                        .with_suggestion("use a different name", format!("{text}_field")))
                }
            }
            Some(token) => Err(self.error(format!(
                "expected identifier, found {}",
                self.describe_token(token)
            ))),
            None => Err(self.error("expected identifier, but reached end of input")),
        }
    }

    fn describe_kind(&self, kind: TokenKind) -> &'static str {
        match kind {
            TokenKind::Keyword => "keyword",
            TokenKind::Identifier => "identifier",
            TokenKind::StringLiteral => "string",
            TokenKind::Number => "number",
            TokenKind::LeftParen => "'('",
            TokenKind::RightParen => "')'",
            TokenKind::LeftBracket => "'['",
            TokenKind::RightBracket => "']'",
            TokenKind::LeftBrace => "'{'",
            TokenKind::RightBrace => "'}'",
            TokenKind::Comma => "','",
            TokenKind::Equals => "'='",
            TokenKind::Newline => "newline",
            TokenKind::Indent => "indent",
            TokenKind::Dedent => "dedent",
            _ => "token",
        }
    }

    fn describe_token(&self, token: &Token) -> String {
        match token.kind {
            TokenKind::Keyword | TokenKind::Identifier => {
                format!(
                    "{} '{}'",
                    self.describe_kind(token.kind),
                    self.token_string(token)
                )
            }
            TokenKind::StringLiteral => {
                let s = self.token_string(token);
                format!("string \"{}\"", &s[1..s.len() - 1])
            }
            TokenKind::Number => {
                format!("number {}", self.token_string(token))
            }
            _ => self.describe_kind(token.kind).to_string(),
        }
    }

    fn synchronize(&mut self) {
        while !self.is_at_end() {
            if self.check(TokenKind::Newline) {
                self.advance();
                if self.check_keyword("get")
                    || self.check_keyword("put")
                    || self.check_keyword("change")
                    || self.check_keyword("remove")
                    || self.check_keyword("make")
                    || self.check_keyword("begin")
                    || self.check_keyword("commit")
                    || self.check_keyword("rollback")
                {
                    return;
                }
            }

            self.advance();
        }
    }

    // Parsing

    pub fn parse(&mut self) -> Result<Vec<Statement>, Vec<Diagnostic>> {
        let mut statements = Vec::new();

        while !self.is_at_end() {
            self.skip_newlines();

            if self.is_at_end() {
                break;
            }

            match self.parse_statement() {
                Ok(stmt) => statements.push(stmt),
                Err(e) => {
                    self.errors.push(e);
                    self.synchronize();
                }
            }
        }

        if self.errors.is_empty() {
            Ok(statements)
        } else {
            Err(self.errors.clone())
        }
    }

    fn parse_statement(&mut self) -> ParseResult<Statement> {
        match self.peek() {
            Some(TokenKind::Keyword) => {
                if self.check_keyword("get") {
                    self.parse_get()
                } else if self.check_keyword("put") {
                    self.parse_put()
                } else if self.check_keyword("change") {
                    self.parse_change()
                } else if self.check_keyword("remove") {
                    self.parse_remove()
                } else if self.check_keyword("make") {
                    self.parse_make()
                } else if self.check_keyword("begin") {
                    self.advance(); // consume 'begin'
                    Ok(Statement::Begin)
                } else if self.check_keyword("commit") {
                    self.advance(); // consume 'commit'
                    Ok(Statement::Commit)
                } else if self.check_keyword("rollback") {
                    self.advance(); // consume 'rollback'
                    Ok(Statement::Rollback)
                } else if self.check_keyword("drop") {
                    self.parse_drop()
                } else if self.check_keyword("describe") {
                    self.parse_describe()
                } else if self.check_keyword("count") {
                    self.parse_count()
                } else {
                    Err(self
                        .error("unexpected keyword at statement level")
                        .with_note("valid statements: get, put, change, remove, make, drop, describe, count"))
                }
            }
            _ => Err(self.error("expected statement verb")),
        }
    }

    // Statement parsing

    fn parse_get(&mut self) -> ParseResult<Statement> {
        self.expect_keyword("get")?;

        // Parse optional field list before 'from'
        // Syntax: get [field1, field2, ...] from <table>
        let fields = if !self.check_keyword("from") {
            let mut field_list = Vec::new();

            // Parse first field
            field_list.push(self.expect_identifier()?);

            // Parse additional fields separated by commas
            while self.check(TokenKind::Comma) {
                self.advance(); // consume comma
                field_list.push(self.expect_identifier()?);
            }

            Some(field_list)
        } else {
            None
        };

        self.expect_keyword("from")?;
        let source = self.expect_identifier()?;

        let mut filter = None;
        let mut order_by = None;
        let mut take = None;
        let mut skip = None;
        let mut extensions = Vec::new();

        // Parse optional clauses
        while !self.is_at_end() && !self.check(TokenKind::Newline) {
            if self.check_keyword("where") {
                filter = Some(self.parse_where_clause()?);
            } else if self.check_keyword("order") {
                order_by = Some(self.parse_order_by_clause()?);
            } else if self.check_keyword("take") {
                take = Some(self.parse_take_clause()?);
            } else if self.check_keyword("skip") {
                skip = Some(self.parse_skip_clause()?);
            } else if self.check_keyword("with") {
                extensions.push(self.parse_extension()?);
            } else {
                break;
            }
        }

        Ok(Statement::Get {
            source,
            filter,
            fields,
            order_by,
            take,
            skip,
            extensions,
        })
    }

    /// Parse ORDER BY clause: `order by field [asc|desc], field2 [asc|desc], ...`
    fn parse_order_by_clause(&mut self) -> ParseResult<Vec<OrderByField>> {
        self.expect_keyword("order")?;
        self.expect_keyword("by")?;

        let mut fields = Vec::new();

        loop {
            let field = self.expect_identifier()?;
            let direction = self.parse_sort_direction()?;

            fields.push(OrderByField { field, direction });

            // Check for comma to continue
            if self.check(TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(fields)
    }

    /// Parse sort direction: `asc`, `desc`, or default to asc
    fn parse_sort_direction(&mut self) -> ParseResult<SortDirection> {
        if self.check_keyword("asc") {
            self.advance();
            Ok(SortDirection::Asc)
        } else if self.check_keyword("desc") {
            self.advance();
            Ok(SortDirection::Desc)
        } else if self.check_identifier("ascending") {
            // Helpful error for common mistake
            self.advance();
            Err(self
                .error("unknown keyword 'ascending'")
                .with_note("use 'asc' instead of 'ascending'"))
        } else if self.check_identifier("descending") {
            // Helpful error for common mistake
            self.advance();
            Err(self
                .error("unknown keyword 'descending'")
                .with_note("use 'desc' instead of 'descending'"))
        } else {
            // Default to ascending
            Ok(SortDirection::Asc)
        }
    }

    /// Check if current token is an identifier matching the given string
    fn check_identifier(&self, name: &str) -> bool {
        if let Some(token) = self.current()
            && token.kind == TokenKind::Identifier
        {
            let text = self.token_text(token);
            return text == name.as_bytes();
        }
        false
    }

    /// Parse TAKE clause: `take <number>`
    fn parse_take_clause(&mut self) -> ParseResult<u64> {
        self.expect_keyword("take")?;
        self.expect_integer()
    }

    /// Parse SKIP clause: `skip <number>`
    fn parse_skip_clause(&mut self) -> ParseResult<u64> {
        self.expect_keyword("skip")?;
        self.expect_integer()
    }

    /// Expect an integer literal and return its value
    fn expect_integer(&mut self) -> ParseResult<u64> {
        match self.current() {
            Some(token) if token.kind == TokenKind::Number => {
                let text = self.token_text(token);
                let text_str = String::from_utf8_lossy(text).to_string();
                self.advance();
                text_str
                    .parse::<u64>()
                    .map_err(|_| self.error("expected a positive integer"))
            }
            _ => Err(self.error("expected a number")),
        }
    }

    fn parse_put(&mut self) -> ParseResult<Statement> {
        self.expect_keyword("put")?;
        self.expect_keyword("into")?;
        let target = self.expect_identifier()?;

        let mut values = Vec::new();
        let mut extensions = Vec::new();

        // Handle both one-liner and multi-line syntax
        if self.check(TokenKind::Newline) {
            self.skip_newlines();
            if self.check(TokenKind::Indent) {
                self.advance();

                while !self.check(TokenKind::Dedent) && !self.is_at_end() {
                    self.skip_newlines();
                    if self.check(TokenKind::Dedent) {
                        break;
                    }

                    if self.check_keyword("with") {
                        extensions.push(self.parse_extension()?);
                    } else {
                        let key = self.expect_identifier()?;
                        self.expect(TokenKind::Equals)?;
                        let value = self.parse_expr()?;
                        values.push(Assignment { key, value });
                    }
                    self.skip_newlines();
                }

                self.expect(TokenKind::Dedent)?;
            }
        } else {
            loop {
                if self.check_keyword("with") {
                    extensions.push(self.parse_extension()?);
                    if !self.check(TokenKind::Comma) {
                        break;
                    }
                    self.advance();
                } else {
                    let key = self.expect_identifier()?;
                    self.expect(TokenKind::Equals)?;
                    let value = self.parse_expr()?;
                    values.push(Assignment { key, value });

                    if !self.check(TokenKind::Comma) {
                        break;
                    }
                    self.advance();
                }
            }
        }

        Ok(Statement::Put {
            target,
            values,
            extensions,
        })
    }

    fn parse_change(&mut self) -> ParseResult<Statement> {
        self.expect_keyword("change")?;
        let target = self.expect_identifier()?;

        let mut filter = None;
        let mut changes = Vec::new();
        let mut extensions = Vec::new();

        // Parse where clause if present
        if self.check_keyword("where") {
            filter = Some(self.parse_where_clause()?);
        }

        // Parse assignments (multi-line with indent or one-liner)
        if self.check(TokenKind::Newline) {
            self.skip_newlines();
            if self.check(TokenKind::Indent) {
                self.advance();

                while !self.check(TokenKind::Dedent) && !self.is_at_end() {
                    self.skip_newlines();
                    if self.check(TokenKind::Dedent) {
                        break;
                    }

                    if self.check_keyword("with") {
                        extensions.push(self.parse_extension()?);
                    } else {
                        let key = self.expect_identifier()?;
                        self.expect(TokenKind::Equals)?;
                        let value = self.parse_expr()?;
                        changes.push(Assignment { key, value });
                    }
                    self.skip_newlines();
                }

                self.expect(TokenKind::Dedent)?;
            }
        } else {
            // One-liner: change users where id = 1, name = "new"
            loop {
                if self.check_keyword("with") {
                    extensions.push(self.parse_extension()?);
                    if !self.check(TokenKind::Comma) {
                        break;
                    }
                    self.advance();
                } else if self.check(TokenKind::Identifier) {
                    let key = self.expect_identifier()?;
                    self.expect(TokenKind::Equals)?;
                    let value = self.parse_expr()?;
                    changes.push(Assignment { key, value });

                    if !self.check(TokenKind::Comma) {
                        break;
                    }
                    self.advance();
                } else {
                    break;
                }
            }
        }

        Ok(Statement::Change {
            target,
            filter,
            changes,
            extensions,
        })
    }

    fn parse_remove(&mut self) -> ParseResult<Statement> {
        self.expect_keyword("remove")?;
        self.expect_keyword("from")?;
        let target = self.expect_identifier()?;

        let mut filter = None;
        let mut extensions = Vec::new();

        if self.check_keyword("where") {
            filter = Some(self.parse_where_clause()?);
        }

        while self.check_keyword("with") {
            extensions.push(self.parse_extension()?);
        }

        Ok(Statement::Remove {
            target,
            filter,
            extensions,
        })
    }

    fn parse_make(&mut self) -> ParseResult<Statement> {
        self.expect_keyword("make")?;

        // Check for "make index" or "make unique index"
        if self.check_keyword("index") || self.check_keyword("unique") {
            return self.parse_make_index();
        }

        self.expect_keyword("table")?;
        let name = self.expect_identifier()?;

        self.skip_newlines();
        self.expect(TokenKind::Indent)?;

        let mut table_type = None;
        let mut schema = None;
        let mut properties = Vec::new();

        while !self.check(TokenKind::Dedent) && !self.is_at_end() {
            self.skip_newlines();

            if self.check(TokenKind::Dedent) {
                break;
            }

            if self.check_keyword("as") {
                self.advance();
                let type_name = if self.check(TokenKind::Keyword) {
                    let token = self.current().unwrap();
                    let name = self.token_string(token);
                    self.advance();
                    name
                } else {
                    self.expect_identifier()?
                };

                table_type = Some(match type_name.as_str() {
                    "relational" => TableType::Relational,
                    "document" => TableType::Document,
                    "keyspace" => TableType::Keyspace,
                    _ => {
                        return Err(self
                            .error(format!("unknown table type '{type_name}'"))
                            .with_note("valid types: relational, document, keyspace"));
                    }
                });
                self.skip_newlines();
            } else if self.check_keyword("fields") {
                schema = Some(self.parse_fields()?);
            } else {
                let prop = self.parse_table_property()?;
                properties.push(prop);
            }
        }

        self.expect(TokenKind::Dedent)?;

        if table_type.is_none() {
            return Err(self
                .error("missing table type declaration")
                .with_note("add: as relational/document/keyspace"));
        }

        Ok(Statement::Make {
            table_type: table_type.unwrap(),
            name,
            schema,
            properties,
        })
    }

    /// Parse: make [unique] index <name> on <table>(<columns>)
    fn parse_make_index(&mut self) -> ParseResult<Statement> {
        // Check for optional "unique"
        let unique = if self.check_keyword("unique") {
            self.advance();
            true
        } else {
            false
        };

        self.expect_keyword("index")?;
        let index_name = self.expect_identifier()?;
        self.expect_keyword("on")?;
        let table_name = self.expect_identifier()?;

        // Parse column list: (col1, col2, ...)
        self.expect(TokenKind::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.expect_identifier()?);
            if self.check(TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(TokenKind::RightParen)?;

        if columns.is_empty() {
            return Err(self.error("index must have at least one column"));
        }

        Ok(Statement::MakeIndex {
            index_name,
            table_name,
            columns,
            unique,
        })
    }

    /// Parse: describe <table>
    fn parse_describe(&mut self) -> ParseResult<Statement> {
        self.expect_keyword("describe")?;
        let table_name = self.expect_identifier()?;
        Ok(Statement::Describe { table_name })
    }

    /// Parse: count <table> OR count from <table>
    fn parse_count(&mut self) -> ParseResult<Statement> {
        self.expect_keyword("count")?;
        // Allow optional "from" keyword for SQL-like syntax
        if self.check_keyword("from") {
            self.advance();
        }
        let table_name = self.expect_identifier()?;
        Ok(Statement::Count { table_name })
    }

    /// Parse: drop index <name> on <table> OR drop table <name>
    fn parse_drop(&mut self) -> ParseResult<Statement> {
        self.expect_keyword("drop")?;

        if self.check_keyword("index") {
            self.expect_keyword("index")?;
            let index_name = self.expect_identifier()?;
            self.expect_keyword("on")?;
            let table_name = self.expect_identifier()?;

            Ok(Statement::DropIndex {
                index_name,
                table_name,
            })
        } else if self.check_keyword("table") {
            self.expect_keyword("table")?;
            let table_name = self.expect_identifier()?;

            Ok(Statement::DropTable { table_name })
        } else {
            Err(self
                .error("expected 'index' or 'table' after 'drop'")
                .with_note("usage: drop index <name> on <table> OR drop table <name>"))
        }
    }

    fn parse_where_clause(&mut self) -> ParseResult<Expr> {
        self.expect_keyword("where")?;

        if self.check(TokenKind::Newline) {
            self.skip_newlines();
            if self.check(TokenKind::Indent) {
                self.advance();
                let conditions = self.parse_conditions()?;
                self.expect(TokenKind::Dedent)?;
                Ok(conditions)
            } else {
                self.parse_expr()
            }
        } else {
            self.parse_expr()
        }
    }

    fn parse_extension(&mut self) -> ParseResult<Extension> {
        self.expect_keyword("with")?;
        let name = self.expect_identifier()?;

        let mut args = Vec::new();
        if self.check(TokenKind::LeftParen) {
            self.advance();

            while !self.check(TokenKind::RightParen) && !self.is_at_end() {
                args.push(self.parse_expr()?);
                if !self.check(TokenKind::RightParen) {
                    self.expect(TokenKind::Comma)?;
                }
            }

            self.expect(TokenKind::RightParen)?;
        }

        Ok(Extension { name, args })
    }

    fn parse_fields(&mut self) -> ParseResult<Vec<FieldDef>> {
        self.expect_keyword("fields")?;
        self.skip_newlines();
        self.expect(TokenKind::Indent)?;

        let mut fields = Vec::new();
        let mut field_names = HashSet::new();

        while !self.check(TokenKind::Dedent) && !self.is_at_end() {
            self.skip_newlines();

            if self.check(TokenKind::Dedent) {
                break;
            }

            let name = self.expect_identifier()?;

            // allow a newline between the field name and its type or nested object block
            self.skip_newlines();

            if !field_names.insert(name.clone()) {
                return Err(self.error(format!("duplicate field name '{name}'")));
            }

            // Accept:
            //   name <keyword-type>
            //   name [<keyword-type>]
            //   name <newline> <indent> ...nested fields... <dedent>   -> treat as object
            let field_type = if self.check(TokenKind::Keyword) {
                let type_str = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();
                match type_str.as_str() {
                    "int" => ValueType::Int32,
                    "bigint" => ValueType::Int64,
                    "text" => ValueType::String,
                    "decimal" => ValueType::Float32,
                    "double" => ValueType::Float64,
                    "date" => ValueType::DateTime,
                    "boolean" => ValueType::Bool,
                    "map" => ValueType::Object,
                    "list" => ValueType::Array,
                    _ => {
                        return Err(self
                            .error(format!("invalid field type '{type_str}'"))
                            .with_note(
                                "valid types: number, text, decimal, date, boolean, map, list",
                            ));
                    }
                }
            } else if self.check(TokenKind::LeftBracket) {
                // array type like: tags [string]
                self.advance();
                if self.check(TokenKind::Keyword) {
                    // inner type is currently ignored (we use ValueType::Array)
                    self.advance();
                } else {
                    return Err(self.error("expected type inside array brackets"));
                }
                self.expect(TokenKind::RightBracket)?;
                ValueType::Array
            } else if self.check(TokenKind::Indent) {
                self.advance();
                self.skip_newlines();

                while !self.check(TokenKind::Dedent) && !self.is_at_end() {
                    self.skip_newlines();
                    if self.check(TokenKind::Dedent) {
                        break;
                    }

                    // Child field name
                    let _child_name = self.expect_identifier()?;

                    // Child type: accept keyword or bracketed array
                    if self.check(TokenKind::Keyword) {
                        self.advance();
                    } else if self.check(TokenKind::LeftBracket) {
                        self.advance();
                        if self.check(TokenKind::Keyword) {
                            self.advance();
                        } else {
                            return Err(self.error("expected type inside array brackets"));
                        }
                        self.expect(TokenKind::RightBracket)?;
                    } else {
                        return Err(self.error("expected field type in nested object"));
                    }

                    // optional trailing comma on child lines
                    if self.check(TokenKind::Comma) {
                        self.advance();
                    }

                    self.skip_newlines();
                }

                // consume closing dedent for the nested object
                self.expect(TokenKind::Dedent)?;
                ValueType::Object
            } else {
                return Err(self.error("expected field type"));
            };

            // Parse constraints
            let mut is_primary = false;
            let mut is_unique = false;
            let mut is_required = false;
            let mut default = None;

            while self.check(TokenKind::Keyword) {
                if self.check_keyword("primary") {
                    self.advance();
                    self.expect_keyword("key")?;
                    is_primary = true;
                } else if self.check_keyword("unique") {
                    self.advance();
                    is_unique = true;
                } else if self.check_keyword("required") {
                    self.advance();
                    is_required = true;
                } else if self.check_keyword("default") {
                    self.advance();
                    let expr = self.parse_expr()?;
                    default = Some(self.expr_to_value(expr)?);
                } else {
                    break;
                }
            }

            fields.push(FieldDef {
                name,
                field_type,
                is_primary,
                is_unique,
                is_required,
                default,
            });

            self.skip_newlines();
        }

        self.expect(TokenKind::Dedent)?;
        Ok(fields)
    }

    fn parse_table_property(&mut self) -> ParseResult<TableProperty> {
        // Accept either a keyword or an identifier for the property name.
        match self.current() {
            Some(token)
                if token.kind == TokenKind::Keyword || token.kind == TokenKind::Identifier =>
            {
                let name = self.token_string(token);
                self.advance();

                match name.as_str() {
                    "ttl" => {
                        let token = self.expect(TokenKind::Number)?;
                        let span = token.span;
                        let num_str =
                            String::from_utf8_lossy(&self.source[span.as_range()]).into_owned();
                        match num_str.parse::<u32>() {
                            Ok(ttl) => Ok(TableProperty::Ttl(ttl)),
                            Err(_) => Err(self
                                .error("invalid TTL value")
                                .with_note("TTL must be a positive integer")),
                        }
                    }
                    "description" => {
                        let expr = self.parse_expr()?;
                        match self.expr_to_value(expr)? {
                            Value::String(s) => Ok(TableProperty::Description(s)),
                            _ => Err(self.error("description must be a string")),
                        }
                    }
                    "persistence" => {
                        let expr = self.parse_expr()?;
                        match self.expr_to_value(expr)? {
                            Value::String(s) => Ok(TableProperty::Persistence(s)),
                            _ => Err(self.error("persistence must be a string")),
                        }
                    }
                    "prefix" => {
                        let expr = self.parse_expr()?;
                        match self.expr_to_value(expr)? {
                            Value::String(s) => Ok(TableProperty::Prefix(s)),
                            _ => Err(self.error("prefix must be a string")),
                        }
                    }
                    _ => Err(self.error(format!("unknown table property '{name}'"))),
                }
            }
            Some(token) => Err(self.error(format!(
                "expected table property, found {}",
                self.describe_token(token)
            ))),
            None => Err(self.error("expected table property, but reached end of input")),
        }
    }

    fn parse_conditions(&mut self) -> ParseResult<Expr> {
        let mut conditions = Vec::new();

        while !self.check(TokenKind::Dedent) && !self.is_at_end() {
            self.skip_newlines();
            if self.check(TokenKind::Dedent) {
                break;
            }
            conditions.push(self.parse_expr()?);
            self.skip_newlines();
        }

        if conditions.is_empty() {
            return Err(self.error("empty where clause"));
        }

        Ok(conditions
            .into_iter()
            .reduce(|acc, cond| Expr::BinaryOp {
                left: Box::new(acc),
                op: BinaryOp::And,
                right: Box::new(cond),
            })
            .unwrap())
    }

    // Expression parsing

    fn parse_expr(&mut self) -> ParseResult<Expr> {
        self.parse_expr_bp(0)
    }

    fn parse_expr_bp(&mut self, min_bp: u8) -> ParseResult<Expr> {
        let mut left = self.parse_primary()?;

        loop {
            let (op, left_bp, right_bp) = match self.peek() {
                Some(TokenKind::Dot) => {
                    self.advance();
                    let field = self.expect_identifier()?;
                    left = Expr::FieldAccess {
                        base: Box::new(left),
                        field,
                    };
                    continue;
                }
                Some(TokenKind::Equals) => (BinaryOp::Eq, 5, 6),
                Some(TokenKind::NotEqual) => (BinaryOp::NotEq, 5, 6),
                Some(TokenKind::LessThan) => (BinaryOp::Lt, 7, 8),
                Some(TokenKind::GreaterThan) => (BinaryOp::Gt, 7, 8),
                Some(TokenKind::LessEqual) => (BinaryOp::LtEq, 7, 8),
                Some(TokenKind::GreaterEqual) => (BinaryOp::GtEq, 7, 8),
                Some(TokenKind::Plus) => (BinaryOp::Add, 9, 10),
                Some(TokenKind::Minus) => (BinaryOp::Sub, 9, 10),
                Some(TokenKind::Star) => (BinaryOp::Mul, 11, 12),
                Some(TokenKind::Slash) => (BinaryOp::Div, 11, 12),
                Some(TokenKind::Keyword) if self.check_keyword("and") => (BinaryOp::And, 3, 4),
                Some(TokenKind::Keyword) if self.check_keyword("or") => (BinaryOp::Or, 1, 2),
                Some(TokenKind::Keyword) if self.check_keyword("has") => (BinaryOp::Contains, 7, 8),
                _ => break,
            };

            if left_bp < min_bp {
                break;
            }

            self.advance();
            let right = self.parse_expr_bp(right_bp)?;

            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_primary(&mut self) -> ParseResult<Expr> {
        match self.peek() {
            Some(TokenKind::StringLiteral) => {
                let text = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();
                let s = &text[1..text.len() - 1]; // remove quotes

                // Try parsing as datetime, date, or time
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(Expr::Literal(Value::DateTime(dt)))
                } else if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    Ok(Expr::Literal(Value::Date(d)))
                } else if let Ok(t) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                    Ok(Expr::Literal(Value::Time(t)))
                } else if let Ok(u) = uuid::Uuid::parse_str(s) {
                    Ok(Expr::Literal(Value::Uuid(u)))
                } else {
                    Ok(Expr::Literal(Value::String(s.to_string())))
                }
            }

            Some(TokenKind::Number) => {
                let num_str = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();

                if num_str.contains('.') {
                    match num_str.parse::<f64>() {
                        Ok(f) => Ok(Expr::Literal(Value::Float64(f))),
                        Err(_) => Err(self.error("invalid floating-point number")),
                    }
                } else {
                    match num_str.parse::<i64>() {
                        Ok(i) if i <= i32::MAX as i64 && i >= i32::MIN as i64 => {
                            Ok(Expr::Literal(Value::Int32(i as i32)))
                        }
                        Ok(i) => Ok(Expr::Literal(Value::Int64(i))),
                        Err(_) => Err(self.error("integer overflow")),
                    }
                }
            }

            Some(TokenKind::Identifier) => {
                let id = self.expect_identifier()?;
                Ok(Expr::Identifier(id))
            }

            Some(TokenKind::Variable) => {
                // Parse numbered ($1) or named ($name) parameter
                let text = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();

                // Remove the '$' prefix
                let param_text = &text[1..];

                // Check if it's a numbered parameter
                if let Ok(num) = param_text.parse::<usize>() {
                    if num == 0 {
                        return Err(self.error("numbered parameters must start at $1"));
                    }
                    Ok(Expr::NumberedParam(num))
                } else {
                    // It's a named parameter with $ syntax
                    Ok(Expr::NamedParam(param_text.to_string()))
                }
            }

            Some(TokenKind::NamedParam) => {
                // Parse named parameter with colon syntax (:name)
                let text = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();

                // Remove the ':' prefix
                let param_name = &text[1..];
                Ok(Expr::NamedParam(param_name.to_string()))
            }

            Some(TokenKind::Keyword) => {
                if self.check_keyword("true") {
                    self.advance();
                    Ok(Expr::Literal(Value::Bool(true)))
                } else if self.check_keyword("false") {
                    self.advance();
                    Ok(Expr::Literal(Value::Bool(false)))
                } else if self.check_keyword("null") {
                    self.advance();
                    Ok(Expr::Literal(Value::Null))
                } else if self.check_keyword("now") {
                    self.advance();
                    if self.check(TokenKind::LeftParen) {
                        self.advance();
                        self.expect(TokenKind::RightParen)?;
                    }
                    Ok(Expr::FunctionCall {
                        name: "now".to_string(),
                        args: Vec::new(),
                    })
                } else {
                    let name = {
                        let token = self.current().unwrap();
                        self.token_string(token)
                    };
                    self.advance();

                    if self.check(TokenKind::LeftParen) {
                        self.advance();
                        let mut args = Vec::new();
                        while !self.check(TokenKind::RightParen) && !self.is_at_end() {
                            args.push(self.parse_expr()?);
                            if !self.check(TokenKind::RightParen) {
                                self.expect(TokenKind::Comma)?;
                            }
                        }
                        self.expect(TokenKind::RightParen)?;
                        Ok(Expr::FunctionCall { name, args })
                    } else {
                        Ok(Expr::Identifier(name))
                    }
                }
            }

            Some(TokenKind::LeftBracket) => self.parse_array(),
            Some(TokenKind::LeftBrace) => self.parse_object(),
            Some(TokenKind::LeftParen) => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(TokenKind::RightParen)?;
                Ok(expr)
            }

            _ => Err(self.error("expected expression")),
        }
    }

    fn parse_array(&mut self) -> ParseResult<Expr> {
        self.expect(TokenKind::LeftBracket)?;
        let mut elements = Vec::new();

        while !self.check(TokenKind::RightBracket) && !self.is_at_end() {
            let expr = self.parse_expr()?;
            elements.push(self.expr_to_value(expr)?);

            if !self.check(TokenKind::RightBracket) {
                self.expect(TokenKind::Comma)?;
            }
        }

        self.expect(TokenKind::RightBracket)?;
        Ok(Expr::Literal(Value::Array(elements)))
    }

    fn parse_object(&mut self) -> ParseResult<Expr> {
        self.expect(TokenKind::LeftBrace)?;
        self.skip_newlines();

        let mut object = BTreeMap::new();
        let indented = self.check(TokenKind::Indent);

        if indented {
            self.advance();
        }

        while !self.check(TokenKind::RightBrace) && !self.is_at_end() {
            self.skip_newlines();

            if indented && self.check(TokenKind::Dedent) {
                self.advance();
                break;
            }

            if self.check(TokenKind::RightBrace) {
                break;
            }

            let key = self.expect_identifier()?;
            self.expect(TokenKind::Equals)?;
            let value_expr = self.parse_expr()?;
            let value = self.expr_to_value(value_expr)?;

            object.insert(key, value);

            self.skip_newlines();

            if !indented && !self.check(TokenKind::RightBrace) {
                self.expect(TokenKind::Comma)?;
            }
        }

        self.expect(TokenKind::RightBrace)?;
        Ok(Expr::Literal(Value::Object(object)))
    }

    fn expr_to_value(&self, expr: Expr) -> ParseResult<Value> {
        match expr {
            Expr::Literal(v) => Ok(v),

            Expr::FunctionCall { name, args } if name == "now" && args.is_empty() => {
                Ok(Value::DateTime(chrono::Utc::now().into()))
            }

            // Identifier literals like true, false, null
            Expr::Identifier(s) => match s.as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                "null" => Ok(Value::Null),
                _ => Ok(Value::String(s)),
            },

            _ => Err(self.error("expected literal value")),
        }
    }
}

// Public API

pub fn parse(source: String) -> Result<Vec<Statement>, Vec<String>> {
    let mut lexer = Lexer::new();
    let source_bytes = source.as_bytes();
    let tokens = lexer.lex(source_bytes);

    let mut parser = Parser::new(&tokens, source_bytes);

    match parser.parse() {
        Ok(statements) => Ok(statements),
        Err(diagnostics) => {
            let errors: Vec<String> = diagnostics.iter().map(|d| d.render(&source)).collect();

            Err(errors)
        }
    }
}

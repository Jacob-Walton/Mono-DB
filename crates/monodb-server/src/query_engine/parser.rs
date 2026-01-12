#![allow(dead_code)]

//! # Parser
//!
//! Recursive descent parser for the MonoDB query language.

use std::sync::Arc;

use super::ast::{
    Assignment, BinaryOp, ChangeMutation, ColumnConstraint, ColumnDef, ColumnRef, ControlStatement,
    CountQuery, CreateIndexDdl, CreateTableDdl, DataType, DdlStatement, DescribeQuery,
    DropIndexDdl, DropTableDdl, Expr, GetQuery, Ident, LimitValue, LiteralSlot, LiteralValue,
    MutationStatement, OrderByClause, ParamRef, PutMutation, QueryStatement, RemoveMutation,
    SortDirection, Span, Spanned, Statement, TableProperty, TableType, TransactionStatement,
    UseStatement,
};
use super::lexer::{Token, TokenKind};

// Diagnostics

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

    pub fn render(&self, _source: &str) -> String {
        let mut msg = format!("error at byte {}: {}", self.span.start, self.message);
        for note in &self.notes {
            msg.push_str("\n  note: ");
            msg.push_str(note);
        }
        msg
    }
}

pub type ParseResult<T> = Result<T, Diagnostic>;

// Parser

/// Recursive descent parser
pub struct Parser<'a> {
    tokens: &'a [Token],
    source: &'a [u8],
    pos: usize,
    errors: Vec<Diagnostic>,
    next_slot_id: u32,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: &'a [Token], source: &'a [u8]) -> Self {
        Self {
            tokens,
            source,
            pos: 0,
            errors: Vec::new(),
            next_slot_id: 0,
        }
    }

    // Token management

    #[inline]
    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    #[inline]
    fn current_span(&self) -> Span {
        self.current()
            .map(|t| Span::new(t.span.start, t.span.end))
            .unwrap_or(Span::new(
                self.source.len() as u32,
                self.source.len() as u32,
            ))
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

    fn error(&self, message: impl Into<String>) -> Diagnostic {
        Diagnostic::error(message, self.current_span())
    }

    fn expect(&mut self, kind: TokenKind) -> ParseResult<&Token> {
        match self.current() {
            Some(token) if token.kind == kind => Ok(self.advance().unwrap()),
            Some(token) => Err(self.error(format!("expected {:?}, found {:?}", kind, token.kind))),
            None => Err(self.error(format!("expected {:?}, but reached end of input", kind))),
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> ParseResult<()> {
        if self.check_keyword(keyword) {
            self.advance();
            Ok(())
        } else {
            match self.current() {
                Some(token) => Err(self.error(format!(
                    "expected keyword '{}', found '{}'",
                    keyword,
                    self.token_string(token)
                ))),
                None => Err(self.error(format!(
                    "expected keyword '{}', but reached end of input",
                    keyword
                ))),
            }
        }
    }

    fn expect_identifier(&mut self) -> ParseResult<Spanned<Ident>> {
        let span = self.current_span();
        match self.current() {
            Some(token) if token.kind == TokenKind::Identifier => {
                let name = self.token_string(token);
                self.advance();
                Ok(Spanned::new(Ident::new(name), span))
            }
            Some(token) if token.kind == TokenKind::Keyword => {
                let text = self.token_string(token);
                // Some keywords may be used as identifiers (contextually allowed)
                if matches!(
                    text.as_str(),
                    "key" | "value" | "data" | "ttl" | "default" | "namespace"
                ) {
                    self.advance();
                    Ok(Spanned::new(Ident::new(text), span))
                } else {
                    Err(self
                        .error(format!("cannot use keyword '{}' as identifier", text))
                        .with_suggestion("use a different name", format!("{}_field", text)))
                }
            }
            Some(token) => Err(self.error(format!("expected identifier, found {:?}", token.kind))),
            None => Err(self.error("expected identifier, but reached end of input")),
        }
    }

    fn next_slot(&mut self) -> u32 {
        let id = self.next_slot_id;
        self.next_slot_id += 1;
        id
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
                    || self.check_keyword("use")
                {
                    return;
                }
            }
            self.advance();
        }
    }

    // Main parsing entry point

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
                    Ok(Statement::Query(self.parse_get()?))
                } else if self.check_keyword("put") {
                    Ok(Statement::Mutation(self.parse_put()?))
                } else if self.check_keyword("change") {
                    Ok(Statement::Mutation(self.parse_change()?))
                } else if self.check_keyword("remove") {
                    Ok(Statement::Mutation(self.parse_remove()?))
                } else if self.check_keyword("make") {
                    Ok(Statement::Ddl(self.parse_make()?))
                } else if self.check_keyword("begin") {
                    self.advance();
                    Ok(Statement::Transaction(TransactionStatement::Begin))
                } else if self.check_keyword("commit") {
                    self.advance();
                    Ok(Statement::Transaction(TransactionStatement::Commit))
                } else if self.check_keyword("rollback") {
                    self.advance();
                    Ok(Statement::Transaction(TransactionStatement::Rollback))
                } else if self.check_keyword("drop") {
                    Ok(Statement::Ddl(self.parse_drop()?))
                } else if self.check_keyword("describe") {
                    Ok(Statement::Query(self.parse_describe()?))
                } else if self.check_keyword("count") {
                    Ok(Statement::Query(self.parse_count()?))
                } else if self.check_keyword("use") {
                    Ok(Statement::Control(self.parse_use()?))
                } else {
                    Err(self
                        .error("unexpected keyword at statement level")
                        .with_note("valid statements: get, put, change, remove, make, drop, describe, count, use"))
                }
            }
            _ => Err(self.error("expected statement verb")),
        }
    }

    // Query parsing

    fn parse_get(&mut self) -> ParseResult<QueryStatement> {
        self.expect_keyword("get")?;

        // Parse optional field list before 'from'
        let projection = if !self.check_keyword("from") {
            let mut fields = Vec::new();
            fields.push(self.expect_identifier()?);
            while self.check(TokenKind::Comma) {
                self.advance();
                fields.push(self.expect_identifier()?);
            }
            Some(fields)
        } else {
            None
        };

        self.expect_keyword("from")?;
        let source = self.expect_identifier()?;

        let mut filter = None;
        let mut order_by = None;
        let mut limit = None;
        let mut offset = None;

        while !self.is_at_end() && !self.check(TokenKind::Newline) {
            if self.check_keyword("where") {
                filter = Some(self.parse_where_clause()?);
            } else if self.check_keyword("order") {
                order_by = Some(self.parse_order_by_clause()?);
            } else if self.check_keyword("take") {
                limit = Some(self.parse_take_clause()?);
            } else if self.check_keyword("skip") {
                offset = Some(self.parse_skip_clause()?);
            } else {
                break;
            }
        }

        Ok(QueryStatement::Get(GetQuery {
            source,
            filter,
            projection,
            order_by,
            limit,
            offset,
        }))
    }

    fn parse_order_by_clause(&mut self) -> ParseResult<Vec<OrderByClause>> {
        self.expect_keyword("order")?;
        self.expect_keyword("by")?;

        let mut fields = Vec::new();
        loop {
            let field = self.expect_identifier()?;
            let direction = self.parse_sort_direction()?;
            fields.push(OrderByClause { field, direction });

            if self.check(TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(fields)
    }

    fn parse_sort_direction(&mut self) -> ParseResult<SortDirection> {
        if self.check_keyword("asc") {
            self.advance();
            Ok(SortDirection::Asc)
        } else if self.check_keyword("desc") {
            self.advance();
            Ok(SortDirection::Desc)
        } else {
            Ok(SortDirection::Asc)
        }
    }

    fn parse_take_clause(&mut self) -> ParseResult<LimitValue> {
        self.expect_keyword("take")?;
        let n = self.expect_integer()?;
        Ok(LimitValue::Literal(n))
    }

    fn parse_skip_clause(&mut self) -> ParseResult<LimitValue> {
        self.expect_keyword("skip")?;
        let n = self.expect_integer()?;
        Ok(LimitValue::Literal(n))
    }

    fn expect_integer(&mut self) -> ParseResult<u64> {
        match self.current() {
            Some(token) if token.kind == TokenKind::Number => {
                let text = self.token_string(token);
                self.advance();
                text.parse::<u64>()
                    .map_err(|_| self.error("expected a positive integer"))
            }
            _ => Err(self.error("expected a number")),
        }
    }

    fn parse_describe(&mut self) -> ParseResult<QueryStatement> {
        self.expect_keyword("describe")?;
        let table = self.expect_identifier()?;
        Ok(QueryStatement::Describe(DescribeQuery { table }))
    }

    fn parse_count(&mut self) -> ParseResult<QueryStatement> {
        self.expect_keyword("count")?;
        if self.check_keyword("from") {
            self.advance();
        }
        let table = self.expect_identifier()?;
        Ok(QueryStatement::Count(CountQuery {
            table,
            filter: None,
        }))
    }

    // Control statement parsing

    fn parse_use(&mut self) -> ParseResult<ControlStatement> {
        self.expect_keyword("use")?;
        let namespace = self.expect_identifier()?;
        Ok(ControlStatement::Use(UseStatement { namespace }))
    }

    // Mutation parsing

    fn parse_put(&mut self) -> ParseResult<MutationStatement> {
        self.expect_keyword("put")?;
        self.expect_keyword("into")?;
        let target = self.expect_identifier()?;

        let mut assignments = Vec::new();

        if self.check(TokenKind::Newline) {
            self.skip_newlines();
            if self.check(TokenKind::Indent) {
                self.advance();

                while !self.check(TokenKind::Dedent) && !self.is_at_end() {
                    self.skip_newlines();
                    if self.check(TokenKind::Dedent) {
                        break;
                    }

                    let field = self.expect_identifier()?;
                    self.expect(TokenKind::Equals)?;
                    let value = self.parse_expr()?;
                    assignments.push(Assignment { field, value });
                    self.skip_newlines();
                }

                self.expect(TokenKind::Dedent)?;
            }
        } else {
            loop {
                let field = self.expect_identifier()?;
                self.expect(TokenKind::Equals)?;
                let value = self.parse_expr()?;
                assignments.push(Assignment { field, value });

                if !self.check(TokenKind::Comma) {
                    break;
                }
                self.advance();
            }
        }

        Ok(MutationStatement::Put(PutMutation {
            target,
            assignments,
        }))
    }

    fn parse_change(&mut self) -> ParseResult<MutationStatement> {
        self.expect_keyword("change")?;
        let target = self.expect_identifier()?;

        let mut filter = None;
        let mut assignments = Vec::new();

        if self.check_keyword("where") {
            filter = Some(self.parse_where_clause()?);
        }

        if self.check(TokenKind::Newline) {
            self.skip_newlines();
            if self.check(TokenKind::Indent) {
                self.advance();

                while !self.check(TokenKind::Dedent) && !self.is_at_end() {
                    self.skip_newlines();
                    if self.check(TokenKind::Dedent) {
                        break;
                    }

                    let field = self.expect_identifier()?;
                    self.expect(TokenKind::Equals)?;
                    let value = self.parse_expr()?;
                    assignments.push(Assignment { field, value });
                    self.skip_newlines();
                }

                self.expect(TokenKind::Dedent)?;
            }
        } else {
            loop {
                if !self.check(TokenKind::Identifier) && !self.check(TokenKind::Keyword) {
                    break;
                }
                let field = self.expect_identifier()?;
                self.expect(TokenKind::Equals)?;
                let value = self.parse_expr()?;
                assignments.push(Assignment { field, value });

                if !self.check(TokenKind::Comma) {
                    break;
                }
                self.advance();
            }
        }

        Ok(MutationStatement::Change(ChangeMutation {
            target,
            filter,
            assignments,
        }))
    }

    fn parse_remove(&mut self) -> ParseResult<MutationStatement> {
        self.expect_keyword("remove")?;
        self.expect_keyword("from")?;
        let target = self.expect_identifier()?;

        let filter = if self.check_keyword("where") {
            self.parse_where_clause()?
        } else {
            return Err(self
                .error("remove requires a where clause")
                .with_note("use 'remove from table where <condition>'"));
        };

        Ok(MutationStatement::Remove(RemoveMutation { target, filter }))
    }

    // DDL parsing

    fn parse_make(&mut self) -> ParseResult<DdlStatement> {
        self.expect_keyword("make")?;

        if self.check_keyword("index") || self.check_keyword("unique") {
            return self.parse_make_index();
        }

        self.expect_keyword("table")?;
        let name = self.expect_identifier()?;

        self.skip_newlines();
        self.expect(TokenKind::Indent)?;

        let mut table_type = None;
        let mut columns = Vec::new();
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
                    self.expect_identifier()?.node.to_string()
                };

                table_type = Some(match type_name.as_str() {
                    "relational" => TableType::Relational,
                    "document" => TableType::Document,
                    "keyspace" => TableType::Keyspace,
                    _ => {
                        return Err(self
                            .error(format!("unknown table type '{}'", type_name))
                            .with_note("valid types: relational, document, keyspace"));
                    }
                });
                self.skip_newlines();
            } else if self.check_keyword("fields") {
                columns = self.parse_fields()?;
            } else if self.check_keyword("ttl") {
                self.advance();
                let n = self.expect_integer()? as u32;
                properties.push(TableProperty::Ttl(n));
                self.skip_newlines();
            } else if self.check_keyword("description") {
                self.advance();
                let expr = self.parse_expr()?;
                if let Expr::Literal(slot) = &expr.node
                    && let LiteralValue::String(s) = &slot.value
                {
                    properties.push(TableProperty::Description(s.clone()));
                }
                self.skip_newlines();
            } else {
                // Skip unknown properties
                self.advance();
                self.skip_newlines();
            }
        }

        self.expect(TokenKind::Dedent)?;

        if table_type.is_none() {
            return Err(self
                .error("missing table type declaration")
                .with_note("add: as relational/document/keyspace"));
        }

        Ok(DdlStatement::CreateTable(CreateTableDdl {
            name,
            table_type: table_type.unwrap(),
            columns,
            constraints: Vec::new(),
            properties,
            if_not_exists: false,
        }))
    }

    fn parse_make_index(&mut self) -> ParseResult<DdlStatement> {
        let unique = if self.check_keyword("unique") {
            self.advance();
            true
        } else {
            false
        };

        self.expect_keyword("index")?;
        let name = self.expect_identifier()?;
        self.expect_keyword("on")?;
        let table = self.expect_identifier()?;

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

        Ok(DdlStatement::CreateIndex(CreateIndexDdl {
            name,
            table,
            columns,
            unique,
            if_not_exists: false,
        }))
    }

    fn parse_drop(&mut self) -> ParseResult<DdlStatement> {
        self.expect_keyword("drop")?;

        if self.check_keyword("index") {
            self.expect_keyword("index")?;
            let name = self.expect_identifier()?;
            self.expect_keyword("on")?;
            let table = self.expect_identifier()?;

            Ok(DdlStatement::DropIndex(DropIndexDdl {
                name,
                table,
                if_exists: false,
            }))
        } else if self.check_keyword("table") {
            self.expect_keyword("table")?;
            let name = self.expect_identifier()?;

            Ok(DdlStatement::DropTable(DropTableDdl {
                name,
                if_exists: false,
            }))
        } else {
            Err(self
                .error("expected 'index' or 'table' after 'drop'")
                .with_note("usage: drop index <name> on <table> OR drop table <name>"))
        }
    }

    fn parse_fields(&mut self) -> ParseResult<Vec<ColumnDef>> {
        self.expect_keyword("fields")?;
        self.skip_newlines();
        self.expect(TokenKind::Indent)?;

        let mut columns = Vec::new();

        while !self.check(TokenKind::Dedent) && !self.is_at_end() {
            self.skip_newlines();

            if self.check(TokenKind::Dedent) {
                break;
            }

            let name = self.expect_identifier()?;
            self.skip_newlines();

            let data_type = if self.check(TokenKind::Keyword) {
                let type_str = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();
                match type_str.as_str() {
                    "int" => DataType::Int32,
                    "bigint" => DataType::Int64,
                    "text" => DataType::String,
                    "decimal" => DataType::Float32,
                    "double" => DataType::Float64,
                    "date" => DataType::DateTime,
                    "boolean" => DataType::Bool,
                    "map" => DataType::Json,
                    "list" => DataType::Array(Box::new(DataType::Json)),
                    _ => DataType::String,
                }
            } else if self.check(TokenKind::LeftBracket) {
                self.advance();
                if self.check(TokenKind::Keyword) {
                    self.advance();
                }
                self.expect(TokenKind::RightBracket)?;
                DataType::Array(Box::new(DataType::Json))
            } else {
                DataType::String
            };

            // Parse constraints
            let mut constraints = Vec::new();

            while self.check(TokenKind::Keyword) {
                if self.check_keyword("primary") {
                    self.advance();
                    self.expect_keyword("key")?;
                    constraints.push(ColumnConstraint::PrimaryKey);
                } else if self.check_keyword("unique") {
                    self.advance();
                    constraints.push(ColumnConstraint::Unique);
                } else if self.check_keyword("required") {
                    self.advance();
                    constraints.push(ColumnConstraint::NotNull);
                } else if self.check_keyword("default") {
                    self.advance();
                    let expr = self.parse_expr()?;
                    constraints.push(ColumnConstraint::Default(expr));
                } else {
                    break;
                }
            }

            columns.push(ColumnDef {
                name,
                data_type,
                constraints,
            });

            self.skip_newlines();
        }

        self.expect(TokenKind::Dedent)?;
        Ok(columns)
    }

    // Helper parsers

    fn parse_where_clause(&mut self) -> ParseResult<Spanned<Expr>> {
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

    fn parse_conditions(&mut self) -> ParseResult<Spanned<Expr>> {
        let mut conditions = Vec::new();
        let _start_span = self.current_span();

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

        let _end_span = self.current_span();
        let combined = conditions
            .into_iter()
            .reduce(|acc, cond| {
                let span = Span::new(acc.span.start, cond.span.end);
                Spanned::new(
                    Expr::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(cond),
                    },
                    span,
                )
            })
            .unwrap();

        Ok(combined)
    }

    // Expression parsing (Pratt parser)

    fn parse_expr(&mut self) -> ParseResult<Spanned<Expr>> {
        self.parse_expr_bp(0)
    }

    fn parse_expr_bp(&mut self, min_bp: u8) -> ParseResult<Spanned<Expr>> {
        let mut left = self.parse_primary()?;

        loop {
            let (op, left_bp, right_bp) = match self.peek() {
                Some(TokenKind::Dot) => {
                    let start = left.span.start;
                    self.advance();
                    let field = self.expect_identifier()?;
                    let end = field.span.end;

                    // Build path for column ref
                    let col = match left.node {
                        Expr::Column(mut col) => {
                            col.path.push(field.node);
                            col
                        }
                        _ => {
                            // Convert to column ref with path
                            ColumnRef {
                                table: None,
                                column: field.node,
                                path: Vec::new(),
                            }
                        }
                    };
                    left = Spanned::new(Expr::Column(col), Span::new(start, end));
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

            let span = Span::new(left.span.start, right.span.end);
            left = Spanned::new(
                Expr::Binary {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
                span,
            );
        }

        Ok(left)
    }

    fn parse_primary(&mut self) -> ParseResult<Spanned<Expr>> {
        let span = self.current_span();

        match self.peek() {
            Some(TokenKind::StringLiteral) => {
                let text = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();
                let s = &text[1..text.len() - 1];
                let slot_id = self.next_slot();

                Ok(Spanned::new(
                    Expr::Literal(LiteralSlot {
                        value: LiteralValue::String(Arc::from(s)),
                        slot_id,
                    }),
                    span,
                ))
            }

            Some(TokenKind::Number) => {
                let num_str = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();
                let slot_id = self.next_slot();

                let value = if num_str.contains('.') {
                    LiteralValue::Float64(num_str.parse().unwrap_or(0.0))
                } else {
                    LiteralValue::Int64(num_str.parse().unwrap_or(0))
                };

                Ok(Spanned::new(
                    Expr::Literal(LiteralSlot { value, slot_id }),
                    span,
                ))
            }

            Some(TokenKind::Identifier) => {
                let id = self.expect_identifier()?;
                Ok(Spanned::new(
                    Expr::Column(ColumnRef::simple(id.node)),
                    id.span,
                ))
            }

            Some(TokenKind::Variable) => {
                let text = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();

                let param_text = &text[1..];

                let param = if let Ok(num) = param_text.parse::<u32>() {
                    if num == 0 {
                        return Err(self.error("numbered parameters must start at $1"));
                    }
                    ParamRef::Positional(num - 1) // Convert to 0-indexed
                } else {
                    ParamRef::Named(Ident::new(param_text))
                };

                Ok(Spanned::new(Expr::Param(param), span))
            }

            Some(TokenKind::NamedParam) => {
                let text = {
                    let token = self.current().unwrap();
                    self.token_string(token)
                };
                self.advance();

                let param_name = &text[1..];
                Ok(Spanned::new(
                    Expr::Param(ParamRef::Named(Ident::new(param_name))),
                    span,
                ))
            }

            Some(TokenKind::Keyword) => {
                if self.check_keyword("true") {
                    self.advance();
                    let slot_id = self.next_slot();
                    Ok(Spanned::new(
                        Expr::Literal(LiteralSlot {
                            value: LiteralValue::Bool(true),
                            slot_id,
                        }),
                        span,
                    ))
                } else if self.check_keyword("false") {
                    self.advance();
                    let slot_id = self.next_slot();
                    Ok(Spanned::new(
                        Expr::Literal(LiteralSlot {
                            value: LiteralValue::Bool(false),
                            slot_id,
                        }),
                        span,
                    ))
                } else if self.check_keyword("null") {
                    self.advance();
                    let slot_id = self.next_slot();
                    Ok(Spanned::new(
                        Expr::Literal(LiteralSlot {
                            value: LiteralValue::Null,
                            slot_id,
                        }),
                        span,
                    ))
                } else if self.check_keyword("now") {
                    self.advance();
                    if self.check(TokenKind::LeftParen) {
                        self.advance();
                        self.expect(TokenKind::RightParen)?;
                    }
                    Ok(Spanned::new(
                        Expr::FunctionCall {
                            name: Spanned::new(Ident::new("now"), span),
                            args: Vec::new(),
                        },
                        span,
                    ))
                } else {
                    // Treat as identifier
                    let name = {
                        let token = self.current().unwrap();
                        self.token_string(token)
                    };
                    self.advance();
                    Ok(Spanned::new(Expr::Column(ColumnRef::simple(name)), span))
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

    fn parse_array(&mut self) -> ParseResult<Spanned<Expr>> {
        let start = self.current_span().start;
        self.expect(TokenKind::LeftBracket)?;
        let mut elements = Vec::new();

        while !self.check(TokenKind::RightBracket) && !self.is_at_end() {
            elements.push(self.parse_expr()?);

            if !self.check(TokenKind::RightBracket) {
                self.expect(TokenKind::Comma)?;
            }
        }

        let end = self.current_span().end;
        self.expect(TokenKind::RightBracket)?;
        Ok(Spanned::new(Expr::Array(elements), Span::new(start, end)))
    }

    fn parse_object(&mut self) -> ParseResult<Spanned<Expr>> {
        let start = self.current_span().start;
        self.expect(TokenKind::LeftBrace)?;
        self.skip_newlines();

        let mut fields = Vec::new();
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
            let value = self.parse_expr()?;

            fields.push((key, value));

            self.skip_newlines();

            if !indented && !self.check(TokenKind::RightBrace) {
                self.expect(TokenKind::Comma)?;
            }
        }

        let end = self.current_span().end;
        self.expect(TokenKind::RightBrace)?;
        Ok(Spanned::new(Expr::Object(fields), Span::new(start, end)))
    }
}

// Public API

pub fn parse(source: &str) -> Result<Vec<Statement>, Vec<String>> {
    let tokens = super::lexer::lex(source.as_bytes());
    let mut parser = Parser::new(&tokens, source.as_bytes());

    match parser.parse() {
        Ok(statements) => Ok(statements),
        Err(diagnostics) => {
            let errors: Vec<String> = diagnostics.iter().map(|d| d.render(source)).collect();
            Err(errors)
        }
    }
}

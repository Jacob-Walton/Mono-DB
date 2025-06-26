use super::error::{ParseError, ParseResult};
use super::lexer::{Lexer, Token};
use crate::nsql::ast::*;
use crate::nsql::interner::{StringInterner};

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current_token: Token,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str, interner: &'a mut StringInterner) -> ParseResult<Self> {
        let mut lexer = Lexer::new(input, interner);
        let current_token = lexer.next()?;

        Ok(Self {
            lexer,
            current_token,
        })
    }

    pub fn parse(mut self) -> ParseResult<Program> {
        let mut statements = Vec::new();

        while self.current_token != Token::Eof {
            statements.push(self.parse_statement()?);

            if self.current_token == Token::Semicolon {
                self.advance()?;
            }
        }

        Ok(Program { statements })
    }

    fn parse_statement(&mut self) -> ParseResult<Statement> {
        match &self.current_token {
            Token::Select => Ok(Statement::Query(self.parse_query()?)),
            Token::Insert => Ok(Statement::Insert(self.parse_insert()?)),
            Token::Update => Ok(Statement::Update(self.parse_update()?)),
            Token::Delete => Ok(Statement::Delete(self.parse_delete()?)),
            Token::Create => Ok(Statement::CreateTable(self.parse_create_table()?)),
            Token::Drop => Ok(Statement::DropTable(self.parse_drop_table()?)),
            Token::Describe => Ok(Statement::DescribeTable(self.parse_describe_table()?)),
            _ => Err(self.unexpected_token("statement")),
        }
    }

    fn parse_query(&mut self) -> ParseResult<Query> {
        self.expect(Token::Select)?;

        let projection = self.parse_select_list()?;

        let from = if self.current_token == Token::From {
            self.advance()?;
            Some(self.parse_qualified_name()?)
        } else {
            None
        };

        let where_clause = if self.current_token == Token::Where {
            self.advance()?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Query {
            projection,
            from,
            where_clause,
        })
    }

    fn parse_select_list(&mut self) -> ParseResult<Vec<SelectItem>> {
        let mut items = vec![self.parse_select_item()?];

        while self.current_token == Token::Comma {
            self.advance()?;
            items.push(self.parse_select_item()?);
        }

        Ok(items)
    }

    fn parse_select_item(&mut self) -> ParseResult<SelectItem> {
        let expr = self.parse_expression()?;

        let alias = if matches!(self.current_token, Token::Identifier(_)) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(SelectItem { expr, alias })
    }

    fn parse_expression(&mut self) -> ParseResult<Expression> {
        self.parse_and_expression()
    }

    fn parse_and_expression(&mut self) -> ParseResult<Expression> {
        let mut left = self.parse_comparison_expression()?;

        while self.current_token == Token::And {
            self.advance()?;
            let right = self.parse_comparison_expression()?;
            left = Expression::BinaryOp {
                op: BinaryOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_comparison_expression(&mut self) -> ParseResult<Expression> {
        let left = self.parse_primary_expression()?;

        let op = match &self.current_token {
            Token::Equal => {
                self.advance()?;
                BinaryOp::Eq
            }
            Token::NotEqual => {
                self.advance()?;
                BinaryOp::NotEq
            }
            Token::Less => {
                self.advance()?;
                BinaryOp::Lt
            }
            Token::Greater => {
                self.advance()?;
                BinaryOp::Gt
            }
            Token::LessEqual => {
                self.advance()?;
                BinaryOp::LtEq
            }
            Token::GreaterEqual => {
                self.advance()?;
                BinaryOp::GtEq
            }
            _ => return Ok(left),
        };

        let right = self.parse_primary_expression()?;

        Ok(Expression::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn parse_primary_expression(&mut self) -> ParseResult<Expression> {
        match &self.current_token {
            Token::Star => {
                self.advance()?;
                Ok(Expression::Star)
            }
            Token::IntLiteral(n) => {
                let value = *n;
                self.advance()?;
                Ok(Expression::Literal(Literal::Integer(value)))
            }
            Token::StringLiteral(s) => {
                let value = *s;
                self.advance()?;
                Ok(Expression::Literal(Literal::String(value)))
            }
            Token::True => {
                self.advance()?;
                Ok(Expression::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance()?;
                Ok(Expression::Literal(Literal::Boolean(false)))
            }
            Token::Null => {
                self.advance()?;
                Ok(Expression::Literal(Literal::Null))
            }
            Token::Identifier(_) => {
                let name = self.parse_qualified_name()?;
                Ok(Expression::Column(name))
            }
            Token::LeftParen => {
                self.advance()?;
                let expr = self.parse_expression()?;
                self.expect(Token::RightParen)?;
                Ok(expr)
            }
            _ => Err(self.unexpected_token("expression")),
        }
    }

    fn parse_insert(&mut self) -> ParseResult<InsertStmt> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let table = self.parse_qualified_name()?;

        let columns = if self.current_token == Token::LeftParen {
            self.advance()?;
            let cols = self.parse_identifier_list()?;
            self.expect(Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect(Token::Values)?;
        let values = self.parse_values_list()?;

        Ok(InsertStmt {
            table,
            columns,
            values,
        })
    }

    fn parse_values_list(&mut self) -> ParseResult<Vec<Vec<Expression>>> {
        let mut rows = vec![self.parse_value_row()?];

        while self.current_token == Token::Comma {
            self.advance()?;
            rows.push(self.parse_value_row()?);
        }

        Ok(rows)
    }

    fn parse_value_row(&mut self) -> ParseResult<Vec<Expression>> {
        self.expect(Token::LeftParen)?;

        let mut values = vec![self.parse_expression()?];

        while self.current_token == Token::Comma {
            self.advance()?;
            values.push(self.parse_expression()?);
        }

        self.expect(Token::RightParen)?;
        Ok(values)
    }

    fn parse_update(&mut self) -> ParseResult<UpdateStmt> {
        self.expect(Token::Update)?;

        let table = self.parse_qualified_name()?;

        self.expect(Token::Set)?;
        let assignments = self.parse_assignments()?;

        let where_clause = if self.current_token == Token::Where {
            self.advance()?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(UpdateStmt {
            table,
            assignments,
            where_clause,
        })
    }

    fn parse_assignments(&mut self) -> ParseResult<Vec<Assignment>> {
        let mut assignments = vec![self.parse_assignment()?];

        while self.current_token == Token::Comma {
            self.advance()?;
            assignments.push(self.parse_assignment()?);
        }

        Ok(assignments)
    }

    fn parse_assignment(&mut self) -> ParseResult<Assignment> {
        let column = self.parse_identifier()?;
        self.expect(Token::Equal)?;
        let value = self.parse_expression()?;

        Ok(Assignment { column, value })
    }

    fn parse_delete(&mut self) -> ParseResult<DeleteStmt> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;

        let table = self.parse_qualified_name()?;

        let where_clause = if self.current_token == Token::Where {
            self.advance()?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(DeleteStmt {
            table,
            where_clause,
        })
    }

    fn parse_create_table(&mut self) -> ParseResult<CreateTableStmt> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;

        let name = self.parse_qualified_name()?;

        self.expect(Token::LeftParen)?;
        let columns = self.parse_column_definitions()?;
        self.expect(Token::RightParen)?;

        Ok(CreateTableStmt { name, columns })
    }

    fn parse_column_definitions(&mut self) -> ParseResult<Vec<ColumnDef>> {
        let mut columns = vec![self.parse_column_def()?];

        while self.current_token == Token::Comma {
            self.advance()?;
            columns.push(self.parse_column_def()?);
        }

        Ok(columns)
    }

    fn parse_column_def(&mut self) -> ParseResult<ColumnDef> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;

        let mut constraints = Vec::new();

        while matches!(self.current_token, Token::Not | Token::Primary) {
            match &self.current_token {
                Token::Not => {
                    self.advance()?;
                    self.expect(Token::Null)?;
                    constraints.push(ColumnConstraint::NotNull);
                }
                Token::Primary => {
                    self.advance()?;
                    self.expect(Token::Key)?;
                    constraints.push(ColumnConstraint::PrimaryKey);
                }
                _ => break,
            }
        }

        Ok(ColumnDef {
            name,
            data_type,
            constraints,
        })
    }

    fn parse_data_type(&mut self) -> ParseResult<DataType> {
        match &self.current_token {
            Token::Integer => {
                self.advance()?;
                Ok(DataType::Integer)
            }
            Token::Text => {
                self.advance()?;
                Ok(DataType::Text)
            }
            Token::Boolean => {
                self.advance()?;
                Ok(DataType::Boolean)
            }
            Token::Varchar => {
                self.advance()?;
                let size = if self.current_token == Token::LeftParen {
                    self.advance()?;
                    let size = self.parse_type_size()?;
                    self.expect(Token::RightParen)?;
                    Some(size)
                } else {
                    None
                };
                Ok(DataType::Varchar(size))
            }
            Token::Char => {
                self.advance()?;
                let size = if self.current_token == Token::LeftParen {
                    self.advance()?;
                    let size = self.parse_type_size()?;
                    self.expect(Token::RightParen)?;
                    Some(size)
                } else {
                    None
                };
                Ok(DataType::Char(size))
            }
            Token::Decimal => {
                self.advance()?;
                let precision_scale = if self.current_token == Token::LeftParen {
                    self.advance()?;

                    if let Token::IntLiteral(precision) = self.current_token {
                        self.advance()?;

                        let scale = if self.current_token == Token::Comma {
                            self.advance()?;
                            if let Token::IntLiteral(scale) = self.current_token {
                                self.advance()?;
                                scale as u8
                            } else {
                                return Err(self.unexpected_token("scale"));
                            }
                        } else {
                            0
                        };

                        self.expect(Token::RightParen)?;
                        Some((precision as u8, scale))
                    } else {
                        return Err(self.unexpected_token("precision"));
                    }
                } else {
                    None
                };
                Ok(DataType::Decimal(precision_scale))
            }
            _ => Err(self.unexpected_token("data type")),
        }
    }

    fn parse_type_size(&mut self) -> ParseResult<u32> {
        if let Token::IntLiteral(size) = self.current_token {
            self.advance()?;
            Ok(size as u32)
        } else {
            Err(self.unexpected_token("size"))
        }
    }

    fn parse_drop_table(&mut self) -> ParseResult<DropTableStmt> {
        self.expect(Token::Drop)?;
        self.expect(Token::Table)?;

        let name = self.parse_qualified_name()?;

        Ok(DropTableStmt { name })
    }

    fn parse_describe_table(&mut self) -> ParseResult<DescribeTableStmt> {
        self.expect(Token::Describe)?;

        let name = self.parse_qualified_name()?;

        Ok(DescribeTableStmt { name })
    }

    fn parse_qualified_name(&mut self) -> ParseResult<QualifiedName> {
        let first = self.parse_identifier()?;
        let mut parts = smallvec::smallvec![first.0];

        while self.current_token == Token::Dot {
            self.advance()?;
            let next = self.parse_identifier()?;
            parts.push(next.0);
        }

        Ok(QualifiedName { parts })
    }

    fn parse_identifier(&mut self) -> ParseResult<Identifier> {
        if let Token::Identifier(id) = &self.current_token {
            let identifier = Identifier(*id);
            self.advance()?;
            Ok(identifier)
        } else {
            Err(self.unexpected_token("identifier"))
        }
    }

    fn parse_identifier_list(&mut self) -> ParseResult<Vec<Identifier>> {
        let mut identifiers = vec![self.parse_identifier()?];

        while self.current_token == Token::Comma {
            self.advance()?;
            identifiers.push(self.parse_identifier()?);
        }

        Ok(identifiers)
    }

    fn advance(&mut self) -> ParseResult<()> {
        self.current_token = self.lexer.next()?;
        Ok(())
    }

    fn expect(&mut self, expected: Token) -> ParseResult<()> {
        if std::mem::discriminant(&self.current_token) == std::mem::discriminant(&expected) {
            self.advance()
        } else {
            Err(self.unexpected_token(&format!("{:?}", expected)))
        }
    }

    fn unexpected_token(&self, expected: &str) -> ParseError {
        ParseError::UnexpectedToken {
            expected: expected.to_string(),
            found: format!("{:?}", self.current_token),
            position: 0,
        }
    }
}

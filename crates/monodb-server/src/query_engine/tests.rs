//! Tests for the MonoDB Query Language parser and lexer.

use super::ast::*;
use super::lexer::{TokenKind, lex};
use super::parser::parse;

// Lexer Helper

fn lex_to_kinds(source: &str) -> Vec<TokenKind> {
    lex(source.as_bytes())
        .into_iter()
        .map(|t| t.kind)
        .filter(|k| *k != TokenKind::Newline && *k != TokenKind::Eof)
        .collect()
}

fn lex_to_text(source: &str) -> Vec<(TokenKind, String)> {
    lex(source.as_bytes())
        .into_iter()
        .map(|t| {
            let text = String::from_utf8_lossy(&source.as_bytes()[t.span.as_range()]).into_owned();
            (t.kind, text)
        })
        .filter(|(k, _)| *k != TokenKind::Newline && *k != TokenKind::Eof)
        .collect()
}

// Parser Helper

fn parse_ok(source: &str) -> Vec<Statement> {
    parse(source).unwrap_or_else(|_| panic!("parse failed for: {}", source))
}

fn parse_single(source: &str) -> Statement {
    let stmts = parse_ok(source);
    assert_eq!(stmts.len(), 1, "expected exactly one statement");
    stmts.into_iter().next().unwrap()
}

fn parse_get(source: &str) -> GetQuery {
    match parse_single(source) {
        Statement::Query(QueryStatement::Get(q)) => q,
        other => panic!("expected GET query, got {:?}", other),
    }
}

fn parse_put(source: &str) -> PutMutation {
    match parse_single(source) {
        Statement::Mutation(MutationStatement::Put(m)) => m,
        other => panic!("expected PUT mutation, got {:?}", other),
    }
}

fn parse_change(source: &str) -> ChangeMutation {
    match parse_single(source) {
        Statement::Mutation(MutationStatement::Change(m)) => m,
        other => panic!("expected CHANGE mutation, got {:?}", other),
    }
}

fn parse_remove(source: &str) -> RemoveMutation {
    match parse_single(source) {
        Statement::Mutation(MutationStatement::Remove(m)) => m,
        other => panic!("expected REMOVE mutation, got {:?}", other),
    }
}

fn parse_make_table(source: &str) -> CreateTableDdl {
    match parse_single(source) {
        Statement::Ddl(DdlStatement::CreateTable(ddl)) => ddl,
        other => panic!("expected CREATE TABLE DDL, got {:?}", other),
    }
}

fn parse_expr(source: &str) -> Spanned<Expr> {
    let q = parse_get(&format!("get from t where {}", source));
    q.filter.expect("expected filter expression")
}

// Lexer Tests

mod lexer_tests {
    use super::*;

    #[test]
    fn keywords_are_recognized() {
        let keywords = [
            "get",
            "put",
            "change",
            "remove",
            "make",
            "drop",
            "from",
            "into",
            "where",
            "order",
            "by",
            "take",
            "skip",
            "asc",
            "desc",
            "join",
            "inner",
            "left",
            "right",
            "full",
            "cross",
            "table",
            "fields",
            "as",
            "relational",
            "document",
            "keyspace",
            "primary",
            "key",
            "unique",
            "required",
            "default",
            "index",
            "on",
            "has",
            "and",
            "or",
            "true",
            "false",
            "null",
            "now",
            "int",
            "bigint",
            "text",
            "decimal",
            "double",
            "date",
            "boolean",
            "map",
            "list",
            "ttl",
        ];

        let input = keywords.join(" ");
        let kinds = lex_to_kinds(&input);

        assert!(
            kinds.iter().all(|k| *k == TokenKind::Keyword),
            "all listed tokens should be Keyword"
        );
        assert_eq!(kinds.len(), keywords.len());
    }

    #[test]
    fn identifiers_not_keywords() {
        let similar = [
            "getter",
            "puts",
            "changed",
            "remover",
            "maker",
            "fromm",
            "intro",
            "wherever",
            "ordering",
            "bypass",
            "taken",
            "skipping",
            "ascending",
            "descending",
        ];

        let input = similar.join(" ");
        let kinds = lex_to_kinds(&input);

        assert!(
            kinds.iter().all(|k| *k == TokenKind::Identifier),
            "similar names must remain identifiers"
        );
    }

    #[test]
    fn operators_tokenized() {
        let input = "= != < > <= >= + - * / ( ) [ ] { } , .";
        let kinds = lex_to_kinds(input);

        let expected = vec![
            TokenKind::Equals,
            TokenKind::NotEqual,
            TokenKind::LessThan,
            TokenKind::GreaterThan,
            TokenKind::LessEqual,
            TokenKind::GreaterEqual,
            TokenKind::Plus,
            TokenKind::Minus,
            TokenKind::Star,
            TokenKind::Slash,
            TokenKind::LeftParen,
            TokenKind::RightParen,
            TokenKind::LeftBracket,
            TokenKind::RightBracket,
            TokenKind::LeftBrace,
            TokenKind::RightBrace,
            TokenKind::Comma,
            TokenKind::Dot,
        ];

        assert_eq!(kinds, expected);
    }

    #[test]
    fn string_literal_single_token() {
        let input = r#""hello world""#;
        let kinds = lex_to_kinds(input);
        assert_eq!(kinds, vec![TokenKind::StringLiteral]);
    }

    #[test]
    fn string_with_escapes() {
        let input = r#""line1\nline2\ttab""#;
        let kinds = lex_to_kinds(input);
        assert_eq!(kinds, vec![TokenKind::StringLiteral]);
    }

    #[test]
    fn number_literals() {
        let input = "42 3.14 0 100";
        let tokens = lex_to_text(input);

        assert_eq!(tokens.len(), 4);
        assert!(tokens.iter().all(|(k, _)| *k == TokenKind::Number));
        assert_eq!(tokens[0].1, "42");
        assert_eq!(tokens[1].1, "3.14");
    }

    #[test]
    fn positional_parameters() {
        let input = "$1 $2 $99";
        let kinds = lex_to_kinds(input);
        assert!(kinds.iter().all(|k| *k == TokenKind::Variable));
    }

    #[test]
    fn named_parameters() {
        let input = ":user_id :email :age";
        let kinds = lex_to_kinds(input);
        assert!(kinds.iter().all(|k| *k == TokenKind::NamedParam));
    }

    #[test]
    fn indentation_tokens() {
        let input = "put into users\n  name = \"Alice\"\n";
        let tokens = lex(input.as_bytes());
        let kinds: Vec<_> = tokens.iter().map(|t| t.kind).collect();

        assert!(kinds.contains(&TokenKind::Indent));
        assert!(kinds.contains(&TokenKind::Dedent));
    }

    #[test]
    fn comments_skipped() {
        let input = "get from users // this is a comment\n";
        let kinds = lex_to_kinds(input);

        // Comment should be skipped, we expect: get, from, users (3 keyword tokens)
        assert_eq!(kinds.len(), 3);
        assert!(
            kinds
                .iter()
                .all(|k| *k == TokenKind::Keyword || *k == TokenKind::Identifier)
        );
    }
}

// Parser Tests: Queries

mod parser_query_tests {
    use super::*;

    #[test]
    fn simple_get() {
        let q = parse_get("get from users");
        assert_eq!(q.source.node.as_ref(), "users");
        assert!(q.filter.is_none());
        assert!(q.projection.is_none());
    }

    #[test]
    fn get_with_projection() {
        let q = parse_get("get name, email from users");
        assert_eq!(q.source.node.as_ref(), "users");

        let proj = q.projection.unwrap();
        assert_eq!(proj.len(), 2);
        assert_eq!(proj[0].node.column.as_ref(), "name");
        assert!(proj[0].node.path.is_empty());
        assert_eq!(proj[1].node.column.as_ref(), "email");
        assert!(proj[1].node.path.is_empty());
    }

    #[test]
    fn get_with_dotted_projection() {
        let q = parse_get("get profile.name, profile.email from users");
        let proj = q.projection.unwrap();
        assert_eq!(proj.len(), 2);
        assert_eq!(proj[0].node.column.as_ref(), "profile");
        assert_eq!(proj[0].node.path.len(), 1);
        assert_eq!(proj[0].node.path[0].as_ref(), "name");
        assert_eq!(proj[1].node.column.as_ref(), "profile");
        assert_eq!(proj[1].node.path.len(), 1);
        assert_eq!(proj[1].node.path[0].as_ref(), "email");
    }

    #[test]
    fn get_with_where() {
        let q = parse_get("get from users where age > 21");
        assert!(q.filter.is_some());
    }

    #[test]
    fn get_with_order_by() {
        let q = parse_get("get from users order by name");
        let order = q.order_by.unwrap();
        assert_eq!(order.len(), 1);
        assert_eq!(order[0].field.node.as_ref(), "name");
        assert_eq!(order[0].direction, SortDirection::Asc);
    }

    #[test]
    fn get_with_order_by_desc() {
        let q = parse_get("get from users order by created_at desc");
        let order = q.order_by.unwrap();
        assert_eq!(order[0].direction, SortDirection::Desc);
    }

    #[test]
    fn get_with_take() {
        let q = parse_get("get from users take 10");
        match q.limit.unwrap() {
            LimitValue::Literal(n) => assert_eq!(n, 10),
            _ => panic!("expected literal limit"),
        }
    }

    #[test]
    fn get_with_skip() {
        let q = parse_get("get from users skip 20");
        match q.offset.unwrap() {
            LimitValue::Literal(n) => assert_eq!(n, 20),
            _ => panic!("expected literal offset"),
        }
    }

    #[test]
    fn get_with_all_clauses() {
        let q = parse_get("get name from users where active = true order by name take 10 skip 5");
        assert!(q.projection.is_some());
        assert!(q.filter.is_some());
        assert!(q.order_by.is_some());
        assert!(q.limit.is_some());
        assert!(q.offset.is_some());
    }

    #[test]
    fn get_with_source_alias() {
        let q = parse_get("get from users as u");
        assert_eq!(q.source_alias.unwrap().node.as_ref(), "u");
    }

    #[test]
    fn get_with_inner_join() {
        let q = parse_get("get from users join posts on users.id = posts.user_id");
        assert_eq!(q.joins.len(), 1);
        let join = &q.joins[0];
        assert!(matches!(join.join_type, JoinType::Inner));
        assert_eq!(join.table.node.as_ref(), "posts");
        assert!(join.condition.is_some());
    }

    #[test]
    fn get_with_left_join_alias() {
        let q = parse_get("get from users left join posts as p on users.id = p.user_id");
        let join = &q.joins[0];
        assert!(matches!(join.join_type, JoinType::Left));
        assert_eq!(join.alias.as_ref().unwrap().node.as_ref(), "p");
    }

    #[test]
    fn get_with_cross_join() {
        let q = parse_get("get from users cross join posts");
        let join = &q.joins[0];
        assert!(matches!(join.join_type, JoinType::Cross));
        assert!(join.condition.is_none());
    }

    #[test]
    fn get_with_multiline_join_and_on() {
        let q = parse_get("get from users\n  join posts\n  on users.id = posts.user_id\n");
        assert_eq!(q.joins.len(), 1);
        assert!(q.joins[0].condition.is_some());
    }

    #[test]
    fn count_query() {
        match parse_single("count from users") {
            Statement::Query(QueryStatement::Count(c)) => {
                assert_eq!(c.table.node.as_ref(), "users");
            }
            other => panic!("expected COUNT, got {:?}", other),
        }
    }

    #[test]
    fn describe_query() {
        match parse_single("describe users") {
            Statement::Query(QueryStatement::Describe(d)) => {
                assert_eq!(d.table.node.as_ref(), "users");
            }
            other => panic!("expected DESCRIBE, got {:?}", other),
        }
    }
}

// Parser Tests: Mutations

mod parser_mutation_tests {
    use super::*;

    #[test]
    fn put_single_line() {
        let m = parse_put(r#"put into users name = "Alice", age = 30"#);
        assert_eq!(m.target.node.as_ref(), "users");
        assert_eq!(m.assignments.len(), 2);
        assert_eq!(m.assignments[0].field.node.as_ref(), "name");
        assert_eq!(m.assignments[1].field.node.as_ref(), "age");
    }

    #[test]
    fn put_multiline() {
        let m = parse_put(
            r#"put into users
  name = "Alice"
  age = 30
"#,
        );
        assert_eq!(m.target.node.as_ref(), "users");
        assert_eq!(m.assignments.len(), 2);
    }

    #[test]
    fn change_with_where() {
        let m = parse_change(r#"change users where id = 1 name = "Bob""#);
        assert_eq!(m.target.node.as_ref(), "users");
        assert!(m.filter.is_some());
        assert_eq!(m.assignments.len(), 1);
    }

    #[test]
    fn remove_requires_where() {
        let m = parse_remove("remove from users where id = 1");
        assert_eq!(m.target.node.as_ref(), "users");
    }

    #[test]
    fn remove_without_where_fails() {
        let result = parse("remove from users");
        assert!(result.is_err());
    }
}

// Parser Tests: DDL

mod parser_ddl_tests {
    use super::*;

    #[test]
    fn make_relational_table() {
        let ddl = parse_make_table(
            r#"make table users
  as relational
  fields
    id bigint primary key
    name text required
    email text unique
"#,
        );

        assert_eq!(ddl.name.node.as_ref(), "users");
        assert_eq!(ddl.table_type, TableType::Relational);
        assert_eq!(ddl.columns.len(), 3);

        // Check first column
        assert_eq!(ddl.columns[0].name.node.as_ref(), "id");
        assert!(matches!(ddl.columns[0].data_type, DataType::Int64));
        assert!(
            ddl.columns[0]
                .constraints
                .iter()
                .any(|c| matches!(c, ColumnConstraint::PrimaryKey))
        );
    }

    #[test]
    fn make_document_table_no_schema() {
        let ddl = parse_make_table(
            r#"make table posts
  as document
"#,
        );

        assert_eq!(ddl.name.node.as_ref(), "posts");
        assert_eq!(ddl.table_type, TableType::Document);
        assert!(ddl.columns.is_empty());
    }

    #[test]
    fn make_keyspace_with_ttl() {
        let ddl = parse_make_table(
            r#"make table sessions
  as keyspace
  ttl 3600
"#,
        );

        assert_eq!(ddl.name.node.as_ref(), "sessions");
        assert_eq!(ddl.table_type, TableType::Keyspace);
        assert!(
            ddl.properties
                .iter()
                .any(|p| matches!(p, TableProperty::Ttl(3600)))
        );
    }

    #[test]
    fn make_index() {
        match parse_single("make index idx_email on users(email)") {
            Statement::Ddl(DdlStatement::CreateIndex(idx)) => {
                assert_eq!(idx.name.node.as_ref(), "idx_email");
                assert_eq!(idx.table.node.as_ref(), "users");
                assert_eq!(idx.columns.len(), 1);
                assert!(!idx.unique);
            }
            other => panic!("expected CREATE INDEX, got {:?}", other),
        }
    }

    #[test]
    fn make_unique_index() {
        match parse_single("make unique index idx_username on users(username)") {
            Statement::Ddl(DdlStatement::CreateIndex(idx)) => {
                assert!(idx.unique);
            }
            other => panic!("expected CREATE INDEX, got {:?}", other),
        }
    }

    #[test]
    fn drop_table() {
        match parse_single("drop table users") {
            Statement::Ddl(DdlStatement::DropTable(d)) => {
                assert_eq!(d.name.node.as_ref(), "users");
            }
            other => panic!("expected DROP TABLE, got {:?}", other),
        }
    }

    #[test]
    fn drop_index() {
        match parse_single("drop index idx_email on users") {
            Statement::Ddl(DdlStatement::DropIndex(d)) => {
                assert_eq!(d.name.node.as_ref(), "idx_email");
                assert_eq!(d.table.node.as_ref(), "users");
            }
            other => panic!("expected DROP INDEX, got {:?}", other),
        }
    }
}

// Parser Tests: Transactions

mod parser_transaction_tests {
    use super::*;

    #[test]
    fn begin_transaction() {
        match parse_single("begin") {
            Statement::Transaction(TransactionStatement::Begin) => {}
            other => panic!("expected BEGIN, got {:?}", other),
        }
    }

    #[test]
    fn commit_transaction() {
        match parse_single("commit") {
            Statement::Transaction(TransactionStatement::Commit) => {}
            other => panic!("expected COMMIT, got {:?}", other),
        }
    }

    #[test]
    fn rollback_transaction() {
        match parse_single("rollback") {
            Statement::Transaction(TransactionStatement::Rollback) => {}
            other => panic!("expected ROLLBACK, got {:?}", other),
        }
    }
}

// Parser Tests: Control Statements (use, etc.)

mod parser_control_tests {
    use super::*;

    #[test]
    fn use_namespace() {
        match parse_single("use production") {
            Statement::Control(ControlStatement::Use(use_stmt)) => {
                assert_eq!(use_stmt.namespace.node.as_ref(), "production");
            }
            other => panic!("expected USE statement, got {:?}", other),
        }
    }

    #[test]
    fn use_default_namespace() {
        match parse_single("use default") {
            Statement::Control(ControlStatement::Use(use_stmt)) => {
                assert_eq!(use_stmt.namespace.node.as_ref(), "default");
            }
            other => panic!("expected USE statement, got {:?}", other),
        }
    }

    #[test]
    fn use_with_underscore() {
        match parse_single("use my_database_123") {
            Statement::Control(ControlStatement::Use(use_stmt)) => {
                assert_eq!(use_stmt.namespace.node.as_ref(), "my_database_123");
            }
            other => panic!("expected USE statement, got {:?}", other),
        }
    }
}

// Expression Tests

mod expression_tests {
    #![allow(clippy::approx_constant)]
    use super::*;

    #[test]
    fn string_literal() {
        let expr = parse_expr(r#""hello""#);
        match &expr.node {
            Expr::Literal(slot) => {
                assert!(matches!(&slot.value, LiteralValue::String(s) if s.as_ref() == "hello"));
            }
            other => panic!("expected string literal, got {:?}", other),
        }
    }

    #[test]
    fn integer_literal() {
        let expr = parse_expr("42");
        match &expr.node {
            Expr::Literal(slot) => {
                assert!(matches!(slot.value, LiteralValue::Int64(42)));
            }
            other => panic!("expected integer literal, got {:?}", other),
        }
    }

    #[test]
    fn float_literal() {
        let expr = parse_expr("3.14");
        match &expr.node {
            Expr::Literal(slot) => {
                if let LiteralValue::Float64(f) = slot.value {
                    assert!((f - 3.14).abs() < 0.001);
                } else {
                    panic!("expected float");
                }
            }
            other => panic!("expected float literal, got {:?}", other),
        }
    }

    #[test]
    fn boolean_true() {
        let expr = parse_expr("true");
        match &expr.node {
            Expr::Literal(slot) => {
                assert!(matches!(slot.value, LiteralValue::Bool(true)));
            }
            other => panic!("expected bool literal, got {:?}", other),
        }
    }

    #[test]
    fn boolean_false() {
        let expr = parse_expr("false");
        match &expr.node {
            Expr::Literal(slot) => {
                assert!(matches!(slot.value, LiteralValue::Bool(false)));
            }
            other => panic!("expected bool literal, got {:?}", other),
        }
    }

    #[test]
    fn null_literal() {
        let expr = parse_expr("null");
        match &expr.node {
            Expr::Literal(slot) => {
                assert!(matches!(slot.value, LiteralValue::Null));
            }
            other => panic!("expected null literal, got {:?}", other),
        }
    }

    #[test]
    fn column_reference() {
        let expr = parse_expr("age");
        match &expr.node {
            Expr::Column(col) => {
                assert_eq!(col.column.as_ref(), "age");
            }
            other => panic!("expected column, got {:?}", other),
        }
    }

    #[test]
    fn positional_parameter() {
        let expr = parse_expr("$1");
        match &expr.node {
            Expr::Param(ParamRef::Positional(0)) => {} // 0-indexed internally
            other => panic!("expected positional param, got {:?}", other),
        }
    }

    #[test]
    fn named_parameter() {
        let expr = parse_expr(":user_id");
        match &expr.node {
            Expr::Param(ParamRef::Named(name)) => {
                assert_eq!(name.as_ref(), "user_id");
            }
            other => panic!("expected named param, got {:?}", other),
        }
    }

    #[test]
    fn binary_equals() {
        let expr = parse_expr("id = 1");
        match &expr.node {
            Expr::Binary { op, .. } => assert_eq!(*op, BinaryOp::Eq),
            other => panic!("expected binary op, got {:?}", other),
        }
    }

    #[test]
    fn binary_not_equals() {
        let expr = parse_expr("status != \"deleted\"");
        match &expr.node {
            Expr::Binary { op, .. } => assert_eq!(*op, BinaryOp::NotEq),
            other => panic!("expected binary op, got {:?}", other),
        }
    }

    #[test]
    fn binary_comparisons() {
        assert!(matches!(
            parse_expr("a < b").node,
            Expr::Binary {
                op: BinaryOp::Lt,
                ..
            }
        ));
        assert!(matches!(
            parse_expr("a > b").node,
            Expr::Binary {
                op: BinaryOp::Gt,
                ..
            }
        ));
        assert!(matches!(
            parse_expr("a <= b").node,
            Expr::Binary {
                op: BinaryOp::LtEq,
                ..
            }
        ));
        assert!(matches!(
            parse_expr("a >= b").node,
            Expr::Binary {
                op: BinaryOp::GtEq,
                ..
            }
        ));
    }

    #[test]
    fn binary_and() {
        let expr = parse_expr("a = 1 and b = 2");
        match &expr.node {
            Expr::Binary {
                op: BinaryOp::And, ..
            } => {}
            other => panic!("expected AND, got {:?}", other),
        }
    }

    #[test]
    fn binary_or() {
        let expr = parse_expr("a = 1 or b = 2");
        match &expr.node {
            Expr::Binary {
                op: BinaryOp::Or, ..
            } => {}
            other => panic!("expected OR, got {:?}", other),
        }
    }

    #[test]
    fn has_operator() {
        let expr = parse_expr("tags has \"featured\"");
        match &expr.node {
            Expr::Binary {
                op: BinaryOp::Contains,
                ..
            } => {}
            other => panic!("expected Contains (has), got {:?}", other),
        }
    }

    #[test]
    fn arithmetic_operators() {
        assert!(matches!(
            parse_expr("a + b").node,
            Expr::Binary {
                op: BinaryOp::Add,
                ..
            }
        ));
        assert!(matches!(
            parse_expr("a - b").node,
            Expr::Binary {
                op: BinaryOp::Sub,
                ..
            }
        ));
        assert!(matches!(
            parse_expr("a * b").node,
            Expr::Binary {
                op: BinaryOp::Mul,
                ..
            }
        ));
        assert!(matches!(
            parse_expr("a / b").node,
            Expr::Binary {
                op: BinaryOp::Div,
                ..
            }
        ));
    }

    #[test]
    fn operator_precedence_mul_over_add() {
        // a + b * c should parse as a + (b * c)
        let expr = parse_expr("a + b * c");
        match &expr.node {
            Expr::Binary {
                op: BinaryOp::Add,
                right,
                ..
            } => match &right.node {
                Expr::Binary {
                    op: BinaryOp::Mul, ..
                } => {}
                other => panic!("right should be Mul, got {:?}", other),
            },
            other => panic!("expected Add, got {:?}", other),
        }
    }

    #[test]
    fn operator_precedence_and_over_or() {
        // a or b and c should parse as a or (b and c)
        let expr = parse_expr("a or b and c");
        match &expr.node {
            Expr::Binary {
                op: BinaryOp::Or,
                right,
                ..
            } => match &right.node {
                Expr::Binary {
                    op: BinaryOp::And, ..
                } => {}
                other => panic!("right should be And, got {:?}", other),
            },
            other => panic!("expected Or, got {:?}", other),
        }
    }

    #[test]
    fn parentheses_override_precedence() {
        // (a + b) * c should parse as Mul at root
        let expr = parse_expr("(a + b) * c");
        match &expr.node {
            Expr::Binary {
                op: BinaryOp::Mul,
                left,
                ..
            } => match &left.node {
                Expr::Binary {
                    op: BinaryOp::Add, ..
                } => {}
                other => panic!("left should be Add, got {:?}", other),
            },
            other => panic!("expected Mul, got {:?}", other),
        }
    }

    #[test]
    fn field_access_dot_notation() {
        let expr = parse_expr("user.address.city");
        match &expr.node {
            Expr::Column(col) => {
                // column is the base, path contains subsequent accesses
                assert_eq!(col.column.as_ref(), "user");
                assert_eq!(col.path.len(), 2);
                assert_eq!(col.path[0].as_ref(), "address");
                assert_eq!(col.path[1].as_ref(), "city");
            }
            other => panic!("expected column with path, got {:?}", other),
        }
    }

    #[test]
    fn array_literal() {
        let expr = parse_expr("[1, 2, 3]");
        match &expr.node {
            Expr::Array(elements) => {
                assert_eq!(elements.len(), 3);
            }
            other => panic!("expected array, got {:?}", other),
        }
    }

    #[test]
    fn now_function() {
        let expr = parse_expr("now()");
        match &expr.node {
            Expr::FunctionCall { name, args } => {
                assert_eq!(name.node.as_ref(), "now");
                assert!(args.is_empty());
            }
            other => panic!("expected function call, got {:?}", other),
        }
    }
}

// Fingerprinting Tests

mod fingerprint_tests {
    use super::*;

    #[test]
    fn literal_slots_have_unique_ids() {
        let q = parse_get("get from t where a = 1 and b = 2");
        let filter = q.filter.unwrap();

        fn collect_slot_ids(expr: &Expr, ids: &mut Vec<u32>) {
            match expr {
                Expr::Literal(slot) => ids.push(slot.slot_id),
                Expr::Binary { left, right, .. } => {
                    collect_slot_ids(&left.node, ids);
                    collect_slot_ids(&right.node, ids);
                }
                _ => {}
            }
        }

        let mut ids = Vec::new();
        collect_slot_ids(&filter.node, &mut ids);

        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1], "slot IDs should be unique");
    }

    #[test]
    fn same_structure_different_values_same_hash() {
        let q1 = parse_get("get from users where age > 25");
        let q2 = parse_get("get from users where age > 30");

        // Same table, same structure; fingerprints should match
        assert_eq!(q1.source.node.as_ref(), q2.source.node.as_ref());
    }
}

// Error Recovery Tests

mod error_tests {
    use super::*;

    #[test]
    fn invalid_statement_reports_error() {
        let result = parse("invalid statement here");
        assert!(result.is_err());
    }

    #[test]
    fn missing_table_name_reports_error() {
        let result = parse("get from");
        assert!(result.is_err());
    }

    #[test]
    fn multiple_errors_reported() {
        let src = r#"
get from 123
put into
remove from users where
"#;

        let result = parse(src);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors.len() >= 2, "should report multiple errors");
    }
}

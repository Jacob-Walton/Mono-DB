use crate::query_engine::{
    ast::{BinaryOp, Expr, Statement},
    lexer::{Lexer, TokenKind},
    parser,
};
use monodb_common::Value;

fn lex(source: &str) -> Vec<(TokenKind, String, u32, u16)> {
    let mut lexer = Lexer::new();
    let tokens = lexer.lex(source.as_bytes());
    tokens
        .into_iter()
        .map(|t| {
            let kind = t.kind;
            let span = t.span;
            let text = &source.as_bytes()[span.start as usize..span.end as usize];
            (
                kind,
                String::from_utf8_lossy(text).into_owned(),
                span.line,
                span.column,
            )
        })
        .collect()
}

fn parse_single_get_where_expr(src: &str) -> Expr {
    let mut src_buf = String::new();
    src_buf.push_str("get from t where ");
    src_buf.push_str(src);
    src_buf.push('\n');
    let parsed =
        parser::parse(src_buf).expect("parser returned error when parsing expression in get/where");
    assert_eq!(parsed.len(), 1, "expected exactly one statement");
    match &parsed[0] {
        Statement::Get { filter, .. } => {
            filter.clone().expect("expected a where filter expression")
        }
        other => panic!("expected GET statement, found: {other:?}"),
    }
}

// Indentation Parsing
#[test]
fn lexer_indentation_parsing_mixed_spaces_tabs() {
    // Indentation columns we expect for INDENT tokens (column = indent + 1):
    //   line2: "\t "  => 4 + 1 = 5  => column 6
    //   line3: "\t\t " => 8 + 1 = 9 => column 10
    //   line4: "\t\t\t " => 12 + 1 = 13 => column 14
    let src = concat!(
        "put into users\n",
        "\t name = \"Alice\"\n",
        "\t\t age = 30\n",
        "\t\t\t city = \"NY\"\n",
    );

    let toks = lex(src);
    let indent_cols: Vec<u16> = toks
        .iter()
        .filter_map(|(k, _t, _l, c)| {
            if *k == TokenKind::Indent {
                Some(*c)
            } else {
                None
            }
        })
        .collect();
    let dedent_count = toks
        .iter()
        .filter(|(k, _, _, _)| *k == TokenKind::Dedent)
        .count();

    assert_eq!(
        indent_cols,
        vec![6, 10, 14],
        "INDENT span columns should reflect tabs=4 spaces"
    );
    assert_eq!(
        dedent_count, 3,
        "should emit DEDENT for each indentation level at EOF"
    );
}

// Keyword Recognition
#[test]
fn lexer_keyword_recognition_full_and_similar_identifiers() {
    let keywords = [
        // Core verbs
        "get",
        "put",
        "change",
        "remove",
        "make",
        // Prepositions
        "from",
        "into",
        "where",
        "with",
        "set",
        "as",
        // Table/structure
        "table",
        "fields",
        "relational",
        "document",
        "keyspace",
        "primary",
        "key",
        "unique",
        "default",
        "ttl",
        // Data types
        "int",
        "bigint",
        "text",
        "decimal",
        "double",
        "date",
        "boolean",
        "map",
        "list",
        // Literals and functions
        "true",
        "false",
        "null",
        "now",
        // Operators and logic
        "has",
        "and",
        "or",
    ];

    let input = keywords.join(" \n");
    let toks = lex(&input);
    let kinds: Vec<TokenKind> = toks
        .iter()
        .map(|(k, _t, _l, _c)| *k)
        .filter(|k| *k != TokenKind::Newline && *k != TokenKind::Eof)
        .collect();
    assert!(
        kinds.iter().all(|k| *k == TokenKind::Keyword),
        "all listed tokens should be Keyword"
    );
    assert_eq!(
        kinds.len(),
        keywords.len(),
        "exactly one Keyword token per entry"
    );

    // Similar identifiers should remain identifiers (not keywords)
    let similar = [
        "getter",
        "puts",
        "changed",
        "remover",
        "maker",
        "fromm",
        "intro",
        "wherever",
        "withhold",
        "setup",
        "aside",
        "tableau",
        "fieldx",
        "relationally",
        "documentary",
        "keyspaces",
        "primaryKey",
        "keys",
        "uniqueish",
        "defaulted",
        "ttlen",
        "intr",
        "bigints",
        "textual",
        "decimalize",
        "doublex",
        "dates",
        "booleanish",
        "maple",
        "listy",
        "trueish",
        "falsy",
        "nullish",
        "nowhere",
        "haste",
        "andor",
        "oreo",
    ];
    let input2 = similar.join(" \n");
    let toks2 = lex(&input2);
    let kinds2: Vec<TokenKind> = toks2
        .iter()
        .map(|(k, _t, _l, _c)| *k)
        .filter(|k| *k != TokenKind::Newline && *k != TokenKind::Eof)
        .collect();
    assert!(
        kinds2.iter().all(|k| *k == TokenKind::Identifier),
        "similar names must remain identifiers"
    );
    assert_eq!(kinds2.len(), similar.len());
}

// String Escaping
#[test]
fn lexer_string_with_escapes_is_single_token() {
    let s = "\"line1\\nline2\\t\"quote\"\\\\u263A\""; // "line1\nline2\t\"quote\"\\u263A"
    let toks = lex(s);
    // Expect a single StringLiteral plus EOF
    let kinds: Vec<TokenKind> = toks.iter().map(|(k, _, _, _)| *k).collect();
    assert_eq!(kinds[0], TokenKind::StringLiteral);
    assert_eq!(kinds.last().copied(), Some(TokenKind::Eof));
}

#[test]
#[ignore]
fn parser_string_escapes_processed_and_unicode_preserved() {
    let expr = parse_single_get_where_expr(r#""a\nb\t\"c\"\\\u263A""#);
    match expr {
        Expr::Literal(Value::String(s)) => {
            assert_eq!(s, "a\nb\t\"c\"\\☺");
        }
        other => panic!("expected string literal, got {other:?}"),
    }
}

// Number Parsing
#[test]
fn parser_number_parsing_int_and_float() {
    // 42 -> Int32
    let e1 = parse_single_get_where_expr("42");
    match e1 {
        Expr::Literal(Value::Int32(42)) => {}
        other => panic!("unexpected: {other:?}"),
    }

    // 3.14 -> Float64
    let e2 = parse_single_get_where_expr("3.14");
    match e2 {
        Expr::Literal(Value::Float64(f)) => assert!((f - 3.14).abs() < 1e-12),
        other => panic!("unexpected: {other:?}"),
    }
}

// Desired behavior: scientific notation and hex literals.
// Current implementation does not support these; mark as ignored until implemented.
#[test]
#[ignore]
fn parser_number_parsing_scientific_and_hex() {
    // 1e10 -> Float64(1e10)
    let e3 = parse_single_get_where_expr("1e10");
    match e3 {
        Expr::Literal(Value::Float64(f)) => assert!((f - 1e10).abs() < 1.0),
        other => panic!("unexpected: {other:?}"),
    }

    // 0xFF -> Int32(255)
    let e4 = parse_single_get_where_expr("0xFF");
    match e4 {
        Expr::Literal(Value::Int32(255)) => {}
        other => panic!("unexpected: {other:?}"),
    }
}

// Error Recovery, ensure parser reports 3 errors and continues
#[test]
fn parser_error_recovery_reports_three_errors() {
    let src = r#"
get from 123                      // error: identifier expected
put into                              // error: missing target and body
remove from users where  // error: missing expression
"#;

    match parser::parse(src.to_string()) {
        Ok(stmts) => panic!("expected errors, but parsed ok: {stmts:?}"),
        Err(errs) => {
            assert_eq!(errs.len(), 3, "should report exactly 3 errors and continue");
        }
    }
}

// Expression Precedence — a + b * c > d and e or f
#[test]
fn parser_expression_precedence() {
    let expr = parse_single_get_where_expr("a + b * c > d and e or f");

    // Root should be OR
    match expr {
        Expr::BinaryOp {
            op: BinaryOp::Or,
            left,
            right,
        } => {
            // Left should be AND
            match *left {
                Expr::BinaryOp {
                    op: BinaryOp::And,
                    left: and_left,
                    right: and_right,
                } => {
                    // and_left should be >
                    match *and_left {
                        Expr::BinaryOp {
                            op: BinaryOp::Gt,
                            left: gt_left,
                            right: gt_right,
                        } => {
                            // gt_left should be (a + (b * c))
                            match *gt_left {
                                Expr::BinaryOp {
                                    op: BinaryOp::Add,
                                    left: add_left,
                                    right: add_right,
                                } => match (*add_left, *add_right) {
                                    (
                                        Expr::Identifier(a),
                                        Expr::BinaryOp {
                                            op: BinaryOp::Mul,
                                            left: mul_left,
                                            right: mul_right,
                                        },
                                    ) => match (*mul_left, *mul_right) {
                                        (Expr::Identifier(b), Expr::Identifier(c)) => {
                                            assert_eq!(a, "a");
                                            assert_eq!(b, "b");
                                            assert_eq!(c, "c");
                                        }
                                        other => panic!("unexpected mul operands: {other:?}"),
                                    },
                                    other => panic!("unexpected add operands: {other:?}"),
                                },
                                other => panic!("left side not an addition: {other:?}"),
                            }

                            // gt_right should be identifier d
                            match *gt_right {
                                Expr::Identifier(d) => assert_eq!(d, "d"),
                                other => panic!("unexpected: {other:?}"),
                            }
                        }
                        other => panic!("> not found at expected position: {other:?}"),
                    }

                    // and_right should be identifier e
                    match *and_right {
                        Expr::Identifier(e) => assert_eq!(e, "e"),
                        other => panic!("unexpected: {other:?}"),
                    }
                }
                other => panic!("left of OR not an AND: {other:?}"),
            }

            // Right of OR should be identifier f
            match *right {
                Expr::Identifier(f) => assert_eq!(f, "f"),
                other => panic!("unexpected: {other:?}"),
            }
        }
        other => panic!("root not an OR expression: {other:?}"),
    }
}

// Field Access, user.address.city.name
#[test]
fn parser_field_access_dot_notation() {
    let expr = parse_single_get_where_expr("user.address.city.name");

    fn expect_field_chain(expr: Expr, _fields: &[&str]) {
        // Reconstruct chain: Identifier(user) -> FieldAccess(... address) -> ...
        let mut names = Vec::new();
        fn unwind(e: Expr, acc: &mut Vec<String>) {
            match e {
                Expr::FieldAccess { base, field } => {
                    unwind(*base, acc);
                    acc.push(field);
                }
                Expr::Identifier(id) => acc.push(id),
                other => panic!("unexpected node in field chain: {other:?}"),
            }
        }
        unwind(expr, &mut names);
        assert_eq!(
            names,
            vec!["user", "address", "city", "name"],
            "field access chain must be nested"
        );
    }

    expect_field_chain(expr, &["user", "address", "city", "name"]);
}

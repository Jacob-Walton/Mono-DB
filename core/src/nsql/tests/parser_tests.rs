use crate::nsql::ast::*;
use crate::nsql::interner::StringInterner;
use crate::nsql::parser::parse;

#[test]
fn test_simple_select() {
    let mut interner = StringInterner::new();
    let input = "SELECT * FROM users";

    let program = parse(input, &mut interner).unwrap();
    assert_eq!(program.statements.len(), 1);

    match &program.statements[0] {
        Statement::Query(query) => {
            assert_eq!(query.projection.len(), 1);
            assert!(query.from.is_some());
        }
        _ => panic!("Expected query statement"),
    }
}

#[test]
fn test_simple_insert() {
    let mut interner = StringInterner::new();
    let input = "INSERT INTO users (name, age) VALUES ('John', 30)";

    let program = parse(input, &mut interner).unwrap();
    assert_eq!(program.statements.len(), 1);

    match &program.statements[0] {
        Statement::Insert(insert) => {
            assert_eq!(insert.values.len(), 1);
            assert_eq!(insert.values[0].len(), 2);
        }
        _ => panic!("Expected insert statement"),
    }
}

#[test]
fn test_create_table() {
    let mut interner = StringInterner::new();
    let input = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";

    let program = parse(input, &mut interner).unwrap();
    assert_eq!(program.statements.len(), 1);

    match &program.statements[0] {
        Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);
        }
        _ => panic!("Expected create table statement"),
    }
}

#![allow(dead_code)]

//! Abstract Syntax Tree (AST) for MonoDB Query Language (MQL).

use std::hash::{Hash, Hasher};
use std::sync::Arc;

// Source location tracking

/// Source location span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Span {
    /// Byte offset of the start of this span
    pub start: u32,
    /// Byte offset of the end of this span (exclusive)
    pub end: u32,
}

impl Span {
    /// A dummy span for programmatically-generated nodes
    pub const DUMMY: Span = Span { start: 0, end: 0 };

    /// Create a new span from start to end byte offsets
    pub const fn new(start: u32, end: u32) -> Self {
        Self { start, end }
    }

    /// Merge two spans into one that covers both
    pub fn merge(self, other: Span) -> Span {
        Span {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    /// Check if this span is a dummy/unknown location
    pub fn is_dummy(&self) -> bool {
        self.start == 0 && self.end == 0
    }

    /// Get the length of this span in bytes
    pub fn len(&self) -> u32 {
        self.end.saturating_sub(self.start)
    }

    /// Check if this span is empty
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// A value with attached source location.
///
/// The span is not included in Hash/PartialEq so that structurally identical
/// AST nodes compare equal regardless of where they appeared in source.
#[derive(Debug, Clone)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

impl<T> Spanned<T> {
    pub fn new(node: T, span: Span) -> Self {
        Self { node, span }
    }

    pub fn dummy(node: T) -> Self {
        Self {
            node,
            span: Span::DUMMY,
        }
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Spanned<U> {
        Spanned {
            node: f(self.node),
            span: self.span,
        }
    }

    pub fn as_ref(&self) -> Spanned<&T> {
        Spanned {
            node: &self.node,
            span: self.span,
        }
    }
}

impl<T: Hash> Hash for Spanned<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Only hash the content, not the span (for query fingerprinting)
        self.node.hash(state);
    }
}

impl<T: PartialEq> PartialEq for Spanned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}

impl<T: Eq> Eq for Spanned<T> {}

// Identifiers

/// Interned identifier for efficient comparison and hashing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident(pub Arc<str>);

impl Ident {
    pub fn new(s: impl AsRef<str>) -> Self {
        Ident(Arc::from(s.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Ident {
    fn from(s: &str) -> Self {
        Ident::new(s)
    }
}

impl From<String> for Ident {
    fn from(s: String) -> Self {
        Ident::new(s)
    }
}

impl AsRef<str> for Ident {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Qualified identifier with optional namespace prefix.
/// Represents names like "namespace.table" or just "table".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedIdent {
    /// Optional namespace prefix (e.g., "myapp" in "myapp.users")
    pub namespace: Option<Ident>,
    /// The actual name (e.g., "users")
    pub name: Ident,
}

impl QualifiedIdent {
    /// Create a new qualified identifier with no namespace.
    pub fn simple(name: impl Into<Ident>) -> Self {
        Self {
            namespace: None,
            name: name.into(),
        }
    }

    /// Create a new qualified identifier with a namespace.
    pub fn qualified(namespace: impl Into<Ident>, name: impl Into<Ident>) -> Self {
        Self {
            namespace: Some(namespace.into()),
            name: name.into(),
        }
    }

    /// Get the full qualified name as a string (namespace.name or just name).
    pub fn full_name(&self) -> String {
        if let Some(ns) = &self.namespace {
            format!("{}.{}", ns, self.name)
        } else {
            self.name.to_string()
        }
    }

    /// Get the full qualified name with a default namespace if none specified.
    pub fn full_name_with_default(&self, default_ns: &str) -> String {
        if let Some(ns) = &self.namespace {
            format!("{}.{}", ns, self.name)
        } else {
            format!("{}.{}", default_ns, self.name)
        }
    }
}

impl std::fmt::Display for QualifiedIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ns) = &self.namespace {
            write!(f, "{}.{}", ns, self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

// Top-Level Statement Types

/// Top-level parsed statement (query, mutation, DDL, transaction, or control).
#[derive(Debug, Clone)]
pub enum Statement {
    Query(QueryStatement),
    Mutation(MutationStatement),
    Ddl(DdlStatement),
    Transaction(TransactionStatement),
    Control(ControlStatement),
}

impl Statement {
    /// Check if this statement is read-only
    pub fn is_read_only(&self) -> bool {
        matches!(self, Statement::Query(_))
    }

    /// Check if this statement modifies the schema
    pub fn is_ddl(&self) -> bool {
        matches!(self, Statement::Ddl(_))
    }
}

// Query statements (read-only)

/// Query statements that only read data.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum QueryStatement {
    /// GET: Retrieve records from a table
    Get(GetQuery),
    /// COUNT: Count records in a table
    Count(CountQuery),
    /// DESCRIBE: Get schema information for a table
    Describe(DescribeQuery),
}

/// GET query with filter, projection, ordering, limit, and offset.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct GetQuery {
    /// Source table/collection
    pub source: Spanned<Ident>,
    /// Optional alias for the source table
    pub source_alias: Option<Spanned<Ident>>,
    /// Optional JOIN clauses
    pub joins: Vec<JoinClause>,
    /// Optional filter expression (WHERE clause)
    pub filter: Option<Spanned<Expr>>,
    /// Optional column projection (SELECT specific fields)
    pub projection: Option<Vec<Spanned<ColumnRef>>>,
    /// Optional ordering (ORDER BY clause)
    pub order_by: Option<Vec<OrderByClause>>,
    /// Optional limit (TAKE/LIMIT clause)
    pub limit: Option<LimitValue>,
    /// Optional offset (SKIP/OFFSET clause)
    pub offset: Option<LimitValue>,
}

/// COUNT query to count records in a table.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CountQuery {
    pub table: Spanned<Ident>,
    pub filter: Option<Spanned<Expr>>,
}

/// DESCRIBE query to fetch table schema information.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct DescribeQuery {
    pub table: Spanned<Ident>,
}

/// ORDER BY clause element.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct OrderByClause {
    pub field: Spanned<Ident>,
    pub direction: SortDirection,
}

/// JOIN clause for GET queries.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: Spanned<Ident>,
    pub alias: Option<Spanned<Ident>>,
    pub condition: Option<Spanned<Expr>>,
}

/// JOIN types supported in GET queries.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// Limit or offset value, can be a literal or parameterized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitValue {
    Literal(u64),
    Param(ParamRef),
}

impl std::hash::Hash for LimitValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the discriminant (Literal vs Param)
        std::mem::discriminant(self).hash(state);

        // For Literal, don't hash the value
        // For Param, hash the reference so different params produce different fingerprints
        match self {
            LimitValue::Literal(_) => {
                // Intentionally don't hash the value
            }
            LimitValue::Param(p) => p.hash(state),
        }
    }
}

// Mutation statements (data modification)

/// Mutation statements that modify data.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum MutationStatement {
    /// PUT: Insert new records
    Put(PutMutation),
    /// CHANGE: Update existing records
    Change(ChangeMutation),
    /// REMOVE: Delete records
    Remove(RemoveMutation),
}

/// PUT mutation to insert new records.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PutMutation {
    pub target: Spanned<Ident>,
    pub assignments: Vec<Assignment>,
}

/// CHANGE mutation to update existing records.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ChangeMutation {
    pub target: Spanned<Ident>,
    pub filter: Option<Spanned<Expr>>,
    pub assignments: Vec<Assignment>,
}

/// REMOVE mutation to delete records.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RemoveMutation {
    pub target: Spanned<Ident>,
    /// Filter is required for safety (prevents accidental DELETE all)
    pub filter: Spanned<Expr>,
}

/// Field assignment in PUT/CHANGE statements.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Assignment {
    pub field: Spanned<Ident>,
    pub value: Spanned<Expr>,
}

// Data definition language (DDL) statements

/// DDL statements for creating and dropping tables, indexes, and namespaces.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum DdlStatement {
    CreateTable(CreateTableDdl),
    DropTable(DropTableDdl),
    AlterTable(AlterTableDdl),
    CreateIndex(CreateIndexDdl),
    DropIndex(DropIndexDdl),
    CreateNamespace(CreateNamespaceDdl),
    DropNamespace(DropNamespaceDdl),
}

/// CREATE TABLE statement with columns and constraints.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CreateTableDdl {
    pub name: Spanned<Ident>,
    pub table_type: TableType,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub properties: Vec<TableProperty>,
    pub if_not_exists: bool,
}

/// DROP TABLE statement.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct DropTableDdl {
    pub name: Spanned<Ident>,
    pub if_exists: bool,
}

/// ALTER TABLE statement for schema modifications.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct AlterTableDdl {
    pub table: Spanned<Ident>,
    pub operations: Vec<AlterTableOperation>,
}

/// Individual schema modification operations.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum AlterTableOperation {
    /// Add new columns to the table.
    AddColumns(Vec<ColumnDef>),
    /// Drop columns from the table.
    DropColumns(Vec<Spanned<Ident>>),
    /// Rename columns (old_name, new_name).
    RenameColumns(Vec<(Spanned<Ident>, Spanned<Ident>)>),
    /// Rename the table itself.
    RenameTable(Spanned<Ident>),
    /// Alter column properties.
    AlterColumns(Vec<ColumnAlteration>),
}

/// Individual column property alteration.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ColumnAlteration {
    /// Column name to alter.
    pub column: Spanned<Ident>,
    /// The alteration to apply.
    pub action: ColumnAlterAction,
}

/// Actions that can be performed on a column.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ColumnAlterAction {
    /// Remove the default value.
    RemoveDefault,
    /// Set a new default value.
    SetDefault(Spanned<Expr>),
    /// Make the column nullable.
    SetNullable,
    /// Make the column required (not null).
    SetRequired,
    /// Change the column's data type.
    SetType(DataType),
}

/// CREATE INDEX statement to create an index on one or more columns.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CreateIndexDdl {
    pub name: Spanned<Ident>,
    pub table: Spanned<Ident>,
    pub columns: Vec<Spanned<Ident>>,
    pub unique: bool,
    pub if_not_exists: bool,
}

/// DROP INDEX statement.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct DropIndexDdl {
    pub name: Spanned<Ident>,
    pub table: Spanned<Ident>,
    pub if_exists: bool,
}

/// CREATE NAMESPACE statement.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CreateNamespaceDdl {
    pub name: Spanned<Ident>,
    pub description: Option<Arc<str>>,
    pub if_not_exists: bool,
}

/// DROP NAMESPACE statement.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct DropNamespaceDdl {
    pub name: Spanned<Ident>,
    pub force: bool,
    pub if_exists: bool,
}

/// Type of table/collection.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum TableType {
    /// Relational table with fixed schema and MVCC
    Relational,
    /// Document collection with flexible schema
    Document,
    /// Key-value keyspace
    Keyspace,
}

/// Column definition in CREATE TABLE.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: Spanned<Ident>,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

/// Data types supported by the query language.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum DataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bool,
    Bytes,
    DateTime,
    Date,
    Time,
    Uuid,
    ObjectId,
    Json,
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    Reference(Ident),
}

/// Column-level constraints.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ColumnConstraint {
    PrimaryKey,
    NotNull,
    Unique,
    Default(Spanned<Expr>),
    References { table: Ident, column: Ident },
    Check(Spanned<Expr>),
}

/// Table-level constraints.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TableConstraint {
    PrimaryKey(Vec<Ident>),
    Unique(Vec<Ident>),
    ForeignKey {
        columns: Vec<Ident>,
        ref_table: Ident,
        ref_columns: Vec<Ident>,
    },
    Check(Spanned<Expr>),
}

/// Table properties/options.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TableProperty {
    Ttl(u32),
    Description(Arc<str>),
    Persistence(PersistenceMode),
    Prefix(Arc<str>),
}

/// Persistence mode for tables.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default)]
pub enum PersistenceMode {
    #[default]
    Persistent,
    Memory,
}

// Transaction control

/// Transaction control statements.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
}

// Control flow statements

/// Control flow statements.
#[derive(Debug, Clone)]
pub enum ControlStatement {
    /// Pipeline: Execute statements in sequence
    Pipeline(Vec<Statement>),
    /// Conditional execution (if_empty, if_failed)
    Conditional {
        condition: ConditionalType,
        primary: Box<Statement>,
        fallback: Option<Box<Statement>>,
    },
    /// Variable assignment
    Assignment {
        variable: Spanned<Ident>,
        statement: Box<Statement>,
    },
    /// Switch to a different namespace
    Use(UseStatement),
}

/// USE statement to switch to a different namespace.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct UseStatement {
    pub namespace: Spanned<Ident>,
}

/// Type of conditional execution.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum ConditionalType {
    /// Execute fallback if primary returns empty result
    IfEmpty,
    /// Execute fallback if primary fails with an error
    IfFailed,
}

// Expression types

/// Expression: literals, columns, parameters, operators, functions, and CASE.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Literal value
    Literal(LiteralSlot),
    /// Column/field reference
    Column(ColumnRef),
    /// Parameter reference ($1, :name)
    Param(ParamRef),
    /// Binary operation (a + b, x = y, etc.)
    Binary {
        left: Box<Spanned<Expr>>,
        op: BinaryOp,
        right: Box<Spanned<Expr>>,
    },
    /// Unary operation (NOT x, -y, etc.)
    Unary {
        op: UnaryOp,
        operand: Box<Spanned<Expr>>,
    },
    /// Function call
    FunctionCall {
        name: Spanned<Ident>,
        args: Vec<Spanned<Expr>>,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Spanned<Expr>>>,
        when_clauses: Vec<WhenClause>,
        else_result: Option<Box<Spanned<Expr>>>,
    },
    /// Array literal [a, b, c]
    Array(Vec<Spanned<Expr>>),
    /// Object literal {a: 1, b: 2}
    Object(Vec<(Spanned<Ident>, Spanned<Expr>)>),
}

// Manual Hash implementation for Expr to handle LiteralSlot fingerprinting
impl Hash for Expr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Expr::Literal(slot) => slot.hash(state),
            Expr::Column(col) => col.hash(state),
            Expr::Param(param) => param.hash(state),
            Expr::Binary { left, op, right } => {
                left.hash(state);
                op.hash(state);
                right.hash(state);
            }
            Expr::Unary { op, operand } => {
                op.hash(state);
                operand.hash(state);
            }
            Expr::FunctionCall { name, args } => {
                name.hash(state);
                args.hash(state);
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.hash(state);
                when_clauses.hash(state);
                else_result.hash(state);
            }
            Expr::Array(items) => items.hash(state),
            Expr::Object(fields) => fields.hash(state),
        }
    }
}

impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Expr::Literal(a), Expr::Literal(b)) => a == b,
            (Expr::Column(a), Expr::Column(b)) => a == b,
            (Expr::Param(a), Expr::Param(b)) => a == b,
            (
                Expr::Binary {
                    left: l1,
                    op: o1,
                    right: r1,
                },
                Expr::Binary {
                    left: l2,
                    op: o2,
                    right: r2,
                },
            ) => l1 == l2 && o1 == o2 && r1 == r2,
            (
                Expr::Unary {
                    op: o1,
                    operand: a1,
                },
                Expr::Unary {
                    op: o2,
                    operand: a2,
                },
            ) => o1 == o2 && a1 == a2,
            (
                Expr::FunctionCall { name: n1, args: a1 },
                Expr::FunctionCall { name: n2, args: a2 },
            ) => n1 == n2 && a1 == a2,
            (
                Expr::Case {
                    operand: o1,
                    when_clauses: w1,
                    else_result: e1,
                },
                Expr::Case {
                    operand: o2,
                    when_clauses: w2,
                    else_result: e2,
                },
            ) => o1 == o2 && w1 == w2 && e1 == e2,
            (Expr::Array(a), Expr::Array(b)) => a == b,
            (Expr::Object(a), Expr::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Expr {}

/// Literal value slot for query fingerprinting.
#[derive(Debug, Clone)]
pub struct LiteralSlot {
    /// The actual literal value
    pub value: LiteralValue,
    /// Unique slot ID for this literal position in the query
    pub slot_id: u32,
}

impl Hash for LiteralSlot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash only the type discriminant, not the value
        std::mem::discriminant(&self.value).hash(state);
        self.slot_id.hash(state);
    }
}

impl PartialEq for LiteralSlot {
    fn eq(&self, other: &Self) -> bool {
        // Compare by type and slot, not value
        std::mem::discriminant(&self.value) == std::mem::discriminant(&other.value)
            && self.slot_id == other.slot_id
    }
}

impl Eq for LiteralSlot {}

/// Concrete literal values.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(Arc<str>),
    Bytes(Arc<[u8]>),
}

impl LiteralValue {
    /// Get a type name for error messages
    pub fn type_name(&self) -> &'static str {
        match self {
            LiteralValue::Null => "null",
            LiteralValue::Bool(_) => "boolean",
            LiteralValue::Int64(_) => "integer",
            LiteralValue::Float64(_) => "float",
            LiteralValue::String(_) => "string",
            LiteralValue::Bytes(_) => "bytes",
        }
    }
}

/// Column reference with optional table qualifier and nested path.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ColumnRef {
    /// Optional table/alias qualifier (e.g., "users" in "users.name")
    pub table: Option<Ident>,
    /// Column name
    pub column: Ident,
    /// Nested field path for document access (e.g., ["address", "city"])
    pub path: Vec<Ident>,
}

impl ColumnRef {
    /// Create a simple column reference without qualifiers
    pub fn simple(name: impl Into<Ident>) -> Self {
        Self {
            table: None,
            column: name.into(),
            path: Vec::new(),
        }
    }

    /// Create a column reference with nested path
    pub fn with_path(name: impl Into<Ident>, path: Vec<Ident>) -> Self {
        Self {
            table: None,
            column: name.into(),
            path,
        }
    }

    /// Create a qualified column reference (table.column)
    pub fn qualified(table: impl Into<Ident>, column: impl Into<Ident>) -> Self {
        Self {
            table: Some(table.into()),
            column: column.into(),
            path: Vec::new(),
        }
    }

    /// Get the full path as a string (for error messages)
    pub fn full_path(&self) -> String {
        let mut result = String::new();
        if let Some(ref t) = self.table {
            result.push_str(t.as_str());
            result.push('.');
        }
        result.push_str(self.column.as_str());
        for p in &self.path {
            result.push('.');
            result.push_str(p.as_str());
        }
        result
    }
}

/// Parameter reference for prepared statements.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ParamRef {
    /// Positional parameter: $1, $2, etc. (1-indexed in syntax, 0-indexed internally)
    Positional(u32),
    /// Named parameter: :name or $name
    Named(Ident),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BinaryOp {
    // Comparison operators
    Eq,    // =
    NotEq, // !=, <>
    Lt,    // <
    Gt,    // >
    LtEq,  // <=
    GtEq,  // >=

    // Logical operators
    And,
    Or,

    // Arithmetic operators
    Add, // +
    Sub, // -
    Mul, // *
    Div, // /
    Mod, // %

    // Special operators
    Contains,   // has, contains
    Like,       // like (SQL-style: %pattern%)
    StartsWith, // starts
    EndsWith,   // ends
    Matches,    // matches (regex)
    In,         // in

    // Compound assignment (for CHANGE statements)
    PlusEq,  // +=
    MinusEq, // -=
}

impl BinaryOp {
    /// Get the precedence of this operator (higher = binds tighter)
    pub fn precedence(&self) -> u8 {
        match self {
            BinaryOp::Or => 1,
            BinaryOp::And => 2,
            BinaryOp::Eq | BinaryOp::NotEq => 3,
            BinaryOp::Lt | BinaryOp::Gt | BinaryOp::LtEq | BinaryOp::GtEq => 4,
            BinaryOp::Contains
            | BinaryOp::Like
            | BinaryOp::StartsWith
            | BinaryOp::EndsWith
            | BinaryOp::Matches
            | BinaryOp::In => 4,
            BinaryOp::Add | BinaryOp::Sub => 5,
            BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => 6,
            BinaryOp::PlusEq | BinaryOp::MinusEq => 1, // Lowest for assignment
        }
    }

    /// Check if this operator is a comparison
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::Gt
                | BinaryOp::LtEq
                | BinaryOp::GtEq
                | BinaryOp::Contains
                | BinaryOp::Like
                | BinaryOp::StartsWith
                | BinaryOp::EndsWith
                | BinaryOp::Matches
                | BinaryOp::In
        )
    }

    /// Check if this operator is logical
    pub fn is_logical(&self) -> bool {
        matches!(self, BinaryOp::And | BinaryOp::Or)
    }

    /// Check if this operator is arithmetic
    pub fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
        )
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum UnaryOp {
    Not,       // not, !
    Neg,       // - (negation)
    IsNull,    // is null
    IsNotNull, // is not null
}

/// WHEN clause in CASE expression.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct WhenClause {
    pub condition: Spanned<Expr>,
    pub result: Spanned<Expr>,
}

// Helper implementations

impl From<bool> for LiteralValue {
    fn from(v: bool) -> Self {
        LiteralValue::Bool(v)
    }
}

impl From<i64> for LiteralValue {
    fn from(v: i64) -> Self {
        LiteralValue::Int64(v)
    }
}

impl From<f64> for LiteralValue {
    fn from(v: f64) -> Self {
        LiteralValue::Float64(v)
    }
}

impl From<String> for LiteralValue {
    fn from(v: String) -> Self {
        LiteralValue::String(Arc::from(v))
    }
}

impl From<&str> for LiteralValue {
    fn from(v: &str) -> Self {
        LiteralValue::String(Arc::from(v))
    }
}

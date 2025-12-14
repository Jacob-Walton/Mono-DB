use monodb_common::value::{Value, ValueType};

pub type Identifier = String;

/// Table storage types
#[derive(Debug, Clone)]
pub enum TableType {
    Relational,
    Document,
    Keyspace,
}

/// Field definition in table schema
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    pub field_type: ValueType,
    pub is_primary: bool,
    pub is_unique: bool,
    pub default: Option<Value>,
}

/// Sort direction for `order by` clauses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// Single ordering specification (field + direction)
#[derive(Debug, Clone)]
pub struct OrderByField {
    pub field: String,
    pub direction: SortDirection,
}

/// Binary operations supported in expressions
#[derive(Debug, Clone)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Contains,
    PlusEq,
    MinusEq,
}

/// Top-level AST node
#[derive(Debug, Clone)]
pub enum AstNode {
    Statement(Statement),
    FunctionDef(FunctionDef),
}

/// Query language statements
#[derive(Debug, Clone)]
pub enum Statement {
    Put {
        target: Identifier,
        values: Vec<Assignment>,
        extensions: Vec<Extension>,
    },
    Change {
        target: Identifier,
        filter: Option<Expr>,
        changes: Vec<Assignment>,
        extensions: Vec<Extension>,
    },
    Remove {
        target: Identifier,
        filter: Option<Expr>,
        extensions: Vec<Extension>,
    },
    Get {
        source: Identifier,
        filter: Option<Expr>,
        fields: Option<Vec<String>>,
        order_by: Option<Vec<OrderByField>>,
        take: Option<u64>,
        skip: Option<u64>,
        extensions: Vec<Extension>,
    },
    Make {
        table_type: TableType,
        name: Identifier,
        schema: Option<Vec<FieldDef>>,
        properties: Vec<TableProperty>,
    },
    // Pipeline and control flow
    Pipeline {
        statements: Vec<Statement>,
    },
    Conditional {
        condition: ConditionalType,
        primary: Box<Statement>,
        fallback: Option<Box<Statement>>,
    },
    Assignment {
        variable: String,
        statement: Box<Statement>,
    },
    // Transaction control statements (MVCC)
    Begin,
    Commit,
    Rollback,
    // Index management
    MakeIndex {
        index_name: String,
        table_name: Identifier,
        columns: Vec<String>,
        unique: bool,
    },
    DropIndex {
        index_name: String,
        table_name: Identifier,
    },
    // Table management
    DropTable {
        table_name: Identifier,
    },
    // Schema inspection
    Describe {
        table_name: Identifier,
    },
    // Row count
    Count {
        table_name: Identifier,
    },
}

/// Conditional operation types
#[derive(Debug, Clone)]
pub enum ConditionalType {
    IfEmpty,  // ??
    IfFailed, // ||
}

/// Query expressions
#[derive(Debug, Clone)]
pub enum QueryExpr {
    Select {
        source: Identifier,
        filter: Option<Expr>,
        prefix: Option<String>,
    },
    Identifier(Identifier),
}

/// Expression types
#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Value),
    Identifier(String),
    Variable(String),
    FieldAccess {
        base: Box<Expr>,
        field: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
}

/// Key-value assignment
#[derive(Debug, Clone)]
pub struct Assignment {
    pub key: String,
    pub value: Expr,
}

/// Function definition
#[derive(Debug, Clone)]
pub struct FunctionDef {
    pub name: String,
    pub params: Vec<String>,
    pub body: Expr,
}

/// Table configuration properties
#[derive(Debug, Clone)]
pub enum TableProperty {
    Ttl(u32),
    Description(String),
    Persistence(String),
    Prefix(String),
    Defaults(Vec<Assignment>),
}

/// Field constraints
#[derive(Debug, Clone)]
pub enum FieldConstraint {
    PrimaryKey,
    Unique,
    References(String, String),
    Default(Value),
}

/// Extension points for plugins
#[derive(Debug, Clone)]
pub struct Extension {
    pub name: String,
    pub args: Vec<Expr>,
}

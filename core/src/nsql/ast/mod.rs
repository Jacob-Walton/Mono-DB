use crate::nsql::interner::InternerId;
use smallvec::SmallVec;

#[derive(Debug, Clone)]
pub struct Program {
	pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Identifier(pub InternerId);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
	pub parts: SmallVec<[InternerId; 3]>,
}

impl QualifiedName {
	pub fn simple(id: InternerId) -> Self {
		Self {
			parts: smallvec::smallvec![id],
		}
	}
}

#[derive(Debug, Clone)]
pub enum Statement {
	Query(Query),
	Insert(InsertStmt),
	Update(UpdateStmt),
	Delete(DeleteStmt),
	CreateTable(CreateTableStmt),
	DropTable(DropTableStmt),
	DescribeTable(DescribeTableStmt),
}

#[derive(Debug, Clone)]
pub struct Query {
	pub projection: Vec<SelectItem>,
	pub from: Option<QualifiedName>,
	pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct SelectItem {
	pub expr: Expression,
	pub alias: Option<Identifier>,
}

#[derive(Debug, Clone)]
pub enum Expression {
	Literal(Literal),
	Column(QualifiedName),
	Star,
	BinaryOp {
		op: BinaryOp,
		left: Box<Expression>,
		right: Box<Expression>,
	},
}

#[derive(Debug, Clone)]
pub enum Literal {
	Integer(i64),
	String(InternerId),
	Boolean(bool),
	Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
	Eq,
	NotEq,
	Lt,
	Gt,
	LtEq,
	GtEq,
	And,
	Or,
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
	pub table: QualifiedName,
	pub columns: Option<Vec<Identifier>>,
	pub values: Vec<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
	pub table: QualifiedName,
	pub assignments: Vec<Assignment>,
	pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
	pub column: Identifier,
	pub value: Expression,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
	pub table: QualifiedName,
	pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
	pub name: QualifiedName,
	pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
	pub name: Identifier,
	pub data_type: DataType,
	pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone)]
pub enum DataType {
	Integer,
	Text,
	Boolean,
	Varchar(Option<u32>), // Support VARCHAR and VARCHAR(n)
	Char(Option<u32>),    // Support CHAR and CHAR(n)
	Decimal(Option<(u8, u8)>),
}

impl DataType {
	pub fn to_string(&self) -> String {
		match self {
			DataType::Integer => "INTEGER".to_string(),
			DataType::Text => "TEXT".to_string(),
			DataType::Boolean => "BOOLEAN".to_string(),
			DataType::Varchar(size) => match size {
				Some(n) => format!("VARCHAR({})", n),
				None => "VARCHAR".to_string(),
			},
			DataType::Char(size) => match size {
				Some(n) => format!("CHAR({})", n),
				None => "CHAR".to_string(),
			},
			DataType::Decimal(size) => match size {
				Some((p, s)) => format!("DECIMAL({}, {})", p, s),
				None => "DECIMAL".to_string(),
			},
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnConstraint {
	NotNull,
	PrimaryKey,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
	pub name: QualifiedName,
}

#[derive(Debug, Clone)]
pub struct DescribeTableStmt {
	pub name: QualifiedName,
}

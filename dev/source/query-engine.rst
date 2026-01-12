Query Engine Implementation
===========================

This document describes the implementation of the query engine for MonoDB
Prototype 3, focusing on the improvements made over Prototype 2.

Overview
--------

The query engine is responsible for parsing, planning, and executing queries.
The architecture follows a traditional database query processing pipeline:

.. code-block:: text

    Query Text → Lexer → Parser → AST → Planner → Logical Plan → Physical Plan → Executor → Results

While the lexer and parser are largely similar to P2 (using the same syntax),
significant improvements were made to the AST representation, caching system,
and planning infrastructure.

Lexer and Keyword System
------------------------

PHF-Based Keyword Recognition
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The lexer uses a perfect hash function (PHF) for O(1) keyword recognition.
Keywords are defined at compile time in ``build.rs`` and generated into a
static lookup table:

.. code-block:: rust

    // In build.rs
    let mut map = Map::<&'static [u8]>::new();
    
    // Core verbs
    map.entry(b"get", "TokenKind::Keyword");
    map.entry(b"put", "TokenKind::Keyword");
    map.entry(b"change", "TokenKind::Keyword");
    // ... etc

This approach provides:

- Zero runtime allocation for keyword matching
- Case-sensitive comparison via byte slices
- Easy addition of new keywords without parser changes

Keyword Categories
^^^^^^^^^^^^^^^^^^

Keywords are grouped by function:

**Query Verbs**: ``get``, ``count``, ``describe``

**Mutation Verbs**: ``put``, ``change``, ``remove``

**DDL Verbs**: ``make``, ``drop``

**Transaction Control**: ``begin``, ``commit``, ``rollback``

**Namespace Control**: ``use``, ``namespace``, ``namespaces``

**Clause Keywords**: ``from``, ``into``, ``where``, ``with``, ``set``, ``as``,
``order``, ``by``, ``asc``, ``desc``, ``take``, ``skip``

**Table/Schema Keywords**: ``table``, ``fields``, ``relational``, ``document``,
``keyspace``, ``index``, ``on``, ``primary``, ``key``, ``unique``, ``required``,
``default``, ``ttl``

**Type Keywords**: ``int``, ``bigint``, ``text``, ``decimal``, ``double``,
``date``, ``boolean``, ``map``, ``list``

**Operators**: ``has``, ``and``, ``or``

**Literals**: ``true``, ``false``, ``null``, ``now``

AST Improvements
----------------

Source Location Tracking with ``Spanned<T>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All AST nodes can carry source location information through the ``Spanned<T>``
wrapper type. This enables:

- Precise error messages with line/column information
- IDE features like go-to-definition
- Query highlighting and formatting

.. code-block:: rust

    #[derive(Debug, Clone)]
    pub struct Spanned<T> {
        pub node: T,
        pub span: Span,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct Span {
        pub start: u32,
        pub end: u32,
    }

The span is explicitly excluded from ``Hash`` and ``PartialEq`` implementations
so that structurally identical AST nodes compare equal regardless of their
source location.

Query Fingerprinting with ``LiteralSlot``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A key optimisation is **query fingerprinting** - two queries with the same
structure but different literal values should share a cached execution plan.

.. code-block:: text

    get users where age > 25   ─┐
                                ├──► same fingerprint ──► shared plan
    get users where age > 30   ─┘

This is achieved through the ``LiteralSlot`` type:

.. code-block:: rust

    #[derive(Debug, Clone)]
    pub struct LiteralSlot {
        /// The actual literal value
        pub value: LiteralValue,
        /// Unique slot ID assigned during parsing
        pub slot_id: u32,
    }

    impl Hash for LiteralSlot {
        fn hash<H: Hasher>(&self, state: &mut H) {
            // Hash the type discriminant, NOT the value
            std::mem::discriminant(&self.value).hash(state);
            self.slot_id.hash(state);
        }
    }

When the same query structure appears with different values, they produce the
same fingerprint hash. The actual values are stored separately and can be
substituted at execution time.

Structured Statement Types
^^^^^^^^^^^^^^^^^^^^^^^^^^

P2 used a flat ``Statement`` enum with all statement types at the top level.
P3 introduces a hierarchical structure that groups statements by their effect:

.. code-block:: rust

    pub enum Statement {
        Query(QueryStatement),       // Read-only operations
        Mutation(MutationStatement), // Data modification
        Ddl(DdlStatement),           // Schema modification
        Transaction(TransactionStatement),
        Control(ControlStatement),
    }

    pub enum QueryStatement {
        Get(GetQuery),
        Count(CountQuery),
        Describe(DescribeQuery),
    }

    pub enum MutationStatement {
        Put(PutMutation),
        Change(ChangeMutation),
        Remove(RemoveMutation),
    }

    pub enum DdlStatement {
        CreateTable(CreateTableDdl),
        DropTable(DropTableDdl),
        CreateIndex(CreateIndexDdl),
        DropIndex(DropIndexDdl),
    }

This structure allows:

- Type-safe pattern matching by statement category
- Easier permission checking (e.g., DDL requires special privileges)
- Cleaner code organisation in the executor

Control Statements and Namespacing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

P3 introduces the ``ControlStatement`` category for session-level operations
that don't directly query or modify data:

.. code-block:: rust

    pub enum ControlStatement {
        Pipeline(Vec<Statement>),
        Conditional { ... },
        Assignment { ... },
        Use(UseStatement),  // Namespace switching
    }

    pub struct UseStatement {
        pub namespace: Spanned<Ident>,
    }

The ``use`` statement allows users to switch between namespaces (logical
database partitions) without reconnecting:

.. code-block:: text

    use production
    get from users where active = true

    use staging
    get from users where active = true  // Different namespace

Namespace Keywords
""""""""""""""""""

Three new keywords were added to support namespace operations:

- ``use`` - Switch to a different namespace
- ``namespace`` - Used in administrative contexts
- ``namespaces`` - Used when listing available namespaces

These are recognised by the lexer's PHF (perfect hash function) map and can
also be used as identifiers in contexts where they don't conflict (e.g.,
``use default`` is valid because ``default`` is contextually allowed as an
identifier after ``use``).

Contextual Identifier Handling
""""""""""""""""""""""""""""""

Certain keywords can act as identifiers depending on context. The parser's
``expect_identifier`` method allows these specific keywords:

.. code-block:: rust

    // Keywords allowed as identifiers
    "key" | "value" | "data" | "ttl" | "default" | "namespace"

This enables natural syntax like ``use default`` or ``change key ...`` without
requiring escaping or quoting.

Interned Identifiers
^^^^^^^^^^^^^^^^^^^^

Identifiers (table names, column names) use ``Arc<str>`` for efficient sharing:

.. code-block:: rust

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct Ident(pub Arc<str>);

This reduces memory allocations when the same identifier appears multiple times
and makes equality comparisons faster through pointer comparison.

Parameter References
^^^^^^^^^^^^^^^^^^^^

P3 has first-class support for query parameters through the ``ParamRef`` type:

.. code-block:: rust

    pub enum ParamRef {
        Positional(u32),      // $1, $2, etc. (0-indexed internally)
        Named(Ident),         // :user_id, :email, etc.
    }

Parameters are represented directly in the AST rather than being substituted
during parsing. This enables:

- Query fingerprinting (parameters don't affect the fingerprint)
- Prepared statement caching
- Better type checking at execution time

Caching Architecture
--------------------

The query engine implements a multi-level caching system:

Parse Cache
^^^^^^^^^^^

Maps query text hash to parsed AST. This avoids re-lexing and parsing for
identical query strings.

.. code-block:: rust

    pub struct ParseCache {
        cache: RwLock<HashMap<u64, CachedParse>>,
        max_entries: usize,
        ttl: Duration,
    }

    struct CachedParse {
        statements: Vec<Statement>,
        last_access: Instant,
        access_count: u32,
    }

The cache uses LRU eviction when full and considers both recency and access
count when selecting entries to evict.

Plan Cache
^^^^^^^^^^

Maps query fingerprints to optimised physical plans. Because fingerprints
ignore literal values, queries with the same structure share cached plans:

.. code-block:: rust

    pub struct PlanCache {
        cache: RwLock<HashMap<QueryFingerprint, CachedPlan>>,
        max_entries: usize,
        ttl: Duration,
    }

The plan cache must be invalidated when:

- A table is modified (DDL operations)
- Indexes are created or dropped
- Statistics change significantly

Prepared Statement Cache
^^^^^^^^^^^^^^^^^^^^^^^^

Stores named prepared statements per session:

.. code-block:: rust

    pub struct PreparedStatementCache {
        statements: RwLock<HashMap<String, PreparedStatement>>,
    }

    pub struct PreparedStatement {
        pub name: String,
        pub fingerprint: QueryFingerprint,
        pub statements: Vec<Statement>,
        pub param_count: usize,
        pub created_at: Instant,
    }

Query Planning
--------------

The query planner converts AST nodes to logical plans, optimises them, and
then generates physical execution plans.

Logical Plan Nodes
^^^^^^^^^^^^^^^^^^

.. code-block:: rust

    pub enum LogicalPlan {
        Scan(ScanNode),
        Filter(FilterNode),
        Project(ProjectNode),
        Sort(SortNode),
        Limit(LimitNode),
        Offset(OffsetNode),
        Aggregate(AggregateNode),
        Join(JoinNode),
        Insert(InsertNode),
        Update(UpdateNode),
        Delete(DeleteNode),
        Values(ValuesNode),
        Empty,
    }

Each logical node captures the semantic intent without specifying execution
strategy.

Physical Plan Nodes
^^^^^^^^^^^^^^^^^^^

Physical plans specify the actual execution strategy:

.. code-block:: rust

    pub enum PhysicalPlan {
        // Query operators
        SeqScan(SeqScanOp),           // Full table scan
        PkLookup(PkLookupOp),         // Primary key lookup
        IndexScan(IndexScanOp),       // Index-based scan
        Filter(FilterOp),
        Project(ProjectOp),
        Sort(SortOp),
        Limit(LimitOp),
        Offset(OffsetOp),
        
        // Aggregation
        HashAggregate(HashAggregateOp),
        StreamAggregate(StreamAggregateOp),
        
        // Joins
        NestedLoopJoin(NestedLoopJoinOp),
        HashJoin(HashJoinOp),
        MergeJoin(MergeJoinOp),
        
        // Data sources
        Values(ValuesOp),
        Empty,
        
        // Mutations
        Insert(InsertOp),
        Update(UpdateOp),
        Delete(DeleteOp),
    }

The ``logical_to_physical`` function in the planner handles all logical plan
types, converting them to appropriate physical operators:

- **Query plans**: Choose between SeqScan, IndexScan, or PkLookup based on
  predicates and available indexes
- **Mutation plans**: Convert Insert/Update/Delete logical nodes directly to
  their physical counterparts with cost estimates
- **Join plans**: Default to NestedLoopJoin; can use HashJoin for equi-joins
  or MergeJoin for pre-sorted inputs
- **Aggregation**: Use HashAggregate for general cases; StreamAggregate when
  input is sorted by group keys

The planner chooses between strategies based on:

- Available indexes
- Estimated row counts
- Filter selectivity
- Sort requirements

Cost Estimation
^^^^^^^^^^^^^^^

The planner uses cost-based optimisation with a simple cost model:

.. code-block:: rust

    #[derive(Debug, Clone, Default)]
    pub struct PlanCost {
        pub io_cost: f64,      // Disk I/O operations
        pub cpu_cost: f64,     // CPU cycles estimate
        pub memory_cost: f64,  // Memory usage estimate
        pub network_cost: f64, // For distributed queries
    }

    impl PlanCost {
        pub fn total(&self) -> f64 {
            self.io_cost * 10.0 + self.cpu_cost + self.memory_cost * 0.1 + self.network_cost * 5.0
        }
    }

Storage Integration
-------------------

The query engine interfaces with storage through the ``QueryStorage`` trait:

.. code-block:: rust

    pub trait QueryStorage: Send + Sync {
        fn scan(&self, table: &str, tx_id: u64, config: ScanConfig) -> Result<RowBatch>;
        fn get(&self, table: &str, key: &Value, tx_id: u64) -> Result<Option<Row>>;
        fn put(&self, table: &str, row: Row, tx_id: u64) -> Result<()>;
        fn update(&self, table: &str, key: &Value, row: Row, tx_id: u64) -> Result<()>;
        fn delete(&self, table: &str, key: &Value, tx_id: u64) -> Result<()>;
        fn schema(&self, table: &str) -> Result<Option<TableSchema>>;
        fn list_tables(&self) -> Result<Vec<String>>;
    }

This abstraction allows the query engine to work with different storage
backends and supports transaction IDs for MVCC.

Network Integration
-------------------

The query engine is integrated with the network layer through the request
handler in ``network/mod.rs``. When a ``Request::Query`` arrives:

1. **Parse**: The query text is parsed into a list of ``Statement`` nodes
2. **Transaction**: An auto-transaction is started if none is active
3. **Plan**: Each statement is converted to a physical plan via
   ``QueryPlanner::plan()``
4. **Execute**: The plan is executed via ``Executor::execute()``
5. **Commit**: Auto-transactions are committed on success

.. code-block:: rust

    async fn handle_query(
        sessions: &Arc<DashMap<u64, Session>>,
        session_id: u64,
        storage: &Arc<StorageAdapter>,
        query: &str,
        params: Vec<Value>,
        start: Instant,
    ) -> Result<Response> {
        let statements = parse(query)?;
        
        let tx_id = storage.begin_transaction()?;
        let executor = Executor::new(storage.clone());
        let ctx = ExecutionContext::new()
            .with_params(params)
            .with_tx(tx_id);
        
        for stmt in statements {
            execute_statement(&executor, &ctx, storage, stmt).await?;
        }
        
        storage.commit(tx_id)?;
        Ok(Response::QueryResult { ... })
    }

Statement Dispatch
^^^^^^^^^^^^^^^^^^

The ``execute_statement`` function dispatches based on statement type:

- **Query statements** (GET, COUNT): Plan and execute, return rows
- **Mutation statements** (PUT, CHANGE, REMOVE): Plan and execute, return
  affected row count
- **DDL statements**: Execute directly against storage (CREATE TABLE, etc.)
- **Transaction statements**: Handled at the request level
- **Control statements**: Session-level operations (USE namespace, etc.)

Future Work
-----------

The current implementation provides a solid foundation. Planned improvements:

- **Index selection**: Use statistics to choose optimal indexes
- **Join ordering**: Optimise multi-way joins
- **Subquery support**: Correlated and uncorrelated subqueries
- **Window functions**: OVER clauses for analytics
- **Parallel execution**: Multi-threaded query execution

use std::{collections::HashMap, fmt, pin::Pin, sync::Arc, time::UNIX_EPOCH};

use chrono::{DateTime, FixedOffset, Utc};
use monodb_common::{
    MonoError, Value, ValueType,
    protocol::ExecutionResult,
    schema::{KeySpacePersistence, Schema, TableColumn},
};

use crate::{
    query_engine::ast::{
        Assignment, BinaryOp, ConditionalType, Expr, Extension, FieldDef, Statement, TableProperty,
        TableType,
    },
    storage::{Data, Filter, Query, engine::StorageEngine},
};

#[derive(Debug)]
pub enum ExecutorError {
    TableNotFound(String),
    TableAlreadyExists(String),
    TypeError { expected: String, actual: String },
    FieldNotFound(String),
    InvalidOperation(String),
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorError::TableNotFound(name) => write!(f, "Table '{name}' not found"),
            ExecutorError::TableAlreadyExists(name) => write!(f, "Table '{name}' already exists"),
            ExecutorError::TypeError { expected, actual } => {
                write!(f, "Type error: expected {expected}, got {actual}")
            }
            ExecutorError::FieldNotFound(field) => write!(f, "Field '{field}' not found"),
            ExecutorError::InvalidOperation(msg) => write!(f, "Invalid operation: {msg}"),
        }
    }
}

impl std::error::Error for ExecutorError {}

// Convert ExecutorError to MonoError
impl From<ExecutorError> for MonoError {
    fn from(error: ExecutorError) -> Self {
        match error {
            ExecutorError::TableNotFound(name) => MonoError::NotFound(name),
            ExecutorError::TableAlreadyExists(name) => MonoError::AlreadyExists(name),
            ExecutorError::TypeError { expected, actual } => {
                MonoError::TypeError { expected, actual }
            }
            ExecutorError::FieldNotFound(field) => MonoError::NotFound(field),
            ExecutorError::InvalidOperation(msg) => MonoError::Execution(msg),
        }
    }
}

type ExecutorResult<T> = Result<T, ExecutorError>;

pub struct QueryExecutor {
    storage: Arc<StorageEngine>,
    variables: HashMap<String, Value>,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            variables: HashMap::new(),
        }
    }

    /// Force flush all LSM tree memtables to disk (test-only)
    #[cfg(test)]
    pub async fn flush_storage(&self) -> monodb_common::Result<()> {
        self.storage.flush_all().await
    }

    /// Execute a single statement
    pub async fn execute(&mut self, statement: Statement) -> ExecutorResult<ExecutionResult> {
        let start = std::time::Instant::now();

        let result = match statement {
            Statement::Pipeline { statements } => self.execute_pipeline(statements).await,
            Statement::Conditional {
                condition,
                primary,
                fallback,
            } => {
                self.execute_conditional(condition, *primary, fallback.map(|f| *f))
                    .await
            }
            Statement::Assignment {
                variable,
                statement,
            } => self.execute_assignment(variable, *statement).await,
            _ => self.execute_simple_statement(statement).await,
        };

        let elapsed = start.elapsed().as_millis() as u64;
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;

        match result {
            Ok(exec_result) => {
                let updated = match exec_result {
                    ExecutionResult::Ok {
                        data, row_count, ..
                    } => ExecutionResult::Ok {
                        data,
                        time: timestamp,
                        commit_timestamp: None,
                        time_elapsed: Some(elapsed),
                        row_count,
                    },
                    ExecutionResult::Created { .. } => ExecutionResult::Created {
                        time: timestamp,
                        commit_timestamp: None,
                    },
                    ExecutionResult::Modified { rows_affected, .. } => ExecutionResult::Modified {
                        time: timestamp,
                        commit_timestamp: None,
                        rows_affected,
                    },
                };
                Ok(updated)
            }
            Err(e) => Err(e),
        }
    }

    /// Execute a simple (non-compound) statement
    async fn execute_simple_statement(
        &mut self,
        statement: Statement,
    ) -> ExecutorResult<ExecutionResult> {
        let start = std::time::Instant::now();

        let result = match statement {
            Statement::Make {
                table_type,
                name,
                schema,
                properties,
            } => {
                self.execute_make_table(name, table_type, schema, properties)
                    .await?;
                ExecutionResult::Created {
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                }
            }
            Statement::Put {
                target,
                values,
                extensions,
            } => {
                self.execute_put(target, values, extensions).await?;
                ExecutionResult::Modified {
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    rows_affected: Some(1),
                }
            }
            Statement::Change {
                target,
                filter,
                changes,
                extensions,
            } => {
                let rows = self
                    .execute_change(target, filter, changes, extensions)
                    .await?;
                ExecutionResult::Modified {
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    rows_affected: Some(rows.0),
                }
            }
            Statement::Remove {
                target,
                filter,
                extensions,
            } => {
                let rows = self.execute_remove(target, filter, extensions).await?;
                ExecutionResult::Modified {
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    rows_affected: Some(rows.0),
                }
            }
            Statement::Get {
                source,
                filter,
                fields,
                extensions,
            } => {
                let data = self.execute_get(source, filter, fields, extensions).await?;
                ExecutionResult::Ok {
                    data: vec![data], // FIXME: Temporary solution
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: Some(1),
                }
            }
            _ => {
                return Err(ExecutorError::InvalidOperation(
                    "Compound statements should not reach simple execution".into(),
                ));
            }
        };

        Ok(result)
    }

    /// Execute a pipeline of statements
    async fn execute_pipeline(
        &mut self,
        statements: Vec<Statement>,
    ) -> ExecutorResult<ExecutionResult> {
        let mut last_result = Value::Null;

        for statement in statements {
            let execution_result = Box::pin(self.execute(statement)).await?;
            if let ExecutionResult::Ok { data, .. } = execution_result {
                last_result = Value::Array(data);
            }
        }

        Ok(ExecutionResult::Ok {
            data: if last_result == Value::Null {
                vec![]
            } else {
                vec![last_result]
            }, // FIXME: Temporary solution
            time: std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            time_elapsed: None,
            commit_timestamp: None,
            row_count: None,
        })
    }

    /// Execute a conditional statement
    async fn execute_conditional(
        &mut self,
        condition: ConditionalType,
        primary: Statement,
        fallback: Option<Statement>,
    ) -> ExecutorResult<ExecutionResult> {
        match Box::pin(self.execute(primary)).await {
            Ok(result) => {
                // Check if we should use fallback
                let should_fallback = match condition {
                    ConditionalType::IfEmpty => {
                        if let ExecutionResult::Ok { ref data, .. } = result {
                            data.is_empty()
                                || data.iter().all(|v| match v {
                                    Value::Null => true,
                                    Value::String(s) => s.is_empty(),
                                    Value::Array(arr) => arr.is_empty(),
                                    Value::Object(obj) => obj.is_empty(),
                                    _ => false,
                                }) // FIXME: Temporary solution
                        } else {
                            false
                        }
                    }
                    ConditionalType::IfFailed => false,
                };

                if let Some(fallback) = fallback {
                    if should_fallback {
                        Box::pin(self.execute(fallback)).await
                    } else {
                        Ok(result)
                    }
                } else {
                    Ok(result)
                }
            }
            Err(_) => {
                // Primary failed
                match condition {
                    ConditionalType::IfFailed => {
                        if let Some(fallback_stmt) = fallback {
                            Box::pin(self.execute(fallback_stmt)).await
                        } else {
                            Err(ExecutorError::InvalidOperation(
                                "primary failed and no fallback provided".to_string(),
                            ))
                        }
                    }
                    ConditionalType::IfEmpty => {
                        if let Some(fallback_stmt) = fallback {
                            Box::pin(self.execute(fallback_stmt)).await
                        } else {
                            Err(ExecutorError::InvalidOperation(
                                "primary failed and no fallback provided".to_string(),
                            ))
                        }
                    }
                }
            }
        }
    }

    /// Execute a variable assignment
    async fn execute_assignment(
        &mut self,
        variable: String,
        statement: Statement,
    ) -> ExecutorResult<ExecutionResult> {
        let result = Box::pin(self.execute(statement)).await?;

        if let ExecutionResult::Ok { ref data, .. } = result {
            self.variables.insert(variable, Value::Array(data.clone())); // FIXME: Temporary solution
        }

        Ok(result)
    }

    /// Execute a table creation
    async fn execute_make_table(
        &mut self,
        name: String,
        table_type: TableType,
        schema: Option<Vec<FieldDef>>,
        properties: Vec<TableProperty>,
    ) -> ExecutorResult<Value> {
        let persistence = if matches!(table_type, TableType::Keyspace) {
            properties
                .iter()
                .find_map(|prop| match prop {
                    TableProperty::Persistence(p) => Some(p.as_str()),
                    _ => None,
                })
                .unwrap_or("persistent")
        } else {
            "persistent"
        };

        let primary_keys = schema
            .as_ref()
            .map(|fields| {
                fields
                    .iter()
                    .filter(|f| f.is_primary)
                    .map(|f| f.name.clone())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        if matches!(table_type, TableType::Relational) && primary_keys.is_empty() {
            return Err(ExecutorError::InvalidOperation(
                "relational tables must have a primary key".to_string(),
            ));
        }

        let columns: Vec<TableColumn> = if let Some(fields) = schema.as_ref() {
            fields
                .iter()
                .map(|f| TableColumn {
                    name: f.name.clone(),
                    data_type: f.field_type.clone(),
                    nullable: f.default.is_none(),
                    default: f.default.clone(),
                    is_primary: f.is_primary,
                    is_unique: f.is_unique,
                })
                .collect()
        } else {
            Vec::new()
        };

        // Capture prefix if provided
        let prefix_value: Option<String> = properties.iter().find_map(|prop| {
            if let TableProperty::Prefix(s) = prop {
                Some(s.clone())
            } else {
                None
            }
        });

        let schema = match table_type {
            TableType::Relational => Schema::Table {
                name: name.clone(),
                columns,
                primary_key: primary_keys,
                indexes: vec![], // TODO: Implement indexes
            },
            TableType::Document => Schema::Collection {
                name: name.clone(),
                validation: None, // TODO: Implement validation rules
                indexes: vec![],  // TODO: Implement indexes
            },
            TableType::Keyspace => Schema::KeySpace {
                name: name.clone(),
                ttl_enabled: properties
                    .iter()
                    .any(|p| matches!(p, TableProperty::Ttl(_))),
                max_size: None,
                persistence: match persistence {
                    "persistent" => KeySpacePersistence::Persistent,
                    "memory" => KeySpacePersistence::Memory,
                    _ => {
                        return Err(ExecutorError::InvalidOperation(format!(
                            "invalid persistence option: {persistence}"
                        )));
                    }
                },
            },
        };

        match self.storage.create_collection(schema).await {
            Ok(_) => {
                // TODO: Handle in storage engine eventually
                // Persist prefix information locally so inserts can apply it
                if let Some(prefix) = prefix_value {
                    let var_name = format!("__table_prefix__{name}");
                    self.variables
                        .insert(var_name, Value::String(prefix.clone()));
                }

                Ok(Value::String(format!(
                    "table '{name}' created successfully"
                )))
            }
            Err(e) => {
                // Check if error is because collection already exists
                if e.to_string().contains("already exists") {
                    // Table already exists, this is not an error for now (FIXME)
                    tracing::debug!("Table '{}' already exists, continuing", name);

                    // Still persist prefix information if provided
                    if let Some(prefix) = prefix_value {
                        let var_name = format!("__table_prefix__{name}");
                        self.variables
                            .insert(var_name, Value::String(prefix.clone()));
                    }

                    Ok(Value::String(format!("table '{name}' already exists")))
                } else {
                    Err(ExecutorError::InvalidOperation(e.to_string()))
                }
            }
        }
    }

    async fn execute_put(
        &mut self,
        target: String,
        values: Vec<Assignment>,
        _extensions: Vec<Extension>,
    ) -> ExecutorResult<Value> {
        // Build row
        let mut row = HashMap::new();
        for assignment in values.clone() {
            let value = self.evaluate_expression(assignment.value).await?;
            row.insert(assignment.key, value);
        }

        // Check schema type first
        let schema_opt = self.storage.schema(&target);

        match schema_opt.as_ref().map(|s| s.as_ref()) {
            Some(Schema::Collection { .. }) => {
                // Collections: Add _id (ObjectId) if missing
                if !row.contains_key("_id") {
                    let oid = monodb_common::ObjectId::new()
                        .map_err(|e| ExecutorError::InvalidOperation(e.to_string()))?;
                    row.insert("_id".to_string(), Value::ObjectId(oid));
                }
            }
            Some(Schema::KeySpace { .. }) => {
                // KeySpaces: User must provide 'key' field, we don't auto-generate
                // The storage layer will handle this
            }
            Some(Schema::Table { .. }) => {
                // Relational tables: handled below, don't add _id/_key
            }
            None => {
                // Unknown collection type - treat as collection for backwards compatibility
                if !row.contains_key("_id") {
                    let oid = monodb_common::ObjectId::new()
                        .map_err(|e| ExecutorError::InvalidOperation(e.to_string()))?;
                    row.insert("_id".to_string(), Value::ObjectId(oid));
                }
            }
        }

        let is_relational = matches!(
            schema_opt.as_ref().map(|s| s.as_ref()),
            Some(Schema::Table { .. })
        );

        // Relational path: Auto-generate primary key if needed
        if is_relational
            && let Some(schema) = &schema_opt
            && let Schema::Table {
                columns,
                primary_key,
                ..
            } = schema.as_ref()
        {
            // Auto-generate primary key value if it's missing and is numeric
            // Only handle single-column primary keys for auto-increment
            if primary_key.len() == 1 {
                let pk_name = &primary_key[0];
                if !row.contains_key(pk_name)
                    && let Some(col) = columns.iter().find(|c| &c.name == pk_name)
                    && matches!(col.data_type, ValueType::Int32 | ValueType::Int64)
                {
                    let next = self
                        .storage
                        .next_sequence_value(&target, pk_name)
                        .await
                        .map_err(|e| ExecutorError::InvalidOperation(e.to_string()))?;

                    row.insert(
                        pk_name.clone(),
                        if col.data_type == ValueType::Int64 {
                            Value::Int64(next as i64)
                        } else {
                            Value::Int32(next as i32)
                        },
                    );
                }
            }
        }

        // Validation
        if let Some(schema) = &schema_opt
            && let Schema::Table { columns, .. } = schema.as_ref()
        {
            for (key, expr) in values.iter().map(|v| (v.key.clone(), v.value.clone())) {
                if let Some(col) = columns.iter().find(|c| c.name == key) {
                    self.validate_column_value(col, &expr)?;
                } else {
                    return Err(ExecutorError::InvalidOperation(format!(
                        "Unknown column '{}'",
                        key
                    )));
                }
            }
        }

        // Insert
        let data = Data::Row(row);

        self.storage
            .insert(&target, &data)
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.to_string()))?;

        Ok(Value::String(format!("Inserted 1 row into '{target}'")))
    }

    fn validate_column_value(&self, column: &TableColumn, expr: &Expr) -> ExecutorResult<()> {
        // Resolve the expression only if it's literal or trivially typed
        let resolved_type = match expr {
            Expr::Literal(v) => v.data_type(),
            Expr::FunctionCall { name, args } => match name.as_str() {
                "now" if args.is_empty() => ValueType::DateTime,
                _ => return Ok(()), // skip unknown functions
            },
            _ => return Ok(()), // can't infer type safely
        };

        let expected = &column.data_type;

        // Accept null for any type
        if resolved_type == ValueType::Null {
            return Ok(());
        }

        // Direct match
        if &resolved_type == expected {
            return Ok(());
        }

        // Numeric compatibility
        let numeric = |t: &ValueType| {
            matches!(
                t,
                ValueType::Int32 | ValueType::Int64 | ValueType::Float32 | ValueType::Float64
            )
        };
        if numeric(expected) && numeric(&resolved_type) {
            return Ok(());
        }

        // String coercions for common typed fields
        if resolved_type == ValueType::String
            && matches!(
                expected,
                ValueType::DateTime
                    | ValueType::Date
                    | ValueType::Time
                    | ValueType::Uuid
                    | ValueType::ObjectId
                    | ValueType::Reference
            )
        {
            return Ok(());
        }

        Err(ExecutorError::InvalidOperation(format!(
            "Type mismatch for column '{}': expected {:?}, got {:?}",
            column.name, expected, resolved_type
        )))
    }

    async fn execute_change(
        &mut self,
        _target: String,
        _filter: Option<Expr>,
        _changes: Vec<Assignment>,
        _extensions: Vec<Extension>,
    ) -> ExecutorResult<(u64, Value)> {
        // TODO: Implement update
        Err(ExecutorError::InvalidOperation(
            "change operations not yet implemented".to_string(),
        ))
    }

    async fn execute_remove(
        &mut self,
        target: String,
        filter: Option<Expr>,
        _extensions: Vec<Extension>,
    ) -> ExecutorResult<(u64, Value)> {
        let filter = if let Some(expr) = filter {
            self.translate_filter(expr).await?
        } else {
            return Err(ExecutorError::InvalidOperation(
                "delete clause where clause is not supported".to_string(),
            ));
        };

        let count = self
            .storage
            .delete_many(&target, filter)
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.to_string()))?;

        Ok((
            count,
            Value::String(format!("Deleted {count} rows from '{target}'")),
        ))
    }

    async fn execute_get(
        &mut self,
        source: String,
        filter: Option<Expr>,
        _fields: Option<Vec<String>>,
        _extensions: Vec<Extension>,
    ) -> ExecutorResult<Value> {
        let filter = if let Some(expr) = filter {
            Some(self.translate_filter(expr).await?)
        } else {
            None
        };

        let query = Query {
            filter,
            ..Default::default()
        };

        let results = self
            .storage
            .find(&source, query)
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.to_string()))?;

        Ok(Value::Array(results))
    }

    async fn translate_filter(&self, expr: Expr) -> ExecutorResult<Filter> {
        self.translate_filter_inner(expr).await
    }

    fn translate_filter_inner(
        &self,
        expr: Expr,
    ) -> Pin<Box<dyn Future<Output = ExecutorResult<Filter>> + Send + '_>> {
        Box::pin(async move {
            match expr {
                Expr::BinaryOp { left, op, right } => match op {
                    BinaryOp::And => {
                        let left_filter = self.translate_filter(*left).await?;
                        let right_filter = self.translate_filter(*right).await?;
                        Ok(Filter::And(vec![left_filter, right_filter]))
                    }
                    BinaryOp::Or => {
                        let left_filter = self.translate_filter(*left).await?;
                        let right_filter = self.translate_filter(*right).await?;
                        Ok(Filter::Or(vec![left_filter, right_filter]))
                    }
                    _ => {
                        let left_expr = self.evaluate_expression(*left).await?;
                        let right_expr = self.evaluate_expression(*right).await?;

                        if let Value::String(field) = left_expr {
                            match op {
                                BinaryOp::Eq => Ok(Filter::Eq(field, right_expr)),
                                BinaryOp::NotEq => Ok(Filter::Neq(field, right_expr)),
                                BinaryOp::Gt => Ok(Filter::Gt(field, right_expr)),
                                BinaryOp::Lt => Ok(Filter::Lt(field, right_expr)),
                                BinaryOp::GtEq => Ok(Filter::Gte(field, right_expr)),
                                BinaryOp::LtEq => Ok(Filter::Lte(field, right_expr)),
                                BinaryOp::Contains => {
                                    // e.g. tags has "beta"
                                    Ok(Filter::Contains(field, right_expr))
                                }
                                _ => Err(ExecutorError::InvalidOperation(format!(
                                    "Unsupported binary operation: {op:?}"
                                ))),
                            }
                        } else {
                            Err(ExecutorError::InvalidOperation(
                                "Invalid filter expression".to_string(),
                            ))
                        }
                    }
                },
                _ => Err(ExecutorError::InvalidOperation(
                    "Unsupported filter expression".to_string(),
                )),
            }
        })
    }

    async fn evaluate_expression(&self, expr: Expr) -> ExecutorResult<Value> {
        self.evaluate_expression_inner(expr).await
    }

    fn evaluate_expression_inner(
        &self,
        expr: Expr,
    ) -> Pin<Box<dyn Future<Output = ExecutorResult<Value>> + Send + '_>> {
        Box::pin(async move {
            match expr {
                Expr::Literal(value) => Ok(value),
                Expr::Identifier(name) => {
                    if let Some(value) = self.variables.get(&name) {
                        Ok(value.clone())
                    } else {
                        // Handle identifiers
                        match name.as_str() {
                            "true" => Ok(Value::Bool(true)),
                            "false" => Ok(Value::Bool(false)),
                            "null" => Ok(Value::Null),
                            _ => Ok(Value::String(name)),
                        }
                    }
                }
                Expr::Variable(name) => {
                    // TODO: Handle variables
                    Ok(Value::String(format!("${name}")))
                }
                Expr::FunctionCall { name, .. } => match name.as_str() {
                    "now" => {
                        let now_utc: DateTime<Utc> = Utc::now();
                        let offset = FixedOffset::east_opt(0).unwrap();
                        let now_fixed: DateTime<FixedOffset> = now_utc.with_timezone(&offset);
                        Ok(Value::DateTime(now_fixed))
                    }
                    // TODO: Add stdlib functions
                    _ => Err(ExecutorError::InvalidOperation(format!(
                        "unknown function: {name}"
                    ))),
                },
                Expr::BinaryOp { left, op, right } => {
                    let left_val = self.evaluate_expression(*left).await?;
                    let right_val = self.evaluate_expression(*right).await?;

                    match op {
                        BinaryOp::Add => match (left_val, right_val) {
                            // Integer combinations
                            (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a + b)),
                            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a + b)),
                            (Value::Int64(a), Value::Int32(b)) => Ok(Value::Int64(a + b as i64)),
                            (Value::Int32(a), Value::Int64(b)) => Ok(Value::Int64(a as i64 + b)),

                            // Float combinations
                            (Value::Float32(a), Value::Float32(b)) => Ok(Value::Float32(a + b)),
                            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a + b)),
                            (Value::Float64(a), Value::Float32(b)) => {
                                Ok(Value::Float64(a + b as f64))
                            }
                            (Value::Float32(a), Value::Float64(b)) => {
                                Ok(Value::Float64(a as f64 + b))
                            }

                            // Mixed int and float
                            (Value::Int32(a), Value::Float32(b)) => {
                                Ok(Value::Float32(a as f32 + b))
                            }
                            (Value::Int32(a), Value::Float64(b)) => {
                                Ok(Value::Float64(a as f64 + b))
                            }
                            (Value::Int64(a), Value::Float32(b)) => {
                                Ok(Value::Float64(a as f64 + b as f64))
                            }
                            (Value::Int64(a), Value::Float64(b)) => {
                                Ok(Value::Float64(a as f64 + b))
                            }
                            (Value::Float32(a), Value::Int32(b)) => {
                                Ok(Value::Float32(a + b as f32))
                            }
                            (Value::Float64(a), Value::Int64(b)) => {
                                Ok(Value::Float64(a + b as f64))
                            }

                            // String concatenation
                            (Value::String(a), Value::String(b)) => {
                                Ok(Value::String(format!("{a}{b}")))
                            }

                            _ => Err(ExecutorError::TypeError {
                                expected: "numeric or string types".to_string(),
                                actual: "incompatible types for addition".to_string(),
                            }),
                        },

                        BinaryOp::Sub => match (left_val, right_val) {
                            // Integer combinations
                            (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a - b)),
                            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
                            (Value::Int64(a), Value::Int32(b)) => Ok(Value::Int64(a - b as i64)),
                            (Value::Int32(a), Value::Int64(b)) => Ok(Value::Int64(a as i64 - b)),

                            // Float combinations
                            (Value::Float32(a), Value::Float32(b)) => Ok(Value::Float32(a - b)),
                            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a - b)),
                            (Value::Float64(a), Value::Float32(b)) => {
                                Ok(Value::Float64(a - b as f64))
                            }
                            (Value::Float32(a), Value::Float64(b)) => {
                                Ok(Value::Float64(a as f64 - b))
                            }

                            // Mixed int and float
                            (Value::Int32(a), Value::Float32(b)) => {
                                Ok(Value::Float32(a as f32 - b))
                            }
                            (Value::Int32(a), Value::Float64(b)) => {
                                Ok(Value::Float64(a as f64 - b))
                            }
                            (Value::Int64(a), Value::Float32(b)) => {
                                Ok(Value::Float64(a as f64 - b as f64))
                            }
                            (Value::Int64(a), Value::Float64(b)) => {
                                Ok(Value::Float64(a as f64 - b))
                            }
                            (Value::Float32(a), Value::Int32(b)) => {
                                Ok(Value::Float32(a - b as f32))
                            }
                            (Value::Float64(a), Value::Int64(b)) => {
                                Ok(Value::Float64(a - b as f64))
                            }

                            _ => Err(ExecutorError::TypeError {
                                expected: "numeric types".to_string(),
                                actual: "non-numeric types for subtraction".to_string(),
                            }),
                        },

                        _ => Err(ExecutorError::InvalidOperation(format!(
                            "Unsupported binary operation: {op:?}"
                        ))),
                    }
                }
                Expr::FieldAccess { base, field } => {
                    // If this is a static field path like `address.city`, return it as a string
                    // so filters can use the field path (instead of trying to resolve a variable
                    // named `address` at runtime)
                    if let Some(prefix) = Self::collect_field_path(&base) {
                        return Ok(Value::String(format!("{prefix}.{field}")));
                    }

                    // Otherwise evaluate the base at runtime and perform field access
                    let base_val = self.evaluate_expression(*base).await?;
                    match base_val {
                        Value::Object(ref obj) => {
                            Ok(obj.get(&field).cloned().unwrap_or(Value::Null))
                        }
                        _ => Err(ExecutorError::TypeError {
                            expected: "object".to_string(),
                            actual: "non-object type for field access".to_string(),
                        }),
                    }
                }
            }
        })
    }

    /// If an Expr represents a dotted field path like `a.b.c` (identifiers/field accesses),
    /// return that as a single dotted string. Returns None if the expression is not a pure
    /// identifier/field-access chain.
    fn collect_field_path(expr: &crate::query_engine::ast::Expr) -> Option<String> {
        use crate::query_engine::ast::Expr::*;

        match expr {
            Identifier(name) => Some(name.clone()),
            FieldAccess { base, field } => {
                Self::collect_field_path(base).map(|p| format!("{p}.{field}"))
            }
            _ => None,
        }
    }
}

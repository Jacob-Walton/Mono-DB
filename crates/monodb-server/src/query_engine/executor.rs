use std::{collections::HashMap, fmt, pin::Pin, sync::Arc, time::UNIX_EPOCH};

use chrono::{DateTime, FixedOffset, Utc};
use monodb_common::{
    MonoError, Value, ValueType,
    protocol::ExecutionResult,
    schema::{KeySpacePersistence, Schema, TableColumn},
};

use crate::{
    query_engine::ast::{
        Assignment, BinaryOp, ConditionalType, Expr, Extension, FieldDef, OrderByField,
        SortDirection, Statement, TableProperty, TableType,
    },
    storage::{
        Data, Filter, Query,
        engine::{StorageEngine, Transaction},
    },
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
            ExecutorError::InvalidOperation(msg) => write!(f, "{msg}"),
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
            ExecutorError::InvalidOperation(msg) => MonoError::InvalidOperation(msg),
        }
    }
}

type ExecutorResult<T> = Result<T, ExecutorError>;

pub struct QueryExecutor {
    storage: Arc<StorageEngine>,
    variables: HashMap<String, Value>,
    /// Current active transaction for this executor (MVCC)
    current_tx: Option<Transaction>,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            variables: HashMap::new(),
            current_tx: None,
        }
    }

    /// Get access to the storage engine
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Get the current active transaction, if any
    pub fn current_transaction(&self) -> Option<&Transaction> {
        self.current_tx.as_ref()
    }

    /// Check if there's an active transaction
    pub fn in_transaction(&self) -> bool {
        self.current_tx.is_some()
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
                order_by,
                take,
                skip,
                extensions,
            } => {
                // Translate filter for query planner
                let translated_filter = if let Some(ref expr) = filter {
                    Some(self.translate_filter(expr.clone()).await?)
                } else {
                    None
                };

                // Check if we can use an index (very basic query planner)
                let index_plan = self.storage.find_usable_index(
                    &source,
                    translated_filter.as_ref(),
                    order_by.as_deref(),
                );

                let (mut rows, used_index_for_order) = if let Some(ref plan) = index_plan {
                    use crate::storage::IndexScanType;

                    match &plan.scan_type {
                        IndexScanType::PrimaryKeyLookup => {
                            // Primary key point lookup
                            if let Some(ref lookup_val) = plan.lookup_value {
                                let results = self
                                    .storage
                                    .find_by_primary_key(
                                        &source,
                                        lookup_val,
                                        self.current_tx.as_ref(),
                                    )
                                    .await
                                    .map_err(|e| {
                                        ExecutorError::InvalidOperation(e.message().to_string())
                                    })?;
                                (results, false)
                            } else {
                                (vec![], false)
                            }
                        }
                        IndexScanType::ExactMatch => {
                            // Secondary index equality lookup
                            if let Some(ref lookup_val) = plan.lookup_value {
                                let results = self
                                    .storage
                                    .find_by_index(
                                        &source,
                                        &plan.index_name,
                                        lookup_val,
                                        self.current_tx.as_ref(),
                                    )
                                    .await
                                    .map_err(|e| {
                                        ExecutorError::InvalidOperation(e.message().to_string())
                                    })?;
                                (results, false)
                            } else {
                                // Fallback to full scan
                                let data = self
                                    .execute_get(source.clone(), filter, fields, extensions, None)
                                    .await?;
                                let rows = match data {
                                    Value::Array(arr) => arr,
                                    other => vec![other],
                                };
                                (rows, false)
                            }
                        }
                        IndexScanType::OrderedScanAsc | IndexScanType::OrderedScanDesc => {
                            // Use secondary index for ordered scan
                            let ascending = matches!(plan.scan_type, IndexScanType::OrderedScanAsc);
                            let limit = if skip.is_none() {
                                take.map(|t| t as usize)
                            } else {
                                // Need more rows if we're skipping
                                take.map(|t| t as usize + skip.unwrap_or(0) as usize)
                            };

                            let results = self
                                .storage
                                .scan_by_index_ordered(
                                    &source,
                                    &plan.index_name,
                                    ascending,
                                    limit,
                                    self.current_tx.as_ref(),
                                )
                                .await
                                .map_err(|e| {
                                    ExecutorError::InvalidOperation(e.message().to_string())
                                })?;
                            (results, true) // Used index for ordering
                        }
                        IndexScanType::PrimaryKeyScanAsc | IndexScanType::PrimaryKeyScanDesc => {
                            // Use direct LSM scan for primary key ordering
                            let ascending =
                                matches!(plan.scan_type, IndexScanType::PrimaryKeyScanAsc);
                            let limit = if skip.is_none() {
                                take.map(|t| t as usize)
                            } else {
                                take.map(|t| t as usize + skip.unwrap_or(0) as usize)
                            };

                            let results = self
                                .storage
                                .scan_by_primary_key_order(
                                    &source,
                                    ascending,
                                    limit,
                                    self.current_tx.as_ref(),
                                )
                                .await
                                .map_err(|e| {
                                    ExecutorError::InvalidOperation(e.message().to_string())
                                })?;
                            (results, true) // Data is already in order
                        }
                    }
                } else {
                    // No usable index, use full scan
                    let storage_limit = if order_by.is_none() && skip.is_none() {
                        take.map(|t| t as usize)
                    } else {
                        None
                    };
                    let data = self
                        .execute_get(source, filter, fields, extensions, storage_limit)
                        .await?;
                    let rows = match data {
                        Value::Array(arr) => arr,
                        other => vec![other],
                    };
                    (rows, false)
                };

                // Apply ordering if specified AND we didn't use index for ordering
                if let Some(order_fields) = order_by
                    && !used_index_for_order
                {
                    self.apply_ordering(&mut rows, &order_fields);
                }

                // Apply skip (offset) if specified
                if let Some(skip_count) = skip {
                    let skip_usize = skip_count as usize;
                    if skip_usize < rows.len() {
                        rows = rows.into_iter().skip(skip_usize).collect();
                    } else {
                        rows.clear();
                    }
                }

                // Apply take (limit) if specified
                if let Some(take_count) = take {
                    rows.truncate(take_count as usize);
                }

                let row_count = rows.len() as u64;
                ExecutionResult::Ok {
                    data: rows,
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: Some(row_count),
                }
            }
            // Transaction control statements (MVCC)
            Statement::Begin => {
                if self.current_tx.is_some() {
                    return Err(ExecutorError::InvalidOperation(
                        "Transaction already active. COMMIT or ROLLBACK first.".into(),
                    ));
                }
                let tx = self.storage.tx_manager().begin();
                let tx_id = tx.tx_id;
                let start_ts = tx.start_ts;

                // Write TxBegin to WAL
                self.storage.wal_tx_begin(tx_id, start_ts).map_err(|e| {
                    ExecutorError::InvalidOperation(format!("WAL write failed: {}", e))
                })?;

                self.current_tx = Some(tx);
                ExecutionResult::Ok {
                    data: vec![Value::Object(
                        [
                            ("operation".to_string(), Value::String("begin".to_string())),
                            ("tx_id".to_string(), Value::Int64(tx_id as i64)),
                        ]
                        .into_iter()
                        .collect(),
                    )],
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: None,
                }
            }
            Statement::Commit => {
                let tx = self.current_tx.take().ok_or_else(|| {
                    ExecutorError::InvalidOperation("No active transaction to COMMIT".into())
                })?;
                let commit_ts = self.storage.tx_manager().commit(tx.tx_id).map_err(|e| {
                    ExecutorError::InvalidOperation(format!("Commit failed: {}", e))
                })?;

                // Write TxCommit to WAL
                self.storage
                    .wal_tx_commit(tx.tx_id, commit_ts)
                    .map_err(|e| {
                        ExecutorError::InvalidOperation(format!("WAL write failed: {}", e))
                    })?;

                ExecutionResult::Ok {
                    data: vec![Value::Object(
                        [
                            ("operation".to_string(), Value::String("commit".to_string())),
                            ("tx_id".to_string(), Value::Int64(tx.tx_id as i64)),
                            ("commit_ts".to_string(), Value::Int64(commit_ts as i64)),
                        ]
                        .into_iter()
                        .collect(),
                    )],
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: Some(commit_ts),
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: None,
                }
            }
            Statement::Rollback => {
                let tx = self.current_tx.take().ok_or_else(|| {
                    ExecutorError::InvalidOperation("No active transaction to ROLLBACK".into())
                })?;

                // Delete all versioned entries written by this transaction
                self.storage
                    .rollback_transaction_writes(tx.tx_id)
                    .await
                    .map_err(|e| {
                        ExecutorError::InvalidOperation(format!("Rollback failed: {}", e))
                    })?;

                // Write TxRollback to WAL
                self.storage.wal_tx_rollback(tx.tx_id).map_err(|e| {
                    ExecutorError::InvalidOperation(format!("WAL write failed: {}", e))
                })?;

                self.storage.tx_manager().abort(tx.tx_id).map_err(|e| {
                    ExecutorError::InvalidOperation(format!("Rollback failed: {}", e))
                })?;
                ExecutionResult::Ok {
                    data: vec![Value::Object(
                        [
                            (
                                "operation".to_string(),
                                Value::String("rollback".to_string()),
                            ),
                            ("tx_id".to_string(), Value::Int64(tx.tx_id as i64)),
                        ]
                        .into_iter()
                        .collect(),
                    )],
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: None,
                }
            }
            Statement::MakeIndex {
                index_name,
                table_name,
                columns,
                unique,
            } => {
                self.execute_make_index(
                    index_name.clone(),
                    table_name.clone(),
                    columns.clone(),
                    unique,
                )
                .await?;
                ExecutionResult::Ok {
                    data: vec![Value::Object(
                        [
                            (
                                "operation".to_string(),
                                Value::String("create_index".to_string()),
                            ),
                            ("index_name".to_string(), Value::String(index_name)),
                            ("table_name".to_string(), Value::String(table_name)),
                            (
                                "columns".to_string(),
                                Value::Array(columns.into_iter().map(Value::String).collect()),
                            ),
                            ("unique".to_string(), Value::Bool(unique)),
                            (
                                "status".to_string(),
                                Value::String("backfill_in_progress".to_string()),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    )],
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: None,
                }
            }
            Statement::DropIndex {
                index_name,
                table_name,
            } => {
                self.execute_drop_index(index_name.clone(), table_name.clone())
                    .await?;
                ExecutionResult::Ok {
                    data: vec![Value::Object(
                        [
                            (
                                "operation".to_string(),
                                Value::String("drop_index".to_string()),
                            ),
                            ("index_name".to_string(), Value::String(index_name)),
                            ("table_name".to_string(), Value::String(table_name)),
                        ]
                        .into_iter()
                        .collect(),
                    )],
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: None,
                }
            }
            Statement::DropTable { table_name } => {
                self.execute_drop_table(table_name.clone()).await?;
                ExecutionResult::Ok {
                    data: vec![Value::Object(
                        [
                            (
                                "operation".to_string(),
                                Value::String("drop_table".to_string()),
                            ),
                            ("table_name".to_string(), Value::String(table_name)),
                        ]
                        .into_iter()
                        .collect(),
                    )],
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: None,
                }
            }
            Statement::Describe { table_name } => {
                let description = self.execute_describe(table_name).await?;
                ExecutionResult::Ok {
                    data: vec![description],
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: None,
                }
            }
            Statement::Count { table_name } => {
                let count = self.execute_count(&table_name).await?;
                ExecutionResult::Ok {
                    data: vec![Value::Object(
                        [("count".to_string(), Value::Int64(count as i64))]
                            .into_iter()
                            .collect(),
                    )],
                    time: chrono::Utc::now().timestamp_millis() as u64,
                    commit_timestamp: None,
                    time_elapsed: Some(start.elapsed().as_millis() as u64),
                    row_count: Some(count as u64),
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
                    // Primary key and required columns are non-nullable
                    nullable: !f.is_primary && !f.is_required,
                    default: f.default.clone(),
                    is_primary: f.is_primary,
                    // Primary key columns are always unique
                    is_unique: f.is_primary || f.is_unique,
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

    /// Create a secondary index on a table
    async fn execute_make_index(
        &mut self,
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
    ) -> ExecutorResult<Value> {
        // Validate table exists and get schema
        let schema = self.storage.get_schema(&table_name).ok_or_else(|| {
            ExecutorError::InvalidOperation(format!("table '{table_name}' not found"))
        })?;

        // Validate columns exist in schema
        match &schema {
            Schema::Table {
                columns: table_cols,
                ..
            } => {
                for col in &columns {
                    if !table_cols.iter().any(|tc| &tc.name == col) {
                        return Err(ExecutorError::InvalidOperation(format!(
                            "column '{col}' not found in table '{table_name}'"
                        )));
                    }
                }
            }
            Schema::Collection { .. } => {
                // Collections don't have strict schema, allow any column
            }
            Schema::KeySpace { .. } => {
                return Err(ExecutorError::InvalidOperation(
                    "cannot create index on keyspace".to_string(),
                ));
            }
        }

        // Create the index
        self.storage
            .create_index(&table_name, &index_name, columns.clone(), unique)
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;

        Ok(Value::String(format!(
            "index '{}' created on '{}'({})",
            index_name,
            table_name,
            columns.join(", ")
        )))
    }

    /// Drop an index from a table
    async fn execute_drop_index(
        &mut self,
        index_name: String,
        table_name: String,
    ) -> ExecutorResult<Value> {
        self.storage
            .drop_index(&table_name, &index_name)
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;

        Ok(Value::String(format!(
            "index '{}' dropped from '{}'",
            index_name, table_name
        )))
    }

    /// Drop a table/collection
    async fn execute_drop_table(&mut self, table_name: String) -> ExecutorResult<Value> {
        self.storage
            .drop_collection(&table_name)
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;

        Ok(Value::String(format!("table '{}' dropped", table_name)))
    }

    /// Describe a table/collection
    async fn execute_describe(&mut self, table_name: String) -> ExecutorResult<Value> {
        use monodb_common::schema::Schema;
        use std::collections::BTreeMap;

        let schema = self.storage.get_schema(&table_name).ok_or_else(|| {
            ExecutorError::InvalidOperation(format!("table '{table_name}' not found"))
        })?;

        let mut result: BTreeMap<String, Value> = BTreeMap::new();

        match &schema {
            Schema::Table {
                name,
                columns,
                primary_key,
                indexes,
            } => {
                result.insert("name".to_string(), Value::String(name.clone()));
                result.insert("type".to_string(), Value::String("table".to_string()));

                // Columns as array of objects
                let cols: Vec<Value> = columns
                    .iter()
                    .map(|col| {
                        Value::Object(
                            [
                                ("name".to_string(), Value::String(col.name.clone())),
                                (
                                    "data_type".to_string(),
                                    Value::String(format!("{:?}", col.data_type).to_lowercase()),
                                ),
                                ("is_primary".to_string(), Value::Bool(col.is_primary)),
                                ("is_unique".to_string(), Value::Bool(col.is_unique)),
                                ("nullable".to_string(), Value::Bool(col.nullable)),
                            ]
                            .into_iter()
                            .collect(),
                        )
                    })
                    .collect();
                result.insert("columns".to_string(), Value::Array(cols));

                // Primary key as array of column names
                let pk: Vec<Value> = primary_key
                    .iter()
                    .map(|k| Value::String(k.clone()))
                    .collect();
                result.insert("primary_key".to_string(), Value::Array(pk));

                // Indexes as array of objects
                let idxs: Vec<Value> = indexes
                    .iter()
                    .map(|idx| {
                        Value::Object(
                            [
                                ("name".to_string(), Value::String(idx.name.clone())),
                                (
                                    "columns".to_string(),
                                    Value::Array(
                                        idx.columns
                                            .iter()
                                            .map(|c| Value::String(c.clone()))
                                            .collect(),
                                    ),
                                ),
                                ("unique".to_string(), Value::Bool(idx.unique)),
                            ]
                            .into_iter()
                            .collect(),
                        )
                    })
                    .collect();
                result.insert("indexes".to_string(), Value::Array(idxs));
            }
            Schema::Collection {
                name,
                validation,
                indexes,
            } => {
                result.insert("name".to_string(), Value::String(name.clone()));
                result.insert("type".to_string(), Value::String("collection".to_string()));

                if let Some(rule) = validation {
                    result.insert(
                        "validation".to_string(),
                        Value::String(format!("{:?}", rule)),
                    );
                }

                let idxs: Vec<Value> = indexes
                    .iter()
                    .map(|idx| {
                        Value::Object(
                            [
                                ("name".to_string(), Value::String(idx.name.clone())),
                                (
                                    "columns".to_string(),
                                    Value::Array(
                                        idx.columns
                                            .iter()
                                            .map(|c| Value::String(c.clone()))
                                            .collect(),
                                    ),
                                ),
                                ("unique".to_string(), Value::Bool(idx.unique)),
                            ]
                            .into_iter()
                            .collect(),
                        )
                    })
                    .collect();
                result.insert("indexes".to_string(), Value::Array(idxs));
            }
            Schema::KeySpace {
                name,
                ttl_enabled,
                max_size,
                persistence,
            } => {
                result.insert("name".to_string(), Value::String(name.clone()));
                result.insert("type".to_string(), Value::String("keyspace".to_string()));
                result.insert("ttl_enabled".to_string(), Value::Bool(*ttl_enabled));
                result.insert(
                    "persistence".to_string(),
                    Value::String(format!("{:?}", persistence)),
                );
                if let Some(size) = max_size {
                    result.insert("max_size".to_string(), Value::Int64(*size as i64));
                }
            }
        }

        Ok(Value::Object(result))
    }

    /// Count rows in a table/collection
    async fn execute_count(&mut self, table_name: &str) -> ExecutorResult<usize> {
        let count = self
            .storage
            .count_rows(table_name, self.current_tx.as_ref())
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;
        Ok(count)
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
                        .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;
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
                // Unknown collection type, treat as collection
                if !row.contains_key("_id") {
                    let oid = monodb_common::ObjectId::new()
                        .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;
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
                        .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;

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

        // Insert with transaction support
        let data = Data::Row(row);

        // Use insert_with_tx for MVCC support on relational tables
        self.storage
            .insert_with_tx(&target, &data, self.current_tx.as_ref())
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;

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
        target: String,
        filter: Option<Expr>,
        changes: Vec<Assignment>,
        _extensions: Vec<Extension>,
    ) -> ExecutorResult<(u64, Value)> {
        // Convert filter expression
        let filter = if let Some(expr) = filter {
            Some(self.translate_filter(expr).await?)
        } else {
            None
        };

        // Convert changes to HashMap<String, Value>
        let mut updates = std::collections::HashMap::new();
        for assignment in changes {
            let value = self.evaluate_expression(assignment.value).await?;
            updates.insert(assignment.key, value);
        }

        // Call storage engine update with transaction context
        let count = self
            .storage
            .update_many_with_tx(&target, filter, updates, self.current_tx.as_ref())
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;

        Ok((
            count,
            Value::String(format!("Updated {count} rows in '{target}'")),
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
            .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;

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
        limit: Option<usize>,
    ) -> ExecutorResult<Value> {
        let filter = if let Some(expr) = filter {
            Some(self.translate_filter(expr).await?)
        } else {
            None
        };

        let query = Query {
            filter,
            limit,
            ..Default::default()
        };

        // Use find_with_tx for MVCC visibility on relational tables
        let results = self
            .storage
            .find_with_tx(&source, query, self.current_tx.as_ref())
            .await
            .map_err(|e| ExecutorError::InvalidOperation(e.message().to_string()))?;

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

    /// Apply ordering to a list of rows based on `order by` fields.
    fn apply_ordering(&self, rows: &mut [Value], order_fields: &[OrderByField]) {
        rows.sort_by(|a, b| {
            for order_field in order_fields {
                let cmp = self.compare_values_by_field(a, b, &order_field.field);
                let cmp = match order_field.direction {
                    SortDirection::Asc => cmp,
                    SortDirection::Desc => cmp.reverse(),
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    /// Compare two values by a specific field name.
    fn compare_values_by_field(&self, a: &Value, b: &Value, field: &str) -> std::cmp::Ordering {
        let val_a = Self::get_field_value(a, field);
        let val_b = Self::get_field_value(b, field);
        Self::compare_values(&val_a, &val_b)
    }

    /// Extract a field value from a Value (Row or Object).
    fn get_field_value(value: &Value, field: &str) -> Option<Value> {
        match value {
            Value::Row(row) => row.get(field).cloned(),
            Value::Object(obj) => obj.get(field).cloned(),
            _ => None,
        }
    }

    /// Compare two optional values for sorting.
    fn compare_values(a: &Option<Value>, b: &Option<Value>) -> std::cmp::Ordering {
        match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less, // NULLs first
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(va), Some(vb)) => Self::compare_value_inner(va, vb),
        }
    }

    /// Compare two concrete values.
    fn compare_value_inner(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            // Integer comparisons (same type)
            (Value::Int32(ia), Value::Int32(ib)) => ia.cmp(ib),
            (Value::Int64(ia), Value::Int64(ib)) => ia.cmp(ib),

            // Integer cross-type comparisons
            (Value::Int32(i32a), Value::Int64(i64b)) => (*i32a as i64).cmp(i64b),
            (Value::Int64(i64a), Value::Int32(i32b)) => i64a.cmp(&(*i32b as i64)),

            // Float comparisons (same type)
            (Value::Float32(fa), Value::Float32(fb)) => {
                fa.partial_cmp(fb).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(fa), Value::Float64(fb)) => {
                fa.partial_cmp(fb).unwrap_or(Ordering::Equal)
            }

            // Float cross-type comparisons
            (Value::Float32(f32a), Value::Float64(f64b)) => {
                (*f32a as f64).partial_cmp(f64b).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(f64a), Value::Float32(f32b)) => {
                f64a.partial_cmp(&(*f32b as f64)).unwrap_or(Ordering::Equal)
            }

            // Int-Float cross-type comparisons
            (Value::Int32(i), Value::Float64(f)) => {
                (*i as f64).partial_cmp(f).unwrap_or(Ordering::Equal)
            }
            (Value::Int64(i), Value::Float64(f)) => {
                (*i as f64).partial_cmp(f).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(f), Value::Int32(i)) => {
                f.partial_cmp(&(*i as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(f), Value::Int64(i)) => {
                f.partial_cmp(&(*i as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Int32(i), Value::Float32(f)) => {
                (*i as f32).partial_cmp(f).unwrap_or(Ordering::Equal)
            }
            (Value::Int64(i), Value::Float32(f)) => {
                (*i as f32).partial_cmp(f).unwrap_or(Ordering::Equal)
            }
            (Value::Float32(f), Value::Int32(i)) => {
                f.partial_cmp(&(*i as f32)).unwrap_or(Ordering::Equal)
            }
            (Value::Float32(f), Value::Int64(i)) => {
                f.partial_cmp(&(*i as f32)).unwrap_or(Ordering::Equal)
            }

            // String comparison
            (Value::String(sa), Value::String(sb)) => sa.cmp(sb),

            // Boolean comparison
            (Value::Bool(ba), Value::Bool(bb)) => ba.cmp(bb),

            // Date/time comparison
            (Value::Date(da), Value::Date(db)) => da.cmp(db),
            (Value::DateTime(dta), Value::DateTime(dtb)) => dta.cmp(dtb),

            // Fallback: convert to string and compare
            _ => a.to_string().cmp(&b.to_string()),
        }
    }
}

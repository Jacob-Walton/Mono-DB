use indexmap::IndexMap;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use monodb_common::ObjectId;
use monodb_common::schema::Schema;
use monodb_common::value::Value as MonoValue;
use monodb_common::{MonoError, Result, schema::KeySpacePersistence};
use once_cell::sync::Lazy;

use crate::config::StorageConfig;
use crate::storage::{
    Data, btree::BTree, buffer_pool::BufferPool, disk_manager::DiskManager, lsm::LsmTree, wal::Wal,
};
use parking_lot::RwLock;
use tracing::{debug, info};

/// Secondary index entry mapping field values to primary keys
type IndexEntry = DashMap<Vec<u8>, HashSet<Vec<u8>>>;

pub struct StorageEngine {
    btrees: Arc<DashMap<String, Arc<BTree>>>,
    lsm_trees: Arc<DashMap<String, Arc<LsmTree>>>,
    buffer_pool: Option<Arc<BufferPool>>,
    #[allow(dead_code)]
    disk_manager: Option<Arc<DiskManager>>,
    schemas: Arc<DashMap<String, Schema>>,
    memory_keyspaces: Arc<DashMap<String, Arc<DashMap<String, MonoValue>>>>,
    secondary_indexes: Arc<DashMap<String, DashMap<String, Arc<IndexEntry>>>>,
    config: StorageConfig,
    /// Transaction manager for MVCC (relational tables only)
    tx_manager: Arc<TransactionManager>,
}

impl StorageEngine {
    pub async fn new(config: StorageConfig) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.data_dir)?;

        let engine = Arc::new(Self {
            btrees: Arc::new(DashMap::new()),
            lsm_trees: Arc::new(DashMap::new()),
            buffer_pool: None,
            disk_manager: None,
            schemas: Arc::new(DashMap::new()),
            memory_keyspaces: Arc::new(DashMap::new()),
            secondary_indexes: Arc::new(DashMap::new()),
            config,
            tx_manager: Arc::new(TransactionManager::new()),
        });

        // Load persisted schemas
        engine.load_schemas().await?;

        Ok(engine)
    }

    /// Validate data against schema before insertion
    fn validate_data(&self, collection: &str, data: &Data<'_>) -> Result<()> {
        let schema = self
            .schemas
            .get(collection)
            .ok_or_else(|| MonoError::NotFound(format!("Collection '{collection}' not found")))?;

        match (schema.value(), data) {
            (Schema::Table { columns, .. }, Data::Row(row)) => {
                self.validate_table_row(columns, row)
            }
            (Schema::Collection { validation, .. }, Data::Document(doc)) => {
                self.validate_collection_document(validation, doc)
            }
            (Schema::KeySpace { max_size, .. }, Data::KeyValue { value, .. }) => {
                self.validate_keyspace_value(max_size, value)
            }
            _ => Ok(()),
        }
    }

    /// Validate a table row against column definitions
    fn validate_table_row(
        &self,
        columns: &[monodb_common::schema::TableColumn],
        row: &HashMap<String, MonoValue>,
    ) -> Result<()> {
        let column_names: HashSet<_> = columns.iter().map(|c| &c.name).collect();

        // Check for extra fields
        for key in row.keys() {
            if !column_names.contains(key) {
                return Err(MonoError::InvalidOperation(format!(
                    "Unknown field '{key}' in row"
                )));
            }
        }

        // Validate each column
        for col in columns {
            match row.get(&col.name) {
                Some(value) => {
                    // Type checking
                    if value.data_type() != col.data_type {
                        return Err(MonoError::TypeError {
                            expected: format!("{:?}", col.data_type),
                            actual: format!("{:?}", value.data_type()),
                        });
                    }

                    // Additional type-specific validation
                    self.validate_value_constraints(value, &col.name)?;
                }
                None => {
                    // Check if field is required
                    if !col.nullable && col.default.is_none() {
                        return Err(MonoError::InvalidOperation(format!(
                            "Missing required field '{}'",
                            col.name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate collection document against validation rules
    fn validate_collection_document(
        &self,
        validation: &Option<monodb_common::schema::ValidationRule>,
        doc: &MonoValue,
    ) -> Result<()> {
        if let Some(rule) = validation {
            match rule {
                monodb_common::schema::ValidationRule::JsonSchema(schema_str) => {
                    self.validate_json_schema(doc, schema_str)?;
                }
                monodb_common::schema::ValidationRule::Custom(rule_str) => {
                    self.validate_custom_rule(doc, rule_str)?;
                }
            }
        }
        Ok(())
    }

    /// Validate against JSON Schema
    fn validate_json_schema(&self, doc: &MonoValue, _schema: &str) -> Result<()> {
        // TODO: Implement
        if !matches!(doc, MonoValue::Object(_)) {
            return Err(MonoError::InvalidOperation(
                "Document must be an object".to_string(),
            ));
        }
        Ok(())
    }

    /// Validate against custom rules
    fn validate_custom_rule(&self, doc: &MonoValue, _rule: &str) -> Result<()> {
        // TODO: Implement
        if !matches!(doc, MonoValue::Object(_)) {
            return Err(MonoError::InvalidOperation(
                "Document must be an object for custom validation".to_string(),
            ));
        }
        Ok(())
    }

    /// Validate keyspace value size constraints
    fn validate_keyspace_value(&self, max_size: &Option<usize>, value: &[u8]) -> Result<()> {
        if let Some(max) = max_size
            && value.len() > *max
        {
            return Err(MonoError::InvalidOperation(format!(
                "Value size {} exceeds maximum size {}",
                value.len(),
                max
            )));
        }
        Ok(())
    }

    /// Validate value-specific constraints
    fn validate_value_constraints(&self, value: &MonoValue, field_name: &str) -> Result<()> {
        if let MonoValue::String(s) = value {
            // Example: Check for empty strings in certain fields
            if s.is_empty() && field_name.ends_with("_id") {
                return Err(MonoError::InvalidOperation(format!(
                    "Field '{field_name}' cannot be empty"
                )));
            }
        }
        Ok(())
    }

    /// Check uniqueness constraints before insertion
    async fn check_uniqueness_constraints(
        &self,
        collection: &str,
        data: &Data<'_>,
        primary_key: Option<&[u8]>,
    ) -> Result<()> {
        let schema = self
            .schemas
            .get(collection)
            .ok_or_else(|| MonoError::NotFound(format!("Collection '{collection}' not found")))?;

        match schema.value() {
            Schema::Table {
                columns, indexes, ..
            } => {
                // Check explicit unique indexes first
                for index in indexes.iter().filter(|idx| idx.unique) {
                    self.check_unique_index_violation(collection, data, index, primary_key)
                        .await?;
                }

                // Then check any column marked is_unique
                for col in columns.iter().filter(|c| c.is_unique) {
                    let field_name = &col.name;

                    // Extract the value being inserted
                    let inserted_value_opt = match data {
                        Data::Row(row) => row.get(field_name),
                        Data::Document(doc) => doc.as_object().and_then(|m| m.get(field_name)),
                        _ => None,
                    };

                    if let Some(inserted_value) = inserted_value_opt {
                        // Scan existing records for duplicates
                        let query = crate::storage::models::Query::default();
                        let existing_rows = self.find(collection, query).await?;

                        for row_val in existing_rows {
                            if let MonoValue::Object(map) = &row_val
                                && let Some(existing_val) = map.get(field_name)
                                && existing_val == inserted_value
                            {
                                return Err(MonoError::InvalidOperation(format!(
                                    "Unique constraint violation on field '{}'",
                                    field_name
                                )));
                            }
                        }
                    }
                }
            }
            Schema::Collection { indexes, .. } => {
                // Keep existing behavior for collections
                for index in indexes.iter().filter(|idx| idx.unique) {
                    self.check_unique_index_violation(collection, data, index, primary_key)
                        .await?;
                }
            }
            Schema::KeySpace { .. } => return Ok(()),
        }

        Ok(())
    }

    /// Check if inserting data would violate a unique index
    async fn check_unique_index_violation(
        &self,
        collection: &str,
        data: &Data<'_>,
        index: &monodb_common::schema::Index,
        primary_key: Option<&[u8]>,
    ) -> Result<()> {
        // Extract field values for this index
        let index_values = self.extract_index_values(data, &index.columns)?;

        // Get or create the secondary index
        let collection_indexes = self
            .secondary_indexes
            .entry(collection.to_string())
            .or_default();

        let secondary_index = collection_indexes
            .entry(index.name.clone())
            .or_insert_with(|| Arc::new(DashMap::new()));

        // Serialize the index key
        let index_key = Self::encode_composite_key(&index_values);

        // Check if this value already exists
        if let Some(existing_pks) = secondary_index.get(&index_key) {
            // If the primary key is the same, this is an update, not a duplicate
            if let Some(pk) = primary_key {
                if !existing_pks.contains(pk) {
                    return Err(MonoError::InvalidOperation(format!(
                        "Unique constraint violation on index '{}': value already exists",
                        index.name
                    )));
                }
            } else {
                // New insert with duplicate value
                return Err(MonoError::InvalidOperation(format!(
                    "Unique constraint violation on index '{}': value already exists",
                    index.name
                )));
            }
        }

        Ok(())
    }

    /// Extract index values from data
    fn extract_index_values(&self, data: &Data<'_>, columns: &[String]) -> Result<Vec<MonoValue>> {
        let mut values = Vec::new();

        match data {
            Data::Row(row) => {
                for col in columns {
                    let value = row.get(col).ok_or_else(|| {
                        MonoError::InvalidOperation(format!(
                            "Index column '{col}' not found in row"
                        ))
                    })?;
                    values.push(value.clone());
                }
            }
            Data::Document(doc) => {
                if let MonoValue::Object(obj) = doc {
                    for col in columns {
                        let value = obj.get(col).ok_or_else(|| {
                            MonoError::InvalidOperation(format!(
                                "Index column '{col}' not found in document"
                            ))
                        })?;
                        values.push(value.clone());
                    }
                } else {
                    return Err(MonoError::InvalidOperation(
                        "Document must be an object".to_string(),
                    ));
                }
            }
            Data::KeyValue { .. } => {
                return Err(MonoError::InvalidOperation(
                    "Cannot create index on key-value data".to_string(),
                ));
            }
        }

        Ok(values)
    }

    /// Update secondary indexes after successful insertion
    async fn update_secondary_indexes(
        &self,
        collection: &str,
        data: &Data<'_>,
        primary_key: &[u8],
    ) -> Result<()> {
        let schema = self
            .schemas
            .get(collection)
            .ok_or_else(|| MonoError::NotFound(format!("Collection '{collection}' not found")))?;

        let indexes = match schema.value() {
            Schema::Table { indexes, .. } => indexes,
            Schema::Collection { indexes, .. } => indexes,
            Schema::KeySpace { .. } => return Ok(()),
        };

        let collection_indexes = self
            .secondary_indexes
            .entry(collection.to_string())
            .or_default();

        // Update each index
        for index in indexes {
            let index_values = self.extract_index_values(data, &index.columns)?;
            // Serialize index key
            let index_key = Self::encode_composite_key(&index_values);

            let secondary_index = collection_indexes
                .entry(index.name.clone())
                .or_insert_with(|| Arc::new(DashMap::new()));

            secondary_index
                .entry(index_key)
                .or_default()
                .insert(primary_key.to_vec());
        }

        Ok(())
    }

    /// Remove entry from secondary indexes
    async fn remove_from_secondary_indexes(
        &self,
        collection: &str,
        primary_key: &[u8],
    ) -> Result<()> {
        if let Some(collection_indexes) = self.secondary_indexes.get(collection) {
            for index_entry in collection_indexes.iter() {
                let secondary_index = index_entry.value();

                // Find and remove the primary key from all index entries
                let mut keys_to_remove = Vec::new();
                for entry in secondary_index.iter() {
                    let mut pk_set = entry.value().clone();
                    pk_set.remove(primary_key);

                    if pk_set.is_empty() {
                        keys_to_remove.push(entry.key().clone());
                    } else {
                        secondary_index.insert(entry.key().clone(), pk_set);
                    }
                }

                // Remove empty index entries
                for key in keys_to_remove {
                    secondary_index.remove(&key);
                }
            }
        }
        Ok(())
    }

    /// Delete a key from a collection/keyspace with index cleanup
    pub async fn delete(&self, collection: &str, key: &str) -> Result<()> {
        let key_bytes = key.as_bytes();

        // Remove from secondary indexes first
        self.remove_from_secondary_indexes(collection, key_bytes)
            .await?;

        // Memory keyspace short-circuit
        if let Some(schema) = self.schemas.get(collection)
            && let Schema::KeySpace {
                persistence: KeySpacePersistence::Memory,
                ..
            } = schema.value()
            && let Some(store) = self.memory_keyspaces.get(collection)
        {
            store.remove(key);
            return Ok(());
        }

        // Delete from storage
        if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.delete(key_bytes.to_vec()).await
            } else {
                Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )))
            }
        } else if let Some(btree) = self.btrees.get(collection) {
            btree.delete(key_bytes).await
        } else {
            Err(MonoError::NotFound(format!(
                "collection '{collection}' not found"
            )))
        }
    }

    /// Delete all entries from a collection that match a given filter.
    pub async fn delete_many(
        &self,
        collection: &str,
        filter: crate::storage::models::Filter,
    ) -> Result<u64> {
        use crate::storage::models::Query;

        let schema = self
            .schemas
            .get(collection)
            .ok_or_else(|| MonoError::NotFound(format!("collection '{collection}' not found")))?;

        let query = Query {
            filter: Some(filter.clone()),
            ..Default::default()
        };

        let matched_values = match schema.value() {
            Schema::KeySpace { .. } => self.find_kv(collection, query).await?,
            Schema::Collection { .. } | Schema::Table { .. } => {
                self.find_with_filter(collection, query).await?
            }
        };

        // Extract keys based on schema type
        let keys: Vec<String> = match schema.value() {
            Schema::Collection { .. } => {
                // Collections use _id as the key
                matched_values
                    .into_iter()
                    .filter_map(|v| {
                        v.as_object()
                            .and_then(|m| m.get("_id"))
                            .map(|id_val| match id_val {
                                MonoValue::ObjectId(oid) => oid.to_hex(),
                                MonoValue::String(s) => s.clone(),
                                MonoValue::Int32(i) => i.to_string(),
                                MonoValue::Int64(i) => i.to_string(),
                                _ => format!("{id_val}"),
                            })
                    })
                    .collect()
            }
            Schema::KeySpace { .. } => {
                // KeySpaces use the 'key' field
                matched_values
                    .into_iter()
                    .filter_map(|v| {
                        v.as_object()
                            .and_then(|m| m.get("key"))
                            .and_then(|v| v.as_string())
                            .map(|s| s.to_string())
                    })
                    .collect()
            }
            Schema::Table { primary_key, .. } => {
                // Relational tables use the primary key column(s)
                if primary_key.len() == 1 {
                    let pk_name = &primary_key[0];
                    matched_values
                        .into_iter()
                        .filter_map(|v| {
                            v.as_object()
                                .and_then(|m| m.get(pk_name))
                                .map(|pk_val| match pk_val {
                                    MonoValue::String(s) => s.clone(),
                                    MonoValue::Int32(i) => i.to_string(),
                                    MonoValue::Int64(i) => i.to_string(),
                                    _ => format!("{pk_val}"),
                                })
                        })
                        .collect()
                } else {
                    // Composite primary keys not yet supported for delete
                    vec![]
                }
            }
        };

        let mut deleted = 0u64;
        for key in keys.iter() {
            if self.delete(collection, key).await.is_ok() {
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    /// Create a new collection with validation and index setup
    /// Force flush all LSM tree memtables to disk (test-only)
    #[cfg(test)]
    pub async fn flush_all(&self) -> Result<()> {
        for entry in self.lsm_trees.iter() {
            entry.value().flush().await?;
        }
        Ok(())
    }

    /// Get the transaction manager for MVCC operations
    pub fn tx_manager(&self) -> &TransactionManager {
        &self.tx_manager
    }

    // MVCC Visibility and Conflict Detection

    /// Check if a version (identified by version_ts) is visible to the given transaction.
    /// version_ts is the tx_id of the transaction that created this version.
    pub fn is_version_visible(&self, version_ts: u64, reading_tx: &Transaction) -> bool {
        // Check if this is our own write
        if version_ts == reading_tx.tx_id {
            return true;
        }

        // Look up the creating transaction
        if let Some(creator_tx) = self.tx_manager.get(version_ts) {
            match creator_tx.status {
                TxStatus::Committed => {
                    // Visible if committed before our snapshot started
                    if let Some(commit_ts) = creator_tx.commit_ts {
                        commit_ts <= reading_tx.start_ts
                    } else {
                        false // Shouldn't happen for committed tx
                    }
                }
                TxStatus::Active => {
                    // Only visible if it's our own transaction (checked above)
                    false
                }
                TxStatus::Aborted => false,
            }
        } else {
            // Transaction not in manager, assume it's an old committed transaction
            // whose entry was cleaned up. The version_ts IS the commit_ts in this case.
            version_ts <= reading_tx.start_ts
        }
    }

    /// Check if a version is visible when there's no active transaction (auto-commit mode).
    /// In this case, we see all committed versions.
    pub fn is_version_visible_no_tx(&self, version_ts: u64) -> bool {
        if let Some(creator_tx) = self.tx_manager.get(version_ts) {
            creator_tx.status == TxStatus::Committed
        } else {
            // Old committed transaction, always visible
            true
        }
    }

    /// Check for write-write conflicts on a primary key.
    /// Returns error if another transaction has written to this key after our start_ts.
    pub async fn check_write_conflict(
        &self,
        collection: &str,
        pk: &[u8],
        current_tx: &Transaction,
    ) -> Result<()> {
        // Scan for all versions of this key
        let prefix = pk.to_vec();
        let mut scan_end = prefix.clone();
        scan_end.push(0xFF); // Scan all versions of this PK

        let versions = if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.scan(&prefix, &scan_end).await?
            } else {
                return Ok(()); // Collection doesn't exist, no conflict
            }
        } else if let Some(btree) = self.btrees.get(collection) {
            btree.scan(&prefix, &scan_end).await?
        } else {
            return Ok(());
        };

        for (key, _value) in versions {
            if let Some((decoded_pk, version_ts)) = decode_versioned_key(&key) {
                // Only check versions of the same primary key
                if decoded_pk != pk {
                    continue;
                }

                // Skip our own writes
                if version_ts == current_tx.tx_id {
                    continue;
                }

                if let Some(other_tx) = self.tx_manager.get(version_ts) {
                    match other_tx.status {
                        TxStatus::Active => {
                            // Another active transaction is writing to this row
                            return Err(MonoError::WriteConflict(format!(
                                "Row is locked by active transaction {}",
                                other_tx.tx_id
                            )));
                        }
                        TxStatus::Committed => {
                            // Check if committed after our start
                            if let Some(commit_ts) = other_tx.commit_ts {
                                if commit_ts > current_tx.start_ts {
                                    return Err(MonoError::WriteConflict(format!(
                                        "Row was modified by transaction {} after our start",
                                        other_tx.tx_id
                                    )));
                                }
                            }
                        }
                        TxStatus::Aborted => {
                            // Ignore aborted transactions
                        }
                    }
                }
                // If transaction not found in manager, it's old and committed before us
            }
        }

        Ok(())
    }

    pub async fn create_collection(&self, schema: Schema) -> Result<()> {
        let collection_name = match &schema {
            Schema::Table { name, .. }
            | Schema::Collection { name, .. }
            | Schema::KeySpace { name, .. } => name.clone(),
        };

        if self.schemas.contains_key(&collection_name) {
            return Err(MonoError::InvalidOperation(format!(
                "collection '{collection_name}' already exists"
            )));
        }

        // Validate schema definition
        self.validate_schema_definition(&schema)?;

        // Store schema
        self.schemas.insert(collection_name.clone(), schema.clone());

        // Create storage structures
        self.create_storage_structures_for_schema(&collection_name, &schema)
            .await?;

        // Initialize secondary indexes
        self.initialize_indexes(&collection_name, &schema).await?;

        // Persist schemas
        self.persist_schemas().await?;

        Ok(())
    }

    /// Validate schema definition for consistency
    fn validate_schema_definition(&self, schema: &Schema) -> Result<()> {
        match schema {
            Schema::Table {
                primary_key,
                columns,
                indexes,
                ..
            } => {
                // Validate primary key exists in columns
                let column_names: HashSet<_> = columns.iter().map(|c| &c.name).collect();
                for pk in primary_key {
                    if !column_names.contains(pk) {
                        return Err(MonoError::InvalidOperation(format!(
                            "Primary key column '{pk}' not found in table columns"
                        )));
                    }
                }

                // Validate index columns exist
                for index in indexes {
                    for col in &index.columns {
                        if !column_names.contains(col) {
                            return Err(MonoError::InvalidOperation(format!(
                                "Index column '{col}' not found in table columns"
                            )));
                        }
                    }
                }
            }
            Schema::Collection { indexes, .. } => {
                // Basic validation for collections
                for index in indexes {
                    if index.columns.is_empty() {
                        return Err(MonoError::InvalidOperation(format!(
                            "Index '{}' must have at least one column",
                            index.name
                        )));
                    }
                }
            }
            Schema::KeySpace { .. } => {
                // KeySpace validation
            }
        }
        Ok(())
    }

    /// Initialize secondary indexes for a collection
    async fn initialize_indexes(&self, collection_name: &str, schema: &Schema) -> Result<()> {
        let indexes = match schema {
            Schema::Table { indexes, .. } => indexes,
            Schema::Collection { indexes, .. } => indexes,
            Schema::KeySpace { .. } => return Ok(()),
        };

        if !indexes.is_empty() {
            let collection_indexes = DashMap::new();
            for index in indexes {
                collection_indexes.insert(index.name.clone(), Arc::new(DashMap::new()));
            }
            self.secondary_indexes
                .insert(collection_name.to_string(), collection_indexes);
        }

        Ok(())
    }

    /// Insert with validation and uniqueness checking
    pub async fn insert(&self, collection_name: &str, data: &Data<'_>) -> Result<()> {
        // Validate data against schema
        self.validate_data(collection_name, data)?;

        // Check uniqueness constraints
        self.check_uniqueness_constraints(collection_name, data, None)
            .await?;

        // Perform the actual insertion
        let schema = self.schemas.get(collection_name).ok_or_else(|| {
            MonoError::NotFound(format!("Collection '{collection_name}' not found"))
        })?;

        let primary_key = match (schema.value(), data) {
            (Schema::KeySpace { .. }, Data::KeyValue { key, value }) => {
                let value_typed = MonoValue::Binary(value.to_vec());
                self.insert_kv_typed(collection_name, key, &value_typed)
                    .await?;
                key.to_vec()
            }
            (Schema::Collection { .. }, Data::Document(doc)) => {
                self.insert_document(collection_name, doc).await?
            }
            (Schema::Table { .. }, Data::Row(row)) => self.insert_row(collection_name, row).await?,
            (Schema::KeySpace { .. }, Data::Row(row)) => {
                let key = row.get("key").ok_or_else(|| {
                    MonoError::InvalidOperation("Keyspace requires 'key' field".to_string())
                })?;
                let value = row.get("value").ok_or_else(|| {
                    MonoError::InvalidOperation("Keyspace requires 'value' field".to_string())
                })?;

                let key_str = match key {
                    MonoValue::String(s) => s.clone(),
                    _ => key.to_string(),
                };

                self.insert_kv_typed(collection_name, key_str.as_bytes(), value)
                    .await?;
                key_str.as_bytes().to_vec()
            }
            (Schema::Collection { .. }, Data::Row(row)) => {
                let doc_map: BTreeMap<String, MonoValue> =
                    row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

                let document = MonoValue::Object(doc_map);

                self.insert_document(collection_name, &document).await?
            }
            (schema, data) => {
                return Err(MonoError::TypeError {
                    expected: "matching data for collection type".to_string(),
                    actual: format!("collection type {schema:?}, data type {data:?}"),
                });
            }
        };

        // Update secondary indexes
        self.update_secondary_indexes(collection_name, data, &primary_key)
            .await?;

        Ok(())
    }

    /// Insert with MVCC transaction support (for relational tables only).
    /// If tx is Some, uses versioned keys and conflict detection.
    /// If tx is None, auto-commits immediately (like current behavior).
    pub async fn insert_with_tx(
        &self,
        collection_name: &str,
        data: &Data<'_>,
        tx: Option<&Transaction>,
    ) -> Result<()> {
        // Validate data against schema
        self.validate_data(collection_name, data)?;

        let schema = self.schemas.get(collection_name).ok_or_else(|| {
            MonoError::NotFound(format!("Collection '{collection_name}' not found"))
        })?;

        // Only use MVCC for relational tables with an active transaction
        let use_mvcc = tx.is_some() && matches!(schema.value(), Schema::Table { .. });

        if use_mvcc {
            let tx = tx.unwrap();
            // For relational tables with transaction: use versioned insert
            if let (Schema::Table { .. }, Data::Row(row)) = (schema.value(), data) {
                let primary_key = self.insert_row_versioned(collection_name, row, tx).await?;

                // Update secondary indexes (TODO: make these versioned too)
                self.update_secondary_indexes(collection_name, data, &primary_key)
                    .await?;

                return Ok(());
            }
        }

        // Check uniqueness constraints (skip for MVCC as conflict detection handles it)
        if !use_mvcc {
            self.check_uniqueness_constraints(collection_name, data, None)
                .await?;
        }

        // Fall back to non-versioned insert for non-relational or auto-commit
        let primary_key = match (schema.value(), data) {
            (Schema::KeySpace { .. }, Data::KeyValue { key, value }) => {
                let value_typed = MonoValue::Binary(value.to_vec());
                self.insert_kv_typed(collection_name, key, &value_typed)
                    .await?;
                key.to_vec()
            }
            (Schema::Collection { .. }, Data::Document(doc)) => {
                self.insert_document(collection_name, doc).await?
            }
            (Schema::Table { .. }, Data::Row(row)) => {
                // Auto-commit: use current timestamp from tx_manager
                let auto_ts = self.tx_manager.next_timestamp();
                self.insert_row_with_version(collection_name, row, auto_ts)
                    .await?
            }
            (Schema::KeySpace { .. }, Data::Row(row)) => {
                let key = row.get("key").ok_or_else(|| {
                    MonoError::InvalidOperation("Keyspace requires 'key' field".to_string())
                })?;
                let value = row.get("value").ok_or_else(|| {
                    MonoError::InvalidOperation("Keyspace requires 'value' field".to_string())
                })?;

                let key_str = match key {
                    MonoValue::String(s) => s.clone(),
                    _ => key.to_string(),
                };

                self.insert_kv_typed(collection_name, key_str.as_bytes(), value)
                    .await?;
                key_str.as_bytes().to_vec()
            }
            (Schema::Collection { .. }, Data::Row(row)) => {
                let doc_map: BTreeMap<String, MonoValue> =
                    row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

                let document = MonoValue::Object(doc_map);

                self.insert_document(collection_name, &document).await?
            }
            (schema, data) => {
                return Err(MonoError::TypeError {
                    expected: "matching data for collection type".to_string(),
                    actual: format!("collection type {schema:?}, data type {data:?}"),
                });
            }
        };

        // Update secondary indexes
        self.update_secondary_indexes(collection_name, data, &primary_key)
            .await?;

        Ok(())
    }

    /// Insert a row with a specific version timestamp (for auto-commit or MVCC).
    async fn insert_row_with_version(
        &self,
        collection: &str,
        row: &HashMap<String, MonoValue>,
        version_ts: u64,
    ) -> Result<Vec<u8>> {
        let schema = self.schemas.get(collection).unwrap();
        if let Schema::Table {
            primary_key,
            columns,
            ..
        } = schema.value()
        {
            if primary_key.is_empty() {
                return Err(MonoError::InvalidOperation(format!(
                    "table '{collection}' has no primary key defined"
                )));
            }

            // Build the base primary key
            let mut pk_values = Vec::new();
            for pk_field in primary_key {
                let value = row.get(pk_field);
                if let Some(v) = value {
                    pk_values.push(v.to_string());
                } else {
                    return Err(MonoError::InvalidOperation(format!(
                        "primary key '{pk_field}' does not exist in '{collection}'"
                    )));
                }
            }
            let base_pk = pk_values.join(":").as_bytes().to_vec();

            // Create versioned key
            let versioned_key = encode_versioned_key(&base_pk, version_ts);

            // Build ordered row value
            let mut ordered_row: IndexMap<String, MonoValue> = IndexMap::new();
            for col in columns {
                if let Some(value) = row.get(&col.name) {
                    ordered_row.insert(col.name.clone(), value.clone());
                }
            }
            let value_to_store = MonoValue::Row(ordered_row);
            let value_bytes = value_to_store.to_bytes();

            // Store with versioned key
            if self.config.use_lsm {
                if let Some(lsm) = self.lsm_trees.get(collection) {
                    lsm.put(versioned_key, value_bytes).await?;
                } else {
                    return Err(MonoError::NotFound(format!(
                        "collection '{collection}' not found"
                    )));
                }
            } else if let Some(btree) = self.btrees.get(collection) {
                btree.insert(&versioned_key, &value_bytes).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }

            Ok(base_pk)
        } else {
            unreachable!()
        }
    }

    /// Insert a row within a transaction (MVCC).
    /// Uses tx_id as version, checks for conflicts.
    async fn insert_row_versioned(
        &self,
        collection: &str,
        row: &HashMap<String, MonoValue>,
        tx: &Transaction,
    ) -> Result<Vec<u8>> {
        let schema = self.schemas.get(collection).unwrap();
        if let Schema::Table {
            primary_key,
            columns,
            ..
        } = schema.value()
        {
            if primary_key.is_empty() {
                return Err(MonoError::InvalidOperation(format!(
                    "table '{collection}' has no primary key defined"
                )));
            }

            // Build the base primary key
            let mut pk_values = Vec::new();
            for pk_field in primary_key {
                let value = row.get(pk_field);
                if let Some(v) = value {
                    pk_values.push(v.to_string());
                } else {
                    return Err(MonoError::InvalidOperation(format!(
                        "primary key '{pk_field}' does not exist in '{collection}'"
                    )));
                }
            }
            let base_pk = pk_values.join(":").as_bytes().to_vec();

            // Check for write-write conflicts
            self.check_write_conflict(collection, &base_pk, tx).await?;

            // Create versioned key using tx_id
            let versioned_key = encode_versioned_key(&base_pk, tx.tx_id);

            // Add to transaction's write set
            self.tx_manager
                .add_to_write_set(tx.tx_id, collection.to_string(), base_pk.clone());

            // Build ordered row value
            let mut ordered_row: IndexMap<String, MonoValue> = IndexMap::new();
            for col in columns {
                if let Some(value) = row.get(&col.name) {
                    ordered_row.insert(col.name.clone(), value.clone());
                }
            }
            let value_to_store = MonoValue::Row(ordered_row);
            let value_bytes = value_to_store.to_bytes();

            // Store with versioned key
            if self.config.use_lsm {
                if let Some(lsm) = self.lsm_trees.get(collection) {
                    lsm.put(versioned_key, value_bytes).await?;
                } else {
                    return Err(MonoError::NotFound(format!(
                        "collection '{collection}' not found"
                    )));
                }
            } else if let Some(btree) = self.btrees.get(collection) {
                btree.insert(&versioned_key, &value_bytes).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }

            Ok(base_pk)
        } else {
            unreachable!()
        }
    }

    async fn insert_kv_typed(&self, collection: &str, key: &[u8], value: &MonoValue) -> Result<()> {
        let key_str = String::from_utf8_lossy(key).to_string();

        if let Some(schema) = self.schemas.get(collection)
            && let Schema::KeySpace {
                persistence: KeySpacePersistence::Memory,
                ..
            } = schema.value()
        {
            if let Some(memory_store) = self.memory_keyspaces.get(collection) {
                memory_store.insert(key_str.clone(), value.clone());
            } else {
                let memory_store = Arc::new(DashMap::new());
                memory_store.insert(key_str.clone(), value.clone());
                self.memory_keyspaces
                    .insert(collection.to_string(), memory_store);
            }
            return Ok(());
        }

        let key = key.to_vec();
        // Serialize
        let serialized_value = value.to_bytes();

        if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.put(key, serialized_value).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }
        } else if let Some(btree) = self.btrees.get(collection) {
            btree.insert(&key, &serialized_value).await?;
        } else {
            return Err(MonoError::NotFound(format!(
                "collection '{collection}' not found"
            )));
        }

        Ok(())
    }

    /// Fast-path insert for raw key/value bytes into a KeySpace collection.
    /// Skips bincode serialization and schema/type conversions.
    pub async fn insert_kv_raw(&self, collection: &str, key: &[u8], value: &[u8]) -> Result<()> {
        // Memory keyspaces store typed Values; wrap as Binary for compatibility
        if let Some(schema) = self.schemas.get(collection)
            && let Schema::KeySpace {
                persistence: KeySpacePersistence::Memory,
                ..
            } = schema.value()
        {
            let key_str = String::from_utf8_lossy(key).to_string();
            if let Some(memory_store) = self.memory_keyspaces.get(collection) {
                memory_store.insert(key_str.clone(), MonoValue::Binary(value.to_vec()));
            } else {
                let memory_store = Arc::new(DashMap::new());
                memory_store.insert(key_str.clone(), MonoValue::Binary(value.to_vec()));
                self.memory_keyspaces
                    .insert(collection.to_string(), memory_store);
            }
            return Ok(());
        }

        let key_vec = key.to_vec();
        let val_vec = value.to_vec();

        if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.put(key_vec, val_vec).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }
        } else if let Some(btree) = self.btrees.get(collection) {
            btree.insert(&key_vec, &val_vec).await?;
        } else {
            return Err(MonoError::NotFound(format!(
                "collection '{collection}' not found"
            )));
        }

        Ok(())
    }

    async fn insert_document(&self, collection: &str, doc: &MonoValue) -> Result<Vec<u8>> {
        let key = doc
            .as_object()
            .and_then(|o| {
                // For collections, _id is the primary identifier
                if let Some(id_val) = o.get("_id") {
                    // Handle various _id types
                    match id_val {
                        MonoValue::ObjectId(oid) => Some(oid.to_hex()),
                        MonoValue::String(s) => Some(s.clone()),
                        MonoValue::Int32(i) => Some(i.to_string()),
                        MonoValue::Int64(i) => Some(i.to_string()),
                        _ => Some(format!("{id_val}")),
                    }
                } else if let Some(id_val) = o.get("id") {
                    // Fallback to 'id' field
                    match id_val {
                        MonoValue::ObjectId(oid) => Some(oid.to_hex()),
                        MonoValue::String(s) => Some(s.clone()),
                        MonoValue::Int32(i) => Some(i.to_string()),
                        MonoValue::Int64(i) => Some(i.to_string()),
                        _ => Some(format!("{id_val}")),
                    }
                } else {
                    None
                }
            })
            .unwrap_or_else(|| ObjectId::new().unwrap().to_string());

        let key_bytes = key.as_bytes().to_vec();
        // Serialize
        let value_bytes = doc.to_bytes();

        if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.put(key_bytes.clone(), value_bytes).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }
        } else if let Some(btree) = self.btrees.get(collection) {
            btree.insert(&key_bytes, &value_bytes).await?;
        } else {
            return Err(MonoError::NotFound(format!(
                "collection '{collection}' not found"
            )));
        }

        Ok(key_bytes)
    }

    async fn insert_row(
        &self,
        collection: &str,
        row: &HashMap<String, MonoValue>,
    ) -> Result<Vec<u8>> {
        let schema = self.schemas.get(collection).unwrap();
        if let Schema::Table {
            primary_key,
            columns,
            ..
        } = schema.value()
        {
            // Validation already done in insert() method

            if primary_key.is_empty() {
                return Err(MonoError::InvalidOperation(format!(
                    "table '{collection}' has no primary key defined"
                )));
            }

            let mut pk_values = Vec::new();
            for pk_field in primary_key {
                let value = row.get(pk_field);
                if let Some(v) = value {
                    pk_values.push(v.to_string());
                } else {
                    return Err(MonoError::InvalidOperation(format!(
                        "primary key '{pk_field}' does not exist in '{collection}'"
                    )));
                }
            }
            let key_bytes = pk_values.join(":").as_bytes().to_vec();

            // Store as Value::Row (IndexMap) to preserve column order from schema
            let mut ordered_row: IndexMap<String, MonoValue> = IndexMap::new();
            for col in columns {
                if let Some(value) = row.get(&col.name) {
                    ordered_row.insert(col.name.clone(), value.clone());
                }
            }
            let value_to_store = MonoValue::Row(ordered_row);
            // Serialize
            let value_bytes = value_to_store.to_bytes();

            if self.config.use_lsm {
                if let Some(lsm) = self.lsm_trees.get(collection) {
                    lsm.put(key_bytes.clone(), value_bytes).await?;
                } else {
                    return Err(MonoError::NotFound(format!(
                        "collection '{collection}' not found"
                    )));
                }
            } else if let Some(btree) = self.btrees.get(collection) {
                btree.insert(&key_bytes, &value_bytes).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }

            Ok(key_bytes)
        } else {
            unreachable!()
        }
    }

    /// Persist schemas to disk atomically using storage-format.md §3
    async fn persist_schemas(&self) -> Result<()> {
        let schemas_file = Path::new(&self.config.data_dir).join("schemas.bin");

        let schemas_map: HashMap<String, Schema> = self
            .schemas
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        // Serialize
        let serialized = Self::serialize_schemas(&schemas_map)?;

        let tmp = schemas_file.with_extension("tmp");
        tokio::fs::write(&tmp, &serialized)
            .await
            .map_err(|e| MonoError::Io(e.to_string()))?;
        tokio::fs::rename(&tmp, &schemas_file)
            .await
            .map_err(|e| MonoError::Io(e.to_string()))?;

        debug!(
            "Persisted {} schemas ({} bytes) to {:?}",
            schemas_map.len(),
            serialized.len(),
            schemas_file
        );
        Ok(())
    }

    /// Load schema metadata from disk and rebuild indexes
    async fn load_schemas(&self) -> Result<()> {
        let start = std::time::Instant::now();
        let schemas_file = Path::new(&self.config.data_dir).join("schemas.bin");

        if !schemas_file.exists() {
            tracing::info!("No schemas file found, starting with empty schema set");
            return Ok(());
        }

        tracing::info!("Loading schemas...");
        let data = tokio::fs::read(&schemas_file)
            .await
            .map_err(|e| MonoError::Io(e.to_string()))?;

        // Deserialize
        let schemas: HashMap<String, Schema> = Self::deserialize_schemas(&data)?;

        let mut loaded_count = 0;
        for (name, schema) in schemas {
            self.schemas.insert(name.clone(), schema.clone());
            self.create_storage_structures_for_schema(&name, &schema)
                .await?;
            self.initialize_indexes(&name, &schema).await?;
            loaded_count += 1;
        }

        tracing::info!(
            "Loaded {} schemas from disk ({} bytes) in {:?}",
            loaded_count,
            data.len(),
            start.elapsed()
        );

        // Rebuild indexes by scanning collections
        self.rebuild_all_indexes().await?;

        Ok(())
    }

    /// Rebuild all secondary indexes by scanning collections
    async fn rebuild_all_indexes(&self) -> Result<()> {
        info!("Rebuilding secondary indexes...");

        for schema_entry in self.schemas.iter() {
            let collection_name = schema_entry.key();
            let schema = schema_entry.value();

            let indexes = match schema {
                Schema::Table { indexes, .. } => indexes,
                Schema::Collection { indexes, .. } => indexes,
                Schema::KeySpace { .. } => continue,
            };

            if indexes.is_empty() {
                continue;
            }

            debug!("Rebuilding indexes for collection '{}'", collection_name);

            // Scan all entries in the collection
            let query = crate::storage::models::Query::default();
            let entries = self.find(collection_name, query).await?;

            // Rebuild each index
            for entry in entries {
                if let MonoValue::Object(obj) = &entry {
                    // Extract primary key
                    let pk = obj
                        .get("_key")
                        .or_else(|| obj.get("key"))
                        .and_then(|v| v.as_string())
                        .ok_or_else(|| {
                            MonoError::Storage(
                                "Cannot rebuild index: missing primary key".to_string(),
                            )
                        })?;

                    // Update indexes for this entry
                    let data_for_index = if let Schema::Table { .. } = schema {
                        // Convert object back to row format
                        let row: HashMap<String, MonoValue> =
                            obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        Data::Row(row)
                    } else {
                        Data::Document(entry.clone())
                    };

                    self.update_secondary_indexes(collection_name, &data_for_index, pk.as_bytes())
                        .await?;
                }
            }

            debug!("Rebuilt indexes for collection '{}'", collection_name);
        }

        info!("Finished rebuilding secondary indexes");
        Ok(())
    }

    // Helper methods for storage structures

    #[inline(always)]
    fn create_lsm_tree(&self, collection_name: &str) -> Result<Arc<LsmTree>> {
        let lsm_config = crate::config::LsmConfig {
            memtable_size: self.config.lsm.memtable_size,
            level0_file_num_compaction_trigger: self.config.lsm.level0_file_num_compaction_trigger,
            level0_size: self.config.lsm.level0_size,
            level_multiplier: self.config.lsm.level_multiplier,
            compression: self.config.lsm.compression,
            max_level: self.config.lsm.max_level,
        };

        let collection_path = std::path::Path::new(&self.config.data_dir).join(collection_name);
        Ok(Arc::new(crate::storage::lsm::LsmTree::new(
            collection_path,
            lsm_config,
            self.config.wal.clone(),
        )?))
    }

    #[inline(always)]
    fn create_btree(&self, collection_name: &str) -> Result<Arc<BTree>> {
        let collection_dir = std::path::Path::new(&self.config.data_dir).join(collection_name);
        std::fs::create_dir_all(&collection_dir)?;

        let dm = Arc::new(DiskManager::new(&collection_dir)?);
        let bp = Arc::new(BufferPool::new(
            self.config.buffer_pool_size,
            Arc::clone(&dm),
        ));

        let wal_path = collection_dir.join("wal.log");
        let wal = Wal::with_config(&wal_path, self.config.wal.clone())?;

        let btree = BTree::new(collection_name.to_string(), Arc::clone(&bp))
            .with_wal(Arc::new(RwLock::new(wal)));

        Ok(Arc::new(btree))
    }

    #[allow(dead_code)]
    async fn recover_btree_from_wal(
        &self,
        collection_name: &str,
        btree: &Arc<BTree>,
    ) -> Result<()> {
        let wal_path = std::path::Path::new(&self.config.data_dir)
            .join(collection_name)
            .join("wal.log");
        let entries = crate::storage::wal::Wal::replay(&wal_path)?;
        tracing::info!(
            "Recovering B-Tree '{}' from WAL: {} entries",
            collection_name,
            entries.len()
        );

        let mut processed = 0usize;
        for entry in entries {
            use crate::storage::wal::WalEntryType;
            match entry.entry_type {
                WalEntryType::Insert | WalEntryType::Update => {
                    btree.insert_no_wal(&entry.key, &entry.value).await?;
                }
                WalEntryType::Delete => {
                    btree.delete_no_wal(&entry.key).await?;
                }
                WalEntryType::Checkpoint => {}
                WalEntryType::TxBegin | WalEntryType::TxCommit | WalEntryType::TxRollback => {
                    // Transaction markers are not applied during recovery
                }
            }
            processed += 1;
            if processed.is_multiple_of(100) {
                tracing::info!(
                    "Recovering '{}' from WAL: processed {} entries",
                    collection_name,
                    processed
                );
            }
        }
        tracing::info!(
            "Finished recovering '{}' from WAL: processed {} entries",
            collection_name,
            processed
        );

        btree.flush().await?;
        Ok(())
    }

    async fn create_storage_structures_for_schema(
        &self,
        collection_name: &str,
        schema: &Schema,
    ) -> Result<()> {
        match schema {
            Schema::KeySpace {
                persistence: KeySpacePersistence::Memory,
                ..
            } => {
                let memory_store = Arc::new(DashMap::new());
                self.memory_keyspaces
                    .insert(collection_name.to_string(), memory_store);
                tracing::debug!("Created in-memory keyspace: {}", collection_name);
            }
            Schema::KeySpace {
                persistence: KeySpacePersistence::Persistent,
                ..
            }
            | Schema::Collection { .. }
            | Schema::Table { .. } => {
                if self.config.use_lsm {
                    let lsm_tree = self.create_lsm_tree(collection_name)?;
                    self.lsm_trees.insert(collection_name.to_string(), lsm_tree);
                    tracing::debug!("Created LSM tree: {}", collection_name);
                } else {
                    let btree = self.create_btree(collection_name)?;
                    self.btrees.insert(collection_name.to_string(), btree);
                    tracing::debug!("Created B-Tree: {}", collection_name);
                }
            }
        }
        Ok(())
    }

    pub async fn find(
        &self,
        collection_name: &str,
        query: crate::storage::models::Query,
    ) -> Result<Vec<MonoValue>> {
        self.find_with_tx(collection_name, query, None).await
    }

    /// Find with optional transaction context for MVCC visibility.
    pub async fn find_with_tx(
        &self,
        collection_name: &str,
        query: crate::storage::models::Query,
        tx: Option<&Transaction>,
    ) -> Result<Vec<MonoValue>> {
        if !self.schemas.contains_key(collection_name) {
            return Err(MonoError::NotFound(format!(
                "collection '{collection_name}' not found"
            )));
        }

        let schema = self.schemas.get(collection_name).unwrap();
        match schema.value() {
            Schema::KeySpace { .. } => self.find_kv(collection_name, query).await,
            Schema::Collection { .. } | Schema::Table { .. } => {
                self.find_with_filter_and_tx(collection_name, query, tx)
                    .await
            }
        }
    }

    async fn find_kv(
        &self,
        collection: &str,
        query: crate::storage::models::Query,
    ) -> Result<Vec<MonoValue>> {
        use crate::storage::models::Filter;

        if let Some(schema) = self.schemas.get(collection)
            && let Schema::KeySpace {
                persistence: monodb_common::schema::KeySpacePersistence::Memory,
                ..
            } = schema.value()
        {
            return self.find_memory_kv(collection, query).await;
        }

        let namespace_prefix = query
            .filter
            .as_ref()
            .and_then(|f| match f {
                Filter::Eq(field, v) if field == "key" => v.as_string().map(|s| s.to_string()),
                _ => None,
            })
            .and_then(|s| {
                if s.ends_with(':') {
                    Some(s.trim_end_matches(':').to_string())
                } else {
                    None
                }
            });

        let pairs: Vec<(Vec<u8>, Vec<u8>)> = if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                if let Some(prefix) = namespace_prefix {
                    let start = format!("{prefix}:").into_bytes();
                    let mut end = start.clone();
                    end.push(0xFF);
                    lsm.scan(&start, &end).await?
                } else {
                    lsm.scan(&[], &[0xFF]).await?
                }
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection {collection} not found"
                )));
            }
        } else if let Some(btree) = self.btrees.get(collection) {
            if let Some(prefix) = namespace_prefix {
                let start = format!("{prefix}:").into_bytes();
                let mut end = start.clone();
                end.extend(std::iter::repeat_n(0xFFu8, 64));
                btree.scan(&start, &end).await?
            } else {
                let end = vec![0xFFu8; 64];
                btree.scan(&[], &end).await?
            }
        } else {
            return Err(MonoError::NotFound(format!(
                "collection {collection} not found"
            )));
        };

        let mut values: Vec<MonoValue> = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            let key_str = String::from_utf8_lossy(&k).to_string();
            // Deserialize
            match MonoValue::from_bytes(&v) {
                Ok((mut mv, _bytes_read)) => {
                    if let MonoValue::Object(ref mut obj) = mv {
                        obj.insert("_key".to_string(), MonoValue::String(key_str.clone()));
                        obj.insert("key".to_string(), MonoValue::String(key_str));
                    } else {
                        let mut map = BTreeMap::new();
                        map.insert("_key".to_string(), MonoValue::String(key_str.clone()));
                        map.insert("key".to_string(), MonoValue::String(key_str));
                        map.insert("_value".to_string(), mv);
                        mv = MonoValue::Object(map);
                    }
                    values.push(mv);
                }
                Err(e) => {
                    tracing::warn!("failed to deserialize value for key {:?}: {}", key_str, e);
                }
            }
        }

        let filtered = if let Some(filter) = &query.filter {
            match filter {
                crate::storage::models::Filter::Eq(field, MonoValue::String(s))
                    if field == "key" && s.ends_with(':') =>
                {
                    values
                }
                _ => values
                    .into_iter()
                    .filter(|v| self.apply_filter(v, filter))
                    .collect(),
            }
        } else {
            values
        };

        let mut out = filtered;
        if let Some(limit) = query.limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    async fn find_memory_kv(
        &self,
        collection: &str,
        query: crate::storage::models::Query,
    ) -> Result<Vec<MonoValue>> {
        use crate::storage::models::Filter;

        let store = self.memory_keyspaces.get(collection).ok_or_else(|| {
            MonoError::NotFound(format!("memory keyspace {collection} not found"))
        })?;

        let namespace_prefix = query
            .filter
            .as_ref()
            .and_then(|f| match f {
                Filter::Eq(field, v) if field == "key" => v.as_string().map(|s| s.to_string()),
                _ => None,
            })
            .and_then(|s| {
                if s.ends_with(':') {
                    Some(s.trim_end_matches(':').to_string())
                } else {
                    None
                }
            });

        let mut values: Vec<MonoValue> = store
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                if let Some(ref prefix) = namespace_prefix
                    && !key.starts_with(&format!("{prefix}:"))
                {
                    return None;
                }
                let value = entry.value();
                let obj = if let MonoValue::Object(map) = value.clone() {
                    let mut m = map;
                    m.insert("_key".to_string(), MonoValue::String(key.clone()));
                    m.insert("key".to_string(), MonoValue::String(key.clone()));
                    MonoValue::Object(m)
                } else {
                    let mut map = BTreeMap::new();
                    map.insert("_key".to_string(), MonoValue::String(key.clone()));
                    map.insert("key".to_string(), MonoValue::String(key.clone()));
                    map.insert("_value".to_string(), value.clone());
                    MonoValue::Object(map)
                };
                Some(obj)
            })
            .collect();

        values.sort_by(|a, b| {
            let ka = a
                .as_object()
                .and_then(|o| o.get("_key"))
                .and_then(|v| v.as_string())
                .map(|s| s.as_str())
                .unwrap_or("");
            let kb = b
                .as_object()
                .and_then(|o| o.get("_key"))
                .and_then(|v| v.as_string())
                .map(|s| s.as_str())
                .unwrap_or("");
            ka.cmp(kb)
        });

        if let Some(limit) = query.limit {
            values.truncate(limit);
        }
        Ok(values)
    }

    async fn find_with_filter(
        &self,
        collection: &str,
        query: crate::storage::models::Query,
    ) -> Result<Vec<MonoValue>> {
        self.find_with_filter_and_tx(collection, query, None).await
    }

    /// Find with optional transaction context for MVCC visibility.
    /// For relational tables, only returns versions visible to the transaction.
    pub async fn find_with_filter_and_tx(
        &self,
        collection: &str,
        query: crate::storage::models::Query,
        tx: Option<&Transaction>,
    ) -> Result<Vec<MonoValue>> {
        let schema = self.schemas.get(collection);
        let is_relational = matches!(
            schema.as_ref().map(|s| s.value()),
            Some(Schema::Table { .. })
        );

        // Scan all key-value pairs
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.scan(&[], &[0xFF]).await?
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection {collection} not found"
                )));
            }
        } else if let Some(btree) = self.btrees.get(collection) {
            let end = vec![0xFFu8; 64];
            btree.scan(&[], &end).await?
        } else {
            return Err(MonoError::NotFound(format!(
                "collection {collection} not found"
            )));
        };

        let mut values: Vec<MonoValue> = Vec::with_capacity(pairs.len());

        if is_relational {
            // MVCC visibility filtering for relational tables
            // Group by primary key and pick the first visible version
            let mut seen_pks: HashSet<Vec<u8>> = HashSet::new();

            for (key, value) in pairs {
                // Try to decode as versioned key
                if let Some((pk, version_ts)) = decode_versioned_key(&key) {
                    // Check if we've already found a visible version for this PK
                    if seen_pks.contains(&pk) {
                        continue;
                    }

                    // Check visibility
                    let visible = if let Some(tx) = tx {
                        self.is_version_visible(version_ts, tx)
                    } else {
                        self.is_version_visible_no_tx(version_ts)
                    };

                    if visible {
                        // Deserialize and add
                        match MonoValue::from_bytes(&value) {
                            Ok((mv, _)) => {
                                values.push(mv);
                                seen_pks.insert(pk);
                            }
                            Err(e) => {
                                tracing::warn!("failed to deserialize value: {}", e);
                            }
                        }
                    }
                } else {
                    // Non-versioned key (legacy data) - always visible
                    match MonoValue::from_bytes(&value) {
                        Ok((mv, _)) => {
                            values.push(mv);
                        }
                        Err(e) => {
                            tracing::warn!("failed to deserialize value: {}", e);
                        }
                    }
                }
            }
        } else {
            // Non-relational: no MVCC, return all values
            for (_k, v) in pairs {
                match MonoValue::from_bytes(&v) {
                    Ok((mv, _bytes_read)) => {
                        values.push(mv);
                    }
                    Err(e) => {
                        tracing::warn!("failed to deserialize value: {}", e);
                    }
                }
            }
        }

        let mut filtered = if let Some(filter) = &query.filter {
            values
                .into_iter()
                .filter(|v| self.apply_filter(v, filter))
                .collect()
        } else {
            values
        };

        if let Some(limit) = query.limit {
            filtered.truncate(limit);
        }
        Ok(filtered)
    }

    /// Compare two values for ordering, handling numeric types properly
    fn compare_values(a: &MonoValue, b: &MonoValue) -> std::cmp::Ordering {
        use MonoValue::*;
        match (a, b) {
            // Numeric comparisons
            (Int32(x), Int32(y)) => x.cmp(y),
            (Int64(x), Int64(y)) => x.cmp(y),
            (Float32(x), Float32(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Float64(x), Float64(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),

            // Cross-numeric comparisons (promote to f64)
            (Int32(x), Int64(y)) => (*x as i64).cmp(y),
            (Int64(x), Int32(y)) => x.cmp(&(*y as i64)),
            (Int32(x), Float64(y)) => (*x as f64)
                .partial_cmp(y)
                .unwrap_or(std::cmp::Ordering::Equal),
            (Float64(x), Int32(y)) => x
                .partial_cmp(&(*y as f64))
                .unwrap_or(std::cmp::Ordering::Equal),
            (Int64(x), Float64(y)) => (*x as f64)
                .partial_cmp(y)
                .unwrap_or(std::cmp::Ordering::Equal),
            (Float64(x), Int64(y)) => x
                .partial_cmp(&(*y as f64))
                .unwrap_or(std::cmp::Ordering::Equal),

            // String comparison
            (String(x), String(y)) => x.cmp(y),

            // Fallback to string comparison for mixed types
            _ => a.to_string().cmp(&b.to_string()),
        }
    }

    /// Get a field value from either Value::Row or Value::Object
    fn get_field<'a>(value: &'a MonoValue, field: &str) -> Option<&'a MonoValue> {
        match value {
            MonoValue::Row(row) => row.get(field),
            MonoValue::Object(obj) => obj.get(field),
            _ => None,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn apply_filter(&self, row: &MonoValue, filter: &crate::storage::models::Filter) -> bool {
        use crate::storage::models::Filter as F;
        match filter {
            F::Eq(field, v) => Self::get_field(row, field) == Some(v),
            F::Neq(field, v) => Self::get_field(row, field).is_some_and(|x| x != v),
            F::Gt(field, v) => Self::get_field(row, field)
                .is_some_and(|x| Self::compare_values(x, v) == std::cmp::Ordering::Greater),
            F::Gte(field, v) => Self::get_field(row, field).is_some_and(|x| {
                matches!(
                    Self::compare_values(x, v),
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                )
            }),
            F::Lt(field, v) => Self::get_field(row, field)
                .is_some_and(|x| Self::compare_values(x, v) == std::cmp::Ordering::Less),
            F::Lte(field, v) => Self::get_field(row, field).is_some_and(|x| {
                matches!(
                    Self::compare_values(x, v),
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                )
            }),
            F::Contains(field, v) => Self::get_field(row, field).is_some_and(|x| match x {
                MonoValue::Array(a) => a.iter().any(|e| e == v),
                MonoValue::String(s) => v.as_string().map(|pat| s.contains(pat)).unwrap_or(false),
                other => other.to_string().contains(&v.to_string()),
            }),
            F::And(list) => list.iter().all(|f| self.apply_filter(row, f)),
            F::Or(list) => list.iter().any(|f| self.apply_filter(row, f)),
        }
    }

    pub async fn drop_collection(&self, collection_name: &str) -> Result<()> {
        if self.schemas.remove(collection_name).is_none() {
            return Err(MonoError::NotFound(format!(
                "collection {collection_name} not found"
            )));
        }

        if self.config.use_lsm {
            let _ = self.lsm_trees.remove(collection_name);
        } else {
            let _ = self.btrees.remove(collection_name);
        }

        // Remove secondary indexes
        self.secondary_indexes.remove(collection_name);

        self.persist_schemas().await?;
        Ok(())
    }

    pub async fn list_collections(&self) -> Result<Vec<String>> {
        let v = self
            .schemas
            .iter()
            .filter_map(|e| {
                if matches!(e.value(), Schema::Collection { .. }) {
                    Some(e.key().clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(v)
    }

    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let v = self
            .schemas
            .iter()
            .filter_map(|e| {
                if matches!(e.value(), Schema::Table { .. }) {
                    Some(e.key().clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(v)
    }

    pub async fn list_keyspaces(&self) -> Result<Vec<String>> {
        let v = self
            .schemas
            .iter()
            .filter_map(|e| {
                if matches!(e.value(), Schema::KeySpace { .. }) {
                    Some(e.key().clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(v)
    }

    pub async fn flush(&self) -> Result<()> {
        info!("Flushing storage engine...");
        if let Some(bp) = &self.buffer_pool {
            bp.flush_all()?;
        }
        for e in self.btrees.iter() {
            e.value().flush().await?;
        }
        for e in self.lsm_trees.iter() {
            e.value().flush().await?;
        }
        self.persist_schemas().await?;
        Ok(())
    }

    pub async fn checkpoint_all(&self) -> Result<Vec<(String, u64)>> {
        let mut out = Vec::new();
        for e in self.lsm_trees.iter() {
            let seq = e.value().checkpoint().await?;
            out.push((e.key().clone(), seq));
        }
        Ok(out)
    }

    pub async fn checkpoint_collection(&self, collection_name: &str) -> Result<u64> {
        if let Some(lsm) = self.lsm_trees.get(collection_name) {
            lsm.checkpoint().await
        } else {
            Err(MonoError::NotFound(format!(
                "collection '{collection_name}' not found or not LSM-backed"
            )))
        }
    }

    pub fn get_wal_stats(&self, collection_name: &str) -> Result<crate::storage::wal::WalStats> {
        if let Some(lsm) = self.lsm_trees.get(collection_name) {
            Ok(lsm.wal_stats())
        } else {
            Err(MonoError::NotFound(format!(
                "collection '{collection_name}' not found or not LSM-backed"
            )))
        }
    }

    pub async fn sync_wal(&self, collection_name: &str) -> Result<()> {
        if let Some(lsm) = self.lsm_trees.get(collection_name) {
            lsm.sync_wal().await
        } else {
            Err(MonoError::NotFound(format!(
                "collection '{collection_name}' not found or not LSM-backed"
            )))
        }
    }

    /// Commit barrier: wait until current WAL writes for a collection are durable.
    /// Only effective when WAL async mode is enabled; returns Ok(false) on timeout.
    pub async fn wal_commit_current(
        &self,
        collection_name: &str,
        timeout_ms: Option<u64>,
    ) -> Result<bool> {
        if let Some(lsm) = self.lsm_trees.get(collection_name) {
            let to = timeout_ms.map(std::time::Duration::from_millis);
            lsm.wal_commit_current(to).await
        } else {
            Err(MonoError::NotFound(format!(
                "collection '{collection_name}' not found or not LSM-backed"
            )))
        }
    }

    pub async fn maintenance(&self) -> Result<()> {
        info!("Running storage maintenance");
        for e in self.lsm_trees.iter() {
            e.value().auto_checkpoint_if_needed().await?;
            let stats = e.value().wal_stats();
            debug!(
                "collection '{}' WAL size {} next_seq {} last_checkpoint {:?}",
                e.key(),
                stats.current_size,
                stats.next_sequence,
                stats.last_checkpoint_sequence
            );
        }
        Ok(())
    }

    pub fn schemas(&self) -> Vec<Arc<Schema>> {
        self.schemas
            .iter()
            .map(|entry| Arc::new(entry.value().clone()))
            .collect()
    }

    pub fn schema(&self, collection: &str) -> Option<Arc<Schema>> {
        self.schemas
            .get(collection)
            .map(|entry| Arc::new(entry.clone()))
    }

    /// Encode a composite key
    ///
    /// CompositeKey format:
    ///   [component_count : u8]
    ///   [Component 0][Component 1]...[Component {component_count-1}]
    ///
    /// Each Component is encoded using Value::to_bytes()
    fn encode_composite_key(values: &[MonoValue]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(values.len() as u8);
        for value in values {
            bytes.extend(value.to_bytes());
        }
        bytes
    }

    /// Decode a composite key
    #[allow(dead_code)]
    fn decode_composite_key(buf: &[u8]) -> Result<Vec<MonoValue>> {
        if buf.is_empty() {
            return Err(MonoError::Parse("Empty composite key buffer".into()));
        }

        let count = buf[0] as usize;
        let mut offset = 1;
        let mut values = Vec::with_capacity(count);

        for _ in 0..count {
            let (value, used) = MonoValue::from_bytes(&buf[offset..])?;
            offset += used;
            values.push(value);
        }

        Ok(values)
    }

    /// Serialize schemas
    ///
    /// File Layout:
    ///   - Header (16 bytes)
    ///   - Schema Entries ([count: u32][Schema 0][Schema 1]...)
    ///   - Footer (8 bytes)
    fn serialize_schemas(schemas: &HashMap<String, Schema>) -> Result<Vec<u8>> {
        use crc32c::crc32c;

        let mut bytes = Vec::new();

        // §3.2 Header (16 bytes)
        // MAGIC: 'SCHM' (0x5343484D)
        bytes.extend(&0x5343484Du32.to_le_bytes());
        // VERSION: u16 = 1
        bytes.extend(&1u16.to_le_bytes());
        // FLAGS: u16 = 0 (reserved)
        bytes.extend(&0u16.to_le_bytes());
        // SCHEMA_COUNT: u32
        bytes.extend(&(schemas.len() as u32).to_le_bytes());
        // RESERVED: u32 = 0
        bytes.extend(&0u32.to_le_bytes());

        // §3.3 Schema Entries
        for schema in schemas.values() {
            Self::serialize_schema_entry(&mut bytes, schema)?;
        }

        // §3.4 Footer (8 bytes)
        // CRC32 of all preceding bytes
        let crc = crc32c(&bytes);
        bytes.extend(&crc.to_le_bytes());
        // MAGIC: 'SCMF' (0x53434D46)
        bytes.extend(&0x53434D46u32.to_le_bytes());

        Ok(bytes)
    }

    /// Serialize a single schema entry according to storage-format.md §3.3
    fn serialize_schema_entry(bytes: &mut Vec<u8>, schema: &Schema) -> Result<()> {
        match schema {
            Schema::Table {
                name,
                columns,
                primary_key,
                indexes,
            } => {
                // schema_type: u8 = 0 (Table)
                bytes.push(0);
                // name: String
                Self::write_string(bytes, name);

                // §3.3.1 Table Schema Payload
                // column_count: u32
                bytes.extend(&(columns.len() as u32).to_le_bytes());
                // Columns
                for col in columns {
                    Self::serialize_column(bytes, col)?;
                }
                // pk_count: u32
                bytes.extend(&(primary_key.len() as u32).to_le_bytes());
                // pk_column_names: String (repeated)
                for pk in primary_key {
                    Self::write_string(bytes, pk);
                }
                // index_count: u32
                bytes.extend(&(indexes.len() as u32).to_le_bytes());
                // Indexes
                for idx in indexes {
                    Self::serialize_index(bytes, idx)?;
                }
            }
            Schema::Collection {
                name,
                validation,
                indexes,
            } => {
                // schema_type: u8 = 1 (Collection)
                bytes.push(1);
                // name: String
                Self::write_string(bytes, name);

                // §3.3.4 Collection Schema Payload
                // has_validation: u8
                if let Some(rule) = validation {
                    bytes.push(1);
                    Self::serialize_validation_rule(bytes, rule)?;
                } else {
                    bytes.push(0);
                }
                // index_count: u32
                bytes.extend(&(indexes.len() as u32).to_le_bytes());
                // Indexes
                for idx in indexes {
                    Self::serialize_index(bytes, idx)?;
                }
            }
            Schema::KeySpace {
                name,
                ttl_enabled,
                max_size,
                persistence,
            } => {
                // schema_type: u8 = 2 (KeySpace)
                bytes.push(2);
                // name: String
                Self::write_string(bytes, name);

                // §3.3.6 KeySpace Schema Payload
                // flags: u8 - bit 0: ttl_enabled, bit 1: persistent
                let mut flags: u8 = 0;
                if *ttl_enabled {
                    flags |= 0b0000_0001;
                }
                if matches!(persistence, KeySpacePersistence::Persistent) {
                    flags |= 0b0000_0010;
                }
                bytes.push(flags);

                // has_max_size: u8
                if let Some(size) = max_size {
                    bytes.push(1);
                    bytes.extend(&(*size as u64).to_le_bytes());
                } else {
                    bytes.push(0);
                }
            }
        }
        Ok(())
    }

    /// Serialize a column definition
    fn serialize_column(
        bytes: &mut Vec<u8>,
        col: &monodb_common::schema::TableColumn,
    ) -> Result<()> {
        use monodb_common::ValueType;

        // name: String
        Self::write_string(bytes, &col.name);

        // type: u8
        let type_tag: u8 = match col.data_type {
            ValueType::Null => 0,
            ValueType::Bool => 1,
            ValueType::Int32 => 2,
            ValueType::Int64 => 3,
            ValueType::Float32 => 4,
            ValueType::Float64 => 5,
            ValueType::String => 6,
            ValueType::Binary => 7,
            ValueType::DateTime => 8,
            ValueType::Date => 9,
            ValueType::Time => 10,
            ValueType::Uuid => 11,
            ValueType::ObjectId => 12,
            ValueType::Array => 13,
            ValueType::Object => 14,
            ValueType::Set => 15,
            ValueType::Row => 16,
            ValueType::SortedSet => 17,
            ValueType::GeoPoint => 18,
            ValueType::Reference => 19,
        };
        bytes.push(type_tag);

        // flags: u8 - bit 0: nullable, bit 1: is_primary, bit 2: is_unique
        let mut flags: u8 = 0;
        if col.nullable {
            flags |= 0b0000_0001;
        }
        if col.is_primary {
            flags |= 0b0000_0010;
        }
        if col.is_unique {
            flags |= 0b0000_0100;
        }
        bytes.push(flags);

        // has_default: u8, default: Value (if has_default == 1)
        if let Some(default) = &col.default {
            bytes.push(1);
            bytes.extend(default.to_bytes());
        } else {
            bytes.push(0);
        }

        Ok(())
    }

    /// Serialize an index definition
    fn serialize_index(bytes: &mut Vec<u8>, idx: &monodb_common::schema::Index) -> Result<()> {
        use monodb_common::schema::IndexType;

        // name: String
        Self::write_string(bytes, &idx.name);
        // column_count: u32
        bytes.extend(&(idx.columns.len() as u32).to_le_bytes());
        // column_names: String (repeated)
        for col in &idx.columns {
            Self::write_string(bytes, col);
        }
        // flags: u8 - bit 0: unique
        let flags: u8 = if idx.unique { 0b0000_0001 } else { 0 };
        bytes.push(flags);
        // index_type: u8
        let type_tag: u8 = match idx.index_type {
            IndexType::BTree => 0,
            IndexType::Hash => 1,
            IndexType::FullText => 2,
            IndexType::Spatial => 3,
        };
        bytes.push(type_tag);

        Ok(())
    }

    /// Serialize a validation rule
    fn serialize_validation_rule(
        bytes: &mut Vec<u8>,
        rule: &monodb_common::schema::ValidationRule,
    ) -> Result<()> {
        use monodb_common::schema::ValidationRule;

        match rule {
            ValidationRule::JsonSchema(schema_str) => {
                bytes.push(0); // rule_type: 0 = JSONSchema
                Self::write_string(bytes, schema_str);
            }
            ValidationRule::Custom(rule_str) => {
                bytes.push(1); // rule_type: 1 = Custom
                Self::write_string(bytes, rule_str);
            }
        }
        Ok(())
    }

    /// Deserialize schemas
    fn deserialize_schemas(data: &[u8]) -> Result<HashMap<String, Schema>> {
        use crc32c::crc32c;

        if data.len() < 24 {
            // Minimum: 16 (header) + 8 (footer)
            return Err(MonoError::Parse("Schema file too short".into()));
        }

        let mut offset = 0;

        // §3.2 Header (16 bytes)
        let magic = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if magic != 0x5343484D {
            return Err(MonoError::Parse(format!(
                "Invalid schema magic: expected 0x5343484D, got 0x{:08X}",
                magic
            )));
        }

        let version = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if version != 1 {
            return Err(MonoError::Parse(format!(
                "Unsupported schema version: {}",
                version
            )));
        }

        let _flags = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let schema_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let _reserved = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // §3.4 Footer verification
        let footer_start = data.len() - 8;
        let stored_crc =
            u32::from_le_bytes(data[footer_start..footer_start + 4].try_into().unwrap());
        let footer_magic =
            u32::from_le_bytes(data[footer_start + 4..footer_start + 8].try_into().unwrap());

        if footer_magic != 0x53434D46 {
            return Err(MonoError::Parse(format!(
                "Invalid schema footer magic: expected 0x53434D46, got 0x{:08X}",
                footer_magic
            )));
        }

        let computed_crc = crc32c(&data[..footer_start]);
        if computed_crc != stored_crc {
            return Err(MonoError::Parse(format!(
                "Schema CRC mismatch: expected 0x{:08X}, got 0x{:08X}",
                stored_crc, computed_crc
            )));
        }

        // §3.3 Schema Entries
        let mut schemas = HashMap::new();
        for _ in 0..schema_count {
            let (schema, used) = Self::deserialize_schema_entry(&data[offset..footer_start])?;
            offset += used;
            let name = match &schema {
                Schema::Table { name, .. }
                | Schema::Collection { name, .. }
                | Schema::KeySpace { name, .. } => name.clone(),
            };
            schemas.insert(name, schema);
        }

        Ok(schemas)
    }

    /// Deserialize a single schema entry
    fn deserialize_schema_entry(data: &[u8]) -> Result<(Schema, usize)> {
        if data.is_empty() {
            return Err(MonoError::Parse("Empty schema entry".into()));
        }

        let mut offset = 0;
        let schema_type = data[offset];
        offset += 1;

        let (name, used) = Self::read_string(&data[offset..])?;
        offset += used;

        let schema = match schema_type {
            0 => {
                // Table
                let column_count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut columns = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    let (col, used) = Self::deserialize_column(&data[offset..])?;
                    offset += used;
                    columns.push(col);
                }

                let pk_count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut primary_key = Vec::with_capacity(pk_count);
                for _ in 0..pk_count {
                    let (pk, used) = Self::read_string(&data[offset..])?;
                    offset += used;
                    primary_key.push(pk);
                }

                let index_count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut indexes = Vec::with_capacity(index_count);
                for _ in 0..index_count {
                    let (idx, used) = Self::deserialize_index(&data[offset..])?;
                    offset += used;
                    indexes.push(idx);
                }

                Schema::Table {
                    name,
                    columns,
                    primary_key,
                    indexes,
                }
            }
            1 => {
                // Collection
                let has_validation = data[offset];
                offset += 1;

                let validation = if has_validation == 1 {
                    let (rule, used) = Self::deserialize_validation_rule(&data[offset..])?;
                    offset += used;
                    Some(rule)
                } else {
                    None
                };

                let index_count =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut indexes = Vec::with_capacity(index_count);
                for _ in 0..index_count {
                    let (idx, used) = Self::deserialize_index(&data[offset..])?;
                    offset += used;
                    indexes.push(idx);
                }

                Schema::Collection {
                    name,
                    validation,
                    indexes,
                }
            }
            2 => {
                // KeySpace
                let flags = data[offset];
                offset += 1;

                let ttl_enabled = (flags & 0b0000_0001) != 0;
                let is_persistent = (flags & 0b0000_0010) != 0;

                let has_max_size = data[offset];
                offset += 1;

                let max_size = if has_max_size == 1 {
                    let size = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    Some(size as usize)
                } else {
                    None
                };

                let persistence = if is_persistent {
                    KeySpacePersistence::Persistent
                } else {
                    KeySpacePersistence::Memory
                };

                Schema::KeySpace {
                    name,
                    ttl_enabled,
                    max_size,
                    persistence,
                }
            }
            _ => {
                return Err(MonoError::Parse(format!(
                    "Unknown schema type: {}",
                    schema_type
                )));
            }
        };

        Ok((schema, offset))
    }

    /// Deserialize a column definition
    fn deserialize_column(data: &[u8]) -> Result<(monodb_common::schema::TableColumn, usize)> {
        use monodb_common::ValueType;

        let mut offset = 0;

        let (name, used) = Self::read_string(&data[offset..])?;
        offset += used;

        let type_tag = data[offset];
        offset += 1;

        let data_type = match type_tag {
            0 => ValueType::Null,
            1 => ValueType::Bool,
            2 => ValueType::Int32,
            3 => ValueType::Int64,
            4 => ValueType::Float32,
            5 => ValueType::Float64,
            6 => ValueType::String,
            7 => ValueType::Binary,
            8 => ValueType::DateTime,
            9 => ValueType::Date,
            10 => ValueType::Time,
            11 => ValueType::Uuid,
            12 => ValueType::ObjectId,
            13 => ValueType::Array,
            14 => ValueType::Object,
            15 => ValueType::Set,
            16 => ValueType::Row,
            17 => ValueType::SortedSet,
            18 => ValueType::GeoPoint,
            19 => ValueType::Reference,
            _ => {
                return Err(MonoError::Parse(format!(
                    "Unknown value type: {}",
                    type_tag
                )));
            }
        };

        let flags = data[offset];
        offset += 1;

        let nullable = (flags & 0b0000_0001) != 0;
        let is_primary = (flags & 0b0000_0010) != 0;
        let is_unique = (flags & 0b0000_0100) != 0;

        let has_default = data[offset];
        offset += 1;

        let default = if has_default == 1 {
            let (val, used) = MonoValue::from_bytes(&data[offset..])?;
            offset += used;
            Some(val)
        } else {
            None
        };

        Ok((
            monodb_common::schema::TableColumn {
                name,
                data_type,
                nullable,
                default,
                is_primary,
                is_unique,
            },
            offset,
        ))
    }

    /// Deserialize an index definition
    fn deserialize_index(data: &[u8]) -> Result<(monodb_common::schema::Index, usize)> {
        use monodb_common::schema::IndexType;

        let mut offset = 0;

        let (name, used) = Self::read_string(&data[offset..])?;
        offset += used;

        let column_count =
            u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let (col, used) = Self::read_string(&data[offset..])?;
            offset += used;
            columns.push(col);
        }

        let flags = data[offset];
        offset += 1;
        let unique = (flags & 0b0000_0001) != 0;

        let type_tag = data[offset];
        offset += 1;

        let index_type = match type_tag {
            0 => IndexType::BTree,
            1 => IndexType::Hash,
            2 => IndexType::FullText,
            3 => IndexType::Spatial,
            _ => {
                return Err(MonoError::Parse(format!(
                    "Unknown index type: {}",
                    type_tag
                )));
            }
        };

        Ok((
            monodb_common::schema::Index {
                name,
                columns,
                unique,
                index_type,
            },
            offset,
        ))
    }

    /// Deserialize a validation rule
    fn deserialize_validation_rule(
        data: &[u8],
    ) -> Result<(monodb_common::schema::ValidationRule, usize)> {
        use monodb_common::schema::ValidationRule;

        let mut offset = 0;
        let rule_type = data[offset];
        offset += 1;

        let (rule_data, used) = Self::read_string(&data[offset..])?;
        offset += used;

        let rule = match rule_type {
            0 => ValidationRule::JsonSchema(rule_data),
            1 => ValidationRule::Custom(rule_data),
            _ => {
                return Err(MonoError::Parse(format!(
                    "Unknown validation rule type: {}",
                    rule_type
                )));
            }
        };

        Ok((rule, offset))
    }

    /// Helper to write a string in our format: [len: u32][UTF-8 bytes]
    fn write_string(bytes: &mut Vec<u8>, s: &str) {
        let b = s.as_bytes();
        bytes.extend(&(b.len() as u32).to_le_bytes());
        bytes.extend(b);
    }

    /// Helper to read a string in our format: [len: u32][UTF-8 bytes]
    fn read_string(data: &[u8]) -> Result<(String, usize)> {
        if data.len() < 4 {
            return Err(MonoError::Parse(
                "Buffer too short for string length".into(),
            ));
        }
        let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if data.len() < 4 + len {
            return Err(MonoError::Parse("Buffer too short for string data".into()));
        }
        let s = std::str::from_utf8(&data[4..4 + len])
            .map_err(|e| MonoError::Parse(format!("Invalid UTF-8: {}", e)))?;
        Ok((s.to_owned(), 4 + len))
    }

    /// Generate the next sequential integer value for a given table column.
    ///
    /// This provides basic auto-increment functionality for relational tables.
    /// The counter is stored in memory; persistence can be added later.
    pub async fn next_sequence_value(&self, table: &str, column: &str) -> Result<u64> {
        static SEQUENCES: Lazy<DashMap<String, AtomicU64>> = Lazy::new(DashMap::new);

        // Key is "table:column"
        let key = format!("{table}:{column}");

        let entry = SEQUENCES
            .entry(key.clone())
            .or_insert_with(|| AtomicU64::new(1));
        let next = entry.fetch_add(1, Ordering::SeqCst);

        Ok(next)
    }
}

// MVCC Transaction Support

/// Transaction status for MVCC
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Active,
    Committed,
    Aborted,
}

/// Transaction metadata for MVCC with Snapshot Isolation
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Unique transaction identifier
    pub tx_id: u64,
    /// Timestamp when transaction started (used for visibility)
    pub start_ts: u64,
    /// Timestamp when transaction committed (None if not yet committed)
    pub commit_ts: Option<u64>,
    /// Current transaction status
    pub status: TxStatus,
    /// Keys modified by this transaction (collection_name, primary_key)
    pub write_set: HashSet<(String, Vec<u8>)>,
}

impl Transaction {
    /// Create a new transaction with the given IDs
    pub fn new(tx_id: u64, start_ts: u64) -> Self {
        Self {
            tx_id,
            start_ts,
            commit_ts: None,
            status: TxStatus::Active,
            write_set: HashSet::new(),
        }
    }

    /// Check if this transaction is still active
    pub fn is_active(&self) -> bool {
        self.status == TxStatus::Active
    }
}

/// Transaction Manager for MVCC
/// Manages transaction lifecycle and provides visibility information for snapshot isolation
pub struct TransactionManager {
    /// Monotonically increasing timestamp generator (used for both tx_id and timestamps)
    next_ts: AtomicU64,
    /// Active and recently committed transactions
    transactions: DashMap<u64, Transaction>,
    /// Oldest active transaction start timestamp (for GC decisions)
    oldest_active_ts: AtomicU64,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new() -> Self {
        Self {
            next_ts: AtomicU64::new(1),
            transactions: DashMap::new(),
            oldest_active_ts: AtomicU64::new(u64::MAX),
        }
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Transaction {
        let ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        let tx = Transaction::new(ts, ts); // tx_id == start_ts for simplicity

        // Update oldest active timestamp
        self.oldest_active_ts.fetch_min(ts, Ordering::SeqCst);

        self.transactions.insert(ts, tx.clone());
        tx
    }

    /// Commit a transaction, returns the commit timestamp
    pub fn commit(&self, tx_id: u64) -> Result<u64> {
        let commit_ts = self.next_ts.fetch_add(1, Ordering::SeqCst);

        if let Some(mut tx) = self.transactions.get_mut(&tx_id) {
            if tx.status != TxStatus::Active {
                return Err(MonoError::InvalidOperation(format!(
                    "Transaction {} is not active (status: {:?})",
                    tx_id, tx.status
                )));
            }
            tx.commit_ts = Some(commit_ts);
            tx.status = TxStatus::Committed;
        } else {
            return Err(MonoError::NotFound(format!(
                "Transaction {} not found",
                tx_id
            )));
        }

        // Recalculate oldest active timestamp
        self.update_oldest_active();

        Ok(commit_ts)
    }

    /// Abort a transaction
    pub fn abort(&self, tx_id: u64) -> Result<()> {
        if let Some(mut tx) = self.transactions.get_mut(&tx_id) {
            if tx.status != TxStatus::Active {
                return Err(MonoError::InvalidOperation(format!(
                    "Transaction {} is not active (status: {:?})",
                    tx_id, tx.status
                )));
            }
            tx.status = TxStatus::Aborted;
        } else {
            return Err(MonoError::NotFound(format!(
                "Transaction {} not found",
                tx_id
            )));
        }

        // Recalculate oldest active timestamp
        self.update_oldest_active();

        Ok(())
    }

    /// Get a transaction by ID
    pub fn get(&self, tx_id: u64) -> Option<Transaction> {
        self.transactions.get(&tx_id).map(|tx| tx.clone())
    }

    /// Get the oldest active transaction timestamp (for GC)
    pub fn oldest_active(&self) -> u64 {
        self.oldest_active_ts.load(Ordering::SeqCst)
    }

    /// Get next timestamp (for auto-commit operations)
    pub fn next_timestamp(&self) -> u64 {
        self.next_ts.fetch_add(1, Ordering::SeqCst)
    }

    /// Add a key to a transaction's write set
    pub fn add_to_write_set(&self, tx_id: u64, collection: String, pk: Vec<u8>) {
        if let Some(mut tx) = self.transactions.get_mut(&tx_id) {
            tx.write_set.insert((collection, pk));
        }
    }

    /// Recalculate the oldest active transaction timestamp
    fn update_oldest_active(&self) {
        let mut oldest = u64::MAX;
        for entry in self.transactions.iter() {
            if entry.status == TxStatus::Active && entry.start_ts < oldest {
                oldest = entry.start_ts;
            }
        }
        self.oldest_active_ts.store(oldest, Ordering::SeqCst);
    }

    /// Clean up old committed/aborted transactions (call periodically)
    /// Keeps transactions newer than the given threshold
    #[allow(dead_code)]
    pub fn cleanup(&self, older_than: u64) {
        self.transactions
            .retain(|_, tx| tx.status == TxStatus::Active || tx.start_ts >= older_than);
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

// Versioned Key Encoding for MVCC

/// Encode a versioned key: `pk:INVERTED_TIMESTAMP`
/// Inverted timestamp ensures newest versions sort first lexicographically
pub fn encode_versioned_key(pk: &[u8], commit_ts: u64) -> Vec<u8> {
    let inverted = u64::MAX - commit_ts;
    let mut key = pk.to_vec();
    key.push(b':');
    // 16-char hex for consistent sorting
    key.extend_from_slice(format!("{:016X}", inverted).as_bytes());
    key
}

/// Decode a versioned key back to (pk, commit_ts)
/// Returns None if the key doesn't have a valid version suffix
pub fn decode_versioned_key(key: &[u8]) -> Option<(Vec<u8>, u64)> {
    // Find the last ':' separator
    let sep_pos = key.iter().rposition(|&b| b == b':')?;

    // Need at least 16 chars after the separator
    if key.len() < sep_pos + 1 + 16 {
        return None;
    }

    let pk = key[..sep_pos].to_vec();
    let ts_hex = std::str::from_utf8(&key[sep_pos + 1..]).ok()?;

    // Parse hex and un-invert
    let inverted = u64::from_str_radix(ts_hex, 16).ok()?;
    let commit_ts = u64::MAX - inverted;

    Some((pk, commit_ts))
}

/// Check if a key is a versioned key (has timestamp suffix)
pub fn is_versioned_key(key: &[u8]) -> bool {
    if let Some(sep_pos) = key.iter().rposition(|&b| b == b':') {
        // Check if suffix is exactly 16 hex chars
        let suffix = &key[sep_pos + 1..];
        suffix.len() == 16 && suffix.iter().all(|&b| b.is_ascii_hexdigit())
    } else {
        false
    }
}

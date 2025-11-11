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
    disk_manager: Option<Arc<DiskManager>>,
    schemas: Arc<DashMap<String, Schema>>,
    memory_keyspaces: Arc<DashMap<String, Arc<DashMap<String, MonoValue>>>>,
    secondary_indexes: Arc<DashMap<String, DashMap<String, Arc<IndexEntry>>>>,
    config: StorageConfig,
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
        if let Some(max) = max_size {
            if value.len() > *max {
                return Err(MonoError::InvalidOperation(format!(
                    "Value size {} exceeds maximum size {}",
                    value.len(),
                    max
                )));
            }
        }
        Ok(())
    }

    /// Validate value-specific constraints
    fn validate_value_constraints(&self, value: &MonoValue, field_name: &str) -> Result<()> {
        match value {
            MonoValue::String(s) => {
                // Example: Check for empty strings in certain fields
                if s.is_empty() && field_name.ends_with("_id") {
                    return Err(MonoError::InvalidOperation(format!(
                        "Field '{field_name}' cannot be empty"
                    )));
                }
            }
            _ => {}
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
                            if let MonoValue::Object(map) = &row_val {
                                if let Some(existing_val) = map.get(field_name) {
                                    if existing_val == inserted_value {
                                        return Err(MonoError::InvalidOperation(format!(
                                            "Unique constraint violation on field '{}'",
                                            field_name
                                        )));
                                    }
                                }
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
            .or_insert_with(DashMap::new);

        let secondary_index = collection_indexes
            .entry(index.name.clone())
            .or_insert_with(|| Arc::new(DashMap::new()));

        // Serialize the index key
        let index_key = bincode::serialize(&index_values)
            .map_err(|e| MonoError::Storage(format!("Failed to serialize index key: {e}")))?;

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
            .or_insert_with(DashMap::new);

        // Update each index
        for index in indexes {
            let index_values = self.extract_index_values(data, &index.columns)?;
            let index_key = bincode::serialize(&index_values)
                .map_err(|e| MonoError::Storage(format!("Failed to serialize index key: {e}")))?;

            let secondary_index = collection_indexes
                .entry(index.name.clone())
                .or_insert_with(|| Arc::new(DashMap::new()));

            secondary_index
                .entry(index_key)
                .or_insert_with(HashSet::new)
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
        if let Some(schema) = self.schemas.get(collection) {
            if let Schema::KeySpace {
                persistence: KeySpacePersistence::Memory,
                ..
            } = schema.value()
            {
                if let Some(store) = self.memory_keyspaces.get(collection) {
                    store.remove(key);
                    return Ok(());
                }
            }
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
                            .and_then(|id_val| match id_val {
                                MonoValue::ObjectId(oid) => Some(oid.to_hex()),
                                MonoValue::String(s) => Some(s.clone()),
                                MonoValue::Int32(i) => Some(i.to_string()),
                                MonoValue::Int64(i) => Some(i.to_string()),
                                _ => Some(format!("{id_val}")),
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
                                .and_then(|pk_val| match pk_val {
                                    MonoValue::String(s) => Some(s.clone()),
                                    MonoValue::Int32(i) => Some(i.to_string()),
                                    MonoValue::Int64(i) => Some(i.to_string()),
                                    _ => Some(format!("{pk_val}")),
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
                let key = self.insert_document(collection_name, doc).await?;
                key
            }
            (Schema::Table { .. }, Data::Row(row)) => {
                let key = self.insert_row(collection_name, row).await?;
                key
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
                let key = self.insert_document(collection_name, &document).await?;
                key
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
        let serialized_value = bincode::serialize(value)
            .map_err(|e| MonoError::Storage(format!("serialize error: {e}")))?;

        if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.put(key, serialized_value).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }
        } else {
            if let Some(btree) = self.btrees.get(collection) {
                btree.insert(&key, &serialized_value).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }
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
        } else {
            if let Some(btree) = self.btrees.get(collection) {
                btree.insert(&key_vec, &val_vec).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }
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
        let value_bytes = bincode::serialize(doc)
            .map_err(|e| MonoError::Storage(format!("Serialize error: {e}")))?;

        if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.put(key_bytes.clone(), value_bytes).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }
        } else {
            if let Some(btree) = self.btrees.get(collection) {
                btree.insert(&key_bytes, &value_bytes).await?;
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection '{collection}' not found"
                )));
            }
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
            columns,
            primary_key,
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
                if value.is_some() {
                    pk_values.push(value.unwrap().to_string());
                } else {
                    return Err(MonoError::InvalidOperation(format!(
                        "primary key '{pk_field}' does not exist in '{collection}'"
                    )));
                }
            }
            let key_bytes = pk_values.join(":").as_bytes().to_vec();

            let value_to_store =
                MonoValue::Object(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            let value_bytes = bincode::serialize(&value_to_store)
                .map_err(|e| MonoError::Storage(format!("serialize error: {e}")))?;

            if self.config.use_lsm {
                if let Some(lsm) = self.lsm_trees.get(collection) {
                    lsm.put(key_bytes.clone(), value_bytes).await?;
                } else {
                    return Err(MonoError::NotFound(format!(
                        "collection '{collection}' not found"
                    )));
                }
            } else {
                if let Some(btree) = self.btrees.get(collection) {
                    btree.insert(&key_bytes, &value_bytes).await?;
                } else {
                    return Err(MonoError::NotFound(format!(
                        "collection '{collection}' not found"
                    )));
                }
            }

            Ok(key_bytes)
        } else {
            unreachable!()
        }
    }

    /// Persist schemas to disk atomically
    async fn persist_schemas(&self) -> Result<()> {
        let schemas_file = Path::new(&self.config.data_dir).join("schemas.bin");

        let schemas_map: HashMap<String, Schema> = self
            .schemas
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        let serialized = bincode::serialize(&schemas_map)
            .map_err(|e| MonoError::Storage(format!("schema serialization failed: {e}")))?;

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

        let schemas: HashMap<String, Schema> = bincode::deserialize(&data)
            .map_err(|e| MonoError::Parse(format!("schema deserialization failed: {e}")))?;

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
            }
            processed += 1;
            if processed % 100 == 0 {
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
        if !self.schemas.contains_key(collection_name) {
            return Err(MonoError::NotFound(format!(
                "collection '{collection_name}' not found"
            )));
        }

        let schema = self.schemas.get(collection_name).unwrap();
        match schema.value() {
            Schema::KeySpace { .. } => self.find_kv(collection_name, query).await,
            Schema::Collection { .. } | Schema::Table { .. } => {
                self.find_with_filter(collection_name, query).await
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
        } else {
            if let Some(btree) = self.btrees.get(collection) {
                if let Some(prefix) = namespace_prefix {
                    let start = format!("{prefix}:").into_bytes();
                    let mut end = start.clone();
                    end.extend(std::iter::repeat(0xFFu8).take(64));
                    btree.scan(&start, &end).await?
                } else {
                    let end = vec![0xFFu8; 64];
                    btree.scan(&[], &end).await?
                }
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection {collection} not found"
                )));
            }
        };

        let mut values: Vec<MonoValue> = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            let key_str = String::from_utf8_lossy(&k).to_string();
            match bincode::deserialize::<MonoValue>(&v) {
                Ok(mut mv) => {
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
                if let Some(ref prefix) = namespace_prefix {
                    if !key.starts_with(&format!("{prefix}:")) {
                        return None;
                    }
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
        // Scan all documents without adding _key/key fields (those are for KeySpaces only)
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = if self.config.use_lsm {
            if let Some(lsm) = self.lsm_trees.get(collection) {
                lsm.scan(&[], &[0xFF]).await?
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection {collection} not found"
                )));
            }
        } else {
            if let Some(btree) = self.btrees.get(collection) {
                let end = vec![0xFFu8; 64];
                btree.scan(&[], &end).await?
            } else {
                return Err(MonoError::NotFound(format!(
                    "collection {collection} not found"
                )));
            }
        };

        let mut values: Vec<MonoValue> = Vec::with_capacity(pairs.len());
        for (_k, v) in pairs {
            match bincode::deserialize::<MonoValue>(&v) {
                Ok(mv) => {
                    values.push(mv);
                }
                Err(e) => {
                    tracing::warn!("failed to deserialize value: {}", e);
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

    fn apply_filter(&self, row: &MonoValue, filter: &crate::storage::models::Filter) -> bool {
        use crate::storage::models::Filter as F;
        match filter {
            F::Eq(field, v) => row
                .as_object()
                .and_then(|m| m.get(field))
                .map_or(false, |x| x == v),
            F::Neq(field, v) => row
                .as_object()
                .and_then(|m| m.get(field))
                .map_or(false, |x| x != v),
            F::Gt(field, v) => row
                .as_object()
                .and_then(|m| m.get(field))
                .map_or(false, |x| {
                    Self::compare_values(x, v) == std::cmp::Ordering::Greater
                }),
            F::Gte(field, v) => row
                .as_object()
                .and_then(|m| m.get(field))
                .map_or(false, |x| {
                    matches!(
                        Self::compare_values(x, v),
                        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                    )
                }),
            F::Lt(field, v) => row
                .as_object()
                .and_then(|m| m.get(field))
                .map_or(false, |x| {
                    Self::compare_values(x, v) == std::cmp::Ordering::Less
                }),
            F::Lte(field, v) => row
                .as_object()
                .and_then(|m| m.get(field))
                .map_or(false, |x| {
                    matches!(
                        Self::compare_values(x, v),
                        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                    )
                }),
            F::Contains(field, v) => {
                row.as_object()
                    .and_then(|m| m.get(field))
                    .map_or(false, |x| match x {
                        MonoValue::Array(a) => a.iter().any(|e| e == v),
                        MonoValue::String(s) => {
                            v.as_string().map(|pat| s.contains(pat)).unwrap_or(false)
                        }
                        other => other.to_string().contains(&v.to_string()),
                    })
            }
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

pub struct Transaction {
    pub id: u64,
}

impl Transaction {
    fn new() -> Self {
        static CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self {
            id: CTR.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        }
    }
}

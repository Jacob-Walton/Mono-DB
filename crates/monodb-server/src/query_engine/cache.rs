#![allow(dead_code)]

//! Query Caching Layer

use crate::query_engine::ast::{DataType, LiteralValue, Statement};
use crate::query_engine::physical_plan::PhysicalPlan;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Query fingerprinting

/// Fingerprint that uniquely identifies a query structure, enabling plan sharing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryFingerprint(u64);

impl QueryFingerprint {
    /// Create a fingerprint from a parsed statement using structural hashing.
    pub fn from_statement(stmt: &Statement) -> Self {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();

        // Statement implements Hash with LiteralSlot only hashing type discriminant
        hash_statement(stmt, &mut hasher);

        QueryFingerprint(hasher.finish())
    }

    /// Create a simple text-based fingerprint (not structural).
    pub fn from_text(text: &str) -> Self {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        QueryFingerprint(hasher.finish())
    }

    /// Get the raw fingerprint value.
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Hash a statement for fingerprinting (delegates to Statement's Hash impl).
fn hash_statement<H: Hasher>(stmt: &Statement, hasher: &mut H) {
    // Use the variant discriminant
    std::mem::discriminant(stmt).hash(hasher);

    match stmt {
        Statement::Query(q) => q.hash(hasher),
        Statement::Mutation(m) => m.hash(hasher),
        Statement::Ddl(d) => d.hash(hasher),
        Statement::Transaction(t) => t.hash(hasher),
        Statement::Control(_) => {
            // Control statements can't be fingerprinted easily
            0u8.hash(hasher);
        }
    }
}

// Extracted literals from queries

/// Literal values extracted from a parsed query.
///
/// When a query is cached by fingerprint, the literals are extracted and
/// stored separately. At execution time, these literals are used to
/// populate the parameter slots in the cached plan.
#[derive(Debug, Clone, Default)]
pub struct QueryLiterals {
    /// Literal values indexed by slot_id
    pub values: Vec<LiteralValue>,
}

impl QueryLiterals {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Get a literal value by slot ID.
    pub fn get(&self, slot_id: u32) -> Option<&LiteralValue> {
        self.values.get(slot_id as usize)
    }

    /// Add a literal value and return its slot ID.
    pub fn push(&mut self, value: LiteralValue) -> u32 {
        let id = self.values.len() as u32;
        self.values.push(value);
        id
    }
}

// Parse Cache

/// Cache for parsed ASTs.
///
/// Maps query text hash to parsed statement and extracted literals.
pub struct ParseCache {
    cache: RwLock<HashMap<u64, CachedParse>>,
    max_entries: usize,
}

struct CachedParse {
    statement: Arc<Statement>,
    literals: QueryLiterals,
    last_used: Instant,
    hit_count: u64,
}

impl ParseCache {
    /// Create a new parse cache with the given maximum size.
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    /// Look up a cached parse result by query text hash.
    pub fn get(&self, text_hash: u64) -> Option<(Arc<Statement>, QueryLiterals)> {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&text_hash) {
            entry.last_used = Instant::now();
            entry.hit_count += 1;
            Some((entry.statement.clone(), entry.literals.clone()))
        } else {
            None
        }
    }

    /// Insert a parsed statement into the cache.
    pub fn insert(&self, text_hash: u64, stmt: Statement, literals: QueryLiterals) {
        let mut cache = self.cache.write();

        // Evict if at capacity
        if cache.len() >= self.max_entries {
            self.evict_one(&mut cache);
        }

        cache.insert(
            text_hash,
            CachedParse {
                statement: Arc::new(stmt),
                literals,
                last_used: Instant::now(),
                hit_count: 0,
            },
        );
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        self.cache.write().clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.read();
        CacheStats {
            entries: cache.len(),
            max_entries: self.max_entries,
        }
    }

    fn evict_one(&self, cache: &mut HashMap<u64, CachedParse>) {
        // LRU eviction: remove the least recently used entry
        if let Some((&oldest_key, _)) = cache.iter().min_by_key(|(_, v)| v.last_used) {
            cache.remove(&oldest_key);
        }
    }
}

// Plan Cache

/// Cache for optimized physical plans.
///
/// Maps query fingerprints to physical execution plans.
pub struct PlanCache {
    cache: RwLock<HashMap<QueryFingerprint, CachedPlan>>,
    max_entries: usize,
    ttl: Duration,
}

struct CachedPlan {
    plan: Arc<PhysicalPlan>,
    created: Instant,
    hit_count: u64,
}

impl PlanCache {
    /// Create a new plan cache.
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_entries,
            ttl,
        }
    }

    /// Look up a cached plan by fingerprint.
    pub fn get(&self, fingerprint: &QueryFingerprint) -> Option<Arc<PhysicalPlan>> {
        let mut cache = self.cache.write();

        if let Some(entry) = cache.get_mut(fingerprint) {
            // Check TTL
            if entry.created.elapsed() > self.ttl {
                cache.remove(fingerprint);
                return None;
            }
            entry.hit_count += 1;
            Some(entry.plan.clone())
        } else {
            None
        }
    }

    /// Insert a plan into the cache.
    pub fn insert(&self, fingerprint: QueryFingerprint, plan: PhysicalPlan) {
        let mut cache = self.cache.write();

        // Evict if at capacity
        if cache.len() >= self.max_entries {
            self.evict_one(&mut cache);
        }

        cache.insert(
            fingerprint,
            CachedPlan {
                plan: Arc::new(plan),
                created: Instant::now(),
                hit_count: 0,
            },
        );
    }

    /// Invalidate all plans that reference a specific table.
    ///
    /// Call this after DDL operations that modify the table schema.
    pub fn invalidate_table(&self, table: &str) {
        let mut cache = self.cache.write();
        cache.retain(|_, cached| !plan_references_table(&cached.plan, table));
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        self.cache.write().clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.read();
        CacheStats {
            entries: cache.len(),
            max_entries: self.max_entries,
        }
    }

    fn evict_one(&self, cache: &mut HashMap<QueryFingerprint, CachedPlan>) {
        // Evict based on hit count (least frequently used)
        if let Some((oldest_key, _)) = cache
            .iter()
            .min_by_key(|(_, v)| (v.hit_count, std::cmp::Reverse(v.created)))
            .map(|(k, v)| (*k, v))
        {
            cache.remove(&oldest_key);
        }
    }
}

/// Check if a physical plan references a specific table.
fn plan_references_table(plan: &PhysicalPlan, table: &str) -> bool {
    match plan {
        PhysicalPlan::SeqScan(op) => op.table.as_str() == table,
        PhysicalPlan::IndexScan(op) => op.table.as_str() == table,
        PhysicalPlan::PrimaryKeyLookup(op) => op.table.as_str() == table,
        PhysicalPlan::Filter(op) => plan_references_table(&op.input, table),
        PhysicalPlan::Project(op) => plan_references_table(&op.input, table),
        PhysicalPlan::Sort(op) => plan_references_table(&op.input, table),
        PhysicalPlan::Limit(op) => plan_references_table(&op.input, table),
        PhysicalPlan::Offset(op) => plan_references_table(&op.input, table),
        PhysicalPlan::HashAggregate(op) => plan_references_table(&op.input, table),
        PhysicalPlan::StreamAggregate(op) => plan_references_table(&op.input, table),
        PhysicalPlan::NestedLoopJoin(op) => {
            plan_references_table(&op.outer, table) || plan_references_table(&op.inner, table)
        }
        PhysicalPlan::HashJoin(op) => {
            plan_references_table(&op.build, table) || plan_references_table(&op.probe, table)
        }
        PhysicalPlan::MergeJoin(op) => {
            plan_references_table(&op.left, table) || plan_references_table(&op.right, table)
        }
        PhysicalPlan::Insert(op) => op.table.as_str() == table,
        PhysicalPlan::Update(op) => op.table.as_str() == table,
        PhysicalPlan::Delete(op) => op.table.as_str() == table,
        PhysicalPlan::Values(_) | PhysicalPlan::Empty => false,
    }
}

// Prepared Statement Cache

/// Cache for prepared statements.
///
/// Unlike the fingerprint-based plan cache, prepared statements are
/// explicitly named and managed by the client.
pub struct PreparedStatementCache {
    statements: RwLock<HashMap<String, PreparedStatement>>,
}

/// A prepared statement with its compiled plan.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// User-provided name
    pub name: String,
    /// The optimized execution plan
    pub plan: Arc<PhysicalPlan>,
    /// Number of parameter slots
    pub param_count: usize,
    /// Inferred parameter types (if known)
    pub param_types: Vec<Option<DataType>>,
    /// When this statement was prepared
    pub created: Instant,
}

impl PreparedStatementCache {
    pub fn new() -> Self {
        Self {
            statements: RwLock::new(HashMap::new()),
        }
    }

    /// Prepare a new statement.
    pub fn prepare(
        &self,
        name: String,
        plan: PhysicalPlan,
        param_count: usize,
    ) -> PreparedStatement {
        let stmt = PreparedStatement {
            name: name.clone(),
            plan: Arc::new(plan),
            param_count,
            param_types: vec![None; param_count],
            created: Instant::now(),
        };

        self.statements.write().insert(name, stmt.clone());
        stmt
    }

    /// Get a prepared statement by name.
    pub fn get(&self, name: &str) -> Option<PreparedStatement> {
        self.statements.read().get(name).cloned()
    }

    /// Deallocate a prepared statement.
    pub fn deallocate(&self, name: &str) -> bool {
        self.statements.write().remove(name).is_some()
    }

    /// Clear all prepared statements.
    pub fn clear(&self) {
        self.statements.write().clear();
    }

    /// Get the number of cached statements.
    pub fn len(&self) -> usize {
        self.statements.read().len()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.statements.read().is_empty()
    }
}

impl Default for PreparedStatementCache {
    fn default() -> Self {
        Self::new()
    }
}

// Combined Cache Manager

/// Combined query cache manager.
///
/// Provides a unified interface to all caching layers.
pub struct QueryCacheManager {
    pub parse_cache: ParseCache,
    pub plan_cache: PlanCache,
    pub prepared_cache: PreparedStatementCache,
}

impl QueryCacheManager {
    /// Create a new cache manager with the given configuration.
    pub fn new(config: CacheConfig) -> Self {
        Self {
            parse_cache: ParseCache::new(config.parse_cache_size),
            plan_cache: PlanCache::new(config.plan_cache_size, config.plan_cache_ttl),
            prepared_cache: PreparedStatementCache::new(),
        }
    }

    /// Create a cache manager with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(CacheConfig::default())
    }

    /// Invalidate all caches related to a table.
    ///
    /// Call this after DDL operations.
    pub fn invalidate_table(&self, table: &str) {
        self.plan_cache.invalidate_table(table);
        // Parse cache doesn't need invalidation (AST is still valid)
        // Prepared statements might need invalidation at some point, but skipping for now
    }

    /// Clear all caches.
    pub fn clear_all(&self) {
        self.parse_cache.clear();
        self.plan_cache.clear();
        self.prepared_cache.clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheManagerStats {
        CacheManagerStats {
            parse_cache: self.parse_cache.stats(),
            plan_cache: self.plan_cache.stats(),
            prepared_statements: self.prepared_cache.len(),
        }
    }
}

impl Default for QueryCacheManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// Configuration

/// Configuration for the caching system.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum entries in the parse cache
    pub parse_cache_size: usize,
    /// Maximum entries in the plan cache
    pub plan_cache_size: usize,
    /// Time-to-live for cached plans
    pub plan_cache_ttl: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            parse_cache_size: 1000,
            plan_cache_size: 500,
            plan_cache_ttl: Duration::from_secs(300), // 5 minutes
        }
    }
}

// Statistics

/// Statistics for a single cache.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_entries: usize,
}

/// Statistics for all caches.
#[derive(Debug, Clone)]
pub struct CacheManagerStats {
    pub parse_cache: CacheStats,
    pub plan_cache: CacheStats,
    pub prepared_statements: usize,
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engine::ast::{GetQuery, Ident, QueryStatement, Spanned};

    #[test]
    fn test_query_fingerprint_same_structure() {
        // Two queries with same structure but different literals should have same fingerprint
        let stmt1 = Statement::Query(QueryStatement::Get(GetQuery {
            source: Spanned::dummy(Ident::new("users")),
            filter: None,
            projection: None,
            order_by: None,
            limit: Some(crate::query_engine::ast::LimitValue::Literal(10)),
            offset: None,
        }));

        let stmt2 = Statement::Query(QueryStatement::Get(GetQuery {
            source: Spanned::dummy(Ident::new("users")),
            filter: None,
            projection: None,
            order_by: None,
            limit: Some(crate::query_engine::ast::LimitValue::Literal(20)),
            offset: None,
        }));

        let fp1 = QueryFingerprint::from_statement(&stmt1);
        let fp2 = QueryFingerprint::from_statement(&stmt2);

        // Same fingerprint because LimitValue::Literal hashes the same
        // (FIXME: LimitValue should use LiteralSlot)
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_parse_cache_basic() {
        let cache = ParseCache::new(10);

        let stmt = Statement::Query(QueryStatement::Get(GetQuery {
            source: Spanned::dummy(Ident::new("test")),
            filter: None,
            projection: None,
            order_by: None,
            limit: None,
            offset: None,
        }));

        let hash = 12345u64;
        cache.insert(hash, stmt.clone(), QueryLiterals::new());

        let (cached_stmt, _) = cache.get(hash).expect("Should find cached entry");
        assert!(matches!(*cached_stmt, Statement::Query(_)));
    }

    #[test]
    fn test_plan_cache_ttl() {
        let cache = PlanCache::new(10, Duration::from_millis(10));

        let fingerprint = QueryFingerprint(99999);
        let plan = PhysicalPlan::Empty;

        cache.insert(fingerprint, plan);
        assert!(cache.get(&fingerprint).is_some());

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(20));

        // Should be expired now
        assert!(cache.get(&fingerprint).is_none());
    }
}

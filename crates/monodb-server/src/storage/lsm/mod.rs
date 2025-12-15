//! LSM tree storage engine components.
//!
//! This module manages:
//! - The mutable and immutable memtables
//! - Flushing memtables to SSTables on disk
//! - Scheduling background compaction
//! - WAL replay and checkpointing integration

pub mod bloom_filter;
pub mod compaction;
pub mod merge_iterator;
pub mod sstable;

use crate::config::{LsmConfig, WalConfig};
use crate::storage::wal::Wal;
pub use compaction::{CompactionType, run_compaction_task};
use crossbeam_skiplist::SkipMap;
use lru::LruCache;
use monodb_common::Result;
use parking_lot::{Mutex, RwLock};
use sstable::SsTable;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

type Memtable = Arc<SkipMap<Vec<u8>, Vec<u8>>>;
type ImmutableMemtables = Arc<RwLock<Vec<Memtable>>>;
type BlockEntries = Vec<(Vec<u8>, Vec<u8>)>;
type BlockEntriesArc = Arc<BlockEntries>;

pub struct LsmTree {
    memtable: Arc<RwLock<Memtable>>,
    memtable_size: Arc<AtomicUsize>,
    immutable_memtables: ImmutableMemtables,
    sstables: Arc<RwLock<Vec<SsTable>>>,
    /// Optional WAL - if None, uses external/system WAL
    wal: Option<Arc<RwLock<Wal>>>,
    /// Cached flag: true if WAL is in async mode (avoids lock to check)
    #[allow(dead_code)]
    wal_is_async: bool,
    config: LsmConfig,
    compaction_sender: mpsc::Sender<CompactionType>,
    compaction_in_progress: Arc<AtomicBool>,
    data_dir: PathBuf,
    /// Track if memtable contains only replayed WAL data (no new writes)
    memtable_is_replay_only: Arc<AtomicBool>,
    block_cache: Option<Arc<BlockCache>>,
    /// Collection name for this LSM tree (used in system WAL)
    #[allow(dead_code)]
    collection_name: String,
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct BlockKey {
    path: PathBuf,
    offset: u64,
}

struct BlockCache {
    inner: Mutex<LruCache<BlockKey, BlockEntriesArc>>,
}

impl BlockCache {
    fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    // TODO: reuse the buffer_pool LRU-K once it's generic; current cache is plain LRU.
    fn get_or_load(&self, table: &SsTable, offset: u64, end: u64) -> Result<BlockEntriesArc> {
        let key = BlockKey {
            path: table.path.clone(),
            offset,
        };

        if let Some(v) = self.inner.lock().get(&key).cloned() {
            return Ok(v);
        }

        let block = Arc::new(table.read_block(offset, end)?);
        self.inner.lock().put(key, block.clone());
        Ok(block)
    }
}

impl LsmTree {
    /// Create a new LSM tree with its own per-collection WAL (legacy mode).
    #[allow(dead_code)]
    pub fn new<P: AsRef<Path>>(path: P, config: LsmConfig, wal_config: WalConfig) -> Result<Self> {
        let path = path.as_ref();
        let collection_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        Self::with_wal(path, config, Some(wal_config), collection_name)
    }

    /// Create a new LSM tree that uses an external/system WAL.
    /// When wal_config is None, no internal WAL is created - caller must handle WAL.
    pub fn with_external_wal<P: AsRef<Path>>(
        path: P,
        config: LsmConfig,
        collection_name: String,
    ) -> Result<Self> {
        Self::with_wal(path, config, None, collection_name)
    }

    /// Internal constructor - creates LSM tree with optional internal WAL.
    fn with_wal<P: AsRef<Path>>(
        path: P,
        config: LsmConfig,
        wal_config: Option<WalConfig>,
        collection_name: String,
    ) -> Result<Self> {
        let path = path.as_ref();
        let data_dir = path.join("data");

        // Ensure data directory exists
        std::fs::create_dir_all(&data_dir)?;

        // Create internal WAL only if config provided
        let (wal, wal_is_async) = if let Some(wal_cfg) = wal_config {
            let wal = Wal::with_config(path.join("wal.log"), wal_cfg)?;
            let is_async = wal.is_async();
            (Some(Arc::new(RwLock::new(wal))), is_async)
        } else {
            (None, false)
        };

        let (compaction_sender, mut compaction_receiver) = mpsc::channel(1);
        let sstables = Arc::new(RwLock::new(Vec::new()));
        let compaction_in_progress = Arc::new(AtomicBool::new(false));
        let block_cache = if config.block_cache_capacity > 0 {
            Some(Arc::new(BlockCache::new(config.block_cache_capacity)))
        } else {
            None
        };

        let lsm = Self {
            memtable: Arc::new(RwLock::new(Arc::new(SkipMap::new()))),
            memtable_size: Arc::new(AtomicUsize::new(0)),
            immutable_memtables: Arc::new(RwLock::new(Vec::new())),
            sstables,
            wal,
            wal_is_async,
            config,
            compaction_sender,
            compaction_in_progress,
            data_dir: data_dir.clone(),
            memtable_is_replay_only: Arc::new(AtomicBool::new(true)),
            block_cache,
            collection_name,
        };

        // Load existing SSTables from disk
        lsm.load_existing_sstables()?;

        // Replay internal WAL entries into memtable (only if we have internal WAL)
        if lsm.wal.is_some() {
            lsm.replay_wal(path)?;
        }

        // Spawn compaction task with proper references
        let sstables_ref = Arc::clone(&lsm.sstables);
        let config_ref = lsm.config.clone();
        let data_dir_ref = lsm.data_dir.clone();
        let compaction_flag_ref = Arc::clone(&lsm.compaction_in_progress);

        tokio::spawn(async move {
            while let Some(compaction_type) = compaction_receiver.recv().await {
                debug!("Compaction task received: {compaction_type:?}");

                if let Err(e) = run_compaction_task(
                    compaction_type,
                    &sstables_ref,
                    &config_ref,
                    &data_dir_ref,
                    &compaction_flag_ref,
                )
                .await
                {
                    error!("Compaction failed: {e}");
                }
            }
        });

        Ok(lsm)
    }

    /// Load existing SSTables from disk during startup.
    fn load_existing_sstables(&self) -> Result<()> {
        let data_dir = &self.data_dir;

        if !data_dir.exists() {
            tracing::debug!("Data directory doesn't exist, no SSTables to load");
            return Ok(());
        }

        let mut loaded_count = 0;
        let entries = std::fs::read_dir(data_dir)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("sstable") {
                match SsTable::load_metadata(path.clone()) {
                    Ok(sstable) => {
                        self.sstables.write().push(sstable);
                        loaded_count += 1;
                        tracing::debug!("Loaded SSTable: {:?}", path);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to load SSTable {:?}: {}", path, e);
                    }
                }
            }
        }

        // Sort SSTables by creation time (oldest first)
        self.sstables
            .write()
            .sort_by(|a, b| a.created_at.cmp(&b.created_at));

        tracing::info!("Loaded {} SSTables from disk", loaded_count);
        Ok(())
    }

    /// Replay WAL entries into the memtable during startup.
    fn replay_wal<P: AsRef<Path>>(&self, lsm_path: P) -> Result<()> {
        let wal_path = lsm_path.as_ref().join("wal.log");
        let entries = crate::storage::wal::Wal::replay(wal_path)?;

        let memtable = self.memtable.read();
        let mut replayed_size = 0;
        let mut replayed_count = 0;

        for entry in entries {
            use crate::storage::wal::WalEntryType;

            match entry.entry_type {
                WalEntryType::Insert | WalEntryType::Update => {
                    let key_size = entry.key.len();
                    let value_size = entry.value.len();
                    memtable.insert(entry.key, entry.value);
                    replayed_size += key_size + value_size;
                    replayed_count += 1;
                }
                WalEntryType::Delete => {
                    // Represent deletions as tombstones (empty value) for compaction.
                    memtable.insert(entry.key.clone(), Vec::new());
                    replayed_count += 1;
                }
                WalEntryType::Checkpoint => {
                    debug!("WAL: Found checkpoint at sequence {}", entry.sequence);
                }
                WalEntryType::TxBegin | WalEntryType::TxCommit | WalEntryType::TxRollback => {
                    // Transaction markers are not applied to memtable during replay
                    debug!(
                        "WAL: Found transaction marker {:?} at sequence {}",
                        entry.entry_type, entry.sequence
                    );
                }
            }
        }

        // Update the memtable size
        self.memtable_size.store(replayed_size, Ordering::SeqCst);

        if replayed_count > 0 {
            tracing::info!(
                "Replayed {} entries ({} bytes) from WAL into memtable",
                replayed_count,
                replayed_size
            );

            // If replayed data exceeds memtable threshold, we need to flush it
            // to prevent unbounded WAL growth. Mark as non-replay so flush proceeds.
            if replayed_size > self.config.memtable_size {
                tracing::info!(
                    "Replayed data ({} bytes) exceeds memtable threshold ({} bytes), will flush on first write",
                    replayed_size,
                    self.config.memtable_size
                );
                // Mark as non-replay so the next put() triggers a flush
                self.memtable_is_replay_only.store(false, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    pub async fn scan(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_with_limit(start_key, end_key, None).await
    }

    /// Range scan tuned for small key spans (e.g., a single primary key).
    /// Uses memtable prefix scans and sparse SSTable index blocks to avoid full-file walks.
    pub async fn scan_range_with_limit(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use merge_iterator::MergeIterator;

        let mut sources: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
        let mut remaining = limit;

        // Memtable (most recent)
        let mem_entries = Self::collect_range(&self.memtable.read(), start_key, end_key, remaining);
        remaining = remaining.saturating_sub(mem_entries.len());
        sources.push(mem_entries);

        // Immutable memtables (newest last in vec, so iterate in reverse)
        for immutable_memtable in self.immutable_memtables.read().iter().rev() {
            if remaining == 0 {
                break;
            }
            let entries = Self::collect_range(immutable_memtable, start_key, end_key, remaining);
            remaining = remaining.saturating_sub(entries.len());
            sources.push(entries);
        }

        // SSTables (iterate newest to oldest)
        for sstable in self.sstables.read().iter().rev() {
            if remaining == 0 {
                break;
            }
            if sstable.max_key.as_slice() < start_key || sstable.min_key.as_slice() > end_key {
                continue;
            }
            let entries = self.scan_range_sstable_cached(sstable, start_key, end_key, remaining)?;
            let count = entries.len();
            if count > 0 {
                sources.push(entries);
                remaining = remaining.saturating_sub(count);
            }
        }

        let merger = MergeIterator::new(sources);
        Ok(merger.collect_sorted_limit(limit))
    }

    fn scan_range_sstable_cached(
        &self,
        sstable: &SsTable,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if sstable.index.entries.is_empty() {
            return sstable.scan_range(start_key, end_key, Some(limit));
        }

        let mut results = Vec::new();
        let mut block_idx = sstable
            .index
            .entries
            .partition_point(|e| e.key.as_slice() < start_key);

        if block_idx > 0 {
            block_idx = block_idx.saturating_sub(1);
        }

        while block_idx < sstable.index.entries.len() && results.len() < limit {
            let block_start = sstable.index.entries[block_idx].offset;
            let block_end = if block_idx + 1 < sstable.index.entries.len() {
                sstable.index.entries[block_idx + 1].offset
            } else {
                sstable.data_end_offset
            };

            let entries = if let Some(cache) = &self.block_cache {
                cache.get_or_load(sstable, block_start, block_end)?
            } else {
                Arc::new(sstable.read_block(block_start, block_end)?)
            };

            for (k, v) in entries.iter() {
                if k.as_slice() > end_key {
                    return Ok(results);
                }
                if k.as_slice() >= start_key {
                    results.push((k.clone(), v.clone()));
                    if results.len() >= limit {
                        return Ok(results);
                    }
                }
            }

            block_idx += 1;
        }

        Ok(results)
    }

    fn collect_range(
        map: &SkipMap<Vec<u8>, Vec<u8>>,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = start_key.to_vec();
        let end = end_key.to_vec();
        map.range(start..=end)
            .take(limit)
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Scan the LSM tree in sorted key order using merge iterator
    /// This properly merges memtable, immutable memtables, and SSTables
    pub async fn scan_with_limit(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use merge_iterator::MergeIterator;

        let mut sources: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();

        // Source 0: memtable (most recent)
        let mut memtable_entries = Vec::new();
        for entry in self.memtable.read().iter() {
            if entry.key().as_slice() >= start_key && entry.key().as_slice() <= end_key {
                memtable_entries.push((entry.key().clone(), entry.value().clone()));
            }
        }
        sources.push(memtable_entries);

        // Source 1+: immutable memtables (ordered by recency)
        for immutable_memtable in self.immutable_memtables.read().iter() {
            let mut entries = Vec::new();
            for entry in immutable_memtable.iter() {
                if entry.key().as_slice() >= start_key && entry.key().as_slice() <= end_key {
                    entries.push((entry.key().clone(), entry.value().clone()));
                }
            }
            sources.push(entries);
        }

        // Source N+: SSTables (ordered by level/recency)
        let sstables = self.sstables.read().clone();
        for sstable in sstables.iter() {
            // Check if this SSTable might contain keys in our range
            if sstable.max_key.as_slice() >= start_key && sstable.min_key.as_slice() <= end_key {
                let mut entries = Vec::new();
                match sstable::SsTableIterator::new(sstable) {
                    Ok(iterator) => {
                        for entry_result in iterator {
                            match entry_result {
                                Ok((key, value)) => {
                                    if key.as_slice() >= start_key && key.as_slice() <= end_key {
                                        entries.push((key, value));
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Error reading from SSTable {:?}: {}",
                                        sstable.path,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to create iterator for SSTable {:?}: {}",
                            sstable.path,
                            e
                        );
                    }
                }
                sources.push(entries);
            }
        }

        // Use merge iterator to get sorted, deduplicated results
        let merger = MergeIterator::new(sources);
        let results = if let Some(limit) = limit {
            merger.collect_sorted_limit(limit)
        } else {
            merger.collect_sorted()
        };

        Ok(results)
    }

    /// Scan the LSM tree in REVERSE (descending) key order using reverse merge iterator.
    /// This is the proper way to implement DESC queries, we merge all sources
    /// using a max-heap so largest keys come out first, allowing early termination.
    pub async fn scan_reverse_with_limit(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use merge_iterator::ReverseMergeIterator;

        let mut sources: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();

        // Source 0: memtable (most recent) - iterate in ascending order,
        // the ReverseMergeIterator will handle the descending output
        let mut memtable_entries = Vec::new();
        for entry in self.memtable.read().iter() {
            if entry.key().as_slice() >= start_key && entry.key().as_slice() <= end_key {
                memtable_entries.push((entry.key().clone(), entry.value().clone()));
            }
        }
        sources.push(memtable_entries);

        // Source 1+: immutable memtables (ordered by recency)
        for immutable_memtable in self.immutable_memtables.read().iter() {
            let mut entries = Vec::new();
            for entry in immutable_memtable.iter() {
                if entry.key().as_slice() >= start_key && entry.key().as_slice() <= end_key {
                    entries.push((entry.key().clone(), entry.value().clone()));
                }
            }
            sources.push(entries);
        }

        // Source N+: SSTables (ordered by level/recency)
        let sstables = self.sstables.read().clone();
        for sstable in sstables.iter() {
            // Check if this SSTable might contain keys in our range
            if sstable.max_key.as_slice() >= start_key && sstable.min_key.as_slice() <= end_key {
                let mut entries = Vec::new();
                match sstable::SsTableIterator::new(sstable) {
                    Ok(iterator) => {
                        for entry_result in iterator {
                            match entry_result {
                                Ok((key, value)) => {
                                    if key.as_slice() >= start_key && key.as_slice() <= end_key {
                                        entries.push((key, value));
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Error reading from SSTable {:?}: {}",
                                        sstable.path,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to create iterator for SSTable {:?}: {}",
                            sstable.path,
                            e
                        );
                    }
                }
                sources.push(entries);
            }
        }

        // Use REVERSE merge iterator to get sorted results in DESCENDING order
        let merger = ReverseMergeIterator::new(sources);
        let results = if let Some(limit) = limit {
            merger.collect_sorted_limit(limit)
        } else {
            merger.collect_sorted()
        };

        Ok(results)
    }

    async fn schedule_compaction(&self, compaction_type: CompactionType) -> Result<()> {
        // If a compaction is already running, skip scheduling
        if self.compaction_in_progress.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Only enqueue the request; the compaction task owns the flag
        match self.compaction_sender.try_send(compaction_type) {
            Ok(_) => debug!("Successfully scheduled compaction"),
            Err(e) => error!("Failed to schedule compaction: {e}"),
        }

        Ok(())
    }

    fn calculate_level_size(&self, sstables: &[SsTable], level: usize) -> usize {
        sstables
            .iter()
            .filter(|s| s.level == level)
            .map(|s| s.file_size)
            .sum()
    }

    #[allow(dead_code)]
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Write to internal WAL first for durability (if we have one)
        if let Some(ref wal) = self.wal {
            let sequence = if self.wal_is_async {
                wal.read().append_async(&key, &value)?
            } else {
                wal.write().append(&key, &value)?
            };
            debug!("WAL: Wrote entry with sequence {}", sequence);
        }

        // Do the actual memtable insert
        self.put_to_memtable(key, value).await
    }

    /// Insert directly into memtable without WAL write.
    /// Use this when system-level WAL handles persistence.
    pub async fn put_no_wal(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.put_to_memtable(key, value).await
    }

    /// Internal: insert into memtable and handle flush if needed.
    async fn put_to_memtable(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Mark that memtable now contains new writes (not just replay data)
        self.memtable_is_replay_only.store(false, Ordering::Relaxed);

        // Calculate size of this entry
        let entry_size = key.len() + value.len() + 16; // +16 for overhead

        // Check if we're replacing an existing value
        let old_size = {
            let memtable_guard = self.memtable.read();
            if let Some(old_entry) = memtable_guard.get(&key) {
                old_entry.key().len() + old_entry.value().len() + 16
            } else {
                0
            }
        };

        // Insert into memtable (move without cloning)
        self.memtable.read().insert(key, value);

        // Update size atomically
        if old_size > 0 {
            self.memtable_size.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.memtable_size.fetch_add(entry_size, Ordering::Relaxed);

        // Check if memtable is full
        if self.memtable_size.load(Ordering::Relaxed) > self.config.memtable_size {
            self.flush_memtable().await?;
            self.trigger_compaction_if_needed().await?;

            // Consider checkpointing after major operations (only if we have internal WAL)
            if self.wal.is_some() {
                self.auto_checkpoint_if_needed().await?;
            }
        }

        Ok(())
    }

    /// Get the collection name for this LSM tree.
    #[allow(dead_code)]
    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    #[allow(dead_code)]
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check memtable
        if let Some(entry) = self.memtable.read().get(key) {
            return Ok(Some(entry.value().clone()));
        }

        // Check immutable memtables
        for memtable in self.immutable_memtables.read().iter() {
            if let Some(entry) = memtable.get(key) {
                return Ok(Some(entry.value().clone()));
            }
        }

        // Check SSTables (from newest to oldest)
        for sstable in self.sstables.read().iter().rev() {
            if let Some(value) = sstable.search(key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    async fn flush_memtable(&self) -> Result<()> {
        // Skip flush if memtable only contains data from WAL replay.
        if self.memtable_is_replay_only.load(Ordering::Relaxed) {
            debug!(
                "Memtable contains only replayed WAL data, skipping flush to avoid duplicate SSTables"
            );
            return Ok(());
        }

        // Move current memtable to immutable and replace with a new one
        let old_memtable = {
            let mut memtable_guard = self.memtable.write();
            let old = Arc::clone(&*memtable_guard);
            *memtable_guard = Arc::new(SkipMap::new());
            old
        };

        self.memtable_size.store(0, Ordering::Relaxed);
        self.memtable_is_replay_only.store(true, Ordering::Relaxed);

        // Check if memtable is empty before processing
        if old_memtable.is_empty() {
            debug!("Memtable is empty, skipping flush");
            return Ok(());
        }

        self.immutable_memtables.write().push(old_memtable.clone());

        // Create SSTable synchronously (await the result)
        let immutable = old_memtable;
        let data_dir = self.data_dir.clone();

        // Build the SSTable at L0
        let mut builder = sstable::SsTableBuilder::new(0); // Level 0 for new SSTables

        for entry in immutable.iter() {
            builder.add(entry.key().clone(), entry.value().clone())?;
        }

        // Finish and write the SSTable
        let new_sstable = builder.finish(&data_dir).await?;

        // Add to the SSTable list
        self.sstables.write().push(new_sstable);

        // Remove from immutable list
        self.immutable_memtables
            .write()
            .retain(|m| !Arc::ptr_eq(m, &immutable));

        Ok(())
    }

    async fn trigger_compaction_if_needed(&self) -> Result<()> {
        if self.compaction_in_progress.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Check triggers and schedule compaction if needed
        let compaction_type = {
            let sstables = self.sstables.read();

            // Check L0 trigger
            let level0_files = sstables.iter().filter(|s| s.level == 0).count();
            if level0_files >= self.config.level0_file_num_compaction_trigger {
                Some(CompactionType::Level0)
            } else {
                // Check size-based triggers for other levels
                let mut level_to_compact = None;
                for level in 1..=self.config.max_level {
                    let level_size = self.calculate_level_size(&sstables, level);
                    let max_size = self.calculate_max_level_size(level);

                    if level_size > max_size {
                        level_to_compact = Some(CompactionType::Level(level));
                        break;
                    }
                }
                level_to_compact
            }
        };

        // Now we can safely await without holding the lock
        if let Some(compaction_type) = compaction_type {
            self.schedule_compaction(compaction_type).await?;
        }

        Ok(())
    }

    fn calculate_max_level_size(&self, level: usize) -> usize {
        if level == 0 {
            self.config.level0_size
        } else {
            self.config.level0_size * self.config.level_multiplier.pow(level as u32)
        }
    }

    /// Flush the current memtable to disk as an SSTable.
    pub async fn flush(&self) -> Result<()> {
        debug!("Flushing LSM tree memtable to disk");
        self.flush_memtable().await
    }

    /// Delete a key-value pair (writes a tombstone).
    #[allow(dead_code)]
    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        // Write delete entry to internal WAL first (if we have one)
        if let Some(ref wal) = self.wal {
            let sequence = {
                let wal_r = wal.read();
                if wal_r.is_async() {
                    wal_r.append_with_type_async(
                        &key,
                        &[],
                        crate::storage::wal::WalEntryType::Delete,
                    )?
                } else {
                    drop(wal_r);
                    wal.write().append_with_type(
                        &key,
                        &[],
                        crate::storage::wal::WalEntryType::Delete,
                    )?
                }
            };
            debug!("WAL: Wrote delete entry with sequence {}", sequence);
        }

        self.delete_from_memtable(key).await
    }

    /// Delete directly from memtable without WAL write.
    /// Use this when system-level WAL handles persistence.
    pub async fn delete_no_wal(&self, key: Vec<u8>) -> Result<()> {
        self.delete_from_memtable(key).await
    }

    /// Internal: insert tombstone into memtable and handle flush if needed.
    async fn delete_from_memtable(&self, key: Vec<u8>) -> Result<()> {
        // Insert tombstone into memtable (empty value indicates deletion)
        let entry_size = key.len() + 16; // +16 for overhead

        // Check if we're replacing an existing value
        let old_size = {
            let memtable_guard = self.memtable.read();
            if let Some(old_entry) = memtable_guard.get(&key) {
                old_entry.key().len() + old_entry.value().len() + 16
            } else {
                0
            }
        };

        // Insert tombstone (empty value)
        self.memtable.read().insert(key, Vec::new());

        // Update size atomically
        if old_size > 0 {
            self.memtable_size.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.memtable_size.fetch_add(entry_size, Ordering::Relaxed);

        // Check if memtable is full
        if self.memtable_size.load(Ordering::Relaxed) > self.config.memtable_size {
            self.flush_memtable().await?;
            self.trigger_compaction_if_needed().await?;

            // Consider checkpointing after major operations (only if we have internal WAL)
            if self.wal.is_some() {
                self.auto_checkpoint_if_needed().await?;
            }
        }

        Ok(())
    }

    /// Create a comprehensive checkpoint in the WAL with SSTable state.
    /// Returns None if no internal WAL is configured.
    pub async fn checkpoint(&self) -> Result<Option<u64>> {
        let Some(ref wal) = self.wal else {
            return Ok(None);
        };

        // Get list of all current SSTable files
        let sstable_files: Vec<String> = self
            .sstables
            .read()
            .iter()
            .map(|sstable| {
                sstable
                    .path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        // Create checkpoint with comprehensive metadata
        let sequence = wal.write().checkpoint(sstable_files.clone(), 1)?; // Schema version 1 for now

        info!(
            "Created LSM checkpoint at sequence {} with {} SSTable files",
            sequence,
            sstable_files.len()
        );

        // Check if we should truncate the WAL after checkpointing
        if wal.read().should_truncate() {
            info!("WAL size threshold reached, performing truncation");
            wal.write().truncate_to_checkpoint()?;
        }

        Ok(Some(sequence))
    }

    /// Wait for the WAL to be fsynced up to at least the given sequence.
    /// Only effective in async WAL mode. Returns Ok(true) if reached, Ok(false) on timeout.
    /// Returns Ok(true) immediately if no internal WAL is configured.
    pub async fn wal_commit_barrier(
        &self,
        sequence: u64,
        timeout: Option<std::time::Duration>,
    ) -> Result<bool> {
        let Some(ref wal) = self.wal else {
            return Ok(true); // No WAL = nothing to wait for
        };
        let wal_guard = wal.read();
        wal_guard
            .commit_barrier(sequence, timeout)
            .map_err(|e| monodb_common::MonoError::Storage(format!("commit barrier: {}", e)))
    }

    /// Wait until current writes are durable (to current next_sequence - 1).
    /// Returns Ok(true) immediately if no internal WAL is configured.
    pub async fn wal_commit_current(&self, timeout: Option<std::time::Duration>) -> Result<bool> {
        let Some(ref wal) = self.wal else {
            return Ok(true);
        };
        let stats = wal.read().stats();
        let target = stats.next_sequence.saturating_sub(1);
        self.wal_commit_barrier(target, timeout).await
    }

    /// Create a checkpoint automatically after major operations
    /// Does nothing if no internal WAL is configured.
    pub async fn auto_checkpoint_if_needed(&self) -> Result<()> {
        let Some(ref wal) = self.wal else {
            return Ok(());
        };

        let wal_stats = wal.read().stats();

        // Create checkpoint if:
        // 1. We have written more than 1000 entries since last checkpoint, OR
        // 2. WAL is larger than 50% of max size
        let should_checkpoint =
            if let Some(last_checkpoint_seq) = wal_stats.last_checkpoint_sequence {
                let entries_since_checkpoint =
                    wal_stats.next_sequence.saturating_sub(last_checkpoint_seq);
                entries_since_checkpoint > 1000
                    || (wal_stats.current_size as f64 / wal_stats.max_size as f64) > 0.5
            } else {
                // No checkpoint exists, create one if we have significant activity
                wal_stats.next_sequence > 100
            };

        if should_checkpoint {
            debug!(
                "Auto-checkpoint triggered: {} entries since last checkpoint",
                wal_stats
                    .next_sequence
                    .saturating_sub(wal_stats.last_checkpoint_sequence.unwrap_or(0))
            );
            self.checkpoint().await?;
        }

        Ok(())
    }

    /// Force sync the WAL to disk.
    /// Does nothing if no internal WAL is configured.
    pub async fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.write().sync()?;
        }
        Ok(())
    }

    /// Get WAL statistics. Returns None if no internal WAL is configured.
    pub fn wal_stats(&self) -> Option<crate::storage::wal::WalStats> {
        self.wal.as_ref().map(|wal| wal.read().stats())
    }

    /// Check if this LSM tree has an internal WAL.
    #[allow(dead_code)]
    pub fn has_internal_wal(&self) -> bool {
        self.wal.is_some()
    }

    /// Clear the WAL after successful compaction
    #[allow(dead_code)]
    pub async fn clear_wal_after_compaction(&self) -> Result<()> {
        // This should only be called after a successful full compaction
        // when all WAL entries have been written to SSTables
        let wal_path = self.data_dir.parent().unwrap().join("wal.log");
        crate::storage::wal::Wal::clear(wal_path)
    }
    #[cfg(test)]
    pub async fn force_compact_level0_now(&self) -> monodb_common::Result<()> {
        use crate::storage::lsm::compaction::{CompactionType, run_compaction_task};
        run_compaction_task(
            CompactionType::Level0,
            &self.sstables,
            &self.config,
            &self.data_dir,
            &self.compaction_in_progress,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LsmConfig;
    use std::{fs, path::PathBuf, sync::Arc};

    const WAL_CONFIG: WalConfig = WalConfig {
        buffer_size: 65536,
        max_size: 67108864,
        sync_on_write: true,
        async_write: false,
        sync_every_bytes: None,
        sync_interval_ms: None,
    };

    struct TempDirCleaner(PathBuf);
    impl Drop for TempDirCleaner {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
            // Try to remove the parent Temp directory if it's empty
            if let Some(parent) = self.0.parent() {
                let _ = fs::remove_dir(parent);
            }
        }
    }

    fn temp_dir(name: &str) -> (PathBuf, TempDirCleaner) {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let dir = PathBuf::from(format!("Temp/lsm_test_{}_{}_{}", name, pid, nanos));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let cleaner = TempDirCleaner(dir.clone());
        (dir, cleaner)
    }

    /// Verify that inserting entries beyond memtable limit triggers flush → SSTable creation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_memtable_flush_trigger() {
        let (dir, _cleanup) = temp_dir("flush_test");
        let config = LsmConfig {
            memtable_size: 10 * 1024, // 10KB
            ..Default::default()
        };

        let lsm = Arc::new(LsmTree::new(&dir, config, WAL_CONFIG).unwrap());

        // Insert enough data to exceed threshold (100 * 512 bytes = 50KB > 10KB)
        for i in 0..100 {
            let key = format!("key_{:05}", i).into_bytes();
            let val = vec![b'x'; 512];
            lsm.put(key, val).await.unwrap();
        }

        // Give background flush some time
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let count = lsm.sstables.read().len();
        assert!(
            count >= 1,
            "Expected at least one SSTable after memtable flush, found {}",
            count
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compaction_level_trigger() {
        let (dir, _cleanup) = temp_dir("compaction_test");
        let cfg = LsmConfig {
            memtable_size: 1024,
            level0_file_num_compaction_trigger: 4,
            ..Default::default()
        };

        let lsm = Arc::new(LsmTree::new(&dir, cfg, WAL_CONFIG).unwrap());

        // Insert data and flush - now synchronous, no waiting needed
        for batch in 0..8 {
            for k in 0..100 {
                let key = format!("key_{:04}", k).into_bytes();
                let val = format!("val_{batch}").into_bytes();
                lsm.put(key, val).await.unwrap();
            }

            // Flush is now synchronous - it completes before returning
            lsm.flush().await.unwrap();
        }

        // Check state before compaction
        let l0_count_before = lsm.sstables.read().iter().filter(|s| s.level == 0).count();
        let total_before = lsm.sstables.read().len();

        // Count actual files on disk
        let files_on_disk: Vec<_> = std::fs::read_dir(&lsm.data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sstable"))
            .collect();

        println!(
            "Before compaction: {} total SSTables, {} at L0",
            total_before, l0_count_before
        );
        println!("Files on disk: {}", files_on_disk.len());

        // Verify consistency between memory and disk
        assert_eq!(
            total_before,
            files_on_disk.len(),
            "In-memory SSTable count ({}) should match files on disk ({})",
            total_before,
            files_on_disk.len()
        );

        // Verify we have multiple L0 tables to compact
        assert!(
            l0_count_before >= 2,
            "Need at least 2 L0 tables for compaction, found {}",
            l0_count_before
        );

        // Run compaction
        lsm.force_compact_level0_now().await.unwrap();

        // Check state after compaction
        let l0_count_after = lsm.sstables.read().iter().filter(|s| s.level == 0).count();
        let total_after = lsm.sstables.read().len();
        let l1_count_after = lsm.sstables.read().iter().filter(|s| s.level == 1).count();

        println!(
            "After compaction: {} total SSTables ({} L0, {} L1)",
            total_after, l0_count_after, l1_count_after
        );

        // Assertions
        assert_eq!(
            l0_count_after, 0,
            "Expected 0 L0 tables after compaction, found {}",
            l0_count_after
        );

        assert!(
            l1_count_after >= 1,
            "Expected at least 1 L1 table after compaction, found {}",
            l1_count_after
        );

        assert!(
            total_after < total_before,
            "Expected reduction in total tables: before={}, after={}",
            total_before,
            total_after
        );

        // Verify data integrity
        for k in 0..10 {
            let key = format!("key_{:04}", k).into_bytes();
            let value = lsm.get(&key).await.unwrap();
            assert!(
                value.is_some(),
                "Key {:?} should exist after compaction",
                key
            );
        }
    }
}

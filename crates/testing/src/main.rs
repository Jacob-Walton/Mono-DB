use anyhow::Result;
use std::sync::Arc;

use monodb_common::schema::{KeySpacePersistence, Schema};
use monodb_server::config::Config as ServerConfig;
use monodb_server::storage::Data;
use monodb_server::storage::engine::StorageEngine;

#[derive(Debug, Clone)]
struct EngineOptions {
    collection: String,
    key_prefix: String,
    key_binary: bool,
    wal_sync_on_write: Option<bool>,
    memtable_size: Option<usize>,
    wal_buffer_size: Option<usize>,
    wal_sync_every_bytes: Option<u64>,
    wal_sync_interval_ms: Option<u64>,
    use_lsm: Option<bool>,
    wal_async: bool,
    data_dir: Option<String>,
    progress_every: Option<usize>,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            collection: "kv_bench".to_string(),
            key_prefix: "rec:".to_string(),
            key_binary: false,
            wal_sync_on_write: Some(false),
            memtable_size: Some(128 * 1024 * 1024),
            wal_buffer_size: Some(1 * 1024 * 1024),
            wal_sync_every_bytes: None,
            wal_sync_interval_ms: None,
            use_lsm: Some(true),
            wal_async: false,
            data_dir: None,
            progress_every: None,
        }
    }
}

async fn open_engine(opts: &EngineOptions) -> Result<Arc<StorageEngine>> {
    let mut cfg = ServerConfig::load_from_path("config.toml").unwrap_or_default();
    if let Some(b) = opts.use_lsm {
        cfg.storage.use_lsm = b;
    }
    if let Some(sz) = opts.memtable_size {
        cfg.storage.lsm.memtable_size = sz;
    }
    if let Some(b) = opts.wal_sync_on_write {
        cfg.storage.wal.sync_on_write = b;
    }
    if let Some(bs) = opts.wal_buffer_size {
        cfg.storage.wal.buffer_size = bs;
    }
    if let Some(seb) = opts.wal_sync_every_bytes {
        cfg.storage.wal.sync_every_bytes = Some(seb);
    }
    if let Some(sim) = opts.wal_sync_interval_ms {
        cfg.storage.wal.sync_interval_ms = Some(sim);
    }
    cfg.storage.wal.async_write = opts.wal_async;
    if let Some(dir) = &opts.data_dir {
        cfg.storage.data_dir = dir.clone();
    }
    let engine = StorageEngine::new(cfg.storage).await?;
    Ok(engine)
}

async fn ensure_kv_collection_with_persistence(
    engine: &Arc<StorageEngine>,
    collection: &str,
    persistence: KeySpacePersistence,
) -> Result<()> {
    if engine.schema(collection).is_none() {
        let schema = Schema::KeySpace {
            name: collection.to_string(),
            ttl_enabled: false,
            max_size: None,
            persistence,
        };
        engine.create_collection(schema).await?;
    }
    Ok(())
}

async fn ensure_kv_collection(engine: &Arc<StorageEngine>, collection: &str) -> Result<()> {
    ensure_kv_collection_with_persistence(engine, collection, KeySpacePersistence::Persistent).await
}

#[derive(Debug, Clone)]
struct WritePlan {
    total_records: u64,
    value_size: usize,
    start_id: u64,
    concurrency: usize,
}

async fn write_on_the_fly(
    engine: Arc<StorageEngine>,
    opts: &EngineOptions,
    plan: &WritePlan,
) -> Result<()> {
    ensure_kv_collection(&engine, &opts.collection).await?;
    let start = std::time::Instant::now();

    let per_worker = plan.total_records / plan.concurrency as u64;
    let remainder = plan.total_records % plan.concurrency as u64;

    let mut joins = Vec::new();
    for w in 0..plan.concurrency {
        let engine_cloned = Arc::clone(&engine);
        let opts_cloned = opts.clone();
        let value_size = plan.value_size;

        let extra = if w == plan.concurrency - 1 {
            remainder
        } else {
            0
        };
        let my_count = per_worker + extra;
        let my_start = plan.start_id + (w as u64) * per_worker;

        let handle = tokio::spawn(async move {
            use std::fmt::Write as _;
            let mut key_str = String::with_capacity(opts_cloned.key_prefix.len() + 24);
            let prefix_bytes = opts_cloned.key_prefix.as_bytes().to_vec();
            let mut key_bin = Vec::with_capacity(prefix_bytes.len() + 8);
            let mut value = vec![0u8; value_size];

            for i in 0..my_count {
                let id = my_start + i;

                // Prepare key
                if opts_cloned.key_binary {
                    key_bin.clear();
                    key_bin.extend_from_slice(&prefix_bytes);
                    key_bin.extend_from_slice(&id.to_le_bytes());
                } else {
                    key_str.clear();
                    key_str.push_str(&opts_cloned.key_prefix);
                    let _ = write!(&mut key_str, "{}", id);
                }

                // Prepare value
                value.fill((id as u8).wrapping_mul(31));
                if value.len() >= 8 {
                    value[..8].copy_from_slice(&id.to_le_bytes());
                }

                if opts_cloned.key_binary {
                    engine_cloned
                        .insert(
                            &opts_cloned.collection,
                            &Data::KeyValue {
                                key: &key_bin,
                                value: &value,
                            },
                        )
                        .await?;
                } else {
                    engine_cloned
                        .insert(
                            &opts_cloned.collection,
                            &Data::KeyValue {
                                key: key_str.as_bytes(),
                                value: &value,
                            },
                        )
                        .await?;
                }

                if let Some(p) = opts_cloned.progress_every {
                    if p > 0 && (i + 1) % (p as u64) == 0 {
                        println!("worker {}: inserted {} records (last id {} )", w, i + 1, id);
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
        joins.push(handle);
    }

    for j in joins {
        j.await??;
    }

    // Measure ingest time separately from flush time to isolate WAL impact
    let ingest_secs = start.elapsed().as_secs_f64();
    let value_mb = (plan.total_records as f64 * plan.value_size as f64) / (1024.0 * 1024.0);
    let ingest_mbps = if ingest_secs > 0.0 {
        value_mb / ingest_secs
    } else {
        0.0
    };
    let ingest_rps = if ingest_secs > 0.0 {
        plan.total_records as f64 / ingest_secs
    } else {
        0.0
    };
    println!(
        "ingest: records={}, value_size={}, time={:.3}s, value_MB={:.2}, value_MB/s={:.2}, rec/s={:.0}",
        plan.total_records, plan.value_size, ingest_secs, value_mb, ingest_mbps, ingest_rps
    );

    // Now measure flush (SSTable write) separately
    let flush_start = std::time::Instant::now();
    engine.flush().await?;
    let flush_secs = flush_start.elapsed().as_secs_f64();
    println!("flush: time={:.3}s", flush_secs);
    Ok(())
}

fn parse_bytes(s: &str) -> Option<u64> {
    let lower = s.to_ascii_lowercase();
    let (num, mul) = if lower.ends_with("gib") || lower.ends_with("gb") || lower.ends_with('g') {
        (
            lower.trim_end_matches(|c: char| c == 'g' || c == 'b' || c == 'i'),
            1024u64 * 1024 * 1024,
        )
    } else if lower.ends_with("mib") || lower.ends_with("mb") || lower.ends_with('m') {
        (
            lower.trim_end_matches(|c: char| c == 'm' || c == 'b' || c == 'i'),
            1024u64 * 1024,
        )
    } else if lower.ends_with("kib") || lower.ends_with("kb") || lower.ends_with('k') {
        (
            lower.trim_end_matches(|c: char| c == 'k' || c == 'b' || c == 'i'),
            1024u64,
        )
    } else {
        (lower.as_str(), 1u64)
    };
    num.parse::<f64>().ok().map(|n| (n * mul as f64) as u64)
}

fn parse_bytes_u64(s: &str) -> Result<u64, String> {
    parse_bytes(s).ok_or_else(|| format!("invalid size: {}", s))
}
fn parse_bytes_usize(s: &str) -> Result<usize, String> {
    parse_bytes(s)
        .and_then(|v| usize::try_from(v).ok())
        .ok_or_else(|| format!("invalid size: {}", s))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Preset benchmark parameters
    let value_size = 1024; // 1 KiB values
    let total_records = 100_000; // 100k writes per scenario
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    println!("MonoDB storage benchmark preset comparison");
    println!(
        "Records: {total_records}, Value size: {value_size} bytes, Concurrency: {concurrency}\n"
    );

    let plan = WritePlan {
        total_records,
        value_size,
        start_id: 0,
        concurrency,
    };

    // Scenario 1: Full WAL (durable)
    let mut opts_full = EngineOptions::default();
    opts_full.collection = "kv_bench".to_string();
    opts_full.data_dir = Some("./data/kv_bench/full_wal".to_string());
    opts_full.wal_sync_on_write = Some(true);
    opts_full.wal_async = false;
    opts_full.wal_buffer_size = Some(4 * 1024 * 1024); // 4 MiB buffer for throughput
    // Enable group-commit for durability + throughput
    opts_full.wal_sync_every_bytes = Some(1 * 1024 * 1024); // 1 MiB
    opts_full.wal_sync_interval_ms = Some(10); // 10 ms
    run_scenario(
        "Full WAL (durable)",
        &opts_full,
        &plan,
        KeySpacePersistence::Persistent,
    )
    .await?;

    // Scenario 2: High-performance WAL (reduced durability)
    let mut opts_perf = EngineOptions::default();
    opts_perf.collection = "kv_bench".to_string();
    opts_perf.data_dir = Some("./data/kv_bench/high_perf_wal".to_string());
    opts_perf.wal_sync_on_write = Some(false);
    opts_perf.wal_async = true;
    opts_perf.wal_buffer_size = Some(1 * 1024 * 1024); // 1 MiB
    run_scenario(
        "High-performance WAL",
        &opts_perf,
        &plan,
        KeySpacePersistence::Persistent,
    )
    .await?;

    // Scenario 3: No WAL (in-memory keyspace for demo)
    // Note: Engine currently always uses WAL for persistent stores.
    // Using an in-memory keyspace demonstrates write throughput without WAL/disk.
    let mut opts_mem = EngineOptions::default();
    opts_mem.collection = "kv_bench_mem".to_string();
    opts_mem.data_dir = Some("./data/kv_bench/no_wal_demo".to_string());
    // WAL settings irrelevant for memory keyspace, but set to minimal anyway
    opts_mem.wal_sync_on_write = Some(false);
    opts_mem.wal_async = false;
    run_scenario(
        "No WAL (in-memory)",
        &opts_mem,
        &plan,
        KeySpacePersistence::Memory,
    )
    .await?;

    Ok(())
}

async fn run_scenario(
    name: &str,
    opts: &EngineOptions,
    plan: &WritePlan,
    persistence: KeySpacePersistence,
) -> Result<()> {
    println!("=== {name} ===");
    let engine = open_engine(opts).await?;
    ensure_kv_collection_with_persistence(&engine, &opts.collection, persistence).await?;
    write_on_the_fly(engine, opts, plan).await?;
    println!("\n");
    Ok(())
}

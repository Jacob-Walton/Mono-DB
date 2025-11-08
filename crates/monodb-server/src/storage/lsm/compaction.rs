//! Compaction routines for the LSM tree.

use monodb_common::Result;
use parking_lot::RwLock;
use std::{
    collections::BinaryHeap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tracing::{debug, error, info};

use crate::storage::lsm::LsmConfig;
use crate::storage::lsm::sstable::{
    HeapEntry, SsTable, SsTableBuilder, SsTableIterator, is_tombstone,
};

#[derive(Debug, Clone, Copy)]
pub enum CompactionType {
    Level0,
    Level(usize),
}

struct CompactionGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for CompactionGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

// Derive an approximate size target for a level from config.
fn target_bytes_for_level(level: usize, cfg: &LsmConfig) -> usize {
    if level == 0 {
        cfg.level0_size
    } else {
        cfg.level0_size
            .saturating_mul(cfg.level_multiplier.saturating_pow(level as u32))
    }
}

// Merge many SSTables into the next level.
async fn merge_sstables(
    inputs: &[SsTable],
    target_level: usize,
    cfg: &LsmConfig,
    data_dir: &Path,
) -> Result<Vec<SsTable>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    info!(
        "Merging {} SSTables into L{} (target size {} B)",
        inputs.len(),
        target_level,
        target_bytes_for_level(target_level, cfg)
    );

    let mut heap = BinaryHeap::new();
    let mut iters = Vec::new();

    // Open iterators for all inputs and seed the heap with first entries
    for (rank, t) in inputs.iter().enumerate() {
        match SsTableIterator::new(&t.path) {
            Ok(mut it) => {
                if let Some(Ok((k, v))) = it.next() {
                    heap.push(HeapEntry {
                        key: k,
                        value: v,
                        sstable_source_index: rank,
                    });
                }
                iters.push(it);
            }
            Err(e) => return Err(e.into()),
        }
    }

    let split_at = {
        // Aim for roughly 8 files if total is small; otherwise clamp to a
        // window around the configured level size.
        let mut total_bytes = 0u64;
        for t in inputs {
            if let Ok(meta) = tokio::fs::metadata(&t.path).await {
                total_bytes += meta.len();
            }
        }
        let desired = (total_bytes / 8).max(1);
        desired.clamp(
            (target_bytes_for_level(target_level, cfg) / 2) as u64,
            (target_bytes_for_level(target_level, cfg) * 2) as u64,
        ) as usize
    };

    let mut builder = SsTableBuilder::new(target_level);
    let mut outputs = Vec::new();
    let mut current_size = 0usize;
    let mut written = 0usize;

    while let Some(entry) = heap.pop() {
        let key = entry.key;
        let mut val = entry.value;
        let mut best_rank = entry.sstable_source_index;
        let mut to_advance = vec![entry.sstable_source_index];

        // Collapse duplicates: keep the value from the "newest" source
        while let Some(peek) = heap.peek() {
            if peek.key == key {
                let next = heap.pop().unwrap();
                to_advance.push(next.sstable_source_index);
                if next.sstable_source_index > best_rank {
                    best_rank = next.sstable_source_index;
                    val = next.value;
                }
            } else {
                break;
            }
        }

        if !is_tombstone(&val) {
            builder.add(key.clone(), val.clone())?;
            written += 1;
            current_size += key.len() + val.len() + 16;
        }

        // Periodically flush a chunk to keep files reasonably sized
        if current_size >= split_at && builder.has_data() {
            let out = builder.finish(data_dir).await?;
            debug!(
                "Created partial L{} table {:?} ({} entries, {} B est)",
                target_level, out.path, out.num_entries, current_size
            );
            outputs.push(out);
            builder = SsTableBuilder::new(target_level);
            current_size = 0;
        }

        for r in to_advance {
            if let Some(it) = iters.get_mut(r) {
                if let Some(Ok((k2, v2))) = it.next() {
                    heap.push(HeapEntry {
                        key: k2,
                        value: v2,
                        sstable_source_index: r,
                    });
                }
            }
        }
    }

    if builder.has_data() {
        let out = builder.finish(data_dir).await?;
        outputs.push(out);
    }

    info!(
        "Merge finished: {} entries, {} output file(s)",
        written,
        outputs.len()
    );

    Ok(outputs)
}

pub async fn run_compaction_task(
    ty: CompactionType,
    sstables: &Arc<RwLock<Vec<SsTable>>>,
    cfg: &LsmConfig,
    data_dir: &Path,
    in_progress: &Arc<AtomicBool>,
) -> Result<()> {
    if in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        debug!("Compaction already running");
        return Ok(());
    }
    let _guard = CompactionGuard {
        flag: Arc::clone(in_progress),
    };

    match ty {
        CompactionType::Level0 => {
            let inputs: Vec<SsTable> = {
                let g = sstables.read();
                g.iter().filter(|s| s.level == 0).cloned().collect()
            };

            if inputs.len() < 2 {
                debug!("L0 compaction skipped, {} table(s)", inputs.len());
                return Ok(());
            }

            info!("Starting L0→L1 with {} tables", inputs.len());
            let outputs = merge_sstables(&inputs, 1, cfg, data_dir).await?;
            replace_sstables(sstables, &inputs, outputs).await?;
        }
        CompactionType::Level(lv) => {
            let inputs: Vec<SsTable> = {
                let g = sstables.read();
                g.iter().filter(|s| s.level == lv).cloned().collect()
            };
            if inputs.len() < 2 {
                debug!("L{} compaction skipped, only {} tables", lv, inputs.len());
                return Ok(());
            }

            info!("Starting L{}→L{} with {} tables", lv, lv + 1, inputs.len());
            let outputs = merge_sstables(&inputs, lv + 1, cfg, data_dir).await?;
            replace_sstables(sstables, &inputs, outputs).await?;
        }
    }
    Ok(())
}

async fn replace_sstables(
    sstables: &Arc<RwLock<Vec<SsTable>>>,
    old_tables: &[SsTable],
    mut new_tables: Vec<SsTable>,
) -> Result<()> {
    // enforce correct level on outputs
    if let Some(first) = new_tables.first() {
        let lvl = first.level;
        for t in &mut new_tables {
            t.level = lvl;
        }
    }

    // Match by file name to avoid path normalization issues
    let old_names: Vec<_> = old_tables
        .iter()
        .filter_map(|s| s.path.file_name().map(|n| n.to_owned()))
        .collect();

    {
        let mut g = sstables.write();
        let before = g.len();
        g.retain(|s| {
            if let Some(n) = s.path.file_name() {
                !old_names.iter().any(|x| x == n)
            } else {
                true
            }
        });
        g.extend(new_tables);
        g.sort_by(|a, b| {
            a.level
                .cmp(&b.level)
                .then_with(|| a.min_key.cmp(&b.min_key))
        });
        info!("SSTable list: {} → {}", before, g.len());
    }

    // Delete old files on disk
    for t in old_tables {
        if let Err(e) = tokio::fs::remove_file(&t.path).await {
            error!("Failed to delete {:?}: {}", t.path, e);
        } else {
            debug!("Deleted {:?}", t.path);
        }
    }

    Ok(())
}

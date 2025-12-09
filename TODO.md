# To-Do

## Completed

- [x] Switch B-Tree to per-collection directories with their own `data.db`, `meta.db`, and `wal.log`.
- [x] Add validation for constraints like uniqueness
- [x] Persist/rehydrate B-Tree metadata (root page id, etc.) so trees reopen cleanly across restarts.
- [x] Implement the rest of the tasks in `tasks.py`

## Started

- [ ] There are multiple failing tests due to us rewriting the format of execution results.

## Not Started

- [ ] Clean up lsm module
  - [ ] Clean up compaction, make logs debug! instead of info!
  - [ ] Remove unnecessary logs
  - [ ] Clean up comments
- [ ] B-trees and LSM-trees's SSTables aren't reused from the disk
- [ ] Error render should be client-side so we can send smaller data
- [ ] Sharded storage for multi-core scaling (read first)
  - Goal: Scale write/flush/compaction across cores by sharding each collection into S shards (S = CPU cores).
  - Routing: Hash-based (e.g., xxhash) on key → shard id. Keep key order within a shard.
  - Per-shard components: independent memtable, WAL, SSTable dir, compaction workers.
  - WAL: one WAL per shard. Async writer with group commit as default. Optional commit barrier per shard.
  - Flushing/Compaction: limit concurrent compactions per shard; global cap to avoid I/O thrash.
  - Reads: Fan-out to shards when scanning ranges with overlapping shard keyspaces; simple point-lookup uses shard only.
  - Recovery: Replay WAL per shard, rebuild per-shard state.
  - Config: `storage.shards = auto | <N>`; default to `auto = number_of_cores`.
  - Telemetry: Per-shard metrics (memtable size, WAL lag, compaction backlog) to monitor skew.


## Out of Scope

- [ ] Consider unifying WAL/transactions across multiple B-Trees for cross-collection operations.
- [ ] Implement robust B-Tree delete rebalancing (redistribution/merge and interior key updates) beyond current lightweight pointer fix-up.
- [ ] Add a streaming iterator + pagination for B-Tree scans.
- [ ] Segment large files per collection (e.g., 1GB segments) to match PostgreSQL-style relation segments.

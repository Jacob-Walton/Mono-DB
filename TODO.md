# To-Do

## High-Priority

### Client Library & Protocol

- [ ] Redesign the client library API.
- [ ] Add client-side tracking of idle/expired connections.
- [ ] Improve connection robustness and retry behaviour.
- [ ] Add an improved handshake phase to the wire protocol.
- [x] Add a “list tables” request to the codec.
- [ ] Make response formats more flexible and less wasteful.
- [ ] Move error rendering fully client-side to allow lighter error packets.
- [ ] Add parameter binding to queries to prevent injection attacks.

### Storage Engine

- [x] Reimplement and optimise the buffer pool.
- [x] Replace bincode with manual encoding/decoding throughout the codec to remove overhead.
- [ ] Add support for secondary indexes.
- [ ] Add the option to choose B-Tree or LSM per table, based on declared priority (read/write).
- [ ] Allow full database namespaces rather than a single flat collection set.
- [ ] Ensure B-Trees and LSM SSTables are correctly reused from disk across restarts.
- [ ] Rework how result rows are represented to reduce copying and temporary allocations.

### Query Planning & Execution

- [ ] Implement a basic query planner and optimiser.
- [ ] Add reliable variable handling throughout the evaluator.
- [ ] Fully implement the `CHANGE` command.
- [ ] Add MVCC for correct, atomic multi-statement transactions.
- [ ] Introduce full session tracking and basic authentication.

---

## Completed

- [x] Switch B-Trees to per-collection directories with their own `data.db`, `meta.db`, and `wal.log`.
- [x] Add validation for constraints (e.g., uniqueness).
- [x] Persist/rehydrate B-Tree metadata so trees reopen correctly.
- [x] Implement all outstanding tasks in `tasks.py`.
- [x] Fix tests broken by result-format changes.
- [x] Clean up the LSM module:

  - [x] Improve compaction and reduce logging noise.
  - [x] Remove dead code.
  - [x] Clean up comments.

---

## Started

_(None currently tracked.)_

---

## Not Started

- [ ] Reuse persisted B-Tree nodes and LSM SSTables on startup instead of recreating state in memory.
- [ ] Shift detailed error rendering into the client.
- [ ] Redesign result data structures to avoid wasteful allocation.
- [ ] Add support for secondary indexes.
- [ ] Implement MVCC.

---

## If Time Allows / Stretch Goals

- [ ] Window functions.
- [ ] Richer validation rules.
- [ ] Plugin system for custom functions and operators.
- [ ] Additional language keywords or expanded stdlib functions as needed.

---

## Out of Scope (For Now)

- [ ] Unified WAL/transaction layer across collections for multi-collection atomic operations.
- [ ] Full B-Tree delete rebalancing (redistribution/merge + in-tree key updates).
- [ ] Streaming iterators with pagination for large scans.
- [ ] Sharded multi-core storage engine:
  - Per-shard memtable, WAL, and compaction.
  - Hash-based key routing.
  - WAL group commit per shard.
  - Controlled compaction concurrency.
  - Fan-out reads for ranges across shards.
  - Per-shard recovery and metrics.
  - Configurable shard count (`auto` = number of cores).

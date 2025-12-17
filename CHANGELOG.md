# Changelog

## [0.2.5] - 2025-12-17

### Added

- Introduced a new B+Tree data structure for efficient indexing and range queries.
- Added a new utility crate with the `mdb_ready` tool for server readiness checks.
- Added `Serializable` trait implementations for common types (i32, i64, u32, u64, String, Vec<u8>).

### Changed

- Improved cross-platform build logic in `tasks.py` (now detects RAM and CPU count on Windows, Linux, macOS; removed psutil dependency).
- Refactored and expanded test coverage for database operations and performance.

### Removed

- Old in-memory BTree and legacy test code in favor of client-driven, end-to-end benchmarks.
- LSM tree storage backend.

## [0.2.4] - 2025-12-15

### Added

- SSTable block cache with configurable `block_cache_capacity` and footer-backed sparse index loading
- Primary key fast path in planner/executor plus `required` column constraint support
- Point-read benchmark and basic SELECT smoke test in the testing harness

### Changed

- LSM range scans use sparse index blocks and memtable prefix scans to limit primary-key lookups
- Storage engine avoids redundant unique checks, inlines filtering for early exit, and uses PK point lookup helper
- SSTable readers now enforce size sanity checks and validate data sections on load

## [0.2.3] - 2025-12-14

### Added

- **Language Extensions** - New keywords and syntax
  - `make index [index_name] on [table_name]([column_name])` for non-unique indexes
  - `make unique index [index_name] on [table_name]([column_name])` for unique indexes
  - `drop index [index_name]` to remove indexes
  - `describe [table_name]` to show table schema
  - `count <from> [table_name]` to get row count
- **Index Support** - Secondary indexes for faster lookups
  - Index creation and deletion
  - Index metadata stored in catalog
  - Indexes updated on row insert/update/delete
  - Query planner uses indexes for optimization

### Changed

- `QueryExecutor` now handles some basic query planning for index usage

## [0.2.2] - 2025-12-13

### Added

- **System-level WAL** - Unified write-ahead log for all collections
  - Single `system.wal` file in data directory
  - Collection-prefixed keys for recovery routing
  - Transaction markers: `TxBegin`, `TxCommit`, `TxRollback`
  - LSM trees now use external WAL mode

- **MVCC Garbage Collection** - Compaction-time cleanup
  - `GcContext` with oldest_active_ts watermark and aborted tx set
  - Removes entries from aborted transactions
  - Removes old versions superseded by newer committed versions
  - Logs GC statistics during compaction

- **Transaction-aware Recovery** - Crash recovery respects transactions
  - First pass: identifies committed/aborted transactions from WAL
  - Second pass: only replays data from committed transactions
  - Restores timestamp counter to avoid collisions
  - Skips uncommitted transaction data

### Changed

- All data writes now go through system WAL (`lsm_put_with_wal`, `lsm_delete_with_wal`)
- LSM trees created with `with_external_wal` (no per-collection WAL)
- Compaction accepts optional `GcContext` for MVCC-aware merging

## [0.2.1] - 2025-12-13

### Added

- **MVCC for relational tables** - Multi-Version Concurrency Control with Snapshot Isolation
  - `begin`, `commit`, `rollback` transaction statements
  - Versioned key encoding (`pk:inverted_timestamp`) for newest-first sorting
  - Visibility checks - only see committed data from before your transaction started
  - Write-write conflict detection - prevents concurrent modifications to same row
  - Own writes visible within transaction

- **Transaction Manager** - Handles transaction lifecycle
  - `TxStatus` enum (Active, Committed, Aborted)
  - Atomic timestamp generation
  - Transaction tracking with DashMap

- **WriteConflict error type** - For MVCC conflict detection

### Changed

- `QueryExecutor` now persists per session (was recreated per request, breaking transaction state)
- Executor passes transaction context to storage layer for versioned operations
- Storage engine routes to versioned or non-versioned paths based on schema type and transaction presence

### Fixed

- **GUI 3-row display bug** - Transaction control statements (BEGIN/COMMIT/ROLLBACK) were being included as rows in query results. Now only actual row data is displayed.

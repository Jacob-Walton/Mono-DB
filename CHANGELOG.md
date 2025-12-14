# Changelog

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

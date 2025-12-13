# Changelog

## [Unreleased]

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

---
title: Architecture Overview
sidebar_position: 1
---

# MonoDB Architecture

MonoDB is designed with modularity and performance in mind. This document provides an overview of the system architecture.

:::warning Work in Progress
The architecture is evolving. Some components described here may be partially implemented or planned for future releases.
:::

## System Components

```
    
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                     │
│                    (REPL, Drivers, etc.)                    │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                        Network Layer                        │
│                  (Binary Protocol over TCP)                 │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                       Query Processor                       │
│                 (Parser, Planner, Executor)                 │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                     Transaction Manager                     │
│              (ACID, Isolation Levels, Locking)              │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                        Storage Engine                       │
│           (Buffer Pool, Pages, WAL, Disk Manager)           │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Storage Engine

This storage engine provides, persistent, ACID-compliant data storage:

- **Page Management**: Data is stored in fixed-size pages (8KB)
- **Buffer Pool**: LRU cache for frequently accessed pages
- **Write-Ahead Logging**: Ensures durability and crash recovery
- **Disk Manager**: Handles phsyical I/O operations

### 2. Transaction Manager

Manages concurrent access and ensures ACID properties:

- **Isolation Levels**: Supports Read Uncommitted, Read Committed, Repeatable Read, and Serializable
- **Lock Manager**: Row-level and table-level locking
- **MVCC**: Planned for future releases

### 3. Query Processor

Handles SQL parsing and execution

- **Parser**: Converts SQL/NSQL to Abstract Syntax Tree (AST)
- **Planner**: Current basic; query optimization is planned
- **Executor**: Executes query plans against storage engine

### 4. Network Layer

Custom binary protocol for client-server communication:

- **Message Format**: Length-prefixed JSON messages
- **Connection Management**: Handles multiple concurrent clients
- **Heartbeat**: Keeps connections alive

## Data Organization

### Pages

The fundamental unit of storage:

```rust
pub struct Page {
    pub header: PageHeader,
    pub data: Vec<u8>,
}

pub struct PageHeader {
	pub magic: u32,             // Magic number for validation
	pub page_id: u32,           // Page ID
	pub page_type: u8,          // Page type
	pub flags: u8,              // Flags (dirty, pinned, etc.)
	pub lsn: u64,               // Last LSN that modified this page
	pub free_space_offset: u16, // Offset to free space
	pub tuple_count: u16,       // Number of tuples
	pub next_page_id: u32,      // Next page in chain (for overflow)
	pub prev_page_id: u32,      // Previous page in chain
	pub checksum: u32,          // CRC32 checksum
	pub _reserved: [u8; 28],    // Reserved for future use
}
```

### Tables

Tables are collections of pages containing tuples (AKA records):

- **Heap Files**: Currently, all tables use heap file organization
- **Indexes**: B-tree indexes are planned but not yet implemented

### Write-Ahead Log

Every modification is logged before being applied:

- **Log Sequence Numbers (LSN)**: Ensure ordering
- **Checkpoints**: Periodic flushing to reduce recovery time
- **Recovery**: Replay log from last checkpoint

## Query Execution Flow

1. **Parse**: SQL text -> AST
2. **Plan**: AST -> Execution plan (currently basic)
3. **Execute**: Run plan against storage engine
4. **Return**: Format results for client

## Concurrency Control

Currently uses pessimistic locking:

- **Shared locks** for reads
- **Exclusive locks** for writes
- **Deadlock detection**: Planned for future

## Memory Management

- **Buffer Pool**: Configurable size, LRU eviction
- **Query Memory**: Currently unbounded (improvement planned)
- **Connection Memory**: Per-connection buffers

## Future Goals

### Distributed Architecture

- Sharding support
- Replication (primary-replica)
- Distributed transactions

### Query Optimization

- Cost-based optimizer
- Statistics collections
- Join algorithms

### Storage Improvements

- Columnar storage option
- Compression
- Partitioning

## Performance Considerations

### Current Limitations

- Single-threaded query execution
- No query result caching
- Limited index support

### Optimization Tips

- Keep transactions short
- Use appropriate data types
- Monitor buffer pool hit rate

## Monitoring and Debugging

:::note Coming Soon
Monitoring and profiling tools are planned but not yet available
:::

### Planned Features

- Query execution stats
- Buffer pool metrics
- Lock contention analysis
- Slow query log

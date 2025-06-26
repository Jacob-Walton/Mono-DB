---
id: intro
title: Introduction to MonoDB
sidebar_position: 1
---

# MonoDB

**MonoDB** is a modern, multi-paradigm database engine written in Rust that aims to provide a flexible and performant data storage solution. It combines traditional SQL with modern features and a focus on developer experience.

:::warning Early Development
MonoDB is currently in early development. Many features described in this documentation are planned but not yet implemented. The API and functionality may change significantly before any production release.
:::

# Key Features

### Currently Implemented

- **ACID-compliant storage engine** with Write-Ahead Logging (WAL)
- **Buffer pool management** for efficient memory usage
- **Basic SQL support** later to developed into NSQL
- **Client-server architecture** with custom network protocol
- **Interactive REPL** with auto-completion (and soon syntax highlighting)
- **Persistent storage** with page-based architecture
- **Transaction support** with multiple isolation levels (no manual transactions yet)

### In Development

- Advanced query optimization
- Distributed query processing
- Multi-paradigm data models (document, graph, time-series)
- Replication and sharding
- Advanced indexing
- Query caching
- Backup and recovery tools

## Why MonoDB?

MonoDB is designed to address common pain points in modern database systems:

1. **Single Paradigm Limitations**: Most databases force you to choose between relational, document, or graph models. MonoDB aims to support multiple paradigms seamlessly.

2. **Developer Experience**: With features like natural language queries (NSQL) and an intuitive REPL, MonoDB prioritizes developer productivity.

3. **Performance**: Built in Rust with careful attention to memory management and I/O patterns.

4. **Flexibility**: Designed from the ground up to be extensible and adaptable to different use cases.

## Architecture Overview

MonoDB consists of several key components:

- **Storage Engine**: Page-based storage with ACID guarantees
- **Query Engine**: SQL parser and executor with support for natural language extensions
- **Network Layer**: Custom binary protocol for efficient client-server communication
- **REPL**: Interactive shell for database administration and queries

## Current Status

MonoDB is an experimental project under active development. While the core storage engine and basic SQL functionality work, many advanced features are still being worked on.

### What Works Today

- Creating and dropping tables
- Basic INSERT, SELECT, UPDATE, DELETE operations
- Simple WHERE clauses
- Transaction support (single-node)
- Persistent storage with crash recovery

### What's Coming Soon

- JOIN operations
- Complex query optimization
- Secondary indexes
- Aggregate functions
- Stored procedures
- Multi-node deployment

## Getting Help


- **GitHub Issues**: Report bugs or request features
- **Discussions**: Ask questions and share ideas
- **Contributing**: We welcome contributions! See our contributing guide

## License

MonoDB is open-source software licensed under the MIT License.
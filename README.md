# MonoDB

> [!NOTE]
> This is a work in progress. Features are being actively developed and may not be fully implemented yet.

## Overview

MonoDB is a modern, multi-paradigm database engine written in Rust that combines the best of SQL, NoSQL, and key-value storage patterns. It's designed to be flexible, performant, and easy to use while maintaining ACID compliance and data safety.

## Key Features

- **🚀 High Performance**: Written in Rust for memory safety and speed
- **🔄 Multi-Paradigm**: Support for SQL, NoSQL, and key-value operations
- **🛡️ ACID Compliant**: Full transaction support with configurable isolation levels
- **💾 Persistent Storage**: Write-ahead logging and crash recovery
- **🌐 Network Ready**: Built-in TCP server with JSON protocol
- **⚡ Concurrent**: Multi-threaded query processing and transaction handling

## Quick Start

### Prerequisites

- Rust 1.70+ (install from [rustup.rs](https://rustup.rs/))
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/Jacob-Walton/Mono-DB.git
cd Mono-DB

# Build the project
cargo build --release

# Run the server
cargo run --bin mono-core
```

### Basic Usage

```sql
-- Create a table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL
);

-- Insert data
INSERT INTO users (id, name, email) VALUES 
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com');

-- Query data
SELECT * FROM users WHERE name = 'Alice';

-- Update data
UPDATE users SET email = 'alice.new@example.com' WHERE id = 1;
```

## Architecture

MonoDB is built with a modular architecture:

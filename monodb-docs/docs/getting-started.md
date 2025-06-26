---
id: getting-started
title: Getting Started
sidebar_position: 2
---

# Getting Started with MonoDB

This guide will help you get MonoDB up and running on your system.

:::caution Prerequisites

- Rust 1.80 or higher
- Git
- A Unix-like operating system (Linux, macOS) or Windows with WSL (for now)
  :::

## Installation

### Building from Source

Currently, MonoDB must be built from source. We're working on providing pre-built binaries.

1. **Clone the repository**

   ```bash
   git clone https://github.com/Jacob-Walton/Mono-DB.git
   cd Mono-DB
   ```

2. **Build the Project**
   ```
   cargo build --release
   ```

## Starting the Server

Start the MonoDB server with:

```bash
cargo run --bin monod --release
# OR
./monod # Can be found in target/release
```

The server will start with default settings:

- Port: 3282
- Data directory: ./monodb_data
- Max connections: 100

You should see output like:

```text

    ███╗   ███╗ ██████╗ ███╗   ██╗ ██████╗ ██████╗ ██████╗
    ████╗ ████║██╔═══██╗████╗  ██║██╔═══██╗██╔══██╗██╔══██╗
    ██╔████╔██║██║   ██║██╔██╗ ██║██║   ██║██║  ██║██████╔╝
    ██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║██║  ██║██╔══██╗
    ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║╚██████╔╝██████╔╝██████╔╝
    ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝ ╚═════╝ ╚═════╝
                       Database Engine v0.1.0

Starting MonoDB Server...
Configuration:
  Port: 3282
  Data Directory: ./monodb_data
  Max Connections: 100
  WAL Enabled: true
Starting MonoDB server on port 3282
MonoDB server listening on port 3282
```

## Using the REPL CLient

In a new terminal, start the REPL client:

```bash
cargo run --bin repl --release
# OR
./repl # Can be found in target/release
```

You should see the welcome message:

```text
╔═══════════════════════════════════════╗
║       Welcome to NSQL REPL! 🚀        ║
╚═══════════════════════════════════════╝

Type .help for help, .exit to exit
Try .example to see example queries

⚠ Not connected to server (127.0.0.1:3282)
💡 Use .connect to establish connection or .server <address> to change server
nsql[0]>
```

Before we continue, make sure to change the server address if it's listening on a different address and then connect using `.connect`.

### Your First Queries

Let's create a table and insert some data:

1. **Create a Table**

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username VARCHAR(5) NOT NULL,
    email VARCHAR(100),
    created_at INTEGER
);
```

2. **Insert Data**

```sql
INSERT INTO users (id, username, email, created_at)
VALUES (1, 'john', 'john@example.com', 1234567890);

INSERT INTO users (id, username, email, created_at)
VALUES (2, 'jane', 'jane@example.com', 1234567891);
```

3. **Query Data**

```sql
SELECT * FROM users;
```

You should see:

```text
✓ Query executed successfully

2 rows returned

+----+----------+------------------+------------+
| id | username | email            | created_at |
+===============================================+
| 1  | john     | john@example.com | 1234567890 |
|----+----------+------------------+------------|
| 2  | jane     | jane@example.com | 1234567891 |
+----+----------+------------------+------------+

(2 rows)
  Time: 4.535ms (network: 4.612ms)
```

4. **Update Data**

```sql
UPDATE users SET email = 'john@newdomain.com' WHERE id = 1;
```

5. **Delete Data**

```sql
DELETE FROM users WHERE username = 'bob';
```

### REPL Commands

The REPL supports various commands for database management:

| Command           | Description         |
| ----------------- | ------------------- |
| `.help`           | Show help message   |
| `.tables`         | List all tables     |
| `.schema <table>` | Show table schema   |
| `.timing`         | Toggle query timing |
| `.connect`        | Connect to server   |
| `.exit`           | Exit the REPL       |

## Configuration

:::info Coming Soon
Configuration file support is planned but not yet implemented. Currently, all settings must be specified via code changes.
:::

## Next Steps

- Explore the [NSQL Reference](nsql-reference) to learn about the query language
- Read about the [Architecture](architecture/overview) to learn about how MonoDB works
- Check the [API Reference](api/network-protocol) if you want to build a client

## Troubleshooting

### Server won't start

- Check if port 3282 is already in use
- Ensure data directory has write permissions
- Check the console for error messages

### Can't connect from REPL

- Verify the server is running
- Ensure you're using the correct address
- Check firewall settings

## Data not persisting

- Ensure the data directory path is correct
- Check disk space
- Look for WAL errors in server logs

:::note
If you encounter issues not covered here, please open an issue on GitHub with details about your environment and the error messages.
:::
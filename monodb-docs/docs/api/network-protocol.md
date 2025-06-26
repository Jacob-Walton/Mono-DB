---
title: Network Protocol
sidebar_position: 1
---

# MonoDB Network Protocol

MonoDB uses a custom binary protocl for client-server communication over TCP.

:::info Protocol Version
Current protocol version: 1.0 (unstable)
:::

## Overview

The protocol uses a simple request-response model:

1. Client sends a request message
2. Server processes the request
3. Server sends a response message

All messages are:

- Length prefixed (4 bytes, big-endian)
- JSON-encoded
- Maximum size: 1MB

## Connection Flow

```
Client                          Server
  |                               |
  |-------- TCP Connect --------->|
  |                               |
  |-------- Ping Message -------->|
  |<-------- Pong Reply ----------|
  |                               |
  |------ Execute Query --------->|
  |<------ Query Result ----------|
  |                               |
  |-------- Disconnect ---------->|
  |                               |
```

## Message Format

### Wire Format

```
+----------------+------------------+
|  Length (4B)   |   JSON Payload   |
+----------------+------------------+
```

- **Length**: 32-bit unsigned integer, big-endian
- **Payload**: UTF-8 encoded JSON

### Client Messages

#### Ping

Tests server connectivity.

```json
{
  "type": "Ping"
}
```

#### Execute

Executes a SQL query.

```json
{
  "type": "Execute",
  "sql": "SELECT * FROM users",
  "database": null // optional, reserved for future use
}
```

#### List Tables

Lists all tables in the database.

```json
{
  "type": "ListTables",
  "database": null // optional, reserved for future use
}
```

### Server Messages

#### Pong

Response to Ping.

```json
{
  "type": "Pong"
}
```

#### Execute Result

Response to query execution.

```json
{
  "type": "ExecuteResult",
  "results": [
    {
      "result_type": "Select",
      "columns": [
        {
          "name": "id",
          "data_type": "INTEGER",
          "nullable": false
        },
        {
          "name": "username",
          "data_type": "VARCHAR(50)",
          "nullable": true
        }
      ],
      "rows": [
        [{ "Integer": 1 }, { "String": "alice" }],
        [{ "Integer": 2 }, { "String": "bob" }]
      ],
      "rows_affected": null
    }
  ],
  "rows_affected": null,
  "execution_time_ms": 15
}
```

#### Table List

Response to list tables request.

```json
{
  "type": "TableList",
  "tables": ["users", "products", "orders"]
}
```

#### Error

Error response for any failed operation.

```json
{
  "type": "Error",
  "code": "TableNotFound",
  "message": "Table 'users' not found",
  "details": "The table does not exist in the current database"
}
```

## Data Types

Values in result rows are encoded as tagged unions:

```json
{ "Integer": 42 }
{ "Float": 3.14 }
{ "String": "hello" }
{ "Boolean": true }
{ "Null": null }
{ "Date": "2024-01-15" }
{ "DateTime": "2024-01-15 10:30:45" }
```

## Error Codes

| Code                  | Description                     |
| --------------------- | ------------------------------- |
| `ParseError`          | SQL syntax error                |
| `ExecutionError`      | Runtime query error             |
| `TableNotFound`       | Referenced table doesn't exist  |
| `ColumnNotFound`      | Referenced column doesn't exist |
| `TypeMismatch`        | Type conflict in operation      |
| `ConstraintViolation` | Constraint check failed         |
| `InternalError`       | Server internal error           |
| `NotImplemented`      | Feature not yet implemented     |

## Connection Management

### Timeouts
- Default connection timeout: 5 minutes
- Heartbeat interval: 30 seconds

### Connection Pooling
Clients should implement connection pooling for better performance.

## Example Client

### Python Example

```python
import socket
import json
import struct

class MonoDBClient:
    def __init__(self, host='localhost', port=3282):
        self.host = host
        self.port = port
        self.sock = None
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        
        # Send ping to verify connection
        response = self._send_message({"type": "Ping"})
        if response.get("type") != "Pong":
            raise Exception("Invalid handshake")
    
    def execute(self, sql):
        message = {
            "type": "Execute",
            "sql": sql,
            "database": None
        }
        return self._send_message(message)
    
    def _send_message(self, message):
        # Serialize to JSON
        payload = json.dumps(message).encode('utf-8')
        
        # Send length prefix
        length = struct.pack('>I', len(payload))
        self.sock.send(length)
        
        # Send payload
        self.sock.send(payload)
        
        # Read response length
        length_bytes = self._recv_exactly(4)
        length = struct.unpack('>I', length_bytes)[0]
        
        # Read response payload
        payload = self._recv_exactly(length)
        return json.loads(payload.decode('utf-8'))
    
    def _recv_exactly(self, n):
        data = b''
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise Exception("Connection closed")
            data += chunk
        return data
    
    def close(self):
        if self.sock:
            self.sock.close()

# Usage
client = MonoDBClient()
client.connect()
result = client.execute("SELECT * FROM users")
print(result)
client.close()
```

## Security Considerations

:::danger No Authentication
The current protocol has no authentication or encryption. Do not use in production environments!
:::

### Planned Security Features
- TLS/SSL support
- Authentication mechanisms
- Role-based access control
- Query paramter binding (to prevent SQL injection)

## Protocol Evolution

The protocol is versioned to allow future changes:
- Version negotiation during handshake (planned)
- Backward compatibility for minor versions
- Feature capability discovery

## Performance Tips

1. **Reuse connections** - Connection establishment has overhead
2. **Use prepared statements** (when implemented) for repeated queries
3. **Batch operations** when possible
4. **Monitor response times** via `execution_time_ms` field
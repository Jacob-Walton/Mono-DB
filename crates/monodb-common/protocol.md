# Protocol Specification

0. Global Rules

- Endian: all integers are **little-endian**.
- Booleans: `u8` (`0 = false`, `1 = true`)
- Strings: `[len: u32][len bytes UTF-8, no NUL]`
- Bytes: `[len: u32][len bytes]`
- Vec`<T>`: `[len: u32][element_0][element_1]...[element_{len-1}]`
- Option`<T>`: `[present: u8][T if present == 1]` (`0 = None`, `1 = Some`)
- All lengths are `u32`, unless otherwise stated

1. Frame Format

Every message on the wire is a frame:

```text
+-------------------------+
| frame_len      : u32 LE | - total bytes after this field 
+-------------------------+
| version        : u8     | - protocol version (currently 1)
+-------------------------+
| kind           : u8     | - 0 = Request, 1 = Response
+-------------------------+
| msg_type       : u8     | - which Request/Response variant
+-------------------------+
| flags          : u8     | - bitfield, 0 for now
+-------------------------+
| correlation_id : u32 LE | - client-chosen id (0 allowed)
+-------------------------+
| body...                 | - message-specific payload
+-------------------------+
```

Therefore:

```text
frame_len = 1(version) + 1(kind) + 1(msg_type) + 1(flags) + 4(correlation_id) + body_len
```

You read `frame_len`, then read that many more bytes for header+body, then decode.

2. Message type codes

**Requests** (`kind = 0`)

```text
0x01 = Connect
0x02 = Execute
0x03 = List
0x04 = BeginTx
0x05 = CommitTx
0x06 = RollbackTx
```

**Responses** (`kind = 1`)

```text
0x01 = ConnectAck
0x02 = Success
0x03 = Error
0x04 = Stream
0x05 = Ack
```

3. IsolationLevel, ErrorCode enums

**IsolationLevel** (`u8`)

```text
0 = ReadUncommitted
1 = ReadCommitted
2 = RepeatableRead
3 = Serializable
```

**ErrorCode** (`u16 LE`)

```text
1001 = ParseError
1002 = ExecutionError
1003 = NotFound
1004 = AlreadyExists
1005 = TypeError
1006 = TransactionError
1007 = NetworkError
9999 = InternalError
```

4. Value encoding

First byte: **kind tag** (`u8`). Then variant-specific payload.

**Value kind tags**

```text
0  = Null
1  = Bool
2  = Int32
3  = Int64
4  = Float32
5  = Float64
6  = String
7  = Binary
8  = DateTime
9  = Date
10 = Time
11 = Uuid
12 = ObjectId
13 = Array
14 = Object
15 = Set
16 = Row
17 = SortedSet
18 = GeoPoint
19 = Reference
```

**Variant Payloads**

- `Null` (`0`): no payload
- `Bool` (`1`): `[b: u8]` (`0 = false`, `1 = true`)
- `Int32` (`2`): `[i: i32 LE]`
- `Int64` (`3`): `[i: i64 LE]`
- `Float32` (`4`): `[bits: u32 LE]` (IEEE-754 f32 bits)
- `Float64` (`5`): `[bits: u64 LE]` (IEEE-754 f64 bits)
- `String` (`6`): `String` (u32 len + bytes)
- `Binary` (`7`): `Bytes` (u32 len + bytes)
- `DateTime` (`8`):
  ```text
  [unix_micros    : i64 LE] - microseconds since Unix epoch (UTC)
  [offset_minutes : i32 LE] - timezone offset from UTC in minutes
  ```
- `Date` (`9`):
  ```text
  [year  : i32 LE]
  [month : u8    ] - 1-12
  [day   : u8    ] - 1-31
  ```
- `Time` (`10`):
  ```text
  [hour   : u8] - 0-23
  [minute : u8] - 0-59
  [second : u8] - 0-59
  [micros : u32 LE] - 0-999_999
  ```
- `Uuid` (`11`): `[16 bytes]` (RFC 4122 BE format)
- `ObjectId` (`12`): `[12 bytes]` (MongoDB-style raw bytes)
- `Array` (`13`):
  ```text
  [len : u32 LE]
  [Value_0][Value_1]...[Value_{len-1}]
  ```
- `Object` (`14`): (`BTreeMap<String, Value>`; order not significant)
  ```text
  [len : u32 LE]
  repeat len times:
    [key   : String]
    [value : Value]
  ```
- `Set` (`15`): (`HashSet<String>`)
  ```text
  [len : u32 LE]
  repeat len times:
    [item : String]
  ```
- `Row` (`16`): (`IndexMap<String, Value>`; order significant) write format identical to `Object`, but order is preserved.
- `SortedSet` (`17`): (`Vec<(f64, String)>`)
  ```text
  [len : u32 LE]
  repeat len times:
    [score : f64 LE]
    [item  : String]
  ```
- `GeoPoint` (`18`):
  ```text
  [latitude  : f64 LE]
  [longitude : f64 LE]
  ```
- `Reference` (`19`):
  ```text
  [collection : String]
  [id         : Value] - typically Int32, Int64, or ObjectId
  ```

5. Request bodies

Everything here goes in the **body** part of the frame, after the header.

5.1 `Connect` (`kind=0`, `msg_type=0x01`)

Rust: `Connect { protocol_version: u8, auth_token: Option<String> }`

Wire:

```text
[protocol_version : u8            ]
[auth_token       : Option<String>]
```

5.2 `Execute` (`kind=0`, `msg_type=0x02`)

Rust:

```rust
Execute {
  query: String,
  params: Vec<Value>,
  snapshot_timestamp: Option<u64>,
  user_id: Option<String>,
}
```

Wire:

```text
[query : String                     ]
[params : Vec<Value>                ]
[snapshot_timestamp : Option<u64 LE>]
[user_id : Option<String>           ]
```

`Vec<Value>` is encoded as such:

```text
[len : u32 LE]
repeat len times:
  [Value]
```

5.3 `List` (`kind=0`, `msg_type=0x03`)

> [!NOTE]
> I plan to expand this message in the future to support filters and targets.

Rust `List {}`

Wire: (no body)

5.4 `BeginTx` (`kind=0`, `msg_type=0x04`)

Rust:

```rust
BeginTx {
  isolation: IsolationLevel,
  user_id: Option<String>,
  read_timestamp: Option<u64>,
}
```

Wire:

```text
[isolation      : u8            ] - see enum mapping above
[user_id        : Option<String>]
[read_timestamp : Option<u64 LE>]
```

5.5 `CommitTx` (`kind=0`, `msg_type=0x05`)

Rust: `CommitTx { tx_id: u64 }`

Wire:

```text
[tx_id : u64 LE]
```

5.6 `RollbackTx` (`kind=0`, `msg_type=0x06`)

Rust: `RollbackTx { tx_id: u64 }`

Wire:

```text
[tx_id : u64 LE]
```

6. Response bodies

6.1 `ConnectAck` (`kind=1`, `msg_type=0x01`)

Rust:

```rust
ConnectAck {
  protocol_version: u8,
  server_timestamp: Option<u64>,
  user_permissions: Option<Vec<String>>,
}
```

Wire:

```text
[protocol_version  : u8                 ]
[server_timestamp  : Option<u64 LE>     ]
[user_permissions  : Option<Vec<String>> ]
```

`Option<Vec<String>>` is encoded as such:

```text
[present : u8] - 0 = None, 1 = Some
if present == 1:
  [len : u32 LE]
  repeat len times:
    [String]
```

6.2 `Success` (`kind=1`, `msg_type=0x02`)

> [!NOTE]
> This success structure is really poor and will be redesigned in the future.

Rust: `Success { result: Vec<ExecutionResult> }`

Wire:

```text
[result : Vec<ExecutionResult>]
```

Where `ExecutionResult` is:

```rust
enum ExecutionResult {
    Ok { data: Value, time: u64, commit_timestamp: Option<u64>,
         time_elapsed: Option<u64>, row_count: Option<u64> },
    Created { time: u64, commit_timestamp: Option<u64> },
    Modified { time: u64, commit_timestamp: Option<u64>, rows_affected: Option<u64> },
}
```

Wire:

```text
ExecutionResult:
  [tag : u8]
  if tag == 0:  // Ok
      [data             : Value]
      [time             : u64 LE]
      [commit_timestamp : Option<u64 LE>]
      [time_elapsed     : Option<u64 LE>]
      [row_count        : Option<u64 LE>]

  if tag == 1:  // Created
      [time             : u64 LE]
      [commit_timestamp : Option<u64 LE>]

  if tag == 2:  // Modified
      [time             : u64 LE]
      [commit_timestamp : Option<u64 LE>]
      [rows_affected    : Option<u64 LE>]
```

6.3 `Error` (`kind=1`, `msg_type=0x03`)

Rust: `Error { code: ErrorCode, message: String }`

Wire:

```text
[code    : u16 LE] - see enum mapping above
[message : String]
```

6.4 `Stream` (`kind=1`, `msg_type=0x04`)

> [!NOTE]
> This currently hasn't been designed or implemented nor is it used anywhere. It will be in the future so I'm reserving the message type.

6.5 `Ack` (`kind=1`, `msg_type=0x05`)

Also currently payload-free.

Eventually we'll use the correlation_id to identify what is being acked.

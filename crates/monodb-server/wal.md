# MonoDB WAL Specification

The Write-Ahead Log (WAL) is a critical component when it comes to ensuring data durability and consistency in MonoDB. This document outlines the binary format which is used to store the WAL entries on disk.

## 0. Global Rules

- Endian: all integers are **little-endian**.
- Alignment: headers are 32 bytes (half cache line).
- CRC scope: covers header + key + value and only excludes itself.
- Magic scanning: when scanning for entries, only the magic is checked.

## 1. Entry Types

| Code | Name       | Description                        |
| ---- | ---------- | ---------------------------------- |
| 0x01 | Insert     | New key-value pair                 |
| 0x02 | Update     | Modify existing value              |
| 0x03 | Delete     | Tombstone (value may be empty)     |
| 0x04 | Checkpoint | Snapshot marker (value = metadata) |
| 0x05 | TxBegin    | Transaction start                  |
| 0x06 | TxCommit   | Transaction commit                 |
| 0x07 | TxAbort    | Transaction rollback               |

## 2. Flags Field

| Bit  | Name       | Description                             |
| ---- | ---------- | --------------------------------------- |
| 0    | COMPRESSED | Value payload is LZ4 compressed         |
| 1    | BATCH_END  | Last entry in a batch (safe sync point) |
| 2    | EXTENDED   | Uses extended length fields             |
| 3-15 | Reserved   | Must be zero                            |

## 3. Record Format

### 3.1 Standard Record (<= 254 byte key, <=65534 byte value)

Used when the key length is less than or equal to 254 bytes and the value length is less than or equal to 65534 bytes.

```text
Offset   Size   Field              Description
───────────────────────────────────────────────────────────────────────
0        4      MAGIC      : u32   'WLOG' (0x574C4F47)
4        2      VERSION    : u16   format version (currently 2)
6        2      FLAGS      : u16   see §2
8        8      SEQUENCE   : u64   monotonically increasing entry ID
16       8      TIMESTAMP  : u64   microseconds since UNIX epoch
24       1      ENTRY_TYPE : u8    see §1
25       1      KEY_LEN    : u8    0-254, or 255 = extended
26       2      VALUE_LEN  : u16   0-65534, or 65535 = extended
28       4      CRC32      : u32   CRC32 of bytes [0..28] + key + value
───────────────────────────────────────────────────────────────────────
32       K      KEY        : [u8]  K = KEY_LEN bytes
32 + K   V      VALUE      : [u8]  V = VALUE_LEN bytes
───────────────────────────────────────────────────────────────────────
Total: 32 + K + V bytes
```

### 3.2 Extended Record (large key or value)

Used when `FLAGS & EXTENDED` is set (key > 254 bytes or value > 65534 bytes).

```text
Offset   Size   Field                Description
───────────────────────────────────────────────────────────────────────────────────
0        4      MAGIC        : u32   'WLOG' (0x574C4F47)
4        2      VERSION      : u16   format version (currently 2)
6        2      FLAGS        : u16   EXTENDED bit set
8        8      SEQUENCE     : u64   monotonically increasing entry ID
16       8      TIMESTAMP    : u64   microseconds since UNIX epoch
24       1      ENTRY_TYPE   : u8    see §1
25       1      RESERVED     : u8    must be 255
26       2      RESERVED     : u16   must be 65535
28       4      CRC32        : u32   CRC32 bytes [0..28] + ext_header + key + value
───────────────────────────────────────────────────────────────────────────────────
32       4      EXT_KEY_LEN  : u32   actual key length
36       4      EXT_VAL_LEN  : u32   actual value length
───────────────────────────────────────────────────────────────────────────────────
40       K      KEY          : [u8]  K = KEY_LEN bytes
40 + K   V      VALUE        : [u8]  V = VALUE_LEN bytes
───────────────────────────────────────────────────────────────────────────────────
Total: 40 + K + V bytes
```

## 4. Checkpoint Value Format

When `ENTRY_TYPE = 0x04` (Checkpoint), the value contains:

```text
Offset  Size  Field                 Description
────────────────────────────────────────────────────────────────
0       8     SCHEMA_VER   : u64    schema version at checkpoint
8       4     FILE_COUNT   : u32    number of SSTable files
12      ...   FILES        : repeated FILE_ENTRY
────────────────────────────────────────────────────────────────

FILE_ENTRY:
  [name_len : u16][name : UTF-8 bytes]
```

## 5. Transaction Markers

### 6.1 TxBegin Value (0x05)

```text
Offset  Size  Field             Description
───────────────────────────────────────────────────────────────────
0       8     TX_ID     : u64   transaction identifier
8       1     ISOLATION : u8    0=ReadUncommitted, 1=ReadCommitted, 
                                2=RepeatableRead, 3=Serializable
9       8     READ_TS   : u64   snapshot timestamp (0 = current)
───────────────────────────────────────────────────────────────────
total: 17 bytes
```

### 6.2 TxCommit Value (0x06)

```text
Offset  Size  Field             Description
──────────────────────────────────────────────────────
0       8     TX_ID     : u64   transaction identifier
8       8     COMMIT_TS : u64   commit timestamp
──────────────────────────────────────────────────────
total: 16 bytes
```

### 6.3 TxAbort Value (0x07)

```text
Offset  Size  Field             Description
──────────────────────────────────────────────────────
0       8     TX_ID     : u64   transaction identifier
──────────────────────────────────────────────────────
total: 8 bytes
```

## 7. CRC Calculation

The CRC32 (Castagnoli) checksum covers:

1. Header bytes `[0..28]`
2. Extended length fields (if EXTENDED flag set)
3. Key bytes
4. Value bytes

```text
CRC32C(header[0..28] || [ext_key_len || ext_val_len]? || key || value)
```

We use CRC32 because there's hardware acceleration for it on modern CPUs, making it fast to compute whilst still providing good error detection capabilities.

## 8. Recovery Algorithm

```text
1. Open WAL file, seek to start
2. Loop:
    a. Read 32 bytes into header buffer
    b. If EOF: done
    c. If MAGIC != 'WLOG': scan forward for magic, goto 2a
    d. Parse header, determine K and V lenghts
    e. Read K + V bytes
    f. Compute CRC32, compare to header.CRC32
    g. If mismatch: log warning, scan forward for magic, goto 2a
    h. If FLAGS & BATCH_END: this is a safe sync point
    i. Emit entry, continue
3. Truncate file to last BATCH_END position
```

## 9. File Rotation

WAL files are rotated when:

- File size exceeds configured limit (default 64 MiB)
- Explicit checkpoint is issued
- Server shutdown

Rotated files are named: `wal.{sequence}.log` where `{sequence}` is the first entry's sequence number in the file.

## 10. Performance Considerations

We use a 32-byte header to align with CPU cache lines, minimizing read/write overhead. CRC before the payload allows us to validate the header before processing large key/value data, reducing unnecessary I/O. Variable length inline for 99% of keys, avoiding extra reads for small entries. BATCH_END flags allow grouping multiple entries into a single durable write, improving throughput. Magic at offset 0 allows quick scanning for recovery after crashes. Little-endian format matches most modern CPU architectures, reducing conversion overhead.

# MonoDB Storage Format Specification

This document defines the binary formats used by MonoDB's storage engine for persisting data on the disk. These formats are used for SSTable files, schema metadata, and index structures.

## §0 Global Rules

- Endian: all integers are **little-endian**.
- Strings: `[len: u32][UTF-8 bytes no NUL]`
- Bytes: `[len: u32][raw bytes]`
- Vec`<T>`: `[count: u32][element_0][element_1]...[element_{count-1}]`
- Option`<T>`: `[present: u8][T if present == 1]` (`0 = None`, `1 = Some`)
- All lengths/counts are `u32` unless otherwise stated.
- CRC32 uses Castagnoli polynomial.

## §1 Value Encoding

Values are encoded with a leading type tag byte followed by type-specific payload. Please refer to the protocol.md document for detailed type tags and their corresponding payload formats.

## §2 SSTable Format

SSTables are immutable sorted files containing key-value pairs.

### §2.1 File Layout

```text
┌────────────────────────────────┐
│ File Header (64 bytes)         │
├────────────────────────────────┤
│          Data Blocks           │
│   ┌───────────────────────┐    │
│   │ Block 0               │    │
│   ├───────────────────────┤    │
│   │ Block 1               │    │
│   ├───────────────────────┤    │
│   │ ...                   │    │
│   └───────────────────────┘    │
├────────────────────────────────┤
│ Index Block                    │
├────────────────────────────────┤
│ Bloom Filter                   │
├────────────────────────────────┤
│ Footer (48 bytes)              │
└────────────────────────────────┘
```

### §2.2 File Header (64 bytes)

```text
Offset  Size  Field                Description
──────────────────────────────────────────────────────────────────
0       4     MAGIC        : u32   'SSTB' (0x53535442)
4       2     VERSION      : u16   format version (currently 1)
6       2     FLAGS        : u16   see §2.3
8       8     CREATED_AT   : u64   microseconds since UNIX epoch
16      8     ENTRY_COUNT  : u64   total number of key-value pairs
24      4     BLOCK_SIZE   : u32   target block size in bytes
28      1     COMPRESSION  : u8    0=None, 1=LZ4, 2=Snappy, 3=Zstd
29      1     LEVEL        : u8    LSM level (0-7)
30      2     RESERVED     : u16   must be zero
32      16    MIN_KEY_HASH : [u8]  XXH128 of minimum key
48      16    MAX_KEY_HASH : [u8]  XXH128 of maximum key
──────────────────────────────────────────────────────────────────
Total: 64 bytes
```

### §2.3 Header Flags

| Bit  | Name       | Description                        |
| ---- | ---------- | ---------------------------------- |
| 0    | HAS_BLOOM  | File contains bloom filter section |
| 1    | HAS_STATS  | File contains statistics block     |
| 2    | TOMBSTONES | File may contain tombstone entries |
| 3-15 | Reserved   | Must be zero                       |

### §2.4 Data Block Format

Each data block contains sorted key-value entries with prefix compression.

```text
Block Header (16 bytes):

────────────────────────────────────────────────────────────────────────
0       4     BLOCK_LEN   : u32   compressed block length (excl. header)
4       4     UNCOMP_LEN  : u32   uncompressed length
8       4     ENTRY_COUNT : u32   number of entries in block
12      4     CRC32       : u32   CRC of compressed data
────────────────────────────────────────────────────────────────────────
```

Block Data (after decompression):

```text
┌───────────────────────────────────────────────┐
│ Restart Points Section                        │
│   [restart_count    : u32]                    │
│   [restart_offset_0 : u32]                    │
│   [restart_offset_1 : u32]                    │
│   ...                                         │
│   [restart_offset_{restart_count-1} : u32]    │
├───────────────────────────────────────────────┤
│ Entries Section                               │
│   Entry 0 (full key)                          │
│   Entry 1 (prefix-compressed key)             │
│   ...                                         │
│   Entry N (at restart point, full key)        │
│   ...                                         │
└───────────────────────────────────────────────┘
```

### §2.5 Entry Format (within Data Block)

```text
Entry:
    [shared_len   : varint] - bytes shared with previous key
    [unshared_len : varint] - bytes not shared (suffix length)
    [value_len    : varint] - value length (0 = tombstone)
    [key_suffix   : [u8]  ] - unshared_len bytes
    [value        : [u8]  ] - value_len bytes
```

Varint encoding follows the standard LEB128 format (little-endian base-128).

### §2.6 Index Block

The index block maps block boundaries to file offsets.

```text
Index Block Header (8 bytes):
    [entry_count : u32]
    [reserved    : u32]

Index Entry (repeated):
    [block_offset  : u64 ] - file offset of data block
    [block_len     : u32 ] - compressed block length
    [first_key_len : u32 ] - length of first key in block
    [first_key     : [u8]] - first key bytes
    [last_key_len  : u32 ] - length of last key in block
    [last_key      : [u8]] - last key bytes
```

### §2.7 Bloom Filter Section

```text
Bloom Filter Header (16 bytes):
    [filter_len    : u32    ] - length of filter data
    [num_hash_func : u8     ] - number of hash functions (k)
    [reserved      : [u8; 3]]
    [num_keys      : u64    ] - number of keys in the filter

Bloom Filter Data:
    [bits : [u8]] - filter_len bytes of bit array
```

### §2.8 Footer (48 bytes)

```text
Offset  Size  Field                Description
──────────────────────────────────────────────────────────────────────────
0       8     INDEX_OFFSET : u64   file offset of index block
8       4     INDEX_LEN    : u32   length of index block
12      8     BLOOM_OFFSET : u64   file offset of bloom filter (0 if none)
20      4     BLOOM_LEN    : u32   length of bloom filter (0 if none)
24      8     MIN_KEY_OFF  : u64   offset of min key in file
32      4     MIN_KEY_LEN  : u32   length of min key
36      4     MAX_KEY_LEN  : u32   length of max key
40      4     CRC32        : u32   CRC of footer bytes [0..40]
44      4     MAGIC        : u32   'SSTF' (0x53535446) - footer magic
──────────────────────────────────────────────────────────────────────────
Total: 48 bytes
```

## §3 Schema Metadata Format

Schema definitions are stored in `schemas.bin`. Eventually we will replace this way of storing schema with a more robust format.

### §3.1 File Layout

```text
┌─────────────────────────────────────┐
│ Header (16 bytes)                   │
├─────────────────────────────────────┤
│ Schema Entries                      │
│   [count : u32]                     │
│   [Schema 0]                        │
│   [Schema 1]                        │
│   ...                               │
├─────────────────────────────────────┤
│        Footer (8 bytes)             │
└─────────────────────────────────────┘
```

### §3.2 Header (16 bytes)

```text
Offset  Size  Field              Description
───────────────────────────────────────────────────────────────
0       4     MAGIC        : u32   'SCHM' (0x5343484D)
4       2     VERSION      : u16   format version (currently 1)
6       2     FLAGS        : u16   reserved, must be zero
8       4     SCHEMA_COUNT : u32  number of schemas
12      4     RESERVED     : u32   must be zero
───────────────────────────────────────────────────────────────
Total: 16 bytes
```

### §3.3 Schema Entry Format

```text
Schema Entry:
    [schema_type : u8       ] - 0=Table, 1=Collection, 2=KeySpace
    [name        : String   ] - schema name
    [payload     : variable ] - type-specific payload
```

#### §3.3.1 Table Schema (type = 0)

```text
Table Schema Payload:
    [column_count : u32]
    [Column 0][Column 1]...[Column {column_count-1}]
    [pk_count    : u32]
    [pk_column_name : String] (repeated pk_count times)
    [index_count : u32]
    [Index 0][Index 1]...[Index {index_count-1}]
```

#### §3.3.2 Column Definition

```text
Column:
    [name        : String ]
    [type        : u8     ] - see protocol.md for type tags
    [flags       : u8     ] - bit 0: nullable, bit 1: is_primary, bit 2: is_unique
    [has_default : u8     ] - 0 = no default, 1 = has default
    [default     : Value  ] - if has_default == 1
```

#### §3.3.3 Index Definition

```text
Index:
    [name          : String ]
    [column_count  : u32    ]
    [column_name_0 : String ] (repeated column_count times)
    [flags         : u8     ] - bit 0: unique
    [index_type    : u8     ] - 0=BTree, 1=Hash, 2=FullText, 3=Spatial
```

#### §3.3.4 Collection Schema (type = 1)

```text
Collection Schema Payload:
    [has_validation : u8             ]
    [validation     : ValidationRule ] - only if has_validation == 1
    [index_count    : u32            ]
    [Index 0][Index 1]...[Index {index_count-1}]
```

#### §3.3.5 Validation Rule

```text
ValidationRule:
    [rule_type : u8    ] - 0=JSONSchema, 1=Custom
    [rule_data : String] - JSON schema or custom rule definition
```

#### §3.3.6 KeySpace Schema (type = 2)

```text
KeySpace Schema Payload:
    [flags        : u8 ] - bit 0: ttl_enabled, bit 1: persistent (vs memory)
    [has_max_size : u8 ]
    [max_size     : u64] - only if has_max_size == 1
```

### §3.4 Footer (8 bytes)

```text
Offset  Size  Field              Description
───────────────────────────────────────────────────────────────────
0       4     CRC32      : u32   CRC of all preceding bytes
4       4     MAGIC      : u32   'SCMF' (0x53434D46) - footer magic
───────────────────────────────────────────────────────────────────
Total: 8 bytes
```

## §4 Secondary Index Format

Secondary indexes map field values to primary keys for more efficient lookups.

### §4.1 Index File Layout

```text
┌────────────────────────────────┐
│ Header (32 bytes)              │
├────────────────────────────────┤
│ Index Entries                  │
│   [entry_count : u32]          │
│   [Index Entry 0]              │
│   [Index Entry 1]              │
│   ...                          │
├────────────────────────────────┤
│ Footer (16 bytes)              │
└────────────────────────────────┘
```

### §4.2 Index Entry

```text
IndexEntry:
    [key_len      : u32  ]
    [key          : [u8] ] - serialized index key (composite values)
    [pk_count     : u32  ] - number of primary keys
    [pk_len       : u32  ] - length of each primary key
    [primary_keys : [u8] ] - pk_count * pk_len bytes
```

## §5 Manifest Format

The manifest tracks the current state of the LSM tree.

### §5.1 Manifest File Layout

```text
┌─────────────────────────────────────┐
│ Header (32 bytes)                   │
├─────────────────────────────────────┤
│ Version Edits                       │
│   (append-only log of changes)      │
└─────────────────────────────────────┘
```

### §5.2 Header (32 bytes)

```text
Offset  Size  Field              Description
─────────────────────────────────────────────────────────────────
0       4     MAGIC         : u32   'MNFT' (0x4D4E4654)
4       2     VERSION       : u16   format version (currently 1)
6       2     FLAGS         : u16   reserved
8       8     CREATED_AT    : u64   microseconds since UNIX epoch
16      8     NEXT_FILE_NUM : u64 next SSTable file number
24      8     LOG_NUMBER    : u64   current WAL log number
─────────────────────────────────────────────────────────────────
Total: 32 bytes
```

### §5.3 Version Edit

```text
VersionEdit:
    [edit_type : u8       ]
    [payload   : variable ]

Edit Types:
    0x01 = AddFile
    0x02 = DeleteFile
    0x03 = CompactPointer
    0x04 = NextFileNumber
    0x05 = LogNumber
```

#### §5.3.1 AddFile Edit

```text
AddFile:
  [level         : u8]
  [file_number   : u64]
  [file_size     : u64]
  [min_key_len   : u32]
  [min_key       : [u8]]
  [max_key_len   : u32]
  [max_key       : [u8]]
```

#### §5.3.2 DeleteFile Edit

```text
DeleteFile:
  [level       : u8]
  [file_number : u64]
```

## §6 Varint Encoding

All varints use unsigned LEB128 encoding as defined in the [LEB128 specification](https://en.wikipedia.org/wiki/LEB128).

```text
while value >= 0x80
    emit (value & 0x7F) | 0x80
    value >>= 7

emit (value & 0x7F)
```

Maximum varint size is 10 bytes for u64 values.

## §7 Checksum Calculation

CRC32 (Castagnoli) is used throughout the storage format for integrity checks. Using this algorithm allows us to make use of hardware acceleration on supported platforms (SSE4.2, ARMv8, etc.).

## §8 Compression

Supported compression algorithms:

| Code | Algorithm | Block Size | Notes                  |
| ---- | --------- | ---------- | ---------------------- |
| 0    | None      | N/A        | Raw data               |
| 1    | LZ4       | 64KB       | Fast, moderate ratio   |
| 2    | Snappy    | 64KB       | Very fast, lower ratio |
| 3    | Zstd      | 128KB      | Best ratio, slower     |

Compression is applied at the data block level. Each block is compressed independently.

## §9 Key Encoding

Keys are variable-length byte arrays. For composite keys:

```text
CompositeKey:
    [component_count : u8]
    [Component 0][Component 1]...[Component {component_count-1}]

Component:
    [type    : u8       ] - see protocol.md for type tags
    [payload : variable ] - value encoding
```

Sort order is lexicographic on the encoded bytes.
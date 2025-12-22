.. _protocol:

Protocol Specification
======================

Binary wire protocol version 3. Little-endian byte order throughout.

Frame Format
------------

All messages are length-prefixed:

.. code-block:: text

   +------------------+------------------+------------------+
   | frame_len (u32)  | header           | body             |
   +------------------+------------------+------------------+
   4 bytes LE         8 bytes            variable

- ``frame_len``: Length of header + body (excludes the 4-byte length field)
- ``header``: 8-byte message header
- ``body``: Message payload

Header Format
^^^^^^^^^^^^^

.. code-block:: text

   +----------+--------+----------+--------+---------------------+
   | version  | kind   | command  | flags  | correlation_id      |
   +----------+--------+----------+--------+---------------------+
   1 byte     1 byte   1 byte     1 byte   4 bytes LE

- ``version``: ``0x03``
- ``kind``: ``0x00`` = Request, ``0x01`` = Response
- ``command``: See tables below
- ``flags``: Reserved (``0x00``)
- ``correlation_id``: Client-assigned ID for request/response matching

Request Commands
----------------

.. list-table:: Request Command Codes
   :widths: 10 20 70
   :header-rows: 1

   * - Code
     - Command
     - Description
   * - 0x01
     - Hello
     - Initial handshake (no authentication)
   * - 0x02
     - Authenticate
     - Authenticate the connection
   * - 0x03
     - Disconnect
     - Graceful disconnect
   * - 0x04
     - Ping
     - Health check / keepalive
   * - 0x05
     - Query
     - Execute a single query
   * - 0x06
     - Batch
     - Execute multiple queries sequentially
   * - 0x07
     - TxBegin
     - Begin a transaction
   * - 0x08
     - TxCommit
     - Commit a transaction
   * - 0x09
     - TxRollback
     - Rollback a transaction
   * - 0x0A
     - Describe
     - Get schema for a table/collection
   * - 0x0B
     - List
     - List all tables/collections
   * - 0x0C
     - Stats
     - Get server statistics

Hello (0x01)
^^^^^^^^^^^^

.. code-block:: text

   +---------------------+----------------------+
   | client_name         | capabilities         |
   +---------------------+----------------------+
   string                string[]

Authenticate (0x02)
^^^^^^^^^^^^^^^^^^^

.. code-block:: text

   +------------+-------------------+
   | method_tag | method_payload    |
   +------------+-------------------+
   1 byte       variable

Method tags: ``0x01`` Password, ``0x02`` Token, ``0x03`` Certificate (no payload)

.. code-block:: text

   Password (0x01):  [username: string] [password: string]
   Token (0x02):     [token: string]

Disconnect (0x03)
^^^^^^^^^^^^^^^^^

No body.

Ping (0x04)
^^^^^^^^^^^

No body.

Query (0x05)
^^^^^^^^^^^^

.. code-block:: text

   +-------------+---------------+-----------------+
   | statement   | params_count  | params...       |
   +-------------+---------------+-----------------+
   string        u32 LE          Value[]

Batch (0x06)
^^^^^^^^^^^^

.. code-block:: text

   +------------------+--------------------+
   | statements_count | statements...      |
   +------------------+--------------------+
   u32 LE             Statement[]

Each Statement: ``[query: string] [params_count: u32] [params: Value[]]``

TxBegin (0x07)
^^^^^^^^^^^^^^

.. code-block:: text

   +-----------+-----------+
   | isolation | read_only |
   +-----------+-----------+
   1 byte      1 byte

Isolation: ``0x01`` ReadUncommitted, ``0x02`` ReadCommitted, ``0x03`` RepeatableRead, ``0x04`` Serializable

TxCommit (0x08)
^^^^^^^^^^^^^^^

.. code-block:: text

   +-----------+
   | tx_id     |
   +-----------+
   u64 LE

TxRollback (0x09)
^^^^^^^^^^^^^^^^^

.. code-block:: text

   +-----------+
   | tx_id     |
   +-----------+
   u64 LE

Describe (0x0A)
^^^^^^^^^^^^^^^

.. code-block:: text

   +-----------+
   | target    |
   +-----------+
   string

List (0x0B)
^^^^^^^^^^^

No body.

Stats (0x0C)
^^^^^^^^^^^^

.. code-block:: text

   +-----------+
   | detailed  |
   +-----------+
   1 byte

Response Commands
-----------------

.. list-table:: Response Command Codes
   :widths: 10 20 70
   :header-rows: 1

   * - Code
     - Response
     - Description
   * - 0x01
     - Welcome
     - Handshake acknowledgment
   * - 0x02
     - AuthSuccess
     - Authentication succeeded
   * - 0x03
     - AuthFailed
     - Authentication failed
   * - 0x04
     - Pong
     - Ping response
   * - 0x05
     - QueryResult
     - Single query result
   * - 0x06
     - BatchResults
     - Batch query results
   * - 0x07
     - TxStarted
     - Transaction started
   * - 0x08
     - TxCommitted
     - Transaction committed
   * - 0x09
     - TxRolledBack
     - Transaction rolled back
   * - 0x0A
     - TableList
     - List of tables
   * - 0x0B
     - TableDescription
     - Table schema description
   * - 0x0C
     - StatsResult
     - Server statistics
   * - 0x0D
     - Ok
     - Generic success acknowledgment
   * - 0x0E
     - Error
     - Error response

Welcome (0x01)
^^^^^^^^^^^^^^

.. code-block:: text

   +----------------+---------------------+------------------+
   | server_version | server_capabilities | server_timestamp |
   +----------------+---------------------+------------------+
   string           string[]              u64 LE

AuthSuccess (0x02)
^^^^^^^^^^^^^^^^^^

.. code-block:: text

   +------------+----------+-------------+------------+
   | session_id | user_id  | permissions | expires_at |
   +------------+----------+-------------+------------+
   u64 LE       string     PermissionSet opt_u64

The permissions field contains a binary-encoded ``PermissionSet`` (see :ref:`permissions`).
Format: ``[count: u32 LE] [permission_1] [permission_2] ... [permission_n]``

Each permission is encoded as: ``[resource] [action_byte]``

AuthFailed (0x03)
^^^^^^^^^^^^^^^^^

.. code-block:: text

   +----------+-------------+
   | reason   | retry_after |
   +----------+-------------+
   string     opt_u64

Pong (0x04)
^^^^^^^^^^^

.. code-block:: text

   +-------------+
   | timestamp   |
   +-------------+
   u64 LE

QueryResult (0x05)
^^^^^^^^^^^^^^^^^^

.. code-block:: text

   +----------------+--------------+
   | outcome        | elapsed_ms   |
   +----------------+--------------+
   QueryOutcome     u64 LE

BatchResults (0x06)
^^^^^^^^^^^^^^^^^^^

.. code-block:: text

   +---------------+-------------------+--------------+
   | results_count | results...        | elapsed_ms   |
   +---------------+-------------------+--------------+
   u32 LE          QueryOutcome[]      u64 LE

TxStarted (0x07)
^^^^^^^^^^^^^^^^

.. code-block:: text

   +-----------+----------------+
   | tx_id     | read_timestamp |
   +-----------+----------------+
   u64 LE      u64 LE

TxCommitted (0x08)
^^^^^^^^^^^^^^^^^^

.. code-block:: text

   +-----------+------------------+
   | tx_id     | commit_timestamp |
   +-----------+------------------+
   u64 LE      u64 LE

TxRolledBack (0x09)
^^^^^^^^^^^^^^^^^^^

.. code-block:: text

   +-----------+
   | tx_id     |
   +-----------+
   u64 LE

TableList (0x0A)
^^^^^^^^^^^^^^^^

.. code-block:: text

   +--------------+-------------+
   | tables_count | tables...   |
   +--------------+-------------+
   u32 LE         TableInfo[]

TableInfo: ``[name: string] [schema: opt_string] [row_count: opt_u64] [size_bytes: opt_u64]``

TableDescription (0x0B)
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

   +--------+---------------+------------+---------------+-------------+
   | name   | columns_count | columns... | indexes_count | indexes...  |
   +--------+---------------+------------+---------------+-------------+
   string   u32 LE          ColumnInfo[] u32 LE          IndexInfo[]

ColumnInfo: ``[name: string] [data_type: string] [nullable: u8] [default_value: opt_value]``

IndexInfo: ``[name: string] [columns: string[]] [unique: u8]``

StatsResult (0x0C)
^^^^^^^^^^^^^^^^^^

.. code-block:: text

   +----------------+--------------------+--------------+----------------+---------------+
   | uptime_seconds | active_connections | total_queries| cache_hit_rate | storage       |
   +----------------+--------------------+--------------+----------------+---------------+
   u64 LE           u64 LE               u64 LE         f64 LE           opt_StorageStats

StorageStats (if present, prefixed with 0x01): ``[total_size_bytes: u64] [document_count: u64] [table_count: u64] [keyspace_entries: u64]``

Ok (0x0D)
^^^^^^^^^

No body.

Error (0x0E)
^^^^^^^^^^^^

.. code-block:: text

   +--------+----------+---------+
   | code   | message  | details |
   +--------+----------+---------+
   u16 LE   string     opt_value

QueryOutcome
------------

Tagged union for query results.

.. list-table:: QueryOutcome Tags
   :widths: 10 20 70
   :header-rows: 1

   * - Tag
     - Outcome
     - Description
   * - 0x01
     - Rows
     - Query returned rows/documents
   * - 0x02
     - Inserted
     - Insert operation result
   * - 0x03
     - Updated
     - Update operation result
   * - 0x04
     - Deleted
     - Delete operation result
   * - 0x05
     - Dropped
     - Drop operation result
   * - 0x06
     - Executed
     - DDL operation executed

Rows (0x01)
^^^^^^^^^^^

``[0x01] [row_count: u64] [data: Value[]] [columns: opt_str[]] [has_more: u8]``

Inserted (0x02)
^^^^^^^^^^^^^^^

``[0x02] [rows_inserted: u64] [generated_ids: opt_value[]]``

Updated (0x03)
^^^^^^^^^^^^^^

``[0x03] [rows_updated: u64]``

Deleted (0x04)
^^^^^^^^^^^^^^

``[0x04] [rows_deleted: u64]``

Dropped (0x05)
^^^^^^^^^^^^^^

``[0x05] [object_type: string] [object_name: string]``

Executed (0x06)
^^^^^^^^^^^^^^^

``[0x06]`` - no payload.

Primitive Encoding
------------------

string
^^^^^^

``[length: u32 LE] [UTF-8 data]``

string[]
^^^^^^^^

``[count: u32 LE] [strings...]``

Optional values
^^^^^^^^^^^^^^^

``[0x00]`` = absent, ``[0x01] [value]`` = present

Value Binary Format
-------------------

Type tag (1 byte) followed by payload.

Type Tags
^^^^^^^^^

.. list-table:: Type Tag Reference
   :widths: 10 20 70
   :header-rows: 1

   * - Tag
     - Type
     - Description
   * - 0
     - Null
     - Represents absence of value
   * - 1
     - Bool
     - Boolean true/false
   * - 2
     - Int32
     - 32-bit signed integer
   * - 3
     - Int64
     - 64-bit signed integer
   * - 4
     - Float32
     - 32-bit IEEE 754 floating point
   * - 5
     - Float64
     - 64-bit IEEE 754 floating point
   * - 6
     - String
     - UTF-8 encoded string
   * - 7
     - Binary
     - Raw binary data
   * - 8
     - DateTime
     - Timestamp with timezone offset
   * - 9
     - Date
     - Calendar date (year, month, day)
   * - 10
     - Time
     - Time of day (hour, minute, second, microseconds)
   * - 11
     - Uuid
     - 128-bit universally unique identifier
   * - 12
     - ObjectId
     - 96-bit MongoDB-style object identifier
   * - 13
     - Array
     - Ordered collection of values
   * - 14
     - Object
     - Key-value map (sorted by key)
   * - 15
     - Set
     - Unordered collection of unique strings
   * - 16
     - Row
     - Ordered key-value map (preserves insertion order)
   * - 17
     - SortedSet
     - Scored set (Redis ZSET-style)
   * - 18
     - GeoPoint
     - Geographic coordinates (latitude, longitude)
   * - 19
     - Reference
     - Foreign key / document reference

Primitives
^^^^^^^^^^

- **Null (0)**: no payload
- **Bool (1)**: 1 byte
- **Int32 (2)**: i32 LE
- **Int64 (3)**: i64 LE
- **Float32 (4)**: f32 bits LE
- **Float64 (5)**: f64 bits LE
- **String (6)**: ``[len: u32] [UTF-8]``
- **Binary (7)**: ``[len: u32] [bytes]``

Date/Time
^^^^^^^^^

- **DateTime (8)**: ``[micros: i64] [offset_minutes: i32]``
- **Date (9)**: ``[year: i32] [month: u8] [day: u8]``
- **Time (10)**: ``[hour: u8] [minute: u8] [second: u8] [micros: u32]``

Identifiers
^^^^^^^^^^^

- **Uuid (11)**: 16 bytes
- **ObjectId (12)**: 12 bytes

Collections
^^^^^^^^^^^

- **Array (13)**: ``[count: u32] [values...]``
- **Object (14)**: ``[count: u32] [key-value pairs...]`` (sorted keys)
- **Set (15)**: ``[count: u32] [strings...]``
- **Row (16)**: Same as Object but preserves insertion order

Special
^^^^^^^

- **SortedSet (17)**: ``[count: u32] [[score: f64] [member: string]...]``
- **GeoPoint (18)**: ``[lat: f64] [lng: f64]``
- **Reference (19)**: ``[collection: string] [id: Value]``

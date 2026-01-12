.. _storage:

Storage Engine
==============

The storage engine is the heart of any database. For Prototype 3, I decided to
completely rewrite it from scratch. The Prototype 2 implementation had grown to
roughly 4,000 lines of tightly coupled code that was becoming increasingly difficult
to maintain and extend. This document chronicles the design decisions, implementation
challenges, and debugging process for the new storage engine.

Why Rewrite?
------------

The Prototype 2 storage layer had several fundamental issues:

1. **Tight coupling** - The B+Tree, buffer pool, and MVCC logic were intertwined
2. **No clear abstractions** - Adding a new storage backend meant touching everything
3. **Incomplete MVCC** - Transaction visibility was handled inconsistently
4. **Poor testability** - Components couldn't be tested in isolation

For Prototype 3, I wanted a modular design where each component could be developed,
tested, and potentially replaced independently.

Architecture Overview
---------------------

The new storage engine supports three distinct storage paradigms, each optimised for
different use cases:

.. mermaid::

   graph TB
       subgraph "Storage Engine Facade"
           E[StorageEngine]
       end

       subgraph "Storage Backends"
           M[MVCC Tables<br/>Relational]
           D[Document Stores<br/>Versioned]
           K[Keyspaces<br/>Key-Value]
       end

       subgraph "Core Infrastructure"
           B[B+Tree]
           BP[Buffer Pool]
           DM[Disk Manager]
           W[WAL]
       end

       E --> M
       E --> D
       E --> K
       
       M --> B
       D --> B
       K --> B
       
       B --> BP
       BP --> DM
       E --> W

The ``StorageEngine`` acts as a facade, providing a unified API while delegating to
specialised backends. Each backend uses B+Trees for ordered storage, with the buffer
pool managing memory and the disk manager handling I/O.

Module Breakdown
^^^^^^^^^^^^^^^^

The implementation is split across 12 modules, totalling approximately 2,500 lines -
a 40% reduction from Prototype 2 while providing more functionality:

- ``traits.rs`` - Core trait definitions
- ``page.rs`` - 16KB page structures
- ``disk.rs`` - Memory-mapped I/O
- ``buffer.rs`` - LRU buffer pool with clock-sweep eviction
- ``btree.rs`` - Generic B+Tree implementation
- ``mvcc.rs`` - Multi-version concurrency control
- ``document.rs`` - Versioned document store
- ``keyspace.rs`` - Simple key-value storage
- ``wal.rs`` - Write-ahead logging
- ``engine.rs`` - Unified facade

Page Format
-----------

All persistent data uses 16KB pages. I chose this size as a balance between I/O
efficiency (larger pages mean fewer disk operations) and memory efficiency (smaller
pages waste less space for small records).

Each page has a 64-byte header:

.. code-block:: text

    +------------------+
    | Magic (4 bytes)  |  "MONO" (0x4D4F4E4F)
    +------------------+
    | Version (1 byte) |  Currently 1
    +------------------+
    | Type (1 byte)    |  Leaf/Interior/Free/Overflow/Meta
    +------------------+
    | Flags (2 bytes)  |  Reserved for future use
    +------------------+
    | Page ID (8 bytes)|  Unique identifier
    +------------------+
    | Key Count (2)    |  Number of keys
    +------------------+
    | Free Space (2)   |  Available bytes
    +------------------+
    | Next Page (8)    |  For linked lists (leaf scanning)
    +------------------+
    | Prev Page (8)    |  Back pointer
    +------------------+
    | Parent (8)       |  Parent page for trees
    +------------------+
    | LSN (8)          |  Log sequence number for WAL
    +------------------+
    | Checksum (4)     |  CRC32 of entire page
    +------------------+
    | Reserved (8)     |  Future expansion
    +------------------+
    | Data (16,320 B)  |  Actual content
    +------------------+

The checksum is computed over the entire page (excluding the checksum field itself)
using CRC32. This allows detection of corrupted pages during reads.

MVCC Implementation
-------------------

The MVCC (Multi-Version Concurrency Control) system was the trickiest part to get
right. It provides snapshot isolation for relational tables, allowing concurrent
transactions to see consistent views of data.

Each record carries visibility metadata:

.. code-block:: rust

    struct MvccRecord<V> {
        xmin: TxId,        // Transaction that created this version
        xmax: TxId,        // Transaction that deleted this version (0 if live)
        cmin: Timestamp,   // Commit timestamp of xmin
        cmax: Timestamp,   // Commit timestamp of xmax  
        data: V,           // The actual value
    }

Transaction Lifecycle
^^^^^^^^^^^^^^^^^^^^^

.. mermaid::

   sequenceDiagram
       participant C as Client
       participant TM as Transaction Manager
       participant T as MvccTable
       participant B as B+Tree

       C->>TM: begin()
       TM->>TM: Allocate TxId<br/>Create Snapshot
       TM->>C: Transaction { id, snapshot }

       C->>T: write(key, value)
       T->>T: Create versioned key<br/>(key + timestamp)
       T->>TM: record_versioned_key(tx_id, key)
       T->>B: insert(versioned_key, MvccRecord)
       T->>C: Ok(())

       C->>TM: commit(tx_id)
       TM->>TM: Allocate commit timestamp
       TM->>T: finalize_commit(tx_id, commit_ts)
       T->>B: Update cmin/cmax on written records
       TM->>C: Committed

When a transaction commits, ``finalize_commit()`` iterates through all keys written
by that transaction and stamps them with the commit timestamp. This is crucial for
visibility - other transactions need to know *when* a record was committed, not just
*that* it was committed.

Visibility Rules
^^^^^^^^^^^^^^^^

A record is visible to a snapshot if all of these conditions are met:

1. The creating transaction (xmin) is committed
2. The creating transaction committed before the snapshot's timestamp
3. Either no deleting transaction exists (xmax == 0), OR the deleting transaction
   is not committed, OR it committed after the snapshot

This is implemented in the ``is_visible()`` method:

.. code-block:: rust

    fn is_visible(&self, record: &MvccRecord<V>, snapshot: &Snapshot) -> bool {
        // Creator must be committed before our snapshot
        if record.cmin == 0 || record.cmin > snapshot.timestamp {
            return false;
        }
        
        // If deleted, the deletion must be after our snapshot
        if record.xmax != 0 && record.cmax != 0 && record.cmax <= snapshot.timestamp {
            return false;
        }
        
        true
    }

Document Store
--------------

For document-oriented workloads, the full MVCC machinery is overkill. The document
store uses a simpler optimistic concurrency model based on revision numbers:

.. code-block:: rust

    struct Document<V> {
        revision: u64,       // Monotonically increasing
        deleted: bool,       // Tombstone flag
        created_at: u64,     // Creation timestamp
        updated_at: u64,     // Last modification timestamp
        data: V,             // The document content
    }

Updates require the caller to provide the expected revision:

.. code-block:: rust

    pub fn update(&self, key: &K, value: V, expected_rev: u64) -> Result<u64> {
        let existing = self.documents.get(key)?
            .ok_or(MonoError::NotFound("document not found"))?;
        
        if existing.revision != expected_rev {
            return Err(MonoError::WriteConflict(format!(
                "revision mismatch: expected {}, got {}",
                expected_rev, existing.revision
            )));
        }
        
        // Save old version to history, create new version
        // ...
    }

This pattern prevents lost updates without requiring locks - if two clients try to
update the same document simultaneously, one will fail with a revision mismatch and
can retry with the fresh data.

Write-Ahead Log
---------------

The WAL ensures durability by logging all mutations before they're applied:

.. mermaid::

   sequenceDiagram
       participant C as Client
       participant E as Engine
       participant W as WAL
       participant B as Buffer Pool
       participant D as Disk

       C->>E: insert(key, value)
       E->>W: log_insert(tx_id, key, value)
       W->>W: Serialize to buffer
       
       alt Buffer full OR sync interval
           W->>D: fsync() to WAL file
       end
       
       W->>E: LSN
       E->>B: Apply change in memory
       E->>C: Ok(LSN)

       Note over B,D: Later, during checkpoint...
       B->>D: Flush dirty pages

Entry Types
^^^^^^^^^^^

The WAL supports these entry types:

.. code-block:: rust

    pub enum WalEntryType {
        Insert = 1,      // New record
        Update = 2,      // Record modified
        Delete = 3,      // Record removed
        TxBegin = 4,     // Transaction started
        TxCommit = 5,    // Transaction committed
        TxRollback = 6,  // Transaction aborted
        Checkpoint = 7,  // Data flushed to disk
    }

Each entry is prefixed with a 25-byte header containing the LSN, transaction ID,
entry type, payload length, and a CRC32 checksum.

Group Commit
^^^^^^^^^^^^

For better throughput, the WAL batches writes using a background writer thread.
Entries are buffered and flushed when:

- The buffer exceeds 64KB, OR
- 100ms have elapsed since the last flush

Transaction commits always force an immediate sync to ensure durability.

Debugging Journey
-----------------

Getting all these pieces to work together was... challenging. Here's a chronicle of
the bugs I encountered and how I fixed them.

Issue 1: PhantomData for MvccTable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Symptom**: Compiler error E0392 - type parameter ``K`` is never used.

**Root Cause**: The ``MvccTable<K, V>`` struct stored keys as serialized ``Vec<u8>``
internally (for versioned key construction), so the ``K`` type parameter wasn't
actually used in any fields.

**Solution**: Added ``PhantomData<K>`` to preserve the type parameter for API safety:

.. code-block:: rust

    pub struct MvccTable<K, V> {
        tree: BTree<Vec<u8>, MvccRecord<V>>,
        tx_manager: Arc<TransactionManager>,
        _phantom: PhantomData<K>,  // Preserves K for type safety
    }

Issue 2: finalize_commit was a stub
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Symptom**: Records were marked as committed in the transaction manager, but their
``cmin``/``cmax`` fields were always 0. Visibility checks failed unpredictably.

**Root Cause**: The initial ``finalize_commit`` was literally an empty function! I'd
written it as a placeholder and forgot to implement it.

**Solution**: Implemented proper tracking in the ``TransactionManager``:

.. code-block:: rust

    // Track which keys each transaction writes
    tx_written_keys: DashMap<TxId, Vec<Vec<u8>>>,

    pub fn finalize_commit(&self, tx_id: TxId, commit_ts: Timestamp) -> Result<()> {
        let written_keys = self.tx_manager.take_written_keys(tx_id);
        
        for versioned_key in written_keys {
            if let Ok(Some(mut record)) = self.tree.get(&versioned_key) {
                if record.xmin == tx_id && record.cmin == 0 {
                    record.cmin = commit_ts;
                }
                if record.xmax == tx_id && record.cmax == 0 {
                    record.cmax = commit_ts;
                }
                self.tree.insert(versioned_key, record)?;
            }
        }
        Ok(())
    }

Issue 3: WAL header size was wrong
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Symptom**: ``thread panicked at 'range end index 25 is out of bounds'``

**Root Cause**: The ``WalEntryHeader`` had ``const SIZE: usize = 24``, but the actual
layout required 25 bytes:

- LSN: 8 bytes
- TxId: 8 bytes  
- Type: 1 byte
- Length: 4 bytes
- Checksum: 4 bytes
- **Total: 25 bytes**

I'd miscounted when writing the constant.

**Solution**: Change ``SIZE`` from 24 to 25. Simple fix, frustrating bug.

Issue 4: BTree namespace collision
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Symptom**: ``invalid UTF-8: invalid utf-8 sequence of 1 bytes from index 4``

This was the most insidious bug. It only appeared when using ``DocumentStore`` with
history tracking enabled.

**Investigation**: The error occurred when trying to deserialize a ``Document<String>``
from a B+Tree. But the same code worked fine without history tracking. What changed?

With history tracking, ``DocumentStore`` creates *two* BTrees:

.. code-block:: rust

    pub fn new(pool: Arc<LruBufferPool>, keep_history: bool) -> Result<Self> {
        let history = if keep_history {
            Some(BTree::new(pool.clone())?)  // First BTree
        } else {
            None
        };
        
        Ok(Self {
            documents: BTree::new(pool)?,    // Second BTree
            history,
            // ...
        })
    }

**Root Cause**: Both BTrees shared the same buffer pool. The old ``BTree::new()``
checked ``pool.stats().used > 0`` to decide whether to load existing metadata or
create fresh. After the first BTree was created, the pool showed pages in use, so
the second BTree *loaded the first BTree's metadata instead of creating its own*!

The "documents" BTree was actually pointing to the "history" BTree's root page.
When we tried to read document data, we got history keys - which were ``Vec<u8>``
serialized differently than ``String``.

**Solution**: Refactor BTree for proper namespacing. Each BTree now allocates its
own metadata page:

.. code-block:: rust

    pub struct BTree<K, V> {
        pool: Arc<LruBufferPool>,
        meta_page_id: PageId,  // Each tree has its own metadata page
        meta: RwLock<TreeMeta>,
        // ...
    }

    impl<K, V> BTree<K, V> {
        /// Create a new BTree with its own namespace
        pub fn new(pool: Arc<LruBufferPool>) -> Result<Self> {
            let (meta, meta_page_id) = Self::initialize(&pool)?;
            Ok(Self { pool, meta_page_id, meta: RwLock::new(meta), ... })
        }

        /// Reopen an existing BTree from a known metadata page
        pub fn open(pool: Arc<LruBufferPool>, meta_page_id: PageId) -> Result<Self> {
            let meta = Self::load_meta(&pool, meta_page_id)?;
            Ok(Self { pool, meta_page_id, meta: RwLock::new(meta), ... })
        }

        /// Get metadata page ID for persistence
        pub fn meta_page_id(&self) -> PageId {
            self.meta_page_id
        }
    }

This fix also lays groundwork for higher-level namespacing (database.collection).

Issue 5: doc_history missing current version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Symptom**: Test expected 3 history entries but got 2.

**Root Cause**: The history BTree only stores *past* versions. When you update
v1 → v2, v1 goes into history. Update v2 → v3, v2 goes into history. The current
version (v3) is only in the main documents tree, not history.

But the API semantically should return "all versions" including current.

**Solution**: Modified ``doc_history()`` to prepend the current document:

.. code-block:: rust

    pub fn doc_history(&self, key: &[u8], limit: usize) -> Result<Vec<HistoryEntry>> {
        let mut history = Vec::new();

        // Include current document as first (most recent) entry
        if let Some(current) = doc_store.get(&key)? {
            if !current.deleted {
                history.push(HistoryEntry {
                    revision: current.revision,
                    data: current.data,
                    // ...
                });
            }
        }

        // Add past versions
        let past = doc_store.get_history(&key, limit - history.len())?;
        history.extend(past);

        Ok(history)
    }

Namespacing Foundation
----------------------

The BTree namespace fix (Issue 4) establishes the storage-layer foundation for
proper database/collection namespacing. The architecture separates concerns:

.. mermaid::

   graph TB
       subgraph "Catalog Layer (Future)"
           CAT[Namespace Catalog<br/>"mydb.users" → PageId]
       end

       subgraph "Storage Layer (Implemented)"
           BT1[BTree A<br/>meta @ Page 0]
           BT2[BTree B<br/>meta @ Page 1]
           BT3[BTree C<br/>meta @ Page 2]
       end

       subgraph "Buffer Pool"
           P0[Page 0: Meta A]
           P1[Page 1: Meta B]
           P2[Page 2: Meta C]
           P3[Pages 3+: Data]
       end

       CAT --> BT1
       CAT --> BT2
       CAT --> BT3
       
       BT1 --> P0
       BT2 --> P1
       BT3 --> P2

Each BTree tracks its own metadata page ID. A future catalog can map human-readable
names like ``"mydb.users"`` to the appropriate ``PageId``, then use ``BTree::open()``
to access that specific tree.

Test Results
------------

After fixing all the issues above, all 46 storage tests pass:

.. code-block:: text

    test storage::btree::tests::test_insert_and_get ... ok
    test storage::btree::tests::test_delete ... ok
    test storage::btree::tests::test_update ... ok
    test storage::btree::tests::test_range ... ok
    test storage::btree::tests::test_many_inserts ... ok
    test storage::buffer::tests::test_alloc_and_get ... ok
    test storage::buffer::tests::test_eviction ... ok
    test storage::buffer::tests::test_flush ... ok
    test storage::document::tests::test_insert_and_get ... ok
    test storage::document::tests::test_update ... ok
    test storage::document::tests::test_conflict_detection ... ok
    test storage::document::tests::test_delete ... ok
    test storage::document::tests::test_history ... ok
    test storage::document::tests::test_upsert ... ok
    test storage::mvcc::tests::test_transaction_lifecycle ... ok
    test storage::mvcc::tests::test_snapshot_isolation ... ok
    test storage::mvcc::tests::test_record_visibility ... ok
    test storage::mvcc::tests::test_rollback ... ok
    test storage::engine::tests::test_engine_document_store ... ok
    test storage::engine::tests::test_engine_document_history ... ok
    test storage::engine::tests::test_engine_mvcc_isolation ... ok
    test storage::wal::tests::test_wal_basic_operations ... ok
    test storage::wal::tests::test_wal_replay ... ok
    ... and 23 more

    test result: ok. 46 passed; 0 failed

Future Work
-----------

Potential improvements for future prototypes:

- **Namespace catalog** - Full implementation of database/collection namespacing
- **Secondary indexes** - B+Tree indexes on non-primary columns
- **Compression** - LZ4 compression for pages
- **Parallel recovery** - Multi-threaded WAL replay
- **Incremental checkpoints** - Reduce checkpoint I/O
- **Read replicas** - WAL streaming for replication

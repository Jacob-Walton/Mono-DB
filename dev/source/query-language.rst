Query Language Specification
============================

This document specifies the MonoDB Query Language (MQL), a human-readable
query language designed for both interactive use and programmatic access.

Overview
--------

MQL uses a verb-first syntax with optional clauses. Queries are case-insensitive
for keywords. The language supports:

- **Queries**: Read data (``get``, ``count``, ``describe``)
- **Mutations**: Modify data (``put``, ``change``, ``remove``)
- **DDL**: Schema operations (``make``, ``drop``)
- **Transactions**: (``begin``, ``commit``, ``rollback``)

Lexical Structure
-----------------

Comments
^^^^^^^^

Single-line comments start with ``//``:

.. code-block:: text

    // This is a comment
    get from users  // Inline comment

Identifiers
^^^^^^^^^^^

Identifiers are names for tables, columns, and aliases. They must start with
a letter or underscore and may contain letters, digits, and underscores:

.. code-block:: text

    users
    user_table
    _private
    Table1

Keywords are reserved and cannot be used as identifiers without escaping.

Literals
^^^^^^^^

**Strings**: Enclosed in double quotes with backslash escaping:

.. code-block:: text

    "hello world"
    "escaped \"quote\""
    "line1\nline2"

**Numbers**: Integers and floating-point:

.. code-block:: text

    42
    -17
    3.14159
    0.001

**Booleans**:

.. code-block:: text

    true
    false

**Null**:

.. code-block:: text

    null

**Arrays**: Enclosed in square brackets:

.. code-block:: text

    [1, 2, 3]
    ["a", "b", "c"]
    [1, "mixed", true]

Parameters
^^^^^^^^^^

**Positional parameters** use ``$n`` notation (1-indexed):

.. code-block:: text

    get from users where id = $1
    get from users where age > $1 and active = $2

**Named parameters** use ``:name`` notation:

.. code-block:: text

    get from users where id = :user_id
    get from users where email = :email

Query Statements
----------------

GET Query
^^^^^^^^^

Retrieves records from a table.

**Syntax**:

.. code-block:: text

    get [fields] from <table> [where <condition>] [order by <fields>] [take <n>] [skip <n>]

**Examples**:

.. code-block:: text

    // Get all records
    get from users

    // Get specific fields
    get name, email from users

    // With filtering
    get from users where age > 21

    // With sorting (ascending is default)
    get from users order by name
    get from users order by age desc

    // With pagination
    get from users take 10 skip 20

    // Combined
    get name, email from users where active = true order by name take 50

**Multi-line syntax** (indentation-sensitive):

.. code-block:: text

    get from users
      where
        age > 21
        active = true
      order by name
      take 10

COUNT Query
^^^^^^^^^^^

Counts records matching a condition.

**Syntax**:

.. code-block:: text

    count [from] <table> [where <condition>]

**Examples**:

.. code-block:: text

    count from users
    count users where active = true
    count from orders where total > 100

DESCRIBE Query
^^^^^^^^^^^^^^

Returns schema information for a table.

**Syntax**:

.. code-block:: text

    describe <table>

**Examples**:

.. code-block:: text

    describe users
    describe orders

Mutation Statements
-------------------

PUT Mutation
^^^^^^^^^^^^

Inserts a new record.

**Syntax**:

.. code-block:: text

    put into <table> <field> = <value> [, <field> = <value>]*

**Examples**:

.. code-block:: text

    // Single-line
    put into users name = "Alice", age = 30, email = "alice@example.com"

    // Multi-line (indented)
    put into users
      name = "Alice"
      age = 30
      email = "alice@example.com"
      active = true

CHANGE Mutation
^^^^^^^^^^^^^^^

Updates existing records.

**Syntax**:

.. code-block:: text

    change <table> [where <condition>] <field> = <value> [, <field> = <value>]*

**Examples**:

.. code-block:: text

    // Update specific record
    change users where id = 1 name = "Bob"

    // Update multiple fields
    change users where id = 1 name = "Bob", age = 31

    // Multi-line
    change users where id = 1
      name = "Bob"
      age = 31
      updated_at = null

REMOVE Mutation
^^^^^^^^^^^^^^^

Deletes records. **Requires a WHERE clause for safety**.

**Syntax**:

.. code-block:: text

    remove from <table> where <condition>

**Examples**:

.. code-block:: text

    remove from users where id = 1
    remove from sessions where expires_at < $1

DDL Statements
--------------

MAKE TABLE
^^^^^^^^^^

Creates a new table or collection.

**Syntax**:

.. code-block:: text

    make table <name>
      as <type>
      [fields]          // Required for relational, optional for document/keyspace
        <field> <type> [constraints...]
        ...
      [ttl <seconds>]   // Optional TTL for automatic expiry

**Table Types**:

- ``relational``: Traditional structured tables with fixed schema (requires ``fields``)
- ``document``: Flexible document storage with JSON-like records (schema-less)
- ``keyspace``: Key-value storage for simple lookups (schema-less)

**Data Types** (for relational tables):

- ``int``: 32-bit integer
- ``bigint``: 64-bit integer
- ``text``: UTF-8 string
- ``decimal``: 32-bit floating point
- ``double``: 64-bit floating point
- ``date``: Date/time value
- ``boolean``: True/false
- ``map``: JSON object
- ``list``: JSON array

**Column Constraints**:

- ``primary key``: Unique identifier for the row
- ``unique``: Values must be unique
- ``required``: Cannot be null
- ``default <value>``: Default value if not provided

**Examples**:

.. code-block:: text

    // Relational table with schema
    make table users
      as relational
      fields
        id bigint primary key
        name text required
        email text unique
        age int
        active boolean default true
        created_at date default now()

    // Document collection (schema-less)
    make table posts
      as document

    // Keyspace with TTL
    make table sessions
      as keyspace
      ttl 3600

MAKE INDEX
^^^^^^^^^^

Creates an index on a table.

**Syntax**:

.. code-block:: text

    make [unique] index <name> on <table>(<columns>)

**Examples**:

.. code-block:: text

    make index idx_email on users(email)
    make unique index idx_username on users(username)
    make index idx_composite on orders(customer_id, created_at)

DROP TABLE
^^^^^^^^^^

Removes a table.

**Syntax**:

.. code-block:: text

    drop table <name>

DROP INDEX
^^^^^^^^^^

Removes an index.

**Syntax**:

.. code-block:: text

    drop index <name> on <table>

Transaction Statements
----------------------

BEGIN
^^^^^

Starts a new transaction:

.. code-block:: text

    begin

COMMIT
^^^^^^

Commits the current transaction:

.. code-block:: text

    commit

ROLLBACK
^^^^^^^^

Aborts the current transaction:

.. code-block:: text

    rollback

Namespace Statements
--------------------

USE
^^^

Switches to a different namespace (database). All subsequent queries will
operate within this namespace until changed.

**Syntax**:

.. code-block:: text

    use <namespace>

**Examples**:

.. code-block:: text

    // Switch to a namespace
    use production

    // Use the default namespace
    use default

    // Switch to user-specific namespace
    use tenant_12345

**Notes**:

- Namespaces provide logical isolation between different databases or tenants
- Tables are qualified with namespace: ``namespace.table``
- If a table name contains a dot, it's treated as fully qualified
- The default namespace is ``default``

Expressions
-----------

Comparison Operators
^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Operator
     - Description
     - Example
   * - ``=``
     - Equal
     - ``age = 21``
   * - ``!=``
     - Not equal
     - ``status != "deleted"``
   * - ``<``
     - Less than
     - ``price < 100``
   * - ``>``
     - Greater than
     - ``age > 18``
   * - ``<=``
     - Less than or equal
     - ``quantity <= 10``
   * - ``>=``
     - Greater than or equal
     - ``score >= 80``

Logical Operators
^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Operator
     - Description
     - Example
   * - ``and``
     - Logical AND
     - ``age > 18 and active = true``
   * - ``or``
     - Logical OR
     - ``role = "admin" or role = "mod"``
   * - ``not``
     - Logical NOT
     - ``not deleted``

Arithmetic Operators
^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Operator
     - Description
     - Example
   * - ``+``
     - Addition
     - ``price + tax``
   * - ``-``
     - Subtraction
     - ``total - discount``
   * - ``*``
     - Multiplication
     - ``quantity * unit_price``
   * - ``/``
     - Division
     - ``total / count``

Special Operators
^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Operator
     - Description
     - Example
   * - ``in``
     - Value in list
     - ``status in ["active", "pending"]``
   * - ``has``
     - Collection contains value
     - ``tags has "featured"``
   * - ``like``
     - Pattern matching (SQL-style)
     - ``name like "John%"``

Unary Operators
^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Operator
     - Description
     - Example
   * - ``-``
     - Negation
     - ``-amount``
   * - ``not``
     - Logical NOT
     - ``not active``
   * - ``is null``
     - Null check
     - ``email is null``
   * - ``is not null``
     - Non-null check
     - ``email is not null``

Operator Precedence
^^^^^^^^^^^^^^^^^^^

From highest to lowest:

1. ``*``, ``/`` (multiplication, division)
2. ``+``, ``-`` (addition, subtraction)
3. ``<``, ``>``, ``<=``, ``>=``, ``has``, ``in``, ``like`` (comparison)
4. ``=``, ``!=`` (equality)
5. ``and`` (logical AND)
6. ``or`` (logical OR)

Parentheses can override precedence:

.. code-block:: text

    (age > 18 or parent_consent = true) and active = true

Field Access
^^^^^^^^^^^^

Access nested fields using dot notation:

.. code-block:: text

    get from users where address.city = "London"
    get from orders where items.0.name = "Widget"

Keywords
--------

The following words are reserved keywords:

**Query Keywords**: ``get``, ``count``, ``describe``, ``from``

**Mutation Keywords**: ``put``, ``into``, ``change``, ``remove``

**Clause Keywords**: ``where``, ``order``, ``by``, ``take``, ``skip``,
``asc``, ``desc``

**DDL Keywords**: ``make``, ``drop``, ``table``, ``index``, ``on``,
``unique``, ``fields``, ``as``, ``relational``, ``document``, ``keyspace``

**Type Keywords**: ``int``, ``bigint``, ``text``, ``decimal``, ``double``,
``date``, ``boolean``, ``map``, ``list``

**Constraint Keywords**: ``primary``, ``key``, ``required``, ``default``

**Transaction Keywords**: ``begin``, ``commit``, ``rollback``

**Namespace Keywords**: ``use``, ``namespace``, ``namespaces``

**Logical Keywords**: ``and``, ``or``, ``not``, ``in``, ``like``, ``has``

**Literal Keywords**: ``true``, ``false``, ``null``, ``now``

**Other Keywords**: ``ttl``, ``with``

Example Queries
---------------

Basic CRUD Operations
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    // Create
    put into users name = "Alice", email = "alice@example.com"

    // Read
    get from users where id = 1
    get name, email from users where active = true

    // Update
    change users where id = 1 name = "Alicia"

    // Delete
    remove from users where id = 1

Filtering and Sorting
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    // Multiple conditions
    get from products where price < 100 and in_stock = true

    // OR conditions
    get from users where role = "admin" or role = "moderator"

    // Sorting with limit
    get from orders order by created_at desc take 10

    // Complex filter
    get from users where (age >= 18 and country = "UK") or verified = true

Parameterised Queries
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    // Positional parameters
    get from users where id = $1
    get from orders where customer_id = $1 and status = $2

    // Named parameters
    get from users where email = :email
    change users where id = :id name = :new_name

Transactions
^^^^^^^^^^^^

.. code-block:: text

    begin
    put into orders customer_id = 1, total = 99.99
    change inventory where product_id = 42 quantity = quantity - 1
    commit

Indentation Syntax
------------------

MQL supports Python-style significant whitespace for readability.
Indented blocks are used for multi-line statements:

.. code-block:: text

    put into users
      name = "Alice"
      email = "alice@example.com"
      profile = {
        age = 30
        city = "London"
      }

    get from orders
      where
        status = "pending"
        created_at > $1
      order by priority desc
      take 100

Within parentheses, brackets, or braces, indentation is ignored:

.. code-block:: text

    get from users where status in [
      "active",
      "pending",
      "review"
    ]

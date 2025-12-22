.. _permissions:

Permissions
===========

MonoDB implements a granular permission system inspired by MongoDB's Role-Based
Access Control (RBAC) and PostgreSQL's GRANT/REVOKE model. This hybrid approach
provides flexibility and fine-grained control over user permissions.

The permission system is built around four core concepts:

- **Actions**: Operations that can be performed (select, insert, update, etc.)
- **Resources**: Objects that actions can be performed on (cluster, namespaces, collections)
- **Permissions**: Combinations of actions and resources that define what is allowed
- **Roles**: Collections of permissions that can be assigned to users

Architecture Overview
---------------------

The permission system is implemented in the ``monodb-common`` crate at
``crates/monodb-common/src/permissions.rs``. This allows both the server and
client libraries to share the same permission primitives, ensuring consistency
across the system.

.. mermaid::

   graph TB
       User[User] --> |has| Roles[Assigned Roles]
       User --> |has| DirectPerms[Direct Permissions]
       Roles --> |contains| Role1[Role: dbAdmin]
       Roles --> |contains| Role2[Role: read]
       Role1 --> |inherits| Role3[Role: base]
       Role1 --> |grants| Perms1[Permissions]
       Role2 --> |grants| Perms2[Permissions]
       Role3 --> |grants| Perms3[Permissions]
       DirectPerms --> Merge[Merge All Permissions]
       Perms1 --> Merge
       Perms2 --> Merge
       Perms3 --> Merge
       Merge --> EffectivePerms[Effective PermissionSet]
       EffectivePerms --> |authorize| Request[Client Request]

This diagram illustrates how a user's effective permissions are computed by
combining direct permissions with all permissions from assigned roles and
their inherited roles.

Actions
-------

Actions represent operations that can be performed on resources. They are
encoded as single bytes in the wire protocol for efficiency.

Data Operations
^^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Action
     - Code
     - Description
   * - Select
     - 0x01
     - Read/query data (``get``)
   * - Insert
     - 0x02
     - Insert new data (``put``)
   * - Update
     - 0x03
     - Update existing data (``change``)
   * - Delete
     - 0x04
     - Delete data (``remove``)

Schema Operations
^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Action
     - Code
     - Description
   * - Create
     - 0x10
     - Create new objects (collections, indexes, etc.)
   * - Drop
     - 0x11
     - Drop/delete objects
   * - Alter
     - 0x12
     - Alter existing objects (rename, modify schema, etc.)
   * - Index
     - 0x13
     - Create or manage indexes

Administrative Operations
^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Action
     - Code
     - Description
   * - Grant
     - 0x20
     - Grant permissions to users/roles
   * - Revoke
     - 0x21
     - Revoke permissions from users/roles
   * - ManageUsers
     - 0x22
     - Create, modify, delete users
   * - ManageRoles
     - 0x23
     - Create, modify, delete roles

System Operations
^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Action
     - Code
     - Description
   * - Stats
     - 0x30
     - View server/namespace statistics
   * - Describe
     - 0x31
     - Describe schema of objects
   * - List
     - 0x32
     - List objects (collections, namespaces)
   * - Connect
     - 0x33
     - Connect to namespace/server
   * - Shutdown
     - 0x34
     - Shutdown or manage server lifecycle

Transaction Operations
^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Action
     - Code
     - Description
   * - BeginTransaction
     - 0x40
     - Begin a transaction

Special Actions
^^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Action
     - Code
     - Description
   * - All
     - 0xFF
     - All actions (wildcard) - use with extreme caution

Resources
---------

Resources define the scope of permissions. MonoDB supports a hierarchical
resource model with wildcards for flexible permission grants.

Resource Types
^^^^^^^^^^^^^^

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Resource
     - Description
   * - ``Cluster``
     - Cluster-wide resource (system operations)
   * - ``Namespace { name }``
     - A specific namespace (database/schema)
   * - ``AllNamespaces``
     - All namespaces (wildcard)
   * - ``Collection { namespace, name }``
     - A specific collection within a namespace
   * - ``AllCollections { namespace }``
     - All collections within a namespace
   * - ``AllCollectionsGlobal``
     - All collections in all namespaces

String Notation
^^^^^^^^^^^^^^^

Resources use a colon-separated string notation for human-readable representation:

.. code-block:: text

   cluster                    # Cluster resource
   ns:mydb                    # Specific namespace
   ns:*                       # All namespaces
   ns:mydb:col:users          # Specific collection
   ns:mydb:col:*              # All collections in namespace
   ns:*:col:*                 # All collections globally

Hierarchical Matching
^^^^^^^^^^^^^^^^^^^^^

The permission system uses hierarchical matching when checking if a permission
allows an action on a resource. A resource matches if:

- They are exactly equal, OR
- The permission's resource is a wildcard that encompasses the target resource

For example:

- ``Cluster`` matches everything
- ``AllNamespaces`` matches any namespace or collection
- ``Namespace { name: "mydb" }`` matches all collections in "mydb"
- ``AllCollections { namespace: "mydb" }`` matches any collection in "mydb"
- ``AllCollectionsGlobal`` matches any collection in any namespace

This matching logic is implemented in ``Resource::matches()`` at
``permissions.rs:277``.

.. mermaid::

   graph TD
       Cluster[Cluster<br/>cluster] --> |matches| Everything[Everything]

       AllNS[AllNamespaces<br/>ns:*] --> |matches| AnyNS[Any Namespace]
       AllNS --> |matches| AnyCol1[Any Collection]

       NS[Namespace<br/>ns:mydb] --> |matches| SameNS[Namespace: mydb]
       NS --> |matches| ColInNS[Collections in mydb]

       AllCol[AllCollections<br/>ns:mydb:col:*] --> |matches| AnyColInNS[Any collection in mydb]

       AllColGlobal[AllCollectionsGlobal<br/>ns:*:col:*] --> |matches| AnyColAnywhere[Any collection anywhere]

       Collection[Collection<br/>ns:mydb:col:users] --> |matches| ExactCol[Exactly ns:mydb:col:users]

       style Cluster fill:#ff6b6b
       style AllNS fill:#ffd93d
       style NS fill:#6bcf7f
       style AllCol fill:#4d96ff
       style AllColGlobal fill:#c77dff
       style Collection fill:#95e1d3

This diagram shows the hierarchical resource matching. Higher-level resources
(colored in red/yellow) match broader scopes, while specific resources (green/blue)
match narrower scopes.

Permissions
-----------

A permission combines a resource and an action to define what is allowed.

String Format
^^^^^^^^^^^^^

Permissions use the format: ``<resource>:<action>``

Examples:

.. code-block:: text

   cluster:*                   # All actions on cluster
   ns:mydb:select              # Select on namespace mydb
   ns:mydb:col:users:insert    # Insert on users collection
   ns:*:col:*:select           # Select on all collections

Permission Checking
^^^^^^^^^^^^^^^^^^^

When checking if a permission allows an action on a resource, the system checks:

1. Does the action match? (exact match or ``Action::All``)
2. Does the resource match? (using hierarchical matching)

Both conditions must be true for the permission to allow the operation.

.. mermaid::

   sequenceDiagram
       participant Client
       participant Server
       participant AuthZ as Authorization
       participant PermSet as PermissionSet

       Client->>Server: Request: UPDATE users in mydb
       Server->>AuthZ: Check permission
       AuthZ->>AuthZ: Extract resource & action<br/>Resource: ns:mydb:col:users<br/>Action: Update
       AuthZ->>PermSet: allows(resource, action)?

       loop For each permission in set
           PermSet->>PermSet: Check action match<br/>(Update == Update or *)
           alt Action matches
               PermSet->>PermSet: Check resource match<br/>(hierarchical)
               alt Resource matches
                   PermSet-->>AuthZ: ALLOWED
               end
           end
       end

       alt No permission allows
           PermSet-->>AuthZ: DENIED
           AuthZ-->>Server: Permission denied
           Server-->>Client: Error: Insufficient permissions
       else Permission allows
           AuthZ-->>Server: Authorized
           Server->>Server: Execute request
           Server-->>Client: Success
       end

This sequence diagram shows the permission checking flow when a client makes
a request that requires authorization.

.. code-block:: rust

   let perm = Permission::new(
       Resource::AllCollections {
           namespace: "mydb".to_string(),
       },
       Action::Select,
   );

   // Allowed: SELECT on users collection in mydb
   assert!(perm.allows(
       &Resource::Collection {
           namespace: "mydb".to_string(),
           name: "users".to_string()
       },
       Action::Select
   ));

   // Not allowed: INSERT (wrong action)
   assert!(!perm.allows(
       &Resource::Collection {
           namespace: "mydb".to_string(),
           name: "users".to_string()
       },
       Action::Insert
   ));

   // Not allowed: Different namespace
   assert!(!perm.allows(
       &Resource::Collection {
           namespace: "other".to_string(),
           name: "users".to_string()
       },
       Action::Select
   ));

Permission Sets
---------------

A ``PermissionSet`` is a collection of permissions assigned to a user or role.
When checking if a permission set allows an action, the system checks if **any**
permission in the set allows it.

.. code-block:: rust

   let mut set = PermissionSet::new();

   // Add read permission on mydb
   set.add(Permission::new(
       Resource::AllCollections {
           namespace: "mydb".to_string(),
       },
       Action::Select,
   ));

   // Add write permission on users collection
   set.add(Permission::new(
       Resource::Collection {
           namespace: "mydb".to_string(),
           name: "users".to_string(),
       },
       Action::Insert,
   ));

   // Check permissions
   assert!(set.allows(
       &Resource::Collection {
           namespace: "mydb".to_string(),
           name: "posts".to_string()
       },
       Action::Select  // Allowed by first permission
   ));

   assert!(set.allows(
       &Resource::Collection {
           namespace: "mydb".to_string(),
           name: "users".to_string()
       },
       Action::Insert  // Allowed by second permission
   ));

Permission sets can be merged together, which is useful when combining
permissions from multiple roles:

.. code-block:: rust

   let mut user_perms = PermissionSet::new();
   user_perms.merge(&role1.permissions);
   user_perms.merge(&role2.permissions);

Built-in Roles
--------------

MonoDB provides several built-in roles with predefined permission sets. These
roles follow MongoDB's naming conventions and cover common use cases.

Namespace-Scoped Roles
^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Role
     - Permissions
   * - ``read``
     - Read-only access to all collections in a namespace (select, list, describe)
   * - ``readWrite``
     - Read and write access to all collections (select, insert, update, delete, list, describe)
   * - ``dbAdmin``
     - Schema management for a namespace (create, drop, alter, index, stats, list, describe)
   * - ``userAdmin``
     - User and role management for a namespace (manage_users, manage_roles, grant, revoke)
   * - ``dbOwner``
     - Full administrative access to a namespace (combines readWrite, dbAdmin, and userAdmin)

Cluster-Scoped Roles
^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Role
     - Permissions
   * - ``clusterMonitor``
     - Cluster-wide read access (stats, list, describe on all namespaces)
   * - ``clusterAdmin``
     - Cluster-wide administrative access (all actions on cluster)
   * - ``root``
     - Full access to everything (superuser)

Using Built-in Roles
^^^^^^^^^^^^^^^^^^^^

Built-in roles can be scoped to a specific namespace or applied globally:

.. code-block:: rust

   // Read role for specific namespace
   let perms = BuiltinRole::Read.permissions(Some("mydb"));

   // Read role for all namespaces
   let perms = BuiltinRole::Read.permissions(None);

   // Root role (always global)
   let perms = BuiltinRole::Root.permissions(None);

Custom Roles
------------

In addition to built-in roles, administrators can create custom roles with
specific permission sets. Custom roles can inherit from other roles, enabling
flexible permission hierarchies.

Role Structure
^^^^^^^^^^^^^^

A custom role consists of:

- **name**: Unique identifier for the role (within its namespace)
- **namespace**: Optional namespace scope (None = cluster-wide)
- **permissions**: Direct permissions granted to this role
- **inherits**: List of other role names this role inherits from

.. code-block:: rust

   let mut role = Role::new("developer", Some("mydb".to_string()));

   // Add direct permissions
   role.add_permission(Permission::new(
       Resource::AllCollections {
           namespace: "mydb".to_string(),
       },
       Action::Select,
   ));

   role.add_permission(Permission::new(
       Resource::Collection {
           namespace: "mydb".to_string(),
           name: "test_data".to_string(),
       },
       Action::All,
   ));

   // Inherit from another role
   role.add_inherit("read");

Role Inheritance
^^^^^^^^^^^^^^^^

When a role inherits from another role, it gains all permissions from the
parent role. This allows creating role hierarchies and composing permissions
from multiple sources.

The inheritance chain is resolved by the server when computing a user's
effective permissions. If role A inherits from role B, which inherits from
role C, the user will receive permissions from all three roles.

Wire Protocol Encoding
----------------------

Permissions are encoded efficiently in the binary wire protocol. The encoding
format is designed to minimize bandwidth while maintaining flexibility.

Action Encoding
^^^^^^^^^^^^^^^

Actions are encoded as single bytes using their defined codes (e.g., Select = 0x01).

Resource Encoding
^^^^^^^^^^^^^^^^^

Resources are encoded with a tag byte followed by their data:

.. code-block:: text

   Cluster:              [0x01]
   Namespace:            [0x02] [name: string]
   AllNamespaces:        [0x03]
   Collection:           [0x04] [namespace: string] [name: string]
   AllCollections:       [0x05] [namespace: string]
   AllCollectionsGlobal: [0x06]

Permission Encoding
^^^^^^^^^^^^^^^^^^^

A permission is encoded as: ``[resource] [action_byte]``

Permission Set Encoding
^^^^^^^^^^^^^^^^^^^^^^^

A permission set is encoded as:

.. code-block:: text

   [count: u32 LE] [permission_1] [permission_2] ... [permission_n]

This encoding is used in the AuthSuccess response (see :ref:`protocol`) to
transmit a user's permissions after successful authentication.

Role Encoding
^^^^^^^^^^^^^

Custom roles are encoded as:

.. code-block:: text

   [name: string]
   [namespace: opt_string]
   [permissions: PermissionSet]
   [inherit_count: u32 LE]
   [inherit_name_1: string] ... [inherit_name_n: string]

Integration with Authentication
--------------------------------

The permission system integrates tightly with the authentication system
(see :ref:`authentication`). When a user authenticates successfully, the
server computes their effective permissions by:

1. Loading the user's directly assigned permissions
2. Loading all roles assigned to the user
3. Recursively resolving role inheritance
4. Merging all permissions into a single ``PermissionSet``
5. Transmitting the permission set in the AuthSuccess response

The client stores this permission set for the duration of the session. The
server also stores it in the session object and uses it to authorize all
subsequent requests.

.. mermaid::

   sequenceDiagram
       participant C as Client
       participant S as Server
       participant Auth as Auth Handler
       participant DB as User Store
       participant RoleResolver as Role Resolver

       C->>S: Authenticate(username, password)
       S->>Auth: Validate credentials
       Auth->>DB: Load user
       DB-->>Auth: User{id, roles: [dbAdmin, read]}

       Auth->>RoleResolver: Resolve permissions
       RoleResolver->>DB: Load role: dbAdmin
       DB-->>RoleResolver: Role{permissions, inherits: [base]}
       RoleResolver->>DB: Load role: base
       DB-->>RoleResolver: Role{permissions, inherits: []}
       RoleResolver->>DB: Load role: read
       DB-->>RoleResolver: Role{permissions, inherits: []}

       RoleResolver->>RoleResolver: Merge all permissions<br/>+ Direct user permissions
       RoleResolver-->>Auth: PermissionSet (effective)

       Auth->>Auth: Encode PermissionSet<br/>to binary format
       Auth-->>S: AuthSuccess response
       S-->>C: AuthSuccess{<br/>  session_id,<br/>  user_id,<br/>  permissions: [binary],<br/>  expires_at<br/>}

       C->>C: Decode & store<br/>PermissionSet

       Note over C,S: All subsequent requests use<br/>this PermissionSet for authorization

This diagram shows how permissions are computed during authentication and
transmitted to the client in binary format.

.. code-block:: text

   AuthSuccess Response:
   [session_id: u64]
   [user_id: string]
   [permissions: PermissionSet]  ← Encoded using PermissionSet binary format
   [expires_at: opt_u64]

The permissions are transmitted using the efficient binary encoding described
above, which is more compact than string arrays and faster to parse.

Future Work
-----------

Several enhancements to the permission system are under consideration:

**Column-Level Permissions**
   Extend resources to support column-level granularity for table-like
   collections, allowing permissions like "select specific columns only."

**Row-Level Security**
   Implement predicate-based permissions that filter which rows a user can
   access based on row content.

**Permission Caching**
   Add caching for permission checks to reduce overhead on high-frequency
   operations.

**Audit Logging**
   Track permission grants, revocations, and failed authorization attempts
   for security auditing.

**Time-Based Permissions**
   Support temporal permissions that are only valid during specific time
   windows.

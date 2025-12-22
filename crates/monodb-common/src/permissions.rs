//! Permissions and authorization primitives for MonoDB.

// Actions

use std::fmt;

use bytes::{BufMut, BytesMut};

use crate::{
    MonoError, Result,
    protocol::{get_opt_string, get_string, get_u8, get_u32_le, put_opt_string, put_string},
};

/// Actions that can be performed on resources.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    // Data operations (0x01 - 0x0F)
    /// Read/query data (`get`)
    Select = 0x01,
    /// Insert new data (`put`)
    Insert = 0x02,
    /// Update existing data (`change`)
    Update = 0x03,
    /// Delete data (`remove`)
    Delete = 0x04,

    // Schema operations (0x10 - 0x1F)
    /// Create new objects (collections, indexes, etc.)
    Create = 0x10,
    /// Drop/delete objects
    Drop = 0x11,
    /// Alter existing objects (rename, modify schema, etc.)
    Alter = 0x12,
    /// Create or manage indexes
    Index = 0x13,

    // Administrative operations (0x20 - 0x2F)
    /// Grant permissions to users/roles
    Grant = 0x20,
    /// Revoke permissions from users/roles
    Revoke = 0x21,
    /// Create, modify, delete users
    ManageUsers = 0x22,
    /// Create, modify, delete roles
    ManageRoles = 0x23,

    // System operations (0x30 - 0x3F)
    /// View server/namespace statistics
    Stats = 0x30,
    /// Describe schema of objects
    Describe = 0x31,
    /// List objects (collections, namespaces)
    List = 0x32,
    /// Connect to namespace/server
    Connect = 0x33,
    /// Shutdown or manage server lifecycle
    Shutdown = 0x34,

    // Transaction operations (0x40 - 0x4F)
    /// Begin a transaction
    BeginTransaction = 0x40,

    // Wildcard (0xFF)
    /// All actions, use with extreme caution
    All = 0xFF,
}

impl Action {
    /// Parse action from bytes
    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            0x01 => Ok(Action::Select),
            0x02 => Ok(Action::Insert),
            0x03 => Ok(Action::Update),
            0x04 => Ok(Action::Delete),
            0x10 => Ok(Action::Create),
            0x11 => Ok(Action::Drop),
            0x12 => Ok(Action::Alter),
            0x13 => Ok(Action::Index),
            0x20 => Ok(Action::Grant),
            0x21 => Ok(Action::Revoke),
            0x22 => Ok(Action::ManageUsers),
            0x23 => Ok(Action::ManageRoles),
            0x30 => Ok(Action::Stats),
            0x31 => Ok(Action::Describe),
            0x32 => Ok(Action::List),
            0x33 => Ok(Action::Connect),
            0x34 => Ok(Action::Shutdown),
            0x40 => Ok(Action::BeginTransaction),
            0xFF => Ok(Action::All),
            _ => Err(MonoError::Parse(format!("Unknown action byte: {b:#04x}"))),
        }
    }

    /// Convert to wire format byte
    pub fn to_byte(self) -> u8 {
        self as u8
    }

    /// Parse action from string
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "select" | "read" | "find" => Ok(Action::Select),
            "insert" | "write" => Ok(Action::Insert),
            "update" => Ok(Action::Update),
            "delete" | "remove" => Ok(Action::Delete),
            "create" => Ok(Action::Create),
            "drop" => Ok(Action::Drop),
            "alter" | "modify" => Ok(Action::Alter),
            "index" => Ok(Action::Index),
            "grant" => Ok(Action::Grant),
            "revoke" => Ok(Action::Revoke),
            "manage_users" | "manageusers" => Ok(Action::ManageUsers),
            "manage_roles" | "manageroles" => Ok(Action::ManageRoles),
            "stats" | "statistics" => Ok(Action::Stats),
            "describe" | "schema" => Ok(Action::Describe),
            "list" => Ok(Action::List),
            "connect" => Ok(Action::Connect),
            "shutdown" => Ok(Action::Shutdown),
            "begin_tx" | "begintransaction" => Ok(Action::BeginTransaction),
            "*" | "all" => Ok(Action::All),
            _ => Err(MonoError::Parse(format!("Unknown action: {s}"))),
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::Select => write!(f, "select"),
            Action::Insert => write!(f, "insert"),
            Action::Update => write!(f, "update"),
            Action::Delete => write!(f, "delete"),
            Action::Create => write!(f, "create"),
            Action::Drop => write!(f, "drop"),
            Action::Alter => write!(f, "alter"),
            Action::Index => write!(f, "index"),
            Action::Grant => write!(f, "grant"),
            Action::Revoke => write!(f, "revoke"),
            Action::ManageUsers => write!(f, "manage_users"),
            Action::ManageRoles => write!(f, "manage_roles"),
            Action::Stats => write!(f, "stats"),
            Action::Describe => write!(f, "describe"),
            Action::List => write!(f, "list"),
            Action::Connect => write!(f, "connect"),
            Action::Shutdown => write!(f, "shutdown"),
            Action::BeginTransaction => write!(f, "begin_tx"),
            Action::All => write!(f, "*"),
        }
    }
}

// Resources

/// Resource types that can have permissions attached.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resource {
    /// Cluster-wide resource (system operations)
    Cluster,

    /// A specific namespace (database/schema)
    Namespace { name: String },

    /// All namespaces (wildcard)
    ///
    /// Think carefully about whether you can use [`Resource::Namespace`]
    /// or more specific resources instead, as this grants permissions
    /// across all namespaces.
    AllNamespaces,

    /// A specific collection within a namespace
    Collection { namespace: String, name: String },

    /// All collections within a namespace
    ///
    /// Think carefully about whether you can use [`Resource::Collection`]
    /// instead, as this grants permissions across all collections
    /// in the specified namespace.
    AllCollections { namespace: String },

    /// All collections in all namespaces
    ///
    /// This is a very broad permission and should be used with caution.
    AllCollectionsGlobal,
}

/// Tags for Resource
///
/// Used in the wire protocol. See [`crate::protocol`].
mod resource_tag {
    pub const CLUSTER: u8 = 0x01;
    pub const NAMESPACE: u8 = 0x02;
    pub const ALL_NAMESPACES: u8 = 0x03;
    pub const COLLECTION: u8 = 0x04;
    pub const ALL_COLLECTIONS: u8 = 0x05;
    pub const ALL_COLLECTIONS_GLOBAL: u8 = 0x06;
}

impl Resource {
    /// Encode resource to bytes
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            Resource::Cluster => {
                buf.put_u8(resource_tag::CLUSTER);
            }
            Resource::Namespace { name } => {
                buf.put_u8(resource_tag::NAMESPACE);
                put_string(buf, name);
            }
            Resource::AllNamespaces => {
                buf.put_u8(resource_tag::ALL_NAMESPACES);
            }
            Resource::Collection { namespace, name } => {
                buf.put_u8(resource_tag::COLLECTION);
                put_string(buf, namespace);
                put_string(buf, name);
            }
            Resource::AllCollections { namespace } => {
                buf.put_u8(resource_tag::ALL_COLLECTIONS);
                put_string(buf, namespace);
            }
            Resource::AllCollectionsGlobal => {
                buf.put_u8(resource_tag::ALL_COLLECTIONS_GLOBAL);
            }
        }
    }

    /// Decode resource from bytes
    pub fn decode(cursor: &mut &[u8]) -> Result<Self> {
        let tag = get_u8(cursor)?;
        match tag {
            resource_tag::CLUSTER => Ok(Resource::Cluster),
            resource_tag::NAMESPACE => {
                let name = get_string(cursor)?;
                Ok(Resource::Namespace { name })
            }
            resource_tag::ALL_NAMESPACES => Ok(Resource::AllNamespaces),
            resource_tag::COLLECTION => {
                let namespace = get_string(cursor)?;
                let name = get_string(cursor)?;
                Ok(Resource::Collection { namespace, name })
            }
            resource_tag::ALL_COLLECTIONS => {
                let namespace = get_string(cursor)?;
                Ok(Resource::AllCollections { namespace })
            }
            resource_tag::ALL_COLLECTIONS_GLOBAL => Ok(Resource::AllCollectionsGlobal),
            _ => Err(MonoError::Parse(format!(
                "Unknown resource tag: {tag:#04x}"
            ))),
        }
    }

    /// Parse resource from permission string notation.
    pub fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split(':').collect();

        match parts.as_slice() {
            ["cluster"] => Ok(Resource::Cluster),
            ["ns", "*"] => Ok(Resource::AllNamespaces),
            ["ns", name] => Ok(Resource::Namespace {
                name: (*name).to_string(),
            }),
            ["ns", "*", "col", "*"] => Ok(Resource::AllCollectionsGlobal),
            ["ns", ns, "col", "*"] => Ok(Resource::AllCollections {
                namespace: (*ns).to_string(),
            }),
            ["ns", ns, "col", name] => Ok(Resource::Collection {
                namespace: (*ns).to_string(),
                name: (*name).to_string(),
            }),
            _ => Err(MonoError::Parse(format!("Invalid resource format: {s}"))),
        }
    }

    /// Check if this resource matches another (for permission checking)
    ///
    /// A resource matches if:
    /// - They are exactly equal, OR
    /// - Self is a wildcard that encompasses the other resource.
    pub fn matches(&self, other: &Resource) -> bool {
        match (self, other) {
            // Exact matches
            (a, b) if a == b => true,

            // Cluster matches everything
            (Resource::Cluster, _) => true,

            // AllNamespaces matches any namespace or collection
            (Resource::AllNamespaces, Resource::Namespace { .. }) => true,
            (Resource::AllNamespaces, Resource::Collection { .. }) => true,
            (Resource::AllNamespaces, Resource::AllCollections { .. }) => true,

            // Namespace matches its collections
            (Resource::Namespace { name: ns1 }, Resource::Collection { namespace: ns2, .. })
                if ns1 == ns2 =>
            {
                true
            }
            (Resource::Namespace { name: ns1 }, Resource::AllCollections { namespace: ns2 })
                if ns1 == ns2 =>
            {
                true
            }

            // AllCollectionsGlobal matches any collection
            (Resource::AllCollectionsGlobal, Resource::Collection { .. }) => true,
            (Resource::AllCollectionsGlobal, Resource::AllCollections { .. }) => true,

            // AllCollections matches collections in its namespace
            (
                Resource::AllCollections { namespace: ns1 },
                Resource::Collection { namespace: ns2, .. },
            ) => ns1 == ns2,

            // No match
            _ => false,
        }
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Resource::Cluster => write!(f, "cluster"),
            Resource::Namespace { name } => write!(f, "ns:{name}"),
            Resource::AllNamespaces => write!(f, "ns:*"),
            Resource::Collection { namespace, name } => write!(f, "ns:{namespace}:col:{name}"),
            Resource::AllCollections { namespace } => write!(f, "ns:{namespace}:col:*"),
            Resource::AllCollectionsGlobal => write!(f, "ns:*:col:*"),
        }
    }
}

// Permission

/// A single permission grant
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Permission {
    pub resource: Resource,
    pub action: Action,
}

impl Permission {
    /// Create a new permission
    pub fn new(resource: Resource, action: Action) -> Self {
        Self { resource, action }
    }

    /// Encode permission to bytes
    pub fn encode(&self, buf: &mut BytesMut) {
        self.resource.encode(buf);
        buf.put_u8(self.action.to_byte());
    }

    /// Decode permission from bytes
    pub fn decode(cursor: &mut &[u8]) -> Result<Self> {
        let resource = Resource::decode(cursor)?;
        let action_byte = get_u8(cursor)?;
        let action = Action::from_byte(action_byte)?;
        Ok(Self { resource, action })
    }

    /// Parse permission from string notation.
    ///
    /// Format: `<resource>:<action>`
    ///
    /// Examples:
    /// - `cluster:*`
    /// - `ns:mydb:select`
    /// - `ns:mydb:col:users:insert`
    pub fn from_str(s: &str) -> Result<Self> {
        // Find the last colon to split resource from action
        let last_colon = s.rfind(':').ok_or_else(|| {
            MonoError::Parse(format!("Invalid permission format (missing action): {s}"))
        })?;

        let resource_str = &s[..last_colon];
        let action_str = &s[last_colon + 1..];

        let resource = Resource::from_str(resource_str)?;
        let action = Action::from_str(action_str)?;

        Ok(Self { resource, action })
    }

    /// Check if this permission allows the given action on the given resource
    pub fn allows(&self, resource: &Resource, action: Action) -> bool {
        // Check if action matches
        let action_matches = self.action == Action::All || self.action == action;

        // Check if resource matches
        let resource_matches = self.resource.matches(resource);

        action_matches && resource_matches
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.resource, self.action)
    }
}

// Permission Set

/// A collection of permissions assigned to a user or role.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionSet {
    permissions: Vec<Permission>,
}

impl PermissionSet {
    /// Create an empty permission set
    pub fn new() -> Self {
        Self {
            permissions: Vec::new(),
        }
    }

    /// Create a permission set with given permissions
    pub fn with_permissions(permissions: Vec<Permission>) -> Self {
        Self { permissions }
    }

    /// Add a permission to the set
    pub fn add(&mut self, permission: Permission) {
        if !self.permissions.contains(&permission) {
            self.permissions.push(permission);
        }
    }

    /// Remove a permission from the set
    pub fn remove(&mut self, permission: &Permission) {
        self.permissions.retain(|p| p != permission);
    }

    /// Check if any permission in the set allows the given actions on the resource
    pub fn allows(&self, resource: &Resource, action: Action) -> bool {
        self.permissions.iter().any(|p| p.allows(resource, action))
    }

    /// Get all permissions as a slice
    pub fn permissions(&self) -> &[Permission] {
        &self.permissions
    }

    /// Encode permission set to bytes
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.permissions.len() as u32);
        for perm in &self.permissions {
            perm.encode(buf);
        }
    }

    /// Decode permission set from bytes
    pub fn decode(cursor: &mut &[u8]) -> Result<Self> {
        let count = get_u32_le(cursor)? as usize;
        let mut permissions = Vec::with_capacity(count);
        for _ in 0..count {
            permissions.push(Permission::decode(cursor)?);
        }

        Ok(Self { permissions })
    }

    /// Convert to string array for protocol backward compatibility
    pub fn to_string_array(&self) -> Vec<String> {
        self.permissions.iter().map(|p| p.to_string()).collect()
    }

    /// Parse from string array
    pub fn from_string_array(arr: &[String]) -> Result<Self> {
        let mut permissions = Vec::with_capacity(arr.len());
        for s in arr {
            permissions.push(Permission::from_str(s)?);
        }
        Ok(Self { permissions })
    }

    /// Merge another permission set into this one
    pub fn merge(&mut self, other: &PermissionSet) {
        for perm in &other.permissions {
            self.add(perm.clone());
        }
    }
}

// Built-in Roles

/// Built-in role definitions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BuiltinRole {
    /// Read-only access to all collections in a namespace
    Read,
    /// Read and write access to all collections in a namespace
    ReadWrite,
    /// Schema management for a namespace
    DbAdmin,
    /// User and role management for a namespace
    UserAdmin,
    /// Full administrative access to a namespace
    DbOwner,
    /// Cluster-wide read access
    ClusterMonitor,
    /// Cluster-wide administrative access
    ClusterAdmin,
    /// Full access to everything (superuser)
    Root,
}

impl BuiltinRole {
    /// Get the permissions for this role, scoped to the given namespace.
    /// For cluster-level roles, namespace is ignored.
    pub fn permissions(&self, namespace: Option<&str>) -> PermissionSet {
        let mut set = PermissionSet::new();

        match self {
            BuiltinRole::Read => {
                let resource = match namespace {
                    Some(ns) => Resource::AllCollections {
                        namespace: ns.to_string(),
                    },
                    None => Resource::AllCollectionsGlobal,
                };
                set.add(Permission::new(resource.clone(), Action::Select));
                set.add(Permission::new(resource.clone(), Action::List));
                set.add(Permission::new(resource, Action::Describe));
            }
            BuiltinRole::ReadWrite => {
                let resource = match namespace {
                    Some(ns) => Resource::AllCollections {
                        namespace: ns.to_string(),
                    },
                    None => Resource::AllCollectionsGlobal,
                };
                set.add(Permission::new(resource.clone(), Action::Select));
                set.add(Permission::new(resource.clone(), Action::Insert));
                set.add(Permission::new(resource.clone(), Action::Update));
                set.add(Permission::new(resource.clone(), Action::Delete));
                set.add(Permission::new(resource.clone(), Action::List));
                set.add(Permission::new(resource, Action::Describe));
            }
            BuiltinRole::DbAdmin => {
                let resource = match namespace {
                    Some(ns) => Resource::Namespace {
                        name: ns.to_string(),
                    },
                    None => Resource::AllNamespaces,
                };
                set.add(Permission::new(resource.clone(), Action::Create));
                set.add(Permission::new(resource.clone(), Action::Drop));
                set.add(Permission::new(resource.clone(), Action::Alter));
                set.add(Permission::new(resource.clone(), Action::Index));
                set.add(Permission::new(resource.clone(), Action::Stats));
                set.add(Permission::new(resource.clone(), Action::List));
                set.add(Permission::new(resource, Action::Describe));
            }
            BuiltinRole::UserAdmin => {
                let resource = match namespace {
                    Some(ns) => Resource::Namespace {
                        name: ns.to_string(),
                    },
                    None => Resource::AllNamespaces,
                };
                set.add(Permission::new(resource.clone(), Action::ManageUsers));
                set.add(Permission::new(resource.clone(), Action::ManageRoles));
                set.add(Permission::new(resource.clone(), Action::Grant));
                set.add(Permission::new(resource, Action::Revoke));
            }
            BuiltinRole::DbOwner => {
                // Combine ReadWrite, DbAdmin, and UserAdmin
                set.merge(&BuiltinRole::ReadWrite.permissions(namespace));
                set.merge(&BuiltinRole::DbAdmin.permissions(namespace));
                set.merge(&BuiltinRole::UserAdmin.permissions(namespace));
            }
            BuiltinRole::ClusterMonitor => {
                set.add(Permission::new(Resource::Cluster, Action::Stats));
                set.add(Permission::new(Resource::AllNamespaces, Action::List));
                set.add(Permission::new(Resource::AllNamespaces, Action::Describe));
            }
            BuiltinRole::ClusterAdmin => {
                set.add(Permission::new(Resource::Cluster, Action::All));
            }
            BuiltinRole::Root => {
                // Full access to everything
                set.add(Permission::new(Resource::Cluster, Action::All));
                set.add(Permission::new(Resource::AllCollectionsGlobal, Action::All));
            }
        }

        set
    }

    /// Parse builtin role from string
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "read" => Ok(BuiltinRole::Read),
            "readwrite" | "read_write" => Ok(BuiltinRole::ReadWrite),
            "dbadmin" | "db_admin" => Ok(BuiltinRole::DbAdmin),
            "useradmin" | "user_admin" => Ok(BuiltinRole::UserAdmin),
            "dbowner" | "db_owner" => Ok(BuiltinRole::DbOwner),
            "clustermonitor" | "cluster_monitor" => Ok(BuiltinRole::ClusterMonitor),
            "clusteradmin" | "cluster_admin" => Ok(BuiltinRole::ClusterAdmin),
            "root" => Ok(BuiltinRole::Root),
            _ => Err(MonoError::Parse(format!("Unknown builtin role: {s}"))),
        }
    }
}

impl fmt::Display for BuiltinRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuiltinRole::Read => write!(f, "read"),
            BuiltinRole::ReadWrite => write!(f, "readWrite"),
            BuiltinRole::DbAdmin => write!(f, "dbAdmin"),
            BuiltinRole::UserAdmin => write!(f, "userAdmin"),
            BuiltinRole::DbOwner => write!(f, "dbOwner"),
            BuiltinRole::ClusterMonitor => write!(f, "clusterMonitor"),
            BuiltinRole::ClusterAdmin => write!(f, "clusterAdmin"),
            BuiltinRole::Root => write!(f, "root"),
        }
    }
}

// Role (user-defined)

/// A user-defined role that can be assigned to users.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    /// Role name (unique within namespace)
    pub name: String,
    /// Namespace this role belongs to (None = cluster-wide)
    pub namespace: Option<String>,
    /// Direct permissions granted to this role
    pub permissions: PermissionSet,
    /// Other roles this role inherits from
    pub inherits: Vec<String>,
}

impl Role {
    /// Create a new role
    pub fn new(name: impl Into<String>, namespace: Option<String>) -> Self {
        Self {
            name: name.into(),
            namespace,
            permissions: PermissionSet::new(),
            inherits: Vec::new(),
        }
    }

    /// Add a permission to this role
    pub fn add_permission(&mut self, permission: Permission) {
        self.permissions.add(permission);
    }

    /// Add a role to inherit from
    pub fn add_inherit(&mut self, role_name: impl Into<String>) {
        let name = role_name.into();
        if !self.inherits.contains(&name) {
            self.inherits.push(name);
        }
    }

    /// Encode role to bytes
    pub fn encode(&self, buf: &mut BytesMut) {
        put_string(buf, &self.name);
        put_opt_string(buf, &self.namespace);
        self.permissions.encode(buf);
        buf.put_u32_le(self.inherits.len() as u32);
        for inherit in &self.inherits {
            put_string(buf, inherit);
        }
    }

    /// Decode role from bytes
    pub fn decode(cursor: &mut &[u8]) -> Result<Self> {
        let name = get_string(cursor)?;
        let namespace = get_opt_string(cursor)?;
        let permissions = PermissionSet::decode(cursor)?;
        let inherit_count = get_u32_le(cursor)? as usize;
        let mut inherits = Vec::with_capacity(inherit_count);
        for _ in 0..inherit_count {
            inherits.push(get_string(cursor)?);
        }
        Ok(Self {
            name,
            namespace,
            permissions,
            inherits,
        })
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_roundtrip() {
        for action in [
            Action::Select,
            Action::Insert,
            Action::Update,
            Action::Delete,
            Action::Create,
            Action::Drop,
            Action::All,
        ] {
            let byte = action.to_byte();
            let parsed = Action::from_byte(byte).unwrap();
            assert_eq!(action, parsed);
        }
    }

    #[test]
    fn test_resource_encoding() {
        let resources = vec![
            Resource::Cluster,
            Resource::Namespace {
                name: "mydb".into(),
            },
            Resource::AllNamespaces,
            Resource::Collection {
                namespace: "mydb".into(),
                name: "users".into(),
            },
            Resource::AllCollections {
                namespace: "mydb".into(),
            },
            Resource::AllCollectionsGlobal,
        ];

        for resource in resources {
            let mut buf = BytesMut::new();
            resource.encode(&mut buf);
            let mut cursor = &buf[..];
            let decoded = Resource::decode(&mut cursor).unwrap();
            assert_eq!(resource, decoded);
        }
    }

    #[test]
    fn test_permission_string_parsing() {
        let test_cases = vec![
            ("cluster:*", Resource::Cluster, Action::All),
            (
                "ns:mydb:select",
                Resource::Namespace {
                    name: "mydb".into(),
                },
                Action::Select,
            ),
            (
                "ns:mydb:col:users:insert",
                Resource::Collection {
                    namespace: "mydb".into(),
                    name: "users".into(),
                },
                Action::Insert,
            ),
            (
                "ns:*:col:*:select",
                Resource::AllCollectionsGlobal,
                Action::Select,
            ),
        ];

        for (s, expected_resource, expected_action) in test_cases {
            let perm = Permission::from_str(s).unwrap();
            assert_eq!(
                perm.resource, expected_resource,
                "Resource mismatch for {s}"
            );
            assert_eq!(perm.action, expected_action, "Action mismatch for {s}");
            // Roundtrip
            assert_eq!(perm.to_string(), s);
        }
    }

    #[test]
    fn test_permission_allows() {
        let perm = Permission::new(
            Resource::AllCollections {
                namespace: "mydb".into(),
            },
            Action::Select,
        );

        // Should allow SELECT on any collection in mydb
        assert!(perm.allows(
            &Resource::Collection {
                namespace: "mydb".into(),
                name: "users".into()
            },
            Action::Select
        ));

        // Should not allow INSERT
        assert!(!perm.allows(
            &Resource::Collection {
                namespace: "mydb".into(),
                name: "users".into()
            },
            Action::Insert
        ));

        // Should not allow on different namespace
        assert!(!perm.allows(
            &Resource::Collection {
                namespace: "other".into(),
                name: "users".into()
            },
            Action::Select
        ));
    }

    #[test]
    fn test_permission_set_encoding() {
        let mut set = PermissionSet::new();
        set.add(Permission::new(Resource::Cluster, Action::Stats));
        set.add(Permission::new(
            Resource::AllCollections {
                namespace: "mydb".into(),
            },
            Action::Select,
        ));

        let mut buf = BytesMut::new();
        set.encode(&mut buf);
        let mut cursor = &buf[..];
        let decoded = PermissionSet::decode(&mut cursor).unwrap();

        assert_eq!(set, decoded);
    }

    #[test]
    fn test_builtin_role_permissions() {
        let read_perms = BuiltinRole::Read.permissions(Some("testdb"));
        assert!(read_perms.allows(
            &Resource::Collection {
                namespace: "testdb".into(),
                name: "any".into()
            },
            Action::Select
        ));
        assert!(!read_perms.allows(
            &Resource::Collection {
                namespace: "testdb".into(),
                name: "any".into()
            },
            Action::Insert
        ));

        let root_perms = BuiltinRole::Root.permissions(None);
        assert!(root_perms.allows(&Resource::Cluster, Action::Shutdown));
        assert!(root_perms.allows(
            &Resource::Collection {
                namespace: "any".into(),
                name: "any".into()
            },
            Action::Delete
        ));
    }
}

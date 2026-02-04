use std::mem;

use anyhow::Result;
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use monodb_client::Client;
use monodb_common::{Value, protocol::TableInfo};

// Lower bound heap sizing (plus a bit of approximation for hash sets / index maps).
pub fn value_heap_size(v: &Value) -> usize {
    match v {
        // No heap
        Value::Null
        | Value::Bool(_)
        | Value::Int32(_)
        | Value::Int64(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::DateTime(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::Uuid(_)
        | Value::ObjectId(_)
        | Value::GeoPoint { .. } => 0,

        // Owned buffers
        Value::String(s) => s.capacity(),
        Value::Binary(b) => b.capacity(),

        // Vec<Value>
        Value::Array(xs) => {
            xs.capacity() * mem::size_of::<Value>() + xs.iter().map(value_heap_size).sum::<usize>()
        }

        // BTreeMap<String, Value> (node overhead not counted; keys/values are)
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| k.capacity() + value_heap_size(v))
            .sum::<usize>(),

        // HashSet<String>
        Value::Set(set) => {
            // Buckets/metadata overhead isn't standardized; use a rough estimate:
            // bucket array ~ capacity * size_of::<String>()
            let buckets = set.capacity() * mem::size_of::<String>();
            let strings = set.iter().map(|s| s.capacity()).sum::<usize>();
            buckets + strings
        }

        // IndexMap<String, Value>
        Value::Row(row) => {
            // IndexMap exposes capacity(); use it to count backing storage for pairs
            // plus key/value heap recursively.
            let backing = row.capacity() * mem::size_of::<(String, Value)>();
            let entries = row
                .iter()
                .map(|(k, v)| k.capacity() + value_heap_size(v))
                .sum::<usize>();
            backing + entries
        }

        // Vec<(f64, String)>
        Value::SortedSet(xs) => {
            xs.capacity() * mem::size_of::<(f64, String)>()
                + xs.iter().map(|(_, s)| s.capacity()).sum::<usize>()
        }

        // Reference { collection: String, id: Box<Value> }
        Value::Reference { collection, id } => {
            collection.capacity()
                // Box allocation holds a Value
                + mem::size_of::<Value>()
                + value_heap_size(id)
        }

        // Extension { type_name, plugin_id, data }
        Value::Extension {
            type_name,
            plugin_id,
            data,
        } => type_name.capacity() + plugin_id.capacity() + data.capacity(),
    }
}

pub struct Namespace {
    pub name: String,
    pub tables: Vec<TableInfo>,
}

fn unqualify_table(table_name: impl Into<String>) -> Result<(String, String)> {
    let parts: Vec<String> = table_name
        .into()
        .split('.')
        .map(|s| s.to_string())
        .collect();

    if parts.len() != 2 {
        anyhow::bail!("Invalid qualified name");
    }

    Ok((parts[1].clone(), parts[0].clone()))
}

pub async fn get_namespaces(client: &Client) -> Result<Vec<Namespace>> {
    let mut namespaces = Vec::new();
    let table_list = client.list_tables().await?;
    for namespace in table_list.namespaces {
        namespaces.push(Namespace {
            name: namespace,
            tables: Vec::new(),
        });
    }

    for table in table_list.tables {
        let (name, namespace) = unqualify_table(table.name).unwrap(); // Assume all tables given will be properly qualified

        // Find the namespace and add the table
        if let Some(ns) = namespaces.iter_mut().find(|ns| ns.name == namespace) {
            ns.tables.push(TableInfo {
                name,
                row_count: table.row_count,
                size_bytes: table.size_bytes,
                schema: table.schema,
            });
        }
    }

    Ok(namespaces)
}

pub async fn hash_password(argon2: &Argon2<'_>, password: impl AsRef<[u8]>, id: u64) -> String {
    let mut salt_bytes = [0u8; 16];
    salt_bytes[..8].copy_from_slice(&id.to_le_bytes());
    salt_bytes[8..16].copy_from_slice(&(id.wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
    let salt = SaltString::encode_b64(&salt_bytes).expect("salt encode");

    argon2
        .hash_password(password.as_ref(), &salt)
        .expect("password hash")
        .to_string()
}

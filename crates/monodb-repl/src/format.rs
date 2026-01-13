#![allow(dead_code)]

//! Output formatting for REPL results

use std::collections::BTreeMap;
use std::time::Duration;

use colored::*;
use indexmap::IndexMap;
use monodb_client::QueryResult;
use monodb_common::Value;

/// Storage type hint for formatting
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageHint {
    Relational,
    Document,
    Keyspace,
    Unknown,
}

impl StorageHint {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "relational" | "relation" | "rel" => StorageHint::Relational,
            "document" | "doc" => StorageHint::Document,
            "keyspace" | "kv" | "key-value" => StorageHint::Keyspace,
            _ => StorageHint::Unknown,
        }
    }
}

/// Handles formatting of query results.
pub struct Formatter {
    /// Maximum column width before truncation
    max_col_width: usize,
    /// Current storage hint for formatting
    storage_hint: StorageHint,
}

impl Formatter {
    pub fn new() -> Self {
        Self {
            max_col_width: 50,
            storage_hint: StorageHint::Unknown,
        }
    }

    /// Set storage hint for next format operation
    pub fn set_storage_hint(&mut self, hint: StorageHint) {
        self.storage_hint = hint;
    }

    /// Format a query result.
    pub fn format_result(&mut self, result: &QueryResult, elapsed: Duration) {
        if result.is_created() {
            println!("{}", "Created".green().bold());
        } else if result.is_modified() {
            let rows = result.rows_affected();
            let plural = if rows == 1 { "" } else { "s" };
            println!("{} ({} row{})", "Modified".green().bold(), rows, plural);
        } else {
            let rows = result.rows();
            if rows.is_empty() {
                println!("{}", "(no results)".dimmed());
            } else {
                match self.storage_hint {
                    StorageHint::Relational => self.format_relational(&rows),
                    StorageHint::Document => self.format_documents(&rows),
                    StorageHint::Keyspace => self.format_keyspace(&rows),
                    StorageHint::Unknown => self.format_auto(&rows),
                }
                let count = rows.len();
                let plural = if count == 1 { "" } else { "s" };
                println!("\n{} row{}", count, plural);
            }
        }

        println!("{}", format!("({:.2?})", elapsed).dimmed());
        println!();

        // Reset hint after use
        self.storage_hint = StorageHint::Unknown;
    }

    /// Auto-detect format based on data structure
    fn format_auto(&self, rows: &[monodb_client::Row]) {
        if rows.is_empty() {
            return;
        }

        // Check if all rows look like documents (have nested objects/arrays)
        let has_nested = rows.iter().any(|r| {
            match r.value() {
                Value::Row(m) => m.values().any(|v| matches!(v, Value::Object(_) | Value::Array(_))),
                Value::Object(m) => m.values().any(|v| matches!(v, Value::Object(_) | Value::Array(_))),
                _ => false,
            }
        });

        // Check if rows look like key-value pairs (only 2 fields: key and value)
        let looks_like_kv = rows.iter().all(|r| {
            let fields = r.fields();
            fields.len() == 2 && (fields.contains(&"key") || fields.contains(&"_id"))
        });

        if looks_like_kv && !has_nested {
            self.format_keyspace(rows);
        } else if has_nested {
            self.format_documents(rows);
        } else {
            self.format_relational(rows);
        }
    }

    /// Format results as a relational table with box-drawing characters
    fn format_relational(&self, rows: &[monodb_client::Row]) {
        if rows.is_empty() {
            return;
        }

        let columns = rows[0].fields().into_iter().map(String::from).collect::<Vec<_>>();
        if columns.is_empty() {
            return;
        }

        // Calculate column widths
        let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
        for row in rows {
            for (i, col) in columns.iter().enumerate() {
                let val_str = self.get_cell_string(row.get(col));
                widths[i] = widths[i].max(val_str.len()).min(self.max_col_width);
            }
        }

        // Print top border
        self.print_table_border(&widths, '┌', '┬', '┐');

        // Print header
        print!("│");
        for (i, col) in columns.iter().enumerate() {
            let display = self.truncate(col, widths[i]);
            print!(" {:<width$} │", display.bold().cyan(), width = widths[i]);
        }
        println!();

        // Print header separator
        self.print_table_border(&widths, '├', '┼', '┤');

        // Print rows
        for row in rows {
            print!("│");
            for (i, col) in columns.iter().enumerate() {
                let val = row.get(col);
                let val_str = self.get_cell_string(val);
                let display = self.truncate(&val_str, widths[i]);
                let colored = self.colorize_cell(&display, val);
                print!(" {:<width$} │", colored, width = widths[i]);
            }
            println!();
        }

        // Print bottom border
        self.print_table_border(&widths, '└', '┴', '┘');
    }

    fn print_table_border(&self, widths: &[usize], left: char, mid: char, right: char) {
        print!("{}", left);
        for (i, width) in widths.iter().enumerate() {
            print!("{}", "─".repeat(width + 2));
            if i < widths.len() - 1 {
                print!("{}", mid);
            }
        }
        println!("{}", right);
    }

    fn get_cell_string(&self, val: Option<&Value>) -> String {
        match val {
            None => "NULL".to_string(),
            Some(v) => self.value_to_plain(v),
        }
    }

    fn colorize_cell(&self, display: &str, val: Option<&Value>) -> ColoredString {
        match val {
            None | Some(Value::Null) => display.dimmed(),
            Some(Value::Bool(true)) => display.green(),
            Some(Value::Bool(false)) => display.red(),
            Some(Value::Int32(_) | Value::Int64(_) | Value::Float32(_) | Value::Float64(_)) => {
                display.cyan()
            }
            Some(Value::String(_)) => display.yellow(),
            _ => display.normal(),
        }
    }

    /// Format results in MongoDB-style JSON format
    fn format_documents(&self, rows: &[monodb_client::Row]) {
        for (i, row) in rows.iter().enumerate() {
            if i > 0 {
                println!();
            }
            self.format_mongo_value(row.value(), 0, true);
        }
    }

    fn format_mongo_value(&self, value: &Value, indent: usize, is_root: bool) {
        let prefix = " ".repeat(indent);

        match value {
            Value::Row(_) | Value::Object(_) => {
                let map: Box<dyn Iterator<Item = (&String, &Value)>> = match value {
                    Value::Row(r) => Box::new(r.iter()),
                    Value::Object(o) => Box::new(o.iter()),
                    _ => unreachable!(),
                };
                let entries: Vec<_> = map.collect();

                println!("{}{{", if is_root { "" } else { &prefix });
                for (i, (key, val)) in entries.iter().enumerate() {
                    let comma = if i < entries.len() - 1 { "," } else { "" };
                    print!("{}  {}: ", prefix, key.cyan());

                    match val {
                        Value::Object(_) | Value::Row(_) | Value::Array(_) => {
                            println!();
                            self.format_mongo_value(val, indent + 2, false);
                            if !comma.is_empty() {
                                println!("{},", "");
                            }
                        }
                        _ => {
                            println!("{}{}", self.value_to_mongo(val), comma);
                        }
                    }
                }
                print!("{}}}", prefix);
                if is_root {
                    println!();
                }
            }
            Value::Array(arr) => {
                if arr.is_empty() {
                    print!("[]");
                } else if arr.iter().all(|v| !matches!(v, Value::Object(_) | Value::Row(_) | Value::Array(_))) {
                    // Simple array, print on one line
                    print!("[");
                    for (i, v) in arr.iter().enumerate() {
                        print!("{}", self.value_to_mongo(v));
                        if i < arr.len() - 1 {
                            print!(", ");
                        }
                    }
                    print!("]");
                } else {
                    // Complex array, print multi-line
                    println!("{}[", if is_root { "" } else { &prefix });
                    for (i, v) in arr.iter().enumerate() {
                        self.format_mongo_value(v, indent + 2, false);
                        if i < arr.len() - 1 {
                            println!(",");
                        } else {
                            println!();
                        }
                    }
                    print!("{}]", prefix);
                }
                if is_root {
                    println!();
                }
            }
            _ => {
                print!("{}", self.value_to_mongo(value));
                if is_root {
                    println!();
                }
            }
        }
    }

    fn value_to_mongo(&self, value: &Value) -> ColoredString {
        match value {
            Value::Null => "null".dimmed(),
            Value::Bool(true) => "true".green(),
            Value::Bool(false) => "false".red(),
            Value::Int32(i) => i.to_string().cyan(),
            Value::Int64(i) => i.to_string().cyan(),
            Value::Float32(f) => f.to_string().cyan(),
            Value::Float64(f) => f.to_string().cyan(),
            Value::String(s) => format!("\"{}\"", s).yellow(),
            Value::Binary(b) => format!("BinData(0, \"{}\")", base64_encode(b)).magenta(),
            Value::DateTime(dt) => format!("ISODate(\"{}\")", dt.to_rfc3339()).green(),
            Value::ObjectId(oid) => format!("ObjectId(\"{}\")", oid).bright_blue(),
            Value::Uuid(u) => format!("UUID(\"{}\")", u).bright_blue(),
            _ => format!("{}", value).normal(),
        }
    }

    /// Format results in Redis-style key-value format
    fn format_keyspace(&self, rows: &[monodb_client::Row]) {
        for (i, row) in rows.iter().enumerate() {
            let idx = format!("{})", i + 1);

            // Try to get key and value from the row
            let key = row.get("key")
                .or_else(|| row.get("_id"))
                .or_else(|| row.get("id"))
                .map(|v| self.value_to_redis_key(v))
                .unwrap_or_else(|| "?".to_string());

            let value = row.get("value")
                .or_else(|| row.get("data"))
                .map(|v| self.value_to_redis_value(v))
                .unwrap_or_else(|| {
                    // If no explicit value field, show all other fields
                    let fields: Vec<_> = row.fields().into_iter()
                        .filter(|f| *f != "key" && *f != "_id" && *f != "id")
                        .collect();
                    if fields.is_empty() {
                        "(nil)".dimmed().to_string()
                    } else {
                        fields.into_iter()
                            .filter_map(|f| row.get(f).map(|v| format!("{}: {}", f, self.value_to_redis_value(v))))
                            .collect::<Vec<_>>()
                            .join(" ")
                    }
                });

            println!("{} {} {}", idx.dimmed(), key.bold(), value);
        }
    }

    fn value_to_redis_key(&self, value: &Value) -> String {
        match value {
            Value::String(s) => format!("\"{}\"", s),
            Value::Binary(b) => format!("0x{}", hex::encode(b)),
            _ => self.value_to_plain(value),
        }
    }

    fn value_to_redis_value(&self, value: &Value) -> String {
        match value {
            Value::Null => "(nil)".dimmed().to_string(),
            Value::String(s) => format!("\"{}\"", s).yellow().to_string(),
            Value::Int32(i) => format!("(integer) {}", i).cyan().to_string(),
            Value::Int64(i) => format!("(integer) {}", i).cyan().to_string(),
            Value::Float32(f) => format!("(float) {}", f).cyan().to_string(),
            Value::Float64(f) => format!("(float) {}", f).cyan().to_string(),
            Value::Bool(true) => "(boolean) true".green().to_string(),
            Value::Bool(false) => "(boolean) false".red().to_string(),
            Value::Binary(b) => format!("(binary) {} bytes", b.len()).magenta().to_string(),
            Value::Array(arr) => format!("(list) {} items", arr.len()).to_string(),
            Value::Object(_) | Value::Row(_) => "(hash)".to_string(),
            Value::Set(s) => format!("(set) {} members", s.len()).to_string(),
            Value::SortedSet(s) => format!("(zset) {} members", s.len()).to_string(),
            _ => self.value_to_plain(value),
        }
    }

    /// Format a single value with proper indentation (legacy)
    pub fn format_value(&self, value: &Value, indent: usize) {
        match value {
            Value::Array(arr) => self.format_array(arr, indent),
            Value::Row(row) => self.format_row(row, indent),
            Value::Object(obj) => self.format_object(obj, indent),
            Value::Set(set) => self.format_set(set),
            Value::SortedSet(sorted) => self.format_sorted_set(sorted),
            _ => println!("{}", self.value_to_colored(value)),
        }
    }

    fn format_array(&self, arr: &[Value], indent: usize) {
        if arr.is_empty() {
            println!("[]");
            return;
        }

        for (i, item) in arr.iter().enumerate() {
            let prefix = format!("[{}] ", i);
            print!("{}", prefix.dimmed());

            match item {
                Value::Array(_) | Value::Object(_) | Value::Row(_) => {
                    println!();
                    self.format_value(item, indent + 2);
                }
                _ => println!("{}", self.value_to_colored(item)),
            }
        }
    }

    fn format_row(&self, row: &IndexMap<String, Value>, indent: usize) {
        let prefix = " ".repeat(indent);
        for (key, value) in row {
            print!("{}{}: ", prefix, key.bold());
            match value {
                Value::Array(_) | Value::Object(_) | Value::Row(_) => {
                    println!();
                    self.format_value(value, indent + 2);
                }
                _ => println!("{}", self.value_to_colored(value)),
            }
        }
    }

    fn format_object(&self, obj: &BTreeMap<String, Value>, indent: usize) {
        let prefix = " ".repeat(indent);
        for (key, value) in obj {
            print!("{}{}: ", prefix, key.bold());
            match value {
                Value::Array(_) | Value::Object(_) | Value::Row(_) => {
                    println!();
                    self.format_value(value, indent + 2);
                }
                _ => println!("{}", self.value_to_colored(value)),
            }
        }
    }

    fn format_set(&self, set: &std::collections::HashSet<String>) {
        let mut items: Vec<_> = set.iter().collect();
        items.sort();
        print!("{{");
        for (i, item) in items.iter().enumerate() {
            print!("{}", item.yellow());
            if i < items.len() - 1 {
                print!(", ");
            }
        }
        println!("}}");
    }

    fn format_sorted_set(&self, sorted: &[(f64, String)]) {
        for (i, (score, member)) in sorted.iter().enumerate() {
            println!(
                "{}) {} {}",
                (i + 1).to_string().dimmed(),
                score.to_string().cyan(),
                member.yellow()
            );
        }
    }

    /// Convert a value to a colored string for display.
    fn value_to_colored(&self, value: &Value) -> String {
        match value {
            Value::Null => "null".dimmed().to_string(),
            Value::Bool(true) => "true".green().to_string(),
            Value::Bool(false) => "false".red().to_string(),
            Value::Int32(i) => i.to_string().cyan().to_string(),
            Value::Int64(i) => i.to_string().cyan().to_string(),
            Value::Float32(f) => f.to_string().cyan().to_string(),
            Value::Float64(f) => f.to_string().cyan().to_string(),
            Value::String(s) => format!("\"{}\"", s).yellow().to_string(),
            Value::Binary(b) => format!("0x{}", hex::encode(b)).magenta().to_string(),
            Value::DateTime(dt) => dt.to_rfc3339().to_string(),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::Uuid(u) => u.to_string().bright_blue().to_string(),
            Value::ObjectId(oid) => oid.to_string().bright_blue().to_string(),
            Value::GeoPoint { lat, lng } => format!("({}, {})", lat, lng),
            Value::Reference { collection, id } => {
                format!(
                    "{}:{}",
                    collection.bright_magenta(),
                    self.value_to_colored(id)
                )
            }
            _ => format!("{}", value),
        }
    }

    /// Convert a value to a plain string (for table cells).
    fn value_to_plain(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int32(i) => i.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float32(f) => f.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::String(s) => s.clone(),
            Value::Binary(b) => format!("0x{}", hex::encode(b)),
            Value::DateTime(dt) => dt.to_rfc3339(),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::Uuid(u) => u.to_string(),
            Value::ObjectId(oid) => oid.to_string(),
            Value::GeoPoint { lat, lng } => format!("({},{})", lat, lng),
            Value::Reference { collection, id } => {
                format!("{}:{}", collection, self.value_to_plain(id))
            }
            Value::Array(a) => format!("[{} items]", a.len()),
            Value::Object(_) | Value::Row(_) => "{...}".to_string(),
            Value::Set(s) => format!("{{{} items}}", s.len()),
            Value::SortedSet(s) => format!("{{{} items}}", s.len()),
            Value::Extension { type_name, .. } => format!("<{type_name}>"),
        }
    }

    /// Truncate a string to max length with ellipsis.
    fn truncate(&self, s: &str, max_len: usize) -> String {
        if s.len() <= max_len {
            s.to_string()
        } else if max_len <= 3 {
            s.chars().take(max_len).collect()
        } else {
            let mut result: String = s.chars().take(max_len - 1).collect();
            result.push('…');
            result
        }
    }
}

impl Default for Formatter {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple base64 encoding for binary display
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

use colored::*;
use indexmap::IndexMap;
use monodb_common::Value;

pub fn format_value(value: &Value, indent: usize) {
    match value {
        Value::Array(arr) => format_array(arr, indent),
        Value::Row(row) => format_row(row, indent),
        Value::Object(obj) => format_object(obj, indent),
        Value::Set(set) => format_set(set),
        Value::SortedSet(sorted) => format_sorted_set(sorted),
        _ => println!("{}", format_simple_value(value)),
    }
}

fn format_simple_value(value: &Value) -> String {
    match value {
        Value::Null => "null".dimmed().to_string(),
        Value::Bool(b) => {
            if *b {
                "true".green().to_string()
            } else {
                "false".red().to_string()
            }
        }
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
                format_simple_value(id)
            )
        }
        _ => format!("{}", value),
    }
}

fn format_array(arr: &[Value], indent: usize) {
    if arr.is_empty() {
        println!("[]");
        return;
    }

    // Check if all elements are rows or objects (for table display)
    let all_rows = arr.iter().all(|v| matches!(v, Value::Row(_)));
    let all_objects = arr.iter().all(|v| matches!(v, Value::Object(_)));

    if arr.first().is_some() && (all_rows || all_objects) {
        format_table(arr);
        return;
    }

    // Otherwise print as list
    for (i, item) in arr.iter().enumerate() {
        print!("[{}] ", i);
        match item {
            Value::Array(_) | Value::Object(_) | Value::Row(_) => {
                println!();
                format_value(item, indent + 2);
            }
            _ => println!("{}", format_simple_value(item)),
        }
    }
}

fn format_table(rows: &[Value]) {
    if rows.is_empty() {
        return;
    }

    // Extract column names from first row
    let columns: Vec<String> = match &rows[0] {
        Value::Row(first_row) => first_row.keys().cloned().collect(),
        Value::Object(first_obj) => first_obj.keys().cloned().collect(),
        _ => return,
    };

    const MAX_COL_WIDTH: usize = 40;

    // Calculate column widths with maximum cap
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();

    for row in rows {
        let map_ref = match row {
            Value::Row(r) => Some(r.iter().map(|(k, v)| (k.as_str(), v)).collect::<Vec<_>>()),
            Value::Object(o) => Some(o.iter().map(|(k, v)| (k.as_str(), v)).collect::<Vec<_>>()),
            _ => None,
        };

        if let Some(map) = map_ref {
            for (i, col) in columns.iter().enumerate() {
                if let Some((_, val)) = map.iter().find(|(k, _)| k == col) {
                    let val_str = value_to_string(val);
                    widths[i] = widths[i].max(val_str.len()).min(MAX_COL_WIDTH);
                }
            }
        }
    }

    // Print header
    for (i, col) in columns.iter().enumerate() {
        let display_col = truncate_string(col, widths[i]);
        print!("{:<width$}", display_col.bold(), width = widths[i]);
        if i < columns.len() - 1 {
            print!(" ");
        }
    }
    println!();

    // Print compact separator
    for (i, width) in widths.iter().enumerate() {
        print!("{}", "-".repeat(*width));
        if i < widths.len() - 1 {
            print!(" ");
        }
    }
    println!();

    // Print rows
    for row in rows {
        for (i, col) in columns.iter().enumerate() {
            let val_str = match row {
                Value::Row(r) => r.get(col).map(|v| value_to_string(v)),
                Value::Object(o) => o.get(col).map(|v| value_to_string(v)),
                _ => None,
            }
            .unwrap_or_else(|| "null".to_string());

            let display_val = truncate_string(&val_str, widths[i]);
            print!("{:<width$}", display_val, width = widths[i]);
            if i < columns.len() - 1 {
                print!(" ");
            }
        }
        println!();
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else if max_len <= 3 {
        s.chars().take(max_len).collect()
    } else {
        let mut result: String = s.chars().take(max_len - 3).collect();
        result.push_str("...");
        result
    }
}

fn format_row(row: &IndexMap<String, Value>, _indent: usize) {
    for (key, value) in row {
        print!("{}: ", key.bold());
        match value {
            Value::Array(_) | Value::Object(_) | Value::Row(_) => {
                println!();
                format_value(value, 2);
            }
            _ => println!("{}", format_simple_value(value)),
        }
    }
}

fn format_object(obj: &std::collections::BTreeMap<String, Value>, _indent: usize) {
    for (key, value) in obj {
        print!("{}: ", key.bold());
        match value {
            Value::Array(_) | Value::Object(_) | Value::Row(_) => {
                println!();
                format_value(value, 2);
            }
            _ => println!("{}", format_simple_value(value)),
        }
    }
}

fn format_set(set: &std::collections::HashSet<String>) {
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

fn format_sorted_set(sorted: &[(f64, String)]) {
    for (score, member) in sorted {
        println!("{:10} {}", score.to_string().cyan(), member.yellow());
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
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
        Value::Reference { collection, id } => format!("{}:{}", collection, value_to_string(id)),
        Value::Array(_) => "[...]".to_string(),
        Value::Object(_) => "{...}".to_string(),
        Value::Row(_) => "{...}".to_string(),
        Value::Set(s) => format!("{{{}...}}", s.len()),
        Value::SortedSet(s) => format!("{{{}...}}", s.len()),
    }
}

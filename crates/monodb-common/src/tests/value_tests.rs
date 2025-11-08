use std::collections::HashMap;

use crate::{ObjectId, Value};

pub use super::*;

#[test]
fn test_addition_operations() {
    let int32_a = Value::Int32(10);
    let int32_b = Value::Int32(20);
    assert_eq!(
        (int32_a.clone() + int32_b.clone()).unwrap(),
        Value::Int32(30)
    );

    let int64_a = Value::Int64(1_000_000_000);
    let int64_b = Value::Int64(2_000_000_000);
    assert_eq!(
        (int64_a.clone() + int64_b.clone()).unwrap(),
        Value::Int64(3_000_000_000)
    );

    let float32_a = Value::Float32(1.5);
    let float32_b = Value::Float32(2.5);
    assert_eq!(
        (float32_a.clone() + float32_b.clone()).unwrap(),
        Value::Float32(4.0)
    );

    let float64_a = Value::Float64(1.5);
    let float64_b = Value::Float64(2.5);
    assert_eq!(
        (float64_a.clone() + float64_b.clone()).unwrap(),
        Value::Float64(4.0)
    );

    let str_a = Value::String("Hello, ".to_string());
    let str_b = Value::String("World!".to_string());
    assert_eq!(
        (str_a.clone() + str_b.clone()).unwrap(),
        Value::String("Hello, World!".to_string())
    );

    let arr_a = Value::Array(vec![Value::Int32(1), Value::Int32(2)]);
    let arr_b = Value::Array(vec![Value::Int32(3), Value::Int32(4)]);
    assert_eq!(
        (arr_a.clone() + arr_b.clone()).unwrap(),
        Value::Array(vec![
            Value::Int32(1),
            Value::Int32(2),
            Value::Int32(3),
            Value::Int32(4)
        ])
    );

    // Mixed type addition
    let mixed_a = Value::Int32(10);
    let mixed_b = Value::Float64(20.5);
    assert_eq!(
        (mixed_a.clone() + mixed_b.clone()).unwrap(),
        Value::Float64(30.5)
    );

    // Invalid addition
    let invalid_a = Value::String("Hello".to_string());
    let invalid_b = Value::Bool(true);
    assert!((invalid_a + invalid_b).is_err());
}

#[test]
fn test_subtraction_operations() {
    let int32_a = Value::Int32(20);
    let int32_b = Value::Int32(10);
    assert_eq!(
        (int32_a.clone() - int32_b.clone()).unwrap(),
        Value::Int32(10)
    );

    let int64_a = Value::Int64(2_000_000_000);
    let int64_b = Value::Int64(1_000_000_000);
    assert_eq!(
        (int64_a.clone() - int64_b.clone()).unwrap(),
        Value::Int64(1_000_000_000)
    );

    let float32_a = Value::Float32(5.5);
    let float32_b = Value::Float32(2.5);
    assert_eq!(
        (float32_a.clone() - float32_b.clone()).unwrap(),
        Value::Float32(3.0)
    );

    let float64_a = Value::Float64(5.5);
    let float64_b = Value::Float64(2.5);
    assert_eq!(
        (float64_a.clone() - float64_b.clone()).unwrap(),
        Value::Float64(3.0)
    );

    // Mixed type subtraction
    let mixed_a = Value::Int32(30);
    let mixed_b = Value::Int64(10);
    assert_eq!(
        (mixed_a.clone() - mixed_b.clone()).unwrap(),
        Value::Int64(20)
    );

    // Invalid subtraction
    let invalid_a = Value::String("Hello".to_string());
    let invalid_b = Value::Bool(true);
    assert!((invalid_a - invalid_b).is_err());
}

#[test]
fn test_multiplication_operations() {
    let int32_a = Value::Int32(10);
    let int32_b = Value::Int32(20);
    assert_eq!(
        (int32_a.clone() * int32_b.clone()).unwrap(),
        Value::Int32(200)
    );

    let int64_a = Value::Int64(1_000_000);
    let int64_b = Value::Int64(2_000_000);
    assert_eq!(
        (int64_a.clone() * int64_b.clone()).unwrap(),
        Value::Int64(2_000_000_000_000)
    );

    let float32_a = Value::Float32(1.5);
    let float32_b = Value::Float32(2.0);
    assert_eq!(
        (float32_a.clone() * float32_b.clone()).unwrap(),
        Value::Float32(3.0)
    );

    let float64_a = Value::Float64(1.5);
    let float64_b = Value::Float64(2.0);
    assert_eq!(
        (float64_a.clone() * float64_b.clone()).unwrap(),
        Value::Float64(3.0)
    );

    // String repetition
    let str_a = Value::String("Hi".to_string());
    let int_b = Value::Int32(3);
    assert_eq!(
        (str_a.clone() * int_b.clone()).unwrap(),
        Value::String("HiHiHi".to_string())
    );

    let str_c = Value::String("Hello".to_string());
    let int_d = Value::Int64(2);
    assert_eq!(
        (str_c.clone() * int_d.clone()).unwrap(),
        Value::String("HelloHello".to_string())
    );

    // Mixed type multiplication
    let mixed_a = Value::Int32(10);
    let mixed_b = Value::Int64(2);
    assert_eq!(
        (mixed_a.clone() * mixed_b.clone()).unwrap(),
        Value::Int64(20)
    );

    // Invalid multiplication
    let invalid_a = Value::String("Hello".to_string());
    let invalid_b = Value::Bool(true);
    assert!((invalid_a * invalid_b).is_err());
}

#[test]
fn test_division_operations() {
    let int32_a = Value::Int32(20);
    let int32_b = Value::Int32(10);
    assert_eq!(
        (int32_a.clone() / int32_b.clone()).unwrap(),
        Value::Int32(2)
    );

    let int64_a = Value::Int64(2_000_000_000);
    let int64_b = Value::Int64(1_000_000_000);
    assert_eq!(
        (int64_a.clone() / int64_b.clone()).unwrap(),
        Value::Int64(2)
    );

    let float32_a = Value::Float32(5.0);
    let float32_b = Value::Float32(2.0);
    assert_eq!(
        (float32_a.clone() / float32_b.clone()).unwrap(),
        Value::Float32(2.5)
    );

    let float64_a = Value::Float64(5.0);
    let float64_b = Value::Float64(2.0);
    assert_eq!(
        (float64_a.clone() / float64_b.clone()).unwrap(),
        Value::Float64(2.5)
    );

    // Mixed type division
    let mixed_a = Value::Int32(30);
    let mixed_b = Value::Int64(10);
    assert_eq!(
        (mixed_a.clone() / mixed_b.clone()).unwrap(),
        Value::Int64(3)
    );

    // Division by zero
    let div_by_zero_a = Value::Int32(10);
    let div_by_zero_b = Value::Int32(0);
    assert!((div_by_zero_a / div_by_zero_b).is_err());

    // Invalid division
    let invalid_a = Value::String("Hello".to_string());
    let invalid_b = Value::Bool(true);
    assert!((invalid_a - invalid_b).is_err());
}

#[test]
fn test_objectid_generation() {
    let oid1 = ObjectId::new().unwrap();
    let oid2 = ObjectId::new().unwrap();
    assert_ne!(oid1, oid2);
    assert_eq!(oid1.to_string().len(), 24);
    assert_eq!(oid2.to_string().len(), 24);

    let bytes1 = oid1.bytes();
    let bytes2 = oid2.bytes();
    assert_eq!(bytes1.len(), 12);
    assert_eq!(bytes2.len(), 12);

    // Ensure the timestamp part is correct
    let ts1 = u32::from_be_bytes([bytes1[0], bytes1[1], bytes1[2], bytes1[3]]);
    let ts2 = u32::from_be_bytes([bytes2[0], bytes2[1], bytes2[2], bytes2[3]]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    assert!(ts1 <= now);
    assert!(ts2 <= now);
}

#[test]
fn test_type_inference() {
    let mut examples: HashMap<&str, &str> = HashMap::new();
    examples.insert("42", "int32");
    examples.insert("2147483648", "int64");
    examples.insert("3.14159", "float32");
    examples.insert("3.14159265358979323846", "float64");
    examples.insert("\"Hello, World!\"", "string");
    examples.insert("true", "bool");
    examples.insert("false", "bool");
    examples.insert("null", "null");
    examples.insert("none", "null");
    examples.insert("", "null");
    examples.insert("0x48656c6c6f", "binary");
    examples.insert("2023-10-05T14:48:00Z", "datetime");
    examples.insert("2023-10-05", "date");
    examples.insert("14:48:00", "time");
    examples.insert("550e8400-e29b-41d4-a716-446655440000", "uuid");
    examples.insert("507f1f77bcf86cd799439011", "objectid");
    examples.insert("[1, 2, 3]", "array");
    examples.insert("{\"key\": \"value\"}", "object");
    examples.insert("Set{\"apple\", \"banana\"}", "set");
    examples.insert("Row(1, \"two\", 3.0)", "row");
    examples.insert("SortedSet{(1.0, \"one\"), (2.0, \"two\")}", "sortedset");
    examples.insert("GeoPoint(37.7749, -122.4194)", "geopoint");
    examples.insert("Reference(\"users\", \"12345\")", "reference");

    let float_overflow: f64 = (f32::MAX as f64) + 1.0;
    let binding = float_overflow.to_string();
    examples.insert(binding.as_str(), "float64");

    for example in examples {
        let value: Value = example.0.parse().unwrap();
        assert_eq!(value.type_name(), example.1);
    }
}

#[tokio::test]
async fn test_objectid_collisions() {
    use std::collections::HashSet;
    use std::thread;
    use std::time::Duration;

    let total_sequential = 100_000;
    let total_rapid = 50_000;
    let thread_count = 10;
    let ids_per_thread = 10_000;
    let total_concurrent = thread_count * ids_per_thread;
    let total_timestamp = 1_000;

    // Sequential generation
    let mut ids: Vec<[u8; 12]> = Vec::with_capacity(total_sequential);
    for _ in 0..total_sequential {
        ids.push(ObjectId::new().unwrap().bytes());
    }
    let unique_count = ids.iter().collect::<HashSet<_>>().len();
    assert_eq!(unique_count, ids.len());

    // Rapid generation
    let mut rapid_ids = Vec::with_capacity(total_rapid);
    for _ in 0..total_rapid {
        rapid_ids.push(ObjectId::new().unwrap().bytes());
    }
    let rapid_unique = rapid_ids.iter().collect::<HashSet<_>>().len();
    assert_eq!(rapid_unique, rapid_ids.len());

    // Concurrent generation
    let handles: Vec<_> = (0..thread_count)
        .map(|_| {
            thread::spawn(move || {
                let mut thread_ids = Vec::with_capacity(ids_per_thread);
                for _ in 0..ids_per_thread {
                    thread_ids.push(ObjectId::new().unwrap().bytes());
                }
                thread_ids
            })
        })
        .collect();

    let mut all_concurrent_ids = Vec::with_capacity(total_concurrent);
    for handle in handles {
        all_concurrent_ids.extend(handle.join().unwrap());
    }
    let concurrent_unique = all_concurrent_ids.iter().collect::<HashSet<_>>().len();
    assert_eq!(concurrent_unique, all_concurrent_ids.len());

    // Timestamp order test
    let mut timestamp_ids = Vec::with_capacity(total_timestamp);
    for _ in 0..total_timestamp {
        timestamp_ids.push(ObjectId::new().unwrap().bytes());
        thread::sleep(Duration::from_micros(100));
    }
    for i in 1..timestamp_ids.len() {
        let prev_ts = u32::from_be_bytes([
            timestamp_ids[i - 1][0],
            timestamp_ids[i - 1][1],
            timestamp_ids[i - 1][2],
            timestamp_ids[i - 1][3],
        ]);
        let curr_ts = u32::from_be_bytes([
            timestamp_ids[i][0],
            timestamp_ids[i][1],
            timestamp_ids[i][2],
            timestamp_ids[i][3],
        ]);
        assert!(curr_ts >= prev_ts);
    }

    // Overall uniqueness
    let mut all_ids = Vec::with_capacity(
        ids.len() + rapid_ids.len() + all_concurrent_ids.len() + timestamp_ids.len(),
    );
    all_ids.extend(ids);
    all_ids.extend(rapid_ids);
    all_ids.extend(all_concurrent_ids);
    all_ids.extend(timestamp_ids);
    let total_unique = all_ids.iter().collect::<HashSet<_>>().len();
    assert_eq!(total_unique, all_ids.len());
}

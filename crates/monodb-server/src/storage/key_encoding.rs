//! Sortable key encoding for LSM storage
//!
//! Provides binary encoding that preserves sort order:
//! - Integers: big-endian with sign bit flipped (so -1 < 0 < 1)
//! - Strings: as-is (UTF-8 sorts correctly)
//! - Composite keys: length-prefixed segments

use monodb_common::value::Value;

/// Encode a value into bytes that sort correctly
/// The encoding preserves natural sort order for all types
pub fn encode_sortable(value: &Value) -> Vec<u8> {
    match value {
        // Signed integers: flip sign bit and use big-endian
        Value::Int32(n) => {
            let mut bytes = [0u8; 5];
            bytes[0] = 0x10; // Type tag for i32
            let encoded = (*n as u32) ^ 0x80000000; // Flip sign bit
            bytes[1..5].copy_from_slice(&encoded.to_be_bytes());
            bytes.to_vec()
        }
        Value::Int64(n) => {
            let mut bytes = [0u8; 9];
            bytes[0] = 0x11; // Type tag for i64
            let encoded = (*n as u64) ^ 0x8000000000000000; // Flip sign bit
            bytes[1..9].copy_from_slice(&encoded.to_be_bytes());
            bytes.to_vec()
        }

        // Unsigned would just be big-endian (if we had them)

        // Floats: IEEE 754 with sign handling
        Value::Float32(f) => {
            let mut bytes = [0u8; 5];
            bytes[0] = 0x20; // Type tag for f32
            let bits = f.to_bits();
            let encoded = if *f >= 0.0 { bits ^ 0x80000000 } else { !bits };
            bytes[1..5].copy_from_slice(&encoded.to_be_bytes());
            bytes.to_vec()
        }
        Value::Float64(f) => {
            let mut bytes = [0u8; 9];
            bytes[0] = 0x21; // Type tag for f64
            let bits = f.to_bits();
            let encoded = if *f >= 0.0 {
                bits ^ 0x8000000000000000
            } else {
                !bits
            };
            bytes[1..9].copy_from_slice(&encoded.to_be_bytes());
            bytes.to_vec()
        }

        // Strings: type tag + raw bytes (UTF-8 already sorts correctly)
        Value::String(s) => {
            let mut bytes = Vec::with_capacity(1 + s.len());
            bytes.push(0x30); // Type tag for string
            bytes.extend_from_slice(s.as_bytes());
            bytes
        }

        // Booleans: false < true
        Value::Bool(b) => {
            vec![0x05, if *b { 1 } else { 0 }]
        }

        // Null sorts first
        Value::Null => vec![0x00],

        // For other types, fall back to string representation
        other => {
            let s = other.to_string();
            let mut bytes = Vec::with_capacity(1 + s.len());
            bytes.push(0x40); // Type tag for other
            bytes.extend_from_slice(s.as_bytes());
            bytes
        }
    }
}

/// Encode multiple values as a composite key
/// Each value is length-prefixed to allow correct sorting of composite keys
pub fn encode_composite_sortable(values: &[Value]) -> Vec<u8> {
    let mut result = Vec::new();
    for value in values {
        let encoded = encode_sortable(value);
        // Use 2-byte length prefix (max 65535 bytes per component)
        let len = encoded.len() as u16;
        result.extend_from_slice(&len.to_be_bytes());
        result.extend_from_slice(&encoded);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i32_sort_order() {
        let neg = encode_sortable(&Value::Int32(-1));
        let zero = encode_sortable(&Value::Int32(0));
        let pos = encode_sortable(&Value::Int32(1));
        let big = encode_sortable(&Value::Int32(1000));

        assert!(neg < zero);
        assert!(zero < pos);
        assert!(pos < big);
    }

    #[test]
    fn test_i64_sort_order() {
        let values = [-100i64, -1, 0, 1, 10, 100, 1000];
        let encoded: Vec<_> = values
            .iter()
            .map(|v| encode_sortable(&Value::Int64(*v)))
            .collect();

        for i in 1..encoded.len() {
            assert!(
                encoded[i - 1] < encoded[i],
                "{} should sort before {}",
                values[i - 1],
                values[i]
            );
        }
    }

    #[test]
    fn test_string_sort_order() {
        let a = encode_sortable(&Value::String("apple".to_string()));
        let b = encode_sortable(&Value::String("banana".to_string()));
        let c = encode_sortable(&Value::String("cherry".to_string()));

        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn test_null_sorts_first() {
        let null = encode_sortable(&Value::Null);
        let zero = encode_sortable(&Value::Int32(0));
        let string = encode_sortable(&Value::String("".to_string()));

        assert!(null < zero);
        assert!(null < string);
    }

    #[test]
    fn test_numeric_vs_string_values() {
        // Numeric encoding should sort 1, 2, 10 correctly
        let one = encode_sortable(&Value::Int32(1));
        let two = encode_sortable(&Value::Int32(2));
        let ten = encode_sortable(&Value::Int32(10));

        assert!(one < two);
        assert!(two < ten);
    }
}

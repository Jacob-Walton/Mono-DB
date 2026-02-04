//! Value bridge between MonoDB values and WIT values

use anyhow::{Result, anyhow};
use monodb_common::Value as MonoValue;

use crate::bindings::monodb::plugin::types::Value as WitValue;

pub struct ValueBridge;

impl ValueBridge {
    /// Convert MonoDB Value to WIT Value (serialized bytes)
    pub fn to_wit(mono: &MonoValue) -> Result<WitValue> {
        Ok(mono.to_bytes())
    }

    /// Convert WIT Value (serialized bytes) to MonoDB Value
    pub fn from_wit(wit: &WitValue) -> Result<MonoValue> {
        let (value, _) = MonoValue::from_bytes(wit)
            .map_err(|e| anyhow!("Failed to deserialize value: {}", e))?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_primitives() {
        let values = vec![
            MonoValue::Null,
            MonoValue::Bool(true),
            MonoValue::Bool(false),
            MonoValue::Int32(42),
            MonoValue::Int32(-42),
            MonoValue::Int64(12345678901234),
            MonoValue::Float32(std::f32::consts::PI),
            MonoValue::Float64(std::f64::consts::E),
            MonoValue::String("test".into()),
            MonoValue::String("".into()),
            MonoValue::Binary(vec![1, 2, 3, 4]),
            MonoValue::Binary(vec![]),
        ];

        for value in values {
            let wit = ValueBridge::to_wit(&value).unwrap();
            let back = ValueBridge::from_wit(&wit).unwrap();
            assert_eq!(value, back, "Failed roundtrip for {:?}", value);
        }
    }

    #[test]
    fn test_roundtrip_collections() {
        let value = MonoValue::Array(vec![
            MonoValue::Int32(1),
            MonoValue::String("test".into()),
            MonoValue::Array(vec![MonoValue::Bool(true), MonoValue::Null]),
        ]);

        let wit = ValueBridge::to_wit(&value).unwrap();
        let back = ValueBridge::from_wit(&wit).unwrap();
        assert_eq!(value, back);
    }

    #[test]
    fn test_roundtrip_object() {
        use std::collections::BTreeMap;
        let mut map = BTreeMap::new();
        map.insert("name".to_string(), MonoValue::String("Alice".into()));
        map.insert("age".to_string(), MonoValue::Int32(25));
        map.insert("active".to_string(), MonoValue::Bool(true));

        let value = MonoValue::Object(map);
        let wit = ValueBridge::to_wit(&value).unwrap();
        let back = ValueBridge::from_wit(&wit).unwrap();
        assert_eq!(value, back);
    }
}

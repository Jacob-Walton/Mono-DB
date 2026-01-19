use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    ops::Index,
    sync::{OnceLock, atomic::AtomicU32},
};

use base64::Engine;
use chrono::{Datelike, Timelike};
use indexmap::IndexMap;
use rand::{TryRngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use uuid::Uuid;

use crate::{MonoError, Result};

/// Enum representing a type of value
///
/// Variants:
/// - Primitive types: Null, Bool, Int32, Int64, Float32, Float64, String, Binary
/// - Date/Time types: DateTime, Date, Time
/// - Identifiers: Uuid, ObjectId
/// - Collection types: Array, Object (Document/JSON), Set
/// - Special types:
///     - Row (SQL row)
///     - SortedSet (Redis ZSET)
///     - GeoPoint (Geospatial)
///     - Reference (foreign key, document reference)
#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueType {
    // Primitive types
    Null,
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,

    // Date/Time types
    DateTime,
    Date,
    Time,

    // Identifiers
    Uuid,
    ObjectId,

    // Collection types
    Array,
    Object,
    Set,

    // Special types
    Row,
    SortedSet,
    GeoPoint,

    // Reference
    Reference,

    // Plugin-defined extension
    Extension,
}

impl ValueType {
    /// Returns the common supertype for type coercion between two types.
    ///
    /// Type coercion follows these rules:
    /// - Numeric promotion: Int32 -> Int64 -> Float64, Float32 -> Float64
    /// - Null is compatible with any type (returns the non-null type)
    /// - Extension types only coerce with the same extension type (handled separately)
    /// - Same types always coerce to themselves
    /// - Incompatible types return None
    ///
    /// # Example
    /// ```rust
    /// use monodb_common::ValueType;
    ///
    /// assert_eq!(ValueType::Int32.common_type(&ValueType::Int64), Some(ValueType::Int64));
    /// assert_eq!(ValueType::Int32.common_type(&ValueType::Float64), Some(ValueType::Float64));
    /// assert_eq!(ValueType::String.common_type(&ValueType::Int32), None);
    /// ```
    pub fn common_type(&self, other: &ValueType) -> Option<ValueType> {
        use ValueType::*;

        // Same type always works
        if self == other {
            return Some(self.clone());
        }

        // Null is compatible with anything
        match (self, other) {
            (Null, other) | (other, Null) => return Some(other.clone()),
            _ => {}
        }

        // Numeric promotion ladder
        match (self, other) {
            // Int32 promotes to Int64
            (Int32, Int64) | (Int64, Int32) => Some(Int64),

            // Int32 promotes to Float32
            (Int32, Float32) | (Float32, Int32) => Some(Float32),

            // Int32 promotes to Float64
            (Int32, Float64) | (Float64, Int32) => Some(Float64),

            // Int64 promotes to Float64
            (Int64, Float64) | (Float64, Int64) => Some(Float64),

            // Float32 promotes to Float64
            (Float32, Float64) | (Float64, Float32) => Some(Float64),

            // Int64 and Float32 both promote to Float64
            (Int64, Float32) | (Float32, Int64) => Some(Float64),

            // Incompatible types
            _ => None,
        }
    }

    /// Check if this type can be coerced to the target type.
    ///
    /// A type can be coerced to another if [`common_type`](Self::common_type) returns the target type.
    pub fn can_coerce_to(&self, target: &ValueType) -> bool {
        match self.common_type(target) {
            Some(common) => common == *target,
            None => false,
        }
    }

    /// Check if this type is a numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            ValueType::Int32 | ValueType::Int64 | ValueType::Float32 | ValueType::Float64
        )
    }

    /// Check if this type is an integer type.
    pub fn is_integer(&self) -> bool {
        matches!(self, ValueType::Int32 | ValueType::Int64)
    }

    /// Check if this type is a floating-point type.
    pub fn is_float(&self) -> bool {
        matches!(self, ValueType::Float32 | ValueType::Float64)
    }

    /// Check if this type supports ordering/comparison operations.
    pub fn is_comparable(&self) -> bool {
        matches!(
            self,
            ValueType::Int32
                | ValueType::Int64
                | ValueType::Float32
                | ValueType::Float64
                | ValueType::String
                | ValueType::DateTime
                | ValueType::Date
                | ValueType::Time
        )
    }

    /// Check if this type supports equality comparison.
    pub fn is_equatable(&self) -> bool {
        // All types except Extension support equality
        // Extension requires same type_name check at runtime
        !matches!(self, ValueType::Extension)
    }

    /// Check if this type is a collection type.
    pub fn is_collection(&self) -> bool {
        matches!(
            self,
            ValueType::Array
                | ValueType::Object
                | ValueType::Set
                | ValueType::Row
                | ValueType::SortedSet
        )
    }

    /// Check if this type is a temporal type.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            ValueType::DateTime | ValueType::Date | ValueType::Time
        )
    }

    /// Returns a user-friendly display name for this type.
    pub fn display_name(&self) -> &'static str {
        match self {
            ValueType::Null => "Null",
            ValueType::Bool => "Bool",
            ValueType::Int32 => "Int32",
            ValueType::Int64 => "Int64",
            ValueType::Float32 => "Float32",
            ValueType::Float64 => "Float64",
            ValueType::String => "String",
            ValueType::Binary => "Binary",
            ValueType::DateTime => "DateTime",
            ValueType::Date => "Date",
            ValueType::Time => "Time",
            ValueType::Uuid => "Uuid",
            ValueType::ObjectId => "ObjectId",
            ValueType::Array => "Array",
            ValueType::Object => "Object",
            ValueType::Set => "Set",
            ValueType::Row => "Row",
            ValueType::SortedSet => "SortedSet",
            ValueType::GeoPoint => "GeoPoint",
            ValueType::Reference => "Reference",
            ValueType::Extension => "Extension",
        }
    }
}

/// Universal value type for MonoDB
///
/// Variants
/// * Primitive types: Null, Bool, Int32, Int64, Float32, Float64, String
/// * Date/Time types: DateTime, Date, Time
/// * Identifiers: Uuid, ObjectId
/// * Collection types: Array, Object (Document/JSON), Set
/// * Special types
///     - Row: Represents a database row with named fields.
///     - SortedSet: Represents a sorted set of unique values.
///     - GeoPoint: Represents a geographical point with latitude and longitude.
///     - Reference: Represents a reference to another value (e.g., foreign key, document reference).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    // Primitive types
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),

    // Date/Time types
    DateTime(chrono::DateTime<chrono::FixedOffset>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),

    // Identifiers
    Uuid(uuid::Uuid),
    ObjectId(ObjectId),

    // Collection types
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
    Set(HashSet<String>),

    // Special types
    Row(IndexMap<String, Value>),
    SortedSet(Vec<(f64, String)>),
    GeoPoint {
        lat: f64,
        lng: f64,
    },

    // Reference type
    Reference {
        collection: String,
        id: Box<Value>,
    },

    // Plugin-defined extension type
    Extension {
        type_name: String,
        plugin_id: String,
        data: Vec<u8>,
    },
}

impl Value {
    /// Get the type name as a string
    ///
    /// Returns a [`Cow<'_, str>`] representing the type name.
    ///
    /// # Example
    /// ```rust
    /// use monodb_common::Value;
    ///
    /// let val = Value::Int32(42);
    /// assert_eq!(val.type_name(), "int32");
    ///
    /// let val = Value::String("Hello".to_string());
    /// assert_eq!(val.type_name(), "string");
    ///
    /// let val = Value::Array(vec![Value::Int32(1), Value::Int32(2)]);
    /// assert_eq!(val.type_name(), "array");
    /// ```
    pub fn type_name(&self) -> Cow<'_, str> {
        match self {
            Value::Null => Cow::Borrowed("null"),
            Value::Bool(_) => Cow::Borrowed("bool"),
            Value::Int32(_) => Cow::Borrowed("int32"),
            Value::Int64(_) => Cow::Borrowed("int64"),
            Value::Float32(_) => Cow::Borrowed("float32"),
            Value::Float64(_) => Cow::Borrowed("float64"),
            Value::String(_) => Cow::Borrowed("string"),
            Value::Binary(_) => Cow::Borrowed("binary"),
            Value::DateTime(_) => Cow::Borrowed("datetime"),
            Value::Date(_) => Cow::Borrowed("date"),
            Value::Time(_) => Cow::Borrowed("time"),
            Value::Uuid(_) => Cow::Borrowed("uuid"),
            Value::ObjectId(_) => Cow::Borrowed("objectid"),
            Value::Array(_) => Cow::Borrowed("array"),
            Value::Object(_) => Cow::Borrowed("object"),
            Value::Set(_) => Cow::Borrowed("set"),
            Value::Row(_) => Cow::Borrowed("row"),
            Value::SortedSet(_) => Cow::Borrowed("sortedset"),
            Value::GeoPoint { .. } => Cow::Borrowed("geopoint"),
            Value::Reference { .. } => Cow::Borrowed("reference"),
            Value::Extension { type_name, .. } => Cow::Borrowed(type_name.as_str()),
        }
    }

    /// Get the corresponding ValueType for this Value
    ///
    /// # Example
    ///
    /// TODO
    pub fn data_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Bool(_) => ValueType::Bool,
            Value::Int32(_) => ValueType::Int32,
            Value::Int64(_) => ValueType::Int64,
            Value::Float32(_) => ValueType::Float32,
            Value::Float64(_) => ValueType::Float64,
            Value::String(_) => ValueType::String,
            Value::Binary(_) => ValueType::Binary,
            Value::DateTime(_) => ValueType::DateTime,
            Value::Date(_) => ValueType::Date,
            Value::Time(_) => ValueType::Time,
            Value::Uuid(_) => ValueType::Uuid,
            Value::ObjectId(_) => ValueType::ObjectId,
            Value::Array(_) => ValueType::Array,
            Value::Object(_) => ValueType::Object,
            Value::Set(_) => ValueType::Set,
            Value::Row(_) => ValueType::Row,
            Value::SortedSet(_) => ValueType::SortedSet,
            Value::GeoPoint { .. } => ValueType::GeoPoint,
            Value::Reference { .. } => ValueType::Reference,
            Value::Extension { .. } => ValueType::Extension,
        }
    }

    /// Attempt to coerce this value to the target type.
    ///
    /// Follows the type coercion rules defined in [`ValueType::common_type`].
    /// Returns coerced [`Value`] on success, or a [`MonoError::TypeError`]
    /// if coercion is not possible.
    ///
    /// # Example
    /// ```rust
    /// use monodb_common::{Value, ValueType};
    ///
    /// let val = Value::Int32(42);
    /// let coerced = val.coerce_to(&ValueType::Int64).unwrap();
    /// assert_eq!(coerced, Value::Int64(42));
    ///
    /// let val = Value::Int32(42);
    /// let coerced = val.coerce_to(&ValueType::Float64).unwrap();
    /// assert_eq!(coerced, Value::Float64(42.0));
    /// ```
    pub fn coerce_to(&self, target: &ValueType) -> Result<Value> {
        let source_type = self.data_type();

        // Same type, no coercion needed
        if source_type == *target {
            return Ok(self.clone());
        }

        // Null coerces to any type as Null
        if source_type == ValueType::Null {
            return Ok(Value::Null);
        }

        match (self, target) {
            // Int32 promotions
            (Value::Int32(v), ValueType::Int64) => Ok(Value::Int64(*v as i64)),
            (Value::Int32(v), ValueType::Float32) => Ok(Value::Float32(*v as f32)),
            (Value::Int32(v), ValueType::Float64) => Ok(Value::Float64(*v as f64)),

            // Int64 promotions
            (Value::Int64(v), ValueType::Float64) => Ok(Value::Float64(*v as f64)),

            // Float32 promotions
            (Value::Float32(v), ValueType::Float64) => Ok(Value::Float64(*v as f64)),

            // Int64 to Float32
            (Value::Int64(v), ValueType::Float32) => Ok(Value::Float32(*v as f32)),

            // Float32 to Int64/Int32 (truncation)
            (Value::Float32(v), ValueType::Int64) => Ok(Value::Int64(*v as i64)),
            (Value::Float32(v), ValueType::Int32) => Ok(Value::Int32(*v as i32)),

            // Float64 to narrower types (truncation/precision loss)
            (Value::Float64(v), ValueType::Int64) => Ok(Value::Int64(*v as i64)),
            (Value::Float64(v), ValueType::Int32) => Ok(Value::Int32(*v as i32)),
            (Value::Float64(v), ValueType::Float32) => Ok(Value::Float32(*v as f32)),

            // Int64 to Int32 (narrowing)
            (Value::Int64(v), ValueType::Int32) => {
                if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                    Ok(Value::Int32(*v as i32))
                } else {
                    Err(MonoError::TypeError {
                        expected: "Int32 range".into(),
                        actual: format!("Int64 value {} out of range", v),
                    })
                }
            }

            // Incompatible types
            _ => Err(MonoError::TypeError {
                expected: target.display_name().into(),
                actual: source_type.display_name().into(),
            }),
        }
    }

    /// Convert Value to JSON representation
    ///
    /// # Example
    /// ```rust
    /// use monodb_common::Value;
    ///
    /// let val = Value::Int32(42);
    /// let json = val.to_json();
    /// assert_eq!(json, serde_json::json!(42));
    ///
    /// let val = Value::String("Hello".to_string());
    /// let json = val.to_json();
    /// assert_eq!(json, serde_json::json!("Hello"));
    /// ```
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int32(i) => serde_json::Value::Number((*i).into()),
            Value::Int64(i) => serde_json::Value::Number((*i).into()),
            Value::Float32(f) => serde_json::Number::from_f64(*f as f64)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            Value::Float64(f) => serde_json::Number::from_f64(*f)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Binary(b) => serde_json::Value::String(format!(
                "b64:{}",
                base64::engine::general_purpose::STANDARD.encode(b)
            )),
            Value::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
            Value::Date(d) => serde_json::Value::String(d.to_string()),
            Value::Time(t) => serde_json::Value::String(t.to_string()),
            Value::Uuid(u) => serde_json::Value::String(u.to_string()),
            Value::ObjectId(oid) => serde_json::Value::String(oid.to_string()),
            Value::Array(arr) => {
                let json_arr: Vec<serde_json::Value> = arr.iter().map(|v| v.to_json()).collect();
                serde_json::Value::Array(json_arr)
            }
            Value::Object(obj) => {
                let json_obj: serde_json::Map<String, serde_json::Value> =
                    obj.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                serde_json::Value::Object(json_obj)
            }
            Value::Set(set) => {
                let json_arr: Vec<serde_json::Value> = set
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect();
                serde_json::Value::Array(json_arr)
            }
            Value::Row(row) => {
                let json_obj: serde_json::Map<String, serde_json::Value> =
                    row.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                serde_json::Value::Object(json_obj)
            }
            Value::SortedSet(ss) => {
                let json_arr: Vec<serde_json::Value> = ss
                    .iter()
                    .map(|(score, val)| {
                        let mut obj = serde_json::Map::new();
                        obj.insert(
                            "score".to_string(),
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(*score)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                        );
                        obj.insert("value".to_string(), serde_json::Value::String(val.clone()));
                        serde_json::Value::Object(obj)
                    })
                    .collect();
                serde_json::Value::Array(json_arr)
            }
            Value::GeoPoint { lat, lng } => {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "lat".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(*lat).unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                obj.insert(
                    "lng".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(*lng).unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                serde_json::Value::Object(obj)
            }
            Value::Reference { collection, id } => {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "collection".to_string(),
                    serde_json::Value::String(collection.clone()),
                );
                obj.insert("id".to_string(), id.to_json());
                obj.insert("id".to_string(), id.to_json());
                serde_json::Value::Object(obj)
            }
            Value::Extension {
                type_name,
                plugin_id,
                data,
            } => {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "$type".to_string(),
                    serde_json::Value::String(type_name.clone()),
                );
                obj.insert(
                    "$plugin".to_string(),
                    serde_json::Value::String(plugin_id.clone()),
                );
                obj.insert(
                    "$data".to_string(),
                    serde_json::Value::String(
                        base64::engine::general_purpose::STANDARD.encode(data),
                    ),
                );
                serde_json::Value::Object(obj)
            }
        }
    }

    /// Convert from JSON representation
    ///
    /// # Example
    /// ```rust
    /// use monodb_common::Value;
    /// use serde_json::json;
    ///
    /// let val = Value::from_json(json!({"key": "value"}));
    /// assert_eq!(val, Value::Object({let mut map = std::collections::BTreeMap::new(); map.insert("key".into(), Value::String("value".into())); map}));
    /// ```
    pub fn from_json(json: serde_json::Value) -> Self {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        Value::Int32(i as i32)
                    } else {
                        Value::Int64(i)
                    }
                } else if let Some(f) = n.as_f64() {
                    Value::Float64(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.into_iter().map(Value::from_json).collect())
            }
            serde_json::Value::Object(map) => {
                let obj: BTreeMap<String, Value> = map
                    .into_iter()
                    .map(|(k, v)| (k, Value::from_json(v)))
                    .collect();
                Value::Object(obj)
            }
        }
    }

    /// Extract the array from Value::Array, returning None if not an array
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Extract the array from Value::Array, consuming the value
    pub fn into_array(self) -> Option<Vec<Value>> {
        match self {
            Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Extract the string from Value::String, returning None if not a string
    pub fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Extract the string from Value::String, consuming the value
    pub fn into_string(self) -> Option<String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Extract the integer from Value::Int64, returning None if not an int
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(i) => Some(*i),
            _ => None,
        }
    }

    /// Extract the integer from Value::Int32, returning None if not an int
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Int32(i) => Some(*i),
            _ => None,
        }
    }

    /// Extract the float from Value::Float64, returning None if not a float
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(f) => Some(*f),
            _ => None,
        }
    }

    /// Extract the float from Value::Float32, returning None if not a float
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Value::Float32(f) => Some(*f),
            _ => None,
        }
    }

    /// Extract the boolean from Value::Bool, returning None if not a boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Extract the object from Value::Object, returning None if not an object
    pub fn as_object(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Object(obj) => Some(obj),
            _ => None,
        }
    }

    /// Extract the object from Value::Object, consuming the value
    pub fn into_object(self) -> Option<BTreeMap<String, Value>> {
        match self {
            Value::Object(obj) => Some(obj),
            _ => None,
        }
    }

    /// Extract the binary data from Value::Binary, returning None if not binary
    pub fn as_binary(&self) -> Option<&Vec<u8>> {
        match self {
            Value::Binary(data) => Some(data),
            _ => None,
        }
    }

    /// Extract the binary data from Value::Binary, consuming the value
    pub fn into_binary(self) -> Option<Vec<u8>> {
        match self {
            Value::Binary(data) => Some(data),
            _ => None,
        }
    }

    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    #[inline]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.write_to(&mut out);

        out
    }

    #[inline]
    pub fn from_bytes(buf: &[u8]) -> crate::Result<(Value, usize)> {
        if buf.is_empty() {
            return Err(MonoError::Parse("Empty buffer".into()));
        }

        let kind = buf[0];
        let mut offset = 1;

        macro_rules! need {
            ($n:expr) => {
                if buf.len() < offset + $n {
                    return Err(MonoError::Parse("Unexpected EOF".into()));
                }
            };
        }

        macro_rules! read_u32 {
            () => {{
                need!(4);
                let v = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
                offset += 4;
                v
            }};
        }

        macro_rules! read_u64 {
            () => {{
                need!(8);
                let v = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
                offset += 8;
                v
            }};
        }

        macro_rules! read_i32 {
            () => {{
                need!(4);
                let v = i32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
                offset += 4;
                v
            }};
        }

        macro_rules! read_i64 {
            () => {{
                need!(8);
                let v = i64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
                offset += 8;
                v
            }};
        }

        macro_rules! read_f32 {
            () => {{ f32::from_bits(read_u32!()) }};
        }

        macro_rules! read_f64 {
            () => {{ f64::from_bits(read_u64!()) }};
        }

        macro_rules! read_string {
            () => {{
                let len = read_u32!() as usize;
                need!(len);
                let s = std::str::from_utf8(&buf[offset..offset + len])
                    .map_err(|e| MonoError::Parse(format!("utf8 error: {e}")))?;
                offset += len;
                s.to_owned()
            }};
        }

        macro_rules! read_bytes {
            () => {{
                let len = read_u32!() as usize;
                need!(len);
                let v = buf[offset..offset + len].to_vec();
                offset += len;
                v
            }};
        }

        let value = match kind {
            0 => Value::Null,

            1 => {
                need!(1);
                let b = buf[offset] != 0;
                offset += 1;
                Value::Bool(b)
            }

            2 => Value::Int32(read_i32!()),

            3 => Value::Int64(read_i64!()),

            4 => Value::Float32(read_f32!()),

            5 => Value::Float64(read_f64!()),

            6 => Value::String(read_string!()),

            7 => Value::Binary(read_bytes!()),

            8 => {
                let micros = read_i64!();
                let offset_minutes = read_i32!();
                let secs = micros / 1_000_000;
                let nsecs = ((micros % 1_000_000) * 1000) as u32;
                let offset = chrono::FixedOffset::east_opt(offset_minutes * 60)
                    .ok_or_else(|| MonoError::Parse("Invalid offset".into()))?;
                let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                    .ok_or_else(|| MonoError::Parse("Invalid timestamp".into()))?
                    .with_timezone(&offset);
                Value::DateTime(dt)
            }

            9 => {
                let year = read_i32!();
                need!(2);
                let month = buf[offset];
                offset += 1;
                let day = buf[offset];
                offset += 1;

                let date = chrono::NaiveDate::from_ymd_opt(year, month as u32, day as u32)
                    .ok_or_else(|| MonoError::Parse("Invalid date".into()))?;

                Value::Date(date)
            }

            10 => {
                need!(3);
                let hour = buf[offset];
                offset += 1;
                let minute = buf[offset];
                offset += 1;
                let second = buf[offset];
                offset += 1;
                let micros = read_u32!();

                let t = chrono::NaiveTime::from_hms_micro_opt(
                    hour as u32,
                    minute as u32,
                    second as u32,
                    micros,
                )
                .ok_or_else(|| MonoError::Parse("Invalid time".into()))?;

                Value::Time(t)
            }

            11 => {
                need!(16);
                let mut b = [0u8; 16];
                b.copy_from_slice(&buf[offset..offset + 16]);
                offset += 16;
                Value::Uuid(uuid::Uuid::from_bytes(b))
            }

            12 => {
                need!(12);
                let mut b = [0u8; 12];
                b.copy_from_slice(&buf[offset..offset + 12]);
                offset += 12;
                Value::ObjectId(ObjectId::from_bytes(b))
            }

            13 => {
                let len = read_u32!() as usize;
                let mut v = Vec::with_capacity(len);

                for _ in 0..len {
                    let (item, used) = Value::from_bytes(&buf[offset..])?;
                    offset += used;
                    v.push(item);
                }

                Value::Array(v)
            }

            14 => {
                let len = read_u32!() as usize;
                let mut map = BTreeMap::new();

                for _ in 0..len {
                    let key = read_string!();
                    let (val, used) = Value::from_bytes(&buf[offset..])?;
                    offset += used;
                    map.insert(key, val);
                }

                Value::Object(map)
            }

            15 => {
                let len = read_u32!() as usize;
                let mut set = HashSet::new();

                for _ in 0..len {
                    let s = read_string!();
                    set.insert(s);
                }

                Value::Set(set)
            }

            16 => {
                let len = read_u32!() as usize;
                let mut row = IndexMap::new();

                for _ in 0..len {
                    let key = read_string!();
                    let (val, used) = Value::from_bytes(&buf[offset..])?;
                    offset += used;
                    row.insert(key, val);
                }

                Value::Row(row)
            }

            17 => {
                let len = read_u32!() as usize;
                let mut v = Vec::with_capacity(len);

                for _ in 0..len {
                    let score = read_f64!();
                    let member = read_string!();
                    v.push((score, member));
                }

                Value::SortedSet(v)
            }

            18 => {
                let lat = read_f64!();
                let lng = read_f64!();
                Value::GeoPoint { lat, lng }
            }

            19 => {
                let collection = read_string!();
                let (id, used) = Value::from_bytes(&buf[offset..])?;
                offset += used;

                Value::Reference {
                    collection,
                    id: Box::new(id),
                }
            }

            20 => {
                let type_name = read_string!();
                let plugin_id = read_string!();
                let data = read_bytes!();

                Value::Extension {
                    type_name,
                    plugin_id,
                    data,
                }
            }

            _ => {
                return Err(MonoError::Parse(format!("Unknown Value tag: {kind}")));
            }
        };

        Ok((value, offset))
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(0),

            Value::Bool(b) => {
                out.push(1);
                out.push(*b as u8);
            }

            Value::Int32(i) => {
                out.push(2);
                out.extend_from_slice(&i.to_le_bytes());
            }

            Value::Int64(i) => {
                out.push(3);
                out.extend_from_slice(&i.to_le_bytes());
            }

            Value::Float32(f) => {
                out.push(4);
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }

            Value::Float64(f) => {
                out.push(5);
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }

            Value::String(s) => {
                out.push(6);
                let b = s.as_bytes();
                let len = b.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(b);
            }

            Value::Binary(b) => {
                out.push(7);
                let len = b.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(b);
            }

            Value::DateTime(dt) => {
                out.push(8);
                let unix_micros = dt.timestamp_micros();
                let offset_minutes = dt.offset().local_minus_utc() / 60;
                out.extend_from_slice(&unix_micros.to_le_bytes());
                out.extend_from_slice(&offset_minutes.to_le_bytes());
            }

            Value::Date(d) => {
                out.push(9);
                out.extend_from_slice(&d.year().to_le_bytes());
                out.push(d.month() as u8);
                out.push(d.day() as u8);
            }

            Value::Time(t) => {
                out.push(10);
                out.push(t.hour() as u8);
                out.push(t.minute() as u8);
                out.push(t.second() as u8);
                let micros = t.nanosecond() / 1000;
                out.extend_from_slice(&micros.to_le_bytes());
            }

            Value::Uuid(u) => {
                out.push(11);
                out.extend_from_slice(u.as_bytes());
            }

            Value::ObjectId(oid) => {
                out.push(12);
                out.extend_from_slice(&oid.bytes());
            }

            Value::Array(arr) => {
                out.push(13);
                out.extend_from_slice(&(arr.len() as u32).to_le_bytes());
                for v in arr {
                    v.write_to(out);
                }
            }

            Value::Object(map) => {
                out.push(14);
                out.extend_from_slice(&(map.len() as u32).to_le_bytes());
                for (k, v) in map {
                    let kb = k.as_bytes();
                    out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    out.extend_from_slice(kb);
                    v.write_to(out);
                }
            }

            Value::Set(set) => {
                out.push(15);
                out.extend_from_slice(&(set.len() as u32).to_le_bytes());
                for item in set {
                    let b = item.as_bytes();
                    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    out.extend_from_slice(b);
                }
            }

            Value::Row(row) => {
                out.push(16);
                out.extend_from_slice(&(row.len() as u32).to_le_bytes());
                for (k, v) in row {
                    let kb = k.as_bytes();
                    out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    out.extend_from_slice(kb);
                    v.write_to(out);
                }
            }

            Value::SortedSet(items) => {
                out.push(17);
                out.extend_from_slice(&(items.len() as u32).to_le_bytes());
                for (score, member) in items {
                    out.extend_from_slice(&score.to_bits().to_le_bytes());
                    let mb = member.as_bytes();
                    out.extend_from_slice(&(mb.len() as u32).to_le_bytes());
                    out.extend_from_slice(mb);
                }
            }

            Value::GeoPoint { lat, lng } => {
                out.push(18);
                out.extend_from_slice(&lat.to_bits().to_le_bytes());
                out.extend_from_slice(&lng.to_bits().to_le_bytes());
            }

            Value::Reference { collection, id } => {
                out.push(19);

                // collection : String
                let cb = collection.as_bytes();
                out.extend_from_slice(&(cb.len() as u32).to_le_bytes());
                out.extend_from_slice(cb);

                // id : Value
                id.write_to(out);
            }

            Value::Extension {
                type_name,
                plugin_id,
                data,
            } => {
                out.push(20);

                // type_name : String
                let tb = type_name.as_bytes();
                out.extend_from_slice(&(tb.len() as u32).to_le_bytes());
                out.extend_from_slice(tb);

                // plugin_id : String
                let pb = plugin_id.as_bytes();
                out.extend_from_slice(&(pb.len() as u32).to_le_bytes());
                out.extend_from_slice(pb);

                // data : Vec<u8>
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(data);
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Bool(_) => 2,

            Value::Int32(_) => 5, // 1 + 4
            Value::Int64(_) => 9, // 1 + 8

            Value::Float32(_) => 5, // 1 + 4
            Value::Float64(_) => 9, // 1 + 8

            Value::String(s) => 1 + 4 + s.len(),
            Value::Binary(b) => 1 + 4 + b.len(),

            Value::DateTime(_) => 13, // 1 + 8 + 4
            Value::Date(_) => 7,      // 1 + 4 + 1 + 1
            Value::Time(_) => 8,      // 1 + 1 + 1 + 1 + 4

            Value::Uuid(_) => 17,     // 1 + 16
            Value::ObjectId(_) => 13, // 1 + 12

            Value::Array(arr) => 1 + 4 + arr.iter().map(|v| v.encoded_len()).sum::<usize>(),

            Value::Object(map) => {
                1 + 4
                    + map
                        .iter()
                        .map(|(k, v)| 4 + k.len() + v.encoded_len())
                        .sum::<usize>()
            }

            Value::Set(set) => 1 + 4 + set.iter().map(|s| 4 + s.len()).sum::<usize>(),

            Value::Row(row) => {
                1 + 4
                    + row
                        .iter()
                        .map(|(k, v)| 4 + k.len() + v.encoded_len())
                        .sum::<usize>()
            }

            Value::SortedSet(items) => {
                1 + 4
                    + items
                        .iter()
                        .map(|(_, member)| 8 + 4 + member.len())
                        .sum::<usize>()
            }

            Value::GeoPoint { .. } => 1 + 8 + 8,

            Value::Reference { collection, id } => 1 + (4 + collection.len()) + id.encoded_len(),

            Value::Extension {
                type_name,
                plugin_id,
                data,
            } => 1 + (4 + type_name.len()) + (4 + plugin_id.len()) + (4 + data.len()),
        }
    }
}

impl Index<&str> for Value {
    type Output = Value;

    fn index(&self, key: &str) -> &Self::Output {
        match self {
            Value::Object(map) => map.get(key).unwrap_or(&Value::Null),
            Value::Row(map) => map.get(key).unwrap_or(&Value::Null),
            _ => panic!("Cannot index non-object value with string key"),
        }
    }
}

impl Index<usize> for Value {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Value::Array(arr) => arr.get(index).unwrap_or(&Value::Null),
            _ => panic!("Cannot index non-array value with usize"),
        }
    }
}

/// Implement addition operations for Value
impl std::ops::Add for Value {
    type Output = Result<Value>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // Int32 + Int32
            (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a + b)),
            // Int64 + Int64
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a + b)),
            // Float32 + Float32
            (Value::Float32(a), Value::Float32(b)) => Ok(Value::Float32(a + b)),
            // Float64 + Float64
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a + b)),
            // Int32 + Int64
            (Value::Int32(a), Value::Int64(b)) => Ok(Value::Int64(a as i64 + b)),
            // Int64 + Int32
            (Value::Int64(a), Value::Int32(b)) => Ok(Value::Int64(a + b as i64)),
            // Float32 + Float64
            (Value::Float32(a), Value::Float64(b)) => Ok(Value::Float64(a as f64 + b)),
            // Float64 + Float32
            (Value::Float64(a), Value::Float32(b)) => Ok(Value::Float64(a + b as f64)),
            // String + String (concatenation)
            (Value::String(a), Value::String(b)) => Ok(Value::String(a + &b)),
            // Array + Array (concatenation)
            (Value::Array(mut a), Value::Array(b)) => {
                a.extend(b);
                Ok(Value::Array(a))
            }
            // Attempt to coerce types for addition
            (a, b) => {
                let a_str = a.to_string();
                let b_str = b.to_string();

                if let (Ok(a_int), Ok(b_int)) = (a_str.parse::<i64>(), b_str.parse::<i64>()) {
                    return Ok(Value::Int64(a_int + b_int));
                }
                if let (Ok(a_float), Ok(b_float)) = (a_str.parse::<f64>(), b_str.parse::<f64>()) {
                    return Ok(Value::Float64(a_float + b_float));
                }
                Err(MonoError::TypeError {
                    expected: format!(
                        "compatible types for addition, got {} and {}",
                        a.type_name(),
                        b.type_name()
                    ),
                    actual: format!("{} and {}", a.type_name(), b.type_name()),
                })
            }
        }
    }
}

/// Implement subtraction operations for Value
impl std::ops::Sub for Value {
    type Output = Result<Value>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // Int32 - Int32
            (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a - b)),
            // Int64 - Int64
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
            // Float32 - Float32
            (Value::Float32(a), Value::Float32(b)) => Ok(Value::Float32(a - b)),
            // Float64 - Float64
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a - b)),
            // Int32 - Int64
            (Value::Int32(a), Value::Int64(b)) => Ok(Value::Int64(a as i64 - b)),
            // Int64 - Int32
            (Value::Int64(a), Value::Int32(b)) => Ok(Value::Int64(a - b as i64)),
            // Float32 - Float64
            (Value::Float32(a), Value::Float64(b)) => Ok(Value::Float64(a as f64 - b)),
            // Float64 - Float32
            (Value::Float64(a), Value::Float32(b)) => Ok(Value::Float64(a - b as f64)),
            // Unsupported types
            (a, b) => Err(MonoError::TypeError {
                expected: format!(
                    "compatible types for subtraction, got {} and {}",
                    a.type_name(),
                    b.type_name()
                ),
                actual: format!("{} and {}", a.type_name(), b.type_name()),
            }),
        }
    }
}

/// Implement multiplication operations for Value
impl std::ops::Mul for Value {
    type Output = Result<Value>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // Int32 * Int32
            (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a * b)),
            // Int64 * Int64
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),
            // Float32 * Float32
            (Value::Float32(a), Value::Float32(b)) => Ok(Value::Float32(a * b)),
            // Float64 * Float64
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a * b)),
            // Int32 * Int64
            (Value::Int32(a), Value::Int64(b)) => Ok(Value::Int64(a as i64 * b)),
            // Int64 * Int32
            (Value::Int64(a), Value::Int32(b)) => Ok(Value::Int64(a * b as i64)),
            // Float32 * Float64
            (Value::Float32(a), Value::Float64(b)) => Ok(Value::Float64(a as f64 * b)),
            // Float64 * Float32
            (Value::Float64(a), Value::Float32(b)) => Ok(Value::Float64(a * b as f64)),
            // String * Int32 (repeat string)
            (Value::String(s), Value::Int32(n)) | (Value::Int32(n), Value::String(s)) if n >= 0 => {
                Ok(Value::String(s.repeat(n as usize)))
            }
            // String * Int64 (repeat string)
            (Value::String(s), Value::Int64(n)) | (Value::Int64(n), Value::String(s)) if n >= 0 => {
                Ok(Value::String(s.repeat(n as usize)))
            }
            // Unsupported types
            (a, b) => Err(MonoError::TypeError {
                expected: format!(
                    "compatible types for multiplication, got {} and {}",
                    a.type_name(),
                    b.type_name()
                ),
                actual: format!("{} and {}", a.type_name(), b.type_name()),
            }),
        }
    }
}

/// Implement division operations for Value
impl std::ops::Div for Value {
    type Output = Result<Value>;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // Int32 / Int32
            (Value::Int32(a), Value::Int32(b)) => {
                if b == 0 {
                    Err(MonoError::TypeError {
                        expected: "non-zero divisor".into(),
                        actual: "division by zero".into(),
                    })
                } else {
                    Ok(Value::Int32(a / b))
                }
            }
            // Int64 / Int64
            (Value::Int64(a), Value::Int64(b)) => {
                if b == 0 {
                    Err(MonoError::TypeError {
                        expected: "non-zero divisor".into(),
                        actual: "division by zero".into(),
                    })
                } else {
                    Ok(Value::Int64(a / b))
                }
            }
            // Float32 / Float32
            (Value::Float32(a), Value::Float32(b)) => {
                if b == 0.0 {
                    Err(MonoError::TypeError {
                        expected: "non-zero divisor".into(),
                        actual: "division by zero".into(),
                    })
                } else {
                    Ok(Value::Float32(a / b))
                }
            }
            // Float64 / Float64
            (Value::Float64(a), Value::Float64(b)) => {
                if b == 0.0 {
                    Err(MonoError::TypeError {
                        expected: "non-zero divisor".into(),
                        actual: "division by zero".into(),
                    })
                } else {
                    Ok(Value::Float64(a / b))
                }
            }
            // Int32 / Int64
            (Value::Int32(a), Value::Int64(b)) => {
                if b == 0 {
                    Err(MonoError::TypeError {
                        expected: "non-zero divisor".into(),
                        actual: "division by zero".into(),
                    })
                } else {
                    Ok(Value::Int64(a as i64 / b))
                }
            }
            // Int64 / Int32
            (Value::Int64(a), Value::Int32(b)) => {
                if b == 0 {
                    Err(MonoError::TypeError {
                        expected: "non-zero divisor".into(),
                        actual: "division by zero".into(),
                    })
                } else {
                    Ok(Value::Int64(a / b as i64))
                }
            }
            // Float32 / Float64
            (Value::Float32(a), Value::Float64(b)) => {
                if b == 0.0 {
                    Err(MonoError::TypeError {
                        expected: "non-zero divisor".into(),
                        actual: "division by zero".into(),
                    })
                } else {
                    Ok(Value::Float64(a as f64 / b))
                }
            }
            // Float64 / Float32
            (Value::Float64(a), Value::Float32(b)) => {
                if b == 0.0 {
                    Err(MonoError::TypeError {
                        expected: "non-zero divisor".into(),
                        actual: "division by zero".into(),
                    })
                } else {
                    Ok(Value::Float64(a / b as f64))
                }
            }
            // Unsupported types
            (a, b) => Err(MonoError::TypeError {
                expected: format!(
                    "compatible types for division, got {} and {}",
                    a.type_name(),
                    b.type_name()
                ),
                actual: format!("{} and {}", a.type_name(), b.type_name()),
            }),
        }
    }
}

impl std::str::FromStr for Value {
    type Err = MonoError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();

        if s.starts_with('"') && s.ends_with('"') || (s.starts_with('\'') && s.ends_with('\'')) {
            return Ok(Value::String(s[1..s.len() - 1].to_string()));
        }

        // Null/None
        if s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("none") || s.is_empty() {
            return Ok(Value::Null);
        }

        // Boolean
        if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") {
            return Ok(Value::Bool(s.parse().unwrap()));
        }

        // UUID (format: 8-4-4-4-12 hex chars)
        if let Ok(uuid) = Uuid::parse_str(s) {
            return Ok(Value::Uuid(uuid));
        }

        // ObjectId (24 hex chars)
        if s.len() == 24
            && s.chars().all(|c| c.is_ascii_hexdigit())
            && let Ok(oid) = ObjectId::from_hex(s)
        {
            return Ok(Value::ObjectId(oid));
        }

        // DateTime (ISO 8601)
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Ok(Value::DateTime(dt));
        }

        // Date (YYYY-MM-DD)
        if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            return Ok(Value::Date(date));
        }

        // Time (HH:MM:SS)
        if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
            return Ok(Value::Time(time));
        }

        // JSON Array/Object
        if ((s.starts_with('[') && s.ends_with(']')) || (s.starts_with('{') && s.ends_with('}')))
            && let Ok(json_value) = serde_json::from_str(s)
        {
            return Ok(Value::from_json(json_value));
        }

        // Integer
        if let Ok(i) = s.parse::<i32>() {
            return Ok(Value::Int32(i));
        }

        if let Ok(i) = s.parse::<i64>() {
            return Ok(Value::Int64(i));
        }

        // Float - always use Float64 for precision
        if s.contains('.')
            && let Ok(f) = s.parse::<f64>()
        {
            return Ok(Value::Float64(f));
        }

        // Marked types
        if s.starts_with("0x") && s.len() > 2 {
            if let Ok(bytes) = hex::decode(&s[2..]) {
                return Ok(Value::Binary(bytes));
            } else {
                return Err(MonoError::Parse("Invalid hex string".into()));
            }
        }

        // GeoPoint(lat, lng)
        if s.starts_with("GeoPoint(") && s.ends_with(')') {
            let inner = &s[9..s.len() - 1];
            let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
            if parts.len() == 2
                && let (Ok(lat), Ok(lng)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>())
            {
                return Ok(Value::GeoPoint { lat, lng });
            }
            return Err(MonoError::Parse("Invalid GeoPoint format".into()));
        }

        // Set{...}
        if s.starts_with("Set{") && s.ends_with('}') {
            let inner = &s[4..s.len() - 1];
            let items: HashSet<String> = inner
                .split(',')
                .map(|item| {
                    item.trim()
                        .trim_matches(|c| c == '"' || c == '\'')
                        .to_string()
                })
                .collect();
            return Ok(Value::Set(items));
        }

        // Row(...)
        if s.starts_with("Row(") && s.ends_with(')') {
            let inner = &s[4..s.len() - 1];
            let items: Vec<Value> = inner
                .split(',')
                .map(|item| item.trim().parse())
                .collect::<Result<Vec<Value>>>()?;
            return Ok(Value::Row(
                items
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| (i.to_string(), v))
                    .collect(),
            ));
        }

        // SortedSet{(score, "member"), ...}
        if s.starts_with("SortedSet{") && s.ends_with('}') {
            let inner = &s[10..s.len() - 1];
            let mut items = Vec::new();
            for part in inner.split("),") {
                let part = part.trim().trim_start_matches('(').trim_end_matches(')');
                let pair: Vec<&str> = part.splitn(2, ',').map(|p| p.trim()).collect();
                if pair.len() == 2 {
                    if let (Ok(score), member) = (
                        pair[0].parse::<f64>(),
                        pair[1].trim_matches(|c| c == '"' || c == '\''),
                    ) {
                        items.push((score, member.to_string()));
                    } else {
                        return Err(MonoError::Parse("Invalid SortedSet format".into()));
                    }
                } else {
                    return Err(MonoError::Parse("Invalid SortedSet format".into()));
                }
            }
            return Ok(Value::SortedSet(items));
        }

        // Reference(collection, id)
        if s.starts_with("Reference(") && s.ends_with(')') {
            let inner = &s[10..s.len() - 1];
            let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
            if parts.len() == 2 {
                let collection = parts[0].trim_matches(|c| c == '"' || c == '\'').to_string();
                let id = parts[1].trim_matches(|c| c == '"' || c == '\'').to_string();
                return Ok(Value::Reference {
                    collection,
                    id: Box::new(Value::String(id)),
                });
            }
            return Err(MonoError::Parse("Invalid Reference format".into()));
        }

        Err(MonoError::TypeError {
            expected: "a valid type".into(),
            actual: s.into(),
        })
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
            Value::Int32(value as i32)
        } else {
            Value::Int64(value)
        }
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        if value <= i32::MAX as u32 {
            Value::Int32(value as i32)
        } else {
            Value::Int64(value as i64)
        }
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        if value <= i32::MAX as u64 {
            Value::Int32(value as i32)
        } else if value <= i64::MAX as u64 {
            Value::Int64(value as i64)
        } else {
            Value::Float64(value as f64)
        }
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float32(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float64(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int32(i) => write!(f, "{}", i),
            Value::Int64(i) => write!(f, "{}", i),
            Value::Float32(fl) => write!(f, "{}", fl),
            Value::Float64(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Binary(b) => write!(f, "0x{}", hex::encode(b)),
            Value::DateTime(dt) => write!(f, "{}", dt.to_rfc3339()),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Uuid(u) => write!(f, "{}", u),
            Value::ObjectId(oid) => write!(f, "{}", oid),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
                write!(f, "[{}]", items.join(", "))
            }
            Value::Object(obj) => {
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!("\"{}\": {}", k, v))
                    .collect();
                write!(f, "{{{}}}", items.join(", "))
            }
            Value::Set(set) => {
                let items: Vec<String> = set.iter().map(|s| format!("\"{}\"", s)).collect();
                write!(f, "Set{{{}}}", items.join(", "))
            }
            Value::Row(row) => {
                let items: Vec<String> = row
                    .iter()
                    .map(|(k, v)| format!("\"{}\": {}", k, v))
                    .collect();
                write!(f, "Row({})", items.join(", "))
            }
            Value::SortedSet(ss) => {
                let items: Vec<String> = ss
                    .iter()
                    .map(|(score, member)| format!("({}, \"{}\")", score, member))
                    .collect();
                write!(f, "SortedSet{{{}}}", items.join(", "))
            }
            Value::GeoPoint { lat, lng } => write!(f, "GeoPoint({}, {})", lat, lng),
            Value::Reference { collection, id } => {
                write!(f, "Reference(\"{}\", {})", collection, id)
            }
            Value::Extension {
                type_name,
                plugin_id,
                data,
            } => {
                write!(
                    f,
                    "Extension({}, {}, {} bytes)",
                    type_name,
                    plugin_id,
                    data.len()
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId([u8; 12]);

impl ObjectId {
    /// Generate a new ObjectId
    ///
    /// # Example
    /// ```rust
    /// use monodb_common::ObjectId;
    ///
    /// let oid = ObjectId::new().unwrap();
    /// println!("Generated ObjectId: {}", oid);
    /// ```
    pub fn new() -> Result<Self> {
        static MACHINE_BYTES: OnceLock<[u8; 3]> = OnceLock::new();
        static PROCESS_BYTES: OnceLock<[u8; 2]> = OnceLock::new();
        static COUNTER: OnceLock<AtomicU32> = OnceLock::new();

        let mut bytes = [0u8; 12];

        // 4-byte timestamp (big-endian)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| MonoError::Io(format!("System time error: {e}")))?
            .as_secs() as u32;
        bytes[0..4].copy_from_slice(&timestamp.to_be_bytes());

        // 3-byte machine identifier
        let machine_bytes = MACHINE_BYTES.get_or_init(|| {
            let mut hasher = Sha1::new();

            // Try hostname
            if let Ok(hostname) = std::env::var("HOSTNAME") {
                hasher.update(hostname.as_bytes());
            } else if let Ok(hostname) = std::env::var("COMPUTERNAME") {
                hasher.update(hostname.as_bytes());
            } else {
                // Fallback to process ID + preset data
                hasher.update(std::process::id().to_be_bytes());
                hasher.update(b"monodb_fallback_id");
            }

            let hash = hasher.finalize();
            [hash[0], hash[1], hash[2]]
        });
        bytes[4..7].copy_from_slice(machine_bytes);

        // 2-byte process identifier
        let process_bytes = PROCESS_BYTES.get_or_init(|| {
            let pid = std::process::id();
            [((pid >> 8) & 0xFF) as u8, (pid & 0xFF) as u8]
        });
        bytes[7..9].copy_from_slice(process_bytes);

        // 3-byte counter
        let counter_atomic = COUNTER.get_or_init(|| {
            let mut rng = OsRng;
            let mut random_bytes = [0u8; 4];

            if rng.try_fill_bytes(&mut random_bytes).is_ok() {
                let initial = u32::from_be_bytes(random_bytes) & 0xFFFFFF;
                std::sync::atomic::AtomicU32::new(initial)
            } else {
                // Fallback for if RNG fails (unlikely)
                let fallback = (std::process::id() ^ 0xDEADBEEF) & 0xFFFFFF;
                std::sync::atomic::AtomicU32::new(fallback)
            }
        });

        let counter = counter_atomic.fetch_add(1, std::sync::atomic::Ordering::SeqCst) & 0xFFFFFF;
        bytes[9] = ((counter >> 16) & 0xFF) as u8;
        bytes[10] = ((counter >> 8) & 0xFF) as u8;
        bytes[11] = (counter & 0xFF) as u8;

        Ok(Self(bytes))
    }

    pub fn bytes(&self) -> [u8; 12] {
        self.0
    }

    pub fn to_hex(&self) -> String {
        self.0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("")
    }

    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 24 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(MonoError::Parse("Invalid ObjectId hex string".into()));
        }
        let mut bytes = [0u8; 12];
        for i in 0..12 {
            let byte_str = &s[i * 2..i * 2 + 2];
            bytes[i] = u8::from_str_radix(byte_str, 16)
                .map_err(|_| MonoError::Parse("Invalid ObjectId hex string".into()))?;
        }
        Ok(Self(bytes))
    }

    pub fn from_bytes(bytes: [u8; 12]) -> Self {
        Self(bytes)
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use std::collections::{BTreeMap, HashSet};

    fn roundtrip(value: Value) {
        let bytes = value.to_bytes();
        let (decoded, used) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(decoded, value);
    }

    #[test]
    fn value_type_helpers() {
        assert_eq!(
            ValueType::Int32.common_type(&ValueType::Int64),
            Some(ValueType::Int64)
        );
        assert_eq!(
            ValueType::Int32.common_type(&ValueType::Float64),
            Some(ValueType::Float64)
        );
        assert_eq!(
            ValueType::Null.common_type(&ValueType::String),
            Some(ValueType::String)
        );
        assert_eq!(ValueType::String.common_type(&ValueType::Int32), None);
        assert!(ValueType::Int32.can_coerce_to(&ValueType::Int64));
        assert!(!ValueType::String.can_coerce_to(&ValueType::Int32));

        assert!(ValueType::Int64.is_numeric());
        assert!(ValueType::Int64.is_integer());
        assert!(ValueType::Float64.is_float());
        assert!(ValueType::Date.is_temporal());
        assert!(ValueType::String.is_comparable());
        assert!(!ValueType::Bool.is_comparable());
        assert!(ValueType::Row.is_collection());
        assert!(!ValueType::Extension.is_equatable());
        assert_eq!(ValueType::GeoPoint.display_name(), "GeoPoint");
    }

    #[test]
    fn value_accessors_and_type_names() {
        let value = Value::String("hi".to_string());
        assert_eq!(value.type_name(), "string");
        assert_eq!(value.data_type(), ValueType::String);
        assert_eq!(value.as_string(), Some(&"hi".to_string()));
        assert_eq!(value.clone().into_string(), Some("hi".to_string()));
        assert!(Value::Null.as_string().is_none());

        let value = Value::Array(vec![Value::Int32(1)]);
        assert!(value.as_array().is_some());
        assert_eq!(value.into_array().unwrap().len(), 1);

        let value = Value::Binary(vec![1, 2, 3]);
        assert_eq!(value.as_binary(), Some(&vec![1, 2, 3]));
        assert_eq!(value.into_binary(), Some(vec![1, 2, 3]));

        let value = Value::Bool(true);
        assert_eq!(value.as_bool(), Some(true));
        assert!(Value::Int32(1).as_bool().is_none());

        let value = Value::Int32(7);
        assert_eq!(value.as_i32(), Some(7));
        assert!(value.as_i64().is_none());
        assert!(Value::String("x".into()).as_i32().is_none());

        let value = Value::Int64(9);
        assert_eq!(value.as_i64(), Some(9));

        let value = Value::Float32(1.25);
        assert_eq!(value.as_f32(), Some(1.25));
        assert!(Value::Int32(1).as_f32().is_none());

        let value = Value::Float64(2.5);
        assert_eq!(value.as_f64(), Some(2.5));
        assert!(Value::Int32(1).as_f64().is_none());

        let value = Value::Extension {
            type_name: "custom".into(),
            plugin_id: "plug".into(),
            data: vec![1],
        };
        assert_eq!(value.type_name(), "custom");
        assert_eq!(value.data_type(), ValueType::Extension);

        assert!(Value::Null.is_null());

        let mut obj = BTreeMap::new();
        obj.insert("k".to_string(), Value::Bool(false));
        let value = Value::Object(obj.clone());
        assert!(value.as_object().is_some());
        assert_eq!(value.into_object(), Some(obj));
        assert!(Value::Int32(1).as_object().is_none());
    }

    #[test]
    fn value_coerce_to() {
        assert_eq!(
            Value::Int32(42).coerce_to(&ValueType::Int64).unwrap(),
            Value::Int64(42)
        );
        assert_eq!(
            Value::Int64(42).coerce_to(&ValueType::Float32).unwrap(),
            Value::Float32(42.0)
        );
        assert_eq!(
            Value::Float64(3.5).coerce_to(&ValueType::Int32).unwrap(),
            Value::Int32(3)
        );
        assert_eq!(
            Value::Null.coerce_to(&ValueType::Int32).unwrap(),
            Value::Null
        );
        let err = Value::Int64(i64::from(i32::MAX) + 1)
            .coerce_to(&ValueType::Int32)
            .unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
        let err = Value::String("nope".into())
            .coerce_to(&ValueType::Int32)
            .unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
    }

    #[test]
    fn value_to_json_variants() {
        let dt = chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(2024, 1, 2, 3, 4, 5)
            .unwrap();
        let oid = ObjectId::from_bytes([1u8; 12]);
        let uuid = Uuid::parse_str("e086130f-a1af-42b3-acd9-45884fc4c06f").unwrap();

        let mut obj = BTreeMap::new();
        obj.insert("a".to_string(), Value::Int32(1));

        let mut set = HashSet::new();
        set.insert("x".to_string());

        let mut row = IndexMap::new();
        row.insert("r".to_string(), Value::Bool(true));

        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int32(1),
            Value::Int64(2),
            Value::Float32(1.25),
            Value::Float64(2.5),
            Value::String("s".into()),
            Value::Binary(vec![1, 2, 3]),
            Value::DateTime(dt),
            Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
            Value::Time(chrono::NaiveTime::from_hms_micro_opt(3, 4, 5, 0).unwrap()),
            Value::Uuid(uuid),
            Value::ObjectId(oid),
            Value::Array(vec![Value::Int32(1), Value::String("a".into())]),
            Value::Object(obj),
            Value::Set(set),
            Value::Row(row),
            Value::SortedSet(vec![(1.5, "a".into())]),
            Value::GeoPoint { lat: 1.0, lng: 2.0 },
            Value::Reference {
                collection: "users".into(),
                id: Box::new(Value::Int32(1)),
            },
            Value::Extension {
                type_name: "ext".into(),
                plugin_id: "plugin".into(),
                data: vec![9, 8, 7],
            },
        ];

        for value in values {
            let json = value.to_json();
            match json {
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_)
                | serde_json::Value::Array(_)
                | serde_json::Value::Object(_) => {}
            }
        }
    }

    #[test]
    fn value_from_json_basic() {
        assert_eq!(Value::from_json(serde_json::Value::Null), Value::Null);
        assert_eq!(
            Value::from_json(serde_json::Value::Bool(true)),
            Value::Bool(true)
        );
        assert_eq!(Value::from_json(serde_json::json!(5)), Value::Int32(5));
        let big = i64::from(i32::MAX) + 10;
        assert_eq!(Value::from_json(serde_json::json!(big)), Value::Int64(big));
        assert_eq!(
            Value::from_json(serde_json::json!(1.5)),
            Value::Float64(1.5)
        );
        assert_eq!(
            Value::from_json(serde_json::json!("hi")),
            Value::String("hi".into())
        );
        assert_eq!(
            Value::from_json(serde_json::json!([1, 2])),
            Value::Array(vec![Value::Int32(1), Value::Int32(2)])
        );
        let mut obj = BTreeMap::new();
        obj.insert("k".to_string(), Value::String("v".into()));
        assert_eq!(
            Value::from_json(serde_json::json!({"k": "v"})),
            Value::Object(obj)
        );
    }

    #[test]
    fn value_from_conversions() {
        assert_eq!(Value::from(1i32), Value::Int32(1));
        assert_eq!(Value::from(1i64), Value::Int32(1));
        assert_eq!(
            Value::from(i64::from(i32::MAX) + 1),
            Value::Int64(i64::from(i32::MAX) + 1)
        );
        assert_eq!(Value::from(1u32), Value::Int32(1));
        let u32_over = (i32::MAX as u32) + 1;
        assert_eq!(Value::from(u32_over), Value::Int64((i32::MAX as i64) + 1));
        assert_eq!(Value::from(1u64), Value::Int32(1));
        let u64_over = (i64::MAX as u64) + 1;
        assert!(matches!(Value::from(u64_over), Value::Float64(_)));
        assert_eq!(Value::from(1f32), Value::Float32(1.0));
        assert_eq!(Value::from(1f64), Value::Float64(1.0));
        assert_eq!(Value::from(true), Value::Bool(true));
        assert_eq!(Value::from("hi"), Value::String("hi".into()));
    }

    #[test]
    fn value_bytes_roundtrip() {
        let dt = chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(2024, 1, 2, 3, 4, 5)
            .unwrap();
        let date = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let time = chrono::NaiveTime::from_hms_micro_opt(3, 4, 5, 700_000).unwrap();
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let oid = ObjectId::from_bytes([7u8; 12]);

        let mut obj = BTreeMap::new();
        obj.insert("k".to_string(), Value::String("v".to_string()));

        let mut set = HashSet::new();
        set.insert("a".to_string());

        let mut row = IndexMap::new();
        row.insert("id".to_string(), Value::Int32(1));

        roundtrip(Value::Null);
        roundtrip(Value::Bool(true));
        roundtrip(Value::Int32(-7));
        roundtrip(Value::Int64(9_000_000_000));
        roundtrip(Value::Float32(1.25));
        roundtrip(Value::Float64(2.5));
        roundtrip(Value::String("hello".into()));
        roundtrip(Value::Binary(vec![1, 2, 3]));
        roundtrip(Value::DateTime(dt));
        roundtrip(Value::Date(date));
        roundtrip(Value::Time(time));
        roundtrip(Value::Uuid(uuid));
        roundtrip(Value::ObjectId(oid));
        roundtrip(Value::Array(vec![
            Value::Int32(1),
            Value::String("a".into()),
        ]));
        roundtrip(Value::Object(obj));
        roundtrip(Value::Set(set));
        roundtrip(Value::Row(row));
        roundtrip(Value::SortedSet(vec![(1.5, "x".into())]));
        roundtrip(Value::GeoPoint { lat: 3.0, lng: 4.0 });
        roundtrip(Value::Reference {
            collection: "users".into(),
            id: Box::new(Value::Int32(42)),
        });
        roundtrip(Value::Extension {
            type_name: "ext".into(),
            plugin_id: "plug".into(),
            data: vec![9, 8, 7],
        });
    }

    #[test]
    fn value_from_bytes_errors() {
        let err = Value::from_bytes(&[]).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));

        let err = Value::from_bytes(&[99]).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));

        let err = Value::from_bytes(&[6, 2, 0, 0, 0, 0xff]).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));

        let err = Value::from_bytes(&[9, 0, 0, 0, 0, 13, 1]).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));

        let err = Value::from_bytes(&[6, 2, 0, 0, 0, 0xff, 0xff]).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));

        let mut buf = vec![8];
        buf.extend_from_slice(&1i64.to_le_bytes());
        buf.extend_from_slice(&2000i32.to_le_bytes());
        let err = Value::from_bytes(&buf).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));

        let mut buf = vec![8];
        buf.extend_from_slice(&i64::MAX.to_le_bytes());
        buf.extend_from_slice(&0i32.to_le_bytes());
        let err = Value::from_bytes(&buf).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));

        let mut buf = vec![10];
        buf.push(25);
        buf.push(0);
        buf.push(0);
        buf.extend_from_slice(&0u32.to_le_bytes());
        let err = Value::from_bytes(&buf).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));

        let mut buf = vec![13];
        buf.extend_from_slice(&1u32.to_le_bytes());
        let err = Value::from_bytes(&buf).unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));
    }

    #[test]
    fn value_from_str_parsing() {
        assert_eq!(
            "\"hello\"".parse::<Value>().unwrap(),
            Value::String("hello".into())
        );
        assert_eq!(
            "'hello'".parse::<Value>().unwrap(),
            Value::String("hello".into())
        );
        assert_eq!("null".parse::<Value>().unwrap(), Value::Null);
        assert_eq!("none".parse::<Value>().unwrap(), Value::Null);
        assert_eq!("".parse::<Value>().unwrap(), Value::Null);
        assert_eq!("true".parse::<Value>().unwrap(), Value::Bool(true));
        assert_eq!("123".parse::<Value>().unwrap(), Value::Int32(123));
        assert_eq!(
            "1234567890123".parse::<Value>().unwrap(),
            Value::Int64(1234567890123)
        );
        assert_eq!("1.5".parse::<Value>().unwrap(), Value::Float64(1.5));
        assert_eq!(
            "0x0a0b".parse::<Value>().unwrap(),
            Value::Binary(vec![0x0a, 0x0b])
        );
        assert_eq!(
            "GeoPoint(1.5, 2.5)".parse::<Value>().unwrap(),
            Value::GeoPoint { lat: 1.5, lng: 2.5 }
        );
        assert_eq!(
            "Set{a,b}".parse::<Value>().unwrap(),
            Value::Set(HashSet::from(["a".to_string(), "b".to_string()]))
        );
        let row = "Row(1, 2)".parse::<Value>().unwrap();
        if let Value::Row(map) = row {
            assert_eq!(map.len(), 2);
        } else {
            panic!("expected row");
        }
        let sorted = "SortedSet{(1.0, \"a\"),(2.0, \"b\")}"
            .parse::<Value>()
            .unwrap();
        if let Value::SortedSet(items) = sorted {
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected sorted set");
        }
        let reference = "Reference(users, 123)".parse::<Value>().unwrap();
        assert_eq!(
            reference,
            Value::Reference {
                collection: "users".into(),
                id: Box::new(Value::String("123".into()))
            }
        );
        let json_obj = "{\"a\":1}".parse::<Value>().unwrap();
        assert!(matches!(json_obj, Value::Object(_)));
        let json_arr = "[1,2]".parse::<Value>().unwrap();
        assert!(matches!(json_arr, Value::Array(_)));

        let uuid = "550e8400-e29b-41d4-a716-446655440000"
            .parse::<Value>()
            .unwrap();
        assert!(matches!(uuid, Value::Uuid(_)));
        let oid = "0123456789abcdef01234567".parse::<Value>().unwrap();
        assert!(matches!(oid, Value::ObjectId(_)));
        let dt = "2024-01-02T03:04:05+00:00".parse::<Value>().unwrap();
        assert!(matches!(dt, Value::DateTime(_)));
        let date = "2024-01-02".parse::<Value>().unwrap();
        assert!(matches!(date, Value::Date(_)));
        let time = "03:04:05".parse::<Value>().unwrap();
        assert!(matches!(time, Value::Time(_)));

        let err = "0xzz".parse::<Value>().unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));
        let err = "GeoPoint(1,)".parse::<Value>().unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));
        let err = "SortedSet{(a, \"b\")}".parse::<Value>().unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));
        let err = "Reference(users)".parse::<Value>().unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));
        let err = "Row(1, ???)".parse::<Value>().unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
        let err = "???".parse::<Value>().unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
    }

    #[test]
    fn value_indexing() {
        let mut obj = BTreeMap::new();
        obj.insert("a".to_string(), Value::Int32(1));
        let value = Value::Object(obj);
        assert_eq!(value["a"], Value::Int32(1));
        assert_eq!(value["missing"], Value::Null);

        let value = Value::Array(vec![Value::Int32(1)]);
        assert_eq!(value[0], Value::Int32(1));
        assert_eq!(value[10], Value::Null);
    }

    #[test]
    #[should_panic]
    fn value_index_string_panics_for_non_object() {
        let value = Value::Int32(1);
        let _ = value["bad"];
    }

    #[test]
    #[should_panic]
    fn value_index_usize_panics_for_non_array() {
        let value = Value::Int32(1);
        let _ = value[0];
    }

    #[test]
    fn value_arithmetic() {
        assert_eq!(
            (Value::Int32(1) + Value::Int32(2)).unwrap(),
            Value::Int32(3)
        );
        assert_eq!(
            (Value::Int32(1) + Value::Int64(2)).unwrap(),
            Value::Int64(3)
        );
        assert_eq!(
            (Value::Int64(1) + Value::Int32(2)).unwrap(),
            Value::Int64(3)
        );
        assert_eq!(
            (Value::Float32(1.0) + Value::Float64(2.5)).unwrap(),
            Value::Float64(3.5)
        );
        assert_eq!(
            (Value::Float64(2.5) + Value::Float32(1.0)).unwrap(),
            Value::Float64(3.5)
        );
        assert_eq!(
            (Value::String("a".into()) + Value::String("b".into())).unwrap(),
            Value::String("ab".into())
        );
        assert_eq!(
            (Value::Array(vec![Value::Int32(1)]) + Value::Array(vec![Value::Int32(2)])).unwrap(),
            Value::Array(vec![Value::Int32(1), Value::Int32(2)])
        );

        assert_eq!(
            (Value::Int32(5) - Value::Int32(3)).unwrap(),
            Value::Int32(2)
        );
        assert_eq!(
            (Value::Int32(2) * Value::Int32(3)).unwrap(),
            Value::Int32(6)
        );
        assert_eq!(
            (Value::String("a".into()) * Value::Int32(3)).unwrap(),
            Value::String("aaa".into())
        );
        assert_eq!(
            (Value::Int64(2) * Value::String("b".into())).unwrap(),
            Value::String("bb".into())
        );
        assert_eq!(
            (Value::Float32(1.0) * Value::Float64(2.5)).unwrap(),
            Value::Float64(2.5)
        );
        assert_eq!(
            (Value::Float64(2.5) * Value::Float32(2.0)).unwrap(),
            Value::Float64(5.0)
        );
        assert_eq!(
            (Value::Int32(8) / Value::Int32(2)).unwrap(),
            Value::Int32(4)
        );
        assert_eq!(
            (Value::Int32(8) / Value::Int64(2)).unwrap(),
            Value::Int64(4)
        );
        assert_eq!(
            (Value::Int64(8) / Value::Int32(2)).unwrap(),
            Value::Int64(4)
        );
        assert_eq!(
            (Value::Float32(2.0) / Value::Float64(2.0)).unwrap(),
            Value::Float64(1.0)
        );
        assert_eq!(
            (Value::Float64(2.0) / Value::Float32(2.0)).unwrap(),
            Value::Float64(1.0)
        );
        assert_eq!(
            (Value::Float64(2.5) + Value::Int32(1)).unwrap(),
            Value::Float64(3.5)
        );
        assert!(matches!(
            (Value::Float32(1.0) / Value::Float32(0.0)).unwrap_err(),
            MonoError::TypeError { .. }
        ));

        let err = (Value::Int32(1) + Value::Bool(true)).unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
        let err = (Value::Int32(1) - Value::Bool(true)).unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
        let err = (Value::String("a".into()) * Value::Int32(-1)).unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
        let err = (Value::Float64(2.0) / Value::Float64(0.0)).unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
        let err = (Value::Int32(1) / Value::Int32(0)).unwrap_err();
        assert!(matches!(err, MonoError::TypeError { .. }));
    }

    #[test]
    fn value_display_variants() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int32(1),
            Value::Int64(2),
            Value::Float32(1.5),
            Value::Float64(2.5),
            Value::String("hi".into()),
            Value::Binary(vec![0x0a, 0x0b]),
            Value::DateTime(
                chrono::FixedOffset::east_opt(0)
                    .unwrap()
                    .with_ymd_and_hms(2024, 1, 2, 3, 4, 5)
                    .unwrap(),
            ),
            Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
            Value::Time(chrono::NaiveTime::from_hms_micro_opt(3, 4, 5, 0).unwrap()),
            Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            Value::ObjectId(ObjectId::from_bytes([1u8; 12])),
            Value::Array(vec![Value::Int32(1)]),
            Value::Object(BTreeMap::from([("k".to_string(), Value::Int32(1))])),
            Value::Set(HashSet::from(["a".to_string()])),
            Value::Row(IndexMap::from([("k".to_string(), Value::Int32(1))])),
            Value::SortedSet(vec![(1.0, "m".into())]),
            Value::GeoPoint { lat: 1.0, lng: 2.0 },
            Value::Reference {
                collection: "users".into(),
                id: Box::new(Value::Int32(1)),
            },
            Value::Extension {
                type_name: "ext".into(),
                plugin_id: "plug".into(),
                data: vec![1, 2],
            },
        ];

        for value in values {
            assert!(!value.to_string().is_empty());
        }
    }

    #[test]
    fn value_to_json_float_nan() {
        let json = Value::Float64(f64::NAN).to_json();
        assert!(matches!(json, serde_json::Value::Null));
    }

    #[test]
    fn object_id_helpers() {
        let oid = ObjectId::new().unwrap();
        let hex = oid.to_hex();
        assert_eq!(hex.len(), 24);
        let parsed = ObjectId::from_hex(&hex).unwrap();
        assert_eq!(parsed.to_hex(), hex);

        let bytes = [1u8; 12];
        let oid = ObjectId::from_bytes(bytes);
        assert_eq!(oid.bytes(), bytes);

        let err = ObjectId::from_hex("bad").unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));
        let err = ObjectId::from_hex("zzzzzzzzzzzzzzzzzzzzzzzz").unwrap_err();
        assert!(matches!(err, MonoError::Parse(_)));
    }
}

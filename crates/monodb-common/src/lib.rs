//! Common library for MonoDB
//!
//! This crate provides common types and utilities used by both client and server components of MonoDB.
//! It includes definitions for errors, protocol messages, data types, and schema representations.
//!
//! Modules:
//! * `error`: Defines error types and handling.
//! * `protocol`: Defines the communication protocol between client and server.
//! * `schema`: Defines database schema representations.
//! * `document`: BSON implementation.
//! * `value`: Defines data value types used in database operations.

// #[cfg(test)]
// pub mod tests;

pub mod error;
pub mod protocol;
// pub mod schema;
pub mod value;

// Re-export commonly used types at the base
pub use error::*;
// pub use value::{ObjectId, Value, ValueType};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PROTOCOL_VERSION: u8 = 1;

// #[cfg(test)]
// pub mod tests;

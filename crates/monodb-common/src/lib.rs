//! Common library for MonoDB
//!
//! This crate provides common types and utilities used by both client and server components of MonoDB.
//! It includes definitions for errors, protocol messages, and data types.
//!
//! Modules:
//! * `error`: Defines error types and handling.
//! * `protocol`: Defines the communication protocol between client and server.
//! * `permissions`: Defines permission and access control types.
//! * `value`: Defines data value types used in database operations.

pub mod error;
pub mod permissions;
pub mod protocol;
pub mod value;

// Re-export commonly used types at the base
pub use error::*;
pub use value::{ObjectId, Value, ValueType};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

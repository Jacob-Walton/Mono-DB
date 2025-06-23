//! Network communication module
//!
//! This module handles network protocols and connection management
//! for client-server communication.

pub mod connection;
pub mod protocol;

pub use connection::NetworkConnection;
pub use protocol::{ClientMessage as Message, ProtocolHandler, ServerMessage as Response};

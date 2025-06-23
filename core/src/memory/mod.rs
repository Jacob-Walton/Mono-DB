//! Memory management module
//!
//! This module provides memory management utilities for the database engine.

pub mod manager;

pub use manager::{Allocation, MemoryManager, MemoryPool};

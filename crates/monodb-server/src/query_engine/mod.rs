//! Query Engine Module
//!
//! Implements the query language for MonoDB.

pub mod ast;
pub mod cache;
pub mod executor;
pub mod lexer;
pub mod logical_plan;
pub mod parser;
pub mod physical_plan;
pub mod planner;
pub mod storage;

#[cfg(test)]
mod tests;

// Re-export commonly used types
pub use ast::Statement;
pub use executor::{ExecutionContext, Executor};
pub use parser::parse;
pub use storage::QueryStorage;

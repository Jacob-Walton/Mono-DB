pub mod ast;
pub mod error;
pub mod interner;
pub mod parser;

pub use error::{Error, Result};
pub use interner::StringInterner;

pub use ast::Program;

#[cfg(test)]
mod tests;

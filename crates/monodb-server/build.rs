//! Build script for monodb-server
//!
//! Generates a PHF (perfect hash function) map for keyword lookup in the lexer.
//! This provides O(1) keyword recognition at runtime with compile-time generation.

use phf_codegen::Map;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

fn main() {
    generate_keywords();
}

fn generate_keywords() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("keywords.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());
    let mut map = Map::<&'static [u8]>::new();

    // Core verbs
    map.entry(b"get", "TokenKind::Keyword");
    map.entry(b"put", "TokenKind::Keyword");
    map.entry(b"change", "TokenKind::Keyword");
    map.entry(b"remove", "TokenKind::Keyword");
    map.entry(b"make", "TokenKind::Keyword");

    // Prepositions
    map.entry(b"from", "TokenKind::Keyword");
    map.entry(b"into", "TokenKind::Keyword");
    map.entry(b"where", "TokenKind::Keyword");
    map.entry(b"with", "TokenKind::Keyword");
    map.entry(b"set", "TokenKind::Keyword");
    map.entry(b"as", "TokenKind::Keyword");

    // Table/structure keywords
    map.entry(b"table", "TokenKind::Keyword");
    map.entry(b"fields", "TokenKind::Keyword");
    map.entry(b"relational", "TokenKind::Keyword");
    map.entry(b"document", "TokenKind::Keyword");
    map.entry(b"keyspace", "TokenKind::Keyword");
    map.entry(b"primary", "TokenKind::Keyword");
    map.entry(b"key", "TokenKind::Keyword");
    map.entry(b"unique", "TokenKind::Keyword");
    map.entry(b"required", "TokenKind::Keyword");
    map.entry(b"default", "TokenKind::Keyword");
    map.entry(b"ttl", "TokenKind::Keyword");

    // Data types
    map.entry(b"int", "TokenKind::Keyword");
    map.entry(b"bigint", "TokenKind::Keyword");
    map.entry(b"text", "TokenKind::Keyword");
    map.entry(b"decimal", "TokenKind::Keyword");
    map.entry(b"double", "TokenKind::Keyword");
    map.entry(b"date", "TokenKind::Keyword");
    map.entry(b"boolean", "TokenKind::Keyword");
    map.entry(b"map", "TokenKind::Keyword");
    map.entry(b"list", "TokenKind::Keyword");

    // Literals and functions
    map.entry(b"true", "TokenKind::Keyword");
    map.entry(b"false", "TokenKind::Keyword");
    map.entry(b"null", "TokenKind::Keyword");
    map.entry(b"now", "TokenKind::Keyword");

    // Operators and logic
    map.entry(b"has", "TokenKind::Keyword");
    map.entry(b"and", "TokenKind::Keyword");
    map.entry(b"or", "TokenKind::Keyword");

    // Transaction control (MVCC)
    map.entry(b"begin", "TokenKind::Keyword");
    map.entry(b"commit", "TokenKind::Keyword");
    map.entry(b"rollback", "TokenKind::Keyword");

    // Query modifiers (ordering, pagination)
    map.entry(b"order", "TokenKind::Keyword");
    map.entry(b"by", "TokenKind::Keyword");
    map.entry(b"asc", "TokenKind::Keyword");
    map.entry(b"desc", "TokenKind::Keyword");
    map.entry(b"take", "TokenKind::Keyword");
    map.entry(b"skip", "TokenKind::Keyword");

    // Index management
    map.entry(b"index", "TokenKind::Keyword");
    map.entry(b"on", "TokenKind::Keyword");
    map.entry(b"drop", "TokenKind::Keyword");

    // Schema inspection
    map.entry(b"describe", "TokenKind::Keyword");

    // Aggregation
    map.entry(b"count", "TokenKind::Keyword");

    // Namespace/database commands
    map.entry(b"use", "TokenKind::Keyword");
    map.entry(b"namespace", "TokenKind::Keyword");
    map.entry(b"namespaces", "TokenKind::Keyword");

    write!(
        &mut file,
        "static KEYWORDS: phf::Map<&'static [u8], TokenKind> = {};",
        map.build()
    )
    .unwrap();
}

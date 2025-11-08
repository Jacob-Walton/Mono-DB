use phf_codegen::Map;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

fn main() {
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

    write!(
        &mut file,
        "static KEYWORDS: phf::Map<&'static [u8], TokenKind> = {};",
        map.build()
    )
    .unwrap();
}

//! Resolving an [`RdfFormat`] from what a user supplies: a file path or a
//! format name. The two entry points below are the only places a format is
//! named rather than passed, so every binding's format argument funnels
//! through them and accepts the same spellings.

use oxrdfio::RdfFormat;

/// Infer the RDF format from a path's extension. `None` when there is no
/// path, no extension, or the extension names no format oxrdfio knows — the
/// caller then needs an explicit format.
pub fn detect_format(path: &Option<std::path::PathBuf>) -> Option<RdfFormat> {
    let path = path.as_ref()?;
    let ext = path.extension()?.to_str()?;
    RdfFormat::from_extension(ext)
}

/// Parse a user-facing RDF format name — case-insensitive, accepting the
/// common aliases (`"ntriples"`, `"ttl"`, `"xml"`, …) — into an
/// [`RdfFormat`]. `None` for an unrecognized name. The name table behind
/// every string-typed format parameter (the JS bindings' `RdfFormatName`).
pub fn format_from_name(name: &str) -> Option<RdfFormat> {
    Some(match name.to_lowercase().as_str() {
        "nt" | "ntriples" => RdfFormat::NTriples,
        "nq" | "nquads" => RdfFormat::NQuads,
        "ttl" | "turtle" => RdfFormat::Turtle,
        "trig" => RdfFormat::TriG,
        "n3" => RdfFormat::N3,
        "rdf" | "rdfxml" | "xml" => RdfFormat::RdfXml,
        "jsonld" => RdfFormat::JsonLd {
            profile: Default::default(),
        },
        _ => return None,
    })
}

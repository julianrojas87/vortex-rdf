use oxrdfio::RdfFormat;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum, Debug)]
pub enum Format {
    /// N-Triples (.nt)
    #[value(name = "nt")]
    NTriples,
    /// N-Quads (.nq)
    #[value(name = "nq")]
    NQuads,
    /// Turtle (.ttl)
    #[value(name = "ttl")]
    Turtle,
    /// TriG (.trig)
    #[value(name = "trig")]
    TriG,
    /// RDF/XML (.rdf, .xml)
    #[value(name = "rdf")]
    RdfXml,
    /// JSON-LD (.jsonld, .json)
    #[value(name = "jsonld")]
    JsonLd,
    /// N3 (.n3)
    #[value(name = "n3")]
    N3,
}

impl From<Format> for RdfFormat {
    fn from(f: Format) -> Self {
        match f {
            Format::NTriples => RdfFormat::NTriples,
            Format::NQuads => RdfFormat::NQuads,
            Format::Turtle => RdfFormat::Turtle,
            Format::TriG => RdfFormat::TriG,
            Format::RdfXml => RdfFormat::RdfXml,
            Format::JsonLd => RdfFormat::JsonLd {
                profile: Default::default(),
            },
            Format::N3 => RdfFormat::N3,
        }
    }
}

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

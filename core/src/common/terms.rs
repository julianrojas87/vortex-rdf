//! RDF term parsing and reconstruction: the store's serialized N-Triples
//! strings back into `oxrdf` terms, and RDF documents into [`RawQuad`]
//! streams.

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;

use futures::{Stream, stream};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Term};
use oxrdfio::{RdfFormat, RdfParser};

/// Parses a string representation of an RDF named node (URI), stripping optional `<` and `>` boundaries.
///
/// **Trusted-input decode path.** Every caller reconstructs a term from the
/// store's *own* serialized columns (see [`super::super::store::layouts`]), whose
/// IRIs were validated by oxrdf's constructors at ingestion — so this uses
/// [`NamedNode::new_unchecked`] rather than re-running `oxiri::Iri::parse`, which
/// profiling showed as ~48% of every many-row read (both in-memory and
/// file-backed). `.vortex` files are likewise trusted to have been checked when
/// written. The `Result` is kept so the decode call sites (which `?` on genuinely
/// fallible neighbours like [`buf_as_str`]) stay uniform.
pub fn parse_named_node(s: &str) -> Result<NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    Ok(NamedNode::new_unchecked(s))
}

/// Parses a string representation of an RDF blank node, stripping the `_:` prefix
/// if present. Trusted-input decode path — see [`parse_named_node`].
pub fn parse_blank_node(s: &str) -> Result<BlankNode> {
    let s = s.trim_start_matches("_:");
    Ok(BlankNode::new_unchecked(s))
}

/// Parses an RDF subject node, which can either be a NamedNode (URI) or a BlankNode.
pub fn parse_subject(s: &str) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(parse_named_node(s)?))
    }
}

/// Reconstructs a literal from its serialized N-Triples form: simple
/// (`"v"`), language-tagged (`"v"@lang`), or typed (`"v"^^<dt>`). Trusted
/// decode path — see [`parse_named_node`].
fn literal_from_serialized(s: &str) -> Literal {
    if s.contains("^^") {
        let parts: Vec<&str> = s.splitn(2, "^^").collect();
        let val = parts[0].trim_matches('"');
        let dt = parts[1].trim_matches(|c| c == '<' || c == '>');
        Literal::new_typed_literal(val, NamedNode::new_unchecked(dt))
    } else if let Some(at_pos) = s.rfind('@') {
        if at_pos > 0 && s.as_bytes()[at_pos - 1] == b'"' {
            let val = s[..at_pos].trim_matches('"');
            let lang = &s[at_pos + 1..];
            Literal::new_language_tagged_literal_unchecked(val, lang)
        } else {
            Literal::new_simple_literal(s.trim_matches('"'))
        }
    } else {
        Literal::new_simple_literal(s.trim_matches('"'))
    }
}

/// Parses an arbitrary RDF term (blank node, literal, or named node) from its string form.
pub fn parse_term(s: &str) -> Result<Term> {
    if s.starts_with('_') {
        Ok(Term::BlankNode(parse_blank_node(s)?))
    } else if s.starts_with('"') {
        Ok(Term::Literal(literal_from_serialized(s)))
    } else {
        Ok(Term::NamedNode(parse_named_node(s)?))
    }
}

/// Parses an RDF graph name, which can be the default graph, a named node, or a blank node.
pub fn parse_graph_name(s: &str) -> Result<GraphName> {
    if s.is_empty() || s.eq_ignore_ascii_case("default") || s == "[]" {
        Ok(GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(GraphName::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(GraphName::NamedNode(parse_named_node(s)?))
    }
}

/// Reconstructs a full structural oxrdf `Term` from its raw serialized string representation.
/// Handles URIs, Blank Nodes, simple literals, language-tagged literals, and typed literals.
pub fn get_as_term(s: &str) -> Option<Term> {
    if s.starts_with('<') {
        // Trusted-input decode path — see `parse_named_node`; `new_unchecked`
        // skips the `oxiri::Iri::parse` re-validation of an already-validated,
        // stored IRI.
        Some(Term::NamedNode(NamedNode::new_unchecked(
            s.trim_matches(|c| c == '<' || c == '>'),
        )))
    } else if s.starts_with("_:") {
        Some(Term::BlankNode(BlankNode::new_unchecked(
            s.trim_start_matches("_:"),
        )))
    } else if s.starts_with('"') {
        Some(Term::Literal(literal_from_serialized(s)))
    } else {
        None
    }
}

/// Parses a stream of RDF quads from any reader using the specified RDF format.
///
/// Yields [`RawQuad`] rather than `oxrdf::Quad`: every builder converts to
/// `RawQuad` as its first act, so handing back the parsed `Quad` would keep a
/// second owned copy of every term alive for no purpose. Converting here lets
/// the `Quad` die inside the map.
pub fn parse_quads_from_reader<R: std::io::Read + Send + 'static>(
    reader: R,
    format: RdfFormat,
) -> impl Stream<Item = Result<RawQuad>> {
    let parser = RdfParser::from_format(format);
    let iter = parser.for_reader(reader).map(|x| {
        x.map(|q| RawQuad::from_quad(&q))
            .map_err(|e| VortexRdfError::Deserialization(format!("Parse error: {}", e)))
    });
    stream::iter(iter)
}

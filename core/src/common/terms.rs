//! RDF term parsing and reconstruction: the store's serialized N-Triples
//! strings back into `oxrdf` terms, and RDF documents into [`RawQuad`]
//! streams.

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;

use std::borrow::Cow;

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
/// fallible neighbours like `buf_as_str`) stay uniform.
pub fn parse_named_node(s: &str) -> Result<NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    Ok(NamedNode::new_unchecked(s))
}

/// Parses a string representation of an RDF blank node, stripping the `_:` prefix
/// if present. Trusted-input decode path — see [`parse_named_node`].
fn parse_blank_node(s: &str) -> Result<BlankNode> {
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

/// The three N-Triples literal shapes, with `value` still in its *escaped*
/// lexical form — the slice between the opening and closing quote.
enum LiteralForm<'a> {
    Simple { value: &'a str },
    Language { value: &'a str, lang: &'a str },
    Typed { value: &'a str, datatype: &'a str },
}

/// Byte offset of the literal's closing quote, honouring `\` escapes, or
/// `None` if `s` does not start with `"` or is unterminated.
///
/// A quote terminates the literal only when the run of backslashes directly
/// before it has even length; an odd run means the last one escapes it. This
/// jumps quote to quote rather than scanning byte by byte, because `str::find`
/// is memchr-accelerated and a hand-rolled loop is not — literal decode is hot
/// enough on long text objects for the difference to show up end to end.
fn closing_quote(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    if b.first() != Some(&b'"') {
        return None;
    }
    // Both delimiters are ASCII, so every index here is a char boundary.
    let mut from = 1;
    loop {
        let quote = s[from..].find('"')? + from;
        let mut run = quote;
        while run > 1 && b[run - 1] == b'\\' {
            run -= 1;
        }
        if (quote - run) % 2 == 0 {
            return Some(quote);
        }
        from = quote + 1;
    }
}

/// Splits a serialized literal into its escaped value and its suffix
/// interpretation: empty suffix => simple, `^^<dt>` => typed, `@lang` =>
/// language-tagged.
///
/// `None` means the form is malformed — unterminated, or trailing text after
/// the closing quote that is neither suffix. The suffix is read only from
/// *after* the closing quote, so `^^` or `"@` occurring inside the value
/// cannot be mistaken for structure.
fn split_literal(s: &str) -> Option<LiteralForm<'_>> {
    let end = closing_quote(s)?;
    let value = &s[1..end];
    let rest = &s[end + 1..];
    if rest.is_empty() {
        return Some(LiteralForm::Simple { value });
    }
    if let Some(datatype) = rest.strip_prefix("^^") {
        return Some(LiteralForm::Typed { value, datatype });
    }
    rest.strip_prefix('@')
        .map(|lang| LiteralForm::Language { value, lang })
}

/// Decodes the N-Triples escapes of a literal's lexical value: `\\`, `\"`,
/// `\'`, `\n`, `\r`, `\t`, `\b`, `\f`, `\uXXXX` and `\UXXXXXXXX`.
///
/// Borrows when there is no backslash — the common case on the hot trusted
/// decode path, which must stay allocation-free. A backslash that starts no
/// recognized escape (or a truncated/invalid `\u`) is preserved verbatim, so
/// this never loses input.
fn unescape_literal_value(s: &str) -> Cow<'_, str> {
    let b = s.as_bytes();
    if !b.contains(&b'\\') {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            let start = i;
            while i < b.len() && b[i] != b'\\' {
                i += 1;
            }
            out.push_str(&s[start..i]);
            continue;
        }
        let decoded = match b.get(i + 1) {
            Some(b't') => Some(('\t', 2)),
            Some(b'b') => Some(('\u{8}', 2)),
            Some(b'n') => Some(('\n', 2)),
            Some(b'r') => Some(('\r', 2)),
            Some(b'f') => Some(('\u{c}', 2)),
            Some(b'"') => Some(('"', 2)),
            Some(b'\'') => Some(('\'', 2)),
            Some(b'\\') => Some(('\\', 2)),
            Some(b'u') => hex_escape(b, i + 2, 4).map(|c| (c, 6)),
            Some(b'U') => hex_escape(b, i + 2, 8).map(|c| (c, 10)),
            _ => None,
        };
        match decoded {
            Some((c, width)) => {
                out.push(c);
                i += width;
            }
            None => {
                out.push('\\');
                i += 1;
            }
        }
    }
    Cow::Owned(out)
}

/// The character `len` hex digits at `at` encode, or `None` if they are
/// truncated, not hex, or not a scalar value.
fn hex_escape(b: &[u8], at: usize, len: usize) -> Option<char> {
    let digits = b.get(at..at + len)?;
    let mut cp: u32 = 0;
    for &d in digits {
        cp = cp * 16 + (d as char).to_digit(16)?;
    }
    char::from_u32(cp)
}

/// Reconstructs a literal from its serialized N-Triples form: simple
/// (`"v"`), language-tagged (`"v"@lang`), or typed (`"v"^^<dt>`). Trusted
/// decode path — see [`parse_named_node`].
///
/// Infallible: a malformed form (which our own writer cannot produce) falls
/// back to the lenient quote-trimming read rather than panicking.
fn literal_from_serialized(s: &str) -> Literal {
    match split_literal(s) {
        Some(LiteralForm::Simple { value }) => {
            Literal::new_simple_literal(unescape_literal_value(value))
        }
        Some(LiteralForm::Language { value, lang }) => {
            Literal::new_language_tagged_literal_unchecked(unescape_literal_value(value), lang)
        }
        Some(LiteralForm::Typed { value, datatype }) => Literal::new_typed_literal(
            unescape_literal_value(value),
            NamedNode::new_unchecked(datatype.trim_matches(|c| c == '<' || c == '>')),
        ),
        None => Literal::new_simple_literal(s.trim_matches('"')),
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

/// The untrusted-boundary counterpart of [`parse_named_node`]: the IRI is
/// validated by [`NamedNode::new`] instead of being trusted.
///
/// The checked family (`*_checked`) is for strings that did NOT come out of
/// the store's own columns — CLI pattern arguments, binding call arguments,
/// anything a user typed. The trusted family above stays the decode path for
/// terms the store itself serialized, where re-validation is pure cost.
pub fn parse_named_node_checked(s: &str) -> Result<NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    NamedNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(format!("invalid IRI {:?}: {}", s, e)))
}

/// Validating counterpart of `parse_blank_node`, used by the checked family
/// wherever a `_:` form is accepted.
fn parse_blank_node_checked(s: &str) -> Result<BlankNode> {
    let id = s.trim_start_matches("_:");
    BlankNode::new(id)
        .map_err(|e| VortexRdfError::Deserialization(format!("invalid blank node {:?}: {}", s, e)))
}

/// The untrusted-boundary counterpart of [`parse_subject`] — see
/// [`parse_named_node_checked`].
pub fn parse_subject_checked(s: &str) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(parse_blank_node_checked(s)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(parse_named_node_checked(s)?))
    }
}

/// The untrusted-boundary counterpart of [`parse_graph_name`] — see
/// [`parse_named_node_checked`]. The default-graph spellings (`""`,
/// `"default"`, `"[]"`) are those of the trusted form.
pub fn parse_graph_name_checked(s: &str) -> Result<GraphName> {
    if s.is_empty() || s.eq_ignore_ascii_case("default") || s == "[]" {
        Ok(GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(GraphName::BlankNode(parse_blank_node_checked(s)?))
    } else {
        Ok(GraphName::NamedNode(parse_named_node_checked(s)?))
    }
}

/// The untrusted-boundary counterpart of [`get_as_term`]/[`parse_term`]: the
/// same N-Triples forms — `<iri>`, `_:id`, `"v"`, `"v"@lang`, `"v"^^<dt>`,
/// plus a bare IRI as [`parse_term`] accepts — with every component built
/// through a validating constructor (including the language tag and the
/// literal's datatype IRI).
///
/// Deliberately does not delegate to [`get_as_term`], which is a trusted
/// decode path built on `new_unchecked` — see [`parse_named_node_checked`].
pub fn parse_term_checked(s: &str) -> Result<Term> {
    if s.starts_with("_:") {
        Ok(Term::BlankNode(parse_blank_node_checked(s)?))
    } else if s.starts_with('"') {
        Ok(Term::Literal(literal_checked(s)?))
    } else {
        Ok(Term::NamedNode(parse_named_node_checked(s)?))
    }
}

/// Validating counterpart of [`literal_from_serialized`]: the datatype IRI
/// goes through [`NamedNode::new`] and the language tag through
/// [`Literal::new_language_tagged_literal`].
/// Shares [`literal_from_serialized`]'s escape-aware structure scan and
/// unescaping, so both paths yield the same term for the same input; only the
/// malformed case differs, which is an error here rather than a lenient read.
fn literal_checked(s: &str) -> Result<Literal> {
    match split_literal(s) {
        Some(LiteralForm::Simple { value }) => {
            Ok(Literal::new_simple_literal(unescape_literal_value(value)))
        }
        Some(LiteralForm::Language { value, lang }) => {
            Literal::new_language_tagged_literal(unescape_literal_value(value), lang).map_err(|e| {
                VortexRdfError::Deserialization(format!("invalid language tag {:?}: {}", lang, e))
            })
        }
        Some(LiteralForm::Typed { value, datatype }) => Ok(Literal::new_typed_literal(
            unescape_literal_value(value),
            parse_named_node_checked(datatype)?,
        )),
        None => Err(VortexRdfError::Deserialization(format!(
            "malformed literal {:?}",
            s
        ))),
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

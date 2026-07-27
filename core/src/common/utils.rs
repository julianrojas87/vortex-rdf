use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;

use futures::{Stream, stream};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::{RdfFormat, RdfParser};

use vortex_array::arrays::varbinview::BinaryView;
use vortex_array::arrays::{BoolArray, VarBinViewArray};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;

/// Zero-cost row access into a canonical `VarBinView` string column: the
/// 16-byte views slice and the data buffers are resolved once, and each row's
/// bytes are then an inline read or a plain slice of the referenced buffer.
///
/// `bytes_at` instead materializes a refcounted `ByteBuffer` per row (two
/// atomic refcount ops plus alignment bookkeeping), which profiling showed
/// costing ~5% of every many-row decode; this reader is the loop-shaped
/// counterpart, sharing the access pattern of the typed residual filter's
/// `StrEq`.
pub(crate) struct StrColReader<'a> {
    arr: &'a VarBinViewArray,
    views: &'a [BinaryView],
}

impl<'a> StrColReader<'a> {
    pub(crate) fn new(arr: &'a VarBinViewArray) -> Self {
        Self {
            arr,
            views: arr.views(),
        }
    }

    #[inline]
    pub(crate) fn bytes_at(&self, i: usize) -> &'a [u8] {
        let view = &self.views[i];
        if view.is_inlined() {
            view.as_inlined().value()
        } else {
            let r = view.as_view();
            &self.arr.buffer(r.buffer_index as usize)[r.as_range()]
        }
    }

    #[inline]
    pub(crate) fn str_at(&self, i: usize) -> Result<&'a str> {
        buf_as_str(self.bytes_at(i))
    }
}

/// Build a Vortex string array (`VarBinView<Utf8>`, non-nullable) from string refs.
///
/// Values are copied once, directly into the array's buffer — no intermediate
/// owned `String` per value.
pub fn make_string_array(values: impl IntoIterator<Item = impl AsRef<str>>) -> ArrayRef {
    VarBinViewArray::from_iter_str(values).into_array()
}

/// Build a nullable Vortex string array for optional fields (e.g. o_datatype, o_lang).
pub fn make_nullable_string_array(values: impl IntoIterator<Item = Option<String>>) -> ArrayRef {
    VarBinViewArray::from_iter_nullable_str(values).into_array()
}

/// Stamp the exact `IsSorted` statistic on an array.
///
/// Only call when the array is sorted by construction: `match_pattern` trusts
/// this stat to binary-search the column, so a false stamp corrupts query
/// results.
pub(crate) fn stamp_is_sorted(arr: &ArrayRef) {
    use vortex_array::expr::stats::{Precision, Stat};
    arr.statistics()
        .set(Stat::IsSorted, Precision::Exact(true.into()));
}

/// Read back the `IsSorted` statistic written by [`stamp_is_sorted`]. An
/// absent stat counts as unsorted — order is never assumed, only trusted
/// when explicitly recorded.
pub(crate) fn column_is_sorted(arr: &ArrayRef) -> bool {
    use vortex_array::expr::stats::{Precision, Stat, StatsProvider};
    match arr.statistics().get(Stat::IsSorted) {
        Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
        Precision::Absent => false,
    }
}

/// Binary-search a sorted column for the `[lo, hi)` run of rows equal to
/// `probe` (`lo == hi` means the value is absent). Only meaningful on
/// columns [`column_is_sorted`] reports as sorted.
pub(crate) fn search_sorted_bounds(
    arr: &ArrayRef,
    probe: &vortex_array::scalar::Scalar,
) -> Result<(usize, usize)> {
    use vortex_array::arrays::Primitive;
    use vortex_array::search_sorted::{SearchResult, SearchSorted, SearchSortedSide};

    // Typed fast path: a canonical non-nullable u32 column (the Dictionary
    // layout's term codes). `partition_point` over the raw slice costs a few
    // dozen loads; the generic kernel below builds a fresh `ExecutionCtx` and
    // materializes a `Scalar` per probe, which profiling showed dominating
    // `match_pattern`'s fixed per-call cost.
    if arr.dtype().is_unsigned_int()
        && !arr.dtype().is_nullable()
        && let Ok(prim) = arr.clone().try_downcast::<Primitive>()
        && prim.ptype() == vortex_array::dtype::PType::U32
        && let Ok(code) = u32::try_from(probe)
    {
        let codes = prim.as_slice::<u32>();
        let lo = codes.partition_point(|&v| v < code);
        let hi = codes.partition_point(|&v| v <= code);
        return Ok((lo, hi));
    }

    let index_of = |result: SearchResult| match result {
        SearchResult::Found(i) | SearchResult::NotFound(i) => i,
    };
    let lo = arr
        .search_sorted(probe, SearchSortedSide::Left)
        .map_err(VortexRdfError::Vortex)?;
    let hi = arr
        .search_sorted(probe, SearchSortedSide::Right)
        .map_err(VortexRdfError::Vortex)?;
    Ok((index_of(lo), index_of(hi)))
}

/// Convert a boolean ArrayRef into a `vortex_mask::Mask` for use with `ArrayRef::filter`.
pub(crate) fn bool_array_to_mask(arr: ArrayRef) -> Result<Mask> {
    // Canonicalize to a concrete boolean array, then reinterpret its packed
    // bit buffer directly as a Mask (no per-bit conversion loop).
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let bool_arr = arr
        .execute::<BoolArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(Mask::from_buffer(bool_arr.into_bit_buffer()))
}

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

/// Parses an arbitrary RDF term (blank node, literal, or named node) from its string form.
pub fn parse_term(s: &str) -> Result<Term> {
    if s.starts_with('_') {
        Ok(Term::BlankNode(parse_blank_node(s)?))
    } else if s.starts_with('"') {
        let val = s.trim_matches('"');
        Ok(Term::Literal(Literal::new_simple_literal(val)))
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

/// Canonical N-Triples string for a graph name: the empty string denotes the
/// default graph.
pub(crate) fn graph_name_str(g: &GraphName) -> String {
    match g {
        GraphName::DefaultGraph => String::new(),
        other => other.to_string(),
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
        if s.contains("^^") {
            let parts: Vec<&str> = s.splitn(2, "^^").collect();
            let val = parts[0].trim_matches('"');
            let dt = parts[1].trim_matches(|c| c == '<' || c == '>');
            Some(Term::Literal(Literal::new_typed_literal(
                val,
                NamedNode::new_unchecked(dt),
            )))
        } else if let Some(at_pos) = s.rfind('@') {
            if at_pos > 0 && s.as_bytes()[at_pos - 1] == b'"' {
                let val = s[..at_pos].trim_matches('"');
                let lang = &s[at_pos + 1..];
                Some(Term::Literal(
                    Literal::new_language_tagged_literal_unchecked(val, lang),
                ))
            } else {
                Some(Term::Literal(Literal::new_simple_literal(
                    s.trim_matches('"'),
                )))
            }
        } else {
            Some(Term::Literal(Literal::new_simple_literal(
                s.trim_matches('"'),
            )))
        }
    } else {
        None
    }
}

/// Borrow the bytes of a UTF-8 string column value as `&str` without copying.
///
/// **Trusted-input decode path**, the same argument as [`parse_named_node`]:
/// every caller reads a column whose dtype is `Utf8`, and vortex validates that
/// invariant when the array is constructed — `VarBinViewData::validate` runs a
/// `from_utf8` over every view on IPC decode, and the file reader validates on
/// its own construction path. Re-validating here walks each term's bytes a
/// second time, which profiling showed at ~7% of every many-row decode
/// (`core::str::converts::from_utf8` under `decode_chunk`).
///
/// The check is kept as a `debug_assert`, so the test suite (which runs debug)
/// still fails loudly if a non-UTF-8 column ever reaches this, while release
/// builds skip the second walk. The `Result` is retained so the decode call
/// sites — which `?` on genuinely fallible neighbours — stay uniform.
#[inline]
pub(crate) fn buf_as_str(buf: &[u8]) -> Result<&str> {
    debug_assert!(
        std::str::from_utf8(buf).is_ok(),
        "string column value is not valid UTF-8, but its dtype claims Utf8"
    );
    // SAFETY: `buf` is the bytes of a value in a `Utf8`-dtyped vortex column,
    // which vortex validates as UTF-8 when the array is constructed (see above).
    Ok(unsafe { std::str::from_utf8_unchecked(buf) })
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

/// Helper function to generate a stream of mock RDF quads for benchmark and test workflows.
/// Generates triples evenly distributed across 10 named graphs.
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<RawQuad>> {
    const EX: &str = "http://example.org/";
    const NUM_GRAPHS: u64 = 10;

    stream::iter((0..size).map(|i| {
        let subject =
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(format!("{}subject/{}", EX, i)));
        let predicate = NamedNode::new_unchecked(format!("{}predicate/{}", EX, i % 100));
        let object = Term::NamedNode(NamedNode::new_unchecked(format!("{}object/{}", EX, i % 50)));
        let graph = GraphName::NamedNode(NamedNode::new_unchecked(format!(
            "{}graph/{}",
            EX,
            (i as u64) % NUM_GRAPHS
        )));

        Ok(RawQuad::from_quad(&Quad::new(
            subject, predicate, object, graph,
        )))
    }))
}

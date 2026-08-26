//! Builder smoke round-trips and the textual export paths (raw
//! N-Triples/N-Quads fast path, structured serializer) checked against
//! oxrdfio.

use super::*;

// ─── Builder round-trips ───────────────────────────────────────────────

async fn run_builder_roundtrip<B: VortexArrayBuilder>() {
    let quad = make_quad(
        "http://example.org/s",
        "http://example.org/p",
        "hello",
        GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
    );

    let arr = build_array::<B>(
        quad_stream(vec![quad.clone()]),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .expect("build failed");

    let store = VortexRdfStore::from_built(arr).unwrap();
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].subject.to_string(), quad.subject.to_string());
    assert_eq!(decoded[0].predicate.to_string(), quad.predicate.to_string());
    assert_eq!(decoded[0].object.to_string(), quad.object.to_string());
    assert_eq!(
        decoded[0].graph_name.to_string(),
        quad.graph_name.to_string()
    );
}

#[tokio::test]
async fn test_builder_roundtrip_sorted_in_memory() {
    run_builder_roundtrip::<SortedInMemoryBuilder>().await;
}
#[tokio::test]
async fn test_builder_roundtrip_sorted_stream() {
    run_builder_roundtrip::<SortedStreamBuilder>().await;
}

/// A bare Dictionary-layout array cannot self-describe, and `from_parts`
/// says so.
#[test]
fn test_from_parts_rejects_bare_dictionary_array() {
    let err = VortexRdfStore::from_parts(crate::store::StoreParts {
        array: bare_code_quad_array(&[1, 2, 3]),
        components: Vec::new(),
        dict: None,
        quads_sorted: false,
    })
    .err()
    .expect("from_parts should fail");
    assert!(
        err.to_string().contains("from_built"),
        "unexpected error: {err}"
    );
}

/// A rebuild over a base without the sorted stamp sorts every row: the
/// re-emitted rows are in `(s, p, o, g)` order, the base and tail rows
/// interleaved, and the adopted store carries the stamp.
#[tokio::test]
async fn test_unstamped_base_rebuild_sorts_fully() {
    let quads = modular_quads(12, 3, 4);
    let reversed: Vec<Quad> = quads.iter().rev().cloned().collect();
    let base = unstamped_store(&reversed);
    assert!(!base.debug_base_subject_sorted());

    let appended = vec![
        make_quad(
            "http://example.org/s05a",
            "http://example.org/p9",
            "object 9",
            GraphName::DefaultGraph,
        ),
        make_quad(
            "http://example.org/s00a",
            "http://example.org/p9",
            "object 9",
            GraphName::DefaultGraph,
        ),
    ];
    let tailed = base.add_quads(appended.clone()).await.unwrap();
    let rebuilt =
        VortexRdfStore::from_parts(tailed.to_serializable_parts().await.unwrap()).unwrap();
    assert!(
        rebuilt.debug_base_subject_sorted(),
        "a rebuild over an unstamped base must sort and stamp its rows"
    );

    let got = rebuilt.quads_vec().await.unwrap();
    let mut union: Vec<crate::store::RawQuad> = quads
        .iter()
        .chain(appended.iter())
        .map(crate::store::RawQuad::from_quad)
        .collect();
    union.sort();
    let union: Vec<(String, String, String, String)> =
        union.into_iter().map(|r| (r.s, r.p, r.o, r.g)).collect();
    assert_eq!(
        tuple_rows(&got),
        union,
        "rows must come back in (s, p, o, g) order"
    );
}

// ─── Textual export ────────────────────────────────────────────────────

/// Quads whose terms exercise everything the N-Triples escape and suffix
/// grammar has: quotes, backslashes, newlines, a language tag, a datatype,
/// a blank node, and a named graph.
fn export_test_quads() -> Vec<Quad> {
    vec![
        Quad::new(
            NamedOrBlankNode::BlankNode(oxrdf::BlankNode::new("b0").unwrap()),
            NamedNode::new("http://example.org/p").unwrap(),
            Term::Literal(
                Literal::new_language_tagged_literal("héllo \"quoted\"\nline\\slash", "en")
                    .unwrap(),
            ),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap()),
            NamedNode::new("http://example.org/p").unwrap(),
            Term::Literal(Literal::new_typed_literal(
                "42",
                NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap(),
            )),
            GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
        ),
    ]
}

/// What oxrdfio's own serializer produces for `quads` — the reference the
/// raw fast path must match byte for byte.
fn oxrdf_reference(quads: &[Quad], format: oxrdfio::RdfFormat) -> String {
    let mut reference = Vec::new();
    let mut ser = oxrdfio::RdfSerializer::from_format(format).for_writer(&mut reference);
    for quad in quads {
        ser.serialize_quad(quad).unwrap();
    }
    ser.finish().unwrap();
    String::from_utf8(reference).unwrap()
}

/// The three layouts whose raw chunk decoding the export fast path reads
/// through: verbatim string columns, the typed object columns, and codes
/// against the dictionary.
const EXPORT_LAYOUTS: [LayoutStrategy; 3] = [
    LayoutStrategy::Default,
    LayoutStrategy::TypedObject,
    LayoutStrategy::Dictionary,
];

/// The N-Quads export fast path writes the store's raw term strings
/// verbatim; this pins it byte-for-byte against oxrdfio's serializer over
/// terms that exercise escaping, tags, datatypes, blank nodes, and a named
/// graph, under every layout's raw chunk decoding.
#[tokio::test]
async fn test_export_nquads_fast_path_matches_oxrdf() {
    let quads = export_test_quads();
    for layout in EXPORT_LAYOUTS {
        let arr = build_array::<SortedInMemoryBuilder>(quad_stream(quads.clone()), layout, vec![])
            .await
            .unwrap();
        let store = VortexRdfStore::from_built(arr).unwrap();

        let stored: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        let mut exported = Vec::new();
        crate::export_rdf(store, &mut exported, oxrdfio::RdfFormat::NQuads)
            .await
            .unwrap();
        // The reference follows the store's own row order, so what this pins
        // is the byte-level escaping rather than the build's ordering.
        assert_eq!(
            String::from_utf8(exported).unwrap(),
            oxrdf_reference(&stored, oxrdfio::RdfFormat::NQuads),
            "{layout:?}"
        );
    }
}

/// The N-Triples fast path: byte-for-byte on a default-graph store, and the
/// same named-graph refusal oxrdfio's serializer gives.
#[tokio::test]
async fn test_export_ntriples_fast_path_and_named_graph_refusal() {
    let quads = export_test_quads();
    let default_graph_only: Vec<Quad> = quads
        .iter()
        .filter(|q| q.graph_name == GraphName::DefaultGraph)
        .cloned()
        .collect();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(default_graph_only.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let stored: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    let mut exported = Vec::new();
    crate::export_rdf(store, &mut exported, oxrdfio::RdfFormat::NTriples)
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(exported).unwrap(),
        oxrdf_reference(&stored, oxrdfio::RdfFormat::NTriples)
    );

    // A named graph cannot be represented in N-Triples: refuse, as the
    // structured path would.
    let arr =
        build_array::<SortedInMemoryBuilder>(quad_stream(quads), LayoutStrategy::Default, vec![])
            .await
            .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let mut exported = Vec::new();
    assert!(
        crate::export_rdf(store, &mut exported, oxrdfio::RdfFormat::NTriples)
            .await
            .is_err()
    );
}

/// The fast path's file arm: the raw chunk stream over a scan must produce
/// the same bytes as the in-memory arm under every layout — the file-backed
/// dictionary's codes resolved through the async read path included.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_export_nquads_fast_path_file_backed() {
    let quads = export_test_quads();
    for layout in EXPORT_LAYOUTS {
        let (_dir, path) = write_store_file(quads.clone(), layout, vec![]).await;

        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let stored: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        let mut exported = Vec::new();
        crate::export_rdf(store, &mut exported, oxrdfio::RdfFormat::NQuads)
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(exported).unwrap(),
            oxrdf_reference(&stored, oxrdfio::RdfFormat::NQuads),
            "{layout:?}"
        );
    }
}

/// The full textual round trip through both export paths: N-Quads (the raw
/// fast path) and TriG (the structured serializer, which owns the format's
/// syntax state) must re-parse to exactly the quad set the store was built
/// from — escaped literals, language tag, datatype, and the named graph
/// included. Blank nodes are deliberately absent: parsers may relabel them,
/// which would turn a set comparison into a graph-isomorphism check.
#[tokio::test]
async fn test_export_reparses_to_the_same_quads() {
    let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
    let quads = vec![
        make_quad(
            "http://example.org/s1",
            "http://example.org/p",
            "with \"quotes\"\nand back\\slash",
            GraphName::DefaultGraph,
        ),
        Quad::new(
            NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s2").unwrap()),
            NamedNode::new("http://example.org/p").unwrap(),
            Term::Literal(Literal::new_language_tagged_literal("say \"hi\"@home", "en").unwrap()),
            g.clone(),
        ),
        Quad::new(
            NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s3").unwrap()),
            NamedNode::new("http://example.org/p").unwrap(),
            Term::Literal(Literal::new_typed_literal(
                "tab\there",
                NamedNode::new("http://example.org/dt").unwrap(),
            )),
            g,
        ),
    ];

    for format in [oxrdfio::RdfFormat::NQuads, oxrdfio::RdfFormat::TriG] {
        let arr = build_array::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::from_built(arr).unwrap();
        let mut exported = Vec::new();
        crate::export_rdf(store, &mut exported, format)
            .await
            .unwrap();

        let reparsed: Vec<Quad> = oxrdfio::RdfParser::from_format(format)
            .for_reader(exported.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap_or_else(|e| panic!("{format:?}: exported document failed to parse: {e}"));
        assert_eq!(
            quad_strings(&reparsed),
            quad_strings(&quads),
            "{format:?}: export did not round-trip"
        );
    }
}

use futures::stream;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_rdf_core::index::SimpleDictionary;
use vortex_rdf_core::io::{
    CottasNativeConfig, NativeIndexPolicy, match_cottas_native_file, match_native_rdf_store_file,
    serialize_cottas_native_file, serialize_cottas_native_quad_source_v10_file,
};

const S1: &str = "http://example.org/s1";
const S2: &str = "http://example.org/s2";
const S3: &str = "http://example.org/s3";
const MISSING: &str = "http://example.org/missing";
const P1: &str = "http://example.org/p1";
const P2: &str = "http://example.org/p2";
const O1: &str = "http://example.org/o1";
const O2: &str = "http://example.org/o2";
const G1: &str = "http://example.org/g1";

fn named(value: &str) -> NamedNode {
    NamedNode::new(value).unwrap()
}

fn subject(value: &str) -> NamedOrBlankNode {
    NamedOrBlankNode::NamedNode(named(value))
}

fn term(value: &str) -> Term {
    Term::NamedNode(named(value))
}

fn fixture() -> Vec<Quad> {
    vec![
        Quad::new(named(S1), named(P1), term(O1), GraphName::DefaultGraph),
        Quad::new(named(S1), named(P1), term(O1), GraphName::DefaultGraph),
        Quad::new(named(S1), named(P1), term(O2), GraphName::DefaultGraph),
        Quad::new(named(S2), named(P1), term(O1), GraphName::DefaultGraph),
        Quad::new(named(S2), named(P2), term(O2), GraphName::DefaultGraph),
        Quad::new(
            named(S3),
            named(P2),
            term(O1),
            GraphName::NamedNode(named(G1)),
        ),
    ]
}

#[derive(Clone)]
struct Query {
    name: &'static str,
    subject: Option<NamedOrBlankNode>,
    predicate: Option<NamedNode>,
    object: Option<Term>,
    graph: Option<GraphName>,
}

fn query_matrix() -> Vec<Query> {
    vec![
        Query {
            name: "unbound",
            subject: None,
            predicate: None,
            object: None,
            graph: None,
        },
        Query {
            name: "subject",
            subject: Some(subject(S1)),
            predicate: None,
            object: None,
            graph: None,
        },
        Query {
            name: "predicate",
            subject: None,
            predicate: Some(named(P1)),
            object: None,
            graph: None,
        },
        Query {
            name: "object",
            subject: None,
            predicate: None,
            object: Some(term(O1)),
            graph: None,
        },
        Query {
            name: "predicate_object",
            subject: None,
            predicate: Some(named(P1)),
            object: Some(term(O1)),
            graph: None,
        },
        Query {
            name: "fully_bound_duplicate",
            subject: Some(subject(S1)),
            predicate: Some(named(P1)),
            object: Some(term(O1)),
            graph: Some(GraphName::DefaultGraph),
        },
        Query {
            name: "named_graph",
            subject: None,
            predicate: None,
            object: None,
            graph: Some(GraphName::NamedNode(named(G1))),
        },
        Query {
            name: "missing_dictionary_term",
            subject: Some(subject(MISSING)),
            predicate: None,
            object: None,
            graph: None,
        },
        Query {
            name: "nonexistent_combination",
            subject: Some(subject(S1)),
            predicate: Some(named(P2)),
            object: Some(term(O1)),
            graph: None,
        },
    ]
}

fn canonical_lines(path: &std::path::Path) -> Vec<String> {
    let text = std::fs::read_to_string(path).expect("read match output");
    let mut lines: Vec<_> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    lines.sort_unstable();
    lines
}

#[tokio::test]
async fn legacy_sidecars_and_unified_v10_match_identically() {
    let dir = tempfile::tempdir().expect("tempdir");
    let legacy_path = dir.path().join("legacy.vortex");
    let unified_path = dir.path().join("unified.vortex");
    let config = CottasNativeConfig {
        row_group_size: 2,
        dict_row_group_size: 2,
        ..Default::default()
    };

    serialize_cottas_native_file::<SimpleDictionary, _>(
        stream::iter(fixture().into_iter().map(Ok)),
        &legacy_path,
        config.clone(),
    )
    .await
    .expect("serialize legacy sidecar artifact");
    serialize_cottas_native_quad_source_v10_file::<SimpleDictionary, _>(
        stream::iter(fixture().into_iter().map(Ok)),
        &unified_path,
        config,
    )
    .await
    .expect("serialize unified v10 artifact");

    for (index, query) in query_matrix().into_iter().enumerate() {
        let legacy_output = dir.path().join(format!("{index}-legacy.nq"));
        let unified_output = dir.path().join(format!("{index}-unified.nq"));
        match_cottas_native_file(
            &legacy_path,
            query.subject.as_ref(),
            query.predicate.as_ref(),
            query.object.as_ref(),
            query.graph.as_ref(),
            std::fs::File::create(&legacy_output).unwrap(),
            oxrdfio::RdfFormat::NQuads,
        )
        .await
        .unwrap_or_else(|error| panic!("legacy query {} failed: {error}", query.name));
        match_native_rdf_store_file(
            &unified_path,
            query.subject.as_ref(),
            query.predicate.as_ref(),
            query.object.as_ref(),
            query.graph.as_ref(),
            NativeIndexPolicy::Auto,
            std::fs::File::create(&unified_output).unwrap(),
            oxrdfio::RdfFormat::NQuads,
        )
        .await
        .unwrap_or_else(|error| panic!("unified query {} failed: {error}", query.name));
        let legacy = canonical_lines(&legacy_output);
        let unified = canonical_lines(&unified_output);
        assert_eq!(
            legacy, unified,
            "cross-format mismatch for query {}",
            query.name
        );
        if query.name == "fully_bound_duplicate" {
            assert_eq!(legacy.len(), 2, "duplicate multiplicity must be preserved");
        }
        if query.name == "named_graph" {
            assert_eq!(legacy.len(), 1, "named graph filter must select one row");
        }
    }
}

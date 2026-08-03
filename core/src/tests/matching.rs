use super::*;

// ─── 2) Core in-memory query semantics (no file I/O) ───────────────────

async fn run_match_pattern_test<B: VortexArrayBuilder>() {
    let q1 = make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o1",
        GraphName::DefaultGraph,
    );
    let q2 = make_quad(
        "http://example.org/s2",
        "http://example.org/p2",
        "o2",
        GraphName::DefaultGraph,
    );

    let arr = VortexRdfStore::build_vortex_array_with_builder::<B>(
        quad_stream(vec![q1.clone(), q2.clone()]),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .expect("build failed");
    let store = VortexRdfStore::from_built(arr).unwrap();

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 1);

    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), q1.subject.to_string());

    let p3 = NamedNode::new("http://example.org/p3").unwrap();
    let empty = store
        .match_pattern(None, Some(&p3), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_match_sorted_in_memory() {
    run_match_pattern_test::<SortedInMemoryBuilder>().await;
}
#[tokio::test]
async fn test_match_sorted_stream() {
    run_match_pattern_test::<SortedStreamBuilder>().await;
}
#[tokio::test]
async fn test_match_unsorted_stream() {
    run_match_pattern_test::<UnsortedStreamBuilder>().await;
}

#[tokio::test]
async fn test_match_typed_object_layout() {
    let q1 = make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o1",
        GraphName::DefaultGraph,
    );
    let q2 = make_quad(
        "http://example.org/s2",
        "http://example.org/p2",
        "o2",
        GraphName::DefaultGraph,
    );

    let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
        quad_stream(vec![q1.clone(), q2.clone()]),
        LayoutStrategy::TypedObject,
        vec![],
    )
    .await
    .expect("build failed");
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.layout(), LayoutStrategy::TypedObject);

    // Match by object literal — exercises the typed o_kind/o_value columns.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let matched = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 1);
    let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), q1.subject.to_string());
    assert_eq!(results[0].object.to_string(), q1.object.to_string());

    // Match by predicate.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let matched_p = store
        .match_pattern(None, Some(&p2), None, None)
        .await
        .unwrap();
    assert_eq!(matched_p.size().await.unwrap(), 1);

    // Non-existent object yields nothing.
    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    let empty = store
        .match_pattern(None, None, Some(&o3), None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_sorted_subject_binary_search() {
    // Multiple quads per subject: the binary-search fast path must return
    // the full [lo, hi) range for the matched subject.
    let mut quads: Vec<Quad> = Vec::new();
    for i in (0..10).rev() {
        for p in ["http://example.org/p1", "http://example.org/p2"] {
            quads.push(make_quad(
                &format!("http://example.org/s{:02}", i),
                p,
                "o",
                GraphName::DefaultGraph,
            ));
        }
    }

    let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
        quad_stream(quads),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
    let matched = store
        .match_pattern(Some(&s5), None, None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 2);

    // Subject + predicate narrows within the sliced range.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched_sp = store
        .match_pattern(Some(&s5), Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(matched_sp.size().await.unwrap(), 1);

    // Missing subject → empty via binary search short-circuit.
    let s99 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s99").unwrap());
    let empty = store
        .match_pattern(Some(&s99), None, None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// The base a view was derived from stays reachable: matching narrows a
/// selection, it does not throw the unselected rows away.
#[tokio::test]
async fn test_derived_view_does_not_lose_base_rows() {
    let quads: Vec<Quad> = (0..10)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                "http://example.org/p",
                &format!("object {}", i % 2),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let object = Term::Literal(Literal::new_simple_literal("object 0"));
    let matched = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 5);

    // Widening back out from the derived view reaches only what the view
    // selects (5 rows) — but the store it came from is untouched, and a
    // fresh match against it still sees all 10.
    let widened = matched.match_pattern(None, None, None, None).await.unwrap();
    assert_eq!(widened.size().await.unwrap(), 5);
    assert_eq!(store.size().await.unwrap(), 10);
}

// ─── 2b) File-backed matching matrix (by layout/builder) ───────────────

#[cfg(feature = "file-io")]
async fn run_match_pattern_file_test<B: VortexArrayBuilder>(layout: LayoutStrategy) {
    use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

    let q1 = make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o1",
        GraphName::DefaultGraph,
    );
    let q2 = make_quad(
        "http://example.org/s2",
        "http://example.org/p2",
        "o2",
        GraphName::DefaultGraph,
    );

    let mut bytes: Vec<u8> = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<B, _, _>(
        quad_stream(vec![q1.clone(), q2.clone()]),
        &mut bytes,
        layout,
        vec![],
    )
    .await
    .unwrap();

    let dir = std::env::temp_dir().join(format!(
        "vortex_rdf_match_file_test_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("match.vortex");
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 1);
    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), q1.subject.to_string());

    let p3 = NamedNode::new("http://example.org/p3").unwrap();
    let empty = store
        .match_pattern(None, Some(&p3), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "file-io")]
async fn run_match_pattern_file_typed_object_test<B: VortexArrayBuilder>() {
    use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

    let q1 = make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o1",
        GraphName::DefaultGraph,
    );
    let q2 = make_quad(
        "http://example.org/s2",
        "http://example.org/p2",
        "o2",
        GraphName::DefaultGraph,
    );

    let mut bytes: Vec<u8> = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<B, _, _>(
        quad_stream(vec![q1.clone(), q2.clone()]),
        &mut bytes,
        LayoutStrategy::TypedObject,
        vec![],
    )
    .await
    .unwrap();

    let dir = std::env::temp_dir().join(format!(
        "vortex_rdf_match_typed_file_test_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("match_typed.vortex");
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let filtered = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 1);
    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), q1.subject.to_string());

    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    let empty = store
        .match_pattern(None, None, Some(&o3), None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "file-io")]
async fn run_match_pattern_file_dictionary_test<B: VortexArrayBuilder>() {
    use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

    let quads = dictionary_test_quads();

    let mut bytes: Vec<u8> = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<B, _, _>(
        quad_stream(quads),
        &mut bytes,
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();

    let dir = std::env::temp_dir().join(format!(
        "vortex_rdf_match_dict_file_test_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("match_dict.vortex");
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 4);

    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    let empty = store
        .match_pattern(None, Some(&missing_p), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_sorted_in_memory() {
    run_match_pattern_file_test::<SortedInMemoryBuilder>(LayoutStrategy::Default).await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_sorted_stream() {
    run_match_pattern_file_test::<SortedStreamBuilder>(LayoutStrategy::Default).await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_unsorted_stream() {
    run_match_pattern_file_test::<UnsortedStreamBuilder>(LayoutStrategy::Default).await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_typed_sorted_in_memory() {
    run_match_pattern_file_typed_object_test::<SortedInMemoryBuilder>().await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_typed_sorted_stream() {
    run_match_pattern_file_typed_object_test::<SortedStreamBuilder>().await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_typed_unsorted_stream() {
    run_match_pattern_file_typed_object_test::<UnsortedStreamBuilder>().await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_dictionary_sorted_in_memory() {
    run_match_pattern_file_dictionary_test::<SortedInMemoryBuilder>().await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_dictionary_sorted_stream() {
    run_match_pattern_file_dictionary_test::<SortedStreamBuilder>().await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_dictionary_unsorted_stream() {
    run_match_pattern_file_dictionary_test::<UnsortedStreamBuilder>().await;
}

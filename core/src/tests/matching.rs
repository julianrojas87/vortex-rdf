use super::*;

// ─── 2) Core in-memory query semantics (no file I/O) ───────────────────

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
        dictionary_indexes(),
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

// ─── 2b) File-backed matching matrix (by layout/builder) ───────────────

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

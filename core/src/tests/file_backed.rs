use super::*;

// ─── 6) File-backed edge behavior ──────────────────────────────────────

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_filtered_size() {
    // 20 quads alternating between two predicates (10 each).
    let quads: Vec<Quad> = (0..20)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                &format!("http://example.org/p{}", i % 2),
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let bytes = quads_stream_to_vortex(quad_stream(quads)).await.unwrap();
    let path = std::env::temp_dir().join(format!(
        "vortex_rdf_size_test_{}.vortex",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 20);

    // size() on a filtered file-backed store must report the *filtered*
    // count, not file.row_count().
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 10);

    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_secondary_index_object_predicate() {
    let quads: Vec<Quad> = (0..30)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 5),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let mut bytes: Vec<u8> = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<UnsortedStreamBuilder, _, _>(
        quad_stream(quads),
        &mut bytes,
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();

    let path = std::env::temp_dir().join(format!(
        "vortex_rdf_file_index_match_{}.vortex",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_object = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_object.size().await.unwrap(), 6);

    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let by_predicate = store
        .match_pattern(None, Some(&p2), None, None)
        .await
        .unwrap();
    assert_eq!(by_predicate.size().await.unwrap(), 10);

    let by_both = store
        .match_pattern(None, Some(&p2), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_both.size().await.unwrap(), 2);

    let _ = std::fs::remove_file(&path);
}

/// Regression: predicate matches whose zone-map hits are NOT contiguous
/// (clusters at both ends of an s-sorted file, with a middle zone whose
/// stats exclude the predicate) must all survive the metadata row-range
/// pre-pass. A first-gap cutoff would silently drop the trailing cluster.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_non_contiguous_predicate_matches() {
    // Three 8192-row zones. The rare predicate appears only in the first
    // and last 64 rows; the middle zone holds exclusively the common
    // predicate (lexicographically greater), so its zone map excludes the
    // rare one and creates an interior prunable gap.
    const N: usize = 3 * 8192;
    const CLUSTER: usize = 64;
    let quads: Vec<Quad> = (0..N)
        .map(|i| {
            let p = if !(CLUSTER..N - CLUSTER).contains(&i) {
                "http://example.org/pAAA"
            } else {
                "http://example.org/pMMM"
            };
            make_quad(
                &format!("http://example.org/s{:06}", i),
                p,
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let mut bytes: Vec<u8> = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
        quad_stream(quads),
        &mut bytes,
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let path = std::env::temp_dir().join(format!(
        "vortex_rdf_noncontig_{}.vortex",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p_rare = NamedNode::new("http://example.org/pAAA").unwrap();
    let matched = store
        .match_pattern(None, Some(&p_rare), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 2 * CLUSTER);

    let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results.len(), 2 * CLUSTER);
    // Both the leading and the trailing cluster must be present.
    assert!(
        results
            .iter()
            .any(|q| q.subject.to_string() == "<http://example.org/s000000>")
    );
    assert!(
        results
            .iter()
            .any(|q| q.subject.to_string() == format!("<http://example.org/s{:06}>", N - 1))
    );

    let _ = std::fs::remove_file(&path);
}

/// Chained matches on a multi-zone file: a subject match narrows the store
/// to a row range from the zone maps; a subsequent indexed object match
/// must restrict its index row ids to that range (not discard the index),
/// and index-to-index chaining must intersect the two id lists.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_chained_subject_then_object_index() {
    const N: usize = 3 * 8192;
    let quads: Vec<Quad> = (0..N)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:06}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 5),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let mut bytes: Vec<u8> = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
        quad_stream(quads),
        &mut bytes,
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let path = std::env::temp_dir().join(format!(
        "vortex_rdf_chained_index_{}.vortex",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // The sorted subject column narrows a bound subject to a sub-range of
    // the file via zone-map pruning (row 12000 lives in the middle zone).
    let s_mid = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s012000").unwrap());
    let range = store.debug_subject_row_range(&s_mid).await.unwrap();
    let range = range.expect("sorted multi-zone file must narrow a bound subject");
    assert!(range.start <= 12000 && 12000 < range.end);
    assert!(
        range.end - range.start < N as u64,
        "envelope must exclude other zones"
    );

    // Subject match first (row range), then indexed object match: the
    // object index ids are restricted to the subject's range.
    let by_subject = store
        .match_pattern(Some(&s_mid), None, None, None)
        .await
        .unwrap();
    let o0 = Term::Literal(Literal::new_simple_literal("o0"));
    let chained = by_subject
        .match_pattern(None, None, Some(&o0), None)
        .await
        .unwrap();
    assert_eq!(chained.size().await.unwrap(), 1); // 12000 % 5 == 0
    let results: Vec<Quad> = chained.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].subject.to_string(),
        "<http://example.org/s012000>"
    );
    assert_eq!(results[0].object.to_string(), "\"o0\"");

    // Same chain with an object the subject's row doesn't carry.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let empty = by_subject
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);

    // Index-to-index chaining: object ids ∩ predicate ids.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let step1 = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    let step2 = step1
        .match_pattern(None, Some(&p2), None, None)
        .await
        .unwrap();
    let expected = (0..N).filter(|i| i % 5 == 1 && i % 3 == 2).count();
    assert_eq!(step2.size().await.unwrap(), expected);
    let results: Vec<Quad> = step2.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results.len(), expected);
    assert!(
        results.iter().all(|q| {
            q.predicate == "http://example.org/p2" && q.object.to_string() == "\"o1\""
        })
    );

    let _ = std::fs::remove_file(&path);
}

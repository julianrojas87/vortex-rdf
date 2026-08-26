//! Materialized and shared-term reads: `quads_vec` against the stream, and
//! `shared_quads_vec` / `shared_quad_chunks` under every layout and view.

use super::*;

// ─── Materialized reads ────────────────────────────────────────────────

/// `quads_vec` (the exact-size materialization) must yield the same quads,
/// in the same order, as one-at-a-time stream collection — for a plain
/// store, a matched view, and a mutated store with a tail.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_quads_vec_matches_stream_collection() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let extra = make_quad(
        "http://example.org/tail-subject",
        "http://example.org/p0",
        "tail object",
        GraphName::DefaultGraph,
    );
    let views = [
        ("full", store.clone()),
        (
            "matched",
            store
                .match_pattern(None, Some(&p0), None, None)
                .await
                .unwrap(),
        ),
        ("tailed", store.add_quad(extra).await.unwrap()),
    ];
    for (tag, view) in views {
        let streamed: Vec<Quad> = view.quads().unwrap().try_collect().await.unwrap();
        let collected = view.quads_vec().await.unwrap();
        assert_eq!(collected, streamed, "{tag}");
    }
}

// ─── Shared-term reads ─────────────────────────────────────────────────

/// `shared_quads_vec` yields `quads_vec`'s rows under every layout, for the
/// whole store and for a matched view, and its terms parse back to the same
/// quads.
#[tokio::test]
async fn test_shared_quads_match_quads_vec_across_layouts() {
    for layout in [
        LayoutStrategy::Default,
        LayoutStrategy::TypedObject,
        LayoutStrategy::Dictionary,
    ] {
        let arr = build_array::<SortedInMemoryBuilder>(
            quad_stream(dictionary_test_quads()),
            layout,
            vec![],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::from_built(arr).unwrap();
        assert_shared_matches_quads(&store, &format!("{layout:?}")).await;
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let matched = store
            .match_pattern(None, Some(&p0), None, None)
            .await
            .unwrap();
        assert_shared_matches_quads(&matched, &format!("{layout:?} matched")).await;
    }
}

/// Under the Dictionary layout a term repeated down a column is decoded once
/// and shared: rows carrying the same predicate or graph hold one allocation.
#[tokio::test]
async fn test_shared_quads_share_repeated_terms() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(modular_quads(64, 3, 4)),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let rows = store.shared_quads_vec().await.unwrap();
    assert_eq!(rows.len(), 64);
    for a in &rows {
        for b in &rows {
            if a.p == b.p {
                assert!(
                    std::sync::Arc::ptr_eq(&a.p, &b.p),
                    "equal predicates must share one allocation"
                );
            }
            if a.g == b.g {
                assert!(
                    std::sync::Arc::ptr_eq(&a.g, &b.g),
                    "equal graphs must share one allocation"
                );
            }
        }
    }
}

/// Every view shape a file-backed store can take — the whole store, an
/// index-served match, a subject-narrowed match, an append tail, a
/// tombstone — decodes the same rows shared as owned, and so does the
/// in-memory adoption's served match.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_shared_quads_match_quads_vec_on_views() {
    let quads = modular_quads(64, 3, 4);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let s1 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s01").unwrap());
    let extra = make_quad(
        "http://example.org/tail-subject",
        "http://example.org/p0",
        "tail object",
        GraphName::DefaultGraph,
    );
    let served = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert!(
        served.debug_has_serve_plan(),
        "the P match must be index-served"
    );
    let views = [
        ("full", store.clone()),
        ("served", served),
        (
            "subject",
            store
                .match_pattern(Some(&s1), None, None, None)
                .await
                .unwrap(),
        ),
        ("tailed", store.add_quad(extra).await.unwrap()),
        ("tombstoned", store.delete_quad(&quads[0]).await.unwrap()),
    ];
    for (tag, view) in views {
        assert_shared_matches_quads(&view, tag).await;
    }

    let resident =
        VortexRdfStore::from_parts(store.to_serializable_parts().await.unwrap()).unwrap();
    let served_mem = resident
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert!(served_mem.debug_has_serve_plan());
    assert_shared_matches_quads(&served_mem, "served in memory").await;
}

/// Shared rows own their strings: held across a compaction that rebuilds
/// the dictionary (renumbering every code), they still read the terms they
/// were decoded with.
#[tokio::test]
async fn test_shared_quads_survive_compaction() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(dictionary_test_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let held = store.shared_quads_vec().await.unwrap();
    let before = shared_tuple_rows(&held);

    // A term sorting before every existing one shifts every code.
    let early = make_quad(
        "http://example.org/a-first",
        "http://example.org/a-first",
        "a first",
        GraphName::DefaultGraph,
    );
    let compacted = store
        .add_quad(early)
        .await
        .unwrap()
        .compact()
        .await
        .unwrap();
    let probe = "<http://example.org/p0>";
    assert_ne!(
        store.code_read_snapshot().unwrap().encode(probe),
        compacted.code_read_snapshot().unwrap().encode(probe),
        "compaction must have renumbered the dictionary"
    );
    assert_eq!(compacted.size().await.unwrap(), 11);
    assert_eq!(shared_tuple_rows(&held), before);
}

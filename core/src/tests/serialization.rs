use super::*;
use crate::io::container;
use crate::store::builders::unsorted_stream::build_chunk_stream;

// ─── 8) The vortex-rdf.store.v1 wire contract ──────────────────────────
//
// Every test here asserts the same contract from a different starting
// state (tailed, tombstoned, file-backed, dictionary, unsorted
// multi-chunk): `to_bytes`/`from_bytes` must carry the store's index
// components and its sortedness provenance faithfully — rebuilt where a
// mutation invalidated them, never silently dropped, never over-claimed.

/// The tailed-serialization wart is fixed: a tailed store's `to_bytes`
/// REBUILDS its index components over the merged rows instead of silently
/// dropping them, so the round-tripped store keeps its index set — under the
/// Dictionary layout (fresh dictionary, code components) and the string
/// layouts alike.
#[tokio::test]
async fn test_tailed_store_serialization_keeps_indexes() {
    for layout in [LayoutStrategy::Dictionary, LayoutStrategy::Default] {
        let quads = modular_quads(12, 3, 4);
        let arr = build_array::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            layout,
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::from_built(arr).unwrap();

        // Tail with a brand-new term (no code in the cached dictionary).
        let added = store
            .add_quads([make_quad(
                "http://example.org/s99",
                "http://example.org/p1",
                "fresh object",
                GraphName::DefaultGraph,
            )])
            .await
            .unwrap();

        let bytes = added.to_bytes().await.unwrap();
        let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        assert_eq!(
            reread.indexes(),
            &[IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
            "{layout:?}: a tailed store's serialization must keep its indexes"
        );
        assert_eq!(reread.size().await.unwrap(), 13);

        // The rebuilt components actually answer: p1 is on base rows 1, 4,
        // 7, 10 plus the appended s99.
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let matched = reread
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        assert_eq!(matched.size().await.unwrap(), 5, "{layout:?}");
        let fresh = Term::Literal(Literal::new_simple_literal("fresh object"));
        assert_eq!(
            reread
                .match_pattern(None, None, Some(&fresh), None)
                .await
                .unwrap()
                .size()
                .await
                .unwrap(),
            1,
            "{layout:?}: the tail row survives the round-trip"
        );
    }
}

/// A mutated owner's serialization keeps BOTH provenance channels: tombstones
/// must not silently drop the index children (they are rebuilt over the
/// surviving rows) nor the subject sortedness (a gather preserves row
/// order).
#[tokio::test]
async fn test_tombstoned_store_serialization_keeps_indexes_and_sortedness() {
    let quads = modular_quads(12, 3, 4);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let s0 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s00").unwrap());
    let deleted = store
        .delete_matching(Some(&s0), None, None, None)
        .await
        .unwrap();
    assert_eq!(deleted.size().await.unwrap(), 11);

    let bytes = deleted.to_bytes().await.unwrap();
    // Sortedness provenance survives the tombstoned gather on the wire.
    let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
    assert!(
        quads_sorted,
        "tombstoned sorted store must keep quads_sorted"
    );
    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(
        reread.indexes(),
        &[IndexType::SecondaryByCopy],
        "tombstoned serialization must rebuild, not drop, the indexes"
    );
    assert_eq!(reread.size().await.unwrap(), 11);
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    // p0 was on rows 0, 3, 6, 9; row 0 was deleted.
    assert_eq!(
        reread
            .match_pattern(None, Some(&p0), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        3
    );
}

/// An unrefined file-backed store's `to_bytes` reads its index children
/// wholesale, so the byte round-trip keeps the index set (previously they
/// were silently dropped on the file→bytes path).
#[tokio::test]
async fn test_file_backed_to_bytes_keeps_indexes() {
    let (_dir, path) = write_store_file::<SortedStreamBuilder>(
        modular_quads(12, 3, 4),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);
    let bytes = store.to_bytes().await.unwrap();
    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(
        reread.indexes(),
        &[IndexType::SecondaryByCopy],
        "file → bytes must carry the index children across"
    );
    // And they route with correct results after the round-trip.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    assert_eq!(
        reread
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        4
    );
}

/// A sorted store's `quads_sorted` provenance survives the file → bytes
/// round-trip: the file arm of serialization re-stamps from the root
/// metadata, never from stats a multi-chunk scan lost.
#[tokio::test]
async fn test_file_backed_to_bytes_keeps_quads_sorted() {
    let quads: Vec<Quad> = (0..500)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:05}", i),
                "http://example.org/p",
                &format!("o{}", i),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let (_dir, path) =
        write_store_file::<SortedStreamBuilder>(quads, LayoutStrategy::Default, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let bytes = store.to_bytes().await.unwrap();
    let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
    assert!(
        quads_sorted,
        "file-backed to_bytes must carry the sorted provenance across"
    );
}

/// Serialization is deterministic: two `to_bytes` of the same store are
/// byte-identical (the invariant the dictionary-child chunk memo will rely
/// on when the pass-through strategy lands).
#[tokio::test]
async fn test_to_bytes_is_byte_stable() {
    let quads: Vec<Quad> = (0..500)
        .map(|i| {
            make_quad(
                &format!("http://example.org/subject/{i:04}"),
                &format!("http://example.org/predicate/{}", i % 8),
                &format!("object value {i:04}"),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let arr =
        build_array::<SortedStreamBuilder>(quad_stream(quads), LayoutStrategy::Dictionary, vec![])
            .await
            .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let bytes_a = store.to_bytes().await.unwrap();
    let bytes_b = store.to_bytes().await.unwrap();
    assert_eq!(bytes_a, bytes_b);
}

/// An unsorted multi-chunk build's index children are per-chunk sorted, not
/// globally sorted. Reading them back must NOT restore binary-search
/// routing: the descriptors carry `sorted: false`, in-memory resolution
/// declines, and the mask scan answers correctly. (Regression: an earlier
/// `from_bytes` unconditionally re-stamped these columns sorted, making this
/// match return 5 rows instead of 4.)
#[tokio::test]
async fn test_unsorted_multichunk_from_bytes_matches_correctly() {
    // Predicates interleave across chunks so per-chunk sorted != global.
    let quads: Vec<Quad> = (0..12)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s/{i:02}"),
                &format!("http://example.org/p/{}", i % 3),
                &format!("o{}", i),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let raws: Vec<crate::store::RawQuad> =
        quads.iter().map(crate::store::RawQuad::from_quad).collect();
    let built = build_chunk_stream(
        Box::new(futures::stream::iter(raws.into_iter().map(Ok))),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
        4,
    )
    .await
    .unwrap();
    let split = crate::store::indexes::tee::split_row_space(
        built.dtype,
        built.chunks,
        built.components_sorted,
    )
    .unwrap();
    let mut components = split.components;
    components.extend(built.components);
    let mut bytes: Vec<u8> = Vec::new();
    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut bytes,
        vortex_array::stream::ArrayStreamAdapter::new(split.quad_dtype, split.quad_chunks),
        container::default_child_strategy(),
        false,
        components,
    )
    .await
    .unwrap();

    let store = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);
    let p = NamedNode::new("http://example.org/p/1").unwrap();
    let matched: Vec<Quad> = store
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap()
        .quads()
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    // Ground truth: p/1 appears for i = 1, 4, 7, 10 -> 4 quads.
    assert_eq!(
        matched.len(),
        4,
        "index-routed match after from_bytes is wrong"
    );
}

/// The streaming twin of the runtime builder dispatch: writing through
/// `quads_stream_to_vortex_writer_with_strategy` must record each variant's
/// sortedness provenance on the wire — both sorted strategies stamp
/// `quads_sorted`, the unsorted one must not. A swapped arm in
/// `BuilderStrategy::build_vortex_stream` would pass every
/// compile-time-generic test and only surface downstream.
#[tokio::test]
async fn test_strategy_writer_dispatch_records_sortedness() {
    for (strategy, expect_sorted) in [
        (BuilderStrategy::UnsortedStream, false),
        (BuilderStrategy::SortedInMemory, true),
        (BuilderStrategy::SortedStream, true),
    ] {
        // Reverse subject order, so only a genuinely sorting builder may
        // claim sortedness.
        let mut quads = modular_quads(10, 3, 4);
        quads.reverse();
        let mut bytes: Vec<u8> = Vec::new();
        crate::io::quads_stream_to_vortex_writer_with_strategy(
            quad_stream(quads),
            &mut bytes,
            LayoutStrategy::Default,
            vec![],
            strategy,
        )
        .await
        .unwrap();
        let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
        assert_eq!(quads_sorted, expect_sorted, "{strategy}");
    }
}

/// `from_bytes` adopts index children *without* canonicalizing them: a
/// deferred component materializes on its first genuine use (here, an
/// index-routed probe) — and only the family the probe touches, so an
/// object family never queried stays un-executed. Serialization is such a
/// use too, and its output must not depend on the deferral state.
#[tokio::test]
async fn test_from_bytes_defers_index_child_materialization() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(modular_quads(12, 3, 4)),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let bytes = VortexRdfStore::from_built(arr)
        .unwrap()
        .to_bytes()
        .await
        .unwrap();

    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    for name in ["index:posg", "index:ospg", "index:ref-o", "index:ref-p"] {
        assert_eq!(
            reread.index_component_materialized(name),
            Some(false),
            "{name}: from_bytes must not materialize index children"
        );
    }

    // Serializing the deferred store materializes for the write without
    // changing the bytes: the wire form is deferral-independent.
    let deferred_bytes = reread.to_bytes().await.unwrap();

    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = reread
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    // p1 is on rows 1, 4, 7, 10 — the probe answered through the index.
    assert_eq!(matched.size().await.unwrap(), 4);
    assert_eq!(
        reread.index_component_materialized("index:posg"),
        Some(true),
        "the probed family materializes on first use"
    );
    for name in ["index:ospg", "index:ref-o", "index:ref-p"] {
        assert_eq!(
            reread.index_component_materialized(name),
            Some(false),
            "{name}: an unprobed component stays deferred"
        );
    }
    assert_eq!(
        reread.to_bytes().await.unwrap(),
        deferred_bytes,
        "serialization output must not depend on the deferral state"
    );
}

/// `from_parts` decodes a wire-encoded base's integer children to canonical
/// primitives — the form the match fast paths bind against — while carrying
/// the subject sortedness provenance honestly in both directions: a sorted
/// build's stamp survives the re-encoding, an unsorted build gains none.
#[tokio::test]
async fn test_from_parts_adopts_canonical_int_children() {
    let quads = modular_quads(12, 3, 4);

    // Sorted Dictionary build: code columns arrive wire-encoded after a
    // bytes round-trip, and the s column carries the sorted stamp.
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let bytes = VortexRdfStore::from_built(arr)
        .unwrap()
        .to_bytes()
        .await
        .unwrap();
    let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let parts = adopted.to_serializable_parts().await.unwrap();
    let store = VortexRdfStore::from_parts(parts).unwrap();
    assert!(
        store.debug_base_int_children_canonical(),
        "from_parts must decode integer children to canonical primitives"
    );
    assert!(
        store.debug_base_subject_sorted(),
        "the sorted build's subject stamp must survive the re-encoding"
    );
    for name in ["index:posg", "index:ospg"] {
        assert_eq!(
            store.debug_index_component_int_children_canonical(name),
            Some(true),
            "{name}: from_parts must adopt components in resident form"
        );
    }

    // The canonical base answers a fully-bound pattern: s01 p1 "object 1"
    // is row 1 exactly.
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s01").unwrap());
    let p = NamedNode::new("http://example.org/p1").unwrap();
    let o = Term::Literal(Literal::new_simple_literal("object 1"));
    assert_eq!(
        store
            .match_pattern(Some(&s), Some(&p), Some(&o), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );

    // Unsorted build: adoption must not invent a stamp the rows never earned.
    let arr = build_array::<UnsortedStreamBuilder>(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let bytes = VortexRdfStore::from_built(arr)
        .unwrap()
        .to_bytes()
        .await
        .unwrap();
    let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let parts = adopted.to_serializable_parts().await.unwrap();
    let store = VortexRdfStore::from_parts(parts).unwrap();
    assert!(store.debug_base_int_children_canonical());
    assert!(
        !store.debug_base_subject_sorted(),
        "an unsorted build must stay unstamped through adoption"
    );
}

/// A lean `from_bytes` store — wire-encoded base, deferred components — must
/// answer every pattern class exactly like the canonical `from_built` store
/// it was serialized from, with its sorted columns bound by encoded search
/// probes rather than the generic per-scalar kernel.
#[tokio::test]
async fn test_from_bytes_matches_equal_canonical_across_patterns() {
    // Repeated subjects (4 quads each) over enough rows that the sorted
    // subject column keeps a compressed wire form worth probing.
    let quads: Vec<Quad> = (0..20_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:05}", i / 4),
                &format!("http://example.org/p{}", i % 7),
                &format!("object {}", i % 11),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let canonical = VortexRdfStore::from_built(arr).unwrap();
    let bytes = canonical.to_bytes().await.unwrap();
    let lean = VortexRdfStore::from_bytes(&bytes).await.unwrap();

    assert!(canonical.debug_base_probe_resolvable());
    assert!(
        lean.debug_base_probe_resolvable(),
        "the lean base's sorted columns must bind encoded probes"
    );
    assert!(
        !lean.debug_base_int_children_canonical(),
        "fixture must exercise a wire-encoded base"
    );

    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s02500").unwrap());
    let s_absent = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/zz").unwrap());
    let p = NamedNode::new("http://example.org/p4").unwrap();
    let o = Term::Literal(Literal::new_simple_literal("object 7"));
    let g = GraphName::DefaultGraph;

    type Pattern<'a> = (
        &'a str,
        Option<&'a NamedOrBlankNode>,
        Option<&'a NamedNode>,
        Option<&'a Term>,
        Option<&'a GraphName>,
    );
    let patterns: [Pattern<'_>; 10] = [
        ("S", Some(&s), None, None, None),
        ("S absent", Some(&s_absent), None, None, None),
        ("P", None, Some(&p), None, None),
        ("O", None, None, Some(&o), None),
        ("G", None, None, None, Some(&g)),
        ("SP", Some(&s), Some(&p), None, None),
        ("PO", None, Some(&p), Some(&o), None),
        ("SPO", Some(&s), Some(&p), Some(&o), None),
        ("SPOG", Some(&s), Some(&p), Some(&o), Some(&g)),
        ("full", None, None, None, None),
    ];
    for (name, ps, pp, po, pg) in patterns {
        let want = canonical.match_pattern(ps, pp, po, pg).await.unwrap();
        let got = lean.match_pattern(ps, pp, po, pg).await.unwrap();
        assert_eq!(
            got.size().await.unwrap(),
            want.size().await.unwrap(),
            "size diverged on [{name}]"
        );
        assert_eq!(
            view_strings(&got).await,
            view_strings(&want).await,
            "results diverged on [{name}]"
        );
    }

    // The subject run itself: 4 quads per subject by construction.
    let s_view = lean.match_pattern(Some(&s), None, None, None).await.unwrap();
    assert_eq!(s_view.size().await.unwrap(), 4);
}

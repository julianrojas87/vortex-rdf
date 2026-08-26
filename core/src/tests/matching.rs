//! In-memory pattern matching: the per-layout probes, the prefix probe over
//! the (s, p, o, g) order, and view derivation.

use super::*;

// ─── In-memory matching ────────────────────────────────────────────────

async fn run_match_pattern_test<B: VortexArrayBuilder>() {
    let arr = build_array::<B>(quad_stream(two_quads()), LayoutStrategy::Default, vec![])
        .await
        .expect("build failed");
    probe_default(VortexRdfStore::from_built(arr).unwrap()).await;
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
async fn test_match_typed_object_layout() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(two_quads()),
        LayoutStrategy::TypedObject,
        vec![],
    )
    .await
    .expect("build failed");
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.layout(), LayoutStrategy::TypedObject);
    probe_typed_object(store).await;
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

    let arr =
        build_array::<SortedInMemoryBuilder>(quad_stream(quads), LayoutStrategy::Default, vec![])
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

// ─── Prefix probe: nested binary search over the (s, p, o, g) order ────

/// Four subjects × three predicates × two objects × two graphs, except that
/// the last subject carries only `p0` — so every predicate exists in the
/// dictionary while `s03 p1` matches nothing. Row `i` of the sorted base is
/// `((si * 3 + pi) * 2 + oi) * 2 + gi` for the first three subjects; `s03`
/// occupies rows 36..40.
fn prefix_quads() -> Vec<Quad> {
    let mut quads = Vec::new();
    for si in 0..4 {
        for pi in 0..3 {
            if si == 3 && pi > 0 {
                continue;
            }
            for oi in 0..2 {
                for gi in 0..2 {
                    let g = if gi == 0 {
                        GraphName::DefaultGraph
                    } else {
                        GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap())
                    };
                    quads.push(make_quad(
                        &format!("http://example.org/s{si:02}"),
                        &format!("http://example.org/p{pi}"),
                        &format!("o{oi}"),
                        g,
                    ));
                }
            }
        }
    }
    quads
}

fn prefix_terms() -> (NamedOrBlankNode, NamedNode, Term, GraphName) {
    (
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s01").unwrap()),
        NamedNode::new("http://example.org/p1").unwrap(),
        Term::Literal(Literal::new_simple_literal("o1")),
        GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
    )
}

/// Every bound prefix of the sort order resolves to one exact row range —
/// under the Dictionary layout, with and without secondary indexes (a
/// subject-narrowed view skips index routing, so the ranges must agree).
#[tokio::test]
async fn test_prefix_probe_resolves_bound_prefixes_to_ranges() {
    let (s, p, o, g) = prefix_terms();
    for indexes in [vec![], vec![IndexType::SecondaryByCopy]] {
        let arr = build_array::<SortedInMemoryBuilder>(
            quad_stream(prefix_quads()),
            LayoutStrategy::Dictionary,
            indexes.clone(),
        )
        .await
        .unwrap();
        let store = VortexRdfStore::from_built(arr).unwrap();

        let label = format!("{indexes:?}");
        let label = &label;
        let expect = |view: VortexRdfStore, range: std::ops::Range<u64>| async move {
            assert_eq!(
                view.debug_selection_range(),
                Some(range.clone()),
                "{label}: prefix must resolve to exactly the sorted run"
            );
            assert_eq!(
                view.size().await.unwrap(),
                (range.end - range.start) as usize
            );
            view
        };

        let by_s = store
            .match_pattern(Some(&s), None, None, None)
            .await
            .unwrap();
        expect(by_s, 12..24).await;
        let by_sp = store
            .match_pattern(Some(&s), Some(&p), None, None)
            .await
            .unwrap();
        let by_sp = expect(by_sp, 16..20).await;
        for q in by_sp.quads_vec().await.unwrap() {
            assert_eq!(q.subject.to_string(), s.to_string());
            assert_eq!(q.predicate, p);
        }
        let by_spo = store
            .match_pattern(Some(&s), Some(&p), Some(&o), None)
            .await
            .unwrap();
        expect(by_spo, 18..20).await;
        let by_spog = store
            .match_pattern(Some(&s), Some(&p), Some(&o), Some(&g))
            .await
            .unwrap();
        expect(by_spog, 19..20).await;
        // The default graph is a dictionary term like any other ("").
        let by_spo_default = store
            .match_pattern(Some(&s), Some(&p), Some(&o), Some(&GraphName::DefaultGraph))
            .await
            .unwrap();
        expect(by_spo_default, 18..19).await;
        // A run that narrows to nothing: `p1` exists, but not under `s03`.
        let s03 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s03").unwrap());
        let none = store
            .match_pattern(Some(&s03), Some(&p), None, None)
            .await
            .unwrap();
        assert_eq!(none.size().await.unwrap(), 0);
        assert!(none.quads_vec().await.unwrap().is_empty());
    }
}

/// The prefix ends at the first unbound role: a bound object or graph behind
/// an unbound predicate is left to the residual scan, whose ids are exact but
/// no longer a range.
#[tokio::test]
async fn test_prefix_probe_stops_at_the_first_unbound_role() {
    let (s, _, o, g) = prefix_terms();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(prefix_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let by_so = store
        .match_pattern(Some(&s), None, Some(&o), None)
        .await
        .unwrap();
    assert_eq!(by_so.debug_selection_range(), None);
    assert_eq!(by_so.size().await.unwrap(), 6); // 3 predicates × 2 graphs
    let by_sg = store
        .match_pattern(Some(&s), None, None, Some(&g))
        .await
        .unwrap();
    assert_eq!(by_sg.debug_selection_range(), None);
    assert_eq!(by_sg.size().await.unwrap(), 6); // 3 predicates × 2 objects
}

/// Under a string layout only the subject has a searchable column: the
/// prefix stops after it and the rest is answered by the residual scan.
#[tokio::test]
async fn test_prefix_probe_string_layout_narrows_by_subject_only() {
    let (s, p, o, _) = prefix_terms();
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let arr = build_array::<SortedInMemoryBuilder>(quad_stream(prefix_quads()), layout, vec![])
            .await
            .unwrap();
        let store = VortexRdfStore::from_built(arr).unwrap();
        let by_s = store
            .match_pattern(Some(&s), None, None, None)
            .await
            .unwrap();
        assert_eq!(by_s.debug_selection_range(), Some(12..24), "{layout:?}");
        let by_spo = store
            .match_pattern(Some(&s), Some(&p), Some(&o), None)
            .await
            .unwrap();
        assert_eq!(by_spo.debug_selection_range(), None, "{layout:?}");
        assert_eq!(by_spo.size().await.unwrap(), 2, "{layout:?}");
    }
}

/// A chained match nests inside the range the previous one left: the rows
/// of any sub-run of the sorted base are still in (s, p, o, g) order.
#[tokio::test]
async fn test_prefix_probe_chains_within_a_range() {
    let (s, p, o, _) = prefix_terms();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(prefix_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let by_s = store
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    let by_sp = by_s
        .match_pattern(Some(&s), Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(by_sp.debug_selection_range(), Some(16..20));
    let by_spo = by_sp
        .match_pattern(Some(&s), Some(&p), Some(&o), None)
        .await
        .unwrap();
    assert_eq!(by_spo.debug_selection_range(), Some(18..20));
    // Without the subject re-bound there is no prefix to probe; the residual
    // scan over the inherited range still answers exactly.
    let by_p_within = by_s
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_within.debug_selection_range(), None);
    assert_eq!(by_p_within.size().await.unwrap(), 4);
}

/// Rows without sorted provenance never see the probe: the arm declines and
/// the scan answers, so a foreign writer's unsorted file still matches
/// exactly.
#[tokio::test]
async fn test_prefix_probe_declines_an_unstamped_base() {
    let (s, p, o, _) = prefix_terms();
    let mut quads = prefix_quads();
    quads.reverse();
    let store = unstamped_store(&quads);
    assert!(!store.debug_base_subject_sorted());

    let by_s = store
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    assert_eq!(by_s.debug_selection_range(), None);
    assert_eq!(by_s.size().await.unwrap(), 12);
    let by_spo = store
        .match_pattern(Some(&s), Some(&p), Some(&o), None)
        .await
        .unwrap();
    assert_eq!(by_spo.debug_selection_range(), None);
    assert_eq!(by_spo.size().await.unwrap(), 2);
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

    let arr = build_array::<SortedInMemoryBuilder>(
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

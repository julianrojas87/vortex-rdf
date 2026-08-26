//! Secondary indexes on file-backed stores: tombstoning over a file, the
//! copy family's served reads by layout, run location through
//! dictionary-coded index children, and the reference family's file
//! resolution.

use super::*;

// ─── Deletes over a file ───────────────────────────────────────────────

/// A file's rows are tombstoned in place too, so deleting from a file-backed
/// store keeps its secondary indexes usable and never rewrites the file —
/// covering both the index-resolved delete path and the filter-scan one.
#[tokio::test]
async fn test_file_backed_delete_keeps_indexes() {
    let (_dir, path) = write_store_file(
        modular_quads(12, 2, 3),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // Index-resolved delete: "object 0" is indexed, so this resolves to
    // exact file row ids (i = 0, 3, 6, 9) without a filter scan.
    let object0 = Term::Literal(Literal::new_simple_literal("object 0"));
    let after = store
        .delete_matching(None, None, Some(&object0), None)
        .await
        .unwrap();
    assert_eq!(after.size().await.unwrap(), 8);
    assert_eq!(
        after.indexes(),
        &vec![IndexType::SecondaryByReference],
        "tombstoning a file row must not invalidate its index"
    );
    // The file on disk is unchanged — the source store still sees all 12.
    assert_eq!(store.size().await.unwrap(), 12);

    // The index still routes the lookup after the delete, and the
    // tombstoned rows must not come back.
    assert_eq!(
        after
            .match_pattern(None, None, Some(&object0), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    // Predicate p0 (i even: 0,2,4,6,8,10) had rows 0 and 6 tombstoned.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_p0 = after
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(by_p0.size().await.unwrap(), 4);
    assert_eq!(by_p0.quads().unwrap().count().await, 4);

    // Filter-scan delete: a subject isn't index-resolved, so this exercises
    // the pruning + filter evaluation path that resolves the doomed rows.
    let s05 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
    let after2 = after
        .delete_matching(Some(&s05), None, None, None)
        .await
        .unwrap();
    assert_eq!(after2.size().await.unwrap(), 7);
    // s05 is object "object 2" (5 % 3); that lookup now returns one fewer.
    let object2 = Term::Literal(Literal::new_simple_literal("object 2"));
    assert_eq!(
        after2
            .match_pattern(None, None, Some(&object2), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        3,
    );

    // A sort-only compaction reclaims every tombstone and drops the index.
    let compacted = after2.compact_with_indexes(vec![]).await.unwrap();
    assert_eq!(compacted.size().await.unwrap(), 7);
    assert!(compacted.indexes().is_empty());
    assert_eq!(compacted.quads().unwrap().count().await, 7);
}

// ─── SecondaryByCopy on a file ─────────────────────────────────────────

/// The file-backed copy index end to end: pattern shapes it accelerates,
/// copy-served `quads()` streams (including residual graph constraints and
/// tombstoned rows filtered through the family's rid column), and chained
/// matches falling back to row ids.
async fn run_copy_index_file_test(layout: LayoutStrategy, located: bool) {
    let graphs = [
        GraphName::NamedNode(NamedNode::new("http://example.org/g0").unwrap()),
        GraphName::NamedNode(NamedNode::new("http://example.org/g1").unwrap()),
    ];
    let quads = graph_modular_quads(30, 2, 3, 5, &graphs);

    let (_dir, path) =
        write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);

    // Predicate-bound: i ≡ 1 (mod 3), served from the POSG family. On a
    // located resolution (sorted dictionary-code copies) the small run's
    // ids resolve eagerly by rid point reads; otherwise the rid scan stays
    // pending until `size` — the served `quads()` never runs it either way.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    assert_eq!(by_p.debug_selection_pending(), !located);
    assert_eq!(by_p.size().await.unwrap(), 10);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );
    // A base-order gather cannot ride the plan: it materializes the pending
    // ids (the deferred index-child scan) and must agree with the served read.
    assert_eq!(by_p.selected_rows().await.unwrap().len(), 10);

    // Object-bound: i ≡ 2 (mod 5), served from the OSPG family.
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert!(by_o.debug_has_serve_plan());
    assert_eq!(by_o.debug_selection_pending(), !located);
    assert_eq!(by_o.size().await.unwrap(), 6);
    assert_eq!(
        view_strings(&by_o).await,
        expected_strings(&quads, |i| i % 5 == 2)
    );

    // Predicate and object bound: one (p, o) prefix resolution —
    // i ≡ 1 (mod 3) ∧ i ≡ 1 (mod 5) ⇔ i ≡ 1 (mod 15).
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_po.size().await.unwrap(), 2);
    assert_eq!(
        view_strings(&by_po).await,
        expected_strings(&quads, |i| i % 15 == 1)
    );

    // A residual graph constraint rides the copy-served scan's filter; the
    // selection's ids cover the resolved predicate only — `size` evaluates
    // the residual on top of them (eager on a located resolution, pending
    // otherwise).
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let by_pg = store
        .match_pattern(None, Some(&p2), None, Some(&graphs[0]))
        .await
        .unwrap();
    assert!(by_pg.debug_has_serve_plan());
    assert_eq!(by_pg.debug_selection_pending(), !located);
    assert_eq!(by_pg.size().await.unwrap(), 5);
    assert_eq!(
        view_strings(&by_pg).await,
        expected_strings(&quads, |i| i % 3 == 2 && i % 2 == 0)
    );

    // Chaining a second match narrows the first view's row ids (the copy
    // plan is dropped — its filter no longer selects exactly the rows).
    let chained = by_p
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert!(!chained.debug_has_serve_plan());
    assert!(!chained.debug_selection_pending());
    assert_eq!(
        view_strings(&chained).await,
        expected_strings(&quads, |i| i % 3 == 1 && i % 5 == 1)
    );

    // A term the store has never seen short-circuits to empty.
    let missing = NamedNode::new("http://example.org/nope").unwrap();
    let none = store
        .match_pattern(None, Some(&missing), None, None)
        .await
        .unwrap();
    assert_eq!(none.size().await.unwrap(), 0);

    // A term the store knows — but never as a predicate — probes the index
    // child and finds nothing.
    let subject_as_p = NamedNode::new("http://example.org/s00").unwrap();
    let zero = store
        .match_pattern(None, Some(&subject_as_p), None, None)
        .await
        .unwrap();
    // A located resolution proves the emptiness at match time and
    // short-circuits; an unlocated one only discovers it at the consumer.
    assert_eq!(zero.debug_selection_pending(), !located);
    assert_eq!(view_strings(&zero).await, Vec::<String>::new());
    assert_eq!(zero.size().await.unwrap(), 0);

    // A tombstoned row must vanish from copy-served streams too: the scan
    // reads copy rows, so the delete reaches it through the rid column.
    let deleted = store.delete_quad(&quads[4]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_after.size().await.unwrap(), 9);
    assert_eq!(
        view_strings(&by_p_after).await,
        expected_strings(&quads, |i| i % 3 == 1 && i != 4)
    );

    // Deleting by a served pattern: the matcher's doomed view carries pending
    // ids, which the delete materializes into tombstones.
    let wiped = deleted
        .delete_matching(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(wiped.size().await.unwrap(), 20);
    let by_p_wiped = wiped
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_wiped.size().await.unwrap(), 0);
    assert_eq!(view_strings(&by_p_wiped).await, Vec::<String>::new());
}

/// A located run wider than the point-read cap keeps the deferred contract:
/// the rid scan stays pending until a consumer needs the selection, and the
/// served stream reads the run by range in several row-count splits — one
/// decoded chunk each — agreeing row for row with the primary read,
/// tombstones inside the run included.
#[tokio::test]
async fn test_copy_index_file_serving_wide_located_run_stays_pending() {
    // 10,000 rows per predicate: several served splits on any host (the
    // split policy floors at 1,024 rows), with the child's lead run longer
    // than the writer's first 8,192-row block, so its `p` column stays a
    // plain flat leaf (the dictionary-coded shape has its own test below).
    let quads = graph_modular_quads(30_000, 5, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    // POSG order puts p1's run right after p0's 10,000 rows.
    assert_eq!(
        by_p.debug_serve_row_range(),
        Some(10_000..20_000),
        "the run must be located for the range scan to serve it"
    );
    assert!(
        by_p.debug_selection_pending(),
        "a 10,000-row run exceeds the point-read cap and must stay deferred"
    );
    assert_eq!(by_p.size().await.unwrap(), 10_000);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );
    // The served stream arrives one chunk per row-count split of the run
    // (plus the empty tail item), never as the single chunk the child's own
    // split would make of it.
    let chunks: Vec<usize> = by_p
        .shared_quad_chunks()
        .unwrap()
        .map(|chunk| chunk.len())
        .filter(|len| futures::future::ready(*len > 0))
        .collect()
        .await;
    assert!(
        chunks.len() >= 2,
        "a wide located run is served in several splits, got {chunks:?}"
    );
    assert_eq!(chunks.iter().sum::<usize>(), 10_000);

    // A tombstone inside the run leaves every served split through the
    // family's rid column, and the count agrees.
    let deleted = store.delete_quad(&quads[1_501]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p_after.debug_has_serve_plan());
    assert_eq!(by_p_after.size().await.unwrap(), 9_999);
    assert_eq!(
        view_strings(&by_p_after).await,
        expected_strings(&quads, |i| i % 3 == 1 && i != 1_501)
    );
}

/// Index-child columns the writer dictionary-encodes at the layout level —
/// a lead column whose first block holds a few predicates, a second key with
/// a handful of objects — still locate their runs: the lead search, the
/// windowed second-key search and the point reads all probe through the
/// codes leaves, so every served shape reads by range and agrees with the
/// primary read.
#[tokio::test]
async fn test_copy_index_file_locates_dictionary_coded_columns() {
    // Three predicates of 3,000 rows: the POSG child's first block holds all
    // three, so its `p` column is dictionary-coded; `o` (seven objects) is
    // dictionary-coded in both families.
    let quads = graph_modular_quads(9_000, 4, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // Predicate-bound: the POSG lead run, located through the dictionary-coded
    // `p` column and served by range in several splits.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    assert_eq!(by_p.debug_serve_row_range(), Some(3_000..6_000));
    assert!(by_p.debug_selection_pending());
    assert_eq!(by_p.size().await.unwrap(), 3_000);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );
    let chunks: Vec<usize> = by_p
        .shared_quad_chunks()
        .unwrap()
        .map(|chunk| chunk.len())
        .filter(|len| futures::future::ready(*len > 0))
        .collect()
        .await;
    assert!(
        chunks.len() >= 2,
        "served in several splits, got {chunks:?}"
    );

    // Predicate and object bound: the windowed second-key search inside the
    // lead run, through the dictionary-coded `o` column — `i ≡ 1 (mod 3)`
    // and `i ≡ 1 (mod 7)` is the second of p1's seven object sub-runs.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert!(by_po.debug_has_serve_plan());
    assert_eq!(by_po.debug_serve_row_range(), Some(3_429..3_858));
    assert_eq!(by_po.size().await.unwrap(), 429);
    assert_eq!(
        view_strings(&by_po).await,
        expected_strings(&quads, |i| i % 3 == 1 && i % 7 == 1)
    );

    // Object-bound: the OSPG lead run through its dictionary-coded `o`.
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert!(by_o.debug_has_serve_plan());
    assert_eq!(by_o.debug_serve_row_range(), Some(2_572..3_858));
    assert_eq!(by_o.size().await.unwrap(), 1_286);
    assert_eq!(
        view_strings(&by_o).await,
        expected_strings(&quads, |i| i % 7 == 2)
    );

    // A term the store knows but never as a predicate: the located run is
    // empty, proving the pattern matches nothing at match time.
    let subject_as_p = NamedNode::new("http://example.org/s0000").unwrap();
    let zero = store
        .match_pattern(None, Some(&subject_as_p), None, None)
        .await
        .unwrap();
    assert!(!zero.debug_selection_pending());
    assert_eq!(zero.size().await.unwrap(), 0);

    // A tombstone inside a served run leaves every split through the rid
    // column.
    let deleted = store.delete_quad(&quads[1_501]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_after.size().await.unwrap(), 2_999);
    assert_eq!(
        view_strings(&by_p_after).await,
        expected_strings(&quads, |i| i % 3 == 1 && i != 1_501)
    );
}

#[tokio::test]
async fn test_copy_index_file_default() {
    run_copy_index_file_test(LayoutStrategy::Default, false).await;
}

#[tokio::test]
async fn test_copy_index_file_typed_object() {
    run_copy_index_file_test(LayoutStrategy::TypedObject, false).await;
}

#[tokio::test]
async fn test_copy_index_file_dictionary() {
    run_copy_index_file_test(LayoutStrategy::Dictionary, true).await;
}

// ─── SecondaryByReference on a file ────────────────────────────────────

/// The file-backed reference index end to end: on a sorted dictionary-code
/// child every covered shape locates its matched run through the value
/// column's chunk probes (small runs read their row ids point by point, wide
/// ones by a scan restricted to the run); on a string-valued child the same
/// shapes decline the location and answer through the pushed-down scan. Both
/// must agree, row for row, with the in-memory store over the same quads.
async fn run_reference_index_file_test(layout: LayoutStrategy, located: bool) {
    // 900 quads: 300 per predicate (a run wider than the point-read cap) and
    // ~129 per object (one narrow enough to point-read).
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);

    let (_dir, path) =
        write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByReference]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByReference]);

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));

    // Object-bound: 129 rows — a located run inside the point-read cap.
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(
        store
            .debug_reference_index_located_run(None, Some(&o2))
            .await
            .unwrap()
            .map(|r| (r.end - r.start) as usize),
        located.then_some(129),
        "object location engages exactly on the code-valued child"
    );
    assert_eq!(by_o.size().await.unwrap(), 129);
    assert_eq!(
        view_strings(&by_o).await,
        expected_strings(&quads, |i| i % 7 == 2)
    );
    // This index serves no quads: the reads gather the primary columns.
    assert!(!by_o.debug_has_serve_plan());

    // Predicate-bound: 300 rows — a located run past the cap, whose ids come
    // from the range-restricted rid scan.
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .debug_reference_index_located_run(Some(&p1), None)
            .await
            .unwrap()
            .map(|r| (r.end - r.start) as usize),
        located.then_some(300),
    );
    assert_eq!(by_p.size().await.unwrap(), 300);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );

    // Predicate and object bound: this index probes the object column only,
    // leaving the predicate as a residual filter over the located rows —
    // i ≡ 2 (mod 7) ∧ i ≡ 1 (mod 3) ⇔ i ≡ 16 (mod 21).
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(
        view_strings(&by_po).await,
        expected_strings(&quads, |i| i % 21 == 16)
    );

    // A term the store has never seen short-circuits before any location.
    let missing = Term::Literal(Literal::new_simple_literal("nope"));
    let none = store
        .match_pattern(None, None, Some(&missing), None)
        .await
        .unwrap();
    assert_eq!(none.size().await.unwrap(), 0);

    // A term the store knows — but never as an object — locates an empty run
    // (or scans to the same conclusion) and answers empty.
    let subject_as_o = Term::NamedNode(NamedNode::new("http://example.org/s0000").unwrap());
    assert_eq!(
        store
            .debug_reference_index_located_run(None, Some(&subject_as_o))
            .await
            .unwrap()
            .map(|r| r.is_empty()),
        located.then_some(true),
    );
    let zero = store
        .match_pattern(None, None, Some(&subject_as_o), None)
        .await
        .unwrap();
    assert_eq!(view_strings(&zero).await, Vec::<String>::new());
    assert_eq!(zero.size().await.unwrap(), 0);

    // Tombstones ride the resolved ids: a deleted row leaves the run.
    let deleted = store.delete_quad(&quads[2]).await.unwrap();
    let after = deleted
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(after.size().await.unwrap(), 128);
    assert_eq!(
        view_strings(&after).await,
        expected_strings(&quads, |i| i % 7 == 2 && i != 2)
    );

    // Chaining composes the resolutions the same way either path resolves
    // them.
    let chained = by_p
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(
        view_strings(&chained).await,
        expected_strings(&quads, |i| i % 21 == 16)
    );
}

#[tokio::test]
async fn test_reference_index_file_dictionary() {
    run_reference_index_file_test(LayoutStrategy::Dictionary, true).await;
}

#[tokio::test]
async fn test_reference_index_file_default() {
    run_reference_index_file_test(LayoutStrategy::Default, false).await;
}

#[tokio::test]
async fn test_reference_index_file_typed_object() {
    run_reference_index_file_test(LayoutStrategy::TypedObject, false).await;
}

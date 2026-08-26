//! Secondary indexes on in-memory stores: the layout × index-family matrix,
//! index survival across views, deletes and compaction, and the copy
//! family's serving path.

use super::*;

// ─── Secondary index behavior ──────────────────────────────────────────

/// The same index requested twice contributes its children exactly once,
/// under every layout: the quad rows carry the layout's primary columns and
/// nothing else, the roster is deduplicated, and where the layout has a
/// single `o` column each child's `val` column shares its dtype.
#[tokio::test]
async fn test_duplicate_index_requests_are_deduplicated() {
    let layouts: [(LayoutStrategy, &[&str]); 3] = [
        (LayoutStrategy::Default, &["s", "p", "o", "g"]),
        (
            LayoutStrategy::TypedObject,
            &["s", "p", "o_kind", "o_value", "o_datatype", "o_lang", "g"],
        ),
        (LayoutStrategy::Dictionary, &["s", "p", "o", "g"]),
    ];
    for (layout, primary_columns) in layouts {
        let arr = build_array::<SortedInMemoryBuilder>(
            quad_stream(two_quads()),
            layout,
            vec![
                IndexType::SecondaryByReference,
                IndexType::SecondaryByReference,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("{layout:?}: build failed: {e}"));

        let vortex_array::dtype::DType::Struct(fields, _) = arr.array.dtype() else {
            panic!("{layout:?}: expected StructArray dtype");
        };
        let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
        assert_eq!(names, primary_columns, "{layout:?}");
        assert_eq!(
            component_names(&arr),
            ["index:ref-o", "index:ref-p"],
            "{layout:?}"
        );
        if let Some(o_dtype) = fields.field("o") {
            for component in &arr.components {
                let rows = component.rows().unwrap();
                let vortex_array::dtype::DType::Struct(child_fields, _) = rows.dtype() else {
                    panic!("{layout:?}: expected a struct child");
                };
                assert_eq!(
                    child_fields.field("val"),
                    Some(o_dtype.clone()),
                    "{layout:?}"
                );
            }
        }

        // Index-routed matching still works.
        let store = VortexRdfStore::from_built(arr).unwrap();
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let matched = store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        assert_eq!(matched.size().await.unwrap(), 1, "{layout:?}");
    }
}

/// A store derived by matching keeps its indexes, because a view narrows a
/// selection over the base rather than rewriting rows — so the components'
/// `rid` values still address the data. This is what lets a chained match keep
/// routing through the index instead of degrading to a scan.
#[tokio::test]
async fn test_in_memory_derived_view_keeps_indexes() {
    let quads = modular_quads(24, 3, 4);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.indexes(), &vec![IndexType::SecondaryByReference]);

    // Match on the object index: 24 quads over 4 objects ⇒ 6 rows.
    let object = Term::Literal(Literal::new_simple_literal("object 1"));
    let matched = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 6);
    assert_eq!(
        matched.indexes(),
        &vec![IndexType::SecondaryByReference],
        "a derived view must keep the base's indexes"
    );

    // Chain a second, index-routed match onto the derived view. Of those 6
    // rows (i = 1, 5, 9, 13, 17, 21), the ones with predicate p1 are
    // i ≡ 1 (mod 3): 1, 13 — the intersection of two index lookups.
    let predicate = NamedNode::new("http://example.org/p1").unwrap();
    let chained = matched
        .match_pattern(None, Some(&predicate), None, None)
        .await
        .unwrap();
    assert_eq!(
        subjects_of(&chained).await,
        vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string()
        ]
    );
}

/// `compact_with_indexes` gathers the live rows and rebuilds the requested
/// indexes over the fresh `0..n` row order — so an independent, compacted
/// store keeps routing through its index instead of degrading to a full
/// scan. It also lets a store be re-indexed: the requested set is what the
/// result carries, whatever the source had.
#[tokio::test]
async fn test_compact_with_indexes_rebuilds() {
    let quads = modular_quads(24, 3, 4);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // A view over the object index: i = 1, 5, 9, 13, 17, 21 ⇒ 6 rows.
    let object = Term::Literal(Literal::new_simple_literal("object 1"));
    let view = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(view.size().await.unwrap(), 6);

    // An empty index set drops the index; a non-empty one rebuilds it.
    assert!(
        view.compact_with_indexes(vec![])
            .await
            .unwrap()
            .indexes()
            .is_empty()
    );
    let indexed = view
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(indexed.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(indexed.size().await.unwrap(), 6);

    // The rebuilt index routes over the new row order: of those 6 rows,
    // predicate p1 is i ≡ 1 (mod 3) ⇒ 1, 13. The result must be exact and
    // the store independent of its source.
    let predicate = NamedNode::new("http://example.org/p1").unwrap();
    let routed = indexed
        .match_pattern(None, Some(&predicate), None, None)
        .await
        .unwrap();
    assert_eq!(
        subjects_of(&routed).await,
        vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string(),
        ]
    );
    assert_eq!(store.size().await.unwrap(), 24, "source untouched");

    // Re-indexing from nothing: an empty set drops every index,
    // and a store built without indexes gains one it never had.
    let bare = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let bare = VortexRdfStore::from_built(bare).unwrap();
    assert!(bare.indexes().is_empty());
    assert!(
        bare.compact_with_indexes(vec![])
            .await
            .unwrap()
            .indexes()
            .is_empty()
    );
    let reindexed = bare
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(reindexed.indexes(), &[IndexType::SecondaryByReference]);
    let routed = reindexed
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(routed.size().await.unwrap(), 6);
}

/// The index rebuild reads its value columns from the materialized array in
/// each layout's own representation: `o`/`p` strings (Default), u32 codes
/// (Dictionary), and the object term recomposed from typed sub-columns
/// (TypedObject). Exercise all three end-to-end.
async fn run_compact_with_indexes_layout(layout: LayoutStrategy) {
    let quads = modular_quads(24, 3, 4);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        layout,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let object = Term::Literal(Literal::new_simple_literal("object 1"));
    let indexed = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap()
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(indexed.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(indexed.size().await.unwrap(), 6);

    // Route through both the rebuilt object and predicate columns.
    let predicate = NamedNode::new("http://example.org/p1").unwrap();
    assert_eq!(
        subjects_of(
            &indexed
                .match_pattern(None, Some(&predicate), None, None)
                .await
                .unwrap()
        )
        .await,
        vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string(),
        ]
    );
    assert_eq!(
        indexed
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        6,
    );
}

/// Compacting down to nothing still builds the requested indexes: a store
/// whose rows all went away keeps the roster (and, under the Dictionary
/// layout, its children's code dtypes) a non-empty compaction would have
/// given it, so the result is a usable indexed store rather than one that
/// silently lost its indexes.
async fn run_compact_to_empty_keeps_indexes(layout: LayoutStrategy) {
    let quads = modular_quads(8, 3, 4);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        layout,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // A pattern nothing matches: the view is empty, and so is its compaction.
    let absent = Term::Literal(Literal::new_simple_literal("no such object"));
    let empty = store
        .match_pattern(None, None, Some(&absent), None)
        .await
        .unwrap()
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
    assert_eq!(empty.indexes(), &[IndexType::SecondaryByReference]);

    // The empty children are still routable — a probe answers empty rather
    // than erroring or falling back.
    let object = Term::Literal(Literal::new_simple_literal("object 1"));
    assert_eq!(
        empty
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0,
    );
}

#[tokio::test]
async fn test_compact_to_empty_keeps_indexes_default() {
    run_compact_to_empty_keeps_indexes(LayoutStrategy::Default).await;
}

#[tokio::test]
async fn test_compact_to_empty_keeps_indexes_dictionary() {
    run_compact_to_empty_keeps_indexes(LayoutStrategy::Dictionary).await;
}

#[tokio::test]
async fn test_compact_with_indexes_dictionary() {
    run_compact_with_indexes_layout(LayoutStrategy::Dictionary).await;
}

#[tokio::test]
async fn test_compact_with_indexes_typed_object() {
    run_compact_with_indexes_layout(LayoutStrategy::TypedObject).await;
}

/// Deleting tombstones rows instead of rewriting them, so base row ids —
/// and the secondary index built against them — survive the delete.
#[tokio::test]
async fn test_delete_keeps_indexes_usable() {
    let quads = modular_quads(12, 2, 3);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Drop one quad: subject s0, which also carries object "object 0".
    let after = store.delete_quad(&quads[0]).await.unwrap();
    assert_eq!(after.size().await.unwrap(), 11);
    assert_eq!(
        after.indexes(),
        &vec![IndexType::SecondaryByReference],
        "tombstoning must not invalidate the index"
    );
    // The source store is untouched — mutations return a new store.
    assert_eq!(store.size().await.unwrap(), 12);

    // "object 0" is on i = 0, 3, 6, 9; the index still routes the lookup,
    // and the tombstoned row must not come back.
    let object = Term::Literal(Literal::new_simple_literal("object 0"));
    let matched = after
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 3);
    assert_eq!(
        subjects_of(&matched).await,
        vec![
            "<http://example.org/s03>".to_string(),
            "<http://example.org/s06>".to_string(),
            "<http://example.org/s09>".to_string()
        ]
    );

    // A sort-only compaction reclaims the tombstoned row (and drops the index).
    let compacted = after.compact_with_indexes(vec![]).await.unwrap();
    assert_eq!(compacted.size().await.unwrap(), 11);
    assert!(compacted.indexes().is_empty());
}

/// One cell of the layout × index matrix: build, check which carrier the
/// requested index families ride in, and run the shared match battery.
async fn run_index_matrix_cell<B: VortexArrayBuilder>(
    builder_name: &'static str,
    layout_name: &'static str,
    layout: LayoutStrategy,
    index_name: &'static str,
    indexes: Indexes,
    quads: Vec<Quad>,
) {
    let arr = build_array::<B>(
        quad_stream(quads.clone()),
        layout,
        indexes.clone(),
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "build failed for builder={builder_name} layout={layout_name} indexes={index_name}: {e}"
        )
    });

    if let vortex_array::dtype::DType::Struct(fields, _) = arr.array.dtype() {
        let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
        // Whatever the builder or layout, index data never rides in the quad
        // rows: those carry the layout's primary columns and nothing else.
        assert!(
            names.iter().all(|n| !n.starts_with("_idx")),
            "index columns leaked into the quad rows for builder={builder_name} layout={layout_name} indexes={index_name}",
        );
        let expect_ref = indexes.contains(&IndexType::SecondaryByReference);
        let expect_copy = indexes.contains(&IndexType::SecondaryByCopy);
        // Each requested family contributes its children, all or nothing.
        let components = component_names(&arr);
        let has = |roster: [&str; 2]| {
            let present = roster.iter().filter(|c| components.contains(c)).count();
            assert!(
                present == 0 || present == roster.len(),
                "partial component roster {components:?} for builder={builder_name} layout={layout_name} indexes={index_name}",
            );
            present == roster.len()
        };
        assert!(
            has(["index:ref-o", "index:ref-p"]) == expect_ref
                && has(["index:posg", "index:ospg"]) == expect_copy,
            "index component mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
        );
    } else {
        panic!(
            "expected StructArray dtype for builder={builder_name} layout={layout_name} indexes={index_name}"
        );
    }

    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(
        store.size().await.unwrap(),
        quads.len(),
        "size mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_pred = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(
        by_pred.size().await.unwrap(),
        8,
        "predicate match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let by_obj = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(
        by_obj.size().await.unwrap(),
        6,
        "object match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_both = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(
        by_both.size().await.unwrap(),
        2,
        "combined match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    let empty = store
        .match_pattern(None, Some(&missing_p), None, None)
        .await
        .unwrap();
    assert_eq!(
        empty.size().await.unwrap(),
        0,
        "missing-term match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );
}

/// The 3-layout × 4-index-config matrix for one builder. The 12 cells are
/// independent in-memory builds, so they are spawned and joined rather than
/// run serially — per-cell failure context stays in each cell's panics,
/// re-raised through the join.
async fn run_index_matrix_test<B: VortexArrayBuilder + 'static>(builder_name: &'static str) {
    let quads = modular_quads(24, 3, 4);

    let layouts = [
        ("default", LayoutStrategy::Default),
        ("typed-object", LayoutStrategy::TypedObject),
        ("dictionary", LayoutStrategy::Dictionary),
    ];
    let index_configs: [(&'static str, Indexes); 4] = [
        ("none", vec![]),
        (
            "secondary-by-reference",
            vec![IndexType::SecondaryByReference],
        ),
        ("secondary-by-copy", vec![IndexType::SecondaryByCopy]),
        (
            "both",
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        ),
    ];

    let mut cells = Vec::new();
    for (layout_name, layout) in layouts {
        for (index_name, indexes) in &index_configs {
            cells.push(tokio::spawn(run_index_matrix_cell::<B>(
                builder_name,
                layout_name,
                layout,
                index_name,
                indexes.clone(),
                quads.clone(),
            )));
        }
    }
    if let Err(e) = futures::future::try_join_all(cells).await {
        std::panic::resume_unwind(e.into_panic());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_matrix_sorted_in_memory() {
    run_index_matrix_test::<SortedInMemoryBuilder>("SortedInMemoryBuilder").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_matrix_sorted_stream() {
    run_index_matrix_test::<SortedStreamBuilder>("SortedStreamBuilder").await;
}

// ─── SecondaryByCopy: sorted full-copy index ───────────────────────────

/// The in-memory copy index's serving path: predicate / object /
/// predicate+object matches read the matched quads straight from the copy
/// family's contiguous run (a plain slice of the base, no row-id gather),
/// while a residual graph constraint or a chained match — which force a mask
/// scan or narrow the selection further — fall back to the row ids. Serving
/// applies exactly when the index fully resolves the pattern, which is also
/// exactly when no gather would otherwise happen.
#[tokio::test]
async fn test_in_memory_copy_index_serving() {
    let graphs = [
        GraphName::NamedNode(NamedNode::new("http://example.org/g0").unwrap()),
        GraphName::NamedNode(NamedNode::new("http://example.org/g1").unwrap()),
    ];
    let quads = graph_modular_quads(30, 2, 3, 5, &graphs);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);

    // Predicate-bound: served from the POSG family's contiguous run — the
    // served `quads()` and the row-id `size()` must agree. The resolution's
    // exact ids stay pending (`size` answers from the run's width; only a
    // consumer that needs the ids decodes them).
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    assert!(by_p.debug_selection_pending());
    assert_eq!(by_p.size().await.unwrap(), 10);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );
    // The derived view keeps the index.
    assert_eq!(by_p.indexes(), &[IndexType::SecondaryByCopy]);
    // A base-order gather cannot ride the plan: it materializes the pending
    // ids and must agree with the served read.
    assert_eq!(by_p.selected_rows().await.unwrap().len(), 10);

    // Object-bound: served from the OSPG family.
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert!(by_o.debug_has_serve_plan());
    assert!(by_o.debug_selection_pending());
    assert_eq!(
        view_strings(&by_o).await,
        expected_strings(&quads, |i| i % 5 == 2)
    );

    // Predicate and object: one (p, o) prefix probe fully resolves the
    // pattern, so the narrowed run is served directly.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert!(by_po.debug_has_serve_plan());
    assert!(by_po.debug_selection_pending());
    assert_eq!(
        view_strings(&by_po).await,
        expected_strings(&quads, |i| i % 15 == 1)
    );

    // A residual graph constraint leaves a mask scan to run — which already
    // gathers the rows — so the serve plan is dropped (it would save
    // nothing), and the result still comes out right.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let by_pg = store
        .match_pattern(None, Some(&p2), None, Some(&graphs[0]))
        .await
        .unwrap();
    assert!(!by_pg.debug_has_serve_plan());
    // The residual scan needed the ids, so nothing is left pending either.
    assert!(!by_pg.debug_selection_pending());
    assert_eq!(
        view_strings(&by_pg).await,
        expected_strings(&quads, |i| i % 3 == 2 && i % 2 == 0)
    );

    // Chaining narrows the first view's row ids — materializing them — so
    // its serve plan drops.
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

    // A tombstoned row vanishes from served streams too: the slice reads
    // copy rows, so the delete reaches it through the rid column.
    let deleted = store.delete_quad(&quads[4]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p_after.debug_has_serve_plan());
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

// ─── Resident form and code reads over indexes ─────────────────────────

/// A built store's resident form: construction compresses the base's code
/// columns and every component's integer children into probe-supported
/// encodings — no canonical primitives are retained — while every sorted
/// column still binds an encoded search probe, and the payload path still
/// serves codes (through the base's `vortex.shared` wrappers).
#[tokio::test]
async fn test_built_store_compresses_resident_form() {
    let quads = modular_quads(200, 4, 8);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    assert!(
        !store.debug_base_int_children_canonical(),
        "construction must retain compressed code columns, not canonical primitives"
    );
    assert!(
        store.debug_base_probe_resolvable(),
        "every sorted column of the compressed base must bind an encoded search probe"
    );
    for name in ["index:posg", "index:ospg"] {
        assert_eq!(
            store.debug_index_component_int_children_canonical(name),
            Some(false),
            "{name}: component children must stay compressed too"
        );
    }

    // The payload path still answers: codes decode to exactly the matched
    // quads (first touch materializes the shared canonical, then zero-copy).
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let cols = matched
        .code_columns()
        .expect("compressed base still serves codes through its shared wrappers");
    let dict = matched.code_read_snapshot().unwrap();
    let mut got: Vec<String> = (0..cols[0].len())
        .map(|i| {
            format!(
                "{} {} {}",
                dict.decode(cols[0][i]).unwrap(),
                dict.decode(cols[1][i]).unwrap(),
                dict.decode(cols[2][i]).unwrap()
            )
        })
        .collect();
    got.sort();
    let mut want: Vec<String> = quads
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 4 == 1)
        .map(|(_, q)| format!("{} {} {}", q.subject, q.predicate, q.object))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

/// The bindings' code-column read on a served in-memory match: `code_columns`
/// rides the serve plan, reading the codes off the answering index's own
/// columns — so the resolution's row ids stay unmaterialized — and the codes
/// it hands out address the cached dictionary and name exactly the matched
/// quads.
#[tokio::test]
async fn test_code_columns_serves_from_the_answering_index() {
    let quads = graph_modular_quads(30, 2, 3, 5, &[GraphName::DefaultGraph]);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(matched.debug_selection_pending());

    let cols = matched
        .code_columns()
        .expect("an in-memory Dictionary view answers codes");
    assert_eq!(
        matched.debug_row_ids_materialized(),
        Some(false),
        "a served code read must not materialize the resolution's row ids"
    );
    let dict = matched.code_read_snapshot().unwrap();
    let mut got: Vec<String> = (0..cols[0].len())
        .map(|i| {
            format!(
                "{} {} {}",
                dict.decode(cols[0][i]).unwrap(),
                dict.decode(cols[1][i]).unwrap(),
                dict.decode(cols[2][i]).unwrap()
            )
        })
        .collect();
    got.sort();
    let mut want: Vec<String> = quads
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 3 == 1)
        .map(|(_, q)| format!("{} {} {}", q.subject, q.predicate, q.object))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

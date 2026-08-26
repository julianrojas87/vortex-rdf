//! Materialized and shared-term reads: `quads_vec` against the stream, and
//! `shared_quads_vec` / `shared_quad_chunks` under every layout and view.

use super::*;

// ─── Materialized reads ────────────────────────────────────────────────

/// `quads_vec` (the exact-size materialization) must yield the same quads,
/// in the same order, as one-at-a-time stream collection — for the plain
/// store, a matched view, and a mutated store with a tail.
async fn run_quads_vec_matches_stream(store: VortexRdfStore, kind: &str) {
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
        assert_eq!(collected, streamed, "{kind}: {tag}");
    }
}

#[tokio::test]
async fn test_quads_vec_matches_stream_collection() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(dictionary_test_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    run_quads_vec_matches_stream(VortexRdfStore::from_built(arr).unwrap(), "in memory").await;
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_quads_vec_matches_stream_collection_file_backed() {
    let (_dir, path) =
        write_store_file(dictionary_test_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    run_quads_vec_matches_stream(store, "file").await;
}

// ─── Chunk decoding errors ─────────────────────────────────────────────

/// Rows for the decode-error cases: `modular_quads(3, 3, 4)` with row 1's
/// object replaced by a string in none of the N-Triples term forms.
fn raws_with_bad_object() -> Vec<crate::store::RawQuad> {
    let mut raws: Vec<crate::store::RawQuad> = modular_quads(3, 3, 4)
        .iter()
        .map(crate::store::RawQuad::from_quad)
        .collect();
    raws[1].o = "bogus".to_string();
    raws
}

/// `chunk` with one column replaced by `column`, everything else as it was.
fn with_column(
    chunk: &vortex_array::ArrayRef,
    name: &str,
    column: vortex_array::ArrayRef,
) -> vortex_array::ArrayRef {
    use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};

    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk.clone().execute::<StructArray>(&mut ctx).unwrap();
    let fields: Vec<vortex_array::ArrayRef> = struct_arr
        .names()
        .iter()
        .map(|field| {
            if field.as_ref() == name {
                column.clone()
            } else {
                struct_arr
                    .unmasked_field_by_name(field.as_ref())
                    .unwrap()
                    .clone()
            }
        })
        .collect();
    StructArray::try_new(
        struct_arr.names().clone(),
        fields,
        struct_arr.len(),
        vortex_array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

/// `layout`'s chunk over `raws`, paired with the resolved layout that
/// decodes it.
fn chunk_for(
    layout: LayoutStrategy,
    raws: &[crate::store::RawQuad],
) -> (
    vortex_array::ArrayRef,
    crate::store::layouts::ResolvedLayout,
) {
    use crate::store::builders::build_struct_array;
    use crate::store::layouts::dictionary::{self, TermDictionary};
    use crate::store::layouts::{DictAccess, ResolvedLayout};

    match layout {
        LayoutStrategy::Default => (
            build_struct_array(raws, layout, false).unwrap(),
            ResolvedLayout::Default,
        ),
        LayoutStrategy::TypedObject => (
            build_struct_array(raws, layout, false).unwrap(),
            ResolvedLayout::TypedObject,
        ),
        LayoutStrategy::Dictionary => {
            let (dict, code_map) = TermDictionary::from_quads_with_map(raws).unwrap();
            let codes = dictionary::encode_quads(raws, &code_map).unwrap();
            (
                dictionary::build_code_chunk(&codes, 0..raws.len(), false).unwrap(),
                ResolvedLayout::Dictionary(DictAccess::Resident(std::sync::Arc::new(dict))),
            )
        }
    }
}

/// `chunk` without its `g` column (the last column of every layout).
fn drop_graph_column(chunk: &vortex_array::ArrayRef) -> vortex_array::ArrayRef {
    use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};

    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk.clone().execute::<StructArray>(&mut ctx).unwrap();
    let names: Vec<_> = struct_arr
        .names()
        .iter()
        .take(struct_arr.names().len() - 1)
        .cloned()
        .collect();
    let fields: Vec<vortex_array::ArrayRef> = names
        .iter()
        .map(|name| {
            struct_arr
                .unmasked_field_by_name(name.as_ref())
                .unwrap()
                .clone()
        })
        .collect();
    StructArray::try_new(
        names.into(),
        fields,
        struct_arr.len(),
        vortex_array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

/// A chunk that cannot be read as the layout's columns at all decodes to a
/// single `Err`, whatever its row count.
#[test]
fn decode_chunk_reports_chunk_failure_as_single_err() {
    let raws: Vec<crate::store::RawQuad> = modular_quads(3, 3, 4)
        .iter()
        .map(crate::store::RawQuad::from_quad)
        .collect();
    for layout in [
        LayoutStrategy::Default,
        LayoutStrategy::TypedObject,
        LayoutStrategy::Dictionary,
    ] {
        let (chunk, resolved) = chunk_for(layout, &raws);
        let decoded = resolved.decode_chunk(&drop_graph_column(&chunk));
        assert_eq!(decoded.len(), 1, "{layout:?}: one error for the chunk");
        assert!(decoded[0].is_err(), "{layout:?}");
    }
}

/// A row whose term fails to parse is reported at its own position, with the
/// rows around it decoded: an object string in no term form under the
/// layouts that store the object spelling, an unknown `o_kind` under the
/// typed layout.
#[test]
fn decode_chunk_reports_bad_row_at_its_position() {
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::PrimitiveArray;

    let good: Vec<crate::store::RawQuad> = modular_quads(3, 3, 4)
        .iter()
        .map(crate::store::RawQuad::from_quad)
        .collect();
    let bad = raws_with_bad_object();
    let typed = {
        let (chunk, resolved) = chunk_for(LayoutStrategy::TypedObject, &good);
        let kinds = PrimitiveArray::from_iter([2u8, 9, 2]).into_array();
        (with_column(&chunk, "o_kind", kinds), resolved)
    };
    let cases = [
        (
            LayoutStrategy::Default,
            chunk_for(LayoutStrategy::Default, &bad),
        ),
        (LayoutStrategy::TypedObject, typed),
        (
            LayoutStrategy::Dictionary,
            chunk_for(LayoutStrategy::Dictionary, &bad),
        ),
    ];

    for (layout, (chunk, resolved)) in cases {
        let decoded = resolved.decode_chunk(&chunk);
        assert_eq!(decoded.len(), 3, "{layout:?}: one result per row");
        assert!(decoded[0].is_ok(), "{layout:?}: row 0");
        assert!(
            decoded[1].is_err(),
            "{layout:?}: row 1 carries the bad object"
        );
        assert!(decoded[2].is_ok(), "{layout:?}: row 2");
        assert_eq!(
            decoded[2].as_ref().unwrap().subject.to_string(),
            "<http://example.org/s02>",
            "{layout:?}"
        );
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

/// An in-memory index-served match reads its shared rows off the serving
/// index's columns, and a tombstoned derivative off the gather path; both
/// must agree with `quads_vec`.
#[tokio::test]
async fn test_shared_quads_match_quads_vec_on_served_view() {
    let quads = modular_quads(64, 3, 4);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let served = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert!(
        served.debug_has_serve_plan(),
        "the P match must be index-served"
    );
    let rows = assert_shared_matches_quads(&served, "served").await;
    assert_eq!(rows.len(), 22);

    let tombstoned = store
        .delete_quad(&quads[0])
        .await
        .unwrap()
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    let rows = assert_shared_matches_quads(&tombstoned, "served, tombstoned").await;
    assert_eq!(rows.len(), 21);
}

/// Every view shape a file-backed store can take — the whole store, an
/// index-served match, a subject-narrowed match, an append tail, a
/// tombstone — decodes the same rows shared as owned.
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

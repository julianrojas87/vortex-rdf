use super::*;

// ─── 3) Streaming/chunking behavior ─────────────────────────────────────

#[tokio::test]
async fn test_streaming_chunk_boundaries() {
    use crate::store::builders::unsorted_stream::build_chunk_stream;

    let quads: Vec<Quad> = (0..10)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    // chunk_size = 3 over 10 quads → chunks of 3, 3, 3, 1.
    let built = build_chunk_stream(
        Box::new(quad_stream(quads)),
        LayoutStrategy::Default,
        vec![],
        3,
    )
    .await
    .unwrap();
    let (dtype, chunks) = (built.dtype, built.chunks);

    if let vortex_array::dtype::DType::Struct(fields, _) = &dtype {
        let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
        assert_eq!(names, ["s", "p", "o", "g"]);
    } else {
        panic!("expected struct dtype");
    }

    let collected: Vec<_> = chunks.collect().await;
    let lens: Vec<usize> = collected
        .iter()
        .map(|c| c.as_ref().unwrap().len())
        .collect();
    assert_eq!(lens, [3, 3, 3, 1]);
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_streaming_write_read_roundtrip() {
    let quads: Vec<Quad> = (0..25)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                "http://example.org/p",
                &format!("object value {}", i),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    // Streaming write to an in-memory Vortex file...
    let bytes = quads_stream_to_vortex(quad_stream(quads.clone()))
        .await
        .unwrap();

    // ...then load it back as a store from the file bytes.
    let store = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 25);

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded[0].subject.to_string(), quads[0].subject.to_string());
    assert_eq!(decoded[24].object.to_string(), quads[24].object.to_string());
}

#[tokio::test]
async fn test_sorted_streaming_chunk_boundaries() {
    use crate::store::builders::sorted_in_memory::build_sorted_chunk_stream;
    use crate::store::builders::sorted_stream::build_sorted_stream_chunk_stream;

    // Quads fed in REVERSE subject order; both sorted builders must emit
    // globally sorted output across chunk boundaries.
    let quads: Vec<Quad> = (0..10)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    for (name, result) in [
        (
            "sorted_in_memory",
            build_sorted_chunk_stream(
                Box::new(quad_stream(quads.clone())),
                LayoutStrategy::Default,
                vec![],
                3,
            )
            .await,
        ),
        (
            "sorted_stream",
            build_sorted_stream_chunk_stream(
                Box::new(quad_stream(quads.clone())),
                LayoutStrategy::Default,
                vec![],
                3,
            )
            .await,
        ),
    ] {
        let chunks = result.unwrap_or_else(|e| panic!("{name}: {e}")).chunks;
        let collected: Vec<_> = chunks.collect().await;

        let lens: Vec<usize> = collected
            .iter()
            .map(|c| c.as_ref().unwrap().len())
            .collect();
        assert_eq!(lens, [3, 3, 3, 1], "{name}: unexpected chunk sizes");

        // Decode all chunks in order and verify global subject sort.
        let subjects: Vec<String> = collected
            .iter()
            .flat_map(|c| store::layouts::ResolvedLayout::Default.decode_chunk(c.as_ref().unwrap()))
            .map(|q| q.unwrap().subject.to_string())
            .collect();
        let mut sorted = subjects.clone();
        sorted.sort();
        assert_eq!(subjects, sorted, "{name}: output not globally sorted");
        assert_eq!(subjects.len(), 10, "{name}: wrong quad count");
    }
}

/// The out-of-core indexed pipeline when the data genuinely spills: with a
/// chunk size far below the dataset the ingest produces several runs, so the
/// quad merge and every index family's `(value, row id)` sort all run
/// through their spill-and-K-way-merge branch rather than the single-run
/// in-memory one. The sibling tests all fit in one run, which leaves that
/// branch — and the global sortedness of the index columns it must still
/// produce — otherwise unexercised for the string layouts.
#[tokio::test]
async fn test_sorted_streaming_spilled_indexes_match_in_memory() {
    use crate::store::builders::assemble_chunks;
    use crate::store::builders::sorted_stream::build_sorted_stream_chunk_stream;

    let indexes = vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
    // Fed in reverse so nothing is accidentally in order, and wide enough
    // (24 quads over a chunk size of 3) to force 8 runs.
    let quads: Vec<Quad> = (0..24)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("object {}", i % 4),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let chunks = build_sorted_stream_chunk_stream(
        Box::new(quad_stream(quads.clone())),
        LayoutStrategy::Default,
        indexes.clone(),
        3,
    )
    .await
    .expect("spilled build")
    .chunks;
    let chunks: Vec<_> = chunks
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|c| c.expect("chunk"))
        .collect();
    assert_eq!(
        chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
        [3, 3, 3, 3, 3, 3, 3, 3],
        "unexpected chunk sizes"
    );

    let store =
        VortexRdfStore::new(assemble_chunks(chunks, LayoutStrategy::Default, &indexes).unwrap())
            .unwrap();
    assert_eq!(store.indexes(), &indexes[..]);

    // Every quad survived the spill round-trip, in global subject order.
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    let mut expected = quad_strings(&quads);
    expected.sort();
    assert_eq!(quad_strings(&decoded), expected);

    // And the index columns the spill merge emitted actually route: the
    // same selectivities the single-run copy-index test asserts.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    assert_eq!(
        store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        8
    );
    assert_eq!(
        store
            .match_pattern(None, None, Some(&o1), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        6
    );
    assert_eq!(
        store
            .match_pattern(None, Some(&p1), Some(&o1), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        2
    );
}

#[cfg(feature = "file-io")]
async fn run_sorted_streaming_write_test<B: VortexArrayBuilder>() {
    let quads: Vec<Quad> = (0..25)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<B, _, _>(
        quad_stream(quads),
        &mut buffer,
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();

    let store = VortexRdfStore::from_bytes(&buffer).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 25);

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded[0].subject.to_string(), "<http://example.org/s00>");
    assert_eq!(decoded[24].subject.to_string(), "<http://example.org/s24>");
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_sorted_in_memory_streaming_write() {
    run_sorted_streaming_write_test::<SortedInMemoryBuilder>().await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_sorted_stream_streaming_write() {
    run_sorted_streaming_write_test::<SortedStreamBuilder>().await;
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_subject_metadata_range_for_missing_subject() {
    let quads: Vec<Quad> = (0..25)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let path = std::env::temp_dir().join(format!(
        "vortex_rdf_subject_range_{}.vortex",
        uuid::Uuid::new_v4()
    ));
    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
        quad_stream(quads),
        &mut buffer,
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    std::fs::write(&path, &buffer).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let missing = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s99").unwrap());
    let row_range = store.debug_subject_row_range(&missing).await.unwrap();
    assert_eq!(row_range, Some(0..0));
    assert_eq!(
        store
            .match_pattern(Some(&missing), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn test_sorted_builder_stamps_is_sorted() {
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
    use vortex_array::expr::stats::{Precision, Stat, StatsProvider};

    let quads: Vec<Quad> = (0..10)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let check = |arr: vortex_array::ArrayRef, expect_sorted: bool, name: &str| {
        let mut ctx = crate::io::VORTEX_SESSION.create_execution_ctx();
        let struct_arr = arr.execute::<StructArray>(&mut ctx).unwrap();
        let s_col = struct_arr.unmasked_field_by_name("s").unwrap();
        let is_sorted = match s_col.statistics().get(Stat::IsSorted) {
            Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
            Precision::Absent => false,
        };
        assert_eq!(is_sorted, expect_sorted, "{name}: IsSorted stat mismatch");
    };

    let sorted = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    check(sorted.array, true, "sorted_in_memory");

    let unsorted = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
        quad_stream(quads),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    check(unsorted.array, false, "unsorted");
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

/// `quads_vec` (the exact-size materialization) must yield the same quads,
/// in the same order, as one-at-a-time stream collection — for a plain
/// store, a matched view, and a mutated store with a tail.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_quads_vec_matches_stream_collection() {
    let quads = dictionary_test_quads();
    let dir = std::env::temp_dir().join(format!(
        "vortex_rdf_quads_vec_test_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.vortex");
    crate::io::quads_stream_to_vortex_file_with_builder::<SortedInMemoryBuilder, _>(
        quad_stream(quads.clone()),
        &path,
        LayoutStrategy::Dictionary,
        dictionary_indexes(),
    )
    .await
    .unwrap();
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
    std::fs::remove_dir_all(&dir).ok();
}

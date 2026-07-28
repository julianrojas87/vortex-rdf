use super::*;

// ─── 7) Dictionary layout ──────────────────────────────────────────────
async fn run_dictionary_roundtrip<B: VortexArrayBuilder>() {
    let quads = dictionary_test_quads();
    let arr = VortexRdfStore::build_vortex_array_with_builder::<B>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        dictionary_indexes(),
    )
    .await
    .expect("dictionary build failed");

    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));
}

#[tokio::test]
async fn test_dictionary_sorted_in_memory() {
    run_dictionary_roundtrip::<SortedInMemoryBuilder>().await;
}
#[tokio::test]
async fn test_dictionary_sorted_stream() {
    run_dictionary_roundtrip::<SortedStreamBuilder>().await;
}
#[tokio::test]
async fn test_dictionary_unsorted_stream() {
    run_dictionary_roundtrip::<UnsortedStreamBuilder>().await;
}

#[tokio::test]
async fn test_dictionary_streaming_chunk_boundaries() {
    use crate::store::builders::assemble_chunks;
    use crate::store::builders::sorted_in_memory::build_sorted_chunk_stream;
    use crate::store::builders::sorted_stream::build_sorted_stream_chunk_stream;
    use crate::store::builders::unsorted_stream::build_chunk_stream;

    let quads = dictionary_test_quads();

    for (name, result) in [
        (
            "unsorted_stream",
            build_chunk_stream(
                Box::new(quad_stream(quads.clone())),
                LayoutStrategy::Dictionary,
                dictionary_indexes(),
                3,
            )
            .await,
        ),
        (
            "sorted_in_memory",
            build_sorted_chunk_stream(
                Box::new(quad_stream(quads.clone())),
                LayoutStrategy::Dictionary,
                dictionary_indexes(),
                3,
            )
            .await,
        ),
        (
            "sorted_stream",
            build_sorted_stream_chunk_stream(
                Box::new(quad_stream(quads.clone())),
                LayoutStrategy::Dictionary,
                dictionary_indexes(),
                3,
            )
            .await,
        ),
    ] {
        let built = result.unwrap_or_else(|e| panic!("{name}: {e}"));
        let dict = built.dict.clone();
        let collected: Vec<_> = built.chunks.collect().await;
        let lens: Vec<usize> = collected
            .iter()
            .map(|c| c.as_ref().unwrap().len())
            .collect();
        assert_eq!(lens, [3, 3, 3, 1], "{name}: unexpected chunk sizes");

        // Reassemble and decode through a store: the chunks hold bare
        // codes, and the dictionary the stream carried beside them is
        // handed back with the reassembled array — all chunks' codes must
        // reference that same global dictionary.
        let chunks: Vec<_> = collected.into_iter().map(|c| c.unwrap()).collect();
        let arr =
            assemble_chunks(chunks, LayoutStrategy::Dictionary, &dictionary_indexes()).unwrap();
        let store =
            VortexRdfStore::from_built(crate::store::builders::BuiltArray { array: arr, dict })
                .unwrap();
        assert_eq!(store.layout(), LayoutStrategy::Dictionary, "{name}");
        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(
            quad_strings(&decoded),
            quad_strings(&quads),
            "{name}: bad roundtrip"
        );
    }
}

#[tokio::test]
async fn test_dictionary_match_and_mutations() {
    let quads = dictionary_test_quads();
    let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        dictionary_indexes(),
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Subject match: hits the IsSorted binary-search fast path on the u32 column.
    let s3 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s03").unwrap());
    let by_subject = store
        .match_pattern(Some(&s3), None, None, None)
        .await
        .unwrap();
    assert_eq!(by_subject.size().await.unwrap(), 1);
    let results: Vec<Quad> = by_subject.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s03>");

    // Predicate match: mask scan over the u32 codes (p0 occurs for i = 0,3,6,9).
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_pred = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(by_pred.size().await.unwrap(), 4);

    // Terms absent from the dictionary match nothing (both routing paths).
    let missing_s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/nope").unwrap());
    assert_eq!(
        store
            .match_pattern(Some(&missing_s), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    assert_eq!(
        store
            .match_pattern(None, Some(&missing_p), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );

    // delete_quad works (mask-based); the cached dictionary is propagated.
    let deleted = store.delete_quad(&quads[0]).await.unwrap();
    assert_eq!(deleted.size().await.unwrap(), 9);
    let decoded: Vec<Quad> = deleted.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded.len(), 9);
    assert!(!quad_strings(&decoded).contains(&quads[0].to_string()));

    // add_quad works despite the sorted codes: the appended quad lands in
    // the string tail (its terms need no dictionary code), so re-adding
    // the deleted quad brings the store back to its full size.
    let readded = deleted.add_quad(quads[0].clone()).await.unwrap();
    assert_eq!(readded.size().await.unwrap(), 10);
    let decoded: Vec<Quad> = readded.quads().unwrap().try_collect().await.unwrap();
    assert!(quad_strings(&decoded).contains(&quads[0].to_string()));
}

#[tokio::test]
async fn test_dictionary_layout_secondary_index_compatibility() {
    let quads = dictionary_test_quads();

    // Dictionary layout composes with secondary reference indexes.
    let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![
            IndexType::SecondaryByReference,
            IndexType::SecondaryByReference,
        ],
    )
    .await
    .unwrap();

    // Deduped index columns appear exactly once, and under the Dictionary
    // layout the index value columns hold u32 codes — same dtype as the
    // primary code columns — instead of strings.
    if let vortex_array::dtype::DType::Struct(fields, _) = arr.array.dtype() {
        let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
        assert_eq!(
            names,
            [
                "s",
                "p",
                "o",
                "g",
                "_idx_o_val",
                "_idx_o_rid",
                "_idx_p_val",
                "_idx_p_rid"
            ],
        );
        assert_eq!(fields.field("_idx_o_val"), fields.field("o"));
        assert_eq!(fields.field("_idx_p_val"), fields.field("p"));
    } else {
        panic!("expected StructArray dtype");
    }

    let store = VortexRdfStore::from_built(arr).unwrap();

    // Full roundtrip decode with the index columns present.
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));

    // Predicate-only match: routes through the code-based `_idx_p_*` index
    // (p0 occurs for i = 0,3,6,9).
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_pred = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(by_pred.size().await.unwrap(), 4);
    let results: Vec<Quad> = by_pred.quads().unwrap().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.predicate == "http://example.org/p0")
    );

    // Object-only match: routes through the code-based `_idx_o_*` index and
    // decodes through the store-cached dictionary ("object 1" for i = 1,5,9).
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let by_obj = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_obj.size().await.unwrap(), 3);
    let results: Vec<Quad> = by_obj.quads().unwrap().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.object.to_string() == "\"object 1\"")
    );

    // Combined o+p pattern: the object index narrows first; the derived
    // store (stale index columns stripped) mask-scans the predicate
    // (i = 1,5,9 with p1 → only i = 1).
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_both = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_both.size().await.unwrap(), 1);
    let results: Vec<Quad> = by_both.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s01>");

    // Terms absent from the dictionary match nothing through the index paths.
    let missing_o = Term::Literal(Literal::new_simple_literal("nope"));
    assert_eq!(
        store
            .match_pattern(None, None, Some(&missing_o), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    assert_eq!(
        store
            .match_pattern(None, Some(&missing_p), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn test_dictionary_sorted_with_secondary_index() {
    // Sorted builder + Dictionary layout + secondary indexes: the subject
    // binary-search fast path and the index routing must compose — after
    // the subject slice, the derived store's stale index columns are
    // stripped and the remaining terms are mask-scanned.
    let quads = dictionary_test_quads();
    let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));

    // s + o pattern (i = 5: s05 has "object 1").
    let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let matched = store
        .match_pattern(Some(&s5), None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 1);
    let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s05>");
    assert_eq!(results[0].object.to_string(), "\"object 1\"");

    // s + o with an object that exists but not on that subject.
    let o0 = Term::Literal(Literal::new_simple_literal("object 0"));
    assert_eq!(
        store
            .match_pattern(Some(&s5), None, Some(&o0), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn test_dictionary_empty_dataset() {
    let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
        quad_stream(vec![]),
        LayoutStrategy::Dictionary,
        dictionary_indexes(),
    )
    .await
    .unwrap();
    assert_eq!(arr.array.len(), 0);

    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(store.size().await.unwrap(), 0);
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert!(decoded.is_empty());
}

/// The dictionary is held and round-tripped FSST-compressed, and that is an
/// invariant this code owns rather than one it inherits: which encoding a
/// column gets is otherwise decided by the writer's sampling and is free to
/// be something else. Asserting on the encoding is the only way this stays
/// true — the terms decode correctly either way, so a regression to
/// plaintext would be silent.
#[tokio::test]
async fn test_dictionary_terms_are_fsst_through_ipc() {
    let quads: Vec<Quad> = (0..2_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/subject/{i:06}"),
                &format!("http://example.org/predicate/{}", i % 16),
                &format!("object value {:06}", i / 2),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let encoding_of_dict_term = |array: &vortex_array::ArrayRef| -> String {
        use vortex_array::IntoArray as _;
        use vortex_array::VortexSessionExecute as _;
        use vortex_array::arrays::chunked::ChunkedArrayExt as _;
        use vortex_array::arrays::masked::MaskedArraySlotsExt as _;
        use vortex_array::arrays::struct_::StructArrayExt as _;
        let mut ctx = crate::io::VORTEX_LIGHT_SESSION.create_execution_ctx();
        let sa = array
            .clone()
            .execute::<vortex_array::arrays::struct_::StructArray>(&mut ctx)
            .unwrap();
        // The dictionary rows are the term column's valid tail; peel the
        // padded form's wrappers (chunk container, nullability mask) down
        // to the leaf term encoding.
        let col = sa.unmasked_field_by_name("_dict_term").unwrap().clone();
        let total = col.len();
        let mask = col
            .validity()
            .unwrap()
            .execute_mask(total, &mut ctx)
            .unwrap();
        let m = mask.true_count();
        let mut tail =
            crate::store::layouts::dictionary::term_tail(&col, total - m, total).unwrap();
        loop {
            tail = match tail.try_downcast::<vortex_array::arrays::Masked>() {
                Ok(masked) => masked.child().clone(),
                Err(not_masked) => {
                    match not_masked.try_downcast::<vortex_array::arrays::Chunked>() {
                        Ok(chunked) if chunked.nchunks() == 1 => chunked.chunk(0).clone(),
                        Ok(chunked) => break chunked.into_array(),
                        Err(other) => break other,
                    }
                }
            };
        }
        .encoding_id()
        .to_string()
    };

    // Written out compressed...
    let written = store.to_ipc_array().await.unwrap();
    assert_eq!(
        encoding_of_dict_term(&written),
        "vortex.fsst",
        "toBytes must not expand the dictionary to plaintext"
    );

    // ...and read back still compressed, not canonicalized on open.
    let mut buf = Vec::new();
    crate::io::write_array_to_ipc(written, &mut buf).unwrap();
    let read_back = crate::io::array_from_ipc_bytes(&buf).unwrap();
    assert_eq!(encoding_of_dict_term(&read_back), "vortex.fsst");

    // And the terms still resolve, through the compressed representation.
    let reread = VortexRdfStore::new(read_back).unwrap();
    assert_eq!(reread.size().await.unwrap(), 2_000);
    let p = NamedNode::new("http://example.org/predicate/3").unwrap();
    let matched = reread
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 125);
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_dictionary_file_roundtrip() {
    use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

    let quads = dictionary_test_quads();

    // Streaming write (two-pass spill pipeline) to an in-memory buffer...
    let mut bytes: Vec<u8> = Vec::new();
    quads_stream_to_vortex_writer_with_builder::<UnsortedStreamBuilder, _, _>(
        quad_stream(quads.clone()),
        &mut bytes,
        LayoutStrategy::Dictionary,
        dictionary_indexes(),
    )
    .await
    .unwrap();

    // ...then open it as a file-backed store (loads the dictionary via a
    // single-column projection scan).
    let dir = std::env::temp_dir().join(format!("vortex_rdf_dict_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("dict.vortex");
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(store.size().await.unwrap(), 10);

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));

    // Pushed-down integer filter on the code columns.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 4);
    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.predicate == "http://example.org/p0")
    );

    // A term absent from the dictionary yields an always-false filter.
    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    let empty = store
        .match_pattern(None, Some(&missing_p), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "file-io")]
/// The placement knob end to end: the same dataset written through the
/// path-based entry in both placements must reopen with identical
/// results — padded as one self-containing file, sidecar as quads +
/// companion.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_dictionary_placement_file_roundtrips() {
    use crate::store::DictionaryPlacement;

    let quads = dictionary_test_quads();
    let dir = std::env::temp_dir().join(format!(
        "vortex_rdf_placement_test_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    for (placement, name) in [
        (DictionaryPlacement::Padded, "padded.vortex"),
        (DictionaryPlacement::Sidecar, "sidecar.vortex"),
    ] {
        let path = dir.join(name);
        crate::io::quads_stream_to_vortex_file_with_builder::<SortedInMemoryBuilder, _>(
            quad_stream(quads.clone()),
            &path,
            LayoutStrategy::Dictionary,
            dictionary_indexes(),
            placement,
        )
        .await
        .unwrap();

        // The sidecar form leaves the quads schema bare and writes the
        // companion; the padded form is one file with the term column.
        let companion = dir.join(name.replace(".vortex", ".dict.vortex"));
        assert_eq!(
            companion.is_file(),
            placement == DictionaryPlacement::Sidecar,
            "{name}: unexpected companion presence"
        );

        let store = VortexRdfStore::from_file(&path).await.unwrap();
        assert_eq!(store.layout(), LayoutStrategy::Dictionary, "{name}");
        assert_eq!(store.size().await.unwrap(), quads.len(), "{name}");

        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let matched = store
            .match_pattern(None, Some(&p0), None, None)
            .await
            .unwrap();
        assert_eq!(matched.size().await.unwrap(), 4, "{name}");
        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(
            quad_strings(&decoded),
            quad_strings(&quads),
            "{name}: bad roundtrip"
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// The sidecar placement round-trip: a quads file with bare code columns
/// plus the `<stem>.dict.vortex` companion written beside it must reopen
/// with identical match results — the sidecar branch of `from_file`.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_sidecar_dictionary_file_roundtrip() {
    let quads = dictionary_test_quads();
    let built = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        dictionary_indexes(),
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(built).unwrap();

    let dir = std::env::temp_dir().join(format!(
        "vortex_rdf_sidecar_dict_test_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("quads.vortex");

    // Bare code columns (with index columns) to the quads file...
    let bare = store.get_quads_array().await.unwrap();
    let file = tokio::fs::File::create(&path).await.unwrap();
    crate::io::serialize(bare, file).await.unwrap();
    // ...and the dictionary to the companion beside it.
    let sidecar = crate::io::write_sidecar_dictionary(&store.dictionary_snapshot().unwrap(), &path)
        .await
        .unwrap();
    assert_eq!(sidecar, dir.join("quads.dict.vortex"));

    let reopened = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(reopened.layout(), LayoutStrategy::Dictionary);
    assert_eq!(reopened.size().await.unwrap(), quads.len());

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let (a, b) = (
        store
            .match_pattern(None, Some(&p0), None, None)
            .await
            .unwrap(),
        reopened
            .match_pattern(None, Some(&p0), None, None)
            .await
            .unwrap(),
    );
    assert_eq!(a.size().await.unwrap(), b.size().await.unwrap());
    let decoded: Vec<Quad> = b.quads().unwrap().try_collect().await.unwrap();
    let direct: Vec<Quad> = a.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&direct));

    std::fs::remove_dir_all(&dir).ok();
}

// ─── 7b) File-backed dictionary ─────────────────────────────────────────

/// Sorted string forms of a pattern match on `store`.
async fn matched_strings(
    store: &VortexRdfStore,
    s: Option<&NamedOrBlankNode>,
    p: Option<&NamedNode>,
    o: Option<&Term>,
    g: Option<&GraphName>,
) -> Vec<String> {
    let matched = store.match_pattern(s, p, o, g).await.unwrap();
    let quads: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    quad_strings(&quads)
}

/// A store opened with the dictionary forced file-backed must answer every
/// pattern family identically to the resident open of the same file.
async fn assert_file_backed_matches_resident(
    placement: crate::store::DictionaryPlacement,
    indexes: Indexes,
    tag: &str,
) {
    let quads = dictionary_test_quads();
    let dir = std::env::temp_dir().join(format!(
        "vortex_rdf_fbdict_{}_{}",
        tag,
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.vortex");
    crate::io::quads_stream_to_vortex_file_with_builder::<SortedStreamBuilder, _>(
        quad_stream(quads.clone()),
        &path,
        LayoutStrategy::Dictionary,
        indexes,
        placement,
    )
    .await
    .unwrap();

    let resident = VortexRdfStore::from_file(&path).await.unwrap();
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();

    // Residency is observable through the sync dictionary surface: a
    // file-backed dictionary has no snapshot and no sync code translation.
    assert!(resident.dictionary_snapshot().is_some(), "{tag}");
    assert!(fb.dictionary_snapshot().is_none(), "{tag}");
    assert_eq!(fb.encode_code("<http://example.org/p0>"), None, "{tag}");
    assert_eq!(fb.decode_code(0), None, "{tag}");

    let s3 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s03").unwrap());
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
    let default_g = GraphName::DefaultGraph;
    let absent = NamedNode::new("http://example.org/absent").unwrap();

    // Full reconstruction.
    assert_eq!(
        matched_strings(&fb, None, None, None, None).await,
        matched_strings(&resident, None, None, None, None).await,
        "{tag}: full scan"
    );
    assert_eq!(fb.size().await.unwrap(), quads.len(), "{tag}");

    // One pattern per family: subject / predicate / object / graph bound,
    // multi-role, fully bound, and a term absent from the dictionary.
    assert_eq!(
        matched_strings(&fb, Some(&s3), None, None, None).await,
        matched_strings(&resident, Some(&s3), None, None, None).await,
        "{tag}: subject-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, Some(&p0), None, None).await,
        matched_strings(&resident, None, Some(&p0), None, None).await,
        "{tag}: predicate-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, None, Some(&o1), None).await,
        matched_strings(&resident, None, None, Some(&o1), None).await,
        "{tag}: object-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, None, None, Some(&g)).await,
        matched_strings(&resident, None, None, None, Some(&g)).await,
        "{tag}: graph-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, None, None, Some(&default_g)).await,
        matched_strings(&resident, None, None, None, Some(&default_g)).await,
        "{tag}: default-graph-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, Some(&p0), Some(&o1), None).await,
        matched_strings(&resident, None, Some(&p0), Some(&o1), None).await,
        "{tag}: predicate+object"
    );
    let q0 = &quads[3];
    assert!(fb.contains(q0).await.unwrap(), "{tag}: contains");
    let empty = matched_strings(&fb, None, Some(&absent), None, None).await;
    assert!(empty.is_empty(), "{tag}: absent term matches nothing");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_file_backed_dictionary_matches_resident_padded() {
    assert_file_backed_matches_resident(
        crate::store::DictionaryPlacement::Padded,
        dictionary_indexes(),
        "padded",
    )
    .await;
}

#[tokio::test]
async fn test_file_backed_dictionary_matches_resident_sidecar() {
    assert_file_backed_matches_resident(
        crate::store::DictionaryPlacement::Sidecar,
        dictionary_indexes(),
        "sidecar",
    )
    .await;
}

/// With a copy index present, an index-served read on a file-backed store
/// must stream through the async decode path and still agree with resident.
#[tokio::test]
async fn test_file_backed_dictionary_serves_from_copy_index() {
    assert_file_backed_matches_resident(
        crate::store::DictionaryPlacement::Padded,
        vec![IndexType::SecondaryByCopy],
        "copy_index",
    )
    .await;

    // And explicitly confirm the serving plan engages on the file-backed
    // store (the equality above would hold even off the fallback path).
    let quads = dictionary_test_quads();
    let dir =
        std::env::temp_dir().join(format!("vortex_rdf_fbdict_serve_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.vortex");
    crate::io::quads_stream_to_vortex_file_with_builder::<SortedStreamBuilder, _>(
        quad_stream(quads.clone()),
        &path,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
        crate::store::DictionaryPlacement::Padded,
    )
    .await
    .unwrap();
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let matched = fb.match_pattern(None, Some(&p0), None, None).await.unwrap();
    assert!(matched.debug_has_serve_plan());
    let served: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(served.len(), 4);
    std::fs::remove_dir_all(&dir).ok();
}

/// The residency threshold is inclusive: exactly at the term count the
/// dictionary lifts resident, one below it stays file-backed.
#[tokio::test]
async fn test_file_backed_dictionary_threshold_boundary() {
    let quads = dictionary_test_quads();
    let dir = std::env::temp_dir().join(format!("vortex_rdf_fbdict_thr_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.vortex");
    crate::io::quads_stream_to_vortex_file_with_builder::<SortedInMemoryBuilder, _>(
        quad_stream(quads.clone()),
        &path,
        LayoutStrategy::Dictionary,
        dictionary_indexes(),
        crate::store::DictionaryPlacement::Padded,
    )
    .await
    .unwrap();

    let resident = VortexRdfStore::from_file(&path).await.unwrap();
    let n_terms = resident.dictionary_snapshot().unwrap().len() as u64;
    assert!(n_terms > 1);

    let at = VortexRdfStore::from_file_with_dict_residency(&path, n_terms)
        .await
        .unwrap();
    assert!(at.dictionary_snapshot().is_some());
    let below = VortexRdfStore::from_file_with_dict_residency(&path, n_terms - 1)
        .await
        .unwrap();
    assert!(below.dictionary_snapshot().is_none());
    std::fs::remove_dir_all(&dir).ok();
}

/// The operations that need the whole dictionary — serialization, mutation
/// with its tail merge, compaction — lift a file-backed dictionary
/// transiently and stay correct.
#[tokio::test]
async fn test_file_backed_dictionary_serializes_and_mutates() {
    let quads = dictionary_test_quads();
    let dir = std::env::temp_dir().join(format!("vortex_rdf_fbdict_mut_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.vortex");
    crate::io::quads_stream_to_vortex_file_with_builder::<SortedInMemoryBuilder, _>(
        quad_stream(quads.clone()),
        &path,
        LayoutStrategy::Dictionary,
        dictionary_indexes(),
        crate::store::DictionaryPlacement::Padded,
    )
    .await
    .unwrap();
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();

    // Serialization lifts the dictionary and emits the padded form, which a
    // fresh in-memory store decodes standalone.
    let array = fb.to_serializable_array().await.unwrap();
    let reread = VortexRdfStore::new(array).unwrap();
    let expected = quad_strings(&quads);
    let got: Vec<Quad> = reread.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&got), expected);

    // Mutation: an added quad lands in the string tail; reads merge it with
    // the file-backed base (tail-merge re-encoding lifts transiently).
    let mut mutated = fb.clone();
    let extra = make_quad(
        "http://example.org/added",
        "http://example.org/p0",
        "added object",
        GraphName::DefaultGraph,
    );
    mutated = mutated.add_quad(extra.clone()).await.unwrap();
    assert_eq!(mutated.size().await.unwrap(), quads.len() + 1);
    assert!(mutated.contains(&extra).await.unwrap());
    let merged = mutated.to_serializable_array().await.unwrap();
    let merged_store = VortexRdfStore::new(merged).unwrap();
    assert_eq!(merged_store.size().await.unwrap(), quads.len() + 1);

    // Deletion + compaction rewrite the source file through the lifted
    // dictionary; the reopened store serves the surviving quads.
    let doomed = quads[0].clone();
    let deleted = fb.delete_quad(&doomed).await.unwrap();
    let compacted = deleted.compact().await.unwrap();
    assert_eq!(compacted.size().await.unwrap(), quads.len() - 1);
    assert!(!compacted.contains(&doomed).await.unwrap());
    std::fs::remove_dir_all(&dir).ok();
}

/// Direct probe parity at multi-split scale: every sampled term must resolve
/// to the same code through the fence-guided file probe as through the
/// resident dictionary, and mutated absent terms must come back `None` —
/// for both placements.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_dictionary_fence_probe_parity() {
    use crate::store::DictionaryPlacement;
    use crate::store::layouts::term_dictionary::{FileBackedDict, padded_dict_extent};
    use std::sync::Arc;

    // Enough unique terms to spread the dictionary across several splits, so
    // the fence's binary search genuinely selects between candidates.
    let quads: Vec<Quad> = (0..20_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:06}"),
                &format!("http://example.org/p{}", i % 3),
                &format!("object {i:06}"),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let dir = std::env::temp_dir().join(format!("vortex_rdf_fence_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();

    for (placement, name) in [
        (DictionaryPlacement::Padded, "padded.vortex"),
        (DictionaryPlacement::Sidecar, "sidecar.vortex"),
    ] {
        let path = dir.join(name);
        crate::io::quads_stream_to_vortex_file_with_builder::<SortedStreamBuilder, _>(
            quad_stream(quads.clone()),
            &path,
            LayoutStrategy::Dictionary,
            vec![],
            placement,
        )
        .await
        .unwrap();

        // The reference answers, from a resident open of the same file.
        let resident = VortexRdfStore::from_file_with_dict_residency(&path, u64::MAX)
            .await
            .unwrap();
        let dict = resident.dictionary_snapshot().unwrap().0;

        // The probe target, built exactly as `from_file` does file-backed.
        let (file, base_row) = match placement {
            DictionaryPlacement::Padded => {
                let file = Arc::new(crate::io::de::open_vortex_file(&path).await.unwrap());
                let (base_row, _) = padded_dict_extent(&file).unwrap();
                (file, base_row)
            }
            DictionaryPlacement::Sidecar => {
                let file = crate::store::layouts::term_dictionary::open_sidecar_file(&path)
                    .await
                    .unwrap();
                (file, 0)
            }
        };
        let len = dict.len() as u64;
        let dict_splits = file
            .splits()
            .unwrap()
            .into_iter()
            .filter(|r| r.end > base_row && r.start < base_row + len)
            .count();
        assert!(
            dict_splits > 1,
            "{name}: expected a multi-split dictionary, got {dict_splits} split(s) \
             for {len} terms"
        );
        let fb = FileBackedDict::new(file, base_row, len);

        // Every ~97th term plus both extremes, probed twice (cold + memo).
        let sample: Vec<u32> = (0..len as u32)
            .step_by(397)
            .chain([0, len as u32 - 1])
            .collect();
        for &code in &sample {
            let term = dict.term_at(code).unwrap();
            assert_eq!(fb.get_id(&term).await.unwrap(), Some(code), "{name}: {term}");
            assert_eq!(fb.get_id(&term).await.unwrap(), Some(code), "{name}: {term}");

            // A control character keeps the probe inside the same fence
            // window but matches no stored term.
            let absent = format!("{term}\u{1}");
            assert_eq!(fb.get_id(&absent).await.unwrap(), None, "{name}: {absent}");
        }
        // Above every stored term: the last split is probed and misses.
        assert_eq!(fb.get_id("\u{10FFFF}").await.unwrap(), None, "{name}");
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// A probe sorting below the first dictionary term is answered absent by the
/// fence alone (the `partition_point == 0` edge): a dataset whose lowest
/// term is a literal (`"…`) probed with `!`, which sorts before `"`.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_dictionary_fence_rejects_below_first_term() {
    use crate::store::DictionaryPlacement;
    use crate::store::layouts::term_dictionary::{FileBackedDict, padded_dict_extent};
    use std::sync::Arc;

    let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
    let quads: Vec<Quad> = (0..3)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i}"),
                "http://example.org/p",
                &format!("object {i}"),
                g.clone(),
            )
        })
        .collect();

    let dir = std::env::temp_dir().join(format!(
        "vortex_rdf_fence_edge_test_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("edge.vortex");
    crate::io::quads_stream_to_vortex_file_with_builder::<SortedInMemoryBuilder, _>(
        quad_stream(quads),
        &path,
        LayoutStrategy::Dictionary,
        vec![],
        DictionaryPlacement::Padded,
    )
    .await
    .unwrap();

    let resident = VortexRdfStore::from_file_with_dict_residency(&path, u64::MAX)
        .await
        .unwrap();
    let dict = resident.dictionary_snapshot().unwrap().0;
    let first_term = dict.term_at(0).unwrap();
    assert!(
        first_term.as_str() > "!",
        "fixture must have no term sorting at or below `!`, got {first_term:?}"
    );

    let file = Arc::new(crate::io::de::open_vortex_file(&path).await.unwrap());
    let (base_row, dict_len) = padded_dict_extent(&file).unwrap();
    let fb = FileBackedDict::new(file, base_row, dict_len);

    assert_eq!(fb.get_id("!").await.unwrap(), None);
    // And the ordinary path still resolves through the same fence.
    assert_eq!(fb.get_id(&first_term).await.unwrap(), Some(0));

    std::fs::remove_dir_all(&dir).ok();
}

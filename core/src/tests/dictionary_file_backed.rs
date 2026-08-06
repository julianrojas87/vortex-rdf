use super::*;
use crate::io::native_file::NativeStoreFile;
use crate::io::store_layout::DICT_COMPONENT_NAME;
use crate::store::term_dictionary::FileBackedDict;

// ─── 7b) File-backed dictionary ─────────────────────────────────────────

/// Sorted string forms of a pattern match on `store`.
async fn matched_strings(
    store: &VortexRdfStore,
    s: Option<&NamedOrBlankNode>,
    p: Option<&NamedNode>,
    o: Option<&Term>,
    g: Option<&GraphName>,
) -> Vec<String> {
    view_strings(&store.match_pattern(s, p, o, g).await.unwrap()).await
}

/// A store opened with the dictionary forced file-backed must answer every
/// pattern family identically to the resident open of the same file.
async fn assert_file_backed_matches_resident(indexes: Indexes, tag: &str) {
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
async fn test_file_backed_dictionary_matches_resident() {
    assert_file_backed_matches_resident(vec![], "dict-child").await;
}

/// With a copy index present, an index-served read on a file-backed store
/// must stream through the async decode path and still agree with resident.
#[tokio::test]
async fn test_file_backed_dictionary_serves_from_copy_index() {
    assert_file_backed_matches_resident(vec![IndexType::SecondaryByCopy], "copy_index").await;

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

/// The residency threshold is inclusive and byte-based: exactly at the
/// dictionary child's on-disk size the dictionary lifts resident, one byte
/// below it stays file-backed.
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
        vec![],
    )
    .await
    .unwrap();

    let file = NativeStoreFile::try_new(
        crate::io::native_file::open_vortex_file(&path)
            .await
            .unwrap(),
    )
    .unwrap();
    let dict_bytes = file
        .component_bytes(DICT_COMPONENT_NAME)
        .unwrap()
        .expect("dictionary child present");
    assert!(dict_bytes > 1);

    let at = VortexRdfStore::from_file_with_dict_residency(&path, dict_bytes)
        .await
        .unwrap();
    assert!(at.dictionary_snapshot().is_some());
    let below = VortexRdfStore::from_file_with_dict_residency(&path, dict_bytes - 1)
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
        vec![],
    )
    .await
    .unwrap();
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();

    // Serialization lifts the dictionary transiently and writes it as the
    // dictionary child, which a fresh store decodes standalone.
    let bytes = fb.to_bytes().await.unwrap();
    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
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
    let merged = mutated.to_bytes().await.unwrap();
    let merged_store = VortexRdfStore::from_bytes(&merged).await.unwrap();
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
/// to the same code through the fence-guided child probe as through the
/// resident dictionary, and mutated absent terms must come back `None`.
#[tokio::test]
async fn test_file_backed_dictionary_fence_probe_parity() {
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
    let path = dir.join("data.vortex");
    crate::io::quads_stream_to_vortex_file_with_builder::<SortedStreamBuilder, _>(
        quad_stream(quads.clone()),
        &path,
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();

    // The reference answers, from a resident open of the same file.
    let resident = VortexRdfStore::from_file_with_dict_residency(&path, u64::MAX)
        .await
        .unwrap();
    let dict = resident.dictionary_snapshot().unwrap().0;

    // The probe target, built exactly as `from_file` does file-backed: the
    // dictionary child's cached layout reader.
    let outer = NativeStoreFile::try_new(
        crate::io::native_file::open_vortex_file(&path)
            .await
            .unwrap(),
    )
    .unwrap();
    let (_, reader) = outer
        .component_reader(DICT_COMPONENT_NAME)
        .unwrap()
        .expect("dictionary child present");
    let len = dict.len() as u64;
    assert_eq!(reader.row_count(), len);
    let fb = FileBackedDict::new(reader, len);

    // Every ~397th term plus both extremes, probed twice (cold + memo).
    let sample: Vec<u32> = (0..len as u32)
        .step_by(397)
        .chain([0, len as u32 - 1])
        .collect();
    for &code in &sample {
        let term = dict.term_at(code).unwrap();
        assert_eq!(fb.get_id(&term).await.unwrap(), Some(code), "{term}");
        assert_eq!(fb.get_id(&term).await.unwrap(), Some(code), "{term}");

        // A control character keeps the probe inside the same fence
        // window but matches no stored term.
        let absent = format!("{term}\u{1}");
        assert_eq!(fb.get_id(&absent).await.unwrap(), None, "{absent}");
    }
    // Above every stored term: the last split is probed and misses.
    assert_eq!(fb.get_id("\u{10FFFF}").await.unwrap(), None);

    std::fs::remove_dir_all(&dir).ok();
}

/// A probe sorting below the first dictionary term is answered absent by the
/// fence alone (the `partition_point == 0` edge): a dataset whose lowest
/// term is a literal (`"…`) probed with `!`, which sorts before `"`.
#[tokio::test]
async fn test_file_backed_dictionary_fence_rejects_below_first_term() {
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

    let outer = NativeStoreFile::try_new(
        crate::io::native_file::open_vortex_file(&path)
            .await
            .unwrap(),
    )
    .unwrap();
    let (_, reader) = outer
        .component_reader(DICT_COMPONENT_NAME)
        .unwrap()
        .expect("dictionary child present");
    let dict_len = reader.row_count();
    let fb = FileBackedDict::new(reader, dict_len);

    assert_eq!(fb.get_id("!").await.unwrap(), None);
    // And the ordinary path still resolves through the same fence.
    assert_eq!(fb.get_id(&first_term).await.unwrap(), Some(0));

    std::fs::remove_dir_all(&dir).ok();
}

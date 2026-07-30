//! Shared bounded-memory native RDF serialization primitives.
//!
//! Used by the unified v10 writer and feature-gated legacy builders.

use super::*;

#[derive(Clone, Debug)]
pub(super) struct NativeDictPair {
    pub(super) id: u32,
    pub(super) term: String,
}
#[derive(Clone, Copy, Debug)]
pub(super) enum PairRunOrder {
    Term,
    Id,
}
/// Sort/spill representation. This is deliberately string-based only for the
/// external-sort phase, where the strings live on disk, not in memory.
#[derive(Clone, Debug)]
pub(super) struct NativeTriple {
    s: String,
    p: String,
    o: String,
    g: String,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct NativeIdTriple {
    pub(super) s: u32,
    pub(super) p: u32,
    pub(super) o: u32,
    pub(super) g: u32,
}
pub(super) fn quad_to_native_triple(quad: &Quad) -> NativeTriple {
    NativeTriple {
        s: quad.subject.to_string(),
        p: quad.predicate.to_string(),
        o: quad.object.to_string(),
        g: quad.graph_name.to_string(),
    }
}
pub(super) async fn spill_sorted_native_id_string_runs<S>(
    mut quad_stream: S,
    ordering: TripleOrdering,
    sort_batch_size: usize,
    temp_dir: &Path,
) -> Result<Vec<PathBuf>>
where
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    let sort_batch_size = sort_batch_size.max(1);
    let mut runs = Vec::new();
    let mut batch = Vec::with_capacity(sort_batch_size);
    let mut run_idx = 0usize;

    while let Some(item) = quad_stream.next().await {
        let quad = item?;
        batch.push(quad_to_native_triple(&quad));

        if batch.len() >= sort_batch_size {
            flush_string_run(&mut batch, ordering, temp_dir, run_idx, &mut runs)?;
            run_idx += 1;
        }
    }

    if !batch.is_empty() {
        flush_string_run(&mut batch, ordering, temp_dir, run_idx, &mut runs)?;
    }

    Ok(runs)
}
pub(super) fn flush_string_run(
    batch: &mut Vec<NativeTriple>,
    ordering: TripleOrdering,
    temp_dir: &Path,
    run_idx: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    if ordering != TripleOrdering::None {
        batch.sort_by(|a, b| a.cmp_by_order(b, ordering));
    }
    let path = temp_dir.join(format!("native_id_string_run_{run_idx:06}.tsv"));
    write_native_string_run(&path, batch)?;
    runs.push(path);
    batch.clear();
    Ok(())
}
pub(super) fn build_dictionary_and_pair_runs<Dict>(
    dictionary: &mut Dict,
    string_run_paths: &[PathBuf],
    temp_dir: &Path,
) -> Result<PairRunPaths>
where
    Dict: RdfDictionary,
{
    let pair_batch_size = std::env::var("VORTEX_RDF_NATIVE_ID_PAIR_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);

    let mut batch: Vec<NativeDictPair> = Vec::with_capacity(pair_batch_size);

    let mut term_run_paths = Vec::new();
    let mut id_run_paths = Vec::new();
    let mut run_idx = 0usize;

    for path in string_run_paths {
        let mut reader = NativeStringRunReader::new(path)?;

        while let Some(triple) = reader.read_one()? {
            insert_term_and_record_pair(dictionary, &triple.s, &mut batch)?;
            insert_term_and_record_pair(dictionary, &triple.p, &mut batch)?;
            insert_term_and_record_pair(dictionary, &triple.o, &mut batch)?;
            insert_term_and_record_pair(dictionary, &triple.g, &mut batch)?;

            if batch.len() >= pair_batch_size {
                flush_pair_runs(
                    &mut batch,
                    temp_dir,
                    run_idx,
                    &mut term_run_paths,
                    &mut id_run_paths,
                )?;
                run_idx += 1;
            }
        }
    }

    if !batch.is_empty() {
        flush_pair_runs(
            &mut batch,
            temp_dir,
            run_idx,
            &mut term_run_paths,
            &mut id_run_paths,
        )?;
    }

    Ok(PairRunPaths {
        term_run_paths,
        id_run_paths,
    })
}
#[derive(Clone, Debug)]
pub(super) struct PairRunPaths {
    pub(super) term_run_paths: Vec<PathBuf>,
    pub(super) id_run_paths: Vec<PathBuf>,
}
pub(super) fn insert_term_and_record_pair<Dict>(
    dictionary: &mut Dict,
    term: &str,
    batch: &mut Vec<NativeDictPair>,
) -> Result<()>
where
    Dict: RdfDictionary,
{
    if dictionary.get_id(term).is_none() {
        dictionary.get_or_insert(term);

        let id = dictionary.get_id(term).ok_or_else(|| {
            VortexRdfError::Serialization(format!(
                "Dictionary inserted term but get_id failed afterward: {}",
                term
            ))
        })?;

        batch.push(NativeDictPair {
            id,
            term: term.to_string(),
        });
    }

    Ok(())
}
pub(super) fn flush_pair_runs(
    batch: &mut Vec<NativeDictPair>,
    temp_dir: &Path,
    run_idx: usize,
    term_run_paths: &mut Vec<PathBuf>,
    id_run_paths: &mut Vec<PathBuf>,
) -> Result<()> {
    let term_path = temp_dir.join(format!("native_id_pair_term_run_{run_idx:06}.tsv"));
    let id_path = temp_dir.join(format!("native_id_pair_id_run_{run_idx:06}.tsv"));

    batch.sort_by(|a, b| a.term.cmp(&b.term).then_with(|| a.id.cmp(&b.id)));
    write_pair_run(&term_path, batch)?;
    term_run_paths.push(term_path);

    batch.sort_by_key(|p| p.id);
    write_pair_run(&id_path, batch)?;
    id_run_paths.push(id_path);

    batch.clear();

    Ok(())
}
pub(super) fn write_pair_run(path: &Path, pairs: &[NativeDictPair]) -> Result<()> {
    let file =
        std::fs::File::create(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let mut writer = BufWriter::new(file);

    for pair in pairs {
        writeln!(writer, "{}\t{}", pair.id, escape_run_field(&pair.term))
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }

    writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    Ok(())
}
pub(super) struct PairRunReader {
    reader: BufReader<std::fs::File>,
}
pub(super) struct PairHeapItem {
    pub(super) pair: NativeDictPair,
    pub(super) run_idx: usize,
    order: PairRunOrder,
}
pub(super) fn encode_string_runs_to_id_runs<Dict>(
    dictionary: &Dict,
    string_run_paths: &[PathBuf],
    ordering: TripleOrdering,
    temp_dir: &Path,
) -> Result<Vec<PathBuf>>
where
    Dict: RdfDictionary,
{
    let mut id_run_paths = Vec::with_capacity(string_run_paths.len());
    for (run_idx, string_path) in string_run_paths.iter().enumerate() {
        let id_path = temp_dir.join(format!("native_id_encoded_run_{run_idx:06}.bin"));
        let mut reader = NativeStringRunReader::new(string_path)?;
        let mut encoded_batch: Vec<NativeIdTriple> = Vec::new();

        while let Some(triple) = reader.read_one()? {
            encoded_batch.push(NativeIdTriple {
                s: dictionary.get_id(&triple.s).ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "Missing subject in dictionary: {}",
                        triple.s
                    ))
                })?,
                p: dictionary.get_id(&triple.p).ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "Missing predicate in dictionary: {}",
                        triple.p
                    ))
                })?,
                o: dictionary.get_id(&triple.o).ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "Missing object in dictionary: {}",
                        triple.o
                    ))
                })?,
                g: dictionary.get_id(&triple.g).ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "Missing graph in dictionary: {}",
                        triple.g
                    ))
                })?,
            });
        }

        // Critical v4 invariant fix: string runs were sorted by RDF lexical term order,
        // but dictionary IDs are assigned independently. After encoding strings -> u32 IDs,
        // each run must be re-sorted by native-ID order before the final k-way merge.
        if ordering != TripleOrdering::None {
            encoded_batch.sort_by(|a, b| a.cmp_by_order(b, ordering));

            for pair in encoded_batch.windows(2) {
                if pair[0].cmp_by_order(&pair[1], ordering) == Ordering::Greater {
                    return Err(VortexRdfError::Serialization(format!(
                        "Encoded native ID run {} is not sorted by {:?}",
                        run_idx, ordering
                    )));
                }
            }
        }

        let file = std::fs::File::create(&id_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        let mut writer = BufWriter::new(file);
        for encoded in encoded_batch {
            write_id_triple(&mut writer, encoded)?;
        }
        writer
            .flush()
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        id_run_paths.push(id_path);
    }
    Ok(id_run_paths)
}
pub(super) fn write_native_string_run(path: &Path, triples: &[NativeTriple]) -> Result<()> {
    let file =
        std::fs::File::create(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let mut writer = BufWriter::new(file);
    for q in triples {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}",
            escape_run_field(&q.s),
            escape_run_field(&q.p),
            escape_run_field(&q.o),
            escape_run_field(&q.g),
        )
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }
    writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    Ok(())
}
pub(super) fn escape_run_field(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
pub(super) fn unescape_run_field(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}
pub(super) struct NativeStringRunReader {
    reader: BufReader<std::fs::File>,
}
pub(super) fn write_id_triple<W: Write>(writer: &mut W, triple: NativeIdTriple) -> Result<()> {
    writer
        .write_all(&triple.s.to_le_bytes())
        .and_then(|_| writer.write_all(&triple.p.to_le_bytes()))
        .and_then(|_| writer.write_all(&triple.o.to_le_bytes()))
        .and_then(|_| writer.write_all(&triple.g.to_le_bytes()))
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))
}
pub(super) struct NativeIdRunReader {
    reader: BufReader<std::fs::File>,
}
pub(super) struct IdRunHeapItem {
    pub(super) triple: NativeIdTriple,
    pub(super) run_idx: usize,
    pub(super) ordering: TripleOrdering,
}
pub(super) fn merge_sorted_id_runs_to_array_stream(
    run_paths: Vec<PathBuf>,
    ordering: TripleOrdering,
    row_group_size: usize,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let row_group_size = row_group_size.max(1);
    Ok(async_stream::try_stream! {
        let mut readers = Vec::with_capacity(run_paths.len());
        for path in &run_paths {
            readers.push(NativeIdRunReader::new(path).map_err(rdf_err_to_vortex_err)?);
        }

        let mut heap = BinaryHeap::new();
        for run_idx in 0..readers.len() {
            if let Some(triple) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(IdRunHeapItem { triple, run_idx, ordering });
            }
        }

        let mut s_ids = Vec::with_capacity(row_group_size);
        let mut p_ids = Vec::with_capacity(row_group_size);
        let mut o_ids = Vec::with_capacity(row_group_size);
        let mut g_ids = Vec::with_capacity(row_group_size);

        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            s_ids.push(item.triple.s);
            p_ids.push(item.triple.p);
            o_ids.push(item.triple.o);
            g_ids.push(item.triple.g);

            if let Some(next) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(IdRunHeapItem { triple: next, run_idx, ordering });
            }

            if s_ids.len() >= row_group_size {
                let array = build_spog_array(
                    std::mem::take(&mut s_ids),
                    std::mem::take(&mut p_ids),
                    std::mem::take(&mut o_ids),
                    std::mem::take(&mut g_ids),
                ).map_err(rdf_err_to_vortex_err)?;
                s_ids = Vec::with_capacity(row_group_size);
                p_ids = Vec::with_capacity(row_group_size);
                o_ids = Vec::with_capacity(row_group_size);
                g_ids = Vec::with_capacity(row_group_size);
                yield array;
            }
        }

        if !s_ids.is_empty() {
            yield build_spog_array(s_ids, p_ids, o_ids, g_ids).map_err(rdf_err_to_vortex_err)?;
        } else if readers.is_empty() {
            yield empty_spog_array().map_err(rdf_err_to_vortex_err)?;
        }
    })
}
pub(super) fn build_spog_array(
    s_ids: Vec<u32>,
    p_ids: Vec<u32>,
    o_ids: Vec<u32>,
    g_ids: Vec<u32>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("s", PrimitiveArray::from_iter(s_ids).into_array()),
        ("p", PrimitiveArray::from_iter(p_ids).into_array()),
        ("o", PrimitiveArray::from_iter(o_ids).into_array()),
        ("g", PrimitiveArray::from_iter(g_ids).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|arr| arr.into_array())
}
pub(super) fn empty_spog_array() -> Result<ArrayRef> {
    build_spog_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())
}
pub(super) fn rdf_err_to_vortex_err(e: VortexRdfError) -> VortexError {
    vortex_error::vortex_err!(
        "vortex-rdf error while streaming native string row group: {}",
        e
    )
}
pub(super) fn build_native_dictionary_array(ids: Vec<u32>, terms: Vec<String>) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("id", PrimitiveArray::from_iter(ids).into_array()),
        ("term", VarBinArray::from(terms).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}
pub(super) fn empty_native_dictionary_array() -> Result<ArrayRef> {
    build_native_dictionary_array(Vec::new(), Vec::new())
}
pub(super) fn dictionary_pair_stream(
    run_paths: Vec<PathBuf>,
    order: PairRunOrder,
    row_group_size: usize,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let row_group_size = row_group_size.max(1);
    Ok(async_stream::try_stream! {
        let mut readers = run_paths
            .iter()
            .map(|path| PairRunReader::new(path))
            .collect::<Result<Vec<_>>>()
            .map_err(rdf_err_to_vortex_err)?;
        let mut heap = BinaryHeap::new();
        for (run_idx, reader) in readers.iter_mut().enumerate() {
            if let Some(pair) = reader.read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(PairHeapItem { pair, run_idx, order });
            }
        }
        let mut ids = Vec::with_capacity(row_group_size);
        let mut terms = Vec::with_capacity(row_group_size);
        let mut previous_id = None;
        let mut previous_term: Option<String> = None;
        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            let pair = item.pair;
            match order {
                PairRunOrder::Id => {
                    if previous_id.is_some_and(|previous| pair.id <= previous) {
                        Err(vortex_error::vortex_err!(
                            "id_to_term dictionary is not strictly ordered: previous={previous_id:?}, next={}",
                            pair.id
                        ))?;
                    }
                    previous_id = Some(pair.id);
                }
                PairRunOrder::Term => {
                    if previous_term.as_ref().is_some_and(|previous| pair.term <= *previous) {
                        Err(vortex_error::vortex_err!(
                            "term_to_id dictionary is not strictly ordered"
                        ))?;
                    }
                    previous_term = Some(pair.term.clone());
                }
            }
            ids.push(pair.id);
            terms.push(pair.term);
            if let Some(next) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(PairHeapItem { pair: next, run_idx, order });
            }
            if ids.len() >= row_group_size {
                yield build_native_dictionary_array(
                    std::mem::take(&mut ids),
                    std::mem::take(&mut terms),
                ).map_err(rdf_err_to_vortex_err)?;
                ids = Vec::with_capacity(row_group_size);
                terms = Vec::with_capacity(row_group_size);
            }
        }
        if !ids.is_empty() {
            yield build_native_dictionary_array(ids, terms).map_err(rdf_err_to_vortex_err)?;
        } else if readers.is_empty() {
            yield empty_native_dictionary_array().map_err(rdf_err_to_vortex_err)?;
        }
    })
}
// VORTEX_RDF_NATIVE_ID_TO_TERM_COMPONENT_V1
#[derive(Clone)]
pub(super) struct PairRunDictionarySource {
    run_paths: Arc<[PathBuf]>,
    order: PairRunOrder,
    row_group_size: usize,
    dtype: vortex_array::dtype::DType,
}
// VORTEX_RDF_SHARED_NATIVE_BUILD_CONTEXT_TERM_DIRECTORY_V1
pub(super) fn native_term_directory_stream(
    run_paths: Vec<PathBuf>,
    fence_rows: usize,
    batch_size: usize,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let fence_rows = fence_rows.max(1);
    let batch_size = batch_size.max(1);
    Ok(async_stream::try_stream! {
        let mut readers = Vec::with_capacity(run_paths.len());
        for path in &run_paths {
            readers.push(PairRunReader::new(path).map_err(rdf_err_to_vortex_err)?);
        }
        let mut heap = BinaryHeap::new();
        for run_idx in 0..readers.len() {
            if let Some(pair) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(PairHeapItem { pair, run_idx, order: PairRunOrder::Term });
            }
        }
        let mut first = Vec::with_capacity(batch_size);
        let mut last = Vec::with_capacity(batch_size);
        let mut starts = Vec::with_capacity(batch_size);
        let mut ends = Vec::with_capacity(batch_size);
        let mut previous: Option<String> = None;
        let mut fence_first: Option<String> = None;
        let mut fence_last: Option<String> = None;
        let mut fence_start = 0u64;
        let mut fence_len = 0usize;
        let mut row = 0u64;
        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            let pair = item.pair;
            if previous.as_ref().is_some_and(|value| value >= &pair.term) {
                Err(vortex_error::vortex_err!("term directory source is not strictly lexical"))?;
            }
            if fence_first.is_none() {
                fence_start = row;
                fence_first = Some(pair.term.clone());
            }
            fence_last = Some(pair.term.clone());
            previous = Some(pair.term);
            fence_len += 1;
            row += 1;
            if let Some(next) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(PairHeapItem { pair: next, run_idx, order: PairRunOrder::Term });
            }
            if fence_len == fence_rows {
                first.push(fence_first.take().expect("fence first is present"));
                last.push(fence_last.take().expect("fence last is present"));
                starts.push(fence_start);
                ends.push(row);
                fence_len = 0;
            }
            if first.len() >= batch_size {
                yield build_native_term_directory_array(
                    std::mem::take(&mut first),
                    std::mem::take(&mut last),
                    std::mem::take(&mut starts),
                    std::mem::take(&mut ends),
                ).map_err(rdf_err_to_vortex_err)?;
                first = Vec::with_capacity(batch_size);
                last = Vec::with_capacity(batch_size);
                starts = Vec::with_capacity(batch_size);
                ends = Vec::with_capacity(batch_size);
            }
        }
        if fence_len != 0 {
            first.push(fence_first.take().expect("partial fence first is present"));
            last.push(fence_last.take().expect("partial fence last is present"));
            starts.push(fence_start);
            ends.push(row);
        }
        if !first.is_empty() {
            yield build_native_term_directory_array(first, last, starts, ends)
                .map_err(rdf_err_to_vortex_err)?;
        } else if readers.is_empty() {
            yield build_native_term_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())
                .map_err(rdf_err_to_vortex_err)?;
        }
    })
}
#[derive(Clone)]
pub(super) struct NativeTermDirectorySource {
    run_paths: Arc<[PathBuf]>,
    fence_rows: usize,
    batch_size: usize,
    dtype: vortex_array::dtype::DType,
}
// VORTEX_RDF_NATIVE_SUBJECT_RANGES_COMPONENT_V1
pub(super) fn build_native_subject_range_array(
    subject_ids: Vec<u32>,
    row_starts: Vec<u64>,
    row_ends: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        (
            "subject_id",
            PrimitiveArray::from_iter(subject_ids).into_array(),
        ),
        (
            "row_start",
            PrimitiveArray::from_iter(row_starts).into_array(),
        ),
        ("row_end", PrimitiveArray::from_iter(row_ends).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}
pub(super) fn empty_native_subject_range_array() -> Result<ArrayRef> {
    build_native_subject_range_array(Vec::new(), Vec::new(), Vec::new())
}
pub(super) fn native_subject_range_stream(
    run_paths: Vec<PathBuf>,
    batch_size: usize,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let batch_size = batch_size.max(1);
    Ok(async_stream::try_stream! {
        let mut readers = Vec::with_capacity(run_paths.len());
        for path in &run_paths {
            readers.push(NativeIdRunReader::new(path).map_err(rdf_err_to_vortex_err)?);
        }
        let mut heap = BinaryHeap::new();
        for run_idx in 0..readers.len() {
            if let Some(triple) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(IdRunHeapItem { triple, run_idx, ordering: TripleOrdering::SPO });
            }
        }
        let mut current_subject = None;
        let mut current_start = 0u64;
        let mut row = 0u64;
        let mut subject_ids = Vec::with_capacity(batch_size);
        let mut row_starts = Vec::with_capacity(batch_size);
        let mut row_ends = Vec::with_capacity(batch_size);
        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            let subject = item.triple.s;
            match current_subject {
                None => {
                    current_subject = Some(subject);
                    current_start = row;
                }
                Some(previous) if previous != subject => {
                    if subject < previous {
                        Err(vortex_error::vortex_err!("SPO merge is not ordered by subject ID"))?;
                    }
                    subject_ids.push(previous);
                    row_starts.push(current_start);
                    row_ends.push(row);
                    current_subject = Some(subject);
                    current_start = row;
                }
                Some(_) => {}
            }
            row += 1;
            if let Some(next) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(IdRunHeapItem {
                    triple: next,
                    run_idx,
                    ordering: TripleOrdering::SPO,
                });
            }
            if subject_ids.len() >= batch_size {
                yield build_native_subject_range_array(
                    std::mem::take(&mut subject_ids),
                    std::mem::take(&mut row_starts),
                    std::mem::take(&mut row_ends),
                ).map_err(rdf_err_to_vortex_err)?;
                subject_ids = Vec::with_capacity(batch_size);
                row_starts = Vec::with_capacity(batch_size);
                row_ends = Vec::with_capacity(batch_size);
            }
        }
        if let Some(subject) = current_subject {
            subject_ids.push(subject);
            row_starts.push(current_start);
            row_ends.push(row);
        }
        if !subject_ids.is_empty() {
            yield build_native_subject_range_array(subject_ids, row_starts, row_ends)
                .map_err(rdf_err_to_vortex_err)?;
        } else if readers.is_empty() {
            yield empty_native_subject_range_array().map_err(rdf_err_to_vortex_err)?;
        }
    })
}
// VORTEX_RDF_NATIVE_PREDICATE_RUN_INDEX_V1
pub(super) fn build_native_predicate_run_array(
    predicate_ids: Vec<u32>,
    row_starts: Vec<u64>,
    row_ends: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        (
            "predicate_id",
            PrimitiveArray::from_iter(predicate_ids).into_array(),
        ),
        (
            "row_start",
            PrimitiveArray::from_iter(row_starts).into_array(),
        ),
        ("row_end", PrimitiveArray::from_iter(row_ends).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}
pub(super) fn empty_native_predicate_run_array() -> Result<ArrayRef> {
    build_native_predicate_run_array(Vec::new(), Vec::new(), Vec::new())
}
/// Stream maximal contiguous predicate runs in physical SPO row order.
///
/// This is deliberately a run posting index, not one posting per triple. It is
/// exact and bounded-memory; a future predicate-ordered QuadSource can replace
/// it if measurements show excessive fragmentation for high-cardinality data.
pub(super) fn native_predicate_run_stream(
    run_paths: Vec<PathBuf>,
    batch_size: usize,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let batch_size = batch_size.max(1);
    Ok(async_stream::try_stream! {
        let mut readers = Vec::with_capacity(run_paths.len());
        for path in &run_paths {
            readers.push(NativeIdRunReader::new(path).map_err(rdf_err_to_vortex_err)?);
        }
        let mut heap = BinaryHeap::new();
        for run_idx in 0..readers.len() {
            if let Some(triple) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(IdRunHeapItem { triple, run_idx, ordering: TripleOrdering::SPO });
            }
        }
        let mut current_predicate = None;
        let mut current_start = 0u64;
        let mut row = 0u64;
        let mut predicate_ids = Vec::with_capacity(batch_size);
        let mut row_starts = Vec::with_capacity(batch_size);
        let mut row_ends = Vec::with_capacity(batch_size);
        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            let predicate = item.triple.p;
            match current_predicate {
                None => {
                    current_predicate = Some(predicate);
                    current_start = row;
                }
                Some(previous) if previous != predicate => {
                    predicate_ids.push(previous);
                    row_starts.push(current_start);
                    row_ends.push(row);
                    current_predicate = Some(predicate);
                    current_start = row;
                }
                Some(_) => {}
            }
            row += 1;
            if let Some(next) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(IdRunHeapItem {
                    triple: next,
                    run_idx,
                    ordering: TripleOrdering::SPO,
                });
            }
            if predicate_ids.len() >= batch_size {
                yield build_native_predicate_run_array(
                    std::mem::take(&mut predicate_ids),
                    std::mem::take(&mut row_starts),
                    std::mem::take(&mut row_ends),
                ).map_err(rdf_err_to_vortex_err)?;
                predicate_ids = Vec::with_capacity(batch_size);
                row_starts = Vec::with_capacity(batch_size);
                row_ends = Vec::with_capacity(batch_size);
            }
        }
        if let Some(predicate) = current_predicate {
            predicate_ids.push(predicate);
            row_starts.push(current_start);
            row_ends.push(row);
        }
        if !predicate_ids.is_empty() {
            yield build_native_predicate_run_array(predicate_ids, row_starts, row_ends)
                .map_err(rdf_err_to_vortex_err)?;
        } else if readers.is_empty() {
            yield empty_native_predicate_run_array().map_err(rdf_err_to_vortex_err)?;
        }
    })
}
#[derive(Clone)]
pub(super) struct NativePredicateRunsSource {
    run_paths: Arc<[PathBuf]>,
    batch_size: usize,
    dtype: vortex_array::dtype::DType,
}
#[derive(Clone)]
pub(super) struct NativeSubjectRangesSource {
    run_paths: Arc<[PathBuf]>,
    batch_size: usize,
    dtype: vortex_array::dtype::DType,
}
pub(super) fn build_native_term_directory_array(
    first: Vec<String>,
    last: Vec<String>,
    starts: Vec<u64>,
    ends: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("first_term", VarBinArray::from(first).into_array()),
        ("last_term", VarBinArray::from(last).into_array()),
        ("row_start", PrimitiveArray::from_iter(starts).into_array()),
        ("row_end", PrimitiveArray::from_iter(ends).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

// Implementations colocated with their serialization types.
impl NativeTriple {
    fn cmp_by_order(&self, other: &Self, ordering: TripleOrdering) -> Ordering {
        match ordering {
            TripleOrdering::SPO => self
                .s
                .cmp(&other.s)
                .then_with(|| self.p.cmp(&other.p))
                .then_with(|| self.o.cmp(&other.o))
                .then_with(|| self.g.cmp(&other.g)),
            TripleOrdering::PSO => self
                .p
                .cmp(&other.p)
                .then_with(|| self.s.cmp(&other.s))
                .then_with(|| self.o.cmp(&other.o))
                .then_with(|| self.g.cmp(&other.g)),
            TripleOrdering::OSP => self
                .o
                .cmp(&other.o)
                .then_with(|| self.s.cmp(&other.s))
                .then_with(|| self.p.cmp(&other.p))
                .then_with(|| self.g.cmp(&other.g)),
            TripleOrdering::None => Ordering::Equal,
        }
    }
}
impl NativeIdTriple {
    fn cmp_by_order(&self, other: &Self, ordering: TripleOrdering) -> Ordering {
        match ordering {
            TripleOrdering::SPO => self
                .s
                .cmp(&other.s)
                .then_with(|| self.p.cmp(&other.p))
                .then_with(|| self.o.cmp(&other.o))
                .then_with(|| self.g.cmp(&other.g)),
            TripleOrdering::PSO => self
                .p
                .cmp(&other.p)
                .then_with(|| self.s.cmp(&other.s))
                .then_with(|| self.o.cmp(&other.o))
                .then_with(|| self.g.cmp(&other.g)),
            TripleOrdering::OSP => self
                .o
                .cmp(&other.o)
                .then_with(|| self.s.cmp(&other.s))
                .then_with(|| self.p.cmp(&other.p))
                .then_with(|| self.g.cmp(&other.g)),
            TripleOrdering::None => Ordering::Equal,
        }
    }
}
impl PairRunReader {
    fn new(path: &Path) -> Result<Self> {
        let file =
            std::fs::File::open(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn read_one(&mut self) -> Result<Option<NativeDictPair>> {
        let mut line = String::new();

        let n = self
            .reader
            .read_line(&mut line)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        if n == 0 {
            return Ok(None);
        }

        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }

        let mut parts = line.splitn(2, '\t');

        let id_raw = parts
            .next()
            .ok_or_else(|| VortexRdfError::Serialization("Malformed dictionary pair run".into()))?;

        let term_raw = parts
            .next()
            .ok_or_else(|| VortexRdfError::Serialization("Malformed dictionary pair run".into()))?;

        let id = id_raw
            .parse::<u32>()
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        Ok(Some(NativeDictPair {
            id,
            term: unescape_run_field(term_raw),
        }))
    }
}
impl PartialEq for PairHeapItem {
    fn eq(&self, other: &Self) -> bool {
        match self.order {
            PairRunOrder::Term => {
                self.pair.term == other.pair.term && self.pair.id == other.pair.id
            }
            PairRunOrder::Id => self.pair.id == other.pair.id,
        }
    }
}
impl Eq for PairHeapItem {}
impl PartialOrd for PairHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PairHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.order {
            PairRunOrder::Term => other
                .pair
                .term
                .cmp(&self.pair.term)
                .then_with(|| other.pair.id.cmp(&self.pair.id))
                .then_with(|| other.run_idx.cmp(&self.run_idx)),

            PairRunOrder::Id => other
                .pair
                .id
                .cmp(&self.pair.id)
                .then_with(|| other.run_idx.cmp(&self.run_idx)),
        }
    }
}
impl NativeStringRunReader {
    fn new(path: &Path) -> Result<Self> {
        let file =
            std::fs::File::open(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn read_one(&mut self) -> Result<Option<NativeTriple>> {
        let mut line = String::new();
        let n = self
            .reader
            .read_line(&mut line)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        if n == 0 {
            return Ok(None);
        }
        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }
        let mut parts = line.splitn(4, '\t');
        let s = parts.next().ok_or_else(|| {
            VortexRdfError::Serialization("Malformed native ID string run".into())
        })?;
        let p = parts.next().ok_or_else(|| {
            VortexRdfError::Serialization("Malformed native ID string run".into())
        })?;
        let o = parts.next().ok_or_else(|| {
            VortexRdfError::Serialization("Malformed native ID string run".into())
        })?;
        let g = parts.next().ok_or_else(|| {
            VortexRdfError::Serialization("Malformed native ID string run".into())
        })?;

        Ok(Some(NativeTriple {
            s: unescape_run_field(s),
            p: unescape_run_field(p),
            o: unescape_run_field(o),
            g: unescape_run_field(g),
        }))
    }
}
impl NativeIdRunReader {
    pub(super) fn new(path: &Path) -> Result<Self> {
        let file =
            std::fs::File::open(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    pub(super) fn read_one(&mut self) -> Result<Option<NativeIdTriple>> {
        let mut buf = [0u8; 16];

        match self.reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(NativeIdTriple {
                s: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
                p: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
                o: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
                g: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            })),

            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),

            Err(e) => Err(VortexRdfError::Serialization(e.to_string())),
        }
    }
}
impl PartialEq for IdRunHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.triple.cmp_by_order(&other.triple, self.ordering) == Ordering::Equal
    }
}
impl Eq for IdRunHeapItem {}
impl PartialOrd for IdRunHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for IdRunHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: BinaryHeap is max-heap; we need min-heap.
        other
            .triple
            .cmp_by_order(&self.triple, self.ordering)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl PairRunDictionarySource {
    pub(super) fn new(
        run_paths: &[PathBuf],
        order: PairRunOrder,
        row_group_size: usize,
    ) -> Result<Self> {
        Ok(Self {
            run_paths: run_paths.to_vec().into(),
            order,
            row_group_size: row_group_size.max(1),
            dtype: empty_native_dictionary_array()?.dtype().clone(),
        })
    }
}
impl NativeComponentSource for PairRunDictionarySource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let stream =
            dictionary_pair_stream(self.run_paths.to_vec(), self.order, self.row_group_size)
                .map_err(rdf_err_to_vortex_err)?;
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }

    // FUTURE(performance): report file-reader buffers once run-backed sources
    // expose explicit buffered-memory accounting.
}
impl NativeTermDirectorySource {
    pub(super) fn new(run_paths: &[PathBuf], fence_rows: usize, batch_size: usize) -> Result<Self> {
        Ok(Self {
            run_paths: run_paths.to_vec().into(),
            fence_rows: fence_rows.max(1),
            batch_size: batch_size.max(1),
            dtype: build_native_term_directory_array(
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )?
            .dtype()
            .clone(),
        })
    }
}
impl NativeComponentSource for NativeTermDirectorySource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let stream =
            native_term_directory_stream(self.run_paths.to_vec(), self.fence_rows, self.batch_size)
                .map_err(rdf_err_to_vortex_err)?;
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }
}
impl NativePredicateRunsSource {
    pub(super) fn new(run_paths: &[PathBuf], batch_size: usize) -> Result<Self> {
        Ok(Self {
            run_paths: run_paths.to_vec().into(),
            batch_size: batch_size.max(1),
            dtype: empty_native_predicate_run_array()?.dtype().clone(),
        })
    }
}
impl NativeComponentSource for NativePredicateRunsSource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let stream = native_predicate_run_stream(self.run_paths.to_vec(), self.batch_size)
            .map_err(rdf_err_to_vortex_err)?;
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }
}
impl NativeSubjectRangesSource {
    pub(super) fn new(run_paths: &[PathBuf], batch_size: usize) -> Result<Self> {
        Ok(Self {
            run_paths: run_paths.to_vec().into(),
            batch_size: batch_size.max(1),
            dtype: empty_native_subject_range_array()?.dtype().clone(),
        })
    }
}
impl NativeComponentSource for NativeSubjectRangesSource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let stream = native_subject_range_stream(self.run_paths.to_vec(), self.batch_size)
            .map_err(rdf_err_to_vortex_err)?;
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }
}

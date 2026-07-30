//! Legacy sidecar result decoding and compact-result APIs.
//!
//! This module is excluded entirely from unified-only builds.

use super::*;

pub async fn match_cottas_native_file_as_triples(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<Vec<(String, String, String)>> {
    let (filter, _term_lookup_ms) =
        build_native_pattern_filter_lazy_with_stats(input_path, subject, predicate, object, graph)
            .await?;

    if matches!(filter, NativePatternFilter::Empty) {
        return Ok(Vec::new());
    }

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;

    let scan = file.scan().map_err(VortexRdfError::from)?;

    let scan = match filter {
        NativePatternFilter::All => scan,
        NativePatternFilter::Empty => unreachable!("handled above"),
        NativePatternFilter::Expr(expr) => scan.with_filter(expr),
    };

    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;

    let matched_quads = stream.read_all().await.map_err(VortexRdfError::from)?;

    if matched_quads.len() == 0 {
        return Ok(Vec::new());
    }

    let (s_ids, p_ids, o_ids, g_ids) = extract_spog_id_columns(&matched_quads)?;
    let unique_ids = collect_unique_ids(&s_ids, &p_ids, &o_ids, &g_ids);
    let id_to_term = lookup_terms_by_ids_from_sidecar(input_path, &unique_ids).await?;

    let mut out = Vec::with_capacity(s_ids.len());

    for i in 0..s_ids.len() {
        let s = id_to_term
            .get(&s_ids[i])
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "S ID {} missing from id_to_term sidecar",
                    s_ids[i]
                ))
            })?
            .clone();

        let p = id_to_term
            .get(&p_ids[i])
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "P ID {} missing from id_to_term sidecar",
                    p_ids[i]
                ))
            })?
            .clone();

        let o = id_to_term
            .get(&o_ids[i])
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "O ID {} missing from id_to_term sidecar",
                    o_ids[i]
                ))
            })?
            .clone();

        out.push((s, p, o));
    }

    Ok(out)
}

/// Executes the same optimized access planner used by
/// `match_cottas_native_file_with_diagnostics`, but returns triples for the
/// Python/RDFLib binding.
///
/// The indexed planner returns projected native-ID rows directly; only IDs from
/// unbound output columns are decoded before constructing returned triples.
pub async fn match_cottas_native_file_as_triples_optimized(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<Vec<(String, String, String)>> {
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;

    projected_native_id_rows_as_triples(input_path, &planned.rows, &planned.bound_terms).await
}

// VORTEX_RDF_COMPACT_PYTHON_RESULT_V1
#[derive(Clone, Debug, Default)]
pub struct NativeCompactTripleBatch {
    pub terms: Vec<String>,
    pub rows: Vec<(u32, u32, u32)>,
}

// VORTEX_RDF_DIRECT_COMPACT_DECODER_V1
fn append_bound_compact_term(
    value: Option<&str>,
    terms: &mut Vec<String>,
    bound_indexes: &mut HashMap<String, u32>,
) -> Result<Option<u32>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(index) = bound_indexes.get(value) {
        return Ok(Some(*index));
    }
    let index = u32::try_from(terms.len()).map_err(|_| {
        VortexRdfError::InvalidOperation("compact query term table exceeds u32".into())
    })?;
    let owned = value.to_owned();
    terms.push(owned.clone());
    bound_indexes.insert(owned, index);
    Ok(Some(index))
}

fn compact_rows_from_id_indexes(
    rows: &NativeIdBatch,
    bound: &BoundNativeRdfTerms,
    terms: Vec<String>,
    id_indexes: HashMap<u32, u32>,
    bound_s: Option<u32>,
    bound_p: Option<u32>,
    bound_o: Option<u32>,
) -> Result<NativeCompactTripleBatch> {
    let resolve = |fixed: Option<u32>, id: Option<u32>, label: &str| -> Result<u32> {
        if let Some(index) = fixed {
            return Ok(index);
        }
        let id = id.ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "{label} projected ID missing for compact output"
            ))
        })?;
        id_indexes.get(&id).copied().ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "{label} ID {id} missing from compact query dictionary"
            ))
        })
    };
    let mut compact = Vec::with_capacity(rows.rows);
    for i in 0..rows.rows {
        compact.push((
            resolve(bound_s, rows.id_at(NativeIdColumn::Subject, bound, i)?, "S")?,
            resolve(
                bound_p,
                rows.id_at(NativeIdColumn::Predicate, bound, i)?,
                "P",
            )?,
            resolve(bound_o, rows.id_at(NativeIdColumn::Object, bound, i)?, "O")?,
        ));
    }
    Ok(NativeCompactTripleBatch {
        terms,
        rows: compact,
    })
}

// VORTEX_RDF_DIRECT_COMPACT_TIMINGS_V1
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeDirectCompactTimings {
    pub rows_out: usize,
    pub unique_ids: usize,
    pub terms_out: usize,
    pub lexical_bytes: usize,
    // VORTEX_RDF_COMPACT_DICTIONARY_OVERRIDE_V1
    pub dictionary_path: String,
    pub unique_id_collect_ms: f64,
    pub dictionary_open_ms: f64,
    pub row_indices_build_ms: f64,
    pub scan_build_ms: f64,
    pub read_all_ms: f64,
    pub struct_execute_ms: f64,
    pub term_column_execute_ms: f64,
    pub lexical_extract_allocate_ms: f64,
    pub id_index_insert_ms: f64,
    pub bound_term_insert_ms: f64,
    pub compact_row_build_ms: f64,
    pub reconstruction_total_ms: f64,
    pub total_rust_ms: f64,
}

// VORTEX_RDF_STREAM_SELECTED_TERMS_V1
#[derive(Default)]
struct NativeCompactTermDecodeTimings {
    struct_execute_ms: f64,
    term_execute_ms: f64,
    lexical_ms: f64,
    insert_ms: f64,
}

fn append_selected_term_field(
    term_field: &ArrayRef,
    requested: &[u32],
    next_index: &mut usize,
    terms: &mut Vec<String>,
    id_indexes: &mut HashMap<u32, u32>,
    timings: &mut NativeCompactTermDecodeTimings,
) -> Result<()> {
    if let Some(chunks) = term_field.as_opt::<Chunked>() {
        for chunk in chunks.iter_chunks() {
            append_selected_term_field(chunk, requested, next_index, terms, id_indexes, timings)?;
        }
        return Ok(());
    }

    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();
    let stage = Instant::now();
    let term_array = term_field
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    timings.term_execute_ms += elapsed_ms(stage);

    for row in 0..term_array.len() {
        let id = *requested.get(*next_index).ok_or_else(|| {
            VortexRdfError::Deserialization(
                "selected dictionary stream exceeded requested IDs".into(),
            )
        })?;
        let stage = Instant::now();
        let bytes = term_array.bytes_at(row);
        let lexical = std::str::from_utf8(&bytes).map_err(|error| {
            VortexRdfError::Deserialization(format!(
                "direct compact dictionary term for ID {id} is invalid UTF-8: {error}"
            ))
        })?;
        let compact_index = u32::try_from(terms.len()).map_err(|_| {
            VortexRdfError::InvalidOperation("compact query term table exceeds u32".into())
        })?;
        terms.push(lexical.to_owned());
        timings.lexical_ms += elapsed_ms(stage);

        let stage = Instant::now();
        if id_indexes.insert(id, compact_index).is_some() {
            return Err(VortexRdfError::Deserialization(format!(
                "direct compact dictionary returned duplicate ID {id}"
            )));
        }
        timings.insert_ms += elapsed_ms(stage);
        *next_index += 1;
    }
    Ok(())
}

fn append_selected_term_batch(
    batch: ArrayRef,
    requested: &[u32],
    next_index: &mut usize,
    terms: &mut Vec<String>,
    id_indexes: &mut HashMap<u32, u32>,
    timings: &mut NativeCompactTermDecodeTimings,
) -> Result<()> {
    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();
    let stage = Instant::now();
    let struct_array = batch
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    timings.struct_execute_ms += elapsed_ms(stage);
    let term_field = struct_array
        .unmasked_field_by_name("term")
        .map_err(VortexRdfError::Vortex)?
        .clone();
    append_selected_term_field(
        &term_field,
        requested,
        next_index,
        terms,
        id_indexes,
        timings,
    )
}

async fn projected_native_id_rows_as_compact_triples_direct_v1_impl(
    data_path: &Path,
    rows: &NativeIdBatch,
    bound: &BoundNativeRdfTerms,
) -> Result<(NativeCompactTripleBatch, NativeDirectCompactTimings)> {
    let total_start = Instant::now();
    let mut timings = NativeDirectCompactTimings {
        rows_out: rows.rows,
        ..Default::default()
    };
    if rows.rows == 0 {
        timings.total_rust_ms = elapsed_ms(total_start);
        return Ok((NativeCompactTripleBatch::default(), timings));
    }
    let stage = Instant::now();
    let mut requested = rows.unique_unbound_ids(bound);
    requested.sort_unstable();
    requested.dedup();
    timings.unique_id_collect_ms = elapsed_ms(stage);
    timings.unique_ids = requested.len();

    // Resolve through the same override-aware path used by production ID-to-term lookup.
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::DictionaryVortex)?;
    timings.dictionary_path = location.cache_key();
    let stage = Instant::now();
    let file = resolver.open(NativeComponent::DictionaryVortex).await?;
    timings.dictionary_open_ms = elapsed_ms(stage);
    let stage = Instant::now();
    let indices = Buffer::from(
        requested
            .iter()
            .map(|id| u64::from(*id))
            .collect::<Vec<_>>(),
    );
    timings.row_indices_build_ms = elapsed_ms(stage);
    let stage = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_indices(indices)
        .with_projection(vortex_array::expr::select(
            ["term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    timings.scan_build_ms = elapsed_ms(stage);

    let mut terms = Vec::with_capacity(requested.len().saturating_add(3));
    let mut id_indexes = HashMap::with_capacity(requested.len());
    let mut decode_timings = NativeCompactTermDecodeTimings::default();
    let mut index = 0usize;
    loop {
        let stage = Instant::now();
        let next = stream.next().await;
        timings.read_all_ms += elapsed_ms(stage);
        let Some(batch) = next else { break };
        append_selected_term_batch(
            batch.map_err(VortexRdfError::from)?,
            &requested,
            &mut index,
            &mut terms,
            &mut id_indexes,
            &mut decode_timings,
        )?;
    }
    if index != requested.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "selected dictionary stream returned {index} rows for {} requested IDs",
            requested.len()
        )));
    }
    timings.struct_execute_ms = decode_timings.struct_execute_ms;
    timings.term_column_execute_ms = decode_timings.term_execute_ms;
    timings.lexical_extract_allocate_ms = decode_timings.lexical_ms;
    timings.id_index_insert_ms = decode_timings.insert_ms;
    timings.lexical_bytes = terms.iter().map(String::len).sum();
    let stage = Instant::now();
    let mut bound_indexes = HashMap::with_capacity(3);
    let bound_s = append_bound_compact_term(bound.s.as_deref(), &mut terms, &mut bound_indexes)?;
    let bound_p = append_bound_compact_term(bound.p.as_deref(), &mut terms, &mut bound_indexes)?;
    let bound_o = append_bound_compact_term(bound.o.as_deref(), &mut terms, &mut bound_indexes)?;
    timings.bound_term_insert_ms = elapsed_ms(stage);
    let stage = Instant::now();
    let batch =
        compact_rows_from_id_indexes(rows, bound, terms, id_indexes, bound_s, bound_p, bound_o)?;
    timings.compact_row_build_ms = elapsed_ms(stage);
    timings.terms_out = batch.terms.len();
    timings.reconstruction_total_ms = timings.unique_id_collect_ms
        + timings.dictionary_open_ms
        + timings.row_indices_build_ms
        + timings.scan_build_ms
        + timings.read_all_ms
        + timings.struct_execute_ms
        + timings.term_column_execute_ms
        + timings.lexical_extract_allocate_ms
        + timings.id_index_insert_ms
        + timings.bound_term_insert_ms
        + timings.compact_row_build_ms;
    timings.total_rust_ms = elapsed_ms(total_start);
    Ok((batch, timings))
}

async fn projected_native_id_rows_as_compact_triples_direct_v1(
    data_path: &Path,
    rows: &NativeIdBatch,
    bound: &BoundNativeRdfTerms,
) -> Result<NativeCompactTripleBatch> {
    projected_native_id_rows_as_compact_triples_direct_v1_impl(data_path, rows, bound)
        .await
        .map(|v| v.0)
}

pub async fn diagnose_cottas_native_direct_compact(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<NativeDirectCompactTimings> {
    let total = Instant::now();
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;
    let (_batch, mut timings) = projected_native_id_rows_as_compact_triples_direct_v1_impl(
        input_path,
        &planned.rows,
        &planned.bound_terms,
    )
    .await?;
    timings.total_rust_ms = elapsed_ms(total);
    Ok(timings)
}

async fn projected_native_id_rows_as_compact_triples(
    data_path: &Path,
    rows: &NativeIdBatch,
    bound: &BoundNativeRdfTerms,
) -> Result<NativeCompactTripleBatch> {
    projected_native_id_rows_as_compact_triples_direct_v1(data_path, rows, bound).await
}

/// Transfers each lexical term once and rows as indexes into that query-local table.
pub async fn match_cottas_native_file_as_compact_triples(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<NativeCompactTripleBatch> {
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;
    projected_native_id_rows_as_compact_triples(input_path, &planned.rows, &planned.bound_terms)
        .await
}

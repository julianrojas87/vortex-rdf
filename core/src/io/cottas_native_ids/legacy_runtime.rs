//! Legacy sidecar matching, counting, and diagnostics APIs.
//!
//! This module is excluded entirely from unified-only builds.

use super::*;
use async_trait::async_trait;
use vortex::VortexSessionDefault;

// Legacy sidecar lookup, cache, and access subsystem.
pub(super) async fn projected_native_id_rows_as_triples(
    data_path: &Path,
    rows: &NativeIdBatch,
    bound_terms: &BoundNativeRdfTerms,
) -> Result<Vec<(String, String, String)>> {
    if rows.rows == 0 {
        return Ok(Vec::new());
    }

    let unique_ids = rows.unique_unbound_ids(bound_terms);
    let id_to_term = lookup_terms_by_ids_from_sidecar(data_path, &unique_ids).await?;
    let mut triples = Vec::with_capacity(rows.rows);

    for row_idx in 0..rows.rows {
        let s_id = rows.id_at(NativeIdColumn::Subject, bound_terms, row_idx)?;
        let p_id = rows.id_at(NativeIdColumn::Predicate, bound_terms, row_idx)?;
        let o_id = rows.id_at(NativeIdColumn::Object, bound_terms, row_idx)?;

        let subject = lookup_projected_or_use_bound(&id_to_term, &bound_terms.s, s_id, "S")?;
        let predicate = lookup_projected_or_use_bound(&id_to_term, &bound_terms.p, p_id, "P")?;
        let object = lookup_projected_or_use_bound(&id_to_term, &bound_terms.o, o_id, "O")?;

        triples.push((subject.to_owned(), predicate.to_owned(), object.to_owned()));
    }

    Ok(triples)
}
pub(super) fn extract_spog_id_columns(
    quads: &ArrayRef,
) -> Result<(Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>)> {
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();

    let quads_struct = quads
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let fields = quads_struct.unmasked_fields();

    let s_ids = fields
        .get(0)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing S IDs".to_string()))?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .as_slice::<u32>()
        .to_vec();

    let p_ids = fields
        .get(1)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing P IDs".to_string()))?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .as_slice::<u32>()
        .to_vec();

    let o_ids = fields
        .get(2)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing O IDs".to_string()))?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .as_slice::<u32>()
        .to_vec();

    let g_ids = fields
        .get(3)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing G IDs".to_string()))?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .as_slice::<u32>()
        .to_vec();

    Ok((s_ids, p_ids, o_ids, g_ids))
}
pub(super) fn collect_unique_ids(
    s_ids: &[u32],
    p_ids: &[u32],
    o_ids: &[u32],
    g_ids: &[u32],
) -> Vec<u32> {
    let mut set = HashSet::new();

    for id in s_ids {
        set.insert(*id);
    }
    for id in p_ids {
        set.insert(*id);
    }
    for id in o_ids {
        set.insert(*id);
    }
    for id in g_ids {
        set.insert(*id);
    }

    let mut ids: Vec<u32> = set.into_iter().collect();
    ids.sort_unstable();
    ids
}
pub(super) async fn lookup_terms_by_ids_from_sidecar(
    data_path: &Path,
    ids: &[u32],
) -> Result<HashMap<u32, String>> {
    let provider = NativeRdfProviders::external_only(data_path);
    let (terms, _stats) = provider.lookup_terms_by_ids(ids).await?;
    Ok(terms)
}
async fn lookup_terms_by_ids_from_sidecar_with_stats(
    data_path: &Path,
    ids: &[u32],
) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)> {
    let total_start = Instant::now();
    let mut stats = NativeIdToTermLookupStats::default();
    stats.strategy = "vortex-id-row-selection".to_string();
    stats.requested_ids_in = ids.len();
    if ids.is_empty() {
        stats.total_ms = elapsed_ms(total_start);
        return Ok((HashMap::new(), stats));
    }
    let sort_start = Instant::now();
    let mut requested = ids.to_vec();
    requested.sort_unstable();
    requested.dedup();
    stats.sort_dedup_ms = elapsed_ms(sort_start);
    stats.requested_ids_unique = requested.len();
    let resolver = runtime_component_resolver(data_path).await?;
    let open_start = Instant::now();
    let file = resolver.open(NativeComponent::DictionaryVortex).await?;
    stats.open_files_ms = elapsed_ms(open_start);
    let indices = Buffer::from(
        requested
            .iter()
            .map(|id| u64::from(*id))
            .collect::<Vec<_>>(),
    );
    let read_start = Instant::now();
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_indices(indices)
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    stats.blob_read_ms = elapsed_ms(read_start);
    let loaded_ids = extract_projected_u32_column(&array, "id")?;
    let terms = extract_projected_utf8_column(&array, "term")?;
    if loaded_ids.len() != requested.len() || terms.len() != requested.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "id_to_term selection returned ids={}, terms={}, requested={}",
            loaded_ids.len(),
            terms.len(),
            requested.len()
        )));
    }
    let mut out = HashMap::with_capacity(requested.len());
    for ((expected, actual), term) in requested.iter().zip(loaded_ids).zip(terms) {
        if *expected != actual {
            return Err(VortexRdfError::Deserialization(format!(
                "id_to_term row invariant failed: requested ID {}, row contained ID {}",
                expected, actual
            )));
        }
        out.insert(actual, term);
    }
    stats.ids_loaded = out.len();
    stats.total_ms = elapsed_ms(total_start);
    Ok((out, stats))
}
async fn lookup_subject_range_from_vortex(
    data_path: &Path,
    subject_id: u32,
) -> Result<Option<Range<u64>>> {
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::SubjectRangesVortex)?;
    let component_label = location.cache_key();
    let file = resolver.open(NativeComponent::SubjectRangesVortex).await?;
    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(eq(col("subject_id"), lit(subject_id)))
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let result = stream.read_all().await.map_err(VortexRdfError::from)?;
    if result.len() == 0 {
        return Ok(None);
    }
    if result.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "Vortex subject index {:?} returned {} rows for subject ID {}; expected at most one",
            component_label,
            result.len(),
            subject_id
        )));
    }
    let starts = extract_projected_u64_column(&result, "row_start")?;
    let ends = extract_projected_u64_column(&result, "row_end")?;
    let start = starts[0];
    let end = ends[0];
    if start > end {
        return Err(VortexRdfError::Deserialization(format!(
            "Vortex subject index {:?} contains invalid range {}..{} for subject ID {}",
            component_label, start, end, subject_id
        )));
    }
    Ok(Some(start..end))
}
#[derive(Clone, Copy, Debug)]
struct NativePoDirectoryEntry {
    range_offset: u64,
    range_count: u32,
    candidate_rows: u64,
}
#[derive(Clone, Copy, Debug)]
struct NativePoPredicatePartition {
    predicate_id: u32,
    directory_start: u64,
    directory_end: u64,
}
static NATIVE_PO_PREDICATE_PARTITION_CACHE: LazyLock<
    Mutex<HashMap<String, Arc<[NativePoPredicatePartition]>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
fn po_partition_cache_lock()
-> Result<std::sync::MutexGuard<'static, HashMap<String, Arc<[NativePoPredicatePartition]>>>> {
    NATIVE_PO_PREDICATE_PARTITION_CACHE.lock().map_err(|_| {
        VortexRdfError::Deserialization("PO predicate partition cache mutex was poisoned".into())
    })
}
async fn po_predicate_partitions(
    data_path: &Path,
) -> Result<Option<Arc<[NativePoPredicatePartition]>>> {
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::PredicateObjectPartitionsVortexV2)?;
    let cache_key = location.cache_key();
    if let Some(cached) = po_partition_cache_lock()?.get(&cache_key).cloned() {
        return Ok(Some(cached));
    }
    let file = resolver
        .open(NativeComponent::PredicateObjectPartitionsVortexV2)
        .await?;
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["predicate_id", "directory_start", "directory_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    let ids = extract_projected_u32_column(&array, "predicate_id")?;
    let starts = extract_projected_u64_column(&array, "directory_start")?;
    let ends = extract_projected_u64_column(&array, "directory_end")?;
    if ids.len() != array.len() || starts.len() != array.len() || ends.len() != array.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "PO predicate partition {:?} has inconsistent column lengths",
            cache_key
        )));
    }
    let mut entries = Vec::with_capacity(array.len());
    for index in 0..array.len() {
        if starts[index] >= ends[index] {
            return Err(VortexRdfError::Deserialization(format!(
                "PO predicate partition {:?} has invalid range {}..{} for predicate {}",
                cache_key, starts[index], ends[index], ids[index]
            )));
        }
        entries.push(NativePoPredicatePartition {
            predicate_id: ids[index],
            directory_start: starts[index],
            directory_end: ends[index],
        });
    }
    if entries.windows(2).any(|pair| {
        pair[0].predicate_id >= pair[1].predicate_id
            || pair[0].directory_end != pair[1].directory_start
    }) {
        return Err(VortexRdfError::Deserialization(format!(
            "PO predicate partition {:?} is not strictly sorted and contiguous",
            cache_key
        )));
    }
    if entries.first().map(|entry| entry.directory_start) != Some(0) {
        return Err(VortexRdfError::Deserialization(format!(
            "PO predicate partition {:?} does not start at directory row zero",
            cache_key
        )));
    }
    let entries: Arc<[NativePoPredicatePartition]> = entries.into();
    let mut cache = po_partition_cache_lock()?;
    Ok(Some(
        cache
            .entry(cache_key)
            .or_insert_with(|| Arc::clone(&entries))
            .clone(),
    ))
}
async fn po_predicate_partition(
    data_path: &Path,
    predicate_id: u32,
) -> Result<Option<NativePoPredicatePartition>> {
    let Some(partitions) = po_predicate_partitions(data_path).await? else {
        return Ok(None);
    };
    Ok(partitions
        .binary_search_by_key(&predicate_id, |entry| entry.predicate_id)
        .ok()
        .map(|index| partitions[index]))
}
async fn lookup_po_directory_entry_from_vortex_v2(
    data_path: &Path,
    predicate_id: u32,
    object_id: u32,
) -> Result<Option<NativePoDirectoryEntry>> {
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::PredicateObjectDirectoryVortexV2)?;
    let component_label = location.cache_key();
    let partition = po_predicate_partition(data_path, predicate_id).await?;
    if partition.is_none() {
        return Ok(None);
    }
    let file = resolver
        .open(NativeComponent::PredicateObjectDirectoryVortexV2)
        .await?;
    let scan = file.scan().map_err(VortexRdfError::from)?;
    let scan = match partition {
        Some(entry) => scan.with_row_range(entry.directory_start..entry.directory_end),
        None => scan,
    };
    let result = scan
        .with_filter(and(
            eq(col("predicate_id"), lit(predicate_id)),
            eq(col("object_id"), lit(object_id)),
        ))
        .with_projection(vortex_array::expr::select(
            ["range_offset", "range_count", "candidate_rows"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    if result.len() == 0 {
        return Ok(None);
    }
    if result.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "PO v2 directory {:?} returned {} rows for ({}, {}); expected one",
            component_label,
            result.len(),
            predicate_id,
            object_id
        )));
    }
    let offsets = extract_projected_u64_column(&result, "range_offset")?;
    let counts = extract_projected_u32_column(&result, "range_count")?;
    let rows = extract_projected_u64_column(&result, "candidate_rows")?;
    if offsets.len() != 1 || counts.len() != 1 || rows.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "PO v2 directory {:?} returned inconsistent metadata columns",
            component_label
        )));
    }
    Ok(Some(NativePoDirectoryEntry {
        range_offset: offsets[0],
        range_count: counts[0],
        candidate_rows: rows[0],
    }))
}
fn decode_exact_range_payload(
    payload: &ArrayRef,
    expected_range_count: usize,
    expected_candidate_rows: u64,
    context: &str,
) -> Result<Vec<Range<u64>>> {
    if payload.len() != expected_range_count {
        return Err(VortexRdfError::Deserialization(format!(
            "{context} returned {} rows; expected {expected_range_count}",
            payload.len()
        )));
    }
    let starts = extract_projected_u64_column(payload, "row_start")?;
    let ends = extract_projected_u64_column(payload, "row_end")?;
    if starts.len() != payload.len() || ends.len() != payload.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "{context} returned inconsistent range columns"
        )));
    }
    let ranges: Vec<_> = starts
        .into_iter()
        .zip(ends)
        .map(|(start, end)| start..end)
        .collect();
    validate_exact_ranges(&ranges, expected_candidate_rows, context)?;
    Ok(ranges)
}
async fn read_po_v2_payload(
    data_path: &Path,
    entry: NativePoDirectoryEntry,
) -> Result<Vec<Range<u64>>> {
    let range_end = entry
        .range_offset
        .checked_add(u64::from(entry.range_count))
        .ok_or_else(|| VortexRdfError::Deserialization("PO v2 payload slice overflow".into()))?;
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::PredicateObjectRangesVortexV2)?;
    let component_label = location.cache_key();
    let file = resolver
        .open(NativeComponent::PredicateObjectRangesVortexV2)
        .await?;
    let payload = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_range(entry.range_offset..range_end)
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    decode_exact_range_payload(
        &payload,
        entry.range_count as usize,
        entry.candidate_rows,
        &format!(
            "PO v2 payload {:?} slice {}..{}",
            component_label, entry.range_offset, range_end
        ),
    )
}
async fn lookup_po_access_from_vortex_v2(
    data_path: &Path,
    predicate_id: u32,
    object_id: u32,
) -> Result<Option<NativePoAccess>> {
    let Some(entry) =
        lookup_po_directory_entry_from_vortex_v2(data_path, predicate_id, object_id).await?
    else {
        return Ok(Some(NativePoAccess {
            ranges: Some(Vec::new()),
            candidate_ranges: 0,
            candidate_rows: 0,
            strategy: "po-exact-ranges-vortex-v2",
        }));
    };
    let candidate_ranges = entry.range_count as usize;
    let accepted = po_exact_access_accepted(candidate_ranges, entry.candidate_rows);
    let ranges = if accepted {
        Some(read_po_v2_payload(data_path, entry).await?)
    } else {
        None
    };
    Ok(Some(NativePoAccess {
        ranges,
        candidate_ranges,
        candidate_rows: entry.candidate_rows,
        strategy: "po-exact-ranges-vortex-v2",
    }))
}
#[derive(Clone, Copy, Debug)]
struct ExactAccessLimits {
    max_ranges: usize,
    max_rows: u64,
}
impl ExactAccessLimits {
    fn from_env(
        ranges_var: &str,
        rows_var: &str,
        default_max_ranges: usize,
        default_max_rows: u64,
    ) -> Self {
        Self {
            max_ranges: env_value_or(ranges_var, default_max_ranges),
            max_rows: env_value_or(rows_var, default_max_rows),
        }
    }

    fn accepts(self, candidate_ranges: usize, candidate_rows: u64) -> bool {
        candidate_ranges <= self.max_ranges && candidate_rows <= self.max_rows
    }
}
fn env_value_or<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
fn predicate_exact_limits() -> ExactAccessLimits {
    ExactAccessLimits::from_env(
        "VORTEX_RDF_P_EXACT_MAX_RANGES",
        "VORTEX_RDF_P_EXACT_MAX_ROWS",
        256,
        100_000,
    )
}
fn po_exact_limits() -> ExactAccessLimits {
    ExactAccessLimits::from_env(
        "VORTEX_RDF_PO_EXACT_MAX_RANGES",
        "VORTEX_RDF_PO_EXACT_MAX_ROWS",
        64,
        100_000,
    )
}
fn object_exact_limits() -> ExactAccessLimits {
    ExactAccessLimits::from_env(
        "VORTEX_RDF_O_EXACT_MAX_RANGES",
        "VORTEX_RDF_O_EXACT_MAX_ROWS",
        512,
        100_000,
    )
}
fn po_exact_access_accepted(candidate_ranges: usize, candidate_rows: u64) -> bool {
    po_exact_limits().accepts(candidate_ranges, candidate_rows)
}
#[derive(Clone, Copy, Debug)]
struct NativePredicateDirectoryEntry {
    predicate_id: u32,
    range_offset: u64,
    range_count: u32,
    candidate_rows: u64,
}
static NATIVE_PREDICATE_V2_DIRECTORY_CACHE: LazyLock<
    Mutex<HashMap<String, Arc<[NativePredicateDirectoryEntry]>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
fn predicate_v2_cache_lock()
-> Result<std::sync::MutexGuard<'static, HashMap<String, Arc<[NativePredicateDirectoryEntry]>>>> {
    NATIVE_PREDICATE_V2_DIRECTORY_CACHE.lock().map_err(|_| {
        VortexRdfError::Deserialization("predicate v2 directory cache mutex was poisoned".into())
    })
}
async fn predicate_v2_directory(data_path: &Path) -> Result<Arc<[NativePredicateDirectoryEntry]>> {
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::PredicateDirectoryVortexV2)?;
    let cache_key = location.cache_key();
    if let Some(cached) = predicate_v2_cache_lock()?.get(&cache_key).cloned() {
        return Ok(cached);
    }
    let file = resolver
        .open(NativeComponent::PredicateDirectoryVortexV2)
        .await?;
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            [
                "predicate_id",
                "range_offset",
                "range_count",
                "candidate_rows",
            ],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;

    let ids = extract_projected_u32_column(&array, "predicate_id")?;
    let offsets = extract_projected_u64_column(&array, "range_offset")?;
    let counts = extract_projected_u32_column(&array, "range_count")?;
    let rows = extract_projected_u64_column(&array, "candidate_rows")?;
    let len = array.len();
    if [ids.len(), offsets.len(), counts.len(), rows.len()]
        .into_iter()
        .any(|column_len| column_len != len)
    {
        return Err(VortexRdfError::Deserialization(format!(
            "predicate v2 directory {:?} has inconsistent column lengths",
            cache_key
        )));
    }

    let mut entries = Vec::with_capacity(len);
    for index in 0..len {
        entries.push(NativePredicateDirectoryEntry {
            predicate_id: ids[index],
            range_offset: offsets[index],
            range_count: counts[index],
            candidate_rows: rows[index],
        });
    }
    if entries
        .windows(2)
        .any(|pair| pair[0].predicate_id >= pair[1].predicate_id)
    {
        return Err(VortexRdfError::Deserialization(format!(
            "predicate v2 directory {:?} is not strictly sorted by predicate_id",
            cache_key
        )));
    }

    let entries: Arc<[NativePredicateDirectoryEntry]> = entries.into();
    let mut cache = predicate_v2_cache_lock()?;
    Ok(cache
        .entry(cache_key)
        .or_insert_with(|| Arc::clone(&entries))
        .clone())
}
fn predicate_access_from_directory_entry(
    entry: Option<NativePredicateDirectoryEntry>,
) -> NativePredicateAccess {
    let Some(entry) = entry else {
        return NativePredicateAccess {
            ranges: Some(Vec::new()),
            candidate_ranges: 0,
            candidate_rows: 0,
            strategy: "p-exact-ranges-vortex-v2-cached",
        };
    };
    let candidate_ranges = entry.range_count as usize;
    let accepted = predicate_exact_limits().accepts(candidate_ranges, entry.candidate_rows);
    NativePredicateAccess {
        ranges: accepted.then(Vec::new),
        candidate_ranges,
        candidate_rows: entry.candidate_rows,
        strategy: "p-exact-ranges-vortex-v2-cached",
    }
}
async fn read_predicate_v2_payload(
    data_path: &Path,
    entry: NativePredicateDirectoryEntry,
) -> Result<Vec<Range<u64>>> {
    let range_end = entry
        .range_offset
        .checked_add(u64::from(entry.range_count))
        .ok_or_else(|| {
            VortexRdfError::Deserialization("predicate v2 payload row range overflow".into())
        })?;
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::PredicateRangesVortexV2)?;
    let component_label = location.cache_key();
    let file = resolver
        .open(NativeComponent::PredicateRangesVortexV2)
        .await?;
    let payload = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_range(entry.range_offset..range_end)
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    decode_exact_range_payload(
        &payload,
        entry.range_count as usize,
        entry.candidate_rows,
        &format!(
            "predicate v2 payload {:?} slice {}..{}",
            component_label, entry.range_offset, range_end
        ),
    )
}
async fn lookup_predicate_access_from_vortex_v2(
    data_path: &Path,
    predicate_id: u32,
) -> Result<Option<NativePredicateAccess>> {
    let directory = predicate_v2_directory(data_path).await?;
    let entry = directory
        .binary_search_by_key(&predicate_id, |entry| entry.predicate_id)
        .ok()
        .map(|index| directory[index]);
    let mut access = predicate_access_from_directory_entry(entry);
    if access.ranges.is_some() {
        if let Some(entry) = entry {
            access.ranges = Some(read_predicate_v2_payload(data_path, entry).await?);
        }
    }
    Ok(Some(access))
}
#[derive(Clone, Copy, Debug)]
struct NativeObjectDirectoryEntry {
    range_offset: u64,
    range_count: u32,
    candidate_rows: u64,
}
async fn lookup_object_v2_directory_entry(
    data_path: &Path,
    object_id: u32,
) -> Result<Option<NativeObjectDirectoryEntry>> {
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::ObjectDirectoryVortexV2)?;
    let component_label = location.cache_key();
    let file = resolver
        .open(NativeComponent::ObjectDirectoryVortexV2)
        .await?;
    let result = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(eq(col("object_id"), lit(object_id)))
        .with_projection(vortex_array::expr::select(
            ["range_offset", "range_count", "candidate_rows"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;

    if result.len() == 0 {
        return Ok(None);
    }
    if result.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "object v2 directory {:?} returned {} rows for object ID {}; expected at most one",
            component_label,
            result.len(),
            object_id
        )));
    }

    let offsets = extract_projected_u64_column(&result, "range_offset")?;
    let counts = extract_projected_u32_column(&result, "range_count")?;
    let rows = extract_projected_u64_column(&result, "candidate_rows")?;
    if offsets.len() != 1 || counts.len() != 1 || rows.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "object v2 directory {:?} returned inconsistent metadata columns for object ID {}",
            component_label, object_id
        )));
    }

    Ok(Some(NativeObjectDirectoryEntry {
        range_offset: offsets[0],
        range_count: counts[0],
        candidate_rows: rows[0],
    }))
}
fn object_access_from_directory_entry(
    entry: Option<NativeObjectDirectoryEntry>,
) -> NativeObjectAccess {
    let Some(entry) = entry else {
        return NativeObjectAccess {
            ranges: Some(Vec::new()),
            candidate_ranges: 0,
            candidate_rows: 0,
            strategy: "o-exact-ranges-vortex-v2-point",
        };
    };
    let candidate_ranges = entry.range_count as usize;
    let accepted = object_exact_limits().accepts(candidate_ranges, entry.candidate_rows);
    NativeObjectAccess {
        ranges: accepted.then(Vec::new),
        candidate_ranges,
        candidate_rows: entry.candidate_rows,
        strategy: "o-exact-ranges-vortex-v2-point",
    }
}
async fn read_object_v2_payload(
    data_path: &Path,
    entry: NativeObjectDirectoryEntry,
) -> Result<Vec<Range<u64>>> {
    let range_end = entry
        .range_offset
        .checked_add(u64::from(entry.range_count))
        .ok_or_else(|| {
            VortexRdfError::Deserialization("object v2 payload row range overflow".into())
        })?;
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::ObjectRangesVortexV2)?;
    let component_label = location.cache_key();
    let file = resolver.open(NativeComponent::ObjectRangesVortexV2).await?;
    let payload = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_range(entry.range_offset..range_end)
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    decode_exact_range_payload(
        &payload,
        entry.range_count as usize,
        entry.candidate_rows,
        &format!(
            "object v2 payload {:?} slice {}..{}",
            component_label, entry.range_offset, range_end
        ),
    )
}
async fn lookup_object_access_from_vortex_v2(
    data_path: &Path,
    object_id: u32,
) -> Result<Option<NativeObjectAccess>> {
    let entry = lookup_object_v2_directory_entry(data_path, object_id).await?;
    let mut access = object_access_from_directory_entry(entry);
    if access.ranges.is_some() {
        if let Some(entry) = entry {
            access.ranges = Some(read_object_v2_payload(data_path, entry).await?);
        }
    }
    Ok(Some(access))
}
// VORTEX_RDF_SPARSE_TERM_DIRECTORY_V1
#[derive(Clone, Debug)]
struct NativeTermDirectoryEntry {
    first_term: String,
    last_term: String,
    row_start: u64,
    row_end: u64,
}
static NATIVE_TERM_DIRECTORY_CACHE: LazyLock<
    Mutex<HashMap<String, Arc<[NativeTermDirectoryEntry]>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
fn term_directory_cache_lock()
-> Result<std::sync::MutexGuard<'static, HashMap<String, Arc<[NativeTermDirectoryEntry]>>>> {
    NATIVE_TERM_DIRECTORY_CACHE.lock().map_err(|_| {
        VortexRdfError::Deserialization("term directory cache mutex was poisoned".into())
    })
}
async fn native_term_directory(data_path: &Path) -> Result<Arc<[NativeTermDirectoryEntry]>> {
    let resolver = runtime_component_resolver(data_path).await?;
    let location = resolver.location(NativeComponent::DictionaryTermDirectoryVortex)?;
    let cache_key = location.cache_key();
    if let Some(v) = term_directory_cache_lock()?.get(&cache_key).cloned() {
        return Ok(v);
    }
    let file = resolver
        .open(NativeComponent::DictionaryTermDirectoryVortex)
        .await?;
    let a = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["first_term", "last_term", "row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    let first = extract_projected_utf8_column(&a, "first_term")?;
    let last = extract_projected_utf8_column(&a, "last_term")?;
    let starts = extract_projected_u64_column(&a, "row_start")?;
    let ends = extract_projected_u64_column(&a, "row_end")?;
    if [first.len(), last.len(), starts.len(), ends.len()]
        .into_iter()
        .any(|n| n != a.len())
    {
        return Err(VortexRdfError::Deserialization(
            "term directory column length mismatch".into(),
        ));
    }
    let mut entries = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        if first[i] > last[i]
            || starts[i] >= ends[i]
            || (i > 0 && (last[i - 1] >= first[i] || ends[i - 1] != starts[i]))
        {
            return Err(VortexRdfError::Deserialization(format!(
                "invalid term directory entry {i}"
            )));
        }
        entries.push(NativeTermDirectoryEntry {
            first_term: first[i].clone(),
            last_term: last[i].clone(),
            row_start: starts[i],
            row_end: ends[i],
        });
    }
    if entries.first().is_some_and(|e| e.row_start != 0) {
        return Err(VortexRdfError::Deserialization(
            "term directory does not start at row zero".into(),
        ));
    }
    let entries: Arc<[NativeTermDirectoryEntry]> = entries.into();
    let mut cache = term_directory_cache_lock()?;
    Ok(cache
        .entry(cache_key)
        .or_insert_with(|| Arc::clone(&entries))
        .clone())
}
fn term_directory_range(entries: &[NativeTermDirectoryEntry], term: &str) -> Option<Range<u64>> {
    let i = entries.partition_point(|e| e.last_term.as_str() < term);
    let e = entries.get(i)?;
    (e.first_term.as_str() <= term).then(|| e.row_start..e.row_end)
}
#[derive(Debug)]
struct NativeTermLookupWindow {
    range: Range<u64>,
    terms: Vec<String>,
}
fn merge_native_term_lookup_windows(
    mut input: Vec<NativeTermLookupWindow>,
) -> Vec<NativeTermLookupWindow> {
    input.sort_by_key(|w| w.range.start);
    let mut out: Vec<NativeTermLookupWindow> = Vec::with_capacity(input.len());
    for mut w in input {
        if let Some(p) = out.last_mut() {
            if w.range.start <= p.range.end {
                p.range.end = p.range.end.max(w.range.end);
                p.terms.append(&mut w.terms);
                continue;
            }
        }
        out.push(w);
    }
    for w in &mut out {
        w.terms.sort();
        w.terms.dedup();
    }
    out
}
async fn lookup_bound_term_ids_sparse_directory(
    data_path: &Path,
    terms: &[(String, &'static str)],
) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
    let total_start = Instant::now();
    if terms.is_empty() {
        return Ok((HashMap::new(), Vec::new(), elapsed_ms(total_start)));
    }
    let directory = native_term_directory(data_path).await?;
    let mut stats: Vec<_> = terms
        .iter()
        .map(|(term, column)| NativeTermToIdLookupStats {
            column: Some((*column).to_string()),
            term_len: term.len(),
            term_preview: native_term_preview(term),
            strategy: "vortex-sparse-directory-v1".to_string(),
            ..Default::default()
        })
        .collect();
    let windows = merge_native_term_lookup_windows(
        terms
            .iter()
            .filter_map(|(term, _)| {
                term_directory_range(&directory, term).map(|range| NativeTermLookupWindow {
                    range,
                    terms: vec![term.clone()],
                })
            })
            .collect(),
    );
    if windows.is_empty() {
        let ms = elapsed_ms(total_start);
        stats[0].total_ms = ms;
        return Ok((HashMap::new(), stats, ms));
    }
    let resolver = runtime_component_resolver(data_path).await?;
    let open_start = Instant::now();
    let file = resolver
        .open(NativeComponent::DictionaryTermToIdVortex)
        .await?;
    let open_ms = elapsed_ms(open_start);
    let (mut scan_ms, mut read_ms, mut extract_ms) = (0.0, 0.0, 0.0);
    let mut found = HashMap::with_capacity(terms.len());
    for window in windows {
        let expr = window
            .terms
            .iter()
            .map(|t| eq(col("term"), lit(t.as_str())))
            .reduce(or)
            .unwrap();
        let t = Instant::now();
        let stream = file
            .scan()
            .map_err(VortexRdfError::from)?
            .with_row_range(window.range)
            .with_filter(expr)
            .with_projection(vortex_array::expr::select(
                ["term", "id"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?;
        scan_ms += elapsed_ms(t);
        let t = Instant::now();
        let result = stream.read_all().await.map_err(VortexRdfError::from)?;
        read_ms += elapsed_ms(t);
        let t = Instant::now();
        let loaded_terms = extract_projected_utf8_column(&result, "term")?;
        let ids = extract_projected_u32_column(&result, "id")?;
        if loaded_terms.len() != ids.len() {
            return Err(VortexRdfError::Deserialization(
                "sparse lookup length mismatch".into(),
            ));
        }
        for (term, id) in loaded_terms.into_iter().zip(ids) {
            if found.insert(term.clone(), id).is_some() {
                return Err(VortexRdfError::Deserialization(format!(
                    "duplicate sparse lookup result {term:?}"
                )));
            }
        }
        extract_ms += elapsed_ms(t);
    }
    let total_ms = elapsed_ms(total_start);
    for (i, (term, _)) in terms.iter().enumerate() {
        stats[i].found_id = found.get(term).copied();
        stats[i].result_array_len = usize::from(stats[i].found_id.is_some());
    }
    stats[0].open_ms = open_ms;
    stats[0].scan_build_ms = scan_ms;
    stats[0].read_all_ms = read_ms;
    stats[0].extract_ms = extract_ms;
    stats[0].total_ms = total_ms;
    Ok((found, stats, total_ms))
}
fn first_bound_native_id_column(
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> &'static str {
    if object.is_some() {
        "o"
    } else if subject.is_some() {
        "s"
    } else if predicate.is_some() {
        "p"
    } else if graph.is_some() {
        "g"
    } else {
        "s"
    }
}
async fn resolve_single_bound_id_for_count(
    data_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<(Option<&'static str>, Option<u32>, f64, bool)> {
    let mut bound: Vec<(&'static str, String)> = Vec::new();

    if let Some(s) = subject {
        bound.push(("s", s.to_string()));
    }
    if let Some(p) = predicate {
        bound.push(("p", p.to_string()));
    }
    if let Some(o) = object {
        bound.push(("o", o.to_string()));
    }
    if let Some(g) = graph {
        bound.push(("g", g.to_string()));
    }

    if bound.is_empty() {
        return Ok((Some("s"), None, 0.0, false));
    }

    if bound.len() != 1 {
        return Err(VortexRdfError::Serialization(
            "native-id manual/execute/rows count diagnostics currently expect zero or one bound term"
                .to_string(),
        ));
    }

    let (col_name, term) = &bound[0];
    let lookup_start = Instant::now();
    let id = lookup_term_id_from_sidecar(data_path, term).await?;
    let term_lookup_ms = elapsed_ms(lookup_start);

    match id {
        Some(id) => Ok((Some(*col_name), Some(id), term_lookup_ms, false)),
        None => Ok((Some(*col_name), None, term_lookup_ms, true)),
    }
}
pub(super) async fn build_native_pattern_filter_lazy_with_stats(
    data_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<(NativePatternFilter, f64)> {
    let start = Instant::now();
    let mut term_lookup_ms = 0.0;
    let mut filters: Vec<Expression> = Vec::new();

    if let Some(subject) = subject {
        let term = subject.to_string();

        let lookup_start = Instant::now();
        let id = lookup_term_id_from_sidecar(data_path, &term).await?;
        term_lookup_ms += elapsed_ms(lookup_start);

        let Some(id) = id else {
            return Ok((NativePatternFilter::Empty, term_lookup_ms));
        };

        filters.push(eq(col("s"), lit(id)));
    }

    if let Some(predicate) = predicate {
        let term = predicate.to_string();

        let lookup_start = Instant::now();
        let id = lookup_term_id_from_sidecar(data_path, &term).await?;
        term_lookup_ms += elapsed_ms(lookup_start);

        let Some(id) = id else {
            return Ok((NativePatternFilter::Empty, term_lookup_ms));
        };

        filters.push(eq(col("p"), lit(id)));
    }

    if let Some(object) = object {
        let term = object.to_string();

        let lookup_start = Instant::now();
        let id = lookup_term_id_from_sidecar(data_path, &term).await?;
        term_lookup_ms += elapsed_ms(lookup_start);

        let Some(id) = id else {
            return Ok((NativePatternFilter::Empty, term_lookup_ms));
        };

        filters.push(eq(col("o"), lit(id)));
    }

    if let Some(graph) = graph {
        let term = graph.to_string();

        let lookup_start = Instant::now();
        let id = lookup_term_id_from_sidecar(data_path, &term).await?;
        term_lookup_ms += elapsed_ms(lookup_start);

        let Some(id) = id else {
            return Ok((NativePatternFilter::Empty, term_lookup_ms));
        };

        filters.push(eq(col("g"), lit(id)));
    }

    let Some(expr) = filters.into_iter().reduce(and) else {
        return Ok((NativePatternFilter::All, term_lookup_ms));
    };

    log::debug!(
        "[cottas_native_ids::build_native_pattern_filter_lazy] built filter in {:?}",
        start.elapsed()
    );

    Ok((NativePatternFilter::Expr(expr), term_lookup_ms))
}
async fn lookup_bound_term_ids_from_sidecar_with_stats(
    data_path: &Path,
    terms: &[(String, &'static str)],
) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
    let strategy = std::env::var("VORTEX_RDF_TERM_LOOKUP_STRATEGY")
        .unwrap_or_else(|_| "batched-or".to_string());
    match strategy.as_str() {
        "batched-or" => lookup_bound_term_ids_batched_or(data_path, terms).await,
        "shared-open-equalities" => {
            lookup_bound_term_ids_shared_open_equalities(data_path, terms).await
        }
        "sparse-directory-v1" => lookup_bound_term_ids_sparse_directory(data_path, terms).await,
        other => Err(VortexRdfError::InvalidOperation(format!(
            "Unsupported VORTEX_RDF_TERM_LOOKUP_STRATEGY={other:?}; expected batched-or, shared-open-equalities, or sparse-directory-v1"
        ))),
    }
}
async fn lookup_bound_term_ids_batched_or(
    data_path: &Path,
    terms: &[(String, &'static str)],
) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
    let total_start = Instant::now();
    if terms.is_empty() {
        return Ok((HashMap::new(), Vec::new(), elapsed_ms(total_start)));
    }

    let resolver = runtime_component_resolver(data_path).await?;

    let mut stats: Vec<NativeTermToIdLookupStats> = terms
        .iter()
        .map(|(term, column)| NativeTermToIdLookupStats {
            column: Some((*column).to_string()),
            term_len: term.len(),
            term_preview: native_term_preview(term),
            strategy: "vortex-batched-term-filter".to_string(),
            ..NativeTermToIdLookupStats::default()
        })
        .collect();

    let open_start = Instant::now();
    let file = resolver
        .open(NativeComponent::DictionaryTermToIdVortex)
        .await?;
    let open_ms = elapsed_ms(open_start);
    let expr = terms
        .iter()
        .map(|(term, _)| eq(col("term"), lit(term.as_str())))
        .reduce(or)
        .expect("non-empty bound-term batch");
    let can_prune_start = Instant::now();
    let can_prune = file.can_prune(&expr).ok();
    let can_prune_ms = elapsed_ms(can_prune_start);
    let scan_start = Instant::now();
    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(expr)
        .with_projection(vortex_array::expr::select(
            ["term", "id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let scan_build_ms = elapsed_ms(scan_start);
    let read_start = Instant::now();
    let result = stream.read_all().await.map_err(VortexRdfError::from)?;
    let read_all_ms = elapsed_ms(read_start);
    let extract_start = Instant::now();
    let loaded_terms = extract_projected_utf8_column(&result, "term")?;
    let loaded_ids = extract_projected_u32_column(&result, "id")?;
    if loaded_terms.len() != loaded_ids.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "batched term_to_id lookup returned terms={} and ids={}",
            loaded_terms.len(),
            loaded_ids.len()
        )));
    }
    let mut out = HashMap::with_capacity(loaded_terms.len());
    for (term, id) in loaded_terms.into_iter().zip(loaded_ids) {
        if out.insert(term.clone(), id).is_some() {
            return Err(VortexRdfError::Deserialization(format!(
                "batched term_to_id lookup returned duplicate term {term:?}"
            )));
        }
    }
    let extract_ms = elapsed_ms(extract_start);
    let total_ms = elapsed_ms(total_start);
    for (index, (term, _)) in terms.iter().enumerate() {
        stats[index].found_id = out.get(term).copied();
        stats[index].result_array_len = usize::from(stats[index].found_id.is_some());
    }
    // Shared costs are stored once so aggregate diagnostics do not multiply them.
    stats[0].open_ms = open_ms;
    stats[0].can_prune_ms = can_prune_ms;
    stats[0].scan_build_ms = scan_build_ms;
    stats[0].read_all_ms = read_all_ms;
    stats[0].extract_ms = extract_ms;
    stats[0].can_prune = can_prune;
    stats[0].total_ms = total_ms;
    Ok((out, stats, total_ms))
}
async fn lookup_bound_term_ids_shared_open_equalities(
    data_path: &Path,
    terms: &[(String, &'static str)],
) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
    let total_start = Instant::now();
    if terms.is_empty() {
        return Ok((HashMap::new(), Vec::new(), elapsed_ms(total_start)));
    }

    let resolver = runtime_component_resolver(data_path).await?;

    let open_start = Instant::now();
    let file = resolver
        .open(NativeComponent::DictionaryTermToIdVortex)
        .await?;
    let open_ms = elapsed_ms(open_start);

    let mut out = HashMap::with_capacity(terms.len());
    let mut stats = Vec::with_capacity(terms.len());
    for (index, (term, column)) in terms.iter().enumerate() {
        let lookup_start = Instant::now();
        let mut item = NativeTermToIdLookupStats {
            column: Some((*column).to_string()),
            term_len: term.len(),
            term_preview: native_term_preview(term),
            strategy: "vortex-shared-open-equality".to_string(),
            ..NativeTermToIdLookupStats::default()
        };
        if index == 0 {
            item.open_ms = open_ms;
        }

        let expr = eq(col("term"), lit(term.as_str()));
        let can_prune_start = Instant::now();
        item.can_prune = file.can_prune(&expr).ok();
        item.can_prune_ms = elapsed_ms(can_prune_start);

        let scan_start = Instant::now();
        let stream = file
            .scan()
            .map_err(VortexRdfError::from)?
            .with_filter(expr)
            .with_projection(vortex_array::expr::select(
                ["id"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?;
        item.scan_build_ms = elapsed_ms(scan_start);

        let read_start = Instant::now();
        let result = stream.read_all().await.map_err(VortexRdfError::from)?;
        item.read_all_ms = elapsed_ms(read_start);
        item.result_array_len = result.len();
        if result.len() > 1 {
            return Err(VortexRdfError::Deserialization(format!(
                "shared-open term_to_id equality returned {} IDs for term {:?}",
                result.len(),
                term
            )));
        }

        let extract_start = Instant::now();
        let id = extract_first_u32_from_single_column_array(&result, "id")?;
        item.extract_ms = elapsed_ms(extract_start);
        item.found_id = id;
        if let Some(id) = id {
            if let Some(previous) = out.insert(term.clone(), id) {
                if previous != id {
                    return Err(VortexRdfError::Deserialization(format!(
                        "shared-open term_to_id lookup returned conflicting IDs for duplicate term {term:?}"
                    )));
                }
            }
        }
        item.total_ms = elapsed_ms(lookup_start) + if index == 0 { open_ms } else { 0.0 };
        stats.push(item);
    }

    let total_ms = elapsed_ms(total_start);
    Ok((out, stats, total_ms))
}
async fn lookup_term_id_from_sidecar(data_path: &Path, term: &str) -> Result<Option<u32>> {
    let (id, _stats) = lookup_term_id_from_sidecar_with_stats(data_path, term, None).await?;
    Ok(id)
}
async fn lookup_term_id_from_sidecar_with_stats(
    data_path: &Path,
    term: &str,
    column: Option<&'static str>,
) -> Result<(Option<u32>, NativeTermToIdLookupStats)> {
    let strategy = std::env::var("VORTEX_RDF_TERM_LOOKUP_STRATEGY")
        .unwrap_or_else(|_| "batched-or".to_string());
    if strategy == "sparse-directory-v1" {
        let requested = vec![(term.to_string(), column.unwrap_or("term"))];
        let (ids, mut stats, _) =
            lookup_bound_term_ids_sparse_directory(data_path, &requested).await?;
        return Ok((ids.get(term).copied(), stats.pop().unwrap()));
    }
    if strategy != "batched-or" && strategy != "shared-open-equalities" {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Unsupported VORTEX_RDF_TERM_LOOKUP_STRATEGY={strategy:?}; expected batched-or, shared-open-equalities, or sparse-directory-v1"
        )));
    }
    let lookup_start = Instant::now();
    let mut stats = NativeTermToIdLookupStats {
        column: column.map(|value| value.to_string()),
        term_len: term.len(),
        term_preview: native_term_preview(term),
        strategy: "vortex-term-filter".to_string(),
        ..NativeTermToIdLookupStats::default()
    };
    let resolver = runtime_component_resolver(data_path).await?;
    let open_start = Instant::now();
    let file = resolver
        .open(NativeComponent::DictionaryTermToIdVortex)
        .await?;
    stats.open_ms = elapsed_ms(open_start);
    let expr = eq(col("term"), lit(term));
    let can_prune_start = Instant::now();
    stats.can_prune = file.can_prune(&expr).ok();
    stats.can_prune_ms = elapsed_ms(can_prune_start);
    let scan_start = Instant::now();
    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(expr)
        .with_projection(vortex_array::expr::select(
            ["id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    stats.scan_build_ms = elapsed_ms(scan_start);
    let read_start = Instant::now();
    let result = stream.read_all().await.map_err(VortexRdfError::from)?;
    stats.read_all_ms = elapsed_ms(read_start);
    stats.result_array_len = result.len();
    if result.len() > 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "term_to_id dictionary returned {} IDs for one exact term",
            result.len()
        )));
    }
    let extract_start = Instant::now();
    let id = extract_first_u32_from_single_column_array(&result, "id")?;
    stats.extract_ms = elapsed_ms(extract_start);
    stats.found_id = id;
    stats.total_ms = elapsed_ms(lookup_start);
    Ok((id, stats))
}

pub(super) fn invalidate_po_partition_cache(output_path: &Path) -> Result<()> {
    po_partition_cache_lock()?.remove(&format!("external:{}", output_path.display()));
    Ok(())
}

pub(super) fn invalidate_predicate_v2_cache(directory_path: &Path) -> Result<()> {
    predicate_v2_cache_lock()?.remove(&format!("external:{}", directory_path.display()));
    Ok(())
}

pub(super) fn invalidate_term_directory_cache(output_path: &Path) -> Result<()> {
    term_directory_cache_lock()?.remove(&format!("external:{}", output_path.display()));
    Ok(())
}

/// Format-independent dictionary access for native-ID planning and decoding.
/// Dictionary access remains behind this contract until Phase C embeds it.
#[async_trait]
pub trait NativeDictionaryProvider: Send + Sync {
    async fn lookup_term_id(
        &self,
        term: &str,
        column: Option<&'static str>,
    ) -> Result<(Option<u32>, NativeTermToIdLookupStats)>;

    async fn lookup_bound_term_ids(
        &self,
        terms: &[(String, &'static str)],
    ) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)>;
    async fn lookup_terms_by_ids(
        &self,
        ids: &[u32],
    ) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)>;
}

/// Format-independent exact row-range access used by the native-ID planner.
#[async_trait]
pub trait NativeIndexProvider: Send + Sync {
    async fn subject_range(&self, subject_id: u32) -> Result<Option<Range<u64>>>;
    async fn po_access(&self, predicate_id: u32, object_id: u32) -> Result<Option<NativePoAccess>>;
    async fn predicate_access(&self, predicate_id: u32) -> Result<Option<NativePredicateAccess>>;
    async fn object_access(&self, object_id: u32) -> Result<Option<NativeObjectAccess>>;
    fn subject_strategy(&self) -> &'static str;
}

#[derive(Clone, Debug)]
pub struct NativePoAccess {
    pub(super) ranges: Option<Vec<Range<u64>>>,
    pub(super) candidate_ranges: usize,
    pub(super) candidate_rows: u64,
    pub(super) strategy: &'static str,
}

#[derive(Clone, Debug)]
pub struct NativePredicateAccess {
    pub(super) ranges: Option<Vec<Range<u64>>>,
    pub(super) candidate_ranges: usize,
    pub(super) candidate_rows: u64,
    pub(super) strategy: &'static str,
}
pub type NativeObjectAccess = NativePredicateAccess;

#[derive(Clone)]
pub struct NativeRdfProviders {
    data_path: PathBuf,
    resolver: NativeComponentResolver,
}

impl NativeRdfProviders {
    pub(super) fn external_only(data_path: &Path) -> Self {
        Self {
            data_path: data_path.to_path_buf(),
            resolver: NativeComponentResolver::legacy_external(data_path),
        }
    }

    async fn inspect(data_path: &Path) -> Result<Self> {
        let resolver = runtime_component_resolver(data_path).await?;
        Ok(Self {
            data_path: data_path.to_path_buf(),
            resolver: (*resolver).clone(),
        })
    }

    fn resolver(&self) -> &NativeComponentResolver {
        &self.resolver
    }

    fn outer_file(&self) -> Result<vortex_file::VortexFile> {
        self.resolver.outer_file()
    }
}

#[async_trait]
impl NativeDictionaryProvider for NativeRdfProviders {
    async fn lookup_term_id(
        &self,
        term: &str,
        column: Option<&'static str>,
    ) -> Result<(Option<u32>, NativeTermToIdLookupStats)> {
        lookup_term_id_from_sidecar_with_stats(&self.data_path, term, column).await
    }

    async fn lookup_bound_term_ids(
        &self,
        terms: &[(String, &'static str)],
    ) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
        lookup_bound_term_ids_from_sidecar_with_stats(&self.data_path, terms).await
    }

    async fn lookup_terms_by_ids(
        &self,
        ids: &[u32],
    ) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)> {
        lookup_terms_by_ids_from_sidecar_with_stats(&self.data_path, ids).await
    }
}

#[async_trait]
impl NativeIndexProvider for NativeRdfProviders {
    async fn subject_range(&self, subject_id: u32) -> Result<Option<Range<u64>>> {
        lookup_subject_range_from_vortex(&self.data_path, subject_id).await
    }

    async fn po_access(&self, predicate_id: u32, object_id: u32) -> Result<Option<NativePoAccess>> {
        lookup_po_access_from_vortex_v2(&self.data_path, predicate_id, object_id).await
    }

    async fn predicate_access(&self, predicate_id: u32) -> Result<Option<NativePredicateAccess>> {
        lookup_predicate_access_from_vortex_v2(&self.data_path, predicate_id).await
    }

    async fn object_access(&self, object_id: u32) -> Result<Option<NativeObjectAccess>> {
        let configured = std::env::var("VORTEX_RDF_NATIVE_OBJECT_INDEX_BACKEND")
            .unwrap_or_else(|_| "auto".to_string());
        let components = [
            NativeComponent::ObjectDirectoryVortexV2,
            NativeComponent::ObjectRangesVortexV2,
        ];
        match configured.as_str() {
            "none" => Ok(None),
            "auto" if !self.resolver.components_available(&components)? => Ok(None),
            "auto" | "vortex-v2" if self.resolver.components_available(&components)? => {
                lookup_object_access_from_vortex_v2(&self.data_path, object_id).await
            }
            "vortex-v2" => Err(VortexRdfError::InvalidOperation(format!(
                "required Vortex object v2 components are unavailable for artifact {:?}",
                self.data_path
            ))),
            other => Err(VortexRdfError::InvalidOperation(format!(
                "Unsupported VORTEX_RDF_NATIVE_OBJECT_INDEX_BACKEND={other:?}; expected auto, none, or vortex-v2"
            ))),
        }
    }

    fn subject_strategy(&self) -> &'static str {
        "subject-ranges-vortex-v1"
    }
}

use crate::io::native_rdf_store::exact_ranges::range_rows;

#[derive(Debug, Default)]
struct NativeAccessPlan {
    ranges: Option<Vec<Range<u64>>>,
    strategy: String,
    lookup_ms: f64,
    candidate_ranges: usize,
    candidate_rows: u64,
    subject_range: Option<Range<u64>>,
    po_index_used: bool,
}
async fn resolve_native_pattern<D: NativeDictionaryProvider + ?Sized>(
    dictionary: &D,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<(
    Option<ResolvedNativePattern>,
    f64,
    Vec<NativeTermToIdLookupStats>,
)> {
    let mut requested = Vec::with_capacity(4);
    if let Some(value) = subject {
        requested.push((value.to_string(), "s"));
    }
    if let Some(value) = predicate {
        requested.push((value.to_string(), "p"));
    }
    if let Some(value) = object {
        requested.push((value.to_string(), "o"));
    }
    if let Some(value) = graph {
        requested.push((value.to_string(), "g"));
    }

    if requested.len() == 1 {
        let (term, column) = &requested[0];
        let (id, stats) = dictionary.lookup_term_id(term, Some(*column)).await?;
        let total_lookup_ms = stats.total_ms;
        let Some(id) = id else {
            return Ok((None, total_lookup_ms, vec![stats]));
        };
        let mut resolved = ResolvedNativePattern::default();
        match *column {
            "s" => resolved.s = Some(id),
            "p" => resolved.p = Some(id),
            "o" => resolved.o = Some(id),
            "g" => resolved.g = Some(id),
            _ => unreachable!("only native SPOG columns are requested"),
        }
        return Ok((Some(resolved), total_lookup_ms, vec![stats]));
    }

    let (ids, stats, total_lookup_ms) = dictionary.lookup_bound_term_ids(&requested).await?;
    if requested.iter().any(|(term, _)| !ids.contains_key(term)) {
        return Ok((None, total_lookup_ms, stats));
    }

    let mut resolved = ResolvedNativePattern::default();
    for (term, column) in requested {
        let id = ids[&term];
        match column {
            "s" => resolved.s = Some(id),
            "p" => resolved.p = Some(id),
            "o" => resolved.o = Some(id),
            "g" => resolved.g = Some(id),
            _ => unreachable!("only native SPOG columns are requested"),
        }
    }
    Ok((Some(resolved), total_lookup_ms, stats))
}
async fn plan_native_access<I: NativeIndexProvider + ?Sized>(
    indexes: &I,
    resolved: ResolvedNativePattern,
    subject_bound: bool,
    predicate_bound: bool,
    object_bound: bool,
) -> Result<NativeAccessPlan> {
    let start = Instant::now();

    if subject_bound {
        if let Some(subject_id) = resolved.s {
            if let Some(range) = indexes.subject_range(subject_id).await? {
                return Ok(NativeAccessPlan {
                    ranges: Some(vec![range.clone()]),
                    strategy: indexes.subject_strategy().to_string(),
                    lookup_ms: elapsed_ms(start),
                    candidate_ranges: 1,
                    candidate_rows: range.end.saturating_sub(range.start),
                    subject_range: Some(range),
                    po_index_used: false,
                });
            }
        }
    }

    if !subject_bound && predicate_bound && object_bound {
        if let (Some(predicate_id), Some(object_id)) = (resolved.p, resolved.o) {
            if let Some(access) = indexes.po_access(predicate_id, object_id).await? {
                let use_ranges = access.ranges.is_some();
                return Ok(NativeAccessPlan {
                    candidate_ranges: access.candidate_ranges,
                    candidate_rows: access.candidate_rows,
                    ranges: access.ranges,
                    strategy: if use_ranges {
                        access.strategy.to_string()
                    } else {
                        "none-high-cardinality-po".to_string()
                    },
                    lookup_ms: elapsed_ms(start),
                    subject_range: None,
                    po_index_used: use_ranges,
                });
            }
        }
    }

    if !subject_bound && predicate_bound && !object_bound {
        if let Some(predicate_id) = resolved.p {
            if let Some(access) = indexes.predicate_access(predicate_id).await? {
                let use_ranges = access.ranges.is_some();
                return Ok(NativeAccessPlan {
                    candidate_ranges: access.candidate_ranges,
                    candidate_rows: access.candidate_rows,
                    ranges: access.ranges,
                    strategy: if use_ranges {
                        access.strategy.to_string()
                    } else {
                        "none-high-cardinality-predicate".to_string()
                    },
                    lookup_ms: elapsed_ms(start),
                    subject_range: None,
                    po_index_used: false,
                });
            }
        }
    }

    if !subject_bound && !predicate_bound && object_bound {
        if let Some(object_id) = resolved.o {
            if let Some(access) = indexes.object_access(object_id).await? {
                let use_ranges = access.ranges.is_some();
                return Ok(NativeAccessPlan {
                    candidate_ranges: access.candidate_ranges,
                    candidate_rows: access.candidate_rows,
                    ranges: access.ranges,
                    strategy: if use_ranges {
                        access.strategy.to_string()
                    } else {
                        "none-high-cardinality-object".to_string()
                    },
                    lookup_ms: elapsed_ms(start),
                    subject_range: None,
                    po_index_used: false,
                });
            }
        }
    }
    Ok(NativeAccessPlan {
        strategy: "none".to_string(),
        lookup_ms: elapsed_ms(start),
        ..NativeAccessPlan::default()
    })
}

#[derive(Debug)]
pub(super) struct NativeMatchPlanResult {
    pub(super) rows: NativeIdBatch,
    pub(super) bound_terms: BoundNativeRdfTerms,
    pub(super) diagnostics: CottasNativeIdsDiagnostics,
}

pub(super) async fn execute_cottas_native_match(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<NativeMatchPlanResult> {
    let total_start = Instant::now();
    let mut diagnostics = CottasNativeIdsDiagnostics::default();
    let bound_terms = BoundNativeRdfTerms::from_pattern(subject, predicate, object, graph);
    let providers = NativeRdfProviders::inspect(input_path).await?;
    let _artifact_kind = providers.resolver().artifact_kind();
    let (resolved, term_lookup_ms, term_to_id_stats) =
        resolve_native_pattern(&providers, subject, predicate, object, graph).await?;
    diagnostics.term_lookup_ms = term_lookup_ms;
    diagnostics.term_to_id_stats = term_to_id_stats;
    let Some(resolved) = resolved else {
        diagnostics.total_ms = elapsed_ms(total_start);
        return Ok(NativeMatchPlanResult {
            rows: NativeIdBatch::default(),
            bound_terms,
            diagnostics,
        });
    };
    let filter = resolved.filter();

    let open_start = Instant::now();
    let file = providers.outer_file()?;
    diagnostics.open_ms = elapsed_ms(open_start);

    if let NativePatternFilter::Expr(expr) = &filter {
        diagnostics.vortex_can_prune = file.can_prune(expr).ok();
    }
    diagnostics.total_splits = file.splits().ok().map(|splits| splits.len());

    let projected_columns = native_projection_columns_for_bound_terms(&bound_terms);
    let access_plan = plan_native_access(
        &providers,
        resolved,
        subject.is_some(),
        predicate.is_some(),
        object.is_some(),
    )
    .await?;
    diagnostics.access_index_strategy = access_plan.strategy.clone();
    diagnostics.access_index_lookup_ms = access_plan.lookup_ms;
    diagnostics.access_candidate_ranges = access_plan.candidate_ranges;
    diagnostics.access_candidate_rows = access_plan.candidate_rows;
    diagnostics.po_rowgroup_index_used = access_plan.po_index_used;
    diagnostics.po_rowgroup_lookup_ms = if access_plan.po_index_used {
        access_plan.lookup_ms
    } else {
        0.0
    };
    diagnostics.po_candidate_ranges = if access_plan.po_index_used {
        access_plan.candidate_ranges
    } else {
        0
    };
    diagnostics.po_candidate_rows = if access_plan.po_index_used {
        access_plan.candidate_rows
    } else {
        0
    };
    if let Some(range) = &access_plan.subject_range {
        diagnostics.subject_range_index_used = true;
        diagnostics.subject_range_lookup_ms = access_plan.lookup_ms;
        diagnostics.subject_range_start = Some(range.start);
        diagnostics.subject_range_end = Some(range.end);
        diagnostics.subject_range_rows = Some(range.end.saturating_sub(range.start));
    }
    let selected_ranges = access_plan.ranges;
    let mut scan_build_ms = 0.0;
    let mut read_all_ms = 0.0;
    let mut matched_rows = NativeIdBatch::default();
    let mut scan_batches = 0usize;
    let mut max_scan_batch_rows = 0usize;
    let mut execution_strategy = "full-scan";
    let mut original_range_count = 0usize;
    let mut executed_scan_count = 0usize;
    let mut selected_rows = 0u64;

    if let Some(ranges) = selected_ranges {
        original_range_count = ranges.len();
        selected_rows = range_rows(&ranges);
        if selected_rows != access_plan.candidate_rows {
            return Err(VortexRdfError::Deserialization(format!(
                "access-plan row mismatch: metadata={}, ranges={}",
                access_plan.candidate_rows, selected_rows
            )));
        }
        if ranges.is_empty() {
            execution_strategy = "empty-index-result";
        } else {
            let scan_build_start = Instant::now();
            let scan = file.scan().map_err(VortexRdfError::from)?;
            let scan = if ranges.len() == 1 {
                execution_strategy = "single-row-range";
                scan.with_row_range(ranges[0].clone())
            } else {
                execution_strategy = "include-by-index";
                scan.with_row_indices(exact_ranges_to_row_indices(
                    &ranges,
                    access_plan.candidate_rows,
                )?)
            };
            let scan = match &filter {
                NativePatternFilter::All => scan,
                NativePatternFilter::Empty => unreachable!("handled above"),
                NativePatternFilter::Expr(expr) => scan.with_filter(expr.clone()),
            };
            let stream = scan
                .with_projection(vortex_array::expr::select(
                    projected_columns.as_slice(),
                    vortex_array::expr::root(),
                ))
                .into_array_stream()
                .map_err(VortexRdfError::from)?;
            scan_build_ms += elapsed_ms(scan_build_start);
            executed_scan_count = 1;
            let read_start = Instant::now();
            let (rows, batches, max_rows) =
                read_native_projected_stream_all_with_scan_stats(stream).await?;
            read_all_ms += elapsed_ms(read_start);
            matched_rows = rows;
            scan_batches = batches;
            max_scan_batch_rows = max_rows;
        }
    } else {
        let scan_build_start = Instant::now();
        let scan = file.scan().map_err(VortexRdfError::from)?;
        let scan = match &filter {
            NativePatternFilter::All => scan,
            NativePatternFilter::Empty => unreachable!("handled above"),
            NativePatternFilter::Expr(expr) => scan.with_filter(expr.clone()),
        };
        let stream = scan
            .with_projection(vortex_array::expr::select(
                projected_columns.as_slice(),
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?;
        scan_build_ms += elapsed_ms(scan_build_start);
        executed_scan_count = 1;
        let read_start = Instant::now();
        let (rows, batches, max_rows) =
            read_native_projected_stream_all_with_scan_stats(stream).await?;
        read_all_ms += elapsed_ms(read_start);
        matched_rows = rows;
        scan_batches = batches;
        max_scan_batch_rows = max_rows;
    }
    diagnostics.scan_build_ms = scan_build_ms;
    diagnostics.read_all_ms = read_all_ms;
    diagnostics.access_execution_strategy = execution_strategy.to_string();
    diagnostics.access_original_range_count = original_range_count;
    diagnostics.access_executed_scan_count = executed_scan_count;
    diagnostics.access_selected_rows = selected_rows;
    diagnostics.scan_batches = scan_batches;
    diagnostics.max_scan_batch_rows = max_scan_batch_rows;
    diagnostics.scan_rows_materialized = matched_rows.rows;
    diagnostics.rows_out = matched_rows.rows;
    diagnostics.total_ms = elapsed_ms(total_start);

    Ok(NativeMatchPlanResult {
        rows: matched_rows,
        bound_terms,
        diagnostics,
    })
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CottasNativeIdsDiagnostics {
    pub term_lookup_ms: f64,
    pub open_ms: f64,
    pub scan_build_ms: f64,
    pub read_all_ms: f64,
    pub id_extract_ms: f64,
    pub id_to_term_lookup_ms: f64,
    pub serialize_ms: f64,
    pub total_ms: f64,
    pub rows_out: usize,
    pub unique_ids_requested: usize,
    pub unique_ids_loaded: usize,
    pub vortex_can_prune: Option<bool>,
    pub total_splits: Option<usize>,
    pub scan_batches: usize,
    pub max_scan_batch_rows: usize,
    pub scan_rows_materialized: usize,
    pub subject_range_index_used: bool,
    pub subject_range_lookup_ms: f64,
    pub subject_range_start: Option<u64>,
    pub subject_range_end: Option<u64>,
    pub subject_range_rows: Option<u64>,
    pub po_rowgroup_index_used: bool,
    pub po_rowgroup_lookup_ms: f64,
    pub po_candidate_ranges: usize,
    pub po_candidate_rows: u64,
    pub access_index_strategy: String,
    pub access_index_lookup_ms: f64,
    pub access_candidate_ranges: usize,
    pub access_candidate_rows: u64,
    pub access_execution_strategy: String,
    pub access_original_range_count: usize,
    pub access_executed_scan_count: usize,
    pub access_selected_rows: u64,
    pub id_to_term_stats: NativeIdToTermLookupStats,
    pub term_to_id_stats: Vec<NativeTermToIdLookupStats>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeIdsCountMode {
    NativeFilter,
    ManualEq,
    ExecuteOnly,
    RowsOnly,
}

#[derive(Clone, Debug, Serialize)]
pub struct CottasNativeIdsCountTimings {
    pub term_lookup_ms: f64,
    pub open_ms: f64,
    pub scan_build_ms: f64,
    pub stream_init_ms: f64,
    pub consume_ms: f64,
    pub total_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct CottasNativeIdsCountDiagnostics {
    pub mode: NativeIdsCountMode,
    pub count: usize,
    pub timings: CottasNativeIdsCountTimings,

    pub filter_empty: bool,
    pub projected_columns: Vec<String>,
    pub bound_column: Option<String>,
    pub bound_id: Option<u32>,

    pub batches: usize,
    pub max_batch_rows: usize,
    pub decoded_values: usize,
}

#[derive(Clone, Debug, Default)]
struct LazyRdfWriteStats {
    id_extract_ms: f64,
    id_to_term_lookup_ms: f64,
    serialize_ms: f64,
    rows_out: usize,
    unique_ids_requested: usize,
    unique_ids_loaded: usize,
    id_to_term_stats: NativeIdToTermLookupStats,
}

async fn write_projected_native_id_rows_as_rdf_lazy<W>(
    data_path: &Path,
    rows: NativeIdBatch,
    bound_terms: &BoundNativeRdfTerms,
    writer: W,
    format: RdfFormat,
) -> Result<LazyRdfWriteStats>
where
    W: Write,
{
    let write_start = Instant::now();

    let id_extract_start = Instant::now();
    let unique_ids = rows.unique_unbound_ids(bound_terms);
    let id_extract_ms = elapsed_ms(id_extract_start);

    let id_lookup_start = Instant::now();
    let (id_to_term, id_to_term_stats) =
        lookup_terms_by_ids_from_sidecar_with_stats(data_path, &unique_ids).await?;
    let id_to_term_lookup_ms = elapsed_ms(id_lookup_start);

    let serialize_start = Instant::now();
    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    for i in 0..rows.rows {
        let s_id = rows.id_at(NativeIdColumn::Subject, bound_terms, i)?;
        let p_id = rows.id_at(NativeIdColumn::Predicate, bound_terms, i)?;
        let o_id = rows.id_at(NativeIdColumn::Object, bound_terms, i)?;
        let g_id = rows.id_at(NativeIdColumn::Graph, bound_terms, i)?;

        let s_raw = lookup_projected_or_use_bound(&id_to_term, &bound_terms.s, s_id, "S")?;
        let p_raw = lookup_projected_or_use_bound(&id_to_term, &bound_terms.p, p_id, "P")?;
        let o_raw = lookup_projected_or_use_bound(&id_to_term, &bound_terms.o, o_id, "O")?;
        let g_raw = lookup_projected_or_use_bound(&id_to_term, &bound_terms.g, g_id, "G")?;

        let subject = crate::common::utils::parse_subject(s_raw)?;
        let predicate = crate::common::utils::parse_named_node(p_raw)?;
        let object = crate::common::utils::parse_term(o_raw)?;
        let graph_name = crate::common::utils::parse_graph_name(g_raw)?;

        let quad = Quad::new(subject, predicate, object, graph_name);

        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    }

    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

    let serialize_ms = elapsed_ms(serialize_start);

    log::debug!(
        "[cottas_native_ids::write_projected_native_id_rows_as_rdf_lazy] wrote {} rows using {} unique dictionary ids in {:?}",
        rows.rows,
        unique_ids.len(),
        write_start.elapsed()
    );

    Ok(LazyRdfWriteStats {
        id_extract_ms,
        id_to_term_lookup_ms,
        serialize_ms,
        rows_out: rows.rows,
        unique_ids_requested: unique_ids.len(),
        unique_ids_loaded: id_to_term.len(),
        id_to_term_stats,
    })
}

async fn write_empty_rdf<W>(writer: W, format: RdfFormat) -> Result<()>
where
    W: Write,
{
    let rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);
    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    Ok(())
}

pub async fn match_cottas_native_file<W>(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    writer: W,
    format: RdfFormat,
) -> Result<()>
where
    W: Write,
{
    let _diagnostics = match_cottas_native_file_with_diagnostics(
        input_path, subject, predicate, object, graph, writer, format,
    )
    .await?;

    Ok(())
}

pub async fn match_cottas_native_file_with_diagnostics<W>(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    writer: W,
    format: RdfFormat,
) -> Result<CottasNativeIdsDiagnostics>
where
    W: Write,
{
    let total_start = Instant::now();
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;
    let mut diagnostics = planned.diagnostics;

    if planned.rows.rows == 0 {
        let serialize_start = Instant::now();
        write_empty_rdf(writer, format).await?;
        diagnostics.serialize_ms = elapsed_ms(serialize_start);
        diagnostics.total_ms = elapsed_ms(total_start);
        return Ok(diagnostics);
    }

    let write_stats = write_projected_native_id_rows_as_rdf_lazy(
        input_path,
        planned.rows,
        &planned.bound_terms,
        writer,
        format,
    )
    .await?;

    diagnostics.id_extract_ms = write_stats.id_extract_ms;
    diagnostics.id_to_term_lookup_ms = write_stats.id_to_term_lookup_ms;
    diagnostics.serialize_ms = write_stats.serialize_ms;
    diagnostics.rows_out = write_stats.rows_out;
    diagnostics.unique_ids_requested = write_stats.unique_ids_requested;
    diagnostics.unique_ids_loaded = write_stats.unique_ids_loaded;
    diagnostics.id_to_term_stats = write_stats.id_to_term_stats;
    diagnostics.total_ms = elapsed_ms(total_start);
    Ok(diagnostics)
}

pub async fn count_cottas_native_ids_file_with_diagnostics_mode(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    mode: NativeIdsCountMode,
) -> Result<CottasNativeIdsCountDiagnostics> {
    let total_start = Instant::now();
    let term_lookup_ms: f64;
    let filter_empty: bool;
    let bound_column: Option<String>;
    let mut bound_id: Option<u32> = None;

    let projection_col: &'static str;
    let native_filter: NativePatternFilter;

    match mode {
        NativeIdsCountMode::NativeFilter => {
            let (filter, lookup_ms) = build_native_pattern_filter_lazy_with_stats(
                input_path, subject, predicate, object, graph,
            )
            .await?;
            term_lookup_ms = lookup_ms;
            filter_empty = matches!(filter, NativePatternFilter::Empty);
            projection_col = first_bound_native_id_column(subject, predicate, object, graph);
            bound_column = Some(projection_col.to_string());
            native_filter = filter;
        }

        NativeIdsCountMode::ManualEq
        | NativeIdsCountMode::ExecuteOnly
        | NativeIdsCountMode::RowsOnly => {
            let (col, id, lookup_ms, empty) =
                resolve_single_bound_id_for_count(input_path, subject, predicate, object, graph)
                    .await?;

            term_lookup_ms = lookup_ms;
            filter_empty = empty;
            projection_col = col.unwrap_or("s");
            bound_column = Some(projection_col.to_string());
            bound_id = id;
            native_filter = NativePatternFilter::All;
        }
    }

    let projected_columns = vec![projection_col.to_string()];

    if filter_empty {
        let timings = CottasNativeIdsCountTimings {
            term_lookup_ms,
            open_ms: 0.0,
            scan_build_ms: 0.0,
            stream_init_ms: 0.0,
            consume_ms: 0.0,
            total_ms: elapsed_ms(total_start),
        };

        return Ok(CottasNativeIdsCountDiagnostics {
            mode,
            count: 0,
            timings,
            filter_empty: true,
            projected_columns,
            bound_column,
            bound_id,
            batches: 0,
            max_batch_rows: 0,
            decoded_values: 0,
        });
    }

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);

    let scan_build_start = Instant::now();
    let scan = file.scan().map_err(VortexRdfError::from)?;

    let scan = match mode {
        NativeIdsCountMode::NativeFilter => match native_filter {
            NativePatternFilter::All => scan,
            NativePatternFilter::Empty => unreachable!("handled above"),
            NativePatternFilter::Expr(expr) => scan.with_filter(expr),
        },
        NativeIdsCountMode::ManualEq
        | NativeIdsCountMode::ExecuteOnly
        | NativeIdsCountMode::RowsOnly => scan,
    };

    let scan = scan.with_projection(vortex_array::expr::select(
        [projection_col].as_slice(),
        vortex_array::expr::root(),
    ));

    let scan_build_ms = elapsed_ms(scan_build_start);

    let stream_init_start = Instant::now();
    let mut stream = scan.into_array_stream().map_err(VortexRdfError::from)?;
    let stream_init_ms = elapsed_ms(stream_init_start);

    let consume_start = Instant::now();

    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();

    let mut count = 0usize;
    let mut batches = 0usize;
    let mut max_batch_rows = 0usize;
    let mut decoded_values = 0usize;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let rows = batch.len();

        batches += 1;
        max_batch_rows = max_batch_rows.max(rows);

        match mode {
            NativeIdsCountMode::NativeFilter | NativeIdsCountMode::RowsOnly => {
                count += rows;
            }

            NativeIdsCountMode::ExecuteOnly => {
                let struct_array = batch
                    .clone()
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                let _ids = struct_array
                    .unmasked_field_by_name(projection_col)
                    .map_err(VortexRdfError::Vortex)?
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                count += rows;
            }

            NativeIdsCountMode::ManualEq => {
                let Some(expected_id) = bound_id else {
                    count += rows;
                    continue;
                };

                let struct_array = batch
                    .clone()
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                let id_array = struct_array
                    .unmasked_field_by_name(projection_col)
                    .map_err(VortexRdfError::Vortex)?
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                let ids = id_array.as_slice::<u32>();
                decoded_values += ids.len();

                for id in ids {
                    if *id == expected_id {
                        count += 1;
                    }
                }
            }
        }
    }

    let consume_ms = elapsed_ms(consume_start);

    let timings = CottasNativeIdsCountTimings {
        term_lookup_ms,
        open_ms,
        scan_build_ms,
        stream_init_ms,
        consume_ms,
        total_ms: elapsed_ms(total_start),
    };

    Ok(CottasNativeIdsCountDiagnostics {
        mode,
        count,
        timings,
        filter_empty,
        projected_columns,
        bound_column,
        bound_id,
        batches,
        max_batch_rows,
        decoded_values,
    })
}

pub async fn count_cottas_native_ids_file_with_diagnostics(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<CottasNativeIdsCountDiagnostics> {
    count_cottas_native_ids_file_with_diagnostics_mode(
        input_path,
        subject,
        predicate,
        object,
        graph,
        NativeIdsCountMode::NativeFilter,
    )
    .await
}

//! Legacy sidecar serializers, index builders, rebuilders, and diagnostics.
//!
//! This module is excluded entirely from unified-only builds.

use super::*;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

// Legacy artifact consolidation and external sidecar writers.
// VORTEX_RDF_COMPLETE_NATIVE_SERIALIZATION_V1
// VORTEX_RDF_TRANSACTIONAL_NATIVE_CONSOLIDATION_WRITER_V1
const NATIVE_CONSOLIDATION_MAX_LAYOUT_PASSES: usize = 4;
fn native_consolidation_staging_path(output_path: &Path) -> PathBuf {
    let name = output_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("native.vortex");
    output_path.with_file_name(format!(".{name}.consolidating.{}.tmp", std::process::id()))
}
fn embedded_native_manifest(
    outer_vortex_length: u64,
    component_lengths: &[(NativeComponent, u64)],
) -> Result<NativeArtifactManifest> {
    let mut manifest = NativeArtifactManifest::production_defaults();
    let mut offset = outer_vortex_length;
    for (component, length) in component_lengths {
        if *length == 0 {
            return Err(VortexRdfError::Serialization(format!(
                "native component {} is empty",
                component.logical_name()
            )));
        }
        let entry = manifest
            .components
            .iter_mut()
            .find(|entry| entry.logical_name == component.logical_name())
            .ok_or_else(|| {
                VortexRdfError::Serialization(format!(
                    "default manifest has no entry for {}",
                    component.logical_name()
                ))
            })?;
        entry.storage = NativeComponentStorage::Embedded {
            offset,
            length: *length,
        };
        offset = offset.checked_add(*length).ok_or_else(|| {
            VortexRdfError::Serialization("consolidated native artifact length overflow".into())
        })?;
    }
    manifest.validate()?;
    Ok(manifest)
}
async fn read_vortex_terminal_tail(path: &Path) -> Result<Vec<u8>> {
    const EOF_SIZE: u64 = 8;
    let file_size = tokio::fs::metadata(path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?
        .len();
    if file_size < EOF_SIZE {
        return Err(VortexRdfError::Deserialization(format!(
            "Vortex file {:?} is shorter than its EOF marker",
            path
        )));
    }
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    file.seek(std::io::SeekFrom::End(-(EOF_SIZE as i64)))
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let mut eof = [0u8; EOF_SIZE as usize];
    file.read_exact(&mut eof)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    if &eof[4..] != b"VTXF" {
        return Err(VortexRdfError::Deserialization(format!(
            "Vortex file {:?} has invalid terminal magic",
            path
        )));
    }
    let postscript_size = u64::from(u16::from_le_bytes([eof[2], eof[3]]));
    let tail_size = postscript_size.checked_add(EOF_SIZE).ok_or_else(|| {
        VortexRdfError::Deserialization("Vortex terminal tail length overflow".into())
    })?;
    let tail_size_usize = usize::try_from(tail_size).map_err(|_| {
        VortexRdfError::Deserialization("Vortex terminal tail does not fit usize".into())
    })?;
    let tail_offset = file_size.checked_sub(tail_size).ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "Vortex postscript in {:?} exceeds the file length",
            path
        ))
    })?;
    file.seek(std::io::SeekFrom::Start(tail_offset))
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let mut tail = vec![0u8; tail_size_usize];
    file.read_exact(&mut tail)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    Ok(tail)
}
async fn write_consolidated_outer_vortex(
    source_path: &Path,
    staging_path: &Path,
    manifest: &NativeArtifactManifest,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<u64> {
    let source = NATIVE_FILE_SESSION
        .open_options()
        .open_path(source_path)
        .await
        .map_err(VortexRdfError::from)?;
    let arrays = source
        .scan()
        .map_err(VortexRdfError::from)?
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let mut staging = tokio::fs::File::create(staging_path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    write_array_stream_to_vortex_file_streaming(
        &mut staging,
        Box::pin(arrays),
        row_group_size,
        compression_profile,
        manifest,
    )
    .await?;
    staging
        .sync_all()
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    drop(staging);
    tokio::fs::metadata(staging_path)
        .await
        .map(|metadata| metadata.len())
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))
}
async fn validate_consolidated_native_artifact(
    artifact_path: &Path,
    expected_manifest: &NativeArtifactManifest,
) -> Result<()> {
    let kind = inspect_native_artifact(artifact_path).await?;
    let actual_manifest = kind.manifest().ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "consolidated native artifact {:?} has no manifest",
            artifact_path
        ))
    })?;
    if actual_manifest != expected_manifest {
        return Err(VortexRdfError::Deserialization(format!(
            "consolidated native artifact manifest mismatch for {:?}",
            artifact_path
        )));
    }

    let outer = NATIVE_FILE_SESSION
        .open_options()
        .open_path(artifact_path)
        .await
        .map_err(VortexRdfError::from)?;
    outer.scan().map_err(VortexRdfError::from)?;

    let resolver = NativeComponentResolver::from_kind(
        artifact_path,
        NativeArtifactKind::ManifestExternal(actual_manifest.clone()),
    )?;
    for component in NativeComponent::ALL {
        match resolver.location(component)? {
            ComponentLocation::Embedded { .. } => {}
            ComponentLocation::External(path) => {
                return Err(VortexRdfError::Deserialization(format!(
                    "consolidated component {} unexpectedly resolves externally at {:?}",
                    component.logical_name(),
                    path
                )));
            }
        }
        let file = resolver.open(component).await?;
        file.scan().map_err(VortexRdfError::from)?;
    }
    Ok(())
}
async fn consolidate_native_artifact(
    output_path: &Path,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<()> {
    let staging_path = native_consolidation_staging_path(output_path);
    if staging_path.exists() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "native consolidation staging path already exists: {:?}",
            staging_path
        )));
    }

    let result: Result<()> = async {
        let mut component_lengths = Vec::with_capacity(NativeComponent::ALL.len());
        for component in NativeComponent::ALL {
            let path = component.external_path(output_path);
            let length = std::fs::metadata(&path)
                .map_err(|error| {
                    VortexRdfError::Serialization(format!(
                        "cannot stat native component {} at {:?}: {error}",
                        component.logical_name(),
                        path
                    ))
                })?
                .len();
            component_lengths.push((component, length));
        }

        // The JSON manifest contains decimal offsets, so its byte length can change the
        // outer Vortex length which, in turn, changes those offsets. Resolve that tiny
        // fixed point before copying multi-gigabyte components. Normally this converges
        // after the first pass because the existing external and embedded manifests have
        // offsets with the same decimal width.
        let mut outer_length = std::fs::metadata(output_path)
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?
            .len();
        let mut final_manifest = None;
        for pass in 0..NATIVE_CONSOLIDATION_MAX_LAYOUT_PASSES {
            let manifest = embedded_native_manifest(outer_length, &component_lengths)?;
            let actual_length = write_consolidated_outer_vortex(
                output_path,
                &staging_path,
                &manifest,
                row_group_size,
                compression_profile,
            )
            .await?;
            if actual_length == outer_length {
                final_manifest = Some(manifest);
                break;
            }
            log::debug!(
                "[cottas_native_ids] consolidation layout pass {} adjusted outer length {} -> {}",
                pass + 1,
                outer_length,
                actual_length
            );
            outer_length = actual_length;
        }
        let manifest = final_manifest.ok_or_else(|| {
            VortexRdfError::Serialization(format!(
                "native consolidation layout did not stabilize after {} passes",
                NATIVE_CONSOLIDATION_MAX_LAYOUT_PASSES
            ))
        })?;

        let terminal_tail = read_vortex_terminal_tail(&staging_path).await?;
        let mut staging = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&staging_path)
            .await
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
        let mut expected_offset = outer_length;
        for (component, expected_length) in &component_lengths {
            let entry = manifest
                .components
                .iter()
                .find(|entry| entry.logical_name == component.logical_name())
                .ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "embedded manifest has no entry for {}",
                        component.logical_name()
                    ))
                })?;
            if entry.storage
                != (NativeComponentStorage::Embedded {
                    offset: expected_offset,
                    length: *expected_length,
                })
            {
                return Err(VortexRdfError::Serialization(format!(
                    "embedded locator mismatch for {}",
                    component.logical_name()
                )));
            }
            let source_path = component.external_path(output_path);
            let mut source = tokio::fs::File::open(&source_path)
                .await
                .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
            let copied = tokio::io::copy(&mut source, &mut staging)
                .await
                .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
            if copied != *expected_length {
                return Err(VortexRdfError::Serialization(format!(
                    "native component {} changed while consolidating: expected {} bytes, copied {}",
                    component.logical_name(),
                    expected_length,
                    copied
                )));
            }
            expected_offset = expected_offset.checked_add(copied).ok_or_else(|| {
                VortexRdfError::Serialization("consolidated artifact offset overflow".into())
            })?;
        }

        // A Vortex reader discovers its postscript at physical EOF. Re-emitting the
        // already-validated terminal postscript keeps all outer footer offsets pointing
        // at the original outer Vortex footer while the opaque component byte ranges sit
        // between that footer and the final postscript.
        tokio::io::AsyncWriteExt::write_all(&mut staging, &terminal_tail)
            .await
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
        staging
            .sync_all()
            .await
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
        drop(staging);

        validate_consolidated_native_artifact(&staging_path, &manifest).await?;
        std::fs::rename(&staging_path, output_path)
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
        runtime_resolver_cache_lock()?.remove(output_path);

        // Intentionally retain sidecars for A/B verification. They are no longer required
        // by the published artifact and can be removed after equivalence testing.
        Ok(())
    }
    .await;

    if result.is_err() {
        let _ = std::fs::remove_file(&staging_path);
    }
    result
}
fn validate_external_native_artifact_inventory(
    data_path: &Path,
    manifest: &NativeArtifactManifest,
) -> Result<()> {
    manifest.validate()?;
    if !data_path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "required native triples component {} is missing at {:?}",
            NATIVE_TRIPLES_LOGICAL_NAME, data_path
        )));
    }
    for component in NativeComponent::ALL {
        let path = component.external_path(data_path);
        if !path.is_file() {
            return Err(VortexRdfError::InvalidOperation(format!(
                "required native artifact component {} is missing at {:?}",
                component.logical_name(),
                path
            )));
        }
    }

    Ok(())
}
fn require_vortex_component(
    data_path: &Path,
    component: NativeComponent,
    label: &str,
) -> Result<PathBuf> {
    let path = native_component_path(data_path, component);
    if !path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "required {label} component {} is missing at {:?}",
            component.logical_name(),
            path
        )));
    }
    Ok(path)
}
fn native_dict_path(data_path: &Path) -> PathBuf {
    if let Ok(raw) = std::env::var("VORTEX_RDF_ID_TO_TERM_VORTEX_PATH") {
        let candidate = PathBuf::from(raw);
        if candidate.extension().is_some_and(|ext| ext == "vortex") && candidate.is_file() {
            return candidate;
        }
        panic!(
            "VORTEX_RDF_ID_TO_TERM_VORTEX_PATH must name an existing .vortex file: {:?}",
            candidate
        );
    }
    native_component_path(data_path, NativeComponent::DictionaryVortex)
}
fn native_dict_term_to_id_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::DictionaryTermToIdVortex)
}
fn native_dict_term_directory_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::DictionaryTermDirectoryVortex)
}
async fn write_native_dictionary_component(
    output_path: &Path,
    run_paths: &[PathBuf],
    order: PairRunOrder,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<()> {
    let temporary_path = output_path.with_extension("vortex.tmp");
    let stream = dictionary_pair_stream(run_paths.to_vec(), order, row_group_size)?;
    let dtype = empty_native_dictionary_array()?.dtype().clone();
    let arrays = ArrayStreamAdapter::new(dtype, stream);
    let mut output = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    // VORTEX_RDF_ID_TO_TERM_BALANCED_COMPRESSION_V1
    let strategy_builder =
        WriteStrategyBuilder::default().with_row_block_size(row_group_size.max(1));
    let strategy_builder = match compression_profile {
        CottasVortexCompressionProfile::Balanced => strategy_builder,
        CottasVortexCompressionProfile::Compact => strategy_builder
            .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact()),
    };
    let strategy = strategy_builder.build();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output, arrays)
        .await
        .map_err(VortexRdfError::from)?;
    drop(output);
    std::fs::rename(&temporary_path, output_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    Ok(())
}
async fn write_dictionary_lookup_sidecars_from_pair_runs(
    pair_run_paths: &PairRunPaths,
    data_path: &Path,
    row_group_size: usize,
) -> Result<()> {
    let id_to_term_path = native_dict_path(data_path);
    let term_to_id_path = native_dict_term_to_id_path(data_path);
    write_native_dictionary_component(
        &id_to_term_path,
        &pair_run_paths.id_run_paths,
        PairRunOrder::Id,
        row_group_size,
        CottasVortexCompressionProfile::Balanced,
    )
    .await?;
    write_native_dictionary_component(
        &term_to_id_path,
        &pair_run_paths.term_run_paths,
        PairRunOrder::Term,
        row_group_size,
        CottasVortexCompressionProfile::Compact,
    )
    .await?;
    log::info!(
        "[cottas_native_ids] wrote Vortex dictionary components {:?} and {:?}",
        id_to_term_path,
        term_to_id_path
    );
    Ok(())
}

// Builder-only private paths, arrays, and state.
fn native_subject_range_vortex_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::SubjectRangesVortex)
}
fn build_po_partition_array(ids: Vec<u32>, starts: Vec<u64>, ends: Vec<u64>) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("predicate_id", PrimitiveArray::from_iter(ids).into_array()),
        (
            "directory_start",
            PrimitiveArray::from_iter(starts).into_array(),
        ),
        (
            "directory_end",
            PrimitiveArray::from_iter(ends).into_array(),
        ),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}
fn native_po_exact_directory_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::PredicateObjectDirectoryVortexV2)
}
fn native_po_predicate_partitions_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(
        data_path,
        NativeComponent::PredicateObjectPartitionsVortexV2,
    )
}
fn native_po_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::PredicateObjectRangesVortexV2)
}
fn build_po_directory_array(
    predicate_ids: Vec<u32>,
    object_ids: Vec<u32>,
    offsets: Vec<u64>,
    counts: Vec<u32>,
    rows: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        (
            "predicate_id",
            PrimitiveArray::from_iter(predicate_ids).into_array(),
        ),
        (
            "object_id",
            PrimitiveArray::from_iter(object_ids).into_array(),
        ),
        (
            "range_offset",
            PrimitiveArray::from_iter(offsets).into_array(),
        ),
        (
            "range_count",
            PrimitiveArray::from_iter(counts).into_array(),
        ),
        (
            "candidate_rows",
            PrimitiveArray::from_iter(rows).into_array(),
        ),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}
fn native_p_exact_directory_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::PredicateDirectoryVortexV2)
}
fn native_p_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::PredicateRangesVortexV2)
}
fn build_predicate_payload_array(starts: Vec<u64>, ends: Vec<u64>) -> Result<ArrayRef> {
    build_exact_range_payload_array(starts, ends)
}
fn build_predicate_directory_array(
    ids: Vec<u32>,
    offsets: Vec<u64>,
    counts: Vec<u32>,
    rows: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("predicate_id", PrimitiveArray::from_iter(ids).into_array()),
        (
            "range_offset",
            PrimitiveArray::from_iter(offsets).into_array(),
        ),
        (
            "range_count",
            PrimitiveArray::from_iter(counts).into_array(),
        ),
        (
            "candidate_rows",
            PrimitiveArray::from_iter(rows).into_array(),
        ),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}
fn native_o_exact_directory_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::ObjectDirectoryVortexV2)
}
fn native_o_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::ObjectRangesVortexV2)
}
fn build_object_payload_array(starts: Vec<u64>, ends: Vec<u64>) -> Result<ArrayRef> {
    build_exact_range_payload_array(starts, ends)
}
fn build_object_directory_array(
    ids: Vec<u32>,
    offsets: Vec<u64>,
    counts: Vec<u32>,
    rows: Vec<u64>,
) -> Result<ArrayRef> {
    build_exact_range_directory_array("object_id", ids, offsets, counts, rows)
}
#[derive(Clone, Debug, Default)]
struct NativeSubjectRangeBuildState {
    rows_scanned: u64,
    ranges_written: u64,
    batches: usize,
    max_batch_rows: usize,
}
fn store_subject_range_build_state(
    shared_state: &Mutex<NativeSubjectRangeBuildState>,
    state: NativeSubjectRangeBuildState,
) -> VortexResult<()> {
    let mut guard = shared_state
        .lock()
        .map_err(|_| vortex_error::vortex_err!("subject range build-state mutex was poisoned"))?;
    *guard = state;
    Ok(())
}
fn build_subject_range_array(
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
fn empty_subject_range_array() -> Result<ArrayRef> {
    build_subject_range_array(Vec::new(), Vec::new(), Vec::new())
}
fn store_rewritten_dictionary_row_count(shared_rows: &Mutex<u64>, rows: u64) -> VortexResult<()> {
    let mut guard = shared_rows
        .lock()
        .map_err(|_| vortex_error::vortex_err!("rewrite row counter mutex was poisoned"))?;

    *guard = rows;
    Ok(())
}

pub async fn serialize_cottas_native_file<Dict, S>(
    quad_stream: S,
    output_path: &Path,
    config: CottasNativeConfig,
) -> Result<()>
where
    Dict: RdfDictionary + Send + Sync + 'static,
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    if config.ordering != TripleOrdering::SPO {
        return Err(VortexRdfError::InvalidOperation(format!(
            "complete cottas-native-ids serialization requires SPO ordering; got {:?}",
            config.ordering
        )));
    }
    let row_group_size = config.row_group_size.max(1);

    let sort_batch_size = std::env::var("VORTEX_RDF_NATIVE_ID_SORT_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(row_group_size.saturating_mul(8).max(1_000_000));

    let temp_dir = tempfile::tempdir().map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    let string_run_paths = spill_sorted_native_id_string_runs(
        quad_stream,
        config.ordering,
        sort_batch_size,
        temp_dir.path(),
    )
    .await?;

    let mut dictionary = Dict::new();

    let pair_run_paths = build_dictionary_and_pair_runs::<Dict>(
        &mut dictionary,
        &string_run_paths,
        temp_dir.path(),
    )?;

    let id_run_paths = encode_string_runs_to_id_runs::<Dict>(
        &dictionary,
        &string_run_paths,
        config.ordering,
        temp_dir.path(),
    )?;

    drop(string_run_paths);

    let array_stream =
        merge_sorted_id_runs_to_array_stream(id_run_paths, config.ordering, row_group_size)?;

    let mut data_file = tokio::fs::File::create(output_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    let manifest = NativeArtifactManifest::production_defaults();
    write_array_stream_to_vortex_file_streaming(
        &mut data_file,
        Box::pin(array_stream),
        row_group_size,
        config.compression_profile,
        &manifest,
    )
    .await?;
    data_file
        .sync_all()
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    drop(data_file);

    let persisted_kind = inspect_native_artifact(output_path).await?;
    let persisted_manifest = persisted_kind.manifest().ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "new native artifact {:?} is missing required metadata segment {:?}",
            output_path, NATIVE_ARTIFACT_METADATA_KEY
        ))
    })?;
    if persisted_manifest != &manifest {
        return Err(VortexRdfError::Deserialization(format!(
            "native artifact manifest metadata round-trip mismatch for {:?}",
            output_path
        )));
    }

    write_dictionary_lookup_sidecars_from_pair_runs(
        &pair_run_paths,
        output_path,
        config.dict_row_group_size,
    )
    .await?;
    let term_directory_stats =
        build_cottas_native_term_directory(output_path, NATIVE_TERM_DIRECTORY_FENCE_ROWS).await?;
    log::info!(
        "[cottas_native_ids] built sparse term directory during serialization: dictionary_rows={}, entries={}, fence_rows={}, total_ms={:.3}",
        term_directory_stats.dictionary_rows,
        term_directory_stats.directory_entries,
        term_directory_stats.fence_rows,
        term_directory_stats.total_ms
    );
    if config.ordering == TripleOrdering::SPO {
        let subject_index_stats = build_cottas_native_subject_range_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built SPO subject range index during serialization: ranges={}, rows={}, total_ms={:.3}",
            subject_index_stats.ranges_written,
            subject_index_stats.rows_scanned,
            subject_index_stats.total_ms
        );
        let po_v2_stats = build_cottas_native_po_exact_ranges_v2_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built typed PO v2 directory/payload: row_groups={}, rows={}, exact_ranges={}, total_ms={:.3}",
            po_v2_stats.row_groups,
            po_v2_stats.rows_scanned,
            po_v2_stats.unique_po_hashes_written,
            po_v2_stats.total_ms
        );
        let p_exact_stats = build_cottas_native_p_exact_ranges_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built predicate exact-ranges index during serialization: row_groups={}, rows={}, exact_ranges={}, total_ms={:.3}",
            p_exact_stats.row_groups,
            p_exact_stats.rows_scanned,
            p_exact_stats.unique_po_hashes_written,
            p_exact_stats.total_ms
        );
        let o_exact_stats = build_cottas_native_o_exact_ranges_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built object exact-ranges index during serialization: row_groups={}, rows={}, exact_ranges={}, total_ms={:.3}",
            o_exact_stats.row_groups,
            o_exact_stats.rows_scanned,
            o_exact_stats.unique_po_hashes_written,
            o_exact_stats.total_ms
        );
    } else {
        unreachable!("SPO ordering is enforced before serialization starts");
    }
    validate_external_native_artifact_inventory(output_path, &manifest)?;
    consolidate_native_artifact(output_path, row_group_size, config.compression_profile).await?;
    Ok(())
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativePoRowGroupIndexBuildStats {
    pub input_path: String,
    pub output_path: String,
    pub row_groups: usize,
    pub rows_scanned: u64,
    pub unique_po_hashes_written: u64,
    pub open_ms: f64,
    pub scan_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativePoPredicatePartitionBuildStats {
    pub input_path: String,
    pub output_path: String,
    pub directory_rows: u64,
    pub predicates: usize,
    pub open_ms: f64,
    pub scan_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}
pub async fn build_cottas_native_po_predicate_partitions_v2(
    data_path: &Path,
) -> Result<NativePoPredicatePartitionBuildStats> {
    let total_start = Instant::now();
    let directory_path = native_po_exact_directory_v2_path(data_path);
    if !directory_path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Cannot build PO predicate partitions: directory {:?} does not exist",
            directory_path
        )));
    }
    let output_path = native_po_predicate_partitions_v2_path(data_path);
    let temporary_path = output_path.with_extension("vortex.tmp");
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&directory_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["predicate_id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let mut ids = Vec::new();
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut current = None;
    let mut current_start = 0u64;
    let mut directory_rows = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let values = extract_projected_u32_column(&batch, "predicate_id")?;
        if values.len() != batch.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "PO directory predicate projection returned {} values for {} rows",
                values.len(),
                batch.len()
            )));
        }
        for predicate_id in values {
            match current {
                None => {
                    current = Some(predicate_id);
                    current_start = directory_rows;
                }
                Some(previous) if previous != predicate_id => {
                    if predicate_id <= previous {
                        return Err(VortexRdfError::Deserialization(format!(
                            "PO directory is not sorted by predicate_id: {} followed by {}",
                            previous, predicate_id
                        )));
                    }
                    ids.push(previous);
                    starts.push(current_start);
                    ends.push(directory_rows);
                    current = Some(predicate_id);
                    current_start = directory_rows;
                }
                Some(_) => {}
            }
            directory_rows += 1;
        }
    }
    if let Some(predicate_id) = current {
        ids.push(predicate_id);
        starts.push(current_start);
        ends.push(directory_rows);
    }
    let scan_ms = elapsed_ms(scan_start);
    let predicates = ids.len();
    let array = build_po_partition_array(ids, starts, ends)?;
    let dtype = build_po_partition_array(Vec::new(), Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let output_stream = ArrayStreamAdapter::new(dtype, futures::stream::iter(vec![Ok(array)]));
    let write_start = Instant::now();
    let mut output_file = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(65_536)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output_file, output_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(output_file);
    std::fs::rename(&temporary_path, &output_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    #[cfg(feature = "legacy-sidecars")]
    invalidate_po_partition_cache(&output_path)?;
    let write_ms = elapsed_ms(write_start);
    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] wrote PO predicate partitions {:?}: directory_rows={}, predicates={}, total_ms={:.3}",
        output_path,
        directory_rows,
        predicates,
        total_ms
    );
    Ok(NativePoPredicatePartitionBuildStats {
        input_path: directory_path.display().to_string(),
        output_path: output_path.display().to_string(),
        directory_rows,
        predicates,
        open_ms,
        scan_ms,
        write_ms,
        total_ms,
    })
}
pub async fn build_cottas_native_po_exact_ranges_v2_index(
    input_path: &Path,
) -> Result<NativePoRowGroupIndexBuildStats> {
    const OUTPUT_BATCH_ROWS: usize = 65_536;
    let total_start = Instant::now();
    let directory_path = native_po_exact_directory_v2_path(input_path);
    let payload_path = native_po_exact_ranges_v2_path(input_path);
    let directory_tmp = directory_path.with_extension("vortex.tmp");
    let payload_tmp = payload_path.with_extension("vortex.tmp");
    let temp_dir =
        tempfile::tempdir().map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let sort_batch = std::env::var("VORTEX_RDF_PO_V2_SORT_BATCH_RANGES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["p", "o"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let mut records = Vec::with_capacity(sort_batch);
    let mut runs = Vec::new();
    let mut rows_scanned = 0u64;
    let mut row_groups = 0usize;
    let mut active_key: Option<(u32, u32)> = None;
    let mut active_start = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let predicates = extract_projected_u32_column(&batch, "p")?;
        let objects = extract_projected_u32_column(&batch, "o")?;
        if predicates.len() != objects.len() || predicates.len() != batch.len() {
            return Err(VortexRdfError::Serialization(format!(
                "PO v2 scan column mismatch: rows={}, predicates={}, objects={}",
                batch.len(),
                predicates.len(),
                objects.len()
            )));
        }
        row_groups += 1;
        for key in predicates.into_iter().zip(objects) {
            match active_key {
                None => {
                    active_key = Some(key);
                    active_start = rows_scanned;
                }
                Some(previous) if previous != key => {
                    records.push(PoRangeRecord {
                        predicate_id: previous.0,
                        object_id: previous.1,
                        row_start: active_start,
                        row_end: rows_scanned,
                    });
                    active_key = Some(key);
                    active_start = rows_scanned;
                    if records.len() >= sort_batch {
                        let run_idx = runs.len();
                        flush_po_range_run(&mut records, temp_dir.path(), run_idx, &mut runs)?;
                    }
                }
                Some(_) => {}
            }
            rows_scanned += 1;
        }
    }
    if let Some((predicate_id, object_id)) = active_key {
        records.push(PoRangeRecord {
            predicate_id,
            object_id,
            row_start: active_start,
            row_end: rows_scanned,
        });
    }
    if !records.is_empty() {
        let run_idx = runs.len();
        flush_po_range_run(&mut records, temp_dir.path(), run_idx, &mut runs)?;
    }
    let scan_ms = elapsed_ms(scan_start);

    let merge_start = Instant::now();
    let merged_path = temp_dir.path().join("po_ranges_merged.bin");
    let mut merged_writer = BufWriter::new(
        std::fs::File::create(&merged_path)
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?,
    );
    let mut readers = runs
        .iter()
        .map(|path| PoRangeRunReader::new(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = reader.read_one()? {
            heap.push(PoRangeHeapItem { value, run_idx });
        }
    }

    let mut dir_predicates = Vec::new();
    let mut dir_objects = Vec::new();
    let mut dir_offsets = Vec::new();
    let mut dir_counts = Vec::new();
    let mut dir_rows = Vec::new();
    let mut payload_offset = 0u64;
    let mut active_key: Option<(u32, u32)> = None;
    let mut active_offset = 0u64;
    let mut active_count = 0u32;
    let mut active_rows = 0u64;
    let finish_entry = |key: (u32, u32),
                        offset: u64,
                        count: u32,
                        rows: u64,
                        predicates: &mut Vec<u32>,
                        objects: &mut Vec<u32>,
                        offsets: &mut Vec<u64>,
                        counts: &mut Vec<u32>,
                        row_counts: &mut Vec<u64>| {
        predicates.push(key.0);
        objects.push(key.1);
        offsets.push(offset);
        counts.push(count);
        row_counts.push(rows);
    };

    while let Some(item) = heap.pop() {
        let value = item.value;
        let key = (value.predicate_id, value.object_id);
        if active_key != Some(key) {
            if let Some(previous) = active_key {
                finish_entry(
                    previous,
                    active_offset,
                    active_count,
                    active_rows,
                    &mut dir_predicates,
                    &mut dir_objects,
                    &mut dir_offsets,
                    &mut dir_counts,
                    &mut dir_rows,
                );
            }
            active_key = Some(key);
            active_offset = payload_offset;
            active_count = 0;
            active_rows = 0;
        }
        if value.row_start >= value.row_end {
            return Err(VortexRdfError::Serialization(format!(
                "PO v2 contains invalid range {}..{} for ({}, {})",
                value.row_start, value.row_end, value.predicate_id, value.object_id
            )));
        }
        write_po_range_record(&mut merged_writer, value)?;
        active_count = active_count
            .checked_add(1)
            .ok_or_else(|| VortexRdfError::Serialization("PO v2 range count overflow".into()))?;
        active_rows = active_rows
            .checked_add(value.row_end - value.row_start)
            .ok_or_else(|| VortexRdfError::Serialization("PO v2 row count overflow".into()))?;
        payload_offset = payload_offset
            .checked_add(1)
            .ok_or_else(|| VortexRdfError::Serialization("PO v2 payload offset overflow".into()))?;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(PoRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some(key) = active_key {
        finish_entry(
            key,
            active_offset,
            active_count,
            active_rows,
            &mut dir_predicates,
            &mut dir_objects,
            &mut dir_offsets,
            &mut dir_counts,
            &mut dir_rows,
        );
    }
    merged_writer
        .flush()
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    drop(merged_writer);
    let merge_ms = elapsed_ms(merge_start);

    let payload_reader = PoRangeRunReader::new(&merged_path)?;
    let payload_arrays = async_stream::try_stream! {
        let mut reader = payload_reader;
        loop {
            let mut starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            let mut ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            while starts.len() < OUTPUT_BATCH_ROWS {
                let Some(value) = reader.read_one().map_err(rdf_err_to_vortex_err)? else { break; };
                starts.push(value.row_start);
                ends.push(value.row_end);
            }
            if starts.is_empty() { break; }
            yield build_predicate_payload_array(starts, ends).map_err(rdf_err_to_vortex_err)?;
        }
    };
    let payload_dtype = build_predicate_payload_array(Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let payload_stream = ArrayStreamAdapter::new(payload_dtype, payload_arrays);
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    let write_start = Instant::now();
    let mut payload_file = tokio::fs::File::create(&payload_tmp)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(
            WriteStrategyBuilder::default()
                .with_row_block_size(OUTPUT_BATCH_ROWS)
                .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
                .build(),
        )
        .write(&mut payload_file, payload_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(payload_file);

    let directory_entries = dir_predicates.len();
    let directory_array = build_po_directory_array(
        dir_predicates,
        dir_objects,
        dir_offsets,
        dir_counts,
        dir_rows,
    )?;
    let directory_dtype =
        build_po_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new())?
            .dtype()
            .clone();
    let directory_stream = ArrayStreamAdapter::new(
        directory_dtype,
        futures::stream::iter(vec![Ok(directory_array)]),
    );
    let mut directory_file = tokio::fs::File::create(&directory_tmp)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut directory_file, directory_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(directory_file);
    std::fs::rename(&payload_tmp, &payload_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    std::fs::rename(&directory_tmp, &directory_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    build_cottas_native_po_predicate_partitions_v2(input_path).await?;
    let write_ms = elapsed_ms(write_start);
    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] PO v2 scan_ms={scan_ms:.3}, merge_ms={merge_ms:.3}, directory_entries={directory_entries}, payload_ranges={payload_offset}"
    );
    Ok(NativePoRowGroupIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: directory_path.display().to_string(),
        row_groups,
        rows_scanned,
        unique_po_hashes_written: payload_offset,
        open_ms,
        scan_ms,
        write_ms,
        total_ms,
    })
}
pub async fn build_cottas_native_p_exact_ranges_index(
    input_path: &Path,
) -> Result<NativePoRowGroupIndexBuildStats> {
    const OUTPUT_BATCH_ROWS: usize = 65_536;
    let total_start = Instant::now();
    let directory_path = native_p_exact_directory_v2_path(input_path);
    let payload_path = native_p_exact_ranges_v2_path(input_path);
    let directory_tmp = directory_path.with_extension("vortex.tmp");
    let payload_tmp = payload_path.with_extension("vortex.tmp");
    let temp_dir = tempfile::tempdir().map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let sort_batch = std::env::var("VORTEX_RDF_P_V2_SORT_BATCH_RANGES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["p"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let mut records = Vec::with_capacity(sort_batch);
    let mut runs = Vec::new();
    let mut rows_scanned = 0u64;
    let mut row_groups = 0usize;
    let mut current_predicate = None;
    let mut current_start = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let values = extract_projected_u32_column(&batch, "p")?;
        row_groups += 1;
        for predicate_id in values {
            match current_predicate {
                None => {
                    current_predicate = Some(predicate_id);
                    current_start = rows_scanned;
                }
                Some(previous) if previous != predicate_id => {
                    records.push(PredicateRangeRecord {
                        predicate_id: previous,
                        row_start: current_start,
                        row_end: rows_scanned,
                    });
                    current_predicate = Some(predicate_id);
                    current_start = rows_scanned;
                    if records.len() >= sort_batch {
                        let idx = runs.len();
                        flush_predicate_range_run(&mut records, temp_dir.path(), idx, &mut runs)?;
                    }
                }
                Some(_) => {}
            }
            rows_scanned += 1;
        }
    }
    if let Some(predicate_id) = current_predicate {
        records.push(PredicateRangeRecord {
            predicate_id,
            row_start: current_start,
            row_end: rows_scanned,
        });
    }
    if !records.is_empty() {
        let idx = runs.len();
        flush_predicate_range_run(&mut records, temp_dir.path(), idx, &mut runs)?;
    }
    let scan_ms = elapsed_ms(scan_start);

    let merge_start = Instant::now();
    let merged_path = temp_dir.path().join("predicate_ranges_merged.bin");
    let mut merged_writer = BufWriter::new(
        std::fs::File::create(&merged_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    let mut readers = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::new();
    for path in &runs {
        readers.push(PredicateRangeRunReader::new(path)?);
    }
    for run_idx in 0..readers.len() {
        if let Some(value) = readers[run_idx].read_one()? {
            heap.push(PredicateRangeHeapItem { value, run_idx });
        }
    }
    let mut dir_ids = Vec::new();
    let mut dir_offsets = Vec::new();
    let mut dir_counts = Vec::new();
    let mut dir_rows = Vec::new();
    let mut payload_offset = 0u64;
    let mut active_predicate = None;
    let mut active_offset = 0u64;
    let mut active_count = 0u32;
    let mut active_rows = 0u64;
    while let Some(item) = heap.pop() {
        let value = item.value;
        if active_predicate != Some(value.predicate_id) {
            if let Some(predicate_id) = active_predicate {
                dir_ids.push(predicate_id);
                dir_offsets.push(active_offset);
                dir_counts.push(active_count);
                dir_rows.push(active_rows);
            }
            active_predicate = Some(value.predicate_id);
            active_offset = payload_offset;
            active_count = 0;
            active_rows = 0;
        }
        if value.row_start > value.row_end {
            return Err(VortexRdfError::Serialization(
                "predicate v2 range start exceeds end".into(),
            ));
        }
        write_predicate_range_record(&mut merged_writer, value)?;
        active_count = active_count.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("predicate v2 range count overflow".into())
        })?;
        active_rows = active_rows.saturating_add(value.row_end - value.row_start);
        payload_offset += 1;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(PredicateRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some(predicate_id) = active_predicate {
        dir_ids.push(predicate_id);
        dir_offsets.push(active_offset);
        dir_counts.push(active_count);
        dir_rows.push(active_rows);
    }
    merged_writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    drop(merged_writer);
    let merge_ms = elapsed_ms(merge_start);

    let payload_reader = PredicateRangeRunReader::new(&merged_path)?;
    let payload_arrays = async_stream::try_stream! {
        let mut reader = payload_reader;
        loop {
            let mut starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            let mut ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            while starts.len() < OUTPUT_BATCH_ROWS {
                let Some(value) = reader.read_one().map_err(rdf_err_to_vortex_err)? else { break; };
                starts.push(value.row_start);
                ends.push(value.row_end);
            }
            if starts.is_empty() { break; }
            yield build_predicate_payload_array(starts, ends).map_err(rdf_err_to_vortex_err)?;
        }
    };
    let payload_dtype = build_predicate_payload_array(Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let payload_stream = ArrayStreamAdapter::new(payload_dtype, payload_arrays);
    let payload_strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    let mut payload_file = tokio::fs::File::create(&payload_tmp)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(payload_strategy)
        .write(&mut payload_file, payload_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(payload_file);

    let directory_rows = dir_ids.len();
    let directory_array =
        build_predicate_directory_array(dir_ids, dir_offsets, dir_counts, dir_rows)?;
    let directory_dtype =
        build_predicate_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())?
            .dtype()
            .clone();
    let directory_arrays = futures::stream::iter(vec![Ok(directory_array)]);
    let directory_stream = ArrayStreamAdapter::new(directory_dtype, directory_arrays);
    let mut directory_file = tokio::fs::File::create(&directory_tmp)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let directory_strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(directory_strategy)
        .write(&mut directory_file, directory_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(directory_file);
    std::fs::rename(&payload_tmp, &payload_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    std::fs::rename(&directory_tmp, &directory_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    // A rebuild in the same process must not retain old directory metadata.
    #[cfg(feature = "legacy-sidecars")]
    invalidate_predicate_v2_cache(&directory_path)?;

    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] wrote predicate v2 directory {:?} predicates={} and payload {:?} ranges={} rows={} runs={} scan_ms={:.3} merge_ms={:.3} total_ms={:.3}",
        directory_path,
        directory_rows,
        payload_path,
        payload_offset,
        rows_scanned,
        runs.len(),
        scan_ms,
        merge_ms,
        total_ms
    );
    Ok(NativePoRowGroupIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: directory_path.display().to_string(),
        row_groups,
        rows_scanned,
        unique_po_hashes_written: payload_offset,
        open_ms,
        scan_ms,
        write_ms: merge_ms,
        total_ms,
    })
}
pub async fn build_cottas_native_o_exact_ranges_index(
    input_path: &Path,
) -> Result<NativePoRowGroupIndexBuildStats> {
    const OUTPUT_BATCH_ROWS: usize = 65_536;
    let total_start = Instant::now();
    let directory_path = native_o_exact_directory_v2_path(input_path);
    let payload_path = native_o_exact_ranges_v2_path(input_path);
    let directory_tmp = directory_path.with_extension("vortex.tmp");
    let payload_tmp = payload_path.with_extension("vortex.tmp");
    let temp_dir = tempfile::tempdir().map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let sort_batch = std::env::var("VORTEX_RDF_O_V2_SORT_BATCH_RANGES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["o"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let mut records = Vec::with_capacity(sort_batch);
    let mut runs = Vec::new();
    let mut rows_scanned = 0u64;
    let mut row_groups = 0usize;
    let mut current_object = None;
    let mut current_start = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let values = extract_projected_u32_column(&batch, "o")?;
        row_groups += 1;
        for object_id in values {
            match current_object {
                None => {
                    current_object = Some(object_id);
                    current_start = rows_scanned;
                }
                Some(previous) if previous != object_id => {
                    records.push(ObjectRangeRecord {
                        object_id: previous,
                        row_start: current_start,
                        row_end: rows_scanned,
                    });
                    current_object = Some(object_id);
                    current_start = rows_scanned;
                    if records.len() >= sort_batch {
                        let idx = runs.len();
                        flush_object_range_run(&mut records, temp_dir.path(), idx, &mut runs)?;
                    }
                }
                Some(_) => {}
            }
            rows_scanned += 1;
        }
    }
    if let Some(object_id) = current_object {
        records.push(ObjectRangeRecord {
            object_id,
            row_start: current_start,
            row_end: rows_scanned,
        });
    }
    if !records.is_empty() {
        let idx = runs.len();
        flush_object_range_run(&mut records, temp_dir.path(), idx, &mut runs)?;
    }
    let scan_ms = elapsed_ms(scan_start);

    let merge_start = Instant::now();
    let merged_path = temp_dir.path().join("object_ranges_merged.bin");
    let mut merged_writer = BufWriter::new(
        std::fs::File::create(&merged_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    let mut readers = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::new();
    for path in &runs {
        readers.push(ObjectRangeRunReader::new(path)?);
    }
    for run_idx in 0..readers.len() {
        if let Some(value) = readers[run_idx].read_one()? {
            heap.push(ObjectRangeHeapItem { value, run_idx });
        }
    }
    let mut dir_ids = Vec::new();
    let mut dir_offsets = Vec::new();
    let mut dir_counts = Vec::new();
    let mut dir_rows = Vec::new();
    let mut payload_offset = 0u64;
    let mut active_object = None;
    let mut active_offset = 0u64;
    let mut active_count = 0u32;
    let mut active_rows = 0u64;
    while let Some(item) = heap.pop() {
        let value = item.value;
        if active_object != Some(value.object_id) {
            if let Some(object_id) = active_object {
                dir_ids.push(object_id);
                dir_offsets.push(active_offset);
                dir_counts.push(active_count);
                dir_rows.push(active_rows);
            }
            active_object = Some(value.object_id);
            active_offset = payload_offset;
            active_count = 0;
            active_rows = 0;
        }
        if value.row_start > value.row_end {
            return Err(VortexRdfError::Serialization(
                "object v2 range start exceeds end".into(),
            ));
        }
        write_object_range_record(&mut merged_writer, value)?;
        active_count = active_count.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("object v2 range count overflow".into())
        })?;
        active_rows = active_rows.saturating_add(value.row_end - value.row_start);
        payload_offset += 1;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(ObjectRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some(object_id) = active_object {
        dir_ids.push(object_id);
        dir_offsets.push(active_offset);
        dir_counts.push(active_count);
        dir_rows.push(active_rows);
    }
    merged_writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    drop(merged_writer);
    let merge_ms = elapsed_ms(merge_start);

    let payload_reader = ObjectRangeRunReader::new(&merged_path)?;
    let payload_arrays = async_stream::try_stream! {
        let mut reader = payload_reader;
        loop {
            let mut starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            let mut ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            while starts.len() < OUTPUT_BATCH_ROWS {
                let Some(value) = reader.read_one().map_err(rdf_err_to_vortex_err)? else { break; };
                starts.push(value.row_start);
                ends.push(value.row_end);
            }
            if starts.is_empty() { break; }
            yield build_object_payload_array(starts, ends).map_err(rdf_err_to_vortex_err)?;
        }
    };
    let payload_dtype = build_object_payload_array(Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let payload_stream = ArrayStreamAdapter::new(payload_dtype, payload_arrays);
    let payload_strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    let mut payload_file = tokio::fs::File::create(&payload_tmp)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(payload_strategy)
        .write(&mut payload_file, payload_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(payload_file);

    let directory_rows = dir_ids.len();
    let directory_array = build_object_directory_array(dir_ids, dir_offsets, dir_counts, dir_rows)?;
    let directory_dtype =
        build_object_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())?
            .dtype()
            .clone();
    let directory_arrays = futures::stream::iter(vec![Ok(directory_array)]);
    let directory_stream = ArrayStreamAdapter::new(directory_dtype, directory_arrays);
    let mut directory_file = tokio::fs::File::create(&directory_tmp)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let directory_strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(directory_strategy)
        .write(&mut directory_file, directory_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(directory_file);
    std::fs::rename(&payload_tmp, &payload_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    std::fs::rename(&directory_tmp, &directory_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] wrote object v2 directory {:?} objects={} and payload {:?} ranges={} rows={} runs={} scan_ms={:.3} merge_ms={:.3} total_ms={:.3}",
        directory_path,
        directory_rows,
        payload_path,
        payload_offset,
        rows_scanned,
        runs.len(),
        scan_ms,
        merge_ms,
        total_ms
    );
    Ok(NativePoRowGroupIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: directory_path.display().to_string(),
        row_groups,
        rows_scanned,
        unique_po_hashes_written: payload_offset,
        open_ms,
        scan_ms,
        write_ms: merge_ms,
        total_ms,
    })
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeSubjectRangeIndexBuildStats {
    pub input_path: String,
    pub output_path: String,
    pub rows_scanned: u64,
    pub ranges_written: u64,
    pub batches: usize,
    pub max_batch_rows: usize,
    pub open_ms: f64,
    pub scan_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}
/// Writes the production subject index directly to Vortex.
pub async fn build_cottas_native_subject_range_index(
    input_path: &Path,
) -> Result<NativeSubjectRangeIndexBuildStats> {
    const OUTPUT_BATCH_ROWS: usize = 65_536;

    let total_start = Instant::now();
    let output_path = native_subject_range_vortex_path(input_path);
    let temporary_path = output_path.with_extension("vortex.tmp");

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);

    let scan_start = Instant::now();
    let mut input_stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["s"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let shared_state = Arc::new(Mutex::new(NativeSubjectRangeBuildState::default()));
    let stream_state = Arc::clone(&shared_state);
    let output_arrays = async_stream::try_stream! {
        let mut subject_ids = Vec::with_capacity(OUTPUT_BATCH_ROWS);
        let mut row_starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
        let mut row_ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
        let mut rows_scanned = 0u64;
        let mut ranges_written = 0u64;
        let mut batches = 0usize;
        let mut max_batch_rows = 0usize;
        let mut current_subject: Option<u32> = None;
        let mut current_start = 0u64;
        let mut last_completed_subject: Option<u32> = None;

        while let Some(batch_result) = input_stream.next().await {
            let batch = batch_result?;
            let batch_rows = batch.len();
            batches += 1;
            max_batch_rows = max_batch_rows.max(batch_rows);
            if batch_rows == 0 {
                continue;
            }

            let values = extract_projected_u32_column(&batch, "s")
                .map_err(rdf_err_to_vortex_err)?;
            if values.len() != batch_rows {
                Err(vortex_error::vortex_err!(
                    "subject range build saw {} subject IDs for {} rows",
                    values.len(),
                    batch_rows
                ))?;
            }

            for subject_id in values {
                match current_subject {
                    None => {
                        current_subject = Some(subject_id);
                        current_start = rows_scanned;
                    }
                    Some(previous) if previous != subject_id => {
                        if let Some(completed) = last_completed_subject {
                            if previous <= completed {
                                Err(vortex_error::vortex_err!(
                                    "subject IDs are not strictly grouped/increasing: completed={}, next={}; SPO ordering is required",
                                    completed,
                                    previous
                                ))?;
                            }
                        }

                        subject_ids.push(previous);
                        row_starts.push(current_start);
                        row_ends.push(rows_scanned);
                        ranges_written += 1;
                        last_completed_subject = Some(previous);
                        current_subject = Some(subject_id);
                        current_start = rows_scanned;

                        if subject_ids.len() >= OUTPUT_BATCH_ROWS {
                            yield build_subject_range_array(
                                std::mem::take(&mut subject_ids),
                                std::mem::take(&mut row_starts),
                                std::mem::take(&mut row_ends),
                            ).map_err(rdf_err_to_vortex_err)?;
                            subject_ids = Vec::with_capacity(OUTPUT_BATCH_ROWS);
                            row_starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
                            row_ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
                        }
                    }
                    Some(_) => {}
                }
                rows_scanned += 1;
            }
        }

        if let Some(subject_id) = current_subject {
            if let Some(completed) = last_completed_subject {
                if subject_id <= completed {
                    Err(vortex_error::vortex_err!(
                        "final subject ID {} is not greater than completed ID {}; SPO ordering is required",
                        subject_id,
                        completed
                    ))?;
                }
            }
            subject_ids.push(subject_id);
            row_starts.push(current_start);
            row_ends.push(rows_scanned);
            ranges_written += 1;
        }

        if !subject_ids.is_empty() {
            yield build_subject_range_array(subject_ids, row_starts, row_ends)
                .map_err(rdf_err_to_vortex_err)?;
        } else if rows_scanned == 0 {
            yield empty_subject_range_array().map_err(rdf_err_to_vortex_err)?;
        }

        store_subject_range_build_state(
            stream_state.as_ref(),
            NativeSubjectRangeBuildState {
                rows_scanned,
                ranges_written,
                batches,
                max_batch_rows,
            },
        )?;
    };

    let dtype = empty_subject_range_array()?.dtype().clone();
    let output_stream = ArrayStreamAdapter::new(dtype, output_arrays);
    let mut output_file = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();

    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output_file, output_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(output_file);
    std::fs::rename(&temporary_path, &output_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;

    let scan_ms = elapsed_ms(scan_start);
    let state = shared_state
        .lock()
        .map_err(|_| {
            VortexRdfError::Serialization(
                "subject range build-state mutex was poisoned".to_string(),
            )
        })?
        .clone();
    let total_ms = elapsed_ms(total_start);

    log::info!(
        "[cottas_native_ids] wrote direct Vortex subject index {:?}: rows={}, ranges={}, batches={}, max_batch_rows={}, total_ms={:.3}",
        output_path,
        state.rows_scanned,
        state.ranges_written,
        state.batches,
        state.max_batch_rows,
        total_ms
    );

    Ok(NativeSubjectRangeIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: output_path.display().to_string(),
        rows_scanned: state.rows_scanned,
        ranges_written: state.ranges_written,
        batches: state.batches,
        max_batch_rows: state.max_batch_rows,
        open_ms,
        scan_ms,
        write_ms: scan_ms,
        total_ms,
    })
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeTermDirectoryBuildStats {
    pub data_path: String,
    pub source_path: String,
    pub output_path: String,
    pub fence_rows: usize,
    pub dictionary_rows: u64,
    pub directory_entries: usize,
    pub open_ms: f64,
    pub scan_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}
/// Builds only the sparse lexical directory from the existing sorted
/// term-to-ID Vortex component. Triples and unrelated components are untouched.
pub async fn build_cottas_native_term_directory(
    data_path: &Path,
    fence_rows: usize,
) -> Result<NativeTermDirectoryBuildStats> {
    let total_start = Instant::now();
    if fence_rows == 0 {
        return Err(VortexRdfError::InvalidOperation(
            "fence_rows must be positive".into(),
        ));
    }
    let source_path = require_vortex_component(
        data_path,
        NativeComponent::DictionaryTermToIdVortex,
        "term-to-ID dictionary",
    )?;
    let output_path = native_dict_term_directory_path(data_path);
    let temporary_path = output_path.with_extension("vortex.tmp");
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&source_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let (mut first, mut last, mut starts, mut ends) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut rows = 0u64;
    let mut fence_first: Option<String> = None;
    let mut fence_last: Option<String> = None;
    let mut fence_start = 0u64;
    let mut fence_len = 0usize;
    let mut previous: Option<String> = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(VortexRdfError::from)?;
        let terms = extract_projected_utf8_column(&batch, "term")?;
        if terms.len() != batch.len() {
            return Err(VortexRdfError::Deserialization(
                "term directory source length mismatch".into(),
            ));
        }
        for term in terms {
            if previous.as_ref().is_some_and(|p| p >= &term) {
                return Err(VortexRdfError::Deserialization(format!(
                    "term-to-ID dictionary is not strictly sorted near row {rows}"
                )));
            }
            if fence_first.is_none() {
                fence_start = rows;
                fence_first = Some(term.clone());
            }
            fence_last = Some(term.clone());
            previous = Some(term);
            fence_len += 1;
            rows = rows
                .checked_add(1)
                .ok_or_else(|| VortexRdfError::Serialization("row overflow".into()))?;
            if fence_len == fence_rows {
                first.push(fence_first.take().unwrap());
                last.push(fence_last.take().unwrap());
                starts.push(fence_start);
                ends.push(rows);
                fence_len = 0;
            }
        }
    }
    if fence_len != 0 {
        first.push(fence_first.take().unwrap());
        last.push(fence_last.take().unwrap());
        starts.push(fence_start);
        ends.push(rows);
    }
    let scan_ms = elapsed_ms(scan_start);
    let directory_entries = first.len();
    let array = build_native_term_directory_array(first, last, starts, ends)?;
    let dtype = build_native_term_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let arrays = ArrayStreamAdapter::new(dtype, futures::stream::iter(vec![Ok(array)]));
    let write_start = Instant::now();
    let mut output = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(directory_entries.max(1).min(65_536))
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    if let Err(error) = NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output, arrays)
        .await
    {
        drop(output);
        let _ = std::fs::remove_file(&temporary_path);
        return Err(VortexRdfError::from(error));
    }
    output
        .sync_all()
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    drop(output);
    std::fs::rename(&temporary_path, &output_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    #[cfg(feature = "legacy-sidecars")]
    invalidate_term_directory_cache(&output_path)?;
    let write_ms = elapsed_ms(write_start);
    Ok(NativeTermDirectoryBuildStats {
        data_path: data_path.display().to_string(),
        source_path: source_path.display().to_string(),
        output_path: output_path.display().to_string(),
        fence_rows,
        dictionary_rows: rows,
        directory_entries,
        open_ms,
        scan_ms,
        write_ms,
        total_ms: elapsed_ms(total_start),
    })
}
// VORTEX_RDF_ID_TO_TERM_LAYOUT_EXPERIMENTS_V1
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeIdToTermRewriteStats {
    pub source_path: String,
    pub output_path: String,
    pub row_group_size: usize,
    pub compression_profile: String,
    pub rows_read: u64,
    pub rows_written: u64,
    pub source_bytes: u64,
    pub output_bytes: u64,
    pub open_ms: f64,
    pub write_ms: f64,
    pub validate_ms: f64,
    pub total_ms: f64,
}
/// Rewrites only the existing ID-to-term Vortex component. No triples, indexes,
/// term-to-ID data, or non-Vortex runtime files are created.
pub async fn rewrite_cottas_native_id_to_term_dictionary(
    data_path: &Path,
    output_path: &Path,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<NativeIdToTermRewriteStats> {
    let total_start = Instant::now();
    if row_group_size == 0 {
        return Err(VortexRdfError::InvalidOperation(
            "row_group_size must be positive".into(),
        ));
    }
    if output_path.extension().is_none_or(|ext| ext != "vortex") {
        return Err(VortexRdfError::InvalidOperation(
            "candidate output must end in .vortex".into(),
        ));
    }
    let source_path = native_component_path(data_path, NativeComponent::DictionaryVortex);
    if !source_path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "source ID-to-term dictionary is missing at {:?}",
            source_path
        )));
    }
    if output_path == data_path || output_path == source_path {
        return Err(VortexRdfError::InvalidOperation(
            "candidate output must not overwrite the artifact or production dictionary".into(),
        ));
    }
    if output_path.exists() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "candidate output already exists: {:?}",
            output_path
        )));
    }
    let temporary_path = PathBuf::from(format!("{}.tmp", output_path.display()));
    if temporary_path.exists() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "temporary candidate already exists: {:?}",
            temporary_path
        )));
    }
    let source_bytes = std::fs::metadata(&source_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?
        .len();
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&source_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let input = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let shared_rows = Arc::new(Mutex::new(0u64));
    let stream_rows = Arc::clone(&shared_rows);
    let arrays = async_stream::try_stream! {
            let mut input=Box::pin(input); let mut ids=Vec::with_capacity(row_group_size); let mut terms=Vec::with_capacity(row_group_size); let mut expected=0u64;
            while let Some(batch)=input.next().await {
                let batch=batch?;
                let batch_ids=extract_projected_u32_column(&batch,"id").map_err(rdf_err_to_vortex_err)?;
                let batch_terms=extract_projected_utf8_column(&batch,"term").map_err(rdf_err_to_vortex_err)?;
                if batch_ids.len()!=batch_terms.len() || batch_ids.len()!=batch.len() { Err(vortex_error::vortex_err!("source dictionary column mismatch"))?; }
                for (id,term) in batch_ids.into_iter().zip(batch_terms) {
                    if u64::from(id)!=expected { Err(vortex_error::vortex_err!("source dictionary row/ID invariant failed at row {}: ID {}",expected,id))?; }
                    ids.push(id); terms.push(term); expected+=1;
                    if ids.len()==row_group_size { yield build_native_dictionary_array(std::mem::take(&mut ids),std::mem::take(&mut terms)).map_err(rdf_err_to_vortex_err)?; ids=Vec::with_capacity(row_group_size); terms=Vec::with_capacity(row_group_size); }
                }
            }
            if !ids.is_empty() { yield build_native_dictionary_array(ids,terms).map_err(rdf_err_to_vortex_err)?; }
    store_rewritten_dictionary_row_count(
        stream_rows.as_ref(),
        expected,
    )?;
        };
    let dtype = empty_native_dictionary_array()?.dtype().clone();
    let output_stream = ArrayStreamAdapter::new(dtype, arrays);
    let strategy = match compression_profile {
        CottasVortexCompressionProfile::Balanced => WriteStrategyBuilder::default()
            .with_row_block_size(row_group_size)
            .build(),
        CottasVortexCompressionProfile::Compact => WriteStrategyBuilder::default()
            .with_row_block_size(row_group_size)
            .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
            .build(),
    };
    let write_start = Instant::now();
    let mut output = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    if let Err(error) = NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output, output_stream)
        .await
    {
        drop(output);
        let _ = std::fs::remove_file(&temporary_path);
        return Err(VortexRdfError::from(error));
    }
    output
        .sync_all()
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    drop(output);
    let rows_read = *shared_rows
        .lock()
        .map_err(|_| VortexRdfError::Serialization("rewrite row counter mutex poisoned".into()))?;
    let write_ms = elapsed_ms(write_start);
    let validate_start = Instant::now();
    let candidate = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&temporary_path)
        .await
        .map_err(VortexRdfError::from)?;
    let check = candidate
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    let check_ids = extract_projected_u32_column(&check, "id")?;
    if check_ids.len() as u64 != rows_read
        || check_ids
            .iter()
            .enumerate()
            .any(|(row, id)| usize::try_from(*id).ok() != Some(row))
    {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(VortexRdfError::Deserialization(
            "candidate dictionary failed row/ID validation".into(),
        ));
    }
    let validate_ms = elapsed_ms(validate_start);
    std::fs::rename(&temporary_path, output_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let output_bytes = std::fs::metadata(output_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?
        .len();
    Ok(NativeIdToTermRewriteStats {
        source_path: source_path.display().to_string(),
        output_path: output_path.display().to_string(),
        row_group_size,
        compression_profile: match compression_profile {
            CottasVortexCompressionProfile::Balanced => "balanced",
            CottasVortexCompressionProfile::Compact => "compact",
        }
        .into(),
        rows_read,
        rows_written: rows_read,
        source_bytes,
        output_bytes,
        open_ms,
        write_ms,
        validate_ms,
        total_ms: elapsed_ms(total_start),
    })
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeDictionaryRebuildStats {
    pub data_path: String,
    pub source_path: String,
    pub output_path: String,
    pub terms_read: u64,
    pub temporary_runs: usize,
    pub row_group_size: usize,
    pub scan_ms: f64,
    pub sort_spill_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}
/// Rebuilds only the lexicographically ordered Vortex term-to-ID dictionary.
///
/// The triple artifact, ID-to-term dictionary, and all native indexes remain
/// untouched. Temporary files are private external-sort runs and are deleted
/// when this function returns; they are not runtime components.
pub async fn rebuild_cottas_native_term_dictionary(
    data_path: &Path,
    row_group_size: usize,
) -> Result<NativeDictionaryRebuildStats> {
    let total_start = Instant::now();
    let row_group_size = row_group_size.max(1);
    let source_path = native_dict_path(data_path);
    let output_path = native_dict_term_to_id_path(data_path);
    if !source_path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "cannot rebuild term dictionary: ID-to-term Vortex component is missing at {:?}",
            source_path
        )));
    }

    let sort_batch_size = std::env::var("VORTEX_RDF_TERM_DICT_REBUILD_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(500_000)
        .max(1);
    let temp_dir =
        tempfile::tempdir().map_err(|error| VortexRdfError::Serialization(error.to_string()))?;

    let scan_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&source_path)
        .await
        .map_err(VortexRdfError::from)?;
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let open_and_scan_build_ms = elapsed_ms(scan_start);

    let spill_start = Instant::now();
    let mut batch = Vec::with_capacity(sort_batch_size);
    let mut term_run_paths = Vec::new();
    let mut terms_read = 0u64;
    while let Some(batch_result) = stream.next().await {
        let array = batch_result.map_err(VortexRdfError::from)?;
        let ids = extract_projected_u32_column(&array, "id")?;
        let terms = extract_projected_utf8_column(&array, "term")?;
        if ids.len() != array.len() || terms.len() != array.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "ID-to-term dictionary projection mismatch: rows={}, ids={}, terms={}",
                array.len(),
                ids.len(),
                terms.len()
            )));
        }
        for (id, term) in ids.into_iter().zip(terms) {
            batch.push(NativeDictPair { id, term });
            terms_read += 1;
            if batch.len() >= sort_batch_size {
                batch.sort_by(|left, right| {
                    left.term
                        .cmp(&right.term)
                        .then_with(|| left.id.cmp(&right.id))
                });
                let path = temp_dir.path().join(format!(
                    "term_dictionary_rebuild_{:06}.tsv",
                    term_run_paths.len()
                ));
                write_pair_run(&path, &batch)?;
                term_run_paths.push(path);
                batch.clear();
            }
        }
    }
    let scan_ms = open_and_scan_build_ms + elapsed_ms(spill_start);
    if !batch.is_empty() {
        batch.sort_by(|left, right| {
            left.term
                .cmp(&right.term)
                .then_with(|| left.id.cmp(&right.id))
        });
        let path = temp_dir.path().join(format!(
            "term_dictionary_rebuild_{:06}.tsv",
            term_run_paths.len()
        ));
        write_pair_run(&path, &batch)?;
        term_run_paths.push(path);
    }
    let sort_spill_ms = elapsed_ms(spill_start);

    let write_start = Instant::now();
    write_native_dictionary_component(
        &output_path,
        &term_run_paths,
        PairRunOrder::Term,
        row_group_size,
        CottasVortexCompressionProfile::Compact,
    )
    .await?;
    let write_ms = elapsed_ms(write_start);
    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] rebuilt only {:?}: terms={}, runs={}, row_group_size={}, total_ms={:.3}",
        output_path,
        terms_read,
        term_run_paths.len(),
        row_group_size,
        total_ms
    );
    Ok(NativeDictionaryRebuildStats {
        data_path: data_path.display().to_string(),
        source_path: source_path.display().to_string(),
        output_path: output_path.display().to_string(),
        terms_read,
        temporary_runs: term_run_paths.len(),
        row_group_size,
        scan_ms,
        sort_spill_ms,
        write_ms,
        total_ms,
    })
}
#[derive(Clone, Debug, Serialize)]
pub struct NativeTermWindowTrial {
    pub strategy: String,
    pub window_rows: usize,
    pub row_start: u64,
    pub row_end: u64,
    pub run: usize,
    pub open_ms: f64,
    pub scan_build_ms: f64,
    pub read_all_ms: f64,
    pub extract_ms: f64,
    pub total_ms: f64,
    pub result_rows: usize,
    pub found_id: Option<u32>,
}
#[derive(Clone, Debug, Serialize)]
pub struct NativeTermWindowDiagnostics {
    pub term: String,
    pub term_preview: String,
    pub dictionary_rows: usize,
    pub discovered_row: u64,
    pub expected_id: u32,
    pub discovery_open_ms: f64,
    pub discovery_read_ms: f64,
    pub discovery_extract_ms: f64,
    pub trials: Vec<NativeTermWindowTrial>,
}
/// Diagnostic-only feasibility test for a sparse lexical term directory.
///
/// Discovery intentionally scans the sorted term_to_id dictionary once and is
/// reported separately. Timed trials then compare the current full-layout
/// equality scan with exact row windows around the discovered lexical row.
/// This function does not alter production lookup routing or persist metadata.
pub async fn diagnose_cottas_native_term_windows(
    data_path: &Path,
    term: &str,
    window_sizes: &[usize],
    runs: usize,
) -> Result<NativeTermWindowDiagnostics> {
    if runs == 0 {
        return Err(VortexRdfError::InvalidOperation(
            "term-window diagnostics require at least one run".into(),
        ));
    }
    if window_sizes.is_empty() || window_sizes.iter().any(|size| *size == 0) {
        return Err(VortexRdfError::InvalidOperation(
            "term-window diagnostics require non-zero window sizes".into(),
        ));
    }

    let path = native_dict_term_to_id_path(data_path);
    if !path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Vortex term_to_id dictionary component is missing at {:?}",
            path
        )));
    }

    let discovery_open_start = Instant::now();
    let discovery_file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let discovery_open_ms = elapsed_ms(discovery_open_start);
    let discovery_read_start = Instant::now();
    let dictionary = discovery_file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["term", "id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    let discovery_read_ms = elapsed_ms(discovery_read_start);
    let discovery_extract_start = Instant::now();
    let terms = extract_projected_utf8_column(&dictionary, "term")?;
    let ids = extract_projected_u32_column(&dictionary, "id")?;
    if terms.len() != ids.len() || terms.len() != dictionary.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "term-window discovery column mismatch: rows={}, terms={}, ids={}",
            dictionary.len(),
            terms.len(),
            ids.len()
        )));
    }
    if terms.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(VortexRdfError::Deserialization(
            "term_to_id dictionary is not strictly lexically sorted".into(),
        ));
    }
    let row = terms
        .binary_search_by(|candidate| candidate.as_str().cmp(term))
        .map_err(|_| {
            VortexRdfError::InvalidOperation(format!(
                "diagnostic term {:?} does not exist in the term_to_id dictionary",
                term
            ))
        })?;
    let expected_id = ids[row];
    let dictionary_rows = terms.len();
    let discovery_extract_ms = elapsed_ms(discovery_extract_start);
    drop(dictionary);
    drop(discovery_file);

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let shared_open_ms = elapsed_ms(open_start);
    let mut trials = Vec::with_capacity(runs.saturating_mul(window_sizes.len() + 1));
    macro_rules! run_trial {
        ($range:expr, $window_rows:expr, $run:expr, $open_ms:expr, $strategy:expr) => {{
            let total_start = Instant::now();
            let scan_start = Instant::now();
            let scan = file.scan().map_err(VortexRdfError::from)?;
            let scan = match $range.clone() {
                Some(range) => scan.with_row_range(range),
                None => scan,
            };
            let stream = scan
                .with_filter(eq(col("term"), lit(term)))
                .with_projection(vortex_array::expr::select(
                    ["id"],
                    vortex_array::expr::root(),
                ))
                .into_array_stream()
                .map_err(VortexRdfError::from)?;
            let scan_build_ms = elapsed_ms(scan_start);
            let read_start = Instant::now();
            let result = stream.read_all().await.map_err(VortexRdfError::from)?;
            let read_all_ms = elapsed_ms(read_start);
            if result.len() > 1 {
                return Err(VortexRdfError::Deserialization(format!(
                    "term-window diagnostic returned {} IDs for exact term {:?}",
                    result.len(),
                    term
                )));
            }
            let extract_start = Instant::now();
            let found_id = extract_first_u32_from_single_column_array(&result, "id")?;
            let extract_ms = elapsed_ms(extract_start);
            let (row_start, row_end) = $range
                .as_ref()
                .map(|range: &Range<u64>| (range.start, range.end))
                .unwrap_or((0, 0));
            NativeTermWindowTrial {
                strategy: $strategy.to_string(),
                window_rows: $window_rows,
                row_start,
                row_end,
                run: $run,
                open_ms: $open_ms,
                scan_build_ms,
                read_all_ms,
                extract_ms,
                total_ms: elapsed_ms(total_start) + $open_ms,
                result_rows: result.len(),
                found_id,
            }
        }};
    }
    for run in 0..runs {
        let baseline_range: Option<Range<u64>> = None;
        let baseline = run_trial!(
            baseline_range,
            dictionary_rows,
            run,
            if run == 0 { shared_open_ms } else { 0.0 },
            "full-layout-equality"
        );
        if baseline.found_id != Some(expected_id) {
            return Err(VortexRdfError::Deserialization(format!(
                "full-layout diagnostic returned {:?}; expected ID {}",
                baseline.found_id, expected_id
            )));
        }
        trials.push(baseline);

        for &window_rows in window_sizes {
            let half = window_rows / 2;
            let mut start = row.saturating_sub(half);
            let end = start.saturating_add(window_rows).min(dictionary_rows);
            start = end.saturating_sub(window_rows).min(start);
            if !(start <= row && row < end) {
                return Err(VortexRdfError::InvalidOperation(format!(
                    "computed diagnostic window {}..{} does not contain row {}",
                    start, end, row
                )));
            }
            let window_range = Some(start as u64..end as u64);
            let window = run_trial!(
                window_range,
                end - start,
                run,
                0.0,
                "known-window-row-range"
            );
            if window.found_id != Some(expected_id) {
                return Err(VortexRdfError::Deserialization(format!(
                    "window {}..{} returned {:?}; expected ID {}",
                    start, end, window.found_id, expected_id
                )));
            }
            trials.push(window);
        }
    }

    Ok(NativeTermWindowDiagnostics {
        term: term.to_string(),
        term_preview: native_term_preview(term),
        dictionary_rows,
        discovered_row: row as u64,
        expected_id,
        discovery_open_ms,
        discovery_read_ms,
        discovery_extract_ms,
        trials,
    })
}

//! Predicate exact-range index boundary.
//!
//! The first slice owns the stable spill representation and external-sort run
//! mechanics. Preparation and component sources follow once this boundary is
//! compiled and protected by indexed-vs-baseline tests.

use crate::error::{Result, VortexRdfError};
use crate::io::native_rdf_store::exact_ranges::{
    build_exact_range_directory_array, build_exact_range_payload_array,
};
use crate::io::vortex_rdf_store_layout::{
    NativeComponentSource, NativeComponentWrite, StoreComponentDescriptor, StoreComponentRole,
};
use futures::stream;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use vortex_array::ArrayRef;
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_error::VortexResult;
use vortex_file::WriteStrategyBuilder;
use vortex_layout::LayoutStrategy;

// VORTEX_RDF_NATIVE_PREDICATE_INDEX_BOUNDARY_V1
pub(crate) const PREDICATE_RANGE_RECORD_BYTES: usize = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PredicateRangeRecord {
    pub(crate) predicate_id: u32,
    pub(crate) row_start: u64,
    pub(crate) row_end: u64,
}
impl Ord for PredicateRangeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.predicate_id
            .cmp(&other.predicate_id)
            .then_with(|| self.row_start.cmp(&other.row_start))
            .then_with(|| self.row_end.cmp(&other.row_end))
    }
}
impl PartialOrd for PredicateRangeRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PredicateRangeRecord {
    fn encode(self) -> [u8; PREDICATE_RANGE_RECORD_BYTES] {
        let mut bytes = [0; PREDICATE_RANGE_RECORD_BYTES];
        bytes[0..4].copy_from_slice(&self.predicate_id.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.row_start.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.row_end.to_le_bytes());
        bytes
    }
    fn decode(bytes: [u8; PREDICATE_RANGE_RECORD_BYTES]) -> Self {
        Self {
            predicate_id: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            row_start: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
            row_end: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        }
    }
}
pub(crate) fn write_predicate_range_record<W: Write>(
    writer: &mut W,
    value: PredicateRangeRecord,
) -> Result<()> {
    writer
        .write_all(&value.encode())
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))
}
pub(crate) struct PredicateRangeRunReader {
    reader: BufReader<std::fs::File>,
}
impl PredicateRangeRunReader {
    pub(crate) fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(
                std::fs::File::open(path)
                    .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
            ),
        })
    }
    pub(crate) fn read_one(&mut self) -> Result<Option<PredicateRangeRecord>> {
        let mut bytes = [0; PREDICATE_RANGE_RECORD_BYTES];
        match self.reader.read_exact(&mut bytes) {
            Ok(()) => Ok(Some(PredicateRangeRecord::decode(bytes))),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(VortexRdfError::Serialization(e.to_string())),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PredicateRangeHeapItem {
    pub(crate) value: PredicateRangeRecord,
    pub(crate) run_idx: usize,
}
impl Ord for PredicateRangeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .value
            .cmp(&self.value)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl PartialOrd for PredicateRangeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
pub(crate) fn flush_predicate_range_run(
    records: &mut Vec<PredicateRangeRecord>,
    temp_dir: &Path,
    run_idx: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    records.sort_unstable();
    let path = temp_dir.join(format!("predicate_range_run_{run_idx:06}.bin"));
    let mut writer = BufWriter::new(
        std::fs::File::create(&path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    for value in records.drain(..) {
        write_predicate_range_record(&mut writer, value)?
    }
    writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    runs.push(path);
    Ok(())
}

// VORTEX_RDF_NATIVE_PREDICATE_PREPARATION_V1
#[derive(Clone)]
pub(crate) struct PreparedPredicateExactRanges {
    pub(crate) payload_path: PathBuf,
    pub(crate) predicate_ids: std::sync::Arc<[u32]>,
    pub(crate) range_offsets: std::sync::Arc<[u64]>,
    pub(crate) range_counts: std::sync::Arc<[u32]>,
    pub(crate) candidate_rows: std::sync::Arc<[u64]>,
}

pub(crate) struct PredicateRangeCollector {
    temp_dir: PathBuf,
    batch_size: usize,
    records: Vec<PredicateRangeRecord>,
    runs: Vec<PathBuf>,
    row: u64,
    active_predicate: Option<u32>,
    active_start: u64,
}
impl PredicateRangeCollector {
    pub(crate) fn new(temp_dir: &Path) -> Self {
        let batch_size = std::env::var("VORTEX_RDF_P_V2_SORT_BATCH_RANGES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000)
            .max(1);
        Self {
            temp_dir: temp_dir.to_path_buf(),
            batch_size,
            records: Vec::with_capacity(batch_size),
            runs: Vec::new(),
            row: 0,
            active_predicate: None,
            active_start: 0,
        }
    }
    pub(crate) fn push_predicate(&mut self, predicate_id: u32) -> Result<()> {
        match self.active_predicate {
            None => {
                self.active_predicate = Some(predicate_id);
                self.active_start = self.row;
            }
            Some(previous) if previous != predicate_id => {
                self.records.push(PredicateRangeRecord {
                    predicate_id: previous,
                    row_start: self.active_start,
                    row_end: self.row,
                });
                self.active_predicate = Some(predicate_id);
                self.active_start = self.row;
                if self.records.len() >= self.batch_size {
                    self.flush()?;
                }
            }
            Some(_) => {}
        }
        self.row = self.row.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("predicate exact v2 row overflow".into())
        })?;
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        let index = self.runs.len();
        flush_predicate_range_run(&mut self.records, &self.temp_dir, index, &mut self.runs)
    }
    pub(crate) fn finish(mut self) -> Result<PreparedPredicateExactRanges> {
        if let Some(predicate_id) = self.active_predicate {
            self.records.push(PredicateRangeRecord {
                predicate_id,
                row_start: self.active_start,
                row_end: self.row,
            });
        }
        self.flush()?;
        merge_predicate_runs(&self.runs, &self.temp_dir)
    }
}
fn merge_predicate_runs(runs: &[PathBuf], temp_dir: &Path) -> Result<PreparedPredicateExactRanges> {
    let payload_path = temp_dir.join("native_predicate_exact_v2_payload.bin");
    let mut payload = BufWriter::new(
        std::fs::File::create(&payload_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    let mut readers = runs
        .iter()
        .map(|p| PredicateRangeRunReader::new(p))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = reader.read_one()? {
            heap.push(PredicateRangeHeapItem { value, run_idx });
        }
    }
    let (mut ids, mut offsets, mut counts, mut rows) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut payload_rows, mut current, mut count, mut candidate_rows, mut previous_end) =
        (0u64, None, 0u32, 0u64, None);
    while let Some(item) = heap.pop() {
        let value = item.value;
        if current != Some(value.predicate_id) {
            if let Some(id) = current {
                ids.push(id);
                offsets.push(payload_rows - u64::from(count));
                counts.push(count);
                rows.push(candidate_rows);
            }
            current = Some(value.predicate_id);
            count = 0;
            candidate_rows = 0;
            previous_end = None;
        }
        if value.row_start >= value.row_end || previous_end.is_some_and(|end| value.row_start < end)
        {
            return Err(VortexRdfError::Serialization(
                "predicate exact v2 contains an invalid or overlapping range".into(),
            ));
        }
        write_predicate_range_record(&mut payload, value)?;
        count = count.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("predicate exact v2 range-count overflow".into())
        })?;
        candidate_rows = candidate_rows
            .checked_add(value.row_end - value.row_start)
            .ok_or_else(|| {
                VortexRdfError::Serialization("predicate exact v2 candidate-row overflow".into())
            })?;
        payload_rows = payload_rows.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("predicate exact v2 payload-offset overflow".into())
        })?;
        previous_end = Some(value.row_end);
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(PredicateRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some(id) = current {
        ids.push(id);
        offsets.push(payload_rows - u64::from(count));
        counts.push(count);
        rows.push(candidate_rows);
    }
    payload
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    Ok(PreparedPredicateExactRanges {
        payload_path,
        predicate_ids: ids.into(),
        range_offsets: offsets.into(),
        range_counts: counts.into(),
        candidate_rows: rows.into(),
    })
}

// VORTEX_RDF_NATIVE_PREDICATE_COMPONENT_SOURCES_V1
fn predicate_directory_array(prepared: &PreparedPredicateExactRanges) -> Result<ArrayRef> {
    build_exact_range_directory_array(
        "predicate_id",
        prepared.predicate_ids.to_vec(),
        prepared.range_offsets.to_vec(),
        prepared.range_counts.to_vec(),
        prepared.candidate_rows.to_vec(),
    )
}

#[derive(Clone)]
struct PredicateDirectorySource {
    prepared: Arc<PreparedPredicateExactRanges>,
    dtype: vortex_array::dtype::DType,
}
impl PredicateDirectorySource {
    fn new(prepared: Arc<PreparedPredicateExactRanges>) -> Result<Self> {
        let dtype =
            build_exact_range_directory_array("predicate_id", vec![], vec![], vec![], vec![])?
                .dtype()
                .clone();
        Ok(Self { prepared, dtype })
    }
}
impl NativeComponentSource for PredicateDirectorySource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let array = predicate_directory_array(&self.prepared)
            .map_err(|error| vortex_error::vortex_err!("{}", error))?;
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream::iter(vec![Ok(array)]),
        )))
    }
}

#[derive(Clone)]
struct PredicatePayloadSource {
    path: PathBuf,
    batch_size: usize,
    dtype: vortex_array::dtype::DType,
}
impl PredicatePayloadSource {
    fn new(path: PathBuf, batch_size: usize) -> Result<Self> {
        Ok(Self {
            path,
            batch_size: batch_size.max(1),
            dtype: build_exact_range_payload_array(vec![], vec![])?
                .dtype()
                .clone(),
        })
    }
}
impl NativeComponentSource for PredicatePayloadSource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let path = self.path.clone();
        let batch_size = self.batch_size;
        let arrays = async_stream::try_stream! {
            let mut reader = PredicateRangeRunReader::new(&path)
                .map_err(|error| vortex_error::vortex_err!("{}", error))?;
            loop {
                let (mut starts, mut ends) = (
                    Vec::with_capacity(batch_size),
                    Vec::with_capacity(batch_size),
                );
                while starts.len() < batch_size {
                    let Some(value) = reader.read_one()
                        .map_err(|error| vortex_error::vortex_err!("{}", error))?
                    else { break; };
                    starts.push(value.row_start);
                    ends.push(value.row_end);
                }
                if starts.is_empty() { break; }
                yield build_exact_range_payload_array(starts, ends)
                    .map_err(|error| vortex_error::vortex_err!("{}", error))?;
            }
        };
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            arrays,
        )))
    }
}

pub(crate) fn predicate_component_writes(
    prepared: Arc<PreparedPredicateExactRanges>,
    row_group_size: usize,
) -> Result<Vec<NativeComponentWrite>> {
    let directory = Arc::new(PredicateDirectorySource::new(Arc::clone(&prepared))?);
    let payload = Arc::new(PredicatePayloadSource::new(
        prepared.payload_path.clone(),
        row_group_size,
    )?);
    let strategy: Arc<dyn LayoutStrategy> = WriteStrategyBuilder::default()
        .with_row_block_size(row_group_size.max(1))
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    Ok(vec![
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "index.predicate.exact-ranges.directory".into(),
                role: StoreComponentRole::Index,
                implementation: "native-predicate-exact-directory-v2-compact".into(),
                version: 2,
                required: false,
                dtype: directory.dtype().clone(),
            },
            directory,
            Arc::clone(&strategy),
        )?,
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "index.predicate.exact-ranges.payload".into(),
                role: StoreComponentRole::Index,
                implementation: "native-predicate-exact-payload-v2-compact".into(),
                version: 2,
                required: false,
                dtype: payload.dtype().clone(),
            },
            payload,
            strategy,
        )?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spill_codec_is_stable() {
        let value = PredicateRangeRecord {
            predicate_id: 7,
            row_start: 11,
            row_end: 19,
        };
        let bytes = value.encode();
        assert_eq!(bytes.len(), 20);
        assert_eq!(PredicateRangeRecord::decode(bytes), value);
    }
    #[test]
    fn empty_flush_is_noop() {
        let temp = tempfile::tempdir().unwrap();
        let mut records = vec![];
        let mut runs = vec![];
        flush_predicate_range_run(&mut records, temp.path(), 0, &mut runs).unwrap();
        assert!(runs.is_empty());
    }
}

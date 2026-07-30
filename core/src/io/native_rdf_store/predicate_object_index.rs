//! Predicate-object exact-range index boundary.
//!
//! Owns the stable 24-byte spill codec and external-sort run mechanics.

use crate::error::{Result, VortexRdfError};
use crate::io::native_rdf_store::exact_ranges::build_exact_range_payload_array;
use crate::io::vortex_rdf_store_layout::{
    NativeComponentSource, NativeComponentWrite, StoreComponentDescriptor, StoreComponentRole,
};
use futures::stream;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_array::{ArrayRef, IntoArray};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_error::VortexResult;
use vortex_file::WriteStrategyBuilder;
use vortex_layout::LayoutStrategy;

// VORTEX_RDF_NATIVE_PREDICATE_OBJECT_INDEX_BOUNDARY_V1
pub(crate) const PO_RANGE_RECORD_BYTES: usize = 24;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PoRangeRecord {
    pub(crate) predicate_id: u32,
    pub(crate) object_id: u32,
    pub(crate) row_start: u64,
    pub(crate) row_end: u64,
}
impl Ord for PoRangeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.predicate_id,
            self.object_id,
            self.row_start,
            self.row_end,
        )
            .cmp(&(
                other.predicate_id,
                other.object_id,
                other.row_start,
                other.row_end,
            ))
    }
}
impl PartialOrd for PoRangeRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PoRangeRecord {
    fn encode(self) -> [u8; PO_RANGE_RECORD_BYTES] {
        let mut bytes = [0; PO_RANGE_RECORD_BYTES];
        bytes[0..4].copy_from_slice(&self.predicate_id.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.object_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.row_start.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.row_end.to_le_bytes());
        bytes
    }
    fn decode(bytes: [u8; PO_RANGE_RECORD_BYTES]) -> Self {
        Self {
            predicate_id: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            object_id: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            row_start: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            row_end: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        }
    }
}
pub(crate) fn write_po_range_record<W: Write>(writer: &mut W, value: PoRangeRecord) -> Result<()> {
    writer
        .write_all(&value.encode())
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))
}
pub(crate) struct PoRangeRunReader {
    reader: BufReader<std::fs::File>,
}
impl PoRangeRunReader {
    pub(crate) fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(
                std::fs::File::open(path)
                    .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
            ),
        })
    }
    pub(crate) fn read_one(&mut self) -> Result<Option<PoRangeRecord>> {
        let mut bytes = [0; PO_RANGE_RECORD_BYTES];
        match self.reader.read_exact(&mut bytes) {
            Ok(()) => Ok(Some(PoRangeRecord::decode(bytes))),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(VortexRdfError::Serialization(e.to_string())),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PoRangeHeapItem {
    pub(crate) value: PoRangeRecord,
    pub(crate) run_idx: usize,
}
impl Ord for PoRangeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .value
            .cmp(&self.value)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl PartialOrd for PoRangeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
pub(crate) fn flush_po_range_run(
    records: &mut Vec<PoRangeRecord>,
    temp_dir: &Path,
    run_idx: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    records.sort_unstable();
    let path = temp_dir.join(format!("po_range_run_{run_idx:06}.bin"));
    let mut writer = BufWriter::new(
        std::fs::File::create(&path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    for value in records.drain(..) {
        write_po_range_record(&mut writer, value)?;
    }
    writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    runs.push(path);
    Ok(())
}

// VORTEX_RDF_NATIVE_PREDICATE_OBJECT_PREPARATION_V1
#[derive(Clone)]
pub(crate) struct PreparedPoExactRanges {
    pub(crate) payload_path: PathBuf,
    pub(crate) partition_predicate_ids: Arc<[u32]>,
    pub(crate) partition_directory_starts: Arc<[u64]>,
    pub(crate) partition_directory_ends: Arc<[u64]>,
    pub(crate) directory_predicate_ids: Arc<[u32]>,
    pub(crate) directory_object_ids: Arc<[u32]>,
    pub(crate) range_offsets: Arc<[u64]>,
    pub(crate) range_counts: Arc<[u32]>,
    pub(crate) candidate_rows: Arc<[u64]>,
}

pub(crate) struct PoRangeCollector {
    temp_dir: PathBuf,
    batch_size: usize,
    records: Vec<PoRangeRecord>,
    runs: Vec<PathBuf>,
    row: u64,
    active_key: Option<(u32, u32)>,
    active_start: u64,
}
impl PoRangeCollector {
    pub(crate) fn new(temp_dir: &Path) -> Self {
        let batch_size = std::env::var("VORTEX_RDF_PO_V2_SORT_BATCH_RANGES")
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
            active_key: None,
            active_start: 0,
        }
    }
    pub(crate) fn push(&mut self, predicate_id: u32, object_id: u32) -> Result<()> {
        let key = (predicate_id, object_id);
        match self.active_key {
            None => {
                self.active_key = Some(key);
                self.active_start = self.row;
            }
            Some(previous) if previous != key => {
                self.records.push(PoRangeRecord {
                    predicate_id: previous.0,
                    object_id: previous.1,
                    row_start: self.active_start,
                    row_end: self.row,
                });
                self.active_key = Some(key);
                self.active_start = self.row;
                if self.records.len() >= self.batch_size {
                    self.flush()?;
                }
            }
            Some(_) => {}
        }
        self.row = self.row.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("predicate-object exact v2 row overflow".into())
        })?;
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        let index = self.runs.len();
        flush_po_range_run(&mut self.records, &self.temp_dir, index, &mut self.runs)
    }
    pub(crate) fn finish(mut self) -> Result<PreparedPoExactRanges> {
        if let Some((predicate_id, object_id)) = self.active_key {
            self.records.push(PoRangeRecord {
                predicate_id,
                object_id,
                row_start: self.active_start,
                row_end: self.row,
            });
        }
        self.flush()?;
        merge_po_runs(&self.runs, &self.temp_dir)
    }
}
fn merge_po_runs(runs: &[PathBuf], temp_dir: &Path) -> Result<PreparedPoExactRanges> {
    let payload_path = temp_dir.join("native_predicate_object_exact_v2_payload.bin");
    let mut payload = BufWriter::new(
        std::fs::File::create(&payload_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    let mut readers = runs
        .iter()
        .map(|p| PoRangeRunReader::new(p))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = reader.read_one()? {
            heap.push(PoRangeHeapItem { value, run_idx });
        }
    }
    let (mut predicates, mut objects, mut offsets, mut counts, mut rows) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut payload_rows, mut current, mut count, mut candidate_rows, mut previous_end) =
        (0u64, None, 0u32, 0u64, None);
    while let Some(item) = heap.pop() {
        let value = item.value;
        let key = (value.predicate_id, value.object_id);
        if current != Some(key) {
            if let Some((p, o)) = current {
                predicates.push(p);
                objects.push(o);
                offsets.push(payload_rows - u64::from(count));
                counts.push(count);
                rows.push(candidate_rows);
            }
            current = Some(key);
            count = 0;
            candidate_rows = 0;
            previous_end = None;
        }
        if value.row_start >= value.row_end || previous_end.is_some_and(|end| value.row_start < end)
        {
            return Err(VortexRdfError::Serialization(
                "predicate-object exact v2 contains an invalid or overlapping range".into(),
            ));
        }
        write_po_range_record(&mut payload, value)?;
        count = count.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("predicate-object exact v2 range-count overflow".into())
        })?;
        candidate_rows = candidate_rows
            .checked_add(value.row_end - value.row_start)
            .ok_or_else(|| {
                VortexRdfError::Serialization(
                    "predicate-object exact v2 candidate-row overflow".into(),
                )
            })?;
        payload_rows = payload_rows.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization(
                "predicate-object exact v2 payload-offset overflow".into(),
            )
        })?;
        previous_end = Some(value.row_end);
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(PoRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some((p, o)) = current {
        predicates.push(p);
        objects.push(o);
        offsets.push(payload_rows - u64::from(count));
        counts.push(count);
        rows.push(candidate_rows);
    }
    payload
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let (mut partition_predicates, mut starts, mut ends) = (Vec::new(), Vec::new(), Vec::new());
    let mut start = 0usize;
    while start < predicates.len() {
        let predicate_id = predicates[start];
        let mut end = start + 1;
        while end < predicates.len() && predicates[end] == predicate_id {
            end += 1;
        }
        partition_predicates.push(predicate_id);
        starts.push(
            u64::try_from(start).map_err(|_| {
                VortexRdfError::Serialization("PO directory start exceeds u64".into())
            })?,
        );
        ends.push(
            u64::try_from(end).map_err(|_| {
                VortexRdfError::Serialization("PO directory end exceeds u64".into())
            })?,
        );
        start = end;
    }
    if starts.first().copied().unwrap_or(0) != 0
        || ends.last().copied().unwrap_or(0) != predicates.len() as u64
        || ends
            .windows(2)
            .zip(starts.iter().skip(1))
            .any(|(e, s)| e[0] != *s)
    {
        return Err(VortexRdfError::Serialization(
            "predicate-object exact v2 partitions are not contiguous".into(),
        ));
    }
    Ok(PreparedPoExactRanges {
        payload_path,
        partition_predicate_ids: partition_predicates.into(),
        partition_directory_starts: starts.into(),
        partition_directory_ends: ends.into(),
        directory_predicate_ids: predicates.into(),
        directory_object_ids: objects.into(),
        range_offsets: offsets.into(),
        range_counts: counts.into(),
        candidate_rows: rows.into(),
    })
}

// VORTEX_RDF_NATIVE_PREDICATE_OBJECT_COMPONENT_SOURCES_V1
fn partition_array(ids: Vec<u32>, starts: Vec<u64>, ends: Vec<u64>) -> Result<ArrayRef> {
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
    .map(|a| a.into_array())
}
fn directory_array(
    p: Vec<u32>,
    o: Vec<u32>,
    offsets: Vec<u64>,
    counts: Vec<u32>,
    rows: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("predicate_id", PrimitiveArray::from_iter(p).into_array()),
        ("object_id", PrimitiveArray::from_iter(o).into_array()),
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
#[derive(Clone, Copy)]
enum MetadataKind {
    Partitions,
    Directory,
}
#[derive(Clone)]
struct PoMetadataSource {
    prepared: Arc<PreparedPoExactRanges>,
    kind: MetadataKind,
    dtype: vortex_array::dtype::DType,
}
impl PoMetadataSource {
    fn partitions(prepared: Arc<PreparedPoExactRanges>) -> Result<Self> {
        Ok(Self {
            prepared,
            kind: MetadataKind::Partitions,
            dtype: partition_array(vec![], vec![], vec![])?.dtype().clone(),
        })
    }
    fn directory(prepared: Arc<PreparedPoExactRanges>) -> Result<Self> {
        Ok(Self {
            prepared,
            kind: MetadataKind::Directory,
            dtype: directory_array(vec![], vec![], vec![], vec![], vec![])?
                .dtype()
                .clone(),
        })
    }
}
impl NativeComponentSource for PoMetadataSource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let p = &self.prepared;
        let a = match self.kind {
            MetadataKind::Partitions => partition_array(
                p.partition_predicate_ids.to_vec(),
                p.partition_directory_starts.to_vec(),
                p.partition_directory_ends.to_vec(),
            ),
            MetadataKind::Directory => directory_array(
                p.directory_predicate_ids.to_vec(),
                p.directory_object_ids.to_vec(),
                p.range_offsets.to_vec(),
                p.range_counts.to_vec(),
                p.candidate_rows.to_vec(),
            ),
        }
        .map_err(|e| vortex_error::vortex_err!("{}", e))?;
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream::iter(vec![Ok(a)]),
        )))
    }
}
#[derive(Clone)]
struct PoPayloadSource {
    path: PathBuf,
    batch_size: usize,
    dtype: vortex_array::dtype::DType,
}
impl PoPayloadSource {
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
impl NativeComponentSource for PoPayloadSource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let path = self.path.clone();
        let batch = self.batch_size;
        let arrays = async_stream::try_stream! {let mut reader=PoRangeRunReader::new(&path).map_err(|e|vortex_error::vortex_err!("{}",e))?;loop{let(mut starts,mut ends)=(Vec::with_capacity(batch),Vec::with_capacity(batch));while starts.len()<batch{let Some(v)=reader.read_one().map_err(|e|vortex_error::vortex_err!("{}",e))? else{break};starts.push(v.row_start);ends.push(v.row_end);}if starts.is_empty(){break}yield build_exact_range_payload_array(starts,ends).map_err(|e|vortex_error::vortex_err!("{}",e))?;}};
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            arrays,
        )))
    }
}
pub(crate) fn po_component_writes(
    prepared: Arc<PreparedPoExactRanges>,
    row_group_size: usize,
) -> Result<Vec<NativeComponentWrite>> {
    let partitions = Arc::new(PoMetadataSource::partitions(Arc::clone(&prepared))?);
    let directory = Arc::new(PoMetadataSource::directory(Arc::clone(&prepared))?);
    let payload = Arc::new(PoPayloadSource::new(
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
                name: "index.predicate-object.predicate-partitions".into(),
                role: StoreComponentRole::Index,
                implementation: "native-predicate-object-partitions-v2-compact".into(),
                version: 2,
                required: false,
                dtype: partitions.dtype().clone(),
            },
            partitions,
            Arc::clone(&strategy),
        )?,
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "index.predicate-object.exact-ranges.directory".into(),
                role: StoreComponentRole::Index,
                implementation: "native-predicate-object-exact-directory-v2-compact".into(),
                version: 2,
                required: false,
                dtype: directory.dtype().clone(),
            },
            directory,
            Arc::clone(&strategy),
        )?,
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "index.predicate-object.exact-ranges.payload".into(),
                role: StoreComponentRole::Index,
                implementation: "native-predicate-object-exact-payload-v2-compact".into(),
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
    fn codec_is_stable() {
        let value = PoRangeRecord {
            predicate_id: 3,
            object_id: 7,
            row_start: 11,
            row_end: 19,
        };
        let bytes = value.encode();
        assert_eq!(bytes.len(), 24);
        assert_eq!(PoRangeRecord::decode(bytes), value);
    }
    #[test]
    fn empty_flush_is_noop() {
        let temp = tempfile::tempdir().unwrap();
        let mut records = vec![];
        let mut runs = vec![];
        flush_po_range_run(&mut records, temp.path(), 0, &mut runs).unwrap();
        assert!(runs.is_empty());
    }
}

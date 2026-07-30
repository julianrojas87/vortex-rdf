//! Object exact-range metadata boundary.
//!
//! SPO replay and the stable binary spill record remain in the legacy writer
//! temporarily. This module owns object-directory accumulation and its
//! contiguous payload-offset invariant, ready for the producer extraction.

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
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_error::VortexResult;
use vortex_file::WriteStrategyBuilder;
use vortex_layout::LayoutStrategy;

// VORTEX_RDF_NATIVE_OBJECT_INDEX_BOUNDARY_V1
#[derive(Clone)]
pub(crate) struct PreparedObjectExactRanges {
    pub(crate) payload_path: PathBuf,
    pub(crate) object_ids: Arc<[u32]>,
    pub(crate) range_offsets: Arc<[u64]>,
    pub(crate) range_counts: Arc<[u32]>,
    pub(crate) candidate_rows: Arc<[u64]>,
}

#[derive(Default)]
pub(crate) struct ObjectDirectoryBuilder {
    object_ids: Vec<u32>,
    range_offsets: Vec<u64>,
    range_counts: Vec<u32>,
    candidate_rows: Vec<u64>,
    payload_rows: u64,
}

impl ObjectDirectoryBuilder {
    pub(crate) fn push(
        &mut self,
        object_id: u32,
        range_count: u32,
        candidate_rows: u64,
    ) -> Result<()> {
        if range_count == 0 || candidate_rows == 0 {
            return Err(VortexRdfError::Serialization(
                "object exact v2 directory entry must reference nonempty ranges".into(),
            ));
        }
        if self
            .object_ids
            .last()
            .is_some_and(|previous| *previous >= object_id)
        {
            return Err(VortexRdfError::Serialization(
                "object exact v2 directory keys are not strictly increasing".into(),
            ));
        }
        self.object_ids.push(object_id);
        self.range_offsets.push(self.payload_rows);
        self.range_counts.push(range_count);
        self.candidate_rows.push(candidate_rows);
        self.payload_rows = self
            .payload_rows
            .checked_add(u64::from(range_count))
            .ok_or_else(|| {
                VortexRdfError::Serialization("object exact v2 payload-offset overflow".into())
            })?;
        Ok(())
    }

    pub(crate) fn finish(
        self,
        payload_path: PathBuf,
        actual_payload_rows: u64,
    ) -> Result<PreparedObjectExactRanges> {
        if self.payload_rows != actual_payload_rows {
            return Err(VortexRdfError::Serialization(format!(
                "object exact v2 payload-row mismatch: directory={}, payload={actual_payload_rows}",
                self.payload_rows
            )));
        }
        Ok(PreparedObjectExactRanges {
            payload_path,
            object_ids: self.object_ids.into(),
            range_offsets: self.range_offsets.into(),
            range_counts: self.range_counts.into(),
            candidate_rows: self.candidate_rows.into(),
        })
    }
}

// VORTEX_RDF_NATIVE_OBJECT_SPILL_CODEC_V1
pub(crate) const OBJECT_RANGE_RECORD_BYTES: usize = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObjectRangeRecord {
    pub(crate) object_id: u32,
    pub(crate) row_start: u64,
    pub(crate) row_end: u64,
}
impl Ord for ObjectRangeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.object_id
            .cmp(&other.object_id)
            .then_with(|| self.row_start.cmp(&other.row_start))
            .then_with(|| self.row_end.cmp(&other.row_end))
    }
}
impl PartialOrd for ObjectRangeRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl ObjectRangeRecord {
    fn encode(self) -> [u8; OBJECT_RANGE_RECORD_BYTES] {
        let mut bytes = [0; OBJECT_RANGE_RECORD_BYTES];
        bytes[0..4].copy_from_slice(&self.object_id.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.row_start.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.row_end.to_le_bytes());
        bytes
    }
    fn decode(bytes: [u8; OBJECT_RANGE_RECORD_BYTES]) -> Self {
        Self {
            object_id: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            row_start: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
            row_end: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        }
    }
}

pub(crate) fn write_object_range_record<W: Write>(
    writer: &mut W,
    value: ObjectRangeRecord,
) -> Result<()> {
    writer
        .write_all(&value.encode())
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))
}

pub(crate) struct ObjectRangeRunReader {
    reader: BufReader<std::fs::File>,
}
impl ObjectRangeRunReader {
    pub(crate) fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(
                std::fs::File::open(path)
                    .map_err(|error| VortexRdfError::Serialization(error.to_string()))?,
            ),
        })
    }
    pub(crate) fn read_one(&mut self) -> Result<Option<ObjectRangeRecord>> {
        let mut bytes = [0; OBJECT_RANGE_RECORD_BYTES];
        match self.reader.read_exact(&mut bytes) {
            Ok(()) => Ok(Some(ObjectRangeRecord::decode(bytes))),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(VortexRdfError::Serialization(error.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObjectRangeHeapItem {
    pub(crate) value: ObjectRangeRecord,
    pub(crate) run_idx: usize,
}
impl Ord for ObjectRangeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .value
            .cmp(&self.value)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl PartialOrd for ObjectRangeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn flush_object_range_run(
    records: &mut Vec<ObjectRangeRecord>,
    temp_dir: &Path,
    run_idx: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    records.sort_unstable();
    let path = temp_dir.join(format!("object_range_run_{run_idx:06}.bin"));
    let mut writer = BufWriter::new(
        std::fs::File::create(&path)
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?,
    );
    for value in records.drain(..) {
        write_object_range_record(&mut writer, value)?;
    }
    writer
        .flush()
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    runs.push(path);
    Ok(())
}

// VORTEX_RDF_NATIVE_OBJECT_PREPARATION_V1
pub(crate) struct ObjectRangeCollector {
    temp_dir: PathBuf,
    sort_batch: usize,
    records: Vec<ObjectRangeRecord>,
    sorted_runs: Vec<PathBuf>,
    row: u64,
    active_object: Option<u32>,
    active_start: u64,
}

impl ObjectRangeCollector {
    pub(crate) fn new(temp_dir: &Path) -> Self {
        let sort_batch = std::env::var("VORTEX_RDF_O_V2_SORT_BATCH_RANGES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1_000_000)
            .max(1);
        Self {
            temp_dir: temp_dir.to_path_buf(),
            sort_batch,
            records: Vec::with_capacity(sort_batch),
            sorted_runs: Vec::new(),
            row: 0,
            active_object: None,
            active_start: 0,
        }
    }

    pub(crate) fn push_object(&mut self, object_id: u32) -> Result<()> {
        match self.active_object {
            None => {
                self.active_object = Some(object_id);
                self.active_start = self.row;
            }
            Some(previous) if previous != object_id => {
                self.records.push(ObjectRangeRecord {
                    object_id: previous,
                    row_start: self.active_start,
                    row_end: self.row,
                });
                self.active_object = Some(object_id);
                self.active_start = self.row;
                self.flush_if_full()?;
            }
            Some(_) => {}
        }
        self.row = self
            .row
            .checked_add(1)
            .ok_or_else(|| VortexRdfError::Serialization("object exact v2 row overflow".into()))?;
        Ok(())
    }

    fn flush_if_full(&mut self) -> Result<()> {
        if self.records.len() < self.sort_batch {
            return Ok(());
        }
        self.flush()
    }

    fn flush(&mut self) -> Result<()> {
        let run_idx = self.sorted_runs.len();
        flush_object_range_run(
            &mut self.records,
            &self.temp_dir,
            run_idx,
            &mut self.sorted_runs,
        )
    }

    pub(crate) fn finish(mut self) -> Result<PreparedObjectExactRanges> {
        if let Some(object_id) = self.active_object {
            self.records.push(ObjectRangeRecord {
                object_id,
                row_start: self.active_start,
                row_end: self.row,
            });
        }
        self.flush()?;
        merge_object_range_runs(&self.sorted_runs, &self.temp_dir)
    }
}

fn merge_object_range_runs(
    run_paths: &[PathBuf],
    temp_dir: &Path,
) -> Result<PreparedObjectExactRanges> {
    let payload_path = temp_dir.join("native_object_exact_v2_payload.bin");
    let mut payload = BufWriter::new(
        std::fs::File::create(&payload_path)
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?,
    );
    let mut readers = run_paths
        .iter()
        .map(|path| ObjectRangeRunReader::new(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = reader.read_one()? {
            heap.push(ObjectRangeHeapItem { value, run_idx });
        }
    }
    let mut directory = ObjectDirectoryBuilder::default();
    let mut payload_rows = 0u64;
    let mut current_object = None;
    let mut current_count = 0u32;
    let mut current_rows = 0u64;
    let mut previous_end = None;
    while let Some(item) = heap.pop() {
        let value = item.value;
        if current_object != Some(value.object_id) {
            if let Some(object_id) = current_object {
                directory.push(object_id, current_count, current_rows)?;
            }
            current_object = Some(value.object_id);
            current_count = 0;
            current_rows = 0;
            previous_end = None;
        }
        if value.row_start >= value.row_end || previous_end.is_some_and(|end| value.row_start < end)
        {
            return Err(VortexRdfError::Serialization(
                "object exact v2 contains an invalid or overlapping range".into(),
            ));
        }
        write_object_range_record(&mut payload, value)?;
        current_count = current_count.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("object exact v2 range-count overflow".into())
        })?;
        current_rows = current_rows
            .checked_add(value.row_end - value.row_start)
            .ok_or_else(|| {
                VortexRdfError::Serialization("object exact v2 candidate-row overflow".into())
            })?;
        payload_rows = payload_rows.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("object exact v2 payload-offset overflow".into())
        })?;
        previous_end = Some(value.row_end);
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(ObjectRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some(object_id) = current_object {
        directory.push(object_id, current_count, current_rows)?;
    }
    payload
        .flush()
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    directory.finish(payload_path, payload_rows)
}

// VORTEX_RDF_NATIVE_OBJECT_COMPONENT_SOURCES_V1
#[derive(Clone)]
struct ObjectDirectorySource {
    prepared: Arc<PreparedObjectExactRanges>,
    dtype: vortex_array::dtype::DType,
}
impl ObjectDirectorySource {
    fn new(prepared: Arc<PreparedObjectExactRanges>) -> Result<Self> {
        let dtype = build_exact_range_directory_array("object_id", vec![], vec![], vec![], vec![])?
            .dtype()
            .clone();
        Ok(Self { prepared, dtype })
    }
}
impl NativeComponentSource for ObjectDirectorySource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let p = &self.prepared;
        let a = build_exact_range_directory_array(
            "object_id",
            p.object_ids.to_vec(),
            p.range_offsets.to_vec(),
            p.range_counts.to_vec(),
            p.candidate_rows.to_vec(),
        )
        .map_err(|e| vortex_error::vortex_err!("{}", e))?;
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream::iter(vec![Ok(a)]),
        )))
    }
}
#[derive(Clone)]
struct ObjectPayloadSource {
    path: PathBuf,
    batch: usize,
    dtype: vortex_array::dtype::DType,
}
impl ObjectPayloadSource {
    fn new(path: PathBuf, batch: usize) -> Result<Self> {
        Ok(Self {
            path,
            batch: batch.max(1),
            dtype: build_exact_range_payload_array(vec![], vec![])?
                .dtype()
                .clone(),
        })
    }
}
impl NativeComponentSource for ObjectPayloadSource {
    fn dtype(&self) -> &vortex_array::dtype::DType {
        &self.dtype
    }
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let path = self.path.clone();
        let batch = self.batch;
        let s = async_stream::try_stream! {let mut r=ObjectRangeRunReader::new(&path).map_err(|e|vortex_error::vortex_err!("{}",e))?;loop{let(mut a,mut b)=(Vec::with_capacity(batch),Vec::with_capacity(batch));while a.len()<batch{let Some(v)=r.read_one().map_err(|e|vortex_error::vortex_err!("{}",e))? else{break};a.push(v.row_start);b.push(v.row_end)}if a.is_empty(){break}yield build_exact_range_payload_array(a,b).map_err(|e|vortex_error::vortex_err!("{}",e))?;}};
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            s,
        )))
    }
}
pub(crate) fn object_component_writes(
    prepared: Arc<PreparedObjectExactRanges>,
    row_group_size: usize,
) -> Result<Vec<NativeComponentWrite>> {
    let d = Arc::new(ObjectDirectorySource::new(Arc::clone(&prepared))?);
    let p = Arc::new(ObjectPayloadSource::new(
        prepared.payload_path.clone(),
        row_group_size,
    )?);
    let strategy: Arc<dyn LayoutStrategy> = WriteStrategyBuilder::default()
        .with_row_block_size(row_group_size)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    Ok(vec![
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "index.object.exact-ranges.directory".into(),
                role: StoreComponentRole::Index,
                implementation: "native-object-exact-directory-v2-compact".into(),
                version: 2,
                required: false,
                dtype: d.dtype().clone(),
            },
            d,
            Arc::clone(&strategy),
        )?,
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "index.object.exact-ranges.payload".into(),
                role: StoreComponentRole::Index,
                implementation: "native-object-exact-payload-v2-compact".into(),
                version: 2,
                required: false,
                dtype: p.dtype().clone(),
            },
            p,
            strategy,
        )?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_builds_disjoint_ranges_and_contiguous_directory() {
        let temp = tempfile::tempdir().unwrap();
        let mut collector = ObjectRangeCollector::new(temp.path());
        for object_id in [2, 2, 7, 2, 9, 7] {
            collector.push_object(object_id).unwrap();
        }
        let prepared = collector.finish().unwrap();
        assert_eq!(&*prepared.object_ids, &[2, 7, 9]);
        assert_eq!(&*prepared.range_offsets, &[0, 2, 4]);
        assert_eq!(&*prepared.range_counts, &[2, 2, 1]);
        assert_eq!(&*prepared.candidate_rows, &[3, 2, 1]);
    }

    #[test]
    fn empty_collector_writes_an_empty_payload_and_directory() {
        let temp = tempfile::tempdir().unwrap();
        let prepared = ObjectRangeCollector::new(temp.path()).finish().unwrap();
        assert!(prepared.object_ids.is_empty());
        assert_eq!(std::fs::metadata(&prepared.payload_path).unwrap().len(), 0);
    }

    #[test]
    fn object_spill_codec_is_stable_and_round_trips() {
        let value = ObjectRangeRecord {
            object_id: 7,
            row_start: 11,
            row_end: 19,
        };
        let bytes = value.encode();
        assert_eq!(bytes.len(), 20);
        assert_eq!(ObjectRangeRecord::decode(bytes), value);
    }

    #[test]
    fn empty_spill_flush_is_a_noop() {
        let temp = tempfile::tempdir().unwrap();
        let mut records = Vec::new();
        let mut runs = Vec::new();
        flush_object_range_run(&mut records, temp.path(), 0, &mut runs).unwrap();
        assert!(runs.is_empty());
    }

    #[test]
    fn directory_builder_derives_contiguous_offsets() {
        let mut builder = ObjectDirectoryBuilder::default();
        builder.push(4, 2, 7).unwrap();
        builder.push(9, 1, 3).unwrap();
        let prepared = builder.finish(PathBuf::from("payload.bin"), 3).unwrap();
        assert_eq!(&*prepared.object_ids, &[4, 9]);
        assert_eq!(&*prepared.range_offsets, &[0, 2]);
        assert_eq!(&*prepared.range_counts, &[2, 1]);
        assert_eq!(&*prepared.candidate_rows, &[7, 3]);
    }

    #[test]
    fn directory_builder_rejects_invalid_entries_and_payload_mismatch() {
        let mut builder = ObjectDirectoryBuilder::default();
        assert!(builder.push(4, 0, 1).is_err());
        assert!(builder.push(4, 1, 0).is_err());
        builder.push(4, 1, 1).unwrap();
        assert!(builder.push(4, 1, 1).is_err());
        assert!(builder.finish(PathBuf::from("payload.bin"), 2).is_err());
    }
}

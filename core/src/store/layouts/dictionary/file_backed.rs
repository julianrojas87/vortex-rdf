//! The file-backed arm of the Dictionary layout's residency axis: a term
//! dictionary left in its serialized child, probed and lifted by scans on
//! demand. The policy enum choosing between this and the resident form is
//! [`DictAccess`](super::access::DictAccess); the whole module only compiles
//! with `file-io`, since without a file there is nothing to leave the terms
//! in.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::expr::{eq, get_item, lit, root, select};
use vortex_array::stream::ArrayStreamExt as _;
use vortex_mask::{AllOr, Mask};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::StrColReader;

use super::term_dict::{ProbeCache, TERM_FIELD, TermDictionary, dict_from_reader};

/// The in-memory fence over a file-backed dictionary's splits: the first
/// term of every dictionary-bearing split, in file order. The term column is
/// sorted, so a probed term can only live in the last split whose first term
/// is `<=` the probe — one in-RAM binary search replaces a per-split pruning
/// loop, and a probe below the first fence term is absent without touching
/// the file.
///
/// Built lazily on the first probe (one row-index scan of the per-split
/// boundary rows), shared across clones, never persisted. It retains one
/// term string per dictionary split — a few KB per file.
struct Fence {
    /// First dictionary term of each split in `splits`, ascending.
    first_terms: Vec<String>,
    /// The dictionary-bearing splits, clamped to the dictionary rows.
    splits: Vec<Range<u64>>,
}

/// A term dictionary left in its layout child: term→ID probes and ID→term
/// decodes scan the sorted `_dict_term` column on demand instead of holding
/// all terms resident.
///
/// `reader` is the dictionary child's layout reader (the native store root's
/// `dictionary` component), so a term's code is its child row. A term probe
/// binary-searches the in-memory [`Fence`] for the one split that can hold
/// the term, evaluates an equality filter over just that split, and memoizes
/// the answer in a [`ProbeCache`]. ID→term reads are row-index scans.
#[derive(Clone)]
pub(crate) struct FileBackedDict {
    /// The dictionary child's layout reader (child-local row coordinates).
    reader: vortex_layout::LayoutReaderRef,
    /// Number of terms.
    len: u64,
    /// term → code memo, shared across clones (every derived view of a store
    /// probes the same immutable dictionary).
    probes: Arc<ProbeCache>,
    /// The probe fence, built on first use and shared across clones.
    fence: Arc<OnceLock<Fence>>,
}

impl FileBackedDict {
    pub(crate) fn new(reader: vortex_layout::LayoutReaderRef, len: u64) -> Self {
        Self {
            reader,
            len,
            probes: Arc::new(ProbeCache::new()),
            fence: Arc::new(OnceLock::new()),
        }
    }

    /// A scan over the dictionary child — the reader-level equivalent of
    /// `file.scan()`.
    fn scan(&self) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
        vortex_layout::scan::scan_builder::ScanBuilder::new(
            VORTEX_SESSION.clone(),
            self.reader.clone(),
        )
    }

    /// The fence, building it on first use. Concurrent first probes may race
    /// to build; the loser's copy is dropped — the fence is derived
    /// deterministically from the immutable file, so any winner is right.
    async fn fence(&self) -> Result<&Fence> {
        if self.fence.get().is_none() {
            let built = self.build_fence().await?;
            let _ = self.fence.set(built);
        }
        Ok(self
            .fence
            .get()
            .expect("the fence was just initialized above"))
    }

    /// Collect the dictionary's splits (clamped to its rows, defensively) and
    /// resolve each one's first term with a single row-index scan.
    async fn build_fence(&self) -> Result<Fence> {
        use itertools::Itertools as _;
        use vortex_array::dtype::FieldMask;
        use vortex_layout::scan::split_by::SplitBy;
        let bounds = SplitBy::Layout
            .splits(
                self.reader.as_ref(),
                &(0..self.reader.row_count()),
                &[FieldMask::All],
            )
            .map_err(VortexRdfError::Vortex)?;
        let mut splits = Vec::new();
        for (start, end) in bounds.into_iter().tuple_windows() {
            let stop = end.min(self.len);
            if start < stop {
                splits.push(start..stop);
            }
        }
        let codes: Vec<u32> = splits.iter().map(|range| range.start as u32).collect();
        let first_terms = self.resolve_terms(&codes).await?;
        Ok(Fence {
            first_terms,
            splits,
        })
    }

    /// Term→ID: the fence's binary search picks the one split whose range
    /// can hold the term (the column is sorted), a single equality-filtered
    /// evaluation of that split decides, and the answer is memoized.
    pub(crate) async fn get_id(&self, term: &str) -> Result<Option<u32>> {
        if let Some(memo) = self.probes.get(term) {
            return Ok(memo);
        }
        let fence = self.fence().await?;
        // The candidate is the last split whose first term is <= the probe;
        // index 0 means every dictionary term sorts above it — absent.
        let idx = fence.first_terms.partition_point(|t| t.as_str() <= term);
        let candidate = idx.checked_sub(1).map(|i| fence.splits[i].clone());
        let code = match candidate {
            None => None,
            Some(range) => {
                let filter = [eq(get_item(TERM_FIELD, root()), lit(term))];
                let mask = crate::store::scan::file_scan::evaluate_filter_split(
                    self.reader.clone(),
                    &filter,
                    &range,
                    Mask::new_true((range.end - range.start) as usize),
                )
                .await?;
                let row = match mask.indices() {
                    AllOr::All => Some(range.start),
                    AllOr::None => None,
                    AllOr::Some(indices) => indices.first().map(|&i| range.start + i as u64),
                };
                row.map(|row| row as u32)
            }
        };
        self.probes.put(term, code);
        Ok(code)
    }

    /// ID→terms for reconstruction: resolve `codes` (ascending, unique) to
    /// their term strings with a single row-index scan — the dictionary's
    /// code→string seam. The layout-side chunk decode
    /// (`ResolvedLayout::decode_chunk_async`) resolves a chunk's distinct
    /// codes through this, and the fence build resolves its split boundaries.
    pub(crate) async fn resolve_terms(&self, codes: &[u32]) -> Result<Vec<String>> {
        if codes.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(&max) = codes.last()
            && max as u64 >= self.len
        {
            return Err(VortexRdfError::Deserialization(format!(
                "Term code {} out of dictionary bounds ({})",
                max, self.len
            )));
        }
        let rows: vortex_buffer::Buffer<u64> = codes.iter().map(|&code| code as u64).collect();
        let arr = self
            .scan()
            .with_row_indices(rows)
            .with_projection(select([TERM_FIELD], root()))
            .into_array_stream()
            .map_err(VortexRdfError::Vortex)?
            .read_all()
            .await
            .map_err(VortexRdfError::Vortex)?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = arr
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let col = struct_arr
            .unmasked_field_by_name(TERM_FIELD)
            .map_err(VortexRdfError::Vortex)?
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        if col.len() != codes.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "Dictionary row-index scan returned {} rows for {} codes",
                col.len(),
                codes.len()
            )));
        }
        let reader = StrColReader::new(&col);
        (0..col.len())
            .map(|i| reader.str_at(i).map(str::to_string))
            .collect()
    }

    /// Lift the whole dictionary resident — the transient full-column read
    /// behind [`DictAccess::ensure_resident`].
    ///
    /// [`DictAccess::ensure_resident`]: super::access::DictAccess::ensure_resident
    pub(crate) async fn load_resident(&self) -> Result<TermDictionary> {
        dict_from_reader(self.reader.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::IntoArray;
    use vortex_array::validity::Validity;

    /// The compression windows survive serialization as the child's splits:
    /// a `FileBackedDict` over a windowed dictionary's written child fences
    /// one split per window and probes correctly across all of them.
    #[tokio::test]
    async fn windowed_dict_child_fence_splits() {
        use crate::io::container;
        use vortex_array::stream::ArrayStreamAdapter;
        use vortex_buffer::ByteBuffer;
        use vortex_file::OpenOptionsSessionExt as _;

        let terms: Vec<String> = (0..600)
            .map(|i| format!("<http://example.org/fence/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let len = d.len() as u64;

        // A minimal native file: a one-row quad child plus the dictionary.
        let quads = StructArray::try_new(
            ["s", "p", "o", "g"].into(),
            (0..4)
                .map(|_| vortex_buffer::Buffer::from_iter([0u32]).into_array())
                .collect::<Vec<_>>(),
            1,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let dtype = quads.dtype().clone();
        let stream = ArrayStreamAdapter::new(
            dtype,
            Box::pin(futures::stream::once(async move { Ok(quads) })),
        );
        let mut bytes: Vec<u8> = Vec::new();
        container::write_store(
            &VORTEX_SESSION,
            &mut bytes,
            stream,
            container::default_child_strategy(),
            false,
            vec![crate::io::ser::dict_component(&d).unwrap()],
        )
        .await
        .unwrap();

        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))
            .unwrap();
        let native = crate::store::native_file::NativeStoreFile::try_new(file).unwrap();
        let (_, reader) = native
            .component_reader(container::DICT_COMPONENT_NAME)
            .unwrap()
            .expect("the dictionary child must be present");
        let fbd = FileBackedDict::new(reader, len);

        // One split per compression window, none merged, none re-cut.
        let fence = fbd.fence().await.unwrap();
        assert_eq!(fence.splits.len(), 6);

        // Probes across every window: interior, first-of-window,
        // last-of-window, and absent.
        for (i, term) in terms.iter().enumerate().step_by(97) {
            assert_eq!(fbd.get_id(term).await.unwrap(), Some(i as u32), "{term}");
        }
        for boundary in (0..600).step_by(100) {
            assert_eq!(
                fbd.get_id(&terms[boundary]).await.unwrap(),
                Some(boundary as u32)
            );
            assert_eq!(
                fbd.get_id(&terms[boundary + 99]).await.unwrap(),
                Some((boundary + 99) as u32)
            );
        }
        assert_eq!(fbd.get_id("<http://zzz>").await.unwrap(), None);
    }
}

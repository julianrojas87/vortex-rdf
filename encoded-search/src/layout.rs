//! File-side probing behind the `layout` feature: locate one sorted column's
//! flat chunk leaves inside a layout tree and answer global bounds queries by
//! fetching and probing only the chunks a binary search touches.
//!
//! A chunk fetch reads the leaf's whole segment and reconstructs the array in
//! its wire encoding (`SerializedArray::decode` rebuilds metadata over the
//! segment buffers — it does not decompress); fetched arrays are cached, so
//! repeated queries probe without further reads.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
use vortex_array::serde::SerializedArray;
use vortex_error::{VortexExpect as _, VortexResult};
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::struct_::Struct as StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;
use vortex_layout::{LayoutChildType, LayoutRef};
use vortex_session::VortexSession;

use crate::SortedProbe;

/// One flat chunk leaf: its layout, logical position, and the fetched
/// wire-encoded array (filled on first use).
struct ChunkSpec {
    layout: LayoutRef,
    row_offset: u64,
    row_count: u64,
    cell: OnceLock<ArrayRef>,
}

/// The flat chunk leaves of one sorted, non-nullable unsigned-integer column
/// of a struct layout, addressable for global bounds queries.
///
/// Sortedness across the whole column is the caller's contract, exactly as
/// for [`SortedProbe`].
pub struct SortedColumnChunks {
    chunks: Vec<ChunkSpec>,
    row_count: u64,
}

impl SortedColumnChunks {
    /// Walks `root` (a struct layout) to `field`'s chunk leaves: the field
    /// child, through any zoned wrappers, then either a chunked layout of
    /// flat leaves or a single flat leaf. Returns `None` when the shape or
    /// the field's dtype (non-nullable unsigned integer) is unsupported.
    pub fn from_struct_layout(root: &LayoutRef, field: &str) -> Option<Self> {
        root.as_opt::<StructLayout>()?;
        let column = (0..root.nchildren()).find_map(|i| {
            matches!(root.child_type(i), LayoutChildType::Field(ref name) if name.as_ref() == field)
                .then(|| root.child(i).ok())
                .flatten()
        })?;
        let dtype = column.dtype();
        if !dtype.is_unsigned_int() || dtype.is_nullable() {
            return None;
        }

        let data = unwrap_zoned(column)?;
        let row_count = data.row_count();
        let mut chunks = Vec::new();
        if data.is::<Flat>() {
            chunks.push(ChunkSpec {
                layout: data,
                row_offset: 0,
                row_count,
                cell: OnceLock::new(),
            });
        } else if data.is::<ChunkedLayout>() {
            for i in 0..data.nchildren() {
                let LayoutChildType::Chunk((_, row_offset)) = data.child_type(i) else {
                    return None;
                };
                let leaf = unwrap_zoned(data.child(i).ok()?)?;
                let chunk_rows = leaf.row_count();
                if chunk_rows == 0 {
                    continue;
                }
                if !leaf.is::<Flat>() {
                    return None;
                }
                chunks.push(ChunkSpec {
                    layout: leaf,
                    row_offset,
                    row_count: chunk_rows,
                    cell: OnceLock::new(),
                });
            }
        } else {
            return None;
        }
        Some(Self { chunks, row_count })
    }

    /// Exact global `[lo, hi)` of `needle` in the column, fetching at most
    /// the chunks a binary search over chunk extremes touches (cached
    /// thereafter). `Ok(None)` when a needed chunk's encoding declines the
    /// probe — the caller falls back to its scan path.
    pub async fn bounds(
        &self,
        needle: u64,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<Range<u64>>> {
        if self.chunks.is_empty() {
            return Ok(Some(0..0));
        }

        // First chunk whose last value is >= needle.
        let (mut lo, mut hi) = (0usize, self.chunks.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let arr = self.chunk_array(mid, source, session).await?;
            let Some(probe) = SortedProbe::resolve(arr) else {
                return Ok(None);
            };
            if probe.value_at(probe.len() - 1) < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let lower = if lo == self.chunks.len() {
            self.row_count
        } else {
            let arr = self.chunk_array(lo, source, session).await?;
            let Some(probe) = SortedProbe::resolve(arr) else {
                return Ok(None);
            };
            self.chunks[lo].row_offset + probe.lower_bound(needle) as u64
        };

        // First chunk whose first value is > needle.
        let (mut lo2, mut hi2) = (0usize, self.chunks.len());
        while lo2 < hi2 {
            let mid = lo2 + (hi2 - lo2) / 2;
            let arr = self.chunk_array(mid, source, session).await?;
            let Some(probe) = SortedProbe::resolve(arr) else {
                return Ok(None);
            };
            if probe.value_at(0) <= needle {
                lo2 = mid + 1;
            } else {
                hi2 = mid;
            }
        }
        let upper = if lo2 == 0 {
            0
        } else {
            let arr = self.chunk_array(lo2 - 1, source, session).await?;
            let Some(probe) = SortedProbe::resolve(arr) else {
                return Ok(None);
            };
            self.chunks[lo2 - 1].row_offset + probe.upper_bound(needle) as u64
        };

        Ok(Some(lower..upper.max(lower)))
    }

    /// The chunk's wire-encoded array, fetched through `source` on first use.
    async fn chunk_array(
        &self,
        idx: usize,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<&ArrayRef> {
        let spec = &self.chunks[idx];
        if spec.cell.get().is_none() {
            let flat = spec
                .layout
                .as_opt::<Flat>()
                .vortex_expect("chunk leaves are validated flat at construction");
            let segment = source.request(flat.segment_id()).await?;
            let parts = match flat.array_tree().cloned() {
                Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree, segment)?,
                None => SerializedArray::try_from(segment)?,
            };
            let row_count =
                usize::try_from(spec.row_count).vortex_expect("chunk row count must fit in usize");
            let array = parts.decode(flat.dtype(), row_count, flat.array_ctx(), session)?;
            let _ = spec.cell.set(array);
        }
        Ok(spec
            .cell
            .get()
            .vortex_expect("chunk cell was just populated"))
    }
}

/// Descend through zoned wrappers to their data child (child 0).
fn unwrap_zoned(mut node: LayoutRef) -> Option<LayoutRef> {
    while node.is::<Zoned>() {
        node = node.child(0).ok()?;
    }
    Some(node)
}

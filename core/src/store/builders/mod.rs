//! Emission orchestration: turning a quad stream into the row space a store
//! is built or serialized from.
//!
//! This hub owns everything the three strategies share — the
//! [`VortexArrayBuilder`] contract and its two products ([`BuiltArray`],
//! [`BuiltStream`]), chunk assembly, the sortedness stamps a build is allowed
//! to claim, and the globally-sorted index emission the sorted strategies
//! slice their chunks out of. A strategy leaf
//! (`sorted_in_memory`, `sorted_stream`, `unsorted_stream`) contributes only
//! its ordering discipline and memory profile; `spill` backs the out-of-core
//! one.
//!
//! Builders emit the *welded* row space — primary columns plus every
//! requested index's `_idx_*` columns in one struct. Splitting that into a
//! store's model or a file's children is the reader/serializer's job (see
//! `indexes::components`), never a
//! builder's. What a builder does own is sortedness *provenance*: only the
//! globally-sorted paths may stamp `IsSorted`, because a reader binary-
//! searches on that stamp alone.

use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::stamp_is_sorted;
use crate::store::indexes::{
    IndexType, Indexes, secondary_by_copy, secondary_by_reference, unique_indexes,
};
use crate::store::layouts::LayoutStrategy;
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::term_dictionary::TermDictionary;
use futures::{Stream, StreamExt, stream};
use std::future::Future;
use std::sync::Arc;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

/// Number of quads per StructArray chunk in streaming/chunked builders.
pub(crate) const DEFAULT_CHUNK_SIZE: usize = 100_000;

/// A stream of StructArray chunks ready for consumption by the Vortex file
/// writer. Items use `VortexResult` because the writer polls the stream
/// directly; builder errors are converted via `into_vortex_error`.
pub type ChunkStream = stream::BoxStream<'static, vortex_error::VortexResult<ArrayRef>>;

/// Convert a builder error into a `VortexError` for use inside a [`ChunkStream`].
pub(crate) fn into_vortex_error(e: VortexRdfError) -> vortex_error::VortexError {
    match e {
        VortexRdfError::Vortex(v) => v,
        other => vortex_error::vortex_err!("{}", other),
    }
}

/// A built dataset: the quad array plus whatever layout state cannot be
/// derived from the array alone — for the Dictionary layout, its term
/// dictionary (the array holds only u32 code columns; the terms travel
/// beside it and reach serialized files as the native container's
/// `dictionary` child).
pub struct BuiltArray {
    pub array: ArrayRef,
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

/// The streaming counterpart of [`BuiltArray`]: the schema dtype, the lazy
/// chunk stream, the native components riding beside it (index children the
/// builder streams from its own spill-run mergers — empty when the builder
/// emits row-space chunks and leaves the split to the serializer), and the
/// dictionary the serializer writes as the `dictionary` child.
pub struct BuiltStream {
    pub dtype: DType,
    pub chunks: ChunkStream,
    pub(crate) components: Vec<crate::io::store_layout::NativeComponentWrite>,
    /// Whether any `_idx_*` columns riding in the row-space chunks are
    /// GLOBALLY sorted — the builder's provenance, recorded on the children
    /// the serializer's tee splits out of them. Per-chunk local sorts must
    /// leave this false: a reader that binary-searched their concatenation
    /// would return wrong rows. (Read by the serializer, so dead in builds
    /// that compile none in.)
    #[cfg_attr(not(feature = "file-io"), allow(dead_code))]
    pub(crate) components_sorted: bool,
    /// Whether the chunks' `s` column is globally sorted — recorded in the
    /// written root's metadata so a materialized read can restore the
    /// subject binary-search stamp truthfully.
    #[cfg_attr(not(feature = "file-io"), allow(dead_code))]
    pub(crate) quads_sorted: bool,
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum BuilderStrategy {
    /// Natural insertion order, no sorting. Chunks stream directly to the
    /// writer during serialization, bounding memory by the chunk size.
    UnsortedStream,
    /// Global sort of quads by Subject -> Predicate -> Object -> Graph in memory.
    SortedInMemory,
    /// External-memory out-of-core global sort using merge runs.
    SortedStream,
}

pub(crate) mod sorted_in_memory;
pub(crate) mod sorted_stream;
pub(crate) mod spill;
pub(crate) mod unsorted_stream;

pub use sorted_in_memory::SortedInMemoryBuilder;
pub use sorted_stream::SortedStreamBuilder;
pub use unsorted_stream::UnsortedStreamBuilder;

pub trait VortexArrayBuilder {
    /// Build the complete dataset as a single (possibly chunked) in-memory
    /// array, together with the layout state the array alone cannot carry
    /// (the Dictionary layout's term dictionary).
    fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltArray>> + Send;

    /// Produce the schema dtype and a lazily-evaluated stream of StructArray
    /// chunks, for feeding directly into the Vortex file writer.
    ///
    /// The default implementation materializes the full array via
    /// [`Self::build_vortex_array`] and yields it as a single chunk. Builders
    /// that can emit chunks incrementally should override this so that writing
    /// a file needs only O(chunk) memory instead of O(dataset).
    fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltStream>> + Send {
        async move {
            let built = Self::build_vortex_array(quad_stream, layout, indexes).await?;
            let dtype = built.array.dtype().clone();
            let array = built.array;
            let (quads_sorted, components_sorted) = row_space_sortedness(&array);
            let chunks: ChunkStream = futures::stream::once(async move { Ok(array) }).boxed();
            Ok(BuiltStream {
                components: Vec::new(),
                components_sorted,
                quads_sorted,
                dtype,
                chunks,
                dict: built.dict,
            })
        }
    }
}

/// Build a complete StructArray chunk: primary columns for the given layout,
/// followed by the columns of every requested index.
///
/// The layout-specific column logic lives in [`crate::store::layouts`] and the
/// index-specific column logic in [`crate::store::indexes`]; this function only
/// orchestrates them.
///
/// `start_row` is the global row ID of the first quad in `quads`; pass the
/// running row offset when building one chunk of a larger array so index row
/// IDs address the assembled array, or `0` for a single-chunk build.
///
/// `s_sorted` must be `true` only when `quads` is sorted by subject: it stamps
/// the `IsSorted` statistic on the `s` column, which enables the binary-search
/// fast path in `match_pattern`. Stamping it on unsorted data would corrupt
/// query results.
///
/// `whole_dataset` must be `true` only when `quads` is the complete dataset
/// (single-chunk builds): the per-chunk index sort is then the global order,
/// and the index value columns get stamped `IsSorted` so `match_pattern` may
/// binary-search them.
pub(crate) fn build_struct_array(
    quads: &[RawQuad],
    layout: LayoutStrategy,
    indexes: &[IndexType],
    n: usize,
    start_row: u32,
    s_sorted: bool,
    whole_dataset: bool,
) -> Result<ArrayRef> {
    let mut field_names = layout.field_names();
    let mut field_arrays = layout.build_columns(quads)?;

    if s_sorted {
        // The s column is first in both layouts.
        stamp_is_sorted(&field_arrays[0]);
    }

    for idx in unique_indexes(indexes) {
        idx.append_columns(
            &mut field_names,
            &mut field_arrays,
            quads,
            start_row,
            whole_dataset,
        );
    }

    StructArray::try_new(field_names.into(), field_arrays, n, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// Globally sorted index columns for every requested index type, built once
/// over the complete in-memory dataset and sliced per chunk — the global
/// counterpart of the per-chunk `IndexType::append_*` dispatch. Lives with
/// the builders because it is pure emission machinery: the sorted builders
/// construct it and slice it into their chunks; nothing on the read side
/// touches it.
pub(crate) struct GlobalIndexes {
    by_copy: Option<secondary_by_copy::GlobalCopyArrays>,
    by_reference: Option<secondary_by_reference::GlobalIndexArrays>,
}

impl GlobalIndexes {
    /// Build from the dataset in final row order (term-string columns).
    pub(crate) fn from_quads(indexes: &[IndexType], quads: &[RawQuad]) -> Self {
        let mut by_copy = None;
        let mut by_reference = None;
        for idx in unique_indexes(indexes) {
            match idx {
                IndexType::SecondaryByCopy => {
                    by_copy = Some(secondary_by_copy::GlobalCopyArrays::from_quads(quads));
                }
                IndexType::SecondaryByReference => {
                    by_reference =
                        Some(secondary_by_reference::GlobalIndexArrays::from_quads(quads));
                }
            }
        }
        Self {
            by_copy,
            by_reference,
        }
    }

    /// Dictionary-layout variant: build from the dataset's u32 codes.
    pub(crate) fn from_codes(indexes: &[IndexType], codes: &QuadCodes) -> Self {
        let mut by_copy = None;
        let mut by_reference = None;
        for idx in unique_indexes(indexes) {
            match idx {
                IndexType::SecondaryByCopy => {
                    by_copy = Some(secondary_by_copy::GlobalCopyArrays::from_codes(codes));
                }
                IndexType::SecondaryByReference => {
                    by_reference =
                        Some(secondary_by_reference::GlobalIndexArrays::from_codes(codes));
                }
            }
        }
        Self {
            by_copy,
            by_reference,
        }
    }

    /// Append window `range` of every index's global order as one chunk's
    /// index columns.
    pub(crate) fn append_slice(
        &self,
        field_names: &mut Vec<Arc<str>>,
        field_arrays: &mut Vec<ArrayRef>,
        range: std::ops::Range<usize>,
    ) -> Result<()> {
        if let Some(sbc) = &self.by_copy {
            sbc.append_slice(field_names, field_arrays, range.clone())?;
        }
        if let Some(sbr) = &self.by_reference {
            sbr.append_slice(field_names, field_arrays, range)?;
        }
        Ok(())
    }
}

/// Build a chunk for rows `range` of an in-memory dataset whose index columns
/// come pre-sorted from a [`GlobalIndexes`] — the sorted in-memory builder's
/// chunked emission path. `quads` is the chunk's slice (`dataset[range]`).
pub(crate) fn build_struct_array_global(
    quads: &[RawQuad],
    layout: LayoutStrategy,
    global_indexes: &GlobalIndexes,
    range: std::ops::Range<usize>,
    s_sorted: bool,
) -> Result<ArrayRef> {
    let mut field_names = layout.field_names();
    let mut field_arrays = layout.build_columns(quads)?;

    if s_sorted {
        stamp_is_sorted(&field_arrays[0]);
    }

    global_indexes.append_slice(&mut field_names, &mut field_arrays, range)?;

    StructArray::try_new(
        field_names.into(),
        field_arrays,
        quads.len(),
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

/// Canonicalize a sorted builder's (possibly chunked) materialized array and
/// re-stamp the sortedness stats the builder guarantees: the `s` column is
/// globally sorted, and — when the chunks were emitted as windows of a global
/// index order — so are the `_idx_*_val` columns and the copy families' lead
/// columns. Canonicalization loses the per-chunk stats, so without this
/// multi-chunk in-memory stores would fall back to mask scans.
pub(crate) fn canonicalize_sorted(arr: ArrayRef) -> Result<ArrayRef> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    for field in std::iter::once(crate::store::schema::COL_S)
        .chain(crate::store::indexes::globally_sorted_columns())
    {
        if let Ok(col) = struct_arr.unmasked_field_by_name(field) {
            stamp_is_sorted(col);
        }
    }
    Ok(struct_arr.into_array())
}

/// Read a welded row-space array's sortedness provenance off its own stamps:
/// `(s globally sorted, index sort-key columns globally sorted)`. Only the
/// global-emission paths stamp these, so a multi-chunk assembly (whose
/// canonicalization dropped the per-chunk stats) correctly lands on `false`.
pub(crate) fn row_space_sortedness(array: &ArrayRef) -> (bool, bool) {
    use crate::store::array::column_is_sorted;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let Ok(struct_arr) = array.clone().execute::<StructArray>(&mut ctx) else {
        return (false, false);
    };
    let col_sorted = |name: &str| {
        struct_arr
            .unmasked_field_by_name(name)
            .map(column_is_sorted)
            .unwrap_or(false)
    };
    let quads_sorted = col_sorted(crate::store::schema::COL_S);
    let index_columns: Vec<&str> = crate::store::indexes::globally_sorted_columns()
        .into_iter()
        .filter(|c| struct_arr.names().iter().any(|n| n.as_ref() == *c))
        .collect();
    let components_sorted =
        !index_columns.is_empty() && index_columns.iter().all(|c| col_sorted(c));
    (quads_sorted, components_sorted)
}

/// Assemble a list of per-chunk StructArrays into a single ArrayRef.
/// Returns an empty StructArray with the correct schema when `chunks` is empty.
pub(crate) fn assemble_chunks(
    mut chunks: Vec<ArrayRef>,
    layout: LayoutStrategy,
    indexes: &Indexes,
) -> Result<ArrayRef> {
    if chunks.is_empty() {
        make_empty_struct(layout, indexes)
    } else if chunks.len() == 1 {
        Ok(chunks.remove(0))
    } else {
        use vortex_array::arrays::ChunkedArray;
        let dtype = chunks[0].dtype().clone();
        let chunked = ChunkedArray::try_new(chunks, dtype)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        Ok(chunked)
    }
}

/// An empty StructArray with the schema of the given layout and indexes.
/// Building from an empty quad slice yields every column empty but with the
/// correct dtype, so this is just the regular build path with no rows.
pub(crate) fn make_empty_struct(layout: LayoutStrategy, indexes: &Indexes) -> Result<ArrayRef> {
    if layout == LayoutStrategy::Dictionary {
        return crate::store::layouts::dictionary::empty_struct(indexes);
    }
    build_struct_array(&[], layout, indexes, 0, 0, false, true)
}

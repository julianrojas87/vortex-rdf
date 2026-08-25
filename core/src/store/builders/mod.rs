//! Emission orchestration: turning a quad stream into the parts a store is
//! built or serialized from.
//!
//! This hub owns everything the two pipelines share — the
//! [`VortexArrayBuilder`] contract and its two products ([`BuiltArray`],
//! [`BuiltStream`]), primary chunk assembly, the sortedness stamps a build is
//! allowed to claim, and the globally-sorted index emission
//! ([`GlobalIndexes`]). A leaf (`sorted_in_memory`, `sorted_stream`)
//! contributes only its memory profile; `spill` backs the out-of-core one.
//!
//! Which pipeline runs is a property of the target, never a caller's choice:
//! where a filesystem exists the rows go through the out-of-core global sort
//! ([`SortedStreamBuilder`]), whose peak memory does not scale with the
//! dataset; `wasm32-unknown-unknown` has no filesystem to spill to, so there
//! the in-memory global sort ([`SortedInMemoryBuilder`]) is the pipeline —
//! and the only one compiled in.
//!
//! Index data has exactly one form: a builder emits primary-only quad rows
//! plus one *component* per requested index's persisted-child role (see
//! `indexes::components`), which is what a store adopts and what a file
//! writes — nothing intermediate, nothing to split. The two pipelines differ
//! only in where the components come from: the in-memory sort builds all of
//! them at once over the dataset ([`GlobalIndexes`]), the out-of-core one
//! streams each family off its own spill-run merger.
//!
//! Both sort globally, which is what lets them declare their components
//! `sorted` — a reader binary-searches on that provenance alone. The quad
//! rows' own subject sortedness is not a constant in the same way: it is
//! true of every build here, but a store that has been appended to loses it,
//! so it travels as the `s` column's `IsSorted` stamp and is re-read rather
//! than assumed.

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::{chunked_or_single, stamp_is_sorted};
use crate::store::indexes::{
    IndexComponent, IndexType, Indexes, secondary_by_copy, secondary_by_reference, unique_indexes,
};
use crate::store::layouts::LayoutStrategy;
use crate::store::layouts::dictionary::{QuadCodes, TermDictionary};
use futures::{Stream, stream};
use std::future::Future;
use std::sync::Arc;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::DType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};

/// Number of quads per StructArray chunk in streaming/chunked builders.
pub(crate) const DEFAULT_CHUNK_SIZE: usize = 100_000;

/// A stream of StructArray chunks ready for consumption by the Vortex file
/// writer. Items use `VortexResult` because the writer polls the stream
/// directly; builder errors are converted via `into_vortex_error`.
pub type ChunkStream = stream::BoxStream<'static, vortex_error::VortexResult<ArrayRef>>;

/// Convert a builder error into a `VortexError` for use inside a [`ChunkStream`].
fn into_vortex_error(e: VortexRdfError) -> vortex_error::VortexError {
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
///
/// Cloning is shallow (Arc'd buffers throughout), so one build can be
/// handed to [`VortexRdfStore::from_built`](crate::VortexRdfStore::from_built)
/// — which consumes it — more than once.
#[derive(Clone)]
pub struct BuiltArray {
    pub array: ArrayRef,
    /// The requested indexes' children, built beside the quad rows — the
    /// store's adoption currency
    /// ([`IndexComponent`](crate::store::indexes::IndexComponent)). Empty
    /// exactly when no indexes were requested.
    pub(crate) components: Vec<IndexComponent>,
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

/// The streaming counterpart of [`BuiltArray`]: the schema dtype, the lazy
/// stream of primary-only quad chunks, the index children riding beside it as
/// writable components, and the dictionary the serializer writes as the
/// `dictionary` child.
pub struct BuiltStream {
    pub dtype: DType,
    pub chunks: ChunkStream,
    // Read by the serializer and the materializing sorted-stream path, both
    // absent on wasm (no file-io feature there, external sort compiled out).
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(crate) components: Vec<crate::io::container::NativeComponentWrite>,
    /// Whether the chunks' `s` column is globally sorted — recorded in the
    /// written root's metadata so a materialized read can restore the
    /// subject binary-search stamp truthfully.
    #[cfg_attr(not(feature = "file-io"), allow(dead_code))]
    pub(crate) quads_sorted: bool,
    /// The Dictionary layout's terms, for the serializer to place as the
    /// `dictionary` child. (Read by the serializer, so dead in builds that
    /// compile none in.)
    #[cfg_attr(not(feature = "file-io"), allow(dead_code))]
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

pub(crate) mod sorted_in_memory;
// The external-sort pipeline spills to a real filesystem, which
// `wasm32-unknown-unknown` does not have: compiling it out (rather than
// erroring at spill time) keeps it, the rkyv serializer paths, and uuid out
// of the wasm artifact, whose size is a recorded constraint.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod sorted_stream;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod spill;

pub use sorted_in_memory::SortedInMemoryBuilder;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use sorted_stream::SortedStreamBuilder;

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
    /// chunks, for feeding directly into the Vortex file writer, so that
    /// writing a file needs only O(chunk) memory for the column arrays
    /// instead of O(dataset).
    fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltStream>> + Send;
}

/// Build one StructArray chunk of primary quad columns for the given layout.
/// Secondary indexes never ride here — they are built as components beside
/// the quad rows (see [`GlobalIndexes`]).
///
/// The layout-specific column logic lives in [`crate::store::layouts`]; this
/// function only orchestrates it.
///
/// `s_sorted` must be `true` only when `quads` is sorted by subject: it stamps
/// the `IsSorted` statistic on the `s` column, which enables the binary-search
/// fast path in `match_pattern`. Stamping it on unsorted data would corrupt
/// query results.
pub(crate) fn build_struct_array(
    quads: &[RawQuad],
    layout: LayoutStrategy,
    s_sorted: bool,
) -> Result<ArrayRef> {
    let field_names = layout.field_names();
    let field_arrays = layout.build_columns(quads)?;

    if s_sorted {
        // The s column is first in both layouts.
        stamp_is_sorted(&field_arrays[0]);
    }

    StructArray::try_new(
        field_names.into(),
        field_arrays,
        quads.len(),
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

/// Every requested index's columns, sorted once over the complete in-memory
/// dataset — the in-memory builders' index emission, handed on as persisted
/// children by [`into_components`](Self::into_components). Lives with the
/// builders because it is pure emission machinery: nothing on the read side
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

    /// Every built index's persisted children, in index declaration order.
    /// Each is globally sorted by construction and says so.
    pub(crate) fn into_components(self) -> Result<Vec<IndexComponent>> {
        let mut components = Vec::new();
        if let Some(sbc) = self.by_copy {
            components.extend(sbc.into_components()?);
        }
        if let Some(sbr) = self.by_reference {
            components.extend(sbr.into_components()?);
        }
        Ok(components)
    }
}

/// The requested indexes' children over a complete in-memory dataset in final
/// row order — the one entry point every in-memory index emission goes
/// through (the builders' construction paths and the mutation/compaction
/// rebuilds alike).
pub(crate) fn build_components(
    indexes: &[IndexType],
    quads: &[RawQuad],
) -> Result<Vec<IndexComponent>> {
    GlobalIndexes::from_quads(indexes, quads).into_components()
}

/// Dictionary-layout counterpart of [`build_components`]: the children are
/// built over the dataset's u32 codes. Sorting codes is order-equivalent to
/// sorting the term strings, so the children stay binary-searchable —
/// queries translate the pattern terms to codes first.
pub(crate) fn build_components_from_codes(
    indexes: &[IndexType],
    codes: &QuadCodes,
) -> Result<Vec<IndexComponent>> {
    GlobalIndexes::from_codes(indexes, codes).into_components()
}

/// Assemble a list of per-chunk StructArrays into a single ArrayRef.
/// Returns an empty StructArray with the correct schema when `chunks` is empty.
// Only the (wasm-gated) external-sort builder materializes chunk streams.
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn assemble_chunks(chunks: Vec<ArrayRef>, layout: LayoutStrategy) -> Result<ArrayRef> {
    if chunks.is_empty() {
        return make_empty_struct(layout);
    }
    let dtype = chunks[0].dtype().clone();
    chunked_or_single(chunks, dtype)
}

/// An empty StructArray with the given layout's primary schema. Building from
/// an empty quad slice yields every column empty but with the correct dtype,
/// so this is just the regular build path with no rows.
fn make_empty_struct(layout: LayoutStrategy) -> Result<ArrayRef> {
    if layout == LayoutStrategy::Dictionary {
        return crate::store::layouts::dictionary::empty_struct();
    }
    build_struct_array(&[], layout, false)
}

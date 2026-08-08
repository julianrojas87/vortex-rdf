//! Low-level helpers over Vortex arrays: string-array construction, canonical
//! string-column access, sortedness statistics, sorted-column binary search,
//! and mask conversion.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;

use vortex_array::arrays::varbinview::BinaryView;
use vortex_array::arrays::{BoolArray, VarBinViewArray};
use vortex_array::expr::stats::{Precision, Stat, StatsProvider};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;

/// Zero-cost row access into a canonical `VarBinView` string column: the
/// 16-byte views slice and the data buffers are resolved once, and each row's
/// bytes are then an inline read or a plain slice of the referenced buffer.
///
/// `bytes_at` instead materializes a refcounted `ByteBuffer` per row (two
/// atomic refcount ops plus alignment bookkeeping), which profiling showed
/// costing ~5% of every many-row decode; this reader is the loop-shaped
/// counterpart, sharing the access pattern of the typed residual filter's
/// `StrEq`.
pub(crate) struct StrColReader<'a> {
    arr: &'a VarBinViewArray,
    views: &'a [BinaryView],
}

impl<'a> StrColReader<'a> {
    pub(crate) fn new(arr: &'a VarBinViewArray) -> Self {
        Self {
            arr,
            views: arr.views(),
        }
    }

    #[inline]
    pub(crate) fn bytes_at(&self, i: usize) -> &'a [u8] {
        let view = &self.views[i];
        if view.is_inlined() {
            view.as_inlined().value()
        } else {
            let r = view.as_view();
            &self.arr.buffer(r.buffer_index as usize)[r.as_range()]
        }
    }

    #[inline]
    pub(crate) fn str_at(&self, i: usize) -> Result<&'a str> {
        buf_as_str(self.bytes_at(i))
    }
}

/// Build a Vortex string array (`VarBinView<Utf8>`, non-nullable) from string refs.
///
/// Values are copied once, directly into the array's buffer — no intermediate
/// owned `String` per value.
pub(crate) fn make_string_array(values: impl IntoIterator<Item = impl AsRef<str>>) -> ArrayRef {
    VarBinViewArray::from_iter_str(values).into_array()
}

/// Build a nullable Vortex string array for optional fields (e.g. o_datatype, o_lang).
pub(crate) fn make_nullable_string_array(
    values: impl IntoIterator<Item = Option<String>>,
) -> ArrayRef {
    VarBinViewArray::from_iter_nullable_str(values).into_array()
}

/// Stamp the exact `IsSorted` statistic on an array.
///
/// Only call when the array is sorted by construction: `match_pattern` trusts
/// this stat to binary-search the column, so a false stamp corrupts query
/// results.
pub(crate) fn stamp_is_sorted(arr: &ArrayRef) {
    arr.statistics()
        .set(Stat::IsSorted, Precision::Exact(true.into()));
}

/// Read back the `IsSorted` statistic written by [`stamp_is_sorted`]. An
/// absent stat counts as unsorted — order is never assumed, only trusted
/// when explicitly recorded.
pub(crate) fn column_is_sorted(arr: &ArrayRef) -> bool {
    match arr.statistics().get(Stat::IsSorted) {
        Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
        Precision::Absent => false,
    }
}

/// Binary-search a sorted column for the `[lo, hi)` run of rows equal to
/// `probe` (`lo == hi` means the value is absent). Only meaningful on
/// columns [`column_is_sorted`] reports as sorted.
pub(crate) fn search_sorted_bounds(
    arr: &ArrayRef,
    probe: &vortex_array::scalar::Scalar,
) -> Result<(usize, usize)> {
    use vortex_array::arrays::Primitive;
    use vortex_array::search_sorted::{SearchResult, SearchSorted, SearchSortedSide};

    // Typed fast path: a canonical non-nullable u32 column (the Dictionary
    // layout's term codes). `partition_point` over the raw slice costs a few
    // dozen loads; the generic kernel below builds a fresh `ExecutionCtx` and
    // materializes a `Scalar` per probe, which profiling showed dominating
    // `match_pattern`'s fixed per-call cost.
    if arr.dtype().is_unsigned_int()
        && !arr.dtype().is_nullable()
        && let Ok(prim) = arr.clone().try_downcast::<Primitive>()
        && prim.ptype() == vortex_array::dtype::PType::U32
        && let Ok(code) = u32::try_from(probe)
    {
        let codes = prim.as_slice::<u32>();
        let lo = codes.partition_point(|&v| v < code);
        let hi = codes.partition_point(|&v| v <= code);
        return Ok((lo, hi));
    }

    // Encoded fast path: probe the compressed representation directly. Covers
    // wire-encoded columns (`from_bytes` adoption, sliced index runs) without
    // decoding them; declines fall through to the generic kernel.
    if arr.dtype().is_unsigned_int()
        && !arr.dtype().is_nullable()
        && let Ok(needle) = u64::try_from(probe)
        && let Some(encoded) = vortex_encoded_search::SortedProbe::resolve(arr)
    {
        return Ok(encoded.bounds(needle));
    }

    let index_of = |result: SearchResult| match result {
        SearchResult::Found(i) | SearchResult::NotFound(i) => i,
    };
    let lo = arr
        .search_sorted(probe, SearchSortedSide::Left)
        .map_err(VortexRdfError::Vortex)?;
    let hi = arr
        .search_sorted(probe, SearchSortedSide::Right)
        .map_err(VortexRdfError::Vortex)?;
    Ok((index_of(lo), index_of(hi)))
}

/// Decode a base struct's integer children (the Dictionary layout's term
/// codes, TypedObject's kind column) to canonical primitives, keeping each
/// child's `IsSorted` stamp across the re-encoding.
///
/// The per-row match fast paths — [`search_sorted_bounds`]' `partition_point`
/// probe and the typed residual loops — read canonical primitives directly
/// and decline encoded columns, so an adopted base that keeps its wire
/// encoding pays a per-call fallback on every match. Decoding once here makes
/// those paths engage for the store's lifetime. String children keep their
/// encoded form: their canonical `VarBinView` costs real memory, and the mask
/// scan handles them at selection cost. Serialization re-compresses every
/// child through the default write strategy, so the wire format is unaffected.
///
/// A nullable struct passes through untouched — the base schema is
/// non-nullable, and rebuilding a struct with validity is not this helper's
/// business.
pub(crate) fn with_canonical_int_children(rows: ArrayRef) -> Result<ArrayRef> {
    use vortex_array::arrays::struct_::StructArrayExt;
    use vortex_array::arrays::{Primitive, PrimitiveArray, StructArray};
    use vortex_array::validity::Validity;

    if rows.dtype().is_nullable() {
        return Ok(rows);
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = rows
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let names = struct_arr.names().clone();
    let mut children = Vec::with_capacity(names.len());
    let mut changed = false;
    for name in names.iter() {
        let child = struct_arr
            .unmasked_field_by_name(name.as_ref())
            .map_err(VortexRdfError::Vortex)?;
        let decode = child.dtype().is_int()
            && !child.dtype().is_nullable()
            && child.clone().try_downcast::<Primitive>().is_err();
        if !decode {
            children.push(child.clone());
            continue;
        }
        let sorted = column_is_sorted(child);
        let canonical = child
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        if sorted {
            stamp_is_sorted(&canonical);
        }
        children.push(canonical);
        changed = true;
    }
    if !changed {
        return Ok(struct_arr.into_array());
    }
    let len = struct_arr.len();
    Ok(
        StructArray::try_new(names, children, len, Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array(),
    )
}

/// Convert a boolean ArrayRef into a `vortex_mask::Mask` for use with `ArrayRef::filter`.
pub(crate) fn bool_array_to_mask(arr: ArrayRef) -> Result<Mask> {
    // Canonicalize to a concrete boolean array, then reinterpret its packed
    // bit buffer directly as a Mask (no per-bit conversion loop).
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let bool_arr = arr
        .execute::<BoolArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(Mask::from_buffer(bool_arr.into_bit_buffer()))
}

/// Borrow the bytes of a UTF-8 string column value as `&str` without copying.
///
/// **Trusted-input decode path**, the same argument as
/// [`parse_named_node`](crate::common::terms::parse_named_node):
/// every caller reads a column whose dtype is `Utf8`, and vortex validates that
/// invariant when the array is constructed — `VarBinViewData::validate` runs a
/// `from_utf8` over every view on IPC decode, and the file reader validates on
/// its own construction path. Re-validating here walks each term's bytes a
/// second time, which profiling showed at ~7% of every many-row decode
/// (`core::str::converts::from_utf8` under `decode_chunk`).
///
/// The check is kept as a `debug_assert`, so the test suite (which runs debug)
/// still fails loudly if a non-UTF-8 column ever reaches this, while release
/// builds skip the second walk. The `Result` is retained so the decode call
/// sites — which `?` on genuinely fallible neighbours — stay uniform.
#[inline]
pub(crate) fn buf_as_str(buf: &[u8]) -> Result<&str> {
    debug_assert!(
        std::str::from_utf8(buf).is_ok(),
        "string column value is not valid UTF-8, but its dtype claims Utf8"
    );
    // SAFETY: `buf` is the bytes of a value in a `Utf8`-dtyped vortex column,
    // which vortex validates as UTF-8 when the array is constructed (see above).
    Ok(unsafe { std::str::from_utf8_unchecked(buf) })
}

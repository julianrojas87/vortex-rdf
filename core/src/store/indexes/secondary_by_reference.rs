//! The [`IndexType::SecondaryByReference`] index: sorted object and predicate
//! value columns, each paired with the primary row IDs they point at. This
//! module owns both halves of the index's lifecycle — building the columns at
//! write time, and executing lookups against them at query time
//! (`resolve_in_memory` / `resolve_file`, which produce primary row ids
//! directly for each backend).
//!
//! The value columns come in two encodings — term strings, or u32 dictionary
//! codes under the Dictionary layout — and in two scopes:
//!
//! - **Per-chunk** (`append_columns` / `append_encoded_columns`): each
//!   chunk sorts its own quads. Cheap and single-pass, but the concatenation
//!   of several chunks is *not* globally sorted, so the `IsSorted` stat is
//!   stamped only when the chunk spans the whole dataset. The chunk-local sort
//!   still pays off in a file-backed store: `resolve_file` pushes the probe
//!   down as a range predicate, and clustering the values shrinks each zone's
//!   min/max so the scan prunes to the few zones that can hold the probe.
//!   Zones are smaller than a chunk (8192 rows), so this holds within a chunk
//!   even though the whole column is unsorted.
//! - **Global** (`GlobalIndexArrays` and the `append_sorted_*` helpers):
//!   the complete dataset's sorted order, emitted per chunk as consecutive
//!   windows. Every value column is stamped `IsSorted`, and the concatenated
//!   columns stay globally binary-searchable.
//!
//! The two backends read that stamp differently. `resolve_in_memory` needs
//! it: binary search over a concatenation of per-chunk orders would be wrong,
//! so an unstamped column makes it decline and `match_pattern` falls back to a
//! mask scan. `resolve_file` never consults it — the range predicate is
//! correct whatever the order, and sortedness only decides how much prunes.
//!
//! [`IndexType::SecondaryByReference`]: super::IndexType::SecondaryByReference
// Inspired by https://clickhouse.com/blog/projections-secondary-indices

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::dtype::DType;
use vortex_array::{ArrayRef, IntoArray};

use super::{IndexResolution, IndexedComponent, sorted_row_ids};
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::{make_string_array, search_sorted_bounds, stamp_is_sorted};
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{PatternCodes, QuadPattern, ResolvedLayout, TermRef};

pub(crate) const REF_O_COMPONENT: &str = "index:ref-o";
pub(crate) const REF_P_COMPONENT: &str = "index:ref-p";

/// The persisted child's struct dtype: sorted values (strings, or u32 codes
/// under the Dictionary layout) plus the u32 primary row id.
pub(crate) fn ref_child_dtype(encoded: bool) -> DType {
    use vortex_array::dtype::{Nullability, PType, StructFields};
    let val = if encoded {
        DType::Primitive(PType::U32, Nullability::NonNullable)
    } else {
        DType::Utf8(Nullability::NonNullable)
    };
    DType::Struct(
        StructFields::new(
            CHILD_COLUMNS
                .iter()
                .map(|n| (*n).into())
                .collect::<Vec<Arc<str>>>()
                .into(),
            vec![val, DType::Primitive(PType::U32, Nullability::NonNullable)],
        ),
        Nullability::NonNullable,
    )
}

/// One chunk of a reference component's persisted child from a window of its
/// merged `(value, row id)` pairs.
pub(crate) fn ref_child_chunk_strings(pairs: &[(String, u32)]) -> Result<ArrayRef> {
    let val = make_string_array(pairs.iter().map(|(v, _)| v.as_str()));
    stamp_is_sorted(&val);
    ref_child_struct(
        val,
        PrimitiveArray::from_iter(pairs.iter().map(|(_, rid)| *rid)).into_array(),
        pairs.len(),
    )
}

/// Code-column variant of [`ref_child_chunk_strings`].
pub(crate) fn ref_child_chunk_codes(pairs: &[(u32, u32)]) -> Result<ArrayRef> {
    let val = PrimitiveArray::from_iter(pairs.iter().map(|(code, _)| *code)).into_array();
    stamp_is_sorted(&val);
    ref_child_struct(
        val,
        PrimitiveArray::from_iter(pairs.iter().map(|(_, rid)| *rid)).into_array(),
        pairs.len(),
    )
}

fn ref_child_struct(val: ArrayRef, rid: ArrayRef, len: usize) -> Result<ArrayRef> {
    use vortex_array::validity::Validity;
    StructArray::try_new(
        CHILD_COLUMNS
            .iter()
            .map(|n| (*n).into())
            .collect::<Vec<Arc<str>>>()
            .into(),
        vec![val, rid],
        len,
        Validity::NonNullable,
    )
    .map(|a| a.into_array())
    .map_err(VortexRdfError::Vortex)
}

/// Column names inside a reference component's persisted child.
pub(crate) const CHILD_COLUMNS: [&str; 2] = ["val", "rid"];
pub(crate) const CHILD_VAL_COL: &str = "val";
pub(crate) const CHILD_RID_COL: &str = "rid";
pub(crate) const O_IMPLEMENTATION: &str = "secondary-by-reference/o";
pub(crate) const P_IMPLEMENTATION: &str = "secondary-by-reference/p";

/// The child components this index persists: one `{val, rid}` table per
/// covered role, assembled from the row-space `_idx_*` columns.
#[cfg(feature = "file-io")]
pub(crate) fn push_component_specs(
    dtype: &vortex_array::dtype::DType,
    sorted: bool,
    out: &mut Vec<super::IndexComponentSpec>,
) -> crate::error::Result<()> {
    use crate::io::store_layout::{StoreComponentDescriptor, StoreComponentRole};
    for role in RefRole::ALL {
        let source_columns = vec![role.val_col(), role.rid_col()];
        let target_columns = CHILD_COLUMNS.to_vec();
        let child_dtype = super::component_child_dtype(dtype, &source_columns, &target_columns)?;
        out.push(super::IndexComponentSpec {
            descriptor: StoreComponentDescriptor {
                name: role.component_name().into(),
                role: StoreComponentRole::Index,
                implementation: role.component_slug().into(),
                version: 1,
                required: false,
                sorted,
                dtype: child_dtype,
            },
            source_columns,
            target_columns,
        });
    }
    Ok(())
}

/// This index's four columns: a sorted copy of a component's values paired
/// with the primary row id each value came from.
pub(crate) const O_VAL_COL: &str = "_idx_o_val";
pub(crate) const O_RID_COL: &str = "_idx_o_rid";
pub(crate) const P_VAL_COL: &str = "_idx_p_val";
pub(crate) const P_RID_COL: &str = "_idx_p_rid";

/// One of the two quad components this index covers, named after it. Each
/// role owns one `{val, rid}` component — its name, implementation slug, and
/// the row-space column pair it is assembled from — so the association is
/// stated once here instead of at every roster site.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefRole {
    /// Object values.
    O,
    /// Predicate values.
    P,
}

impl RefRole {
    pub(crate) const ALL: [RefRole; 2] = [RefRole::O, RefRole::P];

    /// The persisted child's component name.
    pub(crate) fn component_name(self) -> &'static str {
        match self {
            RefRole::O => REF_O_COMPONENT,
            RefRole::P => REF_P_COMPONENT,
        }
    }

    /// The persisted child's implementation slug.
    pub(crate) fn component_slug(self) -> &'static str {
        match self {
            RefRole::O => O_IMPLEMENTATION,
            RefRole::P => P_IMPLEMENTATION,
        }
    }

    /// The row-space column holding this role's sorted values.
    pub(crate) fn val_col(self) -> &'static str {
        match self {
            RefRole::O => O_VAL_COL,
            RefRole::P => P_VAL_COL,
        }
    }

    /// The row-space column holding the primary row id paired with each value.
    pub(crate) fn rid_col(self) -> &'static str {
        match self {
            RefRole::O => O_RID_COL,
            RefRole::P => P_RID_COL,
        }
    }
}

/// Whether a struct dtype carries this index's four columns — how stores
/// detect the index in an array or file schema without reading any data.
pub(crate) fn is_present(dtype: &DType) -> bool {
    match dtype {
        DType::Struct(fields, _) => {
            let has = |name: &str| fields.names().iter().any(|n| n.as_ref() == name);
            has(O_VAL_COL) && has(O_RID_COL) && has(P_VAL_COL) && has(P_RID_COL)
        }
        _ => false,
    }
}

/// The covered role to probe (which names its component and columns), the
/// term to probe for, and which pattern component a hit resolves.
struct ColumnProbe<'a> {
    role: RefRole,
    probe_term: TermRef<'a>,
    resolves: IndexedComponent,
}

/// The column pair and component this index would use for a pattern shape,
/// independent of any backend — the shared front half of both resolvers.
///
/// A bound subject declines the index: the primary `s` column (binary-searched
/// or zone-pruned) is the better access path there. When both object and
/// predicate are bound, the object side is chosen — object equality is usually
/// the more selective constraint. `None` when nothing this index covers is
/// bound.
fn choose<'a>(pattern: QuadPattern<'a>) -> Option<ColumnProbe<'a>> {
    if pattern.subject.is_some() {
        return None;
    }
    if let Some(object) = pattern.object {
        return Some(ColumnProbe {
            role: RefRole::O,
            probe_term: TermRef::Object(object),
            resolves: IndexedComponent::Object,
        });
    }
    if let Some(predicate) = pattern.predicate {
        return Some(ColumnProbe {
            role: RefRole::P,
            probe_term: TermRef::Predicate(predicate),
            resolves: IndexedComponent::Predicate,
        });
    }
    None
}

/// The in-memory components this index assembles from a welded row space:
/// one per covered role whose `_idx_*` columns are both present.
pub(crate) fn components_from_row_space(
    struct_arr: &StructArray,
    out: &mut Vec<super::IndexComponent>,
) -> crate::error::Result<()> {
    for role in RefRole::ALL {
        if let Some(component) = super::component_from_columns(
            struct_arr,
            role.component_name(),
            role.component_slug(),
            &[role.val_col(), role.rid_col()],
            &CHILD_COLUMNS,
            role.val_col(),
        )? {
            out.push(component);
        }
    }
    Ok(())
}

/// Resolve a pattern against this index's in-memory component.
///
/// Binary-searches the sorted value column for the probe term and slices out
/// the paired row ids — the base rows whose indexed component equals the
/// term. Declines (so the store falls back to a mask scan) when the covered
/// role's component is absent, probe-incompatible, or not globally sorted
/// (`IndexComponent::sorted` — per-chunk sorted data is not
/// binary-searchable).
pub(crate) fn resolve_in_memory(
    components: &[super::IndexComponent],
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution> {
    // Pick the column pair for this pattern shape, or decline it entirely.
    let Some(probe) = choose(pattern) else {
        return Ok(IndexResolution::Declined);
    };
    // Route through the index only when the role's component exists and is
    // globally sorted — the writer's provenance, not a stamp inspection.
    let Some(component) = super::IndexComponent::find(components, probe.role.component_name())
    else {
        return Ok(IndexResolution::Declined);
    };
    if !component.sorted {
        return Ok(IndexResolution::Declined);
    }
    // Translate the term to the value column's native probe value (a string, or
    // a dictionary code). Absent from the dictionary ⇒ nothing can match. The
    // probe term is the pattern's own predicate or object, so this shares the
    // match's resolution cache rather than searching the dictionary again.
    let Some(native) = layout.probe_scalar_cached(probe.probe_term, codes) else {
        return Ok(IndexResolution::Empty);
    };
    let rows = &component.array;
    let Ok(val_col) = rows.unmasked_field_by_name(CHILD_VAL_COL) else {
        return Ok(IndexResolution::Declined);
    };
    let Ok(scalar) = native.cast(val_col.dtype()) else {
        return Ok(IndexResolution::Declined);
    };
    // Left/right binary search bounds the [lo, hi) run of rows equal to the
    // probe; an empty run means the term is present in the schema but absent
    // from the data.
    let (lo, hi) = search_sorted_bounds(val_col, &scalar)?;
    if lo == hi {
        return Ok(IndexResolution::Empty);
    }
    // Row ids of every quad whose indexed component equals the probe term.
    // They come out in the index's order (the rid column is ordered by value,
    // not by row), so `sorted_row_ids` puts them back in base row order.
    let row_ids = sorted_row_ids(
        rows.unmasked_field_by_name(CHILD_RID_COL)
            .map_err(VortexRdfError::Vortex)?
            .slice(lo..hi)
            .map_err(VortexRdfError::Vortex)?,
    )?;
    Ok(IndexResolution::Resolved {
        row_ids,
        resolves: probe.resolves,
        // A back-reference index stores no whole quads to serve from.
        serve: None,
    })
}

/// Resolve a pattern against this index's columns in a file-backed store — the
/// file counterpart of [`resolve_in_memory`], reaching the columns through a
/// pushed-down scan instead of an in-memory binary search.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_file(
    file: &crate::io::native_file::NativeStoreFile,
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution> {
    let Some(probe) = choose(pattern) else {
        return Ok(IndexResolution::Declined);
    };
    // Map the probed role onto its persisted child; be graceful when the
    // child is absent (a foreign writer could omit one role).
    let Some((_, reader)) = file
        .component_reader(probe.role.component_name())
        .map_err(VortexRdfError::Vortex)?
    else {
        return Ok(IndexResolution::Declined);
    };
    // Term absent from the dictionary ⇒ the pattern provably matches nothing.
    let Some(native) = layout.probe_scalar_cached(probe.probe_term, codes) else {
        return Ok(IndexResolution::Empty);
    };
    let row_ids =
        super::scan_index_row_ids(reader, &[(CHILD_VAL_COL, native)], CHILD_RID_COL).await?;
    if row_ids.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    Ok(IndexResolution::Resolved {
        row_ids,
        resolves: probe.resolves,
        // A back-reference index stores no whole quads to serve from.
        serve: None,
    })
}

/// Append the four reference secondary-index columns for one chunk, sorting
/// the chunk's own quads: `_idx_o_val`/`_idx_o_rid` (sorted objects) and
/// `_idx_p_val`/`_idx_p_rid` (sorted predicates).
///
/// `start_row` is the global row ID of the first quad in `quads`, so per-chunk
/// index builders can emit row IDs that address the fully assembled array.
/// An empty `quads` slice yields empty columns with the correct dtypes.
///
/// `whole_dataset` must be `true` only when `quads` is the entire dataset
/// (single-chunk builds): the chunk-local sort is then the global order and
/// the value columns are stamped `IsSorted` for binary-search routing.
pub(crate) fn append_columns(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    quads: &[RawQuad],
    start_row: u32,
    whole_dataset: bool,
) {
    let sorted_pairs = |term_of: fn(&RawQuad) -> &str| -> Vec<(&str, u32)> {
        let mut pairs: Vec<(&str, u32)> = quads
            .iter()
            .enumerate()
            .map(|(i, q)| (term_of(q), start_row + i as u32))
            .collect();
        pairs.sort_unstable();
        pairs
    };
    append_sorted_string_pairs(
        field_names,
        field_arrays,
        &sorted_pairs(|q| &q.o),
        &sorted_pairs(|q| &q.p),
        whole_dataset,
    );
}

/// Dictionary-layout variant of [`append_columns`]: `_idx_o_val`/`_idx_p_val`
/// hold the terms' u32 dictionary codes instead of strings. Sorting codes is
/// order-equivalent to sorting the term strings (sorted-dictionary codes are
/// lexicographic ranks), so the index stays binary-searchable — queries
/// translate the pattern term to its code first.
pub(crate) fn append_encoded_columns(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    codes: &QuadCodes,
    start_row: u32,
    whole_dataset: bool,
) {
    let sorted_pairs = |column: &[u32]| -> Vec<(u32, u32)> {
        let mut pairs: Vec<(u32, u32)> = column
            .iter()
            .enumerate()
            .map(|(i, &code)| (code, start_row + i as u32))
            .collect();
        pairs.sort_unstable();
        pairs
    };
    append_sorted_code_pairs(
        field_names,
        field_arrays,
        &sorted_pairs(&codes.o),
        &sorted_pairs(&codes.p),
        whole_dataset,
    );
}

/// Append the four index columns from already-sorted (term, row ID) pairs.
/// Out-of-core builders call this directly with pairs merged from disk runs
/// in global order (`stamp_sorted = true`).
pub(crate) fn append_sorted_string_pairs(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    o_pairs: &[(impl AsRef<str>, u32)],
    p_pairs: &[(impl AsRef<str>, u32)],
    stamp_sorted: bool,
) {
    let o_val = make_string_array(o_pairs.iter().map(|(s, _)| s.as_ref()));
    let p_val = make_string_array(p_pairs.iter().map(|(s, _)| s.as_ref()));
    if stamp_sorted {
        stamp_is_sorted(&o_val);
        stamp_is_sorted(&p_val);
    }
    field_names.extend_from_slice(&[
        O_VAL_COL.into(),
        O_RID_COL.into(),
        P_VAL_COL.into(),
        P_RID_COL.into(),
    ]);
    field_arrays.extend([
        o_val,
        PrimitiveArray::from_iter(o_pairs.iter().map(|(_, rid)| *rid)).into_array(),
        p_val,
        PrimitiveArray::from_iter(p_pairs.iter().map(|(_, rid)| *rid)).into_array(),
    ]);
}

/// Code-column variant of [`append_sorted_string_pairs`].
pub(crate) fn append_sorted_code_pairs(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    o_pairs: &[(u32, u32)],
    p_pairs: &[(u32, u32)],
    stamp_sorted: bool,
) {
    let o_val = PrimitiveArray::from_iter(o_pairs.iter().map(|(code, _)| *code)).into_array();
    let p_val = PrimitiveArray::from_iter(p_pairs.iter().map(|(code, _)| *code)).into_array();
    if stamp_sorted {
        stamp_is_sorted(&o_val);
        stamp_is_sorted(&p_val);
    }
    field_names.extend_from_slice(&[
        O_VAL_COL.into(),
        O_RID_COL.into(),
        P_VAL_COL.into(),
        P_RID_COL.into(),
    ]);
    field_arrays.extend([
        o_val,
        PrimitiveArray::from_iter(o_pairs.iter().map(|(_, rid)| *rid)).into_array(),
        p_val,
        PrimitiveArray::from_iter(p_pairs.iter().map(|(_, rid)| *rid)).into_array(),
    ]);
}

/// The complete dataset's secondary-index columns in global sorted order,
/// built once by in-memory builders and sliced per chunk: chunk `i` carries
/// window `[i·C, (i+1)·C)` of the same order, so the concatenation across
/// chunks is itself the globally sorted index.
pub(crate) struct GlobalIndexArrays {
    o_val: ArrayRef,
    o_rid: ArrayRef,
    p_val: ArrayRef,
    p_rid: ArrayRef,
}

impl GlobalIndexArrays {
    /// Sort by term strings. Row IDs are the quads' positions in `quads`
    /// (the builder must pass the dataset in final row order), so the sort is
    /// just a u32 permutation — no per-term string copies.
    pub(crate) fn from_quads(quads: &[RawQuad]) -> Self {
        let perm_by = |term_of: fn(&RawQuad) -> &str| -> Vec<u32> {
            let mut perm: Vec<u32> = (0..quads.len() as u32).collect();
            perm.sort_unstable_by(|&a, &b| {
                term_of(&quads[a as usize]).cmp(term_of(&quads[b as usize]))
            });
            perm
        };
        let o_perm = perm_by(|q| &q.o);
        let p_perm = perm_by(|q| &q.p);
        Self::from_arrays(
            make_string_array(o_perm.iter().map(|&i| quads[i as usize].o.as_str())),
            o_perm,
            make_string_array(p_perm.iter().map(|&i| quads[i as usize].p.as_str())),
            p_perm,
        )
    }

    /// Dictionary-layout variant: sort the u32 codes.
    pub(crate) fn from_codes(codes: &QuadCodes) -> Self {
        let sorted = |column: &[u32]| -> (ArrayRef, Vec<u32>) {
            let mut pairs: Vec<(u32, u32)> = column
                .iter()
                .enumerate()
                .map(|(i, &code)| (code, i as u32))
                .collect();
            pairs.sort_unstable();
            (
                PrimitiveArray::from_iter(pairs.iter().map(|(code, _)| *code)).into_array(),
                pairs.into_iter().map(|(_, rid)| rid).collect(),
            )
        };
        let (o_val, o_perm) = sorted(&codes.o);
        let (p_val, p_perm) = sorted(&codes.p);
        Self::from_arrays(o_val, o_perm, p_val, p_perm)
    }

    fn from_arrays(o_val: ArrayRef, o_perm: Vec<u32>, p_val: ArrayRef, p_perm: Vec<u32>) -> Self {
        stamp_is_sorted(&o_val);
        stamp_is_sorted(&p_val);
        Self {
            o_val,
            o_rid: PrimitiveArray::from_iter(o_perm).into_array(),
            p_val,
            p_rid: PrimitiveArray::from_iter(p_perm).into_array(),
        }
    }

    /// Append window `range` of the global order as one chunk's index columns.
    /// Value slices are re-stamped `IsSorted` (a slice of a sorted array is
    /// sorted, but slicing does not propagate the stat).
    pub(crate) fn append_slice(
        &self,
        field_names: &mut Vec<Arc<str>>,
        field_arrays: &mut Vec<ArrayRef>,
        range: Range<usize>,
    ) -> Result<()> {
        for (name, arr, is_val) in [
            (O_VAL_COL, &self.o_val, true),
            (O_RID_COL, &self.o_rid, false),
            (P_VAL_COL, &self.p_val, true),
            (P_RID_COL, &self.p_rid, false),
        ] {
            let sliced = arr.slice(range.clone()).map_err(VortexRdfError::Vortex)?;
            if is_val {
                stamp_is_sorted(&sliced);
            }
            field_names.push(name.into());
            field_arrays.push(sliced);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{Literal, NamedNode, NamedOrBlankNode, Term};

    #[test]
    fn choose_component_selection() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("o"));

        // A bound subject declines: the primary sorted `s` column is the
        // better access path than this index.
        assert!(choose(QuadPattern::new(Some(&s), Some(&p), Some(&o), None)).is_none());

        // Object preferred over predicate when both are bound.
        let probe = choose(QuadPattern::new(None, Some(&p), Some(&o), None)).unwrap();
        assert_eq!(probe.resolves, IndexedComponent::Object);
        assert_eq!(probe.role.val_col(), "_idx_o_val");
        assert_eq!(probe.probe_term.to_string(), o.to_string());

        // Predicate-only patterns use the predicate side.
        let probe = choose(QuadPattern::new(None, Some(&p), None, None)).unwrap();
        assert_eq!(probe.resolves, IndexedComponent::Predicate);
        assert_eq!(probe.role.val_col(), "_idx_p_val");
        assert_eq!(probe.probe_term.to_string(), p.to_string());

        // Nothing this index covers is bound: declines.
        assert!(choose(QuadPattern::new(None, None, None, None)).is_none());
    }
}

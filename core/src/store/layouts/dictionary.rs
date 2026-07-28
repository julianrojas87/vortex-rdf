//! Column-building and decoding logic for [`LayoutStrategy::Dictionary`]:
//! s/p/o/g stored as u32 codes into a global sorted term dictionary, which
//! itself lives in the layout's intrinsic `_dict_terms` column (see
//! [`super::term_dictionary`]).
//!
//! Unlike the other layouts, chunks are not built through the generic
//! `build_struct_array` path: encoding requires the global `TermDictionary`
//! (complete only after the whole dataset has been ingested), so the builders
//! run a dedicated two-pass pipeline that calls `build_chunk` directly.
//! Secondary indexes compose normally: they are appended per chunk via
//! `IndexType::append_dictionary_columns`, working on the encoded codes.
//!
//! [`LayoutStrategy::Dictionary`]: super::LayoutStrategy::Dictionary

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;
use web_time::Instant;

use oxrdf::Quad;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{ChunkedArray, ConstantArray, MaskedArray, PrimitiveArray};
use vortex_array::dtype::{DType, Nullability};
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use super::default::decode_spog;
use super::term_dictionary::{DictReader, TERM_FIELD, TermDictionary, TermIdMap};
use crate::common::utils::stamp_is_sorted;
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::indexes::secondary_by_copy::CopyKey;
use crate::store::indexes::{GlobalIndexes, IndexType, unique_indexes};
use crate::store::{QuadCodes, RawQuad};

/// Field names of the primary columns: `s`, `p`, `o`, `g` (all u32 codes).
pub(crate) fn field_names() -> Vec<Arc<str>> {
    vec!["s".into(), "p".into(), "o".into(), "g".into()]
}

/// Encode every term of every quad to its dictionary code.
///
/// Generic over the map's key so both the owned [`TermIdMap`] (streaming
/// builders) and the borrowed [`BorrowedTermIdMap`] (builders holding a live
/// quad slice) work without a second code path — `&str: Borrow<str>` makes the
/// `get(term)` lookup identical for either.
///
/// [`BorrowedTermIdMap`]: super::term_dictionary::BorrowedTermIdMap
pub(crate) fn encode_quads<K>(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &HashMap<K, u32>,
) -> Result<QuadCodes>
where
    K: Borrow<str> + Eq + Hash,
{
    let start = Instant::now();
    let encode_column = |term_of: fn(&RawQuad) -> &str| -> Result<Vec<u32>> {
        let mut ids: Vec<u32> = Vec::with_capacity(quads.len());
        for q in quads {
            let term = term_of(q);
            ids.push(id_map.get(term).copied().ok_or_else(|| {
                VortexRdfError::Serialization(format!(
                    "Term missing from dictionary during encoding: {}",
                    term
                ))
            })?);
        }
        Ok(ids)
    };
    let codes = QuadCodes {
        s: encode_column(|q| &q.s)?,
        p: encode_column(|q| &q.p)?,
        o: encode_column(|q| &q.o)?,
        g: encode_column(|q| &q.g)?,
    };
    log::debug!(
        "[Dictionary] Encoded {} quads ({} term lookups, {} dictionary terms) in {:?}",
        quads.len(),
        quads.len().saturating_mul(4),
        dict.len(),
        start.elapsed()
    );
    Ok(codes)
}

/// Build the primary part of a Dictionary-layout chunk — the four u32 code
/// columns — returning the open field vectors so the caller can append index
/// columns before finalizing.
///
/// The term dictionary is *not* a column of the chunk: in memory it lives in
/// the layout ([`DictAccess`]), and serialized forms carry it as trailing
/// dictionary rows or a sidecar file (see [`pad_with_dictionary`]).
/// `s_sorted` stamps the `IsSorted` statistic on the `s` column; valid
/// because sorted-dictionary codes preserve lexicographic order.
///
/// [`DictAccess`]: super::term_dictionary::DictAccess
fn chunk_parts(
    codes: &QuadCodes,
    range: Range<usize>,
    s_sorted: bool,
) -> (Vec<Arc<str>>, Vec<ArrayRef>) {
    let names = field_names();
    let arrays: Vec<ArrayRef> = vec![
        PrimitiveArray::from_iter(codes.s[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.p[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.o[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.g[range].iter().copied()).into_array(),
    ];

    if s_sorted {
        stamp_is_sorted(&arrays[0]);
    }

    (names, arrays)
}

fn finish_chunk(names: Vec<Arc<str>>, arrays: Vec<ArrayRef>, n: usize) -> Result<ArrayRef> {
    StructArray::try_new(names.into(), arrays, n, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// Build a complete Dictionary-layout StructArray chunk: four u32 code columns
/// encoded against the global dictionary, the `_dict_terms` column, and the
/// columns of every requested secondary index (deduplicated, built over the
/// same codes by sorting this chunk's quads).
///
/// `start_row` has the same global-row-ID semantics as `build_struct_array`,
/// and `whole_dataset` the same index-stamping semantics: pass `true` only
/// when `quads` is the entire dataset, so the per-chunk index sort is the
/// global order.
pub(crate) fn build_chunk<K>(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &HashMap<K, u32>,
    indexes: &[IndexType],
    start_row: u32,
    s_sorted: bool,
    whole_dataset: bool,
) -> Result<ArrayRef>
where
    K: Borrow<str> + Eq + Hash,
{
    let total_start = Instant::now();
    let n = quads.len();
    let encode_start = Instant::now();
    let codes = encode_quads(quads, dict, id_map)?;
    let encode_elapsed = encode_start.elapsed();
    let primary_start = Instant::now();
    let (mut names, mut arrays) = chunk_parts(&codes, 0..n, s_sorted);
    let primary_elapsed = primary_start.elapsed();

    let indexes_start = Instant::now();
    for idx in unique_indexes(indexes) {
        idx.append_dictionary_columns(&mut names, &mut arrays, &codes, start_row, whole_dataset);
    }
    let indexes_elapsed = indexes_start.elapsed();

    let finish_start = Instant::now();
    let chunk = finish_chunk(names, arrays, n)?;
    log::debug!(
        "[Dictionary] Built chunk of {} rows at row {}: encode {:?}, primary columns {:?}, indexes {:?}, struct {:?}, total {:?}",
        n,
        start_row,
        encode_elapsed,
        primary_elapsed,
        indexes_elapsed,
        finish_start.elapsed(),
        total_start.elapsed()
    );
    Ok(chunk)
}

/// Build the whole dataset as one contiguous Dictionary-layout chunk from its
/// codes — the in-memory builders' construction path, fed by the interning
/// ingest ([`InterningQuadBuilder`]) so no owned quad strings are involved.
///
/// `s_sorted` follows the builder: the sorted builder passes codes in global
/// (s, p, o, g) order, the unsorted builder in arrival order. Index columns
/// are globally sorted either way (`GlobalIndexes::from_codes` sorts pairs).
///
/// [`InterningQuadBuilder`]: super::term_dictionary::InterningQuadBuilder
pub(crate) fn build_array(
    codes: &QuadCodes,
    indexes: &[IndexType],
    s_sorted: bool,
) -> Result<ArrayRef> {
    if codes.s.is_empty() {
        return empty_struct(indexes);
    }
    let n = codes.s.len();
    let global_idx = GlobalIndexes::from_codes(indexes, codes);
    build_chunk_global(codes, 0..n, &global_idx, s_sorted)
}

/// Build a Dictionary-layout chunk for rows `range` of a fully encoded
/// dataset, with index columns sliced from the precomputed global order —
/// the sorted in-memory builders' chunked emission path.
pub(crate) fn build_chunk_global(
    codes: &QuadCodes,
    range: Range<usize>,
    global_indexes: &GlobalIndexes,
    s_sorted: bool,
) -> Result<ArrayRef> {
    let start = Instant::now();
    let n = range.len();
    let (mut names, mut arrays) = chunk_parts(codes, range.clone(), s_sorted);
    global_indexes.append_slice(&mut names, &mut arrays, range)?;
    let chunk = finish_chunk(names, arrays, n)?;
    log::debug!(
        "[Dictionary] Built globally encoded chunk of {} rows in {:?}",
        n,
        start.elapsed()
    );
    Ok(chunk)
}

/// The two `SecondaryByReference` code columns of a presorted chunk, as
/// borrowed globally sorted (code, row ID) slices: (objects, predicates).
type RefPairSlices<'a> = (&'a [(u32, u32)], &'a [(u32, u32)]);
/// The two `SecondaryByCopy` code columns of a presorted chunk, as borrowed
/// globally sorted (sort key, row ID) slices: (POSG, OSPG).
type CopyKeySlices<'a> = (&'a [(CopyKey<u32>, u32)], &'a [(CopyKey<u32>, u32)]);

/// Build a Dictionary-layout chunk with index columns taken from
/// already-globally-sorted (code, row ID) entries — the out-of-core sorted
/// builder's emission path, where the entries are merged from disk runs.
/// Each index family is appended only when its entries are supplied.
pub(crate) fn build_chunk_presorted_indexes(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &TermIdMap,
    ref_pairs: Option<RefPairSlices<'_>>,
    copy_keys: Option<CopyKeySlices<'_>>,
    s_sorted: bool,
) -> Result<ArrayRef> {
    use crate::store::indexes::secondary_by_copy::append_sorted_code_keys;
    use crate::store::indexes::secondary_by_reference::append_sorted_code_pairs;

    let total_start = Instant::now();
    let n = quads.len();
    let encode_start = Instant::now();
    let codes = encode_quads(quads, dict, id_map)?;
    let encode_elapsed = encode_start.elapsed();
    let (mut names, mut arrays) = chunk_parts(&codes, 0..n, s_sorted);
    if let Some((posg, ospg)) = copy_keys {
        append_sorted_code_keys(&mut names, &mut arrays, posg, ospg, true);
    }
    if let Some((o_pairs, p_pairs)) = ref_pairs {
        append_sorted_code_pairs(&mut names, &mut arrays, o_pairs, p_pairs, true);
    }
    let chunk = finish_chunk(names, arrays, n)?;
    log::debug!(
        "[Dictionary] Built presorted-index chunk of {} rows: encode {:?}, remaining build {:?}, total {:?}",
        n,
        encode_elapsed,
        total_start.elapsed().saturating_sub(encode_elapsed),
        total_start.elapsed()
    );
    Ok(chunk)
}

/// An empty StructArray with the Dictionary-layout schema (including the
/// columns of any requested secondary indexes).
pub(crate) fn empty_struct(indexes: &[IndexType]) -> Result<ArrayRef> {
    build_chunk(
        &[],
        &TermDictionary::empty(),
        &TermIdMap::new(),
        indexes,
        0,
        false,
        false,
    )
}

/// Make a Dictionary-layout array self-describing for serialization by
/// appending its term dictionary as trailing rows — the *padded* form.
///
/// The quad columns keep their dtypes; one nullable utf8 column
/// ([`TERM_FIELD`]) is added, null on every quad row and holding the sorted
/// terms on the `dict.len()` dictionary rows appended after them (where every
/// quad/index column is a constant zero — a sentinel, not null, so those
/// columns stay non-nullable). A term's ID is its position among the
/// dictionary rows; the split point is recovered from the term column's
/// validity, whose valid run is exactly the tail ([`split_padded`]).
///
/// Readers must never surface the dictionary rows as quads: opens split them
/// off ([`split_padded`]) or restrict scans to the quad rows.
pub(crate) fn pad_with_dictionary(array: &ArrayRef, dict: &TermDictionary) -> Result<ArrayRef> {
    let quad_chunk = append_null_term_column(array)?;
    if dict.len() == 0 {
        return Ok(quad_chunk);
    }
    let tail = dict_tail_chunk(array.dtype(), dict)?;
    ChunkedArray::try_new(vec![quad_chunk.clone(), tail], quad_chunk.dtype().clone())
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// The padded schema for a quads schema: the same fields plus the nullable
/// term column.
pub(crate) fn padded_dtype(quad_dtype: &DType) -> Result<DType> {
    let DType::Struct(fields, nullability) = quad_dtype else {
        return Err(VortexRdfError::Serialization(
            "Dictionary-layout schema is not a struct".to_string(),
        ));
    };
    let mut names: Vec<Arc<str>> = fields.names().iter().map(|n| n.inner().clone()).collect();
    let mut dtypes: Vec<DType> = names
        .iter()
        .map(|n| {
            fields.field(n.as_ref()).ok_or_else(|| {
                VortexRdfError::Serialization(format!("schema field {n} has no dtype"))
            })
        })
        .collect::<Result<_>>()?;
    names.push(TERM_FIELD.into());
    dtypes.push(DType::Utf8(Nullability::Nullable));
    Ok(DType::Struct(
        vortex_array::dtype::StructFields::new(names.into_iter().collect(), dtypes),
        *nullability,
    ))
}

/// Append an all-null term column to one quads chunk — the per-chunk half of
/// the padded form.
pub(crate) fn append_null_term_column(chunk: &ArrayRef) -> Result<ArrayRef> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let n = struct_arr.len();
    let names: Vec<Arc<str>> = match chunk.dtype() {
        DType::Struct(fields, _) => fields.names().iter().map(|f| f.inner().clone()).collect(),
        _ => {
            return Err(VortexRdfError::Serialization(
                "Dictionary-layout chunk is not a struct".to_string(),
            ));
        }
    };
    let mut fields: Vec<ArrayRef> = names
        .iter()
        .map(|name| {
            struct_arr
                .unmasked_field_by_name(name.as_ref())
                .cloned()
                .map_err(VortexRdfError::Vortex)
        })
        .collect::<Result<_>>()?;
    let mut names = names;
    names.push(TERM_FIELD.into());
    fields
        .push(ConstantArray::new(Scalar::null(DType::Utf8(Nullability::Nullable)), n).into_array());
    finish_chunk(names, fields, n)
}

/// The trailing dictionary chunk for a quads schema: a zero sentinel for
/// every quad/index column (not null — those columns stay non-nullable) and
/// the sorted terms, wrapped nullable and all valid, for the term column.
pub(crate) fn dict_tail_chunk(quad_dtype: &DType, dict: &TermDictionary) -> Result<ArrayRef> {
    let m = dict.len();
    let DType::Struct(fields, _) = quad_dtype else {
        return Err(VortexRdfError::Serialization(
            "Dictionary-layout schema is not a struct".to_string(),
        ));
    };
    let mut names: Vec<Arc<str>> = fields.names().iter().map(|f| f.inner().clone()).collect();
    let mut arrays: Vec<ArrayRef> = names
        .iter()
        .map(|name| {
            let dtype = fields.field(name.as_ref()).ok_or_else(|| {
                VortexRdfError::Serialization(format!("schema field {name} has no dtype"))
            })?;
            Ok(ConstantArray::new(Scalar::default_value(&dtype), m).into_array())
        })
        .collect::<Result<_>>()?;
    let terms = MaskedArray::try_new(dict.terms_array(), Validity::AllValid)
        .map_err(VortexRdfError::Vortex)?
        .into_array();
    names.push(TERM_FIELD.into());
    arrays.push(terms);
    finish_chunk(names, arrays, m)
}

/// Split a padded array ([`pad_with_dictionary`]) back into its pure quad
/// rows (term column dropped) and its term dictionary.
///
/// The split point comes from the term column's validity: the valid rows are
/// the dictionary tail, and this validates that they form exactly one run at
/// the end.
pub(crate) fn split_padded(array: &ArrayRef) -> Result<(ArrayRef, TermDictionary)> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let term_col = struct_arr
        .unmasked_field_by_name(TERM_FIELD)
        .map_err(VortexRdfError::Vortex)?
        .clone();
    let (n, dict) = split_term_column(&term_col, &mut ctx)?;

    // The quad rows, with the term column dropped.
    let names: Vec<Arc<str>> = match array.dtype() {
        DType::Struct(fields, _) => fields
            .names()
            .iter()
            .filter(|f| f.as_ref() != TERM_FIELD)
            .map(|f| f.inner().clone())
            .collect(),
        _ => unreachable!("struct execution above guarantees a struct dtype"),
    };
    let quad_fields: Vec<ArrayRef> = names
        .iter()
        .map(|name| {
            struct_arr
                .unmasked_field_by_name(name.as_ref())
                .map_err(VortexRdfError::Vortex)?
                .slice(0..n)
                .map_err(VortexRdfError::Vortex)
        })
        .collect::<Result<_>>()?;
    let quads = finish_chunk(names, quad_fields, n)?;
    Ok((quads, dict))
}

/// Split a serialized term column into the quad row count and the lifted
/// dictionary: the valid rows are the dictionary tail (validated to be
/// exactly one run at the end), and everything before them is quads.
pub(crate) fn split_term_column(
    term_col: &ArrayRef,
    ctx: &mut vortex_array::ExecutionCtx,
) -> Result<(usize, TermDictionary)> {
    let total = term_col.len();
    let mask = term_col
        .validity()
        .map_err(VortexRdfError::Vortex)?
        .execute_mask(total, ctx)
        .map_err(VortexRdfError::Vortex)?;
    let m = mask.true_count();
    let n = total - m;
    // The valid run must be exactly the tail — anything else is corrupt.
    if mask.to_bit_buffer().iter().take(n).any(|v| v) {
        return Err(VortexRdfError::Deserialization(
            "padded dictionary rows are not a contiguous tail".to_string(),
        ));
    }
    let dict = if m == 0 {
        TermDictionary::empty()
    } else {
        TermDictionary::from_terms_array(term_tail(term_col, n, total)?, ctx)?
    };
    Ok((n, dict))
}

/// The dictionary tail of a serialized term column.
///
/// When the tail is exactly the column's final chunk — the shape
/// [`pad_with_dictionary`] writes — that chunk is taken directly: a
/// `slice(n..total)` would wrap it in a lazy slice node, which defeats the
/// FSST downcast on open and canonicalizes (decompresses) the dictionary.
pub(crate) fn term_tail(term_col: &ArrayRef, n: usize, total: usize) -> Result<ArrayRef> {
    use vortex_array::arrays::chunked::ChunkedArrayExt as _;
    if let Ok(chunked) = term_col
        .clone()
        .try_downcast::<vortex_array::arrays::Chunked>()
        && let Some(last) = chunked.nchunks().checked_sub(1)
    {
        let chunk = chunked.chunk(last);
        if chunk.len() == total - n {
            return Ok(chunk.clone());
        }
    }
    term_col.slice(n..total).map_err(VortexRdfError::Vortex)
}

/// Decode a Dictionary-layout StructArray chunk into Quads using the given
/// (store-cached) dictionary. The chunk's own `_dict_terms` column is ignored —
/// derived chunks (sliced/filtered/file-scanned) may have lost the payload row.
pub(crate) fn decode_chunk(chunk: &ArrayRef, dict: &TermDictionary) -> Vec<Result<Quad>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();

    let struct_arr = match chunk.clone().execute::<StructArray>(&mut ctx) {
        Ok(a) => a,
        Err(e) => return vec![Err(VortexRdfError::Vortex(e))],
    };

    let n = struct_arr.len();

    macro_rules! get_u32_col {
        ($name:expr) => {
            match struct_arr
                .unmasked_field_by_name($name)
                .map_err(VortexRdfError::Vortex)
                .and_then(|c| {
                    c.clone()
                        .execute::<PrimitiveArray>(&mut ctx)
                        .map_err(VortexRdfError::Vortex)
                }) {
                Ok(arr) => arr,
                Err(e) => return vec![Err(e)],
            }
        };
    }

    let s_col = get_u32_col!("s");
    let p_col = get_u32_col!("p");
    let o_col = get_u32_col!("o");
    let g_col = get_u32_col!("g");

    let s_ids = s_col.as_slice::<u32>();
    let p_ids = p_col.as_slice::<u32>();
    let o_ids = o_col.as_slice::<u32>();
    let g_ids = g_col.as_slice::<u32>();

    // One reader per role: an FSST read decodes into the reader's scratch, so
    // the four borrows a quad needs at once must come from four readers.
    let (mut rs, mut rp, mut ro, mut rg) =
        (dict.reader(), dict.reader(), dict.reader(), dict.reader());
    let n_terms = dict.len();
    let term_at = |reader: &mut DictReader<'_>, id: u32| -> Result<String> {
        if (id as usize) < n_terms {
            reader.str_at(id as usize).map(str::to_owned)
        } else {
            Err(VortexRdfError::Deserialization(format!(
                "Term code {} out of dictionary bounds ({})",
                id, n_terms
            )))
        }
    };

    (0..n)
        .map(|i| {
            // Zero-copy views over the dictionary's term bytes; the oxrdf
            // constructors make the single owned copy.
            decode_spog(
                &term_at(&mut rs, s_ids[i])?,
                &term_at(&mut rp, p_ids[i])?,
                &term_at(&mut ro, o_ids[i])?,
                &term_at(&mut rg, g_ids[i])?,
            )
        })
        .collect()
}

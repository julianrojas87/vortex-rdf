//! Column-building and decoding logic for [`LayoutStrategy::Dictionary`]:
//! s/p/o/g stored as u32 codes into a global sorted term dictionary (see
//! [`super::term_dictionary`]), which travels beside the array in memory and
//! reaches serialized files as an embedded metadata-segment blob (see
//! [`crate::io::embedded`]).
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
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use futures::{Stream, StreamExt};

use super::default::decode_spog;
use super::term_dictionary::{DictReader, InterningQuadBuilder, TermDictionary, TermIdMap};
use crate::common::array::stamp_is_sorted;
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::builders::BuiltArray;
use crate::store::indexes::secondary_by_copy::CopyKey;
use crate::store::indexes::{GlobalIndexes, IndexType, Indexes, unique_indexes};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS};

/// Field names of the primary columns: `s`, `p`, `o`, `g` (all u32 codes).
pub(crate) fn field_names() -> Vec<Arc<str>> {
    PRIMARY_COLUMNS.iter().map(|&n| n.into()).collect()
}

/// Dictionary-encoded quad columns: [`RawQuad`] terms replaced by their u32
/// codes in the global sorted term dictionary. Produced by the Dictionary
/// layout's encoding pass and consumed by index builders, which can work on
/// codes directly (sorted-dictionary codes preserve lexicographic order).
pub(crate) struct QuadCodes {
    pub s: Vec<u32>,
    pub p: Vec<u32>,
    pub o: Vec<u32>,
    pub g: Vec<u32>,
}

/// Drain a quad stream into an [`InterningQuadBuilder`]: each quad's Strings
/// die here, leaving one copy of every distinct term plus 16 bytes per quad.
///
/// The Dictionary-layout in-memory ingest for both the sorted and unsorted
/// builders; `finish(sort)` then yields the dictionary and the coded quads.
pub(crate) async fn ingest_interning(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
) -> Result<InterningQuadBuilder> {
    let mut interner = InterningQuadBuilder::new();
    while let Some(res) = quads_in.next().await {
        interner.push(res?);
    }
    Ok(interner)
}

/// Push-based Dictionary-layout ingest for callers that produce quads one at
/// a time rather than as a `'static` stream — the wasm array path, whose
/// quads are decoded chunk-by-chunk from a packed JS buffer.
///
/// Feeding those quads through the stream builders would require collecting
/// them into a full `Vec<RawQuad>` first (a `'static` stream cannot borrow
/// from the decode loop), resurrecting exactly the four-Strings-per-quad
/// ingest high-water the interning ingest removes. Pushing into the sink
/// instead lets each quad's Strings die on arrival; `finish` builds the same
/// single-chunk array `SortedInMemoryBuilder`/`UnsortedStreamBuilder`
/// produce for the Dictionary layout.
pub struct DictionaryQuadSink {
    interner: InterningQuadBuilder,
    indexes: Indexes,
    /// Sort the coded quads by (s, p, o, g) in `finish` —
    /// [`BuilderStrategy::SortedInMemory`]'s semantics; `false` keeps arrival
    /// order ([`BuilderStrategy::UnsortedStream`]).
    ///
    /// [`BuilderStrategy::SortedInMemory`]: crate::store::BuilderStrategy::SortedInMemory
    /// [`BuilderStrategy::UnsortedStream`]: crate::store::BuilderStrategy::UnsortedStream
    sorted: bool,
}

impl DictionaryQuadSink {
    pub fn new(sorted: bool, indexes: Indexes) -> Self {
        Self {
            interner: InterningQuadBuilder::new(),
            indexes,
            sorted,
        }
    }

    /// Consume one quad: intern its four terms, keep only their ids.
    pub fn push(&mut self, quad: RawQuad) {
        self.interner.push(quad);
    }

    /// Freeze the dictionary and build the single-chunk Dictionary-layout
    /// array, exactly as the corresponding stream builder would.
    pub fn finish(self) -> Result<BuiltArray> {
        let (dict, codes) = self.interner.finish(self.sorted)?;
        let array = build_array(&codes, &self.indexes, self.sorted)?;
        Ok(BuiltArray {
            array,
            dict: Some(Arc::new(dict)),
        })
    }
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
/// the layout ([`DictAccess`]), and serialized files carry it as an embedded
/// metadata-segment blob.
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
/// encoded against the global dictionary, plus the columns of every requested
/// secondary index (deduplicated, built over the same codes by sorting this
/// chunk's quads).
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

/// Decode a Dictionary-layout StructArray chunk into Quads using the given
/// (store-cached) dictionary.
pub(crate) fn decode_chunk(chunk: &ArrayRef, dict: &TermDictionary) -> Vec<Result<Quad>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();

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

    let s_col = get_u32_col!(COL_S);
    let p_col = get_u32_col!(COL_P);
    let o_col = get_u32_col!(COL_O);
    let g_col = get_u32_col!(COL_G);

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

/// The distinct term codes a chunk's four code columns reference, ascending —
/// what a file-backed dictionary must resolve to decode the chunk.
#[cfg(feature = "file-io")]
pub(crate) fn unique_codes(chunk: &ArrayRef) -> Result<Vec<u32>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let mut codes: Vec<u32> = Vec::with_capacity(struct_arr.len().saturating_mul(4));
    for name in PRIMARY_COLUMNS {
        let col = struct_arr
            .unmasked_field_by_name(name)
            .map_err(VortexRdfError::Vortex)?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        codes.extend_from_slice(col.as_slice::<u32>());
    }
    codes.sort_unstable();
    codes.dedup();
    Ok(codes)
}

/// [`decode_chunk`] against a pre-resolved code→term map instead of a
/// resident dictionary — the file-backed reconstruction path: the caller
/// resolves the chunk's [`unique_codes`] with one scan and decodes with the
/// resulting map.
#[cfg(feature = "file-io")]
pub(crate) fn decode_chunk_mapped(
    chunk: &ArrayRef,
    terms: &HashMap<u32, String>,
) -> Vec<Result<Quad>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();

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

    let s_col = get_u32_col!(COL_S);
    let p_col = get_u32_col!(COL_P);
    let o_col = get_u32_col!(COL_O);
    let g_col = get_u32_col!(COL_G);

    let s_ids = s_col.as_slice::<u32>();
    let p_ids = p_col.as_slice::<u32>();
    let o_ids = o_col.as_slice::<u32>();
    let g_ids = g_col.as_slice::<u32>();

    let term_of = |id: u32| -> Result<&String> {
        terms.get(&id).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "Term code {} missing from the chunk's resolved term map",
                id
            ))
        })
    };

    (0..n)
        .map(|i| {
            decode_spog(
                term_of(s_ids[i])?,
                term_of(p_ids[i])?,
                term_of(o_ids[i])?,
                term_of(g_ids[i])?,
            )
        })
        .collect()
}

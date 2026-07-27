//! The global term dictionary backing [`LayoutStrategy::Dictionary`]:
//! the lexicographically sorted set of unique RDF term strings, where a term's
//! ID is its sorted position. The s/p/o/g columns store these IDs as u32 codes.
//!
//! Because IDs are sorted ranks, ID comparisons are order-isomorphic to string
//! comparisons and term→ID lookup is a binary search — no HashMap is needed on
//! the query side, and the dictionary is held in its compact columnar form
//! (`VarBinViewArray`) rather than as owned `String`s.
//!
//! [`LayoutStrategy::Dictionary`]: super::LayoutStrategy::Dictionary

use std::collections::{HashMap, HashSet};
use std::time::Duration;
use web_time::Instant;

use vortex_array::arrays::listview::ListViewArrayExt as _;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{ListArray, ListViewArray, PrimitiveArray, VarBinViewArray};
#[cfg(feature = "file-io")]
use vortex_array::expr::{root, select};
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt as _;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
#[cfg(feature = "file-io")]
use vortex_file::VortexFile;

use crate::common::utils::StrColReader;
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;

/// Name of the dictionary column: a `list<utf8>` root column where row 0 holds
/// the entire sorted dictionary as one list and every other row is an empty list.
pub(crate) const DICT_FIELD: &str = "_dict_terms";

/// Build-only term-to-ID lookup table, keyed by owned terms. It is deliberately
/// kept separate from [`TermDictionary`] so stores retain only the compact
/// columnar dictionary; builders drop this map as soon as all quad terms have
/// been encoded.
///
/// Prefer [`TermDictionary::from_quads_with_map`]'s borrowed map wherever the
/// quads outlive the encode: the owned keys here duplicate the entire term set
/// on the heap, which for a large dataset costs more than the dictionary
/// itself. This variant exists for the streaming builders, whose quads are
/// moved or re-read from a spill file and so cannot be borrowed from.
pub(crate) type TermIdMap = HashMap<String, u32>;

/// Term-to-ID lookup borrowing its keys from the quads being encoded — the
/// allocation-free counterpart of [`TermIdMap`].
pub(crate) type BorrowedTermIdMap<'a> = HashMap<&'a str, u32>;

/// Incrementally collects the unique term strings of a dataset during the
/// ingestion pass of a build. Owned strings exist only for the build's lifetime.
pub(crate) struct TermDictionaryBuilder {
    set: HashSet<String>,
}

impl TermDictionaryBuilder {
    pub(crate) fn new() -> Self {
        Self {
            set: HashSet::new(),
        }
    }

    pub(crate) fn insert_quad(&mut self, q: &RawQuad) {
        for term in [&q.s, &q.p, &q.o, &q.g] {
            if !self.set.contains(term.as_str()) {
                self.set.insert(term.clone());
            }
        }
    }

    /// Sort the unique terms and freeze them into the columnar dictionary.
    pub(crate) fn finish(self) -> Result<TermDictionary> {
        let total_start = Instant::now();
        let collect_start = Instant::now();
        let mut terms: Vec<String> = self.set.into_iter().collect();
        let collect_elapsed = collect_start.elapsed();
        let sort_start = Instant::now();
        terms.sort_unstable();
        let sort_elapsed = sort_start.elapsed();
        let freeze_start = Instant::now();
        let dict = TermDictionary::from_sorted(terms.iter().map(String::as_str))?;
        log::debug!(
            "[Dictionary] Finished incremental dictionary ({} unique terms): collect {:?}, sort {:?}, freeze {:?}, total {:?}",
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            freeze_start.elapsed(),
            total_start.elapsed()
        );
        Ok(dict)
    }
}

/// The frozen, sorted term dictionary in columnar form.
///
/// term→ID is a host-side binary search over zero-copy `bytes_at` views;
/// ID→term is a zero-copy `bytes_at` read.
#[derive(Clone)]
pub(crate) struct TermDictionary {
    terms: VarBinViewArray,
}

impl TermDictionary {
    pub(crate) fn empty() -> Self {
        Self {
            terms: VarBinViewArray::from_iter_str(std::iter::empty::<&str>()),
        }
    }

    /// Build from already-sorted unique term strings.
    fn from_sorted<'a>(terms: impl Iterator<Item = &'a str> + Clone) -> Result<Self> {
        let dict = Self {
            terms: VarBinViewArray::from_iter_str(terms),
        };
        // List offsets are i32, so the term count must fit in one.
        if dict.len() > i32::MAX as usize {
            return Err(VortexRdfError::Serialization(format!(
                "Dictionary of {} unique terms exceeds the supported maximum ({})",
                dict.len(),
                i32::MAX
            )));
        }
        Ok(dict)
    }

    /// The dataset's unique terms, sorted — the raw material of both
    /// [`from_quads`](Self::from_quads) and
    /// [`from_quads_with_map`](Self::from_quads_with_map). Terms borrow from
    /// `quads`, so nothing is copied.
    fn sorted_unique_terms(quads: &[RawQuad]) -> (Vec<&str>, Duration, Duration) {
        let collect_start = Instant::now();
        let mut set: HashSet<&str> = HashSet::new();
        for q in quads {
            set.insert(&q.s);
            set.insert(&q.p);
            set.insert(&q.o);
            set.insert(&q.g);
        }
        let collect_elapsed = collect_start.elapsed();
        let sort_start = Instant::now();
        let mut terms: Vec<&str> = set.into_iter().collect();
        terms.sort_unstable();
        (terms, collect_elapsed, sort_start.elapsed())
    }

    /// Build from a complete in-memory quad slice (single-pass builders).
    ///
    /// Callers that also need a term→ID map should use
    /// [`from_quads_with_map`](Self::from_quads_with_map) instead — it hands
    /// back the sorted term list this would otherwise discard, avoiding a
    /// second owned copy of every term.
    pub(crate) fn from_quads(quads: &[RawQuad]) -> Result<Self> {
        let total_start = Instant::now();
        let (terms, collect_elapsed, sort_elapsed) = Self::sorted_unique_terms(quads);
        let freeze_start = Instant::now();
        let dict = Self::from_sorted(terms.into_iter())?;
        log::debug!(
            "[Dictionary] Built dictionary from {} quads ({} unique terms): collect {:?}, sort {:?}, freeze {:?}, total {:?}",
            quads.len(),
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            freeze_start.elapsed(),
            total_start.elapsed()
        );
        Ok(dict)
    }

    /// Build the dictionary *and* its term→ID map in one pass, with the map
    /// borrowing its keys from `quads`.
    ///
    /// A term's ID is its position in the sorted unique term list, which this
    /// already computes to build the dictionary — so the map costs one pointer
    /// pair per term and no string data at all. The owned-key alternative
    /// ([`build_id_map`](Self::build_id_map)) re-materializes every term on the
    /// heap, which on a large dataset outweighs the dictionary itself.
    pub(crate) fn from_quads_with_map(quads: &[RawQuad]) -> Result<(Self, BorrowedTermIdMap<'_>)> {
        let total_start = Instant::now();
        let (terms, collect_elapsed, sort_elapsed) = Self::sorted_unique_terms(quads);
        let map_start = Instant::now();
        let id_map: BorrowedTermIdMap<'_> = terms
            .iter()
            .enumerate()
            .map(|(id, term)| (*term, id as u32))
            .collect();
        let map_elapsed = map_start.elapsed();
        let freeze_start = Instant::now();
        let dict = Self::from_sorted(terms.into_iter())?;
        log::debug!(
            "[Dictionary] Built dictionary + borrowed ID map from {} quads ({} unique terms): collect {:?}, sort {:?}, map {:?}, freeze {:?}, total {:?}",
            quads.len(),
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            map_elapsed,
            freeze_start.elapsed(),
            total_start.elapsed()
        );
        Ok((dict, id_map))
    }

    pub(crate) fn len(&self) -> usize {
        self.terms.len()
    }

    /// The sorted term column itself (utf8, non-nullable).
    pub(crate) fn view(&self) -> &VarBinViewArray {
        &self.terms
    }

    /// Decode a code back to its term string (canonical N-Triples form), or
    /// `None` if the code is out of the dictionary's range. Zero-copy read of
    /// the term bytes; the returned `String` is the single owned copy.
    pub(crate) fn term_at(&self, code: u32) -> Option<String> {
        let i = code as usize;
        if i < self.len() {
            let reader = StrColReader::new(&self.terms);
            std::str::from_utf8(reader.bytes_at(i))
                .ok()
                .map(str::to_owned)
        } else {
            None
        }
    }

    /// Materialize a temporary O(1) lookup table for bulk encoding, with owned
    /// keys.
    ///
    /// Query-side lookups remain binary searches over the compact dictionary,
    /// but a build performs four lookups per quad and benefits substantially
    /// from paying this allocation once per build.
    ///
    /// This copies every term onto the heap a second time, so it is only for
    /// the streaming builders, whose quads are moved into the emitting stream
    /// or re-read from a spill file and therefore cannot be borrowed from.
    /// Builders holding a live `&[RawQuad]` must use
    /// [`from_quads_with_map`](Self::from_quads_with_map).
    pub(crate) fn build_id_map(&self) -> TermIdMap {
        let start = Instant::now();
        let reader = StrColReader::new(&self.terms);
        let map = (0..self.len())
            .map(|id| {
                let term = std::str::from_utf8(reader.bytes_at(id))
                    .expect("term dictionary contains only valid UTF-8")
                    .to_owned();
                (term, id as u32)
            })
            .collect();
        log::debug!(
            "[Dictionary] Built temporary term-ID map for {} terms in {:?}",
            self.len(),
            start.elapsed()
        );
        map
    }

    /// Look up a term's ID: its position in the sorted dictionary.
    /// Uses Vortex's SearchSorted compute kernel via ArrayRef for optimized sorted search.
    /// Falls back to manual binary search only if the kernel fails.
    pub(crate) fn get_id(&self, term: &str) -> Option<u32> {
        // Direct byte-compare binary search over the sorted term views.
        // `terms` is a concrete VarBinViewArray, so each probe reads a view
        // without materializing anything; the generic `search_sorted` kernel
        // would instead build a fresh `ExecutionCtx` and a `Scalar` per probe,
        // which profiling showed dominating `match_pattern`'s fixed cost.
        let reader = StrColReader::new(&self.terms);
        let needle = term.as_bytes();
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match reader.bytes_at(mid).cmp(needle) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Some(mid as u32),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }
}

/// Build the `_dict_terms` column for a chunk of `n_rows` quads.
///
/// When `carry_payload` is set (the first chunk of a build), row 0 holds the
/// entire dictionary as one list; otherwise every row is an empty list. Either
/// way the column dtype is identical across chunks.
pub(crate) fn dict_column(
    dict: &TermDictionary,
    n_rows: usize,
    carry_payload: bool,
) -> Result<ArrayRef> {
    let start = Instant::now();
    let m = dict.len() as i32;
    let (elements, offsets): (ArrayRef, Vec<i32>) = if carry_payload && n_rows > 0 {
        (
            dict.view().clone().into_array(),
            std::iter::once(0)
                .chain(std::iter::repeat_n(m, n_rows))
                .collect(),
        )
    } else {
        (
            VarBinViewArray::from_iter_str(std::iter::empty::<&str>()).into_array(),
            vec![0; n_rows + 1],
        )
    };

    let column = ListArray::try_new(
        elements,
        PrimitiveArray::from_iter(offsets).into_array(),
        Validity::NonNullable,
    )
    .map(|a| a.into_array())
    .map_err(VortexRdfError::Vortex)?;
    log::debug!(
        "[Dictionary] Built dictionary payload column for {} rows ({} terms, carry_payload={}) in {:?}",
        n_rows,
        dict.len(),
        carry_payload,
        start.elapsed()
    );
    Ok(column)
}

/// Recover the dictionary from a complete `_dict_terms` column: by
/// construction only row 0 can be non-empty, so its list is the dictionary.
///
/// Must be called on the full column as built/written — derived (sliced or
/// filtered) arrays may have lost row 0; stores cache the dictionary at
/// construction instead of re-reading it from derived data.
pub(crate) fn dict_from_list_column(col: &ArrayRef) -> Result<TermDictionary> {
    if col.is_empty() {
        return Ok(TermDictionary::empty());
    }
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let list = col
        .clone()
        .execute::<ListViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let elements = list.list_elements_at(0).map_err(VortexRdfError::Vortex)?;
    let terms = elements
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(TermDictionary { terms })
}

/// Extract the term dictionary from an in-memory Dictionary-layout array.
///
/// Only row 0 carries the payload, so slice to a single row first — this keeps
/// the extraction cheap (no canonicalization of the full, possibly chunked,
/// array) and zero-copy into the existing buffers.
pub(crate) fn dict_from_array(array: &ArrayRef) -> Result<TermDictionary> {
    let head = if array.is_empty() {
        array.clone()
    } else {
        array.slice(0..1).map_err(VortexRdfError::Vortex)?
    };
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = head
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let col = struct_arr
        .unmasked_field_by_name(DICT_FIELD)
        .map_err(VortexRdfError::Vortex)?
        .clone();
    dict_from_list_column(&col)
}

/// Read the term dictionary from a Dictionary-layout file: a single-column
/// projection scan of `_dict_terms` (the quad columns are never touched).
///
/// The scan is restricted to row 0 — by construction the only row carrying the
/// payload (mirroring `dict_from_array`'s `slice(0..1)`). Without the
/// restriction the scan decodes every row block of the column just to hold
/// empty lists, which profiling showed dominating Dictionary-layout file opens.
#[cfg(feature = "file-io")]
pub(crate) async fn dict_from_file(file: &VortexFile) -> Result<TermDictionary> {
    if file.row_count() == 0 {
        return Ok(TermDictionary::empty());
    }
    let arr = file
        .scan()
        .map_err(VortexRdfError::Vortex)?
        .with_projection(select([DICT_FIELD], root()))
        .with_row_range(0..1)
        .into_array_stream()
        .map_err(VortexRdfError::Vortex)?
        .read_all()
        .await
        .map_err(VortexRdfError::Vortex)?;
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let col = struct_arr
        .unmasked_field_by_name(DICT_FIELD)
        .map_err(VortexRdfError::Vortex)?
        .clone();
    dict_from_list_column(&col)
}

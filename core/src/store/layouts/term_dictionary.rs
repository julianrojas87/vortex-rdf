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
use std::sync::{Arc, RwLock};
use std::time::Duration;
use web_time::Instant;

use vortex_array::arrays::listview::ListViewArrayExt as _;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{ListArray, ListViewArray, PrimitiveArray, VarBinViewArray};
#[cfg(feature = "file-io")]
use vortex_array::expr::{root, select};
use vortex_array::match_each_integer_ptype;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt as _;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
#[cfg(feature = "file-io")]
use vortex_file::VortexFile;
use vortex_fsst::{FSST, FSSTArray, FSSTArraySlotsExt as _, fsst_compress, fsst_train_compressor};

use crate::common::utils::{StrColReader, buf_as_str};
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

/// How a dictionary's sorted terms are held in memory.
///
/// The dictionary is *built* FSST-compressed (see [`TermDictionary::compress`])
/// and written out that way, so `Fsst` is the normal case. `Canonical` remains
/// reachable and is not a legacy path: Vortex picks a column's encoding when it
/// writes, by sampling, and the selector is free to choose something other than
/// FSST — so a dictionary read back from a file or IPC stream may arrive in any
/// encoding, and the read path has to be total over that. Anything that is not
/// FSST is canonicalized to plaintext on open.
#[derive(Clone)]
enum TermStore {
    /// Plaintext terms. `bytes_at` is a zero-copy read.
    Canonical(VarBinViewArray),
    /// FSST-compressed terms: ~4x smaller, and every read decodes.
    Fsst(FsstTerms),
}

/// The frozen, sorted term dictionary in columnar form.
///
/// term→ID is a host-side binary search; ID→term reads the term at a position.
/// Both go through [`reader`](Self::reader), whose cost depends on the encoding
/// the terms are held in.
pub(crate) struct TermDictionary {
    terms: TermStore,
    /// Memo for [`get_id`](Self::get_id); see [`ProbeCache`].
    probes: ProbeCache,
}

/// Cloning shares nothing: the memo starts empty rather than being copied.
///
/// The entries would still be *valid* — they describe the terms, which are
/// cloned with them — but a dictionary is normally shared through an `Arc`, so
/// an actual clone is rare enough that carrying the memo across is not worth
/// the copy.
impl Clone for TermDictionary {
    fn clone(&self) -> Self {
        Self {
            terms: self.terms.clone(),
            probes: ProbeCache::new(),
        }
    }
}

impl TermDictionary {
    /// Wrap the held terms, with an empty lookup memo.
    fn new(terms: TermStore) -> Self {
        Self {
            terms,
            probes: ProbeCache::new(),
        }
    }

    pub(crate) fn empty() -> Self {
        Self::new(TermStore::Canonical(VarBinViewArray::from_iter_str(
            std::iter::empty::<&str>(),
        )))
    }

    /// Build from already-sorted unique term strings.
    ///
    /// The terms are FSST-compressed here rather than left for the writer to
    /// compress: which encoding a column gets is otherwise decided by sampling
    /// at write time and is not guaranteed to be FSST, so compressing at the
    /// source is what makes "the dictionary is FSST" an invariant this code
    /// owns instead of an assumption about somebody else's heuristic.
    fn from_sorted<'a>(terms: impl Iterator<Item = &'a str> + Clone) -> Result<Self> {
        let plain = VarBinViewArray::from_iter_str(terms);
        // List offsets are i32, so the term count must fit in one.
        if plain.len() > i32::MAX as usize {
            return Err(VortexRdfError::Serialization(format!(
                "Dictionary of {} unique terms exceeds the supported maximum ({})",
                plain.len(),
                i32::MAX
            )));
        }
        Self::compress(plain)
    }

    /// FSST-compress a plaintext term column.
    ///
    /// An empty dictionary is left canonical: there is nothing to train a
    /// symbol table on, and `fsst_train_compressor` has no non-null rows to
    /// sample.
    /// Adopt a term column as read back from a file or IPC stream.
    ///
    /// Already-FSST terms are kept compressed — decoding them here would undo
    /// the point of writing them that way, and is what made opening a store
    /// cost a full plaintext copy of its dictionary. Any other encoding is
    /// canonicalized: the write path compresses, but nothing in the format
    /// obliges a producer to have done so.
    fn from_terms_array(elements: ArrayRef, ctx: &mut vortex_array::ExecutionCtx) -> Result<Self> {
        let elements = match elements.try_downcast::<FSST>() {
            Ok(fsst) => {
                return Ok(Self::new(TermStore::Fsst(FsstTerms::new(fsst)?)));
            }
            Err(other) => other,
        };
        let plain = elements
            .execute::<VarBinViewArray>(ctx)
            .map_err(VortexRdfError::Vortex)?;
        Ok(Self::new(TermStore::Canonical(plain)))
    }

    fn compress(plain: VarBinViewArray) -> Result<Self> {
        if plain.is_empty() {
            return Ok(Self::new(TermStore::Canonical(plain)));
        }
        let start = Instant::now();
        let array = plain.into_array();
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let compressor = fsst_train_compressor(&array, &mut ctx).map_err(VortexRdfError::Vortex)?;
        let fsst = fsst_compress(&array, &compressor, &mut ctx).map_err(VortexRdfError::Vortex)?;
        let terms = FsstTerms::new(fsst)?;
        log::debug!(
            "[Dictionary] FSST-compressed {} terms in {:?}",
            terms.len(),
            start.elapsed()
        );
        Ok(Self::new(TermStore::Fsst(terms)))
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
        match &self.terms {
            TermStore::Canonical(a) => a.len(),
            TermStore::Fsst(f) => f.len(),
        }
    }

    /// The sorted term column as it is held, for serialization.
    ///
    /// Returns the FSST array itself when the terms are compressed, so writing
    /// a store out carries the compressed form rather than a re-expanded copy.
    pub(crate) fn terms_array(&self) -> ArrayRef {
        match &self.terms {
            TermStore::Canonical(a) => a.clone().into_array(),
            TermStore::Fsst(f) => f.array.clone().into_array(),
        }
    }

    /// A cursor over the terms. Holds the scratch buffer an FSST read decodes
    /// into, so callers needing several terms at once (a quad's four roles)
    /// must take one reader per role.
    pub(crate) fn reader(&self) -> DictReader<'_> {
        match &self.terms {
            TermStore::Canonical(a) => DictReader::Canonical(StrColReader::new(a)),
            TermStore::Fsst(f) => DictReader::Fsst {
                terms: f,
                scratch: f.new_scratch(),
            },
        }
    }

    /// Decode a code back to its term string (canonical N-Triples form), or
    /// `None` if the code is out of the dictionary's range.
    pub(crate) fn term_at(&self, code: u32) -> Option<String> {
        let i = code as usize;
        if i >= self.len() {
            return None;
        }
        self.reader().str_at(i).ok().map(str::to_owned)
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
        let mut reader = self.reader();
        let map = (0..self.len())
            .map(|id| {
                let term = reader
                    .str_at(id)
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

    /// Look up a term's ID: its position in the sorted dictionary, or `None`
    /// when the dictionary does not hold it.
    ///
    /// Memoized. [`PatternCodes`] already collapses the repeats *within* one
    /// match; this catches the repeats *across* matches, which is the shape
    /// real query workloads have — the same predicate walked over many
    /// patterns, the same subject chained through several matches.
    ///
    /// [`PatternCodes`]: super::PatternCodes
    pub(crate) fn get_id(&self, term: &str) -> Option<u32> {
        if let Some(memoized) = self.probes.get(term) {
            return memoized;
        }
        let found = self.search(term);
        self.probes.put(term, found);
        found
    }

    /// The uncached binary search behind [`get_id`](Self::get_id).
    fn search(&self, term: &str) -> Option<u32> {
        // Direct byte-compare binary search over the sorted terms. The generic
        // `search_sorted` kernel would instead build a fresh `ExecutionCtx` and
        // a `Scalar` per probe, which profiling showed dominating
        // `match_pattern`'s fixed cost.
        //
        // FSST is not order-preserving — roughly half of adjacent sorted pairs
        // invert in compressed space — so the search cannot run over the codes
        // and every probe decodes into the reader's scratch buffer.
        let mut reader = self.reader();
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

/// Slots in a dictionary's [`ProbeCache`]. A power of two, so the slot index is
/// a mask rather than a modulo.
///
/// Sized for the working set of a query workload — the bound terms of the
/// patterns currently being asked — not for the dictionary, which is orders of
/// magnitude larger and would defeat the point of bounding this at all.
const PROBE_CACHE_SLOTS: usize = 256;

/// A memo of term → code lookups, fixed in size and direct-mapped: one slot per
/// hash bucket, overwritten on collision.
///
/// A miss costs a binary search over the sorted terms, decoding a term at every
/// probe under FSST — ~1.6 µs at 3M terms, which is a third of a fully-bound
/// match. Repeats are common enough across matches to be worth catching.
///
/// Direct-mapped rather than an LRU because the memory matters more than the
/// hit rate here: a few KB per dictionary that never grows, against a structure
/// that would accumulate every term ever queried. Wasm linear memory is never
/// returned to the engine, so an unbounded cache is a leak by another name.
///
/// Entries cannot go stale. A dictionary's terms are immutable, so a term's
/// code — or its absence, which is memoized too — is a property of the
/// dictionary itself, not of when it was asked. Mutating a store builds a
/// *new* dictionary, and with it a new cache.
struct ProbeCache {
    slots: RwLock<Box<[Option<ProbeEntry>]>>,
}

struct ProbeEntry {
    /// A `String` rather than a `Box<str>` so an overwrite can reuse the
    /// allocation: terms in a dataset are of similar length, so the replacing
    /// term usually fits the capacity the evicted one left behind. A miss is
    /// then a hash and a copy, with no allocator traffic.
    term: String,
    code: Option<u32>,
}

impl ProbeCache {
    fn new() -> Self {
        Self {
            slots: RwLock::new(
                std::iter::repeat_with(|| None)
                    .take(PROBE_CACHE_SLOTS)
                    .collect(),
            ),
        }
    }

    /// FNV-1a over the whole term. Hashing a prefix would be cheaper but
    /// collides catastrophically on RDF data, where terms in a dataset share
    /// long IRI prefixes and differ only near the end.
    fn slot(term: &str) -> usize {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in term.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (h as usize) & (PROBE_CACHE_SLOTS - 1)
    }

    /// `Some(code_or_absent)` on a hit, `None` when this term is not memoized.
    ///
    /// A poisoned lock degrades to a miss rather than propagating: the memo is
    /// an optimization, and losing it must not fail a query.
    fn get(&self, term: &str) -> Option<Option<u32>> {
        let slots = self.slots.read().ok()?;
        match &slots[Self::slot(term)] {
            Some(entry) if entry.term == term => Some(entry.code),
            _ => None,
        }
    }

    fn put(&self, term: &str, code: Option<u32>) {
        if let Ok(mut slots) = self.slots.write() {
            let slot = Self::slot(term);
            match &mut slots[slot] {
                Some(entry) => {
                    entry.term.clear();
                    entry.term.push_str(term);
                    entry.code = code;
                }
                empty => {
                    *empty = Some(ProbeEntry {
                        term: term.to_owned(),
                        code,
                    })
                }
            }
        }
    }
}

/// Bytes an FSST symbol expands to at most — one code never yields more.
const FSST_SYMBOL_LEN: usize = 8;

/// Extra output headroom for `decompress_into`.
///
/// Its 8-symbols-at-a-time path only runs while the output has
/// `8 * FSST_SYMBOL_LEN` bytes of room left, so a buffer sized exactly to the
/// longest term silently falls back to the byte-at-a-time tail loop.
const FSST_DECODE_HEADROOM: usize = 8 * FSST_SYMBOL_LEN;

/// FSST-compressed sorted terms, with the pieces a hot lookup needs hoisted
/// out of the Vortex array.
#[derive(Clone)]
pub(crate) struct FsstTerms {
    /// Kept whole so the dictionary can be serialized without recompressing.
    array: FSSTArray,
    /// Code offsets, canonicalized once at open. The array stores them
    /// bit-packed, and unpacking a single value allocates a `Scalar` per call —
    /// far too expensive for a per-probe read inside a binary search.
    offsets: Arc<[u32]>,
    /// Scratch size that keeps `decompress_into` on its fast path.
    scratch_cap: usize,
}

impl FsstTerms {
    fn new(array: FSSTArray) -> Result<Self> {
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let offsets = array
            .codes_offsets()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        // Width is whatever the writer's scheme selection produced — a small
        // dictionary's offsets fit in a u8 — so accept every integer type
        // rather than the couple a given dataset happens to yield.
        // The u32 arm of the macro casts u32 -> u32; that is the price of one
        // arm covering every width.
        #[allow(clippy::unnecessary_cast)]
        let offsets: Arc<[u32]> = match_each_integer_ptype!(offsets.ptype(), |O| {
            offsets.as_slice::<O>().iter().map(|&o| o as u32).collect()
        });
        // An FSST code expands to at most one 8-byte symbol, so this bounds the
        // longest decoded term without decoding anything.
        let widest = offsets
            .windows(2)
            .map(|w| (w[1] - w[0]) as usize)
            .max()
            .unwrap_or(0);
        Ok(Self {
            array,
            offsets,
            scratch_cap: widest * FSST_SYMBOL_LEN + FSST_DECODE_HEADROOM,
        })
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn new_scratch(&self) -> Vec<u8> {
        Vec::with_capacity(self.scratch_cap)
    }

    /// Decode term `i` into `scratch`, returning the bytes written.
    fn decode_into<'a>(&self, i: usize, scratch: &'a mut Vec<u8>) -> &'a [u8] {
        let (start, end) = (self.offsets[i] as usize, self.offsets[i + 1] as usize);
        let n = self.array.decompressor().decompress_into(
            &self.array.codes_bytes()[start..end],
            scratch.spare_capacity_mut(),
        );
        // SAFETY: `decompress_into` initialized the first `n` bytes.
        unsafe {
            scratch.set_len(n);
        }
        &scratch[..n]
    }
}

/// A cursor over a dictionary's terms.
///
/// `str_at` borrows from the reader rather than from the dictionary because an
/// FSST read decodes into the reader's own scratch buffer, so the borrow ends
/// at the next call. Callers needing several terms simultaneously — decoding a
/// quad's four roles — take one reader per role.
pub(crate) enum DictReader<'a> {
    Canonical(StrColReader<'a>),
    Fsst {
        terms: &'a FsstTerms,
        scratch: Vec<u8>,
    },
}

impl DictReader<'_> {
    #[inline]
    pub(crate) fn bytes_at(&mut self, i: usize) -> &[u8] {
        match self {
            DictReader::Canonical(r) => r.bytes_at(i),
            DictReader::Fsst { terms, scratch } => {
                scratch.clear();
                terms.decode_into(i, scratch)
            }
        }
    }

    #[inline]
    pub(crate) fn str_at(&mut self, i: usize) -> Result<&str> {
        buf_as_str(self.bytes_at(i))
    }
}

/// An immutable handle on a Dictionary-layout store's term dictionary, taken
/// with [`VortexRdfStore::dictionary_snapshot`].
///
/// Cloning is an `Arc` bump, and the snapshot retains only the dictionary — not
/// the store or its quad columns.
///
/// **Term codes are only meaningful against the dictionary they were produced
/// with.** Mutating a store re-encodes it against a *fresh* dictionary, so a
/// consumer holding codes must decode them through the snapshot taken when it
/// received them, not through the store as it stands later — otherwise codes
/// silently resolve to the wrong terms. Holding the snapshot keeps exactly the
/// dictionary those codes address alive, and nothing more.
#[derive(Clone)]
pub struct DictSnapshot(pub(crate) Arc<TermDictionary>);

impl DictSnapshot {
    /// Decode a term code to its N-Triples string, or `None` when the code is
    /// out of this dictionary's range.
    pub fn decode(&self, code: u32) -> Option<String> {
        self.0.term_at(code)
    }

    /// Number of terms in the dictionary.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the dictionary holds no terms.
    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
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
            dict.terms_array(),
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
    TermDictionary::from_terms_array(elements, &mut ctx)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dict(terms: &[&str]) -> TermDictionary {
        let mut sorted = terms.to_vec();
        sorted.sort_unstable();
        TermDictionary::from_sorted(sorted.into_iter()).unwrap()
    }

    /// The memo must be invisible: every lookup agrees with the uncached search,
    /// on repeats and on absent terms alike.
    #[test]
    fn memoized_lookup_matches_the_search() {
        let terms: Vec<String> = (0..500)
            .map(|i| format!("<http://example.org/resource/{i:04}>"))
            .collect();
        let d = dict(&terms.iter().map(String::as_str).collect::<Vec<_>>());

        for probe in terms
            .iter()
            .map(String::as_str)
            .chain(["<http://absent>", ""])
        {
            let expected = d.search(probe);
            // Twice: the first call fills the slot, the second reads it back.
            assert_eq!(d.get_id(probe), expected, "cold lookup of {probe}");
            assert_eq!(d.get_id(probe), expected, "memoized lookup of {probe}");
        }
    }

    /// Two terms sharing a slot must not read each other's code. With one slot
    /// per bucket the second simply evicts the first, and both stay correct.
    #[test]
    fn colliding_terms_do_not_alias() {
        let terms: Vec<String> = (0..2_000)
            .map(|i| format!("<http://example.org/collide/{i:05}>"))
            .collect();
        let refs: Vec<&str> = terms.iter().map(String::as_str).collect();
        let d = dict(&refs);

        // Far more distinct terms than slots, so collisions are certain.
        assert!(terms.len() > PROBE_CACHE_SLOTS);
        let a = &refs[7];
        let b = refs
            .iter()
            .find(|t| ProbeCache::slot(t) == ProbeCache::slot(a) && *t != a)
            .expect("2000 terms over 256 slots must collide");

        assert_eq!(d.get_id(a), d.search(a));
        assert_eq!(d.get_id(b), d.search(b));
        // `b` evicted `a`; asking again must re-search, not return `b`'s code.
        assert_eq!(d.get_id(a), d.search(a));
        assert_ne!(d.get_id(a), d.get_id(b));
    }

    /// A cloned dictionary answers the same, starting from an empty memo.
    #[test]
    fn clone_keeps_lookups_correct() {
        let d = dict(&["<http://a>", "<http://b>", "<http://c>"]);
        assert_eq!(d.get_id("<http://b>"), Some(1));
        let copy = d.clone();
        assert_eq!(copy.get_id("<http://b>"), Some(1));
        assert_eq!(copy.get_id("<http://zz>"), None);
    }
}

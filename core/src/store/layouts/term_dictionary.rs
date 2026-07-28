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

#[cfg(feature = "file-io")]
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
#[cfg(feature = "file-io")]
use vortex_array::expr::{eq, get_item, gt_eq, lit, root, select};
use vortex_array::match_each_integer_ptype;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt as _;
#[cfg(feature = "file-io")]
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
#[cfg(feature = "file-io")]
use vortex_file::VortexFile;
use vortex_fsst::{FSST, FSSTArray, FSSTArraySlotsExt as _, fsst_compress, fsst_train_compressor};
#[cfg(feature = "file-io")]
use vortex_mask::{AllOr, Mask};

use crate::common::array::{StrColReader, buf_as_str};
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;
use crate::store::layouts::dictionary::QuadCodes;
#[cfg(feature = "file-io")]
use crate::store::schema::TERM_FIELD;

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

/// Ingest-time interner producing the dictionary and the coded quads in one
/// pass: quads are consumed as they arrive, each unique term is held once, and
/// each quad is kept as four u32 ids.
///
/// This replaces buffering the whole stream as a `Vec<RawQuad>` — four owned
/// `String`s per quad, held live until the dictionary and codes were derived
/// from them — which was the measured wasm ingest high-water mark (~377 B/row).
/// The per-quad Strings still exist transiently (the stream hands them over),
/// but they die inside [`push`](Self::push); what accumulates is one copy of
/// each distinct term plus 16 bytes per quad.
///
/// Ids handed out during ingest are provisional (insertion order).
/// [`finish`](Self::finish) sorts the unique terms, freezes them into the
/// [`TermDictionary`], and remaps every quad id to its term's sorted rank —
/// which *is* the dictionary code, since codes are lexicographic ranks. For
/// sorted builders it then sorts the coded quads directly: `[u32; 4]`
/// lexicographic order equals (s, p, o, g) term order (order-isomorphism
/// again), and sorting 16-byte rows is far cheaper than sorting four-String
/// structs.
pub(crate) struct InterningQuadBuilder {
    /// term → provisional id, owning each distinct term exactly once.
    ids: HashMap<Box<str>, u32>,
    /// One `[s, p, o, g]` of provisional ids per quad, in arrival order.
    quads: Vec<[u32; 4]>,
}

impl InterningQuadBuilder {
    pub(crate) fn new() -> Self {
        Self {
            ids: HashMap::new(),
            quads: Vec::new(),
        }
    }

    fn intern(&mut self, term: String) -> u32 {
        let next = self.ids.len() as u32;
        // `into_boxed_str` is free for exact-capacity Strings (the common
        // case from `RawQuad::from_quad`) and shrinks the rest.
        *self.ids.entry(term.into_boxed_str()).or_insert(next)
    }

    /// Consume one quad: intern its four terms, keep only their ids.
    pub(crate) fn push(&mut self, q: RawQuad) {
        let quad = [
            self.intern(q.s),
            self.intern(q.p),
            self.intern(q.o),
            self.intern(q.g),
        ];
        self.quads.push(quad);
    }

    /// Freeze the dictionary and produce the dataset's codes, sorted by
    /// (s, p, o, g) when `sort` is set.
    pub(crate) fn finish(mut self, sort: bool) -> Result<(TermDictionary, QuadCodes)> {
        let total_start = Instant::now();
        let n = self.quads.len();

        let sort_start = Instant::now();
        // Unique terms, so the tuple Ord never reaches the id.
        let mut entries: Vec<(Box<str>, u32)> = self.ids.into_iter().collect();
        entries.sort_unstable();
        let sort_terms_elapsed = sort_start.elapsed();

        // provisional id → sorted rank == dictionary code.
        let mut rank_of = vec![0u32; entries.len()];
        for (rank, (_, pid)) in entries.iter().enumerate() {
            rank_of[*pid as usize] = rank as u32;
        }

        // Freeze by *consuming* the boxes: each term is freed as it is copied
        // into the plain column, so the boxes and the column never coexist in
        // full — that stacking was the finish-phase memory peak.
        let freeze_start = Instant::now();
        // List offsets are i32, so the term count must fit in one (the same
        // guard as `from_sorted`).
        if entries.len() > i32::MAX as usize {
            return Err(VortexRdfError::Serialization(format!(
                "Dictionary of {} unique terms exceeds the supported maximum ({})",
                entries.len(),
                i32::MAX
            )));
        }
        let plain = VarBinViewArray::from_iter_str(entries.into_iter().map(|(t, _)| t));
        let dict = TermDictionary::compress(plain)?;
        let freeze_elapsed = freeze_start.elapsed();

        let remap_start = Instant::now();
        for quad in &mut self.quads {
            for id in quad.iter_mut() {
                *id = rank_of[*id as usize];
            }
        }
        if sort {
            self.quads.sort_unstable();
        }
        let remap_elapsed = remap_start.elapsed();

        let mut codes = QuadCodes {
            s: Vec::with_capacity(n),
            p: Vec::with_capacity(n),
            o: Vec::with_capacity(n),
            g: Vec::with_capacity(n),
        };
        for [s, p, o, g] in self.quads {
            codes.s.push(s);
            codes.p.push(p);
            codes.o.push(o);
            codes.g.push(g);
        }

        log::debug!(
            "[Dictionary] Interned {} quads ({} unique terms): sort terms {:?}, freeze {:?}, remap+sort quads {:?}, total {:?}",
            n,
            dict.len(),
            sort_terms_elapsed,
            freeze_elapsed,
            remap_elapsed,
            total_start.elapsed()
        );
        Ok((dict, codes))
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
    pub(crate) fn from_terms_array(
        elements: ArrayRef,
        ctx: &mut vortex_array::ExecutionCtx,
    ) -> Result<Self> {
        // Peel the structural wrappers a serialized term column can arrive
        // in: the padded form's all-valid nullability wrapper (`Masked`), and
        // a chunk-aligned slice still riding in a one-chunk `Chunked`
        // container. Without this the FSST downcast below would miss and the
        // dictionary would be canonicalized — decompressed — on open, which
        // is exactly the copy holding it compressed exists to avoid.
        let elements = {
            use vortex_array::arrays::chunked::ChunkedArrayExt as _;
            use vortex_array::arrays::masked::MaskedArraySlotsExt as _;
            let mut cur = elements;
            loop {
                cur = match cur.try_downcast::<vortex_array::arrays::Masked>() {
                    Ok(masked) => masked.child().clone(),
                    Err(not_masked) => {
                        match not_masked.try_downcast::<vortex_array::arrays::Chunked>() {
                            Ok(chunked) if chunked.nchunks() == 1 => chunked.chunk(0).clone(),
                            Ok(chunked) => break chunked.into_array(),
                            Err(other) => break other,
                        }
                    }
                };
            }
        };
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

    /// The dataset's unique terms, sorted — the raw material of
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

/// How a resolved Dictionary layout reaches its term dictionary: the
/// *residency* axis, sitting above [`TermStore`]'s encoding axis.
///
/// `Resident` holds the whole dictionary in memory — today's only variant. A
/// planned file-backed variant will leave the terms in a scannable Vortex
/// column and read them on demand, which makes term↔code translation
/// asynchronous. The seam is drawn now so that variant lands as a new arm in
/// these methods rather than a rework of their callers:
///
/// - [`resolve_pattern`](Self::resolve_pattern) is the **async prelude**: the
///   one place a dictionary is allowed to perform I/O during a match. It runs
///   before the synchronous match core and pre-resolves every bound term of
///   the pattern into the match's [`PatternCodes`], so everything downstream
///   resolves from that cache without touching the dictionary again.
/// - The sync accessors ([`get_id`](Self::get_id), [`term_at`](Self::term_at))
///   are total for `Resident`; a file-backed arm must answer them from
///   memoized state filled by the prelude, or its caller must move to an
///   async path.
/// - [`resident`](Self::resident) hands out the in-memory dictionary itself,
///   for the paths that genuinely need the whole column (reconstruction,
///   serialization, snapshots). It is total today; the file-backed variant
///   will change its signature, and the compile errors that causes are the
///   audit of exactly those paths.
#[derive(Clone)]
pub(crate) enum DictAccess {
    /// The whole dictionary in memory (FSST-compressed or canonical).
    Resident(Arc<TermDictionary>),
    /// The dictionary left in its file, probed and decoded by scans on
    /// demand — chosen at open when the term count exceeds the residency
    /// threshold (see `VortexRdfStore::from_file`).
    #[cfg(feature = "file-io")]
    FileBacked(FileBackedDict),
}

impl DictAccess {
    /// Pre-resolve every bound term of `pattern` into `codes` — the async
    /// prelude run before the synchronous match core.
    ///
    /// For `Resident` the lookups are in-memory binary searches, resolved
    /// eagerly rather than lazily at each use site: what this buys is the
    /// invariant the match core is written against — *after the prelude,
    /// every bound role is in `codes`* — which is what lets a file-backed
    /// dictionary do its I/O here and nowhere else.
    pub(crate) async fn resolve_pattern(
        &self,
        pattern: super::QuadPattern<'_>,
        codes: &mut super::PatternCodes,
    ) -> Result<()> {
        use super::TermRef;
        match self {
            DictAccess::Resident(dict) => {
                if let Some(s) = pattern.subject {
                    codes.resolve(TermRef::Subject(s), |t| dict.get_id(t));
                }
                if let Some(p) = pattern.predicate {
                    codes.resolve(TermRef::Predicate(p), |t| dict.get_id(t));
                }
                if let Some(o) = pattern.object {
                    codes.resolve(TermRef::Object(o), |t| dict.get_id(t));
                }
                if let Some(g) = pattern.graph {
                    codes.resolve(TermRef::Graph(g), |t| dict.get_id(t));
                }
                Ok(())
            }
            // Each bound role costs one filtered probe of the term column
            // (memoized in the probe cache); the resolved code is then seeded
            // into `codes` so the sync match core never reaches back here.
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => {
                if let Some(s) = pattern.subject {
                    let id = fb.get_id(codes.render(TermRef::Subject(s))).await?;
                    codes.resolve(TermRef::Subject(s), |_| id);
                }
                if let Some(p) = pattern.predicate {
                    let id = fb.get_id(codes.render(TermRef::Predicate(p))).await?;
                    codes.resolve(TermRef::Predicate(p), |_| id);
                }
                if let Some(o) = pattern.object {
                    let id = fb.get_id(codes.render(TermRef::Object(o))).await?;
                    codes.resolve(TermRef::Object(o), |_| id);
                }
                if let Some(g) = pattern.graph {
                    let id = fb.get_id(codes.render(TermRef::Graph(g))).await?;
                    codes.resolve(TermRef::Graph(g), |_| id);
                }
                Ok(())
            }
        }
    }

    /// Look up a term's code through the *synchronous* surface
    /// (`VortexRdfStore::encode_code`). A file-backed dictionary cannot probe
    /// its file without I/O, so it answers `None` — callers needing
    /// file-backed lookups go through the async prelude instead.
    pub(crate) fn get_id(&self, term: &str) -> Option<u32> {
        match self {
            DictAccess::Resident(dict) => dict.get_id(term),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// [`get_id`](Self::get_id) for the match core's probe closures, which run
    /// strictly after [`resolve_pattern`](Self::resolve_pattern) has seeded
    /// every bound role into the pattern's code cache — so a call ever
    /// reaching a file-backed dictionary is a broken prelude, not a miss.
    pub(crate) fn get_id_resolved(&self, term: &str) -> Option<u32> {
        match self {
            DictAccess::Resident(dict) => dict.get_id(term),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => unreachable!(
                "the async prelude resolves every bound role before the sync match core runs"
            ),
        }
    }

    /// Decode a code back to its term string through the *synchronous*
    /// surface (`VortexRdfStore::decode_code`), or `None` when out of range —
    /// or when the dictionary is file-backed (same contract as
    /// [`get_id`](Self::get_id)).
    pub(crate) fn term_at(&self, code: u32) -> Option<String> {
        match self {
            DictAccess::Resident(dict) => dict.term_at(code),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// The in-memory dictionary, or `None` when it is file-backed — sync
    /// callers (snapshots, in-memory chunk decode) treat `None` as "not
    /// available here"; paths that genuinely need the whole column go through
    /// [`ensure_resident`](Self::ensure_resident).
    pub(crate) fn resident(&self) -> Option<&Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Some(dict),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// The whole dictionary in memory, lifting a file-backed one with a single
    /// term-column scan — for the operations that need the full column
    /// (serialization, compaction, tail-merge re-encoding). The lift is
    /// transient: it is not cached back into the access, so a store's steady
    /// state keeps the file-backed footprint.
    pub(crate) async fn ensure_resident(&self) -> Result<Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Ok(Arc::clone(dict)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => Ok(Arc::new(fb.load_resident().await?)),
        }
    }

    /// Whether reconstruction must decode through the file (async) rather
    /// than the resident dictionary.
    #[cfg(feature = "file-io")]
    pub(crate) fn is_file_backed(&self) -> bool {
        matches!(self, DictAccess::FileBacked(_))
    }
}

/// A term dictionary left in its file: term→ID probes and ID→term decodes
/// scan the sorted `_dict_term` column on demand instead of holding all terms
/// resident.
///
/// Both serialized placements collapse to `(file, base_row)`: the padded form
/// probes the quads file's own trailing dictionary rows, the sidecar form its
/// companion file from row 0. A term probe evaluates an equality filter over
/// the dictionary rows split by split — the column is sorted, so zone
/// pruning discards every split whose min/max range excludes the term without
/// reading it — and memoizes the answer in a [`ProbeCache`]. ID→term reads
/// are row-index scans (`base_row + code`).
#[cfg(feature = "file-io")]
#[derive(Clone)]
pub(crate) struct FileBackedDict {
    /// The file holding the term column: the quads file itself (padded) or
    /// the sidecar companion.
    file: Arc<VortexFile>,
    /// Absolute file row of term ID 0: the quad-row count (padded) or 0
    /// (sidecar).
    base_row: u64,
    /// Number of terms.
    len: u64,
    /// term → code memo, shared across clones (every derived view of a store
    /// probes the same immutable dictionary).
    probes: Arc<ProbeCache>,
}

#[cfg(feature = "file-io")]
impl FileBackedDict {
    pub(crate) fn new(file: Arc<VortexFile>, base_row: u64, len: u64) -> Self {
        Self {
            file,
            base_row,
            len,
            probes: Arc::new(ProbeCache::new()),
        }
    }

    /// Term→ID: one equality-filtered pass over the dictionary rows, zone
    /// pruning first (the sorted column's min/max ranges rule out every split
    /// but the term's), memoized across calls.
    pub(crate) async fn get_id(&self, term: &str) -> Result<Option<u32>> {
        if let Some(memo) = self.probes.get(term) {
            return Ok(memo);
        }
        let filter = [eq(get_item(TERM_FIELD, root()), lit(term))];
        let reader = self.file.layout_reader().map_err(VortexRdfError::Vortex)?;
        let mut code = None;
        for split in self.file.splits().map_err(VortexRdfError::Vortex)? {
            let start = split.start.max(self.base_row);
            let end = split.end.min(self.base_row + self.len);
            if start >= end {
                continue;
            }
            let range = start..end;
            let mask = crate::store::vortex_rdf_store::evaluate_filter_split(
                Arc::clone(&reader),
                &filter,
                &range,
                Mask::new_true((end - start) as usize),
            )
            .await?;
            let row = match mask.indices() {
                AllOr::All => Some(range.start),
                AllOr::None => None,
                AllOr::Some(indices) => indices.first().map(|&i| range.start + i as u64),
            };
            if let Some(row) = row {
                code = Some((row - self.base_row) as u32);
                break;
            }
        }
        self.probes.put(term, code);
        Ok(code)
    }

    /// ID→terms for reconstruction: resolve `codes` (ascending, unique) to
    /// their term strings with a single row-index scan.
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
        let rows: vortex_buffer::Buffer<u64> = codes
            .iter()
            .map(|&code| self.base_row + code as u64)
            .collect();
        let arr = self
            .file
            .scan()
            .map_err(VortexRdfError::Vortex)?
            .with_row_indices(rows)
            .with_projection(select([TERM_FIELD], root()))
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

    /// The terms a chunk of code columns needs, as a code→term map: gather the
    /// chunk's distinct codes, resolve them in one scan.
    pub(crate) async fn chunk_term_map(&self, chunk: &ArrayRef) -> Result<HashMap<u32, String>> {
        let codes = super::dictionary::unique_codes(chunk)?;
        let terms = self.resolve_terms(&codes).await?;
        Ok(codes.into_iter().zip(terms).collect())
    }

    /// Lift the whole dictionary resident — the transient full-column read
    /// behind [`DictAccess::ensure_resident`]. A padded file goes through the
    /// aligned-tail extraction (which keeps the stored FSST encoding); a
    /// sidecar file is one term-column range scan.
    pub(crate) async fn load_resident(&self) -> Result<TermDictionary> {
        if self.base_row > 0 {
            let (_, dict) = dict_from_padded_file(&self.file).await?;
            Ok(dict)
        } else {
            dict_from_term_rows(&self.file, self.base_row, self.len).await
        }
    }
}

/// Read the term dictionary from a padded Dictionary-layout file: a
/// single-column projection scan of [`TERM_FIELD`] (the quad columns are
/// never touched). Returns the quad row count (the split point) alongside the
/// dictionary.
///
/// The quad rows' term values are one all-null run, which decodes to almost
/// nothing; the dictionary tail is lifted resident exactly as
/// [`super::dictionary::split_padded`] does for in-memory arrays.
#[cfg(feature = "file-io")]
pub(crate) async fn dict_from_padded_file(file: &VortexFile) -> Result<(u64, TermDictionary)> {
    if file.row_count() == 0 {
        return Ok((0, TermDictionary::empty()));
    }
    let arr = file
        .scan()
        .map_err(VortexRdfError::Vortex)?
        .with_projection(select([TERM_FIELD], root()))
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
        .unmasked_field_by_name(TERM_FIELD)
        .map_err(VortexRdfError::Vortex)?
        .clone();
    let (n_quads, dict) = super::dictionary::split_term_column(&col, &mut ctx)?;
    Ok((n_quads as u64, dict))
}

/// The sidecar dictionary path for a quads file: `<stem>.dict.vortex` beside
/// it (`data.vortex` → `data.dict.vortex`).
#[cfg(feature = "file-io")]
pub(crate) fn sidecar_dict_path(quads_path: &std::path::Path) -> std::path::PathBuf {
    let stem = quads_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "quads".to_string());
    quads_path.with_file_name(format!("{stem}.dict.vortex"))
}

/// Open the sidecar dictionary file beside `quads_path`, erroring when the
/// companion is missing (a bare-code quads file cannot decode without it).
#[cfg(feature = "file-io")]
pub(crate) async fn open_sidecar_file(quads_path: &std::path::Path) -> Result<Arc<VortexFile>> {
    let path = sidecar_dict_path(quads_path);
    if !path.is_file() {
        return Err(VortexRdfError::Deserialization(format!(
            "Dictionary-layout file {:?} has bare code columns and no sidecar \
             dictionary at {:?}; the sidecar must travel with the quads file",
            quads_path, path
        )));
    }
    Ok(Arc::new(crate::io::de::open_vortex_file(&path).await?))
}

/// Read `len` dictionary rows starting at `base_row` into a resident
/// [`TermDictionary`] — the whole term column of a sidecar file
/// (`base_row == 0`), or a lift of any file-backed range.
#[cfg(feature = "file-io")]
pub(crate) async fn dict_from_term_rows(
    file: &VortexFile,
    base_row: u64,
    len: u64,
) -> Result<TermDictionary> {
    if len == 0 {
        return Ok(TermDictionary::empty());
    }
    let arr = file
        .scan()
        .map_err(VortexRdfError::Vortex)?
        .with_row_range(base_row..base_row + len)
        .with_projection(select([TERM_FIELD], root()))
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
        .unmasked_field_by_name(TERM_FIELD)
        .map_err(VortexRdfError::Vortex)?
        .clone();
    TermDictionary::from_terms_array(col, &mut ctx)
}

/// Locate a padded file's dictionary rows without reading their terms:
/// `(quad_rows, term_count)`, discovered from the term column's validity (a
/// valid term row is a dictionary row) and validated to be one contiguous
/// tail run.
///
/// This is the open-time split that decides residency before anything is
/// lifted: per split, only the term column's zone stats and its (mostly
/// constant-null) validity are evaluated.
#[cfg(feature = "file-io")]
pub(crate) async fn padded_dict_extent(file: &VortexFile) -> Result<(u64, u64)> {
    let total = file.row_count();
    if total == 0 {
        return Ok((0, 0));
    }
    // Any non-null utf8 value satisfies `>= ""`; null (quad) rows never do —
    // so the mask is exactly the dictionary rows.
    let filter = [gt_eq(get_item(TERM_FIELD, root()), lit(""))];
    let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
    let mut count = 0u64;
    let mut first: Option<u64> = None;
    let mut last = 0u64;
    for range in file.splits().map_err(VortexRdfError::Vortex)? {
        let mask = crate::store::vortex_rdf_store::evaluate_filter_split(
            Arc::clone(&reader),
            &filter,
            &range,
            Mask::new_true((range.end - range.start) as usize),
        )
        .await?;
        match mask.indices() {
            AllOr::All => {
                count += range.end - range.start;
                first.get_or_insert(range.start);
                last = range.end - 1;
            }
            AllOr::None => {}
            AllOr::Some(indices) => {
                count += indices.len() as u64;
                if let Some(&i) = indices.first() {
                    first.get_or_insert(range.start + i as u64);
                }
                if let Some(&i) = indices.last() {
                    last = range.start + i as u64;
                }
            }
        }
    }
    let Some(first) = first else {
        // No dictionary rows: an empty dictionary (only reachable with an
        // empty dataset, but well-defined regardless).
        return Ok((total, 0));
    };
    if last != total - 1 || count != total - first {
        return Err(VortexRdfError::Deserialization(
            "padded Dictionary file is malformed: its dictionary rows do not \
             form one contiguous tail run"
                .to_string(),
        ));
    }
    Ok((first, count))
}

#[cfg(feature = "file-io")]
/// The sidecar dictionary as a one-column `{_dict_term: utf8}` array, ready
/// to serialize beside a bare-code quads file. Terms keep the encoding the
/// dictionary is held in (FSST when compressed at the source).
pub(crate) fn sidecar_dict_array(dict: &TermDictionary) -> Result<ArrayRef> {
    StructArray::try_new(
        [TERM_FIELD].into(),
        vec![dict.terms_array()],
        dict.len(),
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
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

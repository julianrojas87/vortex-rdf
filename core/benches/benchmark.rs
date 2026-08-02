//! Benchmark suite for `vortex-rdf-core`.
//!
//! # Design: a star (one-factor-at-a-time) layout, not a full factorial
//!
//! The library exposes four independent axes — builder strategy, layout,
//! secondary index, and source (file vs in-memory) — plus a query pattern with
//! 15 shapes. Their full cross product is ~2,400 match instances, most of which
//! measure the *same* code path: at query time both sorted builders emit
//! identically stamped columns (the store reads only the `IsSorted` stat, so it
//! cannot tell `SortedInMemory` from `SortedStream`), a bound subject always
//! declines every secondary index in favour of the primary sorted `s` column,
//! and a bound graph never routes through an index at all. Measuring those
//! combinations three times over buys no signal and bloats CodSpeed.
//!
//! Instead we fix a baseline and vary one axis at a time, adding back only the
//! interactions that genuinely change behaviour (e.g. Dictionary × index, where
//! the index columns hold u32 codes rather than term strings). Each group below
//! documents its baseline and which axis it sweeps.
//!
//! ## Query patterns, reduced to routing classes
//!
//! The 15 pattern shapes collapse to the six the resolver actually branches on:
//! `S` (primary binary search), `P` and `O` (single-column index probes), `PO`
//! (the two-column family prefix probe — `SecondaryByCopy`'s distinguishing
//! capability), `G` (no index covers graph, so the mask-scan / pushdown
//! fallback), and `SPOG` (every component bound — maximum residual filtering).
//!
//! ## Selectivity of the generated data (`generate_rdf_data_stream`)
//!
//! Subjects are unique; predicates repeat every 100 rows; objects every 50;
//! graphs every 10. Probe terms are chosen to hit rows that actually exist, so
//! at `BENCH_SIZE = 100_000` the matched-row counts are: `S`→1, `P`→1,000,
//! `O`→2,000, `PO`→1,000, `G`→10,000, `SPOG`→1. (The previous suite probed a
//! graph term — `.../graph` — that the generator never emits, so every
//! graph-bound benchmark silently matched zero rows.)

use std::collections::HashMap;
use std::fmt;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use futures::stream;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};

use support::generate_rdf_data_stream;
use vortex_rdf_core::{
    LayoutStrategy, SortedInMemoryBuilder, SortedStreamBuilder, UnsortedStreamBuilder,
    VortexRdfError, VortexRdfStore, io,
};

mod support;
use support::*;

fn main() {
    divan::main();
}

static BYTES_CACHE: OnceLock<Mutex<HashMap<CacheKey, Vec<u8>>>> = OnceLock::new();

fn cached_store_bytes(builder: Builder, layout: Layout, index: Index, size: usize) -> Vec<u8> {
    let cache = BYTES_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (builder, layout, index, size);
    if let Some(bytes) = cache.lock().unwrap().get(&key) {
        return bytes.clone();
    }
    let store = cached_store(builder, layout, index, size);
    let buf = rt().block_on(store.to_bytes()).expect("store bytes");
    cache.lock().unwrap().insert(key, buf.clone());
    buf
}

// ══════════════════════════════════════════════════════════════════════════
// Group 1 — SERIALIZE (write path)
//
// The write path is the one place all three axes genuinely differ, so we vary
// them one at a time around a `sorted_stream / default / no_index` baseline and
// add the two real interactions (Dictionary encodes the index as codes; an
// unsorted builder leaves the index columns unstamped, unlike a sorted one).
// ══════════════════════════════════════════════════════════════════════════

#[derive(Copy, Clone)]
struct SerCfg {
    builder: Builder,
    layout: Layout,
    index: Index,
}

impl fmt::Debug for SerCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.builder.short(),
            self.layout.short(),
            self.index.short()
        )
    }
}

const SERIALIZE_CONFIGS: &[SerCfg] = &[
    // Builder axis (Default layout, no index).
    SerCfg {
        builder: Builder::Unsorted,
        layout: Layout::Default,
        index: Index::None,
    },
    SerCfg {
        builder: Builder::SortedInMemory,
        layout: Layout::Default,
        index: Index::None,
    },
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Default,
        index: Index::None,
    }, // baseline
    // Layout axis (SortedStream, no index).
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::TypedObject,
        index: Index::None,
    },
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Dictionary,
        index: Index::None,
    },
    // Index axis (SortedStream, Default layout).
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Default,
        index: Index::ByReference,
    },
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Default,
        index: Index::ByCopy,
    },
    // Interactions worth keeping: index columns as dictionary codes, and an
    // unsorted (per-chunk, unstamped) index vs the sorted global one above.
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Dictionary,
        index: Index::ByCopy,
    },
    SerCfg {
        builder: Builder::Unsorted,
        layout: Layout::Default,
        index: Index::ByCopy,
    },
];

#[divan::bench(args = SERIALIZE_CONFIGS)]
fn serialize(bencher: divan::Bencher, cfg: &SerCfg) {
    let cfg = *cfg;
    bencher
        .with_inputs(|| materialize_quads(bench_size()))
        .bench_values(|quads| {
            rt().block_on(async move {
                let mut buf = Vec::new();
                let stream = stream::iter(quads.into_iter().map(Ok::<_, VortexRdfError>));
                match cfg.builder {
                    Builder::Unsorted => io::quads_stream_to_vortex_writer_with_builder::<
                        UnsortedStreamBuilder,
                        _,
                        _,
                    >(
                        stream,
                        &mut buf,
                        cfg.layout.strategy(),
                        cfg.index.types(),
                    )
                    .await,
                    Builder::SortedInMemory => io::quads_stream_to_vortex_writer_with_builder::<
                        SortedInMemoryBuilder,
                        _,
                        _,
                    >(
                        stream,
                        &mut buf,
                        cfg.layout.strategy(),
                        cfg.index.types(),
                    )
                    .await,
                    Builder::SortedStream => {
                        io::quads_stream_to_vortex_writer_with_builder::<SortedStreamBuilder, _, _>(
                            stream,
                            &mut buf,
                            cfg.layout.strategy(),
                            cfg.index.types(),
                        )
                        .await
                    }
                }
                .expect("serialize failed");
                black_box(buf.len())
            })
        });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 2 — MATCH (query path)
//
// Baseline: sorted_stream / default / by_copy / file. Each config sweeps the
// six routing patterns. We use SortedStream as the "sorted" representative and
// UnsortedStream as the "unsorted" one; SortedInMemory is omitted because it is
// query-indistinguishable from SortedStream (identical stamped columns).
// ══════════════════════════════════════════════════════════════════════════

/// Run one match config across a pattern: build the store once (untimed), then
/// time `match_pattern` plus materialization of the matched quads (so the lazy
/// derived view is actually executed).
fn run_match(
    bencher: divan::Bencher,
    builder: Builder,
    layout: Layout,
    index: Index,
    source: Source,
    pattern: Pattern,
) {
    bencher
        .with_inputs(|| make_store(source, builder, layout, index, bench_size()))
        .bench_refs(|store| {
            let (s, p, o, g) = terms_for(pattern);
            rt().block_on(async {
                let matched = store
                    .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                    .await
                    .expect("match_pattern failed");
                let quads = matched.quads_vec().await.expect("execute match");
                black_box(quads)
            })
        });
}

macro_rules! match_bench {
    ($name:ident, $builder:expr, $layout:expr, $index:expr, $source:expr) => {
        #[divan::bench(args = PATTERNS)]
        fn $name(bencher: divan::Bencher, pattern: &Pattern) {
            run_match(bencher, $builder, $layout, $index, $source, *pattern);
        }
    };
}

/// The full layout × source × index match matrix (sorted-stream builder
/// throughout; the unsorted builder is its own axis below). One group per
/// cell, named `match_sorted_{layout}_{index}_{source}`.
macro_rules! match_matrix {
    ($(($layout:expr, $index:expr, $source:expr) => $name:ident,)*) => {
        $(match_bench!($name, Builder::SortedStream, $layout, $index, $source);)*
    };
}
match_matrix!(
    // No secondary index.
    (Layout::Default, Index::None, Source::InMemory) => match_sorted_default_noindex_mem,
    (Layout::Default, Index::None, Source::File) => match_sorted_default_noindex_file,
    (Layout::TypedObject, Index::None, Source::InMemory) => match_sorted_typedobj_noindex_mem,
    (Layout::TypedObject, Index::None, Source::File) => match_sorted_typedobj_noindex_file,
    (Layout::Dictionary, Index::None, Source::InMemory) => match_sorted_dict_noindex_mem,
    (Layout::Dictionary, Index::None, Source::File) => match_sorted_dict_noindex_file,
    // Secondary by reference.
    (Layout::Default, Index::ByReference, Source::InMemory) => match_sorted_default_byref_mem,
    (Layout::Default, Index::ByReference, Source::File) => match_sorted_default_byref_file,
    (Layout::TypedObject, Index::ByReference, Source::InMemory) => match_sorted_typedobj_byref_mem,
    (Layout::TypedObject, Index::ByReference, Source::File) => match_sorted_typedobj_byref_file,
    (Layout::Dictionary, Index::ByReference, Source::InMemory) => match_sorted_dict_byref_mem,
    (Layout::Dictionary, Index::ByReference, Source::File) => match_sorted_dict_byref_file,
    // Secondary by copy.
    (Layout::Default, Index::ByCopy, Source::InMemory) => match_sorted_default_bycopy_mem,
    (Layout::Default, Index::ByCopy, Source::File) => match_sorted_default_bycopy_file,
    (Layout::TypedObject, Index::ByCopy, Source::InMemory) => match_sorted_typedobj_bycopy_mem,
    (Layout::TypedObject, Index::ByCopy, Source::File) => match_sorted_typedobj_bycopy_file,
    (Layout::Dictionary, Index::ByCopy, Source::InMemory) => match_sorted_dict_bycopy_mem,
    (Layout::Dictionary, Index::ByCopy, Source::File) => match_sorted_dict_bycopy_file,
);
// Sortedness axis: unsorted builder leaves nothing stamped, so indexes decline
// and everything falls to the mask scan — the worst case, and the typical
// in-memory (JS bindings) case.
match_bench!(
    match_unsorted_default_bycopy_file,
    Builder::Unsorted,
    Layout::Default,
    Index::ByCopy,
    Source::File
);
match_bench!(
    match_unsorted_default_bycopy_mem,
    Builder::Unsorted,
    Layout::Default,
    Index::ByCopy,
    Source::InMemory
);

/// Chained refinement: `match_pattern(P)` then `match_pattern(O)` on the
/// resulting view — the headline "views narrow the same coordinate space"
/// feature, which no single-pattern benchmark exercises.
#[divan::bench(args = [Source::File, Source::InMemory])]
fn match_chained(bencher: divan::Bencher, source: &Source) {
    let source = *source;
    bencher
        .with_inputs(|| {
            make_store(
                source,
                Builder::SortedStream,
                Layout::Default,
                Index::ByCopy,
                bench_size(),
            )
        })
        .bench_refs(|store| {
            let p = NamedNode::new_unchecked("http://example.org/predicate/0");
            let o = Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
            rt().block_on(async {
                let after_p = store
                    .match_pattern(None, Some(&p), None, None)
                    .await
                    .expect("match P");
                let after_po = after_p
                    .match_pattern(None, None, Some(&o), None)
                    .await
                    .expect("match O on view");
                let quads = after_po.quads_vec().await.expect("execute chained match");
                black_box(quads)
            })
        });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 3 — DECODE / LOAD (read-back path)
//
// The full-scan decode is the single most fundamental read and was entirely
// unbenchmarked. It is where layouts diverge most: Dictionary decodes codes to
// terms, TypedObject reassembles the object from four columns. Load costs
// (opening a file, decoding IPC) were previously hidden in untimed setup.
// ══════════════════════════════════════════════════════════════════════════

#[derive(Copy, Clone)]
struct DecodeCfg {
    layout: Layout,
    source: Source,
}

impl fmt::Debug for DecodeCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.layout.short(), self.source.short())
    }
}

const DECODE_CONFIGS: &[DecodeCfg] = &[
    DecodeCfg {
        layout: Layout::Default,
        source: Source::File,
    }, // baseline full scan
    DecodeCfg {
        layout: Layout::TypedObject,
        source: Source::File,
    }, // object reassembly
    DecodeCfg {
        layout: Layout::Dictionary,
        source: Source::File,
    }, // code → term
    DecodeCfg {
        layout: Layout::Default,
        source: Source::InMemory,
    }, // in-memory decode path
    DecodeCfg {
        layout: Layout::TypedObject,
        source: Source::InMemory,
    }, // in-memory object reassembly
    DecodeCfg {
        layout: Layout::Dictionary,
        source: Source::InMemory,
    }, // in-memory code → term
];

/// Decode every quad in the store (`quads()` → `Vec`). Index is irrelevant to a
/// full scan, so it is fixed to `None`.
#[divan::bench(args = DECODE_CONFIGS)]
fn decode_all(bencher: divan::Bencher, cfg: &DecodeCfg) {
    let cfg = *cfg;
    bencher
        .with_inputs(|| {
            make_store(
                cfg.source,
                Builder::SortedStream,
                cfg.layout,
                Index::None,
                bench_size(),
            )
        })
        .bench_refs(|store| {
            rt().block_on(async {
                let quads = store.quads_vec().await.expect("decode all");
                black_box(quads.len())
            })
        });
}

/// Open a file-backed store. Default and TypedObject read the footer only;
/// Dictionary also reads its term dictionary up front (an extra single-column
/// scan), so the layouts are worth distinguishing.
#[divan::bench(args = [Layout::Default, Layout::TypedObject, Layout::Dictionary])]
fn open_file(bencher: divan::Bencher, layout: &Layout) {
    let layout = *layout;
    bencher
        .with_inputs(|| cached_file(Builder::SortedStream, layout, Index::None, bench_size()))
        .bench_refs(|path| {
            rt().block_on(async {
                let store = VortexRdfStore::from_file(path).await.expect("open file");
                black_box(store.layout())
            })
        });
}

/// Load a store from file bytes (`from_bytes`): root-layout validation plus a
/// full in-memory materialization off the buffer-backed file.
#[divan::bench]
fn from_bytes(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| {
            cached_store_bytes(
                Builder::SortedStream,
                Layout::Default,
                Index::None,
                bench_size(),
            )
        })
        .bench_refs(|bytes| {
            rt().block_on(async {
                let store = VortexRdfStore::from_bytes(bytes).await.expect("from_bytes");
                black_box(store)
            })
        });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 4 — DICTIONARY RESIDENCY (file-backed vs resident term dictionary)
//
// A Dictionary file's term dictionary can be lifted resident at open or left
// in its dictionary child and reached by scans through the bounded reader
// (`from_file_with_dict_residency`, byte threshold). The residency axis moves
// cost between phases: resident pays one contiguous child read at open and
// then probes/decodes from memory; file-backed opens on footer metadata alone
// but pays a pruned child scan per cold term probe and a row-index scan per
// decoded chunk.
// ══════════════════════════════════════════════════════════════════════════

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum DictResidency {
    Resident,
    FileBacked,
}

impl DictResidency {
    /// The `max_resident_bytes` value that forces this residency.
    fn threshold(self) -> u64 {
        match self {
            Self::Resident => u64::MAX,
            Self::FileBacked => 0,
        }
    }

    fn short(self) -> &'static str {
        match self {
            Self::Resident => "resident",
            Self::FileBacked => "file_backed",
        }
    }
}

impl fmt::Debug for DictResidency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short())
    }
}

const DICT_CONFIGS: &[DictResidency] = &[DictResidency::Resident, DictResidency::FileBacked];

static DICT_FILE_CACHE: OnceLock<Mutex<HashMap<usize, PathBuf>>> = OnceLock::new();

/// A Dictionary-layout file (sorted-stream builder, no indexes), built once
/// per size.
fn cached_dict_file(size: usize) -> PathBuf {
    let cache = DICT_FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(path) = cache.lock().unwrap().get(&size) {
        return path.clone();
    }
    let dir = PathBuf::from("target/bench_vortex_files");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("dict_{size}.vortex"));
    rt().block_on(async {
        io::quads_stream_to_vortex_file_with_builder::<SortedStreamBuilder, _>(
            generate_rdf_data_stream(size),
            &path,
            LayoutStrategy::Dictionary,
            Vec::new(),
        )
        .await
        .expect("write dictionary bench file");
    });
    cache.lock().unwrap().insert(size, path.clone());
    path
}

fn open_dict_store(residency: DictResidency, size: usize) -> VortexRdfStore {
    let path = cached_dict_file(size);
    rt().block_on(async {
        VortexRdfStore::from_file_with_dict_residency(&path, residency.threshold())
            .await
            .expect("open dictionary store")
    })
}

/// Open cost across the residency axis: resident pays the child read and
/// dictionary lift, file-backed only the footer reads.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_open(bencher: divan::Bencher, residency: &DictResidency) {
    let residency = *residency;
    bencher
        .with_inputs(|| (cached_dict_file(bench_size()), residency.threshold()))
        .bench_refs(|(path, threshold)| {
            rt().block_on(async {
                let store = VortexRdfStore::from_file_with_dict_residency(&*path, *threshold)
                    .await
                    .expect("open");
                black_box(store.layout())
            })
        });
}

/// Cold term→ID probes: a fully bound pattern (four dictionary probes) on a
/// store opened fresh each iteration, so the file-backed probe cache never
/// warms. Resident probes are in-memory binary searches; file-backed ones are
/// zone-pruned column scans.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_probe_cold(bencher: divan::Bencher, residency: &DictResidency) {
    let residency = *residency;
    bencher
        .with_inputs(|| open_dict_store(residency, bench_size()))
        .bench_refs(|store| {
            let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(
                "http://example.org/subject/0",
            ));
            let p = NamedNode::new_unchecked("http://example.org/predicate/0");
            let o = Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
            let g = GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/graph/0"));
            rt().block_on(async {
                let matched = store
                    .match_pattern(Some(&s), Some(&p), Some(&o), Some(&g))
                    .await
                    .expect("match SPOG");
                black_box(matched)
            })
        });
}

/// The same fully bound pattern on one shared store: after the first
/// iteration every file-backed probe hits the memoized probe cache — the
/// steady state of repeated lookups for the same terms.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_probe_warm(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/subject/0"));
    let p = NamedNode::new_unchecked("http://example.org/predicate/0");
    let o = Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
    let g = GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/graph/0"));
    bencher.bench(|| {
        rt().block_on(async {
            let matched = store
                .match_pattern(Some(&s), Some(&p), Some(&o), Some(&g))
                .await
                .expect("match SPOG");
            black_box(matched)
        })
    });
}

/// Reconstruction of a matched subset (predicate-bound, 1% of rows at the
/// default size): resident decodes codes against the in-memory dictionary;
/// file-backed resolves each chunk's distinct codes with a row-index scan
/// first.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_decode_matched(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let p = NamedNode::new_unchecked("http://example.org/predicate/0");
    bencher.bench(|| {
        rt().block_on(async {
            let matched = store
                .match_pattern(None, Some(&p), None, None)
                .await
                .expect("match P");
            let quads = matched.quads_vec().await.expect("decode matched");
            black_box(quads.len())
        })
    });
}

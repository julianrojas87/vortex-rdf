//! Infrastructure shared by the bench targets (`benchmark.rs`, the CodSpeed
//! suite, and `match_lazy.rs`, the local-only lazy-match matrix): the axis
//! enums, dataset/store/file construction with their caches, and the match
//! pattern probes. Compiled once per target; the caches are per-process, so
//! each target builds its own artifacts.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use futures::{Stream, StreamExt, stream};
use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use tokio::runtime::Runtime;

use vortex_rdf_core::error::Result;
use vortex_rdf_core::store::RawQuad;
use vortex_rdf_core::{
    BuiltArray, IndexType, LayoutStrategy, SortedStreamBuilder, VortexArrayBuilder, VortexRdfStore,
};

// The dataset shape — moduli, term spellings, probe patterns — lives on its own
// so `compare.rs` can include just that file (`#[path = "support/dataset.rs"]`)
// without the store-building machinery it has no use for. One definition is what
// keeps the instrumented suite's rows comparable with the comparative suite's.
pub mod dataset;
pub use dataset::{PATTERNS, Pattern, terms_for};

/// Single dataset size for the whole suite. In simulation mode CodSpeed counts
/// instructions deterministically, so one representative size catches
/// regressions in every path; larger sizes only multiply valgrind cost without
/// adding signal (CodSpeed does not analyse scaling curves).
///
/// The default is the size all three CodSpeed suites share — `CODSPEED_BENCH_DIM`
/// is 32 in `js/bench/codspeed.bench.ts` and `python/bench/test_codspeed.py`,
/// and 32³ is this number — so one shared-core regression lands in all three
/// tabs at comparable magnitude instead of showing up in one and hiding in
/// another. It is also 4 zones of 8,192 rows, the smallest round size at which
/// zone pruning has anything to prune.
///
/// Override for a different scale: `BENCH_SIZE=1048576 cargo bench` matches the
/// comparative suites' default, which is what the dashboard workflow runs.
pub fn bench_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("BENCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32_768)
    })
}

/// Samples per query benchmark — the repetition count every comparative suite
/// uses for a query (`QUERY_OPTS.iterations` in `js/bench/shared.ts`,
/// `QUERY_ITERS` in `python/bench/worker.py`), so one number describes a
/// repetition across all three environments.
///
/// Set per bench rather than through `DIVAN_SAMPLE_COUNT` so a plain
/// `cargo bench` reproduces the dashboard's regime without remembering an env
/// var. CodSpeed ignores it entirely: its simulation mode measures one
/// invocation per case, deterministically.
pub const QUERY_SAMPLES: u32 = 10;

/// Samples per heavy benchmark — ingest, full decode, open, serialize. These
/// run for seconds per iteration at the dashboard's 2M scale, where ten
/// samples would add tens of minutes to a refresh for a number that barely
/// moves; the comparative suites make the same trade (`HEAVY_OPTS`,
/// `HEAVY_ITERS`).
pub const HEAVY_SAMPLES: u32 = 3;

// ── shared tokio runtime ────────────────────────────────────────────────────

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn rt() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

// ── configuration axes ──────────────────────────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Layout {
    Default,
    TypedObject,
    Dictionary,
}

impl Layout {
    pub fn strategy(self) -> LayoutStrategy {
        match self {
            Self::Default => LayoutStrategy::Default,
            Self::TypedObject => LayoutStrategy::TypedObject,
            Self::Dictionary => LayoutStrategy::Dictionary,
        }
    }
    pub fn short(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::TypedObject => "typed_object",
            Self::Dictionary => "dictionary",
        }
    }
}

impl fmt::Debug for Layout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short())
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Index {
    None,
    ByReference,
    ByCopy,
}

impl Index {
    pub fn types(self) -> Vec<IndexType> {
        match self {
            Self::None => vec![],
            Self::ByReference => vec![IndexType::SecondaryByReference],
            Self::ByCopy => vec![IndexType::SecondaryByCopy],
        }
    }
    pub fn short(self) -> &'static str {
        match self {
            Self::None => "no_index",
            Self::ByReference => "by_reference",
            Self::ByCopy => "by_copy",
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Source {
    File,
    InMemory,
    /// Lean adoption: `from_bytes` over the serialized store, keeping the
    /// base wire-encoded and the index components deferred — the shape js
    /// `fromBytes` and python `from_bytes` hand out.
    Bytes,
}

impl Source {
    pub fn short(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::InMemory => "in_memory",
            Self::Bytes => "from_bytes",
        }
    }
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short())
    }
}

// ── dataset + artifact construction (all untimed helpers) ────────────────────

/// Materialize the generated quads into an owned `Vec`, eagerly. The generator
/// is a *lazy* stream whose per-quad `format!` allocations would otherwise be
/// polled — and charged — inside the timed serialization region; draining it
/// here keeps those allocations out of the measurement.
pub fn materialize_quads(size: usize) -> Vec<RawQuad> {
    rt().block_on(async move {
        generate_rdf_data_stream(size)
            .map(|q| q.expect("quad generation is infallible"))
            .collect()
            .await
    })
}

/// Run a config's ingest to the builder's `BuiltArray`: the quad array plus
/// whatever travels beside it — under the Dictionary layout the array holds
/// only u32 codes and the term dictionary rides alongside; `from_built` is
/// the one constructor that accepts that pair.
fn build_built(layout: Layout, index: Index, size: usize) -> BuiltArray {
    rt().block_on(async move {
        SortedStreamBuilder::build_vortex_array(
            Box::new(generate_rdf_data_stream(size)),
            layout.strategy(),
            index.types(),
        )
        .await
        .expect("failed to build vortex array")
    })
}

pub type CacheKey = (Layout, Index, usize);

/// Cache of ingest products (`BuiltArray`, cheaply cloneable: Arc-shared
/// buffers). Only the expensive ingest is cached — stores are rebuilt per
/// handout, see [`cached_store`]. Only a handful of distinct configs are ever
/// requested, so this stays naturally bounded.
static BUILT_CACHE: OnceLock<Mutex<HashMap<CacheKey, BuiltArray>>> = OnceLock::new();
static FILE_CACHE: OnceLock<Mutex<HashMap<CacheKey, PathBuf>>> = OnceLock::new();

fn cached_built(layout: Layout, index: Index, size: usize) -> BuiltArray {
    let cache = BUILT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (layout, index, size);
    if let Some(built) = cache.lock().unwrap().get(&key) {
        return built.clone();
    }
    let built = build_built(layout, index, size);
    cache.lock().unwrap().insert(key, built.clone());
    built
}

/// A FRESH store over a config's cached ingest product: `from_built` re-runs
/// component adoption and store construction every call, so no store-level
/// state is shared between the stores this returns. Under CodSpeed's
/// single-measured-invocation model a task must start cold and
/// order-independent, which clones of one cached store cannot be: whatever an
/// earlier benchmark warms on the shared store shifts every later baseline.
///
/// Known residual: the immutable Arc'd buffers — and vortex's
/// interior-mutable per-array stats riding on them — remain shared with the
/// cache, so any stat a read lazily computes lands on the shared array.
/// Closing that too would take a deep copy per handout.
pub fn cached_store(layout: Layout, index: Index, size: usize) -> VortexRdfStore {
    VortexRdfStore::from_built(cached_built(layout, index, size))
        .expect("failed to build vortex store")
}

pub fn cached_file(layout: Layout, index: Index, size: usize) -> PathBuf {
    let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (layout, index, size);
    if let Some(path) = cache.lock().unwrap().get(&key) {
        return path.clone();
    }
    let store = cached_store(layout, index, size);
    let dir = PathBuf::from("target/bench_vortex_files");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!(
        "{}_{}_{}.vortex",
        layout.short(),
        index.short(),
        size
    ));
    rt().block_on(async {
        let bytes = store.to_bytes().await.expect("serialize store");
        std::fs::write(&path, bytes).expect("write file");
    });
    cache.lock().unwrap().insert(key, path.clone());
    path
}

/// Construct a store over a config's data, from the requested source. Both are
/// untimed: `from_file` reads the footer only, and the in-memory arm rebuilds
/// a fresh store from the cached ingest product (see [`cached_store`]).
pub fn make_store(source: Source, layout: Layout, index: Index, size: usize) -> VortexRdfStore {
    match source {
        Source::File => {
            let path = cached_file(layout, index, size);
            rt().block_on(async {
                VortexRdfStore::from_file(path)
                    .await
                    .expect("open file store")
            })
        }
        Source::InMemory => cached_store(layout, index, size),
        Source::Bytes => {
            let path = cached_file(layout, index, size);
            let bytes = std::fs::read(path).expect("read serialized store");
            rt().block_on(async {
                VortexRdfStore::from_bytes_owned(bytes)
                    .await
                    .expect("adopt bytes store")
            })
        }
    }
}

// ── dataset generation ──────────────────────────────────────────────────────

/// The moduli this process's dataset was generated with, at the size
/// [`bench_size`] resolved. Every consumer that needs a term index — the
/// distinct-probe walk, the shape line — derives it from here rather than
/// assuming a cardinality, because the moduli follow from the row count.
pub fn bench_moduli() -> dataset::Moduli {
    static M: OnceLock<dataset::Moduli> = OnceLock::new();
    *M.get_or_init(|| dataset::moduli(bench_size(), dataset::WANT_GRAPHS))
}

/// Generate the bench dataset as a stream of quads — the same rows
/// `write_dataset` writes for the comparative suites, built in process.
///
/// In process is not incidental: `benchmark.rs` is uploaded to CodSpeed and run
/// under valgrind, where reading a multi-hundred-MB N-Triples file would swamp
/// the instruction counts (and the file does not exist in CI at all). The
/// generator is what lets the instrumented suite share the comparative suites'
/// term shape without sharing their delivery.
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<RawQuad>> {
    let m = dataset::moduli(size, dataset::WANT_GRAPHS);

    stream::iter((0..size).map(move |i| {
        let subject = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(dataset::subject_iri(
            i % m.n_subj,
        )));
        let predicate = NamedNode::new_unchecked(dataset::predicate_iri(i % m.n_pred));
        let object = dataset::object_term(i % m.n_obj).into_oxrdf();
        let graph =
            GraphName::NamedNode(NamedNode::new_unchecked(dataset::graph_iri(i % m.n_graph)));

        Ok(RawQuad::from_quad(&Quad::new(
            subject, predicate, object, graph,
        )))
    }))
}

/// Like [`generate_rdf_data_stream`], but with literal objects of every shape —
/// plain, language-tagged, typed, and escape-bearing (quotes, backslashes,
/// newlines, and `"@`/`^^` lookalikes inside the value). The dataset above
/// carries plain literals only, deliberately: escaping cost inside the
/// comparative suites would land in every external adapter's parse timing. So
/// this is what lets a benchmark reach the escape/unescape paths at all.
///
/// Subject and predicate cardinality follow the shared moduli, so the only
/// axis that differs from [`generate_rdf_data_stream`] is what the objects are.
pub fn generate_literal_rdf_data_stream(size: usize) -> impl Stream<Item = Result<RawQuad>> {
    let m = dataset::moduli(size, dataset::WANT_GRAPHS);

    stream::iter((0..size).map(move |i| {
        let subject = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(dataset::subject_iri(
            i % m.n_subj,
        )));
        let predicate = NamedNode::new_unchecked(dataset::predicate_iri(i % m.n_pred));
        let value = i % m.n_obj;
        let object = Term::Literal(match i % 4 {
            0 => Literal::new_simple_literal(format!("plain value {value}")),
            1 => Literal::new_language_tagged_literal_unchecked(
                format!("tagged value {value}"),
                "en",
            ),
            2 => Literal::new_typed_literal(
                format!("{value}"),
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            ),
            _ => Literal::new_simple_literal(format!(
                "say \"hi\"@home ^^ line\nbreak back\\slash {value}"
            )),
        });

        Ok(RawQuad::from_quad(&Quad::new(
            subject,
            predicate,
            object,
            GraphName::DefaultGraph,
        )))
    }))
}

/// The literal-bearing store for `decode_all_literals`: Dictionary layout, no
/// index — the config whose full decode term-decodes every row. Cached and
/// handed out like the main dataset's stores: only the ingest product is
/// cached, and each call rebuilds a fresh store from it (see
/// [`cached_store`] for why shared clones are a contamination hazard).
pub fn cached_literal_store(size: usize) -> VortexRdfStore {
    static CACHE: OnceLock<Mutex<HashMap<usize, BuiltArray>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let built = if let Some(built) = cache.lock().unwrap().get(&size) {
        built.clone()
    } else {
        let built = rt().block_on(async move {
            SortedStreamBuilder::build_vortex_array(
                Box::new(generate_literal_rdf_data_stream(size)),
                LayoutStrategy::Dictionary,
                Vec::new(),
            )
            .await
            .expect("failed to build literal array")
        });
        cache.lock().unwrap().insert(size, built.clone());
        built
    };
    VortexRdfStore::from_built(built).expect("failed to build literal store")
}

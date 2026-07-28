//! Infrastructure shared by the bench targets (`benchmark.rs`, the CodSpeed
//! suite, and `match_lazy.rs`, the local-only lazy-match matrix): the axis
//! enums, dataset/store/file construction with their caches, and the match
//! pattern probes. Compiled once per target; the caches are per-process, so
//! each target builds its own artifacts.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use futures::StreamExt;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use tokio::runtime::Runtime;

use vortex_rdf_core::common::testing::generate_rdf_data_stream;
use vortex_rdf_core::store::RawQuad;
use vortex_rdf_core::{
    DictionaryPlacement, IndexType, LayoutStrategy, SortedInMemoryBuilder, SortedStreamBuilder,
    UnsortedStreamBuilder, VortexRdfStore, io,
};

/// Single dataset size for the whole suite. In simulation mode CodSpeed counts
/// instructions deterministically, so one representative size catches
/// regressions in every path; larger sizes only multiply valgrind cost without
/// adding signal (CodSpeed does not analyse scaling curves). Default matches
/// CodSpeed CI; override locally (e.g. `BENCH_SIZE=2097152 cargo bench`, to
/// match the JS comparative benchmark's default D=128 scale) to see how
/// results shift at a larger size.
pub fn bench_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("BENCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000)
    })
}

// ── shared tokio runtime ────────────────────────────────────────────────────

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn rt() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

// ── configuration axes ──────────────────────────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Builder {
    Unsorted,
    SortedInMemory,
    SortedStream,
}

impl Builder {
    pub fn short(self) -> &'static str {
        match self {
            Self::Unsorted => "unsorted",
            Self::SortedInMemory => "sorted_in_memory",
            Self::SortedStream => "sorted_stream",
        }
    }
}

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
}

impl Source {
    pub fn short(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::InMemory => "in_memory",
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

/// Build the in-memory store for a config, dispatching the generic builder on
/// the runtime `Builder` enum. A store rather than a bare array: under the
/// Dictionary layout the array holds only u32 codes, and the term dictionary
/// travels beside it in the builder's `BuiltArray` — `from_built` is the one
/// constructor that accepts that pair.
pub fn build_store(builder: Builder, layout: Layout, index: Index, size: usize) -> VortexRdfStore {
    rt().block_on(async move {
        let stream = generate_rdf_data_stream(size);
        let strategy = layout.strategy();
        let indexes = index.types();
        match builder {
            Builder::Unsorted => {
                VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
                    stream, strategy, indexes,
                )
                .await
            }
            Builder::SortedInMemory => {
                VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
                    stream, strategy, indexes,
                )
                .await
            }
            Builder::SortedStream => {
                VortexRdfStore::build_vortex_array_with_builder::<SortedStreamBuilder>(
                    stream, strategy, indexes,
                )
                .await
            }
        }
        .and_then(VortexRdfStore::from_built)
        .expect("failed to build vortex store")
    })
}

pub type CacheKey = (Builder, Layout, Index, usize);

/// Cache of built in-memory stores (cheaply cloneable: Arc-shared internals).
/// Under the star design only a handful of distinct configs are ever
/// requested, so this stays naturally bounded (unlike the old full-factorial
/// cache, which held every combination for the process lifetime).
static STORE_CACHE: OnceLock<Mutex<HashMap<CacheKey, VortexRdfStore>>> = OnceLock::new();
static FILE_CACHE: OnceLock<Mutex<HashMap<CacheKey, PathBuf>>> = OnceLock::new();

pub fn cached_store(builder: Builder, layout: Layout, index: Index, size: usize) -> VortexRdfStore {
    let cache = STORE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (builder, layout, index, size);
    if let Some(store) = cache.lock().unwrap().get(&key) {
        return store.clone();
    }
    let store = build_store(builder, layout, index, size);
    cache.lock().unwrap().insert(key, store.clone());
    store
}

pub fn cached_file(builder: Builder, layout: Layout, index: Index, size: usize) -> PathBuf {
    let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (builder, layout, index, size);
    if let Some(path) = cache.lock().unwrap().get(&key) {
        return path.clone();
    }
    let store = cached_store(builder, layout, index, size);
    let dir = PathBuf::from("target/bench_vortex_files");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!(
        "{}_{}_{}_{}.vortex",
        builder.short(),
        layout.short(),
        index.short(),
        size
    ));
    rt().block_on(async {
        let arr = store
            .to_serializable_array()
            .await
            .expect("serializable array");
        let writer = tokio::fs::File::create(&path).await.expect("create file");
        io::serialize(arr, writer).await.expect("serialize file");
    });
    cache.lock().unwrap().insert(key, path.clone());
    path
}

/// Construct a store over a config's data, from the requested source. Both are
/// untimed: `from_file` reads the footer only, and the in-memory arm clones a
/// cached (Arc-shared) store.
pub fn make_store(
    source: Source,
    builder: Builder,
    layout: Layout,
    index: Index,
    size: usize,
) -> VortexRdfStore {
    match source {
        Source::File => {
            let path = cached_file(builder, layout, index, size);
            rt().block_on(async {
                VortexRdfStore::from_file(path)
                    .await
                    .expect("open file store")
            })
        }
        Source::InMemory => cached_store(builder, layout, index, size),
    }
}

type DictIndexedFileKey = (DictionaryPlacement, Index, usize);
static DICT_INDEXED_FILE_CACHE: OnceLock<Mutex<HashMap<DictIndexedFileKey, PathBuf>>> =
    OnceLock::new();

/// Like `cached_dict_file`, but with a secondary index — the files behind
/// the placement rows of the match matrix benches.
pub fn cached_dict_indexed_file(
    placement: DictionaryPlacement,
    index: Index,
    size: usize,
) -> PathBuf {
    let cache = DICT_INDEXED_FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (placement, index, size);
    if let Some(path) = cache.lock().unwrap().get(&key) {
        return path.clone();
    }
    let dir = PathBuf::from("target/bench_vortex_files");
    std::fs::create_dir_all(&dir).unwrap();
    let placement_name = match placement {
        DictionaryPlacement::Padded => "padded",
        DictionaryPlacement::Sidecar => "sidecar",
    };
    let path = dir.join(format!(
        "dict_{placement_name}_{}_{size}.vortex",
        index.short()
    ));
    rt().block_on(async {
        io::quads_stream_to_vortex_file_with_builder::<SortedStreamBuilder, _>(
            generate_rdf_data_stream(size),
            &path,
            LayoutStrategy::Dictionary,
            index.types(),
            placement,
        )
        .await
        .expect("write indexed dictionary bench file");
    });
    cache.lock().unwrap().insert(key, path.clone());
    path
}

// ── match patterns ──────────────────────────────────────────────────────────

// Each variant names the bound components by letter (Subject/Predicate/
// Object/Graph), so `SPOG` is consistent with its siblings, not a word to
// re-case.
#[allow(clippy::upper_case_acronyms)]
#[derive(Copy, Clone, Debug)]
pub enum Pattern {
    S,
    P,
    O,
    PO,
    G,
    SPOG,
}

pub const PATTERNS: &[Pattern] = &[
    Pattern::S,
    Pattern::P,
    Pattern::O,
    Pattern::PO,
    Pattern::G,
    Pattern::SPOG,
];

/// Probe terms, all chosen to hit rows the generator actually emits (see the
/// module docs on selectivity).
#[allow(clippy::type_complexity)]
pub fn terms_for(
    pattern: Pattern,
) -> (
    Option<NamedOrBlankNode>,
    Option<NamedNode>,
    Option<Term>,
    Option<GraphName>,
) {
    let s =
        || NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/subject/0"));
    let p = || NamedNode::new_unchecked("http://example.org/predicate/0");
    let o = || Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
    let g = || GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/graph/0"));

    match pattern {
        Pattern::S => (Some(s()), None, None, None),
        Pattern::P => (None, Some(p()), None, None),
        Pattern::O => (None, None, Some(o()), None),
        Pattern::PO => (None, Some(p()), Some(o()), None),
        Pattern::G => (None, None, None, Some(g())),
        Pattern::SPOG => (Some(s()), Some(p()), Some(o()), Some(g())),
    }
}

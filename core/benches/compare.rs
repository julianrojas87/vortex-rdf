//! Cross-library comparative benchmark for the Rust tab.
//!
//! The Rust counterpart to `js/bench/compare.bench.ts` and `python/bench/run.py`:
//! wall-clock, comparative, **never uploaded to CodSpeed** (`benchmark.rs` is the
//! instrumented library-only suite). It answers the question those two tabs answer
//! for their ecosystems — how does this store compare with what else you would
//! reach for — for the ecosystem the store is actually written in.
//!
//! It reads the *same N-Triples file* the Python suite builds, so a row here and a
//! row on the Python tab measure the same bytes rather than merely the same
//! generator. The file is written once, by whichever suite runs first.
//!
//! # Isolation
//!
//! One adapter per process, re-executing this binary with `--adapter <slug>`. That
//! is not ceremony: a cheap pattern measured after a scan-backed one reads ~3x
//! slow in a shared process, and peak RSS is only attributable to a library that
//! had a process to itself. The parent merges the children's JSON.
//!
//! # Libraries
//!
//! oxigraph enters twice — its in-memory store and its RocksDB-backed one, the
//! same library across the residency axis the Vortex (file, memory) pairs cross.
//! sophia holds its data in memory; hdt is a read-only compressed file format,
//! the other entrant with an artifact to open. hdt has no named graphs, so it
//! declines quad patterns rather than answering a different question.

use std::fmt::Write as _;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::time::Instant;

use sophia::api::MownStr;
use sophia::api::term::{IriRef, SimpleTerm};
use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::{IndexType, LayoutStrategy, VortexRdfStore};

// ─── Scale ──────────────────────────────────────────────────────────────────

/// Rows in the comparative dataset. Matches `BENCH_DIM=128` on the JavaScript and
/// Python tabs (128^3), so the three comparative tables share a scale.
fn dataset_size() -> usize {
    std::env::var("BENCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_097_152)
}

/// Repetitions, matching `QUERY_SAMPLES`/`HEAVY_SAMPLES` in `support/mod.rs` and
/// their twins in the other two harnesses.
const QUERY_ITERS: usize = 10;
const HEAVY_ITERS: usize = 3;
const QUERY_WARMUP: usize = 5;

// ─── Dataset ────────────────────────────────────────────────────────────────
//
// The shape lives in `support/dataset.rs`, included directly rather than
// re-derived: the instrumented suite (`benchmark.rs`) generates the very same
// rows in process, and a second port here is how the two would drift apart into
// tables that cannot be read beside each other. Only the *delivery* differs —
// this suite writes an N-Triples file, because every external library ingests
// from one.
//
// Items the file exposes for the in-process generators (oxrdf probe terms, the
// pattern enum) have no use here.
#[path = "support/dataset.rs"]
#[allow(dead_code)]
mod dataset;

use dataset::{
    BASE, graph_iri as graph_term, predicate_iri as predicate_term, subject_iri as subject_term,
};

/// Moduli for the *triples* dataset: one graph, i.e. the default graph, which is
/// what an N-Triples file can carry.
fn moduli(n: usize) -> dataset::Moduli {
    dataset::moduli(n, 1)
}

/// Objects in their N-Triples spelling: angle-bracketed IRI or quoted literal.
fn object_term(i: usize) -> String {
    dataset::object_term(i).to_ntriples()
}

/// Rows in the named-graph dataset — the quads the G/SPOG probes run against.
/// 32^4, matching `BENCH_DIM_QUADS=32` on the other two tabs.
fn quads_size() -> usize {
    std::env::var("BENCH_SIZE_QUADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_048_576)
}

/// The N-Quads file for the named-graph patterns. Shared with the Python suite
/// (`quads_<n>.nq`, GRAPHS=8 — `dataset::WANT_GRAPHS`, the count the generated
/// dataset asks for too); generated here only when missing.
fn ensure_quads_dataset(n: usize) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let path = root
        .join("python/bench/.data")
        .join(format!("quads_{n}.nq"));
    if path.exists() {
        return path;
    }
    eprintln!("generating {} quads -> {}", n, path.display());
    std::fs::create_dir_all(path.parent().unwrap()).expect("create data dir");
    let m = dataset::moduli(n, dataset::WANT_GRAPHS);
    let tmp = path.with_extension("partial");
    let mut w = BufWriter::with_capacity(1 << 20, std::fs::File::create(&tmp).expect("create"));
    let mut line = String::with_capacity(256);
    for i in 0..n {
        line.clear();
        writeln!(
            line,
            "<{}> <{}> {} <{}> .",
            subject_term(i % m.n_subj),
            predicate_term(i % m.n_pred),
            object_term(i % m.n_obj),
            graph_term(i % m.n_graph)
        )
        .unwrap();
        w.write_all(line.as_bytes()).expect("write");
    }
    w.flush().expect("flush");
    std::fs::rename(&tmp, &path).expect("rename");
    path
}

/// The N-Triples file every adapter ingests. Shared with the Python suite, which
/// writes it to the same path -- whichever runs first pays for it.
fn dataset_path(n: usize) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    root.join("python/bench/.data")
        .join(format!("triples_{n}.nt"))
}

fn ensure_dataset(n: usize) -> PathBuf {
    let path = dataset_path(n);
    if path.exists() {
        return path;
    }
    eprintln!("generating {} triples -> {}", n, path.display());
    std::fs::create_dir_all(path.parent().unwrap()).expect("create data dir");
    let m = moduli(n);
    let tmp = path.with_extension("partial");
    let mut w = BufWriter::with_capacity(1 << 20, std::fs::File::create(&tmp).expect("create"));
    let mut line = String::with_capacity(256);
    for i in 0..n {
        line.clear();
        writeln!(
            line,
            "<{}> <{}> {} .",
            subject_term(i % m.n_subj),
            predicate_term(i % m.n_pred),
            object_term(i % m.n_obj)
        )
        .unwrap();
        w.write_all(line.as_bytes()).expect("write");
    }
    w.flush().expect("flush");
    // Never leave a half-written dataset that looks complete.
    std::fs::rename(&tmp, &path).expect("rename");
    path
}

/// A triple pattern probe. Terms are fixed at index 0, so every probe matches at
/// least row 0 and no pattern silently measures a zero-row query.
struct Pat {
    name: &'static str,
    s: Option<String>,
    p: Option<String>,
    o: Option<String>,
    g: Option<String>,
}

fn probes(n: usize) -> Vec<Pat> {
    let _ = moduli(n);
    let s0 = format!("<{}>", subject_term(0));
    let p0 = format!("<{}>", predicate_term(0));
    let o0 = object_term(0);
    vec![
        Pat {
            name: "S",
            s: Some(s0.clone()),
            p: None,
            o: None,
            g: None,
        },
        Pat {
            name: "P",
            s: None,
            p: Some(p0.clone()),
            o: None,
            g: None,
        },
        Pat {
            name: "O",
            s: None,
            p: None,
            o: Some(o0.clone()),
            g: None,
        },
        Pat {
            name: "PO",
            s: None,
            p: Some(p0.clone()),
            o: Some(o0.clone()),
            g: None,
        },
        Pat {
            name: "SPO",
            s: Some(s0),
            p: Some(p0),
            o: Some(o0),
            g: None,
        },
        Pat {
            name: "full",
            s: None,
            p: None,
            o: None,
            g: None,
        },
    ]
}

/// The two probes only the named-graph dataset can answer. Index-0 terms, like
/// every other probe, so both match at least row 0.
fn quad_probes() -> Vec<Pat> {
    let s0 = format!("<{}>", subject_term(0));
    let p0 = format!("<{}>", predicate_term(0));
    let o0 = object_term(0);
    let g0 = format!("<{}>", graph_term(0));
    vec![
        Pat {
            name: "G",
            s: None,
            p: None,
            o: None,
            g: Some(g0.clone()),
        },
        Pat {
            name: "SPOG",
            s: Some(s0),
            p: Some(p0),
            o: Some(o0),
            g: Some(g0),
        },
    ]
}

// ─── Result rows (the dashboard's shape) ────────────────────────────────────

/// One measured cell, in the same JSON shape `js/bench/shared.ts` and
/// `python/bench/worker.py` emit, so `render_bench_dashboard.py` reads all three
/// with one code path.
#[derive(serde::Serialize)]
struct Row {
    group: String,
    variant: Option<String>,
    id: String,
    fastest: String,
    slowest: String,
    median: String,
    mean: String,
    fastest_ns: Option<f64>,
    slowest_ns: Option<f64>,
    median_ns: Option<f64>,
    mean_ns: Option<f64>,
    samples: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    regime: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsupported: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

fn fmt_ns(ns: f64) -> String {
    // Matches the JS and Python harnesses' formatting so all three tabs read alike.
    if ns < 1_000.0 {
        format!("{:.4} ns", ns)
    } else if ns < 1_000_000.0 {
        format!("{:.4} µs", ns / 1_000.0)
    } else if ns < 1_000_000_000.0 {
        format!("{:.4} ms", ns / 1_000_000.0)
    } else {
        format!("{:.4} s", ns / 1_000_000_000.0)
    }
}

fn row_from(id: &str, mut samples_ns: Vec<f64>, regime: Option<&'static str>) -> Row {
    samples_ns.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples_ns.len();
    let median = samples_ns[n / 2];
    let mean = samples_ns.iter().sum::<f64>() / n as f64;
    let (group, variant) = split_id(id);
    Row {
        group,
        variant,
        id: id.to_string(),
        fastest: fmt_ns(samples_ns[0]),
        slowest: fmt_ns(samples_ns[n - 1]),
        median: fmt_ns(median),
        mean: fmt_ns(mean),
        fastest_ns: Some(samples_ns[0]),
        slowest_ns: Some(samples_ns[n - 1]),
        median_ns: Some(median),
        mean_ns: Some(mean),
        samples: n.to_string(),
        regime,
        unsupported: None,
        reason: None,
    }
}

/// A cell with no measurement to make. Mirrors `unsupported_row` in
/// `python/bench/worker.py` and `unsupportedRow` in `js/bench/shared.ts`: the null
/// median keeps it out of its column's best/ratio arithmetic, and one word covers
/// every kind of absence with the specifics in the tooltip.
fn unsupported_row(id: &str, reason: &str) -> Row {
    let (group, variant) = split_id(id);
    Row {
        group,
        variant,
        id: id.to_string(),
        fastest: "unsupported".into(),
        slowest: "unsupported".into(),
        median: "unsupported".into(),
        mean: "unsupported".into(),
        fastest_ns: None,
        slowest_ns: None,
        median_ns: None,
        mean_ns: None,
        samples: "0".into(),
        regime: None,
        unsupported: Some(true),
        reason: Some(reason.to_string()),
    }
}

fn split_id(id: &str) -> (String, Option<String>) {
    match id.split_once("::") {
        Some((g, v)) => (g.to_string(), Some(v.to_string())),
        None => (id.to_string(), None),
    }
}

/// Time `f` `iters` times after `warmup` untimed runs, and record the median.
fn measure<F: FnMut()>(
    id: &str,
    mut f: F,
    rows: &mut Vec<Row>,
    iters: usize,
    warmup: usize,
    regime: Option<&'static str>,
) {
    for _ in 0..warmup {
        f();
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        samples.push(t.elapsed().as_nanos() as f64);
    }
    rows.push(row_from(id, samples, regime));
}

/// Bytes the build wrote: a file's length, or a directory's recursive total —
/// a RocksDB store is a directory of SSTs, not a single file.
fn artifact_size(path: &Path) -> Option<u64> {
    let md = std::fs::metadata(path).ok()?;
    if md.is_file() {
        return Some(md.len());
    }
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            let md = entry.metadata().ok()?;
            if md.is_dir() {
                stack.push(entry.path());
            } else {
                total += md.len();
            }
        }
    }
    Some(total)
}

/// This process's peak resident set size, in MB.
///
/// `VmHWM` is the kernel's own high-water mark, not a point-in-time sample, so it
/// survives the allocator having already returned the peak. It is attributable to
/// one library only because each adapter runs in its own process.
fn peak_rss_mb() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some((kb / 1024.0 * 10.0).round() / 10.0)
}

// ─── Adapters ───────────────────────────────────────────────────────────────

/// What one library under test must be able to do. `build` goes from the shared
/// N-Triples file to whatever that library calls a store; `open` is the separate
/// cost of getting a *fresh process* to a queryable state over an artifact the
/// build already wrote -- which only a library with a persistent form has.
trait Adapter {
    fn slug(&self) -> &'static str;
    fn label(&self) -> &'static str;

    /// True when opening the artifact is a distinct operation from building it.
    /// False for a store that lives in memory: "opening" it is re-parsing the
    /// source, which the Build column already reports.
    fn has_distinct_open(&self) -> bool {
        true
    }

    fn artifact(&self, workdir: &Path) -> PathBuf;
    fn build(&self, src: &Path, artifact: &Path);
    fn open(&self, artifact: &Path, src: &Path) -> Box<dyn Queryable>;

    /// The named-graph dataset's artifact, or None where the model has no
    /// quads at all (hdt) — those G/SPOG cells read `unsupported`.
    fn quads_artifact(&self, workdir: &Path) -> Option<PathBuf> {
        Some(workdir.join("quads.marker"))
    }
    fn build_quads(&self, _src: &Path, _artifact: &Path) {}
    fn open_quads(&self, _artifact: &Path, _src: &Path) -> Box<dyn Queryable> {
        unreachable!("open_quads on an adapter whose quads_artifact is None")
    }
}

trait Queryable {
    fn count(&self, pat: &Pat) -> usize;
}

// ── Vortex ──

struct VortexAdapter {
    slug: &'static str,
    label: &'static str,
    layout: LayoutStrategy,
    indexes: Vec<IndexType>,
    /// Load the whole store up front instead of staying file-backed and lazy.
    /// The same axis the Python tab crosses with the secondary index, and the
    /// configuration that matches how the in-memory libraries here hold data.
    in_memory: bool,
}

fn rt() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

impl Adapter for VortexAdapter {
    fn slug(&self) -> &'static str {
        self.slug
    }
    fn label(&self) -> &'static str {
        self.label
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        workdir.join(format!("{}.vortex", self.slug))
    }
    fn build(&self, src: &Path, artifact: &Path) {
        let file = std::fs::File::open(src).expect("open source");
        let quads = parse_quads_from_reader(file, oxrdfio::RdfFormat::NTriples);
        rt().block_on(vortex_rdf_core::io::quads_stream_to_vortex_file(
            Box::pin(quads),
            artifact,
            self.layout,
            self.indexes.clone(),
        ))
        .expect("build vortex file");
    }
    fn open(&self, artifact: &Path, _src: &Path) -> Box<dyn Queryable> {
        let store = rt().block_on(async {
            let store = VortexRdfStore::from_file(artifact)
                .await
                .expect("open vortex file");
            if !self.in_memory {
                return store;
            }
            // Round-trip through the serializable parts -- rows, index components
            // and the dictionary those codes address -- which is what the Python
            // bindings' `in_memory=True` does, so the two tabs' memory rows mean
            // the same thing.
            let parts = store
                .to_serializable_parts()
                .await
                .expect("serializable parts");
            VortexRdfStore::from_parts(parts).expect("load store into memory")
        });
        Box::new(VortexStore(store))
    }

    fn quads_artifact(&self, workdir: &Path) -> Option<PathBuf> {
        Some(workdir.join(format!("{}_quads.vortex", self.slug)))
    }
    fn build_quads(&self, src: &Path, artifact: &Path) {
        let file = std::fs::File::open(src).expect("open quads source");
        let quads = parse_quads_from_reader(file, oxrdfio::RdfFormat::NQuads);
        rt().block_on(vortex_rdf_core::io::quads_stream_to_vortex_file(
            Box::pin(quads),
            artifact,
            self.layout,
            self.indexes.clone(),
        ))
        .expect("build vortex quads file");
    }
    fn open_quads(&self, artifact: &Path, src: &Path) -> Box<dyn Queryable> {
        self.open(artifact, src)
    }
}

struct VortexStore(VortexRdfStore);

impl Queryable for VortexStore {
    fn count(&self, pat: &Pat) -> usize {
        let s = pat.s.as_ref().map(|t| parse_subject(t));
        let p = pat.p.as_ref().map(|t| parse_predicate(t));
        let o = pat.o.as_ref().map(|t| parse_object(t));
        let g = pat
            .g
            .as_ref()
            .map(|t| oxrdf::GraphName::NamedNode(oxrdf::NamedNode::new_unchecked(strip_iri(t))));
        rt().block_on(async {
            let view = self
                .0
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .expect("match");
            // Materialize, don't just read a row count. Every other library
            // here iterates its results to count them, and the view's `size`
            // would answer a full scan from metadata in nanoseconds -- a
            // number that compares nothing. `quads_vec` builds four terms per
            // row, so on a triple pattern Vortex does strictly more work than
            // the libraries returning triples, not less.
            view.quads_vec().await.expect("materialize").len()
        })
    }
}

fn parse_subject(t: &str) -> oxrdf::NamedOrBlankNode {
    oxrdf::NamedOrBlankNode::NamedNode(oxrdf::NamedNode::new_unchecked(strip_iri(t)))
}
fn parse_predicate(t: &str) -> oxrdf::NamedNode {
    oxrdf::NamedNode::new_unchecked(strip_iri(t))
}
fn parse_object(t: &str) -> oxrdf::Term {
    if let Some(lit) = t.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(lit))
    } else {
        oxrdf::Term::NamedNode(oxrdf::NamedNode::new_unchecked(strip_iri(t)))
    }
}
fn strip_iri(t: &str) -> String {
    t.trim_start_matches('<').trim_end_matches('>').to_string()
}

// ── oxigraph ──

struct OxigraphAdapter;

impl Adapter for OxigraphAdapter {
    fn slug(&self) -> &'static str {
        "oxigraph"
    }
    fn label(&self) -> &'static str {
        "oxigraph (memory)"
    }
    fn has_distinct_open(&self) -> bool {
        false
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        // No artifact of its own: the source file is what a fresh process reloads.
        workdir.join("oxigraph.marker")
    }
    fn build(&self, src: &Path, artifact: &Path) {
        let store = load_oxigraph(src, oxigraph::io::RdfFormat::NTriples);
        std::fs::write(artifact, format!("{}", store.len().unwrap())).ok();
    }
    fn open(&self, _artifact: &Path, src: &Path) -> Box<dyn Queryable> {
        Box::new(OxStore(load_oxigraph(
            src,
            oxigraph::io::RdfFormat::NTriples,
        )))
    }

    fn build_quads(&self, src: &Path, artifact: &Path) {
        let store = load_oxigraph(src, oxigraph::io::RdfFormat::NQuads);
        std::fs::write(artifact, format!("{}", store.len().unwrap())).ok();
    }
    fn open_quads(&self, _artifact: &Path, src: &Path) -> Box<dyn Queryable> {
        Box::new(OxStore(load_oxigraph(src, oxigraph::io::RdfFormat::NQuads)))
    }
}

fn load_oxigraph(src: &Path, format: oxigraph::io::RdfFormat) -> oxigraph::store::Store {
    let store = oxigraph::store::Store::new().expect("new store");
    let file = std::io::BufReader::new(std::fs::File::open(src).expect("open source"));
    store.load_from_reader(format, file).expect("bulk load");
    store
}

struct OxStore(oxigraph::store::Store);

impl Queryable for OxStore {
    fn count(&self, pat: &Pat) -> usize {
        use oxigraph::model::{GraphNameRef, NamedNodeRef, NamedOrBlankNodeRef, TermRef};
        let s = pat.s.as_ref().map(|t| strip_iri(t));
        let p = pat.p.as_ref().map(|t| strip_iri(t));
        let o = pat.o.clone();
        let g = pat.g.as_ref().map(|t| strip_iri(t));
        let s_ref = s
            .as_ref()
            .map(|v| NamedOrBlankNodeRef::NamedNode(NamedNodeRef::new_unchecked(v)));
        let p_ref = p.as_ref().map(|v| NamedNodeRef::new_unchecked(v));
        let g_ref = g
            .as_ref()
            .map(|v| GraphNameRef::NamedNode(NamedNodeRef::new_unchecked(v)));
        let lit;
        let o_ref = match o.as_ref() {
            None => None,
            Some(t) if t.starts_with('"') => {
                lit = oxigraph::model::Literal::new_simple_literal(
                    t.trim_start_matches('"').trim_end_matches('"'),
                );
                Some(TermRef::Literal(lit.as_ref()))
            }
            Some(t) => {
                let stripped = strip_iri(t);
                // NamedNodeRef borrows, so the owned String must outlive it.
                return self.count_named_object(&s_ref, &p_ref, &stripped, &g_ref);
            }
        };
        self.0.quads_for_pattern(s_ref, p_ref, o_ref, g_ref).count()
    }
}

impl OxStore {
    fn count_named_object(
        &self,
        s: &Option<oxigraph::model::NamedOrBlankNodeRef<'_>>,
        p: &Option<oxigraph::model::NamedNodeRef<'_>>,
        o: &str,
        g: &Option<oxigraph::model::GraphNameRef<'_>>,
    ) -> usize {
        use oxigraph::model::{NamedNodeRef, TermRef};
        let o_ref = TermRef::NamedNode(NamedNodeRef::new_unchecked(o));
        self.0.quads_for_pattern(*s, *p, Some(o_ref), *g).count()
    }
}

// ── oxigraph (rocksdb) ──

/// oxigraph over its persistent RocksDB backend: the artifact is a database
/// directory, built with the bulk loader (oxigraph's recommended large-ingest
/// path) and reopened read-only. Read-only is what makes the harness's
/// overlapping handles legal — RocksDB allows one read-write instance, but any
/// number of read-only ones, and the warm handle is still alive while the cold
/// arms open fresh ones.
struct OxigraphRocksAdapter;

impl Adapter for OxigraphRocksAdapter {
    fn slug(&self) -> &'static str {
        "oxigraph_rocksdb"
    }
    fn label(&self) -> &'static str {
        "oxigraph (rocksdb)"
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        workdir.join("oxigraph.rocksdb")
    }
    fn build(&self, src: &Path, artifact: &Path) {
        build_oxigraph_rocksdb(artifact, src, oxigraph::io::RdfFormat::NTriples);
    }
    fn open(&self, artifact: &Path, _src: &Path) -> Box<dyn Queryable> {
        Box::new(OxStore(
            oxigraph::store::Store::open_read_only(artifact).expect("open rocksdb read-only"),
        ))
    }

    fn quads_artifact(&self, workdir: &Path) -> Option<PathBuf> {
        Some(workdir.join("quads.rocksdb"))
    }
    fn build_quads(&self, src: &Path, artifact: &Path) {
        build_oxigraph_rocksdb(artifact, src, oxigraph::io::RdfFormat::NQuads);
    }
    fn open_quads(&self, artifact: &Path, src: &Path) -> Box<dyn Queryable> {
        self.open(artifact, src)
    }
}

/// Bulk-load `src` into a fresh RocksDB store at `dir`, flushed so the data is
/// in SSTs rather than the WAL — which is what a read-only open reads. Removes
/// any previous database first: each build iteration must pay the whole cost,
/// not a re-insert of rows RocksDB already holds.
fn build_oxigraph_rocksdb(
    dir: &Path,
    src: &Path,
    format: oxigraph::io::RdfFormat,
) -> oxigraph::store::Store {
    let _ = std::fs::remove_dir_all(dir);
    let store = oxigraph::store::Store::open(dir).expect("open rocksdb read-write");
    let mut loader = store.bulk_loader();
    let file = std::io::BufReader::new(std::fs::File::open(src).expect("open source"));
    loader.load_from_reader(format, file).expect("bulk load");
    loader.commit().expect("commit bulk load");
    store.flush().expect("flush");
    store
}

// ── sophia ──

struct SophiaAdapter;

impl Adapter for SophiaAdapter {
    fn slug(&self) -> &'static str {
        "sophia"
    }
    fn label(&self) -> &'static str {
        "sophia (memory)"
    }
    fn has_distinct_open(&self) -> bool {
        false
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        workdir.join("sophia.marker")
    }
    fn build(&self, src: &Path, artifact: &Path) {
        let g = load_sophia(src);
        use sophia::api::graph::Graph;
        std::fs::write(artifact, format!("{}", g.triples().count())).ok();
    }
    fn open(&self, _artifact: &Path, src: &Path) -> Box<dyn Queryable> {
        Box::new(SophiaGraph(load_sophia(src)))
    }

    fn build_quads(&self, src: &Path, artifact: &Path) {
        let d = load_sophia_dataset(src);
        use sophia::api::dataset::Dataset;
        std::fs::write(artifact, format!("{}", d.quads().count())).ok();
    }
    fn open_quads(&self, _artifact: &Path, src: &Path) -> Box<dyn Queryable> {
        Box::new(SophiaDataset(load_sophia_dataset(src)))
    }
}

fn load_sophia_dataset(src: &Path) -> sophia::inmem::dataset::FastDataset {
    use sophia::api::parser::QuadParser;
    use sophia::api::prelude::QuadSource;
    let file = std::io::BufReader::new(std::fs::File::open(src).expect("open quads source"));
    let parser = sophia::turtle::parser::nq::NQuadsParser::default();
    let mut d = sophia::inmem::dataset::FastDataset::new();
    parser
        .parse(file)
        .add_to_dataset(&mut d)
        .expect("parse into sophia dataset");
    d
}

struct SophiaDataset(sophia::inmem::dataset::FastDataset);

impl Queryable for SophiaDataset {
    fn count(&self, pat: &Pat) -> usize {
        use sophia::api::dataset::Dataset;
        use sophia::api::term::matcher::TermMatcher;
        // Same matcher rules as the graph (see `AnyOr`); the graph position
        // needs a GraphNameMatcher, which `gn()` derives from a TermMatcher.
        let s = pat
            .s
            .as_ref()
            .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_iri(t)));
        let p = pat
            .p
            .as_ref()
            .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_iri(t)));
        let o = pat
            .o
            .as_ref()
            .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_object(t)));
        let g = pat
            .g
            .as_ref()
            .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_iri(t)));
        self.0.quads_matching(s, p, o, g.gn()).count()
    }
}

fn load_sophia(src: &Path) -> sophia::inmem::graph::FastGraph {
    use sophia::api::parser::TripleParser;
    use sophia::api::prelude::TripleSource;
    let file = std::io::BufReader::new(std::fs::File::open(src).expect("open source"));
    let parser = sophia::turtle::parser::nt::NTriplesParser::default();
    let mut g = sophia::inmem::graph::FastGraph::new();
    parser
        .parse(file)
        .add_to_graph(&mut g)
        .expect("parse into sophia graph");
    g
}

struct SophiaGraph(sophia::inmem::graph::FastGraph);

impl Queryable for SophiaGraph {
    fn count(&self, pat: &Pat) -> usize {
        use sophia::api::graph::Graph;

        // See `AnyOr`: getting the matcher type right is what decides whether
        // sophia answers from its index or walks the whole graph.
        let s = pat
            .s
            .as_ref()
            .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_iri(t)));
        let p = pat
            .p
            .as_ref()
            .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_iri(t)));
        let o = pat
            .o
            .as_ref()
            .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_object(t)));
        self.0.triples_matching(s, p, o).count()
    }
}

/// "This exact term, or anything" -- the matcher sophia does not ship.
///
/// Two wrong ways to spell this, both of which this suite measured before getting
/// it right. A **closure** matcher reports no [`TermMatcher::constant`], and that
/// constant is exactly what `FastGraph` uses to look a term up in its index, so
/// every pattern degrades to a full scan: `S` came out at 14.19 ms against the
/// graph's own 12.85 ms full scan -- slower than scanning, which is how the
/// mistake announced itself. **`Option<T>`** does report a constant, but its
/// documented meaning is "the wrapped term if any, otherwise *nothing*", so every
/// wildcard silently matches zero rows. This reports the constant when there is
/// one and matches everything when there is not.
enum AnyOr {
    Any,
    Exactly(SimpleTerm<'static>),
}

impl sophia::api::term::matcher::TermMatcher for AnyOr {
    type Term = SimpleTerm<'static>;

    fn matches<T2: sophia::api::term::Term + ?Sized>(&self, term: &T2) -> bool {
        match self {
            Self::Any => true,
            Self::Exactly(t) => sophia::api::term::Term::eq(t, term.borrow_term()),
        }
    }

    fn constant(&self) -> Option<&Self::Term> {
        match self {
            Self::Any => None,
            Self::Exactly(t) => Some(t),
        }
    }
}

fn simple_iri(t: &str) -> SimpleTerm<'static> {
    SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from(strip_iri(t))))
}

/// The dataset's objects are IRIs or plain literals, and a plain literal is an
/// `xsd:string`-typed one -- spelling it any other way would fail to match the
/// terms the parser built, and the count check would catch it as a disagreement.
fn simple_object(t: &str) -> SimpleTerm<'static> {
    if t.starts_with('"') {
        SimpleTerm::LiteralDatatype(
            MownStr::from(t.trim_start_matches('"').trim_end_matches('"').to_string()),
            IriRef::new_unchecked(MownStr::from(
                "http://www.w3.org/2001/XMLSchema#string".to_string(),
            )),
        )
    } else {
        simple_iri(t)
    }
}

// ── hdt ──

struct HdtAdapter;

impl Adapter for HdtAdapter {
    fn slug(&self) -> &'static str {
        "hdt"
    }
    fn label(&self) -> &'static str {
        "hdt (file)"
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        workdir.join("data.hdt")
    }
    fn build(&self, src: &Path, artifact: &Path) {
        let hdt = hdt::Hdt::read_nt(src).expect("build hdt from n-triples");
        let mut out = BufWriter::new(std::fs::File::create(artifact).expect("create hdt"));
        hdt.write(&mut out).expect("write hdt");
    }
    fn open(&self, artifact: &Path, _src: &Path) -> Box<dyn Queryable> {
        let f = std::fs::File::open(artifact).expect("open hdt");
        let hdt = hdt::Hdt::read(std::io::BufReader::new(f)).expect("read hdt");
        Box::new(HdtStore(hdt))
    }

    fn quads_artifact(&self, _workdir: &Path) -> Option<PathBuf> {
        None // triples only: the HDT format has no named graphs
    }
}

struct HdtStore(hdt::Hdt);

impl Queryable for HdtStore {
    fn count(&self, pat: &Pat) -> usize {
        // HDT dictionary strings are bare: IRIs without angle brackets, literals
        // with their quotes. That is exactly what `object_term` already produces
        // for literals, so only IRIs need unwrapping.
        let s = pat.s.as_ref().map(|t| strip_iri(t));
        let p = pat.p.as_ref().map(|t| strip_iri(t));
        let o = pat.o.as_ref().map(|t| {
            if t.starts_with('"') {
                t.clone()
            } else {
                strip_iri(t)
            }
        });
        self.0
            .triples_with_pattern(s.as_deref(), p.as_deref(), o.as_deref())
            .count()
    }
}

/// Time `f` with a fresh `setup()` product per iteration, setup untimed — the
/// delete phase's shape: reopening (or rebuilding) the store must not land in
/// the timed region, or the cell measures the setup instead of the deletes.
fn measure_with_setup<T>(
    id: &str,
    mut setup: impl FnMut() -> T,
    mut f: impl FnMut(T),
    rows: &mut Vec<Row>,
    iters: usize,
) {
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let target = setup();
        let t = Instant::now();
        f(target);
        samples.push(t.elapsed().as_nanos() as f64);
    }
    rows.push(row_from(id, samples, None));
}

/// [`measure_with_setup`] for a cell that carries a cache regime -- the cold
/// query arms, whose per-iteration `open` must stay outside the timed region.
fn measure_with_setup_regime<T>(
    id: &str,
    mut setup: impl FnMut() -> T,
    mut f: impl FnMut(&T),
    rows: &mut Vec<Row>,
    iters: usize,
    regime: Option<&'static str>,
) {
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let target = setup();
        let t = Instant::now();
        f(&target);
        samples.push(t.elapsed().as_nanos() as f64);
    }
    rows.push(row_from(id, samples, regime));
}

fn mut_batch() -> usize {
    std::env::var("MUT_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
}

/// A batch in a namespace disjoint from the dataset's, so the add phase
/// inserts rows the store cannot already hold. Port of datasets.py's
/// `fresh_quads`.
fn fresh_quads(n: usize) -> Vec<oxrdf::Quad> {
    (0..n)
        .map(|i| {
            oxrdf::Quad::new(
                oxrdf::NamedNode::new_unchecked(format!("{BASE}/fresh/2026/subject/{i:09}")),
                oxrdf::NamedNode::new_unchecked(format!("{BASE}/fresh/2026/property/0000")),
                oxrdf::Term::NamedNode(oxrdf::NamedNode::new_unchecked(format!(
                    "{BASE}/fresh/2026/object/{i:09}"
                ))),
                oxrdf::GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// The first `take` quads of the triples dataset — the delete phase needs a
/// batch of rows that actually exist in the store. Port of `dataset_prefix`.
fn dataset_prefix(n: usize, take: usize) -> Vec<oxrdf::Quad> {
    let m = moduli(n);
    (0..take.min(n))
        .map(|i| {
            let o = object_term(i % m.n_obj);
            oxrdf::Quad::new(
                oxrdf::NamedNode::new_unchecked(subject_term(i % m.n_subj)),
                oxrdf::NamedNode::new_unchecked(predicate_term(i % m.n_pred)),
                parse_object(&o),
                oxrdf::GraphName::DefaultGraph,
            )
        })
        .collect()
}

// ─── One adapter's run ──────────────────────────────────────────────────────

fn adapters() -> Vec<Box<dyn Adapter>> {
    /// The same layout x index x residency matrix the Python tab shows, so the two
    /// tabs' Vortex rows line up one for one. A (file, memory) pair builds the same
    /// configuration in its own process and differs only in how it opens the result,
    /// so their Build cells agreeing is a free consistency check on the harness.
    fn vortex(
        slug: &'static str,
        label: &'static str,
        layout: LayoutStrategy,
        indexes: Vec<IndexType>,
        in_memory: bool,
    ) -> Box<dyn Adapter> {
        Box::new(VortexAdapter {
            slug,
            label,
            layout,
            indexes,
            in_memory,
        })
    }
    use IndexType::{SecondaryByCopy, SecondaryByReference};
    use LayoutStrategy::{Default as DefaultLayout, Dictionary};
    vec![
        vortex("vortex_dict", "Vortex Dict", Dictionary, vec![], false),
        vortex(
            "vortex_dict_mem",
            "Vortex Dict (in-memory)",
            Dictionary,
            vec![],
            true,
        ),
        vortex(
            "vortex_default",
            "Vortex Default",
            DefaultLayout,
            vec![],
            false,
        ),
        vortex(
            "vortex_dict_bycopy",
            "Vortex Dict+ByCopy",
            Dictionary,
            vec![SecondaryByCopy],
            false,
        ),
        vortex(
            "vortex_dict_bycopy_mem",
            "Vortex Dict+ByCopy (in-memory)",
            Dictionary,
            vec![SecondaryByCopy],
            true,
        ),
        vortex(
            "vortex_dict_byref",
            "Vortex Dict+ByRef",
            Dictionary,
            vec![SecondaryByReference],
            false,
        ),
        vortex(
            "vortex_dict_byref_mem",
            "Vortex Dict+ByRef (in-memory)",
            Dictionary,
            vec![SecondaryByReference],
            true,
        ),
        Box::new(OxigraphAdapter),
        Box::new(OxigraphRocksAdapter),
        Box::new(SophiaAdapter),
        Box::new(HdtAdapter),
    ]
}

#[derive(serde::Serialize)]
struct WorkerOut {
    slug: String,
    label: String,
    rows: Vec<Row>,
    counts: std::collections::BTreeMap<String, usize>,
    artifact_bytes: Option<u64>,
    peak_rss_mb: Option<f64>,
}

fn run_adapter(a: &dyn Adapter, n: usize) -> WorkerOut {
    let src = ensure_dataset(n);
    let workdir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/bench_compare")
        .join(a.slug());
    std::fs::create_dir_all(&workdir).expect("create workdir");
    let artifact = a.artifact(&workdir);
    let log = |m: &str| eprintln!("  [{}] {}", a.label(), m);

    let mut rows: Vec<Row> = Vec::new();

    log("build…");
    measure(
        &format!("ingest::{}", a.slug()),
        || a.build(&src, &artifact),
        &mut rows,
        HEAVY_ITERS,
        0,
        None,
    );

    // Only a real persistent form has a size; the in-memory adapters' marker
    // files are bookkeeping, and reporting their few bytes would top the
    // smallest-first size panel with a store that writes no artifact at all.
    let artifact_bytes = if a.has_distinct_open() {
        artifact_size(&artifact)
    } else {
        None
    };

    // Open: only where there is an artifact to open. For an in-memory store,
    // opening is re-parsing the source, which the Build column already reports.
    if a.has_distinct_open() {
        log("open…");
        measure(
            &format!("open::{}", a.slug()),
            || {
                let _ = a.open(&artifact, &src);
            },
            &mut rows,
            QUERY_ITERS,
            QUERY_WARMUP,
            None,
        );
    } else {
        rows.push(unsupported_row(
            &format!("open::{}", a.slug()),
            "no artifact to reopen: a fresh process re-parses the source, which is the Build column",
        ));
    }

    let handle = a.open(&artifact, &src);
    let pats = probes(n);
    let mut counts = std::collections::BTreeMap::new();

    for pat in &pats {
        let heavy = pat.name == "full";
        counts.insert(pat.name.to_string(), handle.count(pat));
        log(&format!("match {} (warm)…", pat.name));
        measure(
            &format!("{}::{}", a.slug(), pat.name),
            || {
                handle.count(pat);
            },
            &mut rows,
            if heavy { HEAVY_ITERS } else { QUERY_ITERS },
            if heavy { 0 } else { QUERY_WARMUP },
            Some("warm"),
        );

        // Cold: a fresh handle per iteration, so every probe/segment cache starts
        // empty -- but the open itself is UNTIMED setup. Timing it here would
        // make every cold cell a restatement of the Open column (measured: the
        // in-memory variants' cold S was 3.51 ms against a 3.83 ms open, i.e.
        // ~100% open), and would mean something different from the cold columns
        // on every other tab, which all exclude it.
        if a.has_distinct_open() {
            log(&format!("match {} (cold)…", pat.name));
            measure_with_setup_regime(
                &format!("{}::{}", a.slug(), pat.name),
                || a.open(&artifact, &src),
                |h| {
                    h.count(pat);
                },
                &mut rows,
                if heavy { HEAVY_ITERS } else { QUERY_ITERS },
                Some("cold"),
            );
        }
    }

    drop(handle);

    // ── named-graph patterns, on the quads dataset ──────────────────────────
    let nq = quads_size();
    let qpats = quad_probes();
    match a.quads_artifact(&workdir) {
        Some(qartifact) => {
            let qsrc = ensure_quads_dataset(nq);
            log("build quads…");
            a.build_quads(&qsrc, &qartifact); // untimed: Build is the triples column
            let qhandle = a.open_quads(&qartifact, &qsrc);
            for pat in &qpats {
                counts.insert(pat.name.to_string(), qhandle.count(pat));
                log(&format!("match {} (warm)…", pat.name));
                measure(
                    &format!("{}::{}", a.slug(), pat.name),
                    || {
                        qhandle.count(pat);
                    },
                    &mut rows,
                    QUERY_ITERS,
                    QUERY_WARMUP,
                    Some("warm"),
                );
                if a.has_distinct_open() {
                    log(&format!("match {} (cold)…", pat.name));
                    measure_with_setup_regime(
                        &format!("{}::{}", a.slug(), pat.name),
                        || a.open_quads(&qartifact, &qsrc),
                        |h| {
                            h.count(pat);
                        },
                        &mut rows,
                        QUERY_ITERS,
                        Some("cold"),
                    );
                }
            }
        }
        None => {
            // No quads in the model at all. Both regime cells say so.
            for pat in &qpats {
                for regime in ["warm", "cold"] {
                    let mut r = unsupported_row(
                        &format!("{}::{}", a.slug(), pat.name),
                        "the HDT format has no named graphs — triples only",
                    );
                    r.regime = Some(if regime == "cold" { "cold" } else { "warm" });
                    rows.push(r);
                }
            }
        }
    }

    run_mutation_phase(a.slug(), &src, &artifact, n, &mut rows);

    WorkerOut {
        slug: a.slug().to_string(),
        label: a.label().to_string(),
        rows,
        counts,
        artifact_bytes,
        peak_rss_mb: peak_rss_mb(),
    }
}

/// Add & delete, for the libraries that can. Curated to one Vortex row: the
/// add phase starts from an empty store, which no layout/index knob reaches,
/// so per-variant rows would measure the same thing seven times.
///
/// Add: `MUT_BATCH` per-quad inserts into an empty store (Vortex checks
/// presence per add — set semantics — and auto-compacts when the tail crosses
/// a chunk); `add_batch` is the one-call `add_quads` path. Delete: `MUT_BATCH`
/// tombstoning deletes against the full store, whose reopen/rebuild happens in
/// untimed per-iteration setup.
fn run_mutation_phase(slug: &str, src: &Path, artifact: &Path, n: usize, rows: &mut Vec<Row>) {
    let batch = mut_batch();
    let log = |m: &str| eprintln!("  [{slug}] {m}");
    match slug {
        "vortex_dict" => {
            let fresh = fresh_quads(batch);
            log("add (per-quad)…");
            measure(
                &format!("add::{slug}"),
                || {
                    rt().block_on(async {
                        let mut store = VortexRdfStore::empty();
                        for q in &fresh {
                            store = store.add_quad(q.clone()).await.expect("add_quad");
                        }
                        store
                    });
                },
                rows,
                HEAVY_ITERS,
                0,
                None,
            );
            log("add (batch)…");
            measure(
                &format!("add_batch::{slug}"),
                || {
                    rt().block_on(async {
                        VortexRdfStore::empty()
                            .add_quads(fresh.iter().cloned())
                            .await
                            .expect("add_quads")
                    });
                },
                rows,
                HEAVY_ITERS,
                0,
                None,
            );
            log("delete…");
            let del = dataset_prefix(n, batch);
            measure_with_setup(
                &format!("delete::{slug}"),
                || {
                    rt().block_on(VortexRdfStore::from_file(artifact))
                        .expect("reopen")
                },
                |store| {
                    rt().block_on(async {
                        let mut s = store;
                        for q in &del {
                            s = s.delete_quad(q).await.expect("delete_quad");
                        }
                        s
                    });
                },
                rows,
                HEAVY_ITERS,
            );
        }
        "oxigraph" => {
            let fresh = fresh_quads(batch);
            log("add (per-quad)…");
            measure(
                &format!("add::{slug}"),
                || {
                    let store = oxigraph::store::Store::new().expect("new store");
                    for q in &fresh {
                        store.insert(q.as_ref()).expect("insert");
                    }
                },
                rows,
                HEAVY_ITERS,
                0,
                None,
            );
            log("add (batch)…");
            measure(
                &format!("add_batch::{slug}"),
                || {
                    let store = oxigraph::store::Store::new().expect("new store");
                    let mut loader = store.bulk_loader();
                    loader.load_quads(fresh.iter().cloned()).expect("bulk load");
                    loader.commit().expect("commit");
                },
                rows,
                HEAVY_ITERS,
                0,
                None,
            );
            log("delete…");
            let del = dataset_prefix(n, batch);
            measure_with_setup(
                &format!("delete::{slug}"),
                || load_oxigraph(src, oxigraph::io::RdfFormat::NTriples),
                |store| {
                    for q in &del {
                        store.remove(q.as_ref()).expect("remove");
                    }
                },
                rows,
                HEAVY_ITERS,
            );
        }
        "oxigraph_rocksdb" => {
            let fresh = fresh_quads(batch);
            // A scratch database per iteration; deleting the previous one is
            // untimed setup, but creating the store is part of what an add
            // costs on this backend, so it stays in the timed region — the
            // memory arm's `Store::new` sits inside its timed region too.
            let tmp = artifact.parent().unwrap().join("mut_tmp.rocksdb");
            log("add (per-quad)…");
            measure_with_setup(
                &format!("add::{slug}"),
                || {
                    let _ = std::fs::remove_dir_all(&tmp);
                    tmp.clone()
                },
                |dir| {
                    let store = oxigraph::store::Store::open(&dir).expect("open rocksdb");
                    for q in &fresh {
                        store.insert(q.as_ref()).expect("insert");
                    }
                },
                rows,
                HEAVY_ITERS,
            );
            log("add (batch)…");
            measure_with_setup(
                &format!("add_batch::{slug}"),
                || {
                    let _ = std::fs::remove_dir_all(&tmp);
                    tmp.clone()
                },
                |dir| {
                    let store = oxigraph::store::Store::open(&dir).expect("open rocksdb");
                    let mut loader = store.bulk_loader();
                    loader.load_quads(fresh.iter().cloned()).expect("bulk load");
                    loader.commit().expect("commit");
                },
                rows,
                HEAVY_ITERS,
            );
            log("delete…");
            let del = dataset_prefix(n, batch);
            // Deletes need a read-write store holding the full dataset, and
            // each iteration removes rows the next one must find again — so
            // setup rebuilds the scratch database from the source, the same
            // per-iteration reload the memory arm does.
            measure_with_setup(
                &format!("delete::{slug}"),
                || build_oxigraph_rocksdb(&tmp, src, oxigraph::io::RdfFormat::NTriples),
                |store| {
                    for q in &del {
                        store.remove(q.as_ref()).expect("remove");
                    }
                },
                rows,
                HEAVY_ITERS,
            );
            let _ = std::fs::remove_dir_all(&tmp);
        }
        "sophia" => {
            use sophia::api::graph::MutableGraph;
            let fresh = fresh_quads(batch);
            let fresh_terms: Vec<[SimpleTerm<'static>; 3]> = fresh
                .iter()
                .map(|q| {
                    [
                        simple_iri(q.subject.to_string().as_str()),
                        simple_iri(q.predicate.to_string().as_str()),
                        simple_iri(q.object.to_string().as_str()),
                    ]
                })
                .collect();
            log("add (per-quad)…");
            measure(
                &format!("add::{slug}"),
                || {
                    let mut g = sophia::inmem::graph::FastGraph::new();
                    for [s, p, o] in &fresh_terms {
                        g.insert(s, p, o).expect("insert");
                    }
                },
                rows,
                HEAVY_ITERS,
                0,
                None,
            );
            rows.push(unsupported_row(
                &format!("add_batch::{slug}"),
                "no batch-insert API distinct from the per-quad loop",
            ));
            log("delete…");
            let del = dataset_prefix(n, batch);
            let del_terms: Vec<[SimpleTerm<'static>; 3]> = del
                .iter()
                .map(|q| {
                    let o = match &q.object {
                        oxrdf::Term::Literal(l) => simple_object(&format!("\"{}\"", l.value())),
                        other => simple_iri(other.to_string().as_str()),
                    };
                    [
                        simple_iri(q.subject.to_string().as_str()),
                        simple_iri(q.predicate.to_string().as_str()),
                        o,
                    ]
                })
                .collect();
            measure_with_setup(
                &format!("delete::{slug}"),
                || load_sophia(src),
                |mut g| {
                    for [s, p, o] in &del_terms {
                        g.remove(s, p, o).expect("remove");
                    }
                },
                rows,
                HEAVY_ITERS,
            );
        }
        "hdt" => {
            for id in ["add", "add_batch", "delete"] {
                rows.push(unsupported_row(
                    &format!("{id}::{slug}"),
                    "an HDT file is immutable once built",
                ));
            }
        }
        // The other Vortex variants: mutation starts from an empty store, which
        // no layout/index knob reaches — measured once, on vortex_dict.
        _ => {}
    }
}

// ─── Driver ─────────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n = dataset_size();

    // Child mode: measure one adapter and print its JSON. One process per adapter,
    // so a cheap pattern is never measured in the shadow of another library's scan.
    if let Some(i) = args.iter().position(|a| a == "--adapter") {
        let slug = args.get(i + 1).expect("--adapter needs a slug");
        let all = adapters();
        let a = all
            .iter()
            .find(|a| a.slug() == slug)
            .unwrap_or_else(|| panic!("unknown adapter {slug}"));
        let out = run_adapter(a.as_ref(), n);
        println!("{}", serde_json::to_string(&out).unwrap());
        return;
    }

    eprintln!("Rust comparative benchmark: {n} triples, one process per library");
    ensure_dataset(n);

    let exe = std::env::current_exe().expect("current exe");
    let mut rows: Vec<serde_json::Value> = Vec::new();
    let mut sizes: Vec<serde_json::Value> = Vec::new();
    let mut memory: Vec<serde_json::Value> = Vec::new();
    let mut counts_by_slug: std::collections::BTreeMap<String, serde_json::Value> =
        Default::default();

    for a in adapters() {
        eprintln!("\n== {} ==", a.label());
        let out = std::process::Command::new(&exe)
            .arg("--adapter")
            .arg(a.slug())
            .env("BENCH_SIZE", n.to_string())
            .stderr(std::process::Stdio::inherit())
            .output()
            .expect("spawn adapter child");
        if !out.status.success() {
            // Carry on: a partial tab beats no tab, and the missing rows are
            // visible as absent cells rather than as silently wrong ones.
            eprintln!("!! {} failed ({}), skipping", a.label(), out.status);
            continue;
        }
        let parsed: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("child emitted invalid JSON");
        if let Some(arr) = parsed["rows"].as_array() {
            rows.extend(arr.iter().cloned());
        }
        sizes.push(serde_json::json!({
            "slug": parsed["slug"], "label": parsed["label"],
            "bytes": parsed["artifact_bytes"],
        }));
        memory.push(serde_json::json!({
            "slug": parsed["slug"], "label": parsed["label"], "role": "query",
            "peakRssMb": parsed["peak_rss_mb"],
        }));
        counts_by_slug.insert(a.slug().to_string(), parsed["counts"].clone());
    }

    // Every adapter queried the same dataset with the same probes, so their counts
    // must agree; a disagreement means one was queried wrongly and its timings
    // measure the wrong work.
    let mut warnings: Vec<String> = Vec::new();
    let mut merged: std::collections::BTreeMap<String, u64> = Default::default();
    for (slug, counts) in &counts_by_slug {
        for (pat, v) in counts.as_object().into_iter().flatten() {
            let v = v.as_u64().unwrap_or(0);
            match merged.get(pat) {
                None => {
                    merged.insert(pat.clone(), v);
                }
                Some(&seen) if seen != v => {
                    warnings.push(format!(
                        "{pat}: {slug} matched {v}, an earlier library matched {seen}"
                    ));
                }
                _ => {}
            }
        }
    }
    for w in &warnings {
        eprintln!("!! count disagreement: {w}");
    }

    // Terms per role plus the default graph, matching `moduli` in
    // python/bench/datasets.py -- the same file, so necessarily the same count.
    let distinct_terms = moduli(n).terms();

    let out = serde_json::json!({
        "results": rows,
        "sizes": sizes,
        "memory": memory,
        "config": {
            "triplesCount": n,
            "quadsCount": quads_size(),
            "mutBatch": mut_batch(),
            "distinctTerms": distinct_terms,
            "queryIterations": QUERY_ITERS,
            "heavyIterations": HEAVY_ITERS,
            "matchedRows": merged,
            "countWarnings": warnings,
        },
    });
    let dest = Path::new(env!("CARGO_MANIFEST_DIR")).join("bench/results.json");
    std::fs::create_dir_all(dest.parent().unwrap()).expect("create bench dir");
    std::fs::write(&dest, serde_json::to_string_pretty(&out).unwrap()).expect("write results");
    eprintln!("\nWrote {} rows -> {}", rows.len(), dest.display());
}

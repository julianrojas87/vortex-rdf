//! Decomposition of the JS/WASM bindings' read path.
//!
//! The JS comparative benchmark (`js/bench/compare.bench.ts`) shows a flat
//! ~33µs per `getQuads` call regardless of how many rows match — a fixed
//! per-call cost that makes highly selective patterns (SPO/SPOG, 1 row)
//! 70–80× slower than pure-JS stores. This bench splits the Rust side of that
//! call (`js/src/lib.rs::match_columns`, code-payload branch) into cumulative
//! stages, on the same store shape the JS bench queries (in-memory,
//! `SortedInMemory` builder, `Dictionary` layout, D=20 → 8,000 quads):
//!
//! 1. `match_only`      — `match_pattern` alone: selection resolution.
//! 2. `quads_array`     — + `get_quads_array`: gathering the selected rows.
//! 3. `canonical_cols`  — + `execute::<StructArray>` + 4× `execute::<PrimitiveArray>`
//!                        + copying each u32 column out (the payload the JS
//!                        bindings ship across the boundary).
//!
//! The difference between consecutive stages attributes the fixed cost; the
//! remainder up to the observed wall-clock ~33µs is JS-boundary + promise
//! machinery, which no Rust-side change can reach.
//!
//! Not part of the CodSpeed CI upload (`codspeed.yml` runs `--bench benchmark`
//! only); this target exists for local `codspeed run` iteration.

use std::hint::black_box;
use std::sync::OnceLock;

use futures::stream;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use tokio::runtime::Runtime;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};

use vortex_rdf_core::io::VORTEX_LIGHT_SESSION;
use vortex_rdf_core::{LayoutStrategy, SortedInMemoryBuilder, VortexRdfError, VortexRdfStore};

fn main() {
    divan::main();
}

/// Mirrors the JS comparative bench's local run: D=20 → D³ = 8,000 triples.
const DIM: usize = 20;

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn rt() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

fn ex(n: usize) -> NamedNode {
    NamedNode::new(format!("http://example.org/#{n}")).unwrap()
}

/// The store under test, built once: same generation scheme as the JS bench
/// (`quad(ex#s, ex#p, ex#o)` over `s,p,o ∈ 0..D`, default graph).
static STORE: OnceLock<VortexRdfStore> = OnceLock::new();

fn store() -> &'static VortexRdfStore {
    STORE.get_or_init(|| {
        let quads: Vec<Result<Quad, VortexRdfError>> = (0..DIM)
            .flat_map(|s| {
                (0..DIM).flat_map(move |p| {
                    (0..DIM).map(move |o| {
                        Ok(Quad::new(
                            ex(s),
                            ex(p),
                            Term::NamedNode(ex(o)),
                            GraphName::DefaultGraph,
                        ))
                    })
                })
            })
            .collect();
        let array = rt()
            .block_on(VortexRdfStore::build_vortex_array_with_builder::<
                SortedInMemoryBuilder,
            >(
                stream::iter(quads), LayoutStrategy::Dictionary, vec![]
            ))
            .expect("failed to build vortex array");
        VortexRdfStore::new(array).expect("failed to build store")
    })
}

/// Probe patterns spanning the selectivity range: S → 400 rows, SPO → 1 row,
/// Full → all 8,000 rows. The JS bench observes ~33µs for all three.
#[derive(Copy, Clone)]
enum Pat {
    S,
    Spo,
    Full,
}

impl std::fmt::Debug for Pat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Pat::S => "S",
            Pat::Spo => "SPO",
            Pat::Full => "full",
        })
    }
}

type PatternTerms = (
    Option<NamedOrBlankNode>,
    Option<NamedNode>,
    Option<Term>,
    Option<GraphName>,
);

fn terms_for(pat: Pat) -> PatternTerms {
    let t0 = ex(0);
    match pat {
        Pat::S => (Some(NamedOrBlankNode::NamedNode(t0)), None, None, None),
        Pat::Spo => (
            Some(NamedOrBlankNode::NamedNode(t0.clone())),
            Some(t0.clone()),
            Some(Term::NamedNode(t0)),
            None,
        ),
        Pat::Full => (None, None, None, None),
    }
}

const PATTERNS: [Pat; 3] = [Pat::S, Pat::Spo, Pat::Full];

// ── Mutation: the per-quad add loop (the JS `addQuad` bench's core path) ────

#[divan::bench(sample_count = 3, sample_size = 1)]
fn add_loop(bencher: divan::Bencher) {
    bencher.bench(|| {
        rt().block_on(async {
            let mut st = VortexRdfStore::empty();
            for i in 0..1000usize {
                let q = Quad::new(
                    ex(9_000_000 + i),
                    ex(1),
                    Term::NamedNode(ex(2)),
                    GraphName::DefaultGraph,
                );
                st = st.add_quad(q).await.unwrap();
            }
            black_box(st.tail_len())
        })
    });
}

// ── Stage 1: selection resolution only ──────────────────────────────────────

#[divan::bench(args = PATTERNS)]
fn match_only(bencher: divan::Bencher, pat: &Pat) {
    let st = store();
    let (s, p, o, g) = terms_for(*pat);
    bencher.bench(|| {
        rt().block_on(async {
            black_box(
                st.match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                    .await
                    .unwrap(),
            )
        })
    });
}

// ── Stage 2: + gather the selected rows into one StructArray ────────────────

#[divan::bench(args = PATTERNS)]
fn quads_array(bencher: divan::Bencher, pat: &Pat) {
    let st = store();
    let (s, p, o, g) = terms_for(*pat);
    bencher.bench(|| {
        rt().block_on(async {
            let matched = st
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .unwrap();
            black_box(matched.get_quads_array().await.unwrap())
        })
    });
}

// ── Stage 4 (fast path): + code_columns instead of gather + canonicalize ────
// (The payload path the JS bindings now prefer.)

#[divan::bench(args = PATTERNS)]
fn code_columns(bencher: divan::Bencher, pat: &Pat) {
    let st = store();
    let (s, p, o, g) = terms_for(*pat);
    bencher.bench(|| {
        rt().block_on(async {
            let matched = st
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .unwrap();
            let cols = matched.code_columns().expect("code path applies");
            black_box(cols[0].len() + cols[1].len() + cols[2].len() + cols[3].len())
        })
    });
}

// ── Stage 3: + canonicalize the four u32 columns and copy them out ──────────
// (Everything js/src/lib.rs::match_columns does short of building JS objects.)

#[divan::bench(args = PATTERNS)]
fn canonical_cols(bencher: divan::Bencher, pat: &Pat) {
    let st = store();
    let (s, p, o, g) = terms_for(*pat);
    bencher.bench(|| {
        rt().block_on(async {
            let matched = st
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .unwrap();
            let arr = matched.get_quads_array().await.unwrap();
            let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
            let struct_arr = arr.execute::<StructArray>(&mut ctx).unwrap();
            let mut total = 0usize;
            for name in ["s", "p", "o", "g"] {
                let col = struct_arr.unmasked_field_by_name(name).unwrap();
                let prim = col.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
                let copied: Vec<u32> = prim.as_slice::<u32>().to_vec();
                total += black_box(copied).len();
            }
            black_box(total)
        })
    });
}

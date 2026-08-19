//! Match benchmarks *without* materialization: the cost of `match_pattern`
//! alone — index resolution + row-selection composition into a narrowed view,
//! before any quad is decoded. Same layout × source × index matrix, probe
//! patterns, and cache regimes as `benchmark.rs`'s match groups; subtracting a
//! lazy cell from its materializing twin (same variant, same regime) gives
//! materialization's exact share of a match — what an iterative query plan
//! defers by refining views and decoding only the final result.
//!
//! Deliberately a separate bench target: CodSpeed CI runs
//! `cargo codspeed run --bench benchmark`, so these groups never upload — they
//! exist for the local dashboard's lazy-match table only.

use std::hint::black_box;

// The module is shared with `benchmark.rs` and compiled per-target; items
// only the other target uses (the serialize helpers) are dead
// here by design.
#[allow(dead_code)]
mod support;
use support::*;

fn main() {
    // The dataset this run generated, as in `benchmark.rs` — both targets are
    // concatenated into one bench log, so both stamp what they measured.
    println!(
        "{}",
        dataset::shape_line(bench_size(), dataset::WANT_GRAPHS)
    );
    divan::main();
}

/// Time `match_pattern` only: the returned view (a store sharing Arc'd
/// internals with the base) is black-boxed and dropped per iteration, never
/// executed.
///
/// The COLD regime — `with_inputs` hands each sample a store built fresh, so
/// no probe or segment cache survives between them. [`run_lazy_match_warm`]
/// is the twin that reuses one primed store.
fn run_lazy_match(
    bencher: divan::Bencher,
    layout: Layout,
    index: Index,
    source: Source,
    pattern: Pattern,
) {
    // Probe construction stays OUTSIDE the timed closure: this suite times
    // pure match setup, so terms_for's String allocations per iteration would
    // be a visible fraction of the measurement.
    let (s, p, o, g) = terms_for(pattern);
    bencher
        .with_inputs(|| make_store(source, layout, index, bench_size()))
        .bench_refs(|store| {
            rt().block_on(async {
                let matched = store
                    .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                    .await
                    .expect("match_pattern failed");
                black_box(matched)
            })
        });
}

/// The WARM regime: one store, one untimed priming query (match and
/// materialize, exactly what `run_match_warm` primes with), then
/// `match_pattern` alone per iteration. Against `match_warm_*` in
/// `benchmark.rs`, the difference is materialization's share of a warm match;
/// against `lazy_match_cold_*` here, it is what the caches were worth to
/// resolution itself.
fn run_lazy_match_warm(
    bencher: divan::Bencher,
    layout: Layout,
    index: Index,
    source: Source,
    pattern: Pattern,
) {
    let (s, p, o, g) = terms_for(pattern);
    let store = make_store(source, layout, index, bench_size());
    rt().block_on(async {
        let matched = store
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
            .expect("match_pattern failed");
        black_box(matched.quads_vec().await.expect("execute match"));
    });
    bencher.bench(|| {
        rt().block_on(async {
            let matched = store
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .expect("match_pattern failed");
            black_box(matched)
        })
    });
}

macro_rules! lazy_match_bench {
    ($cold:ident, $warm:ident, $layout:expr, $index:expr, $source:expr) => {
        #[divan::bench(args = PATTERNS, sample_count = QUERY_SAMPLES)]
        fn $cold(bencher: divan::Bencher, pattern: &Pattern) {
            run_lazy_match(bencher, $layout, $index, $source, *pattern);
        }

        #[divan::bench(args = PATTERNS, sample_count = QUERY_SAMPLES)]
        fn $warm(bencher: divan::Bencher, pattern: &Pattern) {
            run_lazy_match_warm(bencher, $layout, $index, $source, *pattern);
        }
    };
}

// The full matrix, named `lazy_` + the materializing twin's group name so the
// dashboard derives one set of ids from the other. The twin carries a cache
// regime in its name and this target has only one — every sample gets a store
// built fresh — so the names say `cold` too rather than leaving the reader to
// infer which of the two a lazy row corresponds to.
// No secondary index.
lazy_match_bench!(
    lazy_match_cold_default_noindex_mem,
    lazy_match_warm_default_noindex_mem,
    Layout::Default,
    Index::None,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_default_noindex_file,
    lazy_match_warm_default_noindex_file,
    Layout::Default,
    Index::None,
    Source::File
);
lazy_match_bench!(
    lazy_match_cold_typedobj_noindex_mem,
    lazy_match_warm_typedobj_noindex_mem,
    Layout::TypedObject,
    Index::None,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_typedobj_noindex_file,
    lazy_match_warm_typedobj_noindex_file,
    Layout::TypedObject,
    Index::None,
    Source::File
);
lazy_match_bench!(
    lazy_match_cold_dict_noindex_mem,
    lazy_match_warm_dict_noindex_mem,
    Layout::Dictionary,
    Index::None,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_dict_noindex_file,
    lazy_match_warm_dict_noindex_file,
    Layout::Dictionary,
    Index::None,
    Source::File
);
// Secondary by reference.
lazy_match_bench!(
    lazy_match_cold_default_byref_mem,
    lazy_match_warm_default_byref_mem,
    Layout::Default,
    Index::ByReference,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_default_byref_file,
    lazy_match_warm_default_byref_file,
    Layout::Default,
    Index::ByReference,
    Source::File
);
lazy_match_bench!(
    lazy_match_cold_typedobj_byref_mem,
    lazy_match_warm_typedobj_byref_mem,
    Layout::TypedObject,
    Index::ByReference,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_typedobj_byref_file,
    lazy_match_warm_typedobj_byref_file,
    Layout::TypedObject,
    Index::ByReference,
    Source::File
);
lazy_match_bench!(
    lazy_match_cold_dict_byref_mem,
    lazy_match_warm_dict_byref_mem,
    Layout::Dictionary,
    Index::ByReference,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_dict_byref_file,
    lazy_match_warm_dict_byref_file,
    Layout::Dictionary,
    Index::ByReference,
    Source::File
);
// Lean from_bytes adoption (wire-encoded base, deferred components) on the
// Dictionary layout: the encoded-probe counterpart of the `_mem` rows.
lazy_match_bench!(
    lazy_match_cold_dict_noindex_bytes,
    lazy_match_warm_dict_noindex_bytes,
    Layout::Dictionary,
    Index::None,
    Source::Bytes
);
lazy_match_bench!(
    lazy_match_cold_dict_byref_bytes,
    lazy_match_warm_dict_byref_bytes,
    Layout::Dictionary,
    Index::ByReference,
    Source::Bytes
);
lazy_match_bench!(
    lazy_match_cold_dict_bycopy_bytes,
    lazy_match_warm_dict_bycopy_bytes,
    Layout::Dictionary,
    Index::ByCopy,
    Source::Bytes
);
// Secondary by copy.
lazy_match_bench!(
    lazy_match_cold_default_bycopy_mem,
    lazy_match_warm_default_bycopy_mem,
    Layout::Default,
    Index::ByCopy,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_default_bycopy_file,
    lazy_match_warm_default_bycopy_file,
    Layout::Default,
    Index::ByCopy,
    Source::File
);
lazy_match_bench!(
    lazy_match_cold_typedobj_bycopy_mem,
    lazy_match_warm_typedobj_bycopy_mem,
    Layout::TypedObject,
    Index::ByCopy,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_typedobj_bycopy_file,
    lazy_match_warm_typedobj_bycopy_file,
    Layout::TypedObject,
    Index::ByCopy,
    Source::File
);
lazy_match_bench!(
    lazy_match_cold_dict_bycopy_mem,
    lazy_match_warm_dict_bycopy_mem,
    Layout::Dictionary,
    Index::ByCopy,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_cold_dict_bycopy_file,
    lazy_match_warm_dict_bycopy_file,
    Layout::Dictionary,
    Index::ByCopy,
    Source::File
);

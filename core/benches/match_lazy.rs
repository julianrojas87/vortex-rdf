//! Match benchmarks *without* materialization: the cost of `match_pattern`
//! alone — index resolution + row-selection composition into a narrowed view,
//! before any quad is decoded. Same cells as `benchmark.rs`'s match groups
//! (layout × index × source, probe patterns, both cache regimes), plus a
//! match_lazy-only from_bytes column (Dictionary layout × every index) that has
//! no materializing twin; subtracting a lazy cell from its twin (same variant,
//! same regime) gives materialization's exact share of a match — what an
//! iterative query plan defers by refining views and decoding only the final
//! result.
//!
//! A separate bench target: CodSpeed CI runs `cargo codspeed run --bench
//! benchmark`, so these groups never upload — they exist for the local
//! dashboard's lazy-match table (twinned cells and the from_bytes column).

// The module is shared with `benchmark.rs` and compiled per-target; items
// only the other target uses are dead here by design.
#[allow(dead_code)]
#[macro_use]
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

// The full matrix, non-materializing, in both regimes. Group names are `lazy_`
// + the `benchmark.rs` group name for cells that have a materializing twin, so
// the dashboard derives one set of ids from the other; the `_bytes` cells are
// match_lazy-only.
match_matrix!(
    false;
    // No secondary index.
    (Layout::Default, Index::None, Source::InMemory) => lazy_match_cold_default_noindex_mem / lazy_match_warm_default_noindex_mem,
    (Layout::Default, Index::None, Source::File) => lazy_match_cold_default_noindex_file / lazy_match_warm_default_noindex_file,
    (Layout::TypedObject, Index::None, Source::InMemory) => lazy_match_cold_typedobj_noindex_mem / lazy_match_warm_typedobj_noindex_mem,
    (Layout::TypedObject, Index::None, Source::File) => lazy_match_cold_typedobj_noindex_file / lazy_match_warm_typedobj_noindex_file,
    (Layout::Dictionary, Index::None, Source::InMemory) => lazy_match_cold_dict_noindex_mem / lazy_match_warm_dict_noindex_mem,
    (Layout::Dictionary, Index::None, Source::File) => lazy_match_cold_dict_noindex_file / lazy_match_warm_dict_noindex_file,
    // Secondary by reference.
    (Layout::Default, Index::ByReference, Source::InMemory) => lazy_match_cold_default_byref_mem / lazy_match_warm_default_byref_mem,
    (Layout::Default, Index::ByReference, Source::File) => lazy_match_cold_default_byref_file / lazy_match_warm_default_byref_file,
    (Layout::TypedObject, Index::ByReference, Source::InMemory) => lazy_match_cold_typedobj_byref_mem / lazy_match_warm_typedobj_byref_mem,
    (Layout::TypedObject, Index::ByReference, Source::File) => lazy_match_cold_typedobj_byref_file / lazy_match_warm_typedobj_byref_file,
    (Layout::Dictionary, Index::ByReference, Source::InMemory) => lazy_match_cold_dict_byref_mem / lazy_match_warm_dict_byref_mem,
    (Layout::Dictionary, Index::ByReference, Source::File) => lazy_match_cold_dict_byref_file / lazy_match_warm_dict_byref_file,
    // Secondary by copy.
    (Layout::Default, Index::ByCopy, Source::InMemory) => lazy_match_cold_default_bycopy_mem / lazy_match_warm_default_bycopy_mem,
    (Layout::Default, Index::ByCopy, Source::File) => lazy_match_cold_default_bycopy_file / lazy_match_warm_default_bycopy_file,
    (Layout::TypedObject, Index::ByCopy, Source::InMemory) => lazy_match_cold_typedobj_bycopy_mem / lazy_match_warm_typedobj_bycopy_mem,
    (Layout::TypedObject, Index::ByCopy, Source::File) => lazy_match_cold_typedobj_bycopy_file / lazy_match_warm_typedobj_bycopy_file,
    (Layout::Dictionary, Index::ByCopy, Source::InMemory) => lazy_match_cold_dict_bycopy_mem / lazy_match_warm_dict_bycopy_mem,
    (Layout::Dictionary, Index::ByCopy, Source::File) => lazy_match_cold_dict_bycopy_file / lazy_match_warm_dict_bycopy_file,
    // Lean from_bytes adoption (wire-encoded base, deferred components) on the
    // Dictionary layout: the encoded-probe counterpart of the `_mem` rows.
    (Layout::Dictionary, Index::None, Source::Bytes) => lazy_match_cold_dict_noindex_bytes / lazy_match_warm_dict_noindex_bytes,
    (Layout::Dictionary, Index::ByReference, Source::Bytes) => lazy_match_cold_dict_byref_bytes / lazy_match_warm_dict_byref_bytes,
    (Layout::Dictionary, Index::ByCopy, Source::Bytes) => lazy_match_cold_dict_bycopy_bytes / lazy_match_warm_dict_bycopy_bytes,
);

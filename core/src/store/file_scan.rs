//! Filter evaluation over a native store file's splits: the scan-side
//! machinery behind the store's file-backed counting, deletion, and pruning
//! paths. Everything here is a pure function of a [`NativeStoreFile`] and a
//! filter expression — no store state — which is what lets the dictionary's
//! file-backed probe reuse [`evaluate_filter_split`] directly.

use std::ops::{BitAnd, Range};
use std::sync::Arc;

use futures::{StreamExt, stream};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use vortex_array::MaskFuture;
use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{Expression, and, eq, get_item, lit, root};
use vortex_layout::LayoutReader;
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::io::native_file::NativeStoreFile;
use crate::store::layouts::{Constraints, PatternCodes, ResolvedLayout};
use crate::store::selection::{RowSelection, split_bounds, split_start_mask};

/// The host's available parallelism, read once — split-loop concurrency is
/// derived from it on every counting/matching call, and the syscall is not
/// free on hot repeated-match paths (the bindings' `triples()` pattern).
static AVAILABLE_PARALLELISM: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
});

/// Evaluate a filter over one file split, threading a narrowing mask through the
/// two phases the layout reader exposes — cheap zone-map/stats pruning first,
/// then real per-conjunct filter evaluation for whatever survives. Returns the
/// split-relative surviving mask; callers either count its set bits or lift them
/// to absolute row ids. Mirrors the filter phase of vortex's own `split_exec`.
pub(crate) async fn evaluate_filter_split(
    reader: Arc<dyn LayoutReader>,
    filter_conjuncts: &[Expression],
    range: &Range<u64>,
    start_mask: Mask,
) -> Result<Mask> {
    let mut mask = start_mask;
    // Phase 1: prune using zone-map/footer stats only — no I/O beyond the
    // cached stats tables. Each conjunct narrows the mask; stop once nothing
    // survives.
    for conjunct in filter_conjuncts {
        if mask.all_false() {
            return Ok(mask);
        }
        let pruned = reader
            .pruning_evaluation(range, conjunct, mask.clone())
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
        mask = mask.bitand(&pruned);
    }
    // Phase 2: for whatever the stats couldn't rule out, read and evaluate each
    // conjunct for real, threading the narrowing mask so later conjuncts see
    // fewer rows.
    for conjunct in filter_conjuncts {
        if mask.all_false() {
            return Ok(mask);
        }
        mask = reader
            .filter_evaluation(range, conjunct, MaskFuture::ready(mask))
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
    }
    Ok(mask)
}

/// Evaluate `filter` over every natural file split the selection touches and
/// map each split's surviving mask through `map` — the shared split loop
/// behind [`count_matching_rows`] and [`matching_file_rows`], which differ
/// only in what they do with a split's mask.
///
/// The splits are clamped to the selection's bounds, evaluated concurrently
/// (bounded by available parallelism), and returned in completion order — the
/// per-split results carry their own range when order matters. The clamped
/// ranges are owned before the task futures are built: an iterator borrowing
/// the memoized splits held across the awaits trips rustc's higher-ranked
/// lifetime inference when callers spawn the resulting future.
async fn map_filter_splits<T, F>(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    map: F,
) -> Result<Vec<T>>
where
    F: Fn(Mask, &Range<u64>) -> T + Clone,
{
    // The cached layout reader tree — reused across every split task below,
    // so zone-map stats are looked up once, not once per split.
    let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
    // Split the filter into its top-level AND-ed conditions: the struct
    // layout can only prune a single-field expression at a time.
    let filter_conjuncts = conjuncts(filter);
    // Translate the view's selection into the two knobs the split loop
    // understands: the bounds it iterates and the per-split starting mask
    // (see `split_start_mask`).
    let (row_selection, bounds) = split_bounds(selection, file.row_count());

    let splits = file.splits().map_err(VortexRdfError::Vortex)?;
    let ranges: Vec<Range<u64>> = splits
        .iter()
        .filter_map(|split| {
            let start = split.start.max(bounds.start);
            let end = split.end.min(bounds.end);
            (start < end).then_some(start..end)
        })
        .collect();
    let tasks = ranges.into_iter().map(|range| {
        let reader = Arc::clone(&reader);
        let filter_conjuncts = filter_conjuncts.clone();
        // The starting mask for this split: the selected rows within
        // `range`, minus any the caller has tombstoned.
        let start_mask = split_start_mask(&row_selection, deleted, &range);
        let map = map.clone();
        async move {
            let mask = evaluate_filter_split(reader, &filter_conjuncts, &range, start_mask).await?;
            Ok::<T, VortexRdfError>(map(mask, &range))
        }
    });

    let concurrency = *AVAILABLE_PARALLELISM * 4;
    let mut results = stream::iter(tasks).buffer_unordered(concurrency);
    let mut out = Vec::new();
    while let Some(r) = results.next().await {
        out.push(r?);
    }
    Ok(out)
}

/// Count rows matching `filter` by driving the layout reader's pruning and
/// filter evaluations directly and summing mask true-counts. A projection
/// scan would additionally decode a data column for every matching row just
/// to measure its length. No column is ever projected or decoded here.
pub(crate) async fn count_matching_rows(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<usize> {
    let counts = map_filter_splits(file, filter, selection, deleted, |mask, _| {
        mask.true_count()
    })
    .await?;
    Ok(counts.into_iter().sum())
}

/// Evaluate a file view's pending filter and selection to a base-wide mask of
/// the file rows it matches — the concrete row ids a deferred `match_pattern`
/// on a file resolves to. With no pending filter the selection alone is exact,
/// so its rows are the matches without a scan.
///
/// Tombstones are deliberately not applied: this answers "which rows does the
/// pattern name", and the caller unions the result into its existing
/// tombstones.
pub(crate) async fn matching_file_rows(
    file: &NativeStoreFile,
    filter: Option<&Expression>,
    selection: &RowSelection,
) -> Result<Mask> {
    let row_count = file.row_count();
    let Some(filter) = filter else {
        return Ok(selection.to_mask(row_count as usize));
    };
    // Same per-split evaluation as the counting path, but lifting each
    // split's surviving rows back to absolute file row ids.
    let ids = map_filter_splits(file, filter, selection, None, |mask, range| {
        let ids: Vec<usize> = match mask.indices() {
            vortex_mask::AllOr::All => (range.start as usize..range.end as usize).collect(),
            vortex_mask::AllOr::None => Vec::new(),
            vortex_mask::AllOr::Some(indices) => {
                indices.iter().map(|&i| range.start as usize + i).collect()
            }
        };
        ids
    })
    .await?;
    let mut matched: Vec<usize> = ids.into_iter().flatten().collect();
    matched.sort_unstable();
    Ok(Mask::from_indices(row_count as usize, matched))
}

/// Convert an RDF pattern (subject, predicate, object, graph) into a Vortex
/// filter expression that can be applied to a file-backed array during
/// scanning. This allows the file reader to push filters down and avoid
/// reading unnecessary data.
pub(crate) fn build_file_filter(
    layout: &ResolvedLayout,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    codes: &mut PatternCodes,
) -> Option<Expression> {
    match layout.constraints_cached(subject, predicate, object, graph, codes) {
        // If the layout determines that no rows can possibly match (e.g.,
        // asking for a term that doesn't exist in a dictionary layout),
        // return a filter that matches nothing (always evaluates to false).
        Constraints::AlwaysFalse => Some(lit(false)),

        // If the layout provides equality constraints (field_name, value
        // pairs), build a filter expression by combining them with AND
        // operations. Each constraint requires a specific column to equal a
        // specific value.
        Constraints::Eq(eqs) => {
            let mut filter: Option<Expression> = None;
            for (field, value) in eqs {
                let expr = eq(get_item(field, root()), lit(value));
                filter = Some(match filter.take() {
                    Some(f) => and(f, expr),
                    None => expr,
                });
            }
            filter
        }
    }
}

/// Zone-map envelope of `filter`: the contiguous row range outside of which
/// the file's statistics prove no row can match.
///
/// One `pruning_evaluation` per filter conjunct over the full file — the
/// zoned layout evaluates its cached zone map vectorized, chunks are
/// evaluated concurrently, and file-level footer stats short-circuit the
/// whole thing (the reader is wrapped in `FileStatsLayoutReader`). The
/// conjuncts are evaluated separately because the struct layout only prunes
/// single-field expressions.
///
/// The envelope is order-agnostic (no sortedness assumption) and keeps
/// interior non-matching stretches — the scan's own per-split pruning skips
/// those from the same cached zone masks.
///
/// Returns `Some(0..0)` when nothing can match and `None` when the stats
/// exclude nothing (leaving the range unset).
pub(crate) async fn row_range_from_pruning(
    file: &NativeStoreFile,
    filter: &Expression,
) -> Result<Option<Range<u64>>> {
    let row_count = file.row_count();
    // A row count that doesn't fit in usize can't back a Mask; bail out to
    // "no envelope known" rather than fail the whole match.
    let Ok(len) = usize::try_from(row_count) else {
        return Ok(None);
    };
    if len == 0 {
        return Ok(Some(0..0));
    }
    // Statistics-only and file-immutable, so the envelope is a pure function
    // of the filter shape — memoized on the shared file handle for the
    // repeated-pattern workloads the bindings serve.
    let memo_key = filter.to_string();
    if let Some(envelope) = file.pruning_envelope(&memo_key) {
        return Ok(envelope);
    }

    // Start from "everything might match" and narrow it down using only
    // statistics (zone maps / footer stats) — no row data is read here.
    let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
    let mut mask = Mask::new_true(len);
    for conjunct in conjuncts(filter) {
        // Once nothing can match, further conjuncts can't un-prune rows.
        if mask.all_false() {
            break;
        }
        // Evaluate this conjunct's prunability over the *entire* file in one
        // call: the zoned reader vectorizes this over all its zones and the
        // file-stats wrapper checks footer-level bounds first.
        let pruned = reader
            .pruning_evaluation(&(0..row_count), &conjunct, mask.clone())
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
        mask = mask.bitand(&pruned);
    }

    // Collapse the surviving mask to its enclosing contiguous range: the
    // first and last set bit. Interior gaps of non-matching rows are kept
    // (the scan's own per-split pruning will skip those later using the same
    // cached zone masks) — only the outer dead space is trimmed.
    let envelope = match (mask.first(), mask.last()) {
        (Some(first), Some(last)) => {
            let range = first as u64..last as u64 + 1;
            // No trimming actually happened — leave the range unset rather
            // than recording a no-op range.
            if range == (0..row_count) {
                None
            } else {
                Some(range)
            }
        }
        // No bit survived: the filter provably matches nothing in this file.
        _ => Some(0..0),
    };
    file.memoize_pruning_envelope(memo_key, envelope.clone());
    Ok(envelope)
}

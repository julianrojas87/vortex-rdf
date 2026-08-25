//! Row-id acquisition for index resolutions: decoding a component's rid
//! column into the ascending, unique `Buffer<u64>` every resolution answers
//! in, and the file-backed readers (point reads through cached chunk probes,
//! rid-only pushed-down scans) that produce it from an index child.

#[cfg(feature = "file-io")]
use std::ops::Range;

use vortex_array::arrays::PrimitiveArray;
#[cfg(feature = "file-io")]
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::DType;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and, eq, get_item, lit, root, select};
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::Buffer;

#[cfg(feature = "file-io")]
use super::{FileServePlan, IndexResolution, IndexedComponent, ResolvedRowIds};
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;

/// The row ids of a located index-child run, read point-by-point from the
/// child's rid column through its cached chunk probes and re-sorted into
/// base row order — the file counterpart of slicing an in-memory rid run.
/// `Ok(None)` when the rid column's chunks decline (the caller keeps its
/// scan); rids are unique by construction, so sorting alone suffices.
///
/// `rid_column` comes from the calling index — the hub names no index's
/// columns.
#[cfg(feature = "file-io")]
pub(crate) async fn rid_point_reads(
    file: &crate::store::native_file::NativeStoreFile,
    component: &str,
    rid_column: &str,
    range: Range<u64>,
) -> Result<Option<Buffer<u64>>> {
    let Some(chunks) = file.component_column_chunks(component, rid_column) else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    let mut ids = Vec::with_capacity((range.end - range.start) as usize);
    for row in range {
        match chunks
            .value_at(row, &source, session)
            .await
            .map_err(VortexRdfError::Vortex)?
        {
            Some(rid) => ids.push(rid),
            None => return Ok(None),
        }
    }
    ids.sort_unstable();
    Ok(Some(Buffer::from(ids)))
}

/// Decode a row-id column into the ascending, unique `Buffer<u64>` every index
/// resolution answers in.
///
/// The whole column is cast and decoded at once rather than pulled one scalar
/// at a time.
/// Sorting is required, not incidental: the ids come out in the index's own
/// order, and both `Selection::IncludeByIndex` and the selection algebra need
/// them ascending. They are unique by construction (each index row references
/// one quad row), so sorting alone suffices.
pub(crate) fn sorted_row_ids(row_id_column: ArrayRef) -> Result<Buffer<u64>> {
    use vortex_array::builtins::ArrayBuiltins;
    use vortex_array::dtype::{Nullability, PType};

    if row_id_column.is_empty() {
        return Ok(Buffer::empty());
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let ids = row_id_column
        .cast(DType::Primitive(PType::U64, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .into_buffer::<u64>();

    // The freshly-executed buffer is normally uniquely owned, so the sort
    // runs in place with no copy; a shared buffer (someone else still holds
    // the execution's output) falls back to one copy.
    match ids.try_into_mut() {
        Ok(mut ids) => {
            ids.as_mut_slice().sort_unstable();
            Ok(ids.freeze())
        }
        Err(ids) => {
            let mut sorted = ids.as_slice().to_vec();
            sorted.sort_unstable();
            Ok(Buffer::from(sorted))
        }
    }
}

/// Scan `row_id_column` for the rows where every `(value_column, probe)`
/// equality holds, returning the primary row ids as an ascending, unique
/// buffer (the shape vortex's `Selection::IncludeByIndex` requires) — the
/// file-backed probe shared by the secondary indexes.
///
/// Each equality is a plain `eq`, the same encoding the serve-plan filter
/// uses: a binary `Eq` falsifies against the same zone min/max envelope as a
/// `>= probe AND <= probe` range pair (see vortex's
/// `stats/rewrite/builtins.rs`) while evaluating a single conjunct. Output
/// order is irrelevant (the ids are sorted afterwards), so the scan may run
/// unordered.
#[cfg(feature = "file-io")]
pub(crate) async fn scan_index_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    value_constraints: &[(&'static str, Scalar)],
    row_id_column: &'static str,
    memo: &crate::store::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<Buffer<u64>> {
    let mut filter: Option<Expression> = None;
    for (column, probe) in value_constraints {
        let expr = eq(get_item(*column, root()), lit(probe.clone()));
        filter = Some(match filter.take() {
            Some(f) => and(f, expr),
            None => expr,
        });
    }
    // Every index probes at least one value column; an empty constraint set
    // would mean "all rows", which no resolver asks for.
    let Some(filter) = filter else {
        return Ok(Buffer::empty());
    };
    let filter = memo
        .bind(scope, &filter, reader.dtype())
        .map_err(VortexRdfError::Vortex)?;

    read_scanned_row_ids(
        rid_scan(reader, row_id_column, memo, scope)?.with_filter(filter),
        row_id_column,
    )
    .await
}

/// The row ids of a *located* index-child run — the rows a resolver bounded
/// by binary-searching the child's cached chunk probes — read by a rid-only
/// scan restricted to that range. The wide-run counterpart of
/// [`rid_point_reads`], for runs too large to read point by point.
///
/// The scan carries no filter: the location bounded exactly the rows the
/// constraints select, so re-testing the value columns would only re-read and
/// re-compare them. A resolver whose location covers only *some* of its
/// constraints must keep [`scan_index_row_ids`], whose filter tests them all.
#[cfg(feature = "file-io")]
pub(crate) async fn scan_located_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    row_id_column: &'static str,
    range: Range<u64>,
    memo: &crate::store::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<Buffer<u64>> {
    read_scanned_row_ids(
        rid_scan(reader, row_id_column, memo, scope)?.with_row_range(range),
        row_id_column,
    )
    .await
}

/// A rid-only scan of an index child: just the row-id column, unordered
/// (callers sort the ids anyway). Restrictions — a filter, a row range — are
/// the caller's to add.
#[cfg(feature = "file-io")]
fn rid_scan(
    reader: vortex_layout::LayoutReaderRef,
    row_id_column: &'static str,
    memo: &crate::store::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>> {
    let projection = memo
        .bind(scope, &select([row_id_column], root()), reader.dtype())
        .map_err(VortexRdfError::Vortex)?;
    Ok(
        vortex_layout::scan::scan_builder::ScanBuilder::new(VORTEX_SESSION.clone(), reader)
            .with_projection(projection)
            .with_ordered(false),
    )
}

/// Run a rid-only scan and decode its row-id column into the ascending,
/// unique buffer every index resolution answers in.
#[cfg(feature = "file-io")]
async fn read_scanned_row_ids(
    scan: vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>,
    row_id_column: &'static str,
) -> Result<Buffer<u64>> {
    let arr = crate::store::scan::file_scan::read_all_rows(scan).await?;

    if arr.is_empty() {
        return Ok(Buffer::empty());
    }

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    sorted_row_ids(
        struct_arr
            .unmasked_field_by_name(row_id_column)
            .cloned()
            .map_err(VortexRdfError::Vortex)?,
    )
}

/// An eager file resolution off the rid-only pushed-down scan — the shared
/// tail of the file resolvers whenever no serving plan defers the ids (a
/// back-reference child, or a copy resolution that couldn't build its plan):
/// `Empty` when the scan proves the probe matches nothing.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_eager_from_scan(
    reader: vortex_layout::LayoutReaderRef,
    constraints: &[(&'static str, Scalar)],
    rid_column: &'static str,
    resolves: IndexedComponent,
    memo: &crate::store::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<IndexResolution<FileServePlan>> {
    let row_ids = scan_index_row_ids(reader, constraints, rid_column, memo, scope).await?;
    if row_ids.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Eager(row_ids),
        resolves,
        serve: None,
    })
}

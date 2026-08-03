//! The index-agnostic *serving* path: reading a resolved view's quads out of
//! the answering index's own columns instead of gathering the primary columns
//! by scattered row id.
//!
//! An index builds a [`ServePlan`] during resolution; the store executes it
//! without knowing which index produced it. Only the acquisition of the
//! matched rows differs by backend ([`ServeSource`]) — both then decode
//! through the same tail. Serving is never load-bearing for correctness: a
//! plan reproduces exactly the rows its resolution's row ids name, so any
//! caller that cannot honor it falls back to those ids.

use std::ops::Range;

use oxrdf::Quad;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::FieldNames;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and, eq, get_item, lit, root};
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_SESSION;
use crate::store::layouts::ResolvedLayout;

/// An alternative physical read path an index offers for serving a resolved
/// view's quads: read them straight from the index's own columns — where the
/// index already clusters the matched rows into a contiguous run — instead of
/// gathering the primary columns by scattered row id.
///
/// This is the generic form of what a permutation index (whole quads in a
/// query-friendly order, e.g. [`IndexType::SecondaryByCopy`]) can provide and a
/// back-reference index (only `(value, row-id)` pairs, e.g.
/// [`IndexType::SecondaryByReference`]) cannot. An index builds a plan during
/// resolution; the store executes it without knowing which index produced it,
/// so serving stays a uniform capability rather than one index's special case.
///
/// Only the *acquisition* of the matched columns differs by backend (see
/// [`ServeSource`]): a file scan filtered to the rows, or a plain slice of an
/// in-memory base. Both then decode through the same shared tail.
///
/// Correctness never depends on the plan: it reproduces exactly the rows the
/// resolution's `row_ids` name, so any operation that can't honor it (chained
/// matches, counting, materializing) simply ignores it and reads through the
/// row ids. The store keeps a plan only while the resolution is a view's sole
/// restriction — see `QuadsSource::File` / `QuadsSource::InMemory`.
///
/// [`IndexType::SecondaryByCopy`]: super::IndexType::SecondaryByCopy
/// [`IndexType::SecondaryByReference`]: super::IndexType::SecondaryByReference
#[derive(Clone)]
pub(crate) struct ServePlan {
    /// The source column for each primary `(s, p, o, g)` component, in that
    /// order — the index's own columns holding the whole quad.
    primary_columns: [&'static str; 4],
    /// The column giving each served row's primary row id, used to drop rows
    /// tombstoned since construction.
    rid_column: &'static str,
    /// The layout the projected source columns decode through (an index that
    /// stores whole terms decodes them as strings, or dictionary codes under
    /// the Dictionary layout).
    decode_layout: ResolvedLayout,
    /// How to reach the matched rows within the index's columns — the one
    /// backend-specific part of a serve.
    source: ServeSource,
}

/// Where a [`ServePlan`]'s matched rows sit within the index's columns, and how
/// to reach them.
#[derive(Clone)]
enum ServeSource {
    /// In-memory component: the matched rows are the contiguous `[start, end)`
    /// run of the index component's own array — the run a binary search over
    /// its sorted lead column bounded. The plan carries the component array
    /// (an `Arc` bump) and slices it directly, with no row-id gather.
    InMemory {
        /// The index component's rows, in child schema.
        array: ArrayRef,
        range: Range<usize>,
    },
    /// File-backed: the matched rows are those where every `(column, value)`
    /// term equality holds, read by a pushed-down scan of the index child
    /// (whose sort order clusters them into a contiguous, zone-prunable run).
    #[cfg(feature = "file-io")]
    File {
        /// The index component child's cached layout reader.
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
    },
}

impl ServePlan {
    /// A plan serving the contiguous `range` of an in-memory index
    /// component's rows.
    pub(crate) fn in_memory(
        primary_columns: [&'static str; 4],
        rid_column: &'static str,
        decode_layout: ResolvedLayout,
        array: ArrayRef,
        range: Range<usize>,
    ) -> Self {
        Self {
            primary_columns,
            rid_column,
            decode_layout,
            source: ServeSource::InMemory { array, range },
        }
    }

    /// A plan serving a file's index columns by a pushed-down scan filtered to
    /// the rows where every `constraints` equality holds.
    #[cfg(feature = "file-io")]
    pub(crate) fn file(
        primary_columns: [&'static str; 4],
        rid_column: &'static str,
        decode_layout: ResolvedLayout,
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
    ) -> Self {
        Self {
            primary_columns,
            rid_column,
            decode_layout,
            source: ServeSource::File {
                reader,
                constraints,
            },
        }
    }

    /// Decode the matched quads straight from the index component's rows:
    /// slice the component to this plan's row run, then decode those columns
    /// as the primary `(s, p, o, g)` — replacing the row-id gather over the
    /// primaries.
    pub(crate) fn decode_in_memory(&self, deleted: Option<&Mask>) -> Vec<Result<Quad>> {
        let (array, range) = match &self.source {
            ServeSource::InMemory { array, range } => (array, range.clone()),
            #[cfg(feature = "file-io")]
            ServeSource::File { .. } => {
                unreachable!("an in-memory view only ever carries an in-memory serve plan")
            }
        };
        match array.slice(range) {
            Ok(rows) => self.decode_columns(&rows, deleted),
            Err(e) => vec![Err(VortexRdfError::Vortex(e))],
        }
    }

    /// Decode the `(s, p, o, g)` quads out of a chunk of this plan's projected
    /// index columns, dropping rows tombstoned in `deleted` via the row-id
    /// column — the shared tail of both backends' serving.
    pub(crate) fn decode_columns(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<Quad>> {
        match self.chunk_rows(chunk, deleted) {
            Ok(rows) => self.decode_layout.decode_chunk(&rows),
            Err(e) => vec![Err(e)],
        }
    }

    /// [`decode_columns`](Self::decode_columns) through the layout's async
    /// decode — for serving a store whose term dictionary is file-backed,
    /// where each chunk's codes are resolved with a dictionary scan.
    #[cfg(feature = "file-io")]
    pub(crate) async fn decode_columns_async(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<Quad>> {
        match self.chunk_rows(chunk, deleted) {
            Ok(rows) => self.decode_layout.decode_chunk_async(&rows).await,
            Err(e) => vec![Err(e)],
        }
    }

    /// A chunk's live rows as a primary-named `(s, p, o, g)` struct: relabel the
    /// source columns, then drop any whose primary row id is tombstoned.
    fn chunk_rows(&self, chunk: &ArrayRef, deleted: Option<&Mask>) -> Result<ArrayRef> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = chunk
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let col = |name: &'static str| {
            struct_arr
                .unmasked_field_by_name(name)
                .cloned()
                .map_err(VortexRdfError::Vortex)
        };
        let [s, p, o, g] = self.primary_columns;
        let len = struct_arr.len();
        let rows = StructArray::try_new(
            FieldNames::from(crate::store::schema::PRIMARY_COLUMNS),
            vec![col(s)?, col(p)?, col(o)?, col(g)?],
            len,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        let Some(deleted) = deleted else {
            return Ok(rows);
        };
        // Tombstones are defined over primary row ids; the rid column says which
        // primary row each served row mirrors.
        let rid_col = col(self.rid_column)?
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let live = Mask::from_indices(
            len,
            rid_col
                .as_slice::<u32>()
                .iter()
                .enumerate()
                .filter(|&(_, &rid)| !deleted.value(rid as usize))
                .map(|(position, _)| position),
        );
        if live.all_true() {
            return Ok(rows);
        }
        rows.filter(live).map_err(VortexRdfError::Vortex)
    }
}

/// File-backed serving: turning the plan's constraints into a scan.
#[cfg(feature = "file-io")]
impl ServePlan {
    /// The columns to project from the file to serve these rows: the four
    /// component sources plus the row-id column (for tombstones).
    pub(crate) fn projection(&self) -> [&'static str; 5] {
        let [s, p, o, g] = self.primary_columns;
        [s, p, o, g, self.rid_column]
    }

    /// A scan over the serving index child — where [`Self::projection`] and
    /// [`Self::filter`] apply.
    pub(crate) fn file_scan(&self) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
        let reader = match &self.source {
            ServeSource::File { reader, .. } => reader.clone(),
            ServeSource::InMemory { .. } => {
                unreachable!("a file view only ever carries a file serve plan")
            }
        };
        vortex_layout::scan::scan_builder::ScanBuilder::new(VORTEX_SESSION.clone(), reader)
    }

    /// The filter selecting exactly the served rows within the index's columns
    /// — the conjunction of this plan's term equalities.
    pub(crate) fn filter(&self) -> Expression {
        let constraints = match &self.source {
            ServeSource::File { constraints, .. } => constraints,
            ServeSource::InMemory { .. } => {
                unreachable!("a file view only ever carries a file serve plan")
            }
        };
        let mut filter: Option<Expression> = None;
        for (column, value) in constraints {
            let expr = eq(get_item(*column, root()), lit(value.clone()));
            filter = Some(match filter.take() {
                Some(f) => and(f, expr),
                None => expr,
            });
        }
        // A serve plan always carries at least one constraint (the resolved
        // lead component), so the conjunction is never empty.
        filter.expect("a serve plan constrains at least one column")
    }
}

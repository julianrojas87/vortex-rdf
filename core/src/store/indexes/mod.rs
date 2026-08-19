//! Secondary-index vocabulary and the store's dispatch into it.
//!
//! This hub owns what is common to every index: the [`IndexType`] enum and
//! its exhaustive dispatch into the per-index modules (persisted-child roles,
//! resolution), the resolution currency (`IndexResolution`,
//! `IndexedComponent`, the eager/lazy `ResolvedRowIds` split and its
//! `LazyRowIds` recipe) both backends answer in, and the planners that try a
//! store's whole index set in preference order.
//!
//! What belongs in a leaf instead: an index's column-name scheme, its sort
//! orders, how it builds its children, and how it probes them
//! (`secondary_by_copy`, `secondary_by_reference`) — the hub never hardcodes
//! a column name. Two further clusters live beside it: `serve` (reading
//! matched quads out of an index's own columns) and `components` (the
//! persisted-child model and the slug registry), both re-exported here so
//! callers see one `indexes::` surface.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::DType;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and, eq, get_item, lit, root, select};
use vortex_array::scalar::Scalar;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::layouts::{PatternCodes, QuadPattern, ResolvedLayout};

pub(crate) mod components;
pub(crate) mod secondary_by_copy;
pub(crate) mod secondary_by_reference;
pub(crate) mod serve;

#[cfg(feature = "file-io")]
pub(crate) use components::adopt_scanned_component;
pub(crate) use components::{
    ComponentRole, IndexComponent, KnownComponent, adopt_component_reader, indexes_from_components,
    known_component,
};
#[cfg(feature = "file-io")]
pub(crate) use serve::FileServePlan;
pub(crate) use serve::InMemoryServePlan;

/// A secondary index, built as its own sorted children beside the primary
/// quad rows.
///
/// Variant declaration order is the resolution preference order: pattern
/// matching tries each index the store's component roster carries, in this
/// order, and takes the first that doesn't decline (see
/// `resolve_indexes_in_memory`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum IndexType {
    /// Two complete extra copies of the quad columns, each in its own sort
    /// order and each paired with the primary row IDs it permutes — the
    /// classic triple-store permutation indexes, giving predicate- and
    /// object-bound patterns the same sorted-column access path the primary
    /// (s, p, o, g) order gives subjects.
    ///
    /// Adds two children beside the quad rows, each a `{s, p, o, g, rid}`
    /// table (`VarBin<Utf8>` term strings, or u32 codes under the Dictionary
    /// layout; `rid` always `u32`):
    /// - `index:posg`: the quads sorted by (p, o, s, g)
    /// - `index:ospg`: the quads sorted by (o, s, p, g)
    ///
    /// Predicate-bound patterns binary-search `index:posg`'s `p` column; a
    /// bound predicate **and** object prefix-search (p, o) in one probe,
    /// resolving both components; object-bound patterns binary-search
    /// `index:ospg`'s `o` column. The copies additionally let reads take the
    /// matching rows from a *contiguous* run of the copy columns — sliced or
    /// point-read in memory, scanned from the index child on a file — instead
    /// of scattering row-id reads across the primary columns. As with
    /// [`SecondaryByReference`](Self::SecondaryByReference), routing engages
    /// only on children whose writer recorded them globally sorted, which
    /// every build here does.
    SecondaryByCopy,

    /// Builds sorted secondary indexes for both predicates **and** objects.
    ///
    /// Adds two children beside the quad rows, each a `{val, rid}` table:
    /// - `index:ref-o`: object values sorted (`VarBin<Utf8>`; u32 codes under
    ///   the Dictionary layout), paired with the primary row id (`u32`) each
    ///   came from
    /// - `index:ref-p`: the same for predicate values
    ///
    /// Enables binary-search routing in `match_pattern` for predicate-only and
    /// object-only patterns, avoiding full scans. Routing engages only on
    /// children whose writer recorded them globally sorted, which every build
    /// here does.
    SecondaryByReference,
}

/// The canonical index name: kebab-case (`"secondary-by-copy"`,
/// `"secondary-by-reference"`), the same spelling the `clap` derive exposes
/// on the CLI — so every frontend reports one vocabulary and a value printed
/// by one can be parsed by another.
impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IndexType::SecondaryByCopy => "secondary-by-copy",
            IndexType::SecondaryByReference => "secondary-by-reference",
        })
    }
}

/// Accepts exactly the canonical kebab-case names
/// [`Display`](std::fmt::Display) emits — `"secondary-by-copy"`,
/// `"secondary-by-reference"` — the one vocabulary every frontend shares.
impl std::str::FromStr for IndexType {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "secondary-by-copy" => Ok(IndexType::SecondaryByCopy),
            "secondary-by-reference" => Ok(IndexType::SecondaryByReference),
            _ => Err(VortexRdfError::Deserialization(format!(
                "unknown index type {s:?}; expected \"secondary-by-copy\" or \
                 \"secondary-by-reference\""
            ))),
        }
    }
}

/// Every [`IndexType`], in declaration = preference order — what the slug
/// registry scans and what [`IndexType::preference_rank`] indexes into.
/// Adding a variant fails to compile at that exhaustive match until the
/// variant is listed here too.
pub(crate) const ALL_INDEX_TYPES: [IndexType; 2] =
    [IndexType::SecondaryByCopy, IndexType::SecondaryByReference];

impl IndexType {
    /// This variant's position in [`ALL_INDEX_TYPES`] — the resolution
    /// preference order, as a sort key.
    pub(crate) const fn preference_rank(self) -> usize {
        match self {
            IndexType::SecondaryByCopy => 0,
            IndexType::SecondaryByReference => 1,
        }
    }

    /// This index's persisted-child roles — the const table every generic
    /// loop is parameterized by: the slug registry ([`known_component`]) and
    /// the roster-to-index-set fold ([`indexes_from_components`]). The
    /// exhaustive match is the compile-fail anchor for those loops: a new
    /// variant answers here once and flows into all of them.
    pub(crate) const fn component_roles(self) -> &'static [ComponentRole] {
        match self {
            IndexType::SecondaryByCopy => &secondary_by_copy::ROLES,
            IndexType::SecondaryByReference => &secondary_by_reference::ROLES,
        }
    }

    /// Resolve this index against an in-memory base array, producing the exact
    /// base row ids for whichever pattern component it covers.
    ///
    /// Each index owns its own execution: it decides which pattern shapes it
    /// accelerates (e.g. `SecondaryByReference` declines when a subject is
    /// bound), chooses and probes its columns, and hands back the row ids to
    /// select — or declines, leaving the store to fall back to a scan. Like
    /// [`component_roles`](Self::component_roles), the exhaustive match makes
    /// the compiler demand a query-side answer from every new index variant.
    pub(crate) fn resolve_in_memory(
        self,
        components: &[IndexComponent],
        layout: &ResolvedLayout,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
    ) -> Result<IndexResolution<InMemoryServePlan>> {
        match self {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::resolve_in_memory(components, layout, pattern, codes)
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::resolve_in_memory(components, pattern, codes)
            }
        }
    }

    /// Resolve this index against a file-backed store, producing the exact
    /// primary row ids for whichever pattern component it covers — the
    /// file-backed counterpart of [`Self::resolve_in_memory`], differing only
    /// in how the index reaches its columns (a pushed-down scan instead of an
    /// in-memory binary search).
    #[cfg(feature = "file-io")]
    pub(crate) async fn resolve_file(
        self,
        file: &crate::store::native_file::NativeStoreFile,
        layout: &ResolvedLayout,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
    ) -> Result<IndexResolution<FileServePlan>> {
        match self {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::resolve_file(file, layout, pattern, codes).await
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::resolve_file(file, pattern, codes).await
            }
        }
    }
}

/// Which pattern component(s) an index lookup resolves. The resolved
/// components can be omitted from any residual filtering over the fetched
/// rows — the index's row ids already are exactly their matches.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum IndexedComponent {
    Predicate,
    Object,
    /// Both predicate and object at once — a prefix search of the
    /// (p, o, …)-sorted copy in [`IndexType::SecondaryByCopy`].
    PredicateObject,
}

impl IndexedComponent {
    /// The pattern with this (index-resolved) component cleared: what still
    /// needs checking against the rows the index returned.
    pub(crate) fn clear<'a>(self, pattern: QuadPattern<'a>) -> QuadPattern<'a> {
        match self {
            IndexedComponent::Predicate => QuadPattern {
                predicate: None,
                ..pattern
            },
            IndexedComponent::Object => QuadPattern {
                object: None,
                ..pattern
            },
            IndexedComponent::PredicateObject => QuadPattern {
                predicate: None,
                object: None,
                ..pattern
            },
        }
    }
}

/// The outcome of asking an index to resolve a quad pattern against a backend.
///
/// Both backends answer in the same currency — ascending, unique *base* row ids
/// — so the store folds either one into a [`RowSelection`] the same way. `Plan`
/// is the backend's serve-plan type ([`InMemoryServePlan`] for in-memory
/// resolutions, `FileServePlan` for file ones), so a resolution can only ever
/// hand the store a plan its own backend can execute.
///
/// [`RowSelection`]: crate::store::selection::RowSelection
// `Resolved` dwarfs the dataless `Declined`, but the enum is a transient
// per-match return value that is destructured immediately and never stored
// in bulk.
#[allow(clippy::large_enum_variant)]
pub(crate) enum IndexResolution<Plan> {
    /// The index does not accelerate this pattern: either its shape isn't one
    /// this index covers, or (in memory) its value column isn't in a usable
    /// sorted form. The caller falls back to its non-indexed path.
    Declined,
    /// The index applies and proved the pattern matches no row — the probed
    /// term is absent from the indexed column. The caller short-circuits to an
    /// empty result.
    Empty,
    /// The index resolved `resolves`, yielding exactly `row_ids`: an
    /// ascending, unique set of base row ids (eager, or lazily computed — see
    /// [`ResolvedRowIds`]). The caller narrows its selection to those ids and
    /// drops `resolves` from any residual filtering, since the ids already
    /// satisfy it.
    ///
    /// `serve` is the optional, index-agnostic *serving plan*: when the index
    /// also holds the matched quads clustered in its own columns, it hands back
    /// the backend's plan so the store can read them straight from there instead
    /// of gathering the primary columns by scattered row id — a contiguous file
    /// scan or, for an in-memory base, a plain array slice. An index that stores
    /// only back-references (no whole quads) leaves it `None`. It is a pure
    /// optimization — `row_ids` already resolve the pattern on their own.
    Resolved {
        row_ids: ResolvedRowIds,
        resolves: IndexedComponent,
        serve: Option<Plan>,
    },
}

/// How a resolution answers its row ids.
///
/// `Eager` is a resolution that had to compute its ids to answer at all (a
/// back-reference probe, or a copy resolution without a serving plan) and is
/// non-empty by construction — an empty scan short-circuits to
/// [`IndexResolution::Empty`] instead. `Lazy` rides only alongside a serve
/// plan: the plan answers reads straight from the index's own
/// columns, so the ids — a second pass over the same data — are deferred until
/// a consumer actually needs the selection (a count, a chained match, a
/// delete, a base-order gather). A lazy resolution may therefore materialize
/// to an *empty* id set; consumers reach it through the view's pending
/// selection, which handles that like any other narrow selection.
pub(crate) enum ResolvedRowIds {
    Eager(Buffer<u64>),
    Lazy(LazyRowIds),
}

/// The exact base row ids of a serve-attached index resolution, computed on
/// first need and shared across every clone of the view that carries them.
///
/// The serving plan makes the ids redundant for the dominant
/// match-then-iterate flow — for a file-backed store they cost a whole extra
/// pushed-down scan of the index child — so the resolution hands back the
/// *recipe* instead and whichever consumer first needs the selection runs it.
/// The result lands in a shared cell: later consumers (and view clones made
/// before materialization) read it back for free. Two consumers racing on
/// first need may both run the recipe, but the source is immutable so they
/// compute identical ids; whichever stores first wins and both return the
/// stored buffer — no lock is held across the computation.
#[derive(Clone)]
pub(crate) struct LazyRowIds {
    cell: Arc<OnceLock<Buffer<u64>>>,
    source: LazyRowIdSource,
}

/// Where a [`LazyRowIds`]' ids come from — mirroring the serve plans'
/// per-backend split ([`InMemoryServePlan`] / `FileServePlan`), holding
/// exactly what the eager path would have consumed at resolution time.
#[derive(Clone)]
enum LazyRowIdSource {
    /// In-memory: the rid-column slice of the component's matched run, decoded
    /// and sorted on demand ([`sorted_row_ids`]).
    Component(ArrayRef),
    /// File-backed: the rid-only pushed-down scan of the index child
    /// ([`scan_index_row_ids`]) the eager path would have run at match time.
    #[cfg(feature = "file-io")]
    IndexChild {
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
        rid_column: &'static str,
        /// The owning file handle's bind memo and this child's scope tag —
        /// so the deferred scan binds with the same identity every plan and
        /// eager scan of this component uses (see `BoundExprMemo`).
        memo: Arc<crate::store::native_file::BoundExprMemo>,
        scope: &'static str,
    },
}

impl LazyRowIds {
    /// Test-only hook: whether the ids have actually been computed — so a
    /// test can pin that a read was answered without them.
    #[cfg(test)]
    pub(crate) fn debug_materialized(&self) -> bool {
        self.cell.get().is_some()
    }

    /// Lazy ids over an in-memory component's matched rid run.
    pub(crate) fn from_component_run(rids: ArrayRef) -> Self {
        Self {
            cell: Arc::new(OnceLock::new()),
            source: LazyRowIdSource::Component(rids),
        }
    }

    /// Lazy ids scanned from a file's index child on first need.
    #[cfg(feature = "file-io")]
    pub(crate) fn from_index_child_scan(
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
        rid_column: &'static str,
        memo: Arc<crate::store::native_file::BoundExprMemo>,
        scope: &'static str,
    ) -> Self {
        Self {
            cell: Arc::new(OnceLock::new()),
            source: LazyRowIdSource::IndexChild {
                reader,
                constraints,
                rid_column,
                memo,
                scope,
            },
        }
    }

    /// How many rows the ids cover, when knowable without computing them: an
    /// in-memory run knows its width up front (so a count on a served match
    /// never decodes), a file child only after materialization.
    pub(crate) fn len_if_known(&self) -> Option<usize> {
        match &self.source {
            LazyRowIdSource::Component(rids) => Some(rids.len()),
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild { .. } => self.cell.get().map(Buffer::len),
        }
    }

    /// The ids, computing (and caching) them on first call.
    #[cfg(feature = "file-io")]
    pub(crate) async fn materialized(&self) -> Result<Buffer<u64>> {
        if let Some(ids) = self.cell.get() {
            return Ok(ids.clone());
        }
        let ids = match &self.source {
            LazyRowIdSource::Component(rids) => sorted_row_ids(rids.clone())?,
            LazyRowIdSource::IndexChild {
                reader,
                constraints,
                rid_column,
                memo,
                scope,
            } => scan_index_row_ids(reader.clone(), constraints, rid_column, memo, scope).await?,
        };
        Ok(self.cell.get_or_init(|| ids).clone())
    }

    /// The synchronous counterpart of [`materialized`](Self::materialized),
    /// for in-memory sources — a file child's ids take I/O, and every
    /// consumer of a file view's selection is already async.
    pub(crate) fn materialized_sync(&self) -> Result<Buffer<u64>> {
        if let Some(ids) = self.cell.get() {
            return Ok(ids.clone());
        }
        let ids = match &self.source {
            LazyRowIdSource::Component(rids) => sorted_row_ids(rids.clone())?,
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild { .. } => {
                unreachable!("an in-memory view only ever carries component-sourced pending ids")
            }
        };
        Ok(self.cell.get_or_init(|| ids).clone())
    }
}

/// Binary-search a component's sorted `column` for the `[lo, hi)` run of rows
/// equal to `native`, searched `within` a row range whose slice of the column
/// is itself sorted (the whole component, or a lead run for a prefix probe) —
/// the probe step shared by the in-memory resolvers.
///
/// `None` when the column is missing or the probe can't cast to its dtype
/// (the resolver declines and the store falls back to a mask scan); an empty
/// range when the probed term is absent from the data.
pub(crate) fn sorted_probe_run(
    rows: &StructArray,
    column: &'static str,
    native: &Scalar,
    within: Range<usize>,
) -> Result<Option<Range<usize>>> {
    use crate::store::array::search_sorted_bounds;

    let Ok(col) = rows.unmasked_field_by_name(column) else {
        return Ok(None);
    };
    let Ok(scalar) = native.cast(col.dtype()) else {
        return Ok(None);
    };
    let run = col.slice(within.clone()).map_err(VortexRdfError::Vortex)?;
    let (lo, hi) = search_sorted_bounds(&run, &scalar)?;
    Ok(Some(within.start + lo..within.start + hi))
}

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

/// [`sorted_probe_run`] through a component's cached probe when its column
/// resolves one (skipping the per-call slice + encoding-tree walk): a full
/// search on the whole column, or a windowed search inside a lead run —
/// window-only, exactly like the slice-then-search path. String-valued
/// components (whose probe scalars are not integers) and probe-declined
/// encodings fall back to the per-call search.
pub(crate) fn component_probe_run(
    component: &IndexComponent,
    column: &'static str,
    native: &Scalar,
    within: Option<Range<usize>>,
) -> Result<Option<Range<usize>>> {
    if let Some(owned) = component.probe(column)
        && let Ok(needle) = u64::try_from(native)
    {
        let (lo, hi) = match within {
            None => owned.bounds(needle),
            Some(range) => owned.bounds_in(range, needle),
        };
        return Ok(Some(lo..hi));
    }
    let rows = component.rows()?;
    let within = within.unwrap_or(0..rows.len());
    sorted_probe_run(rows, column, native, within)
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
    let arr = scan
        .into_array_stream()
        .map_err(VortexRdfError::Vortex)?
        .read_all()
        .await
        .map_err(VortexRdfError::Vortex)?;

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

/// Resolve the pattern against the configured indexes over an in-memory array,
/// returning the first index whose outcome isn't `Declined` (indexes are tried
/// in declaration = preference order). `Declined` when none apply, so the store
/// can fall back to a mask scan.
///
/// The plural `indexes` name marks this as the planner over the store's whole
/// index set; the singular [`IndexType::resolve_in_memory`] it calls resolves
/// one index.
pub(crate) fn resolve_indexes_in_memory(
    indexes: &[IndexType],
    components: &[IndexComponent],
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<InMemoryServePlan>> {
    for index in indexes {
        match index.resolve_in_memory(components, layout, pattern, codes)? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// File-backed counterpart of [`resolve_indexes_in_memory`]: the first index
/// whose file resolution isn't `Declined`, in declaration (preference) order.
///
/// Whether the matched rows can additionally be *served* from the answering
/// index's own columns rides along inside the resolution itself
/// ([`IndexResolution::Resolved::serve`]), so the store never needs to know
/// which index answered.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_indexes_file(
    indexes: &[IndexType],
    file: &crate::store::native_file::NativeStoreFile,
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    for index in indexes {
        match index.resolve_file(file, layout, pattern, codes).await? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// The set of optional secondary indexes to embed in a store.
///
/// An empty `Indexes` means no secondary index columns are written (fastest
/// write, full-scan queries only). Use `vec![IndexType::SecondaryByReference]`
/// for the compact (value, row-id) predicate/object indexes, or
/// `vec![IndexType::SecondaryByCopy]` for the full sorted quad copies.
pub type Indexes = Vec<IndexType>;

/// Deduplicate the requested indexes, preserving first-seen order, so a
/// repeated index (e.g. the same `--indexes` flag passed twice) cannot
/// produce duplicate components.
pub(crate) fn unique_indexes(indexes: &[IndexType]) -> Vec<IndexType> {
    let mut seen: Vec<IndexType> = Vec::with_capacity(indexes.len());
    for &idx in indexes {
        if !seen.contains(&idx) {
            seen.push(idx);
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Term};

    #[test]
    fn slug_registry_covers_every_role() {
        // Every declared role's slug resolves back to its own index and
        // component name — the mapping a persisted child is read through.
        for index in ALL_INDEX_TYPES {
            for role in index.component_roles() {
                let known = known_component(role.slug).expect("declared slug is known");
                assert_eq!(known.index, index);
                assert_eq!(known.role.name, role.name);
            }
        }

        // A slug this version does not implement is skippable, not fatal.
        assert!(known_component("secondary-by-copy/spog").is_none());
        assert!(known_component("").is_none());
    }

    #[test]
    fn indexed_component_clear() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("o"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());

        let bound = QuadPattern::new(Some(&s), Some(&p), Some(&o), Some(&g));

        let r = IndexedComponent::Object.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_some() && r.object.is_none() && r.graph.is_some()
        );

        let r = IndexedComponent::Predicate.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_none() && r.object.is_some() && r.graph.is_some()
        );

        let r = IndexedComponent::PredicateObject.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_none() && r.object.is_none() && r.graph.is_some()
        );
    }
}

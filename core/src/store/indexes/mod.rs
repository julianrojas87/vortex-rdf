use std::ops::Range;
use std::sync::Arc;

use clap::ValueEnum;
use oxrdf::Quad;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::{DType, FieldName, FieldNames};
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and, eq, get_item, gt_eq, lit, lt_eq, root, select};
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{PatternCodes, QuadPattern, ResolvedLayout};

pub mod secondary_by_copy;
pub mod secondary_by_reference;
#[cfg(feature = "file-io")]
pub(crate) mod tee;

/// A secondary index that can be embedded alongside the primary quad columns.
///
/// Variant declaration order is the resolution preference order: pattern
/// matching tries each detected index in this order and takes the first that
/// doesn't decline (see `resolve_indexes_in_memory`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, ValueEnum)]
pub enum IndexType {
    /// Appends two complete extra copies of the quad columns, each in its own
    /// sort order and each paired with the primary row IDs it permutes — the
    /// classic triple-store permutation indexes, giving predicate- and
    /// object-bound patterns the same sorted-column access path the primary
    /// (s, p, o, g) order gives subjects.
    ///
    /// Adds ten extra columns to the StructArray (`VarBin<Utf8>` term strings,
    /// or u32 codes under the Dictionary layout; `_idx_*_rid` always `u32`):
    /// - `_idx_posg_{s,p,o,g,rid}`: the quads sorted by (p, o, s, g)
    /// - `_idx_ospg_{s,p,o,g,rid}`: the quads sorted by (o, s, p, g)
    ///
    /// Predicate-bound patterns binary-search `_idx_posg_p`; a bound
    /// predicate **and** object prefix-search (p, o) in one probe, resolving
    /// both components; object-bound patterns binary-search `_idx_ospg_o`.
    /// In file-backed stores the copies additionally let `quads()` stream the
    /// matching rows from a *contiguous* run of the copy columns instead of
    /// scattering row-id reads across the primary columns. As with
    /// [`SecondaryByReference`](Self::SecondaryByReference), in-memory routing
    /// engages only when the lead value columns carry the `IsSorted` statistic
    /// (single-chunk builds, or the sorted builders' global emission).
    ///
    /// Costs roughly 2× the primary columns in extra storage — choose it over
    /// `SecondaryByReference` when predicate/object reads dominate.
    SecondaryByCopy,

    /// Builds sorted secondary indexes for both predicates **and** objects.
    ///
    /// Adds four extra columns to the StructArray:
    /// - `_idx_o_val`: object values sorted (`VarBin<Utf8>`; u32 codes under
    ///   the Dictionary layout)
    /// - `_idx_o_rid`: primary row IDs corresponding to each sorted object (`u32`)
    /// - `_idx_p_val`: predicate values sorted (`VarBin<Utf8>`; u32 codes under
    ///   the Dictionary layout)
    /// - `_idx_p_rid`: primary row IDs corresponding to each sorted predicate (`u32`)
    ///
    /// Enables binary-search routing in `match_pattern` for predicate-only and
    /// object-only patterns, avoiding full scans. Routing engages only when
    /// the value columns carry the `IsSorted` statistic, which builders stamp
    /// when the columns hold a globally sorted order (single-chunk builds, or
    /// the sorted builders' global emission).
    SecondaryByReference,
}

impl IndexType {
    /// Whether this index needs the sorted builders' global two-pass
    /// emission path so its value columns are globally sorted.
    pub(crate) fn needs_global_sorted_emission(self) -> bool {
        match self {
            IndexType::SecondaryByCopy => true,
            IndexType::SecondaryByReference => true,
        }
    }

    /// Append the columns contributed by this index type to the given field
    /// name/array vectors, sorting the chunk's own quads.
    ///
    /// This is the single dispatch point for per-chunk index-column
    /// generation: adding a new `IndexType` variant only requires a new arm
    /// here delegating to its dedicated module.
    ///
    /// `start_row` is the global row ID of the first quad in `quads`, so
    /// per-chunk builders can emit row IDs that address the fully assembled
    /// array. An empty `quads` slice yields empty columns with the correct
    /// dtypes (used for empty-store schemas).
    ///
    /// `whole_dataset` marks the chunk as spanning the entire dataset, making
    /// the chunk-local sort a global order (stamped `IsSorted` for routing).
    pub(crate) fn append_columns(
        self,
        field_names: &mut Vec<Arc<str>>,
        field_arrays: &mut Vec<ArrayRef>,
        quads: &[RawQuad],
        start_row: u32,
        whole_dataset: bool,
    ) {
        match self {
            IndexType::SecondaryByCopy => secondary_by_copy::append_columns(
                field_names,
                field_arrays,
                quads,
                start_row,
                whole_dataset,
            ),
            IndexType::SecondaryByReference => secondary_by_reference::append_columns(
                field_names,
                field_arrays,
                quads,
                start_row,
                whole_dataset,
            ),
        }
    }

    /// Append this index's columns for a Dictionary-layout chunk, where terms
    /// are already encoded as u32 codes. Sorted-dictionary codes preserve
    /// lexicographic order, so a code-based index is order-equivalent to its
    /// string-based counterpart while sorting integers instead of strings and
    /// storing 4 bytes per entry.
    ///
    /// `start_row` and `whole_dataset` have the same semantics as
    /// [`Self::append_columns`].
    pub(crate) fn append_dictionary_columns(
        self,
        field_names: &mut Vec<Arc<str>>,
        field_arrays: &mut Vec<ArrayRef>,
        codes: &QuadCodes,
        start_row: u32,
        whole_dataset: bool,
    ) {
        match self {
            IndexType::SecondaryByCopy => secondary_by_copy::append_encoded_columns(
                field_names,
                field_arrays,
                codes,
                start_row,
                whole_dataset,
            ),
            IndexType::SecondaryByReference => secondary_by_reference::append_encoded_columns(
                field_names,
                field_arrays,
                codes,
                start_row,
                whole_dataset,
            ),
        }
    }

    /// Whether this index's columns are present in an array/file of `dtype`.
    ///
    /// The query-side counterpart of `append_columns`: each index module owns
    /// its column-name scheme, so detection dispatches there rather than the
    /// store probing hardcoded names.
    pub(crate) fn is_present(self, dtype: &DType) -> bool {
        match self {
            IndexType::SecondaryByCopy => secondary_by_copy::is_present(dtype),
            IndexType::SecondaryByReference => secondary_by_reference::is_present(dtype),
        }
    }

    /// Resolve this index against an in-memory base array, producing the exact
    /// base row ids for whichever pattern component it covers.
    ///
    /// Each index owns its own execution: it decides which pattern shapes it
    /// accelerates (e.g. `SecondaryByReference` declines when a subject is
    /// bound), chooses and probes its columns, and hands back the row ids to
    /// select — or declines, leaving the store to fall back to a scan. Like
    /// `append_columns`, the exhaustive match makes the compiler demand a
    /// query-side answer from every new index variant.
    pub(crate) fn resolve_in_memory(
        self,
        components: &[IndexComponent],
        layout: &ResolvedLayout,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
    ) -> Result<IndexResolution> {
        match self {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::resolve_in_memory(components, layout, pattern, codes)
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::resolve_in_memory(components, layout, pattern, codes)
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
        file: &crate::io::native_file::NativeStoreFile,
        layout: &ResolvedLayout,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
    ) -> Result<IndexResolution> {
        match self {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::resolve_file(file, layout, pattern, codes).await
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::resolve_file(file, layout, pattern, codes).await
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
/// — so the store folds either one into a [`RowSelection`] the same way.
///
/// [`RowSelection`]: crate::store::selection::RowSelection
// `Resolved` dwarfs the dataless `Declined`, but the enum is a transient
// per-match return value that is destructured immediately, never stored in
// bulk — boxing its fields would buy nothing but an allocation per query.
#[allow(clippy::large_enum_variant)]
pub(crate) enum IndexResolution {
    /// The index does not accelerate this pattern: either its shape isn't one
    /// this index covers, or (in memory) its value column isn't in a usable
    /// sorted form. The caller falls back to its non-indexed path.
    Declined,
    /// The index applies and proved the pattern matches no row — the probed
    /// term is absent from the indexed column. The caller short-circuits to an
    /// empty result.
    Empty,
    /// The index resolved `resolves`, yielding exactly `row_ids`: a non-empty,
    /// ascending set of base row ids. The caller narrows its selection to those
    /// ids and drops `resolves` from any residual filtering, since the ids
    /// already satisfy it.
    ///
    /// `serve` is the optional, index-agnostic *serving plan*: when the index
    /// also holds the matched quads clustered in its own columns, it hands back
    /// a [`ServePlan`] so the store can read them straight from there instead of
    /// gathering the primary columns by scattered row id — a contiguous file
    /// scan or, for an in-memory base, a plain array slice. An index that stores
    /// only back-references (no whole quads) leaves it `None`. It is a pure
    /// optimization — `row_ids` already resolve the pattern on their own.
    Resolved {
        row_ids: Buffer<u64>,
        resolves: IndexedComponent,
        serve: Option<ServePlan>,
    },
}

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

/// Decode a row-id column into the ascending, unique `Buffer<u64>` every index
/// resolution answers in.
///
/// The whole column is cast and decoded at once rather than pulled one scalar
/// at a time — this is the dominant cost for a frequent term with many matches.
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

    let mut sorted = ids.as_slice().to_vec();
    sorted.sort_unstable();
    Ok(Buffer::from_iter(sorted))
}

/// Scan `row_id_column` for the rows where every `(value_column, probe)`
/// equality holds, returning the primary row ids as an ascending, unique
/// buffer (the shape vortex's `Selection::IncludeByIndex` requires) — the
/// file-backed probe shared by the secondary indexes.
///
/// Each equality is expressed as a range (`>= probe AND <= probe`): the value
/// columns are sorted (or at least clustered per chunk), so range predicates
/// let Vortex prune to the splits whose min/max can hold the probe without
/// materializing the whole column. Output order is irrelevant (the ids are
/// sorted afterwards), so the scan may run unordered.
#[cfg(feature = "file-io")]
pub(crate) async fn scan_index_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    value_constraints: &[(&'static str, Scalar)],
    row_id_column: &'static str,
) -> Result<Buffer<u64>> {
    let mut filter: Option<Expression> = None;
    for (column, probe) in value_constraints {
        let bounded = and(
            gt_eq(get_item(*column, root()), lit(probe.clone())),
            lt_eq(get_item(*column, root()), lit(probe.clone())),
        );
        filter = Some(match filter.take() {
            Some(f) => and(f, bounded),
            None => bounded,
        });
    }
    // Every index probes at least one value column; an empty constraint set
    // would mean "all rows", which no resolver asks for.
    let Some(filter) = filter else {
        return Ok(Buffer::empty());
    };

    let arr = vortex_layout::scan::scan_builder::ScanBuilder::new(VORTEX_SESSION.clone(), reader)
        .with_projection(select([row_id_column], root()))
        .with_filter(filter)
        .with_ordered(false)
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

/// Index types whose columns are present in `dtype` — how a store discovers
/// its queryable indexes from an array or file schema at construction.
///
/// Iterates every `IndexType` variant via clap's derived
/// `ValueEnum::value_variants`, so a new variant flows in here (and into the
/// resolvers) with no store changes once its `is_present`/`resolve_*` arms
/// exist.
pub(crate) fn detect_indexes(dtype: &DType) -> Indexes {
    IndexType::value_variants()
        .iter()
        .copied()
        .filter(|index| index.is_present(dtype))
        .collect()
}

/// One secondary-index component held in memory beside the store's primary
/// base: the in-memory twin of a native store file's index child, carrying
/// the same rows under the same child schema (plain column names — `s`, `p`,
/// `o`, `g`, `rid` for a copy family; `val`, `rid` for a reference role).
///
/// `rid` values address rows of the base the component was built against;
/// that is what keeps components valid across derived views (a
/// `RowSelection` narrows without renumbering) and what invalidates them on
/// any physical gather.
#[derive(Clone)]
pub(crate) struct IndexComponent {
    /// The component name (`index:posg`, `index:ref-o`, …) — the same
    /// identity the persisted child carries.
    pub(crate) name: &'static str,
    /// The implementation slug (`secondary-by-copy/posg`, …).
    pub(crate) implementation: &'static str,
    /// The component's rows, canonicalized to one struct in child schema.
    pub(crate) array: StructArray,
    /// Whether the sort-key columns are GLOBALLY sorted — the writer's
    /// provenance, not an inspection: binary-search resolution is gated on
    /// this, and per-chunk-sorted data must never claim it (a false stamp
    /// corrupts query results; see the `sorted` field on the wire
    /// descriptor).
    pub(crate) sorted: bool,
}

impl IndexComponent {
    /// The component's rows as an `ArrayRef` (an `Arc` bump).
    pub(crate) fn as_array(&self) -> ArrayRef {
        self.array.clone().into_array()
    }

    /// Look a component up by name.
    pub(crate) fn find<'a>(
        components: &'a [IndexComponent],
        name: &str,
    ) -> Option<&'a IndexComponent> {
        components.iter().find(|c| c.name == name)
    }
}

/// The index set a component roster implies, in declaration (preference)
/// order — the in-memory counterpart of reading a file's child roster.
pub(crate) fn indexes_from_components(components: &[IndexComponent]) -> Indexes {
    let mut indexes: Indexes = Vec::new();
    for component in components {
        if let Some(index) = IndexType::from_component_slug(component.implementation)
            && !indexes.contains(&index)
        {
            indexes.push(index);
        }
    }
    indexes.sort_by_key(|index| *index as usize);
    indexes
}

/// Split a builder's welded row space into the store's model: the primary
/// quad array (index columns projected away) plus one [`IndexComponent`] per
/// persisted-child role the `_idx_*` columns assemble.
///
/// The builders keep emitting the welded form — it is also the streaming
/// write path's wire shape — and the store splits once at construction.
/// Sortedness is read off the welded columns' own `IsSorted` stamps, which
/// only the globally-sorted emission paths set (multi-chunk arrays lose
/// per-chunk stamps in canonicalization, correctly landing on `false`: a
/// concatenation of per-chunk sorts is not binary-searchable).
pub(crate) fn split_built_row_space(array: ArrayRef) -> Result<(ArrayRef, Vec<IndexComponent>)> {
    let detected = detect_indexes(array.dtype());
    if detected.is_empty() {
        return Ok((array, Vec::new()));
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = array
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let mut components = Vec::new();
    for index in &detected {
        match index {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::components_from_row_space(&struct_arr, &mut components)?
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::components_from_row_space(&struct_arr, &mut components)?
            }
        }
    }
    let primary = strip_index_columns(struct_arr.into_array())?;
    Ok((primary, components))
}

/// Rebuild a component from row-space column arrays: relabel
/// `source_columns[i]` (shared, stats and all) to the child's
/// `target_columns[i]` names. `sorted` provenance comes from the lead
/// column's own stamp — set only by globally-sorted emission.
fn component_from_columns(
    struct_arr: &StructArray,
    name: &'static str,
    implementation: &'static str,
    source_columns: &[&'static str],
    target_columns: &[&'static str],
    lead_source: &'static str,
) -> Result<Option<IndexComponent>> {
    let mut arrays = Vec::with_capacity(source_columns.len());
    for column in source_columns {
        match struct_arr.unmasked_field_by_name(column) {
            Ok(a) => arrays.push(a.clone()),
            // A partial column set means this component was never built.
            Err(_) => return Ok(None),
        }
    }
    let sorted = source_columns
        .iter()
        .position(|c| c == &lead_source)
        .map(|i| crate::store::array::column_is_sorted(&arrays[i]))
        .unwrap_or(false);
    let len = struct_arr.len();
    let array = StructArray::try_new(
        target_columns
            .iter()
            .map(|n| FieldName::from(*n))
            .collect::<Vec<_>>()
            .into(),
        arrays,
        len,
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)?;
    Ok(Some(IndexComponent {
        name,
        implementation,
        array,
        sorted,
    }))
}

/// One persisted child component of an index type: the descriptor written
/// into the native store root plus the row-space columns the component is
/// assembled from (`source_columns[i]` becomes the child's
/// `target_columns[i]`).
#[cfg(feature = "file-io")]
pub(crate) struct IndexComponentSpec {
    pub(crate) descriptor: crate::io::store_layout::StoreComponentDescriptor,
    pub(crate) source_columns: Vec<&'static str>,
    pub(crate) target_columns: Vec<&'static str>,
}

/// The index components a row-space dtype implies, over every detected index
/// type — what the serializer turns into auxiliary children. `sorted` is the
/// writer's promise that the emitted columns are globally (not per-chunk)
/// sorted; it travels to the descriptors so a reader lifting the components
/// back into memory knows whether they are binary-searchable.
#[cfg(feature = "file-io")]
pub(crate) fn index_component_specs(
    dtype: &DType,
    sorted: bool,
) -> Result<Vec<IndexComponentSpec>> {
    let mut specs = Vec::new();
    for index in detect_indexes(dtype) {
        match index {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::push_component_specs(dtype, sorted, &mut specs)?
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::push_component_specs(dtype, sorted, &mut specs)?
            }
        }
    }
    Ok(specs)
}

/// The child struct dtype a component assembles: the row-space columns'
/// dtypes under the child's own names.
#[cfg(feature = "file-io")]
pub(crate) fn component_child_dtype(
    dtype: &DType,
    source_columns: &[&'static str],
    target_columns: &[&'static str],
) -> Result<DType> {
    use vortex_array::dtype::{Nullability, StructFields};
    let DType::Struct(fields, _) = dtype else {
        return Err(VortexRdfError::Serialization(format!(
            "index components need a struct row space, got {dtype}"
        )));
    };
    let field_dtypes = source_columns
        .iter()
        .map(|n| {
            fields.field(n).ok_or_else(|| {
                VortexRdfError::Serialization(format!("row space misses index column {n}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DType::Struct(
        StructFields::new(
            target_columns
                .iter()
                .map(|n| (*n).into())
                .collect::<Vec<std::sync::Arc<str>>>()
                .into(),
            field_dtypes,
        ),
        Nullability::NonNullable,
    ))
}

/// The canonical (name, implementation) identity of a known index component,
/// by implementation slug — how a reader adopts a persisted child as an
/// in-memory [`IndexComponent`]. `None` for foreign slugs (skippable when
/// optional).
pub(crate) fn component_identity_for_slug(
    implementation: &str,
) -> Option<(&'static str, &'static str)> {
    use secondary_by_copy::Family;
    match implementation {
        x if x == secondary_by_copy::POSG_IMPLEMENTATION => Some((
            Family::Posg.component_name(),
            secondary_by_copy::POSG_IMPLEMENTATION,
        )),
        x if x == secondary_by_copy::OSPG_IMPLEMENTATION => Some((
            Family::Ospg.component_name(),
            secondary_by_copy::OSPG_IMPLEMENTATION,
        )),
        x if x == secondary_by_reference::O_IMPLEMENTATION => Some((
            secondary_by_reference::REF_O_COMPONENT,
            secondary_by_reference::O_IMPLEMENTATION,
        )),
        x if x == secondary_by_reference::P_IMPLEMENTATION => Some((
            secondary_by_reference::REF_P_COMPONENT,
            secondary_by_reference::P_IMPLEMENTATION,
        )),
        _ => None,
    }
}

/// The in-memory row-space column names a persisted index component's
/// columns re-glue into (positionally matching the child's column order) —
/// the read-side inverse of [`index_component_specs`].
pub(crate) fn row_space_columns_for_slug(implementation: &str) -> Option<Vec<&'static str>> {
    use secondary_by_copy::Family;
    match implementation {
        secondary_by_copy::POSG_IMPLEMENTATION => Some(Family::Posg.column_names().to_vec()),
        secondary_by_copy::OSPG_IMPLEMENTATION => Some(Family::Ospg.column_names().to_vec()),
        secondary_by_reference::O_IMPLEMENTATION => Some(vec![
            secondary_by_reference::O_VAL_COL,
            secondary_by_reference::O_RID_COL,
        ]),
        secondary_by_reference::P_IMPLEMENTATION => Some(vec![
            secondary_by_reference::P_VAL_COL,
            secondary_by_reference::P_RID_COL,
        ]),
        _ => None,
    }
}

impl IndexType {
    /// The index type owning a persisted component, by implementation slug —
    /// how the open path (and [`indexes_from_components`]) maps a component
    /// roster back onto the index set.
    pub(crate) fn from_component_slug(implementation: &str) -> Option<IndexType> {
        match implementation {
            secondary_by_copy::POSG_IMPLEMENTATION | secondary_by_copy::OSPG_IMPLEMENTATION => {
                Some(IndexType::SecondaryByCopy)
            }
            secondary_by_reference::O_IMPLEMENTATION | secondary_by_reference::P_IMPLEMENTATION => {
                Some(IndexType::SecondaryByReference)
            }
            _ => None,
        }
    }
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
) -> Result<IndexResolution> {
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
    file: &crate::io::native_file::NativeStoreFile,
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution> {
    for index in indexes {
        match index.resolve_file(file, layout, pattern, codes).await? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// True when any requested index type requires the sorted builders' global
/// two-pass emission pipeline. The plural name marks the planner over the whole
/// index set; the singular [`IndexType::needs_global_sorted_emission`] it folds
/// over answers for one index.
pub(crate) fn indexes_need_global_sorted_emission(indexes: &[IndexType]) -> bool {
    unique_indexes(indexes)
        .into_iter()
        .any(IndexType::needs_global_sorted_emission)
}

/// The set of optional secondary indexes to embed in a store.
///
/// An empty `Indexes` means no secondary index columns are written (fastest
/// write, full-scan queries only). Use `vec![IndexType::SecondaryByReference]`
/// for the compact (value, row-id) predicate/object indexes, or
/// `vec![IndexType::SecondaryByCopy]` for the full sorted quad copies.
pub type Indexes = Vec<IndexType>;

/// The index value columns a globally sorted emission guarantees sorted —
/// what the sorted builders (re-)stamp `IsSorted` after canonicalization,
/// which drops per-chunk stats. Sourced from the index modules' own name
/// accessors so the names live in exactly one place each.
pub(crate) fn globally_sorted_columns() -> [&'static str; 4] {
    [
        secondary_by_reference::O_VAL_COL,
        secondary_by_reference::P_VAL_COL,
        secondary_by_copy::Family::Posg.lead_col(),
        secondary_by_copy::Family::Ospg.lead_col(),
    ]
}

/// Deduplicate the requested indexes, preserving first-seen order, so a
/// repeated index (e.g. the same `--indexes` flag passed twice) cannot
/// produce duplicate columns in the schema.
pub(crate) fn unique_indexes(indexes: &[IndexType]) -> Vec<IndexType> {
    let mut seen: Vec<IndexType> = Vec::with_capacity(indexes.len());
    for &idx in indexes {
        if !seen.contains(&idx) {
            seen.push(idx);
        }
    }
    seen
}

/// Project away secondary-index columns (`_idx_*`), keeping the layout's
/// primary columns. Returns the array unchanged when it carries no index
/// columns.
pub(crate) fn strip_index_columns(arr: ArrayRef) -> Result<ArrayRef> {
    // Figure out which field names to keep; bail out unchanged if there are
    // no `_idx_*` columns to strip in the first place (the common case).
    let keep: Vec<FieldName> = match arr.dtype() {
        DType::Struct(fields, _)
            if fields
                .names()
                .iter()
                .any(|n| n.as_ref().starts_with("_idx_")) =>
        {
            fields
                .names()
                .iter()
                .filter(|n| !n.as_ref().starts_with("_idx_"))
                .cloned()
                .collect()
        }
        _ => return Ok(arr),
    };

    // Rebuild the struct with only the kept columns.
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let arrays: Vec<ArrayRef> = keep
        .iter()
        .map(|n| {
            struct_arr
                .unmasked_field_by_name(n.as_ref())
                .cloned()
                .map_err(VortexRdfError::Vortex)
        })
        .collect::<Result<_>>()?;
    let len = struct_arr.len();
    StructArray::try_new(keep.into(), arrays, len, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Term};
    use vortex_array::dtype::{Nullability, StructFields};

    fn struct_dtype(names: &[&str]) -> DType {
        DType::Struct(
            StructFields::from_iter(
                names
                    .iter()
                    .map(|n| (*n, DType::Utf8(Nullability::NonNullable))),
            ),
            Nullability::NonNullable,
        )
    }

    #[test]
    fn detect_indexes_by_schema() {
        // All four columns present: the index is detected.
        let with_idx = struct_dtype(&[
            "s",
            "p",
            "o",
            "g",
            "_idx_o_val",
            "_idx_o_rid",
            "_idx_p_val",
            "_idx_p_rid",
        ]);
        assert_eq!(
            detect_indexes(&with_idx),
            vec![IndexType::SecondaryByReference]
        );

        // The ten copy-family columns mark the SecondaryByCopy index.
        let with_copy = struct_dtype(&[
            "s",
            "p",
            "o",
            "g",
            "_idx_posg_s",
            "_idx_posg_p",
            "_idx_posg_o",
            "_idx_posg_g",
            "_idx_posg_rid",
            "_idx_ospg_s",
            "_idx_ospg_p",
            "_idx_ospg_o",
            "_idx_ospg_g",
            "_idx_ospg_rid",
        ]);
        assert_eq!(detect_indexes(&with_copy), vec![IndexType::SecondaryByCopy]);

        // Both index families in one schema: copy first (preference order).
        let with_both = struct_dtype(&[
            "s",
            "p",
            "o",
            "g",
            "_idx_o_val",
            "_idx_o_rid",
            "_idx_p_val",
            "_idx_p_rid",
            "_idx_posg_s",
            "_idx_posg_p",
            "_idx_posg_o",
            "_idx_posg_g",
            "_idx_posg_rid",
            "_idx_ospg_s",
            "_idx_ospg_p",
            "_idx_ospg_o",
            "_idx_ospg_g",
            "_idx_ospg_rid",
        ]);
        assert_eq!(
            detect_indexes(&with_both),
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference]
        );

        // No index columns: nothing detected.
        let without_idx = struct_dtype(&["s", "p", "o", "g"]);
        assert!(detect_indexes(&without_idx).is_empty());

        // A partial column set (e.g. after a lossy projection) must not
        // count as a usable index.
        let partial = struct_dtype(&["s", "p", "o", "g", "_idx_o_val", "_idx_o_rid"]);
        assert!(detect_indexes(&partial).is_empty());
        let partial_copy = struct_dtype(&[
            "s",
            "p",
            "o",
            "g",
            "_idx_posg_s",
            "_idx_posg_p",
            "_idx_posg_o",
            "_idx_posg_g",
            "_idx_posg_rid",
        ]);
        assert!(detect_indexes(&partial_copy).is_empty());

        // Non-struct dtypes carry no indexes.
        assert!(detect_indexes(&DType::Utf8(Nullability::NonNullable)).is_empty());
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

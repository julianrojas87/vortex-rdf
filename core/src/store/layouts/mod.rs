use std::sync::Arc;

use clap::ValueEnum;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::{DType, FieldNames, PType};
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::common::array::StrColReader;
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;

pub mod default;
pub mod dictionary;
pub mod term_dictionary;
pub mod typed_object;

use self::term_dictionary::DictAccess;
use crate::store::schema::{
    COL_G, COL_O, COL_O_DATATYPE, COL_O_KIND, COL_O_LANG, COL_O_VALUE, COL_P, COL_S,
    LEGACY_DICT_FIELD, PRIMARY_COLUMNS, TERM_FIELD,
};

/// Determines the columnar schema used to store RDF quads in the Vortex StructArray.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum LayoutStrategy {
    /// ### `LayoutStrategy::Default` column schema
    ///
    /// All quad columns stored as opaque UTF-8 strings in N-Triples form.
    /// All four quad fields are stored as raw UTF-8 strings in N-Triples serialization form.
    /// Vortex applies `DictionaryLayout` internally to compress repeated values.
    ///
    /// | Column | Type              | Content                                                    |
    /// |--------|-------------------|------------------------------------------------------------|
    /// | `s`    | `VarBin<Utf8>`    | Subject: `<IRI>` or `_:blank`                              |
    /// | `p`    | `VarBin<Utf8>`    | Predicate: `<IRI>`                                         |
    /// | `o`    | `VarBin<Utf8>`    | Object: `<IRI>`, `_:blank`, `"lit"`, `"lit"@lang`, `"lit"^^<dt>` |
    /// | `g`    | `VarBin<Utf8>`    | Graph: `<IRI>`, `_:blank`, or `""` for DefaultGraph        |
    ///
    /// When `Indexes` contains `IndexType::SecondaryByReference`, four additional
    /// columns are appended: `_idx_o_val`, `_idx_o_rid`, `_idx_p_val`, `_idx_p_rid`.
    Default,

    /// ### `LayoutStrategy::TypedObject` column schema
    /// Object column decomposed into typed sub-columns (kind, value, datatype, lang).
    /// Same as `Default` for `s`, `p`, `g`. The `o` column is decomposed into typed fields
    /// so that Vortex can apply datatype-appropriate encodings (delta, RLE, dictionary).
    ///
    /// | Column       | Type                  | Content                                     |
    /// |--------------|-----------------------|---------------------------------------------|
    /// | `s`          | `VarBin<Utf8>`        | (same as Default)                           |
    /// | `p`          | `VarBin<Utf8>`        | (same as Default)                           |
    /// | `o_kind`     | `PrimitiveArray<u8>`  | 0=IRI, 1=BlankNode, 2=PlainLiteral, 3=LangLiteral, 4=TypedLiteral |
    /// | `o_value`    | `VarBin<Utf8>`        | IRI string, blank node ID, or literal value |
    /// | `o_datatype` | `VarBin<Utf8>` (nullable) | Datatype IRI — non-null when `o_kind = 4`  |
    /// | `o_lang`     | `VarBin<Utf8>` (nullable) | Language tag — non-null when `o_kind = 3`  |
    /// | `g`          | `VarBin<Utf8>`        | (same as Default)                           |
    ///
    /// When `Indexes` contains `IndexType::SecondaryByReference`, `_idx_o_val`
    /// sorts the full object terms in N-Triples form (same as the other layouts).
    TypedObject,

    /// ### `LayoutStrategy::Dictionary` column schema
    /// All four quad fields stored as u32 codes into a single global term
    /// dictionary. The dictionary is intrinsic to the layout: it lives in the
    /// `_dict_terms` column, always emitted alongside the code columns (see
    /// [`term_dictionary`]).
    ///
    /// | Column        | Type                  | Content                                             |
    /// |---------------|-----------------------|-----------------------------------------------------|
    /// | `s`,`p`,`o`,`g` | `PrimitiveArray<u32>` | code = position of the term in the sorted dictionary |
    /// | `_dict_terms` | `list<utf8>`          | row 0 = the sorted unique terms as one list; all other rows empty |
    ///
    /// Term IDs are lexicographic ranks, so code comparisons are
    /// order-isomorphic to string comparisons (sorted builders keep the
    /// subject binary-search fast path on the u32 column).
    ///
    /// When `Indexes` contains `IndexType::SecondaryByReference`, the
    /// `_idx_o_val`/`_idx_p_val` columns hold u32 codes instead of strings
    /// (see `IndexType::append_dictionary_columns`).
    Dictionary,
}

impl LayoutStrategy {
    /// Detect the column layout by inspecting the struct schema in the dtype,
    /// without materializing the array.
    pub(crate) fn from_dtype(dtype: &DType) -> LayoutStrategy {
        if let DType::Struct(fields, _) = dtype {
            // A term column (padded serialized form), the retired list-cell
            // column (rejected with an actionable error at open), or u32 code
            // columns (a bare/sidecar quads schema) all mean Dictionary.
            if fields.names().iter().any(|n| n.as_ref() == TERM_FIELD)
                || fields
                    .names()
                    .iter()
                    .any(|n| n.as_ref() == LEGACY_DICT_FIELD)
                || matches!(fields.field(COL_S), Some(DType::Primitive(ptype, _)) if ptype == PType::U32)
            {
                return LayoutStrategy::Dictionary;
            }
            // Presence of the typed-object kind column means TypedObject layout.
            if fields.names().iter().any(|n| n.as_ref() == COL_O_KIND) {
                return LayoutStrategy::TypedObject;
            }
        }
        // Neither marker column found: plain string columns, Default layout.
        LayoutStrategy::Default
    }

    /// Field names of the primary (non-index) columns for this layout.
    pub(crate) fn field_names(self) -> Vec<Arc<str>> {
        match self {
            LayoutStrategy::Default => default::field_names(),
            LayoutStrategy::TypedObject => typed_object::field_names(),
            LayoutStrategy::Dictionary => dictionary::field_names(),
        }
    }

    /// Build the primary column arrays for this layout from raw quads.
    /// An empty slice yields empty columns with the correct dtypes.
    ///
    /// Not available for `Dictionary`: encoding needs the global
    /// [`TermDictionary`], so Dictionary chunks are built by the dedicated
    /// [`dictionary::build_chunk`] pipeline instead.
    ///
    /// [`TermDictionary`]: crate::store::layouts::term_dictionary::TermDictionary
    pub(crate) fn build_columns(self, quads: &[RawQuad]) -> Result<Vec<ArrayRef>> {
        match self {
            LayoutStrategy::Default => Ok(default::build_columns(quads)),
            LayoutStrategy::TypedObject => typed_object::build_columns(quads),
            LayoutStrategy::Dictionary => Err(crate::error::VortexRdfError::Serialization(
                "Dictionary layout chunks are built via the dictionary pipeline, \
                 not the generic column path"
                    .to_string(),
            )),
        }
    }
}

/// Where a serialized Dictionary-layout dataset keeps its term dictionary.
///
/// Both forms hold the same scannable sorted term column ([`TERM_FIELD`]);
/// they differ in which file it lives in. Non-Dictionary layouts carry no
/// dictionary, so the placement is a no-op for them.
#[derive(Copy, Clone, PartialEq, Eq, Hash, ValueEnum, Debug, Default)]
pub enum DictionaryPlacement {
    /// One self-contained file: the dictionary rides as trailing rows of the
    /// quads file, in a nullable term column that is null on every quad row
    /// (see [`dictionary::pad_with_dictionary`]). The default, and the only
    /// form IPC bytes use.
    #[default]
    Padded,
    /// Two files: the quads file keeps bare code columns, and the dictionary
    /// lives in a `<stem>.dict.vortex` companion beside it. The companion
    /// must travel with the quads file — without it the codes cannot be
    /// decoded — but it can be rebuilt or re-compressed without touching the
    /// quads.
    Sidecar,
}

/// Query-time layout: the build-time [`LayoutStrategy`] resolved against a
/// constructed array, carrying any state intrinsic to the layout — for the
/// Dictionary layout, access to the global term dictionary. Holding the state
/// in the variant makes "Dictionary layout without a dictionary"
/// unrepresentable; [`DictAccess`] carries *how* the dictionary is reached
/// (resident today, file-backed planned).
#[derive(Clone)]
pub(crate) enum ResolvedLayout {
    Default,
    TypedObject,
    Dictionary(DictAccess),
}

/// One of the four term positions of a quad pattern.
#[derive(Clone, Copy)]
pub(crate) enum Role {
    S,
    P,
    O,
    G,
}

/// A quad pattern: the four term positions, each bound or free.
///
/// Travels as one value because every stage of a match needs all four together
/// — the fast paths clear whichever components they resolve, and whatever is
/// still bound afterwards is exactly what the residual filter must compare.
#[derive(Clone, Copy, Default)]
pub(crate) struct QuadPattern<'a> {
    pub(crate) subject: Option<&'a NamedOrBlankNode>,
    pub(crate) predicate: Option<&'a NamedNode>,
    pub(crate) object: Option<&'a Term>,
    pub(crate) graph: Option<&'a GraphName>,
}

impl<'a> QuadPattern<'a> {
    pub(crate) fn new(
        subject: Option<&'a NamedOrBlankNode>,
        predicate: Option<&'a NamedNode>,
        object: Option<&'a Term>,
        graph: Option<&'a GraphName>,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            graph,
        }
    }

    /// Whether any component is still bound — i.e. whether there is anything
    /// left for a residual filter to do.
    pub(crate) fn any_bound(&self) -> bool {
        self.subject.is_some()
            || self.predicate.is_some()
            || self.object.is_some()
            || self.graph.is_some()
    }
}

/// A bound term of a quad pattern, tagged with the role it occupies.
///
/// Carrying the role with the term is what makes [`PatternCodes`] safe to share
/// across the match: a probe cannot be attributed to the wrong role, because
/// there is no way to name one separately from its term.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TermRef<'a> {
    Subject(&'a NamedOrBlankNode),
    Predicate(&'a NamedNode),
    Object(&'a Term),
    Graph(&'a GraphName),
}

/// The term's N-Triples form, as the columns store it.
///
/// The default graph renders as the empty string — matching the `g` column —
/// rather than as oxrdf's own `Display`, which writes `DEFAULT`.
impl std::fmt::Display for TermRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TermRef::Subject(s) => write!(f, "{s}"),
            TermRef::Predicate(p) => write!(f, "{p}"),
            TermRef::Object(o) => write!(f, "{o}"),
            TermRef::Graph(GraphName::DefaultGraph) => Ok(()),
            TermRef::Graph(g) => write!(f, "{g}"),
        }
    }
}

impl TermRef<'_> {
    fn role(&self) -> Role {
        match self {
            TermRef::Subject(_) => Role::S,
            TermRef::Predicate(_) => Role::P,
            TermRef::Object(_) => Role::O,
            TermRef::Graph(_) => Role::G,
        }
    }

    /// Render into `out`, which is cleared first — the allocation-free form of
    /// [`Display`](std::fmt::Display), which defines what is written.
    fn write_nt(&self, out: &mut String) {
        use std::fmt::Write as _;
        out.clear();
        write!(out, "{self}").expect("writing to a String cannot fail");
    }
}

/// Memoizes a pattern's term → dictionary-code resolution across the stages of
/// one match, and owns the buffer those terms are rendered into.
///
/// `match_base` derives the same mapping three times from the same terms — the
/// unmatchable-pattern gate, the sorted-subject probe, and the residual
/// constraints after the fast paths have cleared whatever they resolved — so a
/// fully-bound pattern performed 8 dictionary binary searches (9 with
/// `SecondaryByCopy`) to resolve 4 terms, each preceded by its own `to_string`.
/// Caching per role makes that one search and one render per bound term.
///
/// The [`prepare_pattern`](ResolvedLayout::prepare_pattern) prelude fills the
/// cache for every bound role before the match core runs, so the per-stage
/// `resolve` calls below it are reads — and the one place a dictionary may
/// perform I/O to answer them is the prelude (see
/// [`DictAccess::resolve_pattern`](term_dictionary::DictAccess::resolve_pattern)).
///
/// The render goes into `scratch` rather than a fresh `String` per probe: a
/// term's N-Triples form is only needed long enough to search the dictionary
/// with it, so one buffer serves all four roles and every stage.
///
/// Scoped to a single `match_base`: the tail is matched under a *different*
/// layout (it stores terms as strings precisely so a term the base's dictionary
/// has never seen can still match), so a code cached here would be meaningless
/// there — and the tail layout resolves no codes at all.
#[derive(Default)]
pub(crate) struct PatternCodes {
    /// Per role: `None` = not resolved yet, `Some(None)` = resolved and absent
    /// from the dictionary (so the pattern cannot match).
    roles: [Option<Option<u32>>; 4],
    /// Reused render target for the bound terms; see the type docs.
    scratch: String,
}

impl PatternCodes {
    /// The code for `term`'s role, resolving it through `f` the first time
    /// only. `term` is rendered into the shared scratch buffer on a miss, and
    /// not rendered at all on a hit.
    pub(crate) fn resolve(
        &mut self,
        term: TermRef<'_>,
        f: impl FnOnce(&str) -> Option<u32>,
    ) -> Option<u32> {
        let role = term.role() as usize;
        if let Some(cached) = self.roles[role] {
            return cached;
        }
        term.write_nt(&mut self.scratch);
        let resolved = f(&self.scratch);
        self.roles[role] = Some(resolved);
        resolved
    }

    /// `term`'s N-Triples form in the shared scratch buffer — for the layouts
    /// that probe with the string itself rather than a dictionary code, which
    /// have nothing to cache but still benefit from not allocating.
    pub(crate) fn render(&mut self, term: TermRef<'_>) -> &str {
        term.write_nt(&mut self.scratch);
        &self.scratch
    }
}

/// Column equality constraints a quad pattern compiles to under a given
/// layout: the single source of truth consumed by both the in-memory mask
/// scan and the pushed-down file filter in `match_pattern`.
pub(crate) enum Constraints {
    /// A bound term cannot match any quad (e.g. absent from the dictionary).
    AlwaysFalse,
    /// Conjunction of per-column equalities; empty means unconstrained.
    Eq(Vec<(&'static str, Scalar)>),
}

impl ResolvedLayout {
    /// The build-time strategy tag this layout was resolved from.
    pub(crate) fn strategy(&self) -> LayoutStrategy {
        match self {
            ResolvedLayout::Default => LayoutStrategy::Default,
            ResolvedLayout::TypedObject => LayoutStrategy::TypedObject,
            ResolvedLayout::Dictionary(_) => LayoutStrategy::Dictionary,
        }
    }

    /// Field names of the primary (non-index) columns.
    pub(crate) fn primary_column_names(&self) -> Vec<&'static str> {
        match self {
            ResolvedLayout::Default | ResolvedLayout::Dictionary(_) => PRIMARY_COLUMNS.to_vec(),
            ResolvedLayout::TypedObject => {
                vec![
                    COL_S,
                    COL_P,
                    COL_O_KIND,
                    COL_O_VALUE,
                    COL_O_DATATYPE,
                    COL_O_LANG,
                    COL_G,
                ]
            }
        }
    }

    /// Project an array down to this layout's primary (non-index) columns only.
    pub(crate) fn project_primary(&self, arr: &ArrayRef) -> Result<ArrayRef> {
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let struct_arr = arr
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let primary = self.primary_column_names();
        let names: FieldNames = primary.iter().copied().collect();
        let arrays: Vec<ArrayRef> = primary
            .iter()
            .map(|n| {
                struct_arr
                    .unmasked_field_by_name(n)
                    .cloned()
                    .map_err(VortexRdfError::Vortex)
            })
            .collect::<Result<_>>()?;

        let len = arrays.first().map(|a| a.len()).unwrap_or(0);
        StructArray::try_new(names, arrays, len, Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)
            .map(|a| a.into_array())
    }

    /// Decode a StructArray chunk into quads. Dictionary chunks are decoded
    /// through the layout's own dictionary (their `_dict_terms` payload may
    /// have been lost to slicing/filtering/file re-blocking).
    pub(crate) fn decode_chunk(&self, chunk: &ArrayRef) -> Vec<Result<Quad>> {
        match self {
            ResolvedLayout::Default => default::decode_chunk(chunk),
            ResolvedLayout::TypedObject => typed_object::decode_chunk(chunk),
            ResolvedLayout::Dictionary(access) => match access.resident() {
                Some(dict) => dictionary::decode_chunk(chunk, dict),
                // Defensive: the read paths route file-backed stores through
                // `decode_chunk_async`, which resolves each chunk's codes with
                // a scan; reaching here means one of them didn't.
                None => vec![Err(VortexRdfError::Deserialization(
                    "a file-backed dictionary decodes chunks through the async read path"
                        .to_string(),
                ))],
            },
        }
    }

    /// [`decode_chunk`](Self::decode_chunk) with the file-backed Dictionary
    /// case handled: the chunk's distinct codes are resolved to terms with one
    /// dictionary scan, and the chunk decodes against that map. Every other
    /// layout (and a resident dictionary) takes the sync path unchanged.
    #[cfg(feature = "file-io")]
    pub(crate) async fn decode_chunk_async(&self, chunk: &ArrayRef) -> Vec<Result<Quad>> {
        if let ResolvedLayout::Dictionary(DictAccess::FileBacked(fb)) = self {
            return match fb.chunk_term_map(chunk).await {
                Ok(terms) => dictionary::decode_chunk_mapped(chunk, &terms),
                Err(e) => vec![Err(e)],
            };
        }
        self.decode_chunk(chunk)
    }

    /// Write whatever state this layout holds intrinsically back into `array`,
    /// so that it can be serialized and read back without this layout's help.
    ///
    /// This is the counterpart to the splitting done when a serialized form is
    /// opened: state that lives in the array is hoisted into the variant at
    /// construction, and derived arrays no longer carry it. Only the
    /// Dictionary layout has such state (its term dictionary), appended as
    /// trailing dictionary rows — the padded form (see
    /// [`dictionary::pad_with_dictionary`]); the other layouts encode
    /// everything in the columns themselves and pass `array` straight through.
    pub(crate) async fn attach_intrinsic_state(&self, array: ArrayRef) -> Result<ArrayRef> {
        match self {
            ResolvedLayout::Default | ResolvedLayout::TypedObject => Ok(array),
            ResolvedLayout::Dictionary(access) => {
                // A file-backed dictionary is lifted resident transiently for
                // the write — the serialized form must carry the whole column.
                let dict = access.ensure_resident().await?;
                dictionary::pad_with_dictionary(&array, &dict)
            }
        }
    }

    /// Pre-resolve a pattern's bound terms into `codes` before the
    /// synchronous match core runs — the one point in a match where a
    /// dictionary may perform I/O (see [`DictAccess::resolve_pattern`]).
    /// A no-op for the layouts that probe with term strings directly.
    pub(crate) async fn prepare_pattern(
        &self,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
    ) -> Result<()> {
        match self {
            ResolvedLayout::Dictionary(access) => access.resolve_pattern(pattern, codes).await,
            ResolvedLayout::Default | ResolvedLayout::TypedObject => Ok(()),
        }
    }

    /// Scalar for probing a sorted term column — the primary `s` column or a
    /// secondary index's `_idx_*_val` column. Under the Dictionary layout the
    /// term is translated to its u32 code (sorted-dictionary codes preserve
    /// lexicographic order); `None` means the term is absent from the
    /// dictionary and matches nothing.
    ///
    /// Always goes through `cache`, which is what keeps one match to a single
    /// dictionary search and a single render per bound term however many stages
    /// and indexes ask for the same probe. There is deliberately no uncached
    /// variant: every probe in a match is for one of the pattern's four terms,
    /// so an uncached one could only ever repeat work already done.
    pub(crate) fn probe_scalar_cached(
        &self,
        term: TermRef<'_>,
        cache: &mut PatternCodes,
    ) -> Option<Scalar> {
        match self {
            ResolvedLayout::Dictionary(dict) => {
                // Post-prelude, every bound role is already cached; the
                // closure only ever runs for a resident dictionary.
                cache
                    .resolve(term, |s| dict.get_id_resolved(s))
                    .map(Scalar::from)
            }
            _ => Some(Scalar::from(cache.render(term))),
        }
    }

    /// Decode an array of this layout's rows back into [`RawQuad`]s — each
    /// term in its N-Triples string form, without an oxrdf parse round-trip.
    ///
    /// The inverse of the build-time column encoding, for the operations that
    /// rebuild a store from its quads (compaction, and reads that must merge a
    /// string tail into a Dictionary-encoded base): Default reads its four
    /// string columns verbatim, TypedObject recomposes the object term from
    /// its typed sub-columns, and Dictionary resolves each u32 code through
    /// this layout's term dictionary.
    pub(crate) fn raw_quads(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let struct_arr = rows
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let (s, p, o, g) = match self {
            ResolvedLayout::Default => (
                read_string_column(&struct_arr, COL_S)?,
                read_string_column(&struct_arr, COL_P)?,
                read_string_column(&struct_arr, COL_O)?,
                read_string_column(&struct_arr, COL_G)?,
            ),
            ResolvedLayout::TypedObject => (
                read_string_column(&struct_arr, COL_S)?,
                read_string_column(&struct_arr, COL_P)?,
                typed_object::object_terms(&struct_arr)?,
                read_string_column(&struct_arr, COL_G)?,
            ),
            ResolvedLayout::Dictionary(access) => {
                let dict = access.resident().ok_or_else(|| {
                    VortexRdfError::Deserialization(
                        "a file-backed dictionary reconstructs rows through raw_quads_async"
                            .to_string(),
                    )
                })?;
                let term = |codes: Vec<u32>| -> Result<Vec<String>> {
                    let mut reader = dict.reader();
                    codes
                        .into_iter()
                        .map(|code| {
                            if (code as usize) >= dict.len() {
                                return Err(VortexRdfError::Deserialization(format!(
                                    "Term code {} out of dictionary bounds ({})",
                                    code,
                                    dict.len()
                                )));
                            }
                            reader.str_at(code as usize).map(str::to_string)
                        })
                        .collect()
                };
                (
                    term(read_u32_column(&struct_arr, COL_S)?)?,
                    term(read_u32_column(&struct_arr, COL_P)?)?,
                    term(read_u32_column(&struct_arr, COL_O)?)?,
                    term(read_u32_column(&struct_arr, COL_G)?)?,
                )
            }
        };
        Ok(s.into_iter()
            .zip(p)
            .zip(o)
            .zip(g)
            .map(|(((s, p), o), g)| RawQuad { s, p, o, g })
            .collect())
    }

    /// [`raw_quads`](Self::raw_quads) with the file-backed Dictionary case
    /// handled: the whole dictionary is lifted resident transiently (the
    /// callers — compaction and tail-merge re-encoding — touch most of it
    /// anyway), then the sync decode runs unchanged.
    #[cfg(feature = "file-io")]
    pub(crate) async fn raw_quads_async(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        if let ResolvedLayout::Dictionary(access) = self
            && access.is_file_backed()
        {
            let dict = access.ensure_resident().await?;
            let resident = ResolvedLayout::Dictionary(DictAccess::Resident(dict));
            return resident.raw_quads(rows);
        }
        self.raw_quads(rows)
    }

    /// Compile a quad pattern into per-column equality constraints: the
    /// layout-specific term → (column, scalar) mapping.
    ///
    /// Prefer [`constraints_cached`](Self::constraints_cached) inside a single
    /// match: the stages of `match_base` each derive this from the same terms,
    /// and under the Dictionary layout deriving it costs a binary search per
    /// bound term.
    pub(crate) fn constraints(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Constraints {
        self.constraints_cached(
            subject,
            predicate,
            object,
            graph,
            &mut PatternCodes::default(),
        )
    }

    /// [`constraints`](Self::constraints), reusing any term→code resolution
    /// `cache` already holds for the same match.
    pub(crate) fn constraints_cached(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
        cache: &mut PatternCodes,
    ) -> Constraints {
        let mut eqs: Vec<(&'static str, Scalar)> = Vec::new();
        match self {
            ResolvedLayout::Default => {
                if let Some(s) = subject {
                    eqs.push((COL_S, Scalar::from(cache.render(TermRef::Subject(s)))));
                }
                if let Some(p) = predicate {
                    eqs.push((COL_P, Scalar::from(cache.render(TermRef::Predicate(p)))));
                }
                if let Some(o) = object {
                    eqs.push((COL_O, Scalar::from(cache.render(TermRef::Object(o)))));
                }
                if let Some(g) = graph {
                    eqs.push((COL_G, Scalar::from(cache.render(TermRef::Graph(g)))));
                }
            }
            ResolvedLayout::TypedObject => {
                if let Some(s) = subject {
                    eqs.push((COL_S, Scalar::from(cache.render(TermRef::Subject(s)))));
                }
                if let Some(p) = predicate {
                    eqs.push((COL_P, Scalar::from(cache.render(TermRef::Predicate(p)))));
                }
                if let Some(o) = object {
                    let (kind, value, dt, lang) = typed_object::decompose_object(o);
                    eqs.push((COL_O_KIND, Scalar::from(kind)));
                    eqs.push((COL_O_VALUE, Scalar::from(value.as_str())));
                    if let Some(dt_str) = dt {
                        eqs.push((COL_O_DATATYPE, Scalar::from(dt_str.as_str())));
                    }
                    if let Some(lang_str) = lang {
                        eqs.push((COL_O_LANG, Scalar::from(lang_str.as_str())));
                    }
                }
                if let Some(g) = graph {
                    eqs.push((COL_G, Scalar::from(cache.render(TermRef::Graph(g)))));
                }
            }
            ResolvedLayout::Dictionary(dict) => {
                // Resolve every bound term to its code: a term absent from
                // the dictionary cannot match any quad. Each term is resolved
                // through `cache`, so the several stages of one match share a
                // single binary search (and a single `to_string`) per role
                // rather than repeating both.
                macro_rules! bind {
                    ($opt:expr, $ctor:expr, $field:expr) => {
                        if let Some(term) = $opt {
                            match cache.resolve($ctor(term), |s| dict.get_id_resolved(s)) {
                                Some(id) => eqs.push(($field, Scalar::from(id))),
                                None => return Constraints::AlwaysFalse,
                            }
                        }
                    };
                }
                bind!(subject, TermRef::Subject, COL_S);
                bind!(predicate, TermRef::Predicate, COL_P);
                bind!(object, TermRef::Object, COL_O);
                bind!(graph, TermRef::Graph, COL_G);
            }
        }
        Constraints::Eq(eqs)
    }
}

/// Read a UTF-8 string column into owned term strings, one per row.
fn read_string_column(struct_arr: &StructArray, name: &str) -> Result<Vec<String>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let col = struct_arr
        .unmasked_field_by_name(name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let reader = StrColReader::new(&col);
    (0..col.len())
        .map(|i| reader.str_at(i).map(str::to_string))
        .collect()
}

/// Read a u32 code column into owned codes, one per row.
fn read_u32_column(struct_arr: &StructArray, name: &str) -> Result<Vec<u32>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let col = struct_arr
        .unmasked_field_by_name(name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(col.as_slice::<u32>().to_vec())
}

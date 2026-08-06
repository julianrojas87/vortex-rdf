//! The native store container: a custom Vortex layout root.
//!
//! A store file's root layout is `vortex-rdf.store.v1`: child 0 is the
//! *transparent* `quad-source` — the quad table itself, to which the root
//! delegates its dtype, row count, and scan — and every further child is an
//! *auxiliary* component (the term dictionary, index copies, and future
//! additions such as change sets) with its own row space, written through
//! the same segment sink and addressable by name. A session that has this
//! layout registered scans the file exactly like a plain quad table; the
//! components never appear in the row space.
//!
//! The component inventory rides in the root layout's metadata: name, role,
//! implementation slug, version, the required flag, and the component's
//! column shape in a small field-kind vocabulary (no serialized DTypes).
//! Unknown *optional* components are readable-around by construction;
//! unknown *required* components must fail the open — a reader that skipped
//! a required change set would silently resurrect deleted rows.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use vortex_array::RawMetadata;
use vortex_array::dtype::{DType, Nullability, PType, StructFields};
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};
use vortex_layout::segments::SegmentSource;
use vortex_layout::{
    Layout, LayoutChildType, LayoutDeserializeArgs, LayoutEncoding, LayoutId, LayoutReaderContext,
    LayoutReaderRef, LayoutRef, VTable,
};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_layout::{LayoutParts, layout_children};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

/// Stable identity of the store root layout. Changing the wire below means a
/// new versioned id, not a silent reinterpretation.
pub(crate) const STORE_LAYOUT_ID: &str = "vortex-rdf.store.v1";
/// The transparent quad table is always child 0.
pub(crate) const QUAD_SOURCE_CHILD: usize = 0;
pub(crate) const QUAD_SOURCE_NAME: &str = "quad-source";
/// Component name of the term dictionary child.
pub(crate) const DICT_COMPONENT_NAME: &str = "dictionary";
/// Implementation slug of the dictionary child: the lexicographically sorted
/// term column, FSST-compressed as held.
pub(crate) const DICT_IMPLEMENTATION: &str = "sorted-terms-fsst-v1";
const STORE_METADATA_VERSION: u32 = 1;

// ── component descriptors ───────────────────────────────────────────────────

/// Persisted role of an auxiliary child. `ChangeSet` is reserved for
/// immutable delta components (write those `required: true` — see module
/// doc); `Other` keeps the vocabulary open without a wire break.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum StoreComponentRole {
    Dictionary,
    Index,
    ChangeSet,
    Other,
}

/// The column-type vocabulary components may use on the wire. Every
/// component is a non-nullable struct of these leaves; extending the
/// vocabulary is backward-compatible (old readers fail to parse only files
/// that actually use the new kind).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum WireFieldKind {
    U32,
    U64,
    Utf8,
}

impl WireFieldKind {
    fn to_dtype(self) -> DType {
        match self {
            Self::U32 => DType::Primitive(PType::U32, Nullability::NonNullable),
            Self::U64 => DType::Primitive(PType::U64, Nullability::NonNullable),
            Self::Utf8 => DType::Utf8(Nullability::NonNullable),
        }
    }

    fn from_dtype(dtype: &DType) -> Option<Self> {
        match dtype {
            DType::Primitive(PType::U32, Nullability::NonNullable) => Some(Self::U32),
            DType::Primitive(PType::U64, Nullability::NonNullable) => Some(Self::U64),
            DType::Utf8(Nullability::NonNullable) => Some(Self::Utf8),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct WireField {
    name: String,
    kind: WireFieldKind,
}

fn wire_fields_to_dtype(fields: &[WireField]) -> DType {
    DType::Struct(
        StructFields::new(
            fields
                .iter()
                .map(|f| f.name.as_str().into())
                .collect::<Vec<Arc<str>>>()
                .into(),
            fields.iter().map(|f| f.kind.to_dtype()).collect(),
        ),
        Nullability::NonNullable,
    )
}

fn dtype_to_wire_fields(dtype: &DType) -> VortexResult<Vec<WireField>> {
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        vortex_bail!("store component dtype must be a non-nullable struct, got {dtype}");
    };
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, field)| {
            let kind = WireFieldKind::from_dtype(&field).ok_or_else(|| {
                vortex_error::vortex_err!(
                    "store component field {name} has a dtype outside the wire vocabulary: {field}"
                )
            })?;
            Ok(WireField {
                name: name.to_string(),
                kind,
            })
        })
        .collect()
}

/// Descriptor of one auxiliary child, as persisted in the root metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoreComponentDescriptor {
    pub(crate) name: String,
    pub(crate) role: StoreComponentRole,
    /// Implementation slug (e.g. `sorted-terms-fsst-v1`) — how a reader that
    /// knows the component interprets its columns.
    pub(crate) implementation: String,
    pub(crate) version: u32,
    /// Readers must reject the file when they cannot interpret a required
    /// component; unknown optional components are skipped.
    pub(crate) required: bool,
    /// Whether the component's sort-key columns are GLOBALLY sorted (not
    /// merely per-chunk). Provenance, recorded by the writer that knows how
    /// the component was built — a reader lifting the component into memory
    /// may only binary-search it when this is set. Stamping per-chunk-sorted
    /// data as sorted corrupts query results, so absent/false is the safe
    /// default.
    pub(crate) sorted: bool,
    pub(crate) dtype: DType,
}

impl StoreComponentDescriptor {
    pub(crate) fn validate(&self) -> VortexResult<()> {
        if self.name.is_empty() {
            vortex_bail!("store component name must not be empty");
        }
        if self.name == QUAD_SOURCE_NAME {
            vortex_bail!("{QUAD_SOURCE_NAME} is reserved for the transparent root child");
        }
        if self.implementation.is_empty() {
            vortex_bail!("store component implementation must not be empty");
        }
        if self.version == 0 {
            vortex_bail!("store component version must be positive");
        }
        dtype_to_wire_fields(&self.dtype)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct WireComponent {
    name: String,
    role: StoreComponentRole,
    implementation: String,
    version: u32,
    required: bool,
    #[serde(default)]
    sorted: bool,
    fields: Vec<WireField>,
}

#[derive(Serialize, Deserialize)]
struct WireMetadata {
    version: u32,
    /// Whether the quad child's `s` column is GLOBALLY sorted — the writer's
    /// provenance (a sorted builder produced the rows). A reader
    /// materializing the quads may only restore the subject binary-search
    /// stamp when this is set; the file's own statistics do not record
    /// sortedness, and a false stamp corrupts matches.
    #[serde(default)]
    quads_sorted: bool,
    components: Vec<WireComponent>,
}

fn encode_store_metadata(
    quads_sorted: bool,
    components: &[StoreComponentDescriptor],
) -> VortexResult<Vec<u8>> {
    let wire = WireMetadata {
        version: STORE_METADATA_VERSION,
        quads_sorted,
        components: components
            .iter()
            .map(|c| {
                Ok(WireComponent {
                    name: c.name.clone(),
                    role: c.role,
                    implementation: c.implementation.clone(),
                    version: c.version,
                    required: c.required,
                    sorted: c.sorted,
                    fields: dtype_to_wire_fields(&c.dtype)?,
                })
            })
            .collect::<VortexResult<Vec<_>>>()?,
    };
    serde_json::to_vec(&wire).map_err(|e| vortex_error::vortex_err!("{e}"))
}

fn decode_store_metadata(bytes: &[u8]) -> VortexResult<(bool, Vec<StoreComponentDescriptor>)> {
    if bytes.is_empty() {
        return Ok((false, Vec::new()));
    }
    let wire: WireMetadata =
        serde_json::from_slice(bytes).map_err(|e| vortex_error::vortex_err!("{e}"))?;
    vortex_ensure_eq!(
        wire.version,
        STORE_METADATA_VERSION,
        "unsupported vortex-rdf store metadata version"
    );
    let quads_sorted = wire.quads_sorted;
    let mut names = std::collections::BTreeSet::new();
    wire.components
        .into_iter()
        .map(|c| {
            let descriptor = StoreComponentDescriptor {
                dtype: wire_fields_to_dtype(&c.fields),
                name: c.name,
                role: c.role,
                implementation: c.implementation,
                version: c.version,
                required: c.required,
                sorted: c.sorted,
            };
            descriptor.validate()?;
            if !names.insert(descriptor.name.clone()) {
                vortex_bail!("duplicate store component name: {}", descriptor.name);
            }
            Ok(descriptor)
        })
        .collect::<VortexResult<Vec<_>>>()
        .map(|components| (quads_sorted, components))
}

// ── the layout vtable ───────────────────────────────────────────────────────

/// VTable of the native store root layout.
#[derive(Clone, Debug)]
pub(crate) struct RdfStoreLayoutVTable;

pub(crate) type RdfStoreLayout = Layout<RdfStoreLayoutVTable>;

#[derive(Clone, Debug)]
pub(crate) struct RdfStoreLayoutData {
    quads_sorted: bool,
    components: Arc<[StoreComponentDescriptor]>,
}

impl VTable for RdfStoreLayoutVTable {
    type LayoutData = RdfStoreLayoutData;
    type Metadata = RawMetadata;

    fn id(&self) -> LayoutId {
        static ID: CachedId = CachedId::new(STORE_LAYOUT_ID);
        *ID
    }

    fn metadata(layout: &Layout<Self>) -> Self::Metadata {
        RawMetadata(
            encode_store_metadata(layout.data().quads_sorted, &layout.data().components)
                .expect("validated store metadata must serialize"),
        )
    }

    fn deserialize(
        &self,
        args: &LayoutDeserializeArgs<'_>,
        metadata: &Vec<u8>,
    ) -> VortexResult<Self::LayoutData> {
        let (quads_sorted, components) = decode_store_metadata(metadata)?;
        vortex_ensure_eq!(
            args.children.nchildren(),
            1 + components.len(),
            "store root child count does not match its component inventory"
        );
        let quads = args.children.child(QUAD_SOURCE_CHILD, args.dtype)?;
        vortex_ensure_eq!(
            quads.row_count(),
            args.row_count,
            "quad-source row count must match the store root"
        );
        for (index, component) in components.iter().enumerate() {
            let child = args.children.child(index + 1, &component.dtype)?;
            vortex_ensure_eq!(
                child.dtype(),
                &component.dtype,
                "store component dtype does not match its descriptor"
            );
        }
        Ok(RdfStoreLayoutData {
            quads_sorted,
            components: components.into(),
        })
    }

    fn child_dtype(layout: &Layout<Self>, idx: usize) -> VortexResult<DType> {
        match idx {
            QUAD_SOURCE_CHILD => Ok(layout.dtype().clone()),
            _ => layout
                .data()
                .components
                .get(idx - 1)
                .map(|c| c.dtype.clone())
                .ok_or_else(|| vortex_error::vortex_err!("invalid store root child index: {idx}")),
        }
    }

    fn child_type(layout: &Layout<Self>, idx: usize) -> LayoutChildType {
        match idx {
            QUAD_SOURCE_CHILD => LayoutChildType::Transparent(QUAD_SOURCE_NAME.into()),
            _ => layout
                .data()
                .components
                .get(idx - 1)
                .map(|c| LayoutChildType::Auxiliary(c.name.as_str().into()))
                .unwrap_or_else(|| panic!("invalid store root child index: {idx}")),
        }
    }

    fn new_reader(
        layout: &Layout<Self>,
        name: Arc<str>,
        segment_source: Arc<dyn SegmentSource>,
        session: &VortexSession,
        ctx: &LayoutReaderContext,
    ) -> VortexResult<LayoutReaderRef> {
        // The root's scan IS the quad-source scan; auxiliary components stay
        // independently addressable through `store_component`.
        layout
            .child(QUAD_SOURCE_CHILD)?
            .new_reader(name, segment_source, session, ctx)
    }
}

// ── construction & inspection ───────────────────────────────────────────────

/// One descriptor paired with its written child layout.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
#[derive(Clone)]
pub(crate) struct StoreComponent {
    pub(crate) descriptor: StoreComponentDescriptor,
    pub(crate) layout: LayoutRef,
}

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
impl StoreComponent {
    pub(crate) fn new(
        descriptor: StoreComponentDescriptor,
        layout: LayoutRef,
    ) -> VortexResult<Self> {
        descriptor.validate()?;
        vortex_ensure_eq!(
            &descriptor.dtype,
            layout.dtype(),
            "store component descriptor dtype does not match its child layout"
        );
        Ok(Self { descriptor, layout })
    }
}

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) fn new_store_layout_with_components(
    quad_source: LayoutRef,
    quads_sorted: bool,
    components: Vec<StoreComponent>,
) -> VortexResult<RdfStoreLayout> {
    let dtype = quad_source.dtype().clone();
    let row_count = quad_source.row_count();
    let mut names = std::collections::BTreeSet::new();
    let mut descriptors = Vec::with_capacity(components.len());
    let mut children = Vec::with_capacity(1 + components.len());
    children.push(quad_source);
    for component in components {
        component.descriptor.validate()?;
        if !names.insert(component.descriptor.name.clone()) {
            vortex_bail!(
                "duplicate store component name: {}",
                component.descriptor.name
            );
        }
        descriptors.push(component.descriptor);
        children.push(component.layout);
    }
    Ok(LayoutParts::new(
        RdfStoreLayoutVTable,
        dtype,
        row_count,
        Vec::new(),
        layout_children(children),
        RdfStoreLayoutData {
            quads_sorted,
            components: descriptors.into(),
        },
    )
    .into_typed())
}

/// Whether the file's quad rows are recorded as globally `s`-sorted — the
/// writer's provenance for restoring the subject binary-search stamp on a
/// materialized read (see `WireMetadata::quads_sorted`).
pub(crate) fn quads_sorted(layout: &RdfStoreLayout) -> bool {
    layout.data().quads_sorted
}

/// Register the store layout in a session. Called once from the
/// `VORTEX_SESSION` initializer on every target — reading requires it.
pub(crate) fn register(session: &VortexSession) {
    use vortex_layout::session::LayoutSessionExt;
    static LAYOUT: RdfStoreLayoutVTable = RdfStoreLayoutVTable;
    session
        .layouts()
        .register((&LAYOUT as &dyn LayoutEncoding).into());
}

pub(crate) fn is_native_root(layout: &LayoutRef) -> bool {
    layout.encoding_id().as_ref() == STORE_LAYOUT_ID
}

pub(crate) fn is_native_file(file: &vortex_file::VortexFile) -> bool {
    is_native_root(file.footer().layout())
}

/// The persisted component inventory of a native root.
pub(crate) fn store_components(layout: &RdfStoreLayout) -> &[StoreComponentDescriptor] {
    &layout.data().components
}

/// A named auxiliary child of a native root, with its descriptor.
pub(crate) fn store_component(
    layout: &RdfStoreLayout,
    name: &str,
) -> VortexResult<Option<(StoreComponentDescriptor, LayoutRef)>> {
    let Some(index) = layout.data().components.iter().position(|c| c.name == name) else {
        return Ok(None);
    };
    let descriptor = layout.data().components[index].clone();
    layout
        .child(index + 1)
        .map(|child| Some((descriptor, child)))
}

/// On-disk byte size of a layout subtree: the sum of its segments' lengths
/// across all descendants, resolved through the footer's segment map. This
/// is the residency-threshold input for auxiliary components.
#[cfg(feature = "file-io")]
pub(crate) fn subtree_bytes(
    layout: &LayoutRef,
    segment_map: &[vortex_file::SegmentSpec],
) -> VortexResult<u64> {
    let mut total: u64 = layout
        .segment_ids()
        .into_iter()
        .map(|id| {
            segment_map
                .get(*id as usize)
                .map(|spec| u64::from(spec.length))
                .unwrap_or(0)
        })
        .sum();
    for idx in 0..layout.nchildren() {
        total += subtree_bytes(&layout.child(idx)?, segment_map)?;
    }
    Ok(total)
}

// ── the write side ──────────────────────────────────────────────────────────

/// Replayable producer for one independently typed component. Sources may be
/// buffered arrays ([`ReplayableArraySource`]), spill-run mergers, or
/// channels tee-fed by the quad stream — see [`NativeComponentSource::
/// channel_backed`] for the concurrency contract of the last kind.
pub(crate) trait NativeComponentSource: Send + Sync + 'static {
    fn dtype(&self) -> &DType;
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream>;
    // The two write-strategy hooks below are only consulted by the
    // serializer, so builds without one see them as dead.
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    fn buffered_bytes(&self) -> u64 {
        0
    }
    /// True when this source is fed by the quad stream's own poll loop (a
    /// tee channel): the write strategy must then poll every component job
    /// concurrently, or a full channel blocks the quad stream and the write
    /// deadlocks.
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    fn channel_backed(&self) -> bool {
        false
    }
}

/// A component source over already-materialized chunks. Replay is Arc-cheap;
/// large components should prefer run-backed sources.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
#[derive(Clone)]
pub(crate) struct ReplayableArraySource {
    dtype: DType,
    chunks: Arc<[vortex_array::ArrayRef]>,
    retained_bytes: u64,
}

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
impl ReplayableArraySource {
    pub(crate) fn try_new(chunks: Vec<vortex_array::ArrayRef>) -> VortexResult<Self> {
        let Some(first) = chunks.first() else {
            vortex_bail!("a replayable component source requires at least one chunk");
        };
        let dtype = first.dtype().clone();
        for chunk in &chunks {
            vortex_ensure_eq!(
                chunk.dtype(),
                &dtype,
                "component chunks must share one dtype"
            );
        }
        let retained_bytes = chunks.iter().map(|c| c.nbytes()).sum();
        Ok(Self {
            dtype,
            chunks: chunks.into(),
            retained_bytes,
        })
    }
}

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
impl NativeComponentSource for ReplayableArraySource {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let chunks = Arc::clone(&self.chunks);
        let stream = futures::stream::unfold((chunks, 0usize), |(chunks, index)| async move {
            let chunk = chunks.get(index)?.clone();
            Some((Ok(chunk), (chunks, index + 1)))
        });
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }

    fn buffered_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

/// A pull closure yielding one component chunk per call (`Ok(None)` = end).
pub(crate) type PullFn =
    Box<dyn FnMut(usize) -> VortexResult<Option<vortex_array::ArrayRef>> + Send>;

/// A single-shot component source over a pull closure — how spill-run mergers
/// stream a component's chunks without materializing them (each call reads
/// the next window off the merger's run files).
pub(crate) struct PullComponentSource {
    dtype: DType,
    batch_rows: usize,
    pull: std::sync::Mutex<Option<PullFn>>,
}

impl PullComponentSource {
    pub(crate) fn new(dtype: DType, batch_rows: usize, pull: PullFn) -> Self {
        Self {
            dtype,
            batch_rows,
            pull: std::sync::Mutex::new(Some(pull)),
        }
    }
}

impl NativeComponentSource for PullComponentSource {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let pull = self
            .pull
            .lock()
            .expect("pull source lock")
            .take()
            .ok_or_else(|| {
                vortex_error::vortex_err!("a pull-backed component source replays only once")
            })?;
        let batch_rows = self.batch_rows;
        let stream = futures::stream::unfold(Some(pull), move |state| async move {
            let mut pull = state?;
            match pull(batch_rows) {
                Ok(Some(chunk)) => Some((Ok(chunk), Some(pull))),
                Ok(None) => None,
                Err(e) => Some((Err(e), None)),
            }
        });
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }
}

#[derive(Clone)]
pub(crate) struct NativeComponentWrite {
    pub(crate) descriptor: StoreComponentDescriptor,
    pub(crate) source: Arc<dyn NativeComponentSource>,
    /// Read by the write strategy, so dead in builds without a serializer.
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    pub(crate) strategy: Arc<dyn vortex_layout::LayoutStrategy>,
}

impl NativeComponentWrite {
    pub(crate) fn new(
        descriptor: StoreComponentDescriptor,
        source: Arc<dyn NativeComponentSource>,
        strategy: Arc<dyn vortex_layout::LayoutStrategy>,
    ) -> VortexResult<Self> {
        descriptor.validate()?;
        vortex_ensure_eq!(
            &descriptor.dtype,
            source.dtype(),
            "component source dtype mismatch"
        );
        Ok(Self {
            descriptor,
            source,
            strategy,
        })
    }
}

/// The store's write strategy: the input stream becomes the transparent
/// quad-source child, each component becomes an auxiliary child, and all of
/// them share the file's segment sink.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
#[derive(Clone)]
pub(crate) struct RdfStoreWriteStrategy {
    quad_source: Arc<dyn vortex_layout::LayoutStrategy>,
    /// Provenance recorded in the root metadata: whether the quad stream's
    /// `s` column is globally sorted (see `WireMetadata::quads_sorted`).
    quads_sorted: bool,
    components: Arc<[NativeComponentWrite]>,
}

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
impl RdfStoreWriteStrategy {
    pub(crate) fn new(
        quad_source: Arc<dyn vortex_layout::LayoutStrategy>,
        quads_sorted: bool,
    ) -> Self {
        Self {
            quad_source,
            quads_sorted,
            components: Arc::from([]),
        }
    }

    pub(crate) fn with_components(
        mut self,
        components: Vec<NativeComponentWrite>,
    ) -> VortexResult<Self> {
        let mut names = std::collections::BTreeSet::new();
        for component in &components {
            component.descriptor.validate()?;
            vortex_ensure_eq!(&component.descriptor.dtype, component.source.dtype());
            if !names.insert(component.descriptor.name.clone()) {
                vortex_bail!(
                    "duplicate native component write name: {}",
                    component.descriptor.name
                );
            }
        }
        self.components = components.into();
        Ok(self)
    }
}

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
#[async_trait::async_trait]
impl vortex_layout::LayoutStrategy for RdfStoreWriteStrategy {
    async fn write_stream(
        &self,
        ctx: vortex_array::ArrayContext,
        segment_sink: vortex_layout::segments::SegmentSinkRef,
        stream: vortex_layout::sequence::SendableSequentialStream,
        mut eof: vortex_layout::sequence::SequencePointer,
        session: &VortexSession,
    ) -> VortexResult<LayoutRef> {
        use futures::{StreamExt as _, TryStreamExt as _};
        use vortex_layout::sequence::SequentialArrayStreamExt as _;

        // The input stream already occupies the first sequence subtree;
        // reserve its boundary, then ordered sibling subtrees per component.
        let quad_eof = eof.split_off();
        let mut jobs = Vec::with_capacity(self.components.len());
        for component in self.components.iter().cloned() {
            let stream_pointer = eof.split_off();
            let component_eof = eof.split_off();
            let child_ctx = ctx.clone();
            let child_sink = Arc::clone(&segment_sink);
            let child_session = session.clone();
            jobs.push(async move {
                let child_stream = component.source.open()?;
                vortex_ensure_eq!(child_stream.dtype(), &component.descriptor.dtype);
                let layout = component
                    .strategy
                    .write_stream(
                        child_ctx,
                        child_sink,
                        child_stream.sequenced(stream_pointer),
                        component_eof,
                        &child_session,
                    )
                    .await?;
                StoreComponent::new(component.descriptor, layout)
            });
        }

        let quad_future = self.quad_source.write_stream(
            ctx,
            Arc::clone(&segment_sink),
            stream,
            quad_eof,
            session,
        );
        // Channel-backed components are fed by the quad stream's poll loop:
        // every job must then be in flight or the tee blocks the quad write.
        // Lazy (replayable / merger-backed) sources cost nothing until
        // polled, so a small window only bounds concurrent compression.
        let concurrency = if self.components.iter().any(|c| c.source.channel_backed()) {
            jobs.len().max(1)
        } else {
            jobs.len().clamp(1, 2)
        };
        let components_future = futures::stream::iter(jobs)
            .buffered(concurrency)
            .try_collect::<Vec<_>>();
        let (quad_source, components) =
            futures::future::try_join(quad_future, components_future).await?;
        Ok(
            new_store_layout_with_components(quad_source, self.quads_sorted, components)?
                .into_layout(),
        )
    }

    fn buffered_bytes(&self) -> u64 {
        self.quad_source.buffered_bytes()
            + self
                .components
                .iter()
                .map(|c| c.source.buffered_bytes() + c.strategy.buffered_bytes())
                .sum::<u64>()
    }
}

/// Write a native store file: the quad stream as the transparent root child,
/// plus one auxiliary child per component.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) async fn write_store<W, S>(
    session: &VortexSession,
    writer: W,
    stream: S,
    quad_source_strategy: Arc<dyn vortex_layout::LayoutStrategy>,
    quads_sorted: bool,
    components: Vec<NativeComponentWrite>,
) -> VortexResult<vortex_file::WriteSummary>
where
    W: vortex_io::VortexWrite + Unpin,
    S: vortex_array::stream::ArrayStream + Send + 'static,
{
    use vortex_file::WriteOptionsSessionExt as _;
    let strategy = RdfStoreWriteStrategy::new(quad_source_strategy, quads_sorted)
        .with_components(components)?;
    session
        .write_options()
        .with_strategy(Arc::new(strategy))
        .write(writer, stream)
        .await
}

/// The stock write strategy `write_options()` installs — used for the quad
/// child and the index components so their encoding pipeline is exactly what
/// a plain table write produces (the dictionary child instead passes its
/// pre-compressed chunks through [`dict_child_strategy`]). Ungated like the
/// component data types: builders construct [`NativeComponentWrite`]s on
/// every target, even when no serializer is compiled in.
pub(crate) fn default_child_strategy() -> Arc<dyn vortex_layout::LayoutStrategy> {
    Arc::new(vortex_file::WriteStrategyBuilder::default().build())
}

/// The dictionary child's pass-through strategy: every chunk the source
/// emits is written verbatim as one flat leaf under a chunked node — no
/// sampling, no re-encoding. The dictionary is FSST-compressed at the source
/// in self-contained windows (`TermDictionary::compress`), so the default
/// strategy's compressor would only re-do work it cannot improve on, and the
/// window boundaries become the child's splits — the granularity at which
/// `FileBackedDict` fences, probes, and lifts it.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) fn dict_child_strategy() -> Arc<dyn vortex_layout::LayoutStrategy> {
    use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::layouts::struct_::StructStrategy;
    Arc::new(StructStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default())),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::VORTEX_SESSION;
    use vortex_array::IntoArray;
    use vortex_array::arrays::{StructArray, VarBinViewArray};
    use vortex_buffer::Buffer;
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    use vortex_buffer::ByteBuffer;
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    use vortex_file::OpenOptionsSessionExt as _;

    fn quad_chunk(base: u32, rows: u32) -> vortex_array::ArrayRef {
        let s: Buffer<u32> = (0..rows).map(|i| base + i).collect();
        let p: Buffer<u32> = (0..rows).map(|i| (base + i) % 3).collect();
        let o: Buffer<u32> = (0..rows).map(|i| (base + i) % 5).collect();
        let g: Buffer<u32> = (0..rows).map(|_| 0u32).collect();
        StructArray::from_fields(&[
            ("s", s.into_array()),
            ("p", p.into_array()),
            ("o", o.into_array()),
            ("g", g.into_array()),
        ])
        .unwrap()
        .into_array()
    }

    fn dict_chunk(terms: &[&str]) -> vortex_array::ArrayRef {
        StructArray::from_fields(&[(
            "_dict_term",
            VarBinViewArray::from_iter_str(terms.iter().copied()).into_array(),
        )])
        .unwrap()
        .into_array()
    }

    fn dict_descriptor(dtype: DType) -> StoreComponentDescriptor {
        StoreComponentDescriptor {
            name: DICT_COMPONENT_NAME.into(),
            role: StoreComponentRole::Dictionary,
            implementation: "sorted-terms-fsst-v1".into(),
            version: 1,
            required: true,
            sorted: true,
            dtype,
        }
    }

    #[test]
    fn registration_uses_stable_layout_id() {
        use vortex_layout::session::LayoutSessionExt;
        let id = <RdfStoreLayoutVTable as VTable>::id(&RdfStoreLayoutVTable);
        assert_eq!(id.as_ref(), STORE_LAYOUT_ID);
        assert!(VORTEX_SESSION.layouts().registry().find(&id).is_some());
    }

    #[test]
    fn metadata_round_trips_and_rejects_duplicates() {
        let dict = dict_descriptor(dict_chunk(&["a"]).dtype().clone());
        let index = StoreComponentDescriptor {
            name: "index:posg".into(),
            role: StoreComponentRole::Index,
            implementation: "secondary-by-copy/posg".into(),
            version: 1,
            required: false,
            sorted: true,
            dtype: quad_chunk(0, 1).dtype().clone(),
        };
        let bytes = encode_store_metadata(true, &[dict.clone(), index.clone()]).unwrap();
        let decoded = decode_store_metadata(&bytes).unwrap();
        assert_eq!(decoded, (true, vec![dict.clone(), index]));

        let dup = encode_store_metadata(false, &[dict.clone(), dict]).unwrap();
        assert!(decode_store_metadata(&dup).is_err());
    }

    #[test]
    fn metadata_rejects_unknown_version() {
        let json = br#"{"version":999,"components":[]}"#;
        assert!(decode_store_metadata(json).is_err());
    }

    #[test]
    fn descriptor_rejects_reserved_name_and_foreign_dtypes() {
        let mut d = dict_descriptor(dict_chunk(&["a"]).dtype().clone());
        d.name = QUAD_SOURCE_NAME.into();
        assert!(d.validate().is_err());

        let mut d = dict_descriptor(DType::Utf8(Nullability::NonNullable));
        d.name = DICT_COMPONENT_NAME.into();
        assert!(d.validate().is_err(), "non-struct dtype must be rejected");
    }

    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    #[tokio::test]
    async fn quads_only_root_round_trips_scan() {
        let chunks = vec![quad_chunk(0, 4), quad_chunk(4, 3)];
        let dtype = chunks[0].dtype().clone();
        let stream = ArrayStreamAdapter::new(
            dtype.clone(),
            futures::stream::iter(chunks.into_iter().map(Ok)),
        );
        let mut bytes: Vec<u8> = Vec::new();
        let summary = write_store(
            &VORTEX_SESSION,
            &mut bytes,
            stream,
            default_child_strategy(),
            false,
            Vec::new(),
        )
        .await
        .unwrap();
        assert!(is_native_root(summary.footer().layout()));

        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))
            .unwrap();
        assert!(is_native_file(&file));
        let root = file.footer().layout();
        assert_eq!(
            root.child_names().collect::<Vec<_>>(),
            vec![Arc::<str>::from(QUAD_SOURCE_NAME)]
        );
        assert_eq!(file.row_count(), 7);
        assert_eq!(file.dtype(), &dtype);

        let rows = file
            .scan()
            .unwrap()
            .into_array_stream()
            .unwrap()
            .read_all()
            .await
            .unwrap();
        assert_eq!(rows.len(), 7);
        assert_eq!(rows.dtype(), &dtype);
    }

    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    #[tokio::test]
    async fn dictionary_component_shares_file_and_scans_independently() {
        let quads = vec![quad_chunk(0, 5)];
        let dict_chunks = vec![dict_chunk(&["a", "b"]), dict_chunk(&["c"])];
        let dict_dtype = dict_chunks[0].dtype().clone();
        let dtype = quads[0].dtype().clone();

        let component = NativeComponentWrite::new(
            dict_descriptor(dict_dtype.clone()),
            Arc::new(ReplayableArraySource::try_new(dict_chunks).unwrap()),
            default_child_strategy(),
        )
        .unwrap();

        let stream = ArrayStreamAdapter::new(
            dtype.clone(),
            futures::stream::iter(quads.into_iter().map(Ok)),
        );
        let mut bytes: Vec<u8> = Vec::new();
        write_store(
            &VORTEX_SESSION,
            &mut bytes,
            stream,
            default_child_strategy(),
            false,
            vec![component],
        )
        .await
        .unwrap();

        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))
            .unwrap();
        let root = file.footer().layout();
        assert_eq!(
            root.child_names().collect::<Vec<_>>(),
            vec![
                Arc::<str>::from(QUAD_SOURCE_NAME),
                Arc::<str>::from(DICT_COMPONENT_NAME)
            ]
        );
        // The root reads as the quad table…
        assert_eq!(file.row_count(), 5);
        assert_eq!(file.dtype(), &dtype);

        // …while the dictionary child scans independently.
        let typed = root.as_::<RdfStoreLayoutVTable>();
        let (descriptor, child) = store_component(typed, DICT_COMPONENT_NAME)
            .unwrap()
            .unwrap();
        assert_eq!(descriptor.role, StoreComponentRole::Dictionary);
        assert_eq!(child.row_count(), 3);
        assert_eq!(child.dtype(), &dict_dtype);
        assert!(
            subtree_bytes(&child, file.footer().segment_map()).unwrap() > 0,
            "dict child must own segment bytes"
        );

        let reader = child
            .new_reader(
                DICT_COMPONENT_NAME.into(),
                file.segment_source(),
                file.session(),
                &Default::default(),
            )
            .unwrap();
        let terms =
            vortex_layout::scan::scan_builder::ScanBuilder::new(file.session().clone(), reader)
                .into_array_stream()
                .unwrap()
                .read_all()
                .await
                .unwrap();
        assert_eq!(terms.len(), 3);
        assert_eq!(terms.dtype(), &dict_dtype);
    }
}

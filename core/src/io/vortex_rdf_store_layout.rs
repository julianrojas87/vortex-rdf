// SPDX-License-Identifier: Apache-2.0

//! Native Vortex layout root for an immutable RDF store generation.
//!
//! This module intentionally starts with a transparent `quad-source` child.
//! Later vertical slices add auxiliary index children and optional immutable
//! change-set children without changing the logical scan result of the root.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join;
use futures::{StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};
use vortex_array::dtype::DType;
use vortex_array::stream::{ArrayStream, ArrayStreamAdapter, ArrayStreamExt, SendableArrayStream};
use vortex_array::{ArrayContext, ArrayRef, RawMetadata};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex_flatbuffers::{FlatBuffer, WriteFlatBufferExt};
use vortex_io::VortexWrite;
use vortex_layout::LayoutStrategy;
use vortex_layout::segments::{SegmentSinkRef, SegmentSource};
use vortex_layout::sequence::{
    SendableSequentialStream, SequencePointer, SequentialArrayStreamExt,
};
use vortex_layout::session::LayoutSessionExt;
use vortex_layout::{
    Layout, LayoutChildType, LayoutDeserializeArgs, LayoutEncoding, LayoutId, LayoutParts,
    LayoutReaderContext, LayoutReaderRef, LayoutRef, VTable, layout_children,
};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

// VORTEX_RDF_NATIVE_STORE_LAYOUT_SKELETON_V1
pub const VORTEX_RDF_STORE_LAYOUT_ID: &str = "vortex.rdf.store.v1";
const QUAD_SOURCE_CHILD: usize = 0;

/// Immutable publication policy for RDF store generations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UpdatePolicy {
    /// Each build publishes one complete immutable generation.
    #[default]
    Snapshot,
    /// A future generation may include immutable additions and deletions.
    ImmutableChangeSets,
}

/// Logical organization selected for the quad source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuadLayoutKind {
    /// One ID-based SPOG table.
    Spog,
    /// One logical partition per predicate.
    PredicatePartitioned,
    /// Reserved extension point for validity-aware quads.
    Temporal {
        valid_from: Arc<str>,
        valid_to: Arc<str>,
    },
    /// Application-defined quad organization.
    Custom(Arc<str>),
}

/// Stable, versioned identity of an auxiliary index implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDescriptor {
    pub name: Arc<str>,
    pub implementation: Arc<str>,
    pub version: u32,
    pub required: bool,
}

impl IndexDescriptor {
    pub fn validate(&self) -> VortexResult<()> {
        if self.name.is_empty() {
            vortex_bail!("RDF store index name must not be empty");
        }
        if self.implementation.is_empty() {
            vortex_bail!("RDF store index implementation must not be empty");
        }
        if self.version == 0 {
            vortex_bail!("RDF store index version must be positive");
        }
        Ok(())
    }
}

/// Build-time choice independent from the persisted quad layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreBuildStrategy {
    pub quad_layout: QuadLayoutKind,
    pub indexes: Arc<[IndexDescriptor]>,
    pub update_policy: UpdatePolicy,
}

impl StoreBuildStrategy {
    pub fn validate(&self) -> VortexResult<()> {
        let mut names = std::collections::BTreeSet::new();
        for index in self.indexes.iter() {
            index.validate()?;
            if !names.insert(index.name.as_ref()) {
                vortex_bail!("duplicate RDF store index name: {}", index.name);
            }
        }
        Ok(())
    }
}

// VORTEX_RDF_EXTENSIBLE_STORE_COMPONENTS_V1
const STORE_METADATA_VERSION: u32 = 1;

/// Persisted role of a native child layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StoreComponentRole {
    Dictionary,
    Index,
    ChangeSet,
    Other,
}

/// Stable descriptor for one auxiliary native layout child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreComponentDescriptor {
    pub name: Arc<str>,
    pub role: StoreComponentRole,
    pub implementation: Arc<str>,
    pub version: u32,
    pub required: bool,
    pub dtype: DType,
}

impl StoreComponentDescriptor {
    pub fn validate(&self) -> VortexResult<()> {
        if self.name.is_empty() {
            vortex_bail!("store component name must not be empty");
        }
        if self.name.as_ref() == "quad-source" {
            vortex_bail!("quad-source is reserved for the transparent root child");
        }
        if self.implementation.is_empty() {
            vortex_bail!("store component implementation must not be empty");
        }
        if self.version == 0 {
            vortex_bail!("store component version must be positive");
        }
        Ok(())
    }
}

/// One descriptor paired with its physical native layout.
#[derive(Clone)]
pub struct StoreComponent {
    pub descriptor: StoreComponentDescriptor,
    pub layout: LayoutRef,
}

impl StoreComponent {
    pub fn new(descriptor: StoreComponentDescriptor, layout: LayoutRef) -> VortexResult<Self> {
        descriptor.validate()?;
        vortex_ensure_eq!(
            &descriptor.dtype,
            layout.dtype(),
            "store component descriptor dtype does not match child layout"
        );
        Ok(Self { descriptor, layout })
    }
}

#[derive(Clone, Debug)]
pub struct VortexRdfStoreData {
    components: Arc<[StoreComponentDescriptor]>,
}

#[derive(Serialize)]
struct StoreMetadataWireRef<'a> {
    version: u32,
    components: Vec<StoreComponentWireRef<'a>>,
}

#[derive(Serialize)]
struct StoreComponentWireRef<'a> {
    name: &'a str,
    role: StoreComponentRole,
    implementation: &'a str,
    version: u32,
    required: bool,
    dtype: Vec<u8>,
}

#[derive(Deserialize)]
struct StoreMetadataWire {
    version: u32,
    components: Vec<StoreComponentWire>,
}

#[derive(Deserialize)]
struct StoreComponentWire {
    name: String,
    role: StoreComponentRole,
    implementation: String,
    version: u32,
    required: bool,
    dtype: Vec<u8>,
}

fn encode_store_metadata(components: &[StoreComponentDescriptor]) -> VortexResult<Vec<u8>> {
    let wire = StoreMetadataWireRef {
        version: STORE_METADATA_VERSION,
        components: components
            .iter()
            .map(|component| {
                Ok(StoreComponentWireRef {
                    name: component.name.as_ref(),
                    role: component.role,
                    implementation: component.implementation.as_ref(),
                    version: component.version,
                    required: component.required,
                    dtype: component.dtype.write_flatbuffer_bytes()?.as_ref().to_vec(),
                })
            })
            .collect::<VortexResult<Vec<_>>>()?,
    };
    serde_json::to_vec(&wire).map_err(|error| vortex_error::vortex_err!("{}", error))
}

fn decode_store_metadata(
    bytes: &[u8],
    session: &VortexSession,
) -> VortexResult<Vec<StoreComponentDescriptor>> {
    // Compatibility with the initial QuadSource-only v10 proof artifacts.
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let wire: StoreMetadataWire =
        serde_json::from_slice(bytes).map_err(|error| vortex_error::vortex_err!("{}", error))?;
    vortex_ensure_eq!(
        wire.version,
        STORE_METADATA_VERSION,
        "unsupported Vortex-RDF store metadata version"
    );
    let mut names = std::collections::BTreeSet::new();
    wire.components
        .into_iter()
        .map(|component| {
            let dtype = DType::from_flatbuffer(
                FlatBuffer::align_from(vortex_buffer::ByteBuffer::from(component.dtype)),
                session,
            )?;
            let descriptor = StoreComponentDescriptor {
                name: component.name.into(),
                role: component.role,
                implementation: component.implementation.into(),
                version: component.version,
                required: component.required,
                dtype,
            };
            descriptor.validate()?;
            if !names.insert(descriptor.name.to_string()) {
                vortex_bail!("duplicate store component name: {}", descriptor.name);
            }
            Ok(descriptor)
        })
        .collect()
}

/// VTable for the native RDF store root.
#[derive(Clone, Debug)]
pub struct VortexRdfStore;

/// Typed native RDF store layout.
pub type VortexRdfStoreLayout = Layout<VortexRdfStore>;

impl VTable for VortexRdfStore {
    type LayoutData = VortexRdfStoreData;
    type Metadata = RawMetadata;

    fn id(&self) -> LayoutId {
        static ID: CachedId = CachedId::new(VORTEX_RDF_STORE_LAYOUT_ID);
        *ID
    }

    fn metadata(layout: &Layout<Self>) -> Self::Metadata {
        RawMetadata(
            encode_store_metadata(&layout.data().components)
                .expect("validated Vortex-RDF store metadata must serialize"),
        )
    }

    fn deserialize(
        &self,
        args: &LayoutDeserializeArgs<'_>,
        metadata: &Vec<u8>,
    ) -> VortexResult<Self::LayoutData> {
        let components = decode_store_metadata(metadata, args.session)?;
        vortex_ensure_eq!(
            args.children.nchildren(),
            1 + components.len(),
            "VortexRdfStoreLayout child count does not match metadata"
        );
        let quads = args.children.child(QUAD_SOURCE_CHILD, args.dtype)?;
        vortex_ensure_eq!(
            quads.row_count(),
            args.row_count,
            "quad-source row count must match the RDF store root"
        );
        for (component_index, component) in components.iter().enumerate() {
            let child = args.children.child(component_index + 1, &component.dtype)?;
            vortex_ensure_eq!(
                child.dtype(),
                &component.dtype,
                "auxiliary component dtype does not match metadata"
            );
        }
        Ok(VortexRdfStoreData {
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
                .map(|component| component.dtype.clone())
                .ok_or_else(|| {
                    vortex_error::vortex_err!("invalid VortexRdfStoreLayout child index: {idx}")
                }),
        }
    }

    fn child_type(layout: &Layout<Self>, idx: usize) -> LayoutChildType {
        match idx {
            QUAD_SOURCE_CHILD => LayoutChildType::Transparent("quad-source".into()),
            _ => layout
                .data()
                .components
                .get(idx - 1)
                .map(|component| LayoutChildType::Auxiliary(component.name.clone()))
                .unwrap_or_else(|| panic!("invalid VortexRdfStoreLayout child index: {idx}")),
        }
    }

    fn new_reader(
        layout: &Layout<Self>,
        name: Arc<str>,
        segment_source: Arc<dyn SegmentSource>,
        session: &VortexSession,
        ctx: &LayoutReaderContext,
    ) -> VortexResult<LayoutReaderRef> {
        // The root's logical scan is the QuadSource. Auxiliary indexes and
        // change sets added later remain independently addressable children.
        layout
            .child(QUAD_SOURCE_CHILD)?
            .new_reader(name, segment_source, session, ctx)
    }
}

// VORTEX_RDF_NATIVE_STORE_QUAD_SOURCE_STRATEGY_V1
// VORTEX_RDF_STREAMING_NATIVE_COMPONENTS_V1
/// Replayable producer for one independently typed native store component.
/// Production implementations should expose bounded streams backed by runs,
/// temporary files, or generated batches rather than one complete ArrayRef.
pub trait NativeComponentSource: Send + Sync + 'static {
    fn dtype(&self) -> &DType;
    fn open(&self) -> VortexResult<SendableArrayStream>;
    fn buffered_bytes(&self) -> u64 {
        0
    }
}

/// Small replayable source for tests and genuinely small components.
/// Large dictionaries and indexes should use run-backed sources.
#[derive(Clone)]
pub struct ReplayableArraySource {
    dtype: DType,
    chunks: Arc<[ArrayRef]>,
    retained_bytes: u64,
}

impl ReplayableArraySource {
    pub fn try_new(chunks: Vec<ArrayRef>) -> VortexResult<Self> {
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
        let retained_bytes = chunks.iter().map(|chunk| chunk.nbytes()).sum();
        Ok(Self {
            dtype,
            chunks: chunks.into(),
            retained_bytes,
        })
    }
}

impl NativeComponentSource for ReplayableArraySource {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<SendableArrayStream> {
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

#[derive(Clone)]
pub struct NativeComponentWrite {
    pub descriptor: StoreComponentDescriptor,
    pub source: Arc<dyn NativeComponentSource>,
    pub strategy: Arc<dyn LayoutStrategy>,
}

impl NativeComponentWrite {
    pub fn new(
        descriptor: StoreComponentDescriptor,
        source: Arc<dyn NativeComponentSource>,
        strategy: Arc<dyn LayoutStrategy>,
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

/// Streaming native RDF store writer over one shared Vortex SegmentSink.
///
/// FUTURE(Vortex multi-stream writer): replace this adapter with the upstream
/// native multi-stream/root-layout API when publicly available. Persisted
/// component descriptors and layout structure must remain unchanged.
///
/// FUTURE(performance): tune top-level concurrency together with downstream
/// compression concurrency from measured peak RSS and throughput.
#[derive(Clone)]
pub struct VortexRdfStoreLayoutStrategy {
    quad_source: Arc<dyn LayoutStrategy>,
    components: Arc<[NativeComponentWrite]>,
    max_concurrent_components: usize,
}

impl VortexRdfStoreLayoutStrategy {
    pub fn new(quad_source: Arc<dyn LayoutStrategy>) -> Self {
        Self {
            quad_source,
            components: Arc::from([]),
            max_concurrent_components: 2,
        }
    }

    pub fn with_components(mut self, components: Vec<NativeComponentWrite>) -> VortexResult<Self> {
        let mut names = std::collections::BTreeSet::new();
        for component in &components {
            component.descriptor.validate()?;
            vortex_ensure_eq!(&component.descriptor.dtype, component.source.dtype());
            if !names.insert(component.descriptor.name.as_ref()) {
                vortex_bail!(
                    "duplicate native component write name: {}",
                    component.descriptor.name
                );
            }
        }
        self.components = components.into();
        Ok(self)
    }

    pub fn with_max_concurrent_components(mut self, concurrency: usize) -> VortexResult<Self> {
        if concurrency == 0 {
            vortex_bail!("native component concurrency must be positive");
        }
        self.max_concurrent_components = concurrency;
        Ok(self)
    }
}

#[async_trait]
impl LayoutStrategy for VortexRdfStoreLayoutStrategy {
    async fn write_stream(
        &self,
        ctx: ArrayContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        mut eof: SequencePointer,
        session: &VortexSession,
    ) -> VortexResult<LayoutRef> {
        // The input stream already occupies the first sequence subtree. Reserve
        // a boundary for its internal writer, then ordered sibling subtrees for
        // every independently produced component.
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
        let concurrency = self.max_concurrent_components.min(jobs.len().max(1));
        let components_future = futures::stream::iter(jobs)
            .buffered(concurrency)
            .try_collect::<Vec<_>>();
        let (quad_source, components) = try_join(quad_future, components_future).await?;
        Ok(new_vortex_rdf_store_layout_with_components(quad_source, components)?.into_layout())
    }

    fn buffered_bytes(&self) -> u64 {
        self.quad_source.buffered_bytes()
            + self
                .components
                .iter()
                .map(|component| {
                    component.source.buffered_bytes() + component.strategy.buffered_bytes()
                })
                .sum::<u64>()
    }
}

/// Construct a native RDF store root around a quad-source layout.
pub fn new_vortex_rdf_store_layout(quad_source: LayoutRef) -> VortexRdfStoreLayout {
    new_vortex_rdf_store_layout_with_components(quad_source, Vec::new())
        .expect("empty component inventory is valid")
}

pub fn new_vortex_rdf_store_layout_with_components(
    quad_source: LayoutRef,
    components: Vec<StoreComponent>,
) -> VortexResult<VortexRdfStoreLayout> {
    let dtype = quad_source.dtype().clone();
    let row_count = quad_source.row_count();
    let mut names = std::collections::BTreeSet::new();
    let mut descriptors = Vec::with_capacity(components.len());
    let mut children = Vec::with_capacity(1 + components.len());
    children.push(quad_source);
    for component in components {
        component.descriptor.validate()?;
        vortex_ensure_eq!(
            &component.descriptor.dtype,
            component.layout.dtype(),
            "store component dtype does not match child layout"
        );
        if !names.insert(component.descriptor.name.to_string()) {
            vortex_bail!(
                "duplicate store component name: {}",
                component.descriptor.name
            );
        }
        descriptors.push(component.descriptor);
        children.push(component.layout);
    }
    Ok(LayoutParts::new(
        VortexRdfStore,
        dtype,
        row_count,
        Vec::new(),
        layout_children(children),
        VortexRdfStoreData {
            components: descriptors.into(),
        },
    )
    .into_typed())
}

pub fn vortex_rdf_store_component(
    layout: &VortexRdfStoreLayout,
    name: &str,
) -> VortexResult<Option<LayoutRef>> {
    let Some(index) = layout
        .data()
        .components
        .iter()
        .position(|component| component.name.as_ref() == name)
    else {
        return Ok(None);
    };
    layout.child(index + 1).map(Some)
}

pub fn vortex_rdf_store_components(layout: &VortexRdfStoreLayout) -> &[StoreComponentDescriptor] {
    &layout.data().components
}

/// Return the transparent quad-source child of a native RDF store root.
pub fn vortex_rdf_store_quad_source(layout: &VortexRdfStoreLayout) -> VortexResult<LayoutRef> {
    layout.child(QUAD_SOURCE_CHILD)
}

// VORTEX_RDF_NATIVE_STORE_QUAD_SOURCE_ROUNDTRIP_V1
/// Write a QuadSource-only v10 artifact with a native Vortex-RDF root.
pub async fn write_vortex_rdf_quad_source_v10<W, S>(
    session: &VortexSession,
    writer: W,
    stream: S,
    quad_source_strategy: Arc<dyn LayoutStrategy>,
) -> VortexResult<vortex_file::WriteSummary>
where
    W: VortexWrite + Unpin,
    S: ArrayStream + Send + 'static,
{
    register_vortex_rdf_store_layout(session);
    session
        .write_options()
        .with_strategy(Arc::new(VortexRdfStoreLayoutStrategy::new(
            quad_source_strategy,
        )))
        .write(writer, stream)
        .await
}

/// Validate the native root and delegated scan of an in-memory v10 artifact.
pub async fn validate_vortex_rdf_quad_source_v10(
    session: &VortexSession,
    bytes: impl Into<vortex_buffer::ByteBuffer>,
) -> VortexResult<ArrayRef> {
    register_vortex_rdf_store_layout(session);
    let file = session.open_options().open_buffer(bytes)?;
    let root = file.footer().layout();
    vortex_ensure_eq!(
        root.encoding_id().as_ref(),
        VORTEX_RDF_STORE_LAYOUT_ID,
        "v10 artifact root is not the Vortex-RDF store layout"
    );
    vortex_ensure_eq!(root.nchildren(), 1, "v10 root must contain one QuadSource");
    vortex_ensure_eq!(
        root.child_type(QUAD_SOURCE_CHILD).name().as_ref(),
        "quad-source",
        "v10 root has an unexpected QuadSource child name"
    );
    let quad_source = root.child(QUAD_SOURCE_CHILD)?;
    vortex_ensure_eq!(root.dtype(), quad_source.dtype());
    vortex_ensure_eq!(root.row_count(), quad_source.row_count());
    let rows = file.scan()?.into_array_stream()?.read_all().await?;
    vortex_ensure_eq!(rows.dtype(), root.dtype());
    vortex_ensure_eq!(rows.len() as u64, root.row_count());
    Ok(rows)
}

/// Register the custom layout before opening or writing a v10 artifact.
pub fn register_vortex_rdf_store_layout(session: &VortexSession) {
    static STORE_LAYOUT: VortexRdfStore = VortexRdfStore;
    session
        .layouts()
        .register((&STORE_LAYOUT as &dyn LayoutEncoding).into());
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_session::VortexSession;

    #[test]
    fn build_strategy_rejects_duplicate_index_names() {
        let index = IndexDescriptor {
            name: "object-ranges".into(),
            implementation: "exact-ranges".into(),
            version: 1,
            required: false,
        };
        let strategy = StoreBuildStrategy {
            quad_layout: QuadLayoutKind::Spog,
            indexes: vec![index.clone(), index].into(),
            update_policy: UpdatePolicy::Snapshot,
        };
        assert!(strategy.validate().is_err());
    }

    #[test]
    fn quad_source_strategy_wraps_any_layout_strategy() {
        use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;

        let strategy = VortexRdfStoreLayoutStrategy::new(Arc::new(FlatLayoutStrategy::default()));
        assert_eq!(strategy.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn streaming_native_component_uses_shared_sink_and_independent_schema() -> VortexResult<()>
    {
        use std::sync::Mutex;
        use vortex_array::IntoArray;
        use vortex_array::arrays::{StructArray, VarBinArray};
        use vortex_array::buffer::BufferHandle;
        use vortex_buffer::buffer;
        use vortex_buffer::{ByteBuffer, ByteBufferMut};
        use vortex_io::session::RuntimeSession;
        use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
        use vortex_layout::segments::{SegmentFuture, SegmentId, SegmentSink, SegmentSource};

        #[derive(Default)]
        struct LocalSegments {
            buffers: Mutex<Vec<ByteBuffer>>,
        }

        impl SegmentSource for LocalSegments {
            fn request(&self, id: SegmentId) -> SegmentFuture {
                use futures::FutureExt as _;
                let buffer = self
                    .buffers
                    .lock()
                    .expect("segment lock")
                    .get(*id as usize)
                    .cloned();
                async move {
                    buffer
                        .map(BufferHandle::new_host)
                        .ok_or_else(|| vortex_error::vortex_err!("segment not found"))
                }
                .boxed()
            }
        }

        #[async_trait]
        impl SegmentSink for LocalSegments {
            async fn write(
                &self,
                mut sequence_id: vortex_layout::sequence::SequenceId,
                buffers: Vec<ByteBuffer>,
            ) -> VortexResult<SegmentId> {
                sequence_id.collapse().await;
                let mut combined = ByteBufferMut::empty();
                for buffer in buffers {
                    combined.extend_from_slice(buffer.as_ref());
                }
                let mut segments = self.buffers.lock().expect("segment lock");
                let id = SegmentId::from(u32::try_from(segments.len())?);
                segments.push(combined.freeze());
                Ok(id)
            }
        }
        use vortex_layout::sequence::{SequenceId, SequentialArrayStreamExt};
        use vortex_layout::session::LayoutSession;

        let session = vortex_array::array_session()
            .with::<LayoutSession>()
            .with::<RuntimeSession>();
        vortex_file::register_default_encodings(&session);
        register_vortex_rdf_store_layout(&session);

        let quads = StructArray::from_fields(&[
            ("s", buffer![1u32, 2, 3].into_array()),
            ("p", buffer![10u32, 10, 11].into_array()),
            ("o", buffer![20u32, 21, 22].into_array()),
            ("g", buffer![0u32, 0, 1].into_array()),
        ])?
        .into_array();
        let dictionary = StructArray::from_fields(&[
            ("id", buffer![0u32, 1, 2, 3, 4, 5, 6, 7].into_array()),
            (
                "term",
                VarBinArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]).into_array(),
            ),
        ])?
        .into_array();

        let component = NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "dictionary.id-to-term".into(),
                role: StoreComponentRole::Dictionary,
                implementation: "native-id-row-v1".into(),
                version: 1,
                required: true,
                dtype: dictionary.dtype().clone(),
            },
            Arc::new(ReplayableArraySource::try_new(vec![dictionary.clone()])?),
            Arc::new(FlatLayoutStrategy::default()),
        )?;
        let strategy = VortexRdfStoreLayoutStrategy::new(Arc::new(FlatLayoutStrategy::default()))
            .with_components(vec![component])?
            .with_max_concurrent_components(1)?;
        let segments = Arc::new(LocalSegments::default());
        let (ptr, eof) = SequenceId::root().split();
        let layout = strategy
            .write_stream(
                ArrayContext::empty(),
                Arc::<LocalSegments>::clone(&segments),
                quads.to_array_stream().sequenced(ptr),
                eof,
                &session,
            )
            .await?;

        assert_eq!(layout.nchildren(), 2);
        assert_eq!(
            layout.child_names().collect::<Vec<_>>(),
            vec![
                Arc::<str>::from("quad-source"),
                Arc::<str>::from("dictionary.id-to-term"),
            ]
        );
        assert_eq!(layout.row_count(), 3);
        let dictionary_layout = layout.child(1)?;
        assert_eq!(dictionary_layout.row_count(), 8);
        assert_eq!(dictionary_layout.dtype(), dictionary.dtype());
        // Both children emitted physical segments through the same sink.
        assert!(segments.buffers.lock().expect("segment lock").len() >= 2);
        let typed = layout.as_::<VortexRdfStore>();
        assert!(vortex_rdf_store_component(&typed, "dictionary.id-to-term")?.is_some());
        assert!(vortex_rdf_store_component(&typed, "missing")?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn quad_source_v10_round_trips_native_root_and_scan() -> VortexResult<()> {
        use vortex_array::IntoArray;
        use vortex_array::arrays::StructArray;
        use vortex_array::stream::ArrayStreamExt;
        use vortex_buffer::{ByteBuffer, ByteBufferMut, buffer};
        use vortex_io::session::RuntimeSession;
        use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
        use vortex_layout::session::LayoutSession;

        let session = vortex_array::array_session()
            .with::<LayoutSession>()
            .with::<RuntimeSession>();
        vortex_file::register_default_encodings(&session);
        register_vortex_rdf_store_layout(&session);
        let quads = StructArray::from_fields(&[
            ("s", buffer![1u32, 2, 3].into_array()),
            ("p", buffer![10u32, 10, 11].into_array()),
            ("o", buffer![20u32, 21, 22].into_array()),
            ("g", buffer![0u32, 0, 1].into_array()),
        ])?
        .into_array();
        let expected_dtype = quads.dtype().clone();
        let mut bytes = ByteBufferMut::empty();
        let summary = write_vortex_rdf_quad_source_v10(
            &session,
            &mut bytes,
            quads.to_array_stream(),
            Arc::new(FlatLayoutStrategy::default()),
        )
        .await?;
        let root = summary.footer().layout();
        assert_eq!(root.encoding_id().as_ref(), VORTEX_RDF_STORE_LAYOUT_ID);
        assert_eq!(
            root.child_names().collect::<Vec<_>>(),
            vec![Arc::<str>::from("quad-source")]
        );
        assert_eq!(root.row_count(), 3);
        assert_eq!(root.dtype(), &expected_dtype);
        let tree = root.display_tree().to_string();
        assert!(tree.contains("vortex.rdf.store.v1"));
        assert!(tree.contains("quad-source:"));
        let rows = validate_vortex_rdf_quad_source_v10(&session, ByteBuffer::from(bytes)).await?;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.dtype(), &expected_dtype);
        Ok(())
    }

    #[test]
    fn store_component_metadata_round_trips_independent_dtypes() -> VortexResult<()> {
        use vortex_array::dtype::{Nullability, PType, StructFields};

        let session = vortex_array::array_session();
        let components = vec![StoreComponentDescriptor {
            name: "dictionary.id-to-term".into(),
            role: StoreComponentRole::Dictionary,
            implementation: "id-row-v1".into(),
            version: 1,
            required: true,
            dtype: DType::Struct(
                StructFields::new(
                    ["id", "term"].into(),
                    vec![
                        DType::Primitive(PType::U32, Nullability::NonNullable),
                        DType::Utf8(Nullability::NonNullable),
                    ],
                ),
                Nullability::NonNullable,
            ),
        }];
        let bytes = encode_store_metadata(&components)?;
        let decoded = decode_store_metadata(&bytes, &session)?;
        assert_eq!(decoded, components);
        Ok(())
    }

    #[test]
    fn store_component_metadata_rejects_duplicate_names() {
        let session = vortex_array::array_session();
        let json = br#"{"version":1,"components":[{"name":"x","role":"index","implementation":"a","version":1,"required":false,"dtype":{"Primitive":["u32",false]}},{"name":"x","role":"index","implementation":"b","version":1,"required":false,"dtype":{"Primitive":["u32",false]}}]}"#;
        assert!(decode_store_metadata(json, &session).is_err());
    }

    #[test]
    fn registration_uses_stable_layout_id() {
        let session = VortexSession::empty().with::<vortex_layout::session::LayoutSession>();
        register_vortex_rdf_store_layout(&session);
        let layout_id = <VortexRdfStore as VTable>::id(&VortexRdfStore);
        assert!(session.layouts().registry().find(&layout_id).is_some());
    }

    #[test]
    fn store_root_uses_stable_transparent_quad_source_name() {
        let child_type = LayoutChildType::Transparent("quad-source".into());
        assert_eq!(child_type.name().as_ref(), "quad-source");
        assert_eq!(child_type.row_offset(), Some(0));
    }
}

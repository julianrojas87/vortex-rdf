// SPDX-License-Identifier: Apache-2.0

//! Native Vortex layout root for an immutable RDF store generation.
//!
//! This module intentionally starts with a transparent `quad-source` child.
//! Later vertical slices add auxiliary index children and optional immutable
//! change-set children without changing the logical scan result of the root.

use std::sync::Arc;

use async_trait::async_trait;
use vortex_array::dtype::DType;
use vortex_array::stream::{ArrayStream, ArrayStreamExt};
use vortex_array::{ArrayContext, ArrayRef, EmptyMetadata};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex_io::VortexWrite;
use vortex_layout::LayoutStrategy;
use vortex_layout::segments::{SegmentSinkRef, SegmentSource};
use vortex_layout::sequence::{SendableSequentialStream, SequencePointer};
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

/// VTable for the native RDF store root.
#[derive(Clone, Debug)]
pub struct VortexRdfStore;

/// Typed native RDF store layout.
pub type VortexRdfStoreLayout = Layout<VortexRdfStore>;

impl VTable for VortexRdfStore {
    type LayoutData = ();
    type Metadata = EmptyMetadata;

    fn id(&self) -> LayoutId {
        static ID: CachedId = CachedId::new(VORTEX_RDF_STORE_LAYOUT_ID);
        *ID
    }

    fn metadata(_layout: &Layout<Self>) -> Self::Metadata {
        EmptyMetadata
    }

    fn deserialize(
        &self,
        args: &LayoutDeserializeArgs<'_>,
        _metadata: &EmptyMetadata,
    ) -> VortexResult<Self::LayoutData> {
        vortex_ensure_eq!(
            args.children.nchildren(),
            1,
            "VortexRdfStoreLayout v1 expects exactly one quad-source child"
        );
        let quads = args.children.child(QUAD_SOURCE_CHILD, args.dtype)?;
        vortex_ensure_eq!(
            quads.row_count(),
            args.row_count,
            "quad-source row count must match the RDF store root"
        );
        Ok(())
    }

    fn child_dtype(layout: &Layout<Self>, idx: usize) -> VortexResult<DType> {
        match idx {
            QUAD_SOURCE_CHILD => Ok(layout.dtype().clone()),
            _ => vortex_bail!("invalid VortexRdfStoreLayout child index: {idx}"),
        }
    }

    fn child_type(_layout: &Layout<Self>, idx: usize) -> LayoutChildType {
        match idx {
            QUAD_SOURCE_CHILD => LayoutChildType::Transparent("quad-source".into()),
            _ => panic!("invalid VortexRdfStoreLayout child index: {idx}"),
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
/// Writer-side strategy that preserves a configurable native quad layout
/// beneath the transparent Vortex-RDF store root.
#[derive(Clone)]
pub struct VortexRdfStoreLayoutStrategy {
    quad_source: Arc<dyn LayoutStrategy>,
}

impl VortexRdfStoreLayoutStrategy {
    pub fn new(quad_source: Arc<dyn LayoutStrategy>) -> Self {
        Self { quad_source }
    }
}

#[async_trait]
impl LayoutStrategy for VortexRdfStoreLayoutStrategy {
    async fn write_stream(
        &self,
        ctx: ArrayContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        eof: SequencePointer,
        session: &VortexSession,
    ) -> VortexResult<LayoutRef> {
        let quad_source = self
            .quad_source
            .write_stream(ctx, segment_sink, stream, eof, session)
            .await?;
        Ok(new_vortex_rdf_store_layout(quad_source).into_layout())
    }

    fn buffered_bytes(&self) -> u64 {
        self.quad_source.buffered_bytes()
    }
}

/// Construct a native RDF store root around a quad-source layout.
pub fn new_vortex_rdf_store_layout(quad_source: LayoutRef) -> VortexRdfStoreLayout {
    let dtype = quad_source.dtype().clone();
    let row_count = quad_source.row_count();
    LayoutParts::new(
        VortexRdfStore,
        dtype,
        row_count,
        Vec::new(),
        layout_children(vec![quad_source]),
        (),
    )
    .into_typed()
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

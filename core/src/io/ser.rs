#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::error::{Result, VortexRdfError};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_array::ArrayRef;

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::io::store_layout::{
    self, DICT_COMPONENT_NAME, NativeComponentWrite, ReplayableArraySource,
    StoreComponentDescriptor, StoreComponentRole, default_child_strategy,
};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::store::LayoutStrategy;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::store::term_dictionary::{self, TermDictionary};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_array::stream::ArrayStreamAdapter;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_io::VortexWrite;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use web_time::Instant;

#[cfg(feature = "file-io")]
use crate::error;
#[cfg(feature = "file-io")]
use crate::store::builders::{UnsortedStreamBuilder, VortexArrayBuilder};
#[cfg(feature = "file-io")]
use crate::store::{Indexes, RawQuad};
#[cfg(feature = "file-io")]
use futures::Stream;

/// The term dictionary as a native store component: the sorted term column,
/// one chunk per held FSST window, written verbatim through the pass-through
/// strategy as the root's required `dictionary` child (see
/// [`store_layout::dict_child_strategy`]).
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) fn dict_component(dict: &TermDictionary) -> Result<NativeComponentWrite> {
    let chunks = term_dictionary::dict_child_chunks(dict)?;
    let dtype = chunks[0].dtype().clone();
    NativeComponentWrite::new(
        StoreComponentDescriptor {
            name: DICT_COMPONENT_NAME.into(),
            role: StoreComponentRole::Dictionary,
            implementation: store_layout::DICT_IMPLEMENTATION.into(),
            version: 1,
            required: true,
            sorted: true,
            dtype,
        },
        Arc::new(ReplayableArraySource::try_new(chunks)?),
        store_layout::dict_child_strategy(),
    )
    .map_err(VortexRdfError::Vortex)
}

/// Serialize a store's split parts — the primary quad array, its in-memory
/// index components, and (for the Dictionary layout) the term dictionary —
/// as a native store file. Sortedness provenance is carried faithfully: the
/// root records whether `s` is globally sorted (off the primary's own
/// stamp), and each index child records its component's `sorted` flag.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) async fn serialize_parts<W: VortexWrite + Unpin + Send>(
    primary: ArrayRef,
    components: &[crate::store::indexes::IndexComponent],
    dict: Option<&TermDictionary>,
    mut writer: W,
) -> Result<()> {
    let start = Instant::now();

    let layout = LayoutStrategy::from_dtype(primary.dtype());
    if matches!(layout, LayoutStrategy::Dictionary) && dict.is_none() {
        return Err(VortexRdfError::Serialization(
            "a bare Dictionary-layout array cannot be serialized without its term \
             dictionary"
                .to_string(),
        ));
    }

    let mut writes = Vec::with_capacity(components.len() + 1);
    for component in components {
        writes.push(component_write(component)?);
    }
    if let Some(dict) = dict {
        writes.push(dict_component(dict)?);
    }

    let (quads_sorted, _) = crate::store::builders::row_space_sortedness(&primary);
    let dtype = primary.dtype().clone();
    let stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(futures::stream::once(async move { Ok(primary) })),
    );
    store_layout::write_store(
        &super::VORTEX_SESSION,
        &mut writer,
        stream,
        default_child_strategy(),
        quads_sorted,
        writes,
    )
    .await
    .map_err(VortexRdfError::Vortex)?;
    writer
        .shutdown()
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("Failed to shutdown writer: {}", e)))?;

    log::debug!(
        "[ser::serialize_parts] Vortex writing took {:?}",
        start.elapsed()
    );
    Ok(())
}

/// An in-memory [`IndexComponent`] as a replayable native child write, its
/// descriptor carrying the component's own sortedness provenance.
///
/// [`IndexComponent`]: crate::store::indexes::IndexComponent
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
fn component_write(
    component: &crate::store::indexes::IndexComponent,
) -> Result<NativeComponentWrite> {
    let array = component.as_array();
    NativeComponentWrite::new(
        StoreComponentDescriptor {
            name: component.name.into(),
            role: StoreComponentRole::Index,
            implementation: component.implementation.into(),
            version: 1,
            required: false,
            sorted: component.sorted,
            dtype: array.dtype().clone(),
        },
        Arc::new(ReplayableArraySource::try_new(vec![array]).map_err(VortexRdfError::Vortex)?),
        default_child_strategy(),
    )
    .map_err(VortexRdfError::Vortex)
}

/// Stream quads directly into a native store file as compressed chunks.
///
/// The builder's [`VortexArrayBuilder::build_vortex_stream`] produces
/// row-space chunks lazily; the splitter tees each one into the quad child's
/// stream and the index children's channels, and the layout writer
/// compresses all children concurrently through one segment sink. For
/// streaming-capable builders WITHOUT index children, peak memory is bounded
/// by the chunk size instead of the dataset size. With index children the
/// bound is looser: the sequenced segment sink assigns the quad subtree's
/// segment ids ahead of every auxiliary child's, so a component's compressed
/// segments accumulate in the sink until the quad table finishes writing —
/// peak memory then includes the in-flight components' compressed size (far
/// below the raw dataset, but not O(chunk)). The dictionary is complete
/// before any chunk flows, and becomes the required `dictionary` child.
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_writer_with_builder<B, S, W>(
    quads: S,
    mut writer: W,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    B: VortexArrayBuilder,
    S: Stream<Item = error::Result<RawQuad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    let start = Instant::now();

    let built = B::build_vortex_stream(Box::new(quads), layout, indexes).await?;
    // Builders that stream components natively hand them over here; row-space
    // builders leave the split to the tee below (a no-op on primary dtypes).
    let split = crate::store::indexes::tee::split_row_space(
        built.dtype,
        built.chunks,
        built.components_sorted,
    )?;
    let mut components = split.components;
    components.extend(built.components);
    if let Some(dict) = &built.dict {
        components.push(dict_component(dict)?);
    }

    store_layout::write_store(
        &super::VORTEX_SESSION,
        &mut writer,
        ArrayStreamAdapter::new(split.quad_dtype, split.quad_chunks),
        default_child_strategy(),
        built.quads_sorted,
        components,
    )
    .await
    .map_err(VortexRdfError::Vortex)?;

    writer
        .shutdown()
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("Failed to shutdown writer: {}", e)))?;

    log::debug!(
        "[ser::quads_stream_to_vortex_writer_with_builder] Streaming write took {:?}",
        start.elapsed()
    );
    Ok(())
}

/// Serialize a quad stream to a native store file at `path` — the path-based
/// convenience over [`quads_stream_to_vortex_writer_with_builder`].
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_file_with_builder<B, S>(
    quads: S,
    path: &std::path::Path,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    B: VortexArrayBuilder,
    S: Stream<Item = error::Result<RawQuad>> + Unpin + Send + 'static,
{
    let writer = tokio::fs::File::create(path)
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("create {:?}: {}", path, e)))?;
    quads_stream_to_vortex_writer_with_builder::<B, _, _>(quads, writer, layout, indexes).await
}

/// Serialize a stream of quads directly to a Vortex file writer using the
/// default configuration (UnsortedStream builder, Default layout, no indexes).
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_writer<S, W>(quads: S, writer: W) -> error::Result<()>
where
    S: Stream<Item = error::Result<RawQuad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    quads_stream_to_vortex_writer_with_builder::<UnsortedStreamBuilder, _, _>(
        quads,
        writer,
        LayoutStrategy::Default,
        Vec::new(),
    )
    .await
}

/// Serialize a stream of quads to an in-memory Vortex file byte buffer.
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex<S>(quads: S) -> error::Result<Vec<u8>>
where
    S: Stream<Item = error::Result<RawQuad>> + Unpin + Send + 'static,
{
    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer(quads, &mut buffer).await?;
    Ok(buffer)
}

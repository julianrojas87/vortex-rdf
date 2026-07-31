#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::error::{Result, VortexRdfError};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_array::ArrayRef;

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::io::embedded::{DICT_SEGMENT_KEY, MANIFEST_SEGMENT_KEY, Manifest};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::store::LayoutStrategy;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::store::indexes::detect_indexes;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use crate::store::layouts::term_dictionary::{self, TermDictionary};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use futures::stream;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_array::stream::ArrayStreamAdapter;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_buffer::ByteBuffer;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_file::{VortexWriteOptions, WriteOptionsSessionExt};
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

/// Write one array to `writer` as a complete Vortex file with the given
/// options — the shared tail of every serialization path. Component blobs
/// (the embedded dictionary) pass plain options; store files pass options
/// carrying the manifest (and dictionary) metadata segments.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
async fn write_bare_array<W: VortexWrite + Unpin + Send>(
    options: VortexWriteOptions,
    vortex_array: ArrayRef,
    mut writer: W,
) -> Result<()> {
    let dtype = vortex_array.dtype().clone();
    let vortex_stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(stream::once(async move { Ok(vortex_array) })),
    );

    let _summary = options
        .write(&mut writer, vortex_stream)
        .await
        .map_err(VortexRdfError::Vortex)?;

    writer
        .shutdown()
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("Failed to shutdown writer: {}", e)))?;
    Ok(())
}

/// The embedded dictionary blob: the sorted term column serialized as its own
/// complete Vortex file (zone maps intact, terms FSST as held), ready to ride
/// in the quads file's dictionary metadata segment.
///
/// Memoized on the dictionary: it is frozen, so the nested file write runs
/// once per dictionary and every later serialization of the same store (the
/// wasm bindings' repeated `toBytes`) reuses the buffer.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) async fn dict_blob_bytes(dict: &TermDictionary) -> Result<ByteBuffer> {
    if let Some(blob) = dict.blob_cache().get() {
        return Ok(blob.clone());
    }
    let array = term_dictionary::embedded_dict_array(dict)?;
    let mut bytes = Vec::new();
    write_bare_array(super::VORTEX_SESSION.write_options(), array, &mut bytes).await?;
    // A metadata segment's footer locator holds a u32 length. Lifting the cap
    // means sharding the blob across several segments (up to 16 are allowed)
    // behind a multi-range bounded reader — unimplemented until a real
    // dataset needs it.
    if bytes.len() > u32::MAX as usize {
        return Err(VortexRdfError::Serialization(format!(
            "term dictionary blob is {} bytes, over the 4 GiB metadata-segment cap; \
             sharding across multiple segments is not implemented yet",
            bytes.len()
        )));
    }
    // On a concurrent race the first writer wins; the contents are identical.
    Ok(dict
        .blob_cache()
        .get_or_init(|| ByteBuffer::from(bytes))
        .clone())
}

/// Serialize an already-materialized array as a self-describing store file:
/// the manifest (and, for the Dictionary layout, the term-dictionary blob)
/// ride as user metadata segments; the quad columns stay bare.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) async fn serialize_with_dictionary<W: VortexWrite + Unpin + Send>(
    vortex_array: ArrayRef,
    dict: Option<&TermDictionary>,
    writer: W,
) -> Result<()> {
    let start = Instant::now();

    let layout = LayoutStrategy::from_dtype(vortex_array.dtype());
    if matches!(layout, LayoutStrategy::Dictionary) && dict.is_none() {
        return Err(VortexRdfError::Serialization(
            "a bare Dictionary-layout array cannot be serialized without its term \
             dictionary"
                .to_string(),
        ));
    }
    let indexes = detect_indexes(vortex_array.dtype());
    let manifest = Manifest::for_store(layout, &indexes, dict.map(|d| d.len() as u64));
    let mut options = super::VORTEX_SESSION
        .write_options()
        .with_metadata_segment(MANIFEST_SEGMENT_KEY, ByteBuffer::from(manifest.to_json()?));
    if let Some(dict) = dict {
        options = options.with_metadata_segment(DICT_SEGMENT_KEY, dict_blob_bytes(dict).await?);
    }
    write_bare_array(options, vortex_array, writer).await?;

    log::debug!(
        "[ser::serialize_with_dictionary] Vortex writing took {:?}",
        start.elapsed()
    );
    Ok(())
}

/// Serialize an already-materialized, dictionary-less Vortex array to a
/// Vortex file writer, manifest included.
///
/// Errors on a Dictionary-layout array (bare codes are not self-describing) —
/// serialize those through their store, which carries the dictionary. Prefer
/// [`quads_stream_to_vortex_writer_with_builder`] when serializing from a
/// quad stream: it feeds chunks to the writer as they are built instead of
/// requiring the whole array up front.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub async fn serialize<W: VortexWrite + Unpin + Send>(
    vortex_array: ArrayRef,
    writer: W,
) -> Result<()> {
    serialize_with_dictionary(vortex_array, None, writer).await
}

/// Stream quads directly into a Vortex file writer as compressed chunks.
///
/// The builder's [`VortexArrayBuilder::build_vortex_stream`] produces chunks
/// lazily; the Vortex writer consumes, compresses, and flushes each chunk as
/// it arrives. For streaming-capable builders (e.g. `UnsortedStreamBuilder`)
/// peak memory is bounded by the chunk size instead of the dataset size. The
/// manifest — and, under the Dictionary layout, the term-dictionary blob —
/// ride as metadata segments, so the file is self-describing while the quad
/// columns stay bare.
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

    let manifest_indexes = indexes.clone();
    let built = B::build_vortex_stream(Box::new(quads), layout, indexes).await?;
    // The dictionary is complete before any chunk is written, so both
    // metadata segments are attached up front.
    let manifest = Manifest::for_store(
        layout,
        &manifest_indexes,
        built.dict.as_ref().map(|d| d.len() as u64),
    );
    let mut options = super::VORTEX_SESSION
        .write_options()
        .with_metadata_segment(MANIFEST_SEGMENT_KEY, ByteBuffer::from(manifest.to_json()?));
    if let Some(dict) = &built.dict {
        options = options.with_metadata_segment(DICT_SEGMENT_KEY, dict_blob_bytes(dict).await?);
    }
    let vortex_stream = ArrayStreamAdapter::new(built.dtype, built.chunks);

    let _summary = options
        .write(&mut writer, vortex_stream)
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

/// Serialize a quad stream to a Vortex file at `path` — the path-based
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

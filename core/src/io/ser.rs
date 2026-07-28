use crate::error::{Result, VortexRdfError};

use vortex_array::ArrayRef;
use vortex_ipc::iterator::ArrayIteratorIPC;

#[cfg(feature = "file-io")]
use crate::error;
#[cfg(feature = "file-io")]
use crate::store::builders::{UnsortedStreamBuilder, VortexArrayBuilder};
#[cfg(feature = "file-io")]
use crate::store::{Indexes, LayoutStrategy, RawQuad};
#[cfg(feature = "file-io")]
use futures::{Stream, stream};
#[cfg(feature = "file-io")]
#[cfg(feature = "file-io")]
use vortex_array::expr::stats::Stat;
#[cfg(feature = "file-io")]
use vortex_array::stats::PRUNING_STATS;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamAdapter;
#[cfg(feature = "file-io")]
use vortex_file::WriteOptionsSessionExt;
#[cfg(feature = "file-io")]
use vortex_io::VortexWrite;
#[cfg(feature = "file-io")]
use web_time::Instant;

#[cfg(feature = "file-io")]
fn write_options_with_subject_stats() -> vortex_file::VortexWriteOptions {
    let mut stats = PRUNING_STATS.to_vec();
    if !stats.contains(&Stat::IsSorted) {
        stats.push(Stat::IsSorted);
    }
    super::VORTEX_SESSION
        .write_options()
        .with_file_statistics(stats)
}

/// Adapt a Dictionary-layout chunk stream to the padded serialized form: an
/// all-null term column on every quads chunk, and one trailing chunk holding
/// the sorted terms (see [`dictionary::pad_with_dictionary`] for the array
/// counterpart and the format's invariants).
///
/// [`dictionary::pad_with_dictionary`]: crate::store::layouts::dictionary::pad_with_dictionary
#[cfg(feature = "file-io")]
fn pad_chunk_stream(
    quad_dtype: vortex_array::dtype::DType,
    chunks: crate::store::builders::ChunkStream,
    dict: &crate::store::layouts::term_dictionary::TermDictionary,
) -> Result<(
    vortex_array::dtype::DType,
    crate::store::builders::ChunkStream,
)> {
    use crate::store::builders::into_vortex_error;
    use crate::store::layouts::dictionary as dict_layout;
    use futures::StreamExt as _;

    let padded = dict_layout::padded_dtype(&quad_dtype)?;
    // The tail chunk is built eagerly (the dictionary is complete before any
    // chunk is written); an empty dictionary appends nothing.
    let tail = if dict.len() == 0 {
        None
    } else {
        Some(dict_layout::dict_tail_chunk(&quad_dtype, dict).map_err(into_vortex_error))
    };
    let chunks: crate::store::builders::ChunkStream = chunks
        .map(|res| {
            res.and_then(|chunk| {
                dict_layout::append_null_term_column(&chunk).map_err(into_vortex_error)
            })
        })
        .chain(stream::iter(tail))
        .boxed();
    Ok((padded, chunks))
}

/// Serialize an already-materialized Vortex array to a Vortex file writer.
///
/// Prefer [`quads_stream_to_vortex_writer_with_builder`] when serializing from
/// a quad stream: it feeds chunks to the writer as they are built instead of
/// requiring the whole array up front.
#[cfg(feature = "file-io")]
pub async fn serialize<W: VortexWrite + Unpin + Send>(
    vortex_array: ArrayRef,
    mut writer: W,
) -> Result<()> {
    let start = Instant::now();

    let dtype = vortex_array.dtype().clone();
    let vortex_stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(stream::once(async move { Ok(vortex_array) })),
    );

    let _summary = write_options_with_subject_stats()
        .write(&mut writer, vortex_stream)
        .await
        .map_err(VortexRdfError::Vortex)?;

    writer
        .shutdown()
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("Failed to shutdown writer: {}", e)))?;

    log::debug!("[ser::serialize] Vortex writing took {:?}", start.elapsed());
    Ok(())
}

/// Serialize a Vortex array to IPC bytes.
pub fn write_array_to_ipc<W: std::io::Write>(vortex_array: ArrayRef, mut writer: W) -> Result<()> {
    let ipc_iter = vortex_array
        .to_array_iterator()
        .into_ipc(&super::VORTEX_LIGHT_SESSION)
        .map_err(VortexRdfError::Vortex)?;

    for msg_res in ipc_iter {
        let msg = msg_res.map_err(VortexRdfError::Vortex)?;
        writer
            .write_all(&msg)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }

    Ok(())
}

/// Stream quads directly into a Vortex file writer as compressed chunks.
///
/// The builder's [`VortexArrayBuilder::build_vortex_stream`] produces chunks
/// lazily; the Vortex writer consumes, compresses, and flushes each chunk as
/// it arrives. For streaming-capable builders (e.g. `UnsortedStreamBuilder`)
/// peak memory is bounded by the chunk size instead of the dataset size.
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
    // A Dictionary-layout stream carries its dictionary beside the chunks;
    // writing appends it as trailing dictionary rows — the padded form — so
    // the file stays a single self-describing artifact.
    let (dtype, chunks) = match built.dict {
        Some(dict) => pad_chunk_stream(built.dtype, built.chunks, &dict)?,
        None => (built.dtype, built.chunks),
    };
    let vortex_stream = ArrayStreamAdapter::new(dtype, chunks);

    let _summary = write_options_with_subject_stats()
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

/// Write a store's term dictionary as the sidecar file beside `quads_path`
/// (`data.vortex` → `data.dict.vortex`): a one-column `{_dict_term: utf8}`
/// file whose row `i` is the term with ID `i`, kept in the encoding the
/// dictionary is held in (FSST when compressed at the source).
///
/// The sidecar placement's write half: a quads file with bare code columns
/// decodes only through this companion (see
/// `VortexRdfStore::from_file`), so the two files must travel together.
#[cfg(feature = "file-io")]
pub async fn write_sidecar_dictionary(
    snapshot: &crate::store::DictSnapshot,
    quads_path: &std::path::Path,
) -> Result<std::path::PathBuf> {
    use crate::store::layouts::term_dictionary;
    let array = term_dictionary::sidecar_dict_array(&snapshot.0)?;
    let path = term_dictionary::sidecar_dict_path(quads_path);
    let file = tokio::fs::File::create(&path)
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("create {:?}: {}", path, e)))?;
    serialize(array, file).await?;
    Ok(path)
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

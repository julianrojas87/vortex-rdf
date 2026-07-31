use crate::error;
use crate::error::{Result, VortexRdfError};
use crate::store::VortexRdfStore;

use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
use std::io::Write;
use web_time::Instant;

use vortex_array::arrays::ChunkedArray;
use vortex_array::{ArrayRef, IntoArray};

#[cfg(feature = "file-io")]
use vortex_file::OpenOptionsSessionExt;

/// High-level function to deserialize Vortex-RDF data store into an RDF writer.
/// Pulls quads sequentially from the store and serializes them in the specified format (Turtle, N-Triples, etc.).
pub async fn deserialize<W: Write>(
    store: VortexRdfStore,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    let decode_start = Instant::now();
    // Retrieve the quad stream (either in-memory or lazy file-backed stream).
    let mut quads_stream = store.quads()?;
    log::debug!(
        "[deserialize] Quad stream setup took {:?}",
        decode_start.elapsed()
    );

    let write_start = Instant::now();
    // Construct the oxrdf serialization helper for streaming output.
    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    // Dynamically iterate over each quad and push it to the output writer.
    while let Some(quad_res) = quads_stream.next().await {
        let quad = quad_res?;
        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;
    }

    // Finalize the serialization output (e.g. closing syntax blocks).
    rdf_serializer
        .finish()
        .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;

    log::debug!(
        "[deserialize] Serialization/write loop took {:?}",
        write_start.elapsed()
    );

    Ok(())
}

/// Materialize a whole file by driving its scan's per-split futures inline.
///
/// `ScanBuilder::into_array_stream` spawns onto the session's runtime handle;
/// this drives `ScanBuilder::build`'s futures directly instead, so it needs no
/// handle at all — which is what lets buffer-backed files (whose segment reads
/// resolve synchronously) be read on wasm and in no-file-io builds.
pub(crate) async fn scan_all(file: &vortex_file::VortexFile) -> Result<ArrayRef> {
    let dtype = file.dtype().clone();
    let scan = file.scan().map_err(VortexRdfError::Vortex)?;
    let mut chunks = Vec::new();
    for task in scan.build().map_err(VortexRdfError::Vortex)? {
        if let Some(chunk) = task.await.map_err(VortexRdfError::Vortex)? {
            chunks.push(chunk);
        }
    }
    match chunks.len() {
        0 => Ok(ChunkedArray::try_new(vec![], dtype)
            .map_err(VortexRdfError::Vortex)?
            .into_array()),
        1 => Ok(chunks.pop().expect("length checked above")),
        _ => {
            let dtype = chunks[0].dtype().clone();
            Ok(ChunkedArray::try_new(chunks, dtype)
                .map_err(VortexRdfError::Vortex)?
                .into_array())
        }
    }
}

/// Open a Vortex file lazily — no data is read until the returned `VortexFile`
/// is scanned. This is the core entrypoint for our zero-copy, memory-efficient lazy store.
///
/// The layout reader is cached on the file handle: every scan and pruning
/// evaluation over the store shares one reader tree, so zone-map stats tables
/// are read and decoded once and per-expression pruning masks are reused across data access calls.
#[cfg(feature = "file-io")]
pub async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<vortex_file::VortexFile> {
    super::VORTEX_SESSION
        .open_options()
        .with_layout_reader_cache()
        .open_path(path)
        .await
        .map_err(VortexRdfError::from)
}

//! Exporting a store back to textual RDF: the counterpart of parsing, not of
//! the Vortex byte format (writing that is `ser`'s job, reading it
//! `native_file`'s).

use crate::error;
use crate::store::VortexRdfStore;

use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
use std::io::Write;
use web_time::Instant;

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

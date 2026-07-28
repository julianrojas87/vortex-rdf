//! A columnar RDF serialization format and queryable quad store, built on
//! [Vortex](https://docs.vortex.dev).
//!
//! Converts RDF quads (parsed from any format [`oxrdfio`] supports) into a
//! Vortex [`StructArray`](vortex_array::arrays::struct_::StructArray),
//! storable as a `.vortex` file or streamed over Vortex IPC, and queryable
//! in place through [`VortexRdfStore`] without decompressing or copying the
//! underlying data. See the [repository README](https://github.com/vortex-rdf/vortex-rdf)
//! for the full architecture: column layouts, secondary indexes, and
//! ingestion builders.
//!
//! # Example
//!
//! ```
//! use futures::{executor::block_on, stream};
//! use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
//! use vortex_rdf_core::store::RawQuad;
//! use vortex_rdf_core::{VortexRdfError, VortexRdfStore};
//!
//! block_on(async {
//!     let quad = Quad::new(
//!         NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap()),
//!         NamedNode::new("http://example.org/p").unwrap(),
//!         Term::Literal(Literal::new_simple_literal("hello")),
//!         GraphName::DefaultGraph,
//!     );
//!     // Builders consume `RawQuad` — terms already in the N-Triples form the
//!     // columns store. `parse_quads_from_reader` yields these directly.
//!     let quads = stream::iter(vec![Ok::<_, VortexRdfError>(RawQuad::from_quad(&quad))]);
//!
//!     // Build an in-memory Vortex array from the quad stream, then wrap it
//!     // in a queryable store.
//!     let array = VortexRdfStore::build_vortex_array(quads).await.unwrap();
//!     let store = VortexRdfStore::new(array).unwrap();
//!
//!     // Pattern matching narrows a view over the store without copying data.
//!     let p = NamedNode::new("http://example.org/p").unwrap();
//!     let matched = store
//!         .match_pattern(None, Some(&p), None, None)
//!         .await
//!         .unwrap();
//!     assert_eq!(matched.size().await.unwrap(), 1);
//! });
//! ```

pub mod common;
pub mod error;
pub mod io;
pub mod store;

pub use error::VortexRdfError;
pub use io::{array_from_ipc_bytes, array_from_ipc_reader, deserialize};
#[cfg(feature = "file-io")]
pub use io::{
    load_vortex_file_ref, quads_stream_to_vortex, quads_stream_to_vortex_writer,
    quads_stream_to_vortex_writer_with_builder, serialize,
};

pub use store::{
    BuilderStrategy, BuiltArray, BuiltStream, DictSnapshot, DictionaryPlacement,
    DictionaryQuadSink, IndexType, Indexes, LayoutStrategy, SortedInMemoryBuilder,
    SortedStreamBuilder, UnsortedStreamBuilder, VortexArrayBuilder, VortexRdfStore,
};

#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
use mimalloc::MiMalloc;
#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(test)]
mod tests;

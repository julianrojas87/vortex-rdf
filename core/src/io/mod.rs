//! The native container codec: the `vortex-rdf.store.v1` grammar
//! (`container`), the write driver over it (`ser`), and read-side file
//! access (`read`). Textual RDF conversion lives outside this module:
//! parsing in `common::terms`, export in `store::export` (re-exported at the
//! crate root as `export_rdf`); the opened-store runtime handle lives
//! store-side in `store::native_file`.

pub(crate) mod container;
pub(crate) mod read;
/// Every item in `ser` is write-side native-container machinery, compiled only
/// where a store can be written: natively behind `file-io`, and on wasm (whose
/// bindings exchange file bytes).
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) mod ser;

#[cfg(feature = "file-io")]
pub use ser::{quads_stream_to_vortex_file, quads_stream_to_vortex_writer};

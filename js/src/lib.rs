//! WebAssembly bindings for `vortex_rdf_core`: the RDF/JS-flavored
//! `VortexRdfStore` API plus the `rdf_to_vortex`/`vortex_to_rdf` conversion
//! entry points. The lazy RDF/JS read model (LazyQuad/LazyTerm + stream)
//! lives in js-snippets/lazy-rdf.js, copied verbatim into the generated pkg.

use std::io::Cursor;

use vortex_rdf_core::VortexRdfStore as CoreStore;
use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::io::{array_from_ipc_bytes, deserialize, write_array_to_ipc};
use wasm_bindgen::prelude::*;

mod ingest;
mod options;
mod store;
mod terms;

pub use store::{TermDict, VortexRdfStore};

use options::{build_array, parse_build_options, parse_format};

/// The hand-written TypeScript surface of this crate (the Rust items carry
/// `skip_typescript`), kept in its own file for real TS tooling.
#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = include_str!("api.d.ts");

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen(skip_typescript)]
pub async fn rdf_to_vortex(
    input: String,
    format_name: &str,
    options: JsValue,
) -> Result<Vec<u8>, JsValue> {
    let format = parse_format(format_name)?;
    let config = parse_build_options(options)?;
    let quads_stream = parse_quads_from_reader(Cursor::new(input), format);
    let built = build_array(quads_stream, config).await?;
    // Route through the store so the written bytes carry the term dictionary
    // (the padded form) and stay self-describing.
    let store = CoreStore::from_built(built).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let vortex_array = store
        .to_ipc_array()
        .await
        .map_err(|e| JsValue::from_str(&format!("Vortex read error: {}", e)))?;

    let mut buffer = Vec::new();
    write_array_to_ipc(vortex_array, &mut buffer)
        .map_err(|e| JsValue::from_str(&format!("Vortex serialization error: {}", e)))?;
    Ok(buffer)
}

#[wasm_bindgen(skip_typescript)]
pub async fn vortex_to_rdf(vortex_bytes: &[u8], format_name: &str) -> Result<String, JsValue> {
    let format = parse_format(format_name)?;
    let vortex_array = array_from_ipc_bytes(vortex_bytes)
        .map_err(|e| JsValue::from_str(&format!("Vortex read error: {}", e)))?;

    let store = CoreStore::new(vortex_array)
        .map_err(|e| JsValue::from_str(&format!("Store init error: {}", e)))?;

    let mut output_buffer = Vec::new();
    deserialize(store, &mut output_buffer, format)
        .await
        .map_err(|e| JsValue::from_str(&format!("Deserialize error: {}", e)))?;

    String::from_utf8(output_buffer).map_err(|e| JsValue::from_str(&format!("UTF-8 error: {}", e)))
}

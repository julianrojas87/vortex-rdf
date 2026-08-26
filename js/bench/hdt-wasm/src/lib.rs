//! Wasm build of the hdt crate for the JS comparative benchmark: hdt's own
//! `Hdt` class (`hdt::wasm`, compiled for `wasm32`). Read-only — HDT
//! construction uses OS threads and rayon, which trap on
//! `wasm32-unknown-unknown`, so the artifact is built natively and only opened
//! here. Linking the crate is what puts its `#[wasm_bindgen]` exports into
//! this cdylib.

extern crate hdt;

//! Wasm bindings over the [`hdt`] crate for the JS comparative benchmark.
//!
//! The hdt crate ships its own experimental `wasm32`-gated bindings but does
//! not publish the compiled artifact, so this crate is the bench's own build
//! of the same surface (a port of hdt's `src/wasm.rs`): open an HDT file from
//! bytes, query triple ids by pattern, translate ids to term strings. There is
//! deliberately no builder — HDT construction (`Hdt::read_nt`) uses OS threads
//! and rayon, which trap on `wasm32-unknown-unknown`, so the artifact is built
//! natively and this module only reads it.

use hdt::{Hdt, IdKind};
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct HdtStore {
    hdt: Hdt,
}

#[wasm_bindgen]
impl HdtStore {
    /// Parse an HDT file's bytes into a queryable store.
    #[wasm_bindgen(constructor)]
    pub fn new(data: Vec<u8>) -> Result<HdtStore, JsError> {
        std::panic::set_hook(Box::new(console_error_panic_hook::hook));
        let hdt = Hdt::read(Cursor::new(data)).map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self { hdt })
    }

    /// Total triples in the store — the harness's check that the pre-built
    /// artifact matches the dataset the other adapters generate in-process.
    pub fn num_triples(&self) -> u32 {
        self.hdt.triple_ids_with_pattern(None, None, None).count() as u32
    }

    /// Matching triples as a flat `Uint32Array` of ids `[s1,p1,o1, s2,p2,o2, …]`.
    /// Terms are in HDT dictionary spelling: IRIs bare, literals with quotes.
    pub fn triple_ids_with_pattern(
        &self,
        sp: Option<String>,
        pp: Option<String>,
        op: Option<String>,
    ) -> Box<[u32]> {
        self.hdt
            .triple_ids_with_pattern(sp.as_deref(), pp.as_deref(), op.as_deref())
            .flat_map(|[s, p, o]| [s as u32, p as u32, o as u32])
            .collect()
    }

    /// Translate a flat id array back to term strings, `[s1,p1,o1, …]`.
    /// Callers chunk large inputs: several million ids in one call is the
    /// documented OOM risk of this surface.
    pub fn ids_to_strings(&self, ids: &[u32]) -> Result<Vec<String>, JsError> {
        if ids.len() % 3 != 0 {
            return Err(JsError::new("input length must be a multiple of 3"));
        }
        let mut strings = Vec::with_capacity(ids.len());
        for (i, id) in ids.iter().enumerate() {
            strings.push(
                self.hdt
                    .dict
                    .id_to_string(*id as usize, IdKind::KINDS[i % 3])
                    .map_err(|e| JsError::new(&e.to_string()))?,
            );
        }
        Ok(strings)
    }
}

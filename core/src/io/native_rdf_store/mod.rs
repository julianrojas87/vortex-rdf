//! Transparent native Vortex-RDF store architecture.
//!
//! Configuration and persisted component identity are extracted first. Reader,
//! writer, matching, and reusable index builders follow in guarded patches.

pub mod component_names;
pub mod config;

pub use config::{NativeIndexProfile, NativeIndexSelection, NativeIndexSpec};

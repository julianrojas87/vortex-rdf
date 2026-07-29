//! Transparent native Vortex-RDF store architecture.
//!
//! Configuration and persisted component identity are extracted first. Reader,
//! writer, matching, and reusable index builders follow in guarded patches.

pub mod build_context;
pub mod component_names;
pub mod config;
pub(crate) mod exact_ranges;

pub use config::{NativeIndexProfile, NativeIndexSelection, NativeIndexSpec};

pub use build_context::NativeIndexBuildContext;

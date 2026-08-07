//! The store's query-execution tier: turning a view (a selection plus
//! residual constraints) into the concrete rows that match it.
//!
//! `typed_eq` is the in-memory half — typed residual-equality fast paths
//! over canonical columns; `file_scan` is the file-backed half — per-split
//! filter evaluation, statistics-only pruning envelopes, pushed-down filter
//! construction, and the translation of a [`RowSelection`] onto vortex's
//! scan knobs. The row-set algebra itself stays in
//! [`selection`](super::selection); what lives here is its execution against
//! a backend.
//!
//! [`RowSelection`]: super::selection::RowSelection

#[cfg(feature = "file-io")]
pub(crate) mod file_scan;
pub(crate) mod typed_eq;

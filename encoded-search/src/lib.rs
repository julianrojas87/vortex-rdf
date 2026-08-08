//! Bounds search and point access over compressed Vortex arrays without
//! decoding.
//!
//! A [`SortedProbe`] resolves an encoded array into a borrowed probe tree and
//! answers two-sided bounds queries and point reads directly against the
//! compressed representation. Resolution walks the encoding tree with typed
//! downcasts only; probing is slice reads, integer arithmetic, and single
//! bit-packed word extraction — no `ExecutionCtx`, no canonicalization.
//!
//! Supported encoding nodes: Primitive, Constant, Sequence, RunEnd, FoR,
//! BitPacked (with patches), Slice, and Chunked, composed arbitrarily.
//! Anything else — including nullable or non-unsigned-integer dtypes and
//! non-host buffers — declines, and the caller falls back to its generic
//! search path.

mod node;
mod patches;
mod resolve;

use vortex_array::ArrayRef;

/// A resolved, borrowed probe over one encoded (or canonical) array.
///
/// Bounds queries require the array to be sorted ascending — sortedness is a
/// caller contract, exactly like `slice::partition_point`; an unsorted array
/// yields an unspecified (but never panicking) bound. [`Self::value_at`] is
/// exact regardless of sort order.
pub struct SortedProbe<'a> {
    root: node::Node<'a>,
}

impl<'a> SortedProbe<'a> {
    /// Resolves a non-nullable unsigned-integer array whose encoding tree is
    /// drawn from the supported set; returns `None` otherwise.
    pub fn resolve(array: &'a ArrayRef) -> Option<Self> {
        if !array.is_host() {
            return None;
        }
        resolve::resolve_node(array).map(|root| Self { root })
    }

    /// Number of rows in the resolved array.
    pub fn len(&self) -> usize {
        self.root.len()
    }

    /// Whether the resolved array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Index of the first element `>= needle`.
    pub fn lower_bound(&self, needle: u64) -> usize {
        self.root.lower_bound(needle)
    }

    /// Index of the first element `> needle`.
    pub fn upper_bound(&self, needle: u64) -> usize {
        self.root.upper_bound(needle)
    }

    /// `(lower_bound, upper_bound)` — the half-open run of elements equal to
    /// `needle`.
    pub fn bounds(&self, needle: u64) -> (usize, usize) {
        (self.lower_bound(needle), self.upper_bound(needle))
    }

    /// Exact value at `index`, widened to `u64`.
    ///
    /// # Panics
    /// Panics if `index >= self.len()`.
    pub fn value_at(&self, index: usize) -> u64 {
        assert!(index < self.len(), "index {index} out of bounds");
        self.root.value_at(index)
    }

    /// Pre-order encoding kinds of the resolved tree, for tests and
    /// diagnostics.
    pub fn node_kinds(&self) -> Vec<NodeKind> {
        let mut kinds = Vec::new();
        self.root.collect_kinds(&mut kinds);
        kinds
    }
}

/// The closed set of probe-node shapes a [`SortedProbe`] resolves.
///
/// This is deliberately not a vortex type: vortex identifies encodings by an
/// open, registry-backed string id, while probing supports a fixed set of
/// node shapes (including [`NodeKind::Patches`], which is a component of
/// bit-packed arrays rather than an encoding of its own). A closed enum lets
/// tests match exhaustively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Primitive,
    Constant,
    Sequence,
    RunEnd,
    FoR,
    BitPacked,
    Patches,
    Slice,
    Chunked,
}

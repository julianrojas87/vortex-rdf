use std::path::PathBuf;
use std::sync::Arc;

use super::{NativeIndexSelection, NativeIndexSpec};

// VORTEX_RDF_SHARED_NATIVE_BUILD_CONTEXT_TERM_DIRECTORY_V1
/// Replayable, bounded-memory inputs shared by native component producers.
#[derive(Clone, Debug)]
pub struct NativeIndexBuildContext {
    spo_run_paths: Arc<[PathBuf]>,
    pub index_row_group_size: usize,
    pub selection: NativeIndexSelection,
}

impl NativeIndexBuildContext {
    pub fn new(
        spo_run_paths: Vec<PathBuf>,
        index_row_group_size: usize,
        selection: NativeIndexSelection,
    ) -> Self {
        Self {
            spo_run_paths: spo_run_paths.into(),
            index_row_group_size: index_row_group_size.max(1),
            selection,
        }
    }

    pub fn run_paths_for(&self, spec: NativeIndexSpec) -> Option<&[PathBuf]> {
        self.selection
            .contains(spec)
            .then_some(self.spo_run_paths.as_ref())
    }

    // VORTEX_RDF_PREPARE_NATIVE_PREDICATE_EXACT_RANGES_V2
    /// Shared SPO replay paths for a producer that has already been selected.
    /// Keeping this borrow-only prevents each directory/payload child from
    /// owning another path vector.
    pub fn selected_run_paths(&self, spec: NativeIndexSpec) -> Option<&Arc<[PathBuf]>> {
        self.selection.contains(spec).then_some(&self.spo_run_paths)
    }
}

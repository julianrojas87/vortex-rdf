//! Read-side access to native store files: opening them, requiring the
//! native root, materializing whole files or auxiliary children on any
//! target, and — with `file-io` — the [`NativeStoreFile`] runtime handle the
//! store's file-backed query paths drive. The wire format itself (layout
//! VTable, descriptors, write strategy) lives in
//! [`store_layout`](super::store_layout).

use std::future::Future;

use vortex_array::arrays::ChunkedArray;
use vortex_array::{ArrayRef, IntoArray};
use vortex_error::VortexResult;
#[cfg(feature = "file-io")]
use vortex_error::vortex_bail;
#[cfg(feature = "file-io")]
use vortex_file::OpenOptionsSessionExt;
#[cfg(feature = "file-io")]
use vortex_layout::LayoutReaderRef;

#[cfg(feature = "file-io")]
use super::store_layout::{
    RdfStoreLayoutVTable, STORE_LAYOUT_ID, StoreComponentDescriptor, is_native_file, subtree_bytes,
};
use crate::error::{Result, VortexRdfError};

/// Drive a scan's per-split futures inline and assemble the chunks into one
/// array — the shared tail of [`scan_all`] and [`scan_all_reader`].
async fn collect_scan<F>(dtype: vortex_array::dtype::DType, tasks: Vec<F>) -> Result<ArrayRef>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>>,
{
    let mut chunks = Vec::new();
    for task in tasks {
        if let Some(chunk) = task.await.map_err(VortexRdfError::Vortex)? {
            chunks.push(chunk);
        }
    }
    match chunks.len() {
        0 => Ok(ChunkedArray::try_new(vec![], dtype)
            .map_err(VortexRdfError::Vortex)?
            .into_array()),
        1 => Ok(chunks.pop().expect("length checked above")),
        _ => {
            let dtype = chunks[0].dtype().clone();
            Ok(ChunkedArray::try_new(chunks, dtype)
                .map_err(VortexRdfError::Vortex)?
                .into_array())
        }
    }
}

/// Materialize a whole file by driving its scan's per-split futures inline.
///
/// `ScanBuilder::into_array_stream` spawns onto the session's runtime handle;
/// this drives `ScanBuilder::build`'s futures directly instead, so it needs no
/// handle at all — which is what lets buffer-backed files (whose segment reads
/// resolve synchronously) be read on wasm and in no-file-io builds.
pub(crate) async fn scan_all(file: &vortex_file::VortexFile) -> Result<ArrayRef> {
    let dtype = file.dtype().clone();
    let scan = file.scan().map_err(VortexRdfError::Vortex)?;
    let tasks = scan.build().map_err(VortexRdfError::Vortex)?;
    collect_scan(dtype, tasks).await
}

/// [`scan_all`] over an arbitrary layout reader — how the native store root's
/// auxiliary children (dictionary, index copies) are materialized from a
/// buffer-backed file on every target, runtime handle included or not.
pub(crate) async fn scan_all_reader(reader: vortex_layout::LayoutReaderRef) -> Result<ArrayRef> {
    let dtype = reader.dtype().clone();
    let scan =
        vortex_layout::scan::scan_builder::ScanBuilder::new(super::VORTEX_SESSION.clone(), reader);
    let tasks = scan.build().map_err(VortexRdfError::Vortex)?;
    collect_scan(dtype, tasks).await
}

/// The actionable error for a file whose root is not the native store layout.
pub(crate) fn unsupported_file_error(file: &vortex_file::VortexFile) -> VortexRdfError {
    VortexRdfError::Deserialization(format!(
        "not a vortex-rdf store file: expected the {} root layout, found {}",
        super::store_layout::STORE_LAYOUT_ID,
        file.footer().layout().encoding_id()
    ))
}

/// Open a Vortex file lazily — no data is read until the returned `VortexFile`
/// is scanned. This is the core entrypoint for our zero-copy, memory-efficient lazy store.
///
/// The layout reader is cached on the file handle: every scan and pruning
/// evaluation over the store shares one reader tree, so zone-map stats tables
/// are read and decoded once and per-expression pruning masks are reused across data access calls.
#[cfg(feature = "file-io")]
pub async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<vortex_file::VortexFile> {
    super::VORTEX_SESSION
        .open_options()
        .with_layout_reader_cache()
        .open_path(path)
        .await
        .map_err(VortexRdfError::from)
}

// ── the opened native store file ────────────────────────────────────────────

/// An opened native store file: the [`vortex_file::VortexFile`] plus its
/// component inventory and per-component reader cache.
///
/// Derefs to the inner file, whose root reader delegates to the transparent
/// quad-source child — so scans, splits, row counts, and pruning all speak
/// quad coordinates, exactly like a plain quad table. Component readers are
/// built once and cached, so their zone-map stats decode once per store, not
/// per query (the auxiliary analogue of `with_layout_reader_cache`).
#[cfg(feature = "file-io")]
pub(crate) struct NativeStoreFile {
    file: vortex_file::VortexFile,
    components: Vec<StoreComponentDescriptor>,
    /// The root metadata's `quads_sorted` provenance (see `WireMetadata`),
    /// captured at open so read paths can restore the subject stamp on
    /// materialized rows without re-walking the layout.
    quads_sorted: bool,
    child_readers: Vec<std::sync::OnceLock<LayoutReaderRef>>,
    /// The quad table's natural split ranges, computed once — every
    /// counting/matching call iterates them, and deriving them walks the
    /// layout tree.
    splits: std::sync::OnceLock<std::sync::Arc<[std::ops::Range<u64>]>>,
    /// Statistics-only pruning envelopes keyed by filter shape (the
    /// expression's display form): the repeated-pattern workloads the
    /// bindings serve (e.g. rdflib joins) re-ask the same handful of filters,
    /// and each envelope costs a pruning evaluation over every zone. Bounded:
    /// cleared wholesale past [`PRUNING_MEMO_MAX`].
    pruning_envelopes:
        std::sync::Mutex<std::collections::HashMap<String, Option<std::ops::Range<u64>>>>,
}

/// Entry cap on [`NativeStoreFile::pruning_envelopes`] — sized for a query
/// workload's distinct filter shapes, not for arbitrary term churn.
#[cfg(feature = "file-io")]
const PRUNING_MEMO_MAX: usize = 512;

#[cfg(feature = "file-io")]
impl std::ops::Deref for NativeStoreFile {
    type Target = vortex_file::VortexFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

#[cfg(feature = "file-io")]
impl NativeStoreFile {
    /// Wrap an opened file, requiring the native store root.
    pub(crate) fn try_new(file: vortex_file::VortexFile) -> VortexResult<Self> {
        if !is_native_file(&file) {
            vortex_bail!(
                "expected the {STORE_LAYOUT_ID} root layout, found {}",
                file.footer().layout().encoding_id()
            );
        }
        let typed = file.footer().layout().as_::<RdfStoreLayoutVTable>();
        let components = super::store_layout::store_components(typed).to_vec();
        let quads_sorted = super::store_layout::quads_sorted(typed);
        let child_readers = components
            .iter()
            .map(|_| std::sync::OnceLock::new())
            .collect();
        Ok(Self {
            file,
            components,
            quads_sorted,
            child_readers,
            splits: std::sync::OnceLock::new(),
            pruning_envelopes: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// The quad table's natural splits, memoized. Shadows the inner file's
    /// `splits()` (which recomputes from the layout tree per call).
    pub(crate) fn splits(&self) -> VortexResult<std::sync::Arc<[std::ops::Range<u64>]>> {
        if let Some(splits) = self.splits.get() {
            return Ok(std::sync::Arc::clone(splits));
        }
        let computed: std::sync::Arc<[std::ops::Range<u64>]> = self.file.splits()?.into();
        let _ = self.splits.set(std::sync::Arc::clone(&computed));
        Ok(computed)
    }

    /// A memoized statistics-only pruning envelope for `key` (a filter's
    /// display form). The outer `Option` is a memo miss; the inner is the
    /// envelope itself, whose `None` means "nothing prunable".
    #[allow(clippy::option_option)]
    pub(crate) fn pruning_envelope(&self, key: &str) -> Option<Option<std::ops::Range<u64>>> {
        self.pruning_envelopes
            .lock()
            .expect("pruning memo lock")
            .get(key)
            .cloned()
    }

    /// Memoize a pruning envelope, clearing the memo wholesale at the cap
    /// (crude, but a workload with more distinct filter shapes than the cap
    /// was not going to hit anyway).
    pub(crate) fn memoize_pruning_envelope(
        &self,
        key: String,
        envelope: Option<std::ops::Range<u64>>,
    ) {
        let mut memo = self.pruning_envelopes.lock().expect("pruning memo lock");
        if memo.len() >= PRUNING_MEMO_MAX {
            memo.clear();
        }
        memo.insert(key, envelope);
    }

    /// Whether the file records its quad rows as globally `s`-sorted.
    pub(crate) fn quads_sorted(&self) -> bool {
        self.quads_sorted
    }

    /// The persisted component inventory (auxiliary children only).
    pub(crate) fn components(&self) -> &[StoreComponentDescriptor] {
        &self.components
    }

    /// A component's descriptor, child layout, and cached reader, by name.
    pub(crate) fn component_reader(
        &self,
        name: &str,
    ) -> VortexResult<Option<(&StoreComponentDescriptor, LayoutReaderRef)>> {
        let Some(index) = self.components.iter().position(|c| c.name == name) else {
            return Ok(None);
        };
        if self.child_readers[index].get().is_none() {
            let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
            let child = typed.child(index + 1)?;
            let reader = child.new_reader(
                self.components[index].name.as_str().into(),
                self.file.segment_source(),
                self.file.session(),
                &Default::default(),
            )?;
            let _ = self.child_readers[index].set(reader);
        }
        Ok(Some((
            &self.components[index],
            self.child_readers[index]
                .get()
                .expect("the reader was just initialized above")
                .clone(),
        )))
    }

    /// A component's on-disk byte size, by name — the residency-threshold
    /// input.
    pub(crate) fn component_bytes(&self, name: &str) -> VortexResult<Option<u64>> {
        let Some(index) = self.components.iter().position(|c| c.name == name) else {
            return Ok(None);
        };
        let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
        let child = typed.child(index + 1)?;
        subtree_bytes(&child, self.file.footer().segment_map()).map(Some)
    }
}

//! The embedded self-description of a serialized store: a JSON manifest and,
//! for the Dictionary layout, the term dictionary as a complete Vortex file —
//! both carried as user metadata segments of the quads file itself.
//!
//! The quad columns stay bare (nothing rides in their row space), so the
//! quads section is byte-identical to a dictionary-less file and pays no scan
//! or compression penalty for its self-description. The manifest follows the
//! cottas component-descriptor schema (name/role/implementation/version/
//! required), so a later migration to native layout children stays a
//! mechanical translation of the same descriptors.

use serde::{Deserialize, Serialize};

use crate::error::{Result, VortexRdfError};
use crate::store::indexes::detect_indexes;
use crate::store::{IndexType, LayoutStrategy};

use vortex_file::VortexFile;

#[cfg(feature = "file-io")]
use futures::future::BoxFuture;
#[cfg(feature = "file-io")]
use std::path::Path;
#[cfg(feature = "file-io")]
use std::sync::Arc;
#[cfg(feature = "file-io")]
use vortex_array::buffer::BufferHandle;
#[cfg(feature = "file-io")]
use vortex_buffer::{Alignment, ByteBuffer};
#[cfg(feature = "file-io")]
use vortex_error::{VortexResult, vortex_bail};
#[cfg(feature = "file-io")]
use vortex_file::OpenOptionsSessionExt;
#[cfg(feature = "file-io")]
use vortex_file::SegmentSpec;
#[cfg(feature = "file-io")]
use vortex_io::VortexReadAt;

/// Metadata-segment key of the JSON [`Manifest`]. Written on every file.
pub(crate) const MANIFEST_SEGMENT_KEY: &str = "vortex-rdf.manifest";

/// Metadata-segment key of the Dictionary layout's term-dictionary blob: a
/// complete Vortex file (one non-nullable `{_dict_term: utf8}` column, row i =
/// the term with code i, FSST-encoded as held). Present on every
/// Dictionary-layout file, even with an empty dictionary.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) const DICT_SEGMENT_KEY: &str = "vortex-rdf.dictionary";

const MANIFEST_FORMAT: &str = "vortex-rdf";
const MANIFEST_VERSION: u32 = 1;
/// The dictionary component's `implementation` descriptor value: sorted terms
/// (code = lexicographic rank), FSST-compressed where the writer chose to.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
const DICT_IMPLEMENTATION: &str = "sorted-terms-fsst-v1";

/// One embedded component of a serialized store, in the cottas descriptor
/// vocabulary. Today the only component is the term dictionary; the schema
/// leaves room for more (fence directories, independently-rewritable pieces)
/// without a format break.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ComponentDescriptor {
    pub(crate) name: String,
    pub(crate) role: String,
    pub(crate) implementation: String,
    pub(crate) version: u32,
    pub(crate) required: bool,
    pub(crate) row_count: u64,
    /// The metadata-segment key holding the component's bytes.
    pub(crate) segment: String,
}

/// The self-description stamp of a serialized store, validated on every open.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) format: String,
    pub(crate) version: u32,
    pub(crate) layout: String,
    pub(crate) components: Vec<ComponentDescriptor>,
    pub(crate) indexes: Vec<String>,
}

fn layout_slug(layout: LayoutStrategy) -> &'static str {
    match layout {
        LayoutStrategy::Default => "default",
        LayoutStrategy::TypedObject => "typed-object",
        LayoutStrategy::Dictionary => "dictionary",
    }
}

fn index_slugs(indexes: &[IndexType]) -> Vec<String> {
    let mut slugs: Vec<String> = indexes
        .iter()
        .map(|ix| ix.manifest_slug().to_string())
        .collect();
    slugs.sort();
    slugs
}

impl Manifest {
    /// The manifest for a store being serialized: `dict_len` is the term count
    /// when the layout is Dictionary (`Some(0)` for an empty dictionary).
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub(crate) fn for_store(
        layout: LayoutStrategy,
        indexes: &[IndexType],
        dict_len: Option<u64>,
    ) -> Self {
        let components = dict_len
            .map(|row_count| ComponentDescriptor {
                name: "dictionary".to_string(),
                role: "dictionary".to_string(),
                implementation: DICT_IMPLEMENTATION.to_string(),
                version: 1,
                required: true,
                row_count,
                segment: DICT_SEGMENT_KEY.to_string(),
            })
            .into_iter()
            .collect();
        Self {
            format: MANIFEST_FORMAT.to_string(),
            version: MANIFEST_VERSION,
            layout: layout_slug(layout).to_string(),
            components,
            indexes: index_slugs(indexes),
        }
    }

    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub(crate) fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| VortexRdfError::Serialization(format!("manifest encoding: {e}")))
    }

    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| VortexRdfError::Deserialization(format!("manifest decoding: {e}")))
    }

    /// The dictionary component, when this store carries one.
    pub(crate) fn dictionary(&self) -> Option<&ComponentDescriptor> {
        self.components.iter().find(|c| c.role == "dictionary")
    }

    /// Validate this manifest against the opened file: known format/version,
    /// layout agreeing with the schema, every required component's segment
    /// present in the footer, and the declared indexes matching the schema's
    /// index columns. Component *contents* (dtype, row count) are validated
    /// where the component is opened.
    pub(crate) fn validate(&self, file: &VortexFile) -> Result<()> {
        if self.format != MANIFEST_FORMAT || self.version != MANIFEST_VERSION {
            return Err(VortexRdfError::Deserialization(format!(
                "unsupported manifest {}/{} (this build reads {MANIFEST_FORMAT}/{MANIFEST_VERSION})",
                self.format, self.version
            )));
        }
        let schema_layout = layout_slug(LayoutStrategy::from_dtype(file.dtype()));
        if self.layout != schema_layout {
            return Err(VortexRdfError::Deserialization(format!(
                "manifest declares layout {:?} but the schema reads as {schema_layout:?}",
                self.layout
            )));
        }
        for component in self.components.iter().filter(|c| c.required) {
            if file.footer().metadata_segment(&component.segment).is_none() {
                return Err(VortexRdfError::Deserialization(format!(
                    "required component {:?} names metadata segment {:?}, which the file \
                     does not carry",
                    component.name, component.segment
                )));
            }
        }
        let schema_indexes = index_slugs(&detect_indexes(file.dtype()));
        if self.indexes != schema_indexes {
            return Err(VortexRdfError::Deserialization(format!(
                "manifest declares indexes {:?} but the schema carries {:?}",
                self.indexes, schema_indexes
            )));
        }
        Ok(())
    }
}

/// The manifest of an opened file, or the actionable error for files that
/// predate the embedded format (no manifest segment). `bytes` come from
/// either a ranged footer read (native path) or the resolved metadata of a
/// buffer open — both end here so the error reads the same.
pub(crate) fn manifest_from_bytes(bytes: &[u8], file: &VortexFile) -> Result<Manifest> {
    let manifest = Manifest::from_json(bytes)?;
    manifest.validate(file)?;
    Ok(manifest)
}

/// The "this file predates the embedded format" error, shared by every open
/// path that finds no manifest segment.
pub(crate) fn missing_manifest_error() -> VortexRdfError {
    VortexRdfError::Deserialization(format!(
        "file carries no {MANIFEST_SEGMENT_KEY:?} metadata segment; it predates the \
         embedded store format — re-serialize it with a current vortex-rdf build"
    ))
}

// ── the bounded reader: a byte range of the outer file presented as a file ──

/// A [`VortexReadAt`] over one byte range of an outer file — the embedded
/// dictionary blob presented as a complete file of its own, so the inner
/// [`VortexFile`] machinery (footer discovery, zone maps, scans) runs
/// unchanged while every inner read becomes one ranged read of the outer
/// file. Nothing is buffered beyond what a probe touches.
#[cfg(feature = "file-io")]
pub(crate) struct BoundedReadAt {
    /// A dedicated reader over the outer file (independent of the outer
    /// `VortexFile`'s segment source, which is segment-keyed, not byte-keyed).
    outer: Arc<dyn VortexReadAt>,
    /// Absolute outer-file offset of the range (the segment's offset).
    base: u64,
    /// Range length — the inner file's total size.
    len: u64,
}

#[cfg(feature = "file-io")]
impl VortexReadAt for BoundedReadAt {
    fn concurrency(&self) -> usize {
        self.outer.concurrency()
    }

    fn coalesce_config(&self) -> Option<vortex_io::CoalesceConfig> {
        self.outer.coalesce_config()
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let len = self.len;
        Box::pin(async move { Ok(len) })
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        if offset + length as u64 > self.len {
            return Box::pin(async move {
                vortex_bail!("bounded read past the embedded component's range");
            });
        }
        self.outer.read_at(self.base + offset, length, alignment)
    }
}

/// A fresh positioned reader over the outer file — the same construction
/// `open_path` uses internally.
#[cfg(feature = "file-io")]
fn outer_read_at(path: &Path) -> Result<Arc<dyn VortexReadAt>> {
    use vortex_io::session::RuntimeSessionExt;
    use vortex_io::std_file::FileReadAt;
    let reader =
        FileReadAt::open(path, super::VORTEX_SESSION.handle()).map_err(VortexRdfError::Vortex)?;
    Ok(Arc::new(reader))
}

/// Read a metadata segment's bytes with one ranged read of the outer file —
/// the selective alternative to `include_metadata` (which resolves *every*
/// metadata segment on open).
#[cfg(feature = "file-io")]
pub(crate) async fn read_segment_bytes(path: &Path, spec: &SegmentSpec) -> Result<ByteBuffer> {
    let reader = outer_read_at(path)?;
    let handle = reader
        .read_at(spec.offset, spec.length as usize, spec.alignment)
        .await
        .map_err(VortexRdfError::Vortex)?;
    let buffer = handle
        .try_into_host()
        .map_err(VortexRdfError::Vortex)?
        .await
        .map_err(VortexRdfError::Vortex)?;
    Ok(ByteBuffer::copy_from_aligned(
        buffer.as_slice(),
        spec.alignment,
    ))
}

/// Open the embedded dictionary blob as its own lazily-readable
/// [`VortexFile`], without buffering it: inner segment reads become ranged
/// reads of the outer file through [`BoundedReadAt`].
#[cfg(feature = "file-io")]
pub(crate) async fn open_embedded_dict_file(path: &Path, spec: &SegmentSpec) -> Result<VortexFile> {
    let len = u64::from(spec.length);
    let shim = BoundedReadAt {
        outer: outer_read_at(path)?,
        base: spec.offset,
        len,
    };
    super::VORTEX_SESSION
        .open_options()
        .with_file_size(len)
        .with_layout_reader_cache()
        .open(Arc::new(shim))
        .await
        .map_err(VortexRdfError::Vortex)
}

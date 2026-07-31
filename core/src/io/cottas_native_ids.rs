use crate::error::{Result, VortexRdfError};
use crate::index::{RdfDictionary, SimpleDictionaryView};
#[cfg(feature = "legacy-sidecars")]
use crate::io::native_rdf_store::exact_ranges::{
    build_exact_range_directory_array, build_exact_range_payload_array, validate_exact_ranges,
};
use crate::io::native_rdf_store::exact_ranges::{checked_payload_end, decode_exact_range_columns};
use crate::io::native_rdf_store::object_index::{
    ObjectRangeCollector, PreparedObjectExactRanges, object_component_writes,
};
#[cfg(feature = "legacy-sidecars")]
use crate::io::native_rdf_store::object_index::{
    ObjectRangeHeapItem, ObjectRangeRecord, ObjectRangeRunReader, flush_object_range_run,
    write_object_range_record,
};
use crate::io::native_rdf_store::predicate_index::{
    PredicateRangeCollector, PreparedPredicateExactRanges, predicate_component_writes,
};
#[cfg(feature = "legacy-sidecars")]
use crate::io::native_rdf_store::predicate_index::{
    PredicateRangeHeapItem, PredicateRangeRecord, PredicateRangeRunReader,
    flush_predicate_range_run, write_predicate_range_record,
};
use crate::io::native_rdf_store::predicate_object_index::{
    PoRangeCollector, PreparedPoExactRanges, po_component_writes,
};
#[cfg(feature = "legacy-sidecars")]
use crate::io::native_rdf_store::predicate_object_index::{
    PoRangeHeapItem, PoRangeRecord, PoRangeRunReader, flush_po_range_run, write_po_range_record,
};
use crate::io::native_rdf_store::{NativeIndexBuildContext, NativeIndexSelection, NativeIndexSpec};
use crate::io::utils::CottasVortexCompressionProfile;
use crate::store::layout::cottas::TripleOrdering;

use futures::future::{self, BoxFuture};
use futures::{FutureExt, Stream, StreamExt};
use oxrdf::Quad;
use std::cmp::Ordering;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;
use vortex_error::{VortexError, VortexResult};

use crate::io::vortex_rdf_store_layout::{
    NativeComponentSource, NativeComponentWrite, StoreComponentDescriptor, StoreComponentRole,
};
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use vortex_array::VortexSessionExecute;
#[cfg(feature = "legacy-sidecars")]
use vortex_array::arrays::Chunked;
#[cfg(feature = "legacy-sidecars")]
use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray, VarBinArray, VarBinViewArray};
use vortex_array::buffer::BufferHandle;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::{ArrayRef, IntoArray};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_buffer::{Alignment, Buffer};
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_io::session::RuntimeSessionExt;
use vortex_io::std_file::FileReadAt;
use vortex_io::{CoalesceConfig, VortexReadAt, VortexWrite};
use vortex_layout::LayoutStrategy;
use vortex_layout::segments::SegmentId;
use vortex_session::VortexSession;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use oxrdfio::{RdfFormat, RdfSerializer};

#[cfg(feature = "legacy-sidecars")]
use vortex::expr::or;
use vortex::expr::{Expression, and, col, eq, lit};
use vortex_array::stream::ArrayStreamExt;

// VORTEX_RDF_NATIVE_COST_AWARE_PREDICATE_PLANNER_V1
const NATIVE_AUTO_MAX_PREDICATE_SELECTIVITY_NUMERATOR: u64 = 35;
const NATIVE_AUTO_MAX_PREDICATE_SELECTIVITY_DENOMINATOR: u64 = 100;
const NATIVE_AUTO_MAX_PREDICATE_RANGES: usize = 16_384;

static NATIVE_FILE_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    use vortex_array::scalar_fn::session::ScalarFnSession;
    use vortex_array::session::ArraySession;
    use vortex_io::session::RuntimeSession;
    use vortex_layout::session::LayoutSession;

    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();

    vortex_file::register_default_encodings(&session);
    crate::io::vortex_rdf_store_layout::register_vortex_rdf_store_layout(&session);
    session
});
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct CottasNativeConfig {
    pub ordering: TripleOrdering,
    pub row_group_size: usize,
    pub dict_row_group_size: usize,
    pub compression_profile: CottasVortexCompressionProfile,
    pub native_indexes: NativeIndexSelection,
}

impl Default for CottasNativeConfig {
    fn default() -> Self {
        Self {
            ordering: TripleOrdering::SPO,
            row_group_size: 122_880,
            dict_row_group_size: 1_024,
            compression_profile: CottasVortexCompressionProfile::Balanced,
            // VORTEX_RDF_NATIVE_STANDARD_PROFILE_DEFAULT_V1
            native_indexes: NativeIndexSelection::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum NativeComponent {
    DictionaryVortex,
    DictionaryTermToIdVortex,
    DictionaryTermDirectoryVortex,
    SubjectRangesVortex,
    PredicateDirectoryVortexV2,
    PredicateRangesVortexV2,
    PredicateObjectPartitionsVortexV2,
    PredicateObjectDirectoryVortexV2,
    PredicateObjectRangesVortexV2,
    ObjectDirectoryVortexV2,
    ObjectRangesVortexV2,
}

impl NativeComponent {
    fn logical_name(self) -> &'static str {
        match self {
            Self::DictionaryVortex => "rdf.dictionary.id-to-term.vortex",
            Self::DictionaryTermToIdVortex => "rdf.dictionary.term-to-id.vortex",
            Self::DictionaryTermDirectoryVortex => "rdf.dictionary.term-directory.vortex-v1",
            Self::SubjectRangesVortex => "rdf.index.subject.ranges.vortex-v1",
            Self::PredicateDirectoryVortexV2 => "rdf.index.p.directory.vortex-v2",
            Self::PredicateRangesVortexV2 => "rdf.index.p.ranges.vortex-v2",
            Self::PredicateObjectPartitionsVortexV2 => "rdf.index.po.partitions.vortex-v2",
            Self::PredicateObjectDirectoryVortexV2 => "rdf.index.po.directory.vortex-v2",
            Self::PredicateObjectRangesVortexV2 => "rdf.index.po.ranges.vortex-v2",
            Self::ObjectDirectoryVortexV2 => "rdf.index.o.directory.vortex-v2",
            Self::ObjectRangesVortexV2 => "rdf.index.o.ranges.vortex-v2",
        }
    }

    fn external_suffix(self) -> &'static str {
        match self {
            Self::DictionaryVortex => "dict.vortex",
            Self::DictionaryTermToIdVortex => "dict.term_to_id.vortex",
            Self::DictionaryTermDirectoryVortex => "dict.term_directory.v1.vortex",
            Self::SubjectRangesVortex => "subject_ranges.v1.vortex",
            Self::PredicateDirectoryVortexV2 => "p_exact_directory.v2.vortex",
            Self::PredicateRangesVortexV2 => "p_exact_ranges.v2.vortex",
            Self::PredicateObjectPartitionsVortexV2 => "po_predicate_partitions.v2.vortex",
            Self::PredicateObjectDirectoryVortexV2 => "po_exact_directory.v2.vortex",
            Self::PredicateObjectRangesVortexV2 => "po_exact_ranges.v2.vortex",
            Self::ObjectDirectoryVortexV2 => "o_exact_directory.v2.vortex",
            Self::ObjectRangesVortexV2 => "o_exact_ranges.v2.vortex",
        }
    }

    fn external_path(self, data_path: &Path) -> PathBuf {
        let name = data_path
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("data.vortex");
        data_path.with_file_name(format!("{name}.{}", self.external_suffix()))
    }
}

const NATIVE_ARTIFACT_FORMAT: &str = "vortex-rdf-native-ids";
const NATIVE_ARTIFACT_FORMAT_VERSION: u32 = 1;
const NATIVE_ARTIFACT_METADATA_KEY: &str = "vortex.rdf.native-ids.manifest";
const NATIVE_TRIPLES_LOGICAL_NAME: &str = "rdf.triples.native-ids.v1";
const NATIVE_TERM_DIRECTORY_FENCE_ROWS: usize = 512;

impl NativeComponent {
    const ALL: [Self; 11] = [
        Self::DictionaryVortex,
        Self::DictionaryTermToIdVortex,
        Self::DictionaryTermDirectoryVortex,
        Self::SubjectRangesVortex,
        Self::PredicateDirectoryVortexV2,
        Self::PredicateRangesVortexV2,
        Self::PredicateObjectPartitionsVortexV2,
        Self::PredicateObjectDirectoryVortexV2,
        Self::PredicateObjectRangesVortexV2,
        Self::ObjectDirectoryVortexV2,
        Self::ObjectRangesVortexV2,
    ];

    fn default_implementation(self) -> &'static str {
        match self {
            Self::DictionaryVortex => "id-row-v1-balanced-fsst",
            Self::DictionaryTermToIdVortex => "lexical-sorted-v1-compact",
            Self::DictionaryTermDirectoryVortex => "sparse-fence-v1",
            Self::SubjectRangesVortex => "subject-ranges-v1",
            Self::PredicateDirectoryVortexV2 => "predicate-directory-v2",
            Self::PredicateRangesVortexV2 => "predicate-ranges-v2",
            Self::PredicateObjectPartitionsVortexV2 => "po-predicate-partitions-v2",
            Self::PredicateObjectDirectoryVortexV2 => "po-directory-v2",
            Self::PredicateObjectRangesVortexV2 => "po-ranges-v2",
            Self::ObjectDirectoryVortexV2 => "object-directory-v2",
            Self::ObjectRangesVortexV2 => "object-ranges-v2",
        }
    }
}

// VORTEX_RDF_NATIVE_COMPONENT_LOCATOR_SCHEMA_V1
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum NativeComponentStorage {
    #[default]
    External,
    Embedded {
        offset: u64,
        length: u64,
    },
}

impl NativeComponentStorage {
    fn embedded_range(&self) -> Result<Option<Range<u64>>> {
        let Self::Embedded { offset, length } = self else {
            return Ok(None);
        };
        if *length == 0 {
            return Err(VortexRdfError::Deserialization(
                "embedded native component has zero length".into(),
            ));
        }
        let end = offset.checked_add(*length).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "embedded native component range overflows u64: offset={offset}, length={length}"
            ))
        })?;
        Ok(Some(*offset..end))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NativeArtifactComponentManifest {
    logical_name: String,
    implementation: String,
    required: bool,
    row_count: Option<u64>,
    #[serde(default)]
    storage: NativeComponentStorage,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NativeArtifactManifest {
    format: String,
    version: u32,
    components: Vec<NativeArtifactComponentManifest>,
}

impl NativeArtifactManifest {
    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    fn production_defaults() -> Self {
        let mut components = Vec::with_capacity(NativeComponent::ALL.len() + 1);
        components.push(NativeArtifactComponentManifest {
            logical_name: NATIVE_TRIPLES_LOGICAL_NAME.to_string(),
            implementation: "spog-u32-v1".to_string(),
            required: true,
            row_count: None,
            storage: NativeComponentStorage::External,
        });
        components.extend(NativeComponent::ALL.into_iter().map(|component| {
            NativeArtifactComponentManifest {
                logical_name: component.logical_name().to_string(),
                implementation: component.default_implementation().to_string(),
                required: true,
                row_count: None,
                storage: NativeComponentStorage::External,
            }
        }));
        Self {
            format: NATIVE_ARTIFACT_FORMAT.to_string(),
            version: NATIVE_ARTIFACT_FORMAT_VERSION,
            components,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.format != NATIVE_ARTIFACT_FORMAT {
            return Err(VortexRdfError::Deserialization(format!(
                "unsupported native artifact format {:?}; expected {:?}",
                self.format, NATIVE_ARTIFACT_FORMAT
            )));
        }
        if self.version != NATIVE_ARTIFACT_FORMAT_VERSION {
            return Err(VortexRdfError::Deserialization(format!(
                "unsupported native artifact version {}; expected {}",
                self.version, NATIVE_ARTIFACT_FORMAT_VERSION
            )));
        }
        let expected: BTreeSet<&str> = std::iter::once(NATIVE_TRIPLES_LOGICAL_NAME)
            .chain(
                NativeComponent::ALL
                    .into_iter()
                    .map(NativeComponent::logical_name),
            )
            .collect();
        let mut actual = BTreeSet::new();
        let mut embedded_ranges: Vec<(&str, Range<u64>)> = Vec::new();
        for component in &self.components {
            if component.logical_name.is_empty() {
                return Err(VortexRdfError::Deserialization(
                    "native artifact manifest contains an empty logical component name".into(),
                ));
            }
            if component.implementation.is_empty() {
                return Err(VortexRdfError::Deserialization(format!(
                    "native artifact component {:?} has no implementation identifier",
                    component.logical_name
                )));
            }
            if !component.required {
                return Err(VortexRdfError::Deserialization(format!(
                    "production native artifact component {:?} must be required",
                    component.logical_name
                )));
            }
            if !actual.insert(component.logical_name.as_str()) {
                return Err(VortexRdfError::Deserialization(format!(
                    "native artifact manifest contains duplicate component {:?}",
                    component.logical_name
                )));
            }
            if component.logical_name == NATIVE_TRIPLES_LOGICAL_NAME
                && !matches!(component.storage, NativeComponentStorage::External)
            {
                return Err(VortexRdfError::Deserialization(
                    "the triples component must remain the primary outer Vortex layout".into(),
                ));
            }
            if let Some(range) = component.storage.embedded_range()? {
                embedded_ranges.push((component.logical_name.as_str(), range));
            }
        }
        embedded_ranges.sort_by_key(|(_, range)| range.start);
        for pair in embedded_ranges.windows(2) {
            if pair[1].1.start < pair[0].1.end {
                return Err(VortexRdfError::Deserialization(format!(
                    "embedded native component ranges overlap: {}={:?}, {}={:?}",
                    pair[0].0, pair[0].1, pair[1].0, pair[1].1
                )));
            }
        }
        let missing: Vec<_> = expected.difference(&actual).copied().collect();
        let unexpected: Vec<_> = actual.difference(&expected).copied().collect();
        if !missing.is_empty() || !unexpected.is_empty() {
            return Err(VortexRdfError::Deserialization(format!(
                "native artifact component inventory mismatch: missing={missing:?}, unexpected={unexpected:?}"
            )));
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    fn to_metadata_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| {
            VortexRdfError::Serialization(format!(
                "failed to serialize native artifact manifest as JSON: {error}"
            ))
        })
    }

    fn from_metadata_bytes(bytes: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| {
            VortexRdfError::Deserialization(format!(
                "failed to deserialize native artifact manifest metadata as JSON: {error}"
            ))
        })?;
        manifest.validate()?;
        Ok(manifest)
    }
}

// VORTEX_RDF_NATIVE_ARTIFACT_MANIFEST_METADATA_IO_V1
#[derive(Clone, Debug, Eq, PartialEq)]
enum NativeArtifactKind {
    LegacyExternal,
    ManifestExternal(NativeArtifactManifest),
}

impl NativeArtifactKind {
    fn manifest(&self) -> Option<&NativeArtifactManifest> {
        match self {
            Self::LegacyExternal => None,
            Self::ManifestExternal(manifest) => Some(manifest),
        }
    }
}

// VORTEX_RDF_NATIVE_ARTIFACT_INSPECTION_V1
fn metadata_segment_id(data_segment_count: usize, metadata_index: usize) -> Result<SegmentId> {
    let absolute_index = data_segment_count
        .checked_add(metadata_index)
        .ok_or_else(|| {
            VortexRdfError::Deserialization(
                "native artifact metadata segment index overflow".to_string(),
            )
        })?;
    let id = u32::try_from(absolute_index).map_err(|_| {
        VortexRdfError::Deserialization(format!(
            "native artifact metadata segment index {absolute_index} exceeds u32"
        ))
    })?;
    Ok(SegmentId::from(id))
}

// VORTEX_RDF_SELECTIVE_NATIVE_MANIFEST_LOADING_V1
// VORTEX_RDF_CACHED_OPENED_VORTEX_HANDLES_V1
async fn open_and_inspect_native_artifact(
    artifact_path: &Path,
) -> Result<(NativeArtifactKind, vortex_file::VortexFile)> {
    // Keep the opened outer file: manifest inspection and the triples scan must
    // share one footer, segment source, and cached layout-reader tree.
    let file = NATIVE_FILE_SESSION
        .open_options()
        .with_layout_reader_cache()
        .open_path(artifact_path)
        .await
        .map_err(VortexRdfError::from)?;
    let metadata_index = file
        .footer()
        .metadata_segments()
        .position(|(key, _)| key == NATIVE_ARTIFACT_METADATA_KEY);
    let Some(metadata_index) = metadata_index else {
        return Ok((NativeArtifactKind::LegacyExternal, file));
    };
    let segment_id = metadata_segment_id(file.footer().segment_map().len(), metadata_index)?;
    let handle = file
        .segment_source()
        .request(segment_id)
        .await
        .map_err(VortexRdfError::from)?;
    let bytes = handle
        .try_into_host()
        .map_err(VortexRdfError::from)?
        .await
        .map_err(VortexRdfError::from)?;
    let manifest = NativeArtifactManifest::from_metadata_bytes(bytes.as_slice())?;
    Ok((NativeArtifactKind::ManifestExternal(manifest), file))
}

#[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
async fn inspect_native_artifact(artifact_path: &Path) -> Result<NativeArtifactKind> {
    open_and_inspect_native_artifact(artifact_path)
        .await
        .map(|(kind, _)| kind)
}

// VORTEX_RDF_BOUNDED_NATIVE_COMPONENT_READER_V1
#[derive(Clone)]
struct BoundedNativeComponentReader {
    source: Arc<dyn VortexReadAt>,
    base_offset: u64,
    length: u64,
}

impl BoundedNativeComponentReader {
    fn new(source: Arc<dyn VortexReadAt>, base_offset: u64, length: u64) -> Result<Self> {
        if length == 0 {
            return Err(VortexRdfError::InvalidOperation(
                "bounded native component length must be positive".into(),
            ));
        }
        base_offset.checked_add(length).ok_or_else(|| {
            VortexRdfError::InvalidOperation(format!(
                "bounded native component range overflows u64: offset={base_offset}, length={length}"
            ))
        })?;
        Ok(Self {
            source,
            base_offset,
            length,
        })
    }

    fn absolute_read_offset(&self, offset: u64, length: usize) -> VortexResult<u64> {
        let length = u64::try_from(length)
            .map_err(|_| vortex_error::vortex_err!("bounded read length exceeds u64"))?;
        let relative_end = offset.checked_add(length).ok_or_else(|| {
            vortex_error::vortex_err!(
                "bounded read range overflows u64: offset={}, length={}",
                offset,
                length
            )
        })?;
        if relative_end > self.length {
            return Err(vortex_error::vortex_err!(
                "bounded read {}..{} exceeds component length {}",
                offset,
                relative_end,
                self.length
            ));
        }
        self.base_offset.checked_add(offset).ok_or_else(|| {
            vortex_error::vortex_err!(
                "bounded absolute read offset overflows u64: base={}, offset={}",
                self.base_offset,
                offset
            )
        })
    }
}

impl VortexReadAt for BoundedNativeComponentReader {
    fn uri(&self) -> Option<&Arc<str>> {
        self.source.uri()
    }

    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        self.source.coalesce_config()
    }

    fn concurrency(&self) -> usize {
        self.source.concurrency()
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        future::ready(Ok(self.length)).boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let absolute_offset = match self.absolute_read_offset(offset, length) {
            Ok(offset) => offset,
            Err(error) => return future::ready(Err(error)).boxed(),
        };
        self.source.read_at(absolute_offset, length, alignment)
    }
}

// VORTEX_RDF_EMBEDDED_COMPONENT_RESOLUTION_V1
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ComponentLocation {
    External(PathBuf),
    Embedded {
        artifact_path: PathBuf,
        component: NativeComponent,
        offset: u64,
        length: u64,
    },
}

impl ComponentLocation {
    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    fn cache_key(&self) -> String {
        match self {
            Self::External(path) => format!("external:{}", path.display()),
            Self::Embedded {
                artifact_path,
                component,
                offset,
                length,
            } => format!(
                "embedded:{}:{}:{offset}:{length}",
                artifact_path.display(),
                component.logical_name()
            ),
        }
    }
}

// VORTEX_RDF_SHARED_EMBEDDED_ARTIFACT_READER_V1
#[derive(Clone)]
struct NativeComponentResolver {
    artifact_path: PathBuf,
    artifact_kind: NativeArtifactKind,
    artifact_len: Option<u64>,
    embedded_source: Option<Arc<dyn VortexReadAt>>,
    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    outer_file: Option<vortex_file::VortexFile>,
    opened_components: Arc<Mutex<HashMap<NativeComponent, vortex_file::VortexFile>>>,
}

impl NativeComponentResolver {
    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    fn legacy_external(artifact_path: &Path) -> Self {
        Self {
            artifact_path: artifact_path.to_path_buf(),
            artifact_kind: NativeArtifactKind::LegacyExternal,
            artifact_len: None,
            embedded_source: None,
            outer_file: None,
            opened_components: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    fn from_kind(artifact_path: &Path, artifact_kind: NativeArtifactKind) -> Result<Self> {
        Self::from_kind_and_outer_file(artifact_path, artifact_kind, None)
    }

    fn from_kind_and_outer_file(
        artifact_path: &Path,
        artifact_kind: NativeArtifactKind,
        outer_file: Option<vortex_file::VortexFile>,
    ) -> Result<Self> {
        let has_embedded_components = artifact_kind.manifest().is_some_and(|manifest| {
            manifest
                .components
                .iter()
                .any(|entry| matches!(entry.storage, NativeComponentStorage::Embedded { .. }))
        });
        let (artifact_len, embedded_source) = if has_embedded_components {
            let artifact_len = std::fs::metadata(artifact_path)
                .map_err(|error| {
                    VortexRdfError::InvalidOperation(format!(
                        "cannot stat native artifact {:?}: {error}",
                        artifact_path
                    ))
                })?
                .len();
            let source: Arc<dyn VortexReadAt> = Arc::new(
                FileReadAt::open(artifact_path, NATIVE_FILE_SESSION.handle())
                    .map_err(VortexRdfError::from)?,
            );
            (Some(artifact_len), Some(source))
        } else {
            (None, None)
        };
        Ok(Self {
            artifact_path: artifact_path.to_path_buf(),
            artifact_kind,
            artifact_len,
            embedded_source,
            outer_file,
            opened_components: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    async fn inspect(artifact_path: &Path) -> Result<Self> {
        let (artifact_kind, outer_file) = open_and_inspect_native_artifact(artifact_path).await?;
        Self::from_kind_and_outer_file(artifact_path, artifact_kind, Some(outer_file))
    }

    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    fn outer_file(&self) -> Result<vortex_file::VortexFile> {
        self.outer_file.clone().ok_or_else(|| {
            VortexRdfError::InvalidOperation(format!(
                "native artifact {:?} has no retained outer Vortex file",
                self.artifact_path
            ))
        })
    }

    fn opened_components_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<NativeComponent, vortex_file::VortexFile>>> {
        self.opened_components.lock().map_err(|_| {
            VortexRdfError::Deserialization(
                "native opened-component cache mutex was poisoned".into(),
            )
        })
    }

    fn artifact_kind(&self) -> &NativeArtifactKind {
        &self.artifact_kind
    }

    fn location(&self, component: NativeComponent) -> Result<ComponentLocation> {
        match self.artifact_kind() {
            NativeArtifactKind::LegacyExternal => Ok(ComponentLocation::External(
                component.external_path(&self.artifact_path),
            )),
            NativeArtifactKind::ManifestExternal(manifest) => {
                let entry = manifest
                    .components
                    .iter()
                    .find(|entry| entry.logical_name == component.logical_name())
                    .ok_or_else(|| {
                        VortexRdfError::Deserialization(format!(
                            "native artifact manifest has no entry for {}",
                            component.logical_name()
                        ))
                    })?;
                if entry.implementation != component.default_implementation() {
                    return Err(VortexRdfError::Deserialization(format!(
                        "native artifact component {} uses unsupported implementation {:?}; expected {:?}",
                        component.logical_name(),
                        entry.implementation,
                        component.default_implementation()
                    )));
                }
                match entry.storage {
                    NativeComponentStorage::External => Ok(ComponentLocation::External(
                        component.external_path(&self.artifact_path),
                    )),
                    NativeComponentStorage::Embedded { offset, length } => {
                        let artifact_len = self.artifact_len.ok_or_else(|| {
                            VortexRdfError::Deserialization(format!(
                                "embedded component {} has no cached artifact length",
                                component.logical_name()
                            ))
                        })?;
                        let end = offset.checked_add(length).ok_or_else(|| {
                            VortexRdfError::Deserialization(
                                "embedded component range overflow".into(),
                            )
                        })?;
                        if end > artifact_len {
                            return Err(VortexRdfError::Deserialization(format!(
                                "embedded component {} range {offset}..{end} exceeds artifact length {artifact_len}",
                                component.logical_name()
                            )));
                        }
                        Ok(ComponentLocation::Embedded {
                            artifact_path: self.artifact_path.clone(),
                            component,
                            offset,
                            length,
                        })
                    }
                }
            }
        }
    }

    // VORTEX_RDF_RESOLVER_BASED_COMPONENT_AVAILABILITY_V1
    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    fn components_available(&self, components: &[NativeComponent]) -> Result<bool> {
        for &component in components {
            match self.location(component)? {
                ComponentLocation::Embedded { .. } => {}
                ComponentLocation::External(path) if path.is_file() => {}
                ComponentLocation::External(_) => return Ok(false),
            }
        }
        Ok(true)
    }

    async fn open(&self, component: NativeComponent) -> Result<vortex_file::VortexFile> {
        if let Some(file) = self.opened_components_lock()?.get(&component).cloned() {
            return Ok(file);
        }
        let file = match self.location(component)? {
            ComponentLocation::External(path) => {
                if !path.is_file() {
                    return Err(VortexRdfError::InvalidOperation(format!(
                        "required native component {} is missing at {:?}",
                        component.logical_name(),
                        path
                    )));
                }
                NATIVE_FILE_SESSION
                    .open_options()
                    .with_layout_reader_cache()
                    .open_path(path)
                    .await
                    .map_err(VortexRdfError::from)?
            }
            ComponentLocation::Embedded {
                artifact_path,
                component,
                offset,
                length,
            } => {
                let source = self.embedded_source.as_ref().cloned().ok_or_else(|| {
                    VortexRdfError::Deserialization(format!(
                        "embedded component {} in {:?} has no shared artifact reader",
                        component.logical_name(),
                        artifact_path
                    ))
                })?;
                let reader = BoundedNativeComponentReader::new(source, offset, length)?;
                NATIVE_FILE_SESSION
                    .open_options()
                    .with_file_size(length)
                    .with_layout_reader_cache()
                    .open_read(reader)
                    .await
                    .map_err(VortexRdfError::from)?
            }
        };
        let mut cache = self.opened_components_lock()?;
        Ok(cache
            .entry(component)
            .or_insert_with(|| file.clone())
            .clone())
    }
    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    fn external_path(&self, component: NativeComponent) -> Result<PathBuf> {
        match self.location(component)? {
            ComponentLocation::External(path) => Ok(path),
            ComponentLocation::Embedded {
                artifact_path,
                component,
                offset,
                length,
            } => Err(VortexRdfError::InvalidOperation(format!(
                "embedded component {} in {:?} at {}..{} has no external path",
                component.logical_name(),
                artifact_path,
                offset,
                offset + length
            ))),
        }
    }
}

// VORTEX_RDF_RUNTIME_COMPONENT_READ_SIDE_V1
static NATIVE_RUNTIME_RESOLVER_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, Arc<NativeComponentResolver>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn runtime_resolver_cache_lock()
-> Result<std::sync::MutexGuard<'static, HashMap<PathBuf, Arc<NativeComponentResolver>>>> {
    NATIVE_RUNTIME_RESOLVER_CACHE.lock().map_err(|_| {
        VortexRdfError::Deserialization("native runtime resolver cache mutex was poisoned".into())
    })
}

async fn runtime_component_resolver(data_path: &Path) -> Result<Arc<NativeComponentResolver>> {
    if let Some(resolver) = runtime_resolver_cache_lock()?.get(data_path).cloned() {
        return Ok(resolver);
    }
    let inspected = Arc::new(NativeComponentResolver::inspect(data_path).await?);
    let mut cache = runtime_resolver_cache_lock()?;
    Ok(cache
        .entry(data_path.to_path_buf())
        .or_insert_with(|| Arc::clone(&inspected))
        .clone())
}

#[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
fn native_component_path(data_path: &Path, component: NativeComponent) -> PathBuf {
    NativeComponentResolver::legacy_external(data_path)
        .external_path(component)
        .expect("external component resolution is infallible before Phase C")
}

mod common_serialization;
use common_serialization::*;

#[cfg(feature = "legacy-sidecars")]
mod legacy_builders;
#[cfg(feature = "legacy-sidecars")]
mod legacy_results;
#[cfg(feature = "legacy-sidecars")]
pub use legacy_builders::{
    NativeDictionaryRebuildStats, NativeIdToTermRewriteStats, NativePoPredicatePartitionBuildStats,
    NativePoRowGroupIndexBuildStats, NativeSubjectRangeIndexBuildStats,
    NativeTermDirectoryBuildStats, NativeTermWindowDiagnostics, NativeTermWindowTrial,
    build_cottas_native_o_exact_ranges_index, build_cottas_native_p_exact_ranges_index,
    build_cottas_native_po_exact_ranges_v2_index, build_cottas_native_po_predicate_partitions_v2,
    build_cottas_native_subject_range_index, build_cottas_native_term_directory,
    diagnose_cottas_native_term_windows, rebuild_cottas_native_term_dictionary,
    rewrite_cottas_native_id_to_term_dictionary, serialize_cottas_native_file,
};
#[cfg(feature = "legacy-sidecars")]
mod legacy_runtime;
#[cfg(feature = "legacy-sidecars")]
use legacy_runtime::execute_cottas_native_match;
#[cfg(feature = "legacy-sidecars")]
pub use legacy_runtime::{
    CottasNativeIdsCountDiagnostics, CottasNativeIdsCountTimings, CottasNativeIdsDiagnostics,
    NativeIdsCountMode, count_cottas_native_ids_file_with_diagnostics,
    count_cottas_native_ids_file_with_diagnostics_mode, match_cottas_native_file,
    match_cottas_native_file_with_diagnostics,
};
#[cfg(feature = "legacy-sidecars")]
use legacy_runtime::{
    build_native_pattern_filter_lazy_with_stats, collect_unique_ids, extract_spog_id_columns,
    invalidate_po_partition_cache, invalidate_predicate_v2_cache, invalidate_term_directory_cache,
    lookup_terms_by_ids_from_sidecar, projected_native_id_rows_as_triples,
};

#[cfg(feature = "legacy-sidecars")]
pub use legacy_results::{
    NativeCompactTripleBatch, NativeDirectCompactTimings, diagnose_cottas_native_direct_compact,
    match_cottas_native_file_as_compact_triples, match_cottas_native_file_as_triples,
    match_cottas_native_file_as_triples_optimized,
};

// VORTEX_RDF_NATIVE_V10_QUAD_SOURCE_CLI_V1
/// Experimental v10 serializer: native Vortex-RDF root plus an ID-based SPOG
/// QuadSource. It deliberately writes no v9 manifest, sidecars, or embedded files.
pub async fn serialize_cottas_native_quad_source_v10_file<Dict, S>(
    quad_stream: S,
    output_path: &Path,
    config: CottasNativeConfig,
) -> Result<()>
where
    Dict: RdfDictionary + Send + Sync + 'static,
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    config.native_indexes.ensure_materializable_now()?;
    if config.ordering != TripleOrdering::SPO {
        return Err(VortexRdfError::InvalidOperation(format!(
            "native v10 QuadSource serialization requires SPO ordering; got {:?}",
            config.ordering
        )));
    }
    let row_group_size = config.row_group_size.max(1);
    let sort_batch_size = std::env::var("VORTEX_RDF_NATIVE_ID_SORT_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(row_group_size.saturating_mul(8).max(1_000_000));
    let temp_dir =
        tempfile::tempdir().map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let string_runs = spill_sorted_native_id_string_runs(
        quad_stream,
        config.ordering,
        sort_batch_size,
        temp_dir.path(),
    )
    .await?;
    let mut dictionary = Dict::new();
    let pair_runs =
        build_dictionary_and_pair_runs::<Dict>(&mut dictionary, &string_runs, temp_dir.path())?;
    let id_runs = encode_string_runs_to_id_runs::<Dict>(
        &dictionary,
        &string_runs,
        config.ordering,
        temp_dir.path(),
    )?;
    drop(string_runs);
    let index_build_context = NativeIndexBuildContext::new(
        id_runs.clone(),
        config.dict_row_group_size,
        config.native_indexes.clone(),
    );
    let arrays = merge_sorted_id_runs_to_array_stream(id_runs, config.ordering, row_group_size)?;
    let dtype = empty_spog_array()?.dtype().clone();
    let stream = ArrayStreamAdapter::new(dtype, arrays);
    let inner = WriteStrategyBuilder::default().with_row_block_size(row_group_size);
    let inner = match config.compression_profile {
        CottasVortexCompressionProfile::Balanced => inner,
        CottasVortexCompressionProfile::Compact => {
            inner.with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        }
    };
    let staging_path = output_path.with_file_name(format!(
        ".{}.v10-writing.{}.tmp",
        output_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("vortex-rdf"),
        std::process::id()
    ));
    if staging_path.exists() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "native v10 staging path already exists: {:?}",
            staging_path
        )));
    }
    let result: Result<()> = async {
        let mut writer = tokio::fs::File::create(&staging_path)
            .await
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
        // VORTEX_RDF_NATIVE_BIDIRECTIONAL_DICTIONARIES_V1
        let id_to_term_source = Arc::new(PairRunDictionarySource::new(
            &pair_runs.id_run_paths,
            PairRunOrder::Id,
            config.dict_row_group_size,
        )?);
        let id_to_term_strategy: Arc<dyn LayoutStrategy> = WriteStrategyBuilder::default()
            .with_row_block_size(config.dict_row_group_size.max(1))
            .build();
        let id_to_term_component = NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "dictionary.id-to-term".into(),
                role: StoreComponentRole::Dictionary,
                implementation: "native-id-row-v1-balanced".into(),
                version: 1,
                required: true,
                dtype: id_to_term_source.dtype().clone(),
            },
            id_to_term_source,
            id_to_term_strategy,
        )
        .map_err(VortexRdfError::from)?;

        let term_to_id_source = Arc::new(PairRunDictionarySource::new(
            &pair_runs.term_run_paths,
            PairRunOrder::Term,
            config.dict_row_group_size,
        )?);
        let term_to_id_strategy: Arc<dyn LayoutStrategy> = WriteStrategyBuilder::default()
            .with_row_block_size(config.dict_row_group_size.max(1))
            .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
            .build();
        let term_to_id_component = NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "dictionary.term-to-id".into(),
                role: StoreComponentRole::Dictionary,
                implementation: "native-lexical-sorted-v1-compact".into(),
                version: 1,
                required: true,
                dtype: term_to_id_source.dtype().clone(),
            },
            term_to_id_source,
            term_to_id_strategy,
        )
        .map_err(VortexRdfError::from)?;

        let term_directory_source = Arc::new(NativeTermDirectorySource::new(
            &pair_runs.term_run_paths,
            NATIVE_TERM_DIRECTORY_FENCE_ROWS,
            config.dict_row_group_size,
        )?);
        let term_directory_strategy: Arc<dyn LayoutStrategy> = WriteStrategyBuilder::default()
            .with_row_block_size(config.dict_row_group_size.max(1))
            .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
            .build();
        let term_directory_component = NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: "dictionary.term-directory".into(),
                role: StoreComponentRole::Dictionary,
                implementation: "native-sparse-lexical-fence-v1-compact".into(),
                version: 1,
                required: true,
                dtype: term_directory_source.dtype().clone(),
            },
            term_directory_source,
            term_directory_strategy,
        )
        .map_err(VortexRdfError::from)?;
        let mut components = vec![
            id_to_term_component,
            term_to_id_component,
            term_directory_component,
        ];
        for spec in config.native_indexes.resolved() {
            match spec {
                NativeIndexSpec::SubjectRangesV1 => {
                    let run_paths = index_build_context.run_paths_for(spec).ok_or_else(|| {
                        VortexRdfError::Serialization(
                            "subject range replay paths were not retained".into(),
                        )
                    })?;
                    let source = Arc::new(NativeSubjectRangesSource::new(
                        run_paths,
                        index_build_context.index_row_group_size,
                    )?);
                    let strategy: Arc<dyn LayoutStrategy> = WriteStrategyBuilder::default()
                        .with_row_block_size(config.dict_row_group_size.max(1))
                        .with_btrblocks_builder(
                            BtrBlocksCompressorBuilder::default().with_compact(),
                        )
                        .build();
                    components.push(
                        NativeComponentWrite::new(
                            StoreComponentDescriptor {
                                name: "index.subject-ranges".into(),
                                role: StoreComponentRole::Index,
                                implementation: "native-subject-ranges-v1-compact".into(),
                                version: 1,
                                required: false,
                                dtype: source.dtype().clone(),
                            },
                            source,
                            strategy,
                        )
                        .map_err(VortexRdfError::from)?,
                    );
                }
                NativeIndexSpec::PredicateRunsV1 => {
                    let run_paths = index_build_context.run_paths_for(spec).ok_or_else(|| {
                        VortexRdfError::Serialization(
                            "predicate run replay paths were not retained".into(),
                        )
                    })?;
                    let source = Arc::new(NativePredicateRunsSource::new(
                        run_paths,
                        index_build_context.index_row_group_size,
                    )?);
                    let strategy: Arc<dyn LayoutStrategy> = WriteStrategyBuilder::default()
                        .with_row_block_size(config.dict_row_group_size.max(1))
                        .with_btrblocks_builder(
                            BtrBlocksCompressorBuilder::default().with_compact(),
                        )
                        .build();
                    components.push(
                        NativeComponentWrite::new(
                            StoreComponentDescriptor {
                                name: "index.predicate-runs".into(),
                                role: StoreComponentRole::Index,
                                implementation: "native-predicate-runs-v1-compact".into(),
                                version: 1,
                                required: false,
                                dtype: source.dtype().clone(),
                            },
                            source,
                            strategy,
                        )
                        .map_err(VortexRdfError::from)?,
                    );
                }
                NativeIndexSpec::PredicateExactRangesV2 => {
                    let run_paths =
                        index_build_context
                            .selected_run_paths(spec)
                            .ok_or_else(|| {
                                VortexRdfError::Serialization(
                                    "predicate exact-range replay paths were not retained".into(),
                                )
                            })?;
                    let prepared = Arc::new(prepare_native_predicate_exact_v2(
                        run_paths,
                        temp_dir.path(),
                    )?);
                    components.extend(predicate_component_writes(
                        prepared,
                        index_build_context.index_row_group_size,
                    )?);
                }
                NativeIndexSpec::PredicateObjectExactRangesV2 => {
                    let run_paths =
                        index_build_context
                            .selected_run_paths(spec)
                            .ok_or_else(|| {
                                VortexRdfError::Serialization(
                                    "predicate-object exact-range replay paths were not retained"
                                        .into(),
                                )
                            })?;
                    let prepared = Arc::new(prepare_native_predicate_object_exact_v2(
                        run_paths,
                        temp_dir.path(),
                    )?);
                    components.extend(po_component_writes(
                        prepared,
                        index_build_context.index_row_group_size,
                    )?);
                }
                NativeIndexSpec::ObjectExactRangesV2 => {
                    let run_paths =
                        index_build_context
                            .selected_run_paths(spec)
                            .ok_or_else(|| {
                                VortexRdfError::Serialization(
                                    "object exact-range replay paths were not retained".into(),
                                )
                            })?;
                    let prepared =
                        Arc::new(prepare_native_object_exact_v2(run_paths, temp_dir.path())?);
                    components.extend(object_component_writes(
                        prepared,
                        index_build_context.index_row_group_size,
                    )?);
                }
            }
        }
        let expected_component_names: Vec<Arc<str>> =
            std::iter::once(Arc::<str>::from("quad-source"))
                .chain(
                    components
                        .iter()
                        .map(|component| component.descriptor.name.clone()),
                )
                .collect();
        let summary = crate::io::vortex_rdf_store_layout::write_native_rdf_store(
            &NATIVE_FILE_SESSION,
            &mut writer,
            stream,
            inner.build(),
            components,
        )
        .await
        .map_err(VortexRdfError::from)?;
        writer
            .sync_all()
            .await
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
        drop(writer);
        if summary.footer().layout().encoding_id().as_ref()
            != crate::io::vortex_rdf_store_layout::VORTEX_RDF_STORE_LAYOUT_ID
        {
            return Err(VortexRdfError::Serialization(
                "v10 writer returned an unexpected root layout".into(),
            ));
        }
        let reopened = NATIVE_FILE_SESSION
            .open_options()
            .open_path(&staging_path)
            .await
            .map_err(VortexRdfError::from)?;
        let root = reopened.footer().layout();
        if root.encoding_id().as_ref()
            != crate::io::vortex_rdf_store_layout::VORTEX_RDF_STORE_LAYOUT_ID
            || root.nchildren() != expected_component_names.len()
            || root.child_names().collect::<Vec<_>>() != expected_component_names
            || root.row_count() != summary.row_count()
        {
            return Err(VortexRdfError::Deserialization(
                "native RDF store configured component inventory validation failed".into(),
            ));
        }
        reopened.scan().map_err(VortexRdfError::from)?;
        std::fs::rename(&staging_path, output_path)
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_file(&staging_path);
    }
    result
}

#[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
async fn write_array_stream_to_vortex_file_streaming<W>(
    writer: &mut W,
    arrays: Pin<Box<dyn Stream<Item = VortexResult<ArrayRef>> + Send>>,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
    manifest: &NativeArtifactManifest,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let dtype = empty_spog_array()?.dtype().clone();
    let stream = ArrayStreamAdapter::new(dtype, arrays);
    let strategy_builder =
        WriteStrategyBuilder::default().with_row_block_size(row_group_size.max(1));
    let strategy_builder = match compression_profile {
        CottasVortexCompressionProfile::Balanced => strategy_builder,
        CottasVortexCompressionProfile::Compact => strategy_builder
            .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact()),
    };

    let manifest_bytes = manifest.to_metadata_bytes()?;
    let start = Instant::now();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy_builder.build())
        .with_metadata_segment(NATIVE_ARTIFACT_METADATA_KEY, manifest_bytes)
        .write(writer, stream)
        .await
        .map_err(VortexRdfError::from)?;
    log::debug!(
        "[cottas_native_ids] streamed ID-only Vortex data file in {:?}",
        start.elapsed()
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct ResolvedNativePattern {
    s: Option<u32>,
    p: Option<u32>,
    o: Option<u32>,
    g: Option<u32>,
}

impl ResolvedNativePattern {
    fn filter(self) -> NativePatternFilter {
        let mut filters = Vec::new();
        if let Some(id) = self.s {
            filters.push(eq(col("s"), lit(id)));
        }
        if let Some(id) = self.p {
            filters.push(eq(col("p"), lit(id)));
        }
        if let Some(id) = self.o {
            filters.push(eq(col("o"), lit(id)));
        }
        if let Some(id) = self.g {
            filters.push(eq(col("g"), lit(id)));
        }
        match filters.into_iter().reduce(and) {
            Some(expr) => NativePatternFilter::Expr(expr),
            None => NativePatternFilter::All,
        }
    }
}

// VORTEX_RDF_NATIVE_COMPONENT_READER_V1
/// One named top-level component visible through the native RDF store reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeRdfStoreComponentInfo {
    pub name: Arc<str>,
    pub role: Option<StoreComponentRole>,
    pub implementation: Arc<str>,
    pub version: u32,
    pub required: bool,
    pub dtype: vortex_array::dtype::DType,
    pub row_count: u64,
}

/// File-level reader for a transparent native RDF store generation.
///
/// The generic Vortex file scan remains the QuadSource scan. Auxiliary layouts
/// are addressed explicitly by their stable persisted component names.
#[derive(Clone)]
pub struct NativeRdfStoreFile {
    file: vortex_file::VortexFile,
}

impl NativeRdfStoreFile {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let profile = std::env::var_os("VORTEX_RDF_PROFILE_MATCH").is_some();
        let total_start = Instant::now();
        let open_start = Instant::now();
        let file = NATIVE_FILE_SESSION
            .open_options()
            .open_path(path.as_ref())
            .await
            .map_err(VortexRdfError::from)?
            .with_caching();
        let open_elapsed = open_start.elapsed();
        let validate_start = Instant::now();
        let store = Self::try_new(file)?;
        if profile {
            eprintln!(
                "[vortex-rdf-profile] layer=core operation=open path={} file_open_ms={:.3} validate_ms={:.3} total_ms={:.3}",
                path.as_ref().display(),
                open_elapsed.as_secs_f64() * 1_000.0,
                validate_start.elapsed().as_secs_f64() * 1_000.0,
                total_start.elapsed().as_secs_f64() * 1_000.0,
            );
        }
        Ok(store)
    }

    pub fn try_new(file: vortex_file::VortexFile) -> Result<Self> {
        let root = file.footer().layout();
        if root.encoding_id().as_ref()
            != crate::io::vortex_rdf_store_layout::VORTEX_RDF_STORE_LAYOUT_ID
        {
            return Err(VortexRdfError::Deserialization(format!(
                "expected {}, found {}",
                crate::io::vortex_rdf_store_layout::VORTEX_RDF_STORE_LAYOUT_ID,
                root.encoding_id()
            )));
        }
        let typed = root
            .as_opt::<crate::io::vortex_rdf_store_layout::VortexRdfStore>()
            .ok_or_else(|| {
                VortexRdfError::Deserialization(
                    "native RDF store layout was not registered in the opening session".into(),
                )
            })?;
        // Materialize every child once to validate persisted dtypes and metadata.
        for index in 0..root.nchildren() {
            root.child(index).map_err(VortexRdfError::from)?;
        }
        let _ = typed;
        Ok(Self { file })
    }

    pub fn vortex_file(&self) -> &vortex_file::VortexFile {
        &self.file
    }

    pub fn component_names(&self) -> Vec<Arc<str>> {
        self.file.footer().layout().child_names().collect()
    }

    pub fn components(&self) -> Result<Vec<NativeRdfStoreComponentInfo>> {
        let root = self.file.footer().layout();
        let typed = root.as_::<crate::io::vortex_rdf_store_layout::VortexRdfStore>();
        let metadata = crate::io::vortex_rdf_store_layout::vortex_rdf_store_components(typed);
        let mut components = Vec::with_capacity(root.nchildren());
        components.push(NativeRdfStoreComponentInfo {
            name: "quad-source".into(),
            role: None,
            implementation: "configured-quad-source".into(),
            version: 1,
            required: true,
            dtype: root.child(0).map_err(VortexRdfError::from)?.dtype().clone(),
            row_count: root.child(0).map_err(VortexRdfError::from)?.row_count(),
        });
        for (index, descriptor) in metadata.iter().enumerate() {
            let child_index = index + 1;
            components.push(NativeRdfStoreComponentInfo {
                name: descriptor.name.clone(),
                role: Some(descriptor.role),
                implementation: descriptor.implementation.clone(),
                version: descriptor.version,
                required: descriptor.required,
                dtype: descriptor.dtype.clone(),
                row_count: root
                    .child(child_index)
                    .map_err(VortexRdfError::from)?
                    .row_count(),
            });
        }
        Ok(components)
    }

    pub fn component_layout(&self, name: &str) -> Result<Option<vortex_layout::LayoutRef>> {
        let root = self.file.footer().layout();
        let Some(index) = root.child_names().position(|child| child.as_ref() == name) else {
            return Ok(None);
        };
        root.child(index).map(Some).map_err(VortexRdfError::from)
    }

    pub fn component_scan(
        &self,
        name: &str,
    ) -> Result<Option<vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>>> {
        let Some(layout) = self.component_layout(name)? else {
            return Ok(None);
        };
        let reader = layout
            .new_reader(
                name.into(),
                self.file.segment_source(),
                self.file.session(),
                &Default::default(),
            )
            .map_err(VortexRdfError::from)?;
        Ok(Some(vortex_layout::scan::scan_builder::ScanBuilder::new(
            self.file.session().clone(),
            reader,
        )))
    }

    // VORTEX_RDF_NATIVE_DICTIONARY_LOOKUPS_V1
    /// Resolve one lexical RDF term through the native lexically sorted dictionary.
    pub async fn lookup_term_id(&self, term: &str) -> Result<Option<u32>> {
        let scan = self
            .component_scan("dictionary.term-to-id")?
            .ok_or_else(|| {
                VortexRdfError::Deserialization(
                    "native RDF store is missing required dictionary.term-to-id component".into(),
                )
            })?;
        let result = scan
            .with_filter(eq(col("term"), lit(term.to_owned())))
            .with_projection(vortex_array::expr::select(
                ["id"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        match result.len() {
            0 => Ok(None),
            1 => {
                let ids = extract_projected_u32_column(&result, "id")?;
                if ids.len() != 1 {
                    return Err(VortexRdfError::Deserialization(format!(
                        "dictionary.term-to-id returned {} IDs for one matching row",
                        ids.len()
                    )));
                }
                Ok(Some(ids[0]))
            }
            rows => Err(VortexRdfError::Deserialization(format!(
                "dictionary.term-to-id returned {rows} rows for one lexical term"
            ))),
        }
    }

    /// Resolve IDs through the native ID-ordered dictionary.
    ///
    /// Row selection uses the persisted `row number == ID` invariant and then
    /// validates every returned ID, preventing silent positional corruption.
    pub async fn lookup_terms_by_ids(&self, ids: &[u32]) -> Result<HashMap<u32, String>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let layout = self
            .component_layout("dictionary.id-to-term")?
            .ok_or_else(|| {
                VortexRdfError::Deserialization(
                    "native RDF store is missing required dictionary.id-to-term component".into(),
                )
            })?;
        let mut requested = ids.to_vec();
        requested.sort_unstable();
        requested.dedup();
        let row_count = layout.row_count();
        if let Some(invalid) = requested
            .iter()
            .copied()
            .find(|id| u64::from(*id) >= row_count)
        {
            return Err(VortexRdfError::InvalidOperation(format!(
                "dictionary ID {invalid} is outside dictionary row range 0..{row_count}"
            )));
        }
        let scan = self
            .component_scan("dictionary.id-to-term")?
            .ok_or_else(|| {
                VortexRdfError::Deserialization(
                    "native RDF store is missing required dictionary.id-to-term component".into(),
                )
            })?;
        let result = scan
            .with_row_indices(Buffer::from(
                requested
                    .iter()
                    .map(|id| u64::from(*id))
                    .collect::<Vec<_>>(),
            ))
            .with_projection(vortex_array::expr::select(
                ["id", "term"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        let loaded_ids = extract_projected_u32_column(&result, "id")?;
        let terms = extract_projected_utf8_column(&result, "term")?;
        if loaded_ids.len() != requested.len() || terms.len() != requested.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "dictionary.id-to-term returned ids={}, terms={}, requested={}",
                loaded_ids.len(),
                terms.len(),
                requested.len()
            )));
        }
        let mut resolved = HashMap::with_capacity(requested.len());
        for ((expected, actual), term) in requested.iter().zip(loaded_ids).zip(terms) {
            if *expected != actual {
                return Err(VortexRdfError::Deserialization(format!(
                    "dictionary.id-to-term positional invariant failed: requested ID {}, row contained ID {}",
                    expected, actual
                )));
            }
            if resolved.insert(actual, term).is_some() {
                return Err(VortexRdfError::Deserialization(format!(
                    "dictionary.id-to-term returned duplicate ID {actual}"
                )));
            }
        }
        Ok(resolved)
    }

    async fn lookup_native_object_exact_ranges(
        &self,
        object_id: u32,
    ) -> Result<Option<Vec<Range<u64>>>> {
        let Some(directory) = self.component_scan("index.object.exact-ranges.directory")? else {
            return Ok(None);
        };
        let Some(payload) = self.component_scan("index.object.exact-ranges.payload")? else {
            return Err(VortexRdfError::Deserialization(
                "object exact v2 directory exists without payload".into(),
            ));
        };
        let entry = directory
            .with_filter(eq(col("object_id"), lit(object_id)))
            .with_projection(vortex_array::expr::select(
                ["range_offset", "range_count", "candidate_rows"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        if entry.len() == 0 {
            return Ok(Some(Vec::new()));
        }
        if entry.len() != 1 {
            return Err(VortexRdfError::Deserialization(
                "object exact v2 directory entry is not unique".into(),
            ));
        }
        let offsets = extract_projected_u64_column(&entry, "range_offset")?;
        let counts = extract_projected_u32_column(&entry, "range_count")?;
        let candidates = extract_projected_u64_column(&entry, "candidate_rows")?;
        let end = checked_payload_end(offsets[0], counts[0], "object exact v2")?;
        let selected = payload
            .with_row_range(offsets[0]..end)
            .with_projection(vortex_array::expr::select(
                ["row_start", "row_end"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        let starts = extract_projected_u64_column(&selected, "row_start")?;
        let ends = extract_projected_u64_column(&selected, "row_end")?;
        Ok(Some(decode_exact_range_columns(
            starts,
            ends,
            counts[0],
            candidates[0],
            "object exact v2",
        )?))
    }

    async fn lookup_native_predicate_object_exact_ranges(
        &self,
        predicate_id: u32,
        object_id: u32,
    ) -> Result<Option<Vec<Range<u64>>>> {
        let Some(partitions) =
            self.component_scan("index.predicate-object.predicate-partitions")?
        else {
            return Ok(None);
        };
        if self
            .component_layout("index.predicate-object.exact-ranges.directory")?
            .is_none()
            || self
                .component_layout("index.predicate-object.exact-ranges.payload")?
                .is_none()
        {
            return Err(VortexRdfError::Deserialization(
                "predicate-object exact v2 component set is incomplete".into(),
            ));
        }
        let partition = partitions
            .with_filter(eq(col("predicate_id"), lit(predicate_id)))
            .with_projection(vortex_array::expr::select(
                ["directory_start", "directory_end"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        if partition.len() == 0 {
            return Ok(Some(Vec::new()));
        }
        if partition.len() != 1 {
            return Err(VortexRdfError::Deserialization(
                "predicate-object partition is not unique".into(),
            ));
        }
        let starts = extract_projected_u64_column(&partition, "directory_start")?;
        let ends = extract_projected_u64_column(&partition, "directory_end")?;
        if starts[0] >= ends[0] {
            return Err(VortexRdfError::Deserialization(
                "predicate-object partition is empty or reversed".into(),
            ));
        }
        let directory = self
            .component_scan("index.predicate-object.exact-ranges.directory")?
            .unwrap();
        let entry = directory
            .with_row_range(starts[0]..ends[0])
            .with_filter(eq(col("object_id"), lit(object_id)))
            .with_projection(vortex_array::expr::select(
                [
                    "predicate_id",
                    "range_offset",
                    "range_count",
                    "candidate_rows",
                ],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        if entry.len() == 0 {
            return Ok(Some(Vec::new()));
        }
        if entry.len() != 1 {
            return Err(VortexRdfError::Deserialization(
                "predicate-object directory entry is not unique".into(),
            ));
        }
        let predicates = extract_projected_u32_column(&entry, "predicate_id")?;
        if predicates != [predicate_id] {
            return Err(VortexRdfError::Deserialization(
                "predicate-object partition addressed another predicate".into(),
            ));
        }
        let offsets = extract_projected_u64_column(&entry, "range_offset")?;
        let counts = extract_projected_u32_column(&entry, "range_count")?;
        let candidates = extract_projected_u64_column(&entry, "candidate_rows")?;
        let end = checked_payload_end(offsets[0], counts[0], "predicate-object")?;
        let payload = self
            .component_scan("index.predicate-object.exact-ranges.payload")?
            .unwrap()
            .with_row_range(offsets[0]..end)
            .with_projection(vortex_array::expr::select(
                ["row_start", "row_end"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        let row_starts = extract_projected_u64_column(&payload, "row_start")?;
        let row_ends = extract_projected_u64_column(&payload, "row_end")?;
        Ok(Some(decode_exact_range_columns(
            row_starts,
            row_ends,
            counts[0],
            candidates[0],
            "predicate-object exact v2",
        )?))
    }

    async fn lookup_native_predicate_exact_ranges(
        &self,
        predicate_id: u32,
    ) -> Result<Option<Vec<Range<u64>>>> {
        let Some(directory) = self.component_scan("index.predicate.exact-ranges.directory")? else {
            return Ok(None);
        };
        let result = directory
            .with_filter(eq(col("predicate_id"), lit(predicate_id)))
            .with_projection(vortex_array::expr::select(
                ["range_offset", "range_count", "candidate_rows"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        if result.len() == 0 {
            return Ok(Some(Vec::new()));
        }
        if result.len() != 1 {
            return Err(VortexRdfError::Deserialization(format!(
                "predicate exact v2 directory returned {} rows for predicate {predicate_id}",
                result.len()
            )));
        }
        let offsets = extract_projected_u64_column(&result, "range_offset")?;
        let counts = extract_projected_u32_column(&result, "range_count")?;
        let candidates = extract_projected_u64_column(&result, "candidate_rows")?;
        let offset = offsets[0];
        let count = counts[0];
        let Some(payload) = self.component_scan("index.predicate.exact-ranges.payload")? else {
            return Err(VortexRdfError::Deserialization(
                "predicate exact v2 directory exists without payload".into(),
            ));
        };
        let end = checked_payload_end(offset, count, "predicate exact v2")?;
        let payload_result = payload
            .with_row_range(offset..end)
            .with_projection(vortex_array::expr::select(
                ["row_start", "row_end"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        let starts = extract_projected_u64_column(&payload_result, "row_start")?;
        let ends = extract_projected_u64_column(&payload_result, "row_end")?;
        Ok(Some(decode_exact_range_columns(
            starts,
            ends,
            count,
            candidates[0],
            "predicate exact v2",
        )?))
    }

    async fn lookup_native_predicate_ranges(
        &self,
        predicate_id: u32,
    ) -> Result<Option<Vec<Range<u64>>>> {
        let Some(scan) = self.component_scan("index.predicate-runs")? else {
            return Ok(None);
        };
        let started = Instant::now();
        let result = scan
            .with_filter(eq(col("predicate_id"), lit(predicate_id)))
            .with_projection(vortex_array::expr::select(
                ["row_start", "row_end"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        let starts = extract_projected_u64_column(&result, "row_start")?;
        let ends = extract_projected_u64_column(&result, "row_end")?;
        if starts.len() != ends.len() || starts.len() != result.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "index.predicate-runs returned inconsistent columns for predicate ID {predicate_id}"
            )));
        }
        let mut ranges = Vec::with_capacity(starts.len());
        let mut previous_end = None;
        for (start, end) in starts.into_iter().zip(ends) {
            if start >= end || previous_end.is_some_and(|previous| start < previous) {
                return Err(VortexRdfError::Deserialization(format!(
                    "index.predicate-runs returned invalid or unsorted range {start}..{end} for predicate ID {predicate_id}"
                )));
            }
            previous_end = Some(end);
            ranges.push(start..end);
        }
        log::debug!(
            "[native-rdf-store] predicate run lookup predicate_id={} ranges={} candidate_rows={} elapsed={:?}",
            predicate_id,
            ranges.len(),
            ranges
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>(),
            started.elapsed()
        );
        Ok(Some(ranges))
    }

    // VORTEX_RDF_NATIVE_SUBJECT_RANGE_MATCHING_V1
    async fn lookup_native_subject_range(&self, subject_id: u32) -> Result<Option<Range<u64>>> {
        let Some(scan) = self.component_scan("index.subject-ranges")? else {
            return Ok(None);
        };
        let started = Instant::now();
        let result = scan
            .with_filter(eq(col("subject_id"), lit(subject_id)))
            .with_projection(vortex_array::expr::select(
                ["row_start", "row_end"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        log::debug!(
            "[native-rdf-store] subject range lookup subject_id={} rows={} elapsed={:?}",
            subject_id,
            result.len(),
            started.elapsed()
        );
        match result.len() {
            0 => Ok(Some(0..0)),
            1 => {
                let starts = extract_projected_u64_column(&result, "row_start")?;
                let ends = extract_projected_u64_column(&result, "row_end")?;
                if starts.len() != 1 || ends.len() != 1 || starts[0] >= ends[0] {
                    return Err(VortexRdfError::Deserialization(format!(
                        "index.subject-ranges returned invalid range columns for subject ID {subject_id}: starts={starts:?}, ends={ends:?}"
                    )));
                }
                Ok(Some(starts[0]..ends[0]))
            }
            rows => Err(VortexRdfError::Deserialization(format!(
                "index.subject-ranges returned {rows} rows for subject ID {subject_id}; expected at most one"
            ))),
        }
    }

    // VORTEX_RDF_NATIVE_INDEX_INDEPENDENT_MATCHING_V1
    /// Match one RDF quad pattern against the native QuadSource.
    ///
    /// `Disabled` is the correctness baseline. `Auto` currently takes the same
    /// path until compatible native index components are added. `Required`
    /// fails explicitly rather than silently scanning.
    ///
    /// FUTURE(performance): `Auto` will select candidate row ranges from native
    /// indexes, but the exact QuadSource filter below must remain mandatory.
    pub async fn match_pattern_with_policy(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
        policy: NativeIndexPolicy,
    ) -> Result<Vec<(String, String, String, String)>> {
        let profile = std::env::var_os("VORTEX_RDF_PROFILE_MATCH").is_some();
        let total_started = Instant::now();
        let bound_started = Instant::now();
        let bound = BoundNativeRdfTerms::from_pattern(subject, predicate, object, graph);
        let bound_elapsed = bound_started.elapsed();
        let dictionary_started = Instant::now();
        let mut dictionary_lookups = 0usize;
        let requested = [
            (bound.s.as_deref(), "s"),
            (bound.p.as_deref(), "p"),
            (bound.o.as_deref(), "o"),
            (bound.g.as_deref(), "g"),
        ];
        let mut resolved = ResolvedNativePattern::default();
        for (term, column) in requested {
            let Some(term) = term else { continue };
            dictionary_lookups += 1;
            let Some(id) = self.lookup_term_id(term).await? else {
                if profile {
                    eprintln!(
                        "[vortex-rdf-profile] layer=core operation=match_stages outcome=dictionary-miss column={} rows=0 bound_ms={:.3} dictionary_ms={:.3} dictionary_lookups={} availability_ms=0.000 index_ms=0.000 scan_setup_ms=0.000 scan_read_ms=0.000 reconstruct_lookup_ms=0.000 materialize_ms=0.000 total_ms={:.3}",
                        column,
                        bound_elapsed.as_secs_f64() * 1_000.0,
                        dictionary_started.elapsed().as_secs_f64() * 1_000.0,
                        dictionary_lookups,
                        total_started.elapsed().as_secs_f64() * 1_000.0,
                    );
                }
                return Ok(Vec::new());
            };
            match column {
                "s" => resolved.s = Some(id),
                "p" => resolved.p = Some(id),
                "o" => resolved.o = Some(id),
                "g" => resolved.g = Some(id),
                _ => unreachable!("only native SPOG columns are resolved"),
            }
        }

        let dictionary_elapsed = dictionary_started.elapsed();
        let availability_started = Instant::now();
        let subject_index_available = self.component_layout("index.subject-ranges")?.is_some();
        let predicate_exact_available = self
            .component_layout("index.predicate.exact-ranges.directory")?
            .is_some()
            && self
                .component_layout("index.predicate.exact-ranges.payload")?
                .is_some();
        let predicate_run_available = self.component_layout("index.predicate-runs")?.is_some();
        let predicate_index_available = predicate_exact_available || predicate_run_available;
        let predicate_object_index_available = self
            .component_layout("index.predicate-object.predicate-partitions")?
            .is_some()
            && self
                .component_layout("index.predicate-object.exact-ranges.directory")?
                .is_some()
            && self
                .component_layout("index.predicate-object.exact-ranges.payload")?
                .is_some();
        let object_index_available = self
            .component_layout("index.object.exact-ranges.directory")?
            .is_some()
            && self
                .component_layout("index.object.exact-ranges.payload")?
                .is_some();
        let quad_source_rows = self.file.footer().layout().row_count();
        let availability_elapsed = availability_started.elapsed();
        let index_started = Instant::now();
        let mut available_index = "none";
        let mut fallback_reason = "none";
        let (candidate_ranges, access) = if policy == NativeIndexPolicy::Disabled {
            fallback_reason = "policy-disabled";
            (None, "full-scan")
        } else if let Some(subject_id) = resolved.s {
            available_index = if subject_index_available {
                "index.subject-ranges"
            } else {
                "none"
            };
            if subject_index_available {
                (
                    self.lookup_native_subject_range(subject_id)
                        .await?
                        .map(|range| vec![range]),
                    "index.subject-ranges",
                )
            } else if policy == NativeIndexPolicy::Required {
                return Err(VortexRdfError::InvalidOperation(
                    "NativeIndexPolicy::Required requested for a subject-bound pattern, but index.subject-ranges is absent"
                        .into(),
                ));
            } else {
                fallback_reason = "subject-index-absent";
                (None, "full-scan")
            }
        } else if let (Some(predicate_id), Some(object_id)) = (resolved.p, resolved.o) {
            available_index = if predicate_object_index_available {
                "index.predicate-object.exact-ranges"
            } else {
                "none"
            };
            if predicate_object_index_available {
                let ranges = self
                    .lookup_native_predicate_object_exact_ranges(predicate_id, object_id)
                    .await?
                    .unwrap_or_default();
                (Some(ranges), available_index)
            } else if policy == NativeIndexPolicy::Required {
                return Err(VortexRdfError::InvalidOperation(
                    "NativeIndexPolicy::Required requested for a predicate+object-bound pattern, but the complete predicate-object exact-ranges v2 component set is absent".into()));
            } else {
                fallback_reason = "predicate-object-index-absent";
                (None, "full-scan")
            }
        } else if let Some(predicate_id) = resolved.p {
            available_index = if predicate_exact_available {
                "index.predicate.exact-ranges"
            } else if predicate_run_available {
                "index.predicate-runs"
            } else {
                "none"
            };
            if predicate_index_available {
                let ranges = if predicate_exact_available {
                    self.lookup_native_predicate_exact_ranges(predicate_id)
                        .await?
                        .unwrap_or_default()
                } else {
                    self.lookup_native_predicate_ranges(predicate_id)
                        .await?
                        .unwrap_or_default()
                };
                let candidate_rows = ranges
                    .iter()
                    .map(|range| range.end - range.start)
                    .sum::<u64>();
                let range_cost_ok = ranges.len() <= NATIVE_AUTO_MAX_PREDICATE_RANGES;
                let row_cost_ok = candidate_rows
                    .saturating_mul(NATIVE_AUTO_MAX_PREDICATE_SELECTIVITY_DENOMINATOR)
                    <= quad_source_rows
                        .saturating_mul(NATIVE_AUTO_MAX_PREDICATE_SELECTIVITY_NUMERATOR);
                if policy == NativeIndexPolicy::Required || (range_cost_ok && row_cost_ok) {
                    (Some(ranges), available_index)
                } else {
                    fallback_reason = if !range_cost_ok && !row_cost_ok {
                        "predicate-index-too-fragmented-and-unselective"
                    } else if !range_cost_ok {
                        "predicate-index-too-fragmented"
                    } else {
                        "predicate-index-not-selective"
                    };
                    (None, "full-scan")
                }
            } else if policy == NativeIndexPolicy::Required {
                return Err(VortexRdfError::InvalidOperation(
                    "NativeIndexPolicy::Required requested for a predicate-bound pattern, but index.predicate-runs is absent"
                        .into(),
                ));
            } else {
                fallback_reason = "predicate-index-absent";
                (None, "full-scan")
            }
        } else if let Some(object_id) = resolved.o {
            available_index = if object_index_available {
                "index.object.exact-ranges"
            } else {
                "none"
            };
            if object_index_available {
                let ranges = self
                    .lookup_native_object_exact_ranges(object_id)
                    .await?
                    .unwrap_or_default();
                (Some(ranges), available_index)
            } else if policy == NativeIndexPolicy::Required {
                return Err(VortexRdfError::InvalidOperation(
                    "NativeIndexPolicy::Required requested for an object-bound pattern, but the complete object exact-ranges v2 component set is absent".into()));
            } else {
                fallback_reason = "object-index-absent";
                (None, "full-scan")
            }
        } else if policy == NativeIndexPolicy::Required {
            return Err(VortexRdfError::InvalidOperation(
                "NativeIndexPolicy::Required requested, but the query has no compatible bound subject, predicate, or object"
                    .into(),
            ));
        } else {
            fallback_reason = "no-compatible-bound-column";
            (None, "full-scan")
        };
        let index_elapsed = index_started.elapsed();
        if candidate_ranges.as_ref().is_some_and(Vec::is_empty)
            || candidate_ranges
                .as_ref()
                .is_some_and(|ranges| ranges.len() == 1 && ranges[0].is_empty())
        {
            if profile {
                eprintln!(
                    "[vortex-rdf-profile] layer=core operation=match_stages outcome=index-miss access={} rows=0 bound_ms={:.3} dictionary_ms={:.3} dictionary_lookups={} availability_ms={:.3} index_ms={:.3} scan_setup_ms=0.000 scan_read_ms=0.000 reconstruct_lookup_ms=0.000 materialize_ms=0.000 total_ms={:.3}",
                    access,
                    bound_elapsed.as_secs_f64() * 1_000.0,
                    dictionary_elapsed.as_secs_f64() * 1_000.0,
                    dictionary_lookups,
                    availability_elapsed.as_secs_f64() * 1_000.0,
                    index_elapsed.as_secs_f64() * 1_000.0,
                    total_started.elapsed().as_secs_f64() * 1_000.0,
                );
            }
            return Ok(Vec::new());
        }

        let projection = native_projection_columns_for_bound_terms(&bound);
        let scan_started = Instant::now();
        let scan = self.file.scan().map_err(VortexRdfError::from)?;
        let scan = match &candidate_ranges {
            Some(ranges) if ranges.len() == 1 => scan.with_row_range(ranges[0].clone()),
            Some(ranges) => {
                let rows = ranges.iter().map(|range| range.end - range.start).sum();
                scan.with_row_indices(exact_ranges_to_row_indices(ranges, rows)?)
            }
            None => scan,
        };
        let scan = match resolved.filter() {
            NativePatternFilter::All => scan,
            NativePatternFilter::Empty => return Ok(Vec::new()),
            NativePatternFilter::Expr(expr) => scan.with_filter(expr),
        };
        let candidate_range_count = candidate_ranges.as_ref().map_or(0, Vec::len);
        let candidate_row_count = candidate_ranges.as_ref().map_or(0, |ranges| {
            ranges
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>()
        });
        let candidate_selectivity = if quad_source_rows == 0 {
            0.0
        } else {
            candidate_row_count as f64 / quad_source_rows as f64
        };
        log::debug!(
            "[native-rdf-store] match policy={:?} available_index={} selected={} candidate_ranges={} candidate_rows={} total_rows={} selectivity={:.6} fallback={} setup={:?}",
            policy,
            available_index,
            access,
            candidate_range_count,
            candidate_row_count,
            quad_source_rows,
            candidate_selectivity,
            fallback_reason,
            scan_started.elapsed()
        );
        let stream = scan
            .with_projection(vortex_array::expr::select(
                projection.as_slice(),
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?;
        let scan_setup_elapsed = scan_started.elapsed();
        let read_started = Instant::now();
        let (rows, batches, max_batch_rows) =
            read_native_projected_stream_all_with_scan_stats(stream).await?;
        let scan_read_elapsed = read_started.elapsed();
        log::debug!(
            "[native-rdf-store] match scan rows={} batches={} max_batch_rows={} elapsed={:?}",
            rows.rows,
            batches,
            max_batch_rows,
            scan_read_elapsed
        );
        if rows.rows == 0 {
            if profile {
                eprintln!(
                    "[vortex-rdf-profile] layer=core operation=match_stages outcome=scan-empty access={} rows=0 bound_ms={:.3} dictionary_ms={:.3} dictionary_lookups={} availability_ms={:.3} index_ms={:.3} scan_setup_ms={:.3} scan_read_ms={:.3} reconstruct_lookup_ms=0.000 materialize_ms=0.000 total_ms={:.3}",
                    access,
                    bound_elapsed.as_secs_f64() * 1_000.0,
                    dictionary_elapsed.as_secs_f64() * 1_000.0,
                    dictionary_lookups,
                    availability_elapsed.as_secs_f64() * 1_000.0,
                    index_elapsed.as_secs_f64() * 1_000.0,
                    scan_setup_elapsed.as_secs_f64() * 1_000.0,
                    scan_read_elapsed.as_secs_f64() * 1_000.0,
                    total_started.elapsed().as_secs_f64() * 1_000.0,
                );
            }
            return Ok(Vec::new());
        }

        let reconstruct_started = Instant::now();
        let requested_ids = rows.unique_unbound_ids(&bound);
        let requested_id_count = requested_ids.len();
        let id_to_term = self.lookup_terms_by_ids(&requested_ids).await?;
        let reconstruct_lookup_elapsed = reconstruct_started.elapsed();
        let materialize_started = Instant::now();
        let mut quads = Vec::with_capacity(rows.rows);
        for row in 0..rows.rows {
            let subject_id = rows.id_at(NativeIdColumn::Subject, &bound, row)?;
            let predicate_id = rows.id_at(NativeIdColumn::Predicate, &bound, row)?;
            let object_id = rows.id_at(NativeIdColumn::Object, &bound, row)?;
            let graph_id = rows.id_at(NativeIdColumn::Graph, &bound, row)?;
            let subject = lookup_projected_or_use_bound(&id_to_term, &bound.s, subject_id, "S")?;
            let predicate =
                lookup_projected_or_use_bound(&id_to_term, &bound.p, predicate_id, "P")?;
            let object = lookup_projected_or_use_bound(&id_to_term, &bound.o, object_id, "O")?;
            let graph = lookup_projected_or_use_bound(&id_to_term, &bound.g, graph_id, "G")?;
            quads.push((
                subject.to_owned(),
                predicate.to_owned(),
                object.to_owned(),
                graph.to_owned(),
            ));
        }
        let materialize_elapsed = materialize_started.elapsed();
        if profile {
            eprintln!(
                "[vortex-rdf-profile] layer=core operation=match_stages outcome=ok access={} rows={} requested_ids={} bound_ms={:.3} dictionary_ms={:.3} dictionary_lookups={} availability_ms={:.3} index_ms={:.3} scan_setup_ms={:.3} scan_read_ms={:.3} reconstruct_lookup_ms={:.3} materialize_ms={:.3} total_ms={:.3}",
                access,
                quads.len(),
                requested_id_count,
                bound_elapsed.as_secs_f64() * 1_000.0,
                dictionary_elapsed.as_secs_f64() * 1_000.0,
                dictionary_lookups,
                availability_elapsed.as_secs_f64() * 1_000.0,
                index_elapsed.as_secs_f64() * 1_000.0,
                scan_setup_elapsed.as_secs_f64() * 1_000.0,
                scan_read_elapsed.as_secs_f64() * 1_000.0,
                reconstruct_lookup_elapsed.as_secs_f64() * 1_000.0,
                materialize_elapsed.as_secs_f64() * 1_000.0,
                total_started.elapsed().as_secs_f64() * 1_000.0,
            );
        }
        Ok(quads)
    }

    /// Convenience correctness-baseline matcher that never uses indexes.
    pub async fn match_pattern_without_indexes(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Vec<(String, String, String, String)>> {
        self.match_pattern_with_policy(
            subject,
            predicate,
            object,
            graph,
            NativeIndexPolicy::Disabled,
        )
        .await
    }

    /// Render stable logical names even when a generic explorer displays
    /// unregistered custom-layout children positionally as 0, 1, 2, ...
    pub fn named_layout_tree(&self) -> Result<String> {
        let components = self.components()?;
        let mut tree = String::from("vortex.rdf.store.v1\n");
        for (index, component) in components.iter().enumerate() {
            let branch = if index + 1 == components.len() {
                "└──"
            } else {
                "├──"
            };
            use std::fmt::Write as _;
            writeln!(
                tree,
                "{branch} {} (role={}, rows={}, dtype={})",
                component.name,
                component
                    .role
                    .map(|role| format!("{role:?}"))
                    .unwrap_or_else(|| "QuadSource".to_string()),
                component.row_count,
                component.dtype
            )
            .expect("writing to String cannot fail");
        }
        Ok(tree)
    }
}

pub async fn inspect_native_rdf_store_file(path: impl AsRef<Path>) -> Result<String> {
    NativeRdfStoreFile::open(path).await?.named_layout_tree()
}
// VORTEX_RDF_NATIVE_MATCH_CLI_V1
/// Open and match a native RDF store, then serialize lexical triples.
pub async fn match_native_rdf_store_file<W>(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    policy: NativeIndexPolicy,
    writer: W,
    format: RdfFormat,
) -> Result<usize>
where
    W: Write,
{
    let store = NativeRdfStoreFile::open(input_path).await?;
    let quads = store
        .match_pattern_with_policy(subject, predicate, object, graph, policy)
        .await?;
    let count = quads.len();
    let mut serializer = RdfSerializer::from_format(format).for_writer(writer);
    for (subject, predicate, object, graph) in quads {
        let quad = oxrdf::Quad::new(
            crate::common::utils::parse_subject(&subject)?,
            crate::common::utils::parse_named_node(&predicate)?,
            crate::common::utils::parse_term(&object)?,
            crate::common::utils::parse_graph_name(&graph)?,
        );
        serializer
            .serialize_quad(&quad)
            .map_err(|error| VortexRdfError::Deserialization(error.to_string()))?;
    }
    serializer
        .finish()
        .map_err(|error| VortexRdfError::Deserialization(error.to_string()))?;
    Ok(count)
}

/// Controls whether matching may use optional native index components.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NativeIndexPolicy {
    /// Use compatible indexes when available and otherwise scan the QuadSource.
    #[default]
    Auto,
    /// Ignore indexes and execute the correctness baseline scan path.
    Disabled,
    /// Require a compatible index for the query shape or return an error.
    Required,
}

// Indexes only select candidate rows. Exact QuadSource filters remain mandatory,
// so indexed and index-free matching have identical semantics.

enum NativePatternFilter {
    /// No bound RDF terms, so scan all rows.
    All,

    /// At least one bound RDF term was not present in the dictionary.
    /// Therefore the result is definitely empty.
    #[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
    Empty,

    /// A concrete Vortex filter expression over top-level s/p/o/g columns.
    Expr(Expression),
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeTermToIdLookupStats {
    pub column: Option<String>,
    pub term_len: usize,
    pub term_preview: String,
    pub found_id: Option<u32>,
    pub total_ms: f64,
    pub open_ms: f64,
    pub can_prune_ms: f64,
    pub scan_build_ms: f64,
    pub read_all_ms: f64,
    pub extract_ms: f64,
    pub can_prune: Option<bool>,
    pub strategy: String,
    pub binary_probe_count: usize,
    pub binary_entry_read_ms: f64,
    pub binary_blob_read_ms: f64,
    pub binary_metadata_ms: f64,
    pub binary_entry_bytes_read: usize,
    pub binary_blob_bytes_read: usize,
    pub binary_entries_file_bytes: u64,
    pub binary_blob_file_bytes: u64,
    pub result_array_len: usize,
}

#[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
fn native_term_preview(term: &str) -> String {
    const MAX_CHARS: usize = 160;
    let mut out = String::new();
    for (idx, ch) in term.chars().enumerate() {
        if idx >= MAX_CHARS {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out
}

#[derive(Clone, Debug, Default)]
struct BoundNativeRdfTerms {
    s: Option<String>,
    p: Option<String>,
    o: Option<String>,
    g: Option<String>,
}

impl BoundNativeRdfTerms {
    fn from_pattern(
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Self {
        Self {
            s: subject.map(|v| v.to_string()),
            p: predicate.map(|v| v.to_string()),
            o: object.map(|v| v.to_string()),
            g: graph.map(|v| v.to_string()),
        }
    }
}

fn collect_unique_ids_for_unbound_native_columns(
    s_ids: &[u32],
    p_ids: &[u32],
    o_ids: &[u32],
    g_ids: &[u32],
    bound_terms: &BoundNativeRdfTerms,
) -> Vec<u32> {
    let mut set = HashSet::new();

    if bound_terms.s.is_none() {
        for id in s_ids {
            set.insert(*id);
        }
    }

    if bound_terms.p.is_none() {
        for id in p_ids {
            set.insert(*id);
        }
    }

    if bound_terms.o.is_none() {
        for id in o_ids {
            set.insert(*id);
        }
    }

    if bound_terms.g.is_none() {
        for id in g_ids {
            set.insert(*id);
        }
    }

    let mut ids: Vec<u32> = set.into_iter().collect();
    ids.sort_unstable();
    ids
}

// VORTEX_RDF_NATIVE_ID_BATCH_V1
// Reusable columnar handoff before RDF lexical reconstruction. The compact
// RDFLib adapter consumes it today; a future Arrow/DataFusion adapter can
// consume the same native-ID columns without changing scan execution.
#[derive(Clone, Debug, Default)]
struct NativeIdBatch {
    s: Option<Vec<u32>>,
    p: Option<Vec<u32>>,
    o: Option<Vec<u32>>,
    g: Option<Vec<u32>>,
    rows: usize,
}

impl NativeIdBatch {
    fn unique_unbound_ids(&self, bound: &BoundNativeRdfTerms) -> Vec<u32> {
        let empty: &[u32] = &[];
        collect_unique_ids_for_unbound_native_columns(
            self.s.as_deref().unwrap_or(empty),
            self.p.as_deref().unwrap_or(empty),
            self.o.as_deref().unwrap_or(empty),
            self.g.as_deref().unwrap_or(empty),
            bound,
        )
    }

    fn id_at(
        &self,
        column: NativeIdColumn,
        bound: &BoundNativeRdfTerms,
        row: usize,
    ) -> Result<Option<u32>> {
        let (values, fixed, label) = match column {
            NativeIdColumn::Subject => (&self.s, &bound.s, "S"),
            NativeIdColumn::Predicate => (&self.p, &bound.p, "P"),
            NativeIdColumn::Object => (&self.o, &bound.o, "O"),
            NativeIdColumn::Graph => (&self.g, &bound.g, "G"),
        };
        if fixed.is_some() {
            return Ok(None);
        }
        values
            .as_ref()
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "{label} column was required for unbound output but was not projected"
                ))
            })?
            .get(row)
            .copied()
            .map(Some)
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "{label} projected column has no value at row {row}"
                ))
            })
    }
}

#[derive(Clone, Copy, Debug)]
enum NativeIdColumn {
    Subject,
    Predicate,
    Object,
    Graph,
}

fn native_projection_columns_for_bound_terms(
    bound_terms: &BoundNativeRdfTerms,
) -> Vec<&'static str> {
    let mut columns = Vec::new();

    if bound_terms.s.is_none() {
        columns.push("s");
    }
    if bound_terms.p.is_none() {
        columns.push("p");
    }
    if bound_terms.o.is_none() {
        columns.push("o");
    }
    if bound_terms.g.is_none() {
        columns.push("g");
    }

    // Vortex projection cannot be empty in this path because we still need row counts
    // from the filtered stream for fully-bound quad patterns.
    if columns.is_empty() {
        columns.push("s");
    }

    columns
}

fn append_optional_projected_u32_column(
    batch: &ArrayRef,
    column_name: &str,
    target: &mut Option<Vec<u32>>,
) -> Result<()> {
    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();

    let struct_array = batch
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let field = match struct_array.unmasked_field_by_name(column_name) {
        Ok(field) => field.clone(),
        Err(_) => return Ok(()),
    };

    let col = field
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let values = col.as_slice::<u32>();

    let out = target.get_or_insert_with(Vec::new);
    out.extend_from_slice(values);

    Ok(())
}

fn exact_ranges_to_row_indices(ranges: &[Range<u64>], expected_rows: u64) -> Result<Buffer<u64>> {
    let capacity = usize::try_from(expected_rows).map_err(|_| {
        VortexRdfError::InvalidOperation(format!(
            "selected row count {expected_rows} does not fit in usize"
        ))
    })?;
    let mut indices = Vec::with_capacity(capacity);
    let mut previous_end = None;
    let mut actual_rows = 0u64;

    for range in ranges {
        if range.start >= range.end {
            return Err(VortexRdfError::Deserialization(format!(
                "selected row range is empty or reversed: {}..{}",
                range.start, range.end
            )));
        }
        if let Some(end) = previous_end {
            if range.start < end {
                return Err(VortexRdfError::Deserialization(format!(
                    "selected row ranges overlap or are not sorted: previous_end={}, next={}..{}",
                    end, range.start, range.end
                )));
            }
        }
        actual_rows = actual_rows
            .checked_add(range.end - range.start)
            .ok_or_else(|| {
                VortexRdfError::Deserialization(
                    "selected row count overflow while expanding ranges".into(),
                )
            })?;
        indices.extend(range.clone());
        previous_end = Some(range.end);
    }

    if actual_rows != expected_rows || indices.len() != capacity {
        return Err(VortexRdfError::Deserialization(format!(
            "selected row count mismatch: expected={}, ranges={}, expanded={}",
            expected_rows,
            actual_rows,
            indices.len()
        )));
    }
    Ok(Buffer::from(indices))
}

async fn read_native_projected_stream_all_with_scan_stats<S>(
    stream: S,
) -> Result<(NativeIdBatch, usize, usize)>
where
    S: Stream<Item = VortexResult<ArrayRef>>,
{
    let mut stream = Box::pin(stream);

    let mut rows = NativeIdBatch::default();
    let mut batches = 0usize;
    let mut max_batch_rows = 0usize;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let batch_rows = batch.len();

        batches += 1;
        max_batch_rows = max_batch_rows.max(batch_rows);
        rows.rows += batch_rows;

        if batch_rows == 0 {
            continue;
        }

        append_optional_projected_u32_column(&batch, "s", &mut rows.s)?;
        append_optional_projected_u32_column(&batch, "p", &mut rows.p)?;
        append_optional_projected_u32_column(&batch, "o", &mut rows.o)?;
        append_optional_projected_u32_column(&batch, "g", &mut rows.g)?;
    }

    Ok((rows, batches, max_batch_rows))
}

fn lookup_projected_or_use_bound<'a>(
    id_to_term: &'a HashMap<u32, String>,
    bound: &'a Option<String>,
    projected_id: Option<u32>,
    column_label: &str,
) -> Result<&'a str> {
    if let Some(value) = bound {
        return Ok(value.as_str());
    }

    let id = projected_id.ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "{} projected ID missing for unbound column",
            column_label
        ))
    })?;

    id_to_term.get(&id).map(|s| s.as_str()).ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "{} ID {} missing from id_to_term sidecar",
            column_label, id
        ))
    })
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeIdToTermLookupStats {
    pub strategy: String,

    pub total_ms: f64,
    pub open_files_ms: f64,
    pub metadata_ms: f64,
    pub sort_dedup_ms: f64,
    pub offset_read_ms: f64,
    pub blob_read_ms: f64,
    pub utf8_decode_ms: f64,
    pub hashmap_insert_ms: f64,

    pub requested_ids_in: usize,
    pub requested_ids_unique: usize,
    pub ids_loaded: usize,

    pub offset_reads: usize,
    pub offset_bytes_read: usize,
    pub blob_reads: usize,
    pub blob_bytes_read: usize,

    pub offsets_file_bytes: u64,
    pub blob_file_bytes: u64,
}

#[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

pub async fn load_cottas_native_simple_dictionary_view(
    data_path: &Path,
) -> Result<SimpleDictionaryView> {
    let read_dict_start: Instant = Instant::now();

    let resolver = runtime_component_resolver(data_path).await?;
    let file = resolver.open(NativeComponent::DictionaryVortex).await?;

    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let dict_root = stream.read_all().await.map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native::load_cottas_native_simple_dictionary_view] loaded dictionary root array with {} rows in {:?}",
        dict_root.len(),
        read_dict_start.elapsed()
    );

    SimpleDictionaryView::from_dictionary_sidecar_root(&dict_root)
}

// VVO_TYPED_OBJECT_INDEX_V1

// VORTEX_RDF_INTEGRATED_NATIVE_PREDICATE_OBJECT_EXACT_RANGES_V2
type PreparedNativePredicateObjectExactV2 = PreparedPoExactRanges;

fn prepare_native_predicate_object_exact_v2(
    run_paths: &[PathBuf],
    temp_dir: &Path,
) -> Result<PreparedNativePredicateObjectExactV2> {
    let mut readers = run_paths
        .iter()
        .map(|path| NativeIdRunReader::new(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(triple) = reader.read_one()? {
            heap.push(IdRunHeapItem {
                triple,
                run_idx,
                ordering: TripleOrdering::SPO,
            });
        }
    }
    let mut collector = PoRangeCollector::new(temp_dir);
    while let Some(item) = heap.pop() {
        collector.push(item.triple.p, item.triple.o)?;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(IdRunHeapItem {
                triple: next,
                run_idx: item.run_idx,
                ordering: TripleOrdering::SPO,
            });
        }
    }
    collector.finish()
}

// VORTEX_RDF_INTEGRATED_NATIVE_PREDICATE_EXACT_RANGES_V2
type PreparedNativePredicateExactV2 = PreparedPredicateExactRanges;

fn prepare_native_predicate_exact_v2(
    run_paths: &[PathBuf],
    temp_dir: &Path,
) -> Result<PreparedNativePredicateExactV2> {
    let mut readers = run_paths
        .iter()
        .map(|path| NativeIdRunReader::new(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(triple) = reader.read_one()? {
            heap.push(IdRunHeapItem {
                triple,
                run_idx,
                ordering: TripleOrdering::SPO,
            });
        }
    }
    let mut collector = PredicateRangeCollector::new(temp_dir);
    while let Some(item) = heap.pop() {
        collector.push_predicate(item.triple.p)?;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(IdRunHeapItem {
                triple: next,
                run_idx: item.run_idx,
                ordering: TripleOrdering::SPO,
            });
        }
    }
    collector.finish()
}

// VORTEX_RDF_INTEGRATED_NATIVE_OBJECT_EXACT_RANGES_V2
type PreparedNativeObjectExactV2 = PreparedObjectExactRanges;

fn prepare_native_object_exact_v2(
    run_paths: &[PathBuf],
    temp_dir: &Path,
) -> Result<PreparedNativeObjectExactV2> {
    let mut readers = run_paths
        .iter()
        .map(|path| NativeIdRunReader::new(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(triple) = reader.read_one()? {
            heap.push(IdRunHeapItem {
                triple,
                run_idx,
                ordering: TripleOrdering::SPO,
            });
        }
    }
    let mut collector = ObjectRangeCollector::new(temp_dir);
    while let Some(item) = heap.pop() {
        collector.push_object(item.triple.o)?;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(IdRunHeapItem {
                triple: next,
                run_idx: item.run_idx,
                ordering: TripleOrdering::SPO,
            });
        }
    }
    collector.finish()
}

fn extract_projected_u32_column(array: &ArrayRef, column_name: &str) -> Result<Vec<u32>> {
    if array.len() == 0 {
        return Ok(Vec::new());
    }
    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();
    let struct_array = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let column = struct_array
        .unmasked_field_by_name(column_name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(column.as_slice::<u32>().to_vec())
}

fn extract_projected_u64_column(array: &ArrayRef, column_name: &str) -> Result<Vec<u64>> {
    if array.len() == 0 {
        return Ok(Vec::new());
    }
    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();
    let struct_array = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let column = struct_array
        .unmasked_field_by_name(column_name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(column.as_slice::<u64>().to_vec())
}

#[cfg_attr(not(feature = "legacy-sidecars"), allow(dead_code))]
fn extract_first_u32_from_single_column_array(
    array: &ArrayRef,
    column_name: &str,
) -> Result<Option<u32>> {
    if array.len() == 0 {
        return Ok(None);
    }

    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();

    let struct_array = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::from)?;

    let column = struct_array
        .unmasked_field_by_name(column_name)
        .map_err(VortexRdfError::from)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::from)?;

    let values = column.as_slice::<u32>();

    values.first().copied().map(Some).ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "projected column {:?} has no u32 values despite parent array len {}",
            column_name,
            array.len()
        ))
    })
}

fn extract_projected_utf8_column(array: &ArrayRef, column_name: &str) -> Result<Vec<String>> {
    if array.len() == 0 {
        return Ok(Vec::new());
    }

    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();

    let struct_array = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let column = struct_array
        .unmasked_field_by_name(column_name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    (0..column.len())
        .map(|index| {
            String::from_utf8(column.bytes_at(index).to_vec()).map_err(|error| {
                VortexRdfError::Deserialization(format!(
                    "dictionary column {column_name:?} contains \
                         invalid UTF-8 at row {index}: {error}"
                ))
            })
        })
        .collect()
}

// VORTEX_RDF_NATIVE_INDEX_BASELINE_EQUIVALENCE_TESTS_V1
#[cfg(test)]
mod native_index_baseline_equivalence_tests {
    use super::*;
    use crate::index::SimpleDictionary;

    const SKOS_SUBJECT: &str = "http://www.w3.org/2004/02/skos/core#subject";
    const EDUARD: &str = "http://dbpedia.org/resource/Eduard_Winkelmann";
    const BOWER: &str = "http://dbpedia.org/resource/Bower_Manuscript";
    const ANKLESHWAR: &str = "http://dbpedia.org/resource/Ankleshwar";
    const TARTU: &str = "http://dbpedia.org/resource/Category%3AUniversity_of_Tartu_faculty";
    const HEIDELBERG: &str =
        "http://dbpedia.org/resource/Category%3AUniversity_of_Heidelberg_faculty";
    const CENTRAL_ASIA: &str = "http://dbpedia.org/resource/Category%3AHistory_of_Central_Asia";
    const CENTRAL_ASIAN_STUDIES: &str =
        "http://dbpedia.org/resource/Category%3ACentral_Asian_studies";
    const GUJARAT: &str = "http://dbpedia.org/resource/Category%3ACities_and_towns_in_Gujarat";
    const RELATED: &str = "http://example.com/related";

    fn named(value: &str) -> NamedNode {
        NamedNode::new(value).unwrap()
    }

    fn quad(subject: &str, predicate: &str, object: &str) -> Quad {
        Quad::new(
            named(subject),
            named(predicate),
            named(object),
            GraphName::DefaultGraph,
        )
    }

    fn fixture() -> Vec<Quad> {
        vec![
            quad(BOWER, SKOS_SUBJECT, CENTRAL_ASIA),
            quad(BOWER, SKOS_SUBJECT, CENTRAL_ASIAN_STUDIES),
            quad(ANKLESHWAR, SKOS_SUBJECT, GUJARAT),
            quad(EDUARD, SKOS_SUBJECT, TARTU),
            quad(EDUARD, SKOS_SUBJECT, HEIDELBERG),
            // The repeated object is deliberately separated by SPO order so the
            // object index must preserve multiple exact payload ranges.
            quad(BOWER, RELATED, TARTU),
            quad(ANKLESHWAR, RELATED, CENTRAL_ASIA),
            // Preserve duplicate-row semantics as well as set membership.
            quad(EDUARD, SKOS_SUBJECT, TARTU),
        ]
    }

    fn sorted(
        mut quads: Vec<(String, String, String, String)>,
    ) -> Vec<(String, String, String, String)> {
        quads.sort_unstable();
        quads
    }

    async fn assert_auto_equals_disabled(
        store: &NativeRdfStoreFile,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
    ) {
        let auto = store
            .match_pattern_with_policy(subject, predicate, object, None, NativeIndexPolicy::Auto)
            .await
            .unwrap();
        let disabled = store
            .match_pattern_with_policy(
                subject,
                predicate,
                object,
                None,
                NativeIndexPolicy::Disabled,
            )
            .await
            .unwrap();
        assert_eq!(sorted(auto), sorted(disabled));
    }

    #[tokio::test]
    async fn standard_indexes_match_index_free_quad_source_results() {
        let temp = tempfile::tempdir().unwrap();
        let artifact = temp.path().join("native-standard-equivalence.vortex");
        let quads = fixture().into_iter().map(Ok);
        let config = CottasNativeConfig {
            row_group_size: 2,
            dict_row_group_size: 2,
            ..CottasNativeConfig::default()
        };
        serialize_cottas_native_quad_source_v10_file::<SimpleDictionary, _>(
            futures::stream::iter(quads),
            &artifact,
            config,
        )
        .await
        .unwrap();

        let store = NativeRdfStoreFile::open(&artifact).await.unwrap();
        assert_eq!(
            store.component_names(),
            vec![
                Arc::<str>::from("quad-source"),
                Arc::<str>::from("dictionary.id-to-term"),
                Arc::<str>::from("dictionary.term-to-id"),
                Arc::<str>::from("dictionary.term-directory"),
                Arc::<str>::from("index.subject-ranges"),
                Arc::<str>::from("index.predicate.exact-ranges.directory"),
                Arc::<str>::from("index.predicate.exact-ranges.payload"),
                Arc::<str>::from("index.predicate-object.predicate-partitions"),
                Arc::<str>::from("index.predicate-object.exact-ranges.directory"),
                Arc::<str>::from("index.predicate-object.exact-ranges.payload"),
                Arc::<str>::from("index.object.exact-ranges.directory"),
                Arc::<str>::from("index.object.exact-ranges.payload"),
            ]
        );

        let eduard = NamedOrBlankNode::NamedNode(named(EDUARD));
        let bower = NamedOrBlankNode::NamedNode(named(BOWER));
        let missing =
            NamedOrBlankNode::NamedNode(named("http://dbpedia.org/resource/Definitely_Missing"));
        let predicate = named(SKOS_SUBJECT);
        let related = named(RELATED);
        let tartu = Term::NamedNode(named(TARTU));
        let central_asia = Term::NamedNode(named(CENTRAL_ASIA));

        // Subject index, including duplicate preservation.
        assert_auto_equals_disabled(&store, Some(&eduard), None, None).await;
        // Predicate exact ranges over multiple disjoint SPO subject groups.
        assert_auto_equals_disabled(&store, None, Some(&predicate), None).await;
        // Predicate-object exact ranges.
        assert_auto_equals_disabled(&store, None, Some(&predicate), Some(&tartu)).await;
        // Object exact ranges with matches under two predicates and subjects.
        assert_auto_equals_disabled(&store, None, None, Some(&tartu)).await;
        // Fully bound matching keeps the subject-first priority and exact filter.
        assert_auto_equals_disabled(&store, Some(&eduard), Some(&predicate), Some(&tartu)).await;
        // Dictionary miss short-circuits identically.
        assert_auto_equals_disabled(&store, Some(&missing), None, None).await;
        // Every term exists, but this combination does not.
        assert_auto_equals_disabled(&store, Some(&bower), Some(&related), Some(&central_asia))
            .await;

        let predicate_id = store
            .lookup_term_id(&named(SKOS_SUBJECT).to_string())
            .await
            .unwrap()
            .unwrap();
        let object_id = store
            .lookup_term_id(&named(TARTU).to_string())
            .await
            .unwrap()
            .unwrap();
        let subject_id = store
            .lookup_term_id(&named(EDUARD).to_string())
            .await
            .unwrap()
            .unwrap();

        let subject_range = store
            .lookup_native_subject_range(subject_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!subject_range.is_empty());
        let predicate_ranges = store
            .lookup_native_predicate_exact_ranges(predicate_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            predicate_ranges.len() > 1,
            "fixture must exercise disjoint predicate ranges"
        );
        let po_ranges = store
            .lookup_native_predicate_object_exact_ranges(predicate_id, object_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!po_ranges.is_empty());
        let object_ranges = store
            .lookup_native_object_exact_ranges(object_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            object_ranges.len() > 1,
            "fixture must exercise disjoint object ranges"
        );

        // Required proves that every intended query shape has a compatible index.
        for (s, p, o) in [
            (Some(&eduard), None, None),
            (None, Some(&predicate), None),
            (None, Some(&predicate), Some(&tartu)),
            (None, None, Some(&tartu)),
        ] {
            store
                .match_pattern_with_policy(s, p, o, None, NativeIndexPolicy::Required)
                .await
                .unwrap();
        }
    }
}

#[cfg(test)]
mod native_artifact_manifest_tests {
    use super::*;

    #[test]
    fn production_manifest_is_complete_and_valid() {
        let manifest = NativeArtifactManifest::production_defaults();
        manifest.validate().unwrap();
        assert_eq!(manifest.components.len(), NativeComponent::ALL.len() + 1);
        assert_eq!(
            manifest.components[0].logical_name,
            NATIVE_TRIPLES_LOGICAL_NAME
        );
        assert_eq!(
            NATIVE_ARTIFACT_METADATA_KEY,
            "vortex.rdf.native-ids.manifest"
        );
    }

    #[test]
    fn resolver_preserves_external_locations_for_both_artifact_kinds() {
        let artifact = Path::new("fixture.vortex");
        let manifest = NativeArtifactManifest::production_defaults();
        for artifact_kind in [
            NativeArtifactKind::LegacyExternal,
            NativeArtifactKind::ManifestExternal(manifest),
        ] {
            let resolver = NativeComponentResolver::from_kind(artifact, artifact_kind).unwrap();
            for component in NativeComponent::ALL {
                assert_eq!(
                    resolver.external_path(component).unwrap(),
                    component.external_path(artifact)
                );
            }
        }
    }

    #[test]
    fn production_external_inventory_covers_every_manifest_sidecar() {
        let manifest = NativeArtifactManifest::production_defaults();
        let manifest_names: BTreeSet<_> = manifest
            .components
            .iter()
            .skip(1)
            .map(|component| component.logical_name.as_str())
            .collect();
        let external_names: BTreeSet<_> = NativeComponent::ALL
            .into_iter()
            .map(NativeComponent::logical_name)
            .collect();
        assert_eq!(manifest_names, external_names);
        assert!(!external_names.contains(NATIVE_TRIPLES_LOGICAL_NAME));
        assert_eq!(NATIVE_TERM_DIRECTORY_FENCE_ROWS, 512);
    }

    #[test]
    fn external_component_paths_are_unique_and_derived_from_the_artifact() {
        let artifact = Path::new("fixture.vortex");
        let paths: BTreeSet<_> = NativeComponent::ALL
            .into_iter()
            .map(|component| component.external_path(artifact))
            .collect();
        assert_eq!(paths.len(), NativeComponent::ALL.len());
        assert!(paths.iter().all(|path| path != artifact));
    }

    #[test]
    fn manifest_resolver_activates_external_and_embedded_locations() {
        let artifact = Path::new("artifact.vortex");
        let external = NativeComponentResolver::from_kind(
            artifact,
            NativeArtifactKind::ManifestExternal(NativeArtifactManifest::production_defaults()),
        )
        .unwrap();
        assert!(matches!(
            external
                .location(NativeComponent::DictionaryVortex)
                .unwrap(),
            ComponentLocation::External(_)
        ));

        let mut manifest = NativeArtifactManifest::production_defaults();
        let entry = manifest
            .components
            .iter_mut()
            .find(|entry| entry.logical_name == NativeComponent::DictionaryVortex.logical_name())
            .unwrap();
        entry.storage = NativeComponentStorage::Embedded {
            offset: 10,
            length: 20,
        };
        // Missing artifact is rejected before an embedded location can be trusted.
        assert!(
            NativeComponentResolver::from_kind(
                artifact,
                NativeArtifactKind::ManifestExternal(manifest),
            )
            .is_err()
        );
    }

    #[test]
    fn component_location_cache_keys_distinguish_storage_and_ranges() {
        let external = ComponentLocation::External(PathBuf::from("x.vortex.dict.vortex"));
        let embedded_a = ComponentLocation::Embedded {
            artifact_path: PathBuf::from("x.vortex"),
            component: NativeComponent::DictionaryVortex,
            offset: 100,
            length: 10,
        };
        let embedded_b = ComponentLocation::Embedded {
            artifact_path: PathBuf::from("x.vortex"),
            component: NativeComponent::DictionaryVortex,
            offset: 110,
            length: 10,
        };
        assert_ne!(external.cache_key(), embedded_a.cache_key());
        assert_ne!(embedded_a.cache_key(), embedded_b.cache_key());
    }

    #[test]
    fn bounded_component_reader_translates_relative_offsets() {
        let source: Arc<dyn VortexReadAt> =
            Arc::new(vortex_buffer::ByteBuffer::from(vec![0u8; 64]));
        let reader = BoundedNativeComponentReader::new(source, 11, 20).unwrap();
        assert_eq!(reader.absolute_read_offset(0, 1).unwrap(), 11);
        assert_eq!(reader.absolute_read_offset(19, 1).unwrap(), 30);
        assert_eq!(reader.absolute_read_offset(20, 0).unwrap(), 31);
    }

    #[test]
    fn bounded_component_reader_rejects_invalid_ranges() {
        let source: Arc<dyn VortexReadAt> =
            Arc::new(vortex_buffer::ByteBuffer::from(vec![0u8; 64]));
        assert!(BoundedNativeComponentReader::new(Arc::clone(&source), 0, 0).is_err());
        assert!(BoundedNativeComponentReader::new(Arc::clone(&source), u64::MAX, 2).is_err());
        let reader = BoundedNativeComponentReader::new(source, 11, 20).unwrap();
        assert!(reader.absolute_read_offset(20, 1).is_err());
        assert!(reader.absolute_read_offset(u64::MAX, 2).is_err());
    }

    #[tokio::test]
    async fn bounded_component_reader_delegates_only_inside_its_window() {
        let bytes: Vec<u8> = (0u8..64).collect();
        let source: Arc<dyn VortexReadAt> = Arc::new(vortex_buffer::ByteBuffer::from(bytes));
        let reader = BoundedNativeComponentReader::new(source, 10, 20).unwrap();
        assert_eq!(reader.size().await.unwrap(), 20);
        let handle = reader.read_at(3, 4, Alignment::none()).await.unwrap();
        let host = handle.try_into_host().unwrap().await.unwrap();
        assert_eq!(host.as_slice(), &[13, 14, 15, 16]);
        assert!(reader.read_at(18, 3, Alignment::none()).await.is_err());
    }

    #[test]
    fn old_manifest_json_defaults_component_storage_to_external() {
        let manifest = NativeArtifactManifest::production_defaults();
        let mut value = serde_json::to_value(&manifest).unwrap();
        for component in value["components"].as_array_mut().unwrap() {
            component.as_object_mut().unwrap().remove("storage");
        }
        let decoded: NativeArtifactManifest = serde_json::from_value(value).unwrap();
        decoded.validate().unwrap();
        assert!(
            decoded
                .components
                .iter()
                .all(|component| { component.storage == NativeComponentStorage::External })
        );
    }

    #[test]
    fn manifest_rejects_zero_length_and_overflowing_embedded_ranges() {
        let mut manifest = NativeArtifactManifest::production_defaults();
        manifest.components[1].storage = NativeComponentStorage::Embedded {
            offset: 100,
            length: 0,
        };
        assert!(
            manifest
                .validate()
                .unwrap_err()
                .to_string()
                .contains("zero length")
        );
        manifest.components[1].storage = NativeComponentStorage::Embedded {
            offset: u64::MAX,
            length: 2,
        };
        assert!(
            manifest
                .validate()
                .unwrap_err()
                .to_string()
                .contains("overflows")
        );
    }

    #[test]
    fn manifest_rejects_overlapping_embedded_ranges() {
        let mut manifest = NativeArtifactManifest::production_defaults();
        manifest.components[1].storage = NativeComponentStorage::Embedded {
            offset: 100,
            length: 100,
        };
        manifest.components[2].storage = NativeComponentStorage::Embedded {
            offset: 150,
            length: 100,
        };
        assert!(
            manifest
                .validate()
                .unwrap_err()
                .to_string()
                .contains("overlap")
        );
    }

    #[test]
    fn manifest_accepts_adjacent_embedded_ranges() {
        let mut manifest = NativeArtifactManifest::production_defaults();
        manifest.components[1].storage = NativeComponentStorage::Embedded {
            offset: 100,
            length: 50,
        };
        manifest.components[2].storage = NativeComponentStorage::Embedded {
            offset: 150,
            length: 75,
        };
        manifest.validate().unwrap();
    }

    #[test]
    fn manifest_rejects_embedded_primary_triples() {
        let mut manifest = NativeArtifactManifest::production_defaults();
        manifest.components[0].storage = NativeComponentStorage::Embedded {
            offset: 1,
            length: 1,
        };
        assert!(
            manifest
                .validate()
                .unwrap_err()
                .to_string()
                .contains("primary outer")
        );
    }

    #[test]
    fn metadata_segment_ids_follow_data_segments() {
        assert_eq!(*metadata_segment_id(7, 0).unwrap(), 7);
        assert_eq!(*metadata_segment_id(7, 3).unwrap(), 10);
    }

    #[test]
    fn metadata_segment_id_rejects_overflow() {
        assert!(metadata_segment_id(usize::MAX, 1).is_err());
        #[cfg(target_pointer_width = "64")]
        assert!(metadata_segment_id(u32::MAX as usize, 1).is_err());
    }

    #[test]
    fn manifest_json_bytes_are_deterministic_and_validated() {
        let manifest = NativeArtifactManifest::production_defaults();
        let first = manifest.to_metadata_bytes().unwrap();
        let second = manifest.to_metadata_bytes().unwrap();
        assert_eq!(first, second);
        assert_eq!(
            NativeArtifactManifest::from_metadata_bytes(&first).unwrap(),
            manifest
        );
    }

    #[test]
    fn manifest_metadata_rejects_invalid_json() {
        let error = NativeArtifactManifest::from_metadata_bytes(b"not-json").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("deserialize native artifact manifest")
        );
    }

    #[test]
    fn manifest_metadata_rejects_semantically_invalid_manifest() {
        let mut manifest = NativeArtifactManifest::production_defaults();
        manifest.version += 1;
        let bytes = serde_json::to_vec(&manifest).unwrap();
        assert!(
            NativeArtifactManifest::from_metadata_bytes(&bytes)
                .unwrap_err()
                .to_string()
                .contains("unsupported native artifact version")
        );
    }

    #[test]
    fn production_component_logical_names_are_unique() {
        let names: BTreeSet<_> = NativeComponent::ALL
            .into_iter()
            .map(NativeComponent::logical_name)
            .collect();
        assert_eq!(names.len(), NativeComponent::ALL.len());
    }

    #[test]
    fn manifest_rejects_duplicate_component() {
        let mut manifest = NativeArtifactManifest::production_defaults();
        manifest.components.push(manifest.components[0].clone());
        assert!(
            manifest
                .validate()
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
    }

    #[test]
    fn manifest_rejects_missing_component() {
        let mut manifest = NativeArtifactManifest::production_defaults();
        manifest.components.pop();
        assert!(
            manifest
                .validate()
                .unwrap_err()
                .to_string()
                .contains("inventory mismatch")
        );
    }

    #[test]
    fn manifest_rejects_unsupported_version() {
        let mut manifest = NativeArtifactManifest::production_defaults();
        manifest.version += 1;
        assert!(
            manifest
                .validate()
                .unwrap_err()
                .to_string()
                .contains("unsupported native artifact version")
        );
    }
}

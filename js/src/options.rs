//! Build-time options: resolving the JS `BuildOptions` object (or shorthand
//! string) into core strategies, the RDF format-name table, and the single
//! place the builders are monomorphized.

use futures::Stream;
use js_sys::Reflect;
use oxrdfio::RdfFormat;
use vortex_rdf_core::error::Result as CoreResult;
use vortex_rdf_core::store::{BuiltArray, RawQuad};
use vortex_rdf_core::{
    BuilderStrategy, IndexType, Indexes, LayoutStrategy, SortedInMemoryBuilder,
    UnsortedStreamBuilder, VortexRdfStore as CoreStore,
};
use wasm_bindgen::prelude::*;

pub(crate) fn parse_format(format_name: &str) -> Result<RdfFormat, JsValue> {
    match format_name.to_lowercase().as_str() {
        "nt" | "ntriples" => Ok(RdfFormat::NTriples),
        "nq" | "nquads" => Ok(RdfFormat::NQuads),
        "ttl" | "turtle" => Ok(RdfFormat::Turtle),
        "trig" => Ok(RdfFormat::TriG),
        "n3" => Ok(RdfFormat::N3),
        "rdf" | "rdfxml" | "xml" => Ok(RdfFormat::RdfXml),
        "jsonld" => Ok(RdfFormat::JsonLd {
            profile: Default::default(),
        }),
        other => Err(JsValue::from_str(&format!(
            "Unsupported format: {}. Supported formats are 'ntriples', 'nquads', 'turtle', \
             'trig', 'n3', 'rdfxml' and 'jsonld'.",
            other
        ))),
    }
}

/// Build-time configuration resolved from the JS `BuildOptions` object.
pub(crate) struct BuildConfig {
    pub(crate) builder: BuilderStrategy,
    pub(crate) layout: LayoutStrategy,
    pub(crate) indexes: Indexes,
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            builder: BuilderStrategy::UnsortedStream,
            // Dictionary is the JS default: it is the most compact layout and
            // backs the zero-copy code-based read model (integer `.equals`).
            layout: LayoutStrategy::Dictionary,
            indexes: Vec::new(),
        }
    }
}

/// Run the quad stream through the builder named by `config`.
///
/// This is the single place that monomorphizes the builders, so every entry
/// point (`fromString`, `fromQuads`, `rdf_to_vortex`) offers the same choices.
pub(crate) async fn build_array(
    quads: impl Stream<Item = CoreResult<RawQuad>> + Unpin + Send + 'static,
    config: BuildConfig,
) -> Result<BuiltArray, JsValue> {
    let BuildConfig {
        builder,
        layout,
        indexes,
    } = config;
    match builder {
        BuilderStrategy::UnsortedStream => {
            CoreStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
                quads, layout, indexes,
            )
            .await
        }
        BuilderStrategy::SortedInMemory => {
            CoreStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
                quads, layout, indexes,
            )
            .await
        }
        // Defensive: `parse_builder` never yields SortedStream, which spills to disk.
        BuilderStrategy::SortedStream => {
            return Err(JsValue::from_str(
                "The sorted-stream builder strategy is not available in WebAssembly.",
            ));
        }
    }
    .map_err(|e| JsValue::from_str(&format!("Vortex build error: {}", e)))
}

/// Resolve the optional JS build options. Accepts `undefined`/`null` (all
/// defaults), a bare builder-strategy string, or a `BuildOptions` object.
pub(crate) fn parse_build_options(options: JsValue) -> Result<BuildConfig, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(BuildConfig::default());
    }
    if let Some(name) = options.as_string() {
        return Ok(BuildConfig {
            builder: parse_builder(&name)?,
            ..BuildConfig::default()
        });
    }

    let mut config = BuildConfig::default();
    if let Some(name) = get_string_option(&options, "builder")? {
        config.builder = parse_builder(&name)?;
    }
    if let Some(name) = get_string_option(&options, "layout")? {
        config.layout = parse_layout(&name)?;
    }
    let indexes = Reflect::get(&options, &"indexes".into())
        .map_err(|_| JsValue::from_str("Could not read the 'indexes' option"))?;
    if !indexes.is_null() && !indexes.is_undefined() {
        if !js_sys::Array::is_array(&indexes) {
            return Err(JsValue::from_str("Option 'indexes' must be an array"));
        }
        config.indexes = js_sys::Array::from(&indexes)
            .iter()
            .map(|value| match value.as_string() {
                Some(name) => parse_index(&name),
                None => Err(JsValue::from_str("Option 'indexes' must contain strings")),
            })
            .collect::<Result<Indexes, JsValue>>()?;
    }
    Ok(config)
}

/// Read an optional string field, erroring if present but not a string.
fn get_string_option(options: &JsValue, key: &str) -> Result<Option<String>, JsValue> {
    let value = Reflect::get(options, &key.into())
        .map_err(|_| JsValue::from_str(&format!("Could not read the '{}' option", key)))?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    match value.as_string() {
        Some(name) => Ok(Some(name)),
        None => Err(JsValue::from_str(&format!(
            "Option '{}' must be a string",
            key
        ))),
    }
}

fn parse_builder(name: &str) -> Result<BuilderStrategy, JsValue> {
    match name {
        "Unsorted" => Ok(BuilderStrategy::UnsortedStream),
        "Sorted" => Ok(BuilderStrategy::SortedInMemory),
        other => Err(JsValue::from_str(&format!(
            "Unknown builder strategy: {}. Supported strategies are 'Unsorted' and 'Sorted'.",
            other
        ))),
    }
}

fn parse_layout(name: &str) -> Result<LayoutStrategy, JsValue> {
    match name {
        "Default" => Ok(LayoutStrategy::Default),
        "TypedObject" => Ok(LayoutStrategy::TypedObject),
        "Dictionary" => Ok(LayoutStrategy::Dictionary),
        other => Err(JsValue::from_str(&format!(
            "Unknown layout strategy: {}. Supported layouts are 'Default', 'TypedObject' \
             and 'Dictionary'.",
            other
        ))),
    }
}

fn parse_index(name: &str) -> Result<IndexType, JsValue> {
    match name {
        "SecondaryByReference" => Ok(IndexType::SecondaryByReference),
        "SecondaryByCopy" => Ok(IndexType::SecondaryByCopy),
        other => Err(JsValue::from_str(&format!(
            "Unknown index type: {}. Supported indexes are 'SecondaryByReference' \
             and 'SecondaryByCopy'.",
            other
        ))),
    }
}

pub(crate) fn layout_name(layout: LayoutStrategy) -> &'static str {
    match layout {
        LayoutStrategy::Default => "Default",
        LayoutStrategy::TypedObject => "TypedObject",
        LayoutStrategy::Dictionary => "Dictionary",
    }
}

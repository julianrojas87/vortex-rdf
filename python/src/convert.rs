use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vortex_rdf_core::common::formats::{detect_format, format_from_name};
use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::io::quads_stream_to_vortex_file_with_builder;
use vortex_rdf_core::{
    BuilderStrategy, DictionaryPlacement, LayoutStrategy, SortedInMemoryBuilder,
    SortedStreamBuilder, UnsortedStreamBuilder, VortexRdfError,
};

use crate::{RUNTIME, store_err};

/// Accepts the cottas-bench branch's layout names as aliases so its
/// benchmark scripts keep working against these bindings.
fn parse_layout(name: &str) -> PyResult<LayoutStrategy> {
    match name.to_ascii_lowercase().as_str() {
        "default" | "cottas-native-strings" => Ok(LayoutStrategy::Default),
        "typed-object" | "typed_object" => Ok(LayoutStrategy::TypedObject),
        "dictionary" | "cottas-native-ids" | "cottas-native" => Ok(LayoutStrategy::Dictionary),
        other => Err(PyValueError::new_err(format!(
            "unknown layout {other:?}; expected \"default\", \"typed-object\" or \"dictionary\""
        ))),
    }
}

fn parse_placement(name: &str) -> PyResult<DictionaryPlacement> {
    match name.to_ascii_lowercase().as_str() {
        "padded" => Ok(DictionaryPlacement::Padded),
        "sidecar" => Ok(DictionaryPlacement::Sidecar),
        other => Err(PyValueError::new_err(format!(
            "unknown dictionary placement {other:?}; expected \"padded\" or \"sidecar\""
        ))),
    }
}

fn parse_builder(name: &str) -> PyResult<BuilderStrategy> {
    match name.to_ascii_lowercase().as_str() {
        "unsorted-stream" | "unsorted" => Ok(BuilderStrategy::UnsortedStream),
        "sorted-in-memory" | "sorted" => Ok(BuilderStrategy::SortedInMemory),
        "sorted-stream" => Ok(BuilderStrategy::SortedStream),
        other => Err(PyValueError::new_err(format!(
            "unknown builder {other:?}; expected \"unsorted-stream\", \"sorted-in-memory\" or \"sorted-stream\""
        ))),
    }
}

/// Serialize an RDF file into a `.vortex` store file.
///
/// `format` is an RDF format name ("ntriples", "nquads", "turtle", ...);
/// when omitted it is detected from the input file extension.
#[pyfunction]
#[pyo3(signature = (
    input_path,
    output_path,
    layout="default",
    dictionary_placement="padded",
    format=None,
    builder="unsorted-stream",
))]
pub fn serialize_rdf(
    py: Python<'_>,
    input_path: PathBuf,
    output_path: PathBuf,
    layout: &str,
    dictionary_placement: &str,
    format: Option<&str>,
    builder: &str,
) -> PyResult<()> {
    let layout = parse_layout(layout)?;
    let placement = parse_placement(dictionary_placement)?;
    let builder = parse_builder(builder)?;
    let format = match format {
        Some(name) => format_from_name(name)
            .ok_or_else(|| PyValueError::new_err(format!("unknown RDF format {name:?}")))?,
        None => detect_format(&Some(input_path.clone())).ok_or_else(|| {
            PyValueError::new_err(
                "could not detect RDF format from the input file extension; pass format=",
            )
        })?,
    };

    py.detach(|| -> Result<(), VortexRdfError> {
        let file = std::fs::File::open(&input_path)?;
        let quads = parse_quads_from_reader(file, format);
        RUNTIME.block_on(async {
            match builder {
                BuilderStrategy::UnsortedStream => {
                    quads_stream_to_vortex_file_with_builder::<UnsortedStreamBuilder, _>(
                        quads,
                        &output_path,
                        layout,
                        Vec::new(),
                        placement,
                    )
                    .await
                }
                BuilderStrategy::SortedInMemory => {
                    quads_stream_to_vortex_file_with_builder::<SortedInMemoryBuilder, _>(
                        quads,
                        &output_path,
                        layout,
                        Vec::new(),
                        placement,
                    )
                    .await
                }
                BuilderStrategy::SortedStream => {
                    quads_stream_to_vortex_file_with_builder::<SortedStreamBuilder, _>(
                        quads,
                        &output_path,
                        layout,
                        Vec::new(),
                        placement,
                    )
                    .await
                }
            }
        })
    })
    .map_err(store_err)
}

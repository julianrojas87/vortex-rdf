use std::collections::HashMap;
use std::collections::hash_map::Entry;

use oxrdf::{BlankNode, GraphName, NamedNode, NamedOrBlankNode, Term};
use pyo3::exceptions::{PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_rdf_core::common::terms::{get_as_term, parse_graph_name};
use vortex_rdf_core::io::VORTEX_SESSION;
use vortex_rdf_core::{LayoutStrategy, VortexRdfError, VortexRdfStore as CoreStore};

use crate::codes::{TermDict, U32Column};
use crate::{RUNTIME, parse_err, store_err};

type Pattern = (
    Option<NamedOrBlankNode>,
    Option<NamedNode>,
    Option<Term>,
    Option<GraphName>,
);

/// `(term_table, rows)` as returned by [`VortexRdfStore::match_compact`].
type CompactTriples = (Vec<String>, Vec<(u32, u32, u32)>);

/// `(s, p, o, g)` code columns as returned by [`VortexRdfStore::match_codes`].
type CodeColumns = (U32Column, U32Column, U32Column, U32Column);

/// A read-only Vortex-RDF store opened from a `.vortex` file.
///
/// The file is opened lazily: constructing the object reads only the file
/// footer (and, for the Dictionary layout, lifts the term dictionary when it
/// fits the residency budget). Keeping one instance warm across queries is
/// what makes rdflib `triples()` traffic cheap — reopening per call would
/// re-lift the dictionary every time.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct VortexRdfStore {
    store: CoreStore,
    path: String,
}

// Pattern terms arrive from Python callers, so unlike core's trusted decode
// paths (`new_unchecked`) they are validated — the cost is per match call,
// not per row. Objects go through `get_as_term`, which handles all literal
// forms (language-tagged, typed); core's `parse_term` does not.

fn py_parse_named_node(s: &str) -> PyResult<NamedNode> {
    let iri = s
        .strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .unwrap_or(s);
    NamedNode::new(iri).map_err(|e| PyValueError::new_err(format!("invalid IRI {s:?}: {e}")))
}

fn py_parse_subject(s: &str) -> PyResult<NamedOrBlankNode> {
    if let Some(id) = s.strip_prefix("_:") {
        BlankNode::new(id)
            .map(NamedOrBlankNode::BlankNode)
            .map_err(|e| PyValueError::new_err(format!("invalid blank node {s:?}: {e}")))
    } else {
        py_parse_named_node(s).map(NamedOrBlankNode::NamedNode)
    }
}

fn py_parse_object(s: &str) -> PyResult<Term> {
    get_as_term(s).ok_or_else(|| PyValueError::new_err(format!("could not parse RDF term {s:?}")))
}

fn parse_pattern(
    s: Option<&str>,
    p: Option<&str>,
    o: Option<&str>,
    g: Option<&str>,
) -> PyResult<Pattern> {
    Ok((
        s.map(py_parse_subject).transpose()?,
        p.map(py_parse_named_node).transpose()?,
        o.map(py_parse_object).transpose()?,
        g.map(parse_graph_name).transpose().map_err(parse_err)?,
    ))
}

fn intern(ids: &mut HashMap<String, u32>, table: &mut Vec<String>, term: String) -> u32 {
    match ids.entry(term) {
        Entry::Occupied(e) => *e.get(),
        Entry::Vacant(e) => {
            let id = table.len() as u32;
            table.push(e.key().clone());
            e.insert(id);
            id
        }
    }
}

impl VortexRdfStore {
    /// Runs the pattern match off the GIL and returns the matched quads.
    async fn matched_quads(&self, pattern: &Pattern) -> Result<Vec<oxrdf::Quad>, VortexRdfError> {
        let (s, p, o, g) = pattern;
        let view = self
            .store
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await?;
        view.quads_vec().await
    }
}

#[pymethods]
impl VortexRdfStore {
    /// Open `path`. By default the store stays file-backed and lazy (only the
    /// footer is read up front). `in_memory=True` loads the whole store into
    /// memory instead — every subsequent match skips the per-call file-scan
    /// pipeline, which is worth ~1 ms per `triples()` call and decides join
    /// performance under rdflib's per-binding probing. `max_resident_bytes`
    /// overrides the Dictionary layout's term-dictionary residency budget
    /// (the embedded blob's compressed size in bytes).
    #[new]
    #[pyo3(signature = (path, max_resident_bytes=None, in_memory=false))]
    fn new(
        py: Python<'_>,
        path: String,
        max_resident_bytes: Option<u64>,
        in_memory: bool,
    ) -> PyResult<Self> {
        if !std::path::Path::new(&path).is_file() {
            return Err(PyFileNotFoundError::new_err(format!(
                "no such Vortex file: {path}"
            )));
        }
        let store = py
            .detach(|| {
                RUNTIME.block_on(async {
                    let store = match max_resident_bytes {
                        Some(n) => CoreStore::from_file_with_dict_residency(&path, n).await?,
                        None => CoreStore::from_file(&path).await?,
                    };
                    if in_memory {
                        // Round-trip through the serializable parts: rows plus
                        // the dictionary those rows' codes address, exactly
                        // what `from_built` reconstructs a store from.
                        let parts = store.to_serializable_parts().await?;
                        CoreStore::from_built(parts)
                    } else {
                        Ok(store)
                    }
                })
            })
            .map_err(store_err)?;
        Ok(Self { store, path })
    }

    /// Column layout detected from the file: "default", "typed-object" or
    /// "dictionary".
    fn layout(&self) -> &'static str {
        match self.store.layout() {
            LayoutStrategy::Default => "default",
            LayoutStrategy::TypedObject => "typed-object",
            LayoutStrategy::Dictionary => "dictionary",
        }
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| RUNTIME.block_on(self.store.size()))
            .map_err(store_err)
    }

    fn __repr__(&self) -> String {
        format!(
            "VortexRdfStore(path={:?}, layout={:?})",
            self.path,
            self.layout()
        )
    }

    /// Match a triple pattern and return `(subject, predicate, object)`
    /// N-Triples strings. `None` positions are wildcards; `g` narrows to one
    /// graph (default: any graph).
    #[pyo3(signature = (s=None, p=None, o=None, g=None))]
    fn match_triples(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> PyResult<Vec<(String, String, String)>> {
        let pattern = parse_pattern(s, p, o, g)?;
        py.detach(|| -> Result<_, VortexRdfError> {
            let quads = RUNTIME.block_on(self.matched_quads(&pattern))?;
            Ok(quads
                .into_iter()
                .map(|q| {
                    (
                        q.subject.to_string(),
                        q.predicate.to_string(),
                        q.object.to_string(),
                    )
                })
                .collect())
        })
        .map_err(store_err)
    }

    /// The store's term dictionary, or `None` when the code path does not
    /// apply: a non-Dictionary layout, a non-resident (file-backed)
    /// dictionary, or an append tail whose quads are not in the cached
    /// dictionary. Pair with [`Self::match_codes`]; decode each distinct
    /// code once, caching on the Python side.
    fn term_dict(&self) -> Option<TermDict> {
        if self.store.tail_len() != 0 {
            return None;
        }
        self.store
            .dictionary_snapshot()
            .map(|snapshot| TermDict { snapshot })
    }

    /// Match a pattern and return the rows as four zero-copy `u32` term-code
    /// columns `(s, p, o, g)` decodable through [`Self::term_dict`], or
    /// `None` when the code path does not apply (see `term_dict`). Callers
    /// fall back to [`Self::match_compact`].
    #[pyo3(signature = (s=None, p=None, o=None, g=None))]
    fn match_codes(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> PyResult<Option<CodeColumns>> {
        if self.store.layout() != LayoutStrategy::Dictionary
            || self.store.tail_len() != 0
            || self.store.dictionary_snapshot().is_none()
        {
            return Ok(None);
        }
        let pattern = parse_pattern(s, p, o, g)?;
        let [s, p, o, g] = py
            .detach(|| -> Result<_, VortexRdfError> {
                RUNTIME.block_on(async {
                    let (s, p, o, g) = &pattern;
                    let matched = self
                        .store
                        .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                        .await?;
                    let arr = matched.get_quads_array().await?;
                    let mut ctx = VORTEX_SESSION.create_execution_ctx();
                    let struct_arr = arr.execute::<StructArray>(&mut ctx)?;
                    let mut cols = Vec::with_capacity(4);
                    for name in ["s", "p", "o", "g"] {
                        let col = struct_arr.unmasked_field_by_name(name)?;
                        cols.push(col.clone().execute::<PrimitiveArray>(&mut ctx)?);
                    }
                    Ok([
                        cols.remove(0),
                        cols.remove(0),
                        cols.remove(0),
                        cols.remove(0),
                    ])
                })
            })
            .map_err(store_err)?;
        Ok(Some((
            U32Column { prim: s },
            U32Column { prim: p },
            U32Column { prim: o },
            U32Column { prim: g },
        )))
    }

    /// Match a triple pattern and return `(term_table, rows)`: a de-duplicated
    /// list of N-Triples term strings plus `(s, p, o)` indices into it. The
    /// caller parses each distinct term once instead of once per occurrence.
    #[pyo3(signature = (s=None, p=None, o=None, g=None))]
    fn match_compact(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> PyResult<CompactTriples> {
        let pattern = parse_pattern(s, p, o, g)?;
        py.detach(|| -> Result<_, VortexRdfError> {
            let quads = RUNTIME.block_on(self.matched_quads(&pattern))?;
            let mut table = Vec::new();
            let mut ids = HashMap::new();
            let mut rows = Vec::with_capacity(quads.len());
            for q in quads {
                rows.push((
                    intern(&mut ids, &mut table, q.subject.to_string()),
                    intern(&mut ids, &mut table, q.predicate.to_string()),
                    intern(&mut ids, &mut table, q.object.to_string()),
                ));
            }
            Ok((table, rows))
        })
        .map_err(store_err)
    }
}

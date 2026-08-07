use std::collections::HashMap;
use std::collections::hash_map::Entry;

use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use vortex_rdf_core::common::terms::{Pattern, parse_pattern_checked};
use vortex_rdf_core::{VortexRdfError, VortexRdfStore as CoreStore};

use crate::codes::{TermDict, U32Column};
use crate::{RUNTIME, parse_err, store_err};

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
    /// `None` for stores opened from bytes rather than a file.
    path: Option<String>,
}

/// Interns `term`, storing each distinct term exactly once (as the map key —
/// no cloned side table). Ids are assigned in first-appearance order;
/// [`VortexRdfStore::match_compact`] rebuilds the id-ordered term table by
/// moving the keys out of the map after the match loop.
fn intern(ids: &mut HashMap<String, u32>, term: String) -> u32 {
    let next = ids.len() as u32;
    match ids.entry(term) {
        Entry::Occupied(e) => *e.get(),
        Entry::Vacant(e) => {
            e.insert(next);
            next
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
    /// (the dictionary child's compressed size in bytes).
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
                        // Round-trip through the serializable parts: rows,
                        // index components, and the dictionary those rows'
                        // codes address, exactly what `from_parts`
                        // reconstructs a store from.
                        let parts = store.to_serializable_parts().await?;
                        CoreStore::from_parts(parts)
                    } else {
                        Ok(store)
                    }
                })
            })
            .map_err(store_err)?;
        Ok(Self {
            store,
            path: Some(path),
        })
    }

    /// Open a store from native-container bytes — what [`Self::to_bytes`]
    /// (or the JS bindings' `toBytes`, or reading a `.vortex` file into
    /// memory) produces. Unlike the path constructor there is no file to
    /// stay lazily backed by: the whole store lives in memory, and the
    /// buffer crosses the Python boundary in one bulk copy.
    #[staticmethod]
    fn from_bytes(py: Python<'_>, data: Vec<u8>) -> PyResult<Self> {
        let store = py
            .detach(|| RUNTIME.block_on(CoreStore::from_bytes_owned(data)))
            .map_err(store_err)?;
        Ok(Self { store, path: None })
    }

    /// Serialize the store to native-container bytes: the exchange format
    /// shared with [`Self::from_bytes`], the JS bindings and the on-disk
    /// `.vortex` file, carrying the quad table plus the dictionary and
    /// index components.
    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = py
            .detach(|| RUNTIME.block_on(self.store.to_bytes()))
            .map_err(store_err)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Column layout detected from the file: "default", "typed-object" or
    /// "dictionary" — core's canonical strategy names.
    fn layout(&self) -> String {
        self.store.layout().to_string()
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| RUNTIME.block_on(self.store.size()))
            .map_err(store_err)
    }

    fn __repr__(&self) -> String {
        match &self.path {
            Some(path) => format!(
                "VortexRdfStore(path={:?}, layout={:?})",
                path,
                self.layout()
            ),
            None => format!("VortexRdfStore(layout={:?})", self.layout()),
        }
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
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
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
        self.store
            .code_read_snapshot()
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
        if self.store.code_read_snapshot().is_none() {
            return Ok(None);
        }
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        let columns = py
            .detach(|| -> Result<_, VortexRdfError> {
                RUNTIME.block_on(async {
                    let (s, p, o, g) = &pattern;
                    self.store
                        .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                        .await?
                        .code_columns_gathered()
                        .await
                })
            })
            .map_err(store_err)?;
        Ok(columns.map(|[s, p, o, g]| {
            (
                U32Column { codes: s },
                U32Column { codes: p },
                U32Column { codes: o },
                U32Column { codes: g },
            )
        }))
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
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        py.detach(|| -> Result<_, VortexRdfError> {
            let quads = RUNTIME.block_on(self.matched_quads(&pattern))?;
            let mut ids = HashMap::new();
            let mut rows = Vec::with_capacity(quads.len());
            for q in quads {
                rows.push((
                    intern(&mut ids, q.subject.to_string()),
                    intern(&mut ids, q.predicate.to_string()),
                    intern(&mut ids, q.object.to_string()),
                ));
            }
            // Move the interned terms out of the map into id order (the
            // placeholder Strings do not allocate): each distinct term is
            // allocated once for the whole call.
            let mut table = vec![String::new(); ids.len()];
            for (term, id) in ids {
                table[id as usize] = term;
            }
            Ok((table, rows))
        })
        .map_err(store_err)
    }
}

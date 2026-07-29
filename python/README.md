# vortex-rdflib

Python bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a
modern, high-performance columnar RDF serialization format — with an
[rdflib](https://rdflib.readthedocs.io/) `Store` implementation so `.vortex`
files can be queried with SPARQL.

Unlike the JS/wasm bindings (in-memory only), these bindings are built with
file IO enabled: stores are **opened lazily from `.vortex` files** and queried
in place, without loading the dataset into memory.

## Install (development)

```bash
cd python
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop --release
```

## Usage

### With rdflib (SPARQL)

```python
from rdflib import Graph
from vortex_rdflib import VortexStore

graph = Graph(store=VortexStore("data.vortex"))
for row in graph.query("""
    SELECT ?s ?o WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?o } LIMIT 10
"""):
    print(row.s, row.o)
```

SPARQL evaluation is rdflib's engine; the store serves triple patterns from
the Vortex file. The store is read-only.

### Native API

```python
from vortex_rdflib._native import VortexRdfStore, serialize_rdf

# RDF file -> .vortex store file
serialize_rdf("data.nt", "data.vortex", layout="dictionary")

store = VortexRdfStore("data.vortex")   # lazy open; file layout is auto-detected
len(store)                           # number of quads
store.match_triples(p="<http://xmlns.com/foaf/0.1/name>")
store.match_compact(p="<http://xmlns.com/foaf/0.1/name>")  # (term_table, rows)
```

Terms cross the boundary as N-Triples strings (`<iri>`, `_:b0`, `"lit"@en`,
`"3"^^<http://www.w3.org/2001/XMLSchema#integer>`). `match_compact` returns a
de-duplicated term table plus index triples so callers parse each distinct
term once — this is what `VortexStore.triples()` uses.

For Dictionary-layout stores, `match_codes` returns the rows as four
**zero-copy** `u32` term-code columns (`memoryview(col).cast("I")` views the
Rust memory directly) plus a `term_dict()` handle — `VortexStore.triples()`
uses this automatically, decoding each distinct code to an rdflib term once
and caching it. Set `VORTEX_RDF_DISABLE_CODE_PATH=1` to force the string path.

## SPARQL BGP pushdown

Constructing a `VortexStore` registers an rdflib `CUSTOM_EVALS` hook that
evaluates whole basic graph patterns in one pass: each triple pattern is
matched natively once and the join runs as hash joins over `u32` codes,
decoding terms only for the final solutions. This replaces rdflib's default
nested-loop evaluation (one `triples()` call per candidate binding) and is
what makes joins fast — measured ~6x on an in-memory store and ~50x on a
file-backed one, 3x faster than rdflib's own in-memory store. The hook only
fires for VortexStore graphs with the code path available and falls back to
the default evaluator otherwise (other stores, RDF-star patterns, non-BGP
algebra). Set `VORTEX_RDF_DISABLE_PUSHDOWN=1` to keep the default evaluator.

## Layouts

`serialize_rdf(..., layout=...)` accepts `"default"`, `"typed-object"` and
`"dictionary"` (with `dictionary_placement="padded"|"sidecar"`). Opening
auto-detects the layout — `VortexRdfStore` takes no layout argument.

For Dictionary-layout files, the term dictionary is held in memory when it
fits the residency budget; pass `VortexRdfStore(path, max_resident_terms=...)`
to raise the budget (recommended for benchmarking large stores).

## File-backed vs in-memory

The default open is lazy and file-backed. `VortexStore(path, in_memory=True)`
(or env `VORTEX_RDF_IN_MEMORY=1`) loads the store into memory once: each
`triples()` call then skips the per-call file-scan pipeline (~1 ms → ~0.15 ms
per call), which is decisive for SPARQL joins — rdflib evaluates them by
probing `triples()` once per binding.

## Tests

```bash
pip install -e .[test]   # or: maturin develop && pip install pytest
pytest tests
```

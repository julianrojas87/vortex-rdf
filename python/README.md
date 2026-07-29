# vortex-rdf
[![PyPI](https://img.shields.io/pypi/v/vortex-rdf.svg)](https://pypi.org/project/vortex-rdf/)

Python bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a
modern, high-performance columnar RDF serialization format.

Stores can be **opened lazily from `.vortex` files** and queried in place, without loading the dataset into memory.

The [`vortex-rdflib`](https://pypi.org/project/vortex-rdflib/) package, which implements an rdflib `Store` (with SPARQL capabilities) on top of these bindings.

## Install

```bash
pip install vortex-rdf
```

Development build (managed with [uv](https://docs.astral.sh/uv/); maturin
runs under the hood as the build backend):

```bash
cd python
uv sync                      # creates .venv, builds + installs the extension
uv run pytest tests          # run the test suite
uv run maturin develop --uv  # fast rebuild while iterating on Rust code
```

Rust source changes are picked up by `uv sync` automatically (see
`[tool.uv] cache-keys` in pyproject.toml). Without uv, the classic flow
works too: `python -m venv .venv && pip install maturin && maturin develop`.

Building from source (the sdist or a development build) additionally
requires **libclang**: a transitive build dependency of the Vortex file
engine (`custom-labels`, via `vortex-io`) generates C bindings with
`bindgen` at compile time. It is preinstalled on most dev setups (Xcode,
LLVM on Windows); on Linux install e.g. `clang-devel` (dnf) or
`libclang-dev` (apt). Installing a published wheel needs none of this.

## Usage

```python
from vortex_rdf import VortexRdfStore, serialize_rdf

# RDF file -> .vortex store file
serialize_rdf("data.nt", "data.vortex", layout="dictionary")

store = VortexRdfStore("data.vortex")   # lazy open; file layout is auto-detected
len(store)                              # number of quads
store.match_triples(p="<http://xmlns.com/foaf/0.1/name>")
store.match_compact(p="<http://xmlns.com/foaf/0.1/name>")  # (term_table, rows)
```

Terms cross the boundary as N-Triples strings (`<iri>`, `_:b0`, `"lit"@en`,
`"3"^^<http://www.w3.org/2001/XMLSchema#integer>`). `match_compact` returns a
de-duplicated term table plus index triples so callers parse each distinct
term once.

### Code columns (Dictionary layout)

For Dictionary-layout stores, `match_codes` returns the matched rows as four
**zero-copy** `u32` term-code columns — `memoryview(col).cast("I")` views the
Rust memory directly — decodable through a `term_dict()` handle:

```python
cols = store.match_codes(p="<http://xmlns.com/foaf/0.1/name>")  # (s, p, o, g)
dictionary = store.term_dict()
subjects = memoryview(cols[0]).cast("I")
dictionary.decode(subjects[0])           # N-Triples string for that code
```

Consumers can join, count, and de-duplicate entirely in code space and decode
each distinct term once — this is what powers `vortex-rdflib`'s SPARQL BGP
pushdown.

## Layouts

`serialize_rdf(..., layout=...)` accepts `"default"`, `"typed-object"` and
`"dictionary"` (with `dictionary_placement="padded"|"sidecar"`). Opening
auto-detects the layout — `VortexRdfStore` takes no layout argument.

For Dictionary-layout files, the term dictionary is held in memory when it
fits the residency budget; pass `VortexRdfStore(path, max_resident_terms=...)`
to raise the budget (recommended for benchmarking large stores).

## File-backed vs in-memory

The default open is lazy and file-backed. `VortexRdfStore(path, in_memory=True)`
loads the store into memory once: each subsequent match skips the per-call
file-scan pipeline (~1 ms → ~0.15 ms per call).

## Tests

```bash
pip install -e .[test]   # or: maturin develop && pip install pytest
pytest tests
```

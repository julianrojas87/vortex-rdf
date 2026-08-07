# vortex-rdf
[![PyPI](https://img.shields.io/pypi/v/vortex-rdf.svg)](https://pypi.org/project/vortex-rdf/)

Python bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a
modern, high-performance columnar RDF serialization format.

Stores can be **opened lazily from `.vortex` files** and queried in place, without loading the dataset into memory.

The [`vortex-rdflib`](https://pypi.org/project/vortex-rdflib/) package implements an rdflib `Store` (with SPARQL capabilities) on top of these bindings.

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
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")     # [(s, p, o, g), ...]
store.match_columns(p="<http://xmlns.com/foaf/0.1/name>")  # (subjects, predicates, objects, graphs)
store.match_compact(p="<http://xmlns.com/foaf/0.1/name>")  # (term_table, rows)
```

Terms cross the boundary as N-Triples strings (`<iri>`, `_:b0`, `"lit"@en`,
`"3"^^<http://www.w3.org/2001/XMLSchema#integer>`). `match_compact` returns a
de-duplicated term table plus index triples so callers parse each distinct
term once.

`get_quads` returns whole quads — the graph of a quad in the default graph is
the empty string, which is also how a pattern selects it. `match_columns`
returns the same rows transposed into four parallel columns, for callers that
work a position at a time rather than row by row.

Both are served from the term-code columns whenever the store can (Dictionary
layout, resident dictionary, no append tail) — roughly 2x faster on a
65,536-row match than re-serializing each quad, with a term repeated down a
column becoming one shared Python string rather than an equal copy per row.
Rows are identical either way, so this is a speed-up rather than a choice to
make; the code API below is for callers who want to skip building strings
altogether.

### Code columns (Dictionary layout)

For Dictionary-layout stores, `match_codes` returns the matched rows as four
**zero-copy** `u32` term-code columns — `memoryview(col).cast("I")` views the
Rust memory directly — decodable through a `term_dict()` handle:

```python
cols = store.match_codes(p="<http://xmlns.com/foaf/0.1/name>")  # (s, p, o, g)
dictionary = store.term_dict()
subjects = memoryview(cols[0]).cast("I")
dictionary.decode(subjects[0])           # N-Triples string for that code
dictionary.decode_many(cols[0])          # bulk-decode a whole column at once
```

`decode_many` decodes a batch in one GIL-released call. Buffer-protocol
inputs — a column straight from `match_codes`, an `array("I", ...)`, a
`uint32` NumPy array — are read in a single bulk copy with no per-element
int conversion; any sequence of ints works too. Both `term_dict()` and
`match_codes` return `None` when the code path does not apply (non-Dictionary
layout, or a dictionary left file-backed by the residency budget).

Consumers can join, count, and de-duplicate entirely in code space and decode
each distinct term once — this is what powers `vortex-rdflib`'s SPARQL BGP
pushdown.

## Layouts

`serialize_rdf(..., layout=...)` accepts `"default"`, `"typed-object"` and
`"dictionary"`; `store.layout()` reports the same names. `builder=` picks the
build pipeline (`"unsorted-stream"`, `"sorted-in-memory"`, `"sorted-stream"`)
and `indexes=[...]` lists secondary index components to build into the file
(`"secondary-by-copy"`, `"secondary-by-reference"`). `format=` is an RDF
format name (`"ntriples"`, `"nquads"`, `"turtle"`, `"trig"`, `"n3"`,
`"rdfxml"`, `"jsonld"`, or their short aliases), detected from the input file
extension when omitted. Opening auto-detects the layout — `VortexRdfStore`
takes no layout argument.

For Dictionary-layout files, the term dictionary (carried in the file as its
own dictionary component) is held in memory when its byte size fits the
residency budget; pass `VortexRdfStore(path, max_resident_bytes=...)` to
change the budget (recommended for benchmarking large stores).

## File-backed vs in-memory

The default open is lazy and file-backed. `VortexRdfStore(path, in_memory=True)`
loads the store into memory once: each subsequent match skips the per-call
file-scan pipeline (~1 ms → ~0.15 ms per call).

Stores also round-trip through bytes: `store.to_bytes()` serializes to the
native container (the same exchange format as the `.vortex` file and the JS
bindings), and `VortexRdfStore.from_bytes(data)` opens such a buffer as a
fully in-memory store.

## Tests

```bash
uv run pytest tests   # or: maturin develop && pip install pytest && pytest tests
```

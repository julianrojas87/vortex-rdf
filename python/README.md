# Vortex-RDF for Python
[![PyPI](https://img.shields.io/pypi/v/vortex-rdf.svg)](https://pypi.org/project/vortex-rdf/)

Python bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a columnar RDF store format built on Vortex. Stores are opened lazily from `.vortex` files and queried in place, without loading the dataset into memory. The bindings are read-only: build `.vortex` files with `serialize_rdf` (file → file), then open and query them; there is no in-memory build, RDF export, membership test or mutation (the JS bindings have those).

A separate [`vortex-rdflib`](https://pypi.org/project/vortex-rdflib/) package builds an rdflib integration on these bindings; see its own documentation for what it supports.

## Install

```bash
pip install vortex-rdf
```

## Quick start

```python
from vortex_rdf import VortexRdfStore, serialize_rdf

serialize_rdf("data.nt", "data.vortex", layout="dictionary")   # RDF file -> .vortex file
store = VortexRdfStore("data.vortex")                            # lazy open; layout auto-detected
store.count_quads(p="<http://xmlns.com/foaf/0.1/name>")          # match count, no terms materialized
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")            # [(s, p, o, g), ...]
```

## Reading quads

Every read takes a pattern as the keyword arguments `s`, `p`, `o`, `g`; an omitted position is a wildcard. Terms cross the boundary as N-Triples strings (`<iri>`, `_:b0`, `"lit"@en`, `"3"^^<http://www.w3.org/2001/XMLSchema#integer>`); the graph of a quad in the default graph is the empty string, which is also how a pattern selects it. A malformed term raises `ValueError`; a failing store operation raises `VortexRdfError`.

```python
len(store)                                                   # number of quads
store.layout()                                               # "dictionary" | "default" | "typed-object"
store.indexes()                                              # e.g. ["secondary-by-reference"]
store.count_quads(p="<http://xmlns.com/foaf/0.1/name>")      # int
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")        # [(s, p, o, g), ...]
store.match_columns(p="<http://xmlns.com/foaf/0.1/name>")    # (subjects, predicates, objects, graphs)
```

`get_quads` returns whole quads; `match_columns` returns the same rows transposed into four parallel columns, for callers that work a position at a time. Both are served from the term-code columns whenever the store can (Dictionary layout, resident dictionary) and from the matched quads otherwise; results are identical. On the code path a term that repeats down a column is one shared Python string, so a caller converting terms into its own representation can memoize on the string it is handed.

## Term codes (low-level)

For Dictionary-layout stores, `match_codes` returns the matched rows as four **zero-copy** `u32` term-code columns — `memoryview(col).cast("I")` views the Rust memory directly — decodable through a `term_dict()` handle:

```python
cols = store.match_codes(p="<http://xmlns.com/foaf/0.1/name>")  # (s, p, o, g) or None
dictionary = store.term_dict()                                    # TermDict or None
subjects = memoryview(cols[0]).cast("I")
dictionary.decode(subjects[0])                       # N-Triples string for that code
dictionary.decode_many(cols[0])                      # bulk-decode a whole column
dictionary.encode("<http://xmlns.com/foaf/0.1/name>")  # code for a term, or None
```

`decode_many` decodes a batch in one GIL-released call. Buffer-protocol inputs — a column straight from `match_codes`, an `array("I", ...)`, a `uint32` NumPy array — are read in a single bulk copy with no per-element int conversion; any sequence of ints works too. `encode` is the inverse of `decode`. Both `term_dict()` and `match_codes` return `None` when the code path does not apply (a non-Dictionary layout, or a dictionary left file-backed by the residency budget).

Consumers can join, count, and de-duplicate entirely in code space and decode each distinct term once, never materializing a term string for a row they discard.

## Build options

```python
serialize_rdf(input_path, output_path, *, format=None, layout="dictionary", indexes=[])
```

Every option after the two paths is keyword-only. `format` is an RDF format name (`"ntriples"`, `"nquads"`, `"turtle"`, `"trig"`, `"n3"`, `"rdfxml"`, `"jsonld"`, or the short aliases `nt`, `nq`, `ttl`, `rdf`, `xml`), detected from the input file extension when omitted. Opening auto-detects the layout and indexes — `VortexRdfStore` takes no layout argument; `store.layout()` and `store.indexes()` report the same names.

**`layout`** — how terms are encoded into columns. `"dictionary"` is the default in every vortex-rdf frontend (Python, JS and the CLI):

| Value | Notes |
| --- | --- |
| `"dictionary"` (default) | Terms replaced by codes into a sorted term dictionary. Most compact and fastest to query; backs `match_codes`/`term_dict` |
| `"default"` | All four terms as N-Triples strings |
| `"typed-object"` | Object split into kind/value/datatype/language columns |

**`indexes`** — secondary access paths, each costing extra space:

| Value | Notes |
| --- | --- |
| `"secondary-by-reference"` | Sorted predicate/object columns plus row-id back-references, so predicate-only and object-only patterns use a binary search instead of a full scan |
| `"secondary-by-copy"` | Two complete extra copies of the quad columns — one sorted by `(p, o, s, g)`, one by `(o, s, p, g)` — giving predicate- and object-bound patterns (including predicate+object prefix lookups) the same sorted access path subjects have |

## Bytes & files

The default open is lazy and file-backed. `VortexRdfStore(path, in_memory=True)` loads the store into memory once, so each subsequent match skips the per-call file-scan pipeline.

For Dictionary-layout files the term dictionary is lifted into memory when its compressed size in the file fits the residency budget — 512 MiB by default, overridable process-wide with `VORTEX_RDF_DICT_MAX_RESIDENT_BYTES`. `VortexRdfStore(path, max_resident_bytes=n)` sets the budget for that open (the environment variable is ignored for it). A dictionary left file-backed is point-read through its chunk leaves; `term_dict()` and `match_codes` then return `None` and the string reads fall back to the matched quads.

Stores also round-trip through bytes: `store.to_bytes()` serializes to the native container (the same exchange format as the `.vortex` file, the CLI and the JS bindings), and `VortexRdfStore.from_bytes(data)` opens such a buffer — `bytes` or `bytearray` — as a fully in-memory store.

## Development

Managed with [uv](https://docs.astral.sh/uv/); maturin runs under the hood as the build backend:

```bash
cd python
uv sync                      # creates .venv, builds + installs the extension
uv run pytest tests          # run the test suite
uv run maturin develop --uv  # fast rebuild while iterating on Rust code
```

Rust source changes are picked up by `uv sync` automatically (see `[tool.uv] cache-keys` in pyproject.toml). Without uv: `python -m venv .venv && pip install maturin pytest && maturin develop && pytest tests`.

Building from source (the sdist or a development build) additionally requires **libclang**: a transitive build dependency of the Vortex file engine (`custom-labels`, via `vortex-io`) generates C bindings with `bindgen` at compile time. It is preinstalled on most dev setups (Xcode, LLVM on Windows); on Linux install e.g. `clang-devel` (dnf) or `libclang-dev` (apt). Installing a published wheel needs none of this.

### Benchmarks

`bench/run.py` measures these bindings against [pyoxigraph](https://pypi.org/project/pyoxigraph/), [pycottas](https://pypi.org/project/pycottas/), [rdflib](https://pypi.org/project/rdflib/) and [lightrdf](https://pypi.org/project/lightrdf/) on a file → store → query workload and writes `bench/results.json` for the dashboard's Python tab; `bench/test_codspeed.py` is the instrumented suite CodSpeed runs.

```bash
python3 python/bench/run.py                 # full run
BENCH_DIM=32 python3 python/bench/run.py    # quick pilot
uv run pytest bench/test_codspeed.py --codspeed
```

Harness design (per-library virtualenvs, dataset parity with `js/bench/datasets.ts`, `unsupported` cells where a library lacks the operation, matched-row counts cross-checked and any disagreement recorded in `config.countWarnings`) is documented in `bench/run.py`, `bench/worker.py` and `bench/adapters.py`. Knobs:

| Var | Default | Meaning |
| --- | --- | --- |
| `BENCH_SIZE` | 1,048,576 | rows (one knob shared with the Rust and JS suites) |
| `BENCH_DIM` | unset | optional cube shorthand, `D³` rows; an explicit `BENCH_SIZE` wins |
| `BENCH_GRAPHS_QUADS` | 8 | named graphs the comparative bench asks for |
| `MUT_BATCH` | 10000 | quads per add/delete batch |
| `BENCH_PYTHON` | 3.13 | Python version the per-library virtualenvs are provisioned with |
| `BENCH_SUBJ_RATIO` / `BENCH_OBJ_RATIO` | 0.1 / 0.5 | distinct subjects / objects per row |
| `BENCH_PREDICATES` | 32 | distinct predicates |
| `BENCH_GRAPHS` | 1 | distinct named graphs in the generator; 1 means default graph only |
| `BENCH_LITERAL_FRAC` | 0.4 | fraction of objects that are literals |
| `BENCH_SLOW_PHASE_MS` | 30000 | a phase slower than this runs once, without warmup |
| `PY_BENCH_QUERY_ITERS` / `PY_BENCH_QUERY_WARMUP` | 10 / 5 | measured / warmup iterations per query |
| `PY_BENCH_HEAVY_ITERS` / `PY_BENCH_FULL_SCAN_ITERS` | 3 / 3 | iterations for the heavy and full-scan phases |
| `CODSPEED_BENCH_DIM` / `CODSPEED_BENCH_DIM_QUADS` | 32 / 13 | CodSpeed suite: `D³` triples / `D⁴` quads |

## License

MIT

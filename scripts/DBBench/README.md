# DBBench: vortex-rdf vs COTTAS over rdflib

Benchmarks vortex-rdf against [pycottas](https://github.com/cottas-rdf/pycottas)
(COTTAS) on the DBBench query workload. Both engines plug into rdflib as a
`Store`, so SPARQL evaluation is rdflib's engine for both — the store serving
triple patterns is the only variable.

Ported from the `feat/cottas-bench` branch. The branch-only native
diagnostics and the provisional vortex-duckdb engine were dropped.

## Setup

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install vortex-rdflib pycottas psutil

# To benchmark unreleased binding changes from this repo instead of the
# published vortex-rdf wheel:
#   pip install maturin && maturin develop --release -m python/Cargo.toml
```

## Data and queries (not in this repo)

The DBBench query tree and the DBpedia dataset come from the pycottas
authors' DBPedia experiments and must be obtained out-of-band. The expected
query tree shape is:

```
<query-root>/
  TP/<dataset>/*.txt            # e.g. TP/dbpedia/spo.txt
  JOINS/<dataset>/small/*.txt
  JOINS/<dataset>/big/*.txt
```

Each `.txt` file holds **one SPARQL SELECT query per line**.

Suggested local layout (git-ignored): put datasets and artifacts under
`scripts/DBBench/data/`.

### Preparing the benchmark artifacts

From a DBpedia N-Triples dump:

```bash
# Optional: slice the dump to a target size
python3 scripts/utils/dbpedia_gen.py --input dbpedia.nt --limit 5000000 --output dbpedia_5M.nt

# .vortex artifacts (one per layout you want to benchmark)
cargo run --release -p vortex-rdf-cli -- serialize \
    -i dbpedia_5M.nt -o dbpedia_5M-dict.vortex --layout dictionary
cargo run --release -p vortex-rdf-cli -- serialize \
    -i dbpedia_5M.nt -o dbpedia_5M-default.vortex --layout default

# .cottas artifact for the comparison engine
python3 -c "import pycottas; pycottas.rdf2cottas('dbpedia_5M.nt', 'dbpedia_5M.cottas')"
```

(`vortex_rdf.serialize_rdf` does the same as the CLI serialize, from Python.)

## Running

Single invocation (all knobs):

```bash
python3 scripts/DBBench/dbbench_rdflib_benchmark.py \
    --query-root <query-root> \
    --engines cottas vortex \
    --cottas-path dbpedia_5M.cottas \
    --vortex-path dbpedia_5M-dict.vortex --vortex-layout dictionary \
    --timeout-mode worker --query-timeout-s 60 \
    --warmup-runs 1 --measured-runs 5 \
    --out-prefix dbbench_runs/dbpedia_dict
```

Per-engine wrapper with logs + manifest:

```bash
scripts/DBBench/run_all_dbbench_engines.sh \
    --query-root <query-root> \
    --cottas-path dbpedia_5M.cottas --vortex-path dbpedia_5M-dict.vortex \
    --measured-runs 5 --out-dir dbbench_runs/run1
```

Full comparison (cottas + vortex on dictionary and default layouts, outer
watchdog per configuration):

```bash
scripts/DBBench/run_big_dbbench_engines.sh \
    --query-root <query-root> \
    --cottas-path dbpedia_5M.cottas \
    --vortex-dictionary-path dbpedia_5M-dict.vortex \
    --vortex-default-path dbpedia_5M-default.vortex \
    --out-dir dbbench_runs/big1
```

### Timeout modes

- `worker` (recommended): one persistent child process per engine holding a
  warm `Graph`; on timeout the child is hard-killed and lazily restarted.
  Warm-store performance with safe timeouts.
- `process`: a fresh child process and `Graph` per run. Safest isolation, but
  pays the full store-open cost every run.
- `signal`: persistent in-process `Graph` with SIGALRM timeouts. **Cannot
  interrupt a blocking native call** (CPython only runs signal handlers
  between bytecodes), so a stuck query hangs the driver — use the other
  modes for unattended runs.

### BGP pushdown

The vortex engine registers a SPARQL BGP pushdown into rdflib
(joins evaluated in native code space rather than per-binding `triples()`
probes) — active by default and decisive for the JOINS group. Set
`VORTEX_RDF_DISABLE_PUSHDOWN=1` to benchmark the plain rdflib evaluation
path instead.

### File-backed vs in-memory

By default the vortex engine queries the `.vortex` file in place (lazy,
file-backed). Set `VORTEX_RDF_IN_MEMORY=1` to load the store into memory at
open — this removes the ~1 ms per-`triples()`-call file-scan floor, which
dominates JOIN queries (rdflib probes the store once per binding). Report
both modes: file-backed is the fair comparison against pycottas's
file-backed DuckDB engine; in-memory shows the format's ceiling.

### Dictionary residency

For Dictionary-layout stores, terms are served from memory only when the
dictionary fits the residency budget. For large datasets, force residency so
both engines answer from warm state:

```bash
export VORTEX_RDF_DICT_MAX_RESIDENT_TERMS=100000000
```

(Benchmark workers inherit the environment.)

## Outputs

Each run writes `<out-prefix>.queries.json` (inventory), `.raw.json` /
`.raw.csv` (every run: status, elapsed seconds, result count, RSS before /
after / delta), and `.summary.json` / `.summary.csv` (per-query mean /
median / min / max / stdev over the measured OK runs).

Post-processing:

```bash
# Result-count equality between engines (exit 1 on any mismatch)
python3 scripts/DBBench/compare_dbbench_counts.py \
    run1/dbpedia_cottas.raw.csv run1/dbpedia_vortex.raw.csv

# Per-query timing comparison + speedup table
python3 scripts/DBBench/compare_dbbench_engine_timings.py \
    --out-dir run1 --dataset dbpedia --engines cottas vortex

# Exact result-set equality on the first N TP queries (fresh interpreter per engine)
python3 scripts/utils/smoke_compare_engines.py \
    --query-root <query-root> \
    --cottas-path dbpedia_5M.cottas --vortex-path dbpedia_5M-dict.vortex
```

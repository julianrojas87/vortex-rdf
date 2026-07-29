#!/usr/bin/env bash
# Run the DBBench benchmark once per engine, teeing logs and a manifest into
# an output directory. See README.md for data/query acquisition and prep.
set -eo pipefail

PYTHON_BIN="python3"
BENCHMARK_SCRIPT="$(dirname "$0")/dbbench_rdflib_benchmark.py"
DATASET="dbpedia"
GROUP="TP"
JOIN_SIZE_SMALL="small"
JOIN_SIZE_BIG="big"
WARMUP_RUNS="1"
MEASURED_RUNS="1"
MAX_QUERIES=""
ONLY_FILE_CONTAINS=""
VORTEX_LAYOUT="dictionary"
TIMEOUT_MODE="worker"
QUERY_ROOT=""
COTTAS_PATH=""
VORTEX_PATH=""
OUT_DIR=""
NO_SILENCE_STDOUT="0"
ENGINES=("cottas" "vortex")

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      PYTHON_BIN="$2"; shift 2 ;;
    --benchmark-script)
      BENCHMARK_SCRIPT="$2"; shift 2 ;;
    --query-root)
      QUERY_ROOT="$2"; shift 2 ;;
    --dataset)
      DATASET="$2"; shift 2 ;;
    --groups)
      GROUP="$2"; shift 2 ;;
    --engines)
      IFS=' ' read -r -a ENGINES <<< "$2"; shift 2 ;;
    --cottas-path)
      COTTAS_PATH="$2"; shift 2 ;;
    --vortex-path)
      VORTEX_PATH="$2"; shift 2 ;;
    --vortex-layout)
      VORTEX_LAYOUT="$2"; shift 2 ;;
    --timeout-mode)
      TIMEOUT_MODE="$2"; shift 2 ;;
    --warmup-runs)
      WARMUP_RUNS="$2"; shift 2 ;;
    --measured-runs)
      MEASURED_RUNS="$2"; shift 2 ;;
    --max-queries)
      MAX_QUERIES="$2"; shift 2 ;;
    --only-file-contains)
      ONLY_FILE_CONTAINS="$2"; shift 2 ;;
    --out-dir)
      OUT_DIR="$2"; shift 2 ;;
    --no-silence-stdout)
      NO_SILENCE_STDOUT="1"; shift ;;
    -h|--help)
      echo "Usage: $0 --query-root PATH [--cottas-path PATH] [--vortex-path PATH]" \
           "[--engines 'cottas vortex'] [--groups TP] [--max-queries N] [--out-dir DIR]" \
           "[--timeout-mode process|signal|worker]"
      exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2 ;;
  esac
done

if [[ -z "$QUERY_ROOT" ]]; then
  echo "ERROR: --query-root is required." >&2
  exit 2
fi

for ENGINE in "${ENGINES[@]}"; do
  if [[ "$ENGINE" == "cottas" && -z "$COTTAS_PATH" ]]; then
    echo "ERROR: --cottas-path is required for the cottas engine." >&2
    exit 2
  fi
  if [[ "$ENGINE" == "vortex" && -z "$VORTEX_PATH" ]]; then
    echo "ERROR: --vortex-path is required for the vortex engine." >&2
    exit 2
  fi
done

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="dbbench_runs/${DATASET}_$(date +%Y%m%d_%H%M%S)"
fi

mkdir -p "$OUT_DIR/logs"

MANIFEST="$OUT_DIR/manifest.txt"

{
  echo "Started: $(date)"
  echo "PWD: $(pwd)"
  echo "Python: $PYTHON_BIN"
  echo "Benchmark script: $BENCHMARK_SCRIPT"
  echo "Query root: $QUERY_ROOT"
  echo "Dataset: $DATASET"
  echo "Group: $GROUP"
  echo "Engines: ${ENGINES[*]}"
  echo "COTTAS path: ${COTTAS_PATH:-<none>}"
  echo "Vortex path: ${VORTEX_PATH:-<none>}"
  echo "Vortex layout: $VORTEX_LAYOUT"
  echo "Timeout mode: $TIMEOUT_MODE"
  echo "Warmup runs: $WARMUP_RUNS"
  echo "Measured runs: $MEASURED_RUNS"
  echo "Max queries: ${MAX_QUERIES:-<none>}"
  echo "Only file contains: ${ONLY_FILE_CONTAINS:-<none>}"
  echo "Out dir: $OUT_DIR"
  echo
} | tee "$MANIFEST"

FAILED=0

run_engine() {
  local ENGINE="$1"
  local PREFIX="$OUT_DIR/${DATASET}_${ENGINE}"
  local LOG="$OUT_DIR/logs/${ENGINE}.log"

  local CMD=(
    "$PYTHON_BIN" "$BENCHMARK_SCRIPT"
    --query-root "$QUERY_ROOT"
    --dataset "$DATASET"
    --groups "$GROUP"
    --join-sizes "$JOIN_SIZE_SMALL" "$JOIN_SIZE_BIG"
    --vortex-layout "$VORTEX_LAYOUT"
    --timeout-mode "$TIMEOUT_MODE"
    --engines "$ENGINE"
    --warmup-runs "$WARMUP_RUNS"
    --measured-runs "$MEASURED_RUNS"
    --out-prefix "$PREFIX"
  )

  if [[ -n "$COTTAS_PATH" ]]; then
    CMD+=(--cottas-path "$COTTAS_PATH")
  fi

  if [[ -n "$VORTEX_PATH" ]]; then
    CMD+=(--vortex-path "$VORTEX_PATH")
  fi

  if [[ -n "$MAX_QUERIES" ]]; then
    CMD+=(--max-queries "$MAX_QUERIES")
  fi

  if [[ -n "$ONLY_FILE_CONTAINS" ]]; then
    CMD+=(--only-file-contains "$ONLY_FILE_CONTAINS")
  fi

  if [[ "$NO_SILENCE_STDOUT" == "1" ]]; then
    CMD+=(--no-silence-stdout)
  fi

  {
    echo "============================================================"
    echo "ENGINE: $ENGINE"
    echo "START:  $(date)"
    echo "CMD:    ${CMD[*]}"
    echo "============================================================"
  } | tee -a "$MANIFEST" "$LOG"

  set +e
  "${CMD[@]}" | tee -a "$LOG"
  local STATUS=$?
  set -e

  {
    echo "END:    $(date)"
    echo "STATUS: $STATUS"
    echo
  } | tee -a "$MANIFEST" "$LOG"

  if [[ "$STATUS" -ne 0 ]]; then
    FAILED=1
  fi
}

for ENGINE in "${ENGINES[@]}"; do
  run_engine "$ENGINE"
done

{
  echo "Finished: $(date)"
  if [[ "$FAILED" -ne 0 ]]; then
    echo "Overall status: FAILED - at least one engine failed. Check $OUT_DIR/logs"
  else
    echo "Overall status: OK"
  fi
} | tee -a "$MANIFEST"

exit "$FAILED"

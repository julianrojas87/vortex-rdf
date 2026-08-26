#!/usr/bin/env bash
# One-command dashboard refresh, whole or partial.
#
#   scripts/refresh.sh                     # all five stages (~1 h at full scale)
#   scripts/refresh.sh --only js,render    # re-measure one surface, re-render
#   scripts/refresh.sh --only render       # template-only changes: no re-measurement
#   scripts/refresh.sh --force-build       # rebuild bindings even if sources look unchanged
#
# Stages, in the order they run: rust (the internals targets, benchmark +
# match_lazy), compare (the cross-library suite), js, python, render.
#
# Measurement stages run strictly sequentially on purpose: parallel stages
# contend for CPU and memory and contaminate each other's timings — the
# process-per-adapter isolation inside each suite exists for the same reason.
#
# The wasm and Python bindings are rebuilt only when a Rust source is newer
# than the built artifact; wasm-pack alone costs ~10 minutes, which a
# harness- or template-only iteration should never pay.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# The dashboard scale — the indicative-overview default every suite shares
# (all three read BENCH_SIZE): 2^20 rows, one dataset.
# Same number .github/workflows/bench-dashboard.yml pins.
BENCH_SIZE="${BENCH_SIZE:-1048576}"
MUT_BATCH="${MUT_BATCH:-10000}"

ONLY="rust,compare,js,python,render"
FORCE_BUILD=0
while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="$2"; shift 2 ;;
    --only=*) ONLY="${1#*=}"; shift ;;
    --force-build) FORCE_BUILD=1; shift ;;
    -h|--help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1 (see --help)" >&2; exit 2 ;;
  esac
done

for s in ${ONLY//,/ }; do
  case "$s" in rust|compare|js|python|render) ;; *)
    echo "unknown stage: $s (stages: rust, compare, js, python, render)" >&2; exit 2 ;;
  esac
done

has() { case ",$ONLY," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }
t_start=$(date +%s)
stage() { echo; echo "══ $1 · $(date +%H:%M:%S) ══"; }

# fresh ARTIFACT DEP... — true when the artifact exists and no dependency file
# is newer than it, i.e. the build can be skipped.
fresh() {
  local artifact="$1"; shift
  [ "$FORCE_BUILD" = 0 ] || return 1
  [ -f "$artifact" ] || return 1
  [ -z "$(find "$@" -newer "$artifact" -print -quit 2>/dev/null)" ]
}

if has rust; then
  stage "Rust internals (benchmark + match_lazy, BENCH_SIZE=$BENCH_SIZE)"
  BENCH_SIZE="$BENCH_SIZE" cargo bench --bench benchmark | tee bench_output.txt
  BENCH_SIZE="$BENCH_SIZE" cargo bench --bench match_lazy | tee -a bench_output.txt
fi

if has compare; then
  stage "Rust cross-library (compare → core/bench/results.json)"
  BENCH_SIZE="$BENCH_SIZE" cargo bench --bench compare
fi

if has js; then
  stage "JavaScript comparative (BENCH_SIZE=$BENCH_SIZE)"
  (
    cd js
    [ -d node_modules ] || npm ci
    if fresh pkg/web/vortex_rdf_bg.wasm \
        ../core/src ../core/Cargo.toml ../encoded-search/src ../encoded-search/Cargo.toml \
        src Cargo.toml ../Cargo.lock; then
      echo "wasm bindings are newer than every Rust source — skipping npm run build (--force-build overrides)"
    else
      npm run build
    fi
    if fresh bench/hdt-pkg/hdt_wasm_bench_bg.wasm \
        bench/hdt-wasm/src bench/hdt-wasm/Cargo.toml bench/hdt-wasm/Cargo.lock; then
      echo "hdt wasm wrapper is up to date — skipping npm run build:hdt-wasm"
    else
      npm run build:hdt-wasm
    fi
    BENCH_SIZE="$BENCH_SIZE" MUT_BATCH="$MUT_BATCH" npm run bench
  )
fi

if has python; then
  stage "Python comparative (BENCH_SIZE=$BENCH_SIZE)"
  (
    cd python
    if [ ! -x .venv/bin/maturin ]; then
      uv venv .venv --python 3.13
      uv pip install --python .venv/bin/python maturin
    fi
    if fresh vortex_rdf/_native.abi3.so \
        ../core/src ../core/Cargo.toml ../encoded-search/src ../encoded-search/Cargo.toml \
        src Cargo.toml ../Cargo.lock; then
      echo "python bindings are newer than every Rust source — skipping maturin develop (--force-build overrides)"
    else
      # An out-of-date .so is the classic silent trap: run.py reaches the
      # bindings via PYTHONPATH and would happily measure a stale extension.
      VIRTUAL_ENV="$PWD/.venv" .venv/bin/maturin develop --release
    fi
    BENCH_SIZE="$BENCH_SIZE" MUT_BATCH="$MUT_BATCH" python3 bench/run.py
  )
fi

if has render; then
  stage "Render (public/index.html)"
  if [ ! -f bench_output.txt ]; then
    echo "bench_output.txt is missing — run the rust stage at least once first" >&2
    exit 1
  fi
  args=(bench_output.txt public/index.html --bench-size "$BENCH_SIZE")
  [ -f js/bench/results.json ] && args+=(--js js/bench/results.json)
  [ -f python/bench/results.json ] && args+=(--python python/bench/results.json)
  [ -f core/bench/results.json ] && args+=(--rust-compare core/bench/results.json)
  python3 scripts/render_bench_dashboard.py "${args[@]}"
fi

echo
echo "done in $(( ($(date +%s) - t_start) / 60 )) min ($ONLY)"

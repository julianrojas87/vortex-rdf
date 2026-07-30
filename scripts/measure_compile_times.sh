#!/usr/bin/env bash
set -euo pipefail
export RUST_LOG="${RUST_LOG:-vortex_rdf_cli=debug,vortex_rdf_core=debug}"
work=${1:-target/compile-time-ab}
mkdir -p "$work"
report="$work/report.tsv"
printf 'mode\tbuild\tseconds\n' > "$report"
measure() {
  local mode=$1 target=$2
  shift 2
  rm -rf "$target"
  local start end
  start=$(python3 -c 'import time; print(time.monotonic_ns())')
  CARGO_TARGET_DIR="$target" cargo check -p vortex-rdf-cli "$@"
  end=$(python3 -c 'import time; print(time.monotonic_ns())')
  python3 - "$mode" clean "$start" "$end" "$report" <<'PY2'
import sys
mode, build, start, end, report = sys.argv[1:]
with open(report, 'a', encoding='utf-8') as f:
    f.write(f"{mode}\t{build}\t{(int(end)-int(start))/1e9:.3f}\n")
PY2
  start=$(python3 -c 'import time; print(time.monotonic_ns())')
  CARGO_TARGET_DIR="$target" cargo check -p vortex-rdf-cli "$@"
  end=$(python3 -c 'import time; print(time.monotonic_ns())')
  python3 - "$mode" incremental "$start" "$end" "$report" <<'PY2'
import sys
mode, build, start, end, report = sys.argv[1:]
with open(report, 'a', encoding='utf-8') as f:
    f.write(f"{mode}\t{build}\t{(int(end)-int(start))/1e9:.3f}\n")
PY2
}
measure unified "$work/unified-target"
measure legacy "$work/legacy-target" --features legacy-sidecars
cat "$report"

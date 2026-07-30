#!/usr/bin/env bash
set -euo pipefail

export RUST_LOG="${RUST_LOG:-vortex_rdf_cli=debug,vortex_rdf_core=debug}"

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "usage: $0 INPUT_RDF [QUERY_TSV] [WORK_DIR]" >&2
  echo "QUERY_TSV columns: subject<TAB>predicate<TAB>object<TAB>graph; use - for unbound" >&2
  exit 2
fi

input=$1
queries=${2:-}
work=${3:-}
if [[ ! -f "$input" ]]; then echo "missing input: $input" >&2; exit 2; fi
if [[ -n "$queries" && ! -f "$queries" ]]; then echo "missing queries: $queries" >&2; exit 2; fi

cleanup=0
if [[ -z "$work" ]]; then work=$(mktemp -d "${TMPDIR:-/tmp}/vortex-rdf-ab.XXXXXX"); cleanup=1; else mkdir -p "$work"; fi
trap 'if [[ $cleanup -eq 1 ]]; then rm -rf "$work"; fi' EXIT

bin=${VORTEX_RDF_BIN:-target/release/vortex-rdf-cli}
echo "+ RUST_LOG=$RUST_LOG cargo build --release -p vortex-rdf-cli"
cargo build --release -p vortex-rdf-cli

sidecar="$work/sidecar.vortex"
unified="$work/unified.vortex"
common=(serialize --index-type simple-dictionary --input "$input" --ordering SPO)

echo "+ $bin ${common[*]} --storage-layout cottas-native-ids --output $sidecar"
"$bin" "${common[@]}" --storage-layout cottas-native-ids --output "$sidecar"
echo "+ $bin ${common[*]} --storage-layout native-rdf-store --native-index-profile standard --output $unified"
"$bin" "${common[@]}" --storage-layout native-rdf-store --native-index-profile standard --output "$unified"

query_file="$work/queries.tsv"
if [[ -n "$queries" ]]; then cp "$queries" "$query_file"; else printf '%s\n' $'-\t-\t-\t-' > "$query_file"; fi

normalize() { LC_ALL=C sort "$1" > "$2"; }
run_match() {
  local artifact=$1 layout=$2 policy=$3 subject=$4 predicate=$5 object=$6 graph=$7 output=$8
  local args=(match --input "$artifact" --output "$output" --format n-quads --storage-layout "$layout" --native-index-policy "$policy")
  [[ "$subject" == - ]] || args+=(--subject "$subject")
  [[ "$predicate" == - ]] || args+=(--predicate "$predicate")
  [[ "$object" == - ]] || args+=(--object "$object")
  [[ "$graph" == - ]] || args+=(--graph "$graph")
  "$bin" "${args[@]}"
}

report="$work/report.tsv"
printf 'query\tsidecar_rows\tunified_auto_rows\tunified_disabled_rows\n' > "$report"
query_id=0
while IFS=$'\t' read -r subject predicate object graph extra; do
  [[ -z "${subject:-}" || "${subject:0:1}" == '#' ]] && continue
  [[ -z "${extra:-}" ]] || { echo "query $query_id has more than four columns" >&2; exit 2; }
  query_id=$((query_id+1))
  side="$work/q${query_id}.side.nq"; auto="$work/q${query_id}.auto.nq"; disabled="$work/q${query_id}.disabled.nq"
  run_match "$sidecar" cottas-native-ids auto "$subject" "$predicate" "$object" "$graph" "$side"
  run_match "$unified" native-rdf-store auto "$subject" "$predicate" "$object" "$graph" "$auto"
  run_match "$unified" native-rdf-store disabled "$subject" "$predicate" "$object" "$graph" "$disabled"
  normalize "$side" "$side.sorted"; normalize "$auto" "$auto.sorted"; normalize "$disabled" "$disabled.sorted"
  cmp -s "$side.sorted" "$auto.sorted" || { diff -u "$side.sorted" "$auto.sorted" > "$work/q${query_id}.side-vs-auto.diff" || true; echo "A/B mismatch in query $query_id" >&2; exit 1; }
  cmp -s "$auto.sorted" "$disabled.sorted" || { diff -u "$auto.sorted" "$disabled.sorted" > "$work/q${query_id}.auto-vs-disabled.diff" || true; echo "indexed/baseline mismatch in query $query_id" >&2; exit 1; }
  printf '%s\t%s\t%s\t%s\n' "$query_id" "$(wc -l < "$side.sorted" | tr -d ' ')" "$(wc -l < "$auto.sorted" | tr -d ' ')" "$(wc -l < "$disabled.sorted" | tr -d ' ')" >> "$report"
done < "$query_file"

[[ $query_id -gt 0 ]] || { echo "no queries executed" >&2; exit 2; }
echo "A/B equivalence passed for $query_id queries"
echo "report: $report"
echo "artifacts: $sidecar $unified"

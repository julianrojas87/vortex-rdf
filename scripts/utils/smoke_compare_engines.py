#!/usr/bin/env python3
"""Exact-result correctness smoke test across rdflib engines.

Runs the first N DBBench TP queries against every selected engine — each in a
fresh interpreter (subprocess) so engines cannot influence each other — and
compares status, result counts, and the exact (sorted, duplicate-preserving)
result rows. Mismatch details are written to a JSON file in the temp dir.

Ported from the feat/cottas-bench branch (`smoke_compare_enignes.py`), with
the hard-coded absolute paths turned into CLI arguments and the provisional
vortex-duckdb engine dropped.
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


def split_dbbench_queries(path: Path):
    queries = []
    raw = path.read_text(encoding="utf-8", errors="replace")

    for line_no, line in enumerate(raw.splitlines(), start=1):
        q = line.strip()
        if not q:
            continue
        if not q.lower().startswith("select"):
            continue
        queries.append((line_no, q))

    return queries


def build_first_queries(query_root: Path, dataset: str, max_queries: int):
    records = []
    tp_root = query_root / "TP" / dataset

    for path in sorted(tp_root.glob("*.txt")):
        rel = path.relative_to(query_root)
        queries = split_dbbench_queries(path)

        for idx, (line_no, query) in enumerate(queries):
            records.append({
                "query_id": f"{rel}::q{idx:04d}",
                "relative_path": str(rel),
                "line_no": line_no,
                "query": query,
            })

            if len(records) >= max_queries:
                return records

    return records


def make_graph(engine: str, cottas_path: str, vortex_path: str, vortex_layout: str):
    from rdflib import Graph

    if engine == "cottas":
        from pycottas.cottas_store import COTTASStore
        return Graph(store=COTTASStore(cottas_path))

    if engine == "vortex":
        from vortex_rdflib import VortexStore
        return Graph(store=VortexStore(vortex_path, layout=vortex_layout))

    raise ValueError(engine)


def serialize_result_row(row):
    """
    Convert RDFLib ResultRow to deterministic comparable JSON.
    Keeps variable names and RDF terms in n3 form.
    """
    d = row.asdict()
    return {
        str(var): term.n3() if term is not None else None
        for var, term in d.items()
    }


def run_worker(engine, cottas_path, vortex_path, vortex_layout, queries_path, out_path):
    queries = json.loads(queries_path.read_text())
    graph = make_graph(engine, cottas_path, vortex_path, vortex_layout)

    out = []

    for i, qrec in enumerate(queries, start=1):
        print(f"[{engine}] [{i}/{len(queries)}] {qrec['query_id']}", flush=True)

        row = {
            "engine": engine,
            "query_id": qrec["query_id"],
            "relative_path": qrec["relative_path"],
            "line_no": qrec["line_no"],
            "status": "ok",
            "result_count": None,
            "results": None,
            "error": None,
        }

        try:
            result_rows = list(graph.query(qrec["query"]))

            serialized = [
                serialize_result_row(r)
                for r in result_rows
            ]

            # Sort deterministically but preserve duplicates.
            serialized_sorted = sorted(
                serialized,
                key=lambda x: json.dumps(x, sort_keys=True),
            )

            row["result_count"] = len(serialized_sorted)
            row["results"] = serialized_sorted

        except Exception as e:
            row["status"] = "error"
            row["error"] = repr(e)
            row["result_count"] = None
            row["results"] = None

        out.append(row)

    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")


def run_parent(args):
    engines = args.engines
    tmp = Path(tempfile.mkdtemp(prefix="smoke_compare_exact_"))
    queries_path = tmp / "queries.json"

    queries = build_first_queries(
        Path(args.query_root), args.dataset, args.max_queries)
    queries_path.write_text(json.dumps(queries, indent=2), encoding="utf-8")

    print("Temp dir:", tmp)
    print("Queries:", len(queries))
    print("First query:", queries[0]["query_id"] if queries else None)

    paths = {}

    for engine in engines:
        out_path = tmp / f"{engine}.json"
        paths[engine] = out_path

        cmd = [
            sys.executable,
            __file__,
            "--worker",
            "--engine", engine,
            "--queries", str(queries_path),
            "--out", str(out_path),
            "--cottas-path", args.cottas_path or "",
            "--vortex-path", args.vortex_path or "",
            "--vortex-layout", args.vortex_layout,
        ]

        print(f"\n=== Running {engine} ===")
        subprocess.run(cmd, check=True)

    loaded = {
        engine: {
            row["query_id"]: row
            for row in json.loads(path.read_text())
        }
        for engine, path in paths.items()
    }

    print("\n=== Comparing exact results ===")

    mismatches = []
    all_query_ids = [q["query_id"] for q in queries]

    for qid in all_query_ids:
        rows_by_engine = {
            engine: loaded[engine].get(qid)
            for engine in engines
        }

        # Missing row safety.
        if any(v is None for v in rows_by_engine.values()):
            mismatches.append((qid, "missing", rows_by_engine))
            continue

        statuses = {
            engine: rows_by_engine[engine]["status"]
            for engine in engines
        }

        counts = {
            engine: rows_by_engine[engine]["result_count"]
            for engine in engines
        }

        results = [rows_by_engine[engine]["results"] for engine in engines]

        same_status = len(set(statuses.values())) == 1
        same_counts = len(set(counts.values())) == 1
        same_results = all(r == results[0] for r in results[1:])

        if not (same_status and same_counts and same_results):
            mismatches.append((qid, "different", rows_by_engine))

    print(f"Total queries: {len(all_query_ids)}")
    print(f"Mismatches:    {len(mismatches)}")

    if not mismatches:
        print("✅ All exact results match.")
    else:
        print("❌ Mismatches found.")

        for qid, kind, rows_by_engine in mismatches[:20]:
            print("\n---")
            print("Query:", qid)
            print("Kind:", kind)

            for engine in engines:
                r = rows_by_engine.get(engine)
                if r is None:
                    print(f"{engine:15} MISSING")
                    continue

                print(
                    f"{engine:15} "
                    f"status={r['status']} "
                    f"count={r['result_count']} "
                    f"error={r['error']}"
                )

            # Print small result samples for diagnosis.
            print("Samples:")
            for engine in engines:
                r = rows_by_engine.get(engine)
                if r and r["results"] is not None:
                    print(f"  {engine}:")
                    for item in r["results"][:3]:
                        print("   ", item)

        mismatch_path = tmp / "mismatches.json"
        mismatch_path.write_text(
            json.dumps([
                {
                    "query_id": qid,
                    "kind": kind,
                    "rows_by_engine": rows_by_engine,
                }
                for qid, kind, rows_by_engine in mismatches
            ], indent=2),
            encoding="utf-8",
        )

        print("\nWrote mismatch details:", mismatch_path)

    print("\nAll outputs in:", tmp)
    return 1 if mismatches else 0


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--query-root", help="DBBench query tree root")
    parser.add_argument("--dataset", default="dbpedia")
    parser.add_argument("--cottas-path", default=None)
    parser.add_argument("--vortex-path", default=None)
    parser.add_argument("--vortex-layout", default="dictionary")
    parser.add_argument("--engines", nargs="+",
                        default=["cottas", "vortex"], choices=["cottas", "vortex"])
    parser.add_argument("--max-queries", type=int, default=20)

    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--engine")
    parser.add_argument("--queries")
    parser.add_argument("--out")

    args = parser.parse_args()

    if args.worker:
        run_worker(
            engine=args.engine,
            cottas_path=args.cottas_path,
            vortex_path=args.vortex_path,
            vortex_layout=args.vortex_layout,
            queries_path=Path(args.queries),
            out_path=Path(args.out),
        )
    else:
        if not args.query_root:
            parser.error("--query-root is required")
        if "cottas" in args.engines and not args.cottas_path:
            parser.error("--cottas-path is required for the cottas engine")
        if "vortex" in args.engines and not args.vortex_path:
            parser.error("--vortex-path is required for the vortex engine")
        raise SystemExit(run_parent(args))


if __name__ == "__main__":
    main()

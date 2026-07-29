#!/usr/bin/env python3
"""DBBench query benchmark over rdflib stores.

Compares engines that plug into rdflib as a Store — pycottas's COTTASStore
("cottas") and vortex-rdflib's VortexStore ("vortex") — on the DBBench query
workload. SPARQL evaluation is rdflib's engine for every store, so the store
is the only variable.

Ported from the feat/cottas-bench branch; the branch-only native diagnostics
and the provisional vortex-duckdb engine were dropped.

Requires: rdflib, psutil, vortex-rdflib (the rdflib Store package built on
this repo's vortex-rdf bindings), and pycottas when the cottas engine is
selected.
"""

from rdflib import Graph
from pathlib import Path
import time
import statistics
import random
import json
import io
import gc
import argparse
import contextlib
import csv
import psutil
import os
import signal
import multiprocessing as mp
import traceback

# Child processes are spawned, not forked: the vortex bindings hold a tokio
# runtime whose threads (and lock state) a forked child would inherit
# mid-flight. Spawn gives every child a clean interpreter.
_MP = mp.get_context("spawn")


def split_dbbench_queries(path: Path):
    """
    DBBench files contain one SPARQL SELECT query per non-empty line.
    Keep each line as-is; do not rewrite LIMIT/OFFSET/etc.
    """
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


def iter_query_files(query_root: Path, dataset: str, groups, join_sizes):
    files = []

    if "TP" in groups:
        tp_root = query_root / "TP" / dataset
        if tp_root.exists():
            files.extend(sorted(tp_root.glob("*.txt")))

    if "JOINS" in groups:
        for size in join_sizes:
            join_root = query_root / "JOINS" / dataset / size
            if join_root.exists():
                files.extend(sorted(join_root.glob("*.txt")))

    return files


def build_query_records(query_root: Path, dataset: str, groups, join_sizes):
    records = []

    for path in iter_query_files(query_root, dataset, groups, join_sizes):
        rel = path.relative_to(query_root)
        queries = split_dbbench_queries(path)

        for idx, (line_no, query) in enumerate(queries):
            parts = rel.parts
            top_group = parts[0]
            size_group = None

            if top_group == "JOINS":
                size_group = parts[2]

            records.append({
                "query_id": f"{rel}::q{idx:04d}",
                "relative_path": str(rel),
                "top_group": top_group,
                "dataset": dataset,
                "size_group": size_group,
                "file_name": path.name,
                "query_index_in_file": idx,
                "line_no": line_no,
                "contains_limit": "limit" in query.lower(),
                "query": query,
            })

    return records


def make_graph(engine: str, cottas_path: str, vortex_path: str, vortex_layout: str):
    if engine == "cottas":
        from pycottas.cottas_store import COTTASStore
        return Graph(store=COTTASStore(cottas_path))

    if engine == "vortex":
        from vortex_rdflib import VortexStore
        return Graph(store=VortexStore(vortex_path, layout=vortex_layout))

    raise ValueError(f"Unknown engine: {engine}")


_process = psutil.Process(os.getpid())


class QueryTimeoutError(TimeoutError):
    pass


def _query_timeout_handler(signum, frame):
    raise QueryTimeoutError("Query exceeded timeout")


def run_one_query(graph: Graph, query: str, silence_stdout: bool, timeout_s: float):
    """
    Measure:
    - elapsed time
    - result count
    - memory usage (RSS)

    Raises QueryTimeoutError if the query exceeds timeout_s.
    """
    gc.collect()

    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss

    sink = io.StringIO()
    stdout_ctx = contextlib.redirect_stdout(
        sink
    ) if silence_stdout else contextlib.nullcontext()

    old_handler = signal.getsignal(signal.SIGALRM)

    start_ns = time.perf_counter_ns()
    try:
        if timeout_s is not None and timeout_s > 0:
            signal.signal(signal.SIGALRM, _query_timeout_handler)
            signal.setitimer(signal.ITIMER_REAL, timeout_s)

        with stdout_ctx:
            rows = list(graph.query(query))

    finally:
        if timeout_s is not None and timeout_s > 0:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)

    end_ns = time.perf_counter_ns()
    mem_after = process.memory_info().rss
    out = {
        "elapsed_s": (end_ns - start_ns) / 1_000_000_000,
        "result_count": len(rows),
        "rss_before_mb": mem_before / (1024 * 1024),
        "rss_after_mb": mem_after / (1024 * 1024),
        "rss_delta_mb": (mem_after - mem_before) / (1024 * 1024),
    }
    return out


class QueryProcessTimeoutError(TimeoutError):
    pass


def _run_one_query_child(
    queue,
    engine: str,
    cottas_path: str,
    vortex_path: str,
    vortex_layout: str,
    query: str,
    silence_stdout: bool,
):
    """
    Child-process query runner.

    Important: do not use SIGALRM here. The whole point is that a blocking
    PyO3/Rust call might not yield to Python's signal machinery. If it blocks,
    the parent kills this entire child process.
    """
    try:
        graph = make_graph(engine, cottas_path, vortex_path, vortex_layout)
        out = run_one_query(
            graph,
            query,
            silence_stdout=silence_stdout,
            timeout_s=None,
        )
        queue.put({"status": "ok", "out": out})
    except BaseException as e:
        queue.put({
            "status": "error",
            "error": repr(e),
            "traceback": traceback.format_exc(),
        })


def _kill_process_tree(pid: int, grace_s: float = 1.0):
    """Terminate a child and any descendants, then kill survivors."""
    try:
        parent = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return

    children = parent.children(recursive=True)
    processes = children + [parent]
    for proc in processes:
        try:
            proc.terminate()
        except psutil.NoSuchProcess:
            pass

    gone, alive = psutil.wait_procs(processes, timeout=max(grace_s, 0.0))
    for proc in alive:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(alive, timeout=max(grace_s, 0.0))


def run_one_query_process_timeout(
    engine: str,
    cottas_path: str,
    vortex_path: str,
    vortex_layout: str,
    query: str,
    silence_stdout: bool,
    timeout_s: float,
    kill_grace_s: float,
):
    """
    Robust per-query timeout wrapper.

    Runs one query in a child process. If the child is still alive after
    timeout_s, kill the process tree. This works even when the query is blocked
    inside PyO3/Rust and Python SIGALRM would not interrupt it.
    """
    start_ns = time.perf_counter_ns()
    queue = _MP.Queue(maxsize=1)
    proc = _MP.Process(
        target=_run_one_query_child,
        args=(
            queue,
            engine,
            cottas_path,
            vortex_path,
            vortex_layout,
            query,
            silence_stdout,
        ),
    )
    proc.start()

    join_timeout = timeout_s if timeout_s is not None and timeout_s > 0 else None
    proc.join(join_timeout)

    if proc.is_alive():
        _kill_process_tree(proc.pid, grace_s=kill_grace_s)
        proc.join(timeout=max(kill_grace_s, 0.0))
        elapsed_s = (time.perf_counter_ns() - start_ns) / 1_000_000_000
        raise QueryProcessTimeoutError(
            f"Query exceeded process timeout of {timeout_s}s; killed child pid={proc.pid}; elapsed={elapsed_s:.3f}s"
        )

    if proc.exitcode not in (0, None):
        # Try to surface a structured child error first, but do not block.
        try:
            msg = queue.get_nowait()
            if msg.get("status") == "error":
                raise RuntimeError(
                    msg.get("error", f"child exitcode={proc.exitcode}"))
        except Exception as e:
            if isinstance(e, RuntimeError):
                raise
        raise RuntimeError(
            f"Query child process exited with code {proc.exitcode}")

    try:
        msg = queue.get(timeout=1.0)
    except Exception as e:
        raise RuntimeError(f"Query child process produced no result: {e!r}")

    if msg.get("status") == "ok":
        return msg["out"]
    raise RuntimeError(msg.get("error", "unknown child-process query error"))


def _persistent_query_worker(
    connection,
    engine: str,
    cottas_path: str,
    vortex_path: str,
    vortex_layout: str,
):
    """Own one persistent Graph and execute parent-issued requests serially."""
    try:
        graph = make_graph(engine, cottas_path, vortex_path, vortex_layout)
        connection.send({"kind": "ready"})
    except BaseException as error:
        try:
            connection.send({
                "kind": "startup_error",
                "error": repr(error),
                "traceback": traceback.format_exc(),
            })
        finally:
            connection.close()
        return

    try:
        while True:
            request = connection.recv()
            if request.get("kind") == "shutdown":
                return
            request_id = request["request_id"]
            try:
                out = run_one_query(
                    graph,
                    request["query"],
                    silence_stdout=request["silence_stdout"],
                    timeout_s=None,
                )
                connection.send({
                    "kind": "result",
                    "request_id": request_id,
                    "status": "ok",
                    "out": out,
                })
            except BaseException as error:
                connection.send({
                    "kind": "result",
                    "request_id": request_id,
                    "status": "error",
                    "error": repr(error),
                    "traceback": traceback.format_exc(),
                })
    except (EOFError, BrokenPipeError):
        return
    finally:
        connection.close()


class PersistentQueryWorker:
    """Persistent, killable Graph worker restarted after a hard timeout."""

    def __init__(
        self,
        engine: str,
        cottas_path: str,
        vortex_path: str,
        vortex_layout: str,
        startup_timeout_s: float,
        kill_grace_s: float,
    ):
        self.engine = engine
        self.cottas_path = cottas_path
        self.vortex_path = vortex_path
        self.vortex_layout = vortex_layout
        self.startup_timeout_s = startup_timeout_s
        self.kill_grace_s = kill_grace_s
        self._connection = None
        self._process = None
        self._next_request_id = 0

    def _discard(self):
        connection, process = self._connection, self._process
        self._connection = None
        self._process = None
        if connection is not None:
            connection.close()
        if process is not None and process.is_alive():
            _kill_process_tree(process.pid, grace_s=self.kill_grace_s)
            process.join(timeout=max(self.kill_grace_s, 0.0))

    def _start(self):
        self._discard()
        parent_connection, child_connection = _MP.Pipe(duplex=True)
        process = _MP.Process(
            target=_persistent_query_worker,
            args=(
                child_connection,
                self.engine,
                self.cottas_path,
                self.vortex_path,
                self.vortex_layout,
            ),
            name=f"dbbench-{self.engine}-worker",
            daemon=True,
        )
        process.start()
        child_connection.close()
        self._connection = parent_connection
        self._process = process

        if not parent_connection.poll(self.startup_timeout_s):
            pid = process.pid
            self._discard()
            raise RuntimeError(
                f"{self.engine} worker did not initialize within "
                f"{self.startup_timeout_s}s; killed pid={pid}"
            )
        message = parent_connection.recv()
        if message.get("kind") != "ready":
            error = message.get("error", "unknown worker startup failure")
            self._discard()
            raise RuntimeError(f"{self.engine} worker startup failed: {error}")

    def run(self, query: str, silence_stdout: bool, timeout_s: float):
        if self._process is None or not self._process.is_alive():
            self._start()
        request_id = self._next_request_id
        self._next_request_id += 1
        self._connection.send({
            "kind": "query",
            "request_id": request_id,
            "query": query,
            "silence_stdout": silence_stdout,
        })
        wait_timeout = timeout_s if timeout_s is not None and timeout_s > 0 else None
        if not self._connection.poll(wait_timeout):
            pid = self._process.pid
            self._discard()
            raise QueryProcessTimeoutError(
                f"Query exceeded worker timeout of {timeout_s}s; killed worker pid={pid}"
            )
        try:
            message = self._connection.recv()
        except EOFError as error:
            exitcode = self._process.exitcode
            self._discard()
            raise RuntimeError(
                f"{self.engine} worker exited without a response; exitcode={exitcode}"
            ) from error
        if message.get("kind") != "result":
            self._discard()
            raise RuntimeError(f"Unexpected {self.engine} worker message: {message!r}")
        if message.get("request_id") != request_id:
            self._discard()
            raise RuntimeError(
                f"{self.engine} worker response ID mismatch: "
                f"expected={request_id}, actual={message.get('request_id')}"
            )
        if message.get("status") == "ok":
            return message["out"]
        raise RuntimeError(message.get("error", "unknown persistent-worker query error"))

    def close(self):
        if self._connection is not None and self._process is not None:
            if self._process.is_alive():
                try:
                    self._connection.send({"kind": "shutdown"})
                    self._process.join(timeout=max(self.kill_grace_s, 0.0))
                except (BrokenPipeError, EOFError, OSError):
                    pass
        self._discard()


def summarize(results):
    grouped = {}

    for row in results:
        if row["status"] != "ok" or row["phase"] != "measured":
            continue

        key = (
            row["engine"],
            row.get("vortex_layout"),
            row["query_id"],
            row["relative_path"],
            row["top_group"],
            row["size_group"],
        )
        grouped.setdefault(key, []).append(row["elapsed_s"])

    out = []

    for key, times in grouped.items():
        engine, vortex_layout, query_id, relative_path, top_group, size_group = key
        out.append({
            "engine": engine,
            "vortex_layout": vortex_layout,
            "query_id": query_id,
            "relative_path": relative_path,
            "top_group": top_group,
            "size_group": size_group,
            "runs": len(times),
            "mean_s": statistics.mean(times),
            "median_s": statistics.median(times),
            "min_s": min(times),
            "max_s": max(times),
            "stdev_s": statistics.stdev(times) if len(times) > 1 else 0.0,
        })

    return out


def write_csv(path: Path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--query-root", required=True)
    parser.add_argument("--dataset", default="dbpedia")
    parser.add_argument(
        "--groups",
        nargs="+",
        default=["TP", "JOINS"],
        choices=["TP", "JOINS"],
    )
    parser.add_argument(
        "--join-sizes",
        nargs="+",
        default=["small", "big"],
        choices=["small", "big"],
    )

    parser.add_argument("--cottas-path", default=None,
                        help="COTTAS file; required when the cottas engine is selected")
    parser.add_argument("--vortex-path", default=None,
                        help=".vortex file; required when the vortex engine is selected")
    parser.add_argument(
        "--vortex-layout", default="dictionary",
        help="Label recorded in results (the store detects the actual layout from the file)",
    )

    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--measured-runs", type=int, default=5)
    parser.add_argument(
        "--skip-after-warmup-timeout",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip measured runs when the warmup times out (default: enabled)",
    )
    parser.add_argument(
        "--skip-after-warmup-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip measured runs when the warmup errors (default: enabled)",
    )

    # Per-query timeout in seconds. Use 0 or a negative value to disable.
    parser.add_argument("--query-timeout-s", type=float, default=60.0)
    parser.add_argument(
        "--timeout-mode",
        choices=["process", "signal", "worker"],
        default="process",
        help=(
            "process = fresh child/Graph per run; signal = persistent in-process "
            "Graph with unsafe native-call interruption; worker = persistent "
            "killable child/Graph restarted after timeout"
        ),
    )
    parser.add_argument(
        "--timeout-kill-grace-s",
        type=float,
        default=1.0,
        help="Seconds to wait after terminate() before kill() in process timeout mode",
    )

    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=42)

    parser.add_argument("--max-queries", type=int, default=None)
    parser.add_argument("--only-file-contains", default=None)
    parser.add_argument(
        "--query-id-file",
        default=None,
        help="Text file containing one exact query_id per line; blank lines and # comments are ignored",
    )

    parser.add_argument("--out-prefix", default="dbbench_rdflib")
    parser.add_argument("--no-silence-stdout", action="store_true")
    parser.add_argument(
        "--engines",
        nargs="+",
        default=["cottas", "vortex"],
        choices=["cottas", "vortex"],
    )

    args = parser.parse_args()

    if "cottas" in args.engines:
        if not args.cottas_path or not Path(args.cottas_path).is_file():
            parser.error(
                f"--cottas-path must identify an existing COTTAS file: {args.cottas_path}")
    if "vortex" in args.engines:
        vortex_path = Path(args.vortex_path) if args.vortex_path else None
        if vortex_path is None or not vortex_path.is_file():
            parser.error(
                "--vortex-path must identify the main .vortex artifact file, "
                f"not its containing directory: {vortex_path}"
            )
        if vortex_path.suffix != ".vortex":
            parser.error(f"--vortex-path must end in .vortex: {vortex_path}")

    query_root = Path(args.query_root)
    out_prefix = Path(args.out_prefix)

    query_records = build_query_records(
        query_root=query_root,
        dataset=args.dataset,
        groups=args.groups,
        join_sizes=args.join_sizes,
    )

    if args.only_file_contains:
        query_records = [
            r for r in query_records
            if args.only_file_contains in r["relative_path"]
        ]
    if args.query_id_file:
        selected_ids = {
            line.strip()
            for line in Path(args.query_id_file).read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        known_ids = {r["query_id"] for r in query_records}
        missing_ids = sorted(selected_ids - known_ids)
        if missing_ids:
            raise SystemExit(
                "Unknown query IDs in --query-id-file:\n  " + "\n  ".join(missing_ids))
        query_records = [
            r for r in query_records if r["query_id"] in selected_ids]

    if args.shuffle:
        rng = random.Random(args.seed)
        rng.shuffle(query_records)

    if args.max_queries is not None:
        query_records = query_records[:args.max_queries]

    print(f"Loaded {len(query_records)} individual queries")
    print(
        f"Timeout mode: {args.timeout_mode}; query timeout: {args.query_timeout_s}s; kill grace: {args.timeout_kill_grace_s}s")

    inventory_path = out_prefix.with_suffix(".queries.json")
    inventory_path.write_text(
        json.dumps(query_records, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote query inventory: {inventory_path}")

    workers = {}
    if args.timeout_mode == "signal":
        graphs = {
            engine: make_graph(
                engine,
                args.cottas_path,
                args.vortex_path,
                args.vortex_layout,
            )
            for engine in args.engines
        }
    elif args.timeout_mode == "worker":
        graphs = {}
        workers = {
            engine: PersistentQueryWorker(
                engine=engine,
                cottas_path=args.cottas_path,
                vortex_path=args.vortex_path,
                vortex_layout=args.vortex_layout,
                startup_timeout_s=max(args.query_timeout_s, 30.0),
                kill_grace_s=args.timeout_kill_grace_s,
            )
            for engine in args.engines
        }
    else:
        # In process-timeout mode each query run creates its own child process
        # and graph. This is more expensive, but it prevents a blocked PyO3/Rust
        # call from hanging the entire benchmark driver.
        graphs = {}

    results = []
    silence_stdout = not args.no_silence_stdout

    for qrec_idx, qrec in enumerate(query_records):
        print(f"[{qrec_idx + 1}/{len(query_records)}] {qrec['query_id']}")

        counts_by_engine = {}

        for engine in args.engines:
            # In process-timeout mode we intentionally do not keep Graph objects
            # in the parent. Each run creates the graph inside a child process,
            # so a blocked PyO3/Rust call can be killed safely.
            graph = graphs.get(engine)

            total_runs = args.warmup_runs + args.measured_runs
            skip_remaining_reason = None

            for run_idx in range(total_runs):
                phase = "warmup" if run_idx < args.warmup_runs else "measured"
                measured_run_idx = run_idx - args.warmup_runs

                row = {
                    "engine": engine,
                    "vortex_layout": args.vortex_layout if engine == "vortex" else None,
                    "phase": phase,
                    "run": measured_run_idx if phase == "measured" else run_idx,
                    "query_id": qrec["query_id"],
                    "relative_path": qrec["relative_path"],
                    "top_group": qrec["top_group"],
                    "dataset": qrec["dataset"],
                    "size_group": qrec["size_group"],
                    "file_name": qrec["file_name"],
                    "query_index_in_file": qrec["query_index_in_file"],
                    "line_no": qrec["line_no"],
                    "contains_limit": qrec["contains_limit"],
                    "status": "ok",
                    "elapsed_s": None,
                    "result_count": None,
                    "rss_before_mb": None,
                    "rss_after_mb": None,
                    "rss_delta_mb": None,
                    "error": None,
                }

                if skip_remaining_reason is not None:
                    row["status"] = "skipped"
                    row["error"] = skip_remaining_reason
                    results.append(row)
                    print(
                        f"  {engine:13s} {phase:8s} run={row['run']} "
                        f"status=skipped time=None rows=None"
                    )
                    continue

                try:
                    if args.timeout_mode == "process":
                        out = run_one_query_process_timeout(
                            engine=engine,
                            cottas_path=args.cottas_path,
                            vortex_path=args.vortex_path,
                            vortex_layout=args.vortex_layout,
                            query=qrec["query"],
                            silence_stdout=silence_stdout,
                            timeout_s=args.query_timeout_s,
                            kill_grace_s=args.timeout_kill_grace_s,
                        )
                    elif args.timeout_mode == "worker":
                        out = workers[engine].run(
                            qrec["query"],
                            silence_stdout=silence_stdout,
                            timeout_s=args.query_timeout_s,
                        )
                    else:
                        out = run_one_query(
                            graph,
                            qrec["query"],
                            silence_stdout=silence_stdout,
                            timeout_s=args.query_timeout_s,
                        )
                    row.update(out)
                    if phase == "measured" and measured_run_idx == 0:
                        counts_by_engine[engine] = row["result_count"]

                except (QueryTimeoutError, QueryProcessTimeoutError) as e:
                    row["status"] = "timeout"
                    row["elapsed_s"] = args.query_timeout_s
                    row["result_count"] = None
                    row["error"] = str(e)

                    # Parent RSS is meaningful only for in-process signal mode.
                    if args.timeout_mode == "signal":
                        try:
                            mem_after = _process.memory_info().rss
                            row["rss_after_mb"] = mem_after / (1024 * 1024)
                        except Exception:
                            pass
                    if phase == "warmup" and args.skip_after_warmup_timeout:
                        skip_remaining_reason = (
                            f"Skipped because warmup timed out after {args.query_timeout_s}s"
                        )

                except Exception as e:
                    row["status"] = "error"
                    row["error"] = repr(e)
                    if phase == "warmup" and args.skip_after_warmup_error:
                        skip_remaining_reason = (
                            "Skipped because warmup failed with error: "
                            f"{type(e).__name__}: {e}"
                        )

                results.append(row)

                print(
                    f"  {engine:13s} {phase:8s} run={row['run']} "
                    f"status={row['status']} time={row['elapsed_s']} "
                    f"rows={row['result_count']}"
                )

        if (
            set(args.engines) == {"cottas", "vortex"}
            and "cottas" in counts_by_engine
            and "vortex" in counts_by_engine
        ):
            if counts_by_engine["cottas"] != counts_by_engine["vortex"]:
                print(
                    "  WARNING count mismatch: "
                    f"cottas={counts_by_engine['cottas']} "
                    f"vortex={counts_by_engine['vortex']}"
                )

    for worker in workers.values():
        worker.close()
    raw_json = out_prefix.with_suffix(".raw.json")
    raw_csv = out_prefix.with_suffix(".raw.csv")
    summary_json = out_prefix.with_suffix(".summary.json")
    summary_csv = out_prefix.with_suffix(".summary.csv")

    raw_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_csv(raw_csv, results)

    summary_rows = summarize(results)
    summary_json.write_text(json.dumps(
        summary_rows, indent=2), encoding="utf-8")
    write_csv(summary_csv, summary_rows)

    print(f"Wrote {raw_json}")
    print(f"Wrote {raw_csv}")
    print(f"Wrote {summary_json}")
    print(f"Wrote {summary_csv}")


if __name__ == "__main__":
    main()

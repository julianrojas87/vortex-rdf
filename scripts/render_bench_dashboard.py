#!/usr/bin/env python3
"""Turn a `cargo bench --bench benchmark` run into the static HTML dashboard.

Usage:
    cargo bench --bench benchmark | tee bench_output.txt
    python3 scripts/render_bench_dashboard.py bench_output.txt public/index.html

Divan (via codspeed-divan-compat) has no machine-readable output mode, so this
parses its tree-table text output directly. The tree-drawing glyph (U+2502,
"│") is reused for both the indentation guide on nested rows and the column
separator, so a naive split on "│" misaligns nested rows by one column --
the tree prefix is stripped first, and the name/first-value pair (which share
a cell with no separator between them) is split on runs of 2+ spaces instead.
"""
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PREFIX_RE = re.compile(r"^[\s│]*([├╰])─\s*(.*)$")
UNIT_NS = {"ns": 1.0, "µs": 1_000.0, "us": 1_000.0, "ms": 1_000_000.0, "s": 1_000_000_000.0}


def to_ns(value_str):
    value_str = value_str.strip()
    if not value_str:
        return None
    m = re.match(r"^([0-9.]+)\s*(ns|µs|us|ms|s)$", value_str)
    if not m:
        return None
    num, unit = m.groups()
    return float(num) * UNIT_NS[unit]


def parse_bench_output(text):
    lines = text.splitlines()
    start = 0
    timer_precision = None
    for i, line in enumerate(lines):
        m = re.match(r"^Timer precision:\s*(.+)$", line)
        if m:
            timer_precision = m.group(1).strip()
        if line.startswith("benchmark") and "fastest" in line:
            start = i + 1
            break

    results = []
    current_group = None

    for line in lines[start:]:
        if not line.strip():
            continue
        m = PREFIX_RE.match(line)
        if not m:
            continue

        is_top = line[0] in "├╰"
        rest = m.group(2)

        cols = rest.split("│")
        name_and_fastest = cols[0].strip()
        rest_values = [c.strip() for c in cols[1:]]

        pieces = re.split(r"\s{2,}", name_and_fastest) if name_and_fastest else []
        name = pieces[0] if pieces else ""
        fastest = pieces[1] if len(pieces) > 1 else ""

        values = [fastest] + rest_values
        has_values = any(v for v in values)

        if is_top and not has_values:
            current_group = name
            continue

        if is_top and has_values:
            current_group = None
            group, variant = name, None
        else:
            group, variant = current_group, name

        while len(values) < 6:
            values.append("")
        fastest, slowest, median, mean, samples, iters = values[:6]

        results.append({
            "group": group,
            "variant": variant,
            "id": f"{group}::{variant}" if variant else group,
            "fastest": fastest,
            "slowest": slowest,
            "median": median,
            "mean": mean,
            "fastest_ns": to_ns(fastest),
            "slowest_ns": to_ns(slowest),
            "median_ns": to_ns(median),
            "mean_ns": to_ns(mean),
            "samples": samples,
            "iters": iters,
        })

    return results, timer_precision


def parse_dataset_shape(text):
    """The `#dataset {...}` line each Rust bench target prints before measuring.

    The moduli follow from the row count through a coprimality nudge, so the
    selectivity of a run is not something the page can restate from a formula --
    it changes with `BENCH_SIZE`. Reading the shape off the run makes the page's
    matched-row figures a record rather than a re-derivation, which any
    hardcoded arithmetic would silently outlive.

    Both targets stamp their own line into the shared log; they run at one
    `BENCH_SIZE`, so a disagreement means the two halves of the tab measured
    different data and is worth saying out loud.
    """
    found = []
    for line in text.splitlines():
        if line.startswith("#dataset "):
            try:
                found.append(json.loads(line[len("#dataset "):]))
            except json.JSONDecodeError:
                print(f"note: unparseable dataset line: {line!r}", file=sys.stderr)
    if not found:
        return None
    if any(d != found[0] for d in found[1:]):
        print("WARNING: the bench targets report different dataset shapes; using the first",
              file=sys.stderr)
    return found[0]


def fmt_duration(ns):
    """One duration format for the whole page: at most 2 decimals, and none on ns.

    The four sources spell their own numbers differently -- divan uses 4
    significant digits, and the three harnesses each format their own strings --
    so a page built from all of them shows `281.0000 ns` beside `3.427 µs` beside
    `2.781 ms`. Rendering every duration from its `*_ns` value here is the only
    place that sees all four, so it is the only place the page can be made to
    read consistently. Sub-nanosecond counts round to whole nanoseconds because a
    fractional nanosecond is below any timer this dashboard uses.
    """
    if ns is None:
        return None
    # 999.6 must promote to 1 µs, not round to "1000 ns".
    if ns < 999.5:
        return f"{ns:.0f} ns"
    for unit, scale in (("µs", 1_000.0), ("ms", 1_000_000.0), ("s", 1_000_000_000.0)):
        if ns < scale * 1_000 or unit == "s":
            v = ns / scale
            # Trim trailing zeros so 3.40 reads as 3.4 and 216.00 as 216.
            return f"{v:.2f}".rstrip("0").rstrip(".") + f" {unit}"
    return f"{ns:.0f} ns"


def normalize_durations(rows):
    """Rewrite every row's display strings from its `*_ns` values.

    Leaves a row alone where the value is None -- an `unsupported` cell carries a
    word, not a duration, and must not be turned into `0 ns`.
    """
    for r in rows:
        for key in ("fastest", "slowest", "median", "mean"):
            ns = r.get(f"{key}_ns")
            if ns is not None:
                r[key] = fmt_duration(ns)
    return rows


def git(*args, default=""):
    try:
        return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True).strip()
    except Exception:
        return default


def cpu_model():
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unknown CPU"


def default_bench_size():
    """The fallback dataset size baked into `core/benches/support/mod.rs`'s
    `bench_size()`, i.e. what a run used if it didn't override `BENCH_SIZE`."""
    src = (REPO_ROOT / "core" / "benches" / "support" / "mod.rs").read_text(encoding="utf-8")
    m = re.search(r"fn bench_size.*?unwrap_or\(([\d_]+)\)", src, re.DOTALL)
    return int(m.group(1).replace("_", "")) if m else None


def measured_at(source=None):
    """The dashboard's shared "Measured …" stamp: UTC, to the minute.

    All three tabs date themselves this way — `js/bench/compare.bench.ts` and
    `python/bench/run.py` build the same string for the lines they emit — so one
    page never shows three formats (or three timezones) for one run.

    `source` is the file whose measurement is being stamped; its mtime is when
    that suite finished writing, which is what the other two tabs report. Only
    when it is missing does this fall back to now, which for a render long after
    the bench would overstate how fresh the numbers are.
    """
    when = datetime.now(timezone.utc)
    if source is not None:
        try:
            when = datetime.fromtimestamp(source.stat().st_mtime, timezone.utc)
        except OSError:
            pass
    return when.strftime("%Y-%m-%d %H:%M UTC")


def sample_counts(results):
    """The divan sample counts the run actually used, most-used first.

    The suite is not uniform: queries carry `QUERY_SAMPLES` and the
    seconds-per-iteration phases carry `HEAVY_SAMPLES` (see
    `core/benches/support/mod.rs`), so no single number describes it. Reading
    them off the rows rather than the source reports what this run did.
    """
    counts = {}
    for r in results:
        counts[r["samples"]] = counts.get(r["samples"], 0) + 1
    return [c for c, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def build_rust_config(results, bench_size=None, dataset=None):
    """Context the Rust tab needs but divan's text output does not carry.

    The JavaScript and Python harnesses emit their own config alongside their
    results; the Rust suite is scraped from stdout, so its dataset size and
    repetition counts have to be reconstructed here and injected -- otherwise
    the template can only hardcode them, which is how it came to claim 100,000
    quads and 3 samples long after both had changed.

    `dataset` is the run's own `#dataset` stamp and outranks both the CLI
    override and the source default: it is the row count the binary generated,
    not the one the caller meant to ask for.
    """
    samples = sample_counts(results)
    size = bench_size if bench_size is not None else default_bench_size()
    if dataset:
        size = dataset.get("quads", size)
    return {
        "benchSize": size,
        "querySamples": samples[0] if samples else None,
        "heavySamples": samples[1] if len(samples) > 1 else None,
        "dataset": dataset,
    }


def build_provenance(results, timer_precision, bench_size=None, source=None):
    commit = os.environ.get("GITHUB_SHA", git("rev-parse", "HEAD"))[:7]
    branch = os.environ.get("GITHUB_REF_NAME", git("rev-parse", "--abbrev-ref", "HEAD", default="unknown"))
    samples = sample_counts(results)
    samples_str = f"{samples[0]} samples/benchmark" if samples else "? samples/benchmark"
    if len(samples) > 1:
        samples_str += f" ({', '.join(samples[1:])} for the heavy phases)"
    size = bench_size if bench_size is not None else default_bench_size()
    size_str = f"{size:,}" if size else "unknown"
    precision = f", {timer_precision} precision" if timer_precision else ""

    return (
        f"Measured {measured_at(source)} · commit {commit} ({branch}) · {cpu_model()}, {os.cpu_count()} threads · "
        f"BENCH_SIZE = {size_str} quads · {samples_str} · "
        f"codspeed-divan-compat, wall-clock (os) timer{precision}"
    )


def check_script_syntax(html, out_path):
    """Parse the page's inline JavaScript, and fail the render if it does not.

    One unescaped quote inside a `note:"..."` string is a syntax error, and a
    syntax error in the single inline script takes down EVERY table, tile and
    panel on all three tabs -- the page still opens, still looks structurally
    fine in a text editor, and renders nothing. That shipped once (an
    unescaped `class="mono"` in a panel note), so it is worth a gate rather
    than a habit.

    Uses `node --check`, which is present wherever the JavaScript suite runs.
    Where node is missing the check is skipped with a warning rather than
    blocking a render that is probably fine.
    """
    blocks = re.findall(r"<script>(.*?)</script>", html, re.DOTALL)
    if not blocks:
        return True
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as f:
        f.write("\n".join(blocks))
        tmp = f.name
    try:
        proc = subprocess.run(["node", "--check", tmp], capture_output=True, text=True)
    except FileNotFoundError:
        print("note: node not found, skipping the page's JavaScript syntax check", file=sys.stderr)
        return True
    finally:
        os.unlink(tmp)
    if proc.returncode == 0:
        return True
    print(
        f"ERROR: {out_path} contains invalid JavaScript -- the whole page would render "
        f"blank. Most likely an unescaped quote in a panel note.\n{proc.stderr.strip()}",
        file=sys.stderr,
    )
    return False


def load_rust_compare(path):
    """Load `core/benches/compare.rs`'s output -- the Rust tab's cross-library
    comparison against oxigraph, sophia and hdt.

    Same dashboard shape as the JavaScript and Python comparative suites
    (`{"results": [...], "sizes": [...], "config": {...}}`), because that suite
    emits the rows directly rather than being scraped from divan text. Returns
    `(results_list, sizes_list, memory_list, config_dict)` -- `memory` holds one
    `{slug, label, role, peakRssMb}` entry per library process, like its JavaScript
    and Python counterparts.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return (
        data.get("results", []),
        data.get("sizes", []),
        data.get("memory", []),
        data.get("config", {}),
    )


def load_js_results(path):
    """Load the JavaScript benchmark results emitted by `js/bench/compare.bench.ts`.

    That file is already dashboard-shaped. It may be either a bare list of result
    rows, or an object `{"provenance": str, "results": [...], "memory": [...],
    "config": {...}}` (`memory` holds one `{slug, label, role, peakRssMb}` entry per
    adapter process -- see shared.ts's `peakRssMb`; `sizes` holds each store's
    serialized-snapshot byte size (null where no serialization path exists);
    `config` holds the dataset sizes and iteration counts the run used). Returns
    `(results_list, provenance_str, memory_list, sizes_list, config_dict)`.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return (
            data.get("results", []),
            data.get("provenance", ""),
            data.get("memory", []),
            data.get("sizes", []),
            data.get("failures", []),
            data.get("config", {}),
        )
    return data, "", [], [], [], {}


def load_python_results(path):
    """Load the Python benchmark results emitted by `python/bench/run.py`.

    Same dashboard shape as the JavaScript results plus `sizes`, a list of
    `{slug, label, bytes}` giving each library's on-disk artifact -- the Python
    comparison is between file-backed formats, so artifact size is a first-class
    axis there rather than an afterthought. `bytes` is null for a library whose
    store lives only in memory.

    Rows may additionally carry `"unsupported": true` with a `reason`, for an
    operation a library's API does not offer at all (see the mutation column).
    Those rows have `median_ns: null` so the panels exclude them from the
    column's best/ratio arithmetic.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return (
        data.get("results", []),
        data.get("provenance", ""),
        data.get("memory", []),
        data.get("sizes", []),
        data.get("config", {}),
    )


def main():
    if len(sys.argv) < 3:
        print(
            f"usage: {sys.argv[0]} <bench-output.txt> <output.html> "
            "[--js <js-results.json>] [--python <py-results.json>] "
            "[--rust-compare <compare-results.json>] [--bench-size <n>]",
            file=sys.stderr,
        )
        return 1

    positional = [a for a in sys.argv[1:] if not a.startswith("--")]
    in_path, out_path = Path(positional[0]), Path(positional[1])
    js_path = None
    if "--js" in sys.argv:
        js_path = sys.argv[sys.argv.index("--js") + 1]
    py_path = None
    if "--python" in sys.argv:
        py_path = sys.argv[sys.argv.index("--python") + 1]
    rc_path = None
    if "--rust-compare" in sys.argv:
        rc_path = sys.argv[sys.argv.index("--rust-compare") + 1]

    bench_size_override = None
    if "--bench-size" in sys.argv:
        bench_size_override = int(sys.argv[sys.argv.index("--bench-size") + 1])

    template_path = Path(__file__).resolve().parent / "bench_dashboard_template.html"

    bench_text = in_path.read_text(encoding="utf-8")
    results, timer_precision = parse_bench_output(bench_text)
    rust_dataset = parse_dataset_shape(bench_text)
    if rust_dataset and bench_size_override not in (None, rust_dataset.get("quads")):
        print(
            f"note: --bench-size {bench_size_override} disagrees with the run's own "
            f"{rust_dataset.get('quads')} quads; using the run's",
            file=sys.stderr,
        )
        bench_size_override = rust_dataset["quads"]
    if not results:
        print("no benchmark results parsed -- is the input the raw `cargo bench` output?", file=sys.stderr)
        return 1

    js_results, js_provenance, js_memory, js_sizes, js_failures, js_config = ([], "", [], [], [], {})
    if js_path:
        js_results, js_provenance, js_memory, js_sizes, js_failures, js_config = load_js_results(js_path)

    rc_results, rc_sizes, rc_memory, rc_config = ([], [], [], {})
    if rc_path:
        rc_results, rc_sizes, rc_memory, rc_config = load_rust_compare(rc_path)
        rc_config["measuredAt"] = measured_at(Path(rc_path))

    py_results, py_provenance, py_memory, py_sizes, py_config = ([], "", [], [], {})
    if py_path:
        py_results, py_provenance, py_memory, py_sizes, py_config = load_python_results(py_path)

    # One duration format across all four sources (see `fmt_duration`).
    for rows in (results, rc_results, js_results, py_results):
        normalize_durations(rows)

    provenance = build_provenance(results, timer_precision, bench_size_override, in_path)
    template = template_path.read_text(encoding="utf-8")
    out = (
        template
        .replace("__BENCH_DATA__", json.dumps(results))
        .replace("__PROVENANCE__", json.dumps(provenance))
        .replace("__RUST_CONFIG_DATA__",
                 json.dumps(build_rust_config(results, bench_size_override, rust_dataset)))
        .replace("__RUST_COMPARE_DATA__", json.dumps(rc_results))
        .replace("__RUST_COMPARE_SIZES__", json.dumps(rc_sizes))
        .replace("__RUST_COMPARE_MEMORY__", json.dumps(rc_memory))
        .replace("__RUST_COMPARE_CONFIG__", json.dumps(rc_config))
        .replace("__JS_BENCH_DATA__", json.dumps(js_results))
        .replace("__JS_PROVENANCE__", json.dumps(js_provenance))
        .replace("__JS_MEMORY_DATA__", json.dumps(js_memory))
        .replace("__JS_SIZE_DATA__", json.dumps(js_sizes))
        .replace("__JS_FAILURE_DATA__", json.dumps(js_failures))
        .replace("__JS_CONFIG_DATA__", json.dumps(js_config))
        .replace("__PY_BENCH_DATA__", json.dumps(py_results))
        .replace("__PY_PROVENANCE__", json.dumps(py_provenance))
        .replace("__PY_MEMORY_DATA__", json.dumps(py_memory))
        .replace("__PY_SIZE_DATA__", json.dumps(py_sizes))
        .replace("__PY_CONFIG_DATA__", json.dumps(py_config))
    )

    if not check_script_syntax(out, out_path):
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(out, encoding="utf-8")
    print(f"parsed {len(results)} Rust + {len(rc_results)} Rust-compare + {len(js_results)} JS + {len(py_results)} Python "
          f"benchmark results ({len(js_memory) + len(py_memory)} memory readings, "
          f"{len(py_sizes)} artifact sizes) -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

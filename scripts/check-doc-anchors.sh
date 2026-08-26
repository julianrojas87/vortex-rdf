#!/usr/bin/env bash
# Verifies the relative markdown links in the hand-written docs: every linked
# file exists, every `#Lnn` / `#Lnn-Lmm` anchor is within the file's line
# count, and a link whose text is a single backticked identifier
# ([`foo`](path#Lnn)) names something that appears on the anchored line.
# Plain-file labels (`open.rs`, `store/mod.rs`) and `file.rs:A-B` range labels
# are only checked for existence and range.
#
# Prints one `doc:line: message` per failure and exits 1 if there is any.
# Run directly (`scripts/check-doc-anchors.sh`) or through scripts/ci-check.sh.
# Needs only bash and python3.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

docs=(README.md CONTRIBUTING.md docs/*.md js/README.md python/README.md
  js/bench/README.md encoded-search/README.md)

existing=()
for doc in "${docs[@]}"; do
  [ -f "$doc" ] && existing+=("$doc")
done

python3 - "${existing[@]}" <<'PY'
import os
import re
import sys

LINK = re.compile(r"\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
ANCHOR = re.compile(r"^L(\d+)(?:-L(\d+))?$")
IDENT = re.compile(r"^`([A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*)`$")

failures = 0
line_counts = {}


def count_lines(path):
    if path not in line_counts:
        with open(path, "rb") as f:
            line_counts[path] = len(f.read().splitlines())
    return line_counts[path]


def line_text(path, n):
    with open(path, encoding="utf-8", errors="replace") as f:
        for i, text in enumerate(f, 1):
            if i == n:
                return text
    return ""


def fail(doc, lineno, msg):
    global failures
    failures += 1
    print(f"{doc}:{lineno}: {msg}")


for doc in sys.argv[1:]:
    base = os.path.dirname(doc)
    in_fence = False
    with open(doc, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if line.lstrip().startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for m in LINK.finditer(line):
                label, target = m.group(1), m.group(2)
                if re.match(r"^[a-z][a-z0-9+.-]*:", target) or target.startswith("#"):
                    continue  # external URL or same-document heading
                path, _, frag = target.partition("#")
                resolved = os.path.normpath(os.path.join(base, path))
                if not os.path.isfile(resolved):
                    fail(doc, lineno, f"[{label}]({target}): {resolved} does not exist")
                    continue
                a = ANCHOR.match(frag)
                if not a:
                    continue  # heading anchor or no anchor: existence is enough
                start = int(a.group(1))
                end = int(a.group(2)) if a.group(2) else start
                total = count_lines(resolved)
                if start < 1 or end < start or end > total:
                    fail(doc, lineno, f"[{label}]({target}): #{frag} outside 1-{total}")
                    continue
                ident = IDENT.match(label.strip())
                if not ident:
                    continue  # file label, range label, prose label
                name = ident.group(1).rsplit("::", 1)[-1]
                found = any(
                    name in line_text(resolved, n) for n in range(start, end + 1)
                )
                if not found:
                    fail(doc, lineno, f"[{label}]({target}): `{name}` not on {path} line {frag}")

if failures:
    print(f"{failures} broken doc link(s)")
    sys.exit(1)
PY

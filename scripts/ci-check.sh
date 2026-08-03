#!/usr/bin/env bash
# Mirrors the jobs in .github/workflows/ci.yml so failures are caught before
# they reach GitHub. Run directly (`./scripts/ci-check.sh`) or let the
# pre-push hook (scripts/hooks/pre-push) invoke it automatically.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

info() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }

# --- lint job ---
info "cargo fmt --check"
cargo fmt --check

info "cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings

info "cargo clippy -p vortex-rdf-core --no-default-features --all-targets -- -D warnings"
cargo clippy -p vortex-rdf-core --no-default-features --all-targets -- -D warnings

# --- rust-tests job ---
info "cargo test --workspace"
cargo test --workspace

info "cargo test -p vortex-rdf-core --no-default-features"
cargo test -p vortex-rdf-core --no-default-features

# --- python-tests job ---
# Skipped (with a warning, not a failure) when uv is absent, so the hook stays
# usable on a clone that never touches the bindings.
python_ran=no
if command -v uv >/dev/null 2>&1; then
  info "uv sync --locked && uv run pytest tests -q (python)"
  (cd python && uv sync --locked && uv run pytest tests -q)
  python_ran=yes
else
  printf '\033[1;33m==>\033[0m %s\n' \
    "uv not found: skipping the python-tests job (install uv to mirror it)."
fi

# The js-tests job (wasm-pack build + npm test + npm run typecheck) is
# intentionally not mirrored here: the wasm32 build is memory-hungry enough that
# it needs the CARGO_BUILD_JOBS=1 / CODEGEN_UNITS=1 / OPT_LEVEL=s tuning ci.yml
# applies even to run reliably in CI, and it got SIGTERM'd locally even in dev
# mode. Run it manually with `(cd js && npm run build && npm run typecheck &&
# npm test)` before pushing JS/wasm changes; GitHub CI is the source of truth
# for this job.
if [ "$python_ran" = yes ]; then
  info "Passed: lint, rust-tests, python-tests. Not run: js-tests (see above)."
else
  info "Passed: lint, rust-tests. Not run: python-tests (no uv), js-tests (see above)."
fi

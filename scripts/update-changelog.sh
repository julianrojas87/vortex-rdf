#!/usr/bin/env bash
# Maintain CHANGELOG.md (Keep a Changelog format).
#
#   scripts/update-changelog.sh            # refresh the [Unreleased] section
#   scripts/update-changelog.sh v0.3.0     # cut a release
#
# Released sections (## [x.y.z]) are always preserved verbatim; the script only
# ever touches the [Unreleased] section at the top.
#
# [Unreleased] has two modes, switched by a sentinel line inside it:
#   * HAND-WRITTEN  — if [Unreleased] contains `<!-- hand-written`, its entries are
#     curated by hand. `refresh` leaves them untouched; a release stamps them
#     verbatim into the new version section and opens a fresh, auto-managed
#     [Unreleased]. (Used while transitional non-Conventional commits are pending.)
#   * AUTO          — otherwise, `refresh`/release regenerate [Unreleased] from
#     Conventional Commits via git-cliff (requires `cargo install git-cliff`).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# Let git-cliff resolve commit authors to GitHub @handles when possible. Prefer an
# explicit GITHUB_TOKEN; otherwise borrow one from an authenticated `gh` CLI. With
# no token, git-cliff falls back to the plain git author name (see cliff.toml).
if [ -z "${GITHUB_TOKEN:-}" ] && command -v gh >/dev/null 2>&1; then
  GITHUB_TOKEN="$(gh auth token 2>/dev/null || true)"
  [ -n "$GITHUB_TOKEN" ] && export GITHUB_TOKEN
fi

CHANGELOG='CHANGELOG.md'
REPO='https://github.com/vortex-rdf/vortex-rdf'
# Boundary for git-cliff in AUTO mode: commits after this ref populate [Unreleased].
# Follows the latest git tag, so it advances automatically on every tagged release
# — tag each release (`git tag vX.Y.Z`) to keep this correct. Falls back to the
# v0.2.0 boundary commit (PR #43 merge) only if no tags exist yet.
BASE_REF="$(git describe --tags --abbrev=0 2>/dev/null || echo a13b6a1c7e0916d513277bec4d0b9a4ec2b1ed6e)"

version="${1:-}"

# Is the [Unreleased] section hand-written? (sentinel between it and the next heading)
unreleased_block="$(awk '/^## \[Unreleased\]/{f=1;next} /^## \[/{f=0} f' "$CHANGELOG")"
hand_written=false
grep -q '<!-- hand-written' <<<"$unreleased_block" && hand_written=true

# ---------------------------------------------------------------------------
if [ -z "$version" ]; then
  # ---- refresh ----
  if $hand_written; then
    echo "[Unreleased] is hand-written (sentinel present); left untouched."
    exit 0
  fi
  command -v git-cliff >/dev/null || { echo "error: git-cliff not found; install with: cargo install git-cliff" >&2; exit 1; }
  frozen_tail="$(awk '/^## \[[0-9]/{f=1} f' "$CHANGELOG")"
  managed_top="$(git-cliff "${BASE_REF}..")"
  printf '%s\n\n%s\n' "$managed_top" "$frozen_tail" \
    | awk 'NR>1 && /^## \[/ && prev != "" { print "" } { print; prev=$0 }' > "$CHANGELOG"
  echo "Refreshed [Unreleased] from Conventional Commits."
  exit 0
fi

# ---- cut a release: vX.Y.Z ----
date="$(date +%Y-%m-%d)"
if $hand_written; then
  # Stamp the hand-written [Unreleased] into [version]; open a fresh AUTO [Unreleased].
  awk -v ver="${version#v}" -v date="$date" -v repo="$REPO" -v tag="$version" '
    /^## \[Unreleased\]/ {
      print "## [Unreleased](" repo "/compare/" tag "...HEAD)"
      print ""
      print "## [" ver "] - " date
      next
    }
    /<!-- hand-written/ { skip=1 }          # drop the sentinel (may span lines)
    skip && /-->/       { skip=0; next }
    skip                { next }
    { print }
  ' "$CHANGELOG" | cat -s > "$CHANGELOG.tmp" && mv "$CHANGELOG.tmp" "$CHANGELOG"
  echo "Cut release $version from the hand-written [Unreleased]. Next [Unreleased] is auto-managed."
  echo "Tip: tag it so git-cliff has a clean boundary  ->  git tag $version && git push --tags"
  exit 0
fi

# AUTO release: let git-cliff generate the version section.
command -v git-cliff >/dev/null || { echo "error: git-cliff not found; install with: cargo install git-cliff" >&2; exit 1; }
frozen_tail="$(awk '/^## \[[0-9]/{f=1} f' "$CHANGELOG")"
managed_top="$(git-cliff "${BASE_REF}.." --tag "$version")"
printf '%s\n\n%s\n' "$managed_top" "$frozen_tail" \
  | awk 'NR>1 && /^## \[/ && prev != "" { print "" } { print; prev=$0 }' > "$CHANGELOG"
echo "Cut release $version from Conventional Commits."

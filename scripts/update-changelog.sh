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
#     Conventional Commits via git-cliff (`cargo binstall git-cliff`, or cargo install).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# A GitHub token lets us link each commit's author to their GitHub profile. Prefer
# an explicit GITHUB_TOKEN; otherwise borrow one from an authenticated `gh` CLI.
# Used by both git-cliff and enrich_handles() below. No token/offline -> plain name.
if [ -z "${GITHUB_TOKEN:-}" ] && command -v gh >/dev/null 2>&1; then
  GITHUB_TOKEN="$(gh auth token 2>/dev/null || true)"
  [ -n "$GITHUB_TOKEN" ] && export GITHUB_TOKEN
fi

# Rewrite "by <name>" -> a linked GitHub @handle, resolved PER COMMIT via the API
# (GET /repos/:o/:r/commits/:sha -> author.login). Unlike git-cliff's built-in
# enrichment (which only matches the DEFAULT branch), this resolves any commit that
# is PUSHED to GitHub — including feature branches. Reads git-cliff markdown on
# stdin, writes it back enriched; an entry is left untouched when its commit isn't
# pushed, the author has no GitHub account, or there's no token/network. One request
# per unique SHA (cached). Opt out with SKIP_AUTHOR_ENRICH=1. Only ever runs over
# git-cliff output (the [Unreleased]/new release section), never the frozen entries.
enrich_handles() {
  if [ -n "${SKIP_AUTHOR_ENRICH:-}" ] || ! command -v python3 >/dev/null 2>&1; then
    cat; return
  fi
  python3 <(cat <<'PY'
import os, sys, re, json, urllib.request
token = os.environ.get("GITHUB_TOKEN", "")
cache = {}
def login_for(owner, repo, sha):
    key = (owner, repo, sha)
    if key not in cache:
        req = urllib.request.Request(
            f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}",
            headers={"Accept": "application/vnd.github+json", "User-Agent": "update-changelog"},
        )
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                cache[key] = (json.load(r).get("author") or {}).get("login")
        except Exception:
            cache[key] = None
    return cache[key]

# Matches the entry tail: ([`sha`](https://github.com/OWNER/REPO/commit/FULLSHA) by AUTHOR)
entry = re.compile(
    r'(\(\[`[0-9a-f]+`\]\(https://github\.com/([^/)]+)/([^/)]+)/commit/([0-9a-f]{7,40})\) by )'
    r'(.+?)(\)\.?)$'
)
def repl(m):
    login = login_for(m.group(2), m.group(3), m.group(4))
    return f'{m.group(1)}[@{login}](https://github.com/{login}){m.group(6)}' if login else m.group(0)

for raw in sys.stdin:
    sys.stdout.write(entry.sub(repl, raw.rstrip("\n")) + "\n")
PY
)
}

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
  command -v git-cliff >/dev/null || { echo "error: git-cliff not found; install with: cargo binstall git-cliff  (or: cargo install git-cliff)" >&2; exit 1; }
  frozen_tail="$(awk '/^## \[[0-9]/{f=1} f' "$CHANGELOG")"
  managed_top="$(git-cliff "${BASE_REF}.." | enrich_handles)"
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
command -v git-cliff >/dev/null || { echo "error: git-cliff not found; install with: cargo binstall git-cliff  (or: cargo install git-cliff)" >&2; exit 1; }
frozen_tail="$(awk '/^## \[[0-9]/{f=1} f' "$CHANGELOG")"
managed_top="$(git-cliff "${BASE_REF}.." --tag "$version" | enrich_handles)"
printf '%s\n\n%s\n' "$managed_top" "$frozen_tail" \
  | awk 'NR>1 && /^## \[/ && prev != "" { print "" } { print; prev=$0 }' > "$CHANGELOG"
echo "Cut release $version from Conventional Commits."

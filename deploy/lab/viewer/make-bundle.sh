#!/usr/bin/env bash
# Build the standalone viewer bundle from this repository.
#
# Why a generator instead of a checked-in directory: the bundle is the same
# instruments the repo already carries (the swarm driver, its report, the shard
# sweeps, the signer) plus a handful of wrapper scripts, and it is shipped to a
# machine that has no checkout — the compile box that runs the viewer load
# tests. Keeping a second hand-maintained copy is how the two drifted: a driver
# was edited on the far side, back-ported by hand, and the repo grew tolerance
# code for a page variant that only existed in the copy.
#
# With this script the far-side directory is a PRODUCT of the repo: build it,
# ship it, and verify it with `--check` (or `sha256sum -c MANIFEST.sha256`).
#
#   deploy/lab/viewer/make-bundle.sh <out-dir>
#   deploy/lab/viewer/make-bundle.sh --check <dir>     # is that copy current?
#
# The out-dir must be OUTSIDE the repo: the bundle is deliberately not a repo
# artefact (it names a deployment's media URL in use, and the repo stays free
# of any one deployment's identifiers), so generating it into a tracked path
# would silently commit it.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
BUNDLE_SRC="$HERE/bundle"

MODE=build
OUT=""
case "${1:-}" in
  --check) MODE=check; OUT=${2:?usage: make-bundle.sh --check <dir>} ;;
  "") echo "usage: make-bundle.sh <out-dir> | --check <dir>" >&2; exit 2 ;;
  *) OUT=$1 ;;
esac

# Resolve to an absolute path BEFORE the containment check: a relative
# `deploy/lab/out` used to slip past a `"$REPO"/*` case match and land in the
# repo (found by generating into one).
mkdir -p "$(dirname "$OUT")"
OUT="$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")"
case "$OUT" in
  "$REPO"/*) echo "refusing to write inside the repo: $OUT (the bundle is deliberately not a repo artefact)" >&2; exit 2 ;;
esac

copy() { # src dest
  mkdir -p "$(dirname "$2")"
  cp "$1" "$2"
}

# A rename the bundle's own usage text has to follow. The assertion is the
# lesson from pitfall 53: a scripted replacement that silently matches nothing
# prints success and changes nothing.
rewrite() { # file from to
  local file=$1 from=$2 to=$3
  grep -q -- "$from" "$file" || { echo "make-bundle: '$from' not found in $file (rename rule is stale)" >&2; exit 1; }
  sed -i "s|$from|$to|g" "$file"
  grep -q -- "$to" "$file" || { echo "make-bundle: rewrite of $file did not land" >&2; exit 1; }
  ! grep -q -- "$from" "$file" || { echo "make-bundle: '$from' still present in $file" >&2; exit 1; }
}

assemble() { # dest-dir
  local out=$1
  mkdir -p "$out/origin-side"

  # The instruments themselves: one home (the repo), two names (the bundle
  # keeps the short ones its README documents).
  copy "$HERE/player-swarm.mjs" "$out/swarm.mjs"
  rewrite "$out/swarm.mjs" "node player-swarm.mjs" "node swarm.mjs"
  copy "$HERE/swarm-report.mjs" "$out/report.mjs"
  rewrite "$out/report.mjs" "node swarm-report.mjs" "node report.mjs"
  rewrite "$out/report.mjs" "usage: swarm-report.mjs" "usage: report.mjs"
  rewrite "$out/report.mjs" "player-swarm.mjs" "swarm.mjs"
  copy "$HERE/../probe-shard-size.sh" "$out/shard-sweep.sh"
  rewrite "$out/shard-sweep.sh" "deploy/lab/probe-shard-size.sh" "shard-sweep.sh"
  rewrite "$out/shard-sweep.sh" "deploy/oracle/shard-sweep-origin.sh" "origin-side/shard-sweep-origin.sh"
  copy "$HERE/../../oracle/shard-sweep-origin.sh" "$out/origin-side/shard-sweep-origin.sh"
  rewrite "$out/origin-side/shard-sweep-origin.sh" "deploy/lab/probe-shard-size.sh" "shard-sweep.sh"
  rewrite "$out/origin-side/shard-sweep-origin.sh" "deploy/oracle/shard-sweep-origin.sh" "origin-side/shard-sweep-origin.sh"
  copy "$HERE/../../oracle/presign.py" "$out/presign.py"

  # The wrapper scripts and the page: these are the bundle's own sources, but
  # they live in the repo now (`bundle/`), so there is still exactly one home.
  for f in video-page.html run.sh preflight.sh detach.sh monitor.sh install.sh README.md; do
    copy "$BUNDLE_SRC/$f" "$out/$f"
  done
  chmod +x "$out"/*.sh "$out"/*.py "$out"/origin-side/*.sh

  (cd "$out" && find . -type f ! -name MANIFEST.sha256 | LC_ALL=C sort | xargs sha256sum > MANIFEST.sha256)
}

if [ "$MODE" = check ]; then
  TMP=$(mktemp -d)
  trap 'rm -rf "$TMP"' EXIT
  assemble "$TMP/bundle"
  if [ ! -d "$OUT" ]; then
    echo "make-bundle: $OUT does not exist" >&2
    exit 1
  fi
  if diff -r --brief "$TMP/bundle" "$OUT" >/dev/null 2>&1; then
    echo "check: $OUT matches what this repo generates ($(sha256sum "$OUT/MANIFEST.sha256" | cut -d' ' -f1))"
    exit 0
  fi
  echo "check: $OUT differs from what this repo generates:" >&2
  diff -r --brief "$TMP/bundle" "$OUT" >&2 || true
  echo "(ship the generated copy, or regenerate it there: make-bundle.sh will not merge)" >&2
  exit 1
fi

mkdir -p "$OUT"
assemble "$OUT"
echo "bundle: $OUT"
echo "manifest: $(sha256sum "$OUT/MANIFEST.sha256" | cut -d' ' -f1)"
echo "verify on the far side: (cd $OUT && sha256sum -c MANIFEST.sha256)"

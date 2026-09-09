#!/usr/bin/env bash
# PGO (profile-guided optimization) build for origin-cache.
#
# Three phases:
#   1. instrumented build   (-Cprofile-generate, continuous mode requested)
#   2. training traffic     (lab suite + a dedicated heavy pass; profiles
#                            are written at process exit — the instrumented
#                            binary must exit cleanly, SIGKILL loses data)
#   3. optimized build      (-Cprofile-use merged.profdata)
#
# Usage:
#   deploy/pgo-build.sh instrument          # phase 1 (also the default)
#   LLVM_PROFILE_FILE=... <run training>    # phase 2 — see train() below
#   deploy/pgo-build.sh train               # phase 2 helper (suite + heavy pass)
#   deploy/pgo-build.sh use                 # phase 3
set -eu

REPO=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO"
PGO_DIR=target/pgo-data
PROFDATA_BIN=$(find ~/.rustup/toolchains -path '*x86_64-unknown-linux-gnu/bin/llvm-profdata' | head -1)
[ -n "$PROFDATA_BIN" ] || { echo "llvm-profdata not found (rustup component llvm-tools-preview)"; exit 1; }

# NOTE: continuous mode (%c) is requested in the compile flag but LLVM on
# stable lacks the counter-bias symbol, so it falls back to atexit writes —
# LLVM_PROFILE_FILE must be set at runtime or nothing is written.
GEN_FLAGS="-Cprofile-generate=$PWD/$PGO_DIR/%c"
USE_FLAGS="-Cprofile-use=$PWD/$PGO_DIR/merged.profdata"

case "${1:-instrument}" in
instrument)
  mkdir -p "$PGO_DIR"
  RUSTFLAGS="$GEN_FLAGS" cargo build --release
  echo "instrumented binary ready: $PWD/target/release/origin-cache"
  echo "training: export LLVM_PROFILE_FILE=\"$PWD/$PGO_DIR/train-%p-%m.profraw\" RUSTFLAGS=\"$GEN_FLAGS\" then run deploy/lab/run-lab.sh"
  ;;
train)
  # Phase 2: full lab suite (18 items) — every server instance must exit
  # via SIGTERM (graceful) to flush its profile; then a dedicated heavy
  # pass so the kill-9'd cold-pull instance's paths are re-trained.
  export RUSTFLAGS="$GEN_FLAGS" LLVM_PROFILE_FILE="$PWD/$PGO_DIR/train-%p-%m.profraw"
  deploy/lab/run-lab.sh
  deploy/pgo-build.sh heavy-train
  ;;
heavy-train)
  # Cold parallel-segmented pull + ranged seg serving — the paths the
  # lab suite's kill-9 restart instance never contributes.
  export RUSTFLAGS="$GEN_FLAGS" LLVM_PROFILE_FILE="$PWD/$PGO_DIR/heavy-%p-%m.profraw"
  LAB=/mnt/hdd/CDN-LAB
  rm -rf "$LAB/cache-a"/*
  "$REPO/target/release/origin-cache" "$REPO/deploy/lab/config-a.toml" >> "$LAB/cache-a/serve.log" 2>&1 &
  SPID=$!
  trap 'kill -TERM $SPID 2>/dev/null' EXIT
  sleep 2
  curl -s -o /dev/null "http://127.0.0.1:7777/media/big3g.bin"            # cold pull (parallel segmented)
  curl -s -o /dev/null "http://127.0.0.1:7777/media/big3g.bin"            # disk hit
  for off in 0 250000000 500000000 750000000 1000000000 1250000000 \
            1500000000 1750000000 2000000000 2250000000 2500000000 2750000000; do
    curl -s -o /dev/null -H "Range: bytes=$off-$((off+4999999))" "http://127.0.0.1:7777/media/big3g.bin"
  done
  for _ in 1 2 3; do curl -s -o /dev/null "http://127.0.0.1:7777/media/big1mb.bin"; done
  curl -s -o /dev/null "http://127.0.0.1:7777/?list-type=2&prefix=media/"
  # SIGTERM, then wait for the real exit — Pingora drains up to 300s and
  # the profile flush happens only at process exit.
  kill -TERM $SPID 2>/dev/null
  for _ in $(seq 1 120); do kill -0 $SPID 2>/dev/null || break; sleep 5; done
  trap - EXIT
  echo "heavy training done: $(ls "$PGO_DIR"/heavy-*.profraw 2>/dev/null | wc -l) profile(s)"
  ;;
merge)
  $PROFDATA_BIN merge -output="$PGO_DIR/merged.profdata" "$PGO_DIR"/*.profraw
  echo "merged: $PGO_DIR/merged.profdata ($(du -h "$PGO_DIR/merged.profdata" | cut -f1))"
  ;;
use)
  [ -f "$PGO_DIR/merged.profdata" ] || { echo "run merge first"; exit 1; }
  RUSTFLAGS="$USE_FLAGS" cargo build --release
  echo "PGO-optimized binary ready: $PWD/target/release/origin-cache"
  echo "regression: export RUSTFLAGS=\"$USE_FLAGS\" && deploy/lab/run-lab.sh"
  ;;
*)
  echo "usage: deploy/pgo-build.sh {instrument|train|heavy-train|merge|use}"; exit 1
  ;;
esac

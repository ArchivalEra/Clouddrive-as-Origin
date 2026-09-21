#!/usr/bin/env bash
# The watch account (ADR-0018) on the real provider: the SAME walk twice, once
# with the viewer protections on and once with them off, each with a
# minute-scale pause in the middle.
#
# Why a pair: the pause is the whole point, and a single run cannot say what
# held the window. Both configs leave `read_grace_secs = 0` (a lease cannot be
# the answer) and `inactive_ttl_secs = 60` (a 150 s pause outlives the idle
# sweep), so the only difference between the two runs is the watch. Measured on
# the node:
#
#   watch ON : 512 shards -> 12 opens, pause +1 open (the read-ahead it is owed),
#              post-pause re-read of the playhead shard 0 opens, byte-exact
#   watch OFF: 512 shards -> 12 opens, pause +0, post-pause re-read 1 open
#
# Run it on the node, not from here: it starts a loopback-only instance per leg
# and needs the upstream credentials the production units use.
set -u
cd "$(dirname "$0")/../.." 2>/dev/null || cd /home/opc
REPO=$(pwd)
NODE_DIR=${NODE_DIR:-/home/opc}
BIN=${BIN:-$NODE_DIR/origin-cache-eff}
WALK=${WALK:-$NODE_DIR/efficient-walk.sh}
OBJ=${OBJ:-coverage-test-3g.bin}
SHARDS=${SHARDS:-512}
PAUSE=${PAUSE:-150}
PAUSE_AT=${PAUSE_AT:-200}
LOG=${LOG:-$NODE_DIR/pair.log}

# The upstream credentials ride in the deployment env file; never echoed.
set -a; . /opt/origin-cache/origin-cache.env; set +a
echo "watch account pair -> $LOG"
log() { echo "[$(date +%H:%M:%S)] $*"; }

run_one() { # name config metrics_port business_port front_port cache_dir
  local name=$1 cfg=$2 mport=$3 bport=$4 fport=$5 dir=$6
  log "=== $name: $cfg ==="
  pkill -f "origin-cache-ef[f]" 2>/dev/null; sleep 2
  rm -rf "$dir"; mkdir -p "$dir"
  setsid nohup "$BIN" "$cfg" > "$NODE_DIR/eff-$name.log" 2>&1 < /dev/null &
  local up=0
  for _ in $(seq 1 40); do
    curl -fsS -m 3 "http://127.0.0.1:$bport/_internal/healthz" >/dev/null 2>&1 && { up=1; break; }
    sleep 1
  done
  [ "$up" = 1 ] || { log "FAIL: $name instance never came up"; return 1; }
  EFF_BASE="http://127.0.0.1:$fport/googledrive1" EFF_METRICS="http://127.0.0.1:$mport/metrics" \
    EFF_HEALTHZ="http://127.0.0.1:$bport/_internal/healthz" \
    PAUSE_SECS="$PAUSE" PAUSE_AT="$PAUSE_AT" bash "$WALK" "$OBJ" "$SHARDS"
  log "=== $name done ==="
}

{
  run_one watch   "$NODE_DIR/efficient-walk.toml"         9094 8091 7791 "$NODE_DIR/eff-cache"
  run_one nowatch "$NODE_DIR/efficient-walk-nowatch.toml" 9095 8092 7792 "$NODE_DIR/eff-cache-b"
  pkill -f "origin-cache-ef[f]" 2>/dev/null
  log "instances stopped"
} 2>&1 | tee "$LOG"

#!/usr/bin/env bash
# Acceptance for the staged-read runs (ADR-0016) against a REAL provider.
#
# Runs on the origin node against a loopback-only efficient instance: same
# OpenList upstream as the production units, its own cache dir, a magazine big
# enough for the object being walked (admission refuses to stage an object
# larger than the magazine — ADR-0013 — so a 3 GiB object needs a magazine
# above 3 GiB), and ports that no public route points at. Nothing here touches
# the production instances.
#
# Usage:  efficient-walk.sh <object> [shards]
#   object: a key under googledrive1/, e.g. coverage-test-3g.bin
#   shards: 1 MiB shard count (default 512 = 512 MiB walked)
#
# PAUSE_SECS=N pauses the walk in the middle for N seconds with NO requests at
# all — the viewer walked away. That is the watch account (ADR-0018): with a
# watch the key stays protected and its read-ahead stays in place across the
# gap, so the re-read of the shard under the playhead costs no upstream open;
# with `watch_idle_secs = 0` the same pause protects nothing.
# PAUSE_AT=K says after which shard (default: half).
#
# The account it prints is the one the mechanism exists for: upstream opens
# divided by shard requests. One open per `session_window_bytes` window is the
# target; one per request is the behaviour it replaced.
set -u

OBJ=${1:?usage: efficient-walk.sh <object> [shards]}
SHARDS=${2:-512}
CHUNK=$((1024 * 1024))
EFF=${EFF_BASE:-http://127.0.0.1:7791/googledrive1}/"$OBJ"
MET=${EFF_METRICS:-http://127.0.0.1:9094/metrics}
HZ=${EFF_HEALTHZ:-http://127.0.0.1:8091/_internal/healthz}
DAV=${DAV_BASE:-http://127.0.0.1:5244/dav/googledrive1}/"$OBJ"

opens() { curl -s -m 5 "$MET" | awk '/^backend_call_duration_seconds_count\{op="open"\}/{print $2+0}' | head -1; }
stage_total() { curl -s -m 5 "$MET" | awk '/^cache_serve_source_total\{source="stage"\}/{print $2+0}' | head -1; }
mline() { curl -s -m 5 "$MET" | grep -E "^$1" | sed 's/^/    /'; }
hz() { curl -s -m 5 "$HZ"; }

echo "profile: $(hz | grep -oE '"profile":"[a-z]*"' | head -1)"
PAUSE_SECS=${PAUSE_SECS:-0}
PAUSE_AT=${PAUSE_AT:-$((SHARDS / 2))}
before=$(opens)
t0=$(date +%s)
for i in $(seq 0 $((SHARDS - 1))); do
  off=$((i * CHUNK))
  code=$(curl -s -m 120 -o /dev/null -w "%{http_code}" -H "Range: bytes=$off-$((off + CHUNK - 1))" "$EFF")
  [ "$code" = 206 ] || { echo "FAIL: shard $i -> $code"; exit 1; }
  if [ "$PAUSE_SECS" -gt 0 ] && [ "$i" = "$PAUSE_AT" ]; then
    # The viewer walks away: no requests at all for PAUSE_SECS.
    pb=$(opens)
    sleep "$PAUSE_SECS"
    pa=$(opens)
    echo "pause: ${PAUSE_SECS}s with no requests after shard $i -> opens +$((pa - pb))"
    # The shard under the playhead, re-read after the gap: with a watch this is
    # bytes already on this node, so it costs no open.
    rb=$(opens)
    code=$(curl -s -m 120 -o /tmp/walk-pause.bin -w "%{http_code}" -H "Range: bytes=$off-$((off + CHUNK - 1))" "$EFF")
    ra=$(opens)
    echo "post-pause re-read of shard $i: $code, opens +$((ra - rb))"
    [ "$code" = 206 ] || echo "FAIL: post-pause re-read -> $code"
    if [ $((ra - rb)) = 0 ]; then
      echo "PASS: the pause cost no upstream open (the bytes were still here)"
    else
      echo "FAIL: the pause cost $((ra - rb)) upstream open(s): the window was not held"
    fi
    if [ -n "${OPENLIST_USERNAME:-}" ] && [ -n "${OPENLIST_PASSWORD:-}" ]; then
      curl -s -m 120 -u "$OPENLIST_USERNAME:$OPENLIST_PASSWORD" -o /tmp/walk-pause-ref.bin \
        -H "Range: bytes=$off-$((off + CHUNK - 1))" "$DAV"
      cmp -s /tmp/walk-pause.bin /tmp/walk-pause-ref.bin \
        && echo "post-pause re-read: byte-exact against the provider" \
        || echo "FAIL: post-pause re-read differs from the provider"
      rm -f /tmp/walk-pause.bin /tmp/walk-pause-ref.bin
    fi
    echo "watch account: $(curl -s -m 5 "$MET" | grep -E '^cache_watch' | tr '\n' ' ')"
  fi
done
t1=$(date +%s)
after=$(opens)
echo "walk: $SHARDS shard requests (all 206) in $((t1 - t0))s"
echo "upstream opens: $((after - before))  (one per window is the target)"
echo "session runs:"; mline 'cache_session_total'
echo "readers:"; mline 'cache_session_reader_total'
echo "healthz: $(hz | grep -oE '"(segment_bytes|coverage_intervals|coverage_keys|bytes)":[0-9]*' | tr '\n' ' ')"

# Byte-exactness against the PROVIDER hop itself (not against another instance,
# which would only prove the two agree with each other).
if [ -n "${OPENLIST_USERNAME:-}" ] && [ -n "${OPENLIST_PASSWORD:-}" ]; then
  curl -s -m 120 -o /tmp/walk-eff.bin -H "Range: bytes=1048576-2097151" "$EFF"
  curl -s -m 120 -u "$OPENLIST_USERNAME:$OPENLIST_PASSWORD" -o /tmp/walk-dav.bin \
    -H "Range: bytes=1048576-2097151" "$DAV"
  a=$(stat -c%s /tmp/walk-eff.bin 2>/dev/null || echo 0)
  b=$(stat -c%s /tmp/walk-dav.bin 2>/dev/null || echo 0)
  if [ "$a" = 1048576 ] && [ "$b" = 1048576 ] && cmp -s /tmp/walk-eff.bin /tmp/walk-dav.bin; then
    echo "byte-exactness: 1 MiB shard identical to the provider's own bytes ($a bytes)"
  else
    echo "FAIL: shard compare staged=$a provider=$b"
  fi
  rm -f /tmp/walk-eff.bin /tmp/walk-dav.bin
else
  echo "byte-exactness: skipped (source OPENLIST_USERNAME/OPENLIST_PASSWORD to enable)"
fi

# A shard that was walked and sealed costs no upstream open at all.
b=$(opens); s=$(stage_total)
curl -s -m 120 -o /dev/null -H "Range: bytes=0-$((CHUNK - 1))" "$EFF"
a=$(opens); s2=$(stage_total)
echo "re-read of shard 0: opens +$((a - b)), stage-served +$((s2 - ${s:-0}))"

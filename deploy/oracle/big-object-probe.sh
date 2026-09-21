#!/usr/bin/env bash
# What does an object the node can NEVER hold actually cost? (ADR-0013/0014)
#
# The product's object is a 3-hour video of 30-200 GB. The node's magazine is
# 10 GiB and its disk 183 GB, so such an object is neither a magazine member nor
# a resident stray: admission refuses to stage it (ADR-0013) or to fill it
# (ADR-0014), and every ranged request is answered straight from the provider.
#
# This probe measures that in bounded traffic — a handful of 1 MiB shards, a few
# random seeks, one larger range and three concurrent ranges — and prints the
# per-request cost, because that is what the product's smoothness rests on when
# nothing can be cached. It writes nothing: the object cannot be stored, and the
# assertion is exactly that.
#
# Usage:  big-object-probe.sh [object] [shards]      (default: round3.mp4, 24)
#   ORI_BASE   origin front base URL   (default http://127.0.0.1:7791)
#   ORI_MET    metrics URL             (default http://127.0.0.1:9094/metrics)
#   ORI_HZ     healthz URL             (default http://127.0.0.1:8091/_internal/healthz)
set -u
OBJ=${1:-round3.mp4}
SHARDS=${2:-24}
CHUNK=$((1024 * 1024))
BASE=${ORI_BASE:-http://127.0.0.1:7791}/googledrive1/$OBJ
MET=${ORI_MET:-http://127.0.0.1:9094/metrics}
HZ=${ORI_HZ:-http://127.0.0.1:8091/_internal/healthz}

metric() { curl -s -m 5 "$MET" | awk -v k="$1" '$0 ~ "^"k"\\{" || $0 ~ "^"k" " {print $NF; exit}'; }
opens() { curl -s -m 5 "$MET" | awk '/^backend_call_duration_seconds_count\{op="open"\}/{print $2+0}' | head -1; }
stage_src() { curl -s -m 5 "$MET" | awk '/^cache_serve_source_total\{source="stage"\}/{print $2+0}' | head -1; }
up_src() { curl -s -m 5 "$MET" | awk '/^cache_serve_source_total\{source="upstream"\}/{print $2+0}' | head -1; }
seg_bytes() { curl -s -m 5 "$HZ" | grep -oE '"segment_bytes":[0-9]*' | cut -d: -f2; }
# A range header is `bytes=<first>-<last>`, both inclusive: the last byte is
# first + len - 1. (A first > last is a 416, which is how the first cut of this
# probe "measured" zero-millisecond seeks.)
range_hdr() { echo "bytes=$1-$(( $1 + $2 - 1 ))"; }
ttfb() { curl -s -o /dev/null -w "%{time_starttransfer}" -H "Range: $(range_hdr "$1" "$2")" "$BASE"; }
code() { curl -s -o /dev/null -w "%{http_code}" -H "Range: $(range_hdr "$1" "$2")" "$BASE"; }

size=$(curl -sI -m 20 "$BASE" | grep -i content-length | tr -d '\r' | awk '{print $2}')
echo "object: $OBJ  size: ${size:-unknown} bytes"

# 1. A single shard: what one request costs when nothing can be cached.
b=$(opens); u=$(up_src); t0=$(date +%s%N)
c=$(code 0 "$CHUNK")
t1=$(date +%s%N)
a=$(opens); u2=$(up_src)
echo "one 1 MiB shard at 0: http=$c, opens +$((a - b)), upstream-served +$((u2 - u)), $(((t1 - t0) / 1000000)) ms total"

# 2. A bounded sequential walk: one open per request is the behaviour to expect
#    when the object cannot be staged (the run machinery is gated on admission).
b=$(opens); t0=$(date +%s%N)
for i in $(seq 1 "$SHARDS"); do
  off=$((i * CHUNK))
  c=$(code "$off" "$CHUNK")
  [ "$c" = 206 ] || { echo "FAIL: shard $i -> $c"; break; }
done
t1=$(date +%s%N)
a=$(opens)
echo "walk of $SHARDS shards: opens +$((a - b)) (one per request, not one per window), $(((t1 - t0) / 1000000)) ms for $((SHARDS)) MiB"
awk -v ms="$(( (t1 - t0) / 1000000 ))" -v n="$SHARDS" 'BEGIN { printf "  -> %.0f ms per MiB; 4 Mbps playback needs ~0.5 MiB/s\n", ms / n }'

# 3. Random seeks: the scrub shape. Each is one open plus the provider's TTFB.
echo "random seeks (time to first byte):"
for frac in 7 91 43 68 12; do
  off=$(( (size / 100) * frac ))
  ms=$(awk -v s="$(ttfb "$off" "$CHUNK")" 'BEGIN { printf "%.0f", s * 1000 }')
  echo "  at ${frac}% (offset $off): ${ms} ms"
done

# 4. One larger range: what a player's seek really asks for.
b=$(opens); t0=$(date +%s%N)
c=$(code 0 $((16 * CHUNK)))
t1=$(date +%s%N)
a=$(opens)
mbps=$(awk -v ms="$(( (t1 - t0) / 1000000 ))" 'BEGIN { if (ms > 0) printf "%.1f", 16 * 1000 / ms; else print "inf" }')
echo "one 16 MiB range: http=$c, opens +$((a - b)), $(((t1 - t0) / 1000000)) ms -> ${mbps} MiB/s"

# 5. Three concurrent readers of the SAME range: with no staging there is no run
#    to share, so each pays its own open.
b=$(opens)
pids=""
for i in 1 2 3; do
  curl -s -o /dev/null -H "Range: bytes=1048576-2097151" "$BASE" & pids="$pids $!"
done
for p in $pids; do wait "$p" 2>/dev/null; done
a=$(opens)
echo "three concurrent 1 MiB ranges on one key: opens +$((a - b))"

# 6. The cache's own account: nothing was written for this key.
echo "cache state: segment_bytes=$(seg_bytes) stage-served=$(( $(stage_src) )) upstream-served=$(( $(up_src) ))"
echo "healthz: $(curl -s -m 5 "$HZ" | grep -oE '"(entries|segment_bytes|stray_bytes|bytes)":[0-9]*' | tr '\n' ' ')"

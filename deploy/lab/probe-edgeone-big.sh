#!/usr/bin/env bash
# The target-scale account through the CDN (cdn-oracle.isui.ren), from a host
# with direct internet — the node is the usual one. NEVER through an HTTP proxy:
# the RTT it adds is exactly the number this probe is trying to measure.
#
# What it answers: for an object the origin can never hold (a 3-hour, 200 GiB
# video), what does a viewer actually wait for? Measured 2026-09-21 against
# googledrive1/round3.mp4 (214 748 364 800 bytes):
#
#   HEAD                : eo-cache-status MISS, content-length = the object
#   one 1 MiB range     : TTFB 0.206 s, 1.62 s total, origin opens +1
#   the SAME range again: eo-cache-status HIT, origin opens +0
#   three random seeks  : TTFB 0.206-0.209 s (the edge answers first byte and
#                         pulls the rest behind it; the origin's ~1.1 s pull is
#                         not on the client's TTFB path)
#   24-shard 1 MiB walk : 39 s (1.6 s per MiB), origin opens +24 — one per shard,
#                         because each distinct range misses at the edge
#
# The shape of the answer: the EDGE cache is what makes a re-scrub cheap on this
# object (age 2, HIT, zero origin traffic), and the origin's job is the first
# pull of each region. The origin caches nothing for it — the production log
# says `serving without caching: the disk cannot hold this object want=214748364800`
# and every origin request is one open (~1.1 s, 1 MiB ascending shards over h2).
#
# Usage: probe-edgeone-big.sh [object] [shards]     (default round3.mp4, 24)
#   ORIGIN_METRICS  origin metrics URL, for the open deltas (optional; the
#                   deltas are only readable on the node itself)
set -u
OBJ=${1:-round3.mp4}
SHARDS=${2:-24}
BASE=${BASE:-https://cdn-oracle.isui.ren}/googledrive1/$OBJ
MET=${ORIGIN_METRICS:-http://127.0.0.1:9090/metrics}
CHUNK=$((1024 * 1024))

export no_proxy="*"; unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 2>/dev/null
opens() { curl -s -m 5 "$MET" 2>/dev/null | awk '/^backend_call_duration_seconds_count\{op="open"\}/{print $2+0}' | head -1; }
hdr() { curl -skI -m 30 -H "Range: bytes=$1-$(( $1 + $2 - 1 ))" "$BASE" 2>/dev/null; }

echo "object: $OBJ  via $BASE"
echo "--- HEAD ---"
curl -skI -m 30 "$BASE" | grep -iE "^(HTTP|content-length|server|age|eo-cache-status)" | sed 's/^/  /'
echo "--- one 1 MiB range ---"
b=$(opens); curl -sk -o /dev/null -w "  http=%{http_code} ttfb=%{time_starttransfer}s total=%{time_total}s bytes=%{size_download}\n" -H "Range: bytes=0-$((CHUNK-1))" "$BASE"; a=$(opens)
[ -n "$b" ] && echo "  origin opens +$((a-b))"
echo "--- the same range again (the edge's own cache) ---"
b=$(opens); hdr 0 "$CHUNK" | grep -iE "^(HTTP|content-range|age|eo-cache-status)" | sed 's/^/  /'; a=$(opens)
[ -n "$b" ] && echo "  origin opens +$((a-b))"
echo "--- random seeks ---"
size=$(curl -skI -m 30 "$BASE" | grep -i content-length | tr -d '\r' | awk '{print $2}')
for frac in 5 50 95; do
  off=$(( (size / 100) * frac ))
  curl -sk -o /dev/null -w "  ${frac}% ($off): ttfb=%{time_starttransfer}s http=%{http_code}\n" -H "Range: bytes=$off-$((off+CHUNK-1))" "$BASE"
done
echo "--- a bounded ${SHARDS}-shard walk ---"
b=$(opens); t0=$(date +%s%N)
for i in $(seq 1 "$SHARDS"); do
  off=$((i * CHUNK))
  curl -sk -o /dev/null -H "Range: bytes=$off-$((off+CHUNK-1))" "$BASE"
done
t1=$(date +%s%N); a=$(opens)
echo "  $((SHARDS)) MiB in $(( (t1-t0)/1000000 )) ms"
awk -v ms="$(( (t1-t0)/1000000 ))" -v n="$SHARDS" 'BEGIN { printf "  -> %.2f s per MiB\n", ms / 1000 / n }'
[ -n "$b" ] && echo "  origin opens +$((a-b)) (one per shard: each distinct range misses at the edge)"

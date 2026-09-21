#!/usr/bin/env bash
# N concurrent viewers of one object through the CDN, run FROM THE NODE.
#
# Why the node and not a workstation: from a domestic/workstation path, one of
# every 3-4 concurrent connections to this edge stalls for 9 s to 300 s while
# the origin sits completely idle (its open/stat/session counters do not move).
# Measured 2026-09-21: 3 concurrent page loads -> 0.13 s, 0.45 s and 10.2 s; the
# same three ranges from the NODE -> 0.21-0.23 s TTFB, every round. A
# "N viewers through the CDN" number taken from a workstation is therefore a
# measurement of that path, not of the origin or the edge.
#
# What this produces: a per-request TTFB distribution for N viewers each doing a
# seek walk over FRESH offsets, plus the origin-side deltas for the same window
# (upstream opens, sessions sealed/chained, reader attachments) so the client's
# latency and the origin's work are two sides of one account.
#
# Usage: probe-edgeone-viewers.sh [object] [viewers] [chunks] [seeks]
#        defaults: round3.mp4, 3, 6, 6
#   VIEWERS is the concurrency; each viewer reads `chunks` 256 KiB ranges and
#   then makes `seeks` long jumps, all at offsets unique to the run (a fresh
#   band of the object per run, so the edge cache cannot answer for it).
#   ORIGIN_METRICS / BASE override the metrics URL and the CDN base.
set -u
OBJ=${1:-round3.mp4}
VIEWERS=${2:-3}
CHUNKS=${3:-6}
SEEKS=${4:-6}
BASE=${BASE:-https://cdn-oracle.isui.ren}/googledrive1/$OBJ
MET=${ORIGIN_METRICS:-http://127.0.0.1:9090/metrics}
CHUNK=$((256 * 1024))
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

export no_proxy="*"; unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 2>/dev/null

metric() { # name -> the first sample's value, or nothing when unreadable
  curl -s -m 5 "$MET" 2>/dev/null | awk -v k="^$1" '$0 ~ k {print $2+0; exit}'
}
report_metrics() { # label -> one line of the origin-side counters
  curl -s -m 5 "$MET" 2>/dev/null | grep -E \
    '^backend_call_duration_seconds_count\{op="(open|stat)"\}|^cache_session_total|^cache_session_reader_total' \
    | sed 's/^/    /' || echo "    (metrics unreadable from here)"
}

size=$(curl -skI -m 30 "$BASE" | awk 'tolower($1)=="content-length:"{print $2+0}' | tr -d '\r')
if [ -z "${size:-}" ] || [ "$size" = 0 ]; then
  echo "FAIL: no content-length from $BASE (is the CDN reachable without a proxy?)"; exit 1
fi
# A fresh band per run: 10..80% of the object, walked upward, so the edge has
# never seen these ranges and the run measures the origin's pull too.
band=$(( (size / 100) * (10 + (RANDOM % 70)) ))
echo "object: $OBJ ($size bytes)  viewers=$VIEWERS chunks=$CHUNKS seeks=$SEEKS"
echo "base:   $BASE"
echo "band:   offset $band (fresh: the edge cannot answer for it)"
echo "--- origin before ---"; report_metrics before

viewer() { # id
  # One statement per variable: with `set -u`, a later word on the SAME `local`
  # line cannot see an earlier one, and the viewer dies without a trace.
  local id=$1
  local f="$TMP/ttfb.$id"
  local off=$((band + id * CHUNK * 512))
  : > "$f"
  local i
  for i in $(seq 1 "$CHUNKS"); do
    curl -sk -o /dev/null -m 90 -w "%{time_starttransfer} %{http_code} %{size_download}\n" \
      -H "Range: bytes=$off-$((off + CHUNK - 1))" "$BASE" >> "$f"
    off=$((off + CHUNK))
  done
  for i in $(seq 1 "$SEEKS"); do
    # A long jump: 1..40% of the object away from where this viewer is.
    off=$(( (size / 100) * (1 + (RANDOM % 40)) ))
    curl -sk -o /dev/null -m 90 -w "%{time_starttransfer} %{http_code} %{size_download}\n" \
      -H "Range: bytes=$off-$((off + CHUNK - 1))" "$BASE" >> "$f"
  done
}

t0=$(date +%s%N)
for v in $(seq 1 "$VIEWERS"); do viewer "$v" & done
wait
t1=$(date +%s%N)

echo "--- origin after ---"; report_metrics after
echo "--- per viewer ---"
cat "$TMP"/ttfb.* | sort -n > "$TMP/all"
awk '{ if ($2 != 206) bad++; if ($3 != '"$CHUNK"') short++; n++; s+=$1 }
     END {
       printf "  requests=%d  non-206=%d  off-size=%d\n", n, bad+0, short+0
     }' "$TMP/all"
awk '
  function pick(q,   i) { i = int(NR * q + 0.5); if (i < 1) i = 1; if (i > NR) i = NR; return a[i] }
  { a[NR] = $1 }
  END {
    if (NR == 0) { print "  no samples"; exit 1 }
    printf "  TTFB  p50=%.3fs p90=%.3fs max=%.3fs  (n=%d)\n", pick(0.5), pick(0.9), a[NR], NR
  }' "$TMP/all" 
echo "  wall=$(( (t1 - t0) / 1000000 )) ms for $(( VIEWERS * (CHUNKS + SEEKS) )) requests"

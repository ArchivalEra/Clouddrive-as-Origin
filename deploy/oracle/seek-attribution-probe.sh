#!/usr/bin/env bash
# Phase 2: attribute one COLD JUMP's first byte, from the origin's own side.
#
# Why the origin's side is where the split lives: no CDN-side probe can see the
# provider's `stat` and `open` (they happen between the edge and us), but the
# origin times both (`backend_call_duration_seconds{op}`) and its own first body
# byte (`cache_body_ttfb_seconds{source="upstream"}`). Three cold jumps give
# three samples of each, so the split is a delta, not a guess.
#
# Run ON the node. `ORIGIN_METRICS` overrides the metrics URL.
set -u
export no_proxy="*"; unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 2>/dev/null
MET=${ORIGIN_METRICS:-http://127.0.0.1:9090/metrics}
BIZ=${BIZ:-http://127.0.0.1:8080}
F=$BIZ/googledrive1/%E9%9C%87%E6%92%BC%E6%88%91%E4%BB%AC%E7%9A%84%E6%9C%AA%E6%9D%A5%E5%90%A7_200G.mp4

val() { # metric-base label-fragment count|sum -> the first sample's value
  curl -s -m 5 "$MET" | grep -E "^$1_$3\{" | grep -F "$2" | awk '{print $NF}' | head -1
}

mean_ms() { # metric-base label c0 s0 -> mean ms over the samples added since
  local c1 s1 c0=$3 s0=$4
  c1=$(val "$1" "$2" count)
  s1=$(val "$1" "$2" sum)
  if [ -z "$c1" ] || [ -z "$c0" ]; then echo "-"; return; fi
  awk -v c1="$c1" -v s1="$s1" -v c0="$c0" -v s0="$s0" 'BEGIN {
    n = c1 - c0
    if (n <= 0) { print "- (no new samples)"; exit }
    printf "%.0f ms (n=%d)", (s1 - s0) / n * 1000, n
  }'
}

echo "=== one cold jump, attributed at the origin ==="
echo "  segment_bytes before: $(curl -s -m 5 "$BIZ/_internal/healthz" | grep -oE '"segment_bytes":[0-9]+' | cut -d: -f2)"

B_STAT=$(val backend_call_duration_seconds 'op="stat"' count); S_STAT=$(val backend_call_duration_seconds 'op="stat"' sum)
B_OPEN=$(val backend_call_duration_seconds 'op="open"' count); S_OPEN=$(val backend_call_duration_seconds 'op="open"' sum)
B_TTFB=$(val cache_body_ttfb_seconds 'source="upstream"' count); S_TTFB=$(val cache_body_ttfb_seconds 'source="upstream"' sum)
B_HDRS=$(val cache_serve_duration_seconds 'outcome="passthrough"' count); S_HDRS=$(val cache_serve_duration_seconds 'outcome="passthrough"' sum)

for i in 1 2 3; do
  OFF=$(( (214748364800 / 100) * (5 + RANDOM % 90) ))
  T=$(curl -s -m 60 -r "$OFF-$((OFF + 65535))" -o /dev/null -w '%{time_starttransfer}' "$F")
  printf '  jump %d at %s: first byte %.0f ms\n' "$i" "$OFF" "$(awk -v t="$T" 'BEGIN { print t * 1000 }')"
done

echo "  --- what the origin spent inside those jumps (mean per call) ---"
echo "    provider stat     : $(mean_ms backend_call_duration_seconds 'op="stat"' "$B_STAT" "$S_STAT")"
echo "    provider open     : $(mean_ms backend_call_duration_seconds 'op="open"' "$B_OPEN" "$S_OPEN")"
echo "    headers ready     : $(mean_ms cache_serve_duration_seconds 'outcome="passthrough"' "$B_HDRS" "$S_HDRS")"
echo "    our first byte    : $(mean_ms cache_body_ttfb_seconds 'source="upstream"' "$B_TTFB" "$S_TTFB")"
echo "  (a jump's own first byte is printed per jump above; the rows below it are"
echo "   what the origin spent inside that window — stat and open are per CALL, so"
echo "   with retries off and three jumps each count should be 3)"

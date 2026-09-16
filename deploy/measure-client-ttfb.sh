#!/usr/bin/env bash
# Client-side TTFB measurement (spec.md section 10), run FROM A REAL CLIENT.
#
# Why this exists: every measurement in this repo used to originate from the
# origin node itself (loopback), which says nothing about the criterion the
# spec actually states -- "cold miss TTFB < 1.5 s (typical domestic -> VPS)".
# That number had never been taken until 2026-09-16. This script is that
# measurement, made repeatable.
#
# What it does NOT claim: this measures LATENCY, not concurrency. A client on
# a 30 Mbps link can answer "is the first byte fast enough" but cannot
# reproduce a multi-client throughput scenario -- use the node-side
# seek-storm for that (deploy/seek-storm.sh).
#
# Usage:
#   deploy/measure-client-ttfb.sh <url> [trials] [cold]
#     cold = "cold" clears the origin cache for the key over ssh before each
#            trial (requires ssh access to the origin node).
# Example:
#   deploy/measure-client-ttfb.sh https://cdn-oracle.isui.ren/googledrive1/test-page.html 3
#   deploy/measure-client-ttfb.sh https://cdn-oracle.isui.ren/googledrive1/test-page.html 2 cold
set -u

URL="${1:?usage: measure-client-ttfb.sh <url> [trials] [cold]}"
TRIALS="${2:-3}"
COLD="${3:-}"
BUDGET_S="${TTFB_BUDGET:-1.5}"
SSH_HOST="${ORIGIN_SSH:-oracle-cdn}"

# Derive the origin cache path from the URL: /<bucket>/<key>
KEY=$(printf '%s' "$URL" | sed 's|https\?://[^/]*/||')
BUCKET=$(printf '%s' "$KEY" | cut -d/ -f1)
OBJECT=$(printf '%s' "$KEY" | cut -d/ -f2-)

echo "== client TTFB: $URL =="
echo "   budget: ${BUDGET_S}s (spec section 10, cold miss)"
[ "$COLD" = "cold" ] && echo "   mode: COLD (clearing the origin cache per trial over ssh)"
echo

pass=0; fail=0
for i in $(seq 1 "$TRIALS"); do
  if [ "$COLD" = "cold" ]; then
    ssh -o BatchMode=yes "$SSH_HOST" "rm -f /opt/origin-cache/cache-standard/$BUCKET/$OBJECT" 2>/dev/null
  fi
  out=$(curl -s -m 90 -o /dev/null \
    -w "%{http_code} %{time_starttransfer} %{time_total} %{size_download}" \
    "$URL?ttfb=$RANDOM$i" 2>&1)
  read -r code ttfb total size <<<"$out"
  ok=$(awk -v t="$ttfb" -v b="$BUDGET_S" 'BEGIN { print (t <= b) ? "PASS" : "FAIL" }')
  [ "$ok" = "PASS" ] && pass=$((pass+1)) || fail=$((fail+1))
  printf "  trial %d: %s  ttfb=%ss total=%ss http=%s bytes=%s\n" "$i" "$ok" "$ttfb" "$total" "$code" "$size"
done

echo
echo "  budget ${BUDGET_S}s: $pass pass / $fail fail"
echo
echo "NOTE: a failure here is not automatically the origin's fault. Latency"
echo "from a client crosses the CDN edge first, and that segment has been"
echo "measured as the dominant cost (see docs/notes/ and the perf-attribution"
echo "map). Compare against the origin-side numbers before blaming this node:"
echo "  ssh $SSH_HOST 'curl -s http://127.0.0.1:8080/_internal/healthz'"
echo "  ssh $SSH_HOST 'deploy/metrics-report.sh http://127.0.0.1:9090/metrics 30'"
[ "$fail" -eq 0 ]

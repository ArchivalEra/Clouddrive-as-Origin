#!/usr/bin/env bash
# Node acceptance: run after any deploy (ssh <node> 'bash accept.sh').
#
# Covers the ordinary serving contract (GET / range / HEAD, key handling),
# the magazine's admission surfaces (healthz stray_bytes, the three body
# metrics), the watchdog report, and unit health. Added for the 2026-09-19
# admission round; see docs/spec.md section 10 for the evidence each line
# is pinned by.
set -u
APP=/opt/origin-cache
FRONT=https://127.0.0.1:7777
BIZ=http://127.0.0.1:8080
BIZ_NC=http://127.0.0.1:8081
METRICS=http://127.0.0.1:9090/metrics
KEY=googledrive1/test-page.html
VERDICT=0
fail() { echo "  FAIL: $1"; VERDICT=1; }

# Poll for readiness: start-up has been measured between 1 s and 11 s, so a
# fixed sleep is either flaky or slow.
wait_ready() {
  for _ in $(seq 1 90); do
    curl -fsS --max-time 3 "$BIZ/_internal/healthz" >/dev/null 2>&1 \
      && curl -fsS --max-time 3 "$BIZ_NC/_internal/healthz" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  service did not become ready within 90s"; return 1
}
code() { curl --noproxy '*' -k -sS --max-time 30 -o /dev/null -w '%{http_code}' "$@"; }

echo "=== identity ==="
sha256sum "$APP/origin-cache"
# Print selected healthz fields as `k=v`, with the key list passed as argv so
# the shell never has to quote JSON for python.
pick() { python3 -c 'import json,sys; d=json.load(sys.stdin); print(" ".join(f"{k}={d.get(k)}" for k in sys.argv[1:]))' "$@"; }
hz() { curl -fsS --max-time 5 "$1/_internal/healthz"; }

wait_ready || exit 1

echo "=== healthz carries the new field ==="
echo "  standard: $(hz $BIZ | pick status degraded entries bytes stray_bytes segment_bytes)"
h=$(hz $BIZ)
printf '%s' "$h" | grep -q '"status":"ok"' || fail "healthz must be ok"
printf '%s' "$h" | grep -q '"stray_bytes"' || fail "healthz must report stray_bytes"
printf '%s' "$h" | grep -q '"stray_bytes":0' || fail "a fresh node has no strays"
echo "  nocache:  $(hz $BIZ_NC | pick status entries stray_bytes)"

echo "=== ordinary paths unchanged ==="
full=$(code "$FRONT/$KEY"); echo "  GET /$KEY -> $full"
[ "$full" = 200 ] || fail "an ordinary GET must be 200"
hdr=$(curl --noproxy '*' -k -sS --max-time 60 -D - -o /dev/null -H 'range: bytes=0-99' "$FRONT/$KEY")
rcode=$(printf '%s' "$hdr" | head -1 | awk '{print $2}')
cr=$(printf '%s' "$hdr" | grep -i '^content-range:' | tr -d '\r')
echo "  GET (range 0-99) -> $rcode  $cr"
[ "$rcode" = 206 ] || fail "a ranged GET must be 206"
[ -n "$cr" ] || fail "a ranged GET must carry content-range"
head=$(curl --noproxy '*' -k -sS --max-time 30 -I "$FRONT/$KEY")
hcode=$(printf '%s' "$head" | head -1 | awk '{print $2}')
clen=$(printf '%s' "$head" | grep -i '^content-length:' | tr -d '\r' | awk '{print $2}')
echo "  HEAD -> $hcode content-length=$clen"
[ "$hcode" = 200 ] || fail "HEAD must be 200"
[ -n "$clen" ] || fail "HEAD must report a length"

echo "=== key handling ==="
for p in /redb.db /.tmp.a.b.1234; do
  c=$(code "$FRONT$p"); echo "  $p -> $c"; [ "$c" = 400 ] || fail "$p must be 400"
done
for p in /googledrive1/redb.db /googledrive1/.hidden; do
  c=$(code "$FRONT$p"); echo "  $p -> $c"
  [ "$c" = 400 ] && fail "$p must not be rejected as a reserved key"
  [ "$c" = 404 ] || fail "$p must be 404 (absent, not refused)"
done

echo "=== the seek instruments are exposed ==="
m=$(curl -fsS --max-time 10 "$METRICS")
for name in cache_serve_source_total cache_body_ttfb_seconds cache_body_bytes_total; do
  if printf '%s' "$m" | grep -q "$name"; then
    echo "  $name: present"
  else
    fail "$name is missing from $METRICS"
  fi
done
# The registry is process-global and shared by both planes: a served range
# must have produced a labelled sample by now.
printf '%s' "$m" | grep -q 'cache_serve_source_total{source="' || fail "no source label was recorded"

echo "=== reporting ==="
hb=$(curl -sS --max-time 10 -o /dev/null -w '%{http_code}' -X POST \
  -H 'Content-Type: application/json' \
  --data '{"schema":"origin-cache/status/1","host":"origin-1","event":"heartbeat","ts":"2026-09-19T00:00:00Z","service":"origin-cache","version":"probe","status":"active","entries":0,"bytes":0}' \
  "$(grep -oE 'https://[^" }]+' "$APP/watchdog.sh" | head -1)" || true)
echo "  report endpoint reachable: $hb"
[ "$hb" = 200 ] || fail "the heartbeat endpoint must answer 200"

echo "=== watchdog did not report a false down ==="
last=$(journalctl -u origin-cache-watchdog.service --since '-90 min' --no-pager 2>/dev/null | grep -c 'verdict=FAIL' || true)
echo "  FAIL verdicts in the last 90 minutes: $last"
[ "$last" = 0 ] || fail "the watchdog reported a failure while the units were up"

echo "=== units ==="
systemctl is-active origin-cache-efficient origin-cache-nocache origin-cache-watchdog.timer cloudflared | tr '\n' ' '; echo
failed=$(systemctl --failed --no-legend | wc -l)
echo "  failed units: $failed"
[ "$failed" = 0 ] || fail "systemctl --failed is not empty"

echo "VERDICT=$([ "$VERDICT" = 0 ] && echo PASS || echo FAIL)"
exit "$VERDICT"

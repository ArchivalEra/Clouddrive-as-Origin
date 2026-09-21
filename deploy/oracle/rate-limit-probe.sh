#!/usr/bin/env bash
# Would the front's per-IP rate ceiling be safe to enable in production?
#
# This answers the half that can be answered without touching the production
# units: does the ceiling behave on THIS node, with the production binary and the
# production upstream, and does it recover. It starts a loopback-only instance of
# its own (own cache dir, own ports, the same OpenList) with `front_rate_rps = 2`,
# probes it, and removes it. Nothing here touches origin-cache-efficient.
#
# The other half — does EdgeOne retry a 429, and would that make things worse —
# needs the ceiling on the port the CDN pulls from (7777), i.e. a production
# config change with a fast rollback. That is a DECISION, not a test, and this
# script prints the procedure for it instead of doing it.
#
# Usage: rate-limit-probe.sh            (on the node)
# Env:   RPS=<n>       the ceiling to probe with (default 2)
#        RATE_DIR=     the scratch cache dir (default /home/opc/rl-cache)
set -u

RPS=${RPS:-2}
RATE_DIR=${RATE_DIR:-/home/opc/rl-cache}
FRONT=7792
BIZ=8092
MET=9097
CFG=/home/opc/rl-probe.toml
LOG=/home/opc/rl-probe.log
BIN=/opt/origin-cache/origin-cache
ENVF=/opt/origin-cache/origin-cache.env
OBJ=test-page.html

# The upstream credentials live in the production env file (never in a repo).
if [ -r "$ENVF" ]; then
  set -a; . "$ENVF"; set +a
else
  echo "FAIL: $ENVF is not readable; run this as the service user"; exit 1
fi
[ -n "${OPENLIST_USERNAME:-}" ] && [ -n "${OPENLIST_PASSWORD:-}" ] || {
  echo "FAIL: the env file did not provide the upstream credentials"; exit 1; }

cat > "$CFG" <<TOML
front_listen = "127.0.0.1:$FRONT"
front_metrics_listen = "127.0.0.1:$MET"
front_rate_rps = $RPS
listen_addr = "127.0.0.1:$BIZ"
cache_dir = "$RATE_DIR"
max_size_bytes = 268435456
inactive_ttl_secs = 1200
revalidate_ttl_secs = 60
negative_ttl_secs = 60
concurrency_per_upstream = 3
session_window_bytes = 262144
# Required: the binary refuses to start without it (an open prewarm endpoint
# would let any caller force upstream fetches).
prewarm_shared_secret_env = "ORIGIN_PREWARM_SECRET"

[[upstreams]]
id = "googledrive1"
type = "openlist"
base_url = "http://127.0.0.1:5244/dav"
root_path = "googledrive1"
username_env = "OPENLIST_USERNAME"
password_env = "OPENLIST_PASSWORD"
cache_profile = "efficient"

[[routes]]
prefix = ""
upstream = "googledrive1"
TOML

mkdir -p "$RATE_DIR"
setsid nohup "$BIN" "$CFG" > "$LOG" 2>&1 < /dev/null &
PID=$!
trap 'kill "$PID" 2>/dev/null; rm -f "$CFG"' EXIT

for _ in $(seq 1 40); do
  curl -fsS -m 2 "http://127.0.0.1:$BIZ/_internal/healthz" >/dev/null 2>&1 && break
  sleep 0.5
done
curl -fsS -m 2 "http://127.0.0.1:$BIZ/_internal/healthz" >/dev/null 2>&1 || {
  echo "FAIL: the probe instance did not come up"; tail -5 "$LOG"; exit 1; }
echo "probe instance: front 127.0.0.1:$FRONT, rps=$RPS, object $OBJ"
grep -o 'front per-ip rate limit active.*' "$LOG" | head -1 | sed 's/^/    /'

# Wait out any window the readiness probe opened, then fire three fast requests.
sleep "$(awk -v r="$RPS" 'BEGIN { printf "%.1f", 1.2 }')"
codes=""
for _ in 1 2 3; do
  codes="$codes $(curl -s -m 15 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$FRONT/googledrive1/$OBJ")"
done
case "$codes" in
  *" 429") echo "PASS: a request over the ceiling is refused (codes:$codes)";;
  *) echo "FAIL: nothing was refused with rps=$RPS (codes:$codes)";;
esac

# The window is one second: an allowed request must work again afterwards.
sleep 1.5
code=$(curl -s -m 15 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$FRONT/googledrive1/$OBJ")
[ "$code" = 200 ] || [ "$code" = 206 ] && echo "PASS: the ceiling recovers after a second ($code)" \
  || echo "FAIL: a request after the window returned $code"

# Concurrency: several at once must also be counted, not raced past.
sleep 1.5
tmp=$(mktemp -d)
pids=""
for i in 1 2 3 4 5; do
  ( curl -s -m 15 -o /dev/null -w '%{http_code}\n' "http://127.0.0.1:$FRONT/googledrive1/$OBJ" >> "$tmp/$i" ) &
  pids="$pids $!"
done
# NOT a bare `wait`: the probe instance is itself a background job of this script,
# so waiting for "all jobs" waits for it forever — the first version of this
# probe hung exactly there, after printing nothing, with the ssh channel open.
for p in $pids; do wait "$p"; done
codes=$(cat "$tmp"/* | tr '\n' ' ')
refused=$(cat "$tmp"/* | grep -c 429 || true)
rm -rf "$tmp"
[ "${refused:-0}" -ge 1 ] && echo "PASS: five concurrent requests were counted ($refused refused: $codes)" \
  || echo "FAIL: five concurrent requests were not limited ($codes)"

# The ceiling must not touch the BUSINESS plane: it is the front's own gate.
sleep 1.5
code=$(curl -s -m 15 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$BIZ/_internal/healthz")
[ "$code" = 200 ] && echo "PASS: the business plane is not rate limited ($code)" \
  || echo "FAIL: the business plane returned $code"

echo
echo "the other half (needs a decision, not a test):"
echo "  EdgeOne pulls from 7777. Enabling the ceiling there = one line in the"
echo "  production config + a restart; watch it with:"
echo "    front_requests_total{status=\"429\"} on 9090, and the origin's own"
echo "    access log for the CDN's retries (a retry storm would show as a rising"
echo "    request rate with the same keys). Rollback is the same line removed."
echo "  Why it is a decision: a 429 reaches the EDGE, and what the edge does with"
echo "  it is the operator's contract, not ours to discover unattended."

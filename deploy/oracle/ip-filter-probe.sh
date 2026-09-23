#!/usr/bin/env bash
# R9: the front's two IP lists, end to end, against the dual-stack listener.
#
# Why on the node and not in the LAB: the LAB front binds 127.0.0.1, where an
# IPv4 peer is a plain v4 address. The production front binds [::], where the
# same peer arrives as an IPv4-mapped IPv6 address — and before the
# canonicalizing matcher (`ip_in_any`) an operator's v4 CIDR never matched it:
# the origin-token loopback exemption missed accept.sh on this very node, and
# the same hole was latent in both IP lists (pitfall 60). Only a [::] listener
# can exercise that path, so the probe instance binds [::]:7793.
#
# What the two lists ARE (front/src/lib.rs): `front_ip_block` is a
# connection-time filter (the peer never reaches any HTTP exchange);
# `front_ip_allow` only exempts a peer from the per-IP rate ceiling — it can
# never widen access. Each phase below restarts the probe instance with one
# knob set (own ports 7793/8093/9094, own cache dir, the same loopback
# OpenList). Nothing here touches the production units.
#
#   1  ceiling on, allow empty            -> a burst over the ceiling 429s
#   2  allow = 127.0.0.1/32               -> the same burst is exempt (0 x 429)
#   3  allow = a non-matching range       -> 429s again (matching matters, not
#                                            the presence of a list)
#   4  block = 127.0.0.1/32               -> connection dropped before any HTTP
#   5  block = ::1/128                    -> dropped over the pure v6 path
#   6  block = a non-matching range       -> answered as usual; the access log
#                                            shows the mapped peer form
#   7  business refused (temp nft rule)   -> what the front maps that to
#
# Usage: ip-filter-probe.sh                (on the node)
# Env:   KEEP=1  leave the instance and the log behind for debugging
set -u

FRONT=7793
BIZ=8093
MET=9094
CFG=/home/opc/ipf-probe.toml
LOG=/home/opc/ipf-probe.log
BIN=/opt/origin-cache/origin-cache
ENVF=/opt/origin-cache/origin-cache.env
DIR=/home/opc/ipf-cache
OBJ=test-page.html
URL="http://127.0.0.1:$FRONT/googledrive1/$OBJ"

if [ -r "$ENVF" ]; then set -a; . "$ENVF"; set +a
else echo "FAIL: $ENVF not readable; run as the service user"; exit 1; fi
[ -n "${OPENLIST_USERNAME:-}" ] && [ -n "${OPENLIST_PASSWORD:-}" ] || {
  echo "FAIL: the env file did not provide the upstream credentials"; exit 1; }
export ORIGIN_PREWARM_SECRET=ipf-probe-secret

busy=$(ss -ltn | awk '{print $4}' | grep -cE "(:$FRONT|:$BIZ|:$MET)$" || true)
[ "$busy" = 0 ] || { echo "FAIL: $FRONT/$BIZ/$MET are not all free"; exit 1; }

PID=""
cleanup() {
  sudo -n nft delete table ip ipf_probe 2>/dev/null || true
  if [ "${KEEP:-0}" = 1 ]; then echo "(kept: pid $PID, log $LOG, cfg $CFG)"; return; fi
  [ -n "$PID" ] && { kill "$PID" 2>/dev/null; sleep 0.5; }
  rm -f "$CFG"
}
trap cleanup EXIT

mkdir -p "$DIR"
: > "$LOG"

stop_prev() {
  [ -n "$PID" ] || return 0
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  PID=""
  sleep 1.2
  if ss -ltn | awk '{print $4}' | grep -qE "(:$FRONT|:$BIZ)$"; then
    sudo -n fuser -k "$FRONT"/tcp "$BIZ"/tcp "$MET"/tcp 2>/dev/null || true
    sleep 1
  fi
}

start() { # "$@" = extra top-level TOML lines (spliced BEFORE the tables)
  stop_prev
  {
    cat <<TOML
front_listen = "[::]:$FRONT"
front_metrics_listen = "127.0.0.1:$MET"
listen_addr = "127.0.0.1:$BIZ"
cache_dir = "$DIR"
max_size_bytes = 268435456
inactive_ttl_secs = 1200
revalidate_ttl_secs = 60
negative_ttl_secs = 60
concurrency_per_upstream = 3
session_window_bytes = 262144
prewarm_shared_secret_env = "ORIGIN_PREWARM_SECRET"
TOML
    printf '%s\n' "$@"
    cat <<TOML

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
  } > "$CFG"
  setsid nohup "$BIN" "$CFG" >> "$LOG" 2>&1 < /dev/null &
  PID=$!
  for _ in $(seq 1 40); do
    curl -fsS -m 2 "http://127.0.0.1:$BIZ/_internal/healthz" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "FAIL: the probe instance did not come up"; tail -5 "$LOG"; exit 1
}

burst() { # six parallel requests; prints their codes
  tmp=$(mktemp -d); pids=""
  for i in 1 2 3 4 5 6; do
    ( curl -s -m 15 -o /dev/null -w '%{http_code}\n' "$URL" >> "$tmp/$i" ) &
    pids="$pids $!"
  done
  # NOT a bare wait: the probe instance is itself a background job of this
  # script, so a bare wait waits for it forever (pitfall 28).
  for p in $pids; do wait "$p"; done
  codes=$(cat "$tmp"/* | tr '\n' ' ')
  rm -rf "$tmp"
  echo "$codes"
}

echo "== 1: ceiling on, allow empty -> expect a 429 in the burst"
start "front_rate_rps = 2"
codes=$(burst)
case "$codes" in *429*) echo "PASS: the ceiling counts the mapped peer ($codes)";;
  *) echo "FAIL: no 429 without any allow list ($codes)";; esac

echo "== 2: allow = 127.0.0.1/32 -> the mapped loopback peer is exempt"
start "front_rate_rps = 2" 'front_ip_allow = ["127.0.0.1/32"]'
codes=$(burst)
case "$codes" in *429*) echo "FAIL: the v4 allow entry missed the mapped peer ($codes)";;
  *) echo "PASS: exempt through the mapped form, 0 x 429 ($codes)";; esac

echo "== 3: allow = 10.0.0.0/8 (non-matching) -> 429s return"
start "front_rate_rps = 2" 'front_ip_allow = ["10.0.0.0/8"]'
codes=$(burst)
case "$codes" in *429*) echo "PASS: an allow list that does not match exempts nothing ($codes)";;
  *) echo "FAIL: a non-matching allow list still exempted ($codes)";; esac

echo "== 4: block = 127.0.0.1/32 -> connection dropped before any HTTP"
start 'front_ip_block = ["127.0.0.1/32"]'
code=$(curl -s -m 15 -o /dev/null -w '%{http_code}' "$URL")
[ "$code" = 000 ] && echo "PASS: the blocked mapped peer never got an answer" \
  || echo "FAIL: the blocked peer was answered ($code)"

echo "== 5: block = ::1/128 -> the pure v6 path is still matchable"
start 'front_ip_block = ["::1/128"]'
code=$(curl -s -m 15 -o /dev/null -w '%{http_code}' "http://[::1]:$FRONT/googledrive1/$OBJ")
[ "$code" = 000 ] && echo "PASS: the v6 entry still matches a v6 peer" \
  || echo "FAIL: the v6 peer was answered ($code)"

echo "== 6: block = 192.0.2.0/24 (non-matching) -> answered as usual"
start 'front_ip_block = ["192.0.2.0/24"]'
code=$(curl -s -m 15 -o /dev/null -w '%{http_code}' "$URL")
case "$code" in 200|206) echo "PASS: an unrelated blocklist still answers ($code)";;
  *) echo "FAIL: expected 200/206, got $code";; esac
# The log is ANSI-coloured, so `peer=` arrives split by escape sequences and a
# plain grep finds nothing; strip the colour first.
sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -oE 'peer=\[::ffff:127\.0\.0\.1\](:[0-9]+)?' | head -1 | sed 's/^/    evidence: /'

echo "== 7: the business dies (RST on the pooled connection) -> 502, no body"
sudo -n nft add table ip ipf_probe
sudo -n nft add chain ip ipf_probe out '{ type filter hook output priority 0; }'
# `reject with tcp reset` models a process death: the front's pooled connection
# to the business gets a RST. (A plain `reject` - ICMP - leaves that pooled
# connection silent instead, and then the request that lands on it gets NO
# answer at all until the client gives up: access log status=0. Measured
# 2026-09-23; the RST shape is the one production has.)
sudo -n nft add rule ip ipf_probe out ip daddr 127.0.0.1 tcp dport $BIZ reject with tcp reset
c1=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$URL")
hdr=$(curl -s -m 15 -D - -o /dev/null "$URL" | tr -d '\r' | grep -iE '^(HTTP/|content-length|cache-control)' | tr '\n' ' ')
c2=$(curl -s -m 15 -o /dev/null -w '%{http_code}' "$URL")
sudo -n nft delete table ip ipf_probe 2>/dev/null || true
if [ "$c1" = 502 ] && [ "$c2" = 502 ]; then
  echo "PASS: a dead business is a 502 (both requests; $hdr)"
  echo "      (the pooled request is the interesting one: it is the path a restart takes)"
else
  echo "FAIL: expected 502 and 502, got $c1 and $c2 ($hdr)"
fi
code=$(curl -s -m 15 -o /dev/null -w '%{http_code}' "$URL")
[ "$code" = 200 ] && echo "PASS: the front recovers once the business answers again" \
  || echo "FAIL: after the rule was removed the front returned $code"

echo
echo "done. log: $LOG"

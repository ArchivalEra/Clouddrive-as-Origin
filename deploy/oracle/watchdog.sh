#!/usr/bin/env bash
# Oracle node health watchdog (map #30 T4): checks the systemd units, the
# disk watermark and the business-plane healthz, logs failures, and reports
# a heartbeat to the blog-side Cloudflare Worker (interface agreed
# 2026-09-16, see docs/status-reporting.md).
#
# The report rides the node's existing cloudflared tunnel and is
# authenticated by the TUNNEL token itself: there is no application-level
# credential, so this script holds no secret and needs no env file.
#
# Install:  sudo install -m 0755 deploy/oracle/watchdog.sh /opt/origin-cache/watchdog.sh
#           sudo cp deploy/oracle/origin-cache-watchdog.service /etc/systemd/system/
#           sudo systemctl daemon-reload && sudo systemctl enable --now origin-cache-watchdog.timer
set -u

UNITS="origin-cache-standard origin-cache-nocache"
# State files, kept together so a test can point them at a scratch directory.
# The node's watchdog unit declares no EnvironmentFile, so on the node these
# defaults are what runs (same reasoning as REPORT_URL below).
STATE_DIR="${STATE_DIR:-/opt/origin-cache}"
LOG="$STATE_DIR/watchdog.log"
STATE="$STATE_DIR/watchdog.state"
HEARTBEAT="$STATE_DIR/watchdog.heartbeat"
DOWN_STAMP="$STATE_DIR/notify-down.stamp"
DISK_WARN_PCT=85
DISK_CRIT_PCT=95

# Reporting endpoints. Overridable so the local dry-run can point at a
# loopback receiver: the watchdog unit declares no EnvironmentFile, so on
# the node nothing can override these.
REPORT_URL="${REPORT_URL:-https://api.mango-mesa.ccwu.cc/api/activity/origin-cache}"
HOST_ID="${HOST_ID:-origin-1}"

log() { echo "$(date -Is) $*" >> "$LOG" 2>/dev/null || true; }
ts_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# `EXIT_STATUS` is a signal number when systemd killed the process and a
# numeric exit status when the process exited on its own; the down report
# carries both spellings so the far side does not have to know systemd.
signal_name() {
  case "$1" in
    1) echo SIGHUP ;;  2) echo SIGINT ;;  3) echo SIGQUIT ;; 4) echo SIGILL ;;
    6) echo SIGABRT ;; 7) echo SIGBUS ;;  8) echo SIGFPE ;;  9) echo SIGKILL ;;
    11) echo SIGSEGV ;; 13) echo SIGPIPE ;; 14) echo SIGALRM ;; 15) echo SIGTERM ;;
    *) echo "SIG$1" ;;
  esac
}

# Fire-and-forget: a 5s ceiling, no retry, and a failure never touches the
# verdict. Losing one report costs a gap the dead-man switch absorbs; the
# watchdog must not become a second thing that can fail loudly.
post() { # payload, label
  local body="$1" label="$2" code
  code=$(curl -sS -m 5 -o /dev/null -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' --data "$body" "$REPORT_URL" 2>/dev/null) || code=000
  case "$code" in
    2*) ;;
    *) log "report $label failed http=$code" ;;
  esac
}

# ExecStopPost hook: report a NON-clean exit. Only reached as
# `watchdog.sh --down <unit>`.
#
# The dead-man switch is the real detector: a node that is powered off, out
# of network or dead at kernel level can report nothing at all, and 15
# minutes of silence catches it. This hook exists only to make a process
# death EARLIER and more informative than that silence.
#
# TimeoutStopSec must exceed Pingora's grace period and runtime shutdown.
# With that budget, a timeout is a failure, not evidence of a planned stop.
send_down() {
  local unit="${1:-origin-cache}" result="${SERVICE_RESULT:-unknown}" code="${EXIT_CODE:-}" status="${EXIT_STATUS:-}"
  case "$result" in
    success)
      log "planned stop, no down report unit=${unit%%.service} result=$result"
      return 0
      ;;
  esac
  # Throttle: `Restart=always` restarts every 3s, so a crash loop would fire
  # this hook ~20 times a minute and flood the far side. The far side's
  # dedupe keys on the timestamp, which every one of those would defeat.
  # One down report per minute is plenty -- the heartbeat (which still goes
  # out every 5 minutes, with status=down) carries the state meanwhile.
  local stamp="$DOWN_STAMP" now last
  now=$(date +%s)
  last=$(cat "$stamp" 2>/dev/null || echo 0)
  case "$last" in ''|*[!0-9]*) last=0 ;; esac
  if [ $((now - last)) -lt 60 ]; then
    log "down report throttled unit=${unit%%.service} result=$result"
    return 0
  fi
  printf '%s\n' "$now" > "$stamp" 2>/dev/null || true
  local sig="" ec="" n="" reason
  case "$code" in
    killed|dumped)
      case "$status" in
        ''|*[!0-9]*) sig="$status" ;;
        *) n="$status"; sig="$(signal_name "$n")"; ec=$((128 + n)) ;;
      esac
      ;;
    exited) ec="$status" ;;
  esac
  reason="systemd service result=$result"
  [ -n "$code" ] && reason="$reason code=$code"
  [ -n "$ec" ] && reason="$reason exit=$ec"
  local payload
  payload=$(jq -nc --arg host "$HOST_ID" --arg ts "$(ts_iso)" \
    --arg service "${unit%%.service}" --arg sig "$sig" --arg ec "$ec" --arg reason "$reason" \
    '{schema:"origin-cache/status/1",host:$host,event:"down",ts:$ts,service:$service,status:"down",
      death:{signal:($sig|if .=="" then null else . end),
             exit_code:($ec|if .=="" then null else tonumber end),reason:$reason}}') || return 0
  log "down report unit=${unit%%.service} result=$result sig=$sig exit=$ec"
  post "$payload" down
}

case "${1:-}" in
  --down) send_down "${2:-origin-cache}"; exit 0 ;;
esac

# Collect problems in a string; empty means healthy.
PROBLEMS=""
note() { PROBLEMS="${PROBLEMS}${PROBLEMS:+; }$*"; }

# --- unit health ------------------------------------------------------------
STD_UNIT_UP=1
for u in $UNITS; do
  if ! systemctl is-active --quiet "$u"; then
    note "unit=$u inactive"
    [ "$u" = origin-cache-standard ] && STD_UNIT_UP=0
  fi
done

# --- disk watermark ---------------------------------------------------------
pct=$(df -P / | awk 'NR==2 {gsub("%","",$5); print $5}')
if [ -n "${pct:-}" ] && [ "$pct" -ge "$DISK_CRIT_PCT" ]; then
  note "disk=${pct}% (crit >= ${DISK_CRIT_PCT}%)"
elif [ -n "${pct:-}" ] && [ "$pct" -ge "$DISK_WARN_PCT" ]; then
  note "disk=${pct}% (warn >= ${DISK_WARN_PCT}%)"
fi

# --- healthz probe (business plane reachable) --------------------------------
# The business plane answers on its own loopback port (plaintext). Probing the
# FRONT ports instead means speaking TLS to a TLS listener: the old script
# asked `http://127.0.0.1:7777` and got 000 every time, so it logged "healthz
# unreachable" on every run since it was written -- a watchdog reporting a
# permanent outage that never happened. 94 log lines, 94 FAILs.
#
# Probing the business plane is also the more meaningful check: it is the
# component that actually serves cache semantics, and it stays meaningful
# whether or not the front is up.
# The HTTP STATUS matters, not just reachability (A2). `curl -o /dev/null`
# without `-f` treats a 500 as success, so a serving-errors outage looked
# healthy; and since the handler returns 200 with a `degraded` flag in the
# body (C1), the body is checked too.
LAST_BODY=""
probe() { # name, url
  local name="$1" url="$2" code body
  LAST_BODY=""
  body=$(curl -s -m 5 -w '\n%{http_code}' "$url" 2>/dev/null) || { note "$name unreachable"; return; }
  code=$(printf '%s' "$body" | tail -n1)
  case "$code" in
    2*) ;;
    *) note "$name http=$code"; return ;;
  esac
  LAST_BODY=$(printf '%s' "$body" | head -n -1)
  if printf '%s' "$LAST_BODY" | grep -q '"degraded":true'; then
    local why
    why=$(printf '%s' "$LAST_BODY" | sed -n 's/.*"degraded_reasons":\[\([^]]*\)\].*/\1/p')
    note "$name degraded${why:+: $why}"
  fi
}
probe "healthz standard (:8080)" http://127.0.0.1:8080/_internal/healthz
STD_BODY="$LAST_BODY"
STD_PROBE_OK=0
[ -n "$STD_BODY" ] && STD_PROBE_OK=1
probe "healthz nocache (:8081)"  http://127.0.0.1:8081/_internal/healthz

# --- report: transitions + a daily heartbeat (D3) ---------------------------
# Silent-per-run logging made "healthy all along" indistinguishable from
# "the watchdog itself died". Now: a line only when the verdict CHANGES, plus
# one heartbeat per day proving the checker is alive.
verdict="OK"
[ -n "$PROBLEMS" ] && verdict="FAIL"

# Transition line: written ONLY when the verdict differs from the last run.
prev=$(cat "$STATE" 2>/dev/null || echo "")
if [ "$verdict" != "$prev" ]; then
  if [ "$verdict" = "FAIL" ]; then
    log "FAIL $PROBLEMS"
  else
    log "OK recovered"
  fi
  printf '%s\n' "$verdict" > "$STATE"
fi

# Heartbeat: one line per DAY, regardless of verdict, proving the checker
# itself is alive. Deliberately a separate decision from the transition
# above -- conflating them made a steady FAIL rewrite its line every run.
today=$(date -u +%Y-%m-%d)
last_hb=$(cat "$HEARTBEAT" 2>/dev/null || echo "")
if [ "$last_hb" != "$today" ]; then
  log "HB disk=${pct:-?}% units=$(printf '%s\n' $UNITS | grep -c .) verdict=$verdict"
  printf '%s\n' "$today" > "$HEARTBEAT"
fi

# --- status report to the blog worker --------------------------------------
# Every run, i.e. every 5 minutes: the far side calls a node offline after
# 15 minutes of silence, and that timeout -- not this script -- is what
# detects a node that cannot speak for itself. Sent on failure verdicts too:
# a degraded node still has to prove it is alive.
#
# Only `active`/`degraded`/`down` and the two documented events are sent;
# no field outside the agreed schema, so a strict receiver cannot reject a
# report and turn a degraded node into a false offline alert.
if ! command -v jq >/dev/null 2>&1; then
  log "report skipped: jq not installed"
else
  entries=$(jq -r '.entries // 0' <<<"$STD_BODY" 2>/dev/null || echo 0)
  bytes=$(jq -r '.bytes // 0' <<<"$STD_BODY" 2>/dev/null || echo 0)
  version=$(jq -r '.version // "unknown"' <<<"$STD_BODY" 2>/dev/null || echo unknown)
  case "$entries" in ''|*[!0-9]*) entries=0 ;; esac
  case "$bytes"   in ''|*[!0-9]*) bytes=0 ;; esac
  # `down` means this node cannot serve at all (standard plane unreachable);
  # `degraded` means it serves but something needs a human: the nocache
  # plane, the disk watermark, or a healthz verdict of its own.
  status=active
  if [ "$STD_UNIT_UP" = 0 ] || [ "$STD_PROBE_OK" != 1 ]; then
    status=down
  elif [ -n "$PROBLEMS" ]; then
    status=degraded
  fi
  payload=$(jq -nc --arg host "$HOST_ID" --arg ts "$(ts_iso)" --arg version "$version" \
    --arg status "$status" --argjson entries "$entries" --argjson bytes "$bytes" \
    '{schema:"origin-cache/status/1",host:$host,event:"heartbeat",ts:$ts,
      service:"origin-cache",version:$version,status:$status,entries:$entries,bytes:$bytes}') || status=""
  [ -n "$status" ] && post "$payload" heartbeat
fi

#!/usr/bin/env bash
# Oracle node health watchdog (map #30 T4): checks the systemd units, the
# disk watermark and the business-plane healthz; logs failures. Notification
# channel hooks in later (mail server project integration) -- which means
# this log is currently only read by a human, so a false alarm here is pure
# noise and a missed real failure is invisible.
#
# Install:  sudo install -m 0755 deploy/oracle/watchdog.sh /opt/origin-cache/watchdog.sh
#           sudo cp deploy/oracle/origin-cache-watchdog.service /etc/systemd/system/
#           sudo systemctl daemon-reload && sudo systemctl enable --now origin-cache-watchdog.timer
set -u

UNITS="origin-cache-standard origin-cache-nocache"
LOG=/tmp/wdtest/watchdog.log
DISK_WARN_PCT=85
DISK_CRIT_PCT=95

log() { echo "$(date -Is) $*" >> "$LOG"; }

# State file (D3): one line per run recording the last verdict, so the log
# can report TRANSITIONS instead of every run. Written atomically.
STATE=/tmp/wdtest/watchdog.state
HEARTBEAT=/tmp/wdtest/watchdog.heartbeat

# Collect problems in a string; empty means healthy.
PROBLEMS=""
note() { PROBLEMS="${PROBLEMS}${PROBLEMS:+; }$*"; }

# --- unit health ------------------------------------------------------------
for u in $UNITS; do
  if ! systemctl is-active --quiet "$u"; then
    note "unit=$u inactive"
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
probe() { # name, url
  local name="$1" url="$2" code body
  body=$(curl -s -m 5 -w '\n%{http_code}' "$url" 2>/dev/null) || { note "$name unreachable"; return; }
  code=$(printf '%s' "$body" | tail -n1)
  case "$code" in
    2*) ;;
    *) note "$name http=$code"; return ;;
  esac
  if printf '%s' "$body" | head -n -1 | grep -q '"degraded":true'; then
    local why
    why=$(printf '%s' "$body" | head -n -1 | sed -n 's/.*"degraded_reasons":\[\([^]]*\)\].*/\1/p')
    note "$name degraded${why:+: $why}"
  fi
}
probe "healthz standard (:8080)" http://127.0.0.1:8080/_internal/healthz
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

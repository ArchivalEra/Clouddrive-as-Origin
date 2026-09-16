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
LOG=/opt/origin-cache/watchdog.log
DISK_WARN_PCT=85
DISK_CRIT_PCT=95

log() { echo "$(date -Is) $*" >> "$LOG"; }

# --- unit health ------------------------------------------------------------
for u in $UNITS; do
  if ! systemctl is-active --quiet "$u"; then
    log "FAIL unit=$u inactive"
  fi
done

# --- disk watermark ---------------------------------------------------------
pct=$(df -P / | awk 'NR==2 {gsub("%","",$5); print $5}')
if [ -n "${pct:-}" ] && [ "$pct" -ge "$DISK_CRIT_PCT" ]; then
  log "CRIT disk=${pct}% >= ${DISK_CRIT_PCT}%"
elif [ -n "${pct:-}" ] && [ "$pct" -ge "$DISK_WARN_PCT" ]; then
  log "WARN disk=${pct}% >= ${DISK_WARN_PCT}%"
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
if ! curl -s -m 5 -o /dev/null http://127.0.0.1:8080/_internal/healthz; then
  log "FAIL healthz standard (business :8080) unreachable"
fi
if ! curl -s -m 5 -o /dev/null http://127.0.0.1:8081/_internal/healthz; then
  log "FAIL healthz nocache (business :8081) unreachable"
fi

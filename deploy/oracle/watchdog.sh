#!/usr/bin/env bash
# Oracle node health watchdog (map #30 T4): checks the three systemd
# units and the disk watermark; logs failures. Notification channel
# hooks in later (mail server project integration).
#
# Install:  sudo install -m 0755 deploy/oracle/watchdog.sh /opt/origin-cache/watchdog.sh
#           sudo cp deploy/oracle/origin-cache-watchdog.service /etc/systemd/system/
#           sudo systemctl daemon-reload && sudo systemctl enable --now origin-cache-watchdog.timer
set -u

UNITS="origin-cache-standard origin-cache-nocache origin-cache-port80"
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
if ! curl -s -m 5 -o /dev/null http://127.0.0.1:7777/_internal/healthz; then
  log "FAIL healthz standard unreachable"
fi
if ! curl -s -m 5 -o /dev/null http://127.0.0.1:7778/_internal/healthz; then
  log "FAIL healthz nocache unreachable"
fi

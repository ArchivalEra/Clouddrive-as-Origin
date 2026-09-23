#!/usr/bin/env bash
# One-shot installer for the Oracle Clouddrive-as-Origin node.
#   - copies the binary + configs into /opt/origin-cache
#   - writes the 2 systemd units (standard / nocache)
#   - enables and starts them
#
# Usage:  sudo bash deploy/oracle/install.sh <path-to-release-binary> [--keep-env] [--new-token]
#   --keep-env: do not overwrite an existing origin-cache.env (preserves
#               real secrets across reinstalls)
#   --new-token: with --keep-env, generate ORIGIN_TOKEN if the existing env file
#               is missing it (the one value that can be generated; the edge
#               rule must be updated with the same value, see the runbook)
set -euo pipefail

SRC_BIN=""
KEEP_ENV=0
NEW_TOKEN=0
for a in "$@"; do
  case "$a" in
    --keep-env) KEEP_ENV=1 ;;
    --new-token) NEW_TOKEN=1 ;;
    *) [ -z "$SRC_BIN" ] && SRC_BIN="$a" ;;
  esac
done
APP=/opt/origin-cache
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
USER=opc

[ -n "$SRC_BIN" ] && [ -x "$SRC_BIN" ] || { echo "usage: $0 /path/to/origin-cache [--keep-env] [--new-token]"; exit 1; }

mkdir -p "$APP/cache-standard" "$APP/cache-nocache" "$APP/acme-webroot"
install -m 0755 "$SRC_BIN" "$APP/origin-cache"
install -m 0644 "$REPO_DIR/deploy/oracle/config-efficient.toml" "$APP/config-efficient.toml"
install -m 0644 "$REPO_DIR/deploy/oracle/config-nocache.toml"  "$APP/config-nocache.toml"
# The watchdog is the reporting sender AND the ExecStopPost hook below, so
# the units are only valid once this file exists.
install -m 0755 "$REPO_DIR/deploy/oracle/watchdog.sh" "$APP/watchdog.sh"
chown -R "$USER:$USER" "$APP"

# Environment file: contains real secrets at runtime, never committed. The rule
# that it defines every variable the installed config names -- and that a
# missing ORIGIN_TOKEN is a refusal, not a silent gate-off -- lives in
# env-file.sh, so it can be exercised on its own.
ENV_TOOL="$REPO_DIR/deploy/oracle/env-file.sh"
if [ "$KEEP_ENV" = 1 ] && [ -f "$APP/origin-cache.env" ]; then
  if [ "$NEW_TOKEN" = 1 ]; then
    bash "$ENV_TOOL" "$APP/origin-cache.env" "$REPO_DIR/deploy/oracle/config-efficient.toml" --keep --new-token
  else
    bash "$ENV_TOOL" "$APP/origin-cache.env" "$REPO_DIR/deploy/oracle/config-efficient.toml" --keep
  fi
  chown "$USER:$USER" "$APP/origin-cache.env"
  chmod 600 "$APP/origin-cache.env"
else
  bash "$ENV_TOOL" "$APP/origin-cache.env" "$REPO_DIR/deploy/oracle/config-efficient.toml" --fresh
  chown "$USER:$USER" "$APP/origin-cache.env"
fi

cat > /etc/systemd/system/origin-cache-efficient.service <<UNIT
[Unit]
Description=Clouddrive-as-Origin main plane (7777 https)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
Group=$USER
ExecStart=$APP/origin-cache $APP/config-efficient.toml
EnvironmentFile=$APP/origin-cache.env
WorkingDirectory=$APP
CacheDirectory=origin-cache
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=$APP
Restart=always
RestartSec=3
# Pingora waits 300s before shutting down its runtimes (up to 10s more).
TimeoutStopSec=320s
# Report a NON-clean exit to the blog-side worker. Planned stops stay silent
# (systemd marks them SERVICE_RESULT=success), so a deploy is never an
# outage alert; the 15-minute dead-man switch is the real detector.
# See docs/status-reporting.md.
ExecStopPost=$APP/watchdog.sh --down %n

[Install]
WantedBy=multi-user.target
UNIT

cat > /etc/systemd/system/origin-cache-nocache.service <<UNIT
[Unit]
Description=Clouddrive-as-Origin nocache profile (7778 internal)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
Group=$USER
ExecStart=$APP/origin-cache $APP/config-nocache.toml
EnvironmentFile=$APP/origin-cache.env
WorkingDirectory=$APP
CacheDirectory=origin-cache
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=$APP
Restart=always
RestartSec=3
# Pingora waits 300s before shutting down its runtimes (up to 10s more).
TimeoutStopSec=320s
ExecStopPost=$APP/watchdog.sh --down %n

[Install]
WantedBy=multi-user.target
UNIT

cp "$REPO_DIR/deploy/oracle/origin-cache-watchdog.service" /etc/systemd/system/
cp "$REPO_DIR/deploy/oracle/origin-cache-watchdog.timer"   /etc/systemd/system/

# Node-local retention config. Both used to exist only on the node, which
# made a fresh install subtly different from the running one: logs grew
# without a cap and rotation was whatever someone had typed there.
install -m 0644 "$REPO_DIR/deploy/oracle/logrotate-origin-cache" /etc/logrotate.d/origin-cache
install -d -m 0755 /etc/systemd/journald.conf.d
install -m 0644 "$REPO_DIR/deploy/oracle/journald-origin-cache.conf" \
  /etc/systemd/journald.conf.d/origin-cache.conf

systemctl daemon-reload
systemctl restart systemd-journald
systemctl enable --now origin-cache-efficient origin-cache-nocache
systemctl enable --now origin-cache-watchdog.timer
systemctl restart origin-cache-efficient origin-cache-nocache origin-cache-watchdog.timer

# Announce the node as soon as it is serving again. The timer's next tick can
# be 5 minutes away, and the far side reads silence as "maybe gone"; a
# heartbeat seconds after a deploy removes that window. The settle delay is
# so healthz is answering before the probe runs -- probing a service that is
# still binding would report it down.
sleep 3
systemctl start origin-cache-watchdog.service

# The port-80 helper is gone: retired 2026-09-12 after it burned half the
# 2-core node for a week (ThreadingHTTPServer, one thread per connection,
# no timeout, so public scanners never released a thread). Nothing needed
# it -- the certificate renews via DNS-01, and :80 is filtered at the
# cloud layer.
echo "installed: two units enabled+started. Edit $APP/origin-cache.env secrets."

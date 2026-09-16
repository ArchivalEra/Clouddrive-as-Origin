#!/usr/bin/env bash
# One-shot installer for the Oracle Clouddrive-as-Origin node.
#   - copies the binary + configs into /opt/origin-cache
#   - writes the 2 systemd units (standard / nocache)
#   - enables and starts them
#
# Usage:  sudo bash deploy/oracle/install.sh <path-to-release-binary> [--keep-env]
#   --keep-env: do not overwrite an existing origin-cache.env (preserves
#               real secrets across reinstalls).
set -euo pipefail

SRC_BIN="${1:-}"
KEEP_ENV=0
[ "${2:-}" = "--keep-env" ] && KEEP_ENV=1
APP=/opt/origin-cache
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
USER=opc

[ -n "$SRC_BIN" ] && [ -x "$SRC_BIN" ] || { echo "usage: $0 /path/to/origin-cache [--keep-env]"; exit 1; }

mkdir -p "$APP/cache-standard" "$APP/cache-nocache" "$APP/acme-webroot"
install -m 0755 "$SRC_BIN" "$APP/origin-cache"
install -m 0644 "$REPO_DIR/deploy/oracle/config-standard.toml" "$APP/config-standard.toml"
install -m 0644 "$REPO_DIR/deploy/oracle/config-nocache.toml"  "$APP/config-nocache.toml"
# The watchdog is the reporting sender AND the ExecStopPost hook below, so
# the units are only valid once this file exists.
install -m 0755 "$REPO_DIR/deploy/oracle/watchdog.sh" "$APP/watchdog.sh"
chown -R "$USER:$USER" "$APP"

# Environment file: contains real secrets at runtime, written by the
# operator (never committed). Template printed for reference.
if [ "$KEEP_ENV" = 1 ] && [ -f "$APP/origin-cache.env" ]; then
  echo "keeping existing $APP/origin-cache.env"
else
  cat > "$APP/origin-cache.env" <<'ENV'
OPENLIST_USERNAME=REPLACE_ME
OPENLIST_PASSWORD=REPLACE_ME
ORIGIN_PREWARM_SECRET=REPLACE_ME
ORIGIN_TLS_CERT_PATH=/etc/ssl/dib.l.cd/cdn-oracle/cert.pem
ORIGIN_TLS_KEY_PATH=/etc/ssl/dib.l.cd/cdn-oracle/key.pem
ENV
  chown "$USER:$USER" "$APP/origin-cache.env"
  chmod 600 "$APP/origin-cache.env"
fi

cat > /etc/systemd/system/origin-cache-standard.service <<UNIT
[Unit]
Description=Clouddrive-as-Origin standard profile (7777 https)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
Group=$USER
ExecStart=$APP/origin-cache $APP/config-standard.toml
EnvironmentFile=$APP/origin-cache.env
WorkingDirectory=$APP
CacheDirectory=origin-cache
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=$APP
Restart=always
RestartSec=3
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
ExecStopPost=$APP/watchdog.sh --down %n

[Install]
WantedBy=multi-user.target
UNIT

cp "$REPO_DIR/deploy/oracle/origin-cache-watchdog.service" /etc/systemd/system/
cp "$REPO_DIR/deploy/oracle/origin-cache-watchdog.timer"   /etc/systemd/system/

systemctl daemon-reload
systemctl enable --now origin-cache-standard origin-cache-nocache
systemctl enable --now origin-cache-watchdog.timer
systemctl restart origin-cache-standard origin-cache-nocache origin-cache-watchdog.timer

# The port-80 helper is gone: retired 2026-09-12 after it burned half the
# 2-core node for a week (ThreadingHTTPServer, one thread per connection,
# no timeout, so public scanners never released a thread). Nothing needed
# it -- the certificate renews via DNS-01, and :80 is filtered at the
# cloud layer.
echo "installed: two units enabled+started. Edit $APP/origin-cache.env secrets."

# Clouddrive-as-Origin systemd units (deployed shape)

Single binary, two planes (front TLS terminator + axum business plane on
loopback). The oracle node runs **three** units, installed by
`deploy/oracle/install.sh` (which writes them verbatim):

- `origin-cache-standard.service` — https front on `[::]:7777` (TLS)
- `origin-cache-nocache.service` — internal front on `[::]:7778`
- `origin-cache-port80.service` — acme webroot + 301 helper on `:80`

Install: `sudo bash deploy/oracle/install.sh <binary> [--keep-env]`
(`--keep-env` preserves an existing `origin-cache.env` with real secrets).

## Unit (as installed)

```ini
[Unit]
Description=Clouddrive-as-Origin standard profile (7777 https)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=opc
Group=opc
ExecStart=/opt/origin-cache/origin-cache /opt/origin-cache/config-standard.toml
EnvironmentFile=/opt/origin-cache/origin-cache.env
WorkingDirectory=/opt/origin-cache
CacheDirectory=origin-cache
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/opt/origin-cache
Restart=on-failure
RestartSec=3
# Graceful shutdown: SIGTERM is handled by the binary (front + business
# planes drain, then exit).
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Notes vs the earlier template: `DynamicUser=yes` was replaced by a real
`opc` user (the cache dir and redb must survive restarts with a stable
owner); the port80 unit runs as root (privileged port 80).

## Env file (`/etc/origin-cache/origin-cache.env`)

```sh
# TLS material for the front plane (must match the EdgeOne origin Host).
ORIGIN_TLS_CERT_PATH=/etc/ssl/dib.l.cd/apple/cert.pem
ORIGIN_TLS_KEY_PATH=/etc/ssl/dib.l.cd/apple/key.pem
# OpenList web-UI credentials (loopback http is allowed).
OPENLIST_USERNAME=...
OPENLIST_PASSWORD=...
# Optional: inbound SigV4 verification (both set = enabled).
# SIGV4_ACCESS_KEY_ID=...
# SIGV4_SECRET_ACCESS_KEY=...
# Optional: prewarm endpoint shared secret.
# ORIGIN_PREWARM_SECRET=...
```

## Notes

- The binary reads the config path as its first argument; the config
  declares `front_listen` / `listen_addr` / `cache_dir` and the upstream
  list. Secrets stay in the env file, never in the TOML.
- The deployed node uses a real `opc` user (stable cache-dir owner);
  `DynamicUser=yes` remains an option for ephemeral installs.
- EdgeOne origin-pull is HTTPS to `front_listen` (`[::]:7777` on the
  oracle node — dual-stack, required for IPv6 origin-pull); the
  business plane stays on loopback (`127.0.0.1:8080`) and is never
  exposed.
- `EnvironmentFile` values must be plain `KEY=value` lines (no `export`
  prefix, no quotes) — systemd parses them directly.

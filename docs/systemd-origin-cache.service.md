# Clouddrive-as-Origin systemd unit (template)

Single binary, two planes (front TLS terminator + axum business plane on
loopback). This unit runs the binary under systemd with the config path
and env-file references. Copy to `/etc/systemd/system/origin-cache.service`,
fill the placeholders, then:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now origin-cache
sudo systemctl status origin-cache
```

## Unit

```ini
[Unit]
Description=Clouddrive-as-Origin pull-through origin shield
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
# Path to the release binary (build with: cargo build --release)
ExecStart=/usr/local/bin/origin-cache /etc/origin-cache/config.toml
# Env file: every secret/hostname is an env reference, never a literal.
# See config.example.toml for the variable names.
EnvironmentFile=/etc/origin-cache/origin-cache.env
# Cache dir must exist and be writable by the service user.
CacheDirectory=origin-cache
# Run as an unprivileged user (create it first, or use DynamicUser).
DynamicUser=yes
# Hardening
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/origin-cache
Restart=on-failure
RestartSec=3
# Graceful shutdown: SIGTERM is handled by the binary (front + business
# planes drain, then exit).
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

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
- `DynamicUser=yes` gives an ephemeral unprivileged user; if the cache
  dir must persist across restarts with a fixed owner, create a real
  `origin-cache` user and set `User=`/`Group=` instead.
- EdgeOne origin-pull is HTTPS to `front_listen` (default `0.0.0.0:8443`);
  the business plane stays on loopback (`127.0.0.1:8080`) and is never
  exposed.

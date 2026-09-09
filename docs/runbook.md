# Clouddrive-as-Origin Runbook (oracle node)

Operational procedures for the oracle node (`129.146.127.22`, `opc@the-vnic`).
SSH: `ssh oracle-cdn` (2080 proxy + agent). All commands run as `opc` with
`sudo` where noted.

## Topology

- **EdgeOne** → origin-pull `apple.dib.l.cd:7777` (https) / `:80` (http),
  Host header `cdn-oracle.isui.ren`. Edge cert is EdgeOne-managed; origin
  cert is Let's Encrypt `cdn-oracle.isui.ren` (DNS-01 via dnspod).
- **origin-cache** (3 systemd units): standard `[::]:7777` TLS / nocache
  `[::]:7778` / port80 helper `:80` (acme webroot + 301).
- **OpenList** on the same box: `127.0.0.1:5244`, mount `googledrive1`.
- **Watchdog**: `origin-cache-watchdog.timer` every 5 min → logs to
  `/opt/origin-cache/watchdog.log`.

## Traffic switch (EdgeOne → oracle)

1. EdgeOne console: add origin `apple.dib.l.cd` port 7777 (https) / 80
   (http), Host header `cdn-oracle.isui.ren`, origin cert verification ON.
2. Verify origin reachable: `curl -s https://cdn-oracle.isui.ren/_internal/healthz`
   → 200.
3. Switch the site's origin to the new config. EdgeOne propagates in
   seconds.
4. Watch: `sudo journalctl -u origin-cache-standard -f` for origin-pull
   traffic; `curl -sI https://cdn-oracle.isui.ren/<key>` for `age`/`eo-cache-status`.

**Rollback**: EdgeOne console → switch origin back to the previous config.
One click, seconds. No origin-side change needed.

## Failure handling

### Service down (unit inactive)

```sh
systemctl is-active origin-cache-standard origin-cache-nocache origin-cache-port80
sudo journalctl -u origin-cache-standard --no-pager -n 50
sudo systemctl restart origin-cache-standard
```

`Restart=always` self-heals on crash; a manual `systemctl stop` stays
stopped (by design). The watchdog logs failures every 5 min.

### Mysterious 404s after a config change

The redb negative-cache tombstone survives reinstalls: a key that 404'd
once (e.g. wrong upstream id → double-prefixed path) stays tombstoned.
Clear it:

```sh
sudo systemctl stop origin-cache-standard
sudo rm -f /opt/origin-cache/cache-standard/redb.db
sudo systemctl start origin-cache-standard
```

(First request re-stats the upstream; cached files on disk are reused.)

### Certificate expiry / renewal

acme.sh auto-renews (next: 2026-11-08). After renewal the unit must be
restarted to load the new cert:

```sh
sudo systemctl restart origin-cache-standard
```

Verify: `sudo openssl x509 -in /etc/ssl/dib.l.cd/cdn-oracle/cert.pem -noout -dates`.
If renewal failed: `sudo ~/.acme.sh/acme.sh --renew -d cdn-oracle.isui.ren --dns dns_dp`
(needs `DP_Id`/`DP_Key` from `~/dnspod`).

### Disk full

```sh
df -h /   # watchdog warns at 85%, crit at 95%
du -sh /opt/origin-cache/cache-*   # cache dirs
```

Cache is LRU-evicted by `max_size_bytes`; if the disk still fills, lower
`max_size_bytes` in the config and restart. The 3 GiB test file
(`coverage-test-3g.bin`) in googledrive1 can be deleted via WebDAV.

### OpenList down

```sh
systemctl is-active openlist
sudo journalctl -u openlist --no-pager -n 30
sudo systemctl restart openlist
```

origin-cache serves stale-if-error from disk while OpenList is down
(standard profile); nocache profile has no disk fallback (by design).

## Health checks

```sh
curl -s http://127.0.0.1:7777/_internal/healthz   # standard: entries/bytes/flights
curl -s http://127.0.0.1:7778/_internal/healthz   # nocache: entries=0 by design
cat /opt/origin-cache/watchdog.log                # watchdog failures
```

## Test data cleanup

The 3 GiB coverage test file lives in googledrive1 (`coverage-test-3g.bin`)
and its promoted cache entry in `cache-efficient/`. Delete both when done:
WebDAV DELETE + `sudo rm -rf /opt/origin-cache/cache-efficient`.

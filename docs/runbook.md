# Clouddrive-as-Origin Runbook (oracle node)

Operational procedures for the oracle node (`129.146.127.22`, `opc@the-vnic`).
SSH: `ssh oracle-cdn` (2080 proxy + agent). All commands run as `opc` with
`sudo` where noted.

## Topology

- **EdgeOne** → origin-pull `apple.dib.l.cd:7777` (https) only, Host header
  `cdn-oracle.isui.ren`. Edge cert is EdgeOne-managed; origin cert is Let's
  Encrypt `cdn-oracle.isui.ren` (DNS-01 via dnspod). Port 80 is **not** an
  origin path: it is filtered at the cloud layer and nothing listens on it
  (see "Retired: port-80 helper").
- **origin-cache** (2 systemd units): standard `[::]:7777` TLS / nocache
  `[::]:7778`.

  The port-80 helper was retired 2026-09-12 (see "Retired: port-80 helper"
  below). :80 is filtered at the cloud layer and the certificate renews via
  DNS-01, so nothing needed it.
- **OpenList** on the same box: `127.0.0.1:5244`, mount `googledrive1`.
- **cloudflared** (`cloudflared.service`): outbound-only tunnel to the
  Cloudflare Worker that receives this node's status reports.
- **Watchdog** (`origin-cache-watchdog.timer`): every 5 min it logs verdict
  transitions to `/opt/origin-cache/watchdog.log` and sends a status
  heartbeat to the blog worker (`docs/status-reporting.md`). Both service
  units additionally run `/opt/origin-cache/watchdog.sh --down %n` as
  `ExecStopPost`, so a non-clean exit is reported with its cause.

## Traffic switch (EdgeOne → oracle)

1. EdgeOne console: add origin `apple.dib.l.cd` port 7777 (https), Host
   header `cdn-oracle.isui.ren`, origin cert verification ON.
2. Verify the origin is serving. From the node (the public path does not
   expose healthz by design -- see "Health checks"):
   ```sh
   curl -s http://127.0.0.1:8080/_internal/healthz
   ```
   And from your own machine, that the CDN reaches it:
   ```sh
   curl -sI https://cdn-oracle.isui.ren/googledrive1/<known-key> | head -3
   ```
3. Switch the site's origin to the new config. EdgeOne propagates in
   seconds.
4. Watch: `sudo journalctl -u origin-cache-standard -f` for origin-pull
   traffic; `curl -sI https://cdn-oracle.isui.ren/<key>` for `age`/`eo-cache-status`.

**Rollback**: EdgeOne console → switch origin back to the previous config.
One click, seconds. No origin-side change needed.

## Restarting: the stop takes about five minutes

`systemctl stop` (and therefore every deploy, since a deploy is stop-start)
needs roughly **305 seconds** to complete. This is Pingora's graceful drain,
not a hang:

- `run_front` accepts SIGTERM, then sleeps Pingora's 300s grace period
  unconditionally, then spends up to ~10s dropping its runtimes.
- Both units set `TimeoutStopSec=320s` to cover that. The systemd default is
  90s, and with the default every stop was escalated to SIGKILL of every
  thread -- so graceful shutdown never actually ran in production, and a
  deploy looked like a hard crash to the journal.
- A stop that reports `SERVICE_RESULT=timeout` is therefore a **real
  failure** now, not the normal path: it means the process did not come down
  inside a budget that has already been measured to be sufficient.
- The `ExecStopPost` hook reports every non-clean exit. A successful stop is
  silent and leaves `planned stop, no down report` in the watchdog log.
- A crash-looping unit is throttled to one down report a minute, so a loop
  cannot flood the receiver.

Shortening the window means lowering Pingora's grace period, which is a
deliberate trade (draining in-flight transfers versus deploy latency) and has
not been decided.

## Provisioning a fresh node

`deploy/oracle/install.sh <binary> [--keep-env]` does the mechanical part:
configs, both service units with `ExecStopPost`, the watchdog script and its
timer, the logrotate and journald configs. What it cannot do, because these
are node-local and secret-bearing:

1. **`/opt/origin-cache/origin-cache.env`** -- OpenList credentials,
   `ORIGIN_PREWARM_SECRET`, and the TLS cert/key paths. `install.sh` writes
   `REPLACE_ME` placeholders on a fresh node; `--keep-env` preserves the real
   file on reinstall.
2. **TLS material** at the paths that env file names
   (`/etc/ssl/dib.l.cd/<host>/cert.pem` + `key.pem` here).
3. **acme.sh** with a deploy hook, so renewal reinstalls the cert and
   restarts the service (see "Certificate expiry / renewal").
4. **The cloudflared tunnel** (`/etc/cloudflared/token`, `cloudflared.service`)
   -- the status reports ride it, and the tunnel token is the only credential
   they carry.
5. **`jq`**, which the watchdog needs to build its report; without it the
   watchdog logs `report skipped: jq not installed` and stops reporting.
6. **OpenList** on `127.0.0.1:5244` with the mount the config names.

Verify a fresh node with the "Health checks" commands below, then
`sudo -u opc /opt/origin-cache/watchdog.sh` to send one report by hand and
read the answer.

The watchdog's rules are covered by `deploy/oracle/test-watchdog.sh`, which
`cargo test` runs (it stubs `systemctl`/`df`/`curl` and drives the real
script). Run it directly to check a behaviour change before deploying:
`bash deploy/oracle/test-watchdog.sh`.

## Failure handling

### Service down (unit inactive)

```sh
systemctl is-active origin-cache-standard origin-cache-nocache
sudo journalctl -u origin-cache-standard --no-pager -n 50
sudo systemctl restart origin-cache-standard
```

`Restart=always` self-heals on crash; a manual `systemctl stop` stays
stopped (by design). The watchdog runs every 5 min but writes only on a
verdict CHANGE, one `HB` line a day, and any report failure -- so a quiet
log is the healthy case, not a silent watchdog.

### Mysterious 404s after a config change

The redb negative-cache tombstone survives reinstalls: a key that 404'd
once (e.g. wrong upstream id → double-prefixed path) stays tombstoned.
Clear it:

```sh
sudo systemctl stop origin-cache-standard
sudo rm -f /opt/origin-cache/cache-standard/redb.db
sudo systemctl start origin-cache-standard
```

This is a deliberate manual act. The service never reaps its own metadata
store: `redb.db` sits in the cache directory next to the objects, and an
earlier build treated it as a cached object, counted it, and deleted it
after the inactivity TTL -- so the node silently ran on an unlinked database
and lost every row at the next start. A rebuild bug also registered the store
as an entry; those rows are dropped at load, and `redb.db*` is now both
reserved as a key and refused at the write and reap paths.

On restart with no rows present, the service **rebuilds entry rows from the
object tree** (`scan_object_files`) so the cached files on disk are served
and reaped rather than orphaned; each rebuilt row carries no ETag, so the
first access re-stats the upstream and installs real metadata. See ADR-0008.

### Metadata store will not open (boot loop)

If `redb.db` cannot be opened (corrupt in a way redb rejects, or the path is
not a file), the service **does not crash**: it logs an error, moves the bad
file aside as `redb.db.corrupt-<epoch>`, and starts with a fresh store. The
entry rows are then rebuilt from the object tree as above. Inspect
`journalctl -u origin-cache-standard | grep -i metadata` for the quarantine
record; the quarantined file can be deleted once the cause is understood.

### Retired: port-80 helper (2026-09-12)

`origin-cache-port80.service` (a Python `ThreadingHTTPServer` doing ACME
webroot + 301) was **removed from this node and from the installer**. It had
one thread per connection and no timeout, so public scanners holding
connections open kept every thread alive: measured 1021 threads, 1020
established connections, a sustained full core on a 2-core box, and eventual
fd exhaustion that left it not answering at all. It served no purpose —
certificates renew via DNS-01 and `:80` is filtered at the Oracle cloud
layer. If a future need for `:80` appears, install a server with connection
timeouts rather than reviving that script.

### Certificate expiry / renewal

acme.sh auto-renews (next: 2026-11-07). A deploy hook
(`~/.acme.sh/deploy/origin-cache.sh`, registered as `Le_DeployHook` in
the domain conf) installs the new cert to `/etc/ssl/dib.l.cd/cdn-oracle/`
and restarts `origin-cache-standard` automatically — no manual step.

Verify: `sudo openssl x509 -in /etc/ssl/dib.l.cd/cdn-oracle/cert.pem -noout -dates`.
If renewal failed: `sudo ~/.acme.sh/acme.sh --renew -d cdn-oracle.isui.ren --dns dns_dp`
(needs `DP_Id`/`DP_Key` from `~/dnspod`).

### Log rotation

`/etc/logrotate.d/origin-cache` rotates `watchdog.log` (daily, 7 copies,
compressed). journald is capped at 500M by
`/etc/systemd/journald.conf.d/origin-cache.conf`. Both files are **in the
repo** (`deploy/oracle/`) and installed by `install.sh`; they used to live
only on the node, so a fresh install did not match the running one.

### Test artifacts (kept for regression)

- `coverage-test-3g.bin` in googledrive1: 3 GiB coverage test file. Delete
  via WebDAV when no longer needed. Only the upstream object is durable
  state -- any `/tmp` copy is gone at the next reboot. The efficient test
  instance that used to promote it is gone (see "Test data cleanup").

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

The health endpoints live on the **business plane's loopback ports** (plain
HTTP). They are NOT reachable on the front TLS ports: the front refuses
`/_internal/*` (ADR-0009), and probing `http://127.0.0.1:7777` speaks
plaintext to a TLS listener, which hangs up with no response.

```sh
curl -s http://127.0.0.1:8080/_internal/healthz   # standard
curl -s http://127.0.0.1:8081/_internal/healthz   # nocache (entries=0 by design)
cat /opt/origin-cache/watchdog.log                # verdict transitions, daily HB, report failures
```

`healthz` answers `200` whenever the process is serving, and reports a
health **verdict** in the body: `"degraded": true` plus
`"degraded_reasons": [...]`. Read the reasons, not just the status code:

```sh
curl -s http://127.0.0.1:8080/_internal/healthz | python3 -m json.tool
```

The watchdog log is quiet by design: it writes a line only when the verdict
CHANGES, plus one `HB` line per day. A long silence means "still healthy";
a missing `HB` for more than a day means **the watchdog itself stopped**.

The same watchdog also sends a status heartbeat to the blog worker every 5
minutes; the full interface is in `docs/status-reporting.md`.

## The blog card says the node is offline

The card is fed by the watchdog's heartbeat, and the far side calls the node
offline after 15 minutes of silence. Silence has two possible causes and the
node being healthy is consistent with the second one, so check both:

```sh
systemctl is-active origin-cache-watchdog.timer cloudflared   # the senders
tail -5 /opt/origin-cache/watchdog.log                        # report failures
sudo -u opc /opt/origin-cache/watchdog.sh                     # send one now
```

A `report heartbeat failed http=NNN` line localises the problem to the
receiver (a 4xx means the far side rejected the payload; a 000 means the
request never left). No line at all, with the timer active, means the run
never happened — check `systemctl list-timers origin-cache-watchdog.timer`.
`report skipped: jq not installed` means the host lost `jq`, which the
reporting path needs.

## Service is up but requests are failing

Symptom: the units are active and healthz answers, but clients see errors.
`healthz` reports the cache's view; the request path's view is in the
front's metrics and access log.

```sh
# Error rate by status (5xx is the signal; 4xx is usually client-side)
curl -s http://127.0.0.1:9090/metrics | grep 'front_requests_total{.*status="5"'

# Recent failures, with the error string the front recorded
sudo journalctl -u origin-cache-standard --no-pager -n 100 | grep -i 'front access' | grep -v 'status="2'

# Upstream health: 401/403 auth, 429 throttling, 5xx provider errors
curl -s http://127.0.0.1:9090/metrics | grep 'backend_call_duration_seconds_count'
```

Common causes: OpenList credentials expired (`AuthRequired` ⇒ check
`healthz` upstream entries), the provider throttling us (`429` ⇒ the cache
backs off per ADR-0010 and serves stale where it can), or disk pressure
(see "Disk full" above).

## Requests are slow

Symptom: no errors, but latency is high. Split the latency by segment
before changing anything — the bottleneck has historically been the CDN
edge, not this node.

```sh
# Attribution: front TTFB vs cache serve vs upstream call, with percentiles
deploy/metrics-report.sh http://127.0.0.1:9090/metrics 30
```

Read the verdict line it prints: a large `front_upstream_ttfb` with a small
`cache_serve` means the time is going to OpenList/the provider; a large
`cache_serve` with a small `backend_call` means it is going to this node.

```sh
# Is it the node or the link? Local disk throughput, no network involved
sudo dd if=/opt/origin-cache/cache-standard/<key> of=/dev/null bs=1M count=200
```

If local reads are fast and `metrics-report` blames the upstream segment,
the problem is OpenList or the provider — not this service. If the whole
path is fast from the node but slow from a client, the time is in the edge
segment (see `docs/notes/` for the EdgeOne findings).

## Test data cleanup

The 3 GiB coverage test file lives in googledrive1 (`coverage-test-3g.bin`).
The retired efficient test instance used to promote it into
`cache-efficient/`; that instance is gone (ADR-0009), so only the upstream
object needs deleting. Use the repo's lab suite (`deploy/lab/run-lab.sh`
with `deploy/lab/config-c.toml`) if the efficient profile needs re-testing.

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

## Restarting: seconds when idle, about five minutes when busy

`systemctl stop` (and therefore every deploy, since a deploy is stop-start)
takes **about 2 seconds when nothing is in flight** and **about 305 seconds
when something is**. Both were measured on the node; ADR-0011 records the
decision and the measurements.

- Pingora stops the listener the instant SIGTERM arrives and then sleeps its
  300s grace period unconditionally, with no early exit for an idle server.
  The adaptive path in `src/shutdown.rs` samples the front's in-flight
  connection count across a 3s settle window: never leaves zero → the process
  exits immediately (`exiting without the drain window` in the journal);
  anything in flight → the full drain runs (`connections in flight; draining
  gracefully`).
- Both units set `TimeoutStopSec=320s` to cover the drain. The systemd default
  is 90s, and with the default every stop was escalated to SIGKILL of every
  thread — graceful shutdown never actually ran, and a deploy looked like a
  hard crash to the journal.
- A stop that reports `SERVICE_RESULT=timeout` is therefore a **real
  failure**: the budget has been measured to be sufficient, so exceeding it
  means something is wrong rather than that the stop was ordinary.
- **A busy stop truncates the tail of what is in flight**, by roughly the
  data still in the network when the session closes — measured as ~4 MiB of a
  100 MiB transfer to a client throttled at 1 MB/s, while the same transfer
  with no restart completed. The drain still extends service a long way (that
  client reached 98s of a 100s transfer), and it is not a matter of the grace
  period being short: the front had finished writing the body and the session
  had ended. A client with a retry resumes. Deploying while a large transfer
  is running costs that transfer its tail, so prefer a quiet moment.
- The `ExecStopPost` hook reports every non-clean exit. A successful stop is
  silent and leaves `planned stop, no down report` in the watchdog log.
- A crash-looping unit is throttled to one down report a minute, so a loop
  cannot flood the receiver.


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

## Node acceptance after a deploy

`deploy/oracle/accept.sh` is the post-deploy gate: it polls readiness, then
asserts the serving contract (GET 200 / range 206 with a content-range /
HEAD), key handling (reserved names 400, nested look-alikes 404), the
healthz surfaces (`stray_bytes` present, verdict `ok`), the three body
metrics on the front plane's `/metrics`, the watchdog report endpoint, and
that every unit is active with `systemctl --failed` empty.

```sh
scp deploy/oracle/accept.sh <node>:/home/opc/ && ssh <node> 'bash accept.sh'
```

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

### Reading the shaping account (efficient profile)

The efficient profile's whole claim is that a scrub costs few upstream
requests. Two numbers answer it, both from the front's metrics port:

```sh
# Upstream byte-stream opens (the ~640 ms fixed cost each, measured)
curl -s http://127.0.0.1:9090/metrics | grep 'backend_call_duration_seconds_count{op="open"'

# Who served the bytes: disk entry, staged span, or upstream
curl -s http://127.0.0.1:9090/metrics | grep 'cache_serve_source_total'
```

Divide delivered bytes by the `open` delta over the same window: a scrub that
re-reads staged bytes moves `source="stage"` while `op="open"` stays put, and
an `open` per request means the read missed the ledger (the sidecar was
evicted or its interval decayed) and went upstream. `segment_bytes` and
`coverage_intervals` in healthz say how much the window currently holds.

A scrub's reads ride **runs** (ADR-0016): the first ranged miss on a key opens
one upstream stream covering `session_window_bytes` (default 64 MiB, about a
second at the measured 63 MB/s) and every request inside that window is served
from its watermark. So the account above reads differently than it used to:

```sh
# Runs by outcome (sealed / failed / chained) and requests by how they were served
curl -s http://127.0.0.1:9090/metrics | grep 'cache_session_total'
curl -s http://127.0.0.1:9090/metrics | grep 'cache_session_reader_total'
```

The node-side acceptance for this mechanism is
`deploy/oracle/efficient-walk.sh <object> [shards]`, run against a
loopback-only efficient instance (its own cache dir and ports, the same
OpenList upstream, a magazine larger than the walked object — admission
refuses to stage an object the magazine cannot hold). It walks ascending 1 MiB
shards and prints the account: on the 3 GiB test object, 512 shards cost 12
opens (499 of the 512 requests rode a run), 128 unvisited shards cost 2, a
sealed shard re-read costs 0, and a shard compares byte-identical to the
provider's own bytes.

`result="attached"` counts requests that cost no upstream open and no stream
permit; `result="standalone"` counts the escape (a far seek, a run already
starting, or no admission). `op="open"` divided by the walk's byte length is
the shaping account: one open per window is the target, one per request is the
old behaviour. For a viewer scrubbing forward continuously, expect roughly
`bytes / session_window_bytes` opens; for random seeking, expect one per seek
no matter how large the window is.

A key that is **being read** is not evicted at all (ADR-0017): the body holds a
read lease for its whole life (a viewer who disconnects drops it), and policy
eviction keeps skipping the key for `read_grace_secs` (default 300) after the
last body ends, so a pause between two requests of one session does not cost a
re-fetch. Only disk pressure outranks a lease — it may reclaim a resident stray
mid-stream, because the disk is the last resort and the stream in flight
survives the unlink. Set `read_grace_secs = 0` to keep live bodies protected
but drop the grace.

A key being **watched** is protected around its viewer's position (ADR-0018).
A viewing session lasts hours while its bodies last milliseconds, so the lease
alone is the wrong unit: a key that has been requested stays watched for
`watch_idle_secs` (default 900) after its last body, and what a trim must leave
alone is `watch_pin_bytes` (default 128 MiB) around the end of the most recent
response — not the whole key. Read the two gauges together:
`cache_watch_active` is how many sessions the cache is holding a neighbourhood
for and `cache_watch_pinned_bytes` is what that costs the budget; pinning that
grows while the active count falls is how a budget goes soft.
`cache_watch_resume_total{outcome}` counts responses that arrived with no body
on the key — an EdgeOne shard walk contributes one per shard, so read it as how
often the watch (rather than a live body) was what answered, and `miss` on it
means that path still paid an upstream open.

A pin is a preference rather than an exemption: the trim takes bytes outside
every pin first, and spends a pin only when the rest of the cache cannot cover
the need — from its BACK, because the span the viewer has already watched is
worth less than the one it is about to need. `watch_pin_bytes = 0` switches
pinning off (ADR-0017's coarser rule returns: a key being read keeps all its
spans), and `watch_idle_secs = 0` switches watching off (only live bodies
protect). Both switches are what the reverse verification of this round used,
and the LAB's two efficient accounts are built on them: config-d runs with both
off so its numbers measure the policy alone, config-e with both on so it can
measure what they cost.

Two knobs govern what leaves the window (ADR-0015), both span-level:

```toml
eviction_policy = "lru"   # default: the stalest span in the least-recently-
                          # touched key goes first (a plain sliding window)
# eviction_policy = "heat"  # the coldest span by READ COUNT goes first,
                            # compared inside the trailing 20 spans
```

`heat` is the one to try when viewers scrub BACK to a region they already
watched: it keeps a re-read span alive even when the clock says it is the
oldest, which `lru` would evict first. Both policies trim single `.seg`
files, never a key's whole window, and neither touches a row younger than
60 s. A tick's decisions are logged, one line per eviction:
`evicted staged spans to stay inside the magazine keys=.. bytes=.. policy=..`.

### Is seeking smooth? (added 2026-09-19)

`cache_serve_duration_seconds` stops at response construction, so it cannot
answer this. Three newer metrics can, on the front plane's `/metrics`:

```sh
curl -s http://127.0.0.1:9090/metrics | grep -E 'cache_serve_source_total|cache_body_ttfb_seconds_count|cache_body_bytes_total'
```

- `cache_serve_source_total{source}` — how many responses were served from
  this node's disk versus pulled from upstream. If almost everything is
  `upstream`, the local cache is not buying anything for that workload.
- `cache_body_ttfb_seconds{source}` — request entry to the first body byte,
  split by source. Compare the two histograms: the gap between
  `source="disk"` and `source="upstream"` is the per-open upstream cost
  (~640 ms measured) that a local read avoids.
- `cache_body_bytes_total{source}` — bandwidth actually spent upstream.

A seek that lands on already-cached bytes shows up as a `disk` sample with a
much smaller TTFB. If seeks are slow *and* mostly `upstream`, the object is
not being kept — check `stray_bytes` and `bytes` in healthz (below) before
reaching for a config change.

### resident strays: an object larger than the magazine

`max_size_bytes` is a policy bound, not a disk bound. An object larger than it
cannot be brought into budget by evicting anyone, so since ADR-0014 the node
keeps it as a **resident stray**: cached while the disk allows, outside the
byte budget, reclaimed by the 20-minute inactivity clock or when free space
falls below reserve + 2 GiB.

```sh
# How much of the cache the byte budget does not govern
curl -s http://127.0.0.1:8080/_internal/healthz | jq '{bytes, stray_bytes, segment_bytes, entries}'
```

Two operational consequences:

- `bytes` can legitimately exceed `max_size_bytes` by `stray_bytes`. That is
  not a bug, and the watchdog's disk watermarks (85% / 95%) still cover the
  disk itself.
- `inactive_ttl_secs` now decides how long a large object survives between
  viewing sessions. Re-pulling one costs a full upstream stream (~18 minutes
  for 30 GB at the measured 27.4 MB/s), so raise the TTL if viewers come back
  after long gaps.

An efficient-profile object larger than the magazine is never staged at all
(ADR-0013): the magazine cannot hold it, so the spans it would write could
never be evicted on terms the budget understands. It is served as a stray when
the disk can hold it (ADR-0014), and through the pipe otherwise. Note the
staged-byte eviction knob in the cache section: `eviction_policy = "lru"`
(default) or `"heat"` (ADR-0015).

## Test data cleanup

The 3 GiB coverage test file lives in googledrive1 (`coverage-test-3g.bin`).
The retired efficient test instance used to promote it into
`cache-efficient/`; that instance is gone (ADR-0009), so only the upstream
object needs deleting. Use the repo's lab suite (`deploy/lab/run-lab.sh`
with `deploy/lab/config-c.toml`) if the efficient profile needs re-testing.

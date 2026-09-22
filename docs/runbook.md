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
- **origin-cache** (2 systemd units): `origin-cache-efficient` `[::]:7777` TLS
  (the name is historical — it runs the default, `efficient`, profile) /
  `origin-cache-nocache` `[::]:7778`.

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
4. Watch: `sudo journalctl -u origin-cache-efficient -f` for origin-pull
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


## Would rate limiting be safe to enable? (2026-09-21)

`front_rate_rps` is 0 in production today (off). Enabling it has two halves, and
only one of them is a test:

```sh
# ON the node: the half that can be measured without touching production.
bash deploy/oracle/rate-limit-probe.sh
```

It starts a loopback-only instance of its own (own cache dir and ports, the
production binary, the same OpenList) with `front_rate_rps = 2`, probes it, and
removes it. Measured 2026-09-21: the third request in a second is refused
(`200 200 429`), the ceiling recovers after a second, five concurrent requests are
counted rather than raced past (three refused), and the business plane is not
rate limited — the gate is the front's own, on the front's port.

The other half is a decision, because a 429 arrives at the EDGE: whether EdgeOne
retries it (and how hard) is its contract, not ours to discover unattended.
Enabling it is one line in the production config plus a restart; watch
`front_requests_total{status="429"}` on 9090 and the origin's access log for a
retry pattern (a rising request rate on unchanged keys), and roll back by
removing the same line. The probe prints this procedure too.

## Looking at ONE key (2026-09-21)

`curl -s 'http://127.0.0.1:8080/_internal/healthz?key=googledrive1%2Fround3.mp4' | jq .key`
answers the questions an investigation starts with, without a debugger: is the
object installed here, which spans are staged on disk (`staged_spans`,
`staged_bytes`), what does the ledger believe (`ledger_spans` with each span's
last read and read count, `ledger_total`, `ledger_etag`, the row's age), where is
the viewer pinned (`pin`), and is a body holding the key (`leased`). The key is
percent-decoded, so a path with `/` goes in as `%2F`. Without `?key=` the body is
what it always was.

## Retiring the old unit name (standard -> efficient, 2026-09-21)

The main plane's unit and config used to be named after the `standard` profile,
which ADR-0022 retired. On a node still running the old names:

```sh
# 1. Put the new files in place (install.sh does this too).
sudo install -m 0644 deploy/oracle/config-efficient.toml /opt/origin-cache/
sudo cp deploy/oracle/origin-cache-efficient.service /etc/systemd/system/   # see install.sh
# 2. Swap the units. The port does not change, so EdgeOne keeps pointing at it,
#    but NOTHING is listening on 7777 for the seconds between the two commands.
sudo systemctl daemon-reload
sudo systemctl disable --now origin-cache-standard
sudo systemctl enable  --now origin-cache-efficient
sudo systemctl is-active origin-cache-efficient origin-cache-nocache
curl -s http://127.0.0.1:8080/_internal/healthz | jq -r .upstreams[0].profile   # efficient
```

Two things NOT to touch:

- **`cache_dir`** (`/opt/origin-cache/cache-standard`) is not renamed with the
  unit. It holds the live metadata database and every staged sidecar; renaming it
  orphans both and starts the node on an empty cache.
- **The `service` field the watchdog reports** changes with the unit, because
  `ExecStopPost` passes `%n`. The dead-man switch is silence-based (15 minutes)
  and heartbeats report a different spelling (`origin-cache`), so the receiver
  does not match on the unit name; a down event's label changes and nothing else.

Keep the old unit file on disk until the new one is verified serving, so a
rollback is one `enable --now` away.

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
systemctl is-active origin-cache-efficient origin-cache-nocache
sudo journalctl -u origin-cache-efficient --no-pager -n 50
sudo systemctl restart origin-cache-efficient
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
sudo systemctl stop origin-cache-efficient
sudo rm -f /opt/origin-cache/cache-standard/redb.db
sudo systemctl start origin-cache-efficient
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
`journalctl -u origin-cache-efficient | grep -i metadata` for the quarantine
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
and restarts `origin-cache-efficient` automatically — no manual step.

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

origin-cache serves stale-if-error from disk while OpenList is down (any cached
profile — for an object it can hold, a durable entry is served regardless of
upstream health); nocache has no disk fallback (by design).

## Building for the node (aarch64)

The node is aarch64 on Oracle Linux 9.8 (glibc 2.34). Two ways to produce its
binary, and one that does not work:

- **On the node** — always works, and the fallback: ship the source tree and
  build there. Measured 1m47s; it spends the node's own CPU, which is production.
- **On the compile machine, cross, STATIC MUSL** — fast and free of the node's
  CPU: `deploy/oracle/build-aarch64.sh` drives a static cross toolchain unpacked
  at `/mnt/hdd/crossbuild-tools/aarch64-linux-musl-cross` (the HDD: read-mostly,
  no IO pressure), with the build's target dir on the NVMe home
  (`~/cds-musl-target`), which is where the IO is. Measured 1m11s, and the result
  is `statically linked, ARM aarch64`.
- **NOT the compile machine's plain cross-gcc.** Its `aarch64-linux-gnu-gcc`
  links against Debian trixie's glibc **2.43**, so the binary asks for
  `GLIBC_2.38` and dies on the node with `version 'GLIBC_2.38' not found`
  (measured 2026-09-22). A static toolchain removes the question instead of
  answering it.

Check every build before installing it — a wrong-architecture binary takes the
service down (`Exec format error` at `EXEC`, both units stuck in `activating`;
measured 2026-09-22, when a local x86_64 binary was copied to the aarch64 node
and `install.sh` was run against it):

```sh
scp <binary> <node>:/home/opc/origin-cache.new
ssh <node> 'file /home/opc/origin-cache.new | head -1'      # aarch64; static when musl
ssh <node> '/home/opc/origin-cache.new /nonexistent.toml'   # "Error: load config" = it runs
ssh <node> 'sudo -n cp -a /opt/origin-cache/origin-cache /home/opc/origin-cache.prev \
            && sudo -n install -o opc -g opc -m 0755 /home/opc/origin-cache.new /opt/origin-cache/origin-cache \
            && sudo -n systemctl restart origin-cache-efficient origin-cache-nocache'
ssh <node> 'bash /home/opc/repo/deploy/oracle/accept.sh'     # expect VERDICT=PASS
```

`install.sh` is for changing configs or units; a binary-only update uses
`install -o opc -g opc -m 0755` as above.

A musl binary is a different libc, so it is accepted by the gate rather than
trusted for being newer. The 2026-09-22 musl deploy passed `accept.sh` (PASS, all
four units active) *and* served a 4 MiB ranged read of the 200 GiB film from a
fresh offset through the CDN (206, TTFB 1.36 s) — which exercises the upstream
path, stat plus open plus stream through Google Drive via OpenList, under musl's
DNS and TLS resolution.

## The neighbouring H1.1 mux, carried against this origin (spike, 2026-09-22)

A sibling repo (`cloudflare-related`, branch `mux-final-20260922`) ships an H1.1
stream multiplexer: N logical streams inside one H1.1 exchange — uplink framed in
the request body (`POST /up?sid=N`), downlink in the response body
(`GET /down?sid=N`), LEB128 frames with policy-chosen random padding, per-stream
credit flow control and deficit-round-robin scheduling. It does not know what it
carries (S3, HTTP, arbitrary bytes), and it needs no handshake, no ALPN and no new
endpoint: it is private to an already-established connection.

**Can it carry THIS origin's traffic?** Its `mux-twohost` boots a front whose job
is to bridge every mux stream to `ORIGIN_ADDR` as opaque bytes, and its gates ask
for `/corpus/<n>` — paths this origin does not have.
`deploy/lab/mux-origin-shim.py` answers them from this origin instead: it rewrites
the request line to the real key, adds a `Range` for the byte count the path names,
and splices the exchange, over an `ssh -L` tunnel to the node (the only route to
the origin that does not pass through the CDN). The gates assert byte counts and
timing, not content, so the bytes really are this origin's.

Result: **ALL GATES GREEN** — the 5-step WAN sequence, 10 MiB bulk, the
small-response timing gate, and carrier rotation (the old carrier retires loudly,
a fresh one serves) — with `bulk 10m: OK 10486282 bytes in 3.52s (2.84 MB/s)`.

Read that number with its handicap: the harness runs the LIBRARY default policy
(`stream_window = 256 KiB`), which the mux's own README says caps a single stream
at window/RTT. The same leg, same payload, plain HTTP through the same tunnel
measured **3.5 MB/s on one connection and 8.8 MB/s with four in parallel** (40 MiB
in 4.57 s). So on this leg what wins is several connections — the shape this
project's clients already use (5 MB shards) — and the leg is the cap: 0.6-1.3 MB/s
per connection through the CDN, 2-3.4 MB/s with a few.

**Through EdgeOne the carrier's fate follows from its wire shape.** The `POST /up`
half is uncacheable and its body is relayed, so it survives as an opaque body
(subject to the edge's request-body and time limits). The `GET /down` half is an
endless streaming response: an edge whose business is buffering dynamic content is
the wrong place for it, and this origin must never answer such a path with the
`cache-control: public, immutable` its cache-hit path sets, or the edge would try
to cache a body that never ends. Neither half can be *terminated* at the edge —
EdgeOne does not speak a private framing — so the mux can only help on a hop we
control, and the hop it is designed for (client to origin) is exactly the one a
CDN breaks into two.

Verdict: a clean, well-instrumented multiplexer (its own doctor passes at
212 MiB/s aggregate on loopback, with backpressure and rotation exercised), and
the wrong instrument for this deployment's bottleneck — every limit measured on
2026-09-22 sits in the link, not in the framing or the number of logical streams.

**With the window their deployment uses, and several streams (measured later the
same day).** A temporary spike (`/tmp/mux-spike`, path-dependent on their crates,
policy from env) replaces the harness's library default: same front, same shim,
same ssh tunnel to this origin, 10 MiB per stream, padding off.

| streams on ONE carrier | window 4 MiB | window 32 MiB | plain HTTP, N connections |
| --- | --- | --- | --- |
| 1 | 3.15 MB/s | - | 3.5 MB/s |
| 4 | 7.54 MB/s | - | 8.8 MB/s |
| 8 | **12.50 MB/s** | 9.59 MB/s | 9.87 MB/s |
| 16 | 13.18 MB/s | 11.80 MB/s | **15.22 MB/s** |

Every run: zero errors, zero stream timeouts — their flow control held sixteen
10 MiB streams over a ~250 ms leg. Read it as: both shapes plateau at the leg
(13-15 MB/s), the mux is *not* faster, and it is better at 8 than eight separate
connections (12.5 vs 9.9); its real property is delivering that aggregate over
ONE client-facing carrier (two TCP connections, POST + GET) instead of N. Their
production window (4 MiB) beats 32 MiB here, which matches their own sizing note
that the window has to cover the BDP and no more.

So for this project the mux is a *connection-count* tool, not a speed tool: it
would matter to a client that cannot open many connections (a browser has six per
host) and not to one that can (our shard clients do). And through EdgeOne it
cannot be terminated at the edge at all, so the hop it would help is the one a CDN
splits in two.

## Standard H2 already does what the private mux was for (measured 2026-09-22)

The mux spike's lesson was "connection count, not speed", so the same question was
put to plain HTTP/2 through EdgeOne: one connection carrying N concurrent Range
requests (standard, cacheable 206s) versus N separate connections. Instrument:
`deploy/lab/probe-h2-vs-conns.sh` (N x 5 MiB, fresh offsets, three repetitions,
`ss` counts curl's own sockets).

| shape | result |
| --- | --- |
| 4 separate connections | 4/4, 4/4, 3/4 completed — 7.9 s, 8.6 s and 16.2 s (one connection lost to the path's stall; an earlier run of this shape hit 90 s) |
| ONE connection, 4 streams | 3/3 completed, 4.5-6.2 s, `curl sockets=1` — 3.1, 3.4, 4.2 MB/s |
| ONE connection, N streams | N=4: 3.6 MB/s, N=8: 6.2, N=16: 5.7 (N=1 and N=2 timed out at 60 s: with one connection there is nothing to fall back on when the path stalls) |
| one connection, 4 streams, edge-warm offsets | 4.95 and **17.36 MB/s** in two consecutive runs — the edge's cache state moves this more than any knob |

So: multiplexing over one connection is *stable* (no per-connection stall to lose)
and its aggregate rises with the stream count, plateauing around 6 MB/s here
against ~9.7 MB/s for four connections — a connection-level ceiling, which is what
a flow-control window governs. This curl (8.21.0, nghttp2) has no window knobs, so
the ceiling could not be moved from here; hyper's client exposes
`initial_stream_window_size` / `initial_connection_window_size`, which is the
knob to test next.

Who can turn that knob is the part that decides whether it is usable:

- **client <-> edge**: the client owns its receive window, so this is tunable — in
  clients WE ship (a Rust/hyper reader, the LAB harness). Browsers do not expose
  it.
- **edge <-> origin**: the EDGE owns that window (our front log shows it speaking
  h2 to us); nothing on our side changes it, which is the same wall the upstream
  concurrency experiment hit from the other side.

So the usable form of "an H2 flow-control design" is not a new private protocol
(the private one cannot be terminated at the edge, and H2 is already the transport
on both hops) but: **one H2 connection, N concurrent Range streams, with the
connection and stream windows sized to the BDP of the client's leg** — standard,
cacheable, and deliverable in our own client code.

## The edge's control plane, from here (tccli, 2026-09-22)

The remaining origin-leg levers are EdgeOne settings rather than code, so the
console was the only door to them. It is not the only one now: `tccli` reaches the
same API. It is used directly, with no local wrapper -- a shell wrapper around a
cloud vendor's own CLI is a maintenance liability that buys nothing, and the two
things it would "encapsulate" are one flag and one `unset`.

```sh
uv tool install tccli        # tccli 3.1.172.1, the version tested here
```

Credentials come from the environment: `TENCENTCLOUD_SECRET_ID`,
`TENCENTCLOUD_SECRET_KEY`, plus `TENCENTCLOUD_TOKEN` for a temporary key. Never in
a file, never in the repository.

```sh
export TENCENTCLOUD_SECRET_ID=...    # environment, not a file in the repo
export TENCENTCLOUD_SECRET_KEY=...
```

### Two traps, both already paid for

**The endpoint.** EdgeOne International is served by
`teo.intl.tencentcloudapi.com`, and tccli 3.x has no international routing: it
defaults to the domestic endpoint, where an international key answers
`AuthFailure.SecretIdNotFound`. That is an error about the key for a problem that
is the endpoint, and it sends you off to regenerate a key that was fine. Pass
`--endpoint teo.intl.tencentcloudapi.com` on every call (or set `EO_ENDPOINT` in
your shell and paste it). Checked with a black-hole proxy: point `HTTP_PROXY` at a
closed port and a call still returns a real `requestId` only when the proxy
variables are unset; with them set, the same call dies trying to reach the proxy.

**The proxy.** This workstation exports `HTTP(S)_PROXY=http://127.0.0.1:2080`
globally, and nothing here may travel through it. Unset the proxy variables in the
shell that makes the call:

```sh
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy
tccli teo DescribeZones --Offset 0 --Limit 100 \
  --endpoint teo.intl.tencentcloudapi.com --region ap-hongkong
```

**The parameter names.** teo's CLI parameters are capitalised (`--Offset`, not
`--offset`), and an unknown option gets a bare usage message rather than an error
naming it.

### Where each knob lives

| knob | layer | API | request field |
| --- | --- | --- | --- |
| HTTP/2 to the origin | site-wide | `ModifyL7AccSetting` | `ZoneConfig.UpstreamHTTP2.Switch` |
| sharded origin pull | rule only | `ModifyL7AccRule` | `Rule.Branches[0].Actions[].RangeOriginPullParameters.Switch` |
| origin-read timeout, 5-600 s | rule only | `ModifyL7AccRule` | `...HTTPUpstreamTimeoutParameters.ResponseTimeout` |
| origin address / protocol / ports | per domain | `ModifyAccelerationDomain` | `OriginInfo`, `OriginProtocol`, `Http(s)OriginPort` |

Note the asymmetry that makes a round trip dangerous: the read returns
`ZoneSetting.UpstreamHttp2` while the write takes `ZoneConfig.UpstreamHTTP2`.
Copying a read response straight back into a write request drops settings
silently, so write only the block you are changing, then re-read and diff the
whole setting object.

### Calling it

```sh
# the site-wide setting, and the rules (RangeOriginPull lives only in the rules)
tccli teo DescribeL7AccSetting --ZoneId <zone> --endpoint ... --region ...
tccli teo DescribeL7AccRules   --ZoneId <zone> --Limit 200 --endpoint ... --region ...
# which domains, and where each one pulls from
tccli teo DescribeAccelerationDomains --ZoneId <zone> --Offset 0 --Limit 200 --endpoint ... --region ...

# HTTP/2 to the origin: send only this block
tccli teo ModifyL7AccSetting --ZoneId <zone> \
  --ZoneConfig '{"UpstreamHTTP2":{"Switch":"on"}}' --endpoint ... --region ...

# sharded origin pull, on an existing rule (ModifyL7AccRule wants the whole Rule object)
tccli teo ModifyL7AccRule --ZoneId <zone> --Rule '{"RuleId":"...","Status":"enable",...}' --endpoint ... --region ...
```

`RangeOriginPull` cannot be set site-wide -- it is absent from the site-wide
request schema entirely and exists only as a rule action, so it needs a rule to
hang on and a rule carries a match condition. That is an operator's decision; if
the zone has no rule to put it on, that is a finding to report rather than
something to invent.

Dumps of the raw responses carry the origin address, so keep them out of the
repository (`/tmp/eo/` is fine; the `.gitignore` is a whitelist, but a file named
by hand into `deploy/` would still be tracked).

### What the origin-pull settings actually are (read 2026-09-22)

Read with the read-only key, one call per layer. The zone is `isui.ren`
(`zone-3taqnjqfr1zo`), area `overseas`, type `partial`, `ActiveStatus: active`
(the `Status: pending` field is the zone *type*'s provisioning state and does not
affect serving), and the plan is **`plan-free`** -- which decides what is
available at all.

| layer | what is set |
| --- | --- |
| site-wide | `UpstreamHTTP2` **on**, `HTTP2` (client) on, `SmartRouting` **off**, `AccelerateMainland` off, `OfflineCache` on, `CachePrefresh` on at 90% of TTL, `Cache` follow-origin, `MaxAge` 600 s, `PostMaxSize` 800 MiB |
| rules (exactly one) | `rule-3usngannhvqa`, enabled, priority 1, condition `${http.request.uri.path} in ['/*'] and ${http.request.host} in ['cdn-oracle.isui.ren']`, action `RangeOriginPull` **on** -- and nothing else set on it |
| domain `cdn-oracle.isui.ren` | `online`, `OriginProtocol: FOLLOW`, HTTP port 80 / HTTPS port **7777**, `HostHeader: cdn-oracle.isui.ren`, free certificate |

So **both knobs the origin-leg discussion named are already on**: sharded origin
pull (as a rule action, scoped to that host) and HTTP/2 to the origin. Lever B is
therefore not "switch sharding on"; what is left in it is:

- **the origin-read timeout**, which is the one action in the list that is *not*
  set, so the platform default applies. The API exposes only the settable range
  (5-600 s) and does not report the default; the doc page that would name it could
  not be fetched from here. This is the only remaining knob that matches the
  measured failure shape, since a product that answers an open-ended range with a
  200 GiB promise holds a body open for a long time -- worth one experiment.
- **the plan tier.** `SmartRouting` (smart acceleration) is off and
  `AdvancedOriginRouting` is unset on both layers; the latter is documented as
  requiring the former, and neither is a free-plan feature. The geographic
  adaptation knob that would suit a domestic client talking to a Singapore POP
  with a Hong Kong origin-pull is therefore **not reachable from this account**
  without a plan change -- which is a decision, not a setting.

The origin itself resolves straight to the node's own address (an Oracle Cloud
IPv4 and IPv6), with no tunnel between: EdgeOne talks directly to the front on port
7777. That closes the question the earlier correction raised, from the account's
own side rather than from the peer addresses.

CDN-side account, `DescribeTimingL7OriginPullData`, 09-14 16:00 to 09-22 08:00
UTC, hourly:

- edge <- origin response flux total **2.09 GB**; busiest hour **431 MB**
  (09-22 00:00 UTC, the heavy test window)
- peak hourly-average bandwidth **5.9 Mbps**
- `l7Flow_request_hy` came back all zeros, which is a metric a free plan does not
  appear to retain -- read it as unavailable, not as "no origin pulls"

The other domain in the zone (`pure-dns.isui.ren`, origin `baidu.com`) is unrelated
to this project and was not touched.

## One command from source to a serving node

`deploy/oracle/deploy-node.sh` (run on the workstation) does the whole path and
refuses to install anything that has not proved itself on the node: package the
source, cross-build on the compile machine, `file` the artifact, run it once with
a bogus config path (a `load config` error means it executes; a `GLIBC` or exec
error means the toolchain is wrong), back up the running binary, install with
`install -o opc -g opc -m 0755`, restart, and run `accept.sh`. A failed
acceptance rolls back to `/home/opc/origin-cache.prev` by itself.

```sh
bash deploy/oracle/deploy-node.sh          # asks before installing
bash deploy/oracle/deploy-node.sh --yes    # unattended
```

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
sudo journalctl -u origin-cache-efficient --no-pager -n 100 | grep -i 'front access' | grep -v 'status="2'

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

The real-provider account of the rule above is a PAIR of runs, because one run
cannot say what held the window: `deploy/oracle/efficient-walk-pair.sh` walks the
same object twice on two loopback-only instances — `efficient-walk.toml` with the
protections on, `efficient-walk-nowatch.toml` with them off — each with a pause
in the middle, and both with `read_grace_secs = 0` and a 60 s idle TTL so a
150 s pause outlives every other protection. Measured:

```
watch ON : 512 shards -> 12 opens; pause +1 open (the read-ahead it is owed);
           post-pause re-read of the playhead's shard 0 opens, byte-exact
watch OFF: 512 shards -> 12 opens; pause +0; post-pause re-read 1 open
```

Read the pause lines together with the totals: the walk costs the same either
way, so the read-ahead is not extra upstream traffic, only differently timed —
during the gap instead of at the resume.

### A run for an object the node can never hold (ADR-0019)

`efficient-walk.sh` against a loopback efficient instance, real provider, the same
200 GiB object, after the run path was opened to un-keepable objects:

```
24 shards  -> 1 upstream open   (was 24), 2 s, 24/24 requests rode a run
400 shards -> 10 upstream opens (was 400), 17 s for 400 MiB (was ~450 s)
staged footprint: rises to ~515 MiB while spans are younger than the
                  minimum-age guard, then the cap takes it to EXACTLY
                  201 326 592 bytes (128 MiB pin + 64 MiB window) and it stays
cache_unkeepable_keys=1, cache_unkeepable_trim_bytes_total=338 690 048
```

Sample `segment_bytes` every 15 s for three minutes after a walk to watch the cap
land. Two facts make the numbers make sense: the cap is enforced on every reaper
tick, but a span is a candidate only once it is older than `STAGE_MIN_AGE_MS`
(60 s); and the chain's read-ahead means staged bytes can grow for a little while
after the last request.

The production upstream gets this win without a config change: `efficient` is the
default profile (ADR-0022), and a ranged request takes the run path whatever its
upstream is named (the profile no longer selects the response shape — it carries
`min_file_size` and the ledger window). The deployed unit and its config file are
named `origin-cache-efficient.service` and `config-efficient.toml`; neither is the
old `standard` name any more, and neither sets `cache_profile`, so the default
applies. One identifier was deliberately NOT renamed: `cache_dir =
/opt/origin-cache/cache-standard`, which holds the live metadata database and
every staged sidecar.

### A real player, and the request shape it actually sends (2026-09-21)

`deploy/lab/viewer/player-probe.mjs` plays the object in real Chromium and records
what the browser asked for — through CDP, because page JavaScript cannot see the
media requests — alongside the events the viewer feels (`waiting`, `stalled`,
`error`, and how far it played). It has a LAB section (15); run it by hand with:

```sh
node deploy/lab/viewer/player-probe.mjs --target http://127.0.0.1:7779 \
  --object media/viewer-object.mp4 --page media/hello.txt --play-secs 12
```

What it found on the product's own object (`googledrive1/round3.mp4`, 200 GiB)
through the CDN, from a workstation:

- The player's FIRST request is **open-ended**: `Range: bytes=0-`. Chromium then
  asks for `bytes=30932992-`.
- Our origin answers `bytes=0-` literally, which on this object means
  `content-range: bytes 0-214748364799/214748364800` and
  `content-length: 214748364800` — a 200 GiB promise. Check it directly:

```sh
curl -s -r 0- -D- -o /dev/null http://127.0.0.1:8080/googledrive1/round3.mp4 | head -4
```
- Through the CDN the player never loads metadata (`readyState` 0, a `stalled`
  event, 300 bytes in 8.9 s). That was recorded here as an open-ended-range
  problem. It is not, and the rest of this section is the correction.

**What the edge does with an open-ended range (measured 2026-09-21).** From the
node (`deploy/lab/probe-edgeone-range-shape.sh`), on this object:

| ask | what EdgeOne answers |
| --- | --- |
| `Range: bytes=0-` | 206, `content-range: bytes 0-214748364799/214748364800`, `content-length: 214748364800` |
| `Range: bytes=118111608559-`, an offset never requested before | 206, `content-length: 96636756241` |
| `Range: bytes=0-1048575` | 206, 1 MiB in 0.50 s |
| the same 1300 bytes, CDN vs origin | identical sha256 |
| a 3 s open-ended pull, then idle | one more upstream open inside 12 s, then flat for 24 s |

The edge relays the promise literally and does not cap what it pulls, so "let the
CDN cap it" is dead on this CDN; it also does not run away pulling the whole
object after the client leaves. What it does add is granularity: an 8 s
open-ended pull made the origin do +6 upstream opens and +9 stats for ~14 MB
delivered, where a bounded 1 MiB range makes exactly +1 open and +1 stat. The
edge pulls in ~2 MB pieces, so the origin's one-open-per-window win is spent at
the edge's granularity whatever shape the client asked in.

**Why the player still does not start.** `deploy/lab/viewer/cdn-wire-probe.mjs`
records, per media request, the bytes the browser actually received and who ended
the request. Through the CDN every request is a 206 carrying the literal promise
and every one is cancelled by the BROWSER (`net::ERR_ABORTED (canceled)`) after a
few hundred KB, in 29 532 992-byte steps — Chromium's block size for a 200 GiB
total. Rewriting each open-ended request into a bounded 1 MiB one (CDP `Fetch`,
`--window 1048576`) changes nothing: same steps, same failure. Answering
`bytes=N-` with a bounded window would not have fixed this.

Three local controls say what did:

- The same object's head, served by a plain local server, PLAYS: readyState 4,
  duration known, video decoded until the transfer was cut. The object is
  playable and the browser is capable.
- The same 33 MB of bytes with only the advertised total changed to 214748364800
  reproduce the 29.5 MiB block stepping exactly and then die with
  `PIPELINE_ERROR_READ: FFmpegDemuxer: data source error` once the server runs out
  of data (`deploy/lab/fake-total-server.py`). The advertised total decides the
  player's block size and the offsets it plans against.
- An unindexed fragmented MP4 built locally (ftyp + moov with `duration=0`, no
  `sidx`) plays from a fast local server, but its trace reads the file's tail
  before settling: with no index, a player has to scan for the duration.

And the object is not what the test assumed. `round3.mp4` is 214748364800 bytes —
exactly 200 GiB — of which the real content is a two-fragment, ~5-second fMP4
clip: `ftyp` + `moov` (`duration=0`) + two `moof`/`mdat` pairs ending by ~31 MB,
and 0xFF filler from there to the end (verified at 33 MB, 50 MB, 1 GB, 50 GB,
150 GB and the last 8 KB: all 0xFF). A player that must scan for a duration walks
a 200 GiB address space of filler in 29.5 MiB blocks and never starts. Nothing the
origin answers can change that, and the same bytes with a true total play fine.

So all three options recorded here are resolved, none of them by a change in the
origin: (1) a bounded window does not make this player start, measured; (2) the
CDN does not cap, measured; (3) another CDN is not needed, because the failure is
not the shape. What the criterion needs is a REAL long video — one with an index
or a real `mvhd` duration. The object in the bucket never was a 30-hour film, and
the domestic-vantage run still needs a viewer outside this network.

### The real 30-hour film, and the route it exposes (2026-09-22)

The bucket now holds a REAL long film — key `googledrive1/%E9%9C%87%E6%92%BC%E6%88%91%E4%BB%AC%E7%9A%84%E6%9C%AA%E6%9D%A5%E5%90%A7_200G.mp4`, 214,748,364,800 bytes,
uploaded 2026-09-21 18:04. Verified before any test: `mvhd` duration 110,716,239
ms = 30.75 h at timescale 1000, `avc1` + `mp4a` (Chromium decodes both), `moov`
at the head with real sample tables (stts/stss/ctts/stsc/stsz/stco) plus
`mvex`/`trex`, and no filler — the last 64 bytes and three mid-file offsets (50%,
90%, 99% of the object) are real data, unlike round3.mp4.

- It PLAYS in Chromium from a plain local server: readyState 4, playback running
  through the probe's window, no error. Object and codec are fine.
- Through the CDN from the node the edge is healthy for it: fresh 1 MiB ranges
  complete (TTFB 0.21-0.63 s), an open-ended 8 s pull streams steadily.
- Through the CDN from the workstation the player still never starts — and this
  time the object is exonerated; the route is not. The workstation has no IPv6
  route, so GeoDNS serves it an OVERSEAS IPv4 POP (43.174.246/247.108), which
  delivered 0.4-1.4 MB/s at best and hard-stalled during the test (36 KB in 10 s,
  full timeouts); the player's first request was cancelled with 300 bytes —
  headers only. The node reaches a domestic IPv6 POP (240d:c010::/32) and gets
  steady service from the same hostname.

So the last unknown for the criterion is the vantage — and asking where the
vantage actually lands corrected the guess above:

- This workstation's DIRECT egress is 39.172.36.80 = China Mobile, Zhejiang
  Huzhou (three services agree), and all three domestic public resolvers
  (223.5.5.5, 119.29.29.29, 114.114.114.114) return the same Singapore addresses
  for this hostname. Nothing classified it as overseas: **the zone has no
  mainland acceleration, so a Chinese viewer is served from Singapore.**
- The origin itself is in Phoenix, Arizona (Oracle Cloud, 129.146.127.22), and
  the NODE's own lookup also lands on a Singapore POP. A Chinese viewer's bytes
  therefore cross the border twice — Zhejiang -> Singapore -> Phoenix — which is
  exactly what 0.4-1.4 MB/s with stalls looks like.
- For the record on the 2080 proxy: it exits at 129.146.127.22, the ORIGIN host
  itself. Every measurement in this document is direct — `--noproxy '*'`,
  verified by curl's own `Established connection to cdn-oracle.isui.ren
  (43.174.246.108) from 10.194.6.139`, and the browser probes pass
  `--no-proxy-server`. No CDN account here is a proxy account.

What the criterion needs next is a deployment decision, not a code change:
mainland acceleration for the zone (which needs the domain's filing), or an
origin closer to the audience. Until one of those lands, a 30-hour film cannot
play smoothly for a Chinese viewer however correct the origin is: 200 GiB over
30 h needs about 1.9 MB/s sustained, and the leg measured here peaks at 1.4.

**The shape is the measurement: the same object, the same route, the same night,
four orders of magnitude apart.**

Everything above was taken with the shape a browser player uses — one
open-ended `Range: bytes=N-`. That shape is not what this CDN is good at, and
the difference is not marginal:

| shape (same object, same client, same route, minutes apart) | result |
| --- | --- |
| one open-ended pull, `bytes=N-` | 300 B to 1.4 KB/s, player never starts |
| 4 concurrent readers x 5 MB shards, shared offsets | 180 MiB in 24.6 s = **7.3 MB/s**, 0 gaps, 0 errors |
| 4 concurrent readers x 5 MB shards, DISTINCT offsets (cold fill) | 180 MiB in 26.7 s = **7.1 MB/s**, 0 gaps, checksums 4/4, seek TTFB p50 1422 ms |

The film needs 214,748,364,800 B / 110,716 s = 15.5 Mbit/s = **1.94 MB/s
sustained**, so the shape the product promises clears it by 3.6x from this very
workstation, through the Singapore POP the domestic resolvers hand out, with the
edge filling its cold bytes from Phoenix at ~2.6 MB/s and serving the rest from
its own cache. `deploy/lab/viewer/multi-viewer.mjs --chunk-bytes 5242880
--viewers N` is the instrument, and `--unique-seeds` is the honest variant: it
stops four viewers from sharing one fill.

Two conclusions follow, and they replace the topology reading this section
carried for a day:

1. **There is no route problem and no topology verdict to take.** The same
   domestic client, on the same hostname, moves 7.1 MB/s when it asks in shards.
   Whatever the open-ended shape runs into is a property of that shape.
2. **The origin's contract is to answer shards correctly and cheaply, and it
   does**: byte-identical ranges (4/4 checksums), 1 MiB from staged bytes in
   86 ms, one upstream open per window instead of one per request, and 20.3 MB/s
   cold from Google Drive when a span is big enough to amortise the open.

**The supply side of that shape (same night, at the origin).** While the edge is
actively filling it pulls 1 MiB per request, and the origin answers with a median
of 265 ms (p90 2236 ms) — roughly 4 MB/s of cold supply, shared by every viewer
who wants bytes the edge does not already have. Over a 7-minute window that mixed
filling with idle time the edge pulled 133.1 MiB, i.e. 0.33 MB/s averaged. Cache
hits are served at line rate; cold bytes are a shared budget. One viewer of this
1.94 MB/s film fits comfortably; six viewers at distinct cold offsets (a 390 MB
read) do not — a run that asked for exactly that was stopped after five minutes
rather than allowed to finish, and its `page.evaluate` exception is the kill, not
a harness failure.

### The window decision: a jump pays the floor (ADR-0024, 2026-09-22)

The reading that motivated it, and the reading after it, both on the node
against the real 200 GiB object. `deploy/oracle/window-decision-probe.sh` takes
the second one (fresh random bands per run, and it settles between rows because
a seal lands after the body it seals):

| read | before (whole-window policy) | after (deploy `db6c07ce…`) |
| --- | --- | --- |
| a 5 MiB cold jump | `segment_bytes` +67,108,864 — exactly one 64 MiB window, **12.8x the bytes read** | **+8 MiB** (the floor), one upstream open — measured on three separate jumps |
| another 5 MiB inside that window | 0.04 s, no open | 0 new bytes, no open |
| a single 20 MiB request | one 20 MiB window, one open | unchanged: the floor is a floor, never a cap |
| 41 MiB of reads across 5 requests (3 jumps) | ~256 MiB staged | **+52 MiB staged, 5 opens** |

The LAB pins the shape a CDN actually asks in — 24 ascending 1 MiB shards
against the default 64 MiB window and the 8 MiB floor: **3 upstream opens** (the
ramp climbs 8 → 16 MiB, and the boundary hands over), where a per-request open
would be 24. Before the handover existed that measured **8**. The old
whole-window policy measured 1; the walk pays one or two extra opens for the
ramp and stops paying a whole window for every seek.

Tests: 310 green, zero warnings. Reverse verification: pinning `floor_bytes` to
the configured window turns seven of the new tests red (`cache::window` three,
`cache::session` four, one integration) and leaves every pre-existing test
green, so the new tests measure the ramp and nothing else does.

After the change, the CDN shape through the deployed binary (4 viewers x 5 MB
shards, distinct offsets, `--unique-seeds`): three viewers moved 45 MiB each —
**141 MiB, zero gaps, three distinct checksums** — with seek TTFB p50 91-101 ms,
while the fourth hit the workstation path's long-standing one-in-N stall (0 bytes
in 90 s) and was reported as one failed row instead of hanging the run. The
origin's side of that same window (`fill-account.sh`): **zero `front access`
lines** — every byte came out of the edge's own cache, which is the shape a CDN
is for.

**Cold regions are a different regime, measured 2026-09-22** with
`deploy/lab/probe-cold-viewers.sh` and `multi-viewer.mjs --cold-band` (a fresh
band and a fresh jump seed per viewer per run, and the report's `edgeHIT`/
`edgeMISS` columns so a run proves it was cold instead of claiming it):

| shape | delivered | gaps > 1.5 s | seek TTFB p50 |
| --- | --- | --- | --- |
| 4 viewers, 5 MB shards, offsets the edge already held | 141 MiB in 26.7 s (5.3 MB/s) | **0** | 101 ms |
| 4 viewers, 5 MB shards, fresh bands (`edgeMISS=4`) | 120 MiB in 43.3 s (2.8 MB/s) | **6**, worst 2118 ms | 1179 ms |
| 6 viewers, fresh bands | 120 MiB, one viewer 0 bytes in 240 s | 8, worst 2460 ms | 1479 ms |
| 1 viewer, a fresh band | 30 MiB in 16.5 s (1.8 MB/s) | 0 | 1463 ms |

**The upstream concurrency gate is not what limits the cold fill** (asked
directly on 2026-09-22, because "more parallelism upstream" is the obvious
suspicion). `concurrency_per_upstream` was raised 3 -> 12 on the node and the
same 4-viewer cold-band run repeated: the origin's per-ask service time did not
move (**p50 225 ms at 3, 223 ms at 12**), its ask count did not rise, and the
delivered aggregate did not improve. Then the link itself was measured from the
node — a vantage whose path to the edge is clean — with 16 MiB in ONE request:

| shape from the node | measured |
| --- | --- |
| 16 MiB in one Range, sample 1 | 26.6 s = **0.63 MB/s** (TTFB 0.21 s) |
| 16 MiB in one Range, sample 2 | 12.9 s = **1.31 MB/s** (TTFB 0.25 s) |
| a fresh 1 MiB Range (three samples) | TTFB 0.21-0.94 s, total 1.75-2.74 s |
| 5 MB shards, four viewers, warm at the edge | 5.3 MB/s aggregate, zero gaps |

So the origin answers in ~0.2 s and the bytes then crawl: **the constraint is the
path between this origin and the edge** (Phoenix <-> the edge's own network),
0.6-1.3 MB/s per connection and 2-3.4 MB/s with a few — and in a cold run the
edge's *fill* and its delivery both cross that same path.

Where the edge's pull actually comes from (measured 2026-09-22, after a wrong
guess that the cloudflared tunnel was in this path — it is not: that tunnel
carries the watchdog's status reports). The peers connected to the origin's front
port are Tencent's AS139341 nodes: three in **Hong Kong** (43.168.149.241,
43.152.24.44, 43.175.119.174) and two in **New Mexico, US** (43.174.106.43,
43.146.63.79). The client-facing POP the domestic resolvers hand out is
**Singapore** (43.174.246/247.108). So content can cross the Pacific twice:
origin (Phoenix) to a Hong Kong puller, then to the Singapore POP a domestic
viewer lands on.

The per-ask arithmetic says the same from the other side: 223 ms per 1 MiB ask, of
which our staging is 86 ms, leaves ~137 ms of network — a Hong Kong-class round
trip. One serial ask chain at that RTT could move ~7 MB/s, and the cold
multi-viewer runs achieved 2.8 MB/s aggregate, so the pacer is the edge's ask
cadence, not our answers.

Two levers follow, and both are outside this repository: the origin's placement
(closer to the pullers) and the origin-pull concurrency/batch settings in the
EdgeOne console. Our end is already at its floor: no extra hop in the path, 86 ms
of staging per 1 MiB ask, and 20 MB/s from the provider on a contiguous read. Raising the gate cannot
exceed the pipe; Google Drive's parallelism is irrelevant for the same reason (a
contiguous read from it measures 20 MB/s). It also shows why the shard shape is
worth what it is: 5 MB shards move 5-10x what one large Range does on this link.
The lever is the deployment's topology — an origin near the edge — which is the
same conclusion the domestic-vantage section reached from the other end. The gate
was restored to 3.

Three things follow. **The origin is not the limit**: its side of the 4-viewer
cold run is 141 asks for 115.5 MiB at p50 225 ms (each ask is a 1 MiB fill, most
served from staged bytes). **The edge's cold fill is the limit and it is shared**:
one cold viewer gets ~1.8 MB/s — right at the 1.94 MB/s the film needs — and four
cold viewers split ~2.8 MB/s, so the same shape that shows zero gaps on warm
bytes shows gaps of two seconds on cold ones. **The 6-viewer run's total is
unchanged** (120 MiB, the same budget) and its one dead viewer had `requests=0`:
that is the workstation path's known stall, not the fill.

**A cold jump's ~800 ms is the provider's `open`** (attributed at the origin with
`deploy/oracle/seek-attribution-probe.sh`: three cold jumps from loopback, with
the deltas of `backend_call_duration_seconds{op}` and
`cache_body_ttfb_seconds{source="upstream"}` around them):

| inside one cold jump | measured |
| --- | --- |
| the jump's own first byte (loopback) | 727-839 ms (1-2 ms when the offset was already staged) |
| provider `stat` | **1 ms** in the window; **5.8 ms mean over the process's 273 calls** |
| provider `open` | **806 ms** in the window; **824 ms mean over 165 calls** |
| our overhead (headers ready, first body byte) | the difference, i.e. nothing measurable |

So the seek's cost is the provider's connection, not our bookkeeping — which is
what ADR-0016 already judged ("the open side is at its floor", and chasing it
"would change what a run IS"). A seek *inside* the watched neighbourhood
(ADR-0018's pin) is already warm: 1-2 ms.

Two instruments were added with it, both because a CDN round needs both sides:
`deploy/oracle/fill-account.sh` reads the origin's `front access` log into a
per-key account (requests, MiB, p50/p90 ms, distinct `xff`) plus the live
counters, and `deploy/lab/viewer/multi-viewer.mjs` grew
`--viewer-timeout-secs` (default 120) with a progress line per viewer — a hung
viewer is now one failed row instead of a run with no numbers, which is how a
six-viewer CDN round ended on 2026-09-22 before it had any.

For the record, the leg table measured while chasing the wrong shape, which is
still useful as a description of each leg's ceiling: Google Drive -> origin
20.3 MB/s cold (64 MiB in 3.31 s); origin -> edge 12 MB/s for bytes already
staged; a cold 1 MiB is ~1.1 s because that is the upstream OPEN's latency, not a
rate; and a pony season (`mlp-s02-concat.mkv`, 13,009,202,351 B / 21,184 s =
4.9 Mbit/s) needs only 0.61 MB/s, which is why it played smoothly through this
edge: that session really did use the domain and therefore genuine EdgeOne, and
the origin's log agrees — it holds almost no pulls for that key, because the edge
kept it and served the viewer itself. It is positive evidence for the shard rule,
not a curiosity: 0.61 MB/s is far inside what a sharded concurrent reader gets.

### Multi-viewer accounts through the CDN: from the NODE

A "N viewers through the CDN" number taken from a workstation measures the
workstation's path to the edge, not the origin or the edge. Reproduce it:

```sh
# From the workstation: 3 concurrent GETs of the SAME small object.
P=https://cdn-oracle.isui.ren/googledrive1/test-page.html
for i in 1 2 3; do curl -s --noproxy '*' -m 90 -o /dev/null \
  -w "v$i ttfb=%{time_starttransfer}s total=%{time_total}s\n" "$P" & done; wait
```

Three rounds, 2026-09-21: one of the three waited 10.2 s (total 11.9 s), one
waited over 300 s, and one round was clean — while the origin's own counters
(`backend_call_duration_seconds_count`, `cache_session_total`) did not move at
all, i.e. the stalled request never reached the origin. The same three ranges
from the NODE answer in 0.21-0.23 s, every round.

So the concurrent account comes from the node:

```sh
# ON the node: N viewers, each walking a fresh band and then jumping.
bash deploy/lab/probe-edgeone-viewers.sh round3.mp4 3 6 6
```

It prints a per-request TTFB distribution and the origin-side deltas for the same
window (upstream opens, sessions sealed/chained, reader attachments). Measured
2026-09-21 against the real 200 GiB object, fresh bands (the edge cannot answer):

| shape | requests | TTFB p50 | p90 | max | origin opens |
|---|---|---|---|---|---|
| 1 viewer, 3+3 | 6 | 0.213 s | 0.216 s | 0.221 s | +5 |
| 3 viewers, 6+6 | 36 | 0.208 s | 0.212 s | 0.245 s | +27 |

Concurrency costs nothing at the client's first byte, and the origin pays opens
per WINDOW rather than per request (27 opens for 36 requests: the three viewers
walk overlapping windows). `deploy/lab/viewer/multi-viewer.mjs` (real browsers)
is for the LAB and loopback targets; through the CDN its page loads hit exactly
the stall above.

### Multi-viewer accounts (browsers, both paths)

`deploy/lab/viewer/` is a generic large-object reader (`reader.js`, EVALUATED in
the page rather than injected as a script, so a CSP in front of the origin cannot
block it) plus a driver (`multi-viewer.mjs`) that gives every viewer its OWN
browser context — its own cache, its own connection pool — driving the system
chromium through `playwright-core`. Nothing in it is media-aware: it reads byte
ranges sequentially and jumps, and records what a viewer feels (gaps between
bytes) and what a request costs (time to first byte after a jump).

```
node deploy/lab/viewer/multi-viewer.mjs --target <base> --object <path> \
  --page <path-of-a-small-object-on-the-same-origin> --size <bytes> \
  --viewers 3 --chunks 4 --chunk-bytes 262144 --seeks 3
```

The page MUST be an object the target itself serves (same origin, no CORS, and
nothing is uploaded). Measured:

```
LAB (7779, keepable object)     1 viewer:  8 opens / 7 requests
                                3 viewers: 9 opens / 21 requests, 0 gaps, one checksum
LAB (7781, un-keepable object)  3 viewers: 11 opens / 21 requests, staged bounded
CDN, 200 GiB object, cold       1 viewer:  7 requests, seek TTFB p50 1341 ms, +7 origin opens
CDN, the SAME regions           3 viewers: 9 requests, 0 gaps, seek TTFB p50 87 ms,
                                           IDENTICAL checksums, ZERO extra origin opens
```

The last two lines are the product story for a large object: the origin pays for
the first pull of a region, and overlapping viewer traffic is absorbed by the
edge. `--no-proxy-server` is passed to the browser, and nothing here may run
through an HTTP proxy: the proxy's RTT is the thing being measured.

### The target-scale account: an object the node can never hold

The product's object is a 3-hour video of 30-200 GB. Measured on the node against
a real one — `/googledrive1/round3.mp4`, 214 748 364 800 bytes (200 GiB) — with
`big-object-probe.sh` (bounded traffic: a shard, a short walk, five seeks, one
larger range, three concurrent ranges):

```
efficient profile, loopback instance (magazine 6 GiB)
  admission            : every request logged "passthrough without staging: the
                         magazine cannot hold this object"; segment_bytes = 0,
                         entries = 0, stray_bytes = 0 for the whole probe
  one 1 MiB shard      : 1 upstream open, 1277 ms
  walk of 24 shards    : 24 opens (one per request), 1126 ms per MiB
  random seeks         : 823-924 ms to first byte
  one 16 MiB range     : 1 open, 1475 ms -> 10.8 MiB/s
  three concurrent 1 MiB ranges on one key: 3 opens
the production instance before the windowed walk (magazine 10 GiB)
  1 MiB shard          : 1 open, 1246 ms, upstream-served
  random seeks         : 858-901 ms to first byte
  cache                : nothing written for the key
```

Through the CDN the picture has one more layer, and it is the layer the viewer
actually feels (`probe-edgeone-big.sh`, run from the node itself — never through
an HTTP proxy, whose RTT is the thing being measured):

```
HEAD                : eo-cache-status MISS, content-length = the object
one 1 MiB range     : TTFB 0.206 s, 1.62 s total, origin opens +1
the SAME range again: eo-cache-status HIT (age 2), origin opens +0
three random seeks  : TTFB 0.206-0.209 s
24-shard 1 MiB walk : 39 s (1.6 s per MiB), origin opens +24
origin log          : "serving without caching: the disk cannot hold this
                      object want=214748364800", one open per request, 1 MiB
                      ascending shards over h2, ~1.1 s each
```

So the EDGE is what makes a re-scrub cheap on an object the origin cannot hold:
EdgeOne caches the shards it has pulled (a repeat is a HIT with zero origin
traffic), and its first-byte latency is its own (~0.2 s) rather than the
origin's pull. The origin's job for such an object is the first pull of each
region, and it does that with one `open` per request because nothing can be
staged — which is where the run machinery stops helping.

Read this as the boundary of what the cache currently buys. An object larger than
the magazine is neither a magazine member nor a resident stray (ADR-0013/0014),
so **nothing about it is cached, and the run machinery never starts** — a run is
gated on staging admission, so a 1 MiB-granularity walk pays one ~1.2 s provider
open per MiB (a stageable object pays about one per 64 MiB window: 512 shards ->
12 opens). Sequential playback is still feasible (24 shards in 27 s is ~0.9 MiB/s,
and a player asks for ranges far larger than 1 MiB, which amortizes the open:
16 MiB cost 1 open and 1.5 s), but every seek costs ~0.9 s, and scrubbing across
a 200 GiB object is a seek per gesture.

That is the next mechanism's case, and it is measurable from here: a NO-RETENTION
run — one open per window, readers riding the watermark, nothing written to disk
— would make the walk cost one open per window instead of one per request with
zero disk cost, which is exactly what an object too large to keep needs.

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

## Reading the suite, and where the knowledge lives

`deploy/lab/run-lab.sh` is seventeen sections, each with a `note` line naming what
it pins. Two of them were added last and are the ones to look at when a change
touches the front or the window default:

- **16** — the front's own guards: healthz refused with 404 on the public port
  (not 403: a caller should not learn the surface exists), the per-IP rate ceiling
  returning 429, and an over-sized prewarm body 413'd before the business plane
  sees it. `config-f.toml` runs with `front_rate_rps = 2` for it.
- **17** — the production DEFAULT window (64 MiB) walked with the shape the edge
  really produces: ascending 1 MiB shards on a 3 GiB object the magazine cannot
  hold. `config-g.toml` deliberately sets no `session_window_bytes`, so a change
  to that default cannot pass silently.

Two files carry the rest:

- **`docs/pitfalls.md`** — every trap that has cost real time, with its evidence.
  Read it before an experiment, and add to it after one that surprises you.
- **`docs/adr/README.md`** — one line per decision, which ones amend which, and
  the cross-cutting rule (ADR-0012: a guard is a deadline, not an exemption).

Three modes, in rising cost:

```sh
bash deploy/lab/run-lab.sh --smoke    # ~2 min: the core set (see below)
bash deploy/lab/run-lab.sh --quick    # ~7 min: everything but the 3 GB pull
bash deploy/lab/run-lab.sh            # ~9 min: the whole matrix
```

`--smoke` keeps the binary boot, the front's guards, the acceptance matrix and the
run/window account (sections 1-9 and 16) and skips the accounts that need a 150 s
poll, the browsers and the shard walk (10-15, 17) — the parts a change to one
module rarely breaks. Use it between edits; use `--quick` before a commit.

The suite refuses to start on a busy box (`MAX_LOAD`, default 6) or with orphan
browsers alive; the same revision produced 7 false FAILs at load 46 and 60 PASS /
0 FAIL at load 4. `MAX_LOAD=<n>` overrides when a noisy run is worth having.

## Test data cleanup

The 3 GiB coverage test file lives in googledrive1 (`coverage-test-3g.bin`).
The retired efficient test instance used to promote it into
`cache-efficient/`; that instance is gone (ADR-0009), so only the upstream
object needs deleting. Use the repo's lab suite (`deploy/lab/run-lab.sh`
with `deploy/lab/config-c.toml`) if the efficient profile needs re-testing.

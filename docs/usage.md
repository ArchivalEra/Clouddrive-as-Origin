# Using it

For someone who wants to stand this up, point clients at it, and know what to run
when it looks wrong. Design reasoning is in `docs/spec.md` and `docs/adr/`; the
measured behaviour of every mechanism is in `docs/runbook.md`; the traps are in
`docs/pitfalls.md`; every instrument is indexed in `deploy/README.md`. This page
is the order of operations.

## 0. What it is, in one breath

A pull-through origin cache: it sits between a CDN and cloud drives (OpenList /
WebDAV), answers ranged `GET`/`HEAD` from what it holds on disk, and pulls the
rest, streaming to the client while it writes. The client namespace is flat —
`/<key>` — and adding an upstream never changes a URL.

It is **not** a media server (ADR-0026): it knows nothing about content types, it
does not parse containers, it generates no manifests, and it ships no player. A
200 GiB video, a 100 GiB tarball and a VM image are the same object to it. What a
viewer needs beyond bytes — a manifest, MSE, a page — is the client's program.
The exposure is **read-only**: there is no upload path at all.

## 1. Five minutes, no cloud

```sh
cargo build --release --offline
bash deploy/lab/run-lab.sh --smoke      # ~1 minute, core set
bash deploy/lab/run-lab.sh --quick      # ~10 minutes, everything but the 3 GB pull
```

The suite brings up eight loopback instances of the real binary against a local
rclone WebDAV stand-in, and prints one `PASS:`/`FAIL:` line per assertion. A
healthy run ends `PASS=35 FAIL=0` (`--smoke`) or `PASS=75 FAIL=0` (`--quick`).
It refuses to start on a busy box (`load > 6` or any live `chromium`) because its
timing assertions measure the machine too. Browsers are needed for sections
14/15; export `PW_DIR` (or set `PW`) if `run-lab.sh` cannot find
`playwright-core`.

That is the whole local loop. Nothing below is needed to change code; it is
needed to serve traffic.

## 2. One node, in three steps

On the node (Linux, systemd):

1. **Install** — build the aarch64 binary on a compile machine and copy it over
   (`bash deploy/oracle/deploy-node.sh --yes` does packaging, cross-build,
   `file`/bogus-config gates on the node, install, restart, `accept.sh`, and rolls
   back on failure). For a first install, or when configs/units change, use
   `sudo bash deploy/oracle/install.sh <binary> [--keep-env] [--new-token]`
   instead; it lays down the units and calls `deploy/oracle/env-file.sh` to
   write `/opt/origin-cache/origin-cache.env`.
2. **Configure** — `deploy/oracle/config-efficient.toml` is the shape: front
   listen + metrics, the admission token (`front_origin_token_header` /
   `..._env` / `..._exempt`), TLS env names, `listen_addr` for the business
   plane, `cache_dir`, budgets, and the `[[upstreams]]`/`[[routes]]` pair. Every
   `*_env` name it declares **must** exist in the env file: naming one that is
   unset is a boot failure on purpose (fail closed), and unknown keys in any
   config section are a boot failure too (`deny_unknown_fields`).
3. **Run and accept** — `sudo -n systemctl restart origin-cache-efficient
   origin-cache-nocache` (plain `systemctl` may hit interactive auth), then:

```sh
bash deploy/oracle/accept.sh        # expect: VERDICT=PASS
```

`accept.sh` is the contract check (loopback reads, reserved names, HEAD, the
per-key view). It writes its full output to `/home/opc/accept-last.log`.

Four units: `origin-cache-efficient` (the cache), `origin-cache-nocache` (zero
disk), `origin-cache-watchdog.{service,timer}` (health + heartbeat; the service
showing `inactive` between runs is normal).

Two things that look like details and are not: **`cache_dir` is never renamed**
(it holds the live database and every staged sidecar — a rename starts you on an
empty cache), and **never point a trial binary at the live `cache_dir`** (a
valid-but-wrong config quarantines the store; pitfall 55).

## 3. Putting a CDN in front

EdgeOne (or any origin-pull CDN) needs, on the origin-pull side:

| setting | value, and why |
| --- | --- |
| origin | the node's address, port **7777**, `OriginProtocol=FOLLOW` |
| Host header | the public hostname (the origin's certificate is checked against it) |
| `RangeOriginPull` | **on** — the whole design assumes the edge forwards ranges |
| `UpstreamHTTP2` | **on** — one H2 connection carrying many range streams |
| a `ModifyRequestHeader` action | **sets** `X-Origin-Token` to the value in the env file |

The token is the admission gate (ADR-0025): the edge *sets* the header, so
whoever reaches the port cannot forge it; the front requires it from every peer
outside `front_origin_token_exempt` (loopback by default, so the node's own
checks and the LAB keep working). A direct request to the port without it gets
`403`.

The viewer-facing URL is then `https://<public-host>/<upstream-id>/<key>`. The S3
list surface is the same path with `?list-type=2`.

## 4. What a client may do

- `GET` / `HEAD` on `/<key>`; `Range: bytes=a-b` answered with `206` and a
  correct `Content-Range`; `bytes=a-` (open-ended) answered too, with
  `content-range: a-(a+2^31-2)/total`.
- Listing (`?list-type=2`) and a per-key diagnostic view on the business plane
  (`/_internal/healthz?key=<raw key>`) — the latter is loopback-only by design.
- **No writes.** `PUT`/`POST`/`PATCH`/`DELETE`/`MKCOL` are `405`, and the
  business plane has no write verbs at all.
- **No CORS.** There are no `Access-Control-*` headers anywhere, so a browser
  page that fetches objects must be served from the same origin as the objects
  (that is why the fixtures live in the bucket).
- Content-type is passed through, with a fallback table for the generic
  `application/octet-stream` many provider APIs return (spec §3.9).

Two client-shape cautions, both properties of content, not of this service: an
unindexed **fragmented MP4** will not play in a bare `<video>` (needs MSE plus a
manifest), and a page that hands any object to a media element it cannot demux
will look like an origin problem when it is not. Measure before blaming the path
(pitfalls 27/30, and the runbook section on the real film).

## 5. Day two

```sh
# is it healthy (loopback on the node)
curl -s http://127.0.0.1:8080/_internal/healthz        # and :8081 for nocache
curl -s http://127.0.0.1:9090/metrics | head           # front metrics: :9091

# who pulled what, with per-request latency and bytes
bash deploy/oracle/fill-account.sh 300 200G
bash deploy/metrics-report.sh http://127.0.0.1:9090/metrics 30

# logs
journalctl -u origin-cache-efficient --since "1 hour ago" | sed 's/\x1b\[[0-9;]*m//g'
sudo tail /opt/origin-cache/watchdog.log
```

`front access` lines carry `peer=` (who opened the connection) and `xff=` (the
client the edge served) — the split that tells an edge pull from a direct hit.
Retention is capped (journal 500 MB / 14 days, watchdog log rotated): if history
matters, ship it somewhere, do not assume it is there. The watchdog posts a
heartbeat every 5 minutes and the far side declares the node offline after 15
silent ones; `healthz` reports `degraded` with reasons (metadata store, disk
below reserve) rather than lying.

## 6. Measuring it (which instrument is authoritative)

| question | instrument | rule |
| --- | --- | --- |
| what did the origin serve, and how fast | its own counters / histograms, or `fill-account.sh` | a client-side cache label is at best a hint |
| how does the CDN behave | node-side probes (`deploy/lab/probe-edgeone-*.sh`, `deploy/oracle/*probe*.sh`) | browser load tests belong on the LAB; the workstation-to-edge leg has its own stalls |
| how many viewers, how smooth | `deploy/lab/run-lab.sh`, `deploy/lab/viewer/multi-viewer.mjs` | report the machine's load next to client-side numbers |
| cold random load | `deploy/oracle/cold-load.sh` from the node | loopback requests carry `xff=-`; the account comes from the counters |
| a real player on a real film | `deploy/lab/viewer/player-swarm.mjs` + `swarm-report.mjs` | an HLS byte-range playlist can be built for an fMP4 without downloading it (`fmp4-index.mjs`) |

## 7. When it looks wrong

Short triage, each with the section that has the numbers: bytes wrong at the edge
(runbook "What the edge's cache label is worth"); a range request that returns
nothing (pitfall 27 — the object, not the path); slow first byte (runbook "Is
seeking smooth?" — provider `open` dominates, and it is at its floor); a viewer
that stalls (runbook "Ten viewers on the real film through MSE" — arithmetic
first); the store quarantined (pitfall 55 — a trial binary against the live
cache); a `403` from outside (that is R4 working: no edge token).

## 8. Where the knowledge lives

`docs/spec.md` (the contract) · `docs/adr/README.md` (one line per decision) ·
`docs/pitfalls.md` (traps, with evidence) · `docs/runbook.md` (measurements) ·
`deploy/README.md` (every instrument, what it answers) ·
`docs/security-hardening.md` (the exposure and its closures) ·
`skills/clouddrive-origin/SKILL.md` (the same operations, written for an agent).

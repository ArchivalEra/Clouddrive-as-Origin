# What is in `deploy/`

One row per artifact: what it answers, where it runs, what it needs, and where
its durable reading lives. Nothing here is a library — every file is an
instrument or a node-side asset, and the runbook carries the measurements they
produced. If a script's reading is not in the runbook, it was a one-off: that is
how the retired ones were found.

Legend for *where*: **workstation** = the machine this repo is cloned on,
**node** = `ssh oracle-cdn` (aarch64, production), **compile** =
the x86_64 cross-build box (`ssh -i ~/.ssh/compile-key archivalera@192.168.137.136`),
**client** = a host with a real internet vantage (not the node, not a proxy).

## `deploy/oracle/` — the node

| artifact | answers | where | needs | reading in |
| --- | --- | --- | --- | --- |
| `install.sh` | the mechanical install: binary, configs, both units, watchdog, logrotate/journald, the env file | node (as root) | a release binary, `sudo -n` | runbook "First install", "Provisioning a fresh node" |
| `env-file.sh` | does the env file define every variable the installed config names? | node | nothing | runbook "First install" |
| `accept.sh` | the contract after any deploy: units, TLS, ranged read, the front's guards, the origin token | node | the units running | runbook "Node acceptance after a deploy" |
| `deploy-node.sh` | source -> compile machine -> node in one command, with the two hard gates (`file`, fake-config) and rollback | workstation | ssh to both boxes, the musl toolchain | runbook "Deploying a new binary" |
| `build-aarch64.sh` | the cross-build itself (static musl, ~1m11s) | compile | `/mnt/hdd/crossbuild-tools/aarch64-linux-musl-cross` | runbook "Deploying..." |
| `config-efficient.toml` | the main plane's config (7777 TLS, the origin token, the efficient profile) | node | — | `docs/spec.md` |
| `config-nocache.toml` | the internal nocache plane (7778) | node | — | ADR-0022 |
| `watchdog.sh` + `.service`/`.timer` | unit/disk/healthz checks and the heartbeat to the blog Worker | node | the env file, cloudflared | runbook "Health checks", "The blog card says the node is offline" |
| `test-watchdog.sh` | behavioural test for `watchdog.sh` | node | — | runbook same |
| `fill-account.sh` | the origin's side of a CDN round: asks, bytes, latency, clients (`xff`) | node | loopback access | runbook "Who pulls, and how to tell" |
| `window-decision-probe.sh` | a jump pays the floor (ADR-0024), measured on the real provider | node | `efficient-walk.toml` instance | ADR-0024, runbook "The window decision" |
| `efficient-walk.sh` + `.toml` | the staged-read runs (ADR-0016) against the real provider | node | a loopback instance on 7791/8091/9094 | runbook "Reading the shaping account" |
| `efficient-walk-pair.sh` + `efficient-walk-nowatch.toml` | the watch account (ADR-0018): the same walk with the viewer protections off | node | same instance | ADR-0018 |
| `seek-attribution-probe.sh` | one cold jump's first byte, split into stat/open/our side | node | a loopback instance | runbook "Is seeking smooth?" |
| `big-object-probe.sh` | what an object the node can never hold costs (ADR-0013/0014) | node | loopback instance on 7791 | ADR-0019 |
| `rate-limit-probe.sh` | would the per-IP ceiling behave on this box | node | loopback instance on 7792/8092 | runbook "Would rate limiting be safe to enable?" |
| `ip-filter-probe.sh` | the front's two IP lists end to end (R9), incl. a dead business -> 502 | node | loopback instance on 7793/8093/9094 | runbook "The front's two IP lists" |
| `origin-pull-cidrs.sh` | keeps a firewall in step with EdgeOne's origin-pull ranges; refuses non-authoritative data (exit 2) | node or anywhere | tccli (read-only), or a saved JSON | `docs/security-hardening.md` (R3) |
| `journald-origin-cache.conf`, `logrotate-origin-cache` | retention caps for the journal and the watchdog log | node | — | R12 |

## `deploy/lab/` — the acceptance suite

| artifact | answers | where | needs | reading in |
| --- | --- | --- | --- | --- |
| `run-lab.sh` | the whole suite: `--smoke` (~1 min, core set), `--quick` (everything but the 3 GB pull), default (all) | workstation | `cargo build`, a quiet box (load < 6, no orphan chromium), for sections 14/15 `node` + `PW_DIR` | runbook "Reading the suite" |
| `config-a..h.toml` | one instance per question: a = default profile, b = nocache, c = short coverage window, d = tiny magazine, e = watch, f = front guards + rate ceiling, g = the production window vs shard shape, h = empty token exemption | workstation | — | the LAB output itself; ADRs 0016/0018/0024 |
| `flaky-server.py` | a ranged fixture that aborts or stalls on schedule (proves the retry knobs) | workstation | python3 | runbook "Surviving the leg" |
| `fake-total-server.py` | the same bytes with a different advertised total (object vs transport) | workstation | python3 | pitfall 30 |
| `probe-cold-viewers.sh` | N viewers on genuinely COLD bands, both sides accounted | workstation | the CDN, the node's fill-account | runbook "Multi-viewer accounts through the CDN" |
| `probe-edgeone-big.sh` | the target-scale account through the CDN, from the NODE | node | — | runbook same |
| `probe-edgeone-range-shape.sh` | what the CDN does with each range SHAPE | node | — | pitfall 27 |
| `probe-edgeone-viewers.sh` | N concurrent viewers through the CDN, from the NODE | node | — | runbook "…from the NODE" |
| `run-edgeone.sh` | the live CDN regression wrapper | workstation | the CDN | runbook |
| `sigv4-test.py` | the S3 listing signature, against a real bucket | workstation | credentials in env | `docs/spec.md` §S3 |
| `seek-storm.sh` | the scattered-seek shape that card C5 was about — kept as a COUNTER-EXAMPLE: the shape is not production and the question is closed (2026-09-12) | node | — | ADR-0004 "Not decided here" |

## `deploy/lab/viewer/` — the browsers

Every one of these needs `node`, `chromium` and `playwright-core`
(`PW` or `PW_DIR`; `run-lab.sh` exports `PW_DIR` for the suite).
None of them may go through a proxy: they measure the CDN.

| artifact | answers | where | needs | reading in |
| --- | --- | --- | --- | --- |
| `multi-viewer.mjs` | N viewers, sharded reads, gaps, checksums, `edgeHIT/edgeMISS`; `--cold-band` for genuinely cold bands, `--retries`/`--attempt-timeout-secs`/`--max-bytes` for long runs | workstation (or node) | browsers | runbook "Multi-viewer accounts", "Surviving the leg" |
| `reader.js` | the reader those harnesses evaluate in the page (ranges, retries, `window.__readerStats`) | — | — | runbook same |
| `player-probe.mjs` | what a real `<video>` asks for, with `PROXY`/`HOST_MAP` to borrow another vantage | workstation | browsers | runbook "The real player on the real film" |
| `media-wire-dump.mjs` | the full request/response headers, `loadingFailed` reasons and per-request body bytes | workstation | browsers | runbook same, pitfall 56 |
| `player-fetch-probe.mjs` | the same object read by `fetch` in the page (media stack vs fetch) | workstation | browsers | runbook same |
| `cdn-wire-probe.mjs` | who ended each request, from the wire | workstation | browsers | pitfall 27 |
| `player-page.html` + `range-fanout-sw.js` | a ready-made player (hls.js) behind a range fan-out worker | served from the bucket through the CDN | `hls.min.js` + a single-file playlist next to it in the bucket | runbook "The glue that gives a ready-made player more speed" |
| `package-for-player.sh` | packages an ordinary MP4 for that player without re-encoding | workstation | ffmpeg | runbook same |
| `log-server.py` | a tiny local origin for viewer experiments | workstation | python3 | — |

## `deploy/` (loose)

| artifact | answers | where | needs | reading in |
| --- | --- | --- | --- | --- |
| `measure-client-ttfb.sh` | client-side first-byte latency (spec §10) | client | a URL | `docs/spec.md` §10 |
| `metrics-report.sh` | latency attribution from the origin's /metrics | node (or via `ssh -L`) | the metrics port 9090 | runbook "Requests are slow" |

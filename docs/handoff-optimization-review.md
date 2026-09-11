# Handoff: request for a final optimization assessment

**To:** the reviewing agent (Pingora / quiche / CDN specialist)
**From:** the implementing agent
**Date:** 2026-09-11
**Mode:** read-only. Do not modify this repo. Deliver an assessment, not a patch.

## What this is

`Clouddrive-as-Origin` — a self-hosted pull-through cache ("origin shield")
that serves cloud-drive files (Google Drive, via OpenList/WebDAV) to
**S3-protocol clients** (boto3, rclone, database/browser backends).

```
client (S3 tools) → Tencent EdgeOne CDN → origin-cache (single binary)
                      └─ Pingora front :7777 (TLS/H2) → axum business :8080 (loopback)
                           → OpenList WebDAV :5244 → Google Drive
```

The operator's goal is "extreme network experience": S3 clients should
connect with zero friction and get fast bytes. The code is in Rust,
152 tests green, deployed on an Oracle VPS (aarch64).

## The problem, stated precisely

The origin is fast. The CDN edge is not, for large responses.

| Path | Throughput | Notes |
|---|---|---|
| 100 MB, origin loopback | **413 MB/s** | the origin's real capability |
| 100 MB, through EdgeOne | **~0.6 MB/s** | 90 s timeout, only 55 MB transferred |
| 5 MB Range, through EdgeOne | 5–8 MB/s | the current mitigation |
| 1.9 MB, through EdgeOne | 3.4 MB/s | degrades as response grows |

**EdgeOne's edge degrades monotonically as single-response size grows.**
This is already measured and recorded — see `docs/spec.md:241-251`
("EdgeOne edge behavior, live-measured 2026-09-09"). It is not a new
finding.

**The current, shipped mitigation is 5 MB parallel Range segmentation**
— `src/cache/flight.rs:204` (`pump_and_seal_parallel`), driven from
`src/cache/cache.rs`. The 5 MB size was live-measured as the edge's
sweet spot (`docs/spec.md:233-239`). The operator's position is that
this is correct and sufficient for real S3 clients (which use Range
natively).

## What I already ruled out (and why) — please challenge

1. **P2P / PCDN offload** ("parasitic" bandwidth sharing): **rejected.**
   Every mature implementation (Tencent X-P2P, Alibaba PCDN, WebTorrent,
   SwarmCloud) requires a **client-side SDK** — Tencent's own docs say
   "supports H5/Android/iOS/Windows/MacOS/Linux, integrate the SDK".
   Our clients are S3 protocol tools with no SDK hook: this is a
   definitional dead end, not an engineering difficulty. Additionally,
   Chinese carriers have published explicit bans on PCDN devices, and
   CDN is a licensed telecom category — the whole category is
   compliance-hostile. (Sources gathered; can be re-supplied on request.)
2. **H3/QUIC on the current edge**: EdgeOne's tier has no H3. (Please
   verify if you believe otherwise.)
3. **Origin-side optimization** (RAM tier, segment tuning, concurrency):
   the origin is sub-10 ms on the hot path, so any origin change buys
   at most 10 ms while the edge costs ~200 ms. Judged out of scope.

## What is still open — the actual questions

1. **Is the EdgeOne degradation curve configurable?** Can any EdgeOne
   setting (large-file/media/download optimization, origin-pull mode,
   response buffering) change it? Or is it a platform floor?
2. **Would a different CDN change the curve?** If H3 (e.g. Cloudflare
   free tier) removes ~76 ms of TLS handshake *and* handles large
   responses better, is a CDN swap the right move? Our Pingora front
   already terminates TLS/H2, so this is a configuration-level change —
   but please judge whether the front would need H3 support to matter.
3. **Is the relief valve (307 redirect) the better answer for cold
   large files?** It already exists — `src/business.rs:153`
   (`try_relief_valve`), config `cold_miss = "redirect"`, spec §3.11
   mode A (`docs/spec.md:186-189`). It redirects a cold miss to an
   upstream-issued direct link (bypassing the CDN entirely), filling the
   cache in the background. Is this the right default for large files,
   and what are its failure modes with S3 clients?

## Files to read (all read-only; absolute paths)

| File | Lines | Why |
|---|---|---|
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/docs/spec.md` | 499 | the full contract. **§3 (112-252)** cache semantics + edge measurements; **§3.11 (186-189)** outbound modes A/B/C; **§5.1** StorageBackend trait; **§10 (467+)** acceptance checklist |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/docs/front-requirements.md` | 115 | what the front plane must do; §2 lists the hardening gaps that were then closed |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/front/src/lib.rs` | 610 | the entire Pingora front: ProxyHttp impl, TLS/H2 setup, latency histograms, IP filter, rate gate |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/src/cache/flight.rs` | 356 | **`pump_and_seal_parallel` at 204** — the 5 MB segment mitigation |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/src/cache/cache.rs` | 1803 | the cache core: serve modes, coverage ledger, revalidation |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/src/business.rs` | 1710 | axum plane: **`try_relief_valve` at 153**, S3 routes, prewarm |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/src/metrics.rs` | 110 | latency histograms (backend call, cache serve) |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/src/backend/mod.rs` | 435 | StorageBackend trait + TimedBackend decorator |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/deploy/metrics-report.sh` | 202 | attribution report tool — measures each segment's latency in 30 s |
| `/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin/docs/adr/` | 3 files | design decisions + rejected alternatives (routing, cache core, harness) |

## Honest notes on my own measurements

- All numbers were measured **from the origin node itself**, not from a
  domestic Chinese client. The node→EdgeOne path is not the same as
  client→EdgeOne. Do not extrapolate to end-user experience.
- An earlier dataset ("600× variance") was measured while a **broken
  local proxy** (`ALL_PROXY=127.0.0.1:2080`) was set and is **discarded** —
  do not cite it. All current measurements use `--noproxy '*'`.
- The latency histograms measure **TTFB / stream-established**, not
  transfer throughput. Throughput numbers come from separate `curl`
  runs, not from continuous metrics.

## What I want back

A written assessment answering the three open questions, and — if you
disagree with the "5 MB segmentation is sufficient" position — a
concrete alternative with its failure modes. You may propose changes to
the architecture, but do not edit the repo; the operator will decide and
the implementing agent will execute.

# Clouddrive-as-Origin

Pull-through origin cache for cloud drives — a general-purpose
static-asset origin that fronts one or more drives through OpenList
(WebDAV). The Microsoft Graph / OneDrive upstream this repo started on is
gone; the upstream is OpenList today, and the client-facing namespace is flat
so adding an upstream never changes a URL.

  <!-- keep the two knowledge files discoverable from the front door -->
Docs worth knowing about before changing anything: `docs/pitfalls.md` (traps this
repo has actually hit, with evidence) and `docs/adr/README.md` (one line per
decision, and which ones amend which).

EdgeOne (or any CDN) origin-pulls `GET /<key>` over HTTPS. On hit the
service streams from local disk; on miss it fetches from the owning
OpenList upstream, streams to the client while writing to disk
(water-pipe), and caches for 20 minutes of inactivity with LRU eviction
under a `max_size` cap. See [docs/spec.md](docs/spec.md) for the full
contract, cache semantics, and the acceptance checklist (verified
2026-09-17, with evidence named per line; §10 gains nine admission-round
lines on 2026-09-19).

Three things the one-paragraph summary above leaves out (2026-09-19):

* **Fill policy is per upstream** (`cache_profile`): `efficient`, the default,
  stages the served windows of ranged misses as sidecars, serves later reads
  from those spans, and fetches them one upstream stream per window (so a shard
  walk costs an open per window, not per request — ADR-0015/0016); `nocache` is
  pure streaming with
  zero disk writes.
* **The magazine only governs what fits.** An object larger than
  `max_size_bytes` cannot be brought into budget by evicting anyone, so it
  is admitted as a *resident stray*: cached while the disk allows it,
  outside the byte budget, reclaimed by the inactivity clock. `bytes` in
  healthz can therefore legitimately exceed `max_size_bytes` by
  `stray_bytes` (ADR-0013/0014).
* **Observability:** `/_internal/healthz` reports the cache state
  (`entries`, `bytes`, `stray_bytes`, `segment_bytes`, disk headroom, …),
  and the front plane's `/metrics` exposes
  `cache_serve_source_total{source}` and
  `cache_body_ttfb_seconds{source}` — the pair that says whether seeking
  is served from this node's disk or from upstream.

## Architecture

```
EdgeOne (HTTPS) ──▶ Pingora front (TLS termination, H2)
                    │
                    └──▶ axum on 127.0.0.1 (all cache semantics)
                              │
                              ├── cache files on disk
                              └── metadata in redb
```

* **Front plane:** Cloudflare Pingora — TLS, HTTP/2, connection
  management, reverse-proxy to localhost.
* **Business plane:** axum / hyper / tokio — single-flight, water-pipe
  streaming, inactive-TTL, LRU eviction, revalidation, negative cache,
  `Range`, observability.
* **Metadata:** [redb](https://github.com/cberner/redb) — pure-Rust
  embedded KV.

Multi-upstream from day one; flat URL namespace with server-side
prefix routing.

## Repo rules

* Whitelist `.gitignore` — everything ignored by default, only
  `!`-negated paths are tracked.
* **No Chinese** in commits, code, or docs (enforced by the pre-push
  hook at `hooks/pre-push`).
* Credentials only via environment variables; the repo never contains
  usable secrets or the origin hostname.

## Quick start

```sh
git config core.hooksPath hooks
# copy config template, fill env refs, then:
cargo run
```

See [docs/spec.md](docs/spec.md) §5–§7 for configuration and
deployment.

# Clouddrive-as-Origin

Pull-through origin cache for OneDrive — a general-purpose image (and
static-asset) origin that fronts one or more OneDrive accounts via
Microsoft Graph.

EdgeOne (or any CDN) origin-pulls `GET /<key>` over HTTPS. On hit the
service streams from local disk; on miss it fetches from the owning
OneDrive upstream via Graph, streams to the client while writing to
disk (water-pipe), and caches for 20 minutes of inactivity with
LRU eviction under a `max_size` cap. See [docs/spec.md](docs/spec.md)
for the full contract, cache semantics, and acceptance checklist.

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

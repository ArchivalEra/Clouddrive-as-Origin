# Clouddrive-as-Origin

[![ci](https://github.com/ArchivalEra/Clouddrive-as-Origin/actions/workflows/ci.yml/badge.svg)](https://github.com/ArchivalEra/Clouddrive-as-Origin/actions/workflows/ci.yml)
[![license](https://img.shields.io/github/license/ArchivalEra/Clouddrive-as-Origin)](LICENSE)
[![rust](https://img.shields.io/badge/rust-edition%202021-orange)](https://doc.rust-lang.org/edition-guide/)
[![clippy](https://img.shields.io/badge/clippy-0%20warnings-brightgreen)](.github/workflows/ci.yml)

**A pull-through origin cache between a CDN and your cloud drives.** One Rust
binary sits behind EdgeOne (or any origin-pull CDN): it answers ranged `GET` /
`HEAD` from what it holds on disk and pulls everything else from the drives
behind OpenList — streaming to the client while it writes. The client namespace
is flat, so adding an upstream never changes a URL.

```text
   viewers ──> EdgeOne (RangeOriginPull, upstream h2)
                 │   origin-pull :7777; the edge stamps X-Origin-Token
                 ▼
        ┌─────────────────────────────────┐
        │ front (pingora): TLS, H2,       │
        │ connection filters, access log  │
        └───────────────┬─────────────────┘
                        │ reverse proxy over loopback
        ┌───────────────▼─────────────────┐
        │ business (axum): the cache      │  staged spans on disk + a ledger
        │ admission, runs, spans,         │◄───── redb (metadata, ACID)
        │ eviction, watch / lease         │
        └───────────────┬─────────────────┘
                        │ WebDAV, loopback only
                    OpenList ──> Google Drive / hundreds of others
```

## The three ideas it is built on

Measured facts first; each one bought a mechanism. Every decision is one line in
`docs/adr/README.md`.

1. **A 200 GiB film never fits, so nothing is ever "promoted".** Staged spans
   *are* the cache: a window's bytes land on disk as a sidecar and are served
   directly from there; the ledger is only a policy map, and the disk is the
   existence authority (ADR-0015/0019).
2. **An upstream open costs ~800 ms, so opens are paid per window, not per
   request.** One run covers a window and every reader inside it rides its
   watermark; a jump pays the floor, a walk pays the ramp (ADR-0016/0024).
   Under random cold load this measured **one open per ~17.6 MB**.
3. **The port is reachable, so admission is a token the edge stamps, not an IP
   table** (ADR-0025). Whoever reaches the port cannot forge the stamp; a
   request without it gets `403`.

## Measured (2026-09-24, the long-form run)

| | |
| --- | --- |
| ten viewers × 4.5 h on the real 200 GiB film (MSE) | 279 sessions, **85% played the whole ten minutes**, origin **zero 5xx**, healthz never degraded |
| seek first byte (p50 / p90) | **267 ms / 1.24 s** |
| random cold load, 4 h, 8-way | 96,200 × 5 MiB = **469.5 GiB**, 99.99% served |
| what the CDN absorbed | **99.8%** of the viewer traffic (playlist shape; the shard-walk shape differs — see the runbook) |
| the local shape check | 5 MiB shards at **7.06–7.1 MB/s, zero gaps**, checksums verified |

## What it is not

- **Not a media server** (ADR-0026): no content types, no container parsing, no
  manifests, no player. Bytes are the interface; a viewer's player is the
  client's program.
- **Read-only by construction**: there is no upload path, and write verbs answer
  `405`.
- **No CORS**: a browser page must be served from the same origin as the objects
  it fetches.
- **One node, accepted** (ADR-0010): at this size one node is the right shape;
  redundancy is a decision, not a defect.

## Quick start

```sh
cargo build --release --offline
bash deploy/lab/run-lab.sh --smoke      # PASS=35 FAIL=0 in about a minute, no cloud
```

To stand up a node, point a CDN and clients at it, and take its account, read
**`docs/usage.md`**. An agent can carry the same operations in
`skills/clouddrive-origin/SKILL.md` (symlink it into `~/.zcode/skills/`).

## The interface clients get

- ranged `GET`/`HEAD` on `/<key>`: `206` with a correct `Content-Range`;
  open-ended ranges answered;
- an S3-shaped listing (`?list-type=2`);
- a per-key diagnostic view (`/_internal/healthz?key=…`, loopback only);
- strict config (`deny_unknown_fields`, fail-closed boot on a missing env var),
  a constant-time admission token, and a pre-push hook that keeps the tree
  CJK-free with a clippy budget of 0 — CI enforces both as well.

## Code map

| path | what lives there |
| --- | --- |
| `src/cache/` | the response decision, the one ranged body, spans, runs, ledger, magazine, protection |
| `src/business.rs` | the request plane: key resolution, headers, outcomes |
| `front/` | the pingora front: TLS, H2, connection filters, access log, metrics |
| `src/backend/`, `src/list.rs`, `src/sigv4.rs` | the OpenList/WebDAV upstream, S3 listing, SigV4 verification |
| `deploy/` | every instrument, indexed in `deploy/README.md` |

## Docs

`docs/usage.md` (use it) · `docs/spec.md` (the contract) ·
`docs/adr/README.md` (decisions) · `docs/runbook.md` (measurements) ·
`docs/pitfalls.md` (traps, with evidence) · `docs/security-hardening.md` (the
exposure and its closures) · `deploy/README.md` (instruments).

## License

AGPL-3.0 — see [LICENSE](LICENSE).

Built, measured, and shipped on free tiers only. 😆🍻😆

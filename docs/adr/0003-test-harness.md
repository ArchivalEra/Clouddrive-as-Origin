# Test harness & env contract (D3)

Decided after R1–R3 + D1 + ADR 0002. The harness must exercise every item in `docs/spec.md` §10 without touching real OneDrive in CI.

## Harness

- **Fake Graph**: `wiremock` against `http://127.0.0.1:<mock-port>/v1.0` (configurable `graph_base_url` in tests). Mocks `GET /me/drive/root:<drive_root>/<key>` (metadata + `@microsoft.graph.downloadUrl`) and `GET {downloadUrl}` (bytes + `Range`). Scriptable per test: inject `429` with `Retry-After`, `500`, `404`, `304` vs `200` on `If-None-Match`, ETag changes, redirect chains.
- **Clock**: injected `Clock` trait (real vs accelerated). Production uses wall clock; tests advance the clock to hit `inactive_ttl` (20 min), `revalidate_ttl` (60 s), and `negative_ttl` without sleeping. ADR 0002's `last_access` coalescing is disabled in tests (flush immediately) so assertions are deterministic.
- **Temp dirs**: each test gets `tempfile::tempdir()` as `cache_dir`; redb file lives inside it. No shared state between tests.

## Acceptance → harness mapping (§10)

- 20 concurrent same-key → assert exactly 1 metadata + 1 download call on the mock.
- 20 min idle → advance clock, `tick()`, assert file + redb rows gone, `du` equivalent zero.
- max_size fill → write until over cap, assert eviction order = ascending `eligible-at`.
- OneDrive mutation → mock returns new ETag/`200` on revalidation, assert new bytes with no gap (old file served until rename).
- Upstream 500 → serve stale file with `Warning: 110`, else `502`.
- Traversal payloads (`..%2f` etc.) → `400` before routing.
- Restart survival → close `Cache`, reopen same `cache_dir`, assert hits + eviction order preserved.
- Logs/healthz secret leakage → assert no token/secret/`downloadUrl` in log output or `/_internal/healthz` body.
- Two-upstream routing → keys with different prefixes hit different upstream mocks; adding a third upstream only requires a new `[[upstreams]]`+`[[routes]]` entry.

## Env contract

- **CI (GitHub Actions)**: no real OneDrive; all tests use `wiremock` + accelerated clock + temp `cache_dir`. `cargo test` is the gate.
- **Pre-launch smoke (OCI VPS, operator)**: one manual run with `ORIGIN_HOST` env, two real OneDrive upstreams (delegated refresh tokens via `*_REFRESH_TOKEN` env, `consumers` tenant), EdgeOne → `${ORIGIN_HOST}` over HTTPS. Verifies device-code bootstrap, real downloadUrl fetch, and `429`/retry behavior against production throttling.
- **Real hostname `${ORIGIN_HOST}` and all secrets** remain env-only (never in repo), consistent with the standing rule.

## Out of scope for this harness

- EdgeOne site provisioning and `${ORIGIN_HOST}` DNS (operator, §9).
- Upload pipeline that calls `/_internal/prewarm` (ships later; endpoint itself is covered by cache tests).

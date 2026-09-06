# Clouddrive-as-Origin

Pull-through origin cache for OneDrive. EdgeOne CDN origin-pulls `GET /<key>` over HTTPS; the service serves from a local hot cache backed by one or more OneDrive accounts via Microsoft Graph (delegated auth).

## Language

**Upstream**: A single OneDrive account (Entra app registration + delegated refresh token) that owns a slice of the keyspace. One upstream = one token file, one Graph concurrency limiter, one refresh single-flight. _Avoid_: vault, drive, account (overloaded).

**Key**: The flat, relative path in the request URL after `/` (e.g. `2026/08/image.png`). The client's only address. Never an absolute path, never traversal outside the resolved upstream's `drive_root_path`. _Avoid_: path, object key.

**Route**: A first-match prefix rule that maps a key prefix → upstream id. Longest prefix wins; empty prefix is the default. Routing is server-side; adding an upstream never changes existing URLs.

**Water-pipe**: Cold-miss streaming where the service writes to disk while streaming to the client in the same origin fetch. Forbidden to buffer fully before responding.

**Revalidation**: Conditional Graph check (`If-None-Match` / `ETag`) triggered when a cached entry is older than `revalidate_ttl`. `304` resets the inactive clock; `200` atomically replaces the cached file.

**Inactive TTL**: Lifetime of a cached entry measured from last access; expiry evicts the file and its redb metadata. _Avoid_: idle timeout.

**Eligible-at**: `last_access + inactive_ttl`, the timestamp at which an entry becomes eligible for expiry eviction. LRU/max_size eviction walks entries in ascending eligible-at order.

**Negative cache**: A cached 404 tombstone for a key whose upstream confirmed absence; served without an upstream call until `negative_ttl` expires.
**Stale-if-error**: On upstream `5xx` or network error, serving the last cached copy with a `Warning` header when available, otherwise `502`.
**Front plane**: The Pingora process that terminates TLS/H2 and reverse-proxies to the business plane on `127.0.0.1`. Owns no cache semantics.

**Business plane**: The axum service on `127.0.0.1` that owns all cache semantics (single-flight, water-pipe, redb, eviction). Single `redb::Database` handle, single writer via async mutex.
**Coverage ledger**: Per-key transfer history under an efficientcache profile — which byte intervals have been served and staged as sidecars, under which object version. Files on disk are authoritative; memory is a rebuildable view.
**Coverage-triggered promotion**: Background assembly of a full cache entry once staged coverage ≥ threshold: version re-verified by fresh stat, gaps fetched by exact Range, sealed, installed, staged history cleaned. Never a mixed-version file.

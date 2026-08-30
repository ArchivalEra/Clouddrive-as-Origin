# Cache-core module design (D2)

Decided on the R1/R2/R3 + D1 inputs. This ADR freezes the shapes D3's test harness will mock against.

## Module tree (business plane, `src/`)

```
src/
  main.rs       — binary entry, planes wiring
  config.rs     — TOML → Config (already exists, P1)
  business.rs   — axum router assembly
  cache/
    mod.rs      — public Cache API (get_or_fetch, prewarm, health, tick)
    store.rs    — disk layout (cache_dir/<key> files, .tmp.<key>.<rand> temps)
    meta.rs     — redb schema + EntryMeta codec
    inflight.rs — per-key single-flight (DashMap-keyed JoinSet / OnceCell)
    fetch.rs    — Graph metadata + downloadUrl fetch, host allow-list, retry/backoff
    evict.rs    — inactive reaper + max_size LRU scanner (both walk by_last_access)
  upstream/
    mod.rs      — Upstream registry, Route table (longest-prefix), per-upstream
                  concurrency semaphore (≤3) + token state
    auth.rs     — delegated refresh (proactive T-5m, reactive 401 single-flight,
                  0600 atomic persist, needs_reauth surfacing)

Observability (tracing) and key validation (traversal rejection) are free
functions on the cache boundary, not modules.
```

`cache::Cache` is the only public surface: `get_or_fetch(key) → Body`, `prewarm(key)`, `health()`, `tick()` (drives both reapers). All redb writes go through a single `tokio::sync::Mutex<Database>` (R1: single writer). `last_access` bumps are coalesced in memory and flushed every 500–1000 ms (R1).

## Disk layout

```
<cache_dir>/
  redb.db                     # single redb file (cache_dir/redb.db)
  tokens/<upstream-id>.json   # 0600 (auth.rs)
  <key>                       # e.g. 2026/08/image.png — the cached bytes
  .tmp.<key>.<rand>           # partial download, removed on startup scan
```

`store.rs` atomically replaces on revalidation-miss (`write .tmp + fsync + rename`). Startup deletes all `.tmp.*`. Parent dirs pruned when empty after eviction/expiry.

## redb schema (R1 recommendation adopted)

Tables in one `Database` (`cache_dir/redb.db`):

- `entries: Table<&str, &[u8]>` — key `"<key>"` → postcard `EntryMeta` (versioned).
- `by_last_access: Table<&[u8], ()>` — key `BE64(last_access_millis) || 0x00 || key_bytes`, value `()`. Sorted = eligible-at order because `inactive_ttl` is global.
- `globals: Table<&str, &[u8]>` — `total_bytes`, `entry_count` (avoids full scan on startup/healthz).

`EntryMeta` (postcard/bincode, versioned):

```
version, upstream_id, rel_path, size_bytes, etag, last_modified,
content_type, created_at_millis, last_access_millis,
last_revalidated_millis, negative_until_millis (Option)
```

Writes update both `entries` and `by_last_access` in one `WriteTransaction` (delete old index row, insert new). Negative 404s are `entries` tombstones with `negative_until`, no file, no `by_last_access` row.

Reaper (inactive): range `by_last_access[..now - inactive_ttl]` in chunks 500–1000 per txn, deletes file + both tables + prunes dirs, chunked to bound txn size. Evictor (max_size): walks `by_last_access` from smallest upward until `total_bytes ≤ max_size`.

## Single-flight & concurrency

- `inflight.rs`: per-key coalescing — first waiter fetches, others await the same `Shared` future (e.g. `tokio::sync::OnceCell` / `async_once_cell`). Different keys do not block. Token refresh has its own per-upstream single-flight in `upstream/auth.rs` (global per upstream on concurrent 401).
- `upstream`: per-upstream `Semaphore(3)` gates all Graph calls; `fetch.rs` honors `Retry-After` exactly, otherwise jittered exponential backoff capped ~30 s (R3). Host allow-list suffixes from `config.allowed_download_suffixes` (default `.files.1drv.com`, `.sharepoint.com`, `storage.live.com`, R3).

## Water-pipe, Range, negative, stale

- Water-pipe: `fetch.rs` streams `downloadUrl` bytes; `cache::Cache::get_or_fetch` tees to `store.rs` file and to axum `Body` (chunked) in the same fetch. `Content-Length` from Graph metadata is sent up front.
- Range: cached file → slice bytes; cold miss with `Range` → still fetch whole file (spec lenient).
- Negative cache: 404 tombstone in `entries` for `negative_ttl` (60 s); no file.
- Stale-if-error: on `5xx`/network, serve cached file with `Warning: 110` if present, else `502`.

## Alternatives considered

- Sidecar-per-entry JSON (no DB): rejected per R1 — 10k+ entries would scan the whole tree on reaper/evictor/healthz; redb's ordered index is O(n) page walk.
- Separate `MultimapTableDefinition` for `by_last_access`: rejected — composite key in a regular table keeps keys unique and deletion of the old index row trivial.
- `Durability::None` batching: rejected — `Immediate` with coalesced last_access flush is durable enough (≤1 s loss is OK, OneDrive is source of truth).

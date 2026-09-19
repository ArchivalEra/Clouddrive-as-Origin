# Disk capacity: staged bytes share the budget, with a reserve floor

Decided while resolving the production-readiness map's disk-capacity ticket
(#56). Extends ADR-0006 (entry-count budget) with the on-disk side.

## What was wrong

The cache had two counters and only one budget:

- `total_bytes` — the byte size of completed cache entries. This is what
  `evict_pick` watched.
- `segment_bytes` — the size of staged `.seg` sidecars (the efficient
  profile's transfer history). Its doc comment said "swept by age, never
  evicted by LRU", and nothing else bounded it.

So an efficient-profile scrub session could stage arbitrarily many sidecars
within one `inactive_ttl` window (default 20 min) while `total_bytes` stayed
at or near zero. `max_size_bytes` was never consulted. A high-volume scrub
could therefore fill the disk with the cache reporting itself comfortably
inside budget.

Two related leaks compounded it:

- `pump_and_seal` returned `Err` on a mid-write failure **without removing
  its temp file** (only the short-body branch cleaned up), and
- the tmp sweep (`cleanup_tmps`) ran **only at startup**, so a leaked temp
  survived until the next restart — days, on a healthy node.

## Decisions

1. **Staged bytes share the disk budget.** `tick` now checks
   `total_bytes + segment_bytes > max_size_bytes` and, when over, evicts
   ledger rows oldest-touched-first (the same LRU shape as entry eviction).
   Ledger bytes are the accounting authority for `segment_bytes` — it is
   incremented by `finalize_coverage` and rebuilt by `scan_segments` — so
   eviction subtracts the ledger's byte count, not whatever a disk scan
   happens to find.

2. **A reserve floor guards new transfers.** Before starting a cold pull,
   the driver checks free space (`statvfs`) and refuses when
   `free - reserve < object_size`. The reserve is 512 MiB, so the node
   keeps room for logs, the redb file, and an operator's emergency shell.
   The client gets a clean upstream error instead of a mid-body failure.
   Unknown free space does **not** block serving: the write remains the
   backstop.

3. **Temps are swept every tick, age-guarded.** `cleanup_stale_tmps`
   removes `.tmp.*` older than `inactive_ttl`, so a leaked temp is reclaimed
   within one TTL rather than at the next restart. The age guard is what
   keeps it safe: an in-flight pull's temp is young and must never be
   deleted underneath its driver.

4. **Any pump failure removes its temp.** The read/write/flush/fsync error
   paths join the short-body path in cleaning up.

## Active-staging guard

Eviction skips any ledger row touched within the last 60 s
(`STAGE_MIN_AGE_MS`). Without it, a transfer that is staging right now could
have its history yanked mid-flight. Rows that have gone quiet for a minute
are fair game.

## Consequences

- The disk is bounded by `max_size_bytes` across both counters, plus a
  time bound on how long staged history can linger.
- A cold pull on a nearly-full disk fails fast and cleanly rather than
  writing until the filesystem errors.
- `libc` is declared explicitly for `statvfs`. It was already in the
  lockfile via redb/tokio, so no new crate enters the build.

## Not decided here

Whether staged bytes deserve their own budget separate from entry bytes
(a single shared budget means a heavy scrub can evict completed entries).
Observed traffic has not shown that pressure; if it does, split the budget
rather than changing the eviction order.

## Amendment (2026-09-19): "staged bytes share the budget" means the magazine's budget

The shared budget is `resident_bytes + segment_bytes > max_size_bytes`, where
`resident_bytes` excludes resident strays (ADR-0014). A stray is an object the
magazine cannot hold at all, so counting it would leave the cache permanently
"over budget" and make every insert evict someone else for a cap that object
was never subject to.

The disk side of this ADR is unchanged and now has a counterpart: the tick
reclaims strays when free space falls below a working floor (reserve + 2 GiB),
oldest-touched first, and cold-pull admission makes room the same way before it
concludes a request cannot be cached (ADR-0013). Strays are the only population
either mechanism takes, because the byte budget already governs the magazine's
own members.

# Upstream concurrency: two budgets, not one

Decided after the 2026-09-12 performance review (finding P2). Supersedes the
single-gate statement in ADR-0002 ("per-upstream Semaphore(3) gates all Graph
calls").

## What was measured before deciding

- One cold stream: **27.4 MB/s**. Three concurrent streams on distinct cold
  files: **57.9 MB/s aggregate (~2.1x)**. Concurrency does stack, but only
  about twofold — the configured `concurrency_per_upstream = 3` already sits at
  the knee of the curve.
- Every upstream `open` carries a **~640 ms** fixed cost (4 opens summed 2.56 s
  while moving 1.6 GB).
- With the single shared gate: an **idle HEAD took 22 ms**, and the same HEAD
  took **14.2 s** while three cold pulls held the permits.

## Decision

Split the per-upstream gate into two independent semaphores on `BackendSlot`,
each sized to `concurrency_per_upstream`:

- **Metadata gate** (`gate`): `stat`, HEAD, `list`, direct-link lookups. Short,
  bounded, latency-sensitive.
- **Stream gate** (`stream_gate`): cold-miss pump, nocache/passthrough staging,
  promotion assembly. Long-lived, bandwidth-bound.

## Why both halves matter

Raising the shared limit would not have fixed the measured fault: the problem
was scope, not size. A big-but-single pool still lets three long transfers
occupy every permit, and metadata then waits behind bytes. Splitting keeps the
upstream's total pressure at the same configured value while removing the
head-of-line blocking.

## Consequences

- Upstream load is unchanged: the same total permits exist per upstream, just
  partitioned by kind.
- A cold pull can no longer delay a HEAD, a list, or a revalidation stat.
- Config semantics change: `concurrency_per_upstream` now sizes each budget, so
  a node may see up to `2 * concurrency_per_upstream` concurrent upstream calls
  (metadata + streams). Accepted: metadata calls are cheap and short.
- Verified on the node after the change: HEAD during three long pulls went from
  14.2 s to **0.239 s**.

## Not decided here

Cold ranged misses still fetch the whole object, so a seek holds a stream
permit for the full pull. Whether the pull should start at the requested offset
is tracked separately in `docs/notes/c5-pump-start-options.md`; it needs
EdgeOne cold-request ordering data before it can graduate.

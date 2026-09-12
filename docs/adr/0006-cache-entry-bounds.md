# Cache bounds: an entry-count budget alongside bytes

Decided after the 2026-09-12 performance review (finding P10).

## Problem

Eviction watched one budget: `max_size_bytes` (default 100 GiB). Entry rows
cost roughly 500 B of RAM each — the key is stored both as the map key and
inside `EntryMeta`, alongside up to four more `String` fields. A node could
therefore hold millions of small objects far under the byte cap and exhaust
memory. The coverage ledger had the same shape of problem: one row per distinct
staged key, unbounded, with `add_interval` re-sorting the whole interval vector
on every shard.

## Decision

1. **`max_entries` is a second eviction budget** (default 500,000; `0`
   disables it). `evict_pick` now drains while *either* budget is over, evicting
   least-recently-eligible first, so the count cap never sacrifices recency
   ordering.

2. **The coverage ledger merges instead of rebuilding.** `add_interval` inserts
   at the sorted position and merges only the touched neighbours, replacing a
   push-sort-rebuild that cost O(k² log k) over a scrub session.

3. **healthz reports ledger size** (`coverage_keys`, `coverage_intervals`) so an
   operator can see the structure growing before it becomes a problem.

## Consequences

- A deployment with many small objects is bounded by count as well as bytes.
- The interval semantics are unchanged: adjacent (touching) intervals stay
  separate so each keeps its own read time for window decay. This is pinned by
  a test, because the merge rewrite is exactly where that invariant could have
  been lost.
- `max_entries` has a default rather than being required, so existing configs
  keep working; the default is generous enough that byte-bounded deployments
  are unaffected.

## Not decided here

Whether the ledger needs a hard row cap (as opposed to the age sweep it already
has). The `inactive_ttl` sweep removes rows for keys that stopped being read;
a cap would matter only for a workload that touches unbounded distinct keys
within one TTL window, which has not been observed.

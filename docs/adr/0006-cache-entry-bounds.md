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

## Amendment (2026-09-19): the coverage ledger has a ceiling

This ADR left the per-key interval count open ("Not decided here"). It is
decided: `MAX_INTERVALS_PER_KEY` (4096), enforced inside `add_interval`.

Adjacent staged shards are deliberately kept as separate intervals so each
keeps its own read time for window decay, so a sequential scrub grew one key's
vector with the request count — and every staged transfer walked that vector
two or three times under the single process-wide ledger lock. Compaction merges
touching spans first (exact: `[a,b) + [b,c) = [a,c)`, no byte count changes,
and this is what a sequential walk produces). If spans with gaps remain,
merging them would claim coverage of bytes we do not hold, so the coldest are
dropped instead: under-reporting coverage is the safe direction, promotion
waits for real bytes rather than assembling a gap it cannot fill, and it is the
same trade window decay already makes.

`decay` and `covered_bytes` also became one pass, `decay_and_covered`, whose
result is handed to the promotion check — which used to run its own decay and
its own coverage walk, with the same window and clock, immediately after.

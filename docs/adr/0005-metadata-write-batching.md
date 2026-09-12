# Metadata store: batched transactions, serde_json, redb default durability

Decided after the 2026-09-12 performance review (finding P5). Amends ADR-0002's
`EntryMeta` row.

## What was measured before deciding

The coalescing window bounded *ticks*, not *commits*: the access-clock flusher
drained a second's worth of dirty keys and then issued one `WriteTransaction`
(and therefore one fsync, since redb's `Durability` default is `Immediate`) per
key. Eviction and expiry did the same per victim.

## Decisions

1. **Batch the transactions.** `bump_last_access_batch` and `remove_batch` cover
   a whole drained batch in one write transaction. The per-key entry points
   remain, delegating to the batch form, so no call site had to change shape.

2. **Format is serde_json — and the docs now say so.** `src/cache/meta.rs` and
   the original ADR-0002 text claimed "postcard in production"; every call site
   has in fact used `serde_json` since the beginning. The claim is removed
   rather than the format changed: the redb rows are rebuildable metadata, the
   parse cost is not on any request path (reads come from memory), and adding a
   binary codec would buy little at the cost of a dependency and a migration.

3. **Durability stays at redb's default.** ADR-0002 already rejected
   `Durability::None`; that reasoning still holds and is not reopened here. With
   batching, the fsync frequency is now proportional to flush ticks rather than
   to hit count, which was the real cost.

## Consequences

- A second's worth of access-clock work commits once.
- Eviction/expiry bursts commit once per sweep instead of once per victim.
- The stored row still duplicates `key` inside the value (the redb key is the
  same string). Left as is: removing it would be a schema change for a few tens
  of bytes per entry, and the row is rebuilt on startup regardless.

## Not decided here

Whether the blocking commit should move off the async worker
(`spawn_blocking`). It is a real cost but a separate, smaller change; the
measurement that would justify it is fsync-wait time on the redb mutex during a
large flush, which has not been collected.

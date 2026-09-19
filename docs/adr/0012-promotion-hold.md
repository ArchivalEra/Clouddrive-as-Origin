# A promoted entry carries a hold

## Context

Promotion is the one write that pays for many upstream fetches: the efficient
profile stages a key's served intervals as sidecars, and when coverage passes
the threshold it assembles them into a whole cached object. The inactivity
clock (`inactive_ttl_secs`, 20 minutes) has no way to know that. Watch a video
for a while, pause it to look at something else, come back: the merge you just
paid for is swept as "not touched recently", and the next seek re-fetches from
the provider.

The same clock drives eviction order, so a fresh merge also competes for the
magazine on terms that ignore its cost.

## Decision

**A promoted entry is held for `promoted_hold_secs` (default 1800, 0 = off).**
While the hold runs the entry is immune to the inactivity TTL and sorts last
in the eviction order. The deadline is written into the row
(`EntryMeta::hold_until_millis`), so it survives a restart.

**The hold is a deadline, not an exemption**, and that is the whole reason it
is safe to add. It is implemented as one term in the eviction sort key —
`eligible_at = max(last_access + ttl, hold_until)` — so a held entry is picked
*last*, not *never*. When nothing else can bring the cache back under budget,
the hold yields. An absolute immunity would let a single protected object pin
the magazine permanently over its cap and evict everything else to make room
for something that cannot fit; the sort-key formulation gets the protection
and keeps the promise.

**Promotion refuses what the magazine could never keep.** `maybe_promote` now
requires `total <= max_size_bytes` as well as the coverage ratio. Without it
the hold would be asked to protect something that cannot fit, and a
"successful" merge would consume the staged sidecars to hand the cache an
entry it must eject on the next tick — destroying warmth that the segments
were already providing.

## Consequences

- A merge survives a pause. That is the point: the cost and the inactivity
  clock stop being coupled.
- The cache can be over budget for up to the reaper interval (60 s) after a
  promotion, as it already could after any insert; the magazine then ejects
  the least-recently-touched rows, held ones last.
- An object larger than `max_size_bytes` is never promoted, so an object that
  can never be cached whole stays a set of segments, which is what serves a
  seek anyway.
- `hold_until_millis` is `#[serde(default)]`: rows written before this field
  existed have no such key, and without the default the store would fail to
  deserialize on the first start after an upgrade.

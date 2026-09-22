# The window decision: a floor for a jump, a ramp for a walk

Amends ADR-0016 (what a run opens is now a decision rather than the configured
constant, and a window boundary hands over while its predecessor is still
pumping) and leaves ADR-0019's cap arithmetic untouched.

## Context

Measured on the node against a real 200 GiB object (2026-09-22):

- One 5 MiB read at a cold offset took `segment_bytes` up by exactly
  67,108,864 bytes — one `session_window_bytes` — a **12.8x amplification**: 59
  MiB of read-ahead was fetched, staged and later evicted for 5 MiB anybody read.
- The next 5 MiB inside that window cost 0.04 s: no upstream open, no new bytes.
- Through the CDN the same object, client and route moved **7.06 MB/s in 5 MB
  shards with zero gaps** (four concurrent viewers, distinct offsets, checksums
  4/4), against the 1.94 MB/s sustained the film needs to play. The shape the
  product promises was already fine; the read-ahead a *jump* pays for was the
  waste.

The policy also had no home: the same question was answered in four places —
`session::Sessions::start` (the run's own window), `cache` (the write admission),
`staging::working_window_bytes` (the cap an un-keepable key may hold) and
`session::CHAIN_KEEP_AHEAD_WINDOWS` (the chain's bound) — each deriving it from
`session_window_bytes` independently.

## Decision

**One module answers how far ahead a run reads: `cache::window`.**

**A run with nothing behind it opens the floor.** `window_floor_bytes` (default
8 MiB — eight of the 1 MiB shards a CDN asks for), clamped to
`session_window_bytes`, so a config with a smaller window keeps its exact old
behaviour. A jump pays for the read-ahead it uses.

**The floor is a floor, never a cap.** A request wider than it gets what it asked
for (`a_need_larger_than_the_window_widens_it` still holds), so one large
response still costs one open.

**A window that gets read out doubles the next one**, capped at
`session_window_bytes`. The input is the replaced run's own account — what it
covered and how far its readers actually got (`window::behind_of`) — which is
visible exactly once, under the lock that reaps it. A sequential walk therefore
climbs 8 → 16 → 32 → 64 MiB and converges on one open per configured window; a
window left half-read, or a viewer that never arrived, falls back to the floor.

**A request that begins outside the run it replaces is a jump**: it opens the
floor however that run ended, so a scrub cannot inherit the ramp of a region it
is not continuing.

**A window boundary hands over.** A request that begins exactly where a *live*
run's window ends reaps and replaces it, instead of escaping to its own exact
Range until that run seals. This is what makes a small floor affordable: without
it the LAB measured a 24 MiB ascending shard walk at **8 opens (one per three
shards)** — every boundary crossing escaped — against 1 open with a whole-window
floor. The predecessor is undisturbed: its driver keeps pumping into its own
`.segpart`, seals on its own, and its `finish` is pointer-checked, so it cannot
clear the successor's entry. A seek that does *not* continue the window (a gap, a
far offset) still takes the escape, which is what keeps several viewers on one
key from thrashing the slot.

**Admission still asks about the worst case** (`window::worst_case_bytes` =
`max(need, session_window_bytes)`): the ramp's input is read after admission has
answered, and guessing low there would over-commit the disk. The ramp shrinks the
WRITE; it does not shrink what admission must be able to afford.

**The cap arithmetic is unchanged.** `window::reserve_bytes` =
`watch_pin_bytes + session_window_bytes`: the ramp only ever shrinks a live
window, so the configured value stays the upper bound and ADR-0019's ">= the pin
by construction" still holds.

## Consequences

- **A jump stages the floor** (8 MiB) instead of a window: the 12.8x becomes
  1.6x for a 5 MiB read, and the shared cold-fill budget an edge spends per scrub
  drops with it.
- **A walk climbs and converges**: 8 → 16 → 32 → 64 MiB, one open per window
  once warm. The first windows of a walk cost one or two extra opens against the
  old policy — the price of not paying a window for a jump.
- **Every pre-existing test passed unchanged**, because their windows are smaller
  than the default floor and the floor then clamps up to the window: the ramp is
  invisible to a small-window configuration, which is what makes the LAB's
  `config-c` (256 KiB) sections still mean what they meant.
- **Reverse verification**: pinning `floor_bytes` to the configured window (the
  pre-ADR policy) turns seven of the new tests red — three in `cache::window`,
  four in `cache::session`, one integration — and leaves every pre-existing test
  green, so the new tests measure the ramp and nothing else does.

## What this does not cover

- **The first window after a jump is the floor**, so a reader that jumps and then
  reads more than the floor pays one extra open for the remainder.
- **A preempted run loses its chain.** A far seek that takes the slot over a live
  run means that run can no longer chain its successor (the entry is no longer
  its own); its readers are unaffected.
- **The seek's first-byte latency is untouched, and measured (2026-09-22)**: a
  cold jump's first byte is 727-839 ms at the origin, of which the provider's
  `open` is 806 ms in-window (824 ms mean over 165 calls) and the provider's
  `stat` is 1 ms in-window (5.8 ms mean over 273 calls). Our own overhead is not
  measurable in it. So the only lever left would be the open, which ADR-0016
  rejected on the grounds that chasing it changes what a run IS — and that
  judgement now has the account it asked for. ADR-0023's stat is not a lever
  either: see the note on that proposal.
- **The ramp is per run, not per key**: it has no memory of a viewer who returns
  to a key an hour later — that read is a jump again, and takes the floor.

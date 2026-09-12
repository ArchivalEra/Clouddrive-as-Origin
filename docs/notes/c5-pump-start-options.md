# C5 — flight pump start: options and open gate

> Status: PROPOSAL, not a decision. The audit (2026-09-12) marked the
> pump-start idea Speculative; this memo records why it is parked and
> what evidence would graduate it. No code implements any option here.

## The friction

A cold-miss flight always pumps from offset 0. A ranged cold miss at a
high offset converges on the flight and waits for the writer to reach
its offset (single-stream discipline, ~800 ms per upstream open). So a
cold seek at 700 MB of an 803 MB file pays TTFB = time-to-pull-700-MB,
although the seek itself only needs the last 100 MB.

Why this may not matter: EdgeOne's sharded origin-pull delivers shards
in ascending offset order, so on the standard viewer path the seeks a
flight sees are already roughly ascending — the writer reaches each
requested offset about when the request arrives. The storm that broke
multi-client serving used scattered offsets (seek-storm pseudo-random),
which is not the EdgeOne-shaped pattern.

## Feasibility (already proven)

Ranged upstream open from an arbitrary offset works: `openlist::open`
sends the Range header and the earlier segment measurements (100 MB x31,
5 MB x614) were all offset-anchored ranged opens on the real
OpenList -> Google Drive hop. Starting a pump at S is not a new upstream
capability.

## Options (for grilling; none is implemented)

- **A1 — pump start = min(known reader offsets), late-low readers open a
  second ranged stream for the missing prefix.** Fastest TTFB for every
  reader. Cost: breaks the one-upstream-open-per-key invariant for that
  key (one extra open, bounded by readers arriving below the pump start);
  seal logic must merge two streams.
- **A2 — pump start = min(offset of readers already attached at Meta
  time). Later, lower readers wait, as today.** No new failure modes, no
  extra opens; helps only the first wave of an offset-ascending storm.
  The late-low-reader hole (waiting for [0, start)) remains by design.
- **A3 — keep 0-start, restrict the feature to prefetch.** Prewarm
  passes a target offset; regular GET storms behave exactly as today.

## Evidence needed to graduate

1. metrics-report.sh over a real EdgeOne cold pull: the distribution of
   request offsets (ascending? clustered near the head?) and the TTFB
   vs offset curve.
2. A measured storm in EdgeOne-shaped order (ascending) vs scattered
   order, on a cold 800 MB file, to see whether the wait is actually
   near-zero in the shape that matters.

## Recommendation

A2 if the data shows real cold storms at all; drop C5 entirely if
EdgeOne ordering keeps the wait near zero. A1 only if high-offset storms
dominate AND the extra open is acceptable per-key. Decision deferred to
the operator; re-open after T7 acceptance data exists.

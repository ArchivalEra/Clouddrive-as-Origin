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

**CLOSED 2026-09-12: drop C5. The data settles it.**

## The measurement that closed it

Captured EdgeOne's origin-pull requests on the node (loopback 8080 is
plaintext, so `tcpdump -i lo -A 'tcp port 8080'` sees the forwarded
request headers verbatim) while a client pulled a cold 100 MB object
through `cdn-oracle.isui.ren`.

Result — 43 ranged requests, all **exactly 1 MiB**, strictly ascending
from zero:

```
range: bytes=0-0                 (a 1-byte probe first)
range: bytes=0-1048575
range: bytes=1048576-2097151
range: bytes=2097152-3145727
...  (contiguous, +1048576 each step)
range: bytes=44040191             (cut off at the 80 s client timeout)
```

So the premise C5 was built on holds: EdgeOne's sharded origin-pull walks
the object front to back in fixed 1 MiB shards. A flight's writer is
therefore always at or ahead of the offset the next shard asks for, and
the "reader waits for the writer" cost is **already near zero on the
real path**. Starting the pump at a non-zero offset would buy nothing
measurable while adding an extra upstream open and a two-stream seal.

Two further points from the same capture:

- The pull **stalled at 43.8 MB after 80 s** of a 100 MB object, which is
  the already-documented EdgeOne edge-segment limit (a separate, older
  finding), not a pump-start problem.
- Because every shard is 1 MiB, the whole-file pull strategy is
  unaffected: N shards from EdgeOne converge on one flight exactly as
  designed.

## Status

No code will implement A1/A2/A3. The friction C5 described is real only
for **scattered** offset patterns, and those do not come from EdgeOne —
they come from an artificial seek storm, which is a test shape rather
than a production one. If a future workload does produce genuinely
scattered cold seeks, revisit with that evidence.

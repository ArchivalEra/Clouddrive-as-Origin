# One upstream stream per key: staged-read runs

Amends ADR-0015 (the staged spans a run seals are the spans that ADR already
made servable) and closes the ranged half of ADR-0004's stream budget.

## Context

Measured: an upstream `open` costs a fixed ~640 ms whatever the range length,
and the node pulls ~63 MB/s from the provider. The efficient profile opened an
exact Range per ranged miss — one `open` per request — so a viewer's shard walk
paid that fixed cost once per shard. EdgeOne's sharded origin-pull asks for
ascending 1 MiB shards, so a 200 GB walk was ~190k opens, most of them charged
for bytes the node already held or was about to hold.

The opposite shape already existed: `flight` pumps one upstream stream into a
temp file and lets every reader park behind a watermark, which is why whole-file
cold pulls share one stream. The ranged path bypassed it, and could not use it
as it stood: a whole-file pump is only admissible for an object the node will
hold, and the target objects (30–200 GB on a 10 GiB magazine) are exactly the
ones it will not.

## Decision

**A run is one upstream stream covering a window.** The first ranged miss on a
key starts a run at its frontier: one `open` of `[start, start + window)`,
pumped through the existing flight machinery (watermark publication,
inactivity-bounded reader waits, fsync + rename, panic guard) and sealed into a
`.seg` span when it ends. Every request whose range falls entirely inside a live
run's window is served from that run's watermark at no upstream open and no
stream permit. The window is `session_window_bytes` (default 64 MiB: about a
second of transfer at the measured rate).

**Attaching is an optimization, never a dependency.** A request that no run
covers takes its own exact Range, exactly as before — that is the correctness
escape, not a failure path. A request whose gap is larger than the window
widens the window it starts, so one response still costs one open. The cold
stampede is unchanged: the first request reserves the key and pays the
stream-permit wait itself, and its concurrent siblings fall back to one Range
each, which is what they cost before runs existed.

**One run per key at a time.** That is what keeps the `.segpart` and the seal
single-writer, and it makes the ranged path single-flight for free (two viewers
seeking the same window now share the one stream, closing the ranged half of
ADR-0004's budget).

**The chain is bounded by the reader's own position.** When a run is sealed, the
next window may start immediately — but only if a reader is still consuming and
the window's start is within `CHAIN_KEEP_AHEAD_WINDOWS` (one) of the highest
offset any reader has taken. So the open happens ahead of the playhead instead
of at it (the boundary costs a reader nothing), while a paused or departed
viewer stops the chain rather than pulling the object: watching a file never
becomes pulling it. The decision lives in a 250 ms tick rather than in the run's
own driver, because a driver that starts its successor makes `start` recursive
— a type-inference problem (the future stops being provably `Send`) and a
needless coupling of two independent windows.

**The run tracks its own terminal state.** Not the progress channel:
`watch::Sender::send` fails, and leaves the stored value untouched, when no
receiver exists — which is exactly the case for a window nobody is reading, so
the first cut's `is_terminal()` was blind and spent entries lingered.

**`BodySource::Stage` covers a run-served response**, and no new source label
was added: the bytes come from the node (a growing sidecar or a sealed span),
which is what the operator's account asks — "did this request open upstream?" —
and the open count is already the metric that answers it. `cache_session_total`
counts runs by outcome and `cache_session_reader_total` counts requests as
`attached` or `standalone`.

## Consequences

- A shard walk costs one open per window instead of one per request: 200 GB in
  64 MiB windows is ~3100 opens rather than ~190k, and each window's open is
  paid once for every reader inside it.
- The per-upstream stream budget is now charged per WINDOW, not per request:
  many viewers of one window cost one permit, which is what lets
  `concurrency_per_upstream = 3` carry more than three viewers.
- A ranged miss stages its whole window, not just the bytes it serves. That is
  the read-ahead that makes the next shard free, and it is bounded by the
  window; admission asks the disk about the window, so ADR-0013's "refuse to
  cache, never to serve" still holds.
- Cold seeks still queue on the stream gate (the run takes its permit in the
  starting request's name), so gate saturation behaves as before.
- One stream per key is also the upper bound on speculation: a failed window
  costs at most one window's bytes, and the next request either attaches,
  starts a fresh run, or takes its own Range.
- **Known gap, unchanged by this decision:** an efficient full GET goes the
  ordinary whole-file path (its own flight), so a full GET concurrent with a
  ranged run on the same key can still hold two upstream streams. That case is
  narrower than the one this removes (a full GET is one request, not a walk) and
  unifying the two registries is deliberately out of scope here.
- **Not covered here:** a single continuous stream spanning a whole viewing
  session (~1 open per session). The chain already pays the open ahead of the
  reader, so the remaining gain is bounded by the open cost per window; that
  trade can be revisited once real traffic gives the account.

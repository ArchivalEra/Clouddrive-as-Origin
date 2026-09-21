# One ranged body, and the length a stream is promised

Supersedes nothing; makes explicit what three copies of the same code had been
saying separately, and fixes one of them.

## Context

**Three paths built "[staged pieces +] one upstream Range", each its own way.**
`serve_staged_prefix_and_tail`, `serve_upstream_range` and the standalone escape
inside `serve_passthrough_inner` all composed a response from local sidecars
followed by upstream bytes, and each carried a private copy of the 256 KiB pump.
When ADR-0019 taught the escape not to re-fetch a staged prefix, the fix landed in
one of the three — which is what a duplicate is: a place for the next fix to miss.

**Nine sites built a `Content-Range`, with three different clamps.** Four used
`end - 1`, three `end.saturating_sub(1)`, and one a pre-clamped inclusive end.
The two forms agree on every reachable input — both request parsers reject an
empty range, and `resolve_range` refuses an offset at or past the object — but
they disagree on an empty span, where `end - 1` underflows and
`saturating_sub(1)` renders `last < first`, a header no client can use. The
struct's own doc states the invariant (`first <= last < total`); nothing enforced
it.

**A stream's promise was checked on one side only.** `pump_and_seal` stopped at
EOF and compared the result against `total_len` only when the body came up SHORT.
Its comment argued that an over-long body is harmless — "every serving read is
bounded by the promised length" — which is true for a whole-file pull and false
for a run, whose stored artifact IS the span and whose size drives the ledger and
the retention budget. `SizedBackend::overlong` reproduces the shape, and the run's
own comment ("the run's own promise is the WINDOW") shows the field was already
being read as the promise rather than as the object's size.

## Decision

**One builder.** `ranged::upstream_body(stream, want, permit, sink)` is the only
implementation of "upstream bytes into a response body". `sink: Option<StageSink>`
is the single statement of whether this transfer also writes a span, so the one
path that stages what it serves is a parameter rather than a second loop. The
policy around the body — which Range to open, whether a NotFound installs a
tombstone, whether the request may be served at all, and the watcher that seals a
span whose viewer disconnected — stays at the call site. `ranged::stream_permit`
owns the ADR-0004 gate, which the call sites used to hold unchecked.

**One constructor.** `ContentRange::for_span(total, first, end, range_requested)`
is the only clamp: `end` is exclusive, `None` means this response carries no
`Content-Range` (a whole-object request, or an empty span, which callers answer
416 for), so `end - 1` cannot underflow and `last < first` cannot be rendered.

**A promise is read on both sides.** `pump_and_seal` reads at most `total_len`, so
an upstream that streams past the length it was asked for cannot stage the rest of
the object into a "window". The cap is on the read, not a truncation after it: the
extra bytes are never taken off the stream.

**What `total_len` means at the pump** is "what this stream was promised" — the
run sets it to its window, and a whole-file pull's backend reports the object. The
backend field keeps its documented meaning (the object's total, pinned by
`tests/openlist.rs`); the two agree everywhere the pump is called today.

## Consequences

- The escape's prefix rule exists once. Reverting it to `start` turns
  `an_admission_escape_does_not_refetch_the_staged_prefix` red.
- Removing the read cap turns
  `an_overlong_upstream_range_is_cut_at_the_remainder_it_asked_for` red: the run's
  window becomes the rest of the object.
- `cache.rs` is 99 lines shorter and the response shapes are one implementation.
- The staging loop and the sealing watcher still exist in both places they are
  needed (body exhaustion, and a viewer that leaves early); only the transfer is
  shared.

## Not covered here

- `serve_from_disk` and the flight's growing reader keep their own bodies: one
  slices a complete file, the other follows a watermark. Neither is a
  "[pieces +] upstream Range" shape.
- The 256 KiB chunk size remains a literal in three places (this module,
  `flight.rs`'s file body, `pieces_then`). They are three different jobs — an
  upstream pump, a disk slice, a sidecar replay — and one constant spanning them
  would invite a change to one to be justified by another.

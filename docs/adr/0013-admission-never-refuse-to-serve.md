# Admission: refuse to cache, never to serve

Decided while closing the code-side distance to production (round of
2026-09-19). Companion to ADR-0014 (what "large" means to the magazine);
ADR-0007 owns the disk budget, ADR-0004 the per-upstream gates.

## What was wrong

Three defects, one cause: nothing asked whether an object could be held at
all before the machinery committed to holding it.

- **Staging never asked.** The efficient profile staged the served interval of
  every ranged miss, without consulting the magazine's budget or the disk's
  free space. Staged segments have exactly one reader — promotion — and
  promotion refuses an object larger than the magazine, so for an object that
  can never be promoted every staged byte was pure cost, written on every
  seek and read by nobody. An open-ended range made it worse: the staging
  loop was bounded by the range, not by the disk, so one request could write
  until the filesystem filled.
- **The cold pull asked the disk but not the magazine.** `drive_flight`
  checked `free - reserve >= size` and nothing else, so a 50 GB pull was
  admitted on a 10 GiB magazine. Installing it then made the byte budget look
  overshot by 40 GB, and `evict_pick` collected victims until it was back
  under — which meant every other entry *and the new object itself*, because
  its own size was the overshoot. A pull that destroyed the cache it joined.
- **Refusal was not survivable.** The only expression of "the cache cannot
  hold this" was an error from the flight driver, and the flight cannot switch
  to not-caching after it has promised its readers bytes. The viewer got a
  502 for an object the cache could not keep, which for a media fetch is a
  playback that stops.

## Decision

**One predicate, three call sites.** `fits_magazine(size, config)` —
`0 < size <= max_size_bytes` — is now the single answer to "could the magazine
hold an object this size?", used by promotion's fit guard (unchanged
behaviour, previously a hand-written expression), by staging admission, and by
cold-pull admission. The three must agree, and they can no longer drift apart.

**Staging is refused when the object cannot be promoted, or the disk cannot
take the write.** Both halves are the same question promotion already asks,
plus the disk room check the staging loop never had. A refused staged request
is served by the pipe instead: same bytes, same status, no `.segpart`, no
ledger row, no sidecar.

**A cold pull is decided before the flight exists.** The order is now: one
coalesced stat (the size is the input to everything below), then make room,
then either a flight — resident or stray (ADR-0014) — or the water pipe. The
stat moved to the caller, and the driver takes that stat as an argument
instead of taking its own, so a cold fill still issues exactly one PROPFIND
and one GET.

**The cache refuses to cache; it never refuses to serve.** A request the
cache cannot hold is answered through the per-upstream water pipe: no flight,
no entry, no disk write, no 502. This is the same fallback the nocache
profile is built on, now extracted as `serve_upstream_range` so all three
callers share one implementation and one stream gate.

## Consequences

- The driver's capacity check survives as a last-resort backstop: the disk can
  still fill between the caller's probe and the pump, and a mid-pull write
  failure costs a truncated body *and* a leaked temp file. It is no longer the
  admission decision, and the code says so.
- The stat on the cold path holds the metadata gate **inside** the coalescer:
  the permit is paid by whichever caller actually runs the PROPFIND, not by
  every caller that wants the answer. Gating outside would queue a stampede on
  three permits and each straggler would find the shared cell gone — the
  measured "50 concurrent requests, 50 stats" shape.
- A request the cache refuses to hold costs one upstream open per range and
  gets no flight coalescing. That is the price of not caching it, and it is
  the price the nocache profile has always paid; `cache_serve_source_total`
  now makes it visible.
- Every path that serves bytes without keeping them is the same three lines on
  top of `serve_upstream_range` (stat is the caller's business, the helper
  owns the gate, the open and the read loop).

# A run for an object the magazine can never hold, on a bounded working window

Amends ADR-0013 (admission) and ADR-0014 (the object larger than the magazine is
a resident stray), replaces ADR-0017/0018's row-level minimum-age guard with a
span-level one, and completes ADR-0016's promise for the case it could not
reach: an object the node can never keep.

## Context

ADR-0016 gave the efficient profile one upstream stream per key per window, and
it gated that stream on the same admission ADR-0013 wrote for **staging**: an
object bigger than the magazine was refused a run entirely. The premise of that
gate — "staging such an object writes bytes nobody can ever read back" — was
true when staged spans were only a promotion feeder. ADR-0015 deleted promotion
and made staged spans the cache, and the gate was not revisited.

The product's object is exactly the case the gate refused. Measured on the node
against a real 200 GiB object, before this change:

```
one 1 MiB shard      : 1 upstream open, 1277 ms
walk of 24 shards    : 24 opens, 1126 ms per MiB
random seeks         : 823-924 ms to first byte
```

One open per REQUEST, because no run ever started. The same walk on an object
that *fits* the magazine cost 12 opens for 512 requests.

Two things had to be true before the gate could be lifted, and one of them was
not:

- staged bytes are bounded by the magazine's byte budget (`resident +
  segment_bytes - max_size_bytes`) and reclaimed span by span — that part was
  already in place;
- **a key being walked had to be trimmable at all.** `STAGE_MIN_AGE_MS` guarded
  the whole ledger ROW, and the row's `last_touch` is refreshed by every read
  (`cache::staging::touch`) and every seal (`seal_span`). A key a viewer is
  walking therefore had `age < 60 s` at every tick, forever: the budget could
  never be enforced against exactly the key that was filling the disk, and every
  trim had to be paid by other keys. Lifting the gate without fixing this would
  have let one walk accumulate without limit.

## Decision

**What has to be affordable is the WRITE, not the object.** `run_admits(run_len,
max_size_bytes, free, reserve)` asks two questions — is the window small enough
to be kept, and can the disk afford it — and deliberately does not ask the
object's size. A window larger than the magazine is refused because it could
never be kept (it would be evicted the moment it sealed, having cost a full
write); a whole-object request on a 200 GiB object still streams straight through
for exactly that reason. The object's own size is not a question because a
bounded window of an object too large to keep whole is not pure cost: it is the
sliding window its reader is walking through.

**The minimum-age guard belongs on the span, not on the row.** A span younger
than `STAGE_MIN_AGE_MS` is not a candidate, however the row's age reads. The age
of a span is when it was sealed (`add_read` counts reads without moving `t`), and
the in-flight part needs no guard: an unsealed `.segpart` is neither in
`segment_bytes` nor a candidate for anything, so trimming a row mid-walk cannot
endanger a transfer. This also makes the rule honest for keys that DO fit the
magazine: a long viewing session on a keepable object no longer makes the budget
unenforceable against it.

**A key whose object cannot fit is on a bounded working window**: it may keep
`watch_pin_bytes + session_window_bytes` (192 MiB by default) staged, and its
excess is spent before any keepable key's span. The cap is derived at trim time
from `Coverage.total > max_size_bytes`, so there is no new state to persist and
no behaviour change for keys that fit; it is enforced on **every** tick, not only
when the magazine is globally over budget — otherwise "one open per window" would
come with an unbounded disk cost. Since the cap is >= the pin by construction
(pin + one window), a row within its cap can never force the pin to be spent:
the bytes outside the pin are always enough.

**The escape does not re-fetch what it already holds.** When a run is refused,
the response still streams through — but the staged prefix is served from disk
and only `[frontier, end)` is opened upstream. The escape used to hand the
provider the ORIGINAL range, so every staged byte was fetched twice; that was
paid on every seek of exactly the objects a run is most often refused for.

**The account is named for the mechanism, never for content.** The class is
`total > max_size_bytes` — true of any object (a video, a tarball, a disk image, a
database dump) — and the metrics are `cache_unkeepable_keys` and
`cache_unkeepable_trim_bytes_total`. Nothing in `src/` reads a content type, an
extension or a container: this is a general-purpose S3-compatible cache, and the
large-object case is a *size* relationship.

## Consequences

- A walk of an object the node can never hold now costs one open per window.
  Measured on the node, real provider, the same 200 GiB object after this change:
  24 shards -> **1 open** (was 24), 400 shards -> **10 opens** (was 400), and the
  walk of 400 MiB took 17 s instead of ~450 s. Byte-exactness against the
  provider is unchanged, and 395 of 400 requests rode a run's watermark.
- The staged footprint of such a key is bounded and observable. Measured: the
  walk rose to 515 MiB while its spans were younger than the guard, then the cap
  took it to **exactly 201 326 592 bytes** (128 MiB pin + 64 MiB window) and it
  stayed there; `cache_unkeepable_trim_bytes_total` recorded 323 MiB reclaimed.
- A trim can be up to `STAGE_MIN_AGE_MS` late: a burst of writes younger than the
  guard is kept until the next tick past it. That is the bound, and it is why the
  cap is not an instantaneous limit.
- A rejected run no longer doubles the provider's work for a partly-staged range.
- Full coverage of such an object is unreachable by construction (the magazine
  cannot hold it), so a random seek still pays its own Range — unchanged, and the
  edge cache is what makes a re-scrub over already-pulled regions cheap.
- The production deployment's upstream ran the `standard` profile when this was
  written, whose ranged misses did not go through the run machinery at all; this
  decision made the win available to the profile that stages windows, and
  ADR-0022 has since retired the name. Whether production should switch was a
  deployment question,
  and the walk account above is its evidence.

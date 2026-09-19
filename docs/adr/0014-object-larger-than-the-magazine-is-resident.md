# An object larger than the magazine is resident

Decided with ADR-0013, which routes such an object here instead of failing it.

## Context

The magazine's promise (spec §3.4, ADR-0007) is "it holds what fits and ejects
the oldest to make room". One case the promise never covered: an object larger
than `max_size_bytes` itself. Evicting others cannot bring it into budget —
its own size is the overshoot — so the byte budget had no coherent answer, and
what it did was the worst available one: after the install, `evict_pick` saw
`total_bytes` over the cap, collected victims oldest-first, and kept going
until the overshoot was gone. With one oversized object that meant every other
entry, and then the object itself. A 50 GB pull on a 10 GiB magazine left an
empty cache.

The workload that made this concrete: a 30-hour video (30–200 GB) on a node
whose magazine is 10 GiB and whose disk is 183 GB. The magazine is a *policy*
bound, and the disk is where the room actually is.

## Decision

**An object the magazine cannot hold is admitted as a resident stray.**
`EntryMeta::oversize` records that the row was admitted knowing it exceeds the
budget. The byte budget then neither counts it nor evicts it:

- `resident_bytes` (total minus strays) is what the byte budget compares to
  `max_size_bytes`, in both `evict_pick` and the staged-bytes check in `tick`.
- `evict_pick` never picks a stray, and the entry-count budget still counts it
  (one row is one row).
- A stray leaves on the inactivity clock like anything else, and the tick
  reclaims strays when free space falls below a working floor
  (`DISK_RESERVE_BYTES + DISK_PRESSURE_BYTES`, 2 GiB), oldest-touched first.
  Disk pressure picks **strays only**: the byte budget already governs the
  magazine's own members, and freeing those would make the disk a second,
  silent eviction budget for them.
- Cold-pull admission makes room by evicting strays the same way before it
  decides a request cannot be cached at all (ADR-0013).

**The flag is recorded, not inferred.** An entry is a stray because it was
admitted as one, not because `size_bytes > max_size_bytes` happens to hold at
eviction time. Inference would mean an operator lowering `max_size_bytes` from
100 GiB to 10 GiB turns every existing row into an immune one — a config
change silently disabling the magazine. Rows rebuilt from the object tree
after metadata loss have nothing but the size to go on, and there the size
rule is used deliberately (and noted in the code).

**The hold and the stray are different things.** ADR-0012's hold is a
deadline that protects an entry inside the budget and yields when nothing else
can bring the cache back under; a stray is an entry the budget was never going
to govern. Both leave on the inactivity clock, and only the stray is exempt
from the byte sweep.

## Consequences

- A large object is now worth caching: one upstream stream fills it (27.4 MB/s
  measured), and every later range is served from this node's disk instead of
  one upstream open per shard — the measured per-open cost is what makes a
  sequential shard walk slow.
- Strays can hold the disk beyond `max_size_bytes` for as long as they are
  watched. That is bounded by the disk, not by the magazine: the reserve floor
  still refuses writes, the pressure clause reclaims down to the floor, and
  the watchdog's own watermarks (85% warn, 95% crit) still cover the disk
  itself.
- healthz reports `stray_bytes`, the part of `bytes` the byte budget does not
  govern — the number that explains a total above `max_size_bytes`.
- `inactive_ttl_secs` now decides how long a large object survives between
  viewing sessions; on the node that is 20 minutes. Re-pulling costs a full
  upstream stream, so an operator who expects intermittent viewing should
  raise it.
- `oversize` is `#[serde(default)]` for the same reason ADR-0012's hold field
  is: rows written before the field existed must still load.

# A key being read is not evicted

Amends ADR-0015 (span eviction) and ADR-0006/0007 (the byte budget), and
re-establishes — in the form that survives — the protection ADR-0012 tried to
give a freshly promoted entry.

> **Amended by ADR-0018** in three places, each found by measuring this rule on
> the real traffic shape: every response holds its key's lease, not only a
> locally-served one (the longest streams are the upstream ones); a leased key
> is protected around the viewer's *position* rather than in its entirety; and
> the 300 s grace is the short end of a watch that outlives the bodies of one
> viewing session. The rule below is still the rule for a key with a body on it
> and no watch — which is exactly what `watch_idle_secs = 0` produces.

## Context

Every clock in this cache measures **requests**. `last_touch` moves when a
request is answered; the age sweep and the budget both compare it against a
window (`inactive_ttl_secs`, `STAGE_MIN_AGE_MS`). A response body, though, can
stream for a long time after its request was answered — that is the whole shape
of this workload: a 30-hour video, a multi-GB download, a ranged walk.

So the protection a viewer actually needs has two holes:

- **A stream longer than the guards' windows loses its bytes mid-flight.** Past
  `STAGE_MIN_AGE_MS` (60 s) the key's spans become budget-evictable; past
  `inactive_ttl_secs` (20 min) the sweep deletes them. The viewer's *current*
  body survives an unlink (the descriptor is already open), but the next
  request of the same session re-fetches — which is exactly the cost the whole
  staged-read design exists to avoid.
- **A pause loses the window.** Between two requests of one viewing session
  (thinking, a stalled player, a slow scroll) the only thing keeping a key
  alive is the 60 s guard; the next budget pass may take it.

Measured context: at the target bitrate a viewer holds one window for ~34 s,
and a 200 GB download runs for an hour — both longer than the 60 s guard, and
the second longer than the 20-minute TTL.

## Decision

**A response body served from local bytes holds a read lease on its key.** The
lease is taken where the byte source is known (the response is built), released
when the body ends or is dropped — and the drop is exactly what axum does when
a viewer disconnects, so the lease is bounded by the connection rather than by
a timer. Upstream-served bodies hold nothing: there is nothing local to
protect.

**Policy eviction skips leased keys, and keeps skipping them for
`read_grace_secs` (default 300) after the last lease ends.** That covers both
holes: a long stream is protected for as long as it streams, and a pause inside
the grace does not pay a re-fetch. The grace is a deadline, not an exemption —
the shape ADR-0012 established.

The three policy paths that consult the lease map:

- the byte/count budget (`evict_pick`) — a leased row is not a candidate;
- the staged-span trim (`evict_staged`) — a leased row is skipped entirely;
- the inactivity sweep (`reap_collect`, `staging::expired`) — a leased row is
  not idle, whatever its last request says.

**Disk pressure is deliberately NOT covered.** `reclaim_under_pressure` still
takes resident strays while they are being streamed, because the disk is the
last resort: if the floor is reached, refusing to reclaim would fail writes for
everyone instead of costing one viewer a re-fetch. It is also the benign case —
the unlink does not cut the stream in flight, only the *next* request of that
viewer pays.

**Bookkeeping is pruned, not accumulated.** The lease map holds one entry per
key with a live reader or a live grace; a pass with a newer `now` drops the
rest, so it cannot grow with every key the node has ever served.

## Consequences

- A viewer who is streaming keeps the bytes they are streaming, for as long as
  they stream. That is the guarantee the product promises; before this, it held
  only for bodies shorter than 60 s.
- A key can stay un-evictable for up to `read_grace_secs` after its last
  reader leaves, so the magazine can sit over budget for that long. The bound
  is the grace, and it is configurable (`read_grace_secs = 0` restores
  pre-ADR-0017 behaviour for the budget; live bodies are still protected,
  because that part is not a timer).
- A stuck client (a body that is neither consumed nor dropped) pins its key's
  bytes until the connection dies. Disk pressure still reclaims strays in that
  case, and the magazine's own members are bounded by `max_size_bytes` in
  aggregate — the pin delays an eviction, it does not stop the disk from being
  reclaimed.
- The lease is per key, not per span: a reader of one window protects the whole
  key's staged bytes. That is the conservative direction, and it is what a
  viewer's next request is likely to need.
- `Leases` is `pub` for the same reason `flight` and `store` are: the
  integration tests drive the mechanism directly.

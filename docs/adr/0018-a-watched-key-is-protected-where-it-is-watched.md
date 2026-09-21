# A watched key is protected where it is being watched
> **Amended by ADR-0020:** the lease and the watch are taken by `Cache::serve`
> and travel with the response (`Served`), rather than being built by the HTTP
> handler; and the ranged path is no longer gated on the profile NAME. The
> protection rules themselves are unchanged.

Amends ADR-0017 (read leases) in three places — every response holds its key's
lease, not only a local one; protection is a bounded neighbourhood rather than
the whole key; the grace is the short end of a longer watch — refines ADR-0015's
span rule with a pin input, and applies ADR-0012's "a deadline, not an
exemption" rule a third time.

## Context

ADR-0017 gave a response body a lease on its key: while the body lives, and for
`read_grace_secs` after it, policy eviction leaves the key alone. That closed
the hole it was written for, and it left two others, both of which the product
hits on its first real viewing session.

**Protection is anchored to requests, but a viewing session is not.** The
workload is a 30-hour video, and EdgeOne pulls its origin in ascending 1 MiB
shards: measured, the origin sees one request per shard, so a body lasts
milliseconds and a *session* lasts hours. Every protection in the system is a
function of the last request, so a viewer who watches for two hours has the
protection horizon of a 300 s grace: pause for six minutes — thinking, a
stalled player, an idle phone — and the window the chain had already fetched is
ordinary eviction material again. The same shape costs a re-scrub too: parts of
the same session read longer ago than the grace are free to go, while the viewer
is still on the object.

**And the lease is the wrong size in the other direction.** `evict_staged`
skipped a leased *row* entirely, so for as long as a viewer was reading (or
inside the grace) none of that key's staged bytes could be trimmed by the
budget — including the windows hundreds of megabytes behind the playhead that
will never be read again. The magazine's byte budget therefore stopped being
enforceable against exactly the key a long watch was filling; the trim had to
find its bytes in somebody else's cache, and with no other key to take them it
simply could not be satisfied.

Two smaller holes surfaced while measuring this, both in the "who is allowed to
delete" question:

- the install path's own overshoot called `evict_pick` with an **empty**
  protection set, so a concurrent install could unlink the object file a viewer
  was streaming;
- the version-drift reset deletes every `.seg` of a key, and a response body
  opens its staged pieces lazily — so a reset under a live body breaks the
  viewer on the first piece it has not opened yet.

## Decision

**A watch is the unit of protection.** A key that has been requested is
*watched* for `watch_idle_secs` (default 900) after its last body ended, and a
watch remembers where the viewer is: the byte range of the most recent response.
The watch is not a viewer identity (the origin has none — EdgeOne fronts many
people onto one host id); it is the key's own recent-ness, which is what every
policy in this cache can actually act on.

**The protection is a bounded neighbourhood of that position, not the key.**
`watch_pin_bytes` (default 128 MiB, half behind and half ahead of the end of the
last response) is what a trim must leave alone — and even that is a preference:
the trim runs two passes over the whole cache, taking bytes outside every pin
first and spending a pin only when the rest of the cache could not cover the
need. ADR-0012's rule again: a deadline, not an exemption. This is what keeps
the budget real for a 200 GB object on a 10 GiB magazine.

**A pin that has to be spent is spent from the back.** Inside a pin the order is
the viewer's direction rather than the policy: the span the viewer has already
watched goes before the span it is about to need, because forward progress is
continuous while a scrub back is a deliberate act that can pay one open. The
policy's own order (lru, heat) still decides everything outside a pin — and,
while a viewer is on a key, everything beyond its pin. The LAB found this the
hard way: a few bytes of *resident* overshoot put a trim into its second pass,
and the policy's stalest span was the window under the viewer's playhead.

**Every response holds its key's lease, whatever filled it.** ADR-0017 exempted
upstream-served bodies on the reasoning that there is nothing local to protect.
That misses the case the lease was written for: the longest streams in the
system are the upstream ones, and what they need protected is the *key's* own
staged bytes — everything earlier requests of the same viewing session staged,
which a concurrent eviction pass would otherwise take while the viewer is still
on the object. Protection follows the key's session, not the end of the pipe
that happened to fill this body.

**The deletions that are not policy evictions consult the same protection.** The
install overshoot, the inactivity sweep and the reaping budget ask for the
spared set (leases plus live watches) through one accessor, so a new call site
cannot forget half of it.

**A version drift is settled when nobody is watching.** The reset is right — old
bytes must never be served as the new version — but it does not have to happen
under a live body: the drifted request is answered from upstream (never from a
mix) and the reset is deferred to the next request that arrives with the key
unprotected. Serving stale bytes stays impossible; cutting a viewer off does
not happen.

**The chain's condition is the watch, not an attached body.** `wants_successor`
used to require a reader, so a pause stopped the read-ahead immediately and
threw away the window it had already paid for. It now runs while the key is
watched, still bounded by the playhead (`CHAIN_KEEP_AHEAD_WINDOWS`), so a paused
viewer's next window is fetched during the pause and the resume costs no open.

**A successor inherits its predecessor's playhead.** This is the load-bearing
half of the rule above, and the real provider found it: a run's `playhead`
started at its own window, so a chained run nobody had read yet reported a
position equal to its own start, its own end satisfied `next <= playhead +
keep_ahead`, and a paused watch pulled the object. Measured before the fix: 25
upstream opens and 1.6 GiB fetched during a single 90 s pause. With the
inheritance the chain is bounded by the VIEWER's position in every case — a
stalled viewer buys the one window of read-ahead it is owed and no more, and a
consuming viewer keeps chaining — which is also why the idle budget changes the
chain's behaviour only for the case it was written for (a body that drops while
the playhead sits inside a window) and not for a viewer who is merely stalled.

**A seal that did not land is not claimed.** The `.segpart` rename and the
ledger claim are one step: claiming after a failed rename (a strays sweep or a
pressure reclaim can take the part) left the ledger and `segment_bytes`
describing a file that does not exist, invisible until the next restart's scan.

`watch_idle_secs = 0` and `watch_pin_bytes = 0` are the two switches that make
the reverse verification possible: the first reduces the watch to a live body,
the second restores ADR-0017's coarser whole-key rule.

## Consequences

- A viewer's recent window survives a pause of up to 15 minutes, and the
  read-ahead it had already fetched survives with it. That is the guarantee the
  product promises and the lease alone could not give.
- A watched key can be trimmed — behind and far ahead of the viewer — so the
  magazine's budget stays enforceable while a session runs. The cost is that a
  scrub back further than `watch_pin_bytes` behind the playhead is a miss;
  at 128 MiB that is roughly a minute of video at the target bitrate.
- Pinning is per key, and one pin per key: with several viewers on one key the
  pin follows the most recent request. Their other positions are still served
  by the ordinary policy (their span was just read, so it is the freshest
  thing in the row) — the pin decides which bytes need an explicit promise,
  not who gets served.
- The watch's position is the last response's byte range, not a per-body
  progress report. For a shard walk that is exactly the frontier; for one
  single very long body it is the end of its declared range, which points the
  pin past the viewer. That case is served from the run's growing file with its
  descriptor already open, so the pin is not what protects it — recorded here
  as the known shape rather than fixed with a second clock.
- The version-drift deferral means an object replaced while somebody is watching
  keeps its old ledger row (and its old spans) until the watch lapses. Correct —
  nothing mixes versions — but it means the disk holds both versions' bytes for
  up to `watch_idle_secs`.
- The chain now has two conditions rather than one: a live watch AND a playhead
  within `keep_ahead` windows. The node's pair run is the account — 512 shards,
  a 150 s pause, `read_grace_secs = 0`, a 60 s idle TTL: with the watch the
  pause costs one open (the owed read-ahead) and the post-pause re-read of the
  playhead's shard costs none; without it the pause costs nothing and the same
  re-read costs an upstream open, because a 150 s gap outlives both the grace
  and the sweep. Same total opens for the walk either way (12), so the
  read-ahead is not extra traffic — only differently timed.
- A viewer who never comes back holds a pin for `watch_idle_secs` after their
  last byte. The map is pruned on every pass, so it cannot grow with the keys
  the node has served; the disk holds at most one pin per live watch, which is
  what `cache_watch_pinned_bytes` reports.
- `Watches` is `pub` like `leases` and `flight`: the tests drive it directly,
  production goes through the response path.

## A global pin ceiling: measured, and not added (2026-09-21)

The pin is per key. N watched keys therefore name N x `watch_pin_bytes` of
preferred bytes, and nothing caps N. That was left as "measure it before
deciding", and it has now been measured on the node: `cache_watch_pinned_bytes`
reads 134 217 728 with one live watch — exactly the 128 MiB default — and
`cache_watch_active` tracks the number of watched keys, so the total is
N x 128 MiB with no ceiling of its own.

**No ceiling is the right answer, because the number is not a reservation.** A
pin does not hold bytes; it orders the eviction. `evict_staged` walks the cache
twice: everything outside every pin first, the pins only if the rest could not
cover the need. So the disk stays bounded by the magazine (`max_size_bytes`) plus
the transient caps (ADR-0019) whatever N is — the pin decides *which* bytes go,
never *whether* they go. Two tests pin the property:

- `a_pin_is_spent_only_after_the_bytes_outside_it`: 6 KiB staged against a 4 KiB
  magazine, both keys watched, the pin's neighbourhood straddling two spans. The
  two spans lying entirely outside the pins are exactly what the budget needs, so
  they go and the pin is untouched.
- `protection_orders_eviction_and_never_exempts_it`: the pin covers whole rows —
  nothing is outside — and the magazine STILL comes back inside its budget, each
  key spending the span behind its viewer first. Removing the second pass (making
  a pin an exemption) turns this test red, which is the reverse verification.

**What N does affect is the eviction's freedom**, and that is the intended
trade: with every key watched, the reaper spends pins sooner, so a node under
heavy multi-viewer load degrades to "keep each playhead, drop the history"
rather than "drop somebody's playhead". A ceiling would have to choose which
viewer's neighbourhood to sacrifice, and it would do it with less information
than the two passes already have.

The one cost worth watching is not bytes but the map: one entry per watched key,
pruned on every pass, each holding a few words. It cannot grow with the keys the
node has served — only with the viewers present in the last `watch_idle_secs`.


# A covered read answers its metadata from the ledger (proposal)

**Status: proposal, not a decision.** Nothing here is implemented. It is written
down because ADR-0016 recorded the candidate and the readings now exist, so the
question can be answered rather than re-derived.

## The question

A ranged read on a key with no durable entry pays a provider **stat** before it
can answer anything: the response's `Content-Range` total, its `ETag`, its
`Last-Modified`, and the version-gate comparison all come from that stat. A read
that is fully covered by staged spans has all of its BYTES on disk, and the
ledger holds the etag and total the spans were staged under — so could it answer
from the ledger and skip the provider call?

## Readings (node, 2026-09-21, the real 200 GiB object through the CDN)

A 12-request walk over a fresh band, from `deploy/lab/probe-edgeone-viewers.sh`
against the node's own metrics:

| what | delta | mean each |
|---|---|---|
| provider `stat` | 9 calls, 0.974 s total | **108 ms** |
| upstream `open` | 6 calls, 5.613 s total | **936 ms** |
| client TTFB | — | p50 212 ms, p90 223 ms |

Three facts follow, and they are why this is a proposal and not a plan:

1. **The metadata is real but small.** 9 stats cost 0.97 s across 12 requests —
   about 15% of the upstream time, where the 6 opens cost 5.6 s.
2. **The prize is bounded by the covered read**, and a walk is mostly covered
   reads *after* its first window: the same walk paid 6 opens for 12 requests
   because opens follow windows (ADR-0016), so as a walk lengthens, stats grow
   linearly with requests while opens do not. On a 24-shard walk measured earlier
   the split was **2 opens / 24 stats**.
3. **The stat is not on the client's first-byte path** through a CDN: the edge
   answers the first byte in ~210 ms while the origin is still working. What the
   stat costs is origin-side upstream pressure, not viewer latency.

## The constraints, precisely

**The version gate.** `serve_passthrough_inner` compares the stat's etag against
`staging.known_etag(key)` and, when they differ, either resets the ledger or
defers the reset to a request that arrives while nobody is watching (ADR-0020).
That comparison is *how a replaced object is noticed*. Answering from the ledger
means the ledger's etag is compared with nothing, so:

- either the ledger's etag gets a **TTL** (`revalidate_ttl_secs`, default 60 s),
  and a covered read inside the TTL serves bytes whose object may have been
  replaced up to a minute ago — the same window the ordinary cached path already
  accepts for a durable entry, which is the argument that makes this defensible
  rather than reckless;
- or the read still stats but *ignores* the response except for the comparison —
  which saves nothing.

**`Last-Modified` is not in the ledger.** `store::SegMeta` carries `{etag, total}`
and `Coverage` carries `{etag, total, intervals, last_touch_millis}`. A
covered read that skips the stat must therefore either

- **store it**: one more field in `SegMeta`, written at seal time from the meta the
  span was staged under. Cheap (the sidecar is already written on every seal) and
  it keeps the header stable for clients and edge caches; or
- **drop it** for covered reads, which is a visible header change on responses
  whose bytes did not change — the option to reject first.

**The negative tombstone.** The same stat path is where a 404 becomes a tombstone.
A covered read has bytes, so a tombstone cannot apply to it — but a covered read
that skips the stat also skips the chance to learn the key is GONE, and would keep
serving staged bytes for the TTL. That is the same expiry argument as above.

## What the change would look like

1. `store::SegMeta` gains `last_modified: Option<String>` (written by
   `seal_span`/`seal_renamed` from the meta the transfer had).
2. `serve_passthrough_inner` gains one branch before `stat_coalesced`: if a disk
   scan says the requested range is fully covered **and** the ledger's row was
   touched within `revalidate_ttl_secs`, build the response from the ledger's
   `{etag, total, last_modified}` and the spans, and return.
3. The version gate keeps running for every read that is NOT covered, and for the
   first covered read after the TTL — which is exactly the request that would have
   paid the stat anyway.

## Acceptance criteria (for whoever decides to do it)

- A covered read inside the TTL performs **zero** provider calls; outside it,
  exactly one `stat` (the LAB can assert this: `opens`/`stats` deltas around a
  covered re-read, the shape LAB section 10 already uses).
- A flipped object is never served mixed: the existing version-gate tests stay
  green, plus one that flips the etag while a key is covered and asserts the read
  after the TTL settles the drift (the reset path, unchanged).
- `Last-Modified` is byte-identical between a stat-answered and a ledger-answered
  response for the same version (otherwise the header is not stable and the option
  was chosen wrongly).

## Why it is still a proposal

The prize is ~108 ms per covered request of *origin-side* upstream time, not
viewer latency, and the open side — which is 8.7x more expensive per call — is
already at one per window. The honest trigger for doing this is real traffic
showing covered reads dominating the request mix with stats as a measurable share
of upstream time. Until then it costs a version-gate TTL and a new sidecar field
for a saving nobody has measured under real load.

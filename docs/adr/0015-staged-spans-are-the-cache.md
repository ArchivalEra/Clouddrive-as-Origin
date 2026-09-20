# Staged spans are the cache

Supersedes ADR-0012 (promotion and its hold are deleted). Amends ADR-0006's
ledger ceiling with the read-count dimension, and ADR-0007's byte budget with
a policy knob.

## Context

The efficient profile stages the bytes it serves as `.seg` sidecars and
records them in a per-key coverage ledger. Until this decision the only reader
of those sidecars was **promotion**: when coverage passed a threshold, a
background task re-verified the version, fetched the gaps, assembled the
spans into one whole cached object and dropped the history. The served bytes
themselves were never served again.

That made no sense for the workload the profile exists for. A 30-hour video is
30–200 GB on a node whose magazine is 10 GiB: the object can never be held
whole, so promotion was refused by the size guard, and every seek wrote spans
to disk that nothing would ever read back. A seek after the first watch went
back to the provider for bytes the node already held — one upstream `open`
(measured ~640 ms fixed cost) per request, plus the disk write.

The same shape showed up in the ledger's eviction. Staged bytes share the
magazine's byte budget (ADR-0007), and the only over-budget remedy was
row-level: pick the least-recently-touched ledger row and delete every span in
it. For a single big object that row is the entire window — one overshoot of a
few MiB discarded up to the whole magazine, and the next request re-fetched
from upstream for minutes.

## Decision

**A staged span is directly servable, so the ledger IS the cache for objects
the magazine cannot hold whole.** A Range request plans against the spans the
node holds (`plan_staged`): the covered parts are read from their sidecar
files, and only the uncovered remainder is fetched, as **one** exact Range
open. The ledger is therefore a sliding window over what the walk has
recently written, sized by the byte budget rather than by the object.

**Promotion and the promotion hold are deleted.** With spans servable there is
nothing to assemble and nothing to wait for. `promotions`,
`coverage_threshold`, `promoted_hold_secs` and `EntryMeta::hold_until_millis`
are gone; a row written by an older build still loads, because serde ignores
the field it no longer knows.

**Eviction is span-level, and the policy is a knob.** Both policies use one
mechanism — rows least-recently-touched first (so the choice *across* keys is
LRU either way), then spans inside the chosen row:

- `eviction_policy = "lru"` (default): the row's stalest span by its ledger
  time goes first. A plain sliding window over what the walk wrote most
  recently, which is what sequential playback wants.
- `eviction_policy = "heat"`: spans are compared by **read count** inside the
  trailing window (the newest 20 spans by offset, measured back from the
  frontier); when that window cannot cover the overshoot the next window back
  enters. Heat never compares across keys — the row order already carries
  that.

**The disk is the authority for what exists; the ledger is the policy's map of
it** (ADR-0016). Candidates are the key's real `.seg` files, and the ledger
supplies each file's read time and count — the intervals covering that file.
After the files are deleted the row is *rebuilt* from the survivors, carrying
the old stamps across and re-establishing the ceiling with the same merge the
insert path uses. Reading candidates from the ledger's own bounds instead
broke as soon as the two stopped being 1:1: `compact` merges a sequential walk
into one interval that owns no single file, `decay` drops intervals whose
bytes are still on disk, and the ceiling's `drop_coldest` discards records for
surviving files — after any of the three, no interval named a real file and
the key's staged bytes could not be evicted at all until the inactivity
sweep.

Both are span-level, so an overshoot trims the cold tail instead of emptying a
key. The file deletion and the `segment_bytes` subtraction live inside the
staging module, and the tick never sees a span list it could route into the
row-level delete path: the first cut of this change did, and the result was
that span eviction was undone (every remaining span of the key deleted) with
the bytes charged twice.

**A read is credited to every span it touches.** `add_read` counts a request
against each ledger interval it overlaps — the read that straddles two staged
spans credits both, and the partial-coverage path credits the staged head it
served. The first cut credited a span only when it *contained* the whole read,
which for cold sequential playback (every request a partial hit against the
frontier) recorded nothing at all: heat would have degenerated into
recency. Read counts ride along in the ledger and survive a merge as a sum.

## Consequences

- A seek into staged bytes costs zero upstream opens; a seek behind the
  frontier costs one, regardless of how many spans the prefix rode on. This is
  the measurement instrument for the shaping claim: `cache_serve_source_total`
  separates `stage` from `upstream`, and the upstream open count is
  `backend_call_duration_seconds_count{op="open"}`.
- `heat` is a knob, not the default, until real traffic says otherwise: on a
  cold sequential walk every span has the same read count, and heat then
  behaves like LRU with a window. The difference only appears where a viewer
  returns to a region — which is exactly the scrub workload.
- The budget stays soft under both policies: a row younger than
  `STAGE_MIN_AGE_MS` (60 s) is never evicted, and heat can stop short when a
  trailing window has no evictable span, so the overshoot can persist for a
  tick or two rather than being forced by deleting more than asked.
- `covered_bytes` no longer means "progress toward promotion" but "bytes this
  key can serve without upstream". §3.12's promotion vocabulary is retired.
- The window is not a pin: nothing protects a span from eviction while a
  reader streams it. A reader that loses its span re-fetches from upstream
  through the ordinary path, which is correct but costs one open.

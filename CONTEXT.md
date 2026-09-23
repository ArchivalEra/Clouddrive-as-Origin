# CONTEXT — Clouddrive-as-Origin

The domain vocabulary this repository uses, and the architecture vocabulary its
design decisions are argued in. Add a term here when a change introduces one;
keep the definitions short enough to be read before touching the code.

## What this is

A **general-purpose S3-compatible caching origin**: it sits between a CDN and a
storage provider, answers ranged reads from what it holds, and pulls the rest. It
is not a media server and knows nothing about content types — a 200 GiB video, a
100 GiB tarball, a VM image and a database dump are the same object to it. The
video case is the *motivating* one (a viewer scrubbing a 30-hour file), never a
special case in the code. What a viewer needs beyond bytes — a manifest, MSE, a
player page — is the client's packaging, not this service's (ADR-0026).

## Domain terms

**object / key** — what the provider stores and the origin serves. A *key* is the
routed cache key; the *backend key* is the provider-side path (they differ when a
route maps them).

**profile** — a per-upstream fill policy. It carries **knobs only** (ADR-0020):
`min_file_size` (below this, a ranged read takes the ordinary path) and
`coverage_window_secs` (how long a staged interval keeps counting). `efficient`
(built in, the default) or `nocache` (zero disk); the full-file water-pipe that
used to be called `standard` was retired (ADR-0022).

**the ranged path** — the ONE path a ranged request takes (ADR-0020): plan what is
already on disk, serve it, and fetch only the gap. Profile name does not select it.

**staged span** — one completed window's bytes on disk as a `.seg` sidecar, plus
its ledger record. **Staged spans ARE the cache** for objects too large to hold
whole (ADR-0015): they are served directly, there is no promotion step.

**window / run** — a run fetches one window with **one** upstream open and shares
its stream with every reader inside it (ADR-0016). A run chains a successor
window while its key is watched and the reader's **playhead** is close enough
behind; a successor inherits its predecessor's playhead. How big the window is,
and what happens at its boundary, is **the window decision** (ADR-0024).

**the window decision** — `cache::window`, the one module that answers how far
ahead a run reads and how much one key may keep staged. A run with nothing behind
it opens the **floor** (`window_floor_bytes`, default 8 MiB, clamped to
`session_window_bytes`); a window its readers **read out** doubles the next one up
to `session_window_bytes`; a request wider than either gets what it asked for. The
floor is a floor, never a cap. The reserve an un-keepable key may hold stays
`watch_pin_bytes + session_window_bytes` (ADR-0019), because the ramp only ever
shrinks a live window.

**a jump / a continuation** — a *jump* is a ranged request that begins outside
the run it replaces (a cold offset, a scrub); it opens the floor. A *continuation*
begins at or inside that run — the sequential walk arrives exactly at its
frontier — and ramps on what that run achieved. A request at a LIVE window's end
hands the key over rather than escaping (ADR-0024).

**playhead** — how far a reader has consumed. The chain's bound and the pin's
anchor are both functions of it.

**durable entry** — a whole object on this node (the cold-pull/flight path). A key
with one stays on the ordinary path, which is the only path that revalidates.

**lease** — a response body is alive (ADR-0017). Coarse: it protects the whole
key while the body streams, plus a grace. A key under a lease is SPARED, and a
budget pass that may not spend a pin leaves it alone entirely.

**watch / pin** — a key is being VIEWED (ADR-0018). Precise: it remembers where the
viewer is and protects a bounded neighbourhood around that position. A pin is a
preference, not an exemption, and a pin that must be spent is spent from the back.
A watch and a lease are NOT the same question, and the difference is what a budget
pass turns on: a lease holds the whole key, a watch holds only its neighbourhood —
so a key watched with **no pin configured** holds nothing back and the budget still
governs it. Only the age sweep and the relief valve ask the coarser question
("is anything holding this key?").

**protection / the verdict** — `cache::protection`: the two protections behind one
interface, and the questions kept distinct on purpose — `spared(now)` (the union,
a set, for the sweeps), `in_use(key, now)` (the union, one key, for the relief
valve), and `verdict(key, now)` (`{ leased, pin }`, which is what a budget pass
needs). Every eviction site used to derive these from two receivers it had to know
about, which is the shape where a rule goes missing: a new pass that consulted
only leases would silently lose the pin rule, and one that read a watch as a
second reason to spare a key would silently stop enforcing the budget.

**un-keepable / the working window** — a key whose object is larger than the
retention budget (ADR-0019). It gets a run like any other, and may keep
`watch_pin_bytes + session_window_bytes` staged; its excess is spent before any
keepable key's span.

**ledger / coverage** — the per-key record of what is staged, when it was last
read, and how often. The ledger is the policy's coarse map; the DISK is the
authority for what exists. Since the staged state moved behind one receiver
(`cache::ledger`), the ledger also holds the **account** — how many bytes are
staged — because that number is a function of the records and two places for one
fact is how they came to drift. The account changes only by measurement: a file's
real length when bytes arrive, the sweep's measured `freed` when they leave, a
fresh inventory at startup.

**adopt** — the ledger's only way in. Bytes enter a record only after the disk is
asked to back them, and the count comes from the file rather than the caller's
claim, so a short write claims short and a span the disk does not cover is
refused. **Sealing is this same rule**: a rename that landed is a disk fact like
any other, which is why the live path and a test fixture cross the same
interface. What the record cannot express, the account cannot charge for.

**served / touch** — two read-side rules that look alike and are not. `served`
(bytes left the disk for a reader) feeds heat and the interval's read time;
`touch` (the key was reached at all) refreshes only the row's age, so an actively
watched window is not swept for inactivity while its own bytes stay untouched.

**the guard never leaves** — the ledger's lock is taken and released inside the
module: every query, every selection (the sweep's candidates, the eviction's
order, the un-keepable class) and every mutation finishes there. No caller holds
the staged state across an await, and no call site has to know that ADR-0008's
ordering rule exists — that is what "structural" means here.

## Architecture terms

**module / interface / implementation** — an interface is everything a caller must
know: types, invariants, ordering, error modes, cost. **depth** is leverage at the
interface; **locality** is what maintainers get from it. A **seam** is where an
interface lives; an **adapter** fills it.

**the deletion test** — imagine deleting the thing: if the complexity vanishes it
was a pass-through; if it reappears across N callers it earns its keep.

**the interface is the test surface** — callers and tests cross the same seam. A
test that reaches past it (projecting 99 struct fields, fabricating `segment_bytes`)
is evidence the seam is missing something, not a licence to keep projecting.

**Served** — the response shape that carries its own read protection
(`ServeOutcome::Stream(Served)`). An invariant the interface cannot express is one
a future caller loses silently; this one cannot be lost.

**the ranged body** — the ONE builder for "staged pieces, then one upstream
Range" (`ranged::upstream_body`, ADR-0021). `sink: Option<StageSink>` is the
single statement of whether this transfer also writes a span, so the one path
that stages what it serves is a parameter rather than a second loop.

**a promise / `total_len`** — how many bytes a stream was asked for and will
deliver. The pump reads AT MOST it (an upstream that streams past its
Content-Length cannot turn a window into the rest of the object), and a body
that comes up short fails rather than sealing a truncated span.

**key state** — what `Cache::inspect(key)` answers: installed (and whether the
row is a tombstone), the spans on disk, the ledger's map of the same bytes with
their read clock and count, the watch pin, and whether a lease is held. One
look, consistent: the operator's `?key=` view and the tests read the same seam
(ADR-0022).

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
special case in the code.

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

**window / run** — a run fetches one window (default 64 MiB) with **one** upstream
open and shares its stream with every reader inside it (ADR-0016). A run chains a
successor window while its key is watched and the reader's **playhead** is close
enough behind; a successor inherits its predecessor's playhead.

**playhead** — how far a reader has consumed. The chain's bound and the pin's
anchor are both functions of it.

**durable entry** — a whole object on this node (the cold-pull/flight path). A key
with one stays on the ordinary path, which is the only path that revalidates.

**lease** — a response body is alive (ADR-0017). Coarse: it protects the whole
key while the body streams, plus a grace.

**watch / pin** — a key is being VIEWED (ADR-0018). Precise: it remembers where the
viewer is and protects a bounded neighbourhood around that position. A pin is a
preference, not an exemption, and a pin that must be spent is spent from the back.

**un-keepable / the working window** — a key whose object is larger than the
retention budget (ADR-0019). It gets a run like any other, and may keep
`watch_pin_bytes + session_window_bytes` staged; its excess is spent before any
keepable key's span.

**ledger / coverage** — the per-key record of what is staged, when it was last
read, and how often. The ledger is the policy's coarse map; the DISK is the
authority for what exists.

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

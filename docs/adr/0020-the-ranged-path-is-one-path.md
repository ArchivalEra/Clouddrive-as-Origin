# The ranged path is one path, and the read protection travels with the response

Amends ADR-0013 (admission) by reopening its premise that every miss water-pipes
a whole file; narrows the meaning of a cache *profile* to two knobs; and moves the
guard protocol ADR-0017 and ADR-0018 describe out of the caller and behind the
response.

## Context

Two facts, both measured, made the same request cost different things.

**A profile name selected the response path.** `serve` routed a ranged request to
the staged-read run only when the upstream's profile was `efficient` — a name that
was not even built in (it existed only if a `[cache_profiles.efficient]` table
did), and that no deployment used. The node ran `standard`, so a ranged request on
the product's own object opened one upstream stream PER REQUEST: 24 shards of the
200 GiB object cost 24 opens and 27 s, where a windowed walk costs one open.

**The read protection was acquired by the caller.** `business.rs` reached into
`cache.leases` and `cache.watches`, built both guards, and passed them down for
`instrument_body` to hold — while the cache itself depended on them: the version
gate defers a ledger reset only if a guard exists, and eviction spares a key only
while one is held. An invariant the interface cannot express is one every future
caller (a second handler, an admin endpoint, a test harness) loses silently, and
the mid-stream cut it produces is exactly what ADR-0017 was written to prevent.

## Decision

**`range.is_some() && !has_durable_entry` is the whole ranged decision.** What
decides is the request, whether the key already holds a whole object, and its size
against the profile's `min_file_size` — never the profile's name. A key with a
durable entry stays on the ordinary path, which is the only one that revalidates
and which serves a range from the file it already holds at zero upstream cost.

**A profile carries two knobs and no behaviour.** `min_file_size` (the object size
below which a ranged read takes the ordinary path) and `coverage_window_secs` (how
long a staged interval keeps counting for the ledger's policy). `efficient` is
built in, tunable by a table of that name, and **the default**; `standard` and
`nocache` remain available, and `nocache` keeps its own arm and its own outcome
(a failure is an answer, not a fall-through).

**The protection rides the response.** `Cache::serve` returns
`ServeOutcome::Stream(Served)` where `Served { plan, lease, watch }`: whoever holds
the body holds the protection, by construction. The span is taken from the plan's
own byte range (where the viewer is), and the resume counter is recorded there
because that is the single place that knows both the watch's state and whether the
answer cost an upstream open. There is now exactly one production acquisition site
(`Cache::protect`, called from the three stream arms).

## Consequences

- Production gets the ranged win with NO config change: `efficient` is the default,
  and `deploy/oracle/config-standard.toml` sets no `cache_profile`. Measured on the
  node after the deploy, against the real 200 GiB object: **24 shards → 1 upstream
  open in 2.1 s** (one open per window), where the same walk was 24 opens before.
  The unit and its config file keep their historical `standard` name; the runbook
  says so, and `healthz` reports `"profile":"efficient"`.
- A ranged request on a `standard`-named upstream now gets a run too, for objects
  at or above `min_file_size` — pinned by
  `a_standard_profile_upstream_gets_runs_for_ranged_reads`.
- The ranged arm of the cold-pull flight survives, and correctly: it serves the
  ranged requests of objects below `min_file_size` (a small object is worth a
  durable entry) and of `standard` upstreams.
- An operator who wants the old full-file behaviour sets
  `cache_profile = "standard"`; nothing was removed.
- Tests written against the default profile were re-pointed rather than deleted:
  the default-profile ranged miss stages its window (it used to water-pipe and
  install), and the "object the disk cannot hold" claim became two bounded tests
  (a window bigger than the retention budget is not written; an un-keepable object
  stages exactly one window and never the object).
- The interface grew one name (`Served`) and shrank by five (`serve_nocache`,
  `serve_passthrough`, `direct_url_bounded`, `memory_hit_fresh` are private, and
  `has_durable_entry` is back to being the gate's own helper rather than a
  public question).
- Still not unified, deliberately: a *full* GET of an object the node can hold
  takes the cold-pull flight and installs a durable entry, and the two paths meet
  only at `serve_from_disk`. Unifying them would mean teaching the flight about
  staged windows, which is a different round.

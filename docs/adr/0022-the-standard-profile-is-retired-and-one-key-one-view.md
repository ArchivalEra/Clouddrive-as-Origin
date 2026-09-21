# The `standard` profile is retired, and the state of one key is one view

Amends ADR-0020 (the ranged path is one path) by removing the last name that
selected a response shape, and closes the gap `testsupport` recorded when it
said no public interface could answer "is this installed".

## Context

**The `standard` name outlived its meaning.** ADR-0020 made
`range.is_some() && !has_durable_entry` the whole ranged decision, so the profile
name no longer chose a response shape; `standard` survived as the full-file
water-pipe, reachable two ways: an upstream that named it, and the fallback for a
name with no `[cache_profiles.<name>]` table (which boot validation already
rejects, so unreachable). No deployment named it — the node's config sets no
`cache_profile` at all, so it runs the default `efficient`.

What `standard` uniquely carried was the 64 MiB `min_file_size` floor. That floor
is a real mechanism — an object below it takes the ordinary cached path instead of
staging a window — and it is still reachable through a `[cache_profiles.<name>]`
table, which is how the tests that pin it configure it.

**The machinery's state was projected 99 times.** `Cache` exposed 17 pub fields,
and the tests read them, WROTE them to invent a state (`segment_bytes = N`), and
moved `CacheState` wholesale between caches. `testsupport` said so: "no public
interface exposes 'is this installed' — a real gap, recorded here rather than
papered over with an accessor invented for tests." That was half wrong: the
question was already answered by `entry_exists` and `snapshot`, and nobody had
noticed. The other half was real: nothing could say what was staged for one key,
who was watching it, or what the ledger believed.

## Decision

**The name goes.** `EffectiveProfile::standard()` is deleted; an unknown upstream
id and a name with no table resolve to the default profile (`efficient`); boot
validation accepts only `efficient`, `nocache`, and declared names. A config that
still says `cache_profile = "standard"` refuses to start with a message naming the
fix, rather than silently taking a different path than the operator wrote.

**The floor stays.** `min_file_size` remains a knob a custom table sets, with
`default_profile_min_file_size()` (64 MiB) as the omitted-key default. The branch
that reads it (`serve_passthrough_inner`) is unchanged and still pinned by tests
that configure a table.

**The healthz profile label is two states**, `efficient` or `nocache`, because
with the name retired nothing can resolve to a third.

**One read-only view of one key.** `Cache::inspect(key) -> KeyState` reports what
is installed, what is staged on disk, what the ledger believes, the watch pin, and
whether a lease is held — built from one lock pass, one ledger read and one
directory scan, so a caller sees a consistent picture rather than a sequence of
them. It keeps the DISK's answer (what exists, ADR-0015's authority) apart from
the LEDGER's (the policy's map).

**It has a production consumer, not only tests.** healthz takes `?key=<raw key>`
(percent-decoded, since a key is arbitrary bytes) and reports that key's state, so
"why is this object being refetched?" is answerable without a debugger. The body
is byte-identical when the parameter is absent.

**Tests read state the way production does.** Projections of `cache.state` fell
from 75 to 3 and of `cache.coverage` from 21 to 5, all of them inside fixture
helpers. The eight scattered `segment_bytes = N` pokes are gone: a staged state is
now built the way a RESTART builds it — write the sidecars, then run the
production scanner, which derives the byte account from the files that exist. A
test that passes the account in invents the one number the budget math reads, so
an accounting bug cannot show up in it. Two premises a scan cannot express keep a
hand-built ledger row with the reason stated in the test: adjacent intervals stay
separate by design (so a "merged ledger" is a compacted history, not a scan), and
a decayed interval is a state only the decay path produces.

## Consequences

- `stale_if_error_serves_cached` no longer moves `CacheState` into a second cache;
  it restarts over the same directory with `load_and_start`, which is what a node
  actually does — and which now pins that the row survives the restart.
- The deployed unit and its config file were RENAMED with this decision
  (`origin-cache-efficient.service`, `config-efficient.toml`). `cache_dir` was
  deliberately NOT: it holds the live metadata database and every staged sidecar,
  so renaming it would orphan both. The runbook says so, and one identifier does
  change for the far side: a `down` report's `service` field, which names the
  unit. Heartbeats already report a different spelling (`origin-cache`), so the
  receiver does not match on the unit name.

## Not covered here

- A third profile flag, if one is ever wanted, should arrive with a behavior no
  existing name can express — not as a name for an existing one.
- `inspect` is not a metrics source: it is a per-key view for an operator looking
  at one object, and nothing polls it.

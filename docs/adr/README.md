# Decisions (ADRs)

One line per decision: what it settled, and what it costs. Read the file when you
are about to change the thing it governs — several of them exist precisely because
the obvious alternative was tried and did not work.

An ADR is not edited to match the code; a later ADR amends it, and the amendment
is noted here. `docs/pitfalls.md` is the companion list of traps that cost real
time, and `docs/spec.md` is the acceptance contract they add up to.

| # | Decision | Note |
|---|---|---|
| [0001](0001-flat-routing-and-config-shape.md) | Flat routing over a federated origin pool; one config file, one binary | the shape every later ADR assumes |
| [0002](0002-cache-core-design.md) | Cache-core module design: the Cache is the only seam between HTTP and the machinery | `Cache`'s interface, and why its fields stopped being projected (ADR-0022) |
| [0003](0003-test-harness.md) | Test harness and env contract | extended by the fixture in `src/testsupport.rs` |
| [0004](0004-upstream-concurrency-budgets.md) | Two upstream budgets, not one: a metadata gate and a stream gate | a long transfer must not starve a HEAD |
| [0005](0005-metadata-write-batching.md) | Batched metadata transactions, serde_json, redb default durability | |
| [0006](0006-cache-entry-bounds.md) | `max_entries` as a second eviction budget alongside bytes | |
| [0007](0007-disk-capacity-budget.md) | Staged bytes share the disk budget, with a reserve floor | the reserve is what cold pulls are held back from |
| [0008](0008-metadata-loss-recovery.md) | Metadata loss degrades and rebuilds rows from the object tree | the disk is the authority |
| [0009](0009-retries-and-health-reporting.md) | Upstream retries and health reporting | |
| [0010](0010-single-node-spof-accepted.md) | The single-node SPOF is accepted | at this size, one node is the right shape |
| [0011](0011-adaptive-stop.md) | Exit at once when idle; what the drain window protects | |
| [0012](0012-promotion-hold.md) | A promoted entry carries a hold — a deadline, not an exemption | the rule every later guard restates |
| [0013](0013-admission-never-refuse-to-serve.md) | Admission refuses to CACHE, never to serve | amended by 0019 (the premise that every miss water-pipes a file is gone) |
| [0014](0014-object-larger-than-the-magazine-is-resident.md) | An object larger than the magazine is admitted as a resident stray | |
| [0015](0015-staged-spans-are-the-cache.md) | A staged span is directly servable: the ledger IS the cache for un-keepable objects | the reason the disk, not the ledger, decides existence |
| [0016](0016-one-upstream-stream-per-key.md) | One upstream stream per key: a run covers a window, and readers ride its watermark | amended by 0019/0020; its account and the remaining stat cost are recorded in the file |
| [0017](0017-a-key-being-read-is-not-evicted.md) | A response body holds a read lease on its key (+ `read_grace_secs`) | coarse: the whole key |
| [0018](0018-a-watched-key-is-protected-where-it-is-watched.md) | A watch protects a bounded neighbourhood of where the viewer is | includes why no global pin ceiling was added |
| [0019](0019-a-run-for-an-object-larger-than-the-magazine.md) | What must be affordable is the WRITE, not the object; a bounded working window | amends 0013 |
| [0020](0020-the-ranged-path-is-one-path.md) | `range.is_some() && !has_durable_entry` is the whole ranged decision; protection travels with the response | amends 0013; narrowed by 0022 |
| [0021](0021-one-ranged-body-and-the-length-a-stream-is-promised.md) | One ranged body builder, one Content-Range constructor, and a promise read on both sides | |
| [0022](0022-the-standard-profile-is-retired-and-one-key-one-view.md) | The `standard` profile is retired; one read-only view of one key | amended by the `promised_len` rename (ADR-0021's contract, one name) |

Cross-cutting: **ADR-0012's rule** (a guard is a deadline, not an exemption) is
restated by 0017, 0018 and 0019 — when adding any new protection, that is the
question to answer first.

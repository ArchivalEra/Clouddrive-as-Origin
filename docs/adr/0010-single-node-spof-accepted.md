# Single-node deployment: the SPOF is accepted

The production-readiness map (#53) listed "single-node SPOF accepted or not"
as an open question and no decision was ever recorded anywhere. This records
it, because a decision that only exists in someone's head is not a decision
the next operator can rely on.

## Context

One origin node (OCI Ampere, 2 cores, aarch64) sits behind EdgeOne and pulls
from OpenList, which fronts the cloud drives. There is no second node, no
shared state, and no failover: if the node or its disk dies, the origin is
gone until it is rebuilt. The traffic is small and bursty (about 2.9k
requests in the busiest recent week, with long idle stretches), and the drive
behind OpenList is the durable source of truth — the node holds only
discardable hot cache.

## Decision

**Accepted.** A single node is the right shape for this service at this size.
The reasons, in the order they matter:

1. **The node holds nothing that cannot be rebuilt.** Every cached object
   lives in the cloud drive, and the cache index is rebuildable metadata
   (ADR-0008 already treats losing it as a cost in revalidation, not in
   correctness). Losing the node loses warmth, not data.
2. **Recovery is a rebuild, not a restore.** A fresh node is
   `install.sh` plus the six node-local things the runbook lists (env
   secrets, TLS, acme hook, the cloudflared tunnel, `jq`, OpenList). There is
   no backup to lose and no restore procedure to get wrong, which is a real
   advantage over a half-backed-up cluster.
3. **A second node would add a state-sharing problem we do not have.** Two
   origins caching the same keys independently would either duplicate fetches
   or require shared state, and neither is worth its complexity for a cache
   that is already correct when cold.

## What is not accepted

- **No HA promise.** An outage of the node is an outage of the origin. EdgeOne
  serves what its edge cache holds and errors on what it does not; that is the
  accepted behaviour, and it is why the TTFB budget (§10) and the edge
  behaviour matter more here than origin redundancy.
- **No cross-node state, no failover, no automatic rebuild.** Nothing in the
  repo pretends otherwise, and the runbook documents the rebuild as a
  procedure rather than a script.
- **Not a claim that a node cannot be lost silently.** That is what the
  status channel (ADR-0009) and the watchdog are for: the node reports, and
  15 minutes of silence is the alarm.

## Consequences

- Recovery expectations are written down: rebuild from the installer plus the
  runbook's list, then let the cache re-warm on demand. Cold TTFB is the
  measured 0.66–0.91 s (§10), so a cold node is slow, not broken.
- Capacity planning stays a single-node exercise: `max_size_bytes`, the disk
  reserve, and the eviction bounds (ADR-0006/0007) are the whole story.
- Revisit if the traffic grows enough that losing the node is unacceptable,
  or if a second origin becomes necessary for another reason (region, quota,
  a second drive behind its own OpenList). The shape to reach for then is
  independent caches per node with the same flat namespace, not shared state.

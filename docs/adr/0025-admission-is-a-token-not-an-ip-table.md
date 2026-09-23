# Admission is a token, not an IP table

Settles how the origin decides who may pull from it (R4), and why the origin-pull
CIDR catalog is not that mechanism. Companions: `docs/security-hardening.md`
(R3/R4), `deploy/oracle/origin-pull-cidrs.sh`, and ADR-0010 (one node, accepted
SPOF — this ADR adds that its edge is not a place to put enforcement either).

## Context

Port 7777 is reachable from the internet and the CDN is the only caller that
should reach it. Three measured facts narrow the options:

- **The origin-pull ranges are an API, not a guess.** `DescribeOriginACL` returns
  them, and today's family (`gaz-0.0.4-20260907`: 312 IPv4 + 184 IPv6 CIDRs) is
  **effective 2026-10-12** — i.e. versioned — while its `Status` reads `offline`.
  Turning the feature on is refused on the free plan
  (`OperationDenied.PlanNotSupportOriginProtection`). A list that is not
  authoritative today and not binding tomorrow is a future convenience, not an
  access mechanism.
- **The peers are inside the range, and that is not what admits them.** An audit
  first counted `xff=` (the clients EdgeOne was serving) and concluded the catalog
  was wrong; once the access log carried `peer=` (the actual pull node), the six
  real peers — 43.175.104.138/.162/.143, 43.174.106.43, 43.168.149.241,
  43.168.146.198 — were all inside `43.160.0.0/12`. The catalog is accurate; it
  was simply not enforcing anything, which is why a direct GET from outside
  returned 200 before R4.
- **The perimeter is somebody else's list.** The node's ingress rules live in an
  OCI security list; tightening them is a change to the only path production has,
  and the range they would be tightened to is exactly the unverified one above.
  Applying it with `--force` "would probably work today but carries no
  guarantee" — a guarantee is the thing being bought.

## Decision

**Admission is a header the edge sets, required from every peer outside an
exemption list.**

- The edge stamps each origin pull: the CDN rule gains a `ModifyRequestHeader`
  action that **sets** `X-Origin-Token`. *Setting* is the whole point — a caller's
  own copy is overwritten, so whoever reaches the port cannot forge the stamp.
- The front requires that header, compared in constant time, from any peer outside
  `front_origin_token_exempt` (default: loopback, so the node's own `accept.sh`,
  the LAB and the watchdog keep working).
- The knobs are paired (`front_origin_token_header` with
  `front_origin_token_env`) and **fail-closed**: naming an environment variable
  that is unset or empty refuses to boot, rather than serving unstamped traffic.
- The CIDR catalog is still used — as **defense in depth** on the day the plan
  allows it (`origin-pull-cidrs.sh --mode emit-oci`), never as admission. The
  updater refuses to emit from non-authoritative data (exit 2), which is the same
  judgement this ADR makes: a list whose authority is a state, not a property,
  cannot be the gate.

## What it costs

- A secret exists in two places (the edge rule and
  `/opt/origin-cache/origin-cache.env`); rotating it is a two-place edit, and
  there is no automated rotation.
- A caller that cannot carry the header needs an exemption line — a second CDN or
  a debug pull from a laptop is a config change, not a firewall change.
- The stamp is only as strong as the edge rule: whoever can edit the rule can
  admit traffic. That is a control-plane credential, and the sub-account is now
  read-only.

## Evidence

External direct GET :7777 with no stamp → **403** (200 before R4); loopback → 206;
via the CDN (which stamps) → 206; the real 200 GiB film via the CDN → 206/1 MiB.
The LAB carries both arms — `config-f` with the default exemption is the exempt
arm, `config-h` with an empty list is the refusal arm (no stamp → 403, wrong stamp
→ 403, right stamp → 206) — `--quick` 75 PASS / 0 FAIL. The two IP lists that the
token is often confused with are covered separately by
`deploy/oracle/ip-filter-probe.sh` (see the runbook): `front_ip_allow` is only the
rate-limit exemption, and `front_ip_block` never matched a v4 CIDR on the node
until the mapped-peer fix.

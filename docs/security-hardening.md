# Security hardening at the origin node — requirements (2026-09-23)

One page of "why", then requirements, then the tests that prove each one. Written
for the infrastructure agent that owns the Oracle Cloud account; the repo-side
items are marked `ours` and are mine.

## Why this exists

The origin node is meant to be reachable **only** through EdgeOne. Measured on
2026-09-23, it is a public host: several services answer on the public
address, and there is no host firewall at all — the perimeter is whatever the
OCI subnet's security list allows.

## What must not change (constraints of this project)

- `src/` stays free of media specialisation. Nothing here may add content-type
  awareness, and no browser or decoder may be installed on the node.
- Credentials live only in `/opt/origin-cache/origin-cache.env` and are read
  from the environment; they never appear in tickets, logs, or the repo.
- **Do not touch `n-s3shroud`** (ports 8475/8471) or the `sing-box` /
  `reality-server` proxy stack. They are listed below only so the inventory is
  complete; they are out of scope and must be left exactly as they are.
- Do not rename `cache_dir` (`/opt/origin-cache/cache-standard`): it holds the
  live metadata database and every staged sidecar.
- Binary-only updates: install the binary, do **not** run `install.sh`; restart
  with `sudo -n systemctl restart origin-cache-efficient origin-cache-nocache`.
- Repo text stays CJK-free (a pre-push hook rejects it).

## Measured exposure (2026-09-23, from an external host)

| port | what | bound to | reached from the internet | intended? |
|---|---|---|---|---|
| 7777 | origin front (TLS) | `*` | yes — `curl -k --resolve cdn-oracle.isui.ren:7777:<node-ip> https://…:7777/googledrive1/test-page.html` → **200**, `accept-ranges: bytes` | yes: EdgeOne pulls here |
| 7778 | nocache plane (zero disk) | `*` | yes — `http://<node-ip>:7778/googledrive1/test-page.html` → **200** | **no** |
| 5244 | OpenList (the Google Drive gateway) | `*` | yes — `/` → **200**, `/dav/` → 401 | **no** |
| 22 | ssh | `0.0.0.0` + `[::]` | key-only (`passwordauthentication no`, root `without-password`) | yes |
| 111 | rpcbind | `0.0.0.0` + `[::]` | bound publicly; an HTTP probe is not a valid test | **no** |
| 8475, 18500, 17900, 9007 | s3shroud and helpers | `0.0.0.0` | 8475 answers 200 | out of scope — do not touch |
| 443, 8388, 8389, 8443-8446, 9443 | sing-box / reality-server | `*` | proxy stack | out of scope |
| 9090, 9091, 8080, 8081, 4330, 44321, 8471, 20241 | metrics, business plane, PCP, cloudflared | loopback only | no | yes |

Two more facts, both relevant:

- `firewall-cmd --state` → *not running*; `iptables -S` → the three default
  policies and no rules. **There is no host firewall.**
- The origin's upstream credential is **writable**: a `PUT` through the node's
  own WebDAV (loopback 5244) succeeded. The origin only ever reads, so this is
  over-privileged.

## Status (2026-09-23, updated)

- **R11 decided: the content is public.** No signed access; the unsigned business route and
  the unsigned S3 listing are the intended surface. What the exposure must NOT have is a
  write path, and that is now proven rather than claimed: `PUT`/`DELETE`/`PATCH`/`POST`/`MKCOL`
  against the exposed route all return **405** (the only POST is the token-guarded prewarm
  under `/_internal/`, which the front refuses by prefix on the public hostname), the listing
  after the probes shows nothing created, and `src/` contains no upstream write verb at all
  (`grep -rE '"(PUT|POST|DELETE|MKCOL|COPY|MOVE)"'` over `src/` — only `PROPFIND` and
  ranged `GET`). Because of that, **R10 is closed as "keep the credential writable"** by the
  owner's decision: the only peer that can reach the credential's endpoint is the origin
  itself, over loopback.
- **OpenList is isolated to loopback** (the owner's word for it was "absolute isolation"): `scheme.address =
  "127.0.0.1"` in `/opt/openlist/data/config.json` (backup next to it,
  `config.json.pre-isolation`), so it listens on **127.0.0.1:5244 only** — the perimeter
  rule that blocked 5244 is now belt to that braces. Verified: external connect refused,
  loopback UI 200, origin's CDN path unaffected, `accept.sh` VERDICT=PASS. Its other
  services (S3 :5246, FTP :5221, SFTP :5222, MCP) are all `enable: false`, and would follow
  the loopback bind if ever enabled.
- **Done and verified**: R1, R2, R6 — the perimeter is now a whitelist (22, 7777, the proxy
  stack, ICMP); 7778, 5244 and 111 are closed to the internet; rpcbind is disabled. Verified
  from an external host: 5244 and 7778 now time out (they answered 200 before), the CDN path
  is intact (ranged read through the CDN → 206 with the exact byte count, TTFB 0.26 s), and
  `bash /home/opc/cds/deploy/oracle/accept.sh` on the node prints **VERDICT=PASS** (four units
  active, 0 failed, report endpoint 200). The proxy stack and s3shroud ports were left
  untouched, as required.
- **R3 is unblocked** — the authoritative list exists and is API-driven; see R3 below. It was
  previously "stop and report" only because the source was unknown.
- **R3 is plan-gated, not missing.** The ranges are published through the origin-protection
  API, but enabling it is refused on the free plan — verbatim error code:
  `OperationDenied.PlanNotSupportOriginProtection` ("the plan does not support origin
  protection"; announcement: https://www.tencentcloud.com/announce/detail/100833). Attempted
  2026-09-23 with `EnableOriginACL --L7EnableMode specific --L7Hosts '["cdn-oracle.isui.ren"]'`;
  the zone still reads `Status: "offline"`. **R4 is now the primary path** because it needs no
  plan change; R3 stays available to anyone who upgrades.
- **R5 waits on R4 or on an upgrade**: a host default-deny needs the pull ranges, and without
  R3 there is no authoritative list. Do not build one by guessing or by resolving hostnames.
- **R4's edge half is done** (2026-09-23): `rule-3usngannhvqa` now carries a third action,
  `ModifyRequestHeader` setting `X-Origin-Token`, so every origin pull is stamped by the edge
  — and the free plan accepted it, which was the open question. The secret is a 64-hex value
  generated in-shell (never printed, never in the repo) and stored on the node as
  `ORIGIN_TOKEN` in `/opt/origin-cache/origin-cache.env`; the CDN path was re-verified after
  the write (ranged read 206, exact bytes, TTFB 0.35 s). Revert: remove that action from the
  rule (`ModifyL7AccRule` with the two remaining actions). The origin-side check is the
  remaining piece, and it is what turns the stamp into admission control.
- **R4 is DONE and live** (2026-09-23): the edge sets `X-Origin-Token` on every origin pull
  (`ModifyRequestHeader` on `rule-3usngannhvqa`), the front requires it from any non-exempt
  peer, and the four-way test passes — external direct 403, loopback 206, CDN 206, the real
  film through the CDN 206/1 MiB. `accept.sh` on the node: **VERDICT=PASS**. Details, config
  and the two traps paid for on the way are in the runbook's "The origin token" section.
- **The updater for R3 exists**: `deploy/oracle/origin-pull-cidrs.sh` reads the ranges, keeps
  a state hash, and emits them in the shape each consumer needs — nftables set members for the
  node (`--mode apply-nft`) or OCI ingress rules as JSON plus the `oci` command that would
  apply them (`--mode emit-oci`, which this repo never runs). It is **not** a wrapper around
  the vendor CLI: `--fetch` runs the one documented read invocation (endpoint and proxy
  handling spelled out), or you hand it a file. Running it today: `AUTH=no STATUS=offline
  312 IPv4 + 184 IPv6`, and **it refuses to apply** — those ranges are the catalog, not what
  EdgeOne is bound to, so an allowlist built on them would block the pull nodes in use. That
  refusal is the tool's whole point, and `--force` overrides it only with that reason printed.
  It prints the `ConfirmOriginACLUpdate` step when Tencent announces a new version; doing that
  needs write access, so it is left to whoever holds it. Self-tested (`--self-test`, twelve
  checks over both authorities, the refusal, idempotence and the pending-update path).
- **R3 is closed with a decision, not left open** (infra side, 2026-09-23 — recorded in
  `~/.oci/perimeter-hardening-20260923/R3-outcome.md` on this workstation): 7777 stays
  world-reachable at the perimeter **by design**, because R4 identifies the caller at the
  application layer and the plan cannot publish a binding range list. The whitelist applied
  2026-09-23 (22, 7777, proxy-stack ports, ICMP) stands; the infra side independently
  verified all four arms of the token test (external 403, CDN 206, loopback 200/206 by probe
  shape, `peer=` in journald) and keeps the revert at
  `~/.oci/perimeter-hardening-20260923/revert.sh`. The upgrade-day runbook (enable →
  `--fetch` → `emit-oci` → merge only the 7777 rule → verify A1+A4 in the same session) is in
  that same file; two additions when that day comes: require `AUTH=yes` from the tool before
  merging (report mode does not fail on `AUTH=no`, so grep the `meta` line), and after
  applying a new family, confirm it (`ConfirmOriginACLUpdate`) so Tencent stops announcing.
- **R5** stays the infrastructure agent's: with the token live (R4) a default-deny can be
  written against the token, and with this tool the pull-range arm can be kept in step once
  origin protection is available.
- **What remains**: R8 (rate ceiling — still a decision, and it interacts with the edge's
  pull IPs), R9 (`front_ip_allow` end-to-end), R11 (public or not), R12 (retention/alerts),
  R10 (rotate the upstream credential to read-only).

## Requirements

**R1 `infra` + `ours` — the nocache plane must stop being public.** ✅ done (2026-09-23)
It is an internal plane by design (ADR-0022; `config-nocache.toml`,
`front_listen = "[::]:7778"`). Ours: bind it to loopback and redeploy.
Infra: block 7778 at the perimeter as well. Acceptance: A3.

**R2 `infra` — OpenList must not be reachable from the internet.** ✅ done (2026-09-23)
Port 5244 is the gateway to the content store and holds its credentials. Bind it
to loopback (OpenList's own config) and block 5244 at the perimeter. The origin
must keep reaching it at `127.0.0.1:5244`. Acceptance: A2.

**R3 `edgeone` + `infra` — restrict the origin port to EdgeOne's origin-pull ranges. Plan-gated (see Status).**
Kept here because it is the cleanest end state if the plan ever changes: `EnableOriginACL`
first (only then does EdgeOne pull exclusively from the family's ranges), then apply the
312 IPv4 + 184 IPv6 CIDRs of `gaz-0.0.4-20260907` as a **set** (OCI NSG / nftables), then poll
`DescribeOriginACL` about every three days and on a non-empty `NextOriginACL` apply the new
ranges and `ConfirmOriginACLUpdate`. Revert: `DisableOriginACL`. `DescribeOriginProtection`
is the old API (superseded 2025-06-27) — do not use it.

**R4 `ours` + `edgeone` — an origin-access token, which needs no plan change. Primary path.**
The edge sets a header on the request it forwards to the origin; the origin requires that
header. Consequences: a direct hit on the origin port fails (403) even though the port is
open, the CDN path is untouched, and nothing depends on IP churn.

- Edge side (`edgeone`, one rule write, reversible by removing the action): add
  `ModifyRequestHeaderParameters` with `HeaderActions: [{Action: "set", Name: "<name>",
  Value: "<secret>"}]` to `rule-3usngannhvqa`. Read the rule back afterwards and diff it —
  reading and writing use different field spellings (pitfall 48), so write only that action.
  The capability on the free plan is unknown; that is what this write finds out.
- Origin side (`ours`): a config knob naming the header and the env var holding the secret
  (same shape as `prewarm_shared_secret_env`, compared with `sigv4::constant_time_eq`), a
  boot warning when the env var is named but unset, and an exemption for loopback peers so the
  node's own probes (`accept.sh`, LAB) keep working. Default off, so the knob cannot change
  behaviour until it is deliberately turned on.
- Acceptance: A8 plus A4. A8: from an external host, a plain GET on `https://<node-ip>:7777/…`
  returns 403 (today it returns 200); from the node, the same request on `127.0.0.1:7777`
  returns 206; and through the CDN a ranged read still returns 206 with the exact byte count.
  Any failure → remove the rule action and unset the knob.

**R5 `infra` — a host firewall with default-deny inbound.** Blocked until R3 or R4 lands: a
default-deny needs to know which peers legitimately pull.

## Handing the updater to whoever owns the cloud perimeter

`deploy/oracle/origin-pull-cidrs.sh` is self-contained (bash + python3; `tccli` only for
`--fetch`, `nft` only for `--mode apply-nft`). It needs **read-only** EdgeOne access — the
account it was developed against was downgraded to read-only and it still runs — and root only
to touch a firewall. Three things to agree on before it is wired into anything:

1. **What it may write.** It updates members of an *existing* nftables set and never creates
   one; `--mode emit-oci` only prints. The firewall those members belong to (R5) is a decision,
   not a side effect of running a sync.
2. **What it refuses.** While the zone reports `Status: offline` — true today, and true for as
   long as the plan lacks origin protection — it applies nothing and says why, because the
   catalog ranges are not binding. Anything that consumes it must treat exit code 2 as
   "not authoritative", not as a transient failure to retry.
3. **Cadence.** Tencent's own guidance for this data is a poll roughly every three days; on a
   `NextOriginACL`, apply the new set and then confirm it (`ConfirmOriginACLUpdate`, a write
   this repo does not make).

The state file (`origin-pull-cidrs.state`) is what makes a re-run cheap and a change visible:
same hash, nothing to do; new hash, the set is different from the last one applied.

### How to tell who actually pulls (and one way this went wrong)

The front's access record used to carry `xff` and no peer, and `xff` is **the client EdgeOne
was serving**, not the address that opened the connection. Read as if it were the puller, it
produced an audit that concluded the origin-pull catalog "does not contain the node that is
actually pulling" — the two addresses counted (both in Zhejiang Mobile, one of them this
workstation's own direct egress) are clients, and clients are of course not in a catalog of
Tencent's pull ranges. The record now carries `peer=` as well, so one line answers both
questions:

```
front access ... status="206" bytes=1048576 proto="h2"
   peer=[::ffff:43.168.149.241]:5276   xff=39.172.36.93
   ^ who pulled (a Tencent range, in the catalog)   ^ who was being served (a client)
```

Measured that way, the six addresses pulling today are all in the catalog
(`43.175.104.138/162/143`, `43.174.106.43`, `43.168.149.241`, `43.168.146.198` — every one
inside `43.160.0.0/12`). So the catalog *does* describe today's pullers; what it is not is
**binding**, because the zone reports `offline`, and it is **versioned** (the current family
activates 2026-10-12), so a new version can move the set. That is the honest risk statement
for `--force`: it would probably work today and has no guarantee behind it.

Two notes for anything doing address matching at the socket level: peers arrive IPv4-mapped
(`[::ffff:a.b.c.d]`) on the `[::]` listener, which is why the front's own CIDR lists needed
canonicalizing (see the runbook's origin-token section); nftables rules are unaffected, since
the packet on the wire is IPv4.

**R4 `ours`, blocked on a capability check — an origin-access secret instead of an IP list.**
If EdgeOne can inject a request header on origin pull, the front can require it
(a new config knob; today only the prewarm token exists), and then the allowlist
stops being hostage to IP churn. The API model does contain request-header actions
(`ModifyRequestHeader`, `HeaderParameters`), so this looks possible; it needs the
EdgeOne-holding side to confirm it applies to **origin-pull** requests before any code is
written. R3 is now the primary path — R4 stays as the belt to its braces.

**R5 `infra` — a host firewall with default-deny inbound.**
firewalld or nftables, allowing only 22 (from the management CIDRs) and 7777
(from R3's list, or from the EdgeOne ranges if R4 is not ready). Acceptance: A5
plus A1/A4.

**R6 `infra` — rpcbind off** unless something actually needs it
(`systemctl disable --now rpcbind rpcbind.socket`).

**R7 `infra` — keep it that way.** The loopback-only surfaces (9090/9091,
8080/8081, 4330, 44321, 8471, 20241) stay loopback; nothing new gets published
without a line in this document.

**R8 `ours` — decide and set a per-IP rate ceiling.**
Today `front_rate_rps = 0` (off) on both planes. `deploy/oracle/rate-limit-probe.sh`
already proves the mechanism on a throwaway loopback instance; the production
half is a decision, because it needs an observation only the EdgeOne side can
make: **does EdgeOne retry a 429, and would that make things worse?** Acceptance:
A7 on the plane that carries the CDN.

**R9 `ours` — configure and actually test `front_ip_allow`. CLOSED (2026-09-23).**
`deploy/oracle/ip-filter-probe.sh` runs ten assertions against a throwaway
`[::]`-bound instance: the rate ceiling counts the mapped peer (a burst of six
gets four 429s), `127.0.0.1/32` exempts it through the mapped form (six 200s, zero
429s), a non-matching allow list exempts nothing, `front_ip_block` drops a mapped
v4 peer and a pure v6 peer at connection time, an unrelated blocklist answers as
usual, and a dead business is a `502` with `Cache-Control: private, no-store`.
R3's authoritative list turned out not to be a prerequisite: `front_ip_allow` is
only the rate-limit exemption (the connection gate is `front_ip_block` alone),
and admission is R4's token. Readings and the 502 mapping:
`docs/runbook.md` ("The front's two IP lists, end to end").

**R10 `infra`/`ours` — make the origin's upstream credential read-only. CLOSED BY DECISION, superseded by isolation (2026-09-23).**
The owner chose writable ("more convenient") with a stronger mitigation: **network isolation**
(OpenList bound to `127.0.0.1:5244`, verified above) plus a **read-only-by-construction**
exposed surface — the S3/business route registers only `GET`/`HEAD` handlers, every write
verb is refused with 405 before anything is parsed, and `src/` contains no upstream write
call to audit. The credential therefore sits on an endpoint that only the origin itself can
reach, doing only reads. If OpenList ever becomes reachable beyond loopback, this
requirement comes back, and A6 (a PUT with it fails) is the test.

**R11 `decision` — is the content public? DECIDED: yes (2026-09-23).**
The business route *and* the S3 listing answer unsigned (`?list-type=2` → 200
without signing), and that is the intended surface. No signed access will be built; the
exposure's security property is the one R4 gives it (the caller is either the edge or
nobody) plus the 405 write refusal above.

**R12 `infra` + `ours` — retention and alerting.**
journald holds ~211 MB (~2.5 days); the watchdog sends a heartbeat to the
blog-side Worker but there are no thresholds anyone acts on. Decide a retention
cap and at least one alert condition (unit down, disk watermark, healthz).

## Acceptance tests (run from an external host unless stated)

- **A1** `curl -k --resolve cdn-oracle.isui.ren:7777:<node-ip> https://cdn-oracle.isui.ren:7777/googledrive1/test-page.html`
  must fail to connect (today: 200).
- **A2** `curl http://<node-ip>:5244/` must fail (today: 200), while
  `ssh <node> 'curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:5244/'`
  still answers 200.
- **A3** `curl http://<node-ip>:7778/googledrive1/test-page.html` must fail
  (today: 200).
- **A4** the CDN path is intact: a ranged read through the CDN returns 206 with
  the expected byte count, and `deploy/oracle/accept.sh` on the node prints
  `VERDICT=PASS`.
- **A5** `sudo ss -tlnp` shows only the intended public listeners (22, 7777) plus
  loopback ones.
- **A6** a `PUT` with the origin's credential fails (403) and reads still return
  206.
- **A7** (after R8) a burst from one IP gets 429s while a normal read is served.
- **A8** (after R4) an external plain GET on `https://<node-ip>:7777/googledrive1/test-page.html`
  returns 403 (today: 200), the same request from the node on `127.0.0.1:7777` returns 206,
  and A4 still passes.

## Rollback

- Export the "before" state of every perimeter object you touch
  (`oci network security-list get … > before.json`, or the NSG equivalent) and
  keep the exact revert command in the change. For 7777, verify A4 immediately
  after the change and keep the revert in the same session.
- Host firewall: `systemctl stop firewalld`, or `nft flush ruleset`.
- Ours: config changes are single-line reverts plus
  `sudo -n systemctl restart origin-cache-efficient origin-cache-nocache`; the
  previous binary is at `/home/opc/origin-cache.prev`.

## Ordering

1. R1, R2, R6 (they cannot break the CDN) → verify A2, A3.
2. R3 with an authoritative list, or stop and report → verify A1 **and** A4.
3. R5 default-deny → verify A1, A4, A5.
4. Ours, once the perimeter is stable: R8, R9, R10.
5. Decision-gated: R4, R11, R12.

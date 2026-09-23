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

- **Done and verified**: R1, R2, R6 — the perimeter is now a whitelist (22, 7777, the proxy
  stack, ICMP); 7778, 5244 and 111 are closed to the internet; rpcbind is disabled. Verified
  from an external host: 5244 and 7778 now time out (they answered 200 before), the CDN path
  is intact (ranged read through the CDN → 206 with the exact byte count, TTFB 0.26 s), and
  `bash /home/opc/cds/deploy/oracle/accept.sh` on the node prints **VERDICT=PASS** (four units
  active, 0 failed, report endpoint 200). The proxy stack and s3shroud ports were left
  untouched, as required.
- **R3 is unblocked** — the authoritative list exists and is API-driven; see R3 below. It was
  previously "stop and report" only because the source was unknown.
- **R5 waits on R3 step 2**: a host default-deny is only safe once the origin-pull ranges are
  known and applied.

## Requirements

**R1 `infra` + `ours` — the nocache plane must stop being public.** ✅ done (2026-09-23)
It is an internal plane by design (ADR-0022; `config-nocache.toml`,
`front_listen = "[::]:7778"`). Ours: bind it to loopback and redeploy.
Infra: block 7778 at the perimeter as well. Acceptance: A3.

**R2 `infra` — OpenList must not be reachable from the internet.** ✅ done (2026-09-23)
Port 5244 is the gateway to the content store and holds its credentials. Bind it
to loopback (OpenList's own config) and block 5244 at the perimeter. The origin
must keep reaching it at `127.0.0.1:5244`. Acceptance: A2.

**R3 `infra` + `edgeone` — restrict the origin port (7777) to EdgeOne's origin-pull ranges.**

The authoritative list exists and is API-driven. **Do not guess CIDRs, and do not derive
them by resolving hostnames** — Tencent publishes them through the origin-protection API,
as versioned families, with a documented refresh pattern.

Read-only findings, 2026-09-23, with the sub-account
(`--endpoint teo.intl.tencentcloudapi.com`, proxies unset):

- `DescribeAvailableOriginACLFamily` → one family for this zone:
  **`gaz-0.0.4-20260907`**, `ActiveTime 2026-10-12T00:00:00+08:00`,
  **312 IPv4 + 184 IPv6 CIDRs**. (`gaz` = global standard control domain; `mlc` = China,
  `emc` = overseas-excluding-China; the `plat-*` families are lite variants with fewer
  ranges, for approved accounts.)
- `DescribeOriginACL` for `zone-3taqnjqfr1zo` → `Status: "offline"`: origin protection is
  not enabled, so today EdgeOne pulls from whatever it likes and no allowlist can be correct
  yet.
- `DescribeOriginProtection` is the **old API, superseded 2025-06-27** — use
  `DescribeOriginACL`.

Order matters: an allowlist applied before the feature is on would block the pull nodes
EdgeOne actually uses today.

1. `EnableOriginACL` on the zone. From then on EdgeOne pulls **only** from the `gaz`
   ranges. Revert: `DisableOriginACL` (which also stops update notifications).
2. Apply that family's CIDRs to the perimeter — the OCI security list / NSG covering 7777,
   or an nftables set on the host. 496 CIDRs is a **set to be reloaded by a timer**, not
   496 hand-written rules.
3. Refresh: poll `DescribeOriginACL` about every three days (Tencent's own suggested
   cadence). If `NextOriginACL` comes back non-empty, apply the new ranges, then call
   `ConfirmOriginACLUpdate` so the notifications stop.
4. `ModifyOriginACL` binds or unbinds specific domains/instances to the protection; any
   domain added later goes through it.

Acceptance: A1 and A4 verified **in the same session** as step 2, plus the applied set's
count equal to the family's count (312 + 184), and the ordering visible in the change log
(enabled before applied).

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

**R9 `ours` — configure and actually test `front_ip_allow`.**
Once R3's list is authoritative, the allow path (never exercised end to end) gets
a real assertion, not a config line.

**R10 `infra`/`ours` — make the origin's upstream credential read-only.**
A dedicated OpenList user with read permission only, used by
`OPENLIST_USERNAME`/`OPENLIST_PASSWORD`; rotate the current one. Acceptance: A6.

**R11 `decision` — is the content public?**
The business route *and* the S3 listing answer unsigned (`?list-type=2` → 200
without signing). If the content is not meant to be public, the fix is signed
access (a code change), not perimeter work. Needs the owner's answer before
anything is built.

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

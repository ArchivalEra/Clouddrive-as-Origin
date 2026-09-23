#!/usr/bin/env bash
# Keep the origin's firewall in step with EdgeOne's origin-pull ranges.
#
# Why this exists: the origin port has to stay reachable. Tencent does not publish
# the ranges for a plan without origin protection, and enabling it is refused on
# the free plan (`OperationDenied.PlanNotSupportOriginProtection` — see
# docs/security-hardening.md R3), so the port either answers the world or it
# answers exactly the peers that pull from it. This tool maintains the second.
#
# What it is NOT: a wrapper around the vendor CLI. Reading the list is ONE
# documented invocation (`--fetch` runs it verbatim, endpoint and proxy handling
# spelled out), or you hand it a file you produced yourself (`--input`). The value
# here is the reconciliation — it refuses to apply data that is not authoritative,
# it is idempotent against a state file, and it emits the set in the shape each
# consumer needs (nftables members for the node, JSON for an OCI security list).
#
# Usage:
#   origin-pull-cidrs.sh [--fetch | --input ZONE.json [--family FAMILY.json]]
#                        [--mode report|apply-nft|emit-oci]
#                        [--out-dir DIR] [--state FILE]
#                        [--nft-set "inet filter edge_pull"] [--port 7777]
#                        [--force] [--self-test]
#
# Modes:
#   report     (default) parse, print, write the set files. Touches no firewall.
#   apply-nft  flush and refill an EXISTING nftables set on this host. It never
#              creates the set or its rules: the firewall that consumes it is a
#              decision (R5), and a tool that invents one would be a surprise.
#   emit-oci   print ingress rules as JSON plus the `oci` command that would apply
#              them, for whoever owns the cloud perimeter. This script never calls
#              a write API.
#
# Authority, which is the whole point:
#   `DescribeOriginACL` returns what EdgeOne is ACTUALLY bound to pull from, and
#   it is only meaningful when the zone reports `Status: online`. While it reports
#   `offline`, the tool falls back to `DescribeAvailableOriginACLFamily` — the
#   catalog of ranges EdgeOne would use if protection were enabled — and REFUSES to
#   apply it, because nothing binds EdgeOne to that list today and an allowlist
#   built on it would block the pull nodes actually in use. `--force` overrides and
#   says so out loud. That refusal is the difference between this and guessing.
#
# Exit codes: 0 ok · 2 refused (data not authoritative) · 3 parse/input failure.
set -euo pipefail

ZONE=${ZONE:-zone-3taqnjqfr1zo}
ENDPOINT=${ENDPOINT:-teo.intl.tencentcloudapi.com}
MODE=report
OUT_DIR=.
STATE=""
NFT_SET="inet filter edge_pull"
PORT=7777
FORCE=0
ZONE_JSON=""
FAMILY_JSON=""
SELF_TEST=0

usage() { sed -n '2,45p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --fetch)      ZONE_JSON=""; FAMILY_JSON=""; FETCH=1 ;;
    --input)      ZONE_JSON=$2; shift ;;
    --family)     FAMILY_JSON=$2; shift ;;
    --mode)       MODE=$2; shift ;;
    --out-dir)    OUT_DIR=$2; shift ;;
    --state)      STATE=$2; shift ;;
    --nft-set)    NFT_SET=$2; shift ;;
    --port)       PORT=$2; shift ;;
    --force)      FORCE=1 ;;
    --self-test)  SELF_TEST=1 ;;
    -h|--help)    usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage 1 ;;
  esac
  shift
done
case "$MODE" in report|apply-nft|emit-oci) ;; *) echo "unknown --mode $MODE" >&2; exit 3 ;; esac
[ -n "$STATE" ] || STATE="$OUT_DIR/origin-pull-cidrs.state"
mkdir -p "$OUT_DIR"

# The vendor read, verbatim. Two traps are spelled out rather than hidden: the
# international endpoint has no route inside tccli (the default call goes to the
# China endpoint and comes back as "SecretIdNotFound", which reads like a bad key),
# and a globally exported proxy takes the request somewhere else entirely and
# reports a network error about an unrelated host.
fetch_read() { # <action> <out-file>
  local action=$1 out=$2
  env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy -u ALL_PROXY -u all_proxy \
    tccli teo "$action" --ZoneId "$ZONE" --endpoint "$ENDPOINT" >"$out"
}

if [ "${FETCH:-0}" = 1 ]; then
  ZONE_JSON="$OUT_DIR/zone-origin-acl.json"
  FAMILY_JSON="$OUT_DIR/available-origin-acl-family.json"
  echo "fetching (read-only): DescribeOriginACL + DescribeAvailableOriginACLFamily zone=$ZONE"
  fetch_read DescribeOriginACL "$ZONE_JSON"
  fetch_read DescribeAvailableOriginACLFamily "$FAMILY_JSON"
fi

# --- self-test: the two authorities and the refusal, on checked shapes --------
if [ "$SELF_TEST" = 1 ]; then
  T=$(mktemp -d)
  trap 'rm -rf "$T"' EXIT
  cat >"$T/online.json" <<'JSON'
{"OriginACLInfo":{"Status":"online","OriginACLFamily":"gaz","L7Hosts":["cdn-oracle.isui.ren"],"L4ProxyIds":[],
 "CurrentOriginACL":{"Version":"gaz-0.0.5-20261012","EntireAddresses":{"IPv4":["2.2.2.0/24","1.1.1.0/24"],"IPv6":["2402:4e00::/36"]}},"NextOriginACL":null}}
JSON
  cat >"$T/offline.json" <<'JSON'
{"OriginACLInfo":{"Status":"offline","OriginACLFamily":"","L7Hosts":[],"L4ProxyIds":[],"CurrentOriginACL":null,"NextOriginACL":null}}
JSON
  cat >"$T/family.json" <<'JSON'
{"TotalCount":1,"OriginACLFamilyInfos":[{"Version":"gaz-0.0.4-20260907","ActiveTime":"2026-10-12T00:00:00+08:00","OriginACLFamily":"gaz",
 "EntireAddresses":{"IPv4":["1.71.146.0/23","36.150.103.0/24","27.44.204.0/22"],"IPv6":["2402:4e00::/36","2408:8706:2::/48"]}}]}
JSON
  fail=0
  check() { # <label> <expected> <actual>
    if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  FAIL $1 (want $2, got $3)"; fail=1; fi
  }
  echo "self-test: authoritative zone (Status=online)"
  bash "$0" --input "$T/online.json" --out-dir "$T/a" --state "$T/a.state" >"$T/a.out" 2>&1 || true
  check "two v4 prefixes"  "2" "$(wc -l <"$T/a/ipv4.txt" | tr -d ' ')"
  check "one v6 prefix"    "1" "$(wc -l <"$T/a/ipv6.txt" | tr -d ' ')"
  check "sorted"           "1.1.1.0/24" "$(head -1 "$T/a/ipv4.txt")"
  grep -q "AUTH=yes" "$T/a.out" && check "authoritative" "yes" "yes" || check "authoritative" "yes" "no"
  grep -q "gaz-0.0.5-20261012" "$T/a.out" && check "version reported" "yes" "yes" || check "version reported" "yes" "no"
  check "apply refused below" "0" "0"

  echo "self-test: catalog only (Status=offline) must be refused for apply"
  set +e
  bash "$0" --input "$T/offline.json" --family "$T/family.json" --mode apply-nft \
      --out-dir "$T/b" --state "$T/b.state" >"$T/b.out" 2>&1
  rc=$?
  set -e
  check "apply refused (exit 2)" "2" "$rc"
  grep -q "not authoritative" "$T/b.out" && check "refusal says why" "yes" "yes" || check "refusal says why" "yes" "no"
  check "catalog still parsed" "3" "$(wc -l <"$T/b/ipv4.txt" | tr -d ' ')"
  bash "$0" --input "$T/offline.json" --family "$T/family.json" --mode report \
      --out-dir "$T/c" --state "$T/c.state" >"$T/c.out" 2>&1
  grep -q "AUTH=no" "$T/c.out" && check "report works and marks it" "yes" "yes" || check "report works" "yes" "no"
  echo "self-test: idempotent (second report changes nothing)"
  bash "$0" --input "$T/online.json" --out-dir "$T/a" --state "$T/a.state" >"$T/a2.out" 2>&1
  grep -q "unchanged" "$T/a2.out" && check "unchanged on re-run" "yes" "yes" || check "unchanged on re-run" "yes" "no"
  echo "self-test: a pending update is announced"
  sed 's/"NextOriginACL":null/"NextOriginACL":{"Version":"gaz-0.0.6-20261101","EntireAddresses":{"IPv4":["9.9.9.0\/24"],"IPv6":[]}}/' \
      "$T/online.json" >"$T/next.json"
  bash "$0" --input "$T/next.json" --out-dir "$T/d" --state "$T/d.state" >"$T/d.out" 2>&1
  grep -q "ConfirmOriginACLUpdate" "$T/d.out" && check "confirm step named" "yes" "yes" || check "confirm step named" "yes" "no"
  [ "$fail" = 0 ] && { echo "self-test: PASS"; exit 0; } || { echo "self-test: FAIL"; exit 3; }
fi

[ -n "$ZONE_JSON" ] || { echo "need --fetch or --input" >&2; exit 3; }
[ -f "$ZONE_JSON" ] || { echo "no such input: $ZONE_JSON" >&2; exit 3; }

# --- parse ---------------------------------------------------------------------
# Tolerant on purpose: the fields this cares about have moved before (and the
# zone read's own nested type is not in the published model), so it walks the
# document for the shapes it needs and refuses loudly when they are absent rather
# than reporting an empty set as success.
PARSE=$(python3 - "$ZONE_JSON" "${FAMILY_JSON:-}" "$OUT_DIR" <<'PY'
import json, sys, hashlib

zone_path, family_path, out_dir = sys.argv[1], sys.argv[2], sys.argv[3]

def lists_in(node, acc):
    if isinstance(node, dict):
        if isinstance(node.get("IPv4"), list) or isinstance(node.get("IPv6"), list):
            acc.append(node)
        for v in node.values():
            lists_in(v, acc)
    elif isinstance(node, list):
        for v in node:
            lists_in(v, acc)

def collect(node):
    acc = []
    lists_in(node, acc)
    v4 = sorted({p for a in acc for p in (a.get("IPv4") or []) if isinstance(p, str)})
    v6 = sorted({p for a in acc for p in (a.get("IPv6") or []) if isinstance(p, str)})
    return v4, v6

zone = json.load(open(zone_path))
info = zone.get("OriginACLInfo") or zone
status = (info.get("Status") or "").lower()
family_name = info.get("OriginACLFamily") or ""
next_acl = info.get("NextOriginACL")

authoritative = status == "online"
version, active, why = "", "", ""
if authoritative:
    v4, v6 = collect(info.get("CurrentOriginACL") or {})
    cur = info.get("CurrentOriginACL") or {}
    version = cur.get("Version") or family_name or "current"
    active = cur.get("EffectiveTime") or cur.get("ActiveTime") or ""
    why = "zone is online: these are the peers EdgeOne is bound to pull from"
else:
    v4, v6 = [], []
    if family_path:
        fam = json.load(open(family_path))
        infos = fam.get("OriginACLFamilyInfos") or []
        if infos:
            newest = infos[0]
            v4, v6 = collect(newest)
            version = newest.get("Version") or ""
            active = newest.get("ActiveTime") or ""
    why = f"zone reports Status={status or 'unknown'}: these ranges are the catalog, not what EdgeOne is bound to"

if not v4 and not v6:
    print("PARSE_FAIL: no IPv4/IPv6 prefixes found (refusing to report an empty set as success)", file=sys.stderr)
    sys.exit(3)

open(f"{out_dir}/ipv4.txt", "w").write("".join(p + "\n" for p in v4))
open(f"{out_dir}/ipv6.txt", "w").write("".join(p + "\n" for p in v6))
digest = hashlib.sha256(("\n".join(v4) + "|" + "\n".join(v6)).encode()).hexdigest()[:16]
print(f"AUTH={'yes' if authoritative else 'no'}")
print(f"STATUS={status or 'unknown'}")
print(f"VERSION={version}")
print(f"ACTIVE={active}")
print(f"V4N={len(v4)}")
print(f"V6N={len(v6)}")
print(f"NEXT={'yes' if next_acl else 'no'}")
print(f"HASH={digest}")
print(f"WHY={why}")
PY
)

# --- act ------------------------------------------------------------------------
AUTH= STATUS= VERSION= ACTIVE= V4N= V6N= NEXT= HASH= WHY=
while IFS='=' read -r k v; do
  case "$k" in
    AUTH) AUTH=$v ;; STATUS) STATUS=$v ;; VERSION) VERSION=$v ;; ACTIVE) ACTIVE=$v ;;
    V4N) V4N=$v ;; V6N) V6N=$v ;; NEXT) NEXT=$v ;; HASH) HASH=$v ;; WHY) WHY=$v ;;
  esac
done <<<"$PARSE"

say() { printf '%s\n' "$*"; }
say "origin-pull ranges · zone=$ZONE · status=$STATUS · version=${VERSION:-?}${ACTIVE:+ (effective $ACTIVE)}"
say "  prefixes   $V4N IPv4 + $V6N IPv6   hash=$HASH"
say "  authority  $WHY"
# The machine-readable twin of the lines above: for whatever consumes this — a
# timer, a report, the OCI side's own automation.
say "  meta       AUTH=$AUTH STATUS=$STATUS VERSION=$VERSION V4N=$V4N V6N=$V6N NEXT=$NEXT HASH=$HASH"

PREV=$( [ -f "$STATE" ] && cat "$STATE" || echo none )
UNCHANGED=0
if [ "$PREV" = "$HASH" ]; then
  say "  state      unchanged since the last run ($STATE)"
  UNCHANGED=1
else
  say "  state      changed: $PREV -> $HASH"
fi

if [ "$AUTH" = no ] && [ "$FORCE" != 1 ]; then
  say ""
  say "REFUSED: the ranges above are not authoritative for this zone (Status=$STATUS)."
  say "  EdgeOne is not bound to pull from them, so an allowlist built on them can"
  say "  block the pull nodes actually in use — the CDN path would fail and look like"
  say "  a CDN fault. What makes them binding is origin protection, which this plan"
  say "  does not include (docs/security-hardening.md R3): upgrade, or use --force if"
  say "  you have measured which peers really pull and accept that risk."
  if [ "$MODE" = report ]; then
    say "  (report mode: nothing was applied; the set files are written for inspection)"
    echo "$HASH" >"$STATE"
    exit 0
  fi
  exit 2
fi

case "$MODE" in
  report)
    say ""
    say "written: $OUT_DIR/ipv4.txt ($V4N lines), $OUT_DIR/ipv6.txt ($V6N lines)"
    if [ "$NEXT" = yes ]; then
      say ""
      say "A NEW VERSION IS PENDING. Apply the new set, then tell Tencent it is in place:"
      say "  tccli teo ConfirmOriginACLUpdate --ZoneId $ZONE --endpoint $ENDPOINT"
      say "  (until you do, the change notifications keep coming)"
    fi
    echo "$HASH" >"$STATE"
    ;;

  apply-nft)
    # Refill an EXISTING set. Creating the set and the rules around it is R5's
    # decision, not this tool's: a firewall that appears because a sync ran would
    # be a surprise, and the rules that use this set have to be written to match
    # the rest of the node's policy.
    if ! nft list set $NFT_SET >/dev/null 2>&1; then
      say "FAIL: nftables set $NFT_SET does not exist. Create it first (R5), e.g.:"
      say "  nft add set $NFT_SET { type ipv4_addr\\; flags interval\\; }   # and an ipv6_addr set"
      exit 3
    fi
    if [ "$UNCHANGED" = 1 ] && [ "$FORCE" != 1 ]; then
      say "  apply      skipped: nothing changed (use --force to rewrite anyway)"
      exit 0
    fi
    say "  apply      refilling $NFT_SET from $OUT_DIR (needs root)"
    {
      printf 'flush set %s\n' "$NFT_SET"
      if [ "$V4N" != 0 ]; then
        printf 'add element %s { %s }\n' "$NFT_SET" "$(paste -sd, "$OUT_DIR/ipv4.txt")"
      fi
    } | nft -f -
    say "  apply      done: $(nft list set $NFT_SET 2>/dev/null | grep -c '/') element line(s) now in the set"
    echo "$HASH" >"$STATE"
    ;;

  emit-oci)
    RULES="$OUT_DIR/oci-ingress-rules.json"
    python3 - "$OUT_DIR" "$PORT" "$RULES" <<'PY'
import json, sys
out_dir, port, dest = sys.argv[1], int(sys.argv[2]), sys.argv[3]
def read(name):
    try:
        return [l.strip() for l in open(f"{out_dir}/{name}") if l.strip()]
    except FileNotFoundError:
        return []
rules = [{
    "protocol": "6",
    "source": cidr,
    "sourceType": "CIDR_BLOCK",
    "isStateless": False,
    "tcpOptions": {"destinationPortRange": {"min": port, "max": port}},
    "description": "EdgeOne origin pull (origin-pull-cidrs.sh)",
} for cidr in read("ipv4.txt") + read("ipv6.txt")]
json.dump(rules, open(dest, "w"), indent=1)
print(f"  emit       {dest}: {len(rules)} ingress rules for TCP/{port}")
PY
    say ""
    say "Apply with (this script does not call write APIs):"
    say "  oci network security-list update --security-list-id <ocid1.securitylist...> \\"
    say "      --ingress-security-rules file://$RULES --force"
    say ""
    say "Two things the owner of that perimeter should weigh first:"
    say "  · a security list has a rule-count ceiling and this set is $((V4N + V6N)) prefixes;"
    say "    nftables sets are built for that volume (mode apply-nft), and the coarse"
    say "    cloud rule can stay 'allow 7777' while the node does the fine-grained allow."
    say "  · the payload contains ONLY the pull ranges: merge it with whatever else the"
    say "    list already allows (22, ICMP) rather than replacing the list with it."
    echo "$HASH" >"$STATE"
    ;;
esac

#!/usr/bin/env bash
# Read-only: how is the edge configured to fetch from our origin, right now?
#
# Three layers decide that, and they are not in the same place:
#
#   * site-wide L7 acceleration settings (DescribeL7AccSetting) -- this is where
#     HTTP/2 to the origin lives, as UpstreamHttp2.Switch;
#   * the rule engine (DescribeL7AccRules) -- sharded origin pull
#     (RangeOriginPull) exists ONLY here, as a rule action; it is absent from
#     the site-wide request schema entirely, so a zone with no rules has it
#     unset;
#   * each acceleration domain (DescribeAccelerationDomains) -- the origin
#     address, the origin protocol, the ports and the Host header.
#
# Every raw response is saved under $EO_DUMP_DIR (default /tmp/eo-audit), which
# is deliberately outside the repository: a dump carries the origin address, and
# no tracked file may. All three calls are reads; nothing here writes.
#
# Usage:  eo-audit.sh [ZoneId ...]        (no argument: audit every visible zone)
#
# Exit status is non-zero only if a call failed; an unset knob is a reading, not
# an error.

set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
EO="$HERE/eo.sh"
DUMP="${EO_DUMP_DIR:-/tmp/eo-audit}"
mkdir -p "$DUMP"

if [ "$#" -gt 0 ]; then
    ZONES="$*"
else
    "$EO" DescribeZones --Offset 0 --Limit 100 > "$DUMP/zones.json"
    ZONES=$(python3 - "$DUMP/zones.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
for z in d.get("Zones") or []:
    print(z.get("ZoneId", ""))
PY
)
fi

if [ -z "${ZONES// /}" ]; then
    echo "no zones visible to this key" >&2
    exit 1
fi

for zone in $ZONES; do
    "$EO" DescribeL7AccSetting --ZoneId "$zone"        > "$DUMP/$zone.l7acc.json"
    "$EO" DescribeL7AccRules --ZoneId "$zone" --Limit 200 > "$DUMP/$zone.rules.json"
    "$EO" DescribeAccelerationDomains --ZoneId "$zone" --Offset 0 --Limit 200 \
                                                       > "$DUMP/$zone.domains.json"

    python3 - "$DUMP/$zone.l7acc.json" "$DUMP/$zone.rules.json" "$DUMP/$zone.domains.json" <<'PY'
import json, sys

acc = json.load(open(sys.argv[1]))
rules = json.load(open(sys.argv[2]))
domains = json.load(open(sys.argv[3]))

def sw(v):
    """Print a Switch value the way an operator reads it."""
    if v is None:
        return "unset"
    return str(v)

zs = acc.get("ZoneSetting") or {}
print("==", zs.get("ZoneName") or acc.get("RequestId"), "  area=%s" % zs.get("Area", "?"))

print("   site-wide settings that touch the origin leg")
print("     UpstreamHttp2 (HTTP/2 to the origin)   %s" % sw((zs.get("UpstreamHttp2") or {}).get("Switch")))
print("     SmartRouting  (smart acceleration)     %s" % sw((zs.get("SmartRouting") or {}).get("Switch")))
print("     OfflineCache                           %s" % sw((zs.get("OfflineCache") or {}).get("Switch")))
print("     AccelerateMainland                     %s" % sw((zs.get("AccelerateMainland") or {}).get("Switch")))
if zs.get("Origin"):
    print("     site-level Origin block                %s" % json.dumps(zs["Origin"], ensure_ascii=False))

# The rule engine is ordered: earlier rules win. RangeOriginPull can only be
# set on a rule, so this list is the only place it can appear.
rs = rules.get("Rules") or []
print("   rule engine: %d rule(s), in priority order" % len(rs))
origin_actions = (
    "RangeOriginPull", "UpstreamHTTP2", "HTTPUpstreamTimeout",
    "SmartRouting", "AdvancedOriginRouting", "ModifyOrigin", "OriginPullProtocol",
)
for i, r in enumerate(rs):
    print("     [%d] %-7s %-22s %s" % (i + 1, r.get("Status", "?"), r.get("RuleId", "?"),
                                      r.get("RuleName", "")))
    for bi, br in enumerate(r.get("Branches") or []):
        print("         branch %d  condition=%s" % (bi, br.get("Condition", "")))
        acts = br.get("Actions") or []
        seen = False
        for a in acts:
            name = a.get("Name", "")
            if name in origin_actions:
                seen = True
                param = a.get(name + "Parameters") or {}
                print("           %-22s %s" % (name, json.dumps(param, ensure_ascii=False)))
        if not seen:
            print("           (no origin-pull action on this branch)")

ds = domains.get("AccelerationDomains") or []
print("   acceleration domains: %d" % len(ds))
for d in ds:
    print("     %-34s %-8s proto=%-7s ports=%s/%s" % (
        d.get("DomainName", "?"), d.get("DomainStatus", "?"),
        d.get("OriginProtocol", "?"), d.get("HttpOriginPort", "?"), d.get("HttpsOriginPort", "?")))
    od = d.get("OriginDetail")
    if od:
        print("         origin: %s" % json.dumps(od, ensure_ascii=False))

# The one-line verdict this audit exists to produce.
u2 = sw((zs.get("UpstreamHttp2") or {}).get("Switch"))
rop = "unset"
for r in rs:
    if r.get("Status") != "enable":
        continue
    for br in r.get("Branches") or []:
        for a in br.get("Actions") or []:
            if a.get("Name") == "RangeOriginPull":
                rop = sw((a.get("RangeOriginPullParameters") or {}).get("Switch"))
print("   VERDICT  UpstreamHttp2=%s  RangeOriginPull=%s" % (u2, rop))
print()
PY
    echo "   raw: $DUMP/$zone.l7acc.json  $DUMP/$zone.rules.json  $DUMP/$zone.domains.json"
    echo
done

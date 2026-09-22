#!/usr/bin/env bash
# The two origin-pull knobs this project actually measured a reason to touch,
# and nothing else.
#
#   --upstream-http2 on|off
#       Site-wide, UpstreamHttp2.Switch. One HTTP/2 connection to the origin can
#       carry many parallel shard requests, where HTTP/1.1 needs one TCP
#       connection per shard. Our own numbers say multiplexing is not free but
#       is cheaper than connection count, and the edge's shard pulls are
#       exactly the many-parallel-requests shape.
#
#   --range-origin-pull on|off --rule-id <id>
#       Per rule, RangeOriginPullParameters.Switch. Sharded origin pull asks
#       the origin for ranges rather than whole objects. It exists ONLY as a
#       rule action: it is absent from the site-wide schema, so this script does
#       not invent a rule.
#
#   --upstream-timeout <5..600> --rule-id <id>
#       Per rule, HTTPUpstreamTimeoutParameters.ResponseTimeout, in seconds. The
#       product answers an open-ended range with a 200 GiB promise; a short
#       origin-read timeout is one candidate explanation for a client that
#       stalls with a body already open.
#
# Safety, in the order the script applies it:
#   1. Dry run by default. Nothing is written without --yes.
#   2. Reads first and prints the exact request body it would send.
#   3. Writes only the block named on the command line, never a round-tripped
#      copy of everything the read returned (the read and the write use
#      different key spellings -- UpstreamHttp2 vs UpstreamHTTP2 -- and a
#      mechanical round trip is how a setting gets silently dropped).
#   4. Re-reads after the write and diffs the whole setting object; anything
#      changed that was not asked for is reported and the exit status is 3.
#   5. Before and after dumps land in $EO_DUMP_DIR (default /tmp/eo-audit),
#      outside the repository, so the revert value is always on disk.
#
# Usage:
#   eo-origin-pull.sh --zone <ZoneId> --upstream-http2 on            # dry run
#   eo-origin-pull.sh --zone <ZoneId> --upstream-http2 on --yes
#   eo-origin-pull.sh --zone <ZoneId> --range-origin-pull on --rule-id rule-xxx --yes
#   eo-origin-pull.sh --zone <ZoneId> --upstream-timeout 60 --rule-id rule-xxx --yes
#
# With no knob named, it prints the current values and exits.

set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
EO="$HERE/eo.sh"
DUMP="${EO_DUMP_DIR:-/tmp/eo-audit}"
mkdir -p "$DUMP"

ZONE=""
KNOB=""
VALUE=""
RULE_ID=""
CONFIRM=0

die() { echo "error: $*" >&2; exit 2; }

while [ "$#" -gt 0 ]; do
    case "$1" in
        --zone)              ZONE="${2:-}"; shift 2 ;;
        --upstream-http2)    KNOB=upstream-http2; VALUE="${2:-}"; shift 2 ;;
        --range-origin-pull) KNOB=range-origin-pull; VALUE="${2:-}"; shift 2 ;;
        --upstream-timeout)  KNOB=upstream-timeout; VALUE="${2:-}"; shift 2 ;;
        --rule-id)           RULE_ID="${2:-}"; shift 2 ;;
        --yes)               CONFIRM=1; shift ;;
        -h|--help)           sed -n '2,50p' "$0"; exit 0 ;;
        *)                   die "unknown argument: $1" ;;
    esac
done

[ -n "$ZONE" ] || die "--zone <ZoneId> is required"

case "$KNOB" in
    "")                  ;;
    upstream-http2)      case "$VALUE" in on|off) ;; *) die "--upstream-http2 takes on or off" ;; esac ;;
    range-origin-pull)   case "$VALUE" in on|off) ;; *) die "--range-origin-pull takes on or off" ;; esac
                         [ -n "$RULE_ID" ] || die "--range-origin-pull needs --rule-id (this knob lives on a rule, and this script does not invent rules)" ;;
    upstream-timeout)    case "$VALUE" in ''|*[!0-9]*) die "--upstream-timeout takes an integer number of seconds" ;; esac
                         { [ "$VALUE" -ge 5 ] && [ "$VALUE" -le 600 ]; } || die "--upstream-timeout must be 5..600 seconds"
                         [ -n "$RULE_ID" ] || die "--upstream-timeout needs --rule-id" ;;
esac

# --- read current state -------------------------------------------------------

read_settings() { "$EO" DescribeL7AccSetting --ZoneId "$ZONE" > "$1"; }

read_settings "$DUMP/$ZONE.before.json"

if [ -z "$KNOB" ]; then
    python3 - "$DUMP/$ZONE.before.json" <<'PY'
import json, sys
zs = (json.load(open(sys.argv[1])).get("ZoneSetting") or {})
print("UpstreamHttp2.Switch = %s" % ((zs.get("UpstreamHttp2") or {}).get("Switch") or "unset"))
print("(RangeOriginPull and HTTPUpstreamTimeout are rule actions; run eo-audit.sh for those.)")
PY
    exit 0
fi

# --- dry run: show the exact body, change nothing -----------------------------

if [ "$CONFIRM" -ne 1 ]; then
    echo "DRY RUN -- nothing will be written. Re-run with --yes to apply."
fi

apply_site_upstream_http2() {
    local body
    body=$(printf '{"UpstreamHTTP2":{"Switch":"%s"}}' "$VALUE")
    echo "  zone  $ZONE"
    echo "  call  ModifyL7AccSetting"
    echo "  body  ZoneConfig=$body"
    [ "$CONFIRM" -eq 1 ] || return 0
    "$EO" ModifyL7AccSetting --ZoneId "$ZONE" --ZoneConfig "$body" > "$DUMP/$ZONE.write.json"
}

apply_rule_action() {
    local action_name="$1" params_key="$2" params_json="$3"
    local rule_file="$DUMP/$ZONE.rule.$RULE_ID.before.json"

    "$EO" DescribeL7AccRules --ZoneId "$ZONE" --Limit 200 > "$DUMP/$ZONE.rules.before.json"

    # Build the modified rule: same object the API returned, with the one action
    # inserted or replaced on the first branch. A rule with no branch cannot
    # carry an action, and we say so rather than fabricating a condition.
    python3 - "$DUMP/$ZONE.rules.before.json" "$rule_file" "$RULE_ID" \
             "$action_name" "$params_key" "$params_json" <<'PY'
import json, sys
src, out, rule_id, action, params_key, params_json = sys.argv[1:7]
rules = json.load(open(src)).get("Rules") or []
match = None
for r in rules:
    if r.get("RuleId") == rule_id:
        match = r
        break
if match is None:
    sys.exit("  no rule with RuleId %s in this zone. Existing rules: %s"
             % (rule_id, ", ".join("%s(%s)" % (r.get("RuleId"), r.get("RuleName")) for r in rules) or "none"))
branches = match.get("Branches") or []
if not branches:
    sys.exit("  rule %s has no branch, so it cannot carry a %s action. "
             "Add a branch in the console first, or name a rule that has one." % (rule_id, action))
acts = branches[0].setdefault("Actions", [])
for a in acts:
    if a.get("Name") == action:
        a[params_key] = json.loads(params_json)
        break
else:
    entry = {"Name": action, params_key: json.loads(params_json)}
    acts.append(entry)
json.dump(match, open(out, "w"), ensure_ascii=False)
print("  rule  %s  %s" % (rule_id, match.get("RuleName", "")))
print("  call  ModifyL7AccRule")
print("  body  Rule=%s" % json.dumps(match, ensure_ascii=False))
PY

    [ "$CONFIRM" -eq 1 ] || return 0
    "$EO" ModifyL7AccRule --ZoneId "$ZONE" --Rule "$(cat "$rule_file")" > "$DUMP/$ZONE.rule.write.json"
}

case "$KNOB" in
    upstream-http2)
        apply_site_upstream_http2
        ;;
    range-origin-pull)
        apply_rule_action RangeOriginPull RangeOriginPullParameters \
                          "$(printf '{"Switch":"%s"}' "$VALUE")"
        ;;
    upstream-timeout)
        apply_rule_action HTTPUpstreamTimeout HTTPUpstreamTimeoutParameters \
                          "$(printf '{"ResponseTimeout":%s}' "$VALUE")"
        ;;
esac

if [ "$CONFIRM" -ne 1 ]; then
    echo
    echo "no write performed."
    exit 0
fi

# --- verify: re-read, and diff everything that moved ---------------------------

read_settings "$DUMP/$ZONE.after.json"

set +e
python3 - "$DUMP/$ZONE.before.json" "$DUMP/$ZONE.after.json" "$KNOB" <<'PY'
import json, sys

before = (json.load(open(sys.argv[1])).get("ZoneSetting") or {})
after = (json.load(open(sys.argv[2])).get("ZoneSetting") or {})
knob = sys.argv[3]

def flat(o, p=""):
    if isinstance(o, dict):
        for k, v in o.items():
            yield from flat(v, "%s.%s" % (p, k) if p else k)
    elif isinstance(o, list):
        for i, v in enumerate(o):
            yield from flat(v, "%s[%d]" % (p, i))
    else:
        yield p, o

b, a = dict(flat(before)), dict(flat(after))
changed = {}
for k in sorted(set(b) | set(a)):
    if b.get(k) != a.get(k):
        changed[k] = (b.get(k), a.get(k))

if knob == "upstream-http2":
    print("  site-wide setting diff")
else:
    # A rule write should not move the site-wide setting at all; a line here
    # means either collateral damage or somebody editing the zone at the same
    # time. Both want eyes on them, so both are a non-zero exit.
    print("  site-wide setting diff (a rule write must leave this empty)")

for k, (old, new) in changed.items():
    print("    %-40s %s -> %s" % (k, old, new))
if not changed:
    print("    (nothing changed)")

WANT = "UpstreamHttp2.Switch"
if knob == "upstream-http2":
    if WANT not in changed:
        print("  FAIL: %s did not move; the write did not do what it was asked to" % WANT)
        sys.exit(3)
    extra = set(changed) - {WANT}
    if extra:
        print("  FAIL: it also moved %s, which was not asked for" % ", ".join(sorted(extra)))
        sys.exit(3)
elif changed:
    sys.exit(3)
PY
rc=$?
set -e

if [ "$KNOB" = "range-origin-pull" ] || [ "$KNOB" = "upstream-timeout" ]; then
    "$EO" DescribeL7AccRules --ZoneId "$ZONE" --Limit 200 > "$DUMP/$ZONE.rules.after.json"
    python3 - "$DUMP/$ZONE.rules.after.json" "$RULE_ID" <<'PY'
import json, sys
rules = json.load(open(sys.argv[1])).get("Rules") or []
want = sys.argv[2]
for r in rules:
    if r.get("RuleId") != want:
        continue
    for br in r.get("Branches") or []:
        for a in br.get("Actions") or []:
            name = a.get("Name", "")
            if name in ("RangeOriginPull", "HTTPUpstreamTimeout", "UpstreamHTTP2"):
                print("  rule now reads  %-20s %s" % (name, json.dumps(a.get(name + "Parameters") or {}, ensure_ascii=False)))
PY
fi

echo
echo "before: $DUMP/$ZONE.before.json"
echo "after:  $DUMP/$ZONE.after.json"
if [ "$rc" -ne 0 ]; then
    echo "exit $rc: the zone is not in the state this command asked for -- read the diff above." >&2
    echo "the value before the write is in $DUMP/$ZONE.before.json" >&2
fi
exit "$rc"

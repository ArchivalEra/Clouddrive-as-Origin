#!/usr/bin/env bash
# Offline test for the EdgeOne control-plane scripts.
#
# The interesting logic in deploy/edge/ is not the API call, it is what happens
# around it: reading before writing, sending only the block that was asked for,
# and noticing afterwards whether anything else moved. None of that needs an
# account, so none of it should need one to be tested.
#
# This script stands a stub `eo.sh` in place of the real wrapper and replays
# recorded teo responses from fixture files, then asserts the exit codes and the
# text the operator would read. No network, no credentials, no account touched.
#
# Usage:  deploy/edge/selftest.sh
# Prints one line per case and a PASS/FAIL verdict; exit 0 iff every case passed.

set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d "${TMPDIR:-/tmp}/eo-selftest.XXXXXX")
trap 'rm -rf "$T"' EXIT

pass=0
fail=0
ok()  { pass=$((pass + 1)); printf '  PASS  %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf '  FAIL  %s\n' "$1"; }

# The scripts under test, next to the stub that will answer them.
cp "$HERE/eo-audit.sh" "$HERE/eo-origin-pull.sh" "$T/"

cat > "$T/eo.sh" <<'STUB'
#!/usr/bin/env bash
# Stub standing in for the network: replays recorded teo responses from
# $EO_FIXTURES and counts calls, so a test can assert that a dry run made none.
ACT=$1; shift
SET="${EO_FIXTURES:?}"
CNT="$EO_COUNTERS/$ACT"
n=$(cat "$CNT" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$CNT"
f="$SET/$ACT.$n.json"
[ -f "$f" ] || f="$SET/$ACT.json"
if [ ! -f "$f" ]; then
    echo "stub: no fixture for $ACT" >&2
    exit 1
fi
cat "$f"
STUB
chmod +x "$T/eo.sh"

python3 - "$T" <<'PY'
import json, os, sys
T = sys.argv[1]

def mk(setname, files):
    d = os.path.join(T, setname)
    os.makedirs(d, exist_ok=True)
    for name, obj in files.items():
        json.dump(obj, open(os.path.join(d, name), "w"), ensure_ascii=False, indent=1)

base = {
    "ZoneName": "example.test", "Area": "overseas",
    "UpstreamHttp2": {"Switch": "off"}, "SmartRouting": {"Switch": "off"},
    "OfflineCache": {"Switch": "off"}, "AccelerateMainland": {"Switch": "off"},
}
moved = json.loads(json.dumps(base)); moved["UpstreamHttp2"] = {"Switch": "on"}
collateral = json.loads(json.dumps(moved)); collateral["SmartRouting"] = {"Switch": "on"}

rule = {
    "Status": "enable", "RuleId": "rule-aaa", "RuleName": "default",
    "Description": ["catch-all"],
    "Branches": [{"Condition": "true",
                  "Actions": [{"Name": "Cache",
                               "CacheParameters": {"FollowOrigin": {"Switch": "on"}}}]}],
}
rule_after = json.loads(json.dumps(rule))
rule_after["Branches"][0]["Actions"].append(
    {"Name": "RangeOriginPull", "RangeOriginPullParameters": {"Switch": "on"}})

zones = {"TotalCount": 1, "RequestId": "r1", "Zones": [
    {"ZoneId": "zone-test1234", "ZoneName": "example.test", "Area": "overseas", "Status": "active"}]}
domains = {"TotalCount": 1, "RequestId": "r6", "AccelerationDomains": [
    {"DomainName": "cdn.example.test", "DomainStatus": "online", "OriginProtocol": "FOLLOW",
     "HttpOriginPort": 80, "HttpsOriginPort": 443,
     "OriginDetail": {"OriginType": "IP_DOMAIN", "Origin": "origin.example.test"}}]}
write_ok = {"RequestId": "rw"}

# `good`: the setting moves exactly as asked.
mk("good", {"DescribeZones.json": zones, "DescribeL7AccSetting.1.json": {"ZoneSetting": base, "RequestId": "r2"},
            "DescribeL7AccSetting.2.json": {"ZoneSetting": moved, "RequestId": "r3"},
            "DescribeL7AccRules.1.json": {"TotalCount": 1, "Rules": [rule], "RequestId": "r4"},
            "DescribeL7AccRules.2.json": {"TotalCount": 1, "Rules": [rule_after], "RequestId": "r5"},
            "DescribeAccelerationDomains.json": domains,
            "ModifyL7AccSetting.json": write_ok, "ModifyL7AccRule.json": write_ok})
# `collateral`: the intended field moves AND an unrelated one does too.
mk("collateral", {"DescribeZones.json": zones,
                  "DescribeL7AccSetting.1.json": {"ZoneSetting": base, "RequestId": "r2"},
                  "DescribeL7AccSetting.2.json": {"ZoneSetting": collateral, "RequestId": "r3"},
                  "DescribeL7AccRules.1.json": {"TotalCount": 1, "Rules": [rule], "RequestId": "r4"},
                  "DescribeL7AccRules.2.json": {"TotalCount": 1, "Rules": [rule_after], "RequestId": "r5"},
                  "DescribeAccelerationDomains.json": domains,
                  "ModifyL7AccSetting.json": write_ok, "ModifyL7AccRule.json": write_ok})
# `still`: the site-wide setting does not move at all (a rule write's normal timeline).
mk("still", {"DescribeZones.json": zones,
             "DescribeL7AccSetting.1.json": {"ZoneSetting": base, "RequestId": "r2"},
             "DescribeL7AccSetting.2.json": {"ZoneSetting": base, "RequestId": "r3"},
             "DescribeL7AccRules.1.json": {"TotalCount": 1, "Rules": [rule], "RequestId": "r4"},
             "DescribeL7AccRules.2.json": {"TotalCount": 1, "Rules": [rule_after], "RequestId": "r5"},
             "DescribeAccelerationDomains.json": domains,
             "ModifyL7AccSetting.json": write_ok, "ModifyL7AccRule.json": write_ok})
PY

OUT="$T/out.txt"
COUNTERS="$T/counters"

# reset <fixture-set>  -- fresh call counters and dump dir for one case
reset() { rm -rf "$COUNTERS" "$T/dump"; mkdir -p "$COUNTERS"; }

# run <fixture-set> <cmd...>
run() {
    local set=$1; shift
    EO_FIXTURES="$T/$set" EO_COUNTERS="$COUNTERS" EO_DUMP_DIR="$T/dump" "$@" > "$OUT" 2>"$T/err.txt"
}

calls() { cat "$COUNTERS/$1" 2>/dev/null || echo 0; }
has()      { grep -qF -- "$1" "$OUT"; }
has_err()  { grep -qF -- "$1" "$T/err.txt"; }

echo "deploy/edge selftest (offline; no credentials, no account)"

# --- the audit: reads only, names the knobs, writes nothing -------------------
reset good
run good "$T/eo-audit.sh"
rc=$?
[ "$rc" -eq 0 ] && ok "audit exits 0" || bad "audit exit $rc"
has "VERDICT  UpstreamHttp2=off  RangeOriginPull=unset" \
    && ok "audit reports both knobs (UpstreamHttp2=off, RangeOriginPull=unset)" \
    || bad "audit verdict line missing"
has "origin.example.test" && ok "audit prints the origin the edge would pull from" \
    || bad "audit did not print the origin"
[ -f "$T/dump/zone-test1234.rules.json" ] && ok "audit saves the raw rule dump outside the repo" \
    || bad "audit did not save the rule dump"

# --- a dry run must not write ------------------------------------------------
reset good
run good "$T/eo-origin-pull.sh" --zone zone-test1234 --upstream-http2 on
rc=$?
[ "$rc" -eq 0 ] && ok "dry run exits 0" || bad "dry run exit $rc"
has "DRY RUN" && ok "dry run says it is a dry run" || bad "dry run did not announce itself"
[ "$(calls ModifyL7AccSetting)" = "0" ] && ok "dry run made no ModifyL7AccSetting call" \
    || bad "dry run called ModifyL7AccSetting $(calls ModifyL7AccSetting) time(s)"

# --- the site-wide knob, applied ---------------------------------------------
reset good
run good "$T/eo-origin-pull.sh" --zone zone-test1234 --upstream-http2 on --yes
rc=$?
[ "$rc" -eq 0 ] && ok "applied site-wide knob exits 0" || bad "applied site-wide knob exit $rc"
has "UpstreamHttp2.Switch                     off -> on" \
    && ok "the after-read shows the intended field moving" || bad "intended-field diff missing"
has "site-wide setting diff" && ok "the diff is labelled as site-wide" || bad "diff label missing"

# --- collateral damage is caught and is non-zero -----------------------------
reset collateral
run collateral "$T/eo-origin-pull.sh" --zone zone-test1234 --upstream-http2 on --yes
rc=$?
[ "$rc" -eq 3 ] && ok "unasked-for change exits 3" || bad "unasked-for change exit $rc (wanted 3)"
has "FAIL: it also moved SmartRouting.Switch" \
    && ok "the unasked-for field is named" || bad "the unasked-for field was not named"

# --- the rule knob ------------------------------------------------------------
reset still
run still "$T/eo-origin-pull.sh" --zone zone-test1234 --range-origin-pull on --rule-id rule-aaa --yes
rc=$?
[ "$rc" -eq 0 ] && ok "applied rule knob exits 0" || bad "applied rule knob exit $rc"
has '"Name": "RangeOriginPull", "RangeOriginPullParameters": {"Switch": "on"}' \
    && ok "the request body carries the rule action" || bad "request body missing the rule action"
has 'rule now reads  RangeOriginPull      {"Switch": "on"}' \
    && ok "the rule re-read confirms it" || bad "rule re-read did not confirm it"
has "(nothing changed)" && ok "a rule write leaves the site-wide setting alone" \
    || bad "rule write reported site-wide movement"

# --- guards -------------------------------------------------------------------
reset good
run good "$T/eo-origin-pull.sh" --zone zone-test1234 --range-origin-pull on
[ $? -eq 2 ] && ok "rule knob without --rule-id is refused (2)" || bad "rule knob without --rule-id not refused"
has_err "needs --rule-id" && ok "the refusal says why" || bad "refusal message missing"

reset good
run good "$T/eo-origin-pull.sh" --zone zone-test1234 --upstream-timeout 700 --rule-id rule-aaa
[ $? -eq 2 ] && ok "out-of-range timeout is refused (2)" || bad "out-of-range timeout not refused"

reset good
run good "$T/eo-origin-pull.sh" --upstream-http2 on
[ $? -eq 2 ] && ok "missing --zone is refused (2)" || bad "missing --zone not refused"

reset good
run good "$T/eo-origin-pull.sh" --zone zone-test1234 --range-origin-pull on --rule-id rule-nope --yes
rc=$?
[ "$rc" -ne 0 ] && ok "unknown rule id fails (exit $rc)" || bad "unknown rule id did not fail"
has_err "no rule with RuleId rule-nope" && ok "unknown rule id lists the rules that do exist" \
    || bad "unknown rule id message missing"

# --- bare invocation reads, never writes --------------------------------------
reset still
run still "$T/eo-origin-pull.sh" --zone zone-test1234
rc=$?
[ "$rc" -eq 0 ] && ok "bare invocation exits 0" || bad "bare invocation exit $rc"
has "UpstreamHttp2.Switch = off" && ok "bare invocation prints the current value" \
    || bad "bare invocation printed no value"
[ "$(calls ModifyL7AccSetting)" = "0" ] && ok "bare invocation made no write call" \
    || bad "bare invocation called ModifyL7AccSetting"

echo
if [ "$fail" -eq 0 ]; then
    echo "VERDICT PASS  $pass/$pass"
    exit 0
fi
echo "VERDICT FAIL  $pass passed, $fail failed"
exit 1

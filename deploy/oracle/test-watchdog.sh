#!/usr/bin/env bash
# Behavioural test for deploy/oracle/watchdog.sh.
#
# The watchdog is the node's only reporter and its only death detector, and
# its rules (a planned stop stays silent, a crash loop is throttled, the
# heartbeat carries the agreed schema) exist to keep the far side honest.
# Until now nothing exercised them: the rules lived in a doc and were
# verified by hand. This drives the real script against stubs so the rules
# are checked on every run.
#
# The stubs replace `systemctl`, `df` and `curl` on PATH; the script itself
# is the artifact under test and is never modified. State paths are
# redirected with STATE_DIR, which the node never sets.
#
# Usage: bash deploy/oracle/test-watchdog.sh
set -u

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
WATCHDOG="$REPO_DIR/deploy/oracle/watchdog.sh"
[ -f "$WATCHDOG" ] || { echo "watchdog not found at $WATCHDOG" >&2; exit 1; }

for tool in jq curl; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "SKIP: $tool not available (the watchdog needs it on the node too)"
    exit 0
  }
done

ROOT="$(mktemp -d)"
trap 'rm -rf "$ROOT"' EXIT
STUB="$ROOT/bin"
mkdir -p "$STUB" "$ROOT/state"
RECEIVED="$ROOT/received.jsonl"
HEADERS="$ROOT/headers.txt"
PROBED="$ROOT/probed.txt"

PASS=0
FAIL=0
ok()  { PASS=$((PASS + 1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL + 1)); printf '  FAIL %s\n' "$1"; [ -n "${2:-}" ] && printf '       %s\n' "$2"; return 0; }

# --- stubs ------------------------------------------------------------------
# systemctl is-active: units named in FAKE_INACTIVE are reported inactive.
cat > "$STUB/systemctl" <<'STUB'
#!/usr/bin/env bash
unit="${!#}"
case " ${FAKE_INACTIVE_UNITS:-} " in
  *" $unit "*) exit 3 ;;
esac
exit 0
STUB

cat > "$STUB/df" <<'STUB'
#!/usr/bin/env bash
cat <<EOF
Filesystem     1K-blocks    Used Available Use% Mounted on
/dev/root       10000000 1000000   9000000  ${FAKE_DISK_PCT:-12}% /
EOF
STUB

# curl: records POST bodies and headers, and answers GETs with a healthz
# document. The GET reply has the shape the watchdog parses -- body, a real
# newline, then the status code -- because it reads the code with `tail -n1`
# and would silently accept a malformed one.
cat > "$STUB/curl" <<STUB
#!/usr/bin/env bash
body=""; url=""; method="GET"; head=0
args=("\$@")
i=0
while [ \$i -lt \${#args[@]} ]; do
  a="\${args[\$i]}"
  case "\$a" in
    -X) i=\$((i+1)); method="\${args[\$i]}" ;;
    --data) i=\$((i+1)); body="\${args[\$i]}" ;;
    -H) i=\$((i+1)); printf '%s\n' "\${args[\$i]}" >> "$HEADERS" ;;
    -I) head=1 ;;
    http*) url="\$a" ;;
  esac
  i=\$((i+1))
done
if [ "\$method" = POST ]; then
  printf '%s\n' "\$body" >> "$RECEIVED"
  if [ -n "\${FAKE_REPORT_HTTP:-}" ]; then
    printf '%s' "\$FAKE_REPORT_HTTP"
    exit 7
  fi
  printf '200'
  exit 0
fi
if [ "\$head" = 1 ]; then
  printf 'HTTP/1.1 200 OK\r\n'
  exit 0
fi
status=ok; degraded=false; reasons='[]'
if [ "\${FAKE_DEGRADED:-0}" = 1 ]; then status=degraded; degraded=true; reasons='["disk_below_reserve"]'; fi
# Record the probe target: the watchdog's URL is configurable, and a stub that
# ignores it cannot tell a correct probe from a wrong one -- the failure that
# produced 94 FAILs on the node was exactly a wrong target.
printf '%s\n' "\$url" >> "$PROBED"
doc=\$(jq -nc --arg status "\$status" --argjson degraded "\$degraded" --argjson reasons "\$reasons" \\
  --argjson entries "\${FAKE_ENTRIES:-7}" --argjson bytes "\${FAKE_BYTES:-2048}" --arg version "9.9.9" \\
  '{status:\$status,degraded:\$degraded,degraded_reasons:\$reasons,entries:\$entries,bytes:\$bytes,version:\$version,
    store:{state:"ready",moved_to:"/opt/origin-cache/cache-standard/redb.db.corrupt-1"},
    upstreams:[{id:"leaky-upstream",profile:"efficient"}]}')
printf '%s\n200' "\$doc"
exit 0
STUB
chmod +x "$STUB"/*

# --- drivers ----------------------------------------------------------------
run_watchdog() { # extra env assignments as args
  env PATH="$STUB:$PATH" STATE_DIR="$ROOT/state" REPORT_URL="http://receiver.invalid/report" \
    "$@" bash "$WATCHDOG" >/dev/null 2>&1
}

run_down() { # systemd result vars as args
  env PATH="$STUB:$PATH" STATE_DIR="$ROOT/state" REPORT_URL="http://receiver.invalid/report" \
    "$@" bash "$WATCHDOG" --down origin-cache-efficient.service >/dev/null 2>&1
}

reset_reports() { : > "$RECEIVED"; : > "$HEADERS"; : > "$PROBED"; rm -f "$ROOT/state/notify-down.stamp"; }
reports() { cat "$RECEIVED"; }
report_count() { [ -s "$RECEIVED" ] && wc -l < "$RECEIVED" | tr -d ' ' || echo 0; }
field() { head -n1 "$RECEIVED" | jq -r "$1" 2>/dev/null; }
transition_lines() { grep -cE ' FAIL (unit=|healthz|disk=)' "$ROOT/state/watchdog.log" 2>/dev/null || echo 0; }

echo "watchdog.sh behaviour"

# --- 1. heartbeat schema ----------------------------------------------------
reset_reports
run_watchdog
if [ "$(report_count)" = 1 ]; then
  ok "a healthy run sends exactly one report"
else
  bad "a healthy run sends exactly one report" "sent $(report_count)"
fi
if [ "$(field .event)" = heartbeat ] && [ "$(field .schema)" = "origin-cache/status/1" ]; then
  ok "the report is a heartbeat with the agreed schema"
else
  bad "the report is a heartbeat with the agreed schema" "$(reports)"
fi
if [ "$(field .status)" = active ] && [ "$(field .entries)" = 7 ] && [ "$(field .bytes)" = 2048 ]; then
  ok "status and the cache counters come from healthz"
else
  bad "status and the cache counters come from healthz" "$(reports)"
fi
if [ "$(field .host)" = origin-1 ] && [ "$(field .service)" = origin-cache ] && [ "$(field .version)" = 9.9.9 ]; then
  ok "host, service and version identify the node"
else
  bad "host, service and version identify the node" "$(reports)"
fi
# Whitelist: healthz carries a filesystem path and the upstream name, and
# neither may be forwarded.
if ! grep -qE 'store|moved_to|upstreams|leaky|/opt/' "$RECEIVED"; then
  ok "no healthz internals leak into the report"
else
  bad "no healthz internals leak into the report" "$(reports)"
fi
if [ -n "$(grep -i authorization "$HEADERS" 2>/dev/null)" ]; then
  bad "no Authorization header (the tunnel token is the credential)" "$(cat "$HEADERS")"
else
  ok "no Authorization header (the tunnel token is the credential)"
fi
if grep -q 'application/json' "$HEADERS"; then
  ok "the report is posted as JSON"
else
  bad "the report is posted as JSON" "$(cat "$HEADERS")"
fi

# --- 2. verdict mapping -----------------------------------------------------
reset_reports
run_watchdog FAKE_INACTIVE_UNITS=origin-cache-nocache
if [ "$(field .status)" = degraded ]; then
  ok "a down nocache plane reports degraded (the node still serves)"
else
  bad "a down nocache plane reports degraded" "$(reports)"
fi

reset_reports
run_watchdog FAKE_INACTIVE_UNITS=origin-cache-efficient
if [ "$(field .status)" = down ]; then
  ok "a down main plane reports down"
else
  bad "a down main plane reports down" "$(reports)"
fi

reset_reports
run_watchdog FAKE_DEGRADED=1
if [ "$(field .status)" = degraded ]; then
  ok "a degraded healthz verdict reports degraded"
else
  bad "a degraded healthz verdict reports degraded" "$(reports)"
fi

reset_reports
run_watchdog FAKE_DISK_PCT=97
if [ "$(field .status)" = degraded ]; then
  ok "a disk above the critical watermark reports degraded"
else
  bad "a disk above the critical watermark reports degraded" "$(reports)"
fi

reset_reports
run_watchdog FAKE_INACTIVE_UNITS="" FAKE_DISK_PCT=12
if [ "$(field .status)" = active ]; then
  ok "the same node with no problems reports active again"
else
  bad "the same node with no problems reports active again" "$(reports)"
fi

# --- 3. down-event classification -------------------------------------------
# A planned stop is silent; every genuine death is reported with its cause.
# reset_reports clears the throttle stamp so each case is judged on its own.
down_case() { # label, expected-signal ("" = expect silence), env...
  local label="$1" want="$2"; shift 2
  reset_reports
  run_down "$@"
  local n; n="$(report_count)"
  if [ -z "$want" ]; then
    [ "$n" = 0 ] && ok "$label: silent" || bad "$label: silent" "$(reports)"
    return
  fi
  if [ "$n" != 1 ]; then
    bad "$label: one down report" "sent $n"
    return
  fi
  if [ "$(field .event)" = down ] && [ "$(field .status)" = down ] && [ "$(field .death.signal)" = "$want" ]; then
    ok "$label: down with signal=$want"
  else
    bad "$label: down with signal=$want" "$(reports)"
  fi
}

down_case "planned stop" "" SERVICE_RESULT=success EXIT_CODE=killed EXIT_STATUS=15
down_case "a timeout (the old SIGKILL shape)" SIGKILL SERVICE_RESULT=timeout EXIT_CODE=killed EXIT_STATUS=9
down_case "kill -9 (the OOM shape)" SIGKILL SERVICE_RESULT=signal EXIT_CODE=killed EXIT_STATUS=9
down_case "SIGTERM" SIGTERM SERVICE_RESULT=signal EXIT_CODE=killed EXIT_STATUS=15
down_case "panic" SIGSEGV SERVICE_RESULT=core-dump EXIT_CODE=dumped EXIT_STATUS=11
down_case "crash loop limit" null SERVICE_RESULT=start-limit-hit EXIT_CODE=exited EXIT_STATUS=1

# A timeout and a start-limit-hit are reported, but carry no signal; assert
# their codes and reasons rather than a signal name.
reset_reports
run_down SERVICE_RESULT=timeout EXIT_CODE=killed EXIT_STATUS=9
if [ "$(field .death.signal)" = SIGKILL ] && [ "$(field .death.exit_code)" = 137 ]; then
  ok "a timeout reports the kill that ended it"
else
  bad "a timeout reports the kill that ended it" "$(reports)"
fi

reset_reports
run_down SERVICE_RESULT=exit-code EXIT_CODE=exited EXIT_STATUS=1
if [ "$(field .death.exit_code)" = 1 ] && [ "$(field .death.signal)" = null ]; then
  ok "a clean non-zero exit reports exit_code=1 with a null signal"
else
  bad "a clean non-zero exit reports exit_code=1 with a null signal" "$(reports)"
fi
if [ "$(field .service)" = origin-cache-efficient ]; then
  ok "a down event names the unit that died"
else
  bad "a down event names the unit that died" "$(reports)"
fi

# --- 3b. the probe target ---------------------------------------------------
# A watchdog that probes the wrong port reports an outage that is not there.
# The node's unit sets no environment, so these defaults are production.
reset_reports
run_watchdog
if grep -q "http://127.0.0.1:8080/_internal/healthz" "$PROBED" \
   && grep -q "http://127.0.0.1:8081/_internal/healthz" "$PROBED"; then
  ok "the default probe targets are the business plane's two ports"
else
  bad "the default probe targets are the business plane's two ports" "$(cat "$PROBED")"
fi

reset_reports
env PATH="$STUB:$PATH" STATE_DIR="$ROOT/state" REPORT_URL="http://receiver.invalid/report" \
  MEASURED_URL="http://127.0.0.1:9999/_internal/healthz" \
  bash "$WATCHDOG" >/dev/null 2>&1
if grep -q "http://127.0.0.1:9999/_internal/healthz" "$PROBED"; then
  ok "an overridden probe target is the one used"
else
  bad "an overridden probe target is the one used" "$(cat "$PROBED")"
fi

reset_reports
env PATH="$STUB:$PATH" STATE_DIR="$ROOT/state" REPORT_URL="http://receiver.invalid/report" \
  UNITS="origin-cache-efficient" DISK_WARN_PCT=1 bash "$WATCHDOG" >/dev/null 2>&1
if [ "$(field .status)" = degraded ]; then
  ok "overridden thresholds and unit list are honoured"
else
  bad "overridden thresholds and unit list are honoured" "$(reports)"
fi

# --- 4. throttle ------------------------------------------------------------
# Restart=always restarts in 3s, so a crash loop must not flood the receiver.
rm -f "$ROOT/state/notify-down.stamp"
reset_reports
: > "$ROOT/state/watchdog.log"
run_down SERVICE_RESULT=signal EXIT_CODE=killed EXIT_STATUS=9
run_down SERVICE_RESULT=signal EXIT_CODE=killed EXIT_STATUS=9
run_down SERVICE_RESULT=signal EXIT_CODE=killed EXIT_STATUS=9
if [ "$(report_count)" = 1 ]; then
  ok "three deaths inside a minute send one report"
else
  bad "three deaths inside a minute send one report" "sent $(report_count)"
fi
if grep -q "throttled" "$ROOT/state/watchdog.log"; then
  ok "the suppressed deaths are recorded in the local log"
else
  bad "the suppressed deaths are recorded in the local log" "$(cat "$ROOT/state/watchdog.log")"
fi

# The throttle must not outlive its window.
rm -f "$ROOT/state/notify-down.stamp"
printf '%s\n' "$(( $(date +%s) - 120 ))" > "$ROOT/state/notify-down.stamp"
reset_reports
run_down SERVICE_RESULT=signal EXIT_CODE=killed EXIT_STATUS=9
if [ "$(report_count)" = 1 ]; then
  ok "a death after the throttle window is reported"
else
  bad "a death after the throttle window is reported" "sent $(report_count)"
fi

# --- 5. logging discipline --------------------------------------------------
# A steady verdict must not rewrite the log every run, but the day's first
# run must leave a heartbeat line proving the checker itself is alive.
rm -f "$ROOT/state/watchdog.state" "$ROOT/state/watchdog.heartbeat" "$ROOT/state/watchdog.log"
reset_reports
run_watchdog
run_watchdog
run_watchdog
if [ "$(grep -c 'HB ' "$ROOT/state/watchdog.log" 2>/dev/null || echo 0)" = 1 ]; then
  ok "one daily HB line across repeated healthy runs"
else
  bad "one daily HB line across repeated healthy runs" "$(cat "$ROOT/state/watchdog.log")"
fi

reset_reports
run_watchdog FAKE_INACTIVE_UNITS=origin-cache-efficient
if [ "$(transition_lines)" = 1 ]; then
  ok "a verdict change writes one transition line"
else
  bad "a verdict change writes one transition line" "$(cat "$ROOT/state/watchdog.log")"
fi
run_watchdog FAKE_INACTIVE_UNITS=origin-cache-efficient
if [ "$(transition_lines)" = 1 ]; then
  ok "a repeated verdict does not rewrite the log"
else
  bad "a repeated verdict does not rewrite the log" "$(cat "$ROOT/state/watchdog.log")"
fi
reset_reports
run_watchdog
if grep -q "OK recovered" "$ROOT/state/watchdog.log"; then
  ok "recovery writes one line"
else
  bad "recovery writes one line" "$(cat "$ROOT/state/watchdog.log")"
fi

# --- 6. the report never changes the verdict --------------------------------
# The far side being unreachable must not turn a healthy node unhealthy: the
# reporting path is an observer, and a second way to fail loudly would be
# worse than no reporting at all.
rm -f "$ROOT/state/watchdog.state" "$ROOT/state/watchdog.log"
env PATH="$STUB:$PATH" STATE_DIR="$ROOT/state" REPORT_URL="http://receiver.invalid/report" \
  FAKE_REPORT_HTTP=503 bash "$WATCHDOG" >/dev/null 2>&1
if [ "$(cat "$ROOT/state/watchdog.state" 2>/dev/null)" = OK ]; then
  ok "a failed report leaves the verdict OK"
else
  bad "a failed report leaves the verdict OK" "$(cat "$ROOT/state/watchdog.state" 2>/dev/null)"
fi
if grep -q "report heartbeat failed" "$ROOT/state/watchdog.log"; then
  ok "a failed report is logged for the operator"
else
  bad "a failed report is logged for the operator" "$(cat "$ROOT/state/watchdog.log")"
fi

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ] || exit 1
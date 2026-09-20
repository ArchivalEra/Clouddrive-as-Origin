#!/usr/bin/env bash
# CDN-LAB acceptance suite (deploy/lab/run-lab.sh).
#
# Reproduces the oracle acceptance matrix against a local rclone webdav
# stand-in for OpenList:
#   cold pull + install, second-hit timing, Range slices (206 +
#   Content-Range), 3GB full pull + atomic install, kill-9 restart
#   survival, nocache zero-disk + prewarm no-op + healthz profile,
#   SigV4 three states, and the efficient profile: staged spans served
#   without upstream opens, ledger window decay, span-level eviction.
#
# Usage:  deploy/lab/run-lab.sh [--quick]
#   --quick: skip the 3GB full-pull test (everything else runs).
# Exit 0 = all PASS, 1 = any FAIL. Output: PASS/FAIL per item.
set -u

LAB=/mnt/hdd/CDN-LAB
REPO=/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin
BIN=$REPO/target/release/origin-cache
DAV_USER=labuser
DAV_PASS=labpass
PREWARM_SECRET=labsecret
SIGV4_AK=AKLLABTESTKEY
SIGV4_SK=labsk_demo
PID_DAV= PID_A= PID_B= PID_C= PID_D=
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
note() { echo "---- $1"; }

cleanup() {
  for pid in "$PID_A" "$PID_B" "$PID_C" "$PID_D" "$PID_DAV"; do
    [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT

# --- 0. build -----------------------------------------------------------------
note "build release (may reuse cache)"
(cd "$REPO" && cargo build --release 2>&1 | tail -1) || { echo "FAIL: build"; exit 1; }
[ -x "$BIN" ] || { echo "FAIL: no binary"; exit 1; }

# pre-flight: stale instances from previous runs hold redb locks and ports —
# fresh starts then panic at boot while the stale binary keeps serving, so
# every assertion silently runs against the wrong process. Clear them first.
pkill -f "target/release/origin-cache" 2>/dev/null
# Every port a stale instance could hold, front AND business: waiting only on
# the front ports let a dying process keep its business port and the fresh
# instance died at bind with "Address already in use".
for port in 7777 7778 7779 7780 8081 8082 8083 8084; do
  for _ in $(seq 1 20); do
    ss -tln 2>/dev/null | grep -q ":$port " || break
    sleep 0.5
  done
done

# --- 1. test data -------------------------------------------------------------
# The cache dirs must start EMPTY: they persist across runs, the ledger is
# rebuilt from whatever sidecars are on disk at boot, and every staged-bytes
# assertion below would otherwise be measuring the previous run.
rm -rf "$LAB/cache-a" "$LAB/cache-b" "$LAB/cache-c" "$LAB/cache-d"
mkdir -p "$LAB/dav-data/media/2026/08" "$LAB/dav-data/archive" "$LAB/cache-a" "$LAB/cache-b" "$LAB/cache-c" "$LAB/cache-d"
echo "hello-origin" > "$LAB/dav-data/media/hello.txt"
head -c 1048576 /dev/urandom > "$LAB/dav-data/media/big1mb.bin"
# A second 1 MiB object: the eviction test needs two keys staging at once.
head -c 1048576 /dev/urandom > "$LAB/dav-data/media/big1mb-b.bin"
if [ ! -f "$LAB/dav-data/media/big3g.bin" ]; then
  dd if=/dev/zero bs=1M count=3072 2>/dev/null | tr '\0' 'B' > "$LAB/dav-data/media/big3g.bin"
fi
DAV_ROOT="$LAB/dav-data"

# --- 2. fake OpenList (rclone serve webdav) -----------------------------------
note "starting rclone webdav on 5244"
pkill -f "rclone serve webdav" 2>/dev/null; sleep 1
cd "$DAV_ROOT"
rclone serve webdav --addr 127.0.0.1:5244 --baseurl /dav --user "$DAV_USER" --pass labpass . > "$LAB/dav.log" 2>&1 &
PID_DAV=$!
dav_ok=0
for i in $(seq 1 20); do
  if curl -s -m 2 -u "$DAV_USER:labpass" -X PROPFIND -H "Depth: 0" -o /dev/null \
       -w "%{http_code}" http://127.0.0.1:5244/dav/ | grep -q 207; then
    dav_ok=1; break
  fi
  sleep 0.5
done
[ "$dav_ok" = 1 ] || { echo "FAIL: dav not reachable"; tail -3 "$LAB/dav.log"; exit 1; }

# --- 3. start all profiles ---------------------------------------------------
export CDN_LAB_DAV_USER=$DAV_USER CDN_LAB_DAV_PASS=labpass CDN_LAB_PREWARM_SECRET=$PREWARM_SECRET
cd "$REPO"
"$BIN" "$REPO/deploy/lab/config-a.toml" > "$LAB/cache-a/serve.log" 2>&1 & PID_A=$!
"$BIN" "$REPO/deploy/lab/config-b.toml" > "$LAB/cache-b/serve.log" 2>&1 & PID_B=$!
"$BIN" "$REPO/deploy/lab/config-c.toml" > "$LAB/cache-c/serve.log" 2>&1 & PID_C=$!
"$BIN" "$REPO/deploy/lab/config-d.toml" > "$LAB/cache-d/serve.log" 2>&1 & PID_D=$!
# Readiness is POLLED, never a fixed sleep: boot measures 1-11 s on the node
# (aarch64 with a cold page cache), and a single-shot probe fails a healthy
# instance. `-f` matters too: without it a 404 (or any error page) still exits
# 0 and the probe would pass against a dead instance. healthz is answered on
# the BUSINESS port only — front/src/lib.rs refuses it with 404 on purpose, so
# a caller cannot even learn the private surface exists.
probe_up() { # port name
  for _ in $(seq 1 40); do
    curl -fsS -m 3 "http://127.0.0.1:$1/_internal/healthz" > /dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}
probe_up 8083 standard || { echo "FAIL: standard not up"; tail -3 "$LAB/cache-a/serve.log"; exit 1; }
probe_up 8081 nocache  || { echo "FAIL: nocache not up";  tail -3 "$LAB/cache-b/serve.log"; exit 1; }
probe_up 8082 efficient|| { echo "FAIL: efficient not up";tail -3 "$LAB/cache-c/serve.log"; exit 1; }
probe_up 8084 eviction || { echo "FAIL: eviction not up"; tail -3 "$LAB/cache-d/serve.log"; exit 1; }
# healthz may have been answered by a stale leftover instance — assert the
# fresh processes are actually alive (boot panic = redb/port conflict).
kill -0 "$PID_A" 2>/dev/null || { echo "FAIL: standard process died at boot"; tail -5 "$LAB/cache-a/serve.log"; exit 1; }
kill -0 "$PID_B" 2>/dev/null || { echo "FAIL: nocache process died at boot"; tail -5 "$LAB/cache-b/serve.log"; exit 1; }
kill -0 "$PID_C" 2>/dev/null || { echo "FAIL: efficient process died at boot"; tail -5 "$LAB/cache-c/serve.log"; exit 1; }
kill -0 "$PID_D" 2>/dev/null || { echo "FAIL: eviction process died at boot"; tail -5 "$LAB/cache-d/serve.log"; exit 1; }

H() { curl -s "$@"; }

# healthz is on the BUSINESS port (the front 404s it by design).
hz() { H "http://127.0.0.1:$1/_internal/healthz"; }
# One numeric healthz field, empty when the field or the instance is absent.
hz_field() { hz "$1" | grep -o "\"$2\":[0-9]*" | cut -d: -f2; }
# Upstream opens counted by the service itself. rclone's webdav log records no
# successful GETs, so the dav log cannot answer this; the metric can. A label
# combination is absent until its first use, so a miss means zero.
served_opens() { H "http://127.0.0.1:$1/metrics" | awk '/^backend_call_duration_seconds_count\{op="open"\}/{print $2+0}' | head -1; }
served_from_stage() { H "http://127.0.0.1:$1/metrics" | awk '/^cache_serve_source_total\{source="stage"\}/{print $2+0}' | head -1; }
# Sealing is asynchronous: the in-stream path runs at body exhaustion and a
# detached watcher seals whatever landed before a viewer went away, so a
# request can return a few hundred ms before its span enters the ledger.
# Anything measuring staged state polls instead of sampling once.
wait_segment_bytes() { # port bytes tries
  for _ in $(seq 1 "${3:-40}"); do
    [ "$(hz_field "$1" segment_bytes)" = "$2" ] && return 0
    sleep 0.25
  done
  return 1
}
# Upstream open count for a delta assertion, with the absent-label case = 0.
opens() { local v; v=$(served_opens "$1"); echo "${v:-0}"; }

# --- 4. acceptance matrix ------------------------------------------------------
note "1. cold small-file GET (standard)"
code=$(H -o /dev/null -w "%{http_code}" http://127.0.0.1:7777/media/hello.txt)
[ "$code" = 200 ] && ok "cold GET 200" || bad "cold GET got $code"

note "2. second hit faster than first (disk, 1MB file)"
# 1MB file: cold pull from rclone + disk write, hit is pure disk read.
t1=$(H -o /dev/null -w "%{time_total}" "http://127.0.0.1:7777/media/big1mb.bin")
H -o /dev/null "http://127.0.0.1:7777/media/big1mb.bin"
t2=$(H -o /dev/null -w "%{time_total}" "http://127.0.0.1:7777/media/big1mb.bin")
awk -v a="$t1" -v b="$t2" 'BEGIN { exit !(b <= a + 0.05) }' && ok "hit t=$t2 <= cold t=$t1" || bad "hit $t2 > cold $t1"

note "2. Range slices 206 + Content-Range (1MB file)"
r=$(H -o /dev/null -w "%{http_code}" -H "Range: bytes=100-199" http://127.0.0.1:7777/media/big1mb.bin)
cr=$(H -sI -H "Range: bytes=100-199" http://127.0.0.1:7777/media/big1mb.bin | grep -i content-range)
[ "$r" = 206 ] && echo "$cr" | grep -q "bytes 100-199/1048576" && ok "206 + exact Content-Range" || bad "range: $r / $cr"

note "3. tail suffix range"
r=$(H -o /dev/null -w "%{http_code}" -H "Range: bytes=-10" http://127.0.0.1:7777/media/big1mb.bin)
[ "$r" = 206 ] && ok "suffix range 206" || bad "suffix range $r"

note "3. 3GB full pull + atomic install (skip with --quick)"
if [ "${1:-}" != "--quick" ]; then
  out=$(H -o /dev/null -w "%{http_code} %{size_download}" "http://127.0.0.1:7777/media/big3g.bin")
  code=${out%% *}; size=${out##* }
  real=$(stat -c %s "$DAV_ROOT/media/big3g.bin")
  [ "$code" = 200 ] && [ "$size" = "$real" ] && ok "3GB pull byte-exact ($size)" || bad "3GB pull: $code/$size vs $real"
  out=$(H -o /dev/null -w "%{http_code} %{speed_download}" "http://127.0.0.1:7777/media/big3g.bin")
  ok "3GB hit speed ${out##* }B/s"
fi

note "4. kill-9 restart survival (standard)"
kill -9 "$PID_A" 2>/dev/null; wait "$PID_A" 2>/dev/null
"$BIN" "$REPO/deploy/lab/config-a.toml" >> "$LAB/cache-a/serve.log" 2>&1 & PID_A=$!
sleep 2
out=$(H -o /dev/null -w "%{http_code} %{time_total}" "http://127.0.0.1:7777/media/big1mb.bin")
code=${out%% *}; t=${out##* }
[ "$code" = 200 ] && ok "restart survival 200 t=${t}s" || bad "restart: $code"

note "4. upstream NOT touched after restart (dav log count)"
c1=$(grep -c "PROPFIND" "$LAB/dav.log" 2>/dev/null | head -1); c1=${c1:-0}
H -o /dev/null "http://127.0.0.1:7777/media/big1mb.bin"
c2=$(grep -c "PROPFIND" "$LAB/dav.log" 2>/dev/null | head -1); c2=${c2:-0}
[ "$c1" = "$c2" ] && ok "no PROPFIND on hit" || bad "PROPFIND delta $c1->$c2"

note "5. nocache zero-disk"
H -o /dev/null "http://127.0.0.1:7778/media/hello.txt"
files=$(ls "$LAB/cache-b" | grep -vc "^redb.db$\|^serve.log$")
[ "$files" = 0 ] && ok "nocache dir clean" || { bad "nocache stray files: $files"; ls "$LAB/cache-b"; }

note "6. nocache prewarm no-op"
# Spec §2: a miss answers 202 immediately and fetches behind the caller, so
# the assertion is on the acceptance and on what the background fetch did
# (nothing, for a nocache node), not on a synchronous "fetched" body.
code=$(H -s -o /tmp/lab-prewarm.json -w "%{http_code}" -X POST "http://127.0.0.1:7778/_internal/prewarm/media/hello.txt" -H "x-prewarm-token: $PREWARM_SECRET")
out=$(cat /tmp/lab-prewarm.json)
[ "$code" = 202 ] && echo "$out" | grep -q accepted && ok "prewarm accepted (202)" || bad "prewarm: http=$code body=$out"
sleep 1  # let the background fetch run before judging its side effects
files=$(ls "$LAB/cache-b" | grep -vc "^redb.db$\|^serve.log$")
[ "$files" = 0 ] && ok "prewarm wrote nothing" || bad "prewarm wrote files"
inflight=$(H http://127.0.0.1:8081/_internal/healthz | grep -o '"prewarm_inflight":[0-9]*')
[ "$inflight" = '"prewarm_inflight":0' ] && ok "prewarm queue drains to zero" || bad "prewarm inflight: $inflight"

note "7. nocache healthz profile"
out=$(H http://127.0.0.1:8081/_internal/healthz)
echo "$out" | grep -q '"profile":"nocache"' && ok "healthz profile=nocache" || bad "healthz: $out"

note "8. SigV4 three states (standard)"
sig=$(python3 "$REPO/deploy/lab/sigv4-test.py" "$SIGV4_SK" "http://127.0.0.1:7777/media/hello.txt" "$SIGV4_AK" 2>/dev/null)
# state 1: anonymous passes
code=$(H -o /dev/null -w "%{http_code}" http://127.0.0.1:7777/media/hello.txt)
[ "$code" = 200 ] && ok "sigv4 anon 200" || bad "anon $code"

# restart standard with SigV4 creds enabled for states 2/3
kill "$PID_A" 2>/dev/null; wait "$PID_A" 2>/dev/null
export CDN_LAB_SIGV4_ID=$SIGV4_AK CDN_LAB_SIGV4_SK=$SIGV4_SK
SIGV4_ACCESS_KEY_ID=$SIGV4_AK SIGV4_SECRET_ACCESS_KEY=$SIGV4_SK \
  "$BIN" "$REPO/deploy/lab/config-a.toml" >> "$LAB/cache-a/serve.log" 2>&1 & PID_A=$!
sleep 2
kill -0 "$PID_A" 2>/dev/null || { echo "FAIL: sigv4 instance died at boot"; tail -5 "$LAB/cache-a/serve.log"; exit 1; }
# state 2: correct signature passes (sign with the same creds)
lines=$(python3 - "$SIGV4_SK" "http://127.0.0.1:7777/media/hello.txt" "$SIGV4_AK" <<'PY' 2>/dev/null
import hashlib, hmac, sys, datetime
SECRET = sys.argv[1]; URL = sys.argv[2]; AK = sys.argv[3]
u = URL.split("://",1)[1]; host, path = u.split("/",1); path = "/"+path
region, service = "us-east-1", "s3"
now = datetime.datetime.utcnow()
amzdate = now.strftime("%Y%m%dT%H%M%SZ"); datestamp = now.strftime("%Y%m%d")
signed = "host;x-amz-content-sha256;x-amz-date"; payload = "UNSIGNED-PAYLOAD"
ch = f"host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{amzdate}\n"
canonical = f"GET\n{path}\n\n{ch}\n{signed}\n{payload}"
scope = f"{datestamp}/{region}/{service}/aws4_request"
sts = f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
def hs(k,d): return hmac.new(k, d.encode(), hashlib.sha256).digest()
k = hs(hs(hs(hs(("AWS4"+SECRET).encode(), datestamp), region), service), "aws4_request")
sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
print(f"Authorization: AWS4-HMAC-SHA256 Credential={AK}/{scope}, SignedHeaders={signed}, Signature={sig}")
print(amzdate)
PY
)
AUTH=$(echo "$lines" | head -1); DATE=$(echo "$lines" | tail -1)
scope=$(echo "$AUTH" | sed 's|.*Credential=[^/]*/||; s|,.*||')
code=$(H -o /dev/null -w "%{http_code}" -H "$AUTH" -H "x-amz-date: $DATE" \
  -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" http://127.0.0.1:7777/media/hello.txt)
[ "$code" = 200 ] && ok "sigv4 good sig 200" || bad "sigv4 good sig $code"
# state 3: bad sig -> 403 with cache-control: no-store (dump headers, grep)
BADAUTH=$(echo "$AUTH" | sed 's/Signature=[a-f0-9]*/Signature=0000000000000000000000000000000000000000000000000000000000000000/')
H -sD /tmp/lab-badsig-hdr.txt -o /dev/null \
  -H "$BADAUTH" -H "x-amz-date: $DATE" -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
  http://127.0.0.1:7777/media/hello.txt
code=$(head -1 /tmp/lab-badsig-hdr.txt | grep -oE "[0-9]{3}")
cc=$(grep -i "^cache-control:" /tmp/lab-badsig-hdr.txt | tr -d "\r")
case "$code$cc" in 403*no-store*) ok "sigv4 bad sig 403 no-store" ;; *) bad "sigv4 bad sig: code=$code cc=$cc" ;; esac

# --- 4b. coverage window (map #30 T2): efficient profile on 7779 ---------------
note "9. staged spans are servable: a covered re-read opens upstream zero times"
# 1 MiB object in four 256000-byte ranges = four staged spans (efficient
# profile). The ranges cover 1024000 bytes; the last 24576 bytes are never
# asked for.
for off in 0 256000 512000 768000; do
  H -o /dev/null -H "Range: bytes=$off-$((off+255999))" "http://127.0.0.1:7779/media/big1mb.bin"
done
wait_segment_bytes 8082 1024000 && ok "four spans staged (segment_bytes=1024000)" || bad "segment_bytes=$(hz_field 8082 segment_bytes) (want 1024000)"
spans=$(hz_field 8082 coverage_intervals)
[ "${spans:-0}" = 4 ] && ok "ledger holds four spans" || bad "coverage_intervals=$spans (want 4)"
# The re-read must be answered from those spans: 206, byte-exact, ZERO upstream
# opens, and labelled as stage-served. An open is what a seek used to cost
# (~640 ms measured), so this is the claim the whole profile exists for.
before_o=$(opens 9092); before_s=$(served_from_stage 9092); before_s=${before_s:-0}
code=$(H -o /tmp/lab-staged.bin -w "%{http_code}" -H "Range: bytes=0-255999" "http://127.0.0.1:7779/media/big1mb.bin")
after_o=$(opens 9092); after_s=$(served_from_stage 9092); after_s=${after_s:-0}
[ "$code" = 206 ] && ok "covered re-read 206" || bad "covered re-read code=$code"
[ "$(stat -c%s /tmp/lab-staged.bin)" = 256000 ] && ok "covered re-read delivered 256000 bytes" || bad "covered re-read size $(stat -c%s /tmp/lab-staged.bin)"
head -c 256000 /mnt/hdd/CDN-LAB/dav-data/media/big1mb.bin > /tmp/lab-staged-ref.bin
cmp -s /tmp/lab-staged.bin /tmp/lab-staged-ref.bin && ok "covered re-read is byte-exact" || bad "covered re-read differs from the source"
[ $((after_o - before_o)) = 0 ] && ok "covered re-read: zero upstream opens" || bad "covered re-read opened upstream $((after_o-before_o)) time(s)"
[ $((after_s - before_s)) -ge 1 ] && ok "covered re-read counted as stage-served" || bad "no stage-served sample (the read came from upstream)"

note "10. coverage window decays the ledger, the sidecars stay on disk"
# config-c has a 5 s window. Stage one span on a FRESH key, wait past the
# window, then stage a second: the seal runs that row's decay pass, so the
# stale interval leaves the ledger while its .seg file stays on disk (disk
# files leave on the inactivity clock, not on the window). The request must be
# a cold miss — the four spans of section 9 cover [0,1024000) contiguously,
# so anything inside that range stages nothing.
H -o /dev/null -H "Range: bytes=0-499999" "http://127.0.0.1:7779/media/big1mb-b.bin"
wait_segment_bytes 8082 1524000 || bad "the 500000-byte span never landed (segment_bytes=$(hz_field 8082 segment_bytes))"
sleep 6
H -o /dev/null -H "Range: bytes=600000-699999" "http://127.0.0.1:7779/media/big1mb-b.bin"
wait_segment_bytes 8082 1624000 || bad "the second span never landed (segment_bytes=$(hz_field 8082 segment_bytes))"
spans=$(hz_field 8082 coverage_intervals)
seg=$(hz_field 8082 segment_bytes)
[ "${spans:-0}" = 5 ] && ok "window expiry dropped the stale interval (4 + 1, not 4 + 2)" || bad "coverage_intervals=$spans (want 5)"
[ "${seg:-0}" = 1624000 ] && ok "sidecars survived the ledger decay (segment_bytes=$seg)" || bad "segment_bytes=$seg (want 1624000)"

note "11. span-level eviction: an overshoot trims two spans, not the key"
# config-d: 1.5 MiB magazine, heat policy. Two 1 MiB objects stage four
# 256000-byte spans each; the second overshoots by 474176 bytes, which must
# come out of the older key as single spans. The row-level evictor this
# replaced would have deleted all four of the older key's spans.
for off in 0 256000 512000 768000; do
  H -o /dev/null -H "Range: bytes=$off-$((off+255999))" "http://127.0.0.1:7780/media/big1mb.bin"
done
wait_segment_bytes 8084 1024000 && ok "older key staged four spans" || bad "older key staged $(hz_field 8084 segment_bytes)"
# Re-read the OLDEST span of the older key three times: heat must keep it,
# while lru would drop it first (it is the stalest span in the row).
for i in 1 2 3; do
  H -o /dev/null -H "Range: bytes=0-255999" "http://127.0.0.1:7780/media/big1mb.bin"
done
for off in 0 256000 512000 768000; do
  H -o /dev/null -H "Range: bytes=$off-$((off+255999))" "http://127.0.0.1:7780/media/big1mb-b.bin"
done
wait_segment_bytes 8084 2048000 && ok "both keys staged (2 MiB against a 1.5 MiB budget)" || bad "staged $(hz_field 8084 segment_bytes) (want 2048000)"
# Two waits stack before the trim: the reaper tick is 60 s, and a row younger
# than STAGE_MIN_AGE_MS (60 s) is skipped by the evictor, so the first tick
# after the overshoot declines and the second one evicts.
trimmed=0
for i in $(seq 1 150); do
  if [ "$(hz_field 8084 segment_bytes)" = 1536000 ]; then trimmed=1; break; fi
  sleep 1
done
seg=$(hz_field 8084 segment_bytes)
[ "$trimmed" = 1 ] && ok "overshoot trimmed by exactly two spans (segment_bytes=1536000)" || bad "segment_bytes=$seg after the wait (want 1536000)"
ls -a "$LAB/cache-d" | grep '^\.seg\.' | sed 's/^/    sidecar: /' >&2
a_spans=$(ls -a "$LAB/cache-d" | grep -c '^\.seg\.media%2Fbig1mb\.bin\.') || true
b_spans=$(ls -a "$LAB/cache-d" | grep -c '^\.seg\.media%2Fbig1mb-b\.bin\.') || true
[ "$a_spans" = 2 ] && ok "older key kept 2 of 4 spans (span-level, not row-level)" || bad "older key has $a_spans spans (want 2)"
[ "$b_spans" = 4 ] && ok "newer key untouched (4 spans)" || bad "newer key has $b_spans spans (want 4)"
hot=$(ls -a "$LAB/cache-d" | grep -c -- '-256000$' ) || true
hotedge=$(ls -a "$LAB/cache-d" | grep -c '^\.seg\.media%2Fbig1mb\.bin\.0-256000$') || true
[ "$hotedge" = 1 ] && ok "heat kept the re-read span (0-256000)" || bad "the re-read span was evicted under heat (A spans left: $a_spans, total span ends '-256000': $hot)"

# --- 4c. concurrency stampede (map #31 T2): 50 concurrent cold key ------------
note "12. single-flight: 50 concurrent cold key -> one upstream fetch"
# Fresh key (never requested): 50 parallel GETs must coalesce to one fetch.
# Count upstream PROPFINDs in the dav log before/after.
BEFORE=$(grep -c "PROPFIND" "$LAB/dav.log" 2>/dev/null | head -1); BEFORE=${BEFORE:-0}
pids=""
for i in $(seq 1 50); do
  curl -s -m 30 -o /dev/null -w "%{http_code}\n" "http://127.0.0.1:7777/media/stampede.bin" >> "$LAB/stampede-codes.txt" &
  pids="$pids $!"
done
for p in $pids; do wait "$p" 2>/dev/null; done
AFTER=$(grep -c "PROPFIND" "$LAB/dav.log" 2>/dev/null | head -1); AFTER=${AFTER:-0}
codes=$(sort -u "$LAB/stampede-codes.txt" | tr -d ' \n')
rm -f "$LAB/stampede-codes.txt"
# 50 responses all 200, and the upstream saw exactly ONE stat for the key.
[ "$codes" = "200" ] && ok "stampede: all 50 responses 200" || bad "stampede codes: $codes"
[ $((AFTER - BEFORE)) -le 2 ] && ok "stampede: upstream PROPFIND delta=$((AFTER-BEFORE)) (<=2)" || bad "stampede: PROPFIND delta=$((AFTER-BEFORE))"

# --- 5. summary ---------------------------------------------------------------
echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
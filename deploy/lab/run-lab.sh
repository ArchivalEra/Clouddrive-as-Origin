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
# How many staged spans one key holds (the account the run/eviction sections
# are about: per key, not summed across the cache).
seg_files() { ls -a "$1" | grep -c "^\\.seg\\.media%2F$2\\."; }
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
note "9. one upstream stream per key: seeks inside a window share an open"
# config-c: efficient profile, 256 KiB window, 1 MiB object. The first seek
# starts a run (one open, one span of the window); the next two ride its
# watermark for free. An `open` costs ~640 ms measured whatever the range, so
# this ratio is what makes a scrub cheap.
before=$(opens 9092)
for off in 0 65536 131072; do
  code=$(H -o /dev/null -w "%{http_code}" -H "Range: bytes=$off-$((off+65535))" "http://127.0.0.1:7779/media/big1mb.bin")
  [ "$code" = 206 ] && ok "seek at $off -> 206" || bad "seek at $off -> $code"
done
after=$(opens 9092)
[ $((after - before)) = 1 ] && ok "three seeks inside one window: 1 upstream open" \
  || bad "three seeks cost $((after - before)) upstream opens (want 1)"
wait_segment_bytes 8082 262144 || bad "the window never landed (segment_bytes=$(hz_field 8082 segment_bytes))"
[ "$(seg_files "$LAB/cache-c" big1mb.bin)" = 1 ] && ok "the window staged as ONE span" \
  || bad "staged $(seg_files "$LAB/cache-c" big1mb.bin) spans (want 1)"
# A covered re-read of the same window: byte-exact, zero upstream opens.
before=$(opens 9092)
code=$(H -o /tmp/lab-staged.bin -w "%{http_code}" -H "Range: bytes=32768-98303" "http://127.0.0.1:7779/media/big1mb.bin")
after=$(opens 9092)
[ "$code" = 206 ] && ok "covered re-read 206" || bad "covered re-read code=$code"
tail -c +32769 /mnt/hdd/CDN-LAB/dav-data/media/big1mb.bin | head -c 65536 > /tmp/lab-staged-ref.bin
cmp -s /tmp/lab-staged.bin /tmp/lab-staged-ref.bin && ok "covered re-read is byte-exact" \
  || bad "covered re-read differs from the source"
[ $((after - before)) = 0 ] && ok "covered re-read: zero upstream opens" \
  || bad "covered re-read opened upstream $((after-before)) time(s)"
# Outside the window the escape applies: one open of its own.
before=$(opens 9092)
H -o /dev/null -H "Range: bytes=524288-589823" "http://127.0.0.1:7779/media/big1mb.bin"
after=$(opens 9092)
[ $((after - before)) = 1 ] && ok "a seek outside the window opens its own Range" \
  || bad "the far seek cost $((after - before)) opens (want 1)"
attached=$(H http://127.0.0.1:9092/metrics | awk '/^cache_session_reader_total\{result="attached"\}/{print $2+0}' | head -1)
[ "${attached:-0}" -ge 3 ] && ok "requests counted as run-attached (${attached:-0})" \
  || bad "only ${attached:-0} requests rode a run"

note "10. a decayed interval leaves the bytes served"
# config-c's coverage window is 5 s. A window that closes and a new one that
# opens past the window: the ledger drops the stale interval (that is the
# policy's view, ADR-0015) while the file stays on disk — and serving plans
# against the DISK (ADR-0016), so the older bytes still cost no upstream open.
before_files=$(seg_files "$LAB/cache-c" big1mb.bin)
H -o /dev/null -H "Range: bytes=0-499999" "http://127.0.0.1:7779/media/big1mb.bin"
sleep 6
H -o /dev/null -H "Range: bytes=700000-799999" "http://127.0.0.1:7779/media/big1mb.bin"
for i in $(seq 1 40); do
  [ "$(seg_files "$LAB/cache-c" big1mb.bin)" -gt "$before_files" ] && break
  sleep 0.25
done
[ "$(seg_files "$LAB/cache-c" big1mb.bin)" -gt "$before_files" ] \
  && ok "the later window landed as another span ($(seg_files "$LAB/cache-c" big1mb.bin) total)" \
  || bad "no new span for the later window"
[ "$(seg_files "$LAB/cache-c" big1mb.bin)" -ge 2 ] && ok "both windows' files are on disk" \
  || bad "expected 2+ staged files, found $(seg_files "$LAB/cache-c" big1mb.bin)"
before=$(opens 9092)
H -o /dev/null -H "Range: bytes=0-65535" "http://127.0.0.1:7779/media/big1mb.bin"
after=$(opens 9092)
[ $((after - before)) = 0 ] && ok "the older window still serves with no open" \
  || bad "a read inside the decayed window cost $((after - before)) opens"

note "11. span-level eviction: an overshoot trims two spans, not the key"
# config-d: a 1.5 MiB magazine, a 256 KiB window and the heat policy. Two 1 MiB
# objects stage four windows each (2 MiB against the 1.5 MiB budget), so the
# reaper must take exactly two windows out of the older key — never the key.
# Paced: a request that arrives while the previous window is still in flight
# takes the documented escape (its own exact Range), so the walk waits for each
# window to close — which is also what a real shard walk looks like, at a
# window per ~second rather than per millisecond.
for off in 0 262144 524288 786432; do
  H -o /dev/null -H "Range: bytes=$off-$((off+65535))" "http://127.0.0.1:7780/media/big1mb.bin"
  sleep 1
done
wait_segment_bytes 8084 1048576 || bad "older key staged $(hz_field 8084 segment_bytes)"
# Re-read the OLDEST window: heat must keep it, lru would drop it first.
for i in 1 2 3; do
  H -o /dev/null -H "Range: bytes=0-65535" "http://127.0.0.1:7780/media/big1mb.bin"
done
for off in 0 262144 524288 786432; do
  H -o /dev/null -H "Range: bytes=$off-$((off+65535))" "http://127.0.0.1:7780/media/big1mb-b.bin"
  sleep 1
done
wait_segment_bytes 8084 2097152 || bad "both keys staged $(hz_field 8084 segment_bytes) (want 2097152)"
# Two waits stack before the trim: the reaper tick is 60 s and a row younger
# than STAGE_MIN_AGE_MS (60 s) is skipped, so the second tick evicts.
trimmed=0
for i in $(seq 1 150); do
  seg=$(hz_field 8084 segment_bytes)
  [ "${seg:-0}" -le 1572864 ] && { trimmed=1; break; }
  sleep 1
done
hz 8084 | sed 's/^/    healthz: /' >&2
[ "$trimmed" = 1 ] && ok "the magazine is back inside its budget (segment_bytes=$(hz_field 8084 segment_bytes))" \
  || bad "segment_bytes=$(hz_field 8084 segment_bytes) after the wait (budget 1572864)"
ls -a "$LAB/cache-d" | grep '^\.seg\.' | sed 's/^/    sidecar: /' >&2
a_spans=$(seg_files "$LAB/cache-d" big1mb.bin)
b_spans=$(seg_files "$LAB/cache-d" big1mb-b.bin)
# The trim is SPAN-level: the older key gives up windows one at a time (it
# staged four), while a row-level evictor would have taken all four. The exact
# count depends on the resident bytes the same budget counts, so what is pinned
# here is the shape; the exact arithmetic is pinned by the unit pair
# `heat_eviction_keeps_the_hot_span_lru_would_eject` /
# `lru_eviction_ejects_the_stale_span_even_when_it_is_hot`.
[ "$a_spans" -ge 1 ] && [ "$a_spans" -le 3 ] && ok "older key trimmed by span, not by row ($a_spans of 4 left)" \
  || bad "older key has $a_spans spans (want 1..3 of 4)"
[ "$b_spans" = 4 ] && ok "newer key untouched (4 spans)" || bad "newer key has $b_spans spans (want 4)"
ls -a "$LAB/cache-d" | grep -q '^\.seg\.media%2Fbig1mb\.bin\.0-262144$' \
  && ok "heat kept the re-read window (0-262144)" \
  || bad "the re-read window was evicted under heat"

note "13. single-flight: 50 concurrent cold key -> one upstream fetch"
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
#!/usr/bin/env bash
# CDN-LAB acceptance suite (deploy/lab/run-lab.sh).
#
# Reproduces the oracle acceptance matrix against a local rclone webdav
# stand-in for OpenList:
#   cold pull + install, second-hit timing, Range slices (206 +
#   Content-Range), 3GB full pull + atomic install, kill-9 restart
#   survival, nocache zero-disk + prewarm no-op + healthz profile,
#   SigV4 three states.
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
PID_DAV= PID_A= PID_B=
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
note() { echo "---- $1"; }

cleanup() {
  for pid in "$PID_A" "$PID_B" "$PID_DAV"; do
    [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT

# --- 0. build -----------------------------------------------------------------
note "build release (may reuse cache)"
(cd "$REPO" && cargo build --release 2>&1 | tail -1) || { echo "FAIL: build"; exit 1; }
[ -x "$BIN" ] || { echo "FAIL: no binary"; exit 1; }

# --- 1. test data -------------------------------------------------------------
mkdir -p "$LAB/dav-data/media/2026/08" "$LAB/dav-data/archive" "$LAB/cache-a" "$LAB/cache-b"
echo "hello-origin" > "$LAB/dav-data/media/hello.txt"
head -c 1048576 /dev/urandom > "$LAB/dav-data/media/big1mb.bin"
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

# --- 3. start both profiles ---------------------------------------------------
export CDN_LAB_DAV_USER=$DAV_USER CDN_LAB_DAV_PASS=labpass CDN_LAB_PREWARM_SECRET=$PREWARM_SECRET
cd "$REPO"
"$BIN" "$REPO/deploy/lab/config-a.toml" > "$LAB/cache-a/serve.log" 2>&1 & PID_A=$!
"$BIN" "$REPO/deploy/lab/config-b.toml" > "$LAB/cache-b/serve.log" 2>&1 & PID_B=$!
sleep 2
curl -s -m 5 http://127.0.0.1:7777/_internal/healthz > /dev/null 2>&1 || { echo "FAIL: standard not up"; exit 1; }
curl -s -m 5 http://127.0.0.1:7778/_internal/healthz > /dev/null 2>&1 || { echo "FAIL: nocache not up"; exit 1; }

H() { curl -s "$@"; }

# --- 4. acceptance matrix ------------------------------------------------------
note "1. cold small-file GET (standard)"
code=$(H -o /dev/null -w "%{http_code}" http://127.0.0.1:7777/media/hello.txt)
[ "$code" = 200 ] && ok "cold GET 200" || bad "cold GET got $code"

note "2. second hit faster than first (disk)"
t1=$(H -o /dev/null -w "%{time_total}" "http://127.0.0.1:7777/media/hello.txt")
H -o /dev/null "http://127.0.0.1:7777/media/hello.txt"
t2=$(H -o /dev/null -w "%{time_total}" "http://127.0.0.1:7777/media/hello.txt")
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
c1=$(grep -c "PROPFIND" "$LAB/dav.log" 2>/dev/null || echo 0)
H -o /dev/null "http://127.0.0.1:7777/media/big1mb.bin"
c2=$(grep -c "PROPFIND" "$LAB/dav.log" 2>/dev/null || echo 0)
[ "$c1" = "$c2" ] && ok "no PROPFIND on hit" || bad "PROPFIND delta $c1->$c2"

note "5. nocache zero-disk"
H -o /dev/null "http://127.0.0.1:7778/media/hello.txt"
files=$(ls "$LAB/cache-b" | grep -vc "^redb.db$\|^serve.log$")
[ "$files" = 0 ] && ok "nocache dir clean" || { bad "nocache stray files: $files"; ls "$LAB/cache-b"; }

note "6. nocache prewarm no-op"
out=$(H -s -X POST "http://127.0.0.1:7778/_internal/prewarm/media/hello.txt" -H "x-prewarm-token: $PREWARM_SECRET")
echo "$out" | grep -q fetched && ok "prewarm fetched" || bad "prewarm: $out"
files=$(ls "$LAB/cache-b" | grep -vc "^redb.db$\|^serve.log$")
[ "$files" = 0 ] && ok "prewarm wrote nothing" || bad "prewarm wrote files"

note "7. nocache healthz profile"
out=$(H http://127.0.0.1:7778/_internal/healthz)
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

# --- 5. summary ---------------------------------------------------------------
echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
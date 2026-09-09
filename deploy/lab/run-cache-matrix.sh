#!/usr/bin/env bash
# CDN-LAB cache-behavior matrix (deploy/lab/run-cache-matrix.sh).
#
# Exercises the three fill profiles against the local rclone webdav
# stand-in with real large files:
#   standard: full-file water-pipe -> disk hit on second pull
#   efficient: ranged staging -> coverage promotion -> disk hit
#   nocache: zero disk writes, every pull hits the upstream
#
# Usage:  bash deploy/lab/run-cache-matrix.sh
# Exit 0 = all PASS, 1 = any FAIL.
set -u

LAB=/mnt/hdd/CDN-LAB
REPO=/mnt/hdd/zcode-on-the-move/Onedrive-as-Origin
BIN=$REPO/target/release/origin-cache
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
note() { echo "---- $1"; }

cleanup() {
  pkill -f "target/release/origin-cache" 2>/dev/null
  pkill -f "rclone serve webdav" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT

# --- 0. build + start rclone ------------------------------------------------
note "build release"
(cd "$REPO" && cargo build --release 2>&1 | tail -1) || { echo "FAIL: build"; exit 1; }
pkill -f "rclone serve webdav" 2>/dev/null; sleep 1
cd "$LAB/dav-data"
rclone serve webdav --addr 127.0.0.1:5244 --baseurl /dav --user labuser --pass labpass . > "$LAB/dav.log" 2>&1 &
sleep 2
curl -s -m 3 -u labuser:labpass -X PROPFIND -H "Depth: 0" -o /dev/null -w "%{http_code}" http://127.0.0.1:5244/dav/ | grep -q 207 || { echo "FAIL: dav"; exit 1; }

export CDN_LAB_DAV_USER=labuser CDN_LAB_DAV_PASS=labpass CDN_LAB_PREWARM_SECRET=labsecret

# --- 1. standard: full-file water-pipe --------------------------------------
note "1. standard profile (7777): full-file cache"
rm -rf "$LAB/cache-a/redb.db" "$LAB/cache-a/media"
cd "$REPO"
"$BIN" deploy/lab/config-a.toml > "$LAB/cache-a/serve.log" 2>&1 &
sleep 2
# Cold pull of 1MB file
t1=$(curl -s -m 30 -o /dev/null -w "%{time_total}" "http://127.0.0.1:7777/media/big1mb.bin")
# Second pull: disk hit
t2=$(curl -s -m 30 -o /dev/null -w "%{time_total}" "http://127.0.0.1:7777/media/big1mb.bin")
awk -v a="$t1" -v b="$t2" 'BEGIN { exit !(b <= a + 0.05) }' \
  && ok "standard: hit t=$t2 <= cold t=$t1" || bad "standard: hit $t2 > cold $t1"
# File on disk
[ -f "$LAB/cache-a/media/big1mb.bin" ] && ok "standard: file on disk" || bad "standard: no file on disk"
# healthz entries
e=$(curl -s -m 3 http://127.0.0.1:7777/_internal/healthz | python3 -c "import json,sys; print(json.load(sys.stdin)['entries'])" 2>/dev/null)
[ "$e" = "1" ] && ok "standard: entries=1" || bad "standard: entries=$e"
pkill -f "config-a.toml" 2>/dev/null; sleep 1

# --- 2. efficient: ranged staging -> promotion ------------------------------
note "2. efficient profile (7779): coverage promotion"
rm -rf "$LAB/cache-c/redb.db" "$LAB/cache-c/media"
"$BIN" deploy/lab/config-c.toml > "$LAB/cache-c/serve.log" 2>&1 &
sleep 2
# 5 x 200KB ranges = 100% coverage (threshold 0.8)
for off in 0 200000 400000 600000 800000; do
  curl -s -m 10 -o /dev/null -H "Range: bytes=$off-$((off+199999))" "http://127.0.0.1:7779/media/big1mb.bin"
done
# Wait for promotion
promoted=0
for i in $(seq 1 40); do
  e=$(curl -s -m 2 http://127.0.0.1:7779/_internal/healthz | python3 -c "import json,sys; print(json.load(sys.stdin)['entries'])" 2>/dev/null)
  [ "$e" = "1" ] && { promoted=1; break; }
  sleep 0.5
done
[ "$promoted" = 1 ] && ok "efficient: promoted entries=1" || bad "efficient: not promoted"
# Full GET now disk hit
t=$(curl -s -m 10 -o /dev/null -w "%{time_total}" "http://127.0.0.1:7779/media/big1mb.bin")
awk -v a="$t" 'BEGIN { exit !(a < 0.1) }' && ok "efficient: full GET disk hit t=${t}s" || bad "efficient: full GET t=${t}s"
# Staged history cleaned
sb=$(curl -s -m 3 http://127.0.0.1:7779/_internal/healthz | python3 -c "import json,sys; print(json.load(sys.stdin)['segment_bytes'])" 2>/dev/null)
[ "$sb" = "0" ] && ok "efficient: segment_bytes=0 after promotion" || bad "efficient: segment_bytes=$sb"
pkill -f "config-c.toml" 2>/dev/null; sleep 1

# --- 3. nocache: zero disk ---------------------------------------------------
note "3. nocache profile (7778): zero disk writes"
rm -rf "$LAB/cache-b/redb.db"
"$BIN" deploy/lab/config-b.toml > "$LAB/cache-b/serve.log" 2>&1 &
sleep 2
# Pull 1MB file twice
curl -s -m 30 -o /dev/null "http://127.0.0.1:7778/media/big1mb.bin"
curl -s -m 30 -o /dev/null "http://127.0.0.1:7778/media/big1mb.bin"
# Zero cache objects on disk (only redb.db + serve.log)
files=$(ls "$LAB/cache-b" | grep -vc "^redb.db$\|^serve.log$")
[ "$files" = 0 ] && ok "nocache: zero disk objects" || { bad "nocache: $files stray files"; ls "$LAB/cache-b"; }
# entries stays 0
e=$(curl -s -m 3 http://127.0.0.1:7778/_internal/healthz | python3 -c "import json,sys; print(json.load(sys.stdin)['entries'])" 2>/dev/null)
[ "$e" = "0" ] && ok "nocache: entries=0" || bad "nocache: entries=$e"
# Every pull hits upstream: PROPFIND count grows
b=$(grep -c "PROPFIND" "$LAB/dav.log" 2>/dev/null || echo 0)
curl -s -m 30 -o /dev/null "http://127.0.0.1:7778/media/big1mb.bin"
a=$(grep -c "PROPFIND" "$LAB/dav.log" 2>/dev/null || echo 0)
[ $((a - b)) -ge 1 ] && ok "nocache: upstream stat on every pull (delta=$((a-b)))" || bad "nocache: no upstream stat"

echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
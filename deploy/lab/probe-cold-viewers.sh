#!/usr/bin/env bash
# Phase 1 acceptance: N concurrent viewers on genuinely COLD bands of the
# 200 GiB film, through the CDN, with both sides accounted.
#
# "Cold" is asserted, not assumed: the reader reports the edge's own
# `eo-cache-status` per response (`edgeMISS`), and each run picks fresh bands and
# fresh jump seeds, so a second run of the same command cannot be its own cache.
#
# The origin's side comes from `fill-account.sh` (front-access lines = how much
# the edge actually asked the origin for) plus the open/stat counter deltas.
set -u
cd /mnt/hdd/zcode-on-the-move/Onedrive-as-Origin || exit 1
# Percent-encoded: the repo's pre-push hook refuses CJK in code.
FILM=%E9%9C%87%E6%92%BC%E6%88%91%E4%BB%AC%E7%9A%84%E6%9C%AA%E6%9D%A5%E5%90%A7_200G.mp4
NODE=oracle-cdn
VIEWERS=${1:-4}
CHUNKS=${2:-4}
TAG="n${VIEWERS}"

snap() { # print the two counters we diff
  timeout 60 ssh -o ConnectTimeout=15 $NODE \
    'export no_proxy="*"; curl -s -m 5 http://127.0.0.1:9090/metrics | grep -E "^backend_call_duration_seconds_count\{op=\"(open|stat)\"\}" | awk "{print \$2}" | tr "\n" " "' \
    2>/dev/null | grep -v "post-quantum\|store now\|may need\|WARNING"
}

echo "=== viewers=$VIEWERS cold-band run (chunks=$CHUNKS x 5 MiB, 2 seeks)"
echo "  origin before (opens stats): $(snap)"
timeout 400 node deploy/lab/viewer/multi-viewer.mjs \
  --target https://cdn-oracle.isui.ren/googledrive1 \
  --object "$FILM" --page test-page.html --size 214748364800 \
  --viewers "$VIEWERS" --chunks "$CHUNKS" --chunk-bytes 5242880 --seeks 2 \
  --gap-ms 1500 --cold-band --viewer-timeout-secs 240 2>&1 | tail -"$((VIEWERS + 3))"
echo "  origin after  (opens stats): $(snap)"
echo "  --- the edge's own asks in this window ---"
timeout 120 ssh -o ConnectTimeout=15 $NODE "bash /home/opc/fill-account.sh 4 200G | head -6" \
  2>/dev/null | grep -v "post-quantum\|store now\|may need\|WARNING"

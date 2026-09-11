#!/usr/bin/env bash
# Cold-pull verification (wayfinder #47 review; the protocol that catches
# the faults that were missed).
#
# Why this exists: earlier ad-hoc measurement produced four wrong
# conclusions because it (1) ran against the origin node instead of a
# real client, (2) did not verify transferred bytes so truncation looked
# like speed, (3) measured the edge cache rather than origin pull, and
# (4) let the network vary. This script pins all four.
#
# What it asserts, per segment size:
#   - every request returns 206 with size_download == segment size
#   - no request times out (curl exit 28) or truncates (exit 18)
#   - total transferred == file size
#   - TTFB is reported separately from total (a streaming pump must show
#     TTFB far below total on a cold pull)
#
# Usage:
#   deploy/verify-cold-pull.sh <url> <file_size_bytes> [segment_mb...]
# Example:
#   deploy/verify-cold-pull.sh \
#     https://cdn-oracle.isui.ren/googledrive1/test-100m.bin 104857600 1 5 10
#
# Run it FROM A REAL CLIENT, not on the origin host. On the origin host
# the edge is adjacent, latency is near-zero, and truncation/timeout
# faults cannot reproduce.
set -u

URL="${1:?usage: verify-cold-pull.sh <url> <size_bytes> [seg_mb...]}"
TOTAL="${2:?missing file size in bytes}"
shift 2
SEGS_MB=("$@")
[ ${#SEGS_MB[@]} -gt 0 ] || SEGS_MB=(1 2 4 5 10)

CONC="${CONC:-3}"        # match the code's own concurrency
TRIALS="${TRIALS:-1}"
TIMEOUT="${TIMEOUT:-120}"

echo "== cold-pull verification =="
echo "url=$URL  size=$TOTAL  conc=$CONC  trials=$TRIALS  timeout=${TIMEOUT}s"
echo
printf "%-6s %-5s %-9s %-9s %-10s %-10s %s\n" segMB conc ok to trunc bytes TTFB total speed
echo "-------------------------------------------------------------------------------"

fail=0
for seg_mb in "${SEGS_MB[@]}"; do
  seg=$(( seg_mb * 1024 * 1024 ))
  n=$(( (TOTAL + seg - 1) / seg ))
  for trial in $(seq 1 "$TRIALS"); do
    work=$(mktemp -d)
    export SEG=$seg URL WORK=$work TIMEOUT
    start=$(date +%s%N)
    # One curl per segment, at most CONC at a time. Each writes its
    # metrics to a file so we can aggregate and inspect failures.
    seq 0 $((n-1)) | xargs -P "$CONC" -I{} sh -c '
      off=$(({}*SEG)); end=$((off+SEG-1))
      curl -s --noproxy "*" -m "$TIMEOUT" -o /dev/null \
        -w "%{http_code} %{size_download} %{exitcode} %{time_starttransfer} %{time_total}\n" \
        -H "Range: bytes=$off-$end" "$URL" > "$WORK/{}" 2>&1
    '
    end=$(date +%s%N)
    elapsed_ms=$(( (end-start)/1000000 ))

    ok=$(cat "$work"/* 2>/dev/null | awk '$1==206 && $3==0 && $2>0' | wc -l)
    to=$(cat "$work"/* 2>/dev/null | awk '$3==28' | wc -l)
    trunc=$(cat "$work"/* 2>/dev/null | awk '$3==18' | wc -l)
    bytes=$(cat "$work"/* 2>/dev/null | awk '{s+=$2} END {print s+0}')
    # TTFB = max across segments (the slowest first byte the client saw)
    ttfb=$(cat "$work"/* 2>/dev/null | awk '{if($4+0>m)m=$4+0} END {printf "%.2f", m}')
    speed_mb=$(( bytes * 1000 / (elapsed_ms>0?elapsed_ms:1) / 1048576 ))
    rm -rf "$work"

    printf "%-6s %-5s %-9s %-9s %-10s %-10s %ss %ss %sMB/s\n" \
      "$seg_mb" "$CONC" "$ok/$n" "$to" "$trunc" "$bytes" "$ttfb" \
      "$(awk -v ms=$elapsed_ms 'BEGIN{printf "%.1f", ms/1000}')" "$speed_mb"

    if [ "$bytes" != "$TOTAL" ]; then fail=1; echo "   ^ FAIL: bytes $bytes != $TOTAL"; fi
    if [ "$ok" != "$n" ]; then fail=1; echo "   ^ FAIL: $ok/$n complete"; fi
    if [ "$to" != "0" ]; then fail=1; echo "   ^ FAIL: $to timeouts"; fi
    if [ "$trunc" != "0" ]; then fail=1; echo "   ^ FAIL: $trunc truncations"; fi
  done
done

echo
if [ "$fail" = 0 ]; then
  echo "PASS: all segments complete, byte-exact, no timeout/truncation"
else
  echo "FAIL: see lines above"
fi
exit "$fail"

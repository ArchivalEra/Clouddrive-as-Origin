#!/usr/bin/env bash
# Two-client concurrent video seeking — robustness test for the ranged
# cold-miss path (wayfinder: "two things watching video and scrubbing").
#
# Simulates two players seeking through a large file at the same time:
# each client issues Range requests at scattered offsets, repeatedly.
# What it exercises:
#   - the flight: both clients must converge on ONE upstream connection
#   - ranged reads from a still-growing file (growing_reader_from)
#   - seek-ahead (offset beyond the writer's current position)
#   - the S3/HTTP contract: every response 206, byte-exact, correct
#     Content-Range
#
# Usage:
#   deploy/seek-storm.sh <url> <file_size> [clients] [seeks_per_client]
# Example:
#   deploy/seek-storm.sh https://host/bucket/video.mkv 802984713 2 12
set -u

URL="${1:?usage: seek-storm.sh <url> <file_size> [clients] [seeks] }"
SIZE="${2:?missing file size}"
CLIENTS="${3:-2}"
SEEKS="${4:-12}"
CHUNK="${CHUNK:-1048576}"      # 1 MB reads, like a player filling a buffer
TIMEOUT="${TIMEOUT:-120}"

echo "== seek storm: $CLIENTS clients x $SEEKS seeks, ${CHUNK}B chunks =="
echo "url=$URL size=$SIZE"
echo

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Each client: seek to pseudo-random offsets, read a chunk, verify.
client() {
  local id="$1"
  local ok=0 bad=0 to=0
  for s in $(seq 1 "$SEEKS"); do
    # pseudo-random offset inside the file (RANDOM is per-shell; mix with id+seq)
    local off=$(( (RANDOM * 32768 + RANDOM + id * 7919 + s * 104729) % (SIZE - CHUNK) ))
    local end=$(( off + CHUNK - 1 ))
    local out
    out=$(curl -s --noproxy '*' -m "$TIMEOUT" -o /dev/null \
      -w "%{http_code} %{size_download} %{exitcode} %{time_starttransfer}" \
      -H "Range: bytes=$off-$end" "$URL" 2>&1)
    local code sz ec ttfb
    read -r code sz ec ttfb <<<"$out"
    if [ "$ec" = "0" ] && [ "$code" = "206" ] && [ "$sz" = "$CHUNK" ]; then
      ok=$((ok+1))
    elif [ "$ec" = "28" ]; then
      to=$((to+1)); echo "  client$id seek$s off=$off TIMEOUT (got ${sz}B)" >> "$work/errs"
    else
      bad=$((bad+1)); echo "  client$id seek$s off=$off code=$code size=$sz ec=$ec" >> "$work/errs"
    fi
  done
  echo "client$id: ok=$ok bad=$bad timeout=$to"
}

# Launch clients concurrently (they share the same file => same flight).
start=$(date +%s%N)
for c in $(seq 1 "$CLIENTS"); do
  client "$c" &
done
wait
end=$(date +%s%N)
el=$(( (end-start)/1000000 ))

echo
if [ -f "$work/errs" ]; then
  echo "--- failures ---"
  cat "$work/errs"
fi
echo "elapsed: ${el}ms"
echo
echo "== check the origin: how many upstream opens did this cost? =="
echo "  (run on the origin host)"
echo "  curl -s --noproxy '*' http://127.0.0.1:9090/metrics | grep 'count{op=\"open\"}'"
echo
echo "PASS criteria: every seek 206 + exact chunk; upstream open count"
echo "should be ~1 per distinct file, NOT one per seek."

#!/usr/bin/env bash
# Continuous cold-range load on the PRODUCTION origin, from the node itself.
#
# Why it exists: a browser swarm playing a playlist of one window is served
# almost entirely by EdgeOne's cache (measured 2026-09-24: 167 GiB consumed by
# ten viewers, 0.338 GiB pulled from the origin), which leaves the origin's cold
# path unmeasured. This supplies it: fresh random shards spread over the whole
# object, N in parallel, no pauses, for as long as you ask.
#
# It goes to 127.0.0.1:8080 (the business plane) rather than through the CDN, so
# it costs a workstation nothing and separates cleanly in the origin's own
# access log: loopback requests carry `xff=-`, the edge's pulls carry the
# viewer's address. The account comes from the counters/histograms
# (backend_call_duration_seconds, cache_serve_source_total, trims), not from the
# front log.
#
#   cold-load.sh [seconds] [parallel] [shard-mib] [key]
#     seconds   how long to run (default 14400 = 4 h)
#     parallel  concurrent ranges (default 8)
#     shard-mib range size (default 5)
#     key       object under googledrive1/ (default: the real 200 GiB film)
#
# Writes one `"<http code> <bytes>"` line per request to $RESULTS (default
# /home/opc/cold-load.results) and a progress line every 20 rounds to stdout.
set -u
SECS=${1:-14400}
PAR=${2:-8}
MIB=${3:-5}
KEY=${4:-%E9%9C%87%E6%92%BC%E6%88%91%E4%BB%AC%E7%9A%84%E6%9C%AA%E6%9D%A5%E5%90%A7_200G.mp4}
BIZ=${BIZ:-http://127.0.0.1:8080}
RESULTS=${RESULTS:-/home/opc/cold-load.results}
FILM="$BIZ/googledrive1/$KEY"
# The business plane answers HEAD on the loopback (it is the token-exempt path),
# and its content-length is the object's size.
TOTAL=$(curl -s -m 15 -I "$FILM" | awk 'tolower($1)=="content-length:"{print $2+0}' | tr -d '\r')
if [ -z "$TOTAL" ] || [ "$TOTAL" -lt 1048576 ]; then
  echo "cold-load: could not read the object's size from $BIZ (got '${TOTAL:-}')" >&2
  exit 2
fi
SHARD=$((MIB * 1048576))
END=$((SECONDS + SECS))
round=0
: > "$RESULTS"
echo "cold-load: ${SECS}s, ${PAR} parallel ${MIB} MiB shards, object ${TOTAL} bytes, started $(date -u +%FT%TZ)"
while [ "$SECONDS" -lt "$END" ]; do
  pids=""
  for _ in $(seq 1 "$PAR"); do
    # A 32-bit random scaled across the object. The first version used
    # `RANDOM * 32768 + RANDOM` - 30 bits, max 1 073 741 823 - against a 200 GiB
    # object, so the modulo never wrapped and EVERY read landed in the first
    # ~1 GiB. Scale first (`>> 20` keeps the product inside 64 bits), never
    # multiply the full size by a full-width random.
    rnd32=${SRANDOM:-$(( (RANDOM << 15 | RANDOM) * 4 ))}
    off=$(( (rnd32 * ((TOTAL - SHARD) >> 20)) / 4096 ))
    (
      code=$(curl -s -m 40 -o /dev/null -w '%{http_code} %{size_download}' \
        -r "$off-$((off + SHARD - 1))" "$FILM")
      echo "$code" >> "$RESULTS"
    ) &
    pids="$pids $!"
  done
  for p in $pids; do wait "$p"; done
  round=$((round + 1))
  if [ $((round % 20)) -eq 0 ]; then
    echo "cold-load: ${round} rounds, $((round * PAR)) requests, $(date -u +%FT%TZ)"
  fi
done
echo "cold-load: done after ${round} rounds ($((round * PAR)) requests) at $(date -u +%FT%TZ)"

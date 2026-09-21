#!/usr/bin/env bash
# What the CDN in front does with each RANGE SHAPE, measured from the node.
#
# The open question: a browser player's first request is `Range: bytes=0-`, and
# our origin answers it literally (200 GiB promise on our test object). Before
# deciding whether to cap that answer, we need the edge's own behaviour, not the
# player's symptom:
#
#   - does the edge relay the promise (content-length = the whole remainder)?
#   - does it cap what it pulls, or hand the client the literal answer?
#   - does it matter whether the edge's entry is COLD or WARM?
#
# The last one is the discriminator the player probe could not make: if a cold
# open-ended range stalls while a warm one streams, the finding is about cold
# cache fill, not about the shape.
#
# Run it ON THE NODE (see probe-edgeone-viewers.sh for why a workstation path
# measures the workstation). Every request is bounded; the open-ended samples
# stop after SAMPLE seconds so a 200 GiB promise costs seconds, not hours.
#
# Usage: probe-edgeone-range-shape.sh [object]   (default round3.mp4)
#   BASE / ORIGIN / ORIGIN_METRICS / SAMPLE override the CDN base, the local
#   origin, the metrics URL and the per-sample seconds.
set -u
OBJ=${1:-round3.mp4}
BASE=${BASE:-https://cdn-oracle.isui.ren}/googledrive1/$OBJ
ORIGIN=${ORIGIN:-http://127.0.0.1:8080}/googledrive1/$OBJ
MET=${ORIGIN_METRICS:-http://127.0.0.1:9090/metrics}
SAMPLE=${SAMPLE:-8}
MB=$((1024 * 1024))

export no_proxy="*"; unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 2>/dev/null

KEYS='^(content-length|content-range|transfer-encoding|eo-cache-status|age|x-cache|server):'

probe() { # url label range max-time
  local url=$1 label=$2 rng=$3 mt=$4
  echo "--- $label   (Range: bytes=$rng, max-time ${mt}s)"
  curl -s -D- -o /dev/null -m "$mt" -r "$rng" \
    -w '    >>> code=%{http_code} bytes=%{size_download} ttfb=%{time_starttransfer}s total=%{time_total}s\n' \
    "$url" 2>&1 | grep -Ei "$KEYS|>>>" | sed 's/^/    /'
}

account() { # label
  echo "--- origin $1"
  curl -s -m 5 "$MET" 2>/dev/null | grep -E \
    '^backend_call_duration_seconds_count\{op="(open|stat)"\}|^cache_serve_source_total|^cache_session_(started_)?total|^cache_session_reader_total' \
    | sed 's/^/    /' || echo "    (metrics unreadable)"
}

size=$(curl -skI -m 30 "$BASE" | awk 'tolower($1)=="content-length:"{print $2+0}' | tr -d '\r')
if [ -z "${size:-}" ] || [ "$size" = 0 ]; then
  echo "FAIL: no content-length from $BASE (CDN reachable without a proxy?)"; exit 1
fi
# Two disjoint fresh bands: the edge has never seen these offsets, so a request
# there is a MISS and its account is the origin's pull.
band1=$(( (size / 100) * (10 + (RANDOM % 70)) ))
band2=$(( (size / 100) * (10 + (RANDOM % 70)) + 7919 ))
echo "object: $OBJ  size=$size bytes"
echo "cdn:    $BASE"
echo "origin: $ORIGIN"
echo "bands:  bounded@$band1  open@$band2   sample=${SAMPLE}s"
echo

account before
probe "$BASE" "WARM open-ended (bytes=0-, the entry the edge already has)" "0-" "$SAMPLE"
probe "$BASE" "WARM bounded 1 MiB (control: the shape the origin is designed for)" "0-$((MB - 1))" 30
probe "$BASE" "COLD bounded 1 MiB at a fresh offset (edge must pull a bounded span)" "$band1-$((band1 + MB - 1))" 30
account "after cold-bounded"
probe "$BASE" "COLD open-ended at a fresh offset (edge must pull an unbounded span)" "$band2-" "$SAMPLE"
account "after cold-open"
probe "$BASE" "COLD open-ended, immediately repeated (does the partial entry help?)" "$band2-" "$SAMPLE"
echo

# Same bytes through both paths: the edge is only useful if it returns the
# object's real bytes, whatever shape it was asked in.
echo "--- byte-for-byte: 1300 bytes at offset $band2 via CDN vs direct from the origin"
curl -s -m 30 -r "$band2-$((band2 + 1299))" "$BASE" | sha256sum | sed "s|-|cdn   |;s/^/    /"
curl -s -m 30 -r "$band2-$((band2 + 1299))" "$ORIGIN" | sha256sum | sed "s|-|origin|;s/^/    /"
echo

# Rate control: the same bounded span straight from the local origin. If this is
# fast while the CDN path is slow, the bottleneck is the edge or the tunnel, not
# the origin's own upstream pull.
probe "$ORIGIN" "DIRECT bounded 1 MiB at a fresh offset (rate control)" "$((band1 + MB))-$((band1 + 2 * MB - 1))" 60
account after

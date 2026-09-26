#!/usr/bin/env bash
# What a range size COSTS: the shard-size sweep run ON THE ORIGIN HOST.
#
# The client-side half (`deploy/lab/probe-shard-size.sh`) answers "how fast does
# a range arrive"; when the edge already holds the range it says nothing about
# what the origin paid. This one reads the same four sizes on the origin's own
# loopback business plane -- the edge is not in the path -- and reads the
# origin's counters around every request, so each size gets an exact account:
#
#   upstream opens   per request, and per byte (the cost that scales)
#   upstream bytes   equals the delivered bytes on the ranged path: no
#                    amplification, so a shard size changes CALLS PER BYTE
#   which layer answered (stage / upstream / disk) and any trims
#
# Every (size, band) gets its OWN offset, at least 512 MiB from any other, and is
# read twice: cold (pays for the upstream fill) and warm (answered by the stage:
# ~1 GB/s, zero opens). Reading the same offsets at every size is the mistake
# that makes 4/5/10 MiB look free -- the 2 MiB pass has already filled the window,
# so the later sizes measure a warm stage.
#
#   deploy/oracle/shard-sweep-origin.sh [--sizes "2 4 5 10"] [--key <object key>]
#
# Measured 2026-09-26 on the 60 GiB film (loopback, sequential, cold):
#
#   2 MiB    1-2 opens / 1 stat, upstream bytes 2 MiB,  total 1.28-1.32 s
#   4 MiB    1 open  / 1 stat, upstream bytes 4 MiB,  total 1.20-1.39 s
#   5 MiB    1-2 opens / 1 stat, upstream bytes 5 MiB, total 1.25-1.35 s
#   10 MiB   1 open  / 1 stat, upstream bytes 10 MiB, total 1.38-1.66 s
#   warm:    zero opens, 3-10 ms, ~1 GB/s
#
# So the total is ~flat across sizes (a ~1 s first-byte constant from the upstream
# open), and the delivery rate is the fill rate, not the range size. What a shard
# size buys is fewer upstream calls per byte: one open per 2 MiB against one per
# 10 MiB. The client-side numbers and the fit are in the other half's header.
set -u
BIZ=${BIZ:-http://127.0.0.1:8080}
KEY=${KEY:-numb_numb_TAK_60G.mkv}
M=${ORIGIN_METRICS:-http://127.0.0.1:9090/metrics}
SIZES="2 4 5 10"
BANDS="6 20"                       # GiB into the object
while [ $# -gt 0 ]; do
  case $1 in
    --sizes) SIZES=$2; shift 2 ;;
    --key)   KEY=$2; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
MIB=1048576
FILM="$BIZ/googledrive1/$KEY"
TOTAL=$(curl -s -m 15 -I "$FILM" | awk 'tolower($1)=="content-length:"{print $2+0}' | tr -d '\r')
if [ "${TOTAL:-0}" -lt 1048576 ]; then
  echo "could not read content-length from $FILM" >&2
  exit 2
fi
echo "object ${TOTAL} bytes; sizes ${SIZES} MiB; bands ${BANDS} GiB"

snap() {
  curl -s -m 8 "$M" | grep -E '^(backend_call_duration_seconds_count|cache_serve_source_total|cache_body_bytes_total|cache_session_total|cache_unkeepable_trim_bytes_total)' | sort
}
grab() { awk -v m="$2" '$0 ~ m {s += $(NF)} END {printf "%d", s + 0}' <<<"$1"; }
d() { echo $(( $(grab "$2" "$3") - $(grab "$1" "$3") )); }

one() { # size-mib offset pass-label
  local s=$1 off=$2 label=$3 b a out
  b=$(snap)
  out=$(curl -s -o /dev/null -m 150 -r "$off-$((off + s * MIB - 1))" \
    -w '%{http_code} size=%{size_download} ttfb=%{time_starttransfer} total=%{time_total} speed=%{speed_download}' "$FILM")
  a=$(snap)
  printf '%-4s size=%-2sMiB off=%-12s %s\n      opens=%s stat=%s serve[stage/upstream/disk]=%s/%s/%s upstream_bytes=%s trim=%s\n' \
    "$label" "$s" "$off" "$out" \
    "$(d "$b" "$a" 'backend_call_duration_seconds_count.*op="open"')" \
    "$(d "$b" "$a" 'backend_call_duration_seconds_count.*op="stat"')" \
    "$(d "$b" "$a" 'cache_serve_source_total.*source="stage"')" \
    "$(d "$b" "$a" 'cache_serve_source_total.*source="upstream"')" \
    "$(d "$b" "$a" 'cache_serve_source_total.*source="disk"')" \
    "$(d "$b" "$a" 'cache_body_bytes_total')" \
    "$(d "$b" "$a" 'cache_unkeepable_trim_bytes_total')"
}

PAIRS=()
si=0
for s in $SIZES; do
  for band in $BANDS; do
    PAIRS+=("$s $(( band * 1024 * MIB + si * 512 * MIB ))")
  done
  si=$((si + 1))
done
for pass in cold warm; do
  for p in "${PAIRS[@]}"; do
    set -- $p
    one "$1" "$2" "$pass"
  done
done

echo
echo "read it as: cold = one upstream open + one stat per request, upstream bytes == delivered"
echo "bytes, total time ~flat across sizes; warm = stage hit, zero opens, ~1 GB/s. A shard size"
echo "changes CALLS PER BYTE, not the transfer rate."

#!/usr/bin/env bash
# The window decision, measured on the node: a jump pays the floor, a
# continuation ramps, a wide request gets what it asked for.
#
# Before/after, same script shape as the 2026-09-22 reading that motivated it:
# a 5 MiB cold jump took `segment_bytes` up by exactly 67,108,864 bytes (one
# 64 MiB window, a 12.8x amplification).
export no_proxy="*"; unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 2>/dev/null
F=http://127.0.0.1:8080/googledrive1/%E9%9C%87%E6%92%BC%E6%88%91%E4%BB%AC%E7%9A%84%E6%9C%AA%E6%9D%A5%E5%90%A7_200G.mp4
MET=http://127.0.0.1:9090/metrics

seg() { curl -s -m 5 http://127.0.0.1:8080/_internal/healthz 2>/dev/null | grep -oE '"segment_bytes":[0-9]+' | cut -d: -f2; }
opens() { curl -s -m 5 "$MET" | awk -F' ' '/^backend_call_duration_seconds_count\{op="open"\}/ {print $2; exit}'; }
MIB=1048576

# Fresh bands per run: the origin stages what it reads, so a fixed offset
# measures the second run's cache, not the policy. Three bands, far apart.
A=$(( (214748364800 / 100) * (10 + RANDOM % 25) ))
B=$(( (214748364800 / 100) * (40 + RANDOM % 25) ))
C=$(( (214748364800 / 100) * (70 + RANDOM % 20) ))
echo "  bands: A=$A B=$B C=$C"

# A seal lands after the body it seals (the driver keeps pumping), so a delta
# sampled immediately after a response blames the NEXT row for it. Wait until
# `segment_bytes` has been still for a second before each measurement.
settle() {
  local last seen same=0
  last=$(seg)
  for _ in $(seq 1 60); do
    sleep 0.5
    seen=$(seg)
    if [ "$seen" = "$last" ]; then same=$((same + 1)); else same=0; fi
    last=$seen
    [ "$same" -ge 2 ] && return
  done
}

report() { # label, before_seg, before_opens, range, want
  local label=$1 s0=$2 o0=$3 rng=$4
  curl -s -m 60 -r "$rng" -o /dev/null -w "    served bytes=%{size_download} total=%{time_total}s\n" "$F"
  settle
  local s1 o1
  s1=$(seg); o1=$(opens)
  echo "  $label: +$(( (s1 - s0) / MIB )) MiB staged (want $5), +$((o1 - o0)) upstream opens"
}

echo "window decision on the node (floor = the default 8 MiB, window = 64 MiB)"
settle
S=$(seg); O=$(opens)
echo "  start: segment_bytes=$S opens=$O"
echo "  (each row settles before the next is measured)"
report "jump A, 5 MiB" "$S" "$O" "$A-$((A + 5 * MIB - 1))" "8"
S=$(seg); O=$(opens)
report "continue A, next 3 MiB" "$S" "$O" "$((A + 5 * MIB))-$((A + 8 * MIB - 1))" "0"
S=$(seg); O=$(opens)
report "read the floor out, 8 MiB" "$S" "$O" "$((A + 8 * MIB))-$((A + 16 * MIB - 1))" "8"
S=$(seg); O=$(opens)
report "jump B, 5 MiB" "$S" "$O" "$B-$((B + 5 * MIB - 1))" "8"
S=$(seg); O=$(opens)
report "jump C, a single 20 MiB request" "$S" "$O" "$C-$((C + 20 * MIB - 1))" "20"
echo "  end: segment_bytes=$(seg) opens=$(opens)"

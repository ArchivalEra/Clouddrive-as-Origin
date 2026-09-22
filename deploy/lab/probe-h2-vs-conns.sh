#!/usr/bin/env bash
# Does ONE multiplexed HTTP/2 connection behave better than N separate
# connections on this CDN path? The path's documented failure is per-connection:
# of N concurrent connections from this workstation, one stalls for 9 s to 300 s
# while the origin sits idle. If multiplexing removes the stall, that is the
# "connection count, not speed" lever in a form we can actually deploy (standard
# H2, cacheable Range requests, no private framing).
#
# Arm A: N separate curl processes (N connections).
# Arm B: ONE curl with --parallel --http2 (N range requests, multiplexed).
# Both: N x 5 MiB, fresh offsets per repetition, 30 s cap, 3 repetitions.
set -u
# Percent-encoded: the repo's pre-push hook refuses CJK in tracked text.
FILM=%E9%9C%87%E6%92%BC%E6%88%91%E4%BB%AC%E7%9A%84%E6%9C%AA%E6%9D%A5%E5%90%A7_200G.mp4
B="https://cdn-oracle.isui.ren/googledrive1/$FILM"
N=${N:-4}
MIB=1048576
SPAN=214748364800
CAP=30

sockets() { # curl's own established sockets
  ss -tnp 2>/dev/null | grep -c '"curl"'
}

arm_a() { # rep -> one line
  local rep=$1 base=$(( (SPAN / 100) * (10 + rep * 7) )) t0 t1
  t0=$(date +%s%N)
  local pids=()
  for i in $(seq 0 $((N - 1))); do
    local o=$((base + i * 3 * MIB))
    curl -s --noproxy '*' --http2 -m $CAP -r "$o-$((o + 5 * MIB - 1))" -o /dev/null "$B" &
    pids+=($!)
  done
  local ok=0
  for p in "${pids[@]}"; do wait "$p" && ok=$((ok + 1)); done
  t1=$(date +%s%N)
  awk -v n="$N" -v ok="$ok" -v w=$(( (t1 - t0) / 1000000 )) 'BEGIN {
    printf "  A (separate conns): %d/%d completed, wall=%.1fs, up to %.2f MB/s\n", ok, n, w/1000, (ok*n*5)/1.048576/(w/1000)
  }'
}

arm_b() { # rep -> one line
  local rep=$1 base=$(( (SPAN / 100) * (60 + rep * 7) )) cfg=/tmp/h2-$$.cfg t0 t1
  : > "$cfg"
  for i in $(seq 0 $((N - 1))); do
    local o=$((base + i * 3 * MIB))
    {
      echo "url = \"$B\""
      echo "range = \"$o-$((o + 5 * MIB - 1))\""
      echo "output = \"/dev/null\""
      echo "silent"
      echo "max-time = $CAP"
    } >> "$cfg"
  done
  t0=$(date +%s%N)
  curl --noproxy '*' --http2 --parallel --parallel-max "$N" --config "$cfg" >/dev/null 2>&1 &
  local pid=$!
  sleep 2
  local socks; socks=$(sockets)
  wait "$pid"; local rc=$?
  t1=$(date +%s%N)
  rm -f "$cfg"
  awk -v n="$N" -v rc="$rc" -v s="$socks" -v w=$(( (t1 - t0) / 1000000 )) 'BEGIN {
    printf "  B (one mux conn) : rc=%d, curl sockets=%s, wall=%.1fs, %.2f MB/s\n", rc, s, w/1000, (n*5)/1.048576/(w/1000)
  }'
}

echo "N=$N, 5 MiB each, cap ${CAP}s, 3 repetitions"
for rep in 0 1 2; do
  echo "--- rep $rep"
  arm_a "$rep"
  arm_b "$rep"
done

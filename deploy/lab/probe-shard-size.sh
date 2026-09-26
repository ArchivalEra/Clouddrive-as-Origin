#!/usr/bin/env bash
# What a range size actually changes: a shard-size sweep from the CLIENT side.
#
# "Is a 2 MiB range slower than a 10 MiB one?" has three different answers
# depending on which number you read, which is why this comes back as "the
# speed can't be measured" every time it is asked:
#
#   * time to first byte is FLAT across sizes (a seek does not care);
#   * total time is a per-request CONSTANT plus a small marginal cost, so a
#     bigger shard looks faster only because it amortises that constant;
#   * the average rate therefore climbs with size, and two single samples of it
#     decide nothing (this leg moves 300 B/s..7 MB/s minute to minute).
#
# So this measures all three, round-robin over the sizes so network drift hits
# every size equally, and records `eo-cache-status` per request: if the edge
# already holds the range, the origin is not in the path at all and this is a
# client<->edge measurement. What the origin paid is a different number, read on
# the origin's own loopback by `deploy/oracle/shard-sweep-origin.sh`.
#
#   deploy/lab/probe-shard-size.sh --url <media url> [--samples 5] [--sizes "2 4 5 10"]
#
# Measured 2026-09-26 on the 60 GiB Matroska film through the CDN (all rows
# `edge HIT`: the edge held that object, so this is the client<->edge leg):
#
#   range    TTFB          total          average rate
#   2 MiB    0.47-1.13 s   2.86-3.69 s    0.57-0.73 MB/s
#   4 MiB    0.46-0.94 s   3.60-4.34 s    0.97-1.16 MB/s
#   5 MiB    0.47-0.71 s   3.57-5.13 s    1.02-1.47 MB/s
#   10 MiB   0.48-0.72 s   4.94-5.86 s    1.79-2.12 MB/s
#
#   fit: total ~= 2.6-3.0 s + 0.24-0.26 s/MiB (marginal ~4 MB/s)
#
# The origin-side sweep measured on the same day: every cold read costs exactly
# one upstream open and one stat and pulls exactly the bytes asked for (no
# amplification), with a ~1 s first-byte constant and a flat total time across
# sizes -- so a shard size changes CALLS PER BYTE (one open per 2 MiB vs one per
# 10 MiB), not the transfer rate. The origin's counters are the authority; a
# client's rate is a symptom of the leg it is on.
set -u
SAMPLES=5
SIZES="2 4 5 10"
URL=""
while [ $# -gt 0 ]; do
  case $1 in
    --samples) SAMPLES=$2; shift 2 ;;
    --sizes)   SIZES=$2; shift 2 ;;
    --url)     URL=$2; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
: "${URL:?pass --url with the media URL}"

RUN=${OUT_DIR:-/mnt/hdd/shard-sweep}
mkdir -p "$RUN"
LOG=$RUN/shard-sweep.log
: > "$LOG"

TOTAL=$(curl -s -m 20 -I "$URL" | awk 'tolower($1)=="content-length:"{print $2+0}' | tr -d '\r')
if [ "${TOTAL:-0}" -lt 1048576 ]; then
  echo "could not read content-length from the URL (got '${TOTAL:-}')" >&2
  exit 2
fi
MIB=1048576
echo "object ${TOTAL} bytes; sizes ${SIZES} MiB; ${SAMPLES} samples each"
echo "media: $URL"

# Round-robin: one sample of every size, then the next sample -- the same
# wall-clock window for all sizes, so a busy line does not land on one size.
for round in $(seq 1 "$SAMPLES"); do
  for s in $SIZES; do
    shard=$((s * MIB))
    # A 32-bit random scaled to the object. A 30-bit source behind a modulo that
    # never wraps put every read in the first GiB once (pitfall 67): scale the
    # product, never multiply the full size by a full-width random.
    rnd32=${SRANDOM:-$(( (RANDOM << 15 | RANDOM) * 4 ))}
    off=$(( (rnd32 * ((TOTAL - shard) >> 20)) / 4096 ))
    hdr=$(mktemp)
    out=$(curl -s -o /dev/null -m 150 -D "$hdr" -r "$off-$((off + shard - 1))" \
      -w '%{http_code} %{size_download} %{time_starttransfer} %{time_total} %{speed_download}' "$URL")
    rc=$?
    eo=$(grep -i '^eo-cache-status' "$hdr" | tr -d '\r' | awk '{print $2}')
    rm -f "$hdr"
    # fields: size, offset, eo-cache, curl-exit, http, bytes, ttfb, total, speed
    printf '%s\t%s\t%s\t%s\t%s\n' "$s" "$off" "${eo:-?}" "$rc" \
      "$(echo "$out" | tr ' ' '\t')" >> "$LOG"
    echo "  round $round/$SAMPLES  ${s}MiB off=$off eo=${eo:-?} $out rc=$rc"
  done
done

echo
echo "=== per size (medians over $SAMPLES samples each) ==="
awk -F'\t' '
function med(a, n,   b, i, j, t) {
  if (n == 0) return -1
  for (i = 1; i <= n; i++) b[i] = a[i]
  for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) if (b[j] < b[i]) { t = b[i]; b[i] = b[j]; b[j] = t }
  return (n % 2) ? b[(n + 1) / 2] : (b[n / 2] + b[n / 2 + 1]) / 2
}
BEGIN {
  printf "%-9s %4s %9s %9s %10s   %s\n", "size", "n", "ttfb_s", "total_s", "avg_MBps", "206-exact  edge-HIT  other"
}
{
  s = $1; n[s]++
  ttfb[s, n[s]] = $7; tot[s, n[s]] = $8; spd[s, n[s]] = $9
  if ($5 == 206 && $6 + 0 == s * 1048576) ok[s]++
  if ($3 == "HIT") { hit[s]++; hittot++ } else if ($3 != "?") other[s]++
  ntot++
  seen[s] = 1
}
END {
  for (s in seen) {
    for (i = 1; i <= n[s]; i++) { a1[i] = ttfb[s, i]; a2[i] = tot[s, i]; a3[i] = spd[s, i] }
    mt = med(a1, n[s]); mo = med(a2, n[s]); ms = med(a3, n[s])
    printf "%-9s %4d %9.3f %9.3f %10.2f   %3d/%-3d %8d %6d\n", s "MiB", n[s], mt, mo, ms / 1048576, ok[s], n[s], hit[s] + 0, other[s] + 0
    if (lo == "" || s + 0 < lo + 0) { lo = s; lo_t = mo }
    if (hi == "" || s + 0 > hi + 0) { hi = s; hi_t = mo }
  }
  print ""
  if (hi != lo) {
    k = (hi_t - lo_t) / (hi - lo)
    printf "fit: total ~= %.2f s + %.3f s/MiB   (marginal %.1f MiB/s), from the %sMiB and %sMiB medians\n", lo_t - lo * k, k, 1 / k, lo, hi
    print "     the CONSTANT is the per-request cost, the slope is the transfer: a bigger shard is"
    print "     faster on average only because it amortises the constant."
  }
  print ""
  printf "edge-HIT rows: %d of %d. If nearly all are HIT the origin was not in this path at all --\n", hittot, ntot
  print "read the origin account (deploy/oracle/shard-sweep-origin.sh) before concluding anything about cost."
}
' "$LOG"
echo "raw: $LOG"

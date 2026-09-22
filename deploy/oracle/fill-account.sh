#!/usr/bin/env bash
# The origin's side of a CDN round: what the edge asked for, how much, and how
# long the origin took to answer.
#
# Why this exists: the front plane already logs one line per request
# (`front/src/lib.rs`, the "front access" record) carrying the key, the bytes,
# `duration_ms` and the client the edge was serving (`xff`). Every CDN round of
# 2026-09-21/22 rebuilt this account by hand from that log — the 1 MiB fills at
# 86 ms when the bytes were staged against 265 ms median when they were not,
# and the 0.33 MB/s a mixed seven-minute window averaged. This is that account
# as one command, so a client-side number and an origin-side number can be read
# from the same round.
#
# Run it ON the node (the log lives there):
#
#   ssh <node> 'bash fill-account.sh [minutes] [key-substring]'
#
# minutes defaults to 10; the filter is a plain substring match on the key, so
# `fill-account.sh 5 200G` reads one object's fills. `UNIT` and `ORIGIN_METRICS`
# override the unit name and the metrics URL.
set -u
MIN=${1:-10}
FILTER=${2:-}
UNIT=${UNIT:-origin-cache-efficient}
METRICS=${ORIGIN_METRICS:-http://127.0.0.1:9090/metrics}

export no_proxy="*"; unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 2>/dev/null

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

# An empty log is an answer, not an error: grep exits 1 when nothing matched,
# and a round the edge answered from its own cache leaves no lines at all.
{
  journalctl -u "$UNIT" --since "$MIN minutes ago" --no-pager 2>/dev/null || true
} | {
  if [ -n "$FILTER" ]; then grep "front access" | grep -F "$FILTER"; else grep "front access"; fi
} > "$TMP" || true

TOTAL=$(wc -l < "$TMP" | tr -d ' ')
echo "origin fill account  unit=$UNIT  window=${MIN}min  filter=${FILTER:-<none>}"
echo "  front-access lines: $TOTAL"

if [ "$TOTAL" != "0" ]; then
  awk '
    {
      p = ""; n = 0; ms = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^path=/)             { split($i, a, "="); p = a[2] }
        else if ($i ~ /^bytes=/)       { split($i, b, "="); n = b[2] + 0 }
        else if ($i ~ /^duration_ms=/) { split($i, d, "="); ms = d[2] + 0 }
        else if ($i ~ /^xff=/)         { peers[$i] = 1 }
      }
      sub(/.*%2F/, "", p); sub(/.*\//, "", p)
      # A CDN key arrives percent-encoded; one marker keeps the table legible
      # without pretending to decode it.
      sub(/^(%[0-9A-Fa-f][0-9A-Fa-f])+/, "<esc>", p)
      if (length(p) > 44) p = substr(p, 1, 41) "..."
      cnt[p]++; byt[p] += n; n_ms[p]++; ms_sum[p] += ms; sample[p, n_ms[p]] = ms
      if (ms > worst[p]) worst[p] = ms
      t_cnt++; t_byt += n
    }
    END {
      printf "  %-46s %6s %11s %8s %8s %8s\n", "key", "reqs", "MiB", "p50ms", "p90ms", "maxms"
      for (k in cnt) {
        for (i = 1; i <= n_ms[k]; i++)
          for (j = i + 1; j <= n_ms[k]; j++)
            if (sample[k, j] < sample[k, i]) { t = sample[k, i]; sample[k, i] = sample[k, j]; sample[k, j] = t }
        p50 = sample[k, int((n_ms[k] + 1) / 2)]
        p90 = sample[k, int(n_ms[k] * 0.9) + 1]
        printf "  %-46s %6d %11.1f %8d %8d %8d\n", k, cnt[k], byt[k] / 1048576, p50, p90, worst[k]
      }
      printf "  %-46s %6d %11.1f\n", "TOTAL", t_cnt, t_byt / 1048576
      printf "  distinct clients the edge served (xff): %d\n", length(peers)
    }' "$TMP"
fi

echo "  --- origin counters now (diff two runs for a round's account) ---"
curl -s -m 5 "$METRICS" 2>/dev/null | grep -E \
  '^backend_call_duration_seconds_count\{op="(open|stat)"\}|^cache_serve_source_total|^cache_session_total' \
  | sed 's/^/    /' || echo "    (metrics unreadable)"

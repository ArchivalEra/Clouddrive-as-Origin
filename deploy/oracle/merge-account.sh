#!/usr/bin/env bash
# The origin's half of the merge account: what it delivered to viewers, and
# what it paid upstream for those bytes.
#
# Why it exists: the product's central question is what our traffic looks like
# from the provider's side — a handful of long reads, or a flood of small ones.
# The client's report cannot answer it. The ratio between the bytes the origin
# answered with (`cache_body_bytes_total`, split by where they came from) and
# the upstream calls it paid for them (`backend_call_duration_seconds_count`)
# can, and one run of a browser swarm is the workload that gives it a number.
#
# Run it ON the node:
#
#   bash merge-account.sh mark            # before the window you want
#   ... let the workload run ...
#   bash merge-account.sh report          # the account for that window
#
#   bash merge-account.sh mark cold-leg   # named marks: several windows, one node
#   bash merge-account.sh report cold-leg
#
# `mark` snapshots the counters; `report` diffs them against the counters now
# and adds the front-access window since the mark (that is where the edge's
# asks are counted; a viewer's own requests are only visible on the machine
# that made them). The two viewer-side cells of the table — how many requests
# and how many bytes the browsers really moved — come from the viewer harness's
# report on that other machine, so this script prints the origin's half and
# names the origin's own numbers for what they are.
#
# Environment overrides: MERGE_ACCOUNT_DIR (default ~/merge-account),
# ORIGIN_METRICS, UNIT. `ORIGIN_METRICS` may be a `file://` URL, which is how
# the arithmetic is tested without a node: mark from one saved scrape, report
# from another.
set -u
MODE=${1:-report}
NAME=${2:-run}
DIR=${MERGE_ACCOUNT_DIR:-$HOME/merge-account}
METRICS=${ORIGIN_METRICS:-http://127.0.0.1:9090/metrics}
UNIT=${UNIT:-origin-cache-efficient}
MARK=$DIR/mark-$NAME.txt
NOW=$DIR/now-$NAME.txt

usage() { sed -n '2,30p' "$0" | sed 's/^#\{1,\} \{0,1\}//'; }

# The series this account reads. Everything else in the registry is either a
# histogram (read with deploy/metrics-report.sh) or a gauge about the node
# rather than about the merge.
series() {
  grep -E '^(backend_call_duration_seconds_count\{op="(open|stat)"\}|cache_body_bytes_total|cache_serve_source_total|cache_session_total|cache_session_reader_total|front_requests_total)' "$1" || true
}

fetch() { # $1 = where to write the normalized snapshot
  export no_proxy="*"
  unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 2>/dev/null
  if ! curl -sS -m 15 "$METRICS" -o "$1.tmp"; then
    echo "merge-account: $METRICS did not answer" >&2
    exit 2
  fi
  series "$1.tmp" | sort > "$1"
  rm -f "$1.tmp"
}

case "$MODE" in
  mark)
    mkdir -p "$DIR"
    fetch "$MARK"
    printf '# epoch %s %s\n' "$(date -u +%s)" "$(date -u +%FT%TZ)" | cat - "$MARK" > "$MARK.new"
    mv "$MARK.new" "$MARK"
    echo "merge-account: mark '$NAME' -> $MARK ($(( $(wc -l < "$MARK") - 1 )) series)"
    ;;
  report)
    [ -s "$MARK" ] || {
      echo "merge-account: no mark '$NAME' at $MARK (make one: bash merge-account.sh mark $NAME)" >&2
      exit 2
    }
    fetch "$NOW"
    START=$(awk '/^# epoch/{print $3}' "$MARK")
    START_ISO=$(awk '/^# epoch/{print $4}' "$MARK")
    END_ISO=$(date -u +%FT%TZ)
    END=$(date -u +%s)

    # Counter deltas. The key is the series without its trailing value, which is
    # all a counter's identity is; a series the mark did not have (a first 5xx,
    # say) reads as its absolute value, and that is the honest answer.
    awk -v start_iso="$START_ISO" -v end_iso="$END_ISO" -v secs="$((END - START))" '
      FNR == NR { v = $NF; sub(/ [^ ]+$/, "", $0); base[$0] = v; next }
      {
        v = $NF; key = $0; sub(/ [^ ]+$/, "", key)
        d = v - (key in base ? base[key] : 0)
        val[key] = d
      }
      function g(key,   v) { v = val[key] + 0; return v }
      function gib(bytes) { return bytes / 1073741824 }
      function mib(bytes) { return bytes / 1048576 }
      END {
        # There is no unlabelled total: the three sources are the ledger.
        deliv = g("cache_body_bytes_total{source=\"disk\"}") + g("cache_body_bytes_total{source=\"stage\"}") + g("cache_body_bytes_total{source=\"upstream\"}")
        opens = g("backend_call_duration_seconds_count{op=\"open\"}")
        stats = g("backend_call_duration_seconds_count{op=\"stat\"}")
        att = g("cache_session_reader_total{result=\"attached\"}")
        alone = g("cache_session_reader_total{result=\"standalone\"}")
        readers = att + alone

        printf "origin merge account  window %s -> %s (%d s)\n", start_iso, end_iso, secs
        printf "  delivered to viewers   %.3f GiB   (stage %.3f, upstream %.3f, disk %.4f)\n", \
          gib(deliv), gib(g("cache_body_bytes_total{source=\"stage\"}")), \
          gib(g("cache_body_bytes_total{source=\"upstream\"}")), gib(g("cache_body_bytes_total{source=\"disk\"}"))
        printf "  answers by source      stage %d, upstream %d, disk %d\n", \
          g("cache_serve_source_total{source=\"stage\"}"), \
          g("cache_serve_source_total{source=\"upstream\"}"), g("cache_serve_source_total{source=\"disk\"}")
        printf "  upstream calls         open %d, stat %d\n", opens, stats
        if (gib(deliv) > 0)
          printf "  per GiB delivered      open %.3f, stat %.3f, open+stat %.3f   <- how the provider sees it\n", \
            opens / gib(deliv), stats / gib(deliv), (opens + stats) / gib(deliv)
        if (opens > 0)
          printf "  per open               %.2f MiB delivered   <- what one window bought its readers\n", \
            mib(deliv) / opens
        if (gib(deliv) > 0)
          printf "  served by             %.1f%% windowed (stage), %.1f%% open-ended passthrough (upstream), %.2f%% disk\n", \
            100 * g("cache_body_bytes_total{source=\"stage\"}") / deliv, \
            100 * g("cache_body_bytes_total{source=\"upstream\"}") / deliv, \
            100 * g("cache_body_bytes_total{source=\"disk\"}") / deliv
        printf "  readers                attached %d, standalone %d", att, alone
        if (readers > 0) printf "  -> attached share %.1f%%", 100 * att / readers
        printf "\n"
        printf "  runs                   sealed %d, chained %d, open_failed %d\n", \
          g("cache_session_total{outcome=\"sealed\"}"), g("cache_session_total{outcome=\"chained\"}"), \
          g("cache_session_total{outcome=\"open_failed\"}")
        printf "  opens per sealed run   %.3f\n", (g("cache_session_total{outcome=\"sealed\"}") > 0 ? opens / g("cache_session_total{outcome=\"sealed\"}") : 0)
        printf "  front statuses        "
        n = 0
        for (k in val) if (k ~ /^front_requests_total/ && val[k] != 0) {
          s = k
          sub(/^front_requests_total\{/, "", s)
          gsub(/method=|proto=|status=|"|\{|\}/, "", s)
          gsub(/,/, " ", s)
          n++
          printf " %s=%d;", s, val[k]
        }
        if (n == 0) printf " (none in this window)"
        printf "\n"
      }' "$MARK" "$NOW"

    # The front-access window: the edge's asks, and only those (a viewer's
    # requests never reach this log). ANSI must be stripped first or every
    # field grep comes back empty (pitfall 61).
    LOG=$(mktemp)
    trap 'rm -f "$LOG"' EXIT
    journalctl -u "$UNIT" --since "@$START" --no-pager 2>/dev/null \
      | sed 's/\x1b\[[0-9;]*m//g' | grep 'front access' > "$LOG" || true
    awk '
      {
        for (i = 1; i <= NF; i++) {
          if ($i ~ /^bytes=/)             { split($i, b, "="); n = b[2] + 0 }
          else if ($i ~ /^duration_ms=/)  { split($i, d, "="); ms = d[2] + 0 }
          else if ($i ~ /^xff=/)          { x = $i }
        }
        reqs++; bytes += n; dsum += ms; if (ms > dmax) dmax = ms
        if (x == "xff=-") { direct++; dbytes += n } else { edge++; ebytes += n; clients[x] = 1 }
      }
      END {
        printf "origin front window    %d lines, %.3f GiB, mean %d ms, max %d ms\n", \
          reqs, bytes / 1073741824, (reqs ? dsum / reqs : 0), dmax
        printf "  from the edge          %d lines, %.3f GiB, %d distinct clients (xff)\n", \
          edge, ebytes / 1073741824, length(clients)
        printf "  direct to the node     %d lines, %.3f GiB   (loopback probes: accept.sh, cold-load)\n", \
          direct, dbytes / 1073741824
      }' "$LOG"
    echo "  viewer side            from the harness on the machine that ran it: requests / bytes (its report)"
    echo "  (per-key fills and latency percentiles: bash fill-account.sh <minutes> <key-substring>)"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "merge-account: unknown mode '$MODE'" >&2
    usage >&2
    exit 2
    ;;
esac

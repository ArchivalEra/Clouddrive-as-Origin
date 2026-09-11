#!/usr/bin/env bash
# Latency attribution report from the origin-cache /metrics endpoint
# (wayfinder map #47, ticket T4).
#
# Scrapes the prometheus endpoint twice (start + end), computes the delta
# for each latency histogram, and prints a percentile table per segment
# plus a bottleneck verdict. Histogram percentiles are interpolated from
# the cumulative _bucket counts (linear within the matching bucket) —
# prometheus client histograms do not carry exact quantiles.
#
# Usage:
#   deploy/metrics-report.sh [metrics_url] [sample_seconds]
# Defaults: http://127.0.0.1:9090/metrics  30
#
# Segments reported:
#   front_request_duration   total time front spent on the request
#   front_upstream_connect   request start -> upstream connection up
#   front_upstream_ttfb      request start -> upstream response header
#   backend_call_duration    time inside one upstream backend call (op=)
#   cache_serve_duration     time inside the cache serve path (outcome=)
set -u

URL="${1:-http://127.0.0.1:9090/metrics}"
SECS="${2:-30}"

scrape() {
  # --noproxy: this runs on the node where a stray ALL_PROXY would
  # otherwise hijack the loopback request (learned the hard way).
  curl -s --noproxy '*' -m 10 "$URL"
}

echo "== metrics-report: $URL =="
A=$(scrape) || { echo "FAIL: cannot scrape $URL"; exit 1; }
[ -n "$A" ] || { echo "FAIL: empty scrape (is the endpoint up?)"; exit 1; }
echo "sampling ${SECS}s ..."
sleep "$SECS"
B=$(scrape) || { echo "FAIL: second scrape failed"; exit 1; }

printf '%s\n' "$A" > /tmp/.metrics-report-a
printf '%s\n' "$B" > /tmp/.metrics-report-b

python3 - /tmp/.metrics-report-a /tmp/.metrics-report-b <<'PY'
import re, sys
from collections import defaultdict

def load(path):
    """-> {(metric, labels): value} for _bucket/_sum/_count lines."""
    out = {}
    for line in open(path):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        m = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+([0-9.eE+-]+)$', line)
        if not m:
            continue
        name, _, labels, val = m.group(1), m.group(2), m.group(3), m.group(4)
        out[(name, labels or '')] = float(val)
    return out

A, B = load(sys.argv[1]), load(sys.argv[2])

# Group histogram series by (base_metric, label-set-minus-le) so we can
# diff counts and reconstruct percentiles.
def histograms(d):
    h = defaultdict(dict)   # base -> {labelstr: {'buckets': {le: count}, 'sum':, 'count':}}
    for (name, labels), val in d.items():
        m = re.match(r'^(.+)_(bucket|sum|count)$', name)
        if not m:
            continue
        base, kind = m.group(1), m.group(2)
        # strip the le="..." component from the label string for grouping
        key = re.sub(r'le="[^"]*",?', '', labels).strip(',')
        key = re.sub(r',\s*$', '', key)
        if kind == 'bucket':
            le = re.search(r'le="([^"]*)"', labels)
            if not le:
                continue
            h[base].setdefault(key, {'buckets': {}, 'sum': 0.0, 'count': 0.0})
            h[base][key]['buckets'][le.group(1)] = val
        elif kind == 'sum':
            h[base].setdefault(key, {'buckets': {}, 'sum': 0.0, 'count': 0.0})
            h[base][key]['sum'] = val
        elif kind == 'count':
            h[base].setdefault(key, {'buckets': {}, 'sum': 0.0, 'count': 0.0})
            h[base][key]['count'] = val
    return h

HA, HB = histograms(A), histograms(B)

def percentile(buckets, total, q):
    """Interpolate q-th percentile from cumulative bucket counts."""
    if total <= 0:
        return None
    target = q * total
    prev_le, prev_c = 0.0, 0.0
    for le_s in sorted(buckets, key=lambda s: float('inf') if s == '+Inf' else float(s)):
        le = float('inf') if le_s == '+Inf' else float(le_s)
        c = buckets[le_s]
        if c >= target:
            if le == float('inf'):
                return prev_le  # falls in the +Inf tail; report last bound
            if c == prev_c:
                return le
            frac = (target - prev_c) / (c - prev_c)
            return prev_le + frac * (le - prev_le)
        prev_le, prev_c = le, c
    return None

def fmt(v):
    if v is None:
        return "   -  "
    if v >= 1.0:
        return f"{v:6.2f}s"
    if v >= 0.001:
        return f"{v*1000:6.1f}ms"
    return f"{v*1e6:6.0f}us"

# Metric label sets we care about, in attribution order.
WANT = [
    ("front_request_duration_seconds", "front: request total"),
    ("front_upstream_ttfb_seconds", "front: upstream TTFB"),
    ("front_upstream_connect_seconds", "front: upstream connect"),
    ("backend_call_duration_seconds", "backend: call (op=)"),
    ("cache_serve_duration_seconds", "cache: serve (outcome=)"),
]

print()
print(f"{'segment':<28} {'P50':>8} {'P95':>8} {'P99':>8} {'n':>6} {'mean':>8}")
print("-" * 72)

summary = {}
for base, desc in WANT:
    if base not in HB:
        continue
    for key in sorted(HB[base]):
        nb = HB[base][key]
        na = HA.get(base, {}).get(key, {'buckets': {}, 'sum': 0.0, 'count': 0.0})
        # delta
        count = nb['count'] - na['count']
        if count <= 0:
            continue
        buckets = {}
        for le, c in nb['buckets'].items():
            ac = na['buckets'].get(le, 0.0)
            buckets[le] = c - ac
        sumd = nb['sum'] - na['sum']
        label = f"{desc} {key}" if key else desc
        p50 = percentile(buckets, count, 0.50)
        p95 = percentile(buckets, count, 0.95)
        p99 = percentile(buckets, count, 0.99)
        mean = sumd / count
        summary[(base, key)] = (p50, p95, p99, mean, count)
        print(f"{label[:28]:<28} {fmt(p50):>8} {fmt(p95):>8} {fmt(p99):>8} {int(count):>6} {fmt(mean):>8}")

print()
print("== bottleneck verdict ==")
def get(base, key_sub=None):
    for (b, k), v in summary.items():
        if b == base and (key_sub is None or key_sub in k):
            return v
    return None

ttfb = get("front_upstream_ttfb_seconds")
conn = get("front_upstream_connect_seconds")
req = get("front_request_duration_seconds")
bstat = get("backend_call_duration_seconds", 'op="stat"')
bopen = get("backend_call_duration_seconds", 'op="open"')
serve = get("cache_serve_duration_seconds")

if ttfb and conn:
    print(f"  front connect P50 {fmt(conn[0]).strip()}  ->  upstream TTFB P50 {fmt(ttfb[0]).strip()}")
# backend may show stat only (all-hit window) or open too (cold window)
backend = bopen or bstat
bname = "open" if bopen else ("stat" if bstat else None)
if backend:
    print(f"  backend {bname} P50 {fmt(backend[0]).strip()}  (n={int(backend[4])})")

if ttfb is None:
    print("  (no front TTFB samples — widen the window or drive traffic)")
elif backend is None:
    print("  VERDICT: no backend calls in window (all cache hits) -> origin served locally")
    if req and req[0] is not None and req[0] < 0.05:
        print("           front total is sub-50ms; any client slowness is OUTSIDE the origin")
elif backend[4] < 5:
    # Too few backend samples to compare against the TTFB distribution:
    # a 1-sample backend call cannot be compared with a 15-sample TTFB
    # median (the backend call is a subset of TTFB requests). Report the
    # facts, withhold the verdict.
    print(f"  backend {bname} n={int(backend[4])} < 5 — too few to compare against TTFB (n={int(ttfb[4])})")
    print("  (drive more cold traffic, or read the table directly)")
elif backend[0] > 0.5 * (ttfb[0] or 1e-9):
    print(f"  VERDICT: upstream {bname} dominates TTFB -> cloud-drive backend is the bottleneck")
elif ttfb[0] > 1.0 and backend[0] < 0.2 * ttfb[0]:
    print("  VERDICT: TTFB large but backend call small -> front/loopback or downstream wait")
elif req and req[0] is not None and req[0] < 0.05 and ttfb[0] < 0.05:
    print("  VERDICT: origin path all sub-50ms -> any client slowness is OUTSIDE the origin")
    print("           (client<->EdgeOne edge segment; compare with client-side timing)")
else:
    print("  VERDICT: no single dominant segment in this window (see table)")
PY

rm -f /tmp/.metrics-report-a /tmp/.metrics-report-b

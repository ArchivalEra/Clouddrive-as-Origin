#!/usr/bin/env bash
# EdgeOne live regression (deploy/lab/run-edgeone.sh).
#
# Exercises the full Clouddrive-as-Origin feature surface through the
# production EdgeOne domain (cdn-oracle.isui.ren). Run from a machine
# with internet access; the domain must be live.
#
# Usage:  bash deploy/lab/run-edgeone.sh
# Exit 0 = all PASS, 1 = any FAIL.
set -u

BASE="https://cdn-oracle.isui.ren"
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
note() { echo "---- $1"; }

note "1. GET static asset (cached)"
code=$(curl -s -m 20 -o /dev/null -w "%{http_code}" "$BASE/googledrive1/IMG_20260904_003601.png")
[ "$code" = 200 ] && ok "GET 200" || bad "GET $code"

note "2. HEAD (headers only)"
code=$(curl -s -m 20 -I -o /dev/null -w "%{http_code}" "$BASE/googledrive1/IMG_20260904_003601.png")
[ "$code" = 200 ] && ok "HEAD 200" || bad "HEAD $code"

note "3. Range 206 + Content-Range"
out=$(curl -s -m 20 -D - -o /dev/null -H "Range: bytes=0-1023" "$BASE/googledrive1/IMG_20260904_003601.png")
echo "$out" | grep -q "206" && echo "$out" | grep -q "content-range: bytes 0-1023/1907082" \
  && ok "206 + exact Content-Range" || bad "range: $(echo "$out" | head -1)"

note "4. ListObjectsV2 (S3 API)"
body=$(curl -s -m 20 "$BASE/?list-type=2&max-keys=5")
echo "$body" | grep -q "<ListBucketResult" && echo "$body" | grep -q "<KeyCount>" \
  && ok "list XML" || bad "list: $(echo "$body" | head -c 100)"

note "5. healthz"
body=$(curl -s -m 20 "$BASE/_internal/healthz")
echo "$body" | grep -q '"status":"ok"' && ok "healthz ok" || bad "healthz: $body"

note "6. SigV4 signed GET (Authorization forwarded)"
# Sign with the oracle env credentials (must match SIGV4_* on the node).
AK="${SIGV4_AK:-AKLABTESTKEY1234}"
SK="${SIGV4_SK:-origin-lab-secret-9f2c}"
lines=$(python3 - "$SK" "$BASE/googledrive1/IMG_20260904_003601.png" "$AK" <<'PY' 2>/dev/null
import hashlib, hmac, sys, datetime
SECRET = sys.argv[1]; URL = sys.argv[2]; AK = sys.argv[3]
u = URL.split("://",1)[1]; host, path = u.split("/",1); path = "/"+path
region, service = "us-east-1", "s3"
now = datetime.datetime.utcnow()
amzdate = now.strftime("%Y%m%dT%H%M%SZ"); datestamp = now.strftime("%Y%m%d")
signed = "host;x-amz-content-sha256;x-amz-date"; payload = "UNSIGNED-PAYLOAD"
ch = f"host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{amzdate}\n"
canonical = f"GET\n{path}\n\n{ch}\n{signed}\n{payload}"
scope = f"{datestamp}/{region}/{service}/aws4_request"
sts = f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
def hs(k,d): return hmac.new(k, d.encode(), hashlib.sha256).digest()
k = hs(hs(hs(hs(("AWS4"+SECRET).encode(), datestamp), region), service), "aws4_request")
sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
print(f"Authorization: AWS4-HMAC-SHA256 Credential={AK}/{scope}, SignedHeaders={signed}, Signature={sig}")
print(amzdate)
PY
)
AUTH=$(echo "$lines" | head -1); DATE=$(echo "$lines" | tail -1)
code=$(curl -s -m 20 -o /dev/null -w "%{http_code}" -H "$AUTH" -H "x-amz-date: $DATE" \
  -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" "$BASE/googledrive1/IMG_20260904_003601.png")
[ "$code" = 200 ] && ok "sigv4 signed 200" || bad "sigv4 signed $code"

note "7. Bad signature -> 403 no-store (cold URL)"
# A warm URL would be served by the edge cache (200 HIT, auth is
# origin-side) — use a never-cached key so the request reaches the
# origin and the signature gate fires.
COLD="googledrive1/never-cached-$(date +%s).bin"
hdrs=$(curl -s -m 20 -D - -o /dev/null \
  -H "Authorization: AWS4-HMAC-SHA256 Credential=$AK/20260909/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000" \
  -H "x-amz-date: 20260909T000000Z" -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
  "$BASE/$COLD")
# Skip the proxy's "Connection established" line; find the real status.
status=$(echo "$hdrs" | grep -oE "HTTP/[0-9.]+ [0-9]{3}" | tail -1 | grep -oE "[0-9]{3}$")
[ "$status" = "403" ] && echo "$hdrs" | grep -qi "cache-control: no-store" \
  && ok "bad sig 403 no-store" || bad "bad sig: status=$status"

echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
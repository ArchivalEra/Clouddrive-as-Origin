#!/usr/bin/env bash
# EdgeOne live regression (deploy/lab/run-edgeone.sh).
#
# Exercises the full Clouddrive-as-Origin feature surface through a
# production EdgeOne domain. Run from a machine with internet access; the
# domain must be live.
#
# Usage:  BASE=https://your-cdn-host bash deploy/lab/run-edgeone.sh
# Exit 0 = all PASS, 1 = any FAIL.
set -u

BASE=${BASE:?set BASE to the CDN base URL, e.g. https://your-cdn-host}
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

note "5. internal endpoints are not public"
# healthz is refused at the front by design (D2/#54): it discloses upstream
# topology and is meant to be read on the business plane's loopback port.
code=$(curl -s -m 20 -o /dev/null -w "%{http_code}" "$BASE/_internal/healthz")
[ "$code" = 404 ] && ok "healthz refused on the public path (404)" || bad "healthz: expected 404, got $code"
# prewarm IS public but must require its token.
code=$(curl -s -m 20 -o /dev/null -w "%{http_code}" -X POST "$BASE/_internal/prewarm/googledrive1/test-page.html")
[ "$code" = 401 ] && ok "prewarm requires its token (401)" || bad "prewarm: expected 401, got $code"

note "6. SigV4 signed GET (Authorization forwarded)"
# Sign with the oracle env credentials (must match SIGV4_* on the node).
AK="${SIGV4_AK:-AKLABTESTKEY1234}"
SK="${SIGV4_SK:-origin-lab-secret-9f2c}"
lines=$(python3 "$REPO/deploy/oracle/presign.py" --header-auth \
  --host "${BASE#*://}" --key googledrive1/IMG_20260904_003601.png \
  --id "$AK" --secret "$SK" 2>/dev/null)
AUTH=$(printf '%s\n' "$lines" | grep -i '^authorization:')
DATE=$(printf '%s\n' "$lines" | grep -i '^x-amz-date:' | sed 's/^[^:]*: //')
SHA=$(printf '%s\n' "$lines" | grep -i '^x-amz-content-sha256:' | sed 's/^[^:]*: //')
code=$(curl -s -m 20 -o /dev/null -w "%{http_code}" -H "$AUTH" -H "x-amz-date: $DATE" \
  -H "x-amz-content-sha256: $SHA" "$BASE/googledrive1/IMG_20260904_003601.png")
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
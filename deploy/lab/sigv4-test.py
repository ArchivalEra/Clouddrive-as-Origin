#!/usr/bin/env python3
"""Minimal SigV4 header signer for testing the origin's optional verifier."""
import hashlib, hmac, sys, datetime

SECRET = sys.argv[1]          # secret key
URL    = sys.argv[2]          # https://host/path
AK     = sys.argv[3]           # access key id

# parse
u = URL.split("://",1)[1]
host, path = u.split("/",1)
path = "/" + path
region, service = "us-east-1", "s3"
now = datetime.datetime.utcnow()
amzdate = now.strftime("%Y%m%dT%H%M%SZ")
datestamp = now.strftime("%Y%m%d")

# canonical request (signed headers: host;x-amz-content-sha256;x-amz-date)
signed = "host;x-amz-content-sha256;x-amz-date"
payload = "UNSIGNED-PAYLOAD"
ch = f"host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{amzdate}\n"
canonical = f"GET\n{path}\n\n{ch}\n{signed}\n{payload}"
scope = f"{datestamp}/{region}/{service}/aws4_request"
sts = f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"

def hs(k, d): return hmac.new(k, d.encode(), hashlib.sha256).digest()
k = hs(hs(hs(hs(("AWS4"+SECRET).encode(), datestamp), region), service), "aws4_request")
sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
print(f"Authorization: AWS4-HMAC-SHA256 Credential={AK}/{scope}, SignedHeaders={signed}, Signature={sig}")
print(f"x-amz-date: {amzdate}")
print(f"x-amz-content-sha256: {payload}")

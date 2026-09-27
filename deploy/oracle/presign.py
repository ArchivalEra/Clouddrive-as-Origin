#!/usr/bin/env python3
"""Presign one object URL the way an S3 SDK would (SigV4, query auth).

A presigned URL IS the ticket: the player, curl or a browser needs nothing
but the URL itself, and it stops working when `--expires` passes. The origin
(ADR-0027) refuses unsigned content reads and charges the per-session
budgets keyed on the `session` parameter, which is signed like every other
query parameter -- so only whoever holds the secret could have bound this
URL to that session.

    presign.py --host cdn.example --key googledrive1/film.mkv \
               --session viewer-17 [--expires 3600] \
               [--id AKID --secret SECRET] [--scheme https]

The host must be the hostname the CLIENT will request -- a presigned URL
signs the Host header, and the edge forwards it to the origin (spec §6), so
sign the CDN domain when the URL is served through the CDN. Credentials come
from --id/--secret or the SIGV4_ACCESS_KEY_ID / SIGV4_SECRET_ACCESS_KEY env
pair (the same pair the origin's credential store reads).

Stdlib only. Exit 0 prints the URL; exit 2 means the inputs were wrong.
"""

import argparse
import hashlib
import hmac
import os
import sys
import urllib.parse
from datetime import datetime, timezone

# AWS SigV4 unreserved characters. The path keeps its slashes; query values
# encode everything outside this set.
UNRESERVED = "-._~"


def uri_encode(value: str, keep_slash: bool) -> str:
    safe = UNRESERVED + ("/" if keep_slash else "")
    return urllib.parse.quote(value, safe=safe)


def canonical_query(pairs):
    # Sort by (name, value) on the raw pairs, then encode -- the same order
    # the origin's verifier applies, so both sides agree without either
    # depending on AWS's tie-breaking corner cases.
    ordered = sorted(pairs, key=lambda kv: (kv[0], kv[1]))
    return "&".join(f"{uri_encode(k, False)}={uri_encode(v, False)}" for k, v in ordered)


def main() -> int:
    ap = argparse.ArgumentParser(description="presign one object URL (SigV4 query auth)")
    ap.add_argument("--host", required=True, help="hostname the client will request (the signature covers Host)")
    ap.add_argument("--key", required=True, help="object key, e.g. googledrive1/film.mkv")
    ap.add_argument("--session", default="", help="session marker the origin's budgets charge (recommended)")
    ap.add_argument("--expires", type=int, default=3600, help="URL lifetime seconds (default 3600)")
    ap.add_argument("--id", default=os.environ.get("SIGV4_ACCESS_KEY_ID", ""))
    ap.add_argument("--secret", default=os.environ.get("SIGV4_SECRET_ACCESS_KEY", ""))
    ap.add_argument("--scheme", default="https")
    ap.add_argument("--date", default="", help="override X-Amz-Date (YYYYmmddTHHMMSSZ), for reproducible vectors")
    args = ap.parse_args()

    if not args.id or not args.secret:
        print("presign: no credentials -- pass --id/--secret or export SIGV4_ACCESS_KEY_ID/SECRET", file=sys.stderr)
        return 2
    if not args.key.startswith("/"):
        args.key = "/" + args.key

    now = (
        datetime.strptime(args.date, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        if args.date
        else datetime.now(timezone.utc)
    )
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    scope_date = amz_date[:8]
    region, service = "us-east-1", "s3"

    pairs = [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", f"{args.id}/{scope_date}/{region}/{service}/aws4_request"),
        ("X-Amz-Date", amz_date),
        ("X-Amz-Expires", str(args.expires)),
        ("X-Amz-SignedHeaders", "host"),
    ]
    if args.session:
        pairs.append(("session", args.session))

    canonical_request = "\n".join([
        "GET",
        uri_encode(args.key, True),
        canonical_query(pairs),
        f"host:{args.host}",
        "",
        "host",
        "UNSIGNED-PAYLOAD",
    ])
    scope = f"{scope_date}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        scope,
        hashlib.sha256(canonical_request.encode()).hexdigest(),
    ])

    def h(key: bytes, data: bytes) -> bytes:
        return hmac.new(key, data, hashlib.sha256).digest()

    signing_key = h(h(h(h(("AWS4" + args.secret).encode(), scope_date.encode()), region.encode()), service.encode()), b"aws4_request")
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()

    query = canonical_query(pairs) + f"&X-Amz-Signature={signature}"
    print(f"{args.scheme}://{args.host}{uri_encode(args.key, True)}?{query}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

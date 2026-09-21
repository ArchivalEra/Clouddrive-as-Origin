#!/usr/bin/env python3
"""Serve one file while ADVERTISING a different total length.

The question this answers: a player that never starts on a huge object — is it
the range SHAPE it is answered with, or the SIZE it is told the object has?

Both are visible in the same response, so a probe that changes only the real
server cannot separate them. This one serves the same bytes either way and
still reports whatever `Content-Range: bytes a-b/TOTAL` we choose, which is the
one variable under test. It also serves a same-origin page, because a browser
will not hand a media element a cross-origin URL without CORS headers.

Usage:
  fake-total-server.py --file <path> [--total N] [--port 7801]
    --total 0 (default) advertises the file's real size; any other value
    advertises that size in every Content-Range.
"""
import argparse
import http.server
import re

PAGE = b"<!doctype html><title>fake-total</title>ok\n"

ap = argparse.ArgumentParser()
ap.add_argument("--file", required=True)
ap.add_argument("--total", type=int, default=0)
ap.add_argument("--port", type=int, default=7801)
args = ap.parse_args()

with open(args.file, "rb") as fh:
    BLOB = fh.read()
TOTAL = args.total or len(BLOB)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *a):
        print("%s %s" % (self.command, self.path), flush=True)

    def do_HEAD(self):
        self.serve(head=True)

    def do_GET(self):
        self.serve(head=False)

    def serve(self, head):
        if self.path.endswith(".html"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(PAGE)))
            self.end_headers()
            if not head:
                self.wfile.write(PAGE)
            return
        rng = self.headers.get("Range")
        start, end = 0, len(BLOB) - 1
        m = re.match(r"bytes=(\d+)-(\d*)", rng or "")
        if m:
            start = int(m.group(1))
            if m.group(2):
                end = min(int(m.group(2)), len(BLOB) - 1)
        body = BLOB[start:end + 1]
        self.send_response(206 if rng else 200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Accept-Ranges", "bytes")
        if rng:
            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, start + len(body) - 1, TOTAL))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head:
            self.wfile.write(body)


print("serving %s (%d bytes) as total=%d on :%d" % (args.file, len(BLOB), TOTAL, args.port), flush=True)
http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()

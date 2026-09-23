#!/usr/bin/env python3
"""A fixture origin for testing the viewer harnesses themselves.

Serves one small page and one ranged object whose bytes are a fixed pattern,
and — when asked — drops every Nth object response after a prefix, the way a
flaky leg kills a fetch, or stalls it forever after the prefix, the way a flaky
leg hangs one. It is the counterweight to `--retries`: with `--retries 0` a
viewer must fail on the dropped fetch, with `--retries N` it must finish with
the same bytes and the same checksum as a clean run; and with a stall in the
mix, only `--attempt-timeout-secs` gives the retry something to act on.

  python3 flaky-server.py [port] [abort-every] [stall-every]

`abort-every=0` / `stall-every=0` (the defaults) never do either: the clean
reference run.
"""
import re
import socket
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIZE = 4 * 1024 * 1024
# A deterministic, non-repeating byte pattern, so a resumed read that lands on
# the wrong offset produces a different checksum rather than a plausible one.
BODY = bytes(((i * 2654435761) >> 8) & 0xFF for i in range(SIZE))
PAGE = b"<!doctype html><title>fixture</title>hello\n"

ABORT_EVERY = 0
STALL_EVERY = 0
SEEN = 0


class Fixture(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        global SEEN
        path = self.path.split("?")[0]
        if path == "/media/hello.txt":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(PAGE)))
            self.end_headers()
            self.wfile.write(PAGE)
            return
        if path != "/media/viewer-object.bin":
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        SEEN += 1
        start, end = 0, SIZE - 1
        m = re.match(r"bytes=(\d+)-(\d*)", self.headers.get("Range") or "")
        if m:
            start = int(m.group(1))
            if m.group(2):
                end = min(int(m.group(2)), SIZE - 1)
        chunk = BODY[start : end + 1]
        self.send_response(206)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Range", f"bytes {start}-{end}/{SIZE}")
        self.send_header("Content-Length", str(len(chunk)))
        self.end_headers()
        if ABORT_EVERY and SEEN % ABORT_EVERY == 0 and len(chunk) > 65536:
            self.wfile.write(chunk[:65536])
            self.wfile.flush()
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            return
        if STALL_EVERY and SEEN % STALL_EVERY == 0 and len(chunk) > 65536:
            # Hold the connection open with the body unfinished: no rejection
            # will ever arrive, so only a client-side attempt timeout can turn
            # this into a retry.
            self.wfile.write(chunk[:65536])
            self.wfile.flush()
            time.sleep(600)
            return
        self.wfile.write(chunk)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7899
    ABORT_EVERY = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    STALL_EVERY = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    print(
        f"fixture on http://127.0.0.1:{port} size={SIZE} abort_every={ABORT_EVERY} stall_every={STALL_EVERY}",
        flush=True,
    )
    ThreadingHTTPServer(("127.0.0.1", port), Fixture).serve_forever()

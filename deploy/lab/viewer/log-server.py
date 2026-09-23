#!/usr/bin/env python3
"""A static file server that logs every request the way the origin would.

The point is the instrument, not the serving: it records the Range header, the
bytes actually written and the wall-clock interval, one line per request, so
concurrency (overlapping intervals) and request shape can be read off directly.
The browser's own view is not enough here: hls.js loads fragments inside a Web
Worker, and those fetches do not appear in the page's resource timings or in
playwright's request events.

  python3 log-server.py [port] [dir] [rate-bytes-per-sec]

A third argument throttles every response to that rate, which is how the thin
leg is imitated locally: at 900 KB/s a 6-second segment of a 2.5 Mbps stream
takes ~2.2 s, and it is slow enough to see whether the player answers with
parallel loads or keeps fetching one segment at a time.
"""
import os
import re
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = "."
RATE = 0.0
T0 = time.monotonic()


class Logging(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        path = self.path.split("?")[0].lstrip("/")
        full = os.path.join(ROOT, path)
        if not os.path.isfile(full):
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        size = os.path.getsize(full)
        start, end = 0, size - 1
        m = re.match(r"bytes=(\d+)-(\d*)", self.headers.get("Range") or "")
        if m:
            start = int(m.group(1))
            if m.group(2):
                end = min(int(m.group(2)), size - 1)
        length = max(end - start + 1, 0)
        status = 206 if m else 200
        types = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript",
            ".m3u8": "application/vnd.apple.mpegurl",
            ".mpd": "application/dash+xml",
            ".mp4": "video/mp4",
            ".m4s": "video/mp4",
        }
        ext = os.path.splitext(path)[1]
        self.send_response(status)
        self.send_header("Content-Type", types.get(ext, "application/octet-stream"))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(length))
        self.end_headers()
        began = time.monotonic()
        sent = 0
        with open(full, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(262144, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                sent += len(chunk)
                remaining -= len(chunk)
                if RATE > 0:
                    # Pace the writer: after sending `sent` bytes the response
                    # should have taken sent/RATE seconds in total.
                    owed = sent / RATE - (time.monotonic() - began)
                    if owed > 0:
                        time.sleep(owed)
        print(
            f"{time.monotonic() - T0:8.3f}  {path}  {self.headers.get('Range') or '-'}  "
            f"sent={sent}  dur={time.monotonic() - began:.3f}s  status={status}",
            flush=True,
        )


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7900
    ROOT = sys.argv[2] if len(sys.argv) > 2 else "."
    RATE = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
    print(f"log-server on http://127.0.0.1:{port} root={ROOT} rate={RATE or 'unlimited'}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), Logging).serve_forever()

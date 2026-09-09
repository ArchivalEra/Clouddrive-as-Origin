#!/usr/bin/env python3
"""Port-80 edge helper for Clouddrive-as-Origin (oracle node).

EdgeOne http origin-pull and Let's Encrypt http-01 both arrive here:
  /.well-known/acme-challenge/<token>  -> served from WEBROOT if present
  anything else                        -> 301 to the same https URL
Dual-stack: binds [::] (IPv6 + IPv4-mapped on Linux default). Run as
root (privileged port) or give CAP_NET_BIND_SERVICE.
"""
import http.server
import os
import socket
import threading

WEBROOT = "/opt/origin-cache/acme-webroot"
LOG = "/opt/origin-cache/port80.log"
BIND = "::"
PORT = 80


class AcmeOrRedirect(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path.startswith("/.well-known/acme-challenge/"):
            fp = os.path.join(WEBROOT, os.path.basename(self.path))
            if os.path.isfile(fp):
                with open(fp, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
        host = self.headers.get("Host", "")
        self.send_response(301)
        self.send_header("Location", f"https://{host}{self.path}")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, fmt, *args):
        try:
            with open(LOG, "a") as f:
                f.write("%s %s\n" % (self.address_string(), fmt % args))
        except OSError:
            pass


class DualStack(http.server.ThreadingHTTPServer):
    address_family = socket.AF_INET6


if __name__ == "__main__":
    os.makedirs(WEBROOT, exist_ok=True)
    srv = DualStack((BIND, PORT), AcmeOrRedirect)
    srv.serve_forever()
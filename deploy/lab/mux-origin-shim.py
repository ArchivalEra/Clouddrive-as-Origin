#!/usr/bin/env python3
"""Bridge the h11-mux harness's origin to OUR origin.

Why: `mux-twohost` (in the neighbouring `cloudflare-related` repo) boots a front
that bridges every mux stream to `ORIGIN_ADDR` as opaque bytes, and its raw-mode
gates ask for `/corpus/<n>` — paths our origin does not have. This shim answers
those paths from OUR origin instead: it rewrites the request line to our key and
adds a `Range` for the byte count the path names (10m -> 10 MiB, 1k -> 1 KiB,
...), then splices the exchange. Their gates assert byte counts and timing, not
content, so the run proves *our* bytes were carried; the response headers pass
through untouched, so their HTTP client sees a normal 206.

Usage:
  mux-origin-shim.py --listen 127.0.0.1:19090 --upstream 127.0.0.1:18080 \
                     --key googledrive1/<percent-encoded-name>
  (upstream is normally an ssh -L tunnel to the origin node's business plane)
"""
import argparse
import random
import re
import socket
import threading

SIZES = {
    "/corpus/1k": 1024,
    "/corpus/8k": 8 * 1024,
    "/corpus/64k": 64 * 1024,
    "/corpus/100k": 100 * 1024,
    "/corpus/1m": 1024 * 1024,
    "/corpus/10m": 10 * 1024 * 1024,
}
# Kept away from the object's last byte so a range never runs past the end.
SPAN = 214748364800


def splice(a: socket.socket, b: socket.socket) -> None:
    try:
        while True:
            data = a.recv(65536)
            if not data:
                break
            b.sendall(data)
    except OSError:
        pass
    finally:
        try:
            b.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def handle(client: socket.socket, upstream: str, key: str) -> None:
    try:
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = client.recv(4096)
            if not chunk:
                return
            head += chunk
        line = head.split(b"\r\n", 1)[0].decode("latin1")
        parts = line.split(" ")
        if len(parts) < 2:
            return
        method, path, version = parts[0], parts[1], parts[2] if len(parts) > 2 else "HTTP/1.1"
        size = SIZES.get(path.split("?")[0])
        offset = random.randrange(0, SPAN - max(size or 0, 1) - 1) if size else 0
        rewritten = f"{method} /{key} {version}"
        if size:
            rewritten += f"\r\nRange: bytes={offset}-{offset + size - 1}"
        rest = head.split(b"\r\n", 1)[1] if b"\r\n" in head else b"\r\n"
        out = rewritten.encode() + b"\r\n" + rest
        print(f"  {path} -> /{key} range {offset}-{offset + (size or 0) - 1}", flush=True)

        host, port = upstream.rsplit(":", 1)
        up = socket.create_connection((host, int(port)), timeout=120)
        up.sendall(out)
        t = threading.Thread(target=splice, args=(client, up), daemon=True)
        t.start()
        splice(up, client)
        t.join(timeout=5)
        up.close()
    except OSError as e:
        print(f"  bridge error: {e}", flush=True)
    finally:
        client.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="127.0.0.1:19090")
    ap.add_argument("--upstream", required=True)
    ap.add_argument("--key", required=True)
    args = ap.parse_args()

    # Rewrite the key's slashes back: the request line must carry a real path.
    key = re.sub(r"^/+", "", args.key)
    host, port = args.listen.rsplit(":", 1)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, int(port)))
    srv.listen(64)
    print(f"mux-origin-shim: {args.listen} -> {args.upstream} as /{key}", flush=True)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=handle, args=(conn, args.upstream, key), daemon=True).start()


if __name__ == "__main__":
    main()

"""Local demo server: single-page UI + JSON API matching the AWS routes.

    python -m roomready.server --port 8080

Identity: the X-Demo-User header (local demo only). The clock is a controllable ManualClock;
POST /admin/tick runs the scheduler (optionally fast-forwarding to the next T-24h).
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from . import api
from .domain import ManualClock
from .local_orchestrator import LocalRuntime
from .store import MemoryStore

WEB_DIR = Path(__file__).resolve().parent / "web"
STATIC_FILES = {"/": "index.html", "/index.html": "index.html", "/app.js": "app.js", "/style.css": "style.css"}
DEFAULT_DATA = os.path.join("data", "roomready-local.json")


def make_handler(runtime, verbose=False):
    class Handler(BaseHTTPRequestHandler):
        server_version = "RoomReadyLocal/1.0"

        def log_message(self, fmt, *args):
            if verbose:
                sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

        def _send(self, status, body: bytes, content_type):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _handle(self, method):
            path = urlsplit(self.path).path
            if method == "GET" and path in STATIC_FILES:
                file = WEB_DIR / STATIC_FILES[path]
                ctype = mimetypes.guess_type(file.name)[0] or "application/octet-stream"
                if file.suffix == ".js":
                    ctype = "text/javascript"
                return self._send(200, file.read_bytes(), ctype + "; charset=utf-8")
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode("utf-8") if length else ""
            caller = (self.headers.get("X-Demo-User") or "").strip() or None
            status, payload = api.handle_request(runtime.service, method, path, caller, dict(self.headers.items()),
                                                 body, local=runtime)
            self._send(status, json.dumps(payload, indent=1).encode("utf-8"), "application/json")

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

    return Handler


def make_server(runtime, host="127.0.0.1", port=8080, verbose=False):
    return ThreadingHTTPServer((host, port), make_handler(runtime, verbose))


def main(argv=None):
    parser = argparse.ArgumentParser(description="ROOMREADY local demo server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--data", default=DEFAULT_DATA, help="JSON persistence file (default: %(default)s)")
    parser.add_argument("--memory", action="store_true", help="do not persist (in-memory only)")
    parser.add_argument("--reset", action="store_true", help="delete the data file before starting")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    if args.reset and os.path.exists(args.data):
        os.remove(args.data)
    store = MemoryStore(None if args.memory else args.data)
    runtime = LocalRuntime(store=store, clock=ManualClock())
    server = make_server(runtime, args.host, args.port, args.verbose)
    print(f"ROOMREADY local demo on http://{args.host}:{args.port}/  "
          f"(data: {'memory only' if args.memory else args.data}; Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

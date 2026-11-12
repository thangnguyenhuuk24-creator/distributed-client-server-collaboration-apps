import http.server
import json
import os
import threading
from typing import Optional

from network.central_server import CentralServer


class CentralDashboardServer:
    """Loopback-only HTTP dashboard for a CentralServer instance."""

    def __init__(
        self,
        central_server: CentralServer,
        host: str = "127.0.0.1",
        port: int = 9003,
    ):
        self.central_server = central_server
        self.host = host
        self.port = port
        self.httpd: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        dashboard_dir = os.path.join(os.path.dirname(__file__), "dashboard")

        class RequestHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/api/dashboard":
                    payload = json.dumps(
                        central_server.get_dashboard_snapshot(),
                        separators=(",", ":"),
                    ).encode("utf-8")
                    self._send(200, payload, "application/json; charset=utf-8")
                    return
                if self.path == "/api/logs/export":
                    payload = json.dumps(
                        central_server.get_dashboard_snapshot()["events"],
                        ensure_ascii=False,
                        indent=2,
                    ).encode("utf-8")
                    self._send(
                        200,
                        payload,
                        "application/json; charset=utf-8",
                        "attachment; filename=server-events.json",
                    )
                    return

                assets = {
                    "/": ("index.html", "text/html; charset=utf-8"),
                    "/static/dashboard.css": ("dashboard.css", "text/css; charset=utf-8"),
                    "/static/dashboard.js": ("dashboard.js", "text/javascript; charset=utf-8"),
                }
                asset = assets.get(self.path)
                if not asset:
                    self._send(404, b"Not found", "text/plain; charset=utf-8")
                    return

                filename, content_type = asset
                filepath = os.path.join(dashboard_dir, filename)
                try:
                    with open(filepath, "rb") as file:
                        payload = file.read()
                except OSError:
                    self._send(500, b"Dashboard asset could not be loaded", "text/plain; charset=utf-8")
                    return
                self._send(200, payload, content_type)

            def do_POST(self):
                if self.path == "/api/server/start":
                    try:
                        central_server.start()
                    except OSError as exc:
                        self._send_json(409, {"error": str(exc)})
                        return
                    self._send_json(200, {"success": True})
                    return
                if self.path == "/api/server/stop":
                    central_server.stop()
                    self._send_json(200, {"success": True})
                    return
                if self.path == "/api/logs/clear":
                    central_server.clear_events()
                    self._send_json(200, {"success": True})
                    return
                if self.path == "/api/clients/kick":
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                    except ValueError:
                        self._send_json(400, {"error": "Invalid request body"})
                        return
                    if not 0 < length <= 4096:
                        self._send_json(400, {"error": "Invalid request body"})
                        return
                    try:
                        data = json.loads(self.rfile.read(length).decode("utf-8"))
                        if not isinstance(data, dict):
                            raise ValueError("Body must be a JSON object")
                        session_id = data.get("session_id")
                    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError, ValueError):
                        self._send_json(400, {"error": "Invalid request body"})
                        return
                    if (
                        isinstance(session_id, bool)
                        or not isinstance(session_id, int)
                        or not 1 <= session_id <= 0xFFFFFFFF
                    ):
                        self._send_json(400, {"error": "Invalid session ID"})
                        return
                    if not central_server.kick_client(session_id):
                        self._send_json(404, {"error": "Client is no longer online"})
                        return
                    self._send_json(200, {"success": True})
                    return
                self._send_json(404, {"error": "Not found"})

            def _send(
                self,
                status: int,
                payload: bytes,
                content_type: str,
                content_disposition: Optional[str] = None,
            ):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                if content_disposition:
                    self.send_header("Content-Disposition", content_disposition)
                self.end_headers()
                self.wfile.write(payload)

            def _send_json(self, status: int, data: dict):
                payload = json.dumps(data).encode("utf-8")
                self._send(status, payload, "application/json; charset=utf-8")

            def log_message(self, format, *args):
                return

        central_server = self.central_server
        self.httpd = http.server.ThreadingHTTPServer(
            (self.host, self.port),
            RequestHandler,
        )
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self._thread = threading.Thread(
            target=self.httpd.serve_forever,
            name="central-dashboard-http",
            daemon=True,
        )
        self._thread.start()

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)

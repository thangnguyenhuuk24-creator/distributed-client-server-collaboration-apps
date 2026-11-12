import json
import socket
import threading
import time
from typing import Optional

from core.state import ClientState


class CentralClientConnection:
    """Persistent TCP login/presence connection to a self-hosted LAN server."""

    PING_INTERVAL = 3.0  # giây; server mặc định chờ tối đa 15 giây

    def __init__(
        self,
        state: ClientState,
        server_host: str,
        server_tcp_port: int,
        local_udp_port: int,
    ):
        self.state = state
        self.server_host = server_host
        self.server_tcp_port = server_tcp_port
        self.local_udp_port = local_udp_port
        self.connection: Optional[socket.socket] = None
        self._reader = None
        self._thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._closed = False
        self._send_lock = threading.Lock()

    def login(self, username: str) -> int:
        if self.connection:
            raise RuntimeError("Client is already logged in to the central server")
        connection = socket.create_connection(
            (self.server_host, self.server_tcp_port),
            timeout=5,
        )
        connection.settimeout(5)
        reader = connection.makefile("rb")
        try:
            request = {
                "type": "login",
                "username": username,
                "udp_port": self.local_udp_port,
            }
            connection.sendall(json.dumps(request).encode("utf-8") + b"\n")
            raw_response = reader.readline(65537)
            if not raw_response or len(raw_response) > 65536 or not raw_response.endswith(b"\n"):
                raise ConnectionError("Central server returned an invalid login response")
            response = json.loads(raw_response.decode("utf-8"))
            if not response.get("success"):
                raise PermissionError(response.get("error", "Central server rejected login"))
            session_id = response.get("session_id")
            if isinstance(session_id, bool) or not isinstance(session_id, int) or not 1 <= session_id <= 0xFFFFFFFF:
                raise ConnectionError("Central server returned an invalid session ID")
            self.connection = connection
            self._reader = reader
            connection.settimeout(None)
            self.state.set_online(username, session_id)
            self._apply_roster(response.get("peers", []))
            self._thread = threading.Thread(target=self._read_events, daemon=True)
            self._thread.start()
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()
            return session_id
        except Exception:
            reader.close()
            connection.close()
            raise

    def _apply_roster(self, users):
        if not isinstance(users, list):
            raise ValueError("Central server returned an invalid user roster")
        peers = []
        for user in users:
            if not isinstance(user, dict):
                continue
            if user.get("session_id") == self.state.session_id:
                continue
            ip = user.get("ip")
            port = user.get("port")
            username = user.get("username")
            session_id = user.get("session_id")
            if (
                isinstance(ip, str)
                and isinstance(port, int)
                and not isinstance(port, bool)
                and 1 <= port <= 65535
                and isinstance(username, str)
                and isinstance(session_id, int)
                and not isinstance(session_id, bool)
            ):
                peers.append({
                    "ip": ip,
                    "port": port,
                    "username": username,
                    "session_id": session_id,
                })
        self.state.replace_peers(peers)

    def _heartbeat_loop(self):
        """Gửi ping qua TCP định kỳ để server biết client còn sống."""
        while not self._closed:
            time.sleep(self.PING_INTERVAL)
            if self._closed:
                break
            try:
                with self._send_lock:
                    self.connection.sendall(b'{"type":"ping"}\n')
            except (OSError, AttributeError):
                break

    def report_warning(self, category: str):
        if category not in {"invalid_packet", "decrypt_failure", "file_decode_failure"}:
            raise ValueError("Unsupported diagnostic category")
        connection = self.connection
        if not connection or self._closed:
            return
        message = json.dumps(
            {"type": "warning", "category": category},
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        try:
            with self._send_lock:
                connection.sendall(message)
        except OSError as exc:
            if not self._closed:
                print(f"[!] Could not report a client warning to the central server: {exc}")

    def _read_events(self):
        try:
            while not self._closed:
                raw_event = self._reader.readline(65537)
                if not raw_event:
                    break
                if len(raw_event) > 65536 or not raw_event.endswith(b"\n"):
                    raise ValueError("Central server event exceeded the message size limit")
                event = json.loads(raw_event.decode("utf-8"))
                if event.get("type") == "roster":
                    self._apply_roster(event.get("users", []))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            if not self._closed:
                print(f"[!] Central server connection lost: {exc}")
        finally:
            if not self._closed:
                self.state.set_offline()
                self.state.replace_peers([])

    def close(self):
        self._closed = True
        connection = self.connection
        if connection:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        if self._reader:
            self._reader.close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)
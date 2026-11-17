import json
import base64
import socket
import threading
import time
import uuid
from typing import Callable, Optional

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
        on_disconnect: Optional[Callable[[], None]] = None,
        server_name: str = "",
        resume_session_id: Optional[int] = None,
        on_mailbox_event: Optional[Callable[[dict], None]] = None,
    ):
        self.state = state
        self.server_host = server_host
        self.server_tcp_port = server_tcp_port
        self.local_udp_port = local_udp_port
        self.on_disconnect = on_disconnect
        self.server_name = server_name
        self.resume_session_id = resume_session_id
        self.on_mailbox_event = on_mailbox_event
        self.account_created = False
        self.connection: Optional[socket.socket] = None
        self._reader = None
        self._thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._closed = False
        self._send_lock = threading.Lock()
        self._sync_lock = threading.Lock()
        self._sync_batch_lock = threading.Lock()
        self._sync_waiters = {}

    def login(self, username: str, password: Optional[str] = None) -> int:
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
            if self.server_name:
                request["server_name"] = self.server_name
            if password is not None:
                request["password"] = password
            if self.resume_session_id is not None:
                request["resume_session_id"] = self.resume_session_id
            connection.sendall(json.dumps(request).encode("utf-8") + b"\n")
            raw_response = reader.readline(65537)
            if not raw_response or len(raw_response) > 65536 or not raw_response.endswith(b"\n"):
                raise ConnectionError("Central server returned an invalid login response")
            response = json.loads(raw_response.decode("utf-8"))
            if not response.get("success"):
                raise PermissionError(response.get("error", "Central server rejected login"))
            server_name = response.get("server_name")
            if not isinstance(server_name, str) or not server_name:
                raise ConnectionError("Central server returned an invalid server name")
            self.server_name = server_name
            self.account_created = response.get("account_created") is True
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
                elif event.get("type") == "mailbox_event":
                    if self.on_mailbox_event:
                        self.on_mailbox_event(event)
                elif event.get("type") == "offline_file_chunk":
                    request_id = event.get("request_id")
                    with self._sync_lock:
                        waiter = self._sync_waiters.get(request_id)
                        if waiter and waiter["response_type"] == "offline_file_download_ack":
                            try:
                                chunk = base64.b64decode(event.get("data", ""), validate=True)
                                if event.get("index") != len(waiter["chunks"]):
                                    raise ValueError("Unexpected offline file chunk")
                                waiter["chunks"].append(chunk)
                            except (ValueError, TypeError) as exc:
                                waiter["error"] = exc
                                waiter["event"].set()
                elif event.get("type") in {
                    "offline_sync_ack",
                    "user_directory_ack",
                    "offline_message_ack",
                    "offline_inbox_ack",
                    "offline_file_ack",
                    "offline_file_download_ack",
                }:
                    request_id = event.get("request_id")
                    with self._sync_lock:
                        waiter = self._sync_waiters.get(request_id)
                        if waiter and event.get("type") == waiter["response_type"]:
                            waiter["response"] = event
                            waiter["event"].set()
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            if not self._closed:
                print(f"[!] Central server connection lost: {exc}")
        finally:
            with self._sync_lock:
                for waiter in self._sync_waiters.values():
                    waiter["response"] = None
                    waiter["event"].set()
            if not self._closed:
                if self.on_disconnect:
                    self.on_disconnect()
                else:
                    self.state.set_offline()
                    self.state.replace_peers([])

    def sync_offline_history(self):
        with self._sync_batch_lock:
            while True:
                events = self.state.get_unsynced_server_events()
                if not events:
                    return
                self._request("offline_sync", "offline_sync_ack", {
                    "events": events,
                }, timeout=10)
                self.state.mark_server_events_synced(
                    event["event_id"] for event in events
                )

    def request_user_directory(self):
        response = self._request(
            "user_directory",
            "user_directory_ack",
            {},
        )
        return response.get("users", [])

    def send_offline_message(self, message):
        response = self._request(
            "offline_message",
            "offline_message_ack",
            {"message": message},
        )
        return response["stored_count"]

    def send_offline_file(
        self,
        file_id,
        filename,
        filepath,
        original_filesize,
        encrypted_size,
        checksum,
        recipients,
        event_id,
        timestamp,
        event_order,
    ):
        total_chunks = (encrypted_size + 32767) // 32768
        self._request(
            "offline_file_begin",
            "offline_file_ack",
            {
                "file_id": file_id,
                "filename": filename,
                "filesize": original_filesize,
                "encrypted_size": encrypted_size,
                "sha256": checksum,
                "total_chunks": total_chunks,
                "recipients": recipients,
                "event_id": event_id,
                "timestamp": timestamp,
                "event_order": event_order,
            },
            timeout=10,
            phase="begin",
        )
        with open(filepath, "rb") as source:
            for index in range(total_chunks):
                chunk = source.read(32768)
                if not chunk:
                    raise OSError("Temporary encrypted file ended before its declared size")
                self._send_control({
                    "type": "offline_file_chunk",
                    "file_id": file_id,
                    "index": index,
                    "data": base64.b64encode(chunk).decode("ascii"),
                })
            if source.read(1):
                raise OSError("Temporary encrypted file exceeded its declared size")
        self._request(
            "offline_file_complete",
            "offline_file_ack",
            {"file_id": file_id},
            timeout=max(30, encrypted_size // (1024 * 1024) * 10),
            phase="complete",
        )

    def get_offline_messages(self, offset=0, limit=5):
        response = self._request(
            "offline_inbox",
            "offline_inbox_ack",
            {"offset": offset, "limit": limit},
        )
        return response["messages"], response["total"]

    def download_offline_file(self, mail_id, filesize):
        response = self._request(
            "offline_file_download",
            "offline_file_download_ack",
            {"mail_id": mail_id},
            timeout=max(30, filesize // (1024 * 1024) * 10),
        )
        return b"".join(response["chunks"])

    def _send_control(self, control):
        encoded = json.dumps(control, separators=(",", ":")).encode("utf-8") + b"\n"
        with self._send_lock:
            if not self.connection or self._closed:
                raise ConnectionError("Central server connection is unavailable")
            self.connection.sendall(encoded)

    def _request(
        self,
        request_type,
        response_type,
        fields,
        timeout=10,
        phase=None,
    ):
        request_id = uuid.uuid4().hex
        waiter = {
            "event": threading.Event(),
            "response_type": response_type,
            "response": None,
            "chunks": [],
            "error": None,
        }
        with self._sync_lock:
            self._sync_waiters[request_id] = waiter
        control = {
            "type": request_type,
            "request_id": request_id,
            **fields,
        }
        if phase is not None:
            control["phase"] = phase
        try:
            self._send_control(control)
            if not waiter["event"].wait(timeout):
                raise TimeoutError(f"Timed out waiting for {response_type}")
            if waiter["error"]:
                raise waiter["error"]
            response = waiter["response"]
            if not response:
                raise ConnectionError(f"Central server closed during {response_type}")
            if response.get("success") is not True:
                raise ValueError(response.get("error", "Central server rejected the request"))
            response["chunks"] = waiter["chunks"]
            return response
        finally:
            with self._sync_lock:
                self._sync_waiters.pop(request_id, None)

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
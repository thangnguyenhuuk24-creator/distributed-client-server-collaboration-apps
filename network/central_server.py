import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import socket
import sqlite3
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional

from core.protocol import FLAG_ACK, FLAG_FILE, FLAG_MSG, Packet


RELAY_PREFIX = b"RLY1"


@dataclass
class ConnectedUser:
    username: str
    session_id: int
    ip: str
    tcp_port: int
    udp_port: int
    connection: socket.socket
    connected_at: float = field(default_factory=time.time)
    send_lock: threading.Lock = field(default_factory=threading.Lock)


class CentralServer:
    """LAN-only TCP login/presence server with UDP chat relay."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        tcp_port: int = 9001,
        relay_port: int = 9002,
        client_timeout: float = 15.0,
        server_name: str = "LAN Server",
        auth_db_path: Optional[str] = None,
        require_auth: bool = False,
    ):
        self.host = host
        self.tcp_port = tcp_port
        self.relay_port = relay_port
        self.server_name = server_name
        self.auth_db_path = auth_db_path
        self.require_auth = require_auth
        self._identity_configured = auth_db_path is not None or not require_auth
        self._auth_lock = threading.Lock()
        if self.auth_db_path:
            with sqlite3.connect(self.auth_db_path) as database:
                database.execute(
                    "CREATE TABLE IF NOT EXISTS accounts ("
                    "username_key TEXT PRIMARY KEY, username TEXT NOT NULL, "
                    "salt BLOB NOT NULL, password_hash BLOB NOT NULL)"
                )
                database.execute(
                    "CREATE TABLE IF NOT EXISTS user_sessions ("
                    "username_key TEXT PRIMARY KEY, session_id INTEGER NOT NULL UNIQUE)"
                )
                database.execute(
                    "CREATE TABLE IF NOT EXISTS recovered_events ("
                    "event_id TEXT PRIMARY KEY, sender TEXT NOT NULL, "
                    "recipient TEXT NOT NULL, event_type TEXT NOT NULL, "
                    "timestamp REAL NOT NULL, event_order INTEGER NOT NULL, "
                    "filename TEXT NOT NULL, filesize INTEGER NOT NULL)"
                )
                self._create_mailbox_table(database)
        # Client im lặng quá thời gian này (không gửi ping) sẽ bị loại khỏi roster.
        self.client_timeout = client_timeout
        self.running = False
        self._lock = threading.RLock()
        self._lifecycle_lock = threading.Lock()
        self._users: Dict[int, ConnectedUser] = {}
        self._started_at = 0.0
        self._metrics = {
            "tcp_connections": 0,
            "successful_logins": 0,
            "rejected_logins": 0,
            "disconnects": 0,
            "chat_relays": 0,
            "global_chat_relays": 0,
            "private_chat_relays": 0,
            "file_relays": 0,
            "ack_relays": 0,
            "relay_datagrams": 0,
            "relay_bytes": 0,
            "relay_drops": 0,
            "offline_chat_recovered": 0,
            "offline_file_recovered": 0,
        }
        self._events = deque(maxlen=100)
        if self.auth_db_path:
            with sqlite3.connect(self.auth_db_path) as database:
                recovered_rows = database.execute(
                    "SELECT event_id, sender, recipient, event_type, timestamp, "
                    "event_order, filename, filesize FROM recovered_events "
                    "ORDER BY timestamp, event_order"
                ).fetchall()
            for row in recovered_rows:
                event_id, sender, recipient, event_type, timestamp, event_order, filename, filesize = row
                self._events.append({
                    "type": f"offline_{event_type}_recovered",
                    "event_id": event_id,
                    "sender": sender,
                    "recipient": recipient,
                    "timestamp": timestamp,
                    "event_order": event_order,
                    "filename": filename,
                    "filesize": filesize,
                })
                self._metrics[f"offline_{event_type}_recovered"] += 1
        self._throughput_samples = deque()
        self._file_transfers = {}
        self._listener: Optional[socket.socket] = None
        self._relay_socket: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None
        self._relay_thread: Optional[threading.Thread] = None
        self._client_threads = []

    def start(self):
        with self._lifecycle_lock:
            if self.running:
                return
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            relay_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind((self.host, self.tcp_port))
                listener.listen(32)
                relay_socket.bind((self.host, self.relay_port))
                relay_socket.settimeout(0.5)
            except OSError:
                listener.close()
                relay_socket.close()
                raise

            self._listener = listener
            self._relay_socket = relay_socket
            self.tcp_port = listener.getsockname()[1]
            self.relay_port = relay_socket.getsockname()[1]
            self._started_at = time.time()
            self.running = True
            self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
            self._relay_thread = threading.Thread(target=self._relay_loop, daemon=True)
            self._accept_thread.start()
            self._relay_thread.start()
            self._record_event("server_started", {
                "host": self.host,
                "tcp_port": self.tcp_port,
                "relay_port": self.relay_port,
            })

    @staticmethod
    def auth_database_path(server_name: str, tcp_port: int, relay_port: int, directory: str):
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "-", server_name).strip("-") or "server"
        name_id = hashlib.sha256(server_name.casefold().encode("utf-8")).hexdigest()[:12]
        return os.path.abspath(os.path.join(
            directory,
            f"{safe_name}_{name_id}_{tcp_port}_{relay_port}.sqlite3",
        ))

    @staticmethod
    def load_saved_name(tcp_port: int, relay_port: int, directory: str):
        config_path = os.path.join(
            directory,
            f"server_{tcp_port}_{relay_port}.json",
        )
        try:
            with open(config_path, "r", encoding="utf-8") as config_file:
                data = json.load(config_file)
        except FileNotFoundError:
            return ""
        if not isinstance(data, dict) or not isinstance(data.get("server_name"), str):
            raise ValueError(f"Invalid server configuration file: {config_path}")
        return data["server_name"]

    def configure_identity(self, server_name: str, directory: str = "storage/servers"):
        server_name = server_name.strip()
        if not server_name or len(server_name) > 64:
            raise ValueError("Server name must contain 1-64 characters")
        with self._lock:
            if self._identity_configured:
                if self.server_name.casefold() == server_name.casefold():
                    return
                raise RuntimeError("Server identity has already been configured")
            if self._users:
                raise RuntimeError("Cannot configure the server while users are connected")

        os.makedirs(directory, exist_ok=True)
        database_path = self.auth_database_path(
            server_name,
            self.tcp_port,
            self.relay_port,
            directory,
        )
        with sqlite3.connect(database_path) as database:
            database.execute(
                "CREATE TABLE IF NOT EXISTS accounts ("
                "username_key TEXT PRIMARY KEY, username TEXT NOT NULL, "
                "salt BLOB NOT NULL, password_hash BLOB NOT NULL)"
            )
            database.execute(
                "CREATE TABLE IF NOT EXISTS user_sessions ("
                "username_key TEXT PRIMARY KEY, session_id INTEGER NOT NULL UNIQUE)"
            )
            database.execute(
                "CREATE TABLE IF NOT EXISTS recovered_events ("
                "event_id TEXT PRIMARY KEY, sender TEXT NOT NULL, "
                "recipient TEXT NOT NULL, event_type TEXT NOT NULL, "
                "timestamp REAL NOT NULL, event_order INTEGER NOT NULL, "
                "filename TEXT NOT NULL, filesize INTEGER NOT NULL)"
            )
            self._create_mailbox_table(database)
        config_path = os.path.join(
            directory,
            f"server_{self.tcp_port}_{self.relay_port}.json",
        )
        temporary_path = config_path + ".tmp"
        configured_host = (
            "127.0.0.1" if self.host in ("0.0.0.0", "::") else self.host
        )
        with open(temporary_path, "w", encoding="utf-8") as config_file:
            json.dump(
                {"server_name": server_name, "host": configured_host},
                config_file,
                ensure_ascii=False,
            )
        os.replace(temporary_path, config_path)
        with self._lock:
            self.server_name = server_name
            self.auth_db_path = database_path
            self._identity_configured = True
        self._record_event("server_configured", {"server_name": server_name})

    def stop(self):
        with self._lifecycle_lock:
            if not self.running:
                return
            self.running = False
            for sock in (self._listener, self._relay_socket):
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
            with self._lock:
                users = list(self._users.values())
                self._users.clear()
            for user in users:
                try:
                    user.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                user.connection.close()
            for thread in (self._accept_thread, self._relay_thread):
                if thread and thread.is_alive():
                    thread.join(timeout=1)
            for thread in self._client_threads:
                if thread.is_alive():
                    thread.join(timeout=1)
            self._record_event("server_stopped", {})

    def kick_client(self, session_id: int) -> bool:
        with self._lock:
            user = self._users.pop(session_id, None)
            if user:
                self._metrics["disconnects"] += 1
        if not user:
            return False
        self._record_event("client_kicked", {
            "username": user.username,
            "session_id": user.session_id,
            "ip": user.ip,
        })
        try:
            user.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        user.connection.close()
        if self.running:
            self._broadcast_roster()
        return True

    def clear_events(self):
        with self._lock:
            self._events.clear()

    def _record_network_warning(self, address, reason: str):
        self._record_event("network_warning", {
            "ip": address[0],
            "port": address[1],
            "reason": reason,
        })

    def get_users(self):
        with self._lock:
            return [
                self._user_record(user, include_connected=True)
                for user in self._users.values()
            ]

    @staticmethod
    def _user_record(user: ConnectedUser, include_connected: bool = False):
        record = {
            "username": user.username,
            "session_id": user.session_id,
            "ip": user.ip,
            "tcp_port": user.tcp_port,
            "udp_port": user.udp_port,
            "port": user.udp_port,
        }
        if include_connected:
            record["connected_at"] = user.connected_at
            record["connected_seconds"] = max(0, int(time.time() - user.connected_at))
        return record

    def get_dashboard_snapshot(self):
        now = time.time()
        with self._lock:
            samples = [
                sample for sample in self._throughput_samples
                if now - sample["timestamp"] <= 5.0
            ]
            self._throughput_samples = deque(samples)
            transfer_rows = [
                {
                    key: value
                    for key, value in transfer.items()
                    if not key.startswith("_")
                }
                for transfer in self._file_transfers.values()
            ]
            tcp_address = self.host
            udp_address = self.host
            try:
                if self._listener:
                    tcp_address = self._listener.getsockname()[0]
            except OSError:
                pass
            try:
                if self._relay_socket:
                    udp_address = self._relay_socket.getsockname()[0]
            except OSError:
                pass
            return {
                "status": "online" if self.running else "offline",
                "server_name": self.server_name,
                "server_configured": self._identity_configured,
                "uptime_seconds": max(0, int(now - self._started_at)) if self._started_at else 0,
                "listeners": {
                    "tcp": {
                        "host": self.host,
                        "port": self.tcp_port,
                        "address": tcp_address,
                    },
                    "udp_relay": {
                        "host": self.host,
                        "port": self.relay_port,
                        "address": udp_address,
                    },
                },
                "metrics": dict(self._metrics),
                "throughput": {
                    "window_seconds": 5,
                    "bytes_per_second": round(
                        sum(sample["bytes"] for sample in samples) / 5.0, 2
                    ),
                    "packets_per_second": round(len(samples) / 5.0, 2),
                    "file_packets_per_second": round(
                        sum(sample["kind"] == "file" for sample in samples) / 5.0,
                        2,
                    ),
                },
                "file_transfers": transfer_rows,
                "online_count": len(self._users),
                "users": [
                    self._user_record(user, include_connected=True)
                    for user in self._users.values()
                ],
                "events": list(reversed(self._events)),
                "updated_at": now,
            }

    def _record_event(self, event_type: str, details: dict):
        with self._lock:
            self._events.append({
                "timestamp": time.time(),
                "type": event_type,
                **details,
            })

    def _accept_loop(self):
        while self.running:
            try:
                connection, address = self._listener.accept()
            except OSError:
                if self.running:
                    raise
                break
            with self._lock:
                self._metrics["tcp_connections"] += 1
            self._record_event("tcp_connected", {
                "ip": address[0],
                "tcp_port": address[1],
            })
            thread = threading.Thread(
                target=self._handle_client,
                args=(connection, address),
                daemon=True,
            )
            self._client_threads.append(thread)
            thread.start()

    def _handle_client(self, connection: socket.socket, address):
        user = None
        disconnect_reason = "client_disconnected"
        mailbox_upload = None
        connection.settimeout(5)
        reader = connection.makefile("rb")
        try:
            raw_request = reader.readline(4097)
            if not raw_request or len(raw_request) > 4096 or not raw_request.endswith(b"\n"):
                with self._lock:
                    self._metrics["rejected_logins"] += 1
                self._record_event("login_rejected", {
                    "ip": address[0],
                    "reason": "invalid_request",
                })
                self._send_json(connection, {"type": "login_ack", "success": False, "error": "Invalid login request"})
                return
            request = json.loads(raw_request.decode("utf-8"))
            if self.require_auth and not self._identity_configured:
                self._reject_login(connection, address, "Configure the server name in the dashboard first")
                return
            username = request.get("username")
            password = request.get("password")
            requested_server_name = request.get("server_name")
            udp_port = request.get("udp_port")
            if not isinstance(username, str) or not username.strip() or len(username.strip()) > 64:
                with self._lock:
                    self._metrics["rejected_logins"] += 1
                self._record_event("login_rejected", {
                    "ip": address[0],
                    "reason": "invalid_username",
                })
                self._send_json(connection, {"type": "login_ack", "success": False, "error": "Username must contain 1-64 characters"})
                return
            if isinstance(udp_port, bool) or not isinstance(udp_port, int) or not 1 <= udp_port <= 65535:
                with self._lock:
                    self._metrics["rejected_logins"] += 1
                self._record_event("login_rejected", {
                    "ip": address[0],
                    "reason": "invalid_udp_port",
                })
                self._send_json(connection, {"type": "login_ack", "success": False, "error": "Invalid UDP port"})
                return
            username = username.strip()
            account_created = False
            if self.auth_db_path:
                if requested_server_name and requested_server_name != self.server_name:
                    self._reject_login(
                        connection,
                        address,
                        "Server name does not match the selected server",
                        username,
                    )
                    return
                if not isinstance(password, str) or not 8 <= len(password) <= 256:
                    self._reject_login(connection, address, "Password must contain 8-256 characters")
                    return
                account_created, auth_error = self._authenticate(username, password)
                if auth_error:
                    self._reject_login(connection, address, auth_error, username)
                    return
            # Sau khi đăng nhập, client phải gửi ping định kỳ; im lặng quá
            # client_timeout giây thì readline bên dưới ném socket.timeout.
            connection.settimeout(self.client_timeout)
            with self._lock:
                if any(existing.username.casefold() == username.casefold() for existing in self._users.values()):
                    self._metrics["rejected_logins"] += 1
                    self._events.append({
                        "timestamp": time.time(),
                        "type": "login_rejected",
                        "ip": address[0],
                        "username": username,
                        "reason": "username_already_online",
                    })
                    self._send_json(connection, {"type": "login_ack", "success": False, "error": "Username is already online"})
                    return
                session_id = self._account_session_id(username)
                user = ConnectedUser(
                    username,
                    session_id,
                    address[0],
                    address[1],
                    udp_port,
                    connection,
                )
                peers = [self._user_record(existing) for existing in self._users.values()]
                self._users[session_id] = user
                self._metrics["successful_logins"] += 1

            self._record_event("login_accepted", {
                "username": username,
                "session_id": session_id,
                "ip": address[0],
                "tcp_port": address[1],
                "udp_port": udp_port,
            })
            self._send_json(connection, {
                "type": "login_ack",
                "success": True,
                "server_name": self.server_name,
                "account_created": account_created,
                "session_id": session_id,
                "peers": peers,
            }, user.send_lock)
            self._broadcast_roster()
            while self.running:
                line = reader.readline(65537)
                if not line:
                    break  # client đóng kết nối
                if len(line) > 65536 or not line.endswith(b"\n"):
                    raise ValueError("Client control message exceeded the size limit")
                control = json.loads(line.decode("utf-8"))
                if not isinstance(control, dict):
                    raise ValueError("Invalid client control message")
                if control.get("type") == "warning":
                    category = control.get("category")
                    if category in {
                        "invalid_packet",
                        "decrypt_failure",
                        "file_decode_failure",
                    }:
                        self._record_event("client_warning", {
                            "username": user.username,
                            "session_id": user.session_id,
                            "category": category,
                        })
                    else:
                        self._record_event("network_warning", {
                            "ip": address[0],
                            "port": address[1],
                            "reason": "Unknown client warning category",
                        })
                elif control.get("type") == "offline_sync":
                    request_id = control.get("request_id")
                    try:
                        stored_count = self._store_recovered_events(
                            user,
                            request_id,
                            control.get("events"),
                        )
                    except (ValueError, sqlite3.Error) as exc:
                        self._send_json(connection, {
                            "type": "offline_sync_ack",
                            "request_id": request_id,
                            "success": False,
                            "error": str(exc),
                        }, user.send_lock)
                        continue
                    self._send_json(connection, {
                        "type": "offline_sync_ack",
                        "request_id": request_id,
                        "success": True,
                        "stored_count": stored_count,
                    }, user.send_lock)
                elif control.get("type") == "user_directory":
                    self._send_json(connection, {
                        "type": "user_directory_ack",
                        "request_id": control.get("request_id"),
                        "success": True,
                        "users": self._user_directory(user),
                    }, user.send_lock)
                elif control.get("type") == "offline_message":
                    request_id = control.get("request_id")
                    try:
                        stored_count = self._store_offline_messages(
                            user,
                            request_id,
                            control.get("message"),
                        )
                    except (ValueError, sqlite3.Error, RuntimeError) as exc:
                        self._send_json(connection, {
                            "type": "offline_message_ack",
                            "request_id": request_id,
                            "success": False,
                            "error": str(exc),
                        }, user.send_lock)
                        continue
                    self._send_json(connection, {
                        "type": "offline_message_ack",
                        "request_id": request_id,
                        "success": True,
                        "stored_count": stored_count,
                    }, user.send_lock)
                elif control.get("type") == "offline_inbox":
                    request_id = control.get("request_id")
                    offset = control.get("offset", 0)
                    limit = control.get("limit", 5)
                    if (
                        not self._valid_request_id(request_id)
                        or isinstance(offset, bool)
                        or not isinstance(offset, int)
                        or offset < 0
                        or isinstance(limit, bool)
                        or not isinstance(limit, int)
                        or not 1 <= limit <= 5
                        or not self.auth_db_path
                    ):
                        self._send_json(connection, {
                            "type": "offline_inbox_ack",
                            "request_id": request_id,
                            "success": False,
                            "error": "Invalid offline mailbox request",
                        }, user.send_lock)
                        continue
                    messages, total = self._get_offline_messages(
                        user.username.casefold(),
                        offset,
                        limit,
                    )
                    self._send_json(connection, {
                        "type": "offline_inbox_ack",
                        "request_id": request_id,
                        "success": True,
                        "messages": messages,
                        "total": total,
                    }, user.send_lock)
                elif control.get("type") == "offline_file_begin":
                    request_id = control.get("request_id")
                    try:
                        if mailbox_upload:
                            raise ValueError("An offline file upload is already in progress")
                        if not self._valid_request_id(request_id) or not self.auth_db_path:
                            raise ValueError("Invalid offline file request")
                        file_id = control.get("file_id")
                        filename = control.get("filename")
                        filesize = control.get("filesize")
                        encrypted_size = control.get("encrypted_size")
                        checksum = control.get("sha256")
                        total_chunks = control.get("total_chunks")
                        event_id = control.get("event_id")
                        timestamp = control.get("timestamp")
                        event_order = control.get("event_order")
                        recipients = control.get("recipients")
                        if (
                            not isinstance(file_id, str)
                            or len(file_id) != 32
                            or not isinstance(filename, str)
                            or not filename
                            or len(filename) > 255
                            or os.path.basename(filename.replace("\\", "/")) != filename
                            or not isinstance(checksum, str)
                            or not re.fullmatch(r"[0-9a-f]{64}", checksum)
                            or isinstance(filesize, bool)
                            or not isinstance(filesize, int)
                            or not 0 <= filesize <= 200 * 1024 * 1024
                            or isinstance(encrypted_size, bool)
                            or not isinstance(encrypted_size, int)
                            or not filesize <= encrypted_size <= filesize + 32
                            or isinstance(total_chunks, bool)
                            or not isinstance(total_chunks, int)
                            or total_chunks != (encrypted_size + 32767) // 32768
                            or not isinstance(event_id, str)
                            or not 1 <= len(event_id) <= 64
                            or isinstance(timestamp, bool)
                            or not isinstance(timestamp, (int, float))
                            or not 0 <= timestamp <= 4102444800
                            or isinstance(event_order, bool)
                            or not isinstance(event_order, int)
                            or event_order < 1
                            or not isinstance(recipients, list)
                            or not 1 <= len(recipients) <= 100
                        ):
                            raise ValueError("Invalid offline file metadata")
                        directory = {
                            entry["username"].casefold(): entry["username"]
                            for entry in self._user_directory(user)
                        }
                        normalized_recipients = []
                        for item in recipients:
                            if not isinstance(item, dict):
                                raise ValueError("Invalid offline file recipient")
                            name = item.get("username")
                            stream_id = item.get("stream_id")
                            if (
                                not isinstance(name, str)
                                or name.casefold() not in directory
                                or isinstance(stream_id, bool)
                                or not isinstance(stream_id, int)
                                or not 0 <= stream_id <= 0xFFFFFFFF
                            ):
                                raise ValueError("Invalid offline file recipient")
                            normalized_recipients.append((
                                name.casefold(),
                                directory[name.casefold()],
                                stream_id,
                            ))
                        mailbox_dir = self._mailbox_directory()
                        temporary_path = os.path.join(mailbox_dir, f"{file_id}.part")
                        mailbox_upload = {
                            "file_id": file_id,
                            "filename": filename,
                            "filesize": encrypted_size,
                            "display_filesize": filesize,
                            "sha256": checksum,
                            "total_chunks": total_chunks,
                            "received_chunks": 0,
                            "received_size": 0,
                            "temporary_path": temporary_path,
                            "file": open(temporary_path, "wb"),
                            "event_id": event_id,
                            "timestamp": timestamp,
                            "event_order": event_order,
                            "recipients": normalized_recipients,
                        }
                    except (OSError, ValueError, RuntimeError) as exc:
                        self._send_json(connection, {
                            "type": "offline_file_ack",
                            "request_id": request_id,
                            "success": False,
                            "phase": "begin",
                            "error": str(exc),
                        }, user.send_lock)
                        continue
                    self._send_json(connection, {
                        "type": "offline_file_ack",
                        "request_id": request_id,
                        "success": True,
                        "phase": "begin",
                    }, user.send_lock)
                elif control.get("type") == "offline_file_chunk":
                    try:
                        if (
                            not mailbox_upload
                            or control.get("file_id") != mailbox_upload["file_id"]
                            or control.get("index") != mailbox_upload["received_chunks"]
                        ):
                            raise ValueError("Unexpected offline file chunk")
                        chunk = base64.b64decode(control.get("data", ""), validate=True)
                        if not chunk or len(chunk) > 32768:
                            raise ValueError("Invalid offline file chunk size")
                        mailbox_upload["file"].write(chunk)
                        mailbox_upload["received_chunks"] += 1
                        mailbox_upload["received_size"] += len(chunk)
                        if mailbox_upload["received_size"] > mailbox_upload["filesize"]:
                            raise ValueError("Offline file exceeds its declared size")
                    except (ValueError, TypeError) as exc:
                        raise ValueError(f"Invalid offline file upload: {exc}") from exc
                elif control.get("type") == "offline_file_complete":
                    request_id = control.get("request_id")
                    try:
                        if (
                            not mailbox_upload
                            or not self._valid_request_id(request_id)
                            or control.get("file_id") != mailbox_upload["file_id"]
                        ):
                            raise ValueError("Invalid offline file completion")
                        mailbox_upload["file"].close()
                        stored_count = self._store_offline_file(
                            user,
                            mailbox_upload,
                            request_id,
                        )
                    except (OSError, ValueError, sqlite3.Error, RuntimeError) as exc:
                        if mailbox_upload:
                            try:
                                mailbox_upload["file"].close()
                            except OSError:
                                pass
                            try:
                                os.remove(mailbox_upload["temporary_path"])
                            except OSError:
                                pass
                            mailbox_upload = None
                        self._send_json(connection, {
                            "type": "offline_file_ack",
                            "request_id": request_id,
                            "success": False,
                            "phase": "complete",
                            "error": str(exc),
                        }, user.send_lock)
                        continue
                    mailbox_upload = None
                    self._send_json(connection, {
                        "type": "offline_file_ack",
                        "request_id": request_id,
                        "success": True,
                        "phase": "complete",
                        "stored_count": stored_count,
                    }, user.send_lock)
                elif control.get("type") == "offline_file_download":
                    request_id = control.get("request_id")
                    try:
                        if not self._valid_request_id(request_id):
                            raise ValueError("Invalid offline file download request")
                        path = self._read_offline_file(
                            user.username.casefold(),
                            control.get("mail_id"),
                        )
                        with open(path, "rb") as offline_file:
                            index = 0
                            while True:
                                chunk = offline_file.read(32768)
                                if not chunk:
                                    break
                                self._send_json(connection, {
                                    "type": "offline_file_chunk",
                                    "request_id": request_id,
                                    "index": index,
                                    "data": base64.b64encode(chunk).decode("ascii"),
                                }, user.send_lock)
                                index += 1
                    except (OSError, ValueError, RuntimeError) as exc:
                        self._send_json(connection, {
                            "type": "offline_file_download_ack",
                            "request_id": request_id,
                            "success": False,
                            "error": str(exc),
                        }, user.send_lock)
                        continue
                    self._send_json(connection, {
                        "type": "offline_file_download_ack",
                        "request_id": request_id,
                        "success": True,
                    }, user.send_lock)
                elif control.get("type") != "ping":
                    self._record_event("network_warning", {
                        "ip": address[0],
                        "port": address[1],
                        "reason": "Unknown TCP control message",
                    })
        except socket.timeout:
            disconnect_reason = "client_timeout"
            if not user:
                self._record_event("login_timeout", {"ip": address[0]})
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, AttributeError, ValueError) as exc:
            disconnect_reason = (
                "client_protocol_error"
                if isinstance(exc, (UnicodeDecodeError, json.JSONDecodeError, ValueError))
                else "client_disconnected"
            )
            self._record_event("network_warning", {
                "ip": address[0],
                "port": address[1],
                "reason": f"TCP {type(exc).__name__}",
            })
        finally:
            if mailbox_upload:
                try:
                    mailbox_upload["file"].close()
                except OSError:
                    pass
                try:
                    os.remove(mailbox_upload["temporary_path"])
                except OSError:
                    pass
            reader.close()
            if user:
                with self._lock:
                    removed = self._users.pop(user.session_id, None) is not None
                    if removed:
                        self._metrics["disconnects"] += 1
                if removed and self.running:
                    self._record_event(disconnect_reason, {
                        "username": user.username,
                        "session_id": user.session_id,
                        "ip": user.ip,
                    })
                    self._broadcast_roster()
            try:
                connection.close()
            except OSError:
                pass

    def _authenticate(self, username: str, password: str):
        username_key = username.casefold()
        with self._auth_lock:
            with sqlite3.connect(self.auth_db_path) as database:
                account = database.execute(
                    "SELECT salt, password_hash FROM accounts WHERE username_key = ?",
                    (username_key,),
                ).fetchone()
                if account:
                    salt, expected_hash = account
                    password_hash = hashlib.pbkdf2_hmac(
                        "sha256", password.encode("utf-8"), salt, 200_000
                    )
                    if not hmac.compare_digest(password_hash, expected_hash):
                        return False, "Incorrect username or password"
                    return False, None

                salt = secrets.token_bytes(16)
                password_hash = hashlib.pbkdf2_hmac(
                    "sha256", password.encode("utf-8"), salt, 200_000
                )
                database.execute(
                    "INSERT INTO accounts (username_key, username, salt, password_hash) "
                    "VALUES (?, ?, ?, ?)",
                    (username_key, username, salt, password_hash),
                )
                return True, None

    def _reject_login(self, connection, address, reason: str, username=None):
        with self._lock:
            self._metrics["rejected_logins"] += 1
        self._record_event("login_rejected", {
            "ip": address[0],
            "username": username,
            "reason": "authentication_failed",
        })
        self._send_json(
            connection,
            {"type": "login_ack", "success": False, "error": reason},
        )

    def _account_session_id(self, username):
        if not self.auth_db_path:
            return self._new_session_id()
        username_key = username.casefold()
        with self._auth_lock, sqlite3.connect(self.auth_db_path) as database:
            row = database.execute(
                "SELECT session_id FROM user_sessions WHERE username_key = ?",
                (username_key,),
            ).fetchone()
            if row:
                return row[0]
            while True:
                session_id = secrets.randbits(32)
                if not session_id:
                    continue
                try:
                    database.execute(
                        "INSERT INTO user_sessions (username_key, session_id) VALUES (?, ?)",
                        (username_key, session_id),
                    )
                    return session_id
                except sqlite3.IntegrityError:
                    continue

    def _store_recovered_events(self, user, request_id, events):
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 64:
            raise ValueError("Invalid history sync request ID")
        if not isinstance(events, list) or not 1 <= len(events) <= 50:
            raise ValueError("Offline history batch must contain 1-50 events")
        if not self.auth_db_path:
            raise ValueError("Offline history sync is not configured")

        validated = []
        for event in events:
            if not isinstance(event, dict):
                raise ValueError("Invalid offline history event")
            event_id = event.get("event_id")
            event_type = event.get("event_type")
            timestamp = event.get("timestamp")
            event_order = event.get("event_order")
            recipient = event.get("recipient", "")
            filename = event.get("filename", "")
            filesize = event.get("filesize", 0)
            if (
                not isinstance(event_id, str)
                or not 1 <= len(event_id) <= 64
                or event_type not in {"chat", "file"}
                or isinstance(timestamp, bool)
                or not isinstance(timestamp, (int, float))
                or not 0 <= timestamp <= 4102444800
                or isinstance(event_order, bool)
                or not isinstance(event_order, int)
                or event_order < 1
                or not isinstance(recipient, str)
                or len(recipient) > 64
                or not isinstance(filename, str)
                or len(filename) > 255
                or os.path.basename(filename.replace("\\", "/")) != filename
                or isinstance(filesize, bool)
                or not isinstance(filesize, int)
                or not 0 <= filesize <= 200 * 1024 * 1024
            ):
                raise ValueError("Invalid offline history event fields")
            validated.append(
                (event_id, user.username, recipient, event_type, timestamp,
                 event_order, filename, filesize)
            )

        stored = []
        with self._auth_lock, sqlite3.connect(self.auth_db_path) as database:
            for event in validated:
                existing = database.execute(
                    "SELECT sender FROM recovered_events WHERE event_id = ?",
                    (event[0],),
                ).fetchone()
                if existing:
                    if existing[0] != user.username:
                        raise ValueError("Offline history event ID already belongs to another account")
                    continue
                database.execute(
                    "INSERT INTO recovered_events "
                    "(event_id, sender, recipient, event_type, timestamp, "
                    "event_order, filename, filesize) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    event,
                )
                stored.append(event)

        for event in stored:
            event_id, sender, recipient, event_type, timestamp, event_order, filename, filesize = event
            self._record_event(
                f"offline_{event_type}_recovered",
                {
                    "event_id": event_id,
                    "sender": sender,
                    "recipient": recipient,
                    "timestamp": timestamp,
                    "event_order": event_order,
                    "filename": filename,
                    "filesize": filesize,
                },
            )
            with self._lock:
                self._metrics[f"offline_{event_type}_recovered"] += 1
        return len(stored)

    @staticmethod
    def _create_mailbox_table(database):
        database.execute(
            "CREATE TABLE IF NOT EXISTS offline_mailbox ("
            "mail_id TEXT PRIMARY KEY, sender_key TEXT NOT NULL, sender TEXT NOT NULL, "
            "recipient_key TEXT NOT NULL, recipient TEXT NOT NULL, event_id TEXT NOT NULL, "
            "event_timestamp REAL NOT NULL, event_order INTEGER NOT NULL, "
            "stream_id INTEGER NOT NULL, message_type TEXT NOT NULL, content TEXT NOT NULL, "
            "filename TEXT NOT NULL, filesize INTEGER NOT NULL, sha256 TEXT NOT NULL DEFAULT '', "
            "attachment_path TEXT, "
            "expires_at REAL, created_at REAL NOT NULL, "
            "UNIQUE(recipient_key, event_id))"
        )
        columns = {
            row[1] for row in database.execute("PRAGMA table_info(offline_mailbox)")
        }
        if "sha256" not in columns:
            database.execute(
                "ALTER TABLE offline_mailbox "
                "ADD COLUMN sha256 TEXT NOT NULL DEFAULT ''"
            )
        database.execute(
            "CREATE INDEX IF NOT EXISTS offline_mailbox_recipient "
            "ON offline_mailbox(recipient_key, event_timestamp, event_order)"
        )

    def _mailbox_directory(self):
        if not self.auth_db_path:
            raise RuntimeError("Offline mailbox requires a configured server identity")
        path = self.auth_db_path + ".mailbox"
        os.makedirs(path, exist_ok=True)
        return path

    def _user_directory(self, current_user):
        if not self.auth_db_path:
            return []
        with self._auth_lock, sqlite3.connect(self.auth_db_path) as database:
            rows = database.execute(
                "SELECT accounts.username, accounts.username_key, user_sessions.session_id "
                "FROM accounts LEFT JOIN user_sessions "
                "ON user_sessions.username_key = accounts.username_key "
                "ORDER BY accounts.username_key"
            ).fetchall()
        with self._lock:
            online = {
                user.username.casefold(): user
                for user in self._users.values()
            }
        return [
            {
                "username": username,
                "session_id": session_id,
                "online": username_key in online,
                "ip": online[username_key].ip if username_key in online else "",
                "port": online[username_key].udp_port if username_key in online else 0,
            }
            for username, username_key, session_id in rows
            if username_key != current_user.username.casefold() and session_id is not None
        ]

    @staticmethod
    def _valid_request_id(request_id):
        return isinstance(request_id, str) and 1 <= len(request_id) <= 64

    def _store_offline_messages(self, sender, request_id, message):
        if not self._valid_request_id(request_id):
            raise ValueError("Invalid mailbox request ID")
        if not isinstance(message, dict):
            raise ValueError("Invalid offline message")
        recipients = message.get("recipients")
        if not isinstance(recipients, list) or not 1 <= len(recipients) <= 100:
            raise ValueError("Offline message batch must contain 1-100 recipients")
        if not self.auth_db_path:
            raise ValueError("Offline mailbox is not configured")

        validated = []
        known_users = {
            entry["username"].casefold(): entry["username"]
            for entry in self._user_directory(sender)
        }
        event_id = message.get("event_id")
        timestamp = message.get("timestamp")
        event_order = message.get("event_order")
        content = message.get("content")
        if (
            not isinstance(event_id, str)
            or not 1 <= len(event_id) <= 64
            or isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or not 0 <= timestamp <= 4102444800
            or isinstance(event_order, bool)
            or not isinstance(event_order, int)
            or event_order < 1
            or not isinstance(content, str)
            or len(content) > 12000
        ):
            raise ValueError("Invalid offline message fields")
        try:
            if not base64.b64decode(content, validate=True):
                raise ValueError("Offline message payload cannot be empty")
        except (ValueError, TypeError) as exc:
            raise ValueError("Invalid encrypted offline message payload") from exc
        for recipient_info in recipients:
            if not isinstance(recipient_info, dict):
                raise ValueError("Invalid offline message")
            recipient = recipient_info.get("username")
            stream_id = recipient_info.get("stream_id")
            if (
                not isinstance(recipient, str)
                or recipient.casefold() not in known_users
                or isinstance(stream_id, bool)
                or not isinstance(stream_id, int)
                or not 0 <= stream_id <= 0xFFFFFFFF
            ):
                raise ValueError("Invalid offline message fields")
            validated.append(
                (recipient.casefold(), known_users[recipient.casefold()],
                 stream_id, event_id, timestamp, event_order, content)
            )

        stored = []
        with self._auth_lock, sqlite3.connect(self.auth_db_path) as database:
            for recipient_key, recipient, stream_id, event_id, timestamp, event_order, content in validated:
                mail_id = uuid.uuid4().hex
                database.execute(
                    "INSERT OR IGNORE INTO offline_mailbox "
                    "(mail_id, sender_key, sender, recipient_key, recipient, event_id, "
                    "event_timestamp, event_order, stream_id, message_type, content, "
                    "filename, filesize, attachment_path, expires_at, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'chat', ?, '', 0, NULL, NULL, ?)",
                    (
                        mail_id, sender.username.casefold(), sender.username,
                        recipient_key, recipient, event_id, timestamp, event_order,
                        stream_id, content, time.time(),
                    ),
                )
                row = database.execute(
                    "SELECT mail_id, sender, event_id, event_timestamp, event_order, "
                    "stream_id, message_type, content, filename, filesize, "
                    "sha256, attachment_path, expires_at FROM offline_mailbox "
                    "WHERE recipient_key = ? AND event_id = ?",
                    (recipient_key, event_id),
                ).fetchone()
                if row and row[0] == mail_id:
                    stored.append((recipient_key, row))
        for recipient_key, row in stored:
            self._notify_mailbox_user(recipient_key, row)
        return len(stored)

    def _store_offline_file(self, sender, upload, request_id):
        if not self._valid_request_id(request_id):
            raise ValueError("Invalid mailbox request ID")
        if not upload or upload["received_size"] != upload["filesize"]:
            raise ValueError("Offline file size does not match its metadata")
        if upload["received_chunks"] != upload["total_chunks"]:
            raise ValueError("Offline file is missing chunks")

        final_path = os.path.join(self._mailbox_directory(), f"{upload['file_id']}.bin")
        os.replace(upload["temporary_path"], final_path)
        expires_at = time.time() + 60
        stored = []
        try:
            with self._auth_lock, sqlite3.connect(self.auth_db_path) as database:
                for recipient_key, recipient, stream_id in upload["recipients"]:
                    mail_id = uuid.uuid4().hex
                    database.execute(
                        "INSERT OR IGNORE INTO offline_mailbox "
                        "(mail_id, sender_key, sender, recipient_key, recipient, event_id, "
                        "event_timestamp, event_order, stream_id, message_type, content, "
                        "filename, filesize, sha256, attachment_path, expires_at, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'file', '', ?, ?, ?, ?, ?, ?)",
                        (
                            mail_id, sender.username.casefold(), sender.username,
                            recipient_key, recipient, upload["event_id"],
                            upload["timestamp"], upload["event_order"], stream_id,
                            upload["filename"], upload["display_filesize"],
                            upload["sha256"], final_path,
                            expires_at, time.time(),
                        ),
                    )
                    row = database.execute(
                        "SELECT mail_id, sender, event_id, event_timestamp, event_order, "
                        "stream_id, message_type, content, filename, filesize, "
                        "sha256, attachment_path, expires_at FROM offline_mailbox "
                        "WHERE recipient_key = ? AND event_id = ?",
                        (recipient_key, upload["event_id"]),
                    ).fetchone()
                    if row and row[0] == mail_id:
                        stored.append((recipient_key, row))
        except Exception:
            try:
                os.remove(final_path)
            except OSError:
                pass
            raise
        for recipient_key, row in stored:
            self._notify_mailbox_user(recipient_key, row)
        if not stored:
            try:
                os.remove(final_path)
            except OSError:
                pass
        return len(stored)

    def _notify_mailbox_user(self, recipient_key, row):
        with self._lock:
            recipient = next(
                (
                    user for user in self._users.values()
                    if user.username.casefold() == recipient_key
                ),
                None,
            )
        if recipient:
            try:
                self._send_json(
                    recipient.connection,
                    self._mailbox_event(row),
                    recipient.send_lock,
                )
            except OSError as exc:
                print(f"[!] Could not push an offline mailbox event: {exc}")

    @staticmethod
    def _mailbox_event(row):
        (
            mail_id, sender, event_id, timestamp, event_order, stream_id,
            message_type, content, filename, filesize, checksum,
            attachment_path, expires_at,
        ) = row
        return {
            "type": "mailbox_event",
            "mail_id": mail_id,
            "sender": sender,
            "event_id": event_id,
            "timestamp": timestamp,
            "event_order": event_order,
            "stream_id": stream_id,
            "message_type": message_type,
            "content": content,
            "filename": filename,
            "filesize": filesize,
            "sha256": checksum,
            "file_available": bool(attachment_path and expires_at and expires_at > time.time()),
            "expires_at": expires_at,
        }

    def _get_offline_messages(self, username_key, offset, limit):
        self._expire_mailbox_files()
        with sqlite3.connect(self.auth_db_path) as database:
            rows = database.execute(
                "SELECT mail_id, sender, event_id, event_timestamp, event_order, "
                "stream_id, message_type, content, filename, filesize, "
                "sha256, attachment_path, expires_at FROM offline_mailbox "
                "WHERE recipient_key = ? ORDER BY event_timestamp, event_order, mail_id "
                "LIMIT ? OFFSET ?",
                (username_key, limit, offset),
            ).fetchall()
            total = database.execute(
                "SELECT COUNT(*) FROM offline_mailbox WHERE recipient_key = ?",
                (username_key,),
            ).fetchone()[0]
        messages = []
        for row in rows:
            item = self._mailbox_event(row)
            item.pop("type")
            item["expired"] = (
                row[6] == "file"
                and not item["file_available"]
            )
            messages.append(item)
        return messages, total

    def _expire_mailbox_files(self):
        if not self.auth_db_path or not os.path.isfile(self.auth_db_path):
            return
        now = time.time()
        with self._auth_lock, sqlite3.connect(self.auth_db_path) as database:
            paths = [
                row[0]
                for row in database.execute(
                    "SELECT DISTINCT attachment_path FROM offline_mailbox "
                    "WHERE attachment_path IS NOT NULL AND expires_at <= ?",
                    (now,),
                ).fetchall()
            ]
            database.execute(
                "UPDATE offline_mailbox SET attachment_path = NULL "
                "WHERE attachment_path IS NOT NULL AND expires_at <= ?",
                (now,),
            )
        for path in paths:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            except OSError as exc:
                print(f"[!] Could not remove expired mailbox file {path}: {exc}")

    def _read_offline_file(self, username_key, mail_id):
        self._expire_mailbox_files()
        with self._auth_lock, sqlite3.connect(self.auth_db_path) as database:
            row = database.execute(
                "SELECT attachment_path, expires_at FROM offline_mailbox "
                "WHERE recipient_key = ? AND mail_id = ? AND message_type = 'file'",
                (username_key, mail_id),
            ).fetchone()
        if not row or not row[0] or not row[1] or row[1] <= time.time():
            raise FileNotFoundError("File đã hết hạn")
        path = row[0]
        if not os.path.isfile(path):
            raise FileNotFoundError("File đã hết hạn")
        return path

    def _new_session_id(self):
        while True:
            session_id = secrets.randbits(32)
            if session_id and session_id not in self._users:
                return session_id

    @staticmethod
    def _send_json(connection: socket.socket, message: dict, lock=None):
        encoded = json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"
        if lock:
            with lock:
                connection.sendall(encoded)
        else:
            connection.sendall(encoded)

    def _broadcast_roster(self):
        with self._lock:
            users = list(self._users.values())
            roster = [self._user_record(user) for user in users]
        message = {"type": "roster", "users": roster}
        for user in users:
            try:
                self._send_json(user.connection, message, user.send_lock)
            except OSError:
                pass

    def _relay_loop(self):
        next_mailbox_cleanup = time.monotonic()
        while self.running:
            if time.monotonic() >= next_mailbox_cleanup:
                try:
                    self._expire_mailbox_files()
                except sqlite3.Error as exc:
                    print(f"[!] Could not clean expired offline files: {exc}")
                next_mailbox_cleanup = time.monotonic() + 1
            try:
                data, address = self._relay_socket.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                if self.running:
                    raise
                break
            try:
                self._handle_relay(data, address)
            except (ValueError, TypeError, KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                self._record_network_warning(
                    address,
                    f"Invalid relay packet: {type(exc).__name__}",
                )
                continue

    def _handle_relay(self, data: bytes, address):
        if not data.startswith(RELAY_PREFIX):
            self._record_network_warning(address, "Unknown UDP datagram")
            return
        envelope = json.loads(data[len(RELAY_PREFIX):].decode("utf-8"))
        if envelope.get("type") != "relay":
            self._record_network_warning(address, "Unknown relay envelope type")
            return
        sender_session_id = envelope.get("sender_session_id")
        target_session_id = envelope.get("target_session_id")
        if (
            isinstance(sender_session_id, bool)
            or not isinstance(sender_session_id, int)
            or not 1 <= sender_session_id <= 0xFFFFFFFF
        ):
            self._record_network_warning(address, "Invalid sender session ID")
            return
        if (
            isinstance(target_session_id, bool)
            or not isinstance(target_session_id, int)
            or not 0 <= target_session_id <= 0xFFFFFFFF
        ):
            self._record_network_warning(address, "Invalid target session ID")
            return
        packet_bytes = base64.b64decode(envelope["packet"], validate=True)
        packet = Packet.unpack(packet_bytes)
        with self._lock:
            sender = self._users.get(sender_session_id)
            if not sender or sender.ip != address[0] or sender.udp_port != address[1]:
                self._record_network_warning(address, "Relay sender authentication failed")
                return
            if packet.session_id != sender_session_id:
                self._record_network_warning(address, "Packet session does not match sender")
                return
            if packet.flags & FLAG_MSG:
                if packet.flags & FLAG_ACK:
                    return
                if target_session_id == 0:
                    recipients = [user for user in self._users.values() if user.session_id != sender_session_id]
                else:
                    recipient = self._users.get(target_session_id)
                    recipients = [recipient] if recipient else []
            elif packet.flags & FLAG_FILE:
                if packet.flags & FLAG_ACK or target_session_id == 0:
                    return
                recipient = self._users.get(target_session_id)
                recipients = [recipient] if recipient else []
            elif packet.flags & FLAG_ACK:
                recipient = self._users.get(target_session_id)
                recipients = [recipient] if recipient else []
            else:
                self._record_network_warning(address, "Unsupported packet flags")
                return
            file_info = self._validated_file_info(envelope.get("file")) if packet.flags & FLAG_FILE else None
            if packet.flags & FLAG_FILE and file_info is None:
                self._record_network_warning(address, "Missing or invalid file relay metadata")
                return
            retry_count = envelope.get("retry_count", 0)
            if packet.flags & FLAG_FILE and (
                isinstance(retry_count, bool)
                or not isinstance(retry_count, int)
                or not 0 <= retry_count <= 12
            ):
                self._record_network_warning(address, "Invalid file retry count")
                return
            if packet.flags & FLAG_FILE and not (
                file_info["first_seq"]
                <= packet.seq_num
                <= file_info["first_seq"] + file_info["total_chunks"]
            ):
                self._record_network_warning(address, "File sequence is outside declared chunk range")
                return

        delivery = RELAY_PREFIX + json.dumps({
            "type": "deliver",
            "sender_session_id": sender_session_id,
            "packet": base64.b64encode(packet_bytes).decode("ascii"),
        }, separators=(",", ":")).encode("utf-8")
        for recipient in recipients:
            try:
                self._relay_socket.sendto(delivery, (recipient.ip, recipient.udp_port))
                with self._lock:
                    self._metrics["relay_datagrams"] += 1
                    self._metrics["relay_bytes"] += len(delivery)
                    kind = "chat" if packet.flags & FLAG_MSG else "file" if packet.flags & FLAG_FILE else "ack"
                    self._throughput_samples.append({
                        "timestamp": time.time(),
                        "bytes": len(delivery),
                        "kind": kind,
                    })
                    if packet.flags & FLAG_MSG:
                        self._metrics["chat_relays"] += 1
                        metric = (
                            "global_chat_relays"
                            if target_session_id == 0
                            else "private_chat_relays"
                        )
                        self._metrics[metric] += 1
                    elif packet.flags & FLAG_FILE:
                        self._metrics["file_relays"] += 1
                        self._update_file_transfer(
                            file_info,
                            sender,
                            recipient,
                            packet,
                            retry_count,
                        )
                    else:
                        self._metrics["ack_relays"] += 1
                        self._update_file_ack(sender, recipient, packet)
                event_type = (
                    "chat_relayed"
                    if packet.flags & FLAG_MSG
                    else "file_relayed"
                    if packet.flags & FLAG_FILE
                    else "ack_relayed"
                )
                self._record_event(event_type, {
                    "sender": sender.username,
                    "recipient": recipient.username,
                    "bytes": len(packet_bytes),
                    "stream_id": packet.stream_id,
                })
            except OSError:
                with self._lock:
                    self._metrics["relay_drops"] += 1
                self._record_event("relay_dropped", {
                    "sender_session_id": sender_session_id,
                    "target_session_id": recipient.session_id,
                })
                continue

    @staticmethod
    def _validated_file_info(value):
        if not isinstance(value, dict):
            return None
        transfer_id = value.get("transfer_id")
        filename = value.get("filename")
        filesize = value.get("filesize")
        total_chunks = value.get("total_chunks")
        first_seq = value.get("first_seq")
        if (
            not isinstance(transfer_id, str)
            or not transfer_id
            or len(transfer_id) > 64
            or not isinstance(filename, str)
            or not filename
            or len(filename) > 255
            or os.path.basename(filename.replace("\\", "/")) != filename
            or isinstance(filesize, bool)
            or not isinstance(filesize, int)
            or not 0 <= filesize <= 200 * 1024 * 1024
            or isinstance(total_chunks, bool)
            or not isinstance(total_chunks, int)
            or total_chunks != (filesize + 1349) // 1350
            or isinstance(first_seq, bool)
            or not isinstance(first_seq, int)
            or not 1 <= first_seq <= 0xFFFFFFFF
            or first_seq + total_chunks > 0xFFFFFFFF
        ):
            return None
        return {
            "transfer_id": transfer_id,
            "filename": filename,
            "filesize": filesize,
            "total_chunks": total_chunks,
            "first_seq": first_seq,
        }

    def _update_file_transfer(
        self,
        file_info,
        sender,
        recipient,
        packet,
        retry_count: int,
    ):
        key = (
            file_info["transfer_id"],
            sender.session_id,
            recipient.session_id,
            packet.stream_id,
        )
        transfer = self._file_transfers.get(key)
        if transfer is None:
            transfer = {
                "file_id": file_info["transfer_id"],
                "filename": file_info["filename"],
                "filesize": file_info["filesize"],
                "sender": sender.username,
                "recipient": recipient.username,
                "sender_session_id": sender.session_id,
                "recipient_session_id": recipient.session_id,
                "stream_id": packet.stream_id,
                "total_chunks": file_info["total_chunks"],
                "chunks_forwarded": 0,
                "chunks_acked": 0,
                "retransmissions": 0,
                "status": "TRANSFERRING",
                "updated_at": time.time(),
                "_first_seq": file_info["first_seq"],
                "_seen_sequences": bytearray(
                    (file_info["total_chunks"] + 8) // 8
                ),
                "_retry_counts": {},
            }
            self._file_transfers[key] = transfer
            if len(self._file_transfers) > 100:
                oldest_key = min(
                    self._file_transfers,
                    key=lambda item: self._file_transfers[item]["updated_at"],
                )
                if oldest_key != key:
                    del self._file_transfers[oldest_key]

        seen = transfer["_seen_sequences"]
        sequence_offset = packet.seq_num - transfer["_first_seq"]
        if not 0 <= sequence_offset <= transfer["total_chunks"]:
            self._record_network_warning(
                (sender.ip, sender.udp_port),
                "File sequence is outside declared chunk range",
            )
            return
        byte_index, bit_index = divmod(sequence_offset, 8)
        bit_mask = 1 << bit_index
        previous_retries = transfer["_retry_counts"].get(packet.seq_num, 0)
        if retry_count > previous_retries:
            transfer["retransmissions"] += retry_count - previous_retries
            transfer["_retry_counts"][packet.seq_num] = retry_count
        if not seen[byte_index] & bit_mask:
            seen[byte_index] |= bit_mask
            if sequence_offset > 0:
                transfer["chunks_forwarded"] = min(
                    transfer["total_chunks"],
                    transfer["chunks_forwarded"] + 1,
                )
        transfer["updated_at"] = time.time()
        if transfer["total_chunks"] == 0:
            transfer["status"] = "COMPLETED"

    def _update_file_ack(self, sender, recipient, packet):
        for transfer in self._file_transfers.values():
            if (
                transfer["sender_session_id"] == recipient.session_id
                and transfer["recipient_session_id"] == sender.session_id
                and transfer["stream_id"] == packet.stream_id
            ):
                first_chunk_seq = transfer["_first_seq"] + 1
                transfer["chunks_acked"] = max(
                    transfer["chunks_acked"],
                    min(
                        transfer["total_chunks"],
                        max(0, packet.ack_num - first_chunk_seq),
                    ),
                )
                transfer["updated_at"] = time.time()
                if transfer["chunks_acked"] >= transfer["total_chunks"]:
                    transfer["status"] = "COMPLETED"
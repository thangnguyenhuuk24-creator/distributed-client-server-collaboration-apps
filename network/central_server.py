import base64
import json
import os
import secrets
import socket
import threading
import time
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
    ):
        self.host = host
        self.tcp_port = tcp_port
        self.relay_port = relay_port
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
        }
        self._events = deque(maxlen=100)
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
            username = request.get("username")
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
                session_id = self._new_session_id()
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
                "session_id": session_id,
                "peers": peers,
            }, user.send_lock)
            self._broadcast_roster()
            while self.running:
                line = reader.readline(4097)
                if not line:
                    break  # client đóng kết nối
                if len(line) > 4096 or not line.endswith(b"\n"):
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
        while self.running:
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
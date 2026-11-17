import base64
import json
import os
import secrets
import socket
import sys
import threading
import time
import uuid
from typing import Callable, Dict, Optional, Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.protocol import (
    FLAG_ACK,
    FLAG_DISCOV,
    FLAG_FILE,
    FLAG_HEARTBEAT,
    FLAG_MSG,
    Packet,
    decode_sack_blocks,
    encode_sack_blocks,
)
from network.central_client import CentralClientConnection
from network.central_server import RELAY_PREFIX
from core.crypto import CryptoEngine
from core.state import ClientState
from network.discovery import DiscoveryService
from network.reliability import SlidingWindowSender, StreamReceiver
from network.streaming import FileTransferManager, StreamManager


class SocketEngine:
    """UDP socket engine with per-peer sliding-window reliability."""

    def __init__(
        self,
        state: ClientState,
        host: str = "0.0.0.0",
        port: int = 9000,
        discovery_port: int = 9004,
        server_host: Optional[str] = None,
        server_tcp_port: int = 9001,
        server_udp_port: int = 9002,
        server_name: str = "",
    ):
        self.state = state
        self.host = host
        self.port = port
        self.server_host = server_host
        self.server_tcp_port = server_tcp_port
        self.server_udp_port = server_udp_port
        self.server_name = server_name
        self._mode_lock = threading.RLock()
        self._central_active = bool(server_host)
        self._central_fallback = False
        self._fallback_allowed_session_ids = set()
        self._active_username = ""
        self._active_password = None
        self._active_session_id = 0
        self._logged_in = False
        self._reconnect_stop = threading.Event()
        self._reconnect_thread = None
        self.coordinator = (
            CentralClientConnection(
                state,
                server_host,
                server_tcp_port,
                port,
                on_disconnect=self._handle_central_disconnect,
                server_name=server_name,
                on_mailbox_event=self._handle_mailbox_event,
            )
            if server_host
            else None
        )
        self._relay_endpoint = (
            (socket.gethostbyname(server_host), server_udp_port)
            if server_host
            else None
        )
        self.running = False

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        self.crypto = CryptoEngine()
        self.stream_manager = StreamManager(download_dir="storage/downloads")
        self.discovery = DiscoveryService(
            state,
            self.send_raw,
            broadcast_port=discovery_port,
            local_port=port,
        )
        self._reliability_lock = threading.RLock()
        self._outgoing_senders: Dict[Tuple[int, str, int], SlidingWindowSender] = {}
        self._incoming_receivers: Dict[Tuple[int, str, int], StreamReceiver] = {}
        self._relay_targets: Dict[Tuple[int, str, int], int] = {}
        self._relay_file_info: Dict[Tuple[int, str, int], dict] = {}
        self._relay_retry_counts: Dict[Tuple[int, str, int], Dict[int, int]] = {}
        self.recv_thread: Optional[threading.Thread] = None
        self.timer_thread: Optional[threading.Thread] = None

    @property
    def using_central_server(self) -> bool:
        with self._mode_lock:
            return self._central_active

    def start(self):
        self.sock.bind((self.host, self.port))
        self.running = True
        try:
            self.recv_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self.recv_thread.start()
            self.timer_thread = threading.Thread(target=self._retransmit_loop, daemon=True)
            self.timer_thread.start()
            if not self.using_central_server:
                self.discovery.start()
        except OSError:
            self.running = False
            self.sock.close()
            self.discovery.stop()
            raise

    def stop(self):
        self.running = False
        self._logged_in = False
        self._reconnect_stop.set()
        self.discovery.stop()
        if self.coordinator:
            self.coordinator.close()
        try:
            self.sock.close()
        except OSError:
            pass
        for thread in (self.recv_thread, self.timer_thread):
            if thread and thread.is_alive():
                thread.join(timeout=1.0)

    def send_raw(self, raw_bytes: bytes, addr: Tuple[str, int]):
        try:
            self.sock.sendto(raw_bytes, addr)
        except OSError:
            if self.running:
                raise

    def _get_outgoing_sender(
        self,
        stream_id: int,
        dest_ip: str,
        dest_port: int,
    ) -> SlidingWindowSender:
        key = (stream_id, dest_ip, dest_port)
        with self._reliability_lock:
            if key not in self._outgoing_senders:
                self._outgoing_senders[key] = SlidingWindowSender(stream_id)
            return self._outgoing_senders[key]

    def _get_incoming_receiver(
        self,
        stream_id: int,
        sender_ip: str,
        sender_port: int,
    ) -> StreamReceiver:
        key = (stream_id, sender_ip, sender_port)
        with self._reliability_lock:
            if key not in self._incoming_receivers:
                self._incoming_receivers[key] = StreamReceiver(stream_id)
            return self._incoming_receivers[key]

    def configure_central_server(
        self,
        server_host: str,
        server_tcp_port: int,
        server_udp_port: int,
        server_name: str = "",
    ):
        if not server_host or not 1 <= server_tcp_port <= 65535 or not 1 <= server_udp_port <= 65535:
            raise ValueError("Invalid central server address or port")
        if self.coordinator:
            self.coordinator.close()
        self._logged_in = False
        self._reconnect_stop.set()
        self.server_host = server_host
        self.server_tcp_port = server_tcp_port
        self.server_udp_port = server_udp_port
        self.server_name = server_name
        self._relay_endpoint = (socket.gethostbyname(server_host), server_udp_port)
        self.coordinator = CentralClientConnection(
            self.state,
            server_host,
            server_tcp_port,
            self.port,
            on_disconnect=self._handle_central_disconnect,
            server_name=server_name,
            on_mailbox_event=self._handle_mailbox_event,
        )
        with self._mode_lock:
            self._central_active = True
            self._central_fallback = False
            self._fallback_allowed_session_ids.clear()
        self.discovery.stop()

    def configure_p2p(self):
        if self._logged_in:
            raise RuntimeError("Cannot change network mode while logged in")
        with self._mode_lock:
            self._central_active = False
            self._central_fallback = False
            self._fallback_allowed_session_ids.clear()
            self._reconnect_stop.set()
            coordinator = self.coordinator
            self.coordinator = None
            self.server_host = None
            self.server_name = ""
            self._relay_endpoint = None
        if coordinator:
            coordinator.close()
        if not self.discovery.running:
            self.discovery.start()

    def login(self, username: str, password: Optional[str] = None) -> int:
        if self.using_central_server:
            if not self.coordinator:
                raise RuntimeError("Central server must be selected before login")
            self._active_username = username
            self._active_password = password
            self._logged_in = True
            self._reconnect_stop.clear()
            try:
                session_id = self.coordinator.login(username, password)
            except Exception:
                self._logged_in = False
                self._active_username = ""
                self._active_password = None
                raise
            self.server_name = self.coordinator.server_name
            self._active_session_id = session_id
            self._sync_offline_history(self.coordinator)
            return session_id
        session_id = self._login_p2p(username)
        self._active_username = username
        self._active_session_id = session_id
        self._logged_in = True
        return session_id

    def logout(self):
        self._logged_in = False
        self._active_username = ""
        self._active_password = None
        self._active_session_id = 0
        self._reconnect_stop.set()
        if self.coordinator:
            self.coordinator.close()
            self.coordinator = None
        self.state.clear_session_data()

    def _login_p2p(self, username: str) -> int:
        session_id = secrets.randbelow(0xFFFFFFFF) + 1
        self.state.set_online(username, session_id)
        self.discovery.send_broadcast_announce()
        return session_id

    def _handle_central_disconnect(self):
        if not self.using_central_server or not self._logged_in:
            return
        print("[!] Central server connection lost; switching to direct P2P.")
        self._activate_p2p()
        self._start_reconnect_worker()

    def request_user_directory(self):
        if not self.using_central_server or not self.coordinator:
            return []
        return self.coordinator.request_user_directory()

    def sync_offline_mailbox(self):
        if not self.using_central_server or not self.coordinator:
            return
        offset = 0
        while True:
            messages, total = self.coordinator.get_offline_messages(offset)
            for message in messages:
                self._handle_mailbox_event(message)
            offset += len(messages)
            if not messages or offset >= total:
                return

    def queue_offline_message(
        self,
        recipients,
        text,
        event_id,
        timestamp,
        event_order,
    ):
        if not self.using_central_server or not self.coordinator:
            raise ConnectionError("Central server is unavailable")
        payload = b"\x00CHAT1" + json.dumps(
            {
                "text": text,
                "event_id": event_id,
                "timestamp": timestamp,
                "event_order": event_order,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        encrypted = base64.b64encode(self.crypto.encrypt(payload)).decode("ascii")
        return self.coordinator.send_offline_message({
            "recipients": [
                {
                    "username": recipient["username"],
                    "stream_id": recipient["stream_id"],
                }
                for recipient in recipients
            ],
            "event_id": event_id,
            "timestamp": timestamp,
            "event_order": event_order,
            "content": encrypted,
        })

    def queue_offline_file(
        self,
        filepath,
        filename,
        recipients,
        checksum,
        event_id,
        timestamp,
        event_order,
    ):
        if not self.using_central_server or not self.coordinator:
            raise ConnectionError("Central server is unavailable")
        encrypted_path = f"{filepath}.{uuid.uuid4().hex}.encrypted"
        filesize, encrypted_size = self.crypto.encrypt_file(
            filepath,
            encrypted_path,
        )
        try:
            self.coordinator.send_offline_file(
                uuid.uuid4().hex,
                filename,
                encrypted_path,
                filesize,
                encrypted_size,
                checksum,
                recipients,
                event_id,
                timestamp,
                event_order,
            )
        finally:
            try:
                os.remove(encrypted_path)
            except OSError as exc:
                print(f"[!] Could not remove temporary encrypted file: {exc}")

    def download_offline_file(self, mail_id, filesize):
        if not self.using_central_server or not self.coordinator:
            raise ConnectionError("Central server is unavailable")
        encrypted = self.coordinator.download_offline_file(mail_id, filesize)
        return self.crypto.decrypt(encrypted)

    def _handle_mailbox_event(self, message):
        try:
            sender = message["sender"]
            event_id = message["event_id"]
            timestamp = message["timestamp"]
            event_order = message["event_order"]
            stream_id = message["stream_id"]
            if (
                not isinstance(sender, str)
                or not isinstance(event_id, str)
                or isinstance(timestamp, bool)
                or not isinstance(timestamp, (int, float))
                or isinstance(event_order, bool)
                or not isinstance(event_order, int)
                or isinstance(stream_id, bool)
                or not isinstance(stream_id, int)
            ):
                raise ValueError("Invalid offline mailbox event")
            if message.get("message_type") == "chat":
                encrypted = base64.b64decode(message["content"], validate=True)
                plaintext = self.crypto.decrypt(encrypted)
                if not plaintext.startswith(b"\x00CHAT1"):
                    raise ValueError("Invalid offline message payload")
                event = json.loads(plaintext[len(b"\x00CHAT1"):].decode("utf-8"))
                if (
                    not isinstance(event, dict)
                    or not isinstance(event.get("text"), str)
                    or event.get("event_id") != event_id
                    or event.get("timestamp") != timestamp
                    or event.get("event_order") != event_order
                ):
                    raise ValueError("Invalid offline chat event metadata")
                self.state.add_message(
                    stream_id,
                    sender,
                    event["text"],
                    conversation_key=sender if stream_id != 0 else None,
                    event_id=event_id,
                    timestamp=timestamp,
                    event_order=event_order,
                    server_synced=True,
                )
                return
            if message.get("message_type") == "file":
                filename = message["filename"]
                filesize = message["filesize"]
                checksum = message.get("sha256")
                if (
                    not isinstance(filename, str)
                    or not filename
                    or os.path.basename(filename.replace("\\", "/")) != filename
                    or isinstance(filesize, bool)
                    or not isinstance(filesize, int)
                    or not 0 <= filesize <= 200 * 1024 * 1024
                    or not isinstance(checksum, str)
                    or len(checksum) != 64
                    or any(character not in "0123456789abcdef" for character in checksum)
                    or (
                        message.get("expires_at") is not None
                        and (
                            isinstance(message.get("expires_at"), bool)
                            or not isinstance(message.get("expires_at"), (int, float))
                        )
                    )
                ):
                    raise ValueError("Invalid offline file metadata")
                status = "Chờ nhận" if message.get("file_available") else "Hết hạn"
                self.state.add_file_message(
                    stream_id,
                    sender,
                    filename,
                    message["mail_id"],
                    status,
                    filesize,
                    sender if stream_id != 0 else None,
                    event_id=event_id,
                    timestamp=timestamp,
                    event_order=event_order,
                    server_synced=True,
                    sha256=checksum,
                    file_expires_at=message.get("expires_at"),
                )
                if not message.get("file_available"):
                    with self.state._lock:
                        for item in self.state.streams.get(stream_id, []):
                            if item.get("file_id") == message["mail_id"]:
                                item["expired"] = True
                                self.state._persist_message(stream_id, item)
                return
            raise ValueError("Unsupported offline mailbox event type")
        except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            print(f"[!] Could not apply offline mailbox event: {exc}")

    def _start_reconnect_worker(self):
        with self._mode_lock:
            if self._reconnect_thread and self._reconnect_thread.is_alive():
                return
            self._reconnect_thread = threading.Thread(
                target=self._reconnect_loop,
                daemon=True,
            )
            self._reconnect_thread.start()

    def _release_reconnect_worker(self):
        with self._mode_lock:
            if self._reconnect_thread is threading.current_thread():
                self._reconnect_thread = None

    def _reconnect_loop(self):
        while self.running and self._logged_in and not self._reconnect_stop.wait(2.0):
            connection = CentralClientConnection(
                self.state,
                self.server_host,
                self.server_tcp_port,
                self.port,
                on_disconnect=self._handle_central_disconnect,
                server_name=self.server_name,
                resume_session_id=self._active_session_id or None,
                on_mailbox_event=self._handle_mailbox_event,
            )
            try:
                previous = self.coordinator
                self.coordinator = connection
                if previous:
                    previous.close()
                session_id = connection.login(
                    self._active_username,
                    self._active_password,
                )
                if not self._logged_in or self._reconnect_stop.is_set():
                    connection.close()
                    self._release_reconnect_worker()
                    return
                self._active_session_id = session_id
                with self._mode_lock:
                    self._central_active = True
                    self._central_fallback = False
                    self._fallback_allowed_session_ids.clear()
                self.discovery.stop()
                self._sync_offline_history(connection)
                self.sync_offline_mailbox()
                if not self.using_central_server:
                    connection.close()
                    if self.coordinator is connection:
                        self.coordinator = None
                    continue
                print("[+] Reconnected to the central server.")
                self._release_reconnect_worker()
                return
            except PermissionError as exc:
                connection.close()
                if self.coordinator is connection:
                    self.coordinator = None
                print(f"[!] Central server rejected reconnection: {exc}")
                self._release_reconnect_worker()
                return
            except (OSError, ConnectionError, TimeoutError, ValueError) as exc:
                connection.close()
                if self.coordinator is connection:
                    self.coordinator = None
                if self._logged_in and not self._reconnect_stop.is_set():
                    print(f"[!] Central server is still unavailable: {exc}")

    @staticmethod
    def _sync_offline_history(connection):
        try:
            connection.sync_offline_history()
        except (ConnectionError, TimeoutError, OSError, ValueError) as exc:
            print(f"[!] Could not sync offline history with the central server: {exc}")

    def _activate_p2p(self):
        with self._mode_lock:
            if not self._central_active:
                return
            self._central_active = False
            with self.state._lock:
                self._fallback_allowed_session_ids = {
                    peer["session_id"]
                    for peer in self.state.peers.values()
                    if isinstance(peer.get("session_id"), int)
                    and not isinstance(peer.get("session_id"), bool)
                    and peer["session_id"] > 0
                }
                if self.state.session_id:
                    self._fallback_allowed_session_ids.add(self.state.session_id)
            self._central_fallback = True

        self.state.replace_peers([])
        with self._reliability_lock:
            for key, sender in list(self._outgoing_senders.items()):
                if key[1].startswith("session:"):
                    sender.failed = True
                    self._outgoing_senders.pop(key, None)
                    self._relay_targets.pop(key, None)
                    self._relay_file_info.pop(key, None)
                    self._relay_retry_counts.pop(key, None)
            for key in list(self._incoming_receivers):
                if key[1].startswith("session:"):
                    self._incoming_receivers.pop(key, None)

        try:
            self.discovery.start()
            self.discovery.send_broadcast_announce()
        except OSError as exc:
            print(f"[!] Could not start P2P discovery: {exc}")

    def _register_congestion_controlled_packet(
        self,
        sender: SlidingWindowSender,
        flags: int,
        payload: bytes,
        timeout: Optional[float] = None,
        on_wait: Optional[Callable[[], None]] = None,
    ) -> Packet:
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            if not self.running:
                raise ConnectionError("Socket engine stopped while waiting for congestion window")
            if sender.failed:
                raise ConnectionError("Peer không phản hồi (vượt quá số lần gửi lại)")
            with sender.lock:
                if sender.can_send():
                    return sender.register_packet(
                        flags=flags,
                        session_id=self.state.session_id,
                        payload=payload,
                    )
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for congestion window capacity")
            if on_wait:
                on_wait()
            time.sleep(0.01)

    def send_message(
        self,
        dest_ip: str,
        dest_port: int,
        stream_id: int,
        message_text: str,
        target_session_id: Optional[int] = None,
        event_id: Optional[str] = None,
        event_timestamp: Optional[float] = None,
        event_order: Optional[int] = None,
    ):
        """Send an encrypted chat message directly or through the central UDP relay."""
        if event_id is None or event_timestamp is None or event_order is None:
            generated_id, generated_timestamp, generated_order = self.state.next_event_metadata()
            event_id = event_id or generated_id
            event_timestamp = generated_timestamp if event_timestamp is None else event_timestamp
            event_order = generated_order if event_order is None else event_order
        message_payload = b"\x00CHAT1" + json.dumps(
            {
                "text": message_text,
                "event_id": event_id,
                "timestamp": event_timestamp,
                "event_order": event_order,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if self.using_central_server:
            if stream_id == 0:
                target_session_id = 0
            elif target_session_id is None:
                with self.state._lock:
                    target = next(
                        (
                            peer for peer in self.state.peers.values()
                            if peer["ip"] == dest_ip and peer["port"] == dest_port
                        ),
                        None,
                    )
                if target:
                    target_session_id = target["session_id"]
            if target_session_id is None or (stream_id != 0 and target_session_id == 0):
                raise ValueError("A valid online recipient is required for a private message")
            encrypted_payload = self.crypto.encrypt(message_payload)
            if target_session_id == 0:
                with self.state._lock:
                    target_session_ids = [
                        peer["session_id"]
                        for peer in self.state.peers.values()
                        if peer["session_id"] != self.state.session_id
                    ]
            else:
                target_session_ids = [target_session_id]

            for recipient_session_id in target_session_ids:
                key = (stream_id, f"session:{recipient_session_id}", 0)
                sender = self._get_outgoing_sender(stream_id, key[1], key[2])
                packet = self._register_congestion_controlled_packet(
                    sender,
                    flags=FLAG_MSG,
                    payload=encrypted_payload,
                    timeout=5.0,
                )
                with self._reliability_lock:
                    self._relay_targets[key] = recipient_session_id
                self._send_relay(packet, recipient_session_id)
            return

        sender = self._get_outgoing_sender(stream_id, dest_ip, dest_port)
        packet = self._register_congestion_controlled_packet(
            sender,
            flags=FLAG_MSG,
            payload=self.crypto.encrypt(message_payload),
            timeout=5.0,
        )
        self.send_raw(packet.pack(), (dest_ip, dest_port))

    def send_file(
        self,
        dest_ip: str,
        dest_port: int,
        stream_id: int,
        filepath: str,
        chat_stream_id: int = 0,
        transfer_id: Optional[str] = None,
        filename: Optional[str] = None,
        target_session_id: Optional[int] = None,
        event_id: Optional[str] = None,
        event_timestamp: Optional[float] = None,
        event_order: Optional[int] = None,
    ):
        """Send a file directly or through the central UDP relay."""
        metadata, chunks = FileTransferManager.prepare_file_for_sending(filepath)
        if filename:
            metadata["filename"] = filename
        if transfer_id:
            metadata["transfer_id"] = transfer_id
        metadata["chat_stream_id"] = chat_stream_id
        if event_id is None or event_timestamp is None or event_order is None:
            generated_id, generated_timestamp, generated_order = self.state.next_event_metadata()
            event_id = event_id or generated_id
            event_timestamp = generated_timestamp if event_timestamp is None else event_timestamp
            event_order = generated_order if event_order is None else event_order
        metadata["event_id"] = event_id
        metadata["event_timestamp"] = event_timestamp
        metadata["event_order"] = event_order

        relay_target_session_id = None
        sender_ip, sender_port = dest_ip, dest_port
        if self.using_central_server:
            if target_session_id is None:
                with self.state._lock:
                    target = next(
                        (
                            peer for peer in self.state.peers.values()
                            if peer["ip"] == dest_ip and peer["port"] == dest_port
                        ),
                        None,
                    )
                if target:
                    target_session_id = target["session_id"]
            if (
                isinstance(target_session_id, bool)
                or not isinstance(target_session_id, int)
                or not 1 <= target_session_id <= 0xFFFFFFFF
                or target_session_id == self.state.session_id
            ):
                raise ValueError("A valid online recipient is required for a file transfer")
            with self.state._lock:
                if not any(
                    peer["session_id"] == target_session_id
                    for peer in self.state.peers.values()
                ):
                    raise ValueError("The file recipient is no longer online")
            relay_target_session_id = target_session_id
            sender_ip, sender_port = f"session:{target_session_id}", 0

        sender = self._get_outgoing_sender(stream_id, sender_ip, sender_port)
        sender_key = (stream_id, sender_ip, sender_port)
        relay_file_info = {
            "transfer_id": str(transfer_id or stream_id),
            "filename": metadata["filename"],
            "filesize": metadata["filesize"],
            "total_chunks": metadata["total_chunks"],
        }
        packets = [self.crypto.encrypt(FileTransferManager.encode_file_init(metadata))]
        packets.extend(self.crypto.encrypt(chunk) for chunk in chunks)
        with sender.lock:
            first_data_seq = sender.next_seq_num + 1
        relay_file_info["first_seq"] = first_data_seq - 1

        last_reported_chunks = -1

        def update_send_progress() -> int:
            nonlocal last_reported_chunks
            with sender.lock:
                acknowledged_chunks = min(
                    len(chunks),
                    max(0, sender.base_seq_num - first_data_seq),
                )
            if (
                transfer_id
                and acknowledged_chunks != last_reported_chunks
            ):
                self.state.update_file_progress(
                    file_id=transfer_id,
                    filename=metadata["filename"],
                    total_size=metadata["filesize"],
                    received_chunks=acknowledged_chunks,
                    total_chunks=len(chunks),
                    status="SENDING",
                )
                last_reported_chunks = acknowledged_chunks
            return acknowledged_chunks

        for payload in packets:
            packet = self._register_congestion_controlled_packet(
                sender,
                flags=FLAG_FILE,
                payload=payload,
                timeout=5.0,
                on_wait=update_send_progress,
            )
            if relay_target_session_id is not None:
                with self._reliability_lock:
                    self._relay_targets[sender_key] = relay_target_session_id
                    self._relay_file_info[sender_key] = relay_file_info
                self._send_relay(packet, relay_target_session_id, relay_file_info)
            else:
                self.send_raw(packet.pack(), (dest_ip, dest_port))
            update_send_progress()

        while True:
            update_send_progress()
            with sender.lock:
                all_packets_acknowledged = not sender.unacked_packets
            if all_packets_acknowledged:
                break
            if sender.failed:
                raise ConnectionError("Peer không phản hồi, hủy truyền file")
            if not self.running:
                raise ConnectionError("Socket engine stopped before file was acknowledged")
            time.sleep(0.01)

        if transfer_id:
            self.state.update_file_message(transfer_id, "Đã gửi")
        if relay_target_session_id is not None:
            with self._reliability_lock:
                self._relay_targets.pop(sender_key, None)
                self._relay_file_info.pop(sender_key, None)
                self._relay_retry_counts.pop(sender_key, None)

    def _receive_loop(self):
        while self.running:
            try:
                raw_data, addr = self.sock.recvfrom(65535)
            except ConnectionResetError:
                if self.running:
                    continue
                break
            except OSError:
                if self.running:
                    print("[!] UDP receive socket failed")
                break
            if not raw_data:
                continue
            try:
                sender_session_id = None
                if raw_data.startswith(RELAY_PREFIX):
                    if not self.using_central_server:
                        continue
                    envelope = json.loads(raw_data[len(RELAY_PREFIX):].decode("utf-8"))
                    if not isinstance(envelope, dict) or envelope.get("type") != "deliver":
                        continue
                    sender_session_id = envelope.get("sender_session_id")
                    raw_data = base64.b64decode(envelope["packet"], validate=True)
                    if addr != self._relay_endpoint:
                        continue
                packet = Packet.unpack(raw_data)
                if (
                    self._central_fallback
                    and not packet.flags & FLAG_DISCOV
                    and packet.session_id not in self._fallback_allowed_session_ids
                ):
                    continue
                self._dispatch_packet(packet, addr, sender_session_id)
            except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError, AttributeError) as exc:
                print(f"[!] Invalid UDP packet from {addr[0]}:{addr[1]}: {exc}")
                if self.using_central_server and addr == self._relay_endpoint:
                    self.coordinator.report_warning("invalid_packet")

    def _dispatch_packet(
        self,
        packet: Packet,
        addr: Tuple[str, int],
        sender_session_id: Optional[int] = None,
    ):
        sender_ip, sender_port = addr

        if packet.flags & FLAG_ACK:
            if self.using_central_server and sender_session_id is not None:
                key = (packet.stream_id, f"session:{sender_session_id}", 0)
            else:
                key = (packet.stream_id, sender_ip, sender_port)
            with self._reliability_lock:
                sender = self._outgoing_senders.get(key)
            if sender:
                sender.on_ack_received(
                    packet.ack_num,
                    decode_sack_blocks(packet.payload),
                )
            return

        if packet.flags & FLAG_DISCOV:
            try:
                info = json.loads(packet.payload.decode("utf-8"))
                username = info.get("username")
                if (
                    self._central_fallback
                    and packet.session_id not in self._fallback_allowed_session_ids
                ):
                    return
                if username and username != self.state.username:
                    udp_port = int(info.get("udp_port", sender_port))
                    if 1 <= udp_port <= 65535:
                        self.state.update_peer(sender_ip, udp_port, username, packet.session_id)
            except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
                return
            return

        if packet.flags & FLAG_HEARTBEAT:
            with self.state._lock:
                for peer in self.state.peers.values():
                    if peer["ip"] == sender_ip and peer["port"] == sender_port:
                        peer["last_seen"] = time.time()
            return

        receiver_ip, receiver_port = sender_ip, sender_port
        if sender_session_id is not None:
            receiver_ip = f"session:{sender_session_id}"
            receiver_port = 0
        receiver = self._get_incoming_receiver(
            packet.stream_id,
            receiver_ip,
            receiver_port,
        )
        ready_chunks, cumulative_ack, sack_blocks = receiver.process_incoming_packet(
            packet.seq_num,
            packet.payload,
        )
        if packet.flags & FLAG_MSG:
            sender_name = (
                self._peer_username_by_session(sender_session_id)
                if sender_session_id is not None
                else self._peer_username(sender_ip, sender_port)
            )
            for chunk in ready_chunks:
                try:
                    plaintext = self.crypto.decrypt(chunk)
                    event_id = None
                    event_timestamp = None
                    event_order = None
                    if plaintext.startswith(b"\x00CHAT1"):
                        event = json.loads(plaintext[len(b"\x00CHAT1"):].decode("utf-8"))
                        if not isinstance(event, dict):
                            raise ValueError("Invalid chat event")
                        text = event.get("text")
                        event_id = event.get("event_id")
                        event_timestamp = event.get("timestamp")
                        event_order = event.get("event_order")
                        if (
                            not isinstance(text, str)
                            or not isinstance(event_id, str)
                            or isinstance(event_timestamp, bool)
                            or not isinstance(event_timestamp, (int, float))
                            or not 0 <= event_timestamp <= 4102444800
                            or isinstance(event_order, bool)
                            or not isinstance(event_order, int)
                            or event_order < 1
                        ):
                            raise ValueError("Invalid chat event metadata")
                    else:
                        text = plaintext.decode("utf-8")
                except (ValueError, UnicodeDecodeError) as exc:
                    print(f"[!] Bỏ qua tin nhắn không giải mã được: {exc}")
                    if self.using_central_server and self.coordinator:
                        self.coordinator.report_warning("decrypt_failure")
                    continue
                self.state.add_message(
                    packet.stream_id,
                    sender_name,
                    text,
                    conversation_key=sender_name if packet.stream_id != 0 else None,
                    event_id=event_id,
                    timestamp=event_timestamp,
                    event_order=event_order,
                    server_synced=sender_session_id is not None,
                )
        elif packet.flags & FLAG_FILE:
            for chunk in ready_chunks:
                try:
                    self._handle_file_chunk(
                        packet.stream_id,
                        chunk,
                        sender_ip,
                        sender_port,
                        sender_session_id,
                    )
                except (ValueError, KeyError, TypeError, OSError) as exc:
                    self._fail_incoming_file(packet.stream_id, exc)
                    if self.using_central_server and self.coordinator:
                        self.coordinator.report_warning("file_decode_failure")
        else:
            return

        ack = Packet(
            flags=FLAG_ACK,
            session_id=self.state.session_id,
            stream_id=packet.stream_id,
            ack_num=cumulative_ack,
            payload=encode_sack_blocks(sack_blocks),
        )
        if sender_session_id is not None and self.using_central_server:
            self._send_relay(ack, sender_session_id)
        else:
            self.send_raw(ack.pack(), addr)

    def _send_relay(
        self,
        packet: Packet,
        target_session_id: int,
        file_info: Optional[dict] = None,
        retry_count: int = 0,
    ):
        if not self.using_central_server or not self._relay_endpoint:
            raise RuntimeError("UDP relay is not configured")
        envelope = {
            "type": "relay",
            "sender_session_id": self.state.session_id,
            "target_session_id": target_session_id,
            "packet": base64.b64encode(packet.pack()).decode("ascii"),
        }
        if file_info is not None:
            envelope["file"] = file_info
            envelope["retry_count"] = retry_count
        self.send_raw(
            RELAY_PREFIX + json.dumps(envelope, separators=(",", ":")).encode("utf-8"),
            self._relay_endpoint,
        )

    def _peer_username(self, sender_ip: str, sender_port: int) -> str:
        with self.state._lock:
            for peer in self.state.peers.values():
                if peer["ip"] == sender_ip and peer["port"] == sender_port:
                    return peer["username"]
        return f"{sender_ip}:{sender_port}"

    def _peer_username_by_session(self, session_id: int) -> str:
        with self.state._lock:
            for peer in self.state.peers.values():
                if peer["session_id"] == session_id:
                    return peer["username"]
        return f"session:{session_id}"

    def _handle_file_chunk(
        self,
        stream_id: int,
        encrypted_chunk: bytes,
        sender_ip: str,
        sender_port: int,
        sender_session_id: Optional[int] = None,
    ):
        transfer = self.stream_manager.incoming_transfers.get(stream_id)
        if transfer and transfer.get("failed"):
            return
        plain_chunk = self.crypto.decrypt(encrypted_chunk)
        if stream_id not in self.stream_manager.incoming_transfers:
            metadata = FileTransferManager.decode_file_init(plain_chunk)
            if not isinstance(metadata, dict) or not all(
                key in metadata
                for key in ("filename", "filesize", "total_chunks", "sha256")
            ):
                raise ValueError("Invalid file transfer metadata")
            metadata["filename"] = os.path.basename(
                str(metadata["filename"]).replace("\\", "/")
            )
            if not metadata["filename"]:
                raise ValueError("Invalid filename in file transfer metadata")
            self.stream_manager.register_incoming_file(stream_id, metadata)
            transfer_id = str(metadata.get("transfer_id", stream_id))
            chat_stream_id = int(metadata.get("chat_stream_id", 0))
            event_timestamp = metadata.get("event_timestamp")
            event_order = metadata.get("event_order")
            event_id = metadata.get("event_id")
            if (
                isinstance(event_timestamp, bool)
                or not isinstance(event_timestamp, (int, float))
                or not 0 <= event_timestamp <= 4102444800
                or isinstance(event_order, bool)
                or not isinstance(event_order, int)
                or event_order < 1
                or not isinstance(event_id, str)
                or not event_id
            ):
                event_id, event_timestamp, event_order = self.state.next_event_metadata()
            sender_name = (
                self._peer_username_by_session(sender_session_id)
                if sender_session_id is not None
                else self._peer_username(sender_ip, sender_port)
            )
            self.state.add_file_message(
                chat_stream_id,
                sender_name,
                metadata["filename"],
                transfer_id,
                "Đang nhận",
                metadata["filesize"],
                sender_name if chat_stream_id != 0 else None,
                event_id=event_id,
                timestamp=event_timestamp,
                event_order=event_order,
                server_synced=sender_session_id is not None,
            )
            self.state.update_file_progress(
                file_id=transfer_id,
                filename=metadata["filename"],
                total_size=metadata["filesize"],
                received_chunks=0,
                total_chunks=metadata["total_chunks"],
                status="RECEIVING",
            )
            if metadata["total_chunks"] == 0:
                self.stream_manager.save_empty_file(stream_id)
                self._complete_incoming_file(stream_id, transfer_id, metadata)
            return

        received, total, completed = self.stream_manager.append_chunk(stream_id, plain_chunk)
        transfer = self.stream_manager.incoming_transfers[stream_id]
        metadata = transfer["metadata"]
        transfer_id = str(metadata.get("transfer_id", stream_id))
        self.state.update_file_progress(
            file_id=transfer_id,
            filename=metadata["filename"],
            total_size=metadata["filesize"],
            received_chunks=received,
            total_chunks=total,
            status="COMPLETED" if completed else "RECEIVING",
        )
        if completed:
            self._complete_incoming_file(stream_id, transfer_id, metadata)

    def _fail_incoming_file(self, stream_id: int, exc: Exception):
        print(f"[!] Nhận file trên stream {stream_id} thất bại: {exc}")
        transfer = self.stream_manager.incoming_transfers.get(stream_id)
        if not transfer:  # lỗi ngay ở metadata, chưa đăng ký transfer
            return
        transfer["failed"] = True
        meta = transfer["metadata"]
        transfer_id = str(meta.get("transfer_id", stream_id))
        self.state.update_file_progress(
            file_id=transfer_id,
            filename=meta["filename"],
            total_size=meta["filesize"],
            received_chunks=transfer["received_chunks"],
            total_chunks=transfer["total_chunks"],
            status="ERROR",
        )
        self.state.update_file_message(transfer_id, "Nhận thất bại")

    def _complete_incoming_file(self, stream_id: int, transfer_id: str, metadata: dict):
        total_chunks = metadata["total_chunks"]
        self.state.update_file_progress(
            file_id=transfer_id,
            filename=metadata["filename"],
            total_size=metadata["filesize"],
            received_chunks=total_chunks,
            total_chunks=total_chunks,
            status="COMPLETED",
        )
        transfer = self.stream_manager.incoming_transfers[stream_id]
        self.state.update_file_message(
            transfer_id,
            "Đã nhận",
            transfer["metadata"].get("saved_filename"),
        )

    def _retransmit_loop(self):
        while self.running:
            with self._reliability_lock:
                senders = list(self._outgoing_senders.items())
            for (stream_id, dest_ip, dest_port), sender in senders:
                if sender.failed:
                    with self._reliability_lock:
                        self._outgoing_senders.pop((stream_id, dest_ip, dest_port), None)
                        self._relay_targets.pop((stream_id, dest_ip, dest_port), None)
                        self._relay_file_info.pop((stream_id, dest_ip, dest_port), None)
                        self._relay_retry_counts.pop((stream_id, dest_ip, dest_port), None)
                    continue
                for packet in sender.get_packets_for_retransmission():
                    try:
                        key = (stream_id, dest_ip, dest_port)
                        with self._reliability_lock:
                            target_session_id = self._relay_targets.get(key)
                            file_info = self._relay_file_info.get(key)
                            retry_count = 0
                            if file_info is not None:
                                retries_by_sequence = self._relay_retry_counts.setdefault(
                                    key,
                                    {},
                                )
                                retry_count = retries_by_sequence.get(
                                    packet.seq_num,
                                    0,
                                ) + 1
                                retries_by_sequence[packet.seq_num] = retry_count
                        if self.using_central_server and target_session_id is not None:
                            self._send_relay(
                                packet,
                                target_session_id,
                                file_info,
                                retry_count,
                            )
                        else:
                            self.send_raw(packet.pack(), (dest_ip, dest_port))
                    except OSError as exc:
                        print(
                            f"[!] Could not retransmit stream {stream_id} "
                            f"to {dest_ip}:{dest_port}: {exc}"
                        )
                if sender.failed:
                    with self._reliability_lock:
                        self._outgoing_senders.pop((stream_id, dest_ip, dest_port), None)
                        self._relay_targets.pop((stream_id, dest_ip, dest_port), None)
                        self._relay_file_info.pop((stream_id, dest_ip, dest_port), None)
                        self._relay_retry_counts.pop((stream_id, dest_ip, dest_port), None)
                    print(
                        f"[!] Hủy stream {stream_id} tới {dest_ip}:{dest_port}: "
                        f"peer không phản hồi"
                    )
            time.sleep(0.05)
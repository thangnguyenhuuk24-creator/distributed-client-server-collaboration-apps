import base64
import json
import os
import socket
import sys
import threading
import time
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
        discovery_port: int = 9002,
        server_host: Optional[str] = None,
        server_tcp_port: int = 9001,
        server_udp_port: int = 9002,
    ):
        self.state = state
        self.host = host
        self.port = port
        self.server_host = server_host
        self.server_tcp_port = server_tcp_port
        self.server_udp_port = server_udp_port
        self.coordinator = (
            CentralClientConnection(state, server_host, server_tcp_port, port)
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

    def start(self):
        self.sock.bind((self.host, self.port))
        self.running = True
        try:
            self.recv_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self.recv_thread.start()
            self.timer_thread = threading.Thread(target=self._retransmit_loop, daemon=True)
            self.timer_thread.start()
            if not self.coordinator:
                self.discovery.start()
        except OSError:
            self.running = False
            self.sock.close()
            self.discovery.stop()
            raise

    def stop(self):
        self.running = False
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

    def login(self, username: str) -> int:
        if not self.coordinator:
            raise RuntimeError("Central server mode is not configured")
        return self.coordinator.login(username)

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
    ):
        """Send an encrypted chat message directly or through the central UDP relay."""
        if self.coordinator:
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
            encrypted_payload = self.crypto.encrypt(message_text.encode("utf-8"))
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
            payload=self.crypto.encrypt(message_text.encode("utf-8")),
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
    ):
        """Send a file directly or through the central UDP relay."""
        metadata, chunks = FileTransferManager.prepare_file_for_sending(filepath)
        if filename:
            metadata["filename"] = filename
        if transfer_id:
            metadata["transfer_id"] = transfer_id
        metadata["chat_stream_id"] = chat_stream_id

        relay_target_session_id = None
        sender_ip, sender_port = dest_ip, dest_port
        if self.coordinator:
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
            except OSError:
                if self.running:
                    print("[!] UDP receive socket failed")
                break
            if not raw_data:
                continue
            try:
                sender_session_id = None
                if raw_data.startswith(RELAY_PREFIX):
                    envelope = json.loads(raw_data[len(RELAY_PREFIX):].decode("utf-8"))
                    if not isinstance(envelope, dict) or envelope.get("type") != "deliver":
                        continue
                    sender_session_id = envelope.get("sender_session_id")
                    raw_data = base64.b64decode(envelope["packet"], validate=True)
                    if addr != self._relay_endpoint:
                        continue
                packet = Packet.unpack(raw_data)
                self._dispatch_packet(packet, addr, sender_session_id)
            except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError, AttributeError) as exc:
                print(f"[!] Invalid UDP packet from {addr[0]}:{addr[1]}: {exc}")
                if self.coordinator and addr == self._relay_endpoint:
                    self.coordinator.report_warning("invalid_packet")

    def _dispatch_packet(
        self,
        packet: Packet,
        addr: Tuple[str, int],
        sender_session_id: Optional[int] = None,
    ):
        sender_ip, sender_port = addr

        if packet.flags & FLAG_ACK:
            if self.coordinator and sender_session_id is not None:
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
                    text = self.crypto.decrypt(chunk).decode("utf-8")
                except (ValueError, UnicodeDecodeError) as exc:
                    print(f"[!] Bỏ qua tin nhắn không giải mã được: {exc}")
                    if self.coordinator:
                        self.coordinator.report_warning("decrypt_failure")
                    continue
                self.state.add_message(packet.stream_id, sender_name, text)
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
                    if self.coordinator:
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
        if sender_session_id is not None and self.coordinator:
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
        if not self.coordinator or not self._relay_endpoint:
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
                        if self.coordinator and target_session_id is not None:
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
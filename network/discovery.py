import socket
import threading
import time
import json
from typing import Optional
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.protocol import Packet, FLAG_DISCOV, FLAG_HEARTBEAT
from core.state import ClientState


class DiscoveryService:
    """Tự động tìm kiếm các peer trong cùng subnet LAN và duy trì heartbeat."""

    def __init__(
        self,
        state: ClientState,
        socket_send_func,
        broadcast_port: int = 9002,
        local_port: int = 9000,
    ):
        self.state = state
        self.socket_send_func = socket_send_func
        self.broadcast_port = broadcast_port
        self.local_port = local_port
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.receive_thread: Optional[threading.Thread] = None
        self.sock: Optional[socket.socket] = None

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind(("0.0.0.0", self.broadcast_port))
            sock.settimeout(0.5)
        except OSError:
            sock.close()
            raise
        self.sock = sock
        self.running = True
        self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self.receive_thread.start()
        self.thread = threading.Thread(target=self._discovery_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.sock:
            self.sock.close()
        for thread in (self.receive_thread, self.thread):
            if thread and thread.is_alive():
                thread.join(timeout=1.0)

    def send_broadcast_announce(self):
        """Phát sóng UDP Broadcast thông báo mình đang online."""
        if not self.state.username:
            return

        payload_dict = {
            "type": "HELLO",
            "username": self.state.username,
            "session_id": self.state.session_id,
            "udp_port": self.local_port,
        }
        payload = json.dumps(payload_dict).encode("utf-8")
        pkt = Packet(
            flags=FLAG_DISCOV,
            session_id=self.state.session_id,
            stream_id=0,
            seq_num=0,
            ack_num=0,
            payload=payload
        )
        # 255.255.255.255 là địa chỉ Limited Broadcast cho subnet nội bộ
        if self.sock:
            self.sock.sendto(pkt.pack(), ("255.255.255.255", self.broadcast_port))

    def _receive_loop(self):
        sock = self.sock
        if sock is None:
            return
        while self.running:
            try:
                raw_data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                if self.running:
                    raise
                break

            try:
                pkt = Packet.unpack(raw_data)
                self._handle_announcement(pkt, addr)
            except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
                continue

    def _handle_announcement(self, pkt: Packet, addr):
        if not pkt.flags & FLAG_DISCOV:
            return
        info = json.loads(pkt.payload.decode("utf-8"))
        if not isinstance(info, dict):
            return
        username = info.get("username")
        if not isinstance(username, str) or not username or username == self.state.username:
            return
        peer_port = int(info.get("udp_port", addr[1]))
        if 1 <= peer_port <= 65535:
            self.state.update_peer(addr[0], peer_port, username, pkt.session_id)

    def _discovery_loop(self):
        """Vòng lặp chạy ngầm định kỳ gửi discovery/heartbeat và dọn dẹp node chết."""
        while self.running:
            try:
                if self.state.status == "CONNECTED":
                    self.send_broadcast_announce()

                    # Gửi heartbeat định kỳ tới tất cả peer đã biết
                    with self.state._lock:
                        peers = list(self.state.peers.values())
                    for peer in peers:
                        hb_pkt = Packet(
                            flags=FLAG_HEARTBEAT,
                            session_id=self.state.session_id,
                            stream_id=0,
                            seq_num=0,
                            ack_num=0,
                            payload=b"PING"
                        )
                        self.socket_send_func(hb_pkt.pack(), (peer["ip"], peer["port"]))

                # Xóa các node không phản hồi quá 12 giây
                self.state.prune_dead_peers(timeout_seconds=12.0)
            except Exception:
                pass

            time.sleep(3.0)  # Chu kỳ 3 giây
import threading
import time
import zlib
from typing import Dict, List, Any, Optional


def private_stream_id(first_session_id: int, second_session_id: int) -> int:
    lower_id, higher_id = sorted((first_session_id, second_session_id))
    stream_id = zlib.crc32(f"{lower_id}:{higher_id}".encode("ascii")) & 0xFFFFFFFF
    return stream_id if stream_id > 1 else stream_id + 2


class ClientState:
    # Chia sẻ dữ lieu giữa Socket Engine và Web HTTP Server qua đây

    def __init__(self):
        self._lock = threading.RLock()

        # 1. Thông tin định danh Client
        self.username: str = ""
        self.session_id: int = 0
        self.status: str = "DISCONNECTED"  # DISCONNECTED, CONNECTING, CONNECTED, ERROR
        self.last_heartbeat: float = 0.0

        # 2. Danh sách Peers đang online:
        # Cấu trúc: { "ip:port": {"username": ..., "ip": ..., "port": ..., "last_seen": ...} }
        self.peers: Dict[str, Dict[str, Any]] = {}

        # 3. Quản lý Streams tin nhắn (Stream 0: Kênh chung, Stream 1+: Kênh riêng):
        # Cấu trúc: { stream_id: [ {"sender": ..., "text": ..., "timestamp": ...} ] }
        self.streams: Dict[int, List[Dict[str, Any]]] = {0: []}

        # 4. Quản lý tiến trình truyền nhận File:
        # Cấu trúc: { file_id: {"filename": ..., "total_size": ..., "received_chunks": ..., "total_chunks": ..., "status": ...} }
        self.file_transfers: Dict[str, Dict[str, Any]] = {}

        # 5. Hàng đợi tin nhắn gửi đi (Outbox Queue):
        # Được nạp bởi Web API POST /api/send_message, tiêu thụ bởi Socket Engine
        self.outbox_messages: List[Dict[str, Any]] = []

    def set_online(self, username: str, session_id: int):
        with self._lock:
            self.username = username
            self.session_id = session_id
            self.status = "CONNECTED"
            self.last_heartbeat = time.time()

    def set_offline(self):
        with self._lock:
            self.status = "DISCONNECTED"
            self.session_id = 0

    def update_peer(
        self,
        ip: str,
        port: int,
        username: str,
        session_id: int = 0,
    ):
        """Cập nhật hoặc ghi nhận một Peer mới dựa trên thông tin nhận từ Socket."""
        peer_key = f"{ip}:{port}"
        with self._lock:
            self.peers[peer_key] = {
                "peer_key": peer_key,
                "username": username,
                "ip": ip,
                "port": port,
                "session_id": session_id,
                "last_seen": time.time(),
            }

    def replace_peers(self, peers: List[Dict[str, Any]]):
        """Replace the current peer list with the central server's online roster."""
        now = time.time()
        updated = {}
        for peer in peers:
            ip = peer["ip"]
            port = peer["port"]
            peer_key = f"{ip}:{port}"
            updated[peer_key] = {
                "peer_key": peer_key,
                "username": peer["username"],
                "ip": ip,
                "port": port,
                "session_id": peer["session_id"],
                "last_seen": now,
            }
        with self._lock:
            self.peers = updated

    def prune_dead_peers(self, timeout_seconds: float = 12.0) -> List[str]:
        """Tự động loại bỏ peers không gửi heartbeat quá thời gian timeout."""
        now = time.time()
        removed_keys = []
        with self._lock:
            for peer_key, peer_info in list(self.peers.items()):
                if now - peer_info["last_seen"] > timeout_seconds:
                    removed_keys.append(peer_key)
                    del self.peers[peer_key]
        return removed_keys

    def add_message(self, stream_id: int, sender: str, text: str):
        with self._lock:
            if stream_id not in self.streams:
                self.streams[stream_id] = []
            self.streams[stream_id].append({
                "sender": sender,
                "text": text,
                "timestamp": time.time(),
            })

    def add_file_message(
        self,
        stream_id: int,
        sender: str,
        filename: str,
        file_id: str,
        status: str,
    ):
        with self._lock:
            if stream_id not in self.streams:
                self.streams[stream_id] = []
            self.streams[stream_id].append({
                "sender": sender,
                "text": f"Tệp: {filename}",
                "timestamp": time.time(),
                "type": "file",
                "filename": filename,
                "file_id": file_id,
                "status": status,
            })

    def update_file_message(
        self,
        file_id: str,
        status: str,
        download_filename: Optional[str] = None,
    ):
        with self._lock:
            for messages in self.streams.values():
                for message in messages:
                    if message.get("file_id") == file_id:
                        message["status"] = status
                        if download_filename:
                            message["download_filename"] = download_filename

    def queue_outbox_message(self, stream_id: int, recipient_ip: str, recipient_port: int, content: str):
        """Web UI gọi hàm này để yêu cầu Socket Engine gửi tin nhắn đi."""
        with self._lock:
            self.outbox_messages.append({
                "stream_id": stream_id,
                "recipient_ip": recipient_ip,
                "recipient_port": recipient_port,
                "content": content,
                "enqueued_at": time.time(),
            })

    def fetch_outbox_messages(self) -> List[Dict[str, Any]]:
        """Socket Engine gọi hàm này để lấy tất cả tin nhắn đang chờ gửi và làm rỗng hàng đợi."""
        with self._lock:
            messages = list(self.outbox_messages)
            self.outbox_messages.clear()
            return messages

    def update_file_progress(
        self,
        file_id: str,
        filename: str,
        total_size: int,
        received_chunks: int,
        total_chunks: int,
        status: str = "TRANSFERRING",
    ):
        with self._lock:
            percent = (
                received_chunks / total_chunks * 100.0
                if total_chunks > 0
                else 100.0 if status in {"SENT", "COMPLETED"} else 0.0
            )
            self.file_transfers[file_id] = {
                "file_id": file_id,
                "filename": filename,
                "total_size": total_size,
                "received_chunks": received_chunks,
                "total_chunks": total_chunks,
                "progress_percent": round(percent, 2),
                "status": status,
                "updated_at": time.time(),
            }

    def to_json_snapshot(self) -> dict:
        """
        Tạo bản sao dữ liệu (Snapshot) an toàn để trả về cho Member 2 serialize sang JSON.
        """
        with self._lock:
            return {
                "user": {
                    "username": self.username,
                    "session_id": self.session_id,
                    "status": self.status,
                    "last_heartbeat": self.last_heartbeat,
                },
                "peers": list(self.peers.values()),
                "streams": {str(sid): list(msgs) for sid, msgs in self.streams.items()},
                "transfers": list(self.file_transfers.values()),
            }
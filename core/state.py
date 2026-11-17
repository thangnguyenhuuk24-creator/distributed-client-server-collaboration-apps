import json
import os
import sqlite3
import threading
import time
import uuid
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
        self._history_db_path: Optional[str] = None
        self._history_username_key: Optional[str] = None
        self._history_server_key: Optional[str] = None
        self._event_order = 0

    def next_event_metadata(self):
        with self._lock:
            self._event_order += 1
            return uuid.uuid4().hex, time.time(), self._event_order

    def configure_history(self, username: str, server_key: str, database_path: str):
        os.makedirs(os.path.dirname(os.path.abspath(database_path)), exist_ok=True)
        username_key = username.casefold()
        with sqlite3.connect(database_path) as database:
            database.execute(
                "CREATE TABLE IF NOT EXISTS chat_history ("
                "username_key TEXT NOT NULL, server_key TEXT NOT NULL, "
                "stream_id INTEGER NOT NULL, message_id TEXT NOT NULL, "
                "timestamp REAL NOT NULL, message_json TEXT NOT NULL, "
                "PRIMARY KEY (username_key, server_key, stream_id, message_id))"
            )
            rows = database.execute(
                "SELECT stream_id, message_json FROM chat_history "
                "WHERE username_key = ? AND server_key = ? ORDER BY timestamp, rowid",
                (username_key, server_key),
            ).fetchall()
        with self._lock:
            self._history_db_path = database_path
            self._history_username_key = username_key
            self._history_server_key = server_key
            self.streams = {0: []}
            self._event_order = 0
            for stream_id, message_json in rows:
                message = json.loads(message_json)
                self.streams.setdefault(stream_id, []).append(message)
                self._event_order = max(
                    self._event_order,
                    int(message.get("_event_order", 0)),
                )
            for messages in self.streams.values():
                messages.sort(
                    key=lambda message: (
                        message["timestamp"],
                        int(message.get("_event_order", 0)),
                        str(message.get("_event_id", "")),
                    )
                )

    def get_unsynced_server_events(self, limit: int = 50):
        with self._lock:
            events = []
            seen = set()
            for messages in self.streams.values():
                for message in messages:
                    event_id = message.get("_event_id")
                    if (
                        message.get("_server_synced")
                        or message.get("sender") != f"{self.username} (You)"
                        or not event_id
                        or event_id in seen
                    ):
                        continue
                    seen.add(event_id)
                    event = {
                        "event_id": event_id,
                        "event_type": "file" if message.get("type") == "file" else "chat",
                        "timestamp": message["timestamp"],
                        "event_order": int(message.get("_event_order", 0)),
                        "recipient": message.get("conversation_key", ""),
                        "filename": message.get("filename", ""),
                        "filesize": int(message.get("filesize", 0)),
                    }
                    events.append(event)
            events.sort(key=lambda event: (event["timestamp"], event["event_order"]))
            return events[:limit]

    def mark_server_events_synced(self, event_ids):
        event_ids = set(event_ids)
        if not event_ids:
            return
        with self._lock:
            for stream_id, messages in self.streams.items():
                for message in messages:
                    if message.get("_event_id") in event_ids:
                        message["_server_synced"] = True
                        self._persist_message(stream_id, message)

    def _persist_message(self, stream_id: int, message: dict):
        if not self._history_db_path:
            return
        with sqlite3.connect(self._history_db_path) as database:
            database.execute(
                "INSERT OR REPLACE INTO chat_history "
                "(username_key, server_key, stream_id, message_id, timestamp, message_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    self._history_username_key,
                    self._history_server_key,
                    stream_id,
                    message["message_id"],
                    message["timestamp"],
                    json.dumps(message, ensure_ascii=False),
                ),
            )

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

    def add_message(
        self,
        stream_id: int,
        sender: str,
        text: str,
        conversation_key: Optional[str] = None,
        event_id: Optional[str] = None,
        timestamp: Optional[float] = None,
        event_order: Optional[int] = None,
        server_synced: bool = False,
    ):
        with self._lock:
            if stream_id not in self.streams:
                self.streams[stream_id] = []
            if event_id is None or timestamp is None or event_order is None:
                generated_id, generated_timestamp, generated_order = self.next_event_metadata()
                event_id = event_id or generated_id
                timestamp = generated_timestamp if timestamp is None else timestamp
                event_order = generated_order if event_order is None else event_order
            if any(
                message.get("_event_id") == event_id
                for messages in self.streams.values()
                for message in messages
            ):
                return
            message = {
                "message_id": uuid.uuid4().hex,
                "sender": sender,
                "text": text,
                "timestamp": timestamp,
                "_event_id": event_id,
                "_event_order": event_order,
                "_server_synced": server_synced,
            }
            if conversation_key:
                message["conversation_key"] = conversation_key.casefold()
            self.streams[stream_id].append(message)
            self._persist_message(stream_id, message)

    def add_file_message(
        self,
        stream_id: int,
        sender: str,
        filename: str,
        file_id: str,
        status: str,
        filesize: int = 0,
        conversation_key: Optional[str] = None,
        event_id: Optional[str] = None,
        timestamp: Optional[float] = None,
        event_order: Optional[int] = None,
        server_synced: bool = False,
        sha256: Optional[str] = None,
        file_expires_at: Optional[float] = None,
    ):
        with self._lock:
            if stream_id not in self.streams:
                self.streams[stream_id] = []
            if event_id is None or timestamp is None or event_order is None:
                generated_id, generated_timestamp, generated_order = self.next_event_metadata()
                event_id = event_id or generated_id
                timestamp = generated_timestamp if timestamp is None else timestamp
                event_order = generated_order if event_order is None else event_order
            if any(
                message.get("_event_id") == event_id
                for messages in self.streams.values()
                for message in messages
            ):
                return
            message = {
                "message_id": uuid.uuid4().hex,
                "sender": sender,
                "text": f"Tệp: {filename}",
                "timestamp": timestamp,
                "type": "file",
                "filename": filename,
                "file_id": file_id,
                "filesize": filesize,
                "status": status,
                "_event_id": event_id,
                "_event_order": event_order,
                "_server_synced": server_synced,
            }
            if conversation_key:
                message["conversation_key"] = conversation_key.casefold()
            if sha256:
                message["sha256"] = sha256
            if file_expires_at is not None:
                message["file_expires_at"] = file_expires_at
            self.streams[stream_id].append(message)
            self._persist_message(stream_id, message)

    def update_file_message(
        self,
        file_id: str,
        status: str,
        download_filename: Optional[str] = None,
        download_expires_at: Optional[float] = None,
    ):
        with self._lock:
            for stream_id, messages in self.streams.items():
                for message in messages:
                    if message.get("file_id") == file_id:
                        message["status"] = status
                        if status in {"Đã gửi", "Đã gửi (chờ nhận)"}:
                            message["file_expires_at"] = time.time() + 60
                        if download_filename:
                            message["download_filename"] = download_filename
                            message["download_expires_at"] = (
                                download_expires_at
                                if download_expires_at is not None
                                else time.time() + 60
                            )
                        self._persist_message(stream_id, message)

    def expire_file_messages(self, now: Optional[float] = None) -> List[str]:
        current_time = time.time() if now is None else now
        expired_downloads = []
        with self._lock:
            for stream_id, messages in self.streams.items():
                for message in messages:
                    if message.get("type") != "file":
                        continue
                    if message.get("status") in {"Đang gửi", "Đang nhận"}:
                        continue
                    expires_at = message.get(
                        "download_expires_at",
                        message.get(
                            "file_expires_at",
                            message["timestamp"] + 60,
                        ),
                    )
                    if current_time < expires_at:
                        continue
                    download_filename = message.pop("download_filename", None)
                    if download_filename:
                        expired_downloads.append(download_filename)
                    message.pop("download_expires_at", None)
                    file_id = message.get("file_id")
                    if file_id:
                        self.file_transfers.pop(file_id, None)
                    for key in ("text", "file_id", "status"):
                        message.pop(key, None)
                    message["expired"] = True
                    self._persist_message(stream_id, message)
        return expired_downloads

    def clear_session_data(self):
        with self._lock:
            self.username = ""
            self.session_id = 0
            self.status = "DISCONNECTED"
            self.peers = {}
            self.streams = {0: []}
            self.file_transfers = {}
            self.outbox_messages = []

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
                "streams": {
                    str(sid): [
                        {
                            key: value
                            for key, value in message.items()
                            if key != "message_id" and not key.startswith("_")
                        }
                        for message in messages
                    ]
                    for sid, messages in self.streams.items()
                },
                "transfers": list(self.file_transfers.values()),
            }
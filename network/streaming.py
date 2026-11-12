import os
import json
import math
import hashlib
from typing import Dict, List, Optional, Tuple

MAX_CHUNK_SIZE = 1350  # Kích thước chunk an toàn, nằm gọn dưới MTU 1500 bytes


class FileTransferManager:
    """Quản lý băm nhỏ tập tin thành các chunk và tái ghép dữ liệu tại đích đến."""

    @staticmethod
    def prepare_file_for_sending(filepath: str) -> Tuple[dict, List[bytes]]:
        """Đọc file, tính SHA-256, và băm thành danh sách chunks byte."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File {filepath} không tồn tại")

        filesize = os.path.getsize(filepath)
        filename = os.path.basename(filepath)
        sha256 = hashlib.sha256()

        chunks: List[bytes] = []
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(MAX_CHUNK_SIZE)
                if not chunk:
                    break
                sha256.update(chunk)
                chunks.append(chunk)

        metadata = {
            "filename": filename,
            "filesize": filesize,
            "total_chunks": len(chunks),
            "sha256": sha256.hexdigest(),
        }
        return metadata, chunks

    @staticmethod
    def encode_file_init(metadata: dict) -> bytes:
        """Đóng gói metadata khởi tạo file dạng byte string UTF-8."""
        return json.dumps(metadata).encode("utf-8")

    @staticmethod
    def decode_file_init(payload: bytes) -> dict:
        return json.loads(payload.decode("utf-8"))


class StreamManager:
    """Quản lý đa luồng stream nhận nội dung (File hoặc Data Stream)."""

    def __init__(self, download_dir: str = "downloads"):
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Quản lý file đang nhận: { stream_id: { "metadata": ..., "received_chunks": {seq: data} } }
        self.incoming_transfers: Dict[int, dict] = {}

    def register_incoming_file(self, stream_id: int, metadata: dict):
        self.incoming_transfers[stream_id] = {
            "metadata": metadata,
            "chunks": {},
            "total_chunks": metadata.get("total_chunks", 0),
            "received_chunks": 0,
            "saved": False
        }

    def append_chunk(self, stream_id: int, data: bytes) -> Tuple[int, int, bool]:
        """
        Lưu chunk vào stream.
        Trả về: (received_count, total_count, is_completed)
        """
        if stream_id not in self.incoming_transfers:
            return (0, 0, False)

        transfer = self.incoming_transfers[stream_id]
        transfer["received_chunks"] += 1
        transfer["chunks"][transfer["received_chunks"]] = data

        received_count = transfer["received_chunks"]
        total_count = transfer["total_chunks"]

        if received_count == total_count and not transfer["saved"]:
            self._save_assembled_file(stream_id)
            transfer["saved"] = True
            return (received_count, total_count, True)

        return (received_count, total_count, False)

    def save_empty_file(self, stream_id: int):
        transfer = self.incoming_transfers[stream_id]
        if transfer["total_chunks"] == 0 and not transfer["saved"]:
            self._save_assembled_file(stream_id)
            transfer["saved"] = True

    def _save_assembled_file(self, stream_id: int):
        transfer = self.incoming_transfers[stream_id]
        meta = transfer["metadata"]
        save_path = os.path.join(self.download_dir, meta["filename"])

        contents = b"".join(
            transfer["chunks"][seq]
            for seq in range(1, meta["total_chunks"] + 1)
        )
        if hashlib.sha256(contents).hexdigest() != meta["sha256"]:
            raise ValueError(f"SHA-256 mismatch while receiving {meta['filename']}")

        # Tránh ghi đè nếu file cùng tên đã tồn tại
        base, ext = os.path.splitext(save_path)
        counter = 1
        while os.path.exists(save_path):
            save_path = f"{base}_{counter}{ext}"
            counter += 1

        with open(save_path, "wb") as f:
            f.write(contents)

        transfer["saved_path"] = save_path
        transfer["metadata"]["saved_filename"] = os.path.basename(save_path)
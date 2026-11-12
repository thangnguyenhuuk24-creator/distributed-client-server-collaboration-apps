import struct
import zlib
from typing import List, Tuple

# Protocol Constants
MAGIC_NUMBER = 0x4E41
PROTOCOL_VERSION = 1
HEADER_FORMAT = "!HBBIIIIHH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Control Flags
FLAG_AUTH = 0x01
FLAG_DISCOV = 0x02
FLAG_MSG = 0x04
FLAG_FILE = 0x08
FLAG_ACK = 0x10
FLAG_HEARTBEAT = 0x20
FLAG_SYN = 0x40
FLAG_FIN = 0x80

SACK_VERSION = 1
MAX_SACK_BLOCKS = 32


def encode_sack_blocks(blocks: Tuple[Tuple[int, int], ...]) -> bytes:
    if len(blocks) > MAX_SACK_BLOCKS:
        raise ValueError(f"SACK payload supports at most {MAX_SACK_BLOCKS} blocks")

    encoded = bytearray(struct.pack("!BB", SACK_VERSION, len(blocks)))
    previous_right = 0
    for left, right in blocks:
        if (
            isinstance(left, bool)
            or isinstance(right, bool)
            or not isinstance(left, int)
            or not isinstance(right, int)
            or not 1 <= left < right <= 0xFFFFFFFF
            or left < previous_right
        ):
            raise ValueError("SACK blocks must be ordered, non-overlapping uint32 ranges")
        encoded.extend(struct.pack("!II", left, right))
        previous_right = right
    return bytes(encoded)


def decode_sack_blocks(payload: bytes) -> Tuple[Tuple[int, int], ...]:
    if not payload:
        return ()
    if len(payload) < 2:
        raise ValueError("SACK payload is too short")

    version, block_count = struct.unpack("!BB", payload[:2])
    expected_length = 2 + block_count * 8
    if version != SACK_VERSION:
        raise ValueError(f"Unsupported SACK version: {version}")
    if block_count > MAX_SACK_BLOCKS or len(payload) != expected_length:
        raise ValueError("Invalid SACK block count or payload length")

    blocks: List[Tuple[int, int]] = []
    previous_right = 0
    for offset in range(2, expected_length, 8):
        left, right = struct.unpack("!II", payload[offset:offset + 8])
        if left < 1 or left >= right or left < previous_right:
            raise ValueError("SACK blocks must be ordered, non-overlapping uint32 ranges")
        blocks.append((left, right))
        previous_right = right
    return tuple(blocks)


class Packet:
    def __init__(
        self,
        flags: int,
        session_id: int = 0,
        stream_id: int = 0,
        seq_num: int = 0,
        ack_num: int = 0,
        payload: bytes = b"",
    ):
        self.magic: int = MAGIC_NUMBER
        self.version: int = PROTOCOL_VERSION
        self.flags: int = flags
        self.session_id: int = session_id
        self.stream_id: int = stream_id
        self.seq_num: int = seq_num
        self.ack_num: int = ack_num
        self.payload: bytes = payload if isinstance(payload, bytes) else payload.encode("utf-8")
        self.payload_len: int = len(self.payload)

    def pack(self) -> bytes:
        header_zero_crc = struct.pack(
            HEADER_FORMAT,
            self.magic,
            self.version,
            self.flags,
            self.session_id,
            self.stream_id,
            self.seq_num,
            self.ack_num,
            self.payload_len,
            0,
        )
        checksum = zlib.crc32(header_zero_crc + self.payload) & 0xFFFF
        header = struct.pack(
            HEADER_FORMAT,
            self.magic,
            self.version,
            self.flags,
            self.session_id,
            self.stream_id,
            self.seq_num,
            self.ack_num,
            self.payload_len,
            checksum,
        )
        return header + self.payload

    @classmethod
    def unpack(cls, raw_bytes: bytes) -> "Packet":
        if len(raw_bytes) < HEADER_SIZE:
            raise ValueError(
                f"Packet quá ngắn: {len(raw_bytes)} bytes (tối thiểu {HEADER_SIZE} bytes)"
            )

        header = raw_bytes[:HEADER_SIZE]
        (
            magic,
            version,
            flags,
            session_id,
            stream_id,
            seq_num,
            ack_num,
            payload_len,
            received_checksum,
        ) = struct.unpack(HEADER_FORMAT, header)

        if magic != MAGIC_NUMBER:
            raise ValueError(f"Magic Number không hợp lệ: {hex(magic)}")
        if len(raw_bytes) != HEADER_SIZE + payload_len:
            raise ValueError(
                f"Payload length không khớp: khai báo {payload_len} bytes, "
                f"nhận được {len(raw_bytes) - HEADER_SIZE} bytes"
            )

        payload = raw_bytes[HEADER_SIZE:]
        header_zero_crc = struct.pack(
            HEADER_FORMAT,
            magic,
            version,
            flags,
            session_id,
            stream_id,
            seq_num,
            ack_num,
            payload_len,
            0,
        )
        calculated_checksum = zlib.crc32(header_zero_crc + payload) & 0xFFFF
        if calculated_checksum != received_checksum:
            raise ValueError(
                f"Checksum hỏng: tính được {calculated_checksum}, "
                f"trong packet là {received_checksum}"
            )

        packet = cls(
            flags=flags,
            session_id=session_id,
            stream_id=stream_id,
            seq_num=seq_num,
            ack_num=ack_num,
            payload=payload,
        )
        packet.version = version
        return packet

    def __repr__(self) -> str:
        return (
            f"<Packet Flags={hex(self.flags)} Sess={self.session_id} "
            f"Stream={self.stream_id} Seq={self.seq_num} "
            f"ACK={self.ack_num} Len={self.payload_len}>"
        )

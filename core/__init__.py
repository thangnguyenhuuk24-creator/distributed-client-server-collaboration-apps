"""
Core networking, protocol specification, and in-memory state definitions.
Course: Network Application Development (CO3093/CO3094) - HCMUT
"""

from .protocol import (
    Packet,
    MAGIC_NUMBER,
    PROTOCOL_VERSION,
    HEADER_SIZE,
    FLAG_AUTH,
    FLAG_DISCOV,
    FLAG_MSG,
    FLAG_FILE,
    FLAG_ACK,
    FLAG_HEARTBEAT,
    FLAG_SYN,
    FLAG_FIN,
)
from .state import ClientState
from .crypto import CryptoEngine

__all__ = [
    "Packet",
    "MAGIC_NUMBER",
    "PROTOCOL_VERSION",
    "HEADER_SIZE",
    "FLAG_AUTH",
    "FLAG_DISCOV",
    "FLAG_MSG",
    "FLAG_FILE",
    "FLAG_ACK",
    "FLAG_HEARTBEAT",
    "FLAG_SYN",
    "FLAG_FIN",
    "ClientState",
    "CryptoEngine",
]

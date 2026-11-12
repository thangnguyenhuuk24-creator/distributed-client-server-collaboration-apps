import time
import threading
import math
from typing import Dict, List, Optional, Tuple
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.protocol import Packet


class SlidingWindowSender:
    """Per-stream sliding window with TCP CUBIC congestion control."""

    CUBIC_BETA = 0.7
    CUBIC_C = 0.4
    SACK_LOSS_THRESHOLD = 2
    MIN_CWND = 2.0
    INITIAL_CWND = 4.0
    MAX_RETRIES = 12

    def __init__(
        self,
        stream_id: int,
        window_size: int = 8,
        rto_seconds: float = 0.5,
    ) -> None:
        self.stream_id = stream_id
        self.window_size = window_size
        self.rto_seconds = rto_seconds
        self.lock = threading.RLock()

        self.next_seq_num: int = 1
        self.base_seq_num: int = 1
        self.failed = False
        self.cwnd = min(float(window_size), self.INITIAL_CWND)
        self.ssthresh = float(window_size)
        self.w_max = self.cwnd
        self._cubic_epoch: Optional[float] = None
        self._k = 0.0
        self._last_ack_num = 1
        self._loss_recovery_end: Optional[int] = None
        self._sacked_packets: set[int] = set()
        self._fast_retransmitted: set[int] = set()

        self.unacked_packets: Dict[int, Tuple[Packet, float, int]] = {}

    def can_send(self) -> bool:
        with self.lock:
            congestion_window = min(self.window_size, max(1, int(self.cwnd)))
            return len(self.unacked_packets) < congestion_window

    def register_packet(self, flags: int, session_id: int, payload: bytes) -> Packet:
        with self.lock:
            if not self.can_send():
                raise BufferError("Congestion window is full")
            seq = self.next_seq_num
            pkt = Packet(
                flags=flags,
                session_id=session_id,
                stream_id=self.stream_id,
                seq_num=seq,
                ack_num=0,
                payload=payload,
            )
            self.unacked_packets[seq] = (pkt, time.monotonic(), 0)
            self.next_seq_num += 1
            return pkt

    def on_ack_received(
        self,
        ack_num: int,
        sack_blocks: Tuple[Tuple[int, int], ...] = (),
    ) -> int:
        """Apply cumulative and selective acknowledgements."""
        freed = 0
        with self.lock:
            if ack_num < self._last_ack_num or ack_num > self.next_seq_num:
                return 0
            if ack_num > self._last_ack_num:
                for seq in list(self.unacked_packets):
                    if seq < ack_num:
                        del self.unacked_packets[seq]
                        self._sacked_packets.discard(seq)
                        self._fast_retransmitted.discard(seq)
                        freed += 1

                self._last_ack_num = ack_num
                self.base_seq_num = ack_num
                if (
                    self._loss_recovery_end is not None
                    and ack_num > self._loss_recovery_end
                ):
                    self._loss_recovery_end = None

            for seq in self.unacked_packets:
                if any(left <= seq < right for left, right in sack_blocks):
                    self._sacked_packets.add(seq)

            now = time.monotonic()
            if freed:
                self._increase_cwnd(freed, now)

            sacked_above = sorted(self._sacked_packets)
            for seq, (packet, _, retries) in self.unacked_packets.items():
                if (
                    seq not in self._sacked_packets
                    and seq not in self._fast_retransmitted
                    and sum(sacked_seq > seq for sacked_seq in sacked_above)
                    >= self.SACK_LOSS_THRESHOLD
                ):
                    self.unacked_packets[seq] = (
                        packet,
                        now - self.rto_seconds - 0.001,
                        retries,
                    )
                    self._fast_retransmitted.add(seq)
        return freed

    def get_packets_for_retransmission(self) -> List[Packet]:
        """Retransmit expired packets and reduce cwnd once per loss episode."""
        now = time.monotonic()
        to_resend: List[Packet] = []
        with self.lock:
            lost_packets = [
                seq
                for seq, (_, sent_at, _) in self.unacked_packets.items()
                if seq not in self._sacked_packets
                and now - sent_at > self.rto_seconds
            ]
            if lost_packets and self._loss_recovery_end is None:
                self._on_congestion_loss(now)

            for seq, (packet, sent_at, retries) in self.unacked_packets.items():
                if (
                    seq not in self._sacked_packets
                    and now - sent_at > self.rto_seconds
                ):
                    if retries >= self.MAX_RETRIES:
                        self.failed = True
                        return []
                    self.unacked_packets[seq] = (packet, now, retries + 1)
                    to_resend.append(packet)
        return to_resend

    def _start_cubic_epoch(self, now: float, initial_w_max: float):
        if self._cubic_epoch is None:
            self._cubic_epoch = now
            self.w_max = max(self.MIN_CWND, initial_w_max)
            self._k = math.pow(
                self.w_max * (1.0 - self.CUBIC_BETA) / self.CUBIC_C,
                1.0 / 3.0,
            )

    def _increase_cwnd(self, acknowledged_packets: int, now: float) -> None:
        if acknowledged_packets <= 0:
            return

        acknowledgements_for_cubic = acknowledged_packets
        if self.cwnd < self.ssthresh:
            slow_start_acks = min(
                acknowledgements_for_cubic,
                math.ceil(self.ssthresh - self.cwnd),
            )
            self.cwnd = min(
                self.ssthresh,
                self.cwnd + slow_start_acks,
                float(self.window_size),
            )
            acknowledgements_for_cubic -= slow_start_acks

        if acknowledgements_for_cubic <= 0 or self.cwnd >= self.window_size:
            return

        if self._cubic_epoch is None:
            self._start_cubic_epoch(now, self.cwnd / self.CUBIC_BETA)

        elapsed = max(0.0, now - self._cubic_epoch)
        cubic_target = (
            self.CUBIC_C * math.pow(elapsed - self._k, 3) + self.w_max
        )
        rtt_estimate = max(0.01, self.rto_seconds / 2.0)
        reno_factor = (
            3.0 * (1.0 - self.CUBIC_BETA) / (1.0 + self.CUBIC_BETA)
        )
        tcp_target = (
            self.w_max * self.CUBIC_BETA
            + reno_factor * (elapsed / rtt_estimate)
        )
        target = min(
            float(self.window_size),
            max(cubic_target, tcp_target),
        )

        for _ in range(acknowledgements_for_cubic):
            if target <= self.cwnd:
                break
            increment_per_ack = (target - self.cwnd) / self.cwnd
            self.cwnd = min(
                float(self.window_size),
                self.cwnd + increment_per_ack,
            )

    def _on_congestion_loss(self, now: float) -> None:
        self.w_max = max(self.MIN_CWND, self.cwnd)
        self.ssthresh = max(self.MIN_CWND, self.cwnd * self.CUBIC_BETA)
        self.cwnd = self.ssthresh
        self._cubic_epoch = now
        self._k = math.pow(
            self.w_max * (1.0 - self.CUBIC_BETA) / self.CUBIC_C,
            1.0 / 3.0,
        )
        self._loss_recovery_end = max(self.unacked_packets)

    def congestion_snapshot(self) -> Dict[str, object]:
        with self.lock:
            return {
                "algorithm": "CUBIC",
                "cwnd_packets": round(self.cwnd, 3),
                "ssthresh_packets": round(self.ssthresh, 3),
                "max_window_packets": self.window_size,
                "in_flight_packets": len(self.unacked_packets),
                "loss_recovery_end": self._loss_recovery_end,
            }


class StreamReceiver:
    """Quản lý thứ tự nhận (In-order Delivery) và khử lặp gói tin cho một stream nhận."""

    def __init__(self, stream_id: int, window_size: int = 8):
        self.stream_id = stream_id
        self.window_size = window_size
        self.expected_seq: int = 1
        self.lock = threading.RLock()

        # Buffer chứa các gói đến sớm (Out-of-order): { seq_num: payload }
        self.out_of_order_buffer: Dict[int, bytes] = {}

    def process_incoming_packet(
        self,
        seq_num: int,
        payload: bytes,
    ) -> Tuple[List[bytes], int, Tuple[Tuple[int, int], ...]]:
        """
        Xử lý số thứ tự gói tin nhận được.
        Trả về dữ liệu theo thứ tự, ACK tích lũy và các SACK block [left, right).
        """
        ready_data: List[bytes] = []
        with self.lock:
            if self.expected_seq <= seq_num < self.expected_seq + self.window_size:
                self.out_of_order_buffer.setdefault(seq_num, payload)

                while self.expected_seq in self.out_of_order_buffer:
                    ready_data.append(self.out_of_order_buffer.pop(self.expected_seq))
                    self.expected_seq += 1

            sack_blocks: List[Tuple[int, int]] = []
            for seq in sorted(self.out_of_order_buffer):
                if sack_blocks and seq == sack_blocks[-1][1]:
                    left, _ = sack_blocks[-1]
                    sack_blocks[-1] = (left, seq + 1)
                else:
                    sack_blocks.append((seq, seq + 1))

            return (ready_data, self.expected_seq, tuple(sack_blocks))


class ReliabilityLayer:
    """Điều phối toàn bộ cơ chế tin cậy đa luồng cho Client."""

    def __init__(self):
        self.senders: Dict[int, SlidingWindowSender] = {}
        self.receivers: Dict[int, StreamReceiver] = {}
        self.lock = threading.RLock()

    def get_sender(self, stream_id: int) -> SlidingWindowSender:
        with self.lock:
            if stream_id not in self.senders:
                self.senders[stream_id] = SlidingWindowSender(stream_id)
            return self.senders[stream_id]

    def get_receiver(self, stream_id: int) -> StreamReceiver:
        with self.lock:
            if stream_id not in self.receivers:
                self.receivers[stream_id] = StreamReceiver(stream_id)
            return self.receivers[stream_id]
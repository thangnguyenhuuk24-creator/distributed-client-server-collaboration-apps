"""Beginner-friendly checks for UDP retransmission, SACK, and CUBIC.

Run this file with:
    python -m unittest test.test_retransmission -v

Expected summary:
    Ran 10 tests
    OK
"""

import time
import unittest
from typing import List

from core.protocol import (
    FLAG_ACK,
    FLAG_MSG,
    Packet,
    decode_sack_blocks,
    encode_sack_blocks,
)
from network.reliability import SlidingWindowSender, StreamReceiver


class TestRetransmissionAndCongestionControl(unittest.TestCase):
    """Use small packet examples to make the reliability behavior visible."""

    def test_01_sender_starts_with_a_small_congestion_window(self) -> None:
        """The sender starts by allowing at most four packets in flight."""
        sender = SlidingWindowSender(stream_id=1, window_size=8)

        self.assertEqual(sender.cwnd, 4)
        self.assertTrue(sender.can_send())
        print("PASS: New sender starts with a 4-packet congestion window.")

    def test_02_sender_does_not_exceed_its_send_window(self) -> None:
        """The sender pauses when all four allowed packet slots are occupied."""
        sender = SlidingWindowSender(stream_id=1, window_size=8)
        for number in range(4):
            sender.register_packet(FLAG_MSG, session_id=1, payload=f"message {number}".encode())

        self.assertFalse(sender.can_send())
        self.assertEqual(len(sender.unacked_packets), 4)
        print("PASS: Sender stopped at four unacknowledged packets.")

    def test_03_cumulative_ack_releases_confirmed_packets(self) -> None:
        """ACK 3 means packets 1 and 2 arrived, so their slots are released."""
        sender = SlidingWindowSender(stream_id=1, window_size=4)
        for number in range(4):
            sender.register_packet(FLAG_MSG, session_id=1, payload=f"message {number}".encode())

        released = sender.on_ack_received(ack_num=3)

        self.assertEqual(released, 2)
        self.assertEqual(set(sender.unacked_packets), {3, 4})
        self.assertTrue(sender.can_send())
        print("PASS: ACK 3 released packets 1-2 and opened two packet slots.")

    def test_04_receiver_reports_a_missing_packet_and_sacks_later_data(self) -> None:
        """Receiving packet 2 before packet 1 reports a gap and a SACK block."""
        receiver = StreamReceiver(stream_id=5)

        ready, cumulative_ack, sack_blocks = receiver.process_incoming_packet(2, b"second")

        self.assertEqual(ready, [])
        self.assertEqual(cumulative_ack, 1)
        self.assertEqual(sack_blocks, ((2, 3),))
        print("PASS: Receiver says packet 1 is missing and packet 2 has arrived.")

    def test_05_sack_groups_received_packets_and_survives_packet_encoding(self) -> None:
        """Adjacent packets are grouped in SACK; separated packets remain separate."""
        receiver = StreamReceiver(stream_id=5)
        receiver.process_incoming_packet(2, b"two")
        receiver.process_incoming_packet(4, b"four")
        _, cumulative_ack, blocks = receiver.process_incoming_packet(5, b"five")
        ack_packet = Packet(
            flags=FLAG_ACK,
            ack_num=cumulative_ack,
            payload=encode_sack_blocks(blocks),
        )
        decoded_ack = Packet.unpack(ack_packet.pack())

        self.assertEqual(blocks, ((2, 3), (4, 6)))
        self.assertEqual(decode_sack_blocks(decoded_ack.payload), blocks)
        print("PASS: SACK reported packet 2 and the grouped packets 4-5.")

    def test_06_sack_skips_received_packets_and_retransmits_missing_gaps(self) -> None:
        """SACK confirms packets 2, 4, and 5, so only missing 1 and 3 are resent."""
        sender = SlidingWindowSender(stream_id=5, window_size=8, rto_seconds=5)
        sender.cwnd = 8
        packets: List[Packet] = [
            sender.register_packet(FLAG_MSG, session_id=1, payload=f"packet {seq}".encode())
            for seq in range(1, 6)
        ]

        sender.on_ack_received(1, ((2, 3), (4, 6)))
        resent = sender.get_packets_for_retransmission()

        self.assertEqual([packet.seq_num for packet in resent], [1, 3])
        self.assertEqual(sender._sacked_packets, {2, 4, 5})
        self.assertEqual(len(packets), 5)
        print("PASS: Selective retransmission resent only missing packets 1 and 3.")

    def test_07_timeout_retransmits_a_packet_that_was_not_acknowledged(self) -> None:
        """A packet that waits longer than RTO is sent again."""
        sender = SlidingWindowSender(stream_id=1, window_size=4, rto_seconds=0.01)
        packet = sender.register_packet(FLAG_MSG, session_id=1, payload=b"important")

        time.sleep(0.02)
        resent = sender.get_packets_for_retransmission()

        self.assertEqual([item.seq_num for item in resent], [packet.seq_num])
        self.assertEqual(sender.unacked_packets[packet.seq_num][2], 1)
        print("PASS: Timed-out packet was resent once.")

    def test_08_loss_reduces_cwnd_and_sets_the_cubic_epoch(self) -> None:
        """On loss, cwnd falls by beta and K is calculated from W_max."""
        sender = SlidingWindowSender(stream_id=1, window_size=8)
        sender.cwnd = 6.0
        sender.register_packet(FLAG_MSG, session_id=1, payload=b"packet")
        sender._on_congestion_loss(10.0)

        expected_w_max = 6.0
        expected_k = (
            expected_w_max * (1.0 - sender.CUBIC_BETA) / sender.CUBIC_C
        ) ** (1.0 / 3.0)
        expected_cwnd = expected_w_max * sender.CUBIC_BETA
        cubic_at_epoch = (
            sender.CUBIC_C * (0.0 - sender._k) ** 3 + sender.w_max
        )

        self.assertAlmostEqual(sender.w_max, expected_w_max)
        self.assertAlmostEqual(sender.cwnd, expected_cwnd)
        self.assertAlmostEqual(sender.ssthresh, expected_cwnd)
        self.assertAlmostEqual(sender._k, expected_k)
        self.assertAlmostEqual(cubic_at_epoch, sender.cwnd)
        print("PASS: Loss lowered cwnd and CUBIC starts at the reduced window.")

    def test_09_cubic_uses_the_larger_cubic_or_tcp_friendly_target(self) -> None:
        """When Reno would grow faster, the TCP-friendly target is used."""
        sender = SlidingWindowSender(stream_id=1, window_size=8, rto_seconds=0.2)
        sender.cwnd = 6.0
        sender.register_packet(FLAG_MSG, session_id=1, payload=b"packet")
        epoch = 10.0
        sender._on_congestion_loss(epoch)

        elapsed = 0.5
        rtt_estimate = max(0.01, sender.rto_seconds / 2.0)
        reno_factor = 3.0 * (1.0 - sender.CUBIC_BETA) / (1.0 + sender.CUBIC_BETA)
        cubic_target = sender.CUBIC_C * (elapsed - sender._k) ** 3 + sender.w_max
        tcp_target = (
            sender.w_max * sender.CUBIC_BETA
            + reno_factor * (elapsed / rtt_estimate)
        )
        starting_cwnd = sender.cwnd
        expected_cwnd = starting_cwnd + (
            min(float(sender.window_size), max(cubic_target, tcp_target))
            - starting_cwnd
        ) / starting_cwnd

        sender._increase_cwnd(1, epoch + elapsed)

        self.assertGreater(tcp_target, cubic_target)
        self.assertAlmostEqual(sender.cwnd, expected_cwnd)
        print("PASS: CUBIC selected the faster TCP-friendly growth target.")

    def test_10_receiver_delivers_retransmitted_gap_in_order(self) -> None:
        """When missing packet 1 arrives, buffered packets 2 and 3 are delivered after it."""
        receiver = StreamReceiver(stream_id=9)
        ready, ack, blocks = receiver.process_incoming_packet(2, b"second")
        self.assertEqual((ready, ack, blocks), ([], 1, ((2, 3),)))

        ready, ack, blocks = receiver.process_incoming_packet(3, b"third")
        self.assertEqual((ready, ack, blocks), ([], 1, ((2, 4),)))

        ready, ack, blocks = receiver.process_incoming_packet(1, b"first")

        self.assertEqual(ready, [b"first", b"second", b"third"])
        self.assertEqual(ack, 4)
        self.assertEqual(blocks, ())
        print("PASS: After packet 1 was recovered, data was delivered 1-2-3.")
    def test_11_sender_gives_up_after_max_retries(self) -> None:
        """After MAX_RETRIES timeouts the sender is marked failed."""
        sender = SlidingWindowSender(stream_id=1, window_size=4, rto_seconds=0.001)
        sender.register_packet(FLAG_MSG, session_id=1, payload=b"x")
 
        for _ in range(SlidingWindowSender.MAX_RETRIES + 2):
            time.sleep(0.003)
            sender.get_packets_for_retransmission()
 
        self.assertTrue(sender.failed)
        print("PASS: Sender gave up after MAX_RETRIES.")

if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Step 5: UDP message routing and broadcast checks.

Run with: python -m test.test_step5
"""

import os
import shutil
import tempfile
import unittest

from test.net_helpers import Client, start_server, wait_until
from test.test_report import run_step_tests


class TestStep5MessageRouting(unittest.TestCase):
    def setUp(self):
        self.server = start_server()
        self.tmp = tempfile.mkdtemp(prefix="step5_")
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.stop()
        self.server.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_client(self, username):
        client = Client(
            self.server,
            username,
            os.path.join(self.tmp, username),
        )
        self.clients.append(client)
        return client

    def make_pair(self):
        alice = self.make_client("alice")
        bob = self.make_client("bob")
        self.assertTrue(wait_until(lambda: alice.peer("bob") and bob.peer("alice")))
        return alice, bob

    @staticmethod
    def received(client, stream_id):
        with client.state._lock:
            return list(client.state.streams.get(stream_id, []))

    def send_private(self, sender, recipient, stream_id, text):
        peer = sender.peer(recipient.username)
        self.assertIsNotNone(peer)
        sender.engine.send_message(
            peer["ip"],
            peer["port"],
            stream_id,
            text,
            target_session_id=peer["session_id"],
        )

    def test_01_private_message_reaches_its_intended_recipient(self):
        alice, bob = self.make_pair()

        self.send_private(alice, bob, 101, "private hello")

        self.assertTrue(wait_until(lambda: len(self.received(bob, 101)) == 1))
        self.assertEqual(self.received(bob, 101)[0]["text"], "private hello")

    def test_02_private_message_is_not_delivered_to_another_client(self):
        alice, bob = self.make_pair()
        charlie = self.make_client("charlie")
        self.assertTrue(wait_until(lambda: alice.peer("charlie") is not None))

        self.send_private(alice, bob, 102, "only for Bob")

        self.assertTrue(wait_until(lambda: len(self.received(bob, 102)) == 1))
        self.assertEqual(self.received(charlie, 102), [])

    def test_03_global_message_is_broadcast_to_all_other_clients(self):
        alice, bob = self.make_pair()
        charlie = self.make_client("charlie")
        self.assertTrue(wait_until(lambda: bob.peer("charlie") is not None))

        alice.engine.send_message("", 0, 0, "announcement")

        self.assertTrue(wait_until(
            lambda: len(self.received(bob, 0)) == 1
            and len(self.received(charlie, 0)) == 1
        ))
        self.assertEqual(self.received(bob, 0)[0]["text"], "announcement")
        self.assertEqual(self.received(charlie, 0)[0]["text"], "announcement")

    def test_04_global_broadcast_does_not_echo_to_sender(self):
        alice, bob = self.make_pair()

        alice.engine.send_message("", 0, 0, "no echo")

        self.assertTrue(wait_until(lambda: len(self.received(bob, 0)) == 1))
        self.assertEqual(self.received(alice, 0), [])

    def test_05_unicode_message_survives_encryption_and_relay(self):
        alice, bob = self.make_pair()
        message = "Xin chào 👋 — truyền dữ liệu tiếng Việt"

        self.send_private(alice, bob, 105, message)

        self.assertTrue(wait_until(lambda: len(self.received(bob, 105)) == 1))
        self.assertEqual(self.received(bob, 105)[0]["text"], message)

    def test_06_messages_on_one_stream_keep_their_send_order(self):
        alice, bob = self.make_pair()
        expected = [f"message-{index}" for index in range(6)]

        for message in expected:
            self.send_private(alice, bob, 106, message)

        self.assertTrue(wait_until(lambda: len(self.received(bob, 106)) == len(expected)))
        self.assertEqual(
            [message["text"] for message in self.received(bob, 106)],
            expected,
        )

    def test_07_messages_on_different_streams_remain_separate(self):
        alice, bob = self.make_pair()

        self.send_private(alice, bob, 107, "first stream")
        self.send_private(alice, bob, 207, "second stream")

        self.assertTrue(wait_until(
            lambda: len(self.received(bob, 107)) == 1
            and len(self.received(bob, 207)) == 1
        ))
        self.assertEqual(self.received(bob, 107)[0]["text"], "first stream")
        self.assertEqual(self.received(bob, 207)[0]["text"], "second stream")

    def test_08_received_message_uses_the_authenticated_sender_name(self):
        alice, bob = self.make_pair()

        self.send_private(alice, bob, 108, "identify sender")

        self.assertTrue(wait_until(lambda: len(self.received(bob, 108)) == 1))
        self.assertEqual(self.received(bob, 108)[0]["sender"], "alice")

    def test_09_private_message_rejects_an_unknown_recipient(self):
        alice, _ = self.make_pair()

        with self.assertRaises(ValueError):
            alice.engine.send_message("192.0.2.1", 12345, 109, "not routed")

    def test_10_concurrent_senders_can_route_to_the_same_recipient(self):
        alice, bob = self.make_pair()
        charlie = self.make_client("charlie")
        self.assertTrue(wait_until(lambda: alice.peer("charlie") is not None))
        self.assertTrue(wait_until(lambda: bob.peer("charlie") is not None))

        self.send_private(alice, charlie, 110, "from alice")
        self.send_private(bob, charlie, 110, "from bob")

        self.assertTrue(wait_until(lambda: len(self.received(charlie, 110)) == 2))
        self.assertEqual(
            {message["text"] for message in self.received(charlie, 110)},
            {"from alice", "from bob"},
        )


if __name__ == "__main__":
    run_step_tests(__name__)

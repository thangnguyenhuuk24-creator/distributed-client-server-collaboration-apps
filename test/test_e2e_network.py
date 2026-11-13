"""End-to-end tests: a REAL central server + REAL socket engines on localhost,
with packet loss injected on the clients' UDP sockets.

These fill the gaps left by the unit tests (steps 5, 6, 7, 10):
  * file transfer arrives intact although 10-15 % of datagrams are lost
  * private chat is delivered exactly once and in order despite loss
  * chat is not blocked while a big, lossy file transfer is running (no HoL)
  * a sender gives up instead of hanging when the receiver disappears

Run:  python -m test.test_e2e_network      (takes about 1 minute)
"""

import os
import shutil
import tempfile
import threading
import time
import unittest

from test.net_helpers import Client, sha256_of, start_server, wait_until

from core.state import private_stream_id


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        self.server = start_server()
        self.tmp = tempfile.mkdtemp(prefix="e2e_")
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.stop()
        self.server.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_pair(self, loss=0.0):
        alice = Client(self.server, "alice", os.path.join(self.tmp, "alice"),
                       loss=loss, seed=1)
        bob = Client(self.server, "bob", os.path.join(self.tmp, "bob"),
                     loss=loss, seed=2)
        self.clients += [alice, bob]
        self.assertTrue(wait_until(lambda: alice.peer("bob") and bob.peer("alice")),
                        "online roster never reached both clients")
        return alice, bob

    def make_file(self, size):
        path = os.path.join(self.tmp, f"src_{size}.bin")
        with open(path, "wb") as f:
            f.write(os.urandom(size))
        return path

    def send_file(self, sender, receiver_name, path, transfer_id):
        peer = sender.peer(receiver_name)
        sender.engine.send_file(peer["ip"], peer["port"], 4242, path,
                                transfer_id=transfer_id)

    def received_path(self, client, filename):
        found = [n for n in os.listdir(client.download_dir)
                 if n.startswith(os.path.splitext(filename)[0])]
        return os.path.join(client.download_dir, found[0]) if found else None

    def test_01_file_1mb_no_loss(self):
        alice, bob = self.make_pair()
        src = self.make_file(1_000_000)
        self.send_file(alice, "bob", src, "t1")
        self.assertTrue(wait_until(
            lambda: bob.state.file_transfers.get("t1", {}).get("status") == "COMPLETED", 15))
        self.assertEqual(sha256_of(src), sha256_of(self.received_path(bob, "src_1000000.bin")))

    def test_02_file_is_relayed_through_central_server(self):
        alice, bob = self.make_pair(loss=0.10)
        src = self.make_file(16_000)
        bob_peer = alice.peer("bob")

        alice.engine.send_file(
            bob_peer["ip"],
            bob_peer["port"],
            4243,
            src,
            transfer_id="relay-file",
            target_session_id=bob_peer["session_id"],
        )

        self.assertTrue(wait_until(
            lambda: bob.state.file_transfers.get("relay-file", {}).get("status") == "COMPLETED",
            20,
        ))
        self.assertEqual(sha256_of(src), sha256_of(
            self.received_path(bob, "src_16000.bin")
        ))
        self.assertTrue(any(
            message["sender"] == "alice" and message.get("file_id") == "relay-file"
            for message in bob.state.streams[0]
        ))
        snapshot = self.server.get_dashboard_snapshot()
        self.assertGreater(snapshot["metrics"]["file_relays"], 0)
        transfer = next(
            item for item in snapshot["file_transfers"]
            if item["file_id"] == "relay-file"
        )
        self.assertEqual(transfer["chunks_forwarded"], transfer["total_chunks"])
        self.assertEqual(transfer["chunks_acked"], transfer["total_chunks"])
        self.assertEqual(transfer["status"], "COMPLETED")
        self.assertGreater(transfer["retransmissions"], 0)
        self.assertGreater(snapshot["throughput"]["file_packets_per_second"], 0)
        self.assertGreater(alice.dropped + bob.dropped, 0)

    def test_03_file_survives_10_percent_packet_loss(self):
        alice, bob = self.make_pair(loss=0.10)
        src = self.make_file(150_000)
        self.send_file(alice, "bob", src, "t2")
        self.assertTrue(wait_until(
            lambda: bob.state.file_transfers.get("t2", {}).get("status") == "COMPLETED", 40))
        self.assertEqual(sha256_of(src), sha256_of(self.received_path(bob, "src_150000.bin")))
        self.assertGreater(alice.dropped + bob.dropped, 0, "no packet was actually dropped")

    def test_04_private_chat_in_order_exactly_once_with_loss(self):
        alice, bob = self.make_pair(loss=0.15)
        stream = private_stream_id(alice.session_id, bob.session_id)
        bp = alice.peer("bob")
        sent = [f"msg-{i:02d}" for i in range(20)]
        for text in sent:
            alice.engine.send_message(bp["ip"], bp["port"], stream, text)

        def received():
            with bob.state._lock:
                return [m["text"] for m in bob.state.streams.get(stream, [])]

        self.assertTrue(wait_until(lambda: len(received()) >= len(sent), 30))
        time.sleep(1.0)  # give duplicates a chance to show up
        self.assertEqual(received(), sent)
        self.assertGreater(
            self.server.get_dashboard_snapshot()["metrics"]["private_chat_relays"],
            0,
        )

    def test_05_chat_is_not_blocked_by_a_lossy_file_transfer(self):
        alice, bob = self.make_pair(loss=0.15)
        src = self.make_file(400_000)
        def background_upload():
            try:
                self.send_file(alice, "bob", src, "big")
            except (ConnectionError, TimeoutError):
                pass  # engines are stopped at the end of the test on purpose

        sender = threading.Thread(target=background_upload, daemon=True)
        sender.start()
        self.assertTrue(wait_until(lambda: "big" in alice.state.file_transfers, 5))

        for i in range(5):
            alice.engine.send_message("", 0, 0, f"hello-{i}")
        got = lambda: sum(1 for m in bob.state.streams.get(0, [])
                          if m["text"].startswith("hello-"))
        self.assertTrue(wait_until(lambda: got() == 5, 10), "chat was blocked")
        status = bob.state.file_transfers.get("big", {}).get("status")
        self.assertNotEqual(status, "COMPLETED",
                            "file finished too fast to prove chat ran in parallel")

    def test_06_sender_gives_up_when_receiver_disappears(self):
        alice, bob = self.make_pair()
        src = self.make_file(2_000_000)
        outcome = {}

        def run():
            try:
                self.send_file(alice, "bob", src, "dead")
                outcome["result"] = "finished"
            except Exception as exc:  # noqa: BLE001
                outcome["result"] = type(exc).__name__

        t = threading.Thread(target=run, daemon=True)
        t.start()
        time.sleep(0.3)
        bob.stop()  # receiver vanishes mid-transfer
        t.join(timeout=20)
        self.assertFalse(t.is_alive(), "sender hung forever")
        self.assertIn(outcome.get("result"), ("TimeoutError", "ConnectionError"))


if __name__ == "__main__":
    from test.test_report import run_test_module

    run_test_module(__name__)

"""End-to-end tests: a REAL central server + REAL socket engines on localhost,
with packet loss injected on the clients' UDP sockets.

These fill the gaps left by the unit tests (steps 5, 6, 7, 10):
  * file transfer arrives intact although 10-15 % of datagrams are lost
  * private chat is delivered exactly once and in order despite loss
  * chat is not blocked while a big, lossy file transfer is running (no HoL)
  * a sender gives up instead of hanging when the receiver disappears

Run:  python -m test.test_e2e_network      (takes about 1 minute)
"""

import base64
import json
import os
import socket
import shutil
import tempfile
import threading
import time
import urllib.request
import unittest

from test.net_helpers import Client, free_udp_port, sha256_of, start_server, wait_until

from core.protocol import FLAG_DISCOV, FLAG_FILE, Packet
from core.state import private_stream_id
from web.server import LocalAPIServer


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

    def test_07_clients_discover_and_message_each_other_after_server_stops(self):
        discovery_port = free_udp_port()
        clients = []
        for username in ("alice", "bob", "charlie"):
            client = Client(
                self.server,
                username,
                os.path.join(self.tmp, username),
                bind_host="0.0.0.0",
                discovery_port=discovery_port,
            )
            clients.append(client)
        self.clients.extend(clients)
        alice, bob, charlie = clients

        self.assertTrue(all(client.engine.using_central_server for client in clients))
        self.server.stop()

        self.assertTrue(wait_until(
            lambda: all(
                not client.engine.using_central_server
                and client.state.status == "CONNECTED"
                and client.engine.discovery.running
                and client.engine.recv_thread.is_alive()
                for client in clients
            ),
            10,
        ), "clients did not switch to P2P after central server shutdown")
        self.assertTrue(wait_until(
            lambda: all(
                all(client.peer(other.username) for other in clients if other is not client)
                for client in clients
            ),
            15,
        ), "clients did not discover each other after server shutdown")

        rogue = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            rogue.sendto(
                Packet(
                    flags=FLAG_DISCOV,
                    session_id=0xABCDEF01,
                    payload=json.dumps({
                        "username": "new-user",
                        "udp_port": 29999,
                    }).encode("utf-8"),
                ).pack(),
                ("127.0.0.1", bob.port),
            )
        finally:
            rogue.close()
        self.assertTrue(wait_until(
            lambda: bob.peer("new-user") is None,
            1,
        ), "a client that did not join the central server was admitted during fallback")

        peer = alice.peer("bob")
        stream_id = private_stream_id(alice.session_id, bob.session_id)
        alice.engine.send_message(
            peer["ip"],
            peer["port"],
            stream_id,
            "direct message after server shutdown",
        )
        self.assertTrue(wait_until(
            lambda: len(bob.state.streams.get(stream_id, [])) == 1,
            10,
        ), "P2P message did not reach Bob")
        self.assertEqual(
            bob.state.streams[stream_id][0]["text"],
            "direct message after server shutdown",
        )
        self.assertEqual(charlie.state.streams.get(stream_id, []), [])

    def test_08_file_broadcast_fanout_preserves_order_for_every_client(self):
        clients = []
        apis = []
        api_threads = []
        release_bob = threading.Event()
        bob_file_init_sent = threading.Event()
        try:
            for username in ("alice", "bob", "charlie"):
                client = Client(
                    self.server,
                    username,
                    os.path.join(self.tmp, username),
                )
                clients.append(client)
                self.clients.append(client)
            alice, bob, charlie = clients
            self.assertTrue(wait_until(
                lambda: all(
                    all(client.peer(other.username) for other in clients if other is not client)
                    for client in clients
                )
            ))

            for client in clients:
                api = LocalAPIServer(
                    client.state,
                    client.engine,
                    port=0,
                    history_db_path=os.path.join(self.tmp, f"{client.username}-history.sqlite3"),
                )
                thread = threading.Thread(target=api.start, daemon=True)
                thread.start()
                apis.append(api)
                api_threads.append(thread)
            self.assertTrue(wait_until(
                lambda: all(api.httpd is not None and api.port != 0 for api in apis)
            ))

            send_raw = alice.engine.send_raw
            paused = set()

            def pause_bob_after_file_init(raw, address):
                send_raw(raw, address)
                if not raw.startswith(b"RLY1"):
                    return
                envelope = json.loads(raw[4:].decode("utf-8"))
                if (
                    envelope.get("type") != "relay"
                    or envelope.get("target_session_id") != bob.session_id
                ):
                    return
                packet = Packet.unpack(base64.b64decode(envelope["packet"]))
                if packet.flags & FLAG_FILE and bob.session_id not in paused:
                    paused.add(bob.session_id)
                    bob_file_init_sent.set()
                    release_bob.wait(timeout=10)

            alice.engine.send_raw = pause_bob_after_file_init
            upload_request = urllib.request.Request(
                f"http://127.0.0.1:{apis[0].port}/api/send_file?stream_id=0",
                data=b"file broadcast payload",
                method="POST",
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-File-Name": "X.bin",
                },
            )
            with urllib.request.urlopen(upload_request, timeout=3) as response:
                self.assertEqual(response.status, 200)

            self.assertTrue(bob_file_init_sent.wait(timeout=5))
            self.assertTrue(wait_until(
                lambda: any(
                    message.get("filename") == "X.bin"
                    for message in charlie.state.streams.get(0, [])
                ),
                timeout=5,
            ), "Charlie did not see the file while Bob's transfer was still running")
            file_event_id = next(
                message["_event_id"]
                for message in alice.state.streams[0]
                if message.get("filename") == "X.bin"
            )
            file_timestamp = next(
                message["timestamp"]
                for message in alice.state.streams[0]
                if message.get("filename") == "X.bin"
            )

            message_request = urllib.request.Request(
                f"http://127.0.0.1:{apis[0].port}/api/send_message",
                data=json.dumps({
                    "stream_id": 0,
                    "content": "message-after-file",
                }).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(message_request, timeout=5) as response:
                self.assertEqual(response.status, 200)
            release_bob.set()

            for client in clients:
                self.assertTrue(wait_until(
                    lambda client=client: any(
                        message.get("text") == "message-after-file"
                        for message in client.state.streams.get(0, [])
                    ),
                    timeout=10,
                ))
            for api, client in zip(apis, clients):
                request = urllib.request.Request(
                    f"http://127.0.0.1:{api.port}/api/messages?stream_id=0"
                )
                with urllib.request.urlopen(request, timeout=3) as response:
                    messages = json.loads(response.read().decode("utf-8"))["messages"]
                file_message = next(item for item in messages if item.get("filename") == "X.bin")
                chat_message = next(
                    item for item in messages if item.get("text") == "message-after-file"
                )
                self.assertLess(file_message["timestamp"], chat_message["timestamp"])
                self.assertEqual(file_message["timestamp"], file_timestamp)
                received_file = next(
                    message["timestamp"]
                    for message in client.state.streams[0]
                    if message.get("_event_id") == file_event_id
                )
                self.assertEqual(file_message["timestamp"], received_file)
        finally:
            release_bob.set()
            for api in apis:
                api.stop()
            for thread in api_threads:
                thread.join(timeout=2)


if __name__ == "__main__":
    from test.test_report import run_test_module

    run_test_module(__name__)

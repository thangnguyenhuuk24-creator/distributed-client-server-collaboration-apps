import json
import os
import sqlite3
import socket
import threading
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from unittest.mock import patch

from core.state import ClientState, private_stream_id
from network.central_server import CentralServer
from network.socket_engine import SocketEngine
from web.server import LocalAPIServer


class AccountAndHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.auth_db = os.path.join(self.temp_dir.name, "accounts.sqlite3")
        self.server = CentralServer(
            "127.0.0.1",
            tcp_port=0,
            relay_port=0,
            server_name="Test server",
            auth_db_path=self.auth_db,
        )
        self.server.start()
        self.connections = []

    def tearDown(self):
        for connection in self.connections:
            try:
                connection.close()
            except OSError:
                pass
        self.server.stop()
        self.temp_dir.cleanup()

    def login(self, username, password):
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port),
            timeout=2,
        )
        self.connections.append(connection)
        request = {
            "type": "login",
            "username": username,
            "password": password,
            "server_name": "Test server",
            "udp_port": 30001,
        }
        connection.sendall(json.dumps(request).encode("utf-8") + b"\n")
        reader = connection.makefile("rb")
        try:
            return json.loads(reader.readline(65537).decode("utf-8"))
        finally:
            reader.close()

    def test_account_creation_password_check_and_reuse_after_restart(self):
        first = self.login("Alice", "correct-horse")
        self.assertTrue(first["success"])
        self.assertTrue(first["account_created"])
        self.assertEqual(first["server_name"], "Test server")

        self.connections[-1].close()
        self.server.stop()
        self.server = CentralServer(
            "127.0.0.1",
            tcp_port=0,
            relay_port=0,
            server_name="Test server",
            auth_db_path=self.auth_db,
        )
        self.server.start()

        rejected = self.login("alice", "wrong-password")
        self.assertFalse(rejected["success"])
        accepted = self.login("alice", "correct-horse")
        self.assertTrue(accepted["success"])
        self.assertFalse(accepted["account_created"])

    def test_server_name_is_discovered_when_client_omits_it(self):
        state = ClientState()
        engine = SocketEngine(
            state,
            host="127.0.0.1",
            port=self._free_udp_port(),
            server_host="127.0.0.1",
            server_tcp_port=self.server.tcp_port,
            server_udp_port=self.server.relay_port,
        )
        engine.start()
        try:
            engine.login("Alice", "server-name-password")
            self.assertEqual(engine.server_name, "Test server")
            engine.logout()
            engine.configure_p2p()
            self.assertFalse(engine.using_central_server)
            self.assertTrue(engine.discovery.running)
        finally:
            engine.stop()

    @staticmethod
    def _free_udp_port():
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        return port

    def test_history_is_unique_by_user_and_server_and_file_expires_to_metadata(self):
        history_db = os.path.join(self.temp_dir.name, "history.sqlite3")
        state = ClientState()
        state.configure_history("Alice", "alpha|127.0.0.1|9001|9002", history_db)
        state.add_message(0, "Alice (You)", "saved")
        state.add_message(41, "Alice (You)", "private", conversation_key="Bob")
        state.add_file_message(0, "Alice", "report.pdf", "file-1", "Đã nhận", 2048)
        file_message = state.streams[0][1]
        file_message["download_filename"] = "received.pdf"
        file_message["download_expires_at"] = time.time() + 60
        state._persist_message(0, file_message)

        state.configure_history("Bob", "alpha|127.0.0.1|9001|9002", history_db)
        self.assertEqual(state.streams, {0: []})
        state.configure_history("Alice", "alpha|127.0.0.1|9001|9002", history_db)
        self.assertEqual(state.streams[0][0]["text"], "saved")
        self.assertEqual(state.streams[41][0]["conversation_key"], "bob")

        expired = state.expire_file_messages(now=time.time() + 61)
        self.assertEqual(expired, ["received.pdf"])
        self.assertEqual(
            {
                key: value
                for key, value in state.streams[0][1].items()
                if not key.startswith("_")
            },
            {
                "message_id": state.streams[0][1]["message_id"],
                "sender": "Alice",
                "timestamp": state.streams[0][1]["timestamp"],
                "type": "file",
                "filename": "report.pdf",
                "filesize": 2048,
                "expired": True,
            },
        )

    def test_file_expiration_starts_after_transfer_completion(self):
        state = ClientState()
        started_at = time.time() - 120
        state.add_file_message(
            0,
            "Alice (You)",
            "large.bin",
            "large-file-id",
            "Đang gửi",
            30 * 1024 * 1024,
            event_id="large-file-event",
            timestamp=started_at,
            event_order=1,
        )

        self.assertEqual(state.expire_file_messages(now=started_at + 120), [])
        self.assertFalse(state.streams[0][0].get("expired", False))

        with patch("core.state.time.time", return_value=started_at + 120):
            state.update_file_message("large-file-id", "Đã gửi")

        self.assertEqual(
            state.expire_file_messages(now=started_at + 179),
            [],
        )
        self.assertEqual(
            state.expire_file_messages(now=started_at + 180),
            [],
        )
        self.assertTrue(state.streams[0][0]["expired"])

    def test_offline_file_expiration_uses_server_completion_time(self):
        state = ClientState()
        sent_at = time.time() - 120
        expires_at = time.time() + 60
        state.add_file_message(
            0,
            "Alice",
            "offline.bin",
            "offline-file-id",
            "Chờ nhận",
            30 * 1024 * 1024,
            event_id="offline-file-event",
            timestamp=sent_at,
            event_order=1,
            file_expires_at=expires_at,
        )

        self.assertEqual(state.expire_file_messages(), [])
        self.assertFalse(state.streams[0][0].get("expired", False))
        self.assertEqual(state.expire_file_messages(now=expires_at), [])
        self.assertTrue(state.streams[0][0]["expired"])

    def test_downloading_offline_file_does_not_extend_server_expiration(self):
        state = ClientState()
        expires_at = time.time() + 60
        state.add_file_message(
            0,
            "Alice",
            "offline.bin",
            "offline-file-id",
            "Chờ nhận",
            100,
            event_id="offline-file-event",
            timestamp=time.time() - 120,
            event_order=1,
            file_expires_at=expires_at,
        )

        with patch("core.state.time.time", return_value=expires_at - 1):
            state.update_file_message(
                "offline-file-id",
                "Đã nhận",
                "offline.bin",
                expires_at,
            )

        self.assertEqual(state.expire_file_messages(now=expires_at - 0.1), [])
        self.assertEqual(
            state.expire_file_messages(now=expires_at),
            ["offline.bin"],
        )
        self.assertTrue(state.streams[0][0]["expired"])

    def test_broadcast_reaches_all_peers_after_central_server_reconnect(self):
        def free_udp_port():
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
            probe.close()
            return port

        engines = []
        discovery_port = free_udp_port()

        def connect_user(username):
            state = ClientState()
            engine = SocketEngine(
                state,
                host="127.0.0.1",
                port=free_udp_port(),
                discovery_port=discovery_port,
                server_host="127.0.0.1",
                server_tcp_port=self.server.tcp_port,
                server_udp_port=self.server.relay_port,
                server_name="Test server",
            )
            engine.start()
            engine.login(username, "reconnect-password")
            engines.append(engine)
            return engine

        def wait_until(predicate, timeout=10):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if predicate():
                    return True
                time.sleep(0.02)
            return predicate()

        try:
            alice, bob, carol = (
                connect_user(name) for name in ("Alice", "Bob", "Carol")
            )
            self.assertTrue(wait_until(
                lambda: all(
                    len(engine.state.peers) == 2
                    for engine in (alice, bob, carol)
                )
            ))
            alice.send_message("", 0, 0, "before reconnect", target_session_id=0)
            self.assertTrue(wait_until(
                lambda: all(
                    any(
                        message.get("text") == "before reconnect"
                        for message in engine.state.streams[0]
                    )
                    for engine in (bob, carol)
                )
            ))

            self.server.stop()
            self.assertTrue(wait_until(
                lambda: all(not engine.using_central_server for engine in engines)
            ))
            self.assertTrue(wait_until(
                lambda: all(
                    any(
                        peer["username"] == other.state.username
                        for peer in engine.state.peers.values()
                    )
                    for engine in (alice, bob, carol)
                    for other in (alice, bob, carol)
                    if other is not engine
                )
            ))
            for recipient in ("Bob", "Carol"):
                peer = next(
                    peer for peer in alice.state.peers.values()
                    if peer["username"] == recipient
                )
                alice.send_message(
                    "127.0.0.1",
                    peer["port"],
                    0,
                    "Alice broadcast during outage",
                )
            self.assertTrue(wait_until(
                lambda: all(
                    any(
                        message.get("text") == "Alice broadcast during outage"
                        for message in engine.state.streams[0]
                    )
                    for engine in (bob, carol)
                )
            ))
            self.server.start()
            self.assertTrue(wait_until(
                lambda: all(
                    engine.using_central_server
                    and len(engine.state.peers) == 2
                    for engine in engines
                ),
                timeout=15,
            ))

            dave = connect_user("Dave")
            self.assertTrue(wait_until(
                lambda: len(dave.state.peers) == 3
                and all(len(engine.state.peers) == 3 for engine in engines[:3])
            ))
            dave.send_message("", 0, 0, "Dave broadcast", target_session_id=0)
            self.assertTrue(wait_until(
                lambda: all(
                    any(
                        message.get("text") == "Dave broadcast"
                        for message in engine.state.streams[0]
                    )
                    for engine in (alice, bob, carol)
                )
            ))

            alice.send_message("", 0, 0, "Alice broadcast after reconnect", target_session_id=0)
            self.assertTrue(wait_until(
                lambda: all(
                    any(
                        message.get("text") == "Alice broadcast after reconnect"
                        for message in engine.state.streams[0]
                    )
                    for engine in (bob, carol, dave)
                )
            ))
        finally:
            for engine in engines:
                engine.stop()

    def test_web_login_history_logout_and_server_down_admission(self):
        def free_udp_port():
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
            probe.close()
            return port

        state = ClientState()
        engine = SocketEngine(
            state,
            host="127.0.0.1",
            port=free_udp_port(),
            discovery_port=free_udp_port(),
        )
        engine.start()
        api = LocalAPIServer(
            state,
            engine,
            port=0,
            history_db_path=os.path.join(self.temp_dir.name, "web-history.sqlite3"),
        )
        api_thread = threading.Thread(target=api.start, daemon=True)
        api_thread.start()
        deadline = time.monotonic() + 2
        while (api.httpd is None or api.port == 0) and time.monotonic() < deadline:
            time.sleep(0.01)
        base_url = f"http://127.0.0.1:{api.port}"

        def request(path, body=None):
            payload = None if body is None else json.dumps(body).encode("utf-8")
            req = urllib.request.Request(
                base_url + path,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=3) as response:
                    return response.status, json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as error:
                return error.code, json.loads(error.read().decode("utf-8"))

        login = {
            "username": "WebUser",
            "password": "valid-password",
            "server_name": "Test server",
            "server_host": "127.0.0.1",
            "tcp_port": self.server.tcp_port,
            "relay_port": self.server.relay_port,
        }
        try:
            status, response = request("/api/login", login)
            self.assertEqual(status, 200)
            self.assertTrue(response["account_created"])
            state.add_message(0, "WebUser (You)", "persistent")
            self.assertEqual(request("/api/logout", {})[0], 200)

            status, response = request("/api/login", login)
            self.assertEqual(status, 200)
            self.assertFalse(response["account_created"])
            self.assertEqual(state.streams[0][0]["text"], "persistent")

            request("/api/logout", {})
            self.server.stop()
            status, response = request("/api/login", login)
            self.assertEqual(status, 502)
            self.assertIn("Không thể đăng nhập", response["error"])
            self.assertEqual(state.session_id, 0)
        finally:
            api.stop()
            engine.stop()
            api_thread.join(timeout=2)

    def test_clients_reconnect_and_sync_chat_file_metadata_after_server_restart(self):
        udp_probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_probe.bind(("127.0.0.1", 0))
        client_port = udp_probe.getsockname()[1]
        udp_probe.close()
        state = ClientState()
        state.configure_history(
            "ReconnectUser",
            "test server|127.0.0.1|9001|9002",
            os.path.join(self.temp_dir.name, "reconnect-history.sqlite3"),
        )
        engine = SocketEngine(
            state,
            host="127.0.0.1",
            port=client_port,
            server_host="127.0.0.1",
            server_tcp_port=self.server.tcp_port,
            server_udp_port=self.server.relay_port,
            server_name="Test server",
        )
        engine.start()
        try:
            original_session_id = engine.login("ReconnectUser", "reconnect-password")
            server_tcp_port = self.server.tcp_port
            server_relay_port = self.server.relay_port
            self.server.stop()

            deadline = time.monotonic() + 5
            while (
                engine.using_central_server or not engine.discovery.running
            ) and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertFalse(engine.using_central_server)
            self.assertTrue(engine.discovery.running)
            self.assertNotEqual(engine.discovery.broadcast_port, server_relay_port)

            chat_id, chat_time, chat_order = state.next_event_metadata()
            state.add_message(
                0,
                "ReconnectUser (You)",
                "sent during outage",
                event_id=chat_id,
                timestamp=chat_time,
                event_order=chat_order,
            )
            file_id, file_time, file_order = state.next_event_metadata()
            state.add_file_message(
                0,
                "ReconnectUser (You)",
                "outage.txt",
                file_id,
                "Đã gửi",
                12,
                event_id=file_id,
                timestamp=file_time,
                event_order=file_order,
            )

            self.server = CentralServer(
                "127.0.0.1",
                tcp_port=server_tcp_port,
                relay_port=server_relay_port,
                server_name="Test server",
                auth_db_path=self.auth_db,
            )
            self.server.start()
            deadline = time.monotonic() + 15
            while (
                not engine.using_central_server
                or self.server.get_dashboard_snapshot()["online_count"] != 1
                or self.server.get_dashboard_snapshot()["metrics"]["offline_chat_recovered"] != 1
                or self.server.get_dashboard_snapshot()["metrics"]["offline_file_recovered"] != 1
            ) and time.monotonic() < deadline:
                time.sleep(0.05)

            self.assertTrue(engine.using_central_server, "client did not reconnect")
            self.assertEqual(state.session_id, original_session_id)
            snapshot = self.server.get_dashboard_snapshot()
            self.assertEqual(snapshot["metrics"]["offline_chat_recovered"], 1)
            self.assertEqual(snapshot["metrics"]["offline_file_recovered"], 1)
            recovered = {
                event["event_id"]: event
                for event in snapshot["events"]
                if event["type"] in {"offline_chat_recovered", "offline_file_recovered"}
            }
            self.assertEqual(recovered[chat_id]["timestamp"], chat_time)
            self.assertEqual(recovered[file_id]["filename"], "outage.txt")

            self.server.stop()
            self.server = CentralServer(
                "127.0.0.1",
                tcp_port=server_tcp_port,
                relay_port=server_relay_port,
                server_name="Test server",
                auth_db_path=self.auth_db,
            )
            restored = self.server.get_dashboard_snapshot()
            self.assertEqual(restored["metrics"]["offline_chat_recovered"], 1)
            self.assertEqual(restored["metrics"]["offline_file_recovered"], 1)
            self.assertTrue(any(event["event_id"] == chat_id for event in restored["events"]))
            self.server.start()
            deadline = time.monotonic() + 15
            while (
                not engine.using_central_server
                or self.server.get_dashboard_snapshot()["online_count"] != 1
            ) and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertTrue(engine.using_central_server, "client did not reconnect after the next restart")
            self.assertEqual(state.session_id, original_session_id)
        finally:
            engine.stop()

    def test_offline_accounts_receive_queued_direct_and_broadcast_messages_and_files(self):
        bob_login = self.login("Bob", "offline-password")
        self.assertTrue(bob_login["success"])
        self.connections[-1].close()
        carol_login = self.login("Carol", "offline-password")
        self.assertTrue(carol_login["success"])
        self.connections[-1].close()
        deadline = time.monotonic() + 3
        while self.server.get_users() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(self.server.get_users(), [])

        def free_udp_port():
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
            probe.close()
            return port

        alice_state = ClientState()
        alice_engine = SocketEngine(
            alice_state,
            host="127.0.0.1",
            port=free_udp_port(),
        )
        alice_engine.start()
        alice_history_path = os.path.join(
            self.temp_dir.name,
            "alice-history.sqlite3",
        )
        api = LocalAPIServer(
            alice_state,
            alice_engine,
            port=0,
            history_db_path=alice_history_path,
        )
        api_thread = threading.Thread(target=api.start, daemon=True)
        api_thread.start()
        deadline = time.monotonic() + 2
        while (api.httpd is None or api.port == 0) and time.monotonic() < deadline:
            time.sleep(0.01)
        base_url = f"http://127.0.0.1:{api.port}"

        def request(path, body=None, raw=None, headers=None, method=None):
            payload = raw if raw is not None else (
                None if body is None else json.dumps(body).encode("utf-8")
            )
            req = urllib.request.Request(
                base_url + path,
                data=payload,
                method=method or ("POST" if payload is not None else "GET"),
                headers=headers or {"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                content = response.read()
                if response.headers.get_content_type() == "application/json":
                    return response.status, json.loads(content.decode("utf-8"))
                return response.status, content

        bob_engine = None
        carol_engine = None
        bob_api = None
        bob_api_thread = None
        try:
            _, alice_response = request("/api/login", body={
                "username": "Alice",
                "password": "alice-password",
                "server_host": "127.0.0.1",
                "tcp_port": self.server.tcp_port,
                "relay_port": self.server.relay_port,
            })
            alice_session = alice_response["session_id"]
            servers_directory = os.path.join(
                os.path.dirname(alice_history_path),
                "servers",
            )
            os.makedirs(servers_directory, exist_ok=True)
            for tcp_port, relay_port, name, host in (
                (
                    self.server.tcp_port,
                    self.server.relay_port,
                    "Test server",
                    "127.0.0.1",
                ),
                (9101, 9102, "Lâu đài tình ái", "127.0.0.3"),
            ):
                config_path = os.path.join(
                    servers_directory,
                    f"server_{tcp_port}_{relay_port}.json",
                )
                with open(config_path, "w", encoding="utf-8") as config_file:
                    json.dump(
                        {"server_name": name, "host": host},
                        config_file,
                        ensure_ascii=False,
                    )
            with sqlite3.connect(alice_history_path) as database:
                database.execute(
                    "INSERT INTO chat_history VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        "legacy-user",
                        "lâu đài tình ái|127.0.0.3|9101|9102",
                        0,
                        "legacy-message",
                        time.time() - 2,
                        "{}",
                    ),
                )
            _, server_choices = request("/api/servers")
            self.assertEqual(
                server_choices["selected_id"],
                f"127.0.0.1|{self.server.tcp_port}|{self.server.relay_port}",
            )
            self.assertEqual(
                server_choices["servers"][0]["name"],
                "Test server",
            )
            self.assertEqual(
                {server["name"] for server in server_choices["servers"]},
                {
                    "Test server",
                    "Lâu đài tình ái",
                },
            )
            _, directory = request("/api/peers")
            offline_names = {
                user["username"]
                for user in directory
                if not user["online"]
            }
            self.assertEqual(offline_names, {"Bob", "Carol"})

            private_stream = private_stream_id(alice_session, bob_login["session_id"])
            request("/api/send_message", body={
                "stream_id": private_stream,
                "content": "private while Bob is offline",
                "recipient_username": "Bob",
            })
            request("/api/send_message", body={
                "stream_id": 0,
                "content": "broadcast while Bob is offline",
            })

            for stream_id, filename, content in (
                (private_stream, "private.txt", b"private file"),
                (0, "broadcast.txt", b"broadcast file"),
            ):
                query = urllib.parse.urlencode({
                    "stream_id": stream_id,
                    "recipient_username": "Bob" if stream_id else "",
                })
                request(
                    f"/api/send_file?{query}",
                    raw=content,
                    headers={
                        "Content-Type": "application/octet-stream",
                        "X-File-Name": urllib.parse.quote(filename),
                    },
                )

            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                _, bob_total = self.server._get_offline_messages("bob", 0, 5)
                _, carol_total = self.server._get_offline_messages("carol", 0, 5)
                if bob_total == 4 and carol_total == 2:
                    break
                time.sleep(0.02)
            self.assertEqual(bob_total, 4)
            self.assertEqual(carol_total, 2)
            with sqlite3.connect(self.auth_db) as database:
                database.execute(
                    "UPDATE offline_mailbox SET event_timestamp = ? "
                    "WHERE message_type = 'file' "
                    "AND recipient_key IN ('bob', 'carol')",
                    (time.time() - 120,),
                )
            queued_files, _ = self.server._get_offline_messages("bob", 0, 5)
            self.assertTrue(all(
                item["file_available"] and not item["expired"]
                for item in queued_files
                if item["message_type"] == "file"
            ))

            bob_state = ClientState()
            bob_engine = SocketEngine(
                bob_state,
                host="127.0.0.1",
                port=free_udp_port(),
                server_host="127.0.0.1",
                server_tcp_port=self.server.tcp_port,
                server_udp_port=self.server.relay_port,
                server_name="Test server",
            )
            bob_engine.stream_manager.download_dir = os.path.join(
                self.temp_dir.name,
                "bob-downloads",
            )
            bob_engine.start()
            bob_engine.login("Bob", "offline-password")
            bob_state.configure_history(
                "Bob",
                "test server|127.0.0.1|9001|9002",
                os.path.join(self.temp_dir.name, "bob-history.sqlite3"),
            )
            bob_engine.sync_offline_mailbox()

            private_messages = bob_state.streams[private_stream]
            self.assertTrue(any(
                message.get("text") == "private while Bob is offline"
                for message in private_messages
            ))
            self.assertTrue(any(
                message.get("text") == "broadcast while Bob is offline"
                for message in bob_state.streams[0]
            ))
            queued_files = [
                message
                for messages in bob_state.streams.values()
                for message in messages
                if message.get("type") == "file"
            ]
            self.assertEqual(len(queued_files), 2)
            available_file = next(
                message for message in queued_files
                if message["filename"] == "private.txt"
            )
            self.assertEqual(available_file["status"], "Chờ nhận")
            bob_api = LocalAPIServer(
                bob_state,
                bob_engine,
                port=0,
                history_db_path=os.path.join(
                    self.temp_dir.name,
                    "bob-history.sqlite3",
                ),
            )
            bob_api_thread = threading.Thread(target=bob_api.start, daemon=True)
            bob_api_thread.start()
            deadline = time.monotonic() + 2
            while (bob_api.httpd is None or bob_api.port == 0) and time.monotonic() < deadline:
                time.sleep(0.01)
            download_request = urllib.request.Request(
                f"http://127.0.0.1:{bob_api.port}/api/offline-files/"
                f"{available_file['file_id']}"
            )
            with urllib.request.urlopen(download_request, timeout=5) as response:
                self.assertTrue(json.loads(response.read())["success"])
            with open(
                os.path.join(bob_engine.stream_manager.download_dir, "private.txt"),
                "rb",
            ) as downloaded:
                self.assertEqual(downloaded.read(), b"private file")

            carol_state = ClientState()
            carol_engine = SocketEngine(
                carol_state,
                host="127.0.0.1",
                port=free_udp_port(),
                server_host="127.0.0.1",
                server_tcp_port=self.server.tcp_port,
                server_udp_port=self.server.relay_port,
                server_name="Test server",
            )
            carol_engine.start()
            carol_engine.login("Carol", "offline-password")
            carol_state.configure_history(
                "Carol",
                "test server|127.0.0.1|9001|9002",
                os.path.join(self.temp_dir.name, "carol-history.sqlite3"),
            )
            carol_engine.sync_offline_mailbox()
            self.assertTrue(any(
                message.get("text") == "broadcast while Bob is offline"
                for message in carol_state.streams[0]
            ))
            self.assertTrue(any(
                message.get("filename") == "broadcast.txt"
                for message in carol_state.streams[0]
            ))
            restored_state = ClientState()
            restored_state.configure_history(
                "Bob",
                "test server|127.0.0.1|9001|9002",
                os.path.join(self.temp_dir.name, "bob-history.sqlite3"),
            )
            self.assertTrue(any(
                message.get("text") == "private while Bob is offline"
                for message in restored_state.streams[private_stream]
            ))

            with sqlite3.connect(self.auth_db) as database:
                database.execute(
                    "UPDATE offline_mailbox SET expires_at = ? "
                    "WHERE recipient_key IN ('bob', 'carol') AND message_type = 'file'",
                    (time.time() - 1,),
                )
            expired_mail, _ = self.server._get_offline_messages("bob", 0, 5)
            carol_expired_mail, _ = self.server._get_offline_messages("carol", 0, 5)
            self.assertTrue(all(
                item["expired"] for item in expired_mail
                if item["message_type"] == "file"
            ))
            self.assertTrue(all(
                item["expired"] for item in carol_expired_mail
                if item["message_type"] == "file"
            ))
            with sqlite3.connect(self.auth_db) as database:
                self.assertEqual(
                    database.execute(
                        "SELECT COUNT(*) FROM offline_mailbox "
                        "WHERE recipient_key IN ('bob', 'carol') "
                        "AND message_type = 'file' "
                        "AND attachment_path IS NOT NULL"
                    ).fetchone()[0],
                    0,
                )
        finally:
            if bob_api:
                bob_api.stop()
            api.stop()
            alice_engine.stop()
            if bob_engine:
                bob_engine.stop()
            if carol_engine:
                carol_engine.stop()
            api_thread.join(timeout=2)
            if bob_api_thread:
                bob_api_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()

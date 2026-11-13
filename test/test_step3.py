"""Step 3: authentication and connected-user state checks.

Run with: python -m test.test_step3
"""

import json
import socket
import time
import unittest

from network.central_server import CentralServer
from test.test_report import run_step_tests


class TestStep3AuthenticationAndState(unittest.TestCase):
    def setUp(self):
        self.server = CentralServer("127.0.0.1", tcp_port=0, relay_port=0)
        self.server.start()
        self.connections = []

    def tearDown(self):
        for connection in self.connections:
            try:
                connection.close()
            except OSError:
                pass
        self.server.stop()

    def login(self, username, udp_port=25000):
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port), timeout=2
        )
        connection.settimeout(2)
        self.connections.append(connection)
        request = {"type": "login", "username": username, "udp_port": udp_port}
        connection.sendall(json.dumps(request).encode("utf-8") + b"\n")
        reader = connection.makefile("rb")
        try:
            response = json.loads(reader.readline(65537).decode("utf-8"))
        finally:
            reader.close()
        return response, connection

    def wait_until(self, predicate, timeout=2.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.01)
        return predicate()

    def test_01_valid_login_creates_an_online_session(self):
        response, _ = self.login("Alice")

        self.assertTrue(response["success"])
        self.assertGreater(response["session_id"], 0)
        self.assertEqual(len(self.server.get_users()), 1)

    def test_02_server_tracks_client_ip_and_active_ports(self):
        response, _ = self.login("Bob", udp_port=26002)
        self.assertTrue(response["success"])
        self.assertTrue(self.wait_until(lambda: len(self.server.get_users()) == 1))

        user = self.server.get_users()[0]
        self.assertEqual(user["ip"], "127.0.0.1")
        self.assertGreater(user["tcp_port"], 0)
        self.assertEqual(user["udp_port"], 26002)

    def test_03_session_ids_are_unique_and_fit_unsigned_32_bits(self):
        first, _ = self.login("First", 26003)
        second, _ = self.login("Second", 26004)

        self.assertTrue(first["success"])
        self.assertTrue(second["success"])
        self.assertNotEqual(first["session_id"], second["session_id"])
        self.assertLessEqual(second["session_id"], 0xFFFFFFFF)

    def test_04_username_whitespace_is_trimmed_before_registration(self):
        response, _ = self.login("  Alice  ")

        self.assertTrue(response["success"])
        self.assertEqual(self.server.get_users()[0]["username"], "Alice")

    def test_05_duplicate_username_is_rejected_without_replacing_session(self):
        first, _ = self.login("Alice", 26005)
        second, _ = self.login("alice", 26006)

        self.assertTrue(first["success"])
        self.assertFalse(second["success"])
        self.assertIn("already online", second["error"])
        self.assertEqual(len(self.server.get_users()), 1)

    def test_06_empty_username_is_rejected(self):
        response, _ = self.login("")

        self.assertFalse(response["success"])
        self.assertIn("Username", response["error"])
        self.assertEqual(self.server.get_users(), [])

    def test_07_whitespace_only_username_is_rejected(self):
        response, _ = self.login("   ")

        self.assertFalse(response["success"])
        self.assertEqual(self.server.get_users(), [])

    def test_08_out_of_range_udp_port_is_rejected(self):
        response, _ = self.login("BadPort", 65536)

        self.assertFalse(response["success"])
        self.assertIn("UDP port", response["error"])

    def test_09_new_login_receives_existing_online_user_details(self):
        first, _ = self.login("Alice", 26009)
        second, _ = self.login("Bob", 26010)

        self.assertTrue(first["success"])
        self.assertTrue(second["success"])
        self.assertEqual(second["peers"][0]["username"], "Alice")
        self.assertEqual(second["peers"][0]["udp_port"], 26009)
        self.assertEqual(second["peers"][0]["ip"], "127.0.0.1")

    def test_10_disconnected_session_is_removed_from_online_state(self):
        response, connection = self.login("Leaving", 26011)
        self.assertTrue(response["success"])

        connection.close()

        self.assertTrue(self.wait_until(lambda: self.server.get_users() == []))
        self.assertEqual(
            self.server.get_dashboard_snapshot()["metrics"]["disconnects"], 1
        )


if __name__ == "__main__":
    run_step_tests(__name__)

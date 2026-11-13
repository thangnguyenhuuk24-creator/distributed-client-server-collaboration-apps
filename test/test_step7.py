"""Step 7: network failure handling and graceful shutdown checks.

Run with: python -m test.test_step7
"""

import base64
import json
import socket
import time
import unittest

from core.protocol import Packet
from network.central_server import CentralServer, RELAY_PREFIX
from test.test_report import run_step_tests


class TestStep7NetworkResilience(unittest.TestCase):
    def setUp(self):
        self.server = CentralServer(
            "127.0.0.1",
            tcp_port=0,
            relay_port=0,
            client_timeout=0.5,
        )
        self.server.start()
        self.connections = []
        self.udp_sockets = []

    def tearDown(self):
        for connection in self.connections:
            try:
                connection.close()
            except OSError:
                pass
        for udp_socket in self.udp_sockets:
            udp_socket.close()
        self.server.stop()

    def wait_until(self, predicate, timeout=2.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.01)
        return predicate()

    def wait_for_warning(self, reason):
        return self.wait_until(lambda: any(
            event["type"] == "network_warning"
            and reason in event.get("reason", "")
            for event in self.server.get_dashboard_snapshot()["events"]
        ))

    def login(self, username="Resilient", udp_port=None):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(("127.0.0.1", 0))
        self.udp_sockets.append(udp_socket)
        active_udp_port = udp_port or udp_socket.getsockname()[1]
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port), timeout=2
        )
        connection.settimeout(2)
        self.connections.append(connection)
        connection.sendall(json.dumps({
            "type": "login",
            "username": username,
            "udp_port": active_udp_port,
        }).encode("utf-8") + b"\n")
        reader = connection.makefile("rb")
        try:
            response = json.loads(reader.readline(65537).decode("utf-8"))
        finally:
            reader.close()
        return response, connection, udp_socket

    def send_relay(self, udp_socket, envelope):
        udp_socket.sendto(
            RELAY_PREFIX + json.dumps(envelope).encode("utf-8"),
            ("127.0.0.1", self.server.relay_port),
        )

    def test_01_malformed_tcp_json_does_not_crash_server(self):
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port), timeout=2
        )
        self.connections.append(connection)
        connection.sendall(b"{not-json}\n")

        self.assertTrue(self.wait_for_warning("JSONDecodeError"))
        self.assertTrue(self.server.running)
        self.assertEqual(self.server.get_users(), [])

    def test_02_incomplete_login_request_is_rejected_without_registration(self):
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port), timeout=2
        )
        connection.settimeout(2)
        self.connections.append(connection)
        connection.shutdown(socket.SHUT_WR)
        reader = connection.makefile("rb")
        try:
            response = json.loads(reader.readline(65537).decode("utf-8"))
        finally:
            reader.close()

        self.assertFalse(response["success"])
        self.assertEqual(self.server.get_users(), [])
        self.assertTrue(self.server.running)

    def test_03_silent_logged_in_client_is_removed_after_timeout(self):
        response, _, _ = self.login("Silent")

        self.assertTrue(response["success"])
        self.assertTrue(self.wait_until(lambda: self.server.get_users() == [], 2.0))
        self.assertTrue(self.server.running)

    def test_04_abrupt_tcp_disconnect_removes_client_state(self):
        response, connection, _ = self.login("Abrupt")
        self.assertTrue(response["success"])
        self.assertTrue(self.wait_until(lambda: len(self.server.get_users()) == 1))

        connection.close()

        self.assertTrue(self.wait_until(lambda: self.server.get_users() == []))

    def test_05_unknown_udp_datagram_is_logged_and_ignored(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(("127.0.0.1", 0))
        self.udp_sockets.append(udp_socket)
        udp_socket.sendto(b"not-a-relay", ("127.0.0.1", self.server.relay_port))

        self.assertTrue(self.wait_for_warning("Unknown UDP datagram"))
        self.assertTrue(self.server.running)

    def test_06_malformed_relay_json_is_handled_without_stopping_relay(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(("127.0.0.1", 0))
        self.udp_sockets.append(udp_socket)
        udp_socket.sendto(RELAY_PREFIX + b"{bad-json", ("127.0.0.1", self.server.relay_port))

        self.assertTrue(self.wait_for_warning("JSONDecodeError"))
        self.assertTrue(self.server.running)

    def test_07_invalid_base64_relay_packet_is_dropped(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(("127.0.0.1", 0))
        self.udp_sockets.append(udp_socket)
        self.send_relay(udp_socket, {
            "type": "relay",
            "sender_session_id": 1,
            "target_session_id": 2,
            "packet": "!",
        })

        self.assertTrue(self.wait_for_warning("Invalid relay packet"))
        self.assertTrue(self.server.running)

    def test_08_invalid_packet_bytes_are_dropped_and_reported(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(("127.0.0.1", 0))
        self.udp_sockets.append(udp_socket)
        self.send_relay(udp_socket, {
            "type": "relay",
            "sender_session_id": 1,
            "target_session_id": 2,
            "packet": base64.b64encode(b"short").decode("ascii"),
        })

        self.assertTrue(self.wait_for_warning("Invalid relay packet"))
        self.assertTrue(self.server.running)

    def test_09_relay_from_unregistered_sender_is_rejected(self):
        response, _, udp_socket = self.login("ValidSender")
        self.assertTrue(response["success"])
        packet = Packet(flags=0, session_id=response["session_id"]).pack()
        self.send_relay(udp_socket, {
            "type": "relay",
            "sender_session_id": 0xFFFFFFFF,
            "target_session_id": 1,
            "packet": base64.b64encode(packet).decode("ascii"),
        })

        self.assertTrue(self.wait_for_warning("Relay sender authentication failed"))
        self.assertEqual(len(self.server.get_users()), 1)

    def test_10_server_shutdown_closes_clients_gracefully(self):
        response, connection, _ = self.login("Shutdown")
        self.assertTrue(response["success"])

        self.server.stop()
        connection.settimeout(2)
        while connection.recv(65536):
            pass

        self.assertFalse(self.server.running)
        self.assertEqual(self.server.get_users(), [])


if __name__ == "__main__":
    run_step_tests(__name__)

"""Step 2: beginner-friendly TCP client/server handshake checks.

Run this file with:
    python -m test.test_step2

Example result:
    Result: 11/11 tests passed
"""

import json
import socket
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Tuple

from core.state import ClientState
from network.central_client import CentralClientConnection
from network.central_server import CentralServer


class TestStep2TCPHandshake(unittest.TestCase):
    """Verify that a TCP client can connect, log in, and receive an ACK."""

    def setUp(self) -> None:
        self.server = CentralServer("127.0.0.1", tcp_port=0, relay_port=0)
        self.server.start()
        self.connections: List[socket.socket] = []
        self.connection_lock = threading.Lock()
        self.client_connections: List[CentralClientConnection] = []

    def tearDown(self) -> None:
        for client in self.client_connections:
            client.close()
        for connection in self.connections:
            try:
                connection.close()
            except OSError:
                pass
        self.server.stop()

    def _connect_and_login(self, username: str, udp_port: int = 25000) -> Tuple[Dict[str, Any], socket.socket]:
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port),
            timeout=2,
        )
        connection.settimeout(2)
        with self.connection_lock:
            self.connections.append(connection)

        request = {
            "type": "login",
            "username": username,
            "udp_port": udp_port,
        }
        connection.sendall(json.dumps(request).encode("utf-8") + b"\n")
        reader = connection.makefile("rb")
        try:
            response_line = reader.readline(65537)
        finally:
            reader.close()
        self.assertTrue(response_line, "Server should return a login acknowledgment")
        self.assertTrue(response_line.endswith(b"\n"), "Acknowledgment should end with a newline")
        return json.loads(response_line.decode("utf-8")), connection

    def _wait_until(self, condition, timeout: float = 2.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return True
            time.sleep(0.01)
        return condition()

    def test_01_tcp_server_accepts_a_client_connection(self) -> None:
        """A client can open a TCP connection to the server's listening port."""
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port),
            timeout=2,
        )
        self.connections.append(connection)
        self.assertTrue(self._wait_until(
            lambda: self.server.get_dashboard_snapshot()["metrics"]["tcp_connections"] >= 1
        ))

    def test_02_client_receives_successful_login_acknowledgment(self) -> None:
        """A valid login receives success=True and a server-issued session ID."""
        state = ClientState()
        client = CentralClientConnection(
            state,
            "127.0.0.1",
            self.server.tcp_port,
            local_udp_port=25001,
        )
        self.client_connections.append(client)

        session_id = client.login("Alice")

        self.assertGreater(session_id, 0)
        self.assertEqual(state.username, "Alice")
        self.assertEqual(state.session_id, session_id)
        self.assertEqual(state.status, "CONNECTED")

    def test_03_server_records_username_ip_and_udp_port(self) -> None:
        """The server's online list shows the logged-in user and endpoint."""
        response, _ = self._connect_and_login("Bob", udp_port=25002)

        self.assertTrue(response["success"])
        self.assertTrue(self._wait_until(lambda: len(self.server.get_users()) == 1))
        user = self.server.get_users()[0]
        self.assertEqual(user["username"], "Bob")
        self.assertEqual(user["ip"], "127.0.0.1")
        self.assertEqual(user["udp_port"], 25002)
        self.assertGreater(user["tcp_port"], 0)

    def test_04_server_rejects_an_empty_username(self) -> None:
        """A login without a username receives a clear rejection acknowledgment."""
        response, _ = self._connect_and_login("   ")

        self.assertFalse(response["success"])
        self.assertIn("Username", response["error"])

    def test_05_server_rejects_an_invalid_udp_port(self) -> None:
        """A port outside the valid 1-65535 range cannot be registered."""
        response, _ = self._connect_and_login("InvalidPort", udp_port=65536)

        self.assertFalse(response["success"])
        self.assertIn("UDP port", response["error"])

    def test_06_server_rejects_duplicate_online_usernames(self) -> None:
        """A second login using the same name, ignoring letter case, is rejected."""
        first_response, _ = self._connect_and_login("Alice")
        second_response, _ = self._connect_and_login("alice", udp_port=25003)

        self.assertTrue(first_response["success"])
        self.assertFalse(second_response["success"])
        self.assertIn("already online", second_response["error"])

    def test_07_new_client_receives_roster_of_online_users(self) -> None:
        """The login acknowledgment tells a new client who is already online."""
        first_response, _ = self._connect_and_login("Alice")
        second_response, _ = self._connect_and_login("Bob", udp_port=25004)

        self.assertTrue(first_response["success"])
        self.assertEqual(
            [peer["username"] for peer in second_response["peers"]],
            ["Alice"],
        )

    def test_08_closing_tcp_connection_removes_user_from_online_list(self) -> None:
        """Closing the persistent TCP connection marks the user offline."""
        response, connection = self._connect_and_login("Leaving")
        self.assertTrue(response["success"])
        self.assertTrue(self._wait_until(lambda: len(self.server.get_users()) == 1))

        connection.close()

        self.assertTrue(self._wait_until(lambda: self.server.get_users() == []))

    def test_09_server_handles_several_clients_connecting_together(self) -> None:
        """Ten clients can complete the TCP login handshake concurrently."""

        def connect_client(index: int) -> Dict[str, Any]:
            response, _ = self._connect_and_login(f"Client{index}", 25100 + index)
            return response

        with ThreadPoolExecutor(max_workers=10) as executor:
            responses = list(executor.map(connect_client, range(10)))

        self.assertTrue(all(response["success"] for response in responses))
        self.assertTrue(self._wait_until(lambda: len(self.server.get_users()) == 10))
        self.assertEqual(len({response["session_id"] for response in responses}), 10)

    def test_10_malformed_login_request_is_closed_without_success(self) -> None:
        """Invalid JSON cannot be mistaken for a successful login."""
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port),
            timeout=2,
        )
        connection.settimeout(2)
        self.connections.append(connection)
        connection.sendall(b"{this is not JSON}\n")

        self.assertEqual(connection.recv(1), b"")
        self.assertEqual(self.server.get_users(), [])

    def test_11_silent_client_is_removed_after_timeout(self) -> None:
        """A client that logs in then stays silent is dropped after the timeout."""
        self.server.stop()
        self.server = CentralServer(
            "127.0.0.1", tcp_port=0, relay_port=0, client_timeout=1.0
        )
        self.server.start()
    
        response, _ = self._connect_and_login("Ghost")  # đăng nhập rồi im lặng
    
        self.assertTrue(response["success"])
        self.assertTrue(self._wait_until(lambda: self.server.get_users() == [], timeout=4))

if __name__ == "__main__":
    from test.test_report import run_step_tests

    run_step_tests(__name__)

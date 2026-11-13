"""Step 4: concurrent client connection and disconnection checks.

Run with: python -m test.test_step4
"""

import json
import socket
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from network.central_server import CentralServer
from test.test_report import run_step_tests


class TestStep4Concurrency(unittest.TestCase):
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

    def login(self, username, udp_port):
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port), timeout=3
        )
        connection.settimeout(3)
        request = {"type": "login", "username": username, "udp_port": udp_port}
        connection.sendall(json.dumps(request).encode("utf-8") + b"\n")
        reader = connection.makefile("rb")
        try:
            response = json.loads(reader.readline(65537).decode("utf-8"))
        finally:
            reader.close()
        self.connections.append(connection)
        return response, connection

    def wait_until(self, predicate, timeout=3.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.01)
        return predicate()

    def test_01_ten_clients_can_login_at_the_same_time(self):
        with ThreadPoolExecutor(max_workers=10) as pool:
            responses = list(pool.map(
                lambda index: self.login(f"Client{index}", 27000 + index),
                range(10),
            ))

        self.assertTrue(all(response["success"] for response, _ in responses))
        self.assertTrue(self.wait_until(lambda: len(self.server.get_users()) == 10))

    def test_02_concurrent_logins_receive_distinct_session_ids(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(
                lambda index: self.login(f"User{index}", 27100 + index)[0],
                range(8),
            ))

        session_ids = [response["session_id"] for response in responses]
        self.assertEqual(len(set(session_ids)), 8)

    def test_03_server_records_each_concurrent_clients_tcp_and_udp_ports(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(
                lambda index: self.login(f"Peer{index}", 27200 + index),
                range(6),
            ))

        self.assertTrue(self.wait_until(lambda: len(self.server.get_users()) == 6))
        users = self.server.get_users()
        self.assertEqual({user["udp_port"] for user in users}, set(range(27200, 27206)))
        self.assertEqual(len({user["tcp_port"] for user in users}), 6)

    def test_04_simultaneous_duplicate_logins_allow_only_one_session(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(
                lambda index: self.login("SharedName", 27300 + index)[0],
                range(8),
            ))

        self.assertEqual(sum(response["success"] for response in responses), 1)
        self.assertEqual(len(self.server.get_users()), 1)

    def test_05_all_clients_can_disconnect_concurrently(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            clients = list(pool.map(
                lambda index: self.login(f"Leaving{index}", 27400 + index),
                range(6),
            ))
        self.assertTrue(self.wait_until(lambda: len(self.server.get_users()) == 6))

        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda item: item[1].close(), clients))

        self.assertTrue(self.wait_until(lambda: self.server.get_users() == []))

    def test_06_username_can_reconnect_after_previous_session_closes(self):
        first, connection = self.login("Reconnect", 27501)
        connection.close()
        self.assertTrue(self.wait_until(lambda: self.server.get_users() == []))

        second, _ = self.login("Reconnect", 27502)

        self.assertTrue(first["success"])
        self.assertTrue(second["success"])
        self.assertNotEqual(first["session_id"], second["session_id"])

    def test_07_parallel_connection_churn_leaves_no_stale_sessions(self):
        def connect_disconnect(index):
            response, connection = self.login(f"Churn{index}", 27600 + index)
            connection.close()
            return response["success"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertTrue(all(pool.map(connect_disconnect, range(8))))

        self.assertTrue(self.wait_until(lambda: self.server.get_users() == []))

    def test_08_metrics_count_successful_parallel_logins(self):
        with ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(
                lambda index: self.login(f"Metric{index}", 27700 + index),
                range(5),
            ))

        metrics = self.server.get_dashboard_snapshot()["metrics"]
        self.assertEqual(metrics["successful_logins"], 5)
        self.assertEqual(metrics["tcp_connections"], 5)

    def test_09_server_accepts_a_new_client_after_many_disconnects(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            clients = list(pool.map(
                lambda index: self.login(f"Batch{index}", 27800 + index),
                range(4),
            ))
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda item: item[1].close(), clients))
        self.assertTrue(self.wait_until(lambda: self.server.get_users() == []))

        response, _ = self.login("AfterBatch", 27810)

        self.assertTrue(response["success"])
        self.assertEqual(len(self.server.get_users()), 1)

    def test_10_server_remains_running_after_concurrent_client_exits(self):
        with ThreadPoolExecutor(max_workers=5) as pool:
            clients = list(pool.map(
                lambda index: self.login(f"Exit{index}", 27900 + index),
                range(5),
            ))
        with ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(lambda item: item[1].close(), clients))

        self.assertTrue(self.wait_until(lambda: self.server.get_users() == []))
        self.assertTrue(self.server.running)
        self.assertEqual(self.server.get_dashboard_snapshot()["status"], "online")


if __name__ == "__main__":
    run_step_tests(__name__)

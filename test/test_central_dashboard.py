import json
import socket
import time
import urllib.request
import unittest

from network.central_server import CentralServer
from web.central_dashboard import CentralDashboardServer


class CentralDashboardTest(unittest.TestCase):
    def setUp(self):
        self.server = CentralServer(host="127.0.0.1", tcp_port=0, relay_port=0)
        self.server.start()
        self.dashboard = CentralDashboardServer(
            self.server,
            host="127.0.0.1",
            port=0,
        )
        self.dashboard.start()
        self.base_url = f"http://127.0.0.1:{self.dashboard.port}"
        self.client_connections = []

    def tearDown(self):
        for connection in self.client_connections:
            connection.close()
        self.dashboard.stop()
        self.server.stop()

    def request(self, path, method="GET", body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.status, response.read()

    def test_dashboard_reports_status_and_exports_and_clears_logs(self):
        unknown_datagram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        unknown_datagram.sendto(
            b"not-a-relay-packet",
            ("127.0.0.1", self.server.relay_port),
        )
        unknown_datagram.close()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if any(
                event["type"] == "network_warning"
                for event in self.server.get_dashboard_snapshot()["events"]
            ):
                break
            time.sleep(0.01)

        status, body = self.request("/api/dashboard")
        snapshot = json.loads(body)
        self.assertEqual(status, 200)
        self.assertEqual(snapshot["status"], "online")
        self.assertEqual(snapshot["listeners"]["tcp"]["port"], self.server.tcp_port)
        self.assertEqual(snapshot["listeners"]["udp_relay"]["port"], self.server.relay_port)
        self.assertIn("bytes_per_second", snapshot["throughput"])
        self.assertTrue(any(event["type"] == "network_warning" for event in snapshot["events"]))

        _, exported = self.request("/api/logs/export")
        self.assertEqual(json.loads(exported)[-1]["type"], "server_started")

        self.request("/api/logs/clear", method="POST", body={})
        _, body = self.request("/api/dashboard")
        self.assertEqual(json.loads(body)["events"], [])

    def test_dashboard_can_stop_and_restart_server(self):
        self.request("/api/server/stop", method="POST", body={})
        _, body = self.request("/api/dashboard")
        self.assertEqual(json.loads(body)["status"], "offline")

        self.request("/api/server/start", method="POST", body={})
        _, body = self.request("/api/dashboard")
        self.assertEqual(json.loads(body)["status"], "online")

    def test_dashboard_can_kick_a_client(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(("127.0.0.1", 0))
        connection = socket.create_connection(
            ("127.0.0.1", self.server.tcp_port),
            timeout=3,
        )
        self.client_connections.append(connection)
        connection.sendall(json.dumps({
            "type": "login",
            "username": "dashboard-test",
            "udp_port": udp_socket.getsockname()[1],
        }).encode("utf-8") + b"\n")
        reader = connection.makefile("rb")
        response = json.loads(reader.readline().decode("utf-8"))
        self.assertTrue(response["success"])

        self.request(
            "/api/clients/kick",
            method="POST",
            body={"session_id": response["session_id"]},
        )
        self.assertEqual(self.server.get_users(), [])
        self.assertEqual(self.server.get_dashboard_snapshot()["events"][0]["type"], "client_kicked")
        reader.close()
        udp_socket.close()


if __name__ == "__main__":
    unittest.main()

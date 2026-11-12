"""Shared helpers for the end-to-end network tests (real server + real engines)."""

import hashlib
import os
import random
import socket
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.state import ClientState
from network.central_server import CentralServer
from network.socket_engine import SocketEngine
from network.streaming import StreamManager


def free_udp_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_server() -> CentralServer:
    """Central server on localhost with OS-chosen free ports."""
    server = CentralServer(host="127.0.0.1", tcp_port=0, relay_port=0)
    server.start()
    return server


class Client:
    """One full client: ClientState + SocketEngine, logged in to `server`."""

    def __init__(self, server, username, download_dir, loss=0.0, seed=None,
                 port=None):
        """`server` is a CentralServer, or a (host, tcp_port, relay_port) tuple."""
        host, tcp_port, relay_port = (
            (server.host, server.tcp_port, server.relay_port)
            if hasattr(server, "tcp_port") else server)
        self.state = ClientState()
        self.port = port or free_udp_port()
        self.engine = SocketEngine(
            state=self.state,
            host="127.0.0.1" if host in ("0.0.0.0", "") else host,
            port=self.port,
            server_host=host,
            server_tcp_port=tcp_port,
            server_udp_port=relay_port,
        )
        self.engine.stream_manager = StreamManager(download_dir=download_dir)
        self.download_dir = download_dir
        self.dropped = 0
        self.sent = 0
        if loss > 0:
            self._install_loss(loss, random.Random(seed))
        self.engine.start()
        self.session_id = self.engine.login(username)
        self.username = username

    def _install_loss(self, loss, rng):
        real_send = self.engine.send_raw

        def lossy_send(raw, addr):
            self.sent += 1
            if rng.random() < loss:
                self.dropped += 1  # packet "lost in the network"
                return
            real_send(raw, addr)

        self.engine.send_raw = lossy_send

    def peer(self, username):
        with self.state._lock:
            return next((p for p in self.state.peers.values()
                         if p["username"] == username), None)

    def stop(self):
        self.engine.stop()


def wait_until(predicate, timeout=10.0, interval=0.02):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def sha256_of(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()

"""Load test for the collaboration app (Step 4 / Step 10: 10-20 clients).

Starts N real clients (ClientState + SocketEngine, TCP login + UDP relay),
lets every client broadcast one chat message and checks that every client
receives the messages of all the others. Optionally sends a file too.

Usage (from the project root):
    python tools/load_test.py --clients 20
    python tools/load_test.py --clients 20 --file-mb 1

By default it starts its own server on localhost. To load-test a server you
started yourself (and watch it on the dashboard), pass its address:
    python main.py --server --host 0.0.0.0 --tcp-port 9001 --relay-port 9002
    python tools/load_test.py --clients 20 --server-host 127.0.0.1
"""

import argparse
import os
import shutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from test.net_helpers import (Client, free_udp_port, sha256_of, start_server,  # noqa: E402
                              wait_until)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--clients", type=int, default=20)
    ap.add_argument("--file-mb", type=float, default=0.0,
                    help="also send a file of this size from user01 to user02")
    ap.add_argument("--server-host", default=None,
                    help="use an already running server instead of starting one")
    ap.add_argument("--tcp-port", type=int, default=9001)
    ap.add_argument("--relay-port", type=int, default=9002)
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()
    n = args.clients

    own_server = None
    if args.server_host:
        server = (args.server_host, args.tcp_port, args.relay_port)
    else:
        own_server = start_server()
        server = own_server
    tmp = tempfile.mkdtemp(prefix="load_")
    clients, ok = [], True

    def check(label, passed, detail=""):
        nonlocal ok
        ok = ok and passed
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""))

    try:
        print(f"Load test: {n} clients -> "
              f"{'own localhost server' if own_server else args.server_host}")

        # 1. all clients log in at the same time
        ports = [free_udp_port() for _ in range(n)]
        t0 = time.monotonic()

        def connect(i):
            name = f"user{i + 1:02d}"
            return Client(server, name, os.path.join(tmp, name), port=ports[i])

        with ThreadPoolExecutor(max_workers=n) as pool:
            futures = [pool.submit(connect, i) for i in range(n)]
            for fut in futures:
                try:
                    clients.append(fut.result())
                except Exception as exc:  # noqa: BLE001
                    print(f"  login failed: {exc!r}")
        check("concurrent TCP logins", len(clients) == n,
              f"{len(clients)}/{n} in {time.monotonic() - t0:.2f}s")
        if len(clients) < 2:
            return 1

        # 2. everybody learns about everybody (roster pushed over TCP)
        synced = wait_until(
            lambda: all(len(c.state.peers) == len(clients) - 1 for c in clients),
            args.timeout)
        check("online roster synced on every client", synced,
              f"each client sees {len(clients) - 1} peers")

        # 3. every client broadcasts one message through the UDP relay
        t0 = time.monotonic()
        for c in clients:
            c.engine.send_message("", 0, 0, f"hello from {c.username}")
        expected = len(clients) - 1

        def got(c):
            with c.state._lock:
                return {m["text"] for m in c.state.streams.get(0, [])
                        if m["text"].startswith("hello from")}

        delivered = wait_until(lambda: all(len(got(c)) == expected for c in clients),
                               args.timeout)
        total = sum(len(got(c)) for c in clients)
        check("broadcast chat delivered to everyone", delivered,
              f"{total}/{len(clients) * expected} messages in {time.monotonic() - t0:.2f}s")

        # 4. optional: one file transfer while all the clients are online
        if args.file_mb > 0 and len(clients) >= 2:
            src = os.path.join(tmp, "payload.bin")
            with open(src, "wb") as f:
                f.write(os.urandom(int(args.file_mb * 1_000_000)))
            a, b = clients[0], clients[1]
            peer = a.peer(b.username)
            t0 = time.monotonic()
            a.engine.send_file(peer["ip"], peer["port"], 4242, src, transfer_id="load")
            done = wait_until(
                lambda: b.state.file_transfers.get("load", {}).get("status") == "COMPLETED",
                args.timeout)
            same = False
            if done:
                for name in os.listdir(b.download_dir):
                    if name.startswith("payload"):
                        same = sha256_of(src) == sha256_of(os.path.join(b.download_dir, name))
            check(f"{args.file_mb:g} MB file user01 -> user02 intact (SHA-256)", done and same,
                  f"{time.monotonic() - t0:.2f}s")

        print("RESULT:", "PASS" if ok else "FAIL")
        return 0 if ok else 1
    finally:
        for c in clients:
            c.stop()
        if own_server:
            own_server.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

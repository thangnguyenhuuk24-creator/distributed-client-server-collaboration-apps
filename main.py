import argparse
import sys
import threading
import time
import os

from core.state import ClientState
from network.central_server import CentralServer
from network.socket_engine import SocketEngine
from web.central_dashboard import CentralDashboardServer
from web.server import LocalAPIServer


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Distributed & Streaming-based Client-Server Collaboration Application (HCMUT)"
    )
    parser.add_argument(
        "--udp-port",
        type=int,
        default=9000,
        help="Cổng UDP dùng cho heartbeat (mặc định: 9000)",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=8080,
        help="Cổng HTTP cho giao diện Web GUI (mặc định: 8080)",
    )
    parser.add_argument(
        "--discovery-port",
        type=int,
        default=9004,
        help="Cổng UDP discovery dùng chung cho các client (mặc định: 9004, tách khỏi relay 9002)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Địa chỉ IP lắng nghe socket (mặc định: 0.0.0.0)",
    )
    parser.add_argument(
        "--server",
        action="store_true",
        help="Chạy server trung tâm tự host (TCP login/presence và UDP relay chat)",
    )
    parser.add_argument(
        "--tcp-port",
        type=int,
        default=9001,
        help="Cổng TCP bắt tay/đăng nhập server trung tâm (mặc định: 9001)",
    )
    parser.add_argument(
        "--relay-port",
        type=int,
        default=9002,
        help="Cổng UDP relay trên server trung tâm (mặc định: 9002)",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=9003,
        help="Cổng dashboard quản trị local-only (mặc định: 9003)",
    )
    parser.add_argument(
        "--server-name",
        type=str,
        default="",
        help="Server display name and account namespace",
    )
    parser.add_argument(
        "--server-host",
        type=str,
        help="Địa chỉ IP/hostname server LAN; bật chế độ client trung tâm khi được cung cấp",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.server:
        server_name = args.server_name.strip()
        database_dir = os.path.abspath(os.path.join("storage", "servers"))
        os.makedirs(database_dir, exist_ok=True)
        if not server_name:
            server_name = CentralServer.load_saved_name(
                args.tcp_port,
                args.relay_port,
                database_dir,
            )
        auth_db_path = (
            CentralServer.auth_database_path(
                server_name,
                args.tcp_port,
                args.relay_port,
                database_dir,
            )
            if server_name and not args.server_name.strip()
            else None
        )
        central_server = CentralServer(
            host=args.host,
            tcp_port=args.tcp_port,
            relay_port=args.relay_port,
            server_name=server_name,
            auth_db_path=auth_db_path,
            require_auth=True,
        )
        if args.server_name.strip():
            central_server.configure_identity(server_name, database_dir)
        dashboard = CentralDashboardServer(
            central_server,
            host="127.0.0.1",
            port=args.dashboard_port,
            config_directory=database_dir,
        )
        try:
            central_server.start()
            dashboard.start()
            print("[+] Central LAN server đã khởi chạy")
            print(f"    TCP login/presence: {args.host}:{central_server.tcp_port}")
            print(f"    UDP chat relay    : {args.host}:{central_server.relay_port}")
            print(f"    Dashboard local  : http://127.0.0.1:{dashboard.port}")
            if not central_server.get_dashboard_snapshot()["server_configured"]:
                print("    Hãy mở dashboard để nhập tên server sau khi khởi tạo.")
            print("[!] Nhấn Ctrl+C để dừng server.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[*] Đang dừng central server...")
        finally:
            dashboard.stop()
            central_server.stop()
        return

    # Tạo các thư mục lưu trữ file cần thiết
    os.makedirs("storage/uploads", exist_ok=True)
    os.makedirs("storage/downloads", exist_ok=True)

    print("=" * 65)
    print("   DISTRIBUTED STREAMING COLLABORATION APP - HCMUT")
    print("=" * 65)
    print(f"[*] Cấu hình mạng:")
    print(f"    - UDP Socket Port : {args.udp_port}")
    print(f"    - UDP Discovery   : {args.discovery_port}")
    if args.server_host:
        print(f"    - Central server  : {args.server_host}:{args.tcp_port} (TCP)")
        print(f"    - UDP chat relay  : {args.server_host}:{args.relay_port}")
    print(f"    - Web Interface   : http://localhost:{args.web_port}")
    print(f"    - Thư mục nhận file: storage/downloads/")
    print("=" * 65)

    # 1. Khởi tạo In-Memory Thread-safe ClientState
    shared_state = ClientState()

    # 2. Khởi tạo Socket Engine (Tầng mạng UDP của Member 1)
    socket_engine = SocketEngine(
        state=shared_state,
        host=args.host,
        port=args.udp_port,
        discovery_port=args.discovery_port,
        server_host=args.server_host,
        server_tcp_port=args.tcp_port,
        server_udp_port=args.relay_port,
        server_name=args.server_name,
    )

    # 3. Khởi tạo Web Server (Tầng giao diện HTTP của Member 2)
    web_server = LocalAPIServer(
        state=shared_state, socket_engine=socket_engine, port=args.web_port
    )

    try:
        # Khởi động Socket Engine.
        socket_engine.start()
        print(f"[+] UDP Socket Engine đã khởi chạy trên port {args.udp_port}")

        # Chạy Web API Server trên luồng HTTP riêng
        web_thread = threading.Thread(target=web_server.start, daemon=True)
        web_thread.start()
        print(f"[+] Web Server sẵn sàng tại: http://localhost:{args.web_port}")
        print("\n[!] Mở trình duyệt web và đăng nhập để bắt đầu cộng tác.")
        print("[!] Nhấn Ctrl + C để dừng ứng dụng an toàn.\n")

        # Vòng lặp chính giữ tiến trình chạy ổn định
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[*] Đang tắt ứng dụng...")
    finally:
        socket_engine.stop()
        web_server.stop()
        print("[*] Đã đóng toàn bộ kết nối socket và dừng server an toàn.")
        sys.exit(0)


if __name__ == "__main__":
    main()
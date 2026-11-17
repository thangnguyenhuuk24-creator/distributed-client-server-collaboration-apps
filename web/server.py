import http.server
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
import secrets
import secrets
import sqlite3
import sys
import time
import urllib.parse
import uuid
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.state import ClientState, private_stream_id

MAX_JSON_BODY = 1 * 1024 * 1024        # 1 MB cho tin nhắn, login...
MAX_UPLOAD_BODY = 200 * 1024 * 1024    # 200 MB cho file upload


class LocalAPIServer:
    """HTTP Server nội bộ điều phối giao tiếp giữa Web UI và ClientState trong RAM."""

    def __init__(
        self,
        state: ClientState,
        socket_engine=None,
        port: int = 8080,
        history_db_path: Optional[str] = None,
    ):
        self.state = state
        self.socket_engine = socket_engine
        self.port = port
        self.history_db_path = history_db_path or os.path.abspath(
            os.path.join("storage", "user_history.sqlite3")
        )
        self.httpd: Optional[http.server.ThreadingHTTPServer] = None

    def start(self):
        # Lưu tham chiếu để RequestHandler truy cập
        state_ref = self.state
        engine_ref = self.socket_engine
        history_path_ref = self.history_db_path
        base_dir = os.path.dirname(os.path.abspath(__file__))

        class RequestHandler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=base_dir, **kwargs)

            def log_message(self, format, *args):
                # Ẩn log truy cập HTTP định kỳ để tránh làm ngập màn hình Terminal
                pass

            def end_headers(self):
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                super().end_headers()

            def do_GET(self):
                self._guarded(self._do_GET)
            
            def do_POST(self):
                self._guarded(self._do_POST)
            
            def _guarded(self, handler):
                try:
                    handler()
                except (ValueError, TypeError, AttributeError, UnicodeDecodeError):
                    self._safe_error(400, "Yêu cầu không hợp lệ")
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except Exception as exc:
                    print(f"[!] HTTP handler error: {exc!r}")
                    self._safe_error(500, "Lỗi nội bộ server")
            
            def _safe_error(self, status, message):
                try:
                    self._send_json(status, {"error": message})
                except OSError:
                    pass
            
            def _do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                query_params = urllib.parse.parse_qs(parsed.query)

                if parsed.path == "/api/config":
                    engine = engine_ref
                    self._send_json(200, {
                        "server_host": engine.server_host if engine else "",
                        "tcp_port": engine.server_tcp_port if engine else 9001,
                        "relay_port": engine.server_udp_port if engine else 9002,
                        "server_name": engine.server_name if engine else "",
                    })
                    return

                if parsed.path == "/api/servers":
                    engine = engine_ref
                    servers_by_id = {}
                    selected_id = None
                    if os.path.isfile(history_path_ref):
                        with sqlite3.connect(history_path_ref) as database:
                            tables = {
                                row[0]
                                for row in database.execute(
                                    "SELECT name FROM sqlite_master "
                                    "WHERE type = 'table' AND name = 'known_servers'"
                                )
                            }
                            if "known_servers" in tables:
                                rows = database.execute(
                                    "SELECT server_id, server_name, server_host, "
                                    "tcp_port, relay_port FROM known_servers "
                                    "ORDER BY last_used_at DESC"
                                ).fetchall()
                                for server_id, name, host, tcp_port, relay_port in rows:
                                    servers_by_id[server_id] = {
                                        "id": server_id,
                                        "name": name,
                                        "label": name or f"Server ({host})",
                                        "host": host,
                                        "tcp_port": tcp_port,
                                        "relay_port": relay_port,
                                    }
                                if rows:
                                    selected_id = rows[0][0]
                    servers_directory = os.path.join(
                        os.path.dirname(os.path.abspath(history_path_ref)),
                        "servers",
                    )
                    if os.path.isdir(servers_directory):
                        for filename in os.listdir(servers_directory):
                            if not filename.startswith("server_") or not filename.endswith(".json"):
                                continue
                            parts = filename[:-5].split("_")
                            if len(parts) != 3:
                                continue
                            try:
                                tcp_port, relay_port = int(parts[1]), int(parts[2])
                            except ValueError:
                                continue
                            if not (
                                1 <= tcp_port <= 65535
                                and 1 <= relay_port <= 65535
                            ):
                                continue
                            config_path = os.path.join(servers_directory, filename)
                            try:
                                with open(config_path, "r", encoding="utf-8") as config_file:
                                    config = json.load(config_file)
                            except (OSError, json.JSONDecodeError) as exc:
                                print(f"[!] Could not read server configuration {config_path}: {exc}")
                                continue
                            if (
                                not isinstance(config, dict)
                                or not isinstance(config.get("server_name"), str)
                                or not config["server_name"].strip()
                            ):
                                print(f"[!] Invalid server configuration: {config_path}")
                                continue
                            server_name = config["server_name"].strip()
                            host = config.get("host", "")
                            if isinstance(host, str):
                                host = host.strip()
                            if (
                                not isinstance(host, str)
                                or not host
                                or host == "0.0.0.0"
                            ):
                                current_host = (
                                    engine.server_host
                                    if engine
                                    and engine.server_tcp_port == tcp_port
                                    and engine.server_udp_port == relay_port
                                    else ""
                                )
                                saved = next(
                                    (
                                        server for server in servers_by_id.values()
                                        if server["tcp_port"] == tcp_port
                                        and server["relay_port"] == relay_port
                                    ),
                                    None,
                                )
                                host = (
                                    current_host
                                    or (saved["host"] if saved else "")
                                    or (engine.server_host if engine else "")
                                    or "127.0.0.1"
                                )
                            server_id = f"{host.casefold()}|{tcp_port}|{relay_port}"
                            servers_by_id[server_id] = {
                                "id": server_id,
                                "name": server_name,
                                "label": server_name,
                                "host": host,
                                "tcp_port": tcp_port,
                                "relay_port": relay_port,
                            }
                            if selected_id is None:
                                selected_id = server_id
                    if engine and engine.server_host:
                        server_id = (
                            f"{engine.server_host.casefold()}|"
                            f"{engine.server_tcp_port}|{engine.server_udp_port}"
                        )
                        saved = servers_by_id.get(server_id, {})
                        server_name = saved.get("name") or engine.server_name
                        servers_by_id[server_id] = {
                            "id": server_id,
                            "name": server_name,
                            "label": server_name or f"Server ({engine.server_host})",
                            "host": engine.server_host,
                            "tcp_port": engine.server_tcp_port,
                            "relay_port": engine.server_udp_port,
                        }
                        current_server = servers_by_id.pop(server_id)
                        servers = [current_server, *servers_by_id.values()]
                        selected_id = server_id
                    else:
                        servers = list(servers_by_id.values())
                    self._send_json(200, {
                        "servers": servers,
                        "selected_id": selected_id,
                    })
                    return

                if parsed.path.startswith("/api/downloads/"):
                    if not state_ref.session_id:
                        self._send_json(401, {"error": "Vui lòng đăng nhập"})
                        return
                    self._expire_file_downloads()
                    requested_name = urllib.parse.unquote(parsed.path[len("/api/downloads/"):])
                    filename = os.path.basename(requested_name.replace("\\", "/"))
                    with state_ref._lock:
                        available = any(
                            message.get("download_filename") == filename
                            and message.get("download_expires_at", 0) > time.time()
                            for messages in state_ref.streams.values()
                            for message in messages
                        )
                    download_dir = (
                        engine_ref.stream_manager.download_dir
                        if engine_ref
                        else os.path.join(os.getcwd(), "storage", "downloads")
                    )
                    filepath = os.path.join(download_dir, filename)
                    if not filename or not available or not os.path.isfile(filepath):
                        self._send_json(404, {"error": "Không tìm thấy file đã nhận"})
                        return
                    with open(filepath, "rb") as file:
                        content = file.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header(
                        "Content-Disposition",
                        f"attachment; filename*=UTF-8''{urllib.parse.quote(filename)}",
                    )
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)
                    return

                if parsed.path.startswith("/api/offline-files/"):
                    if not state_ref.session_id or not engine_ref:
                        self._send_json(401, {"error": "Vui lòng đăng nhập"})
                        return
                    if not engine_ref.using_central_server:
                        self._send_json(503, {"error": "Server trung tâm chưa kết nối"})
                        return
                    mail_id = parsed.path[len("/api/offline-files/"):]
                    if not mail_id or len(mail_id) != 32:
                        self._send_json(400, {"error": "ID file không hợp lệ"})
                        return
                    with state_ref._lock:
                        message = next(
                            (
                                item
                                for stream_messages in state_ref.streams.values()
                                for item in stream_messages
                                if item.get("file_id") == mail_id
                            ),
                            None,
                        )
                    if not message or message.get("expired"):
                        self._send_json(410, {"error": "File đã hết hạn"})
                        return
                    try:
                        content = engine_ref.download_offline_file(
                            mail_id,
                            message.get("filesize", 0),
                        )
                        if (
                            len(content) != message.get("filesize")
                            or hashlib.sha256(content).hexdigest()
                            != message.get("sha256")
                        ):
                            raise ValueError("SHA-256 mismatch for offline file")
                        download_dir = engine_ref.stream_manager.download_dir
                        os.makedirs(download_dir, exist_ok=True)
                        filename = os.path.basename(
                            message["filename"].replace("\\", "/")
                        )
                        base, extension = os.path.splitext(filename)
                        path = os.path.join(download_dir, filename)
                        counter = 1
                        while os.path.exists(path):
                            path = os.path.join(
                                download_dir,
                                f"{base}_{counter}{extension}",
                            )
                            counter += 1
                        with open(path, "wb") as downloaded_file:
                            downloaded_file.write(content)
                        state_ref.update_file_message(
                            mail_id,
                            "Đã nhận",
                            os.path.basename(path),
                            message.get("file_expires_at"),
                        )
                    except FileNotFoundError as exc:
                        state_ref.update_file_message(mail_id, "Hết hạn")
                        with state_ref._lock:
                            for stream_id, messages in state_ref.streams.items():
                                for item in messages:
                                    if item.get("file_id") == mail_id:
                                        item["expired"] = True
                                        state_ref._persist_message(stream_id, item)
                        self._send_json(410, {"error": "File đã hết hạn"})
                        return
                    except (OSError, ConnectionError, RuntimeError, TimeoutError, ValueError) as exc:
                        self._send_json(502, {"error": f"Không tải được file: {exc}"})
                        return
                    self._send_json(200, {"success": True})
                    return

                # Route 1: Lấy snapshot trạng thái hệ thống
                if parsed.path == "/api/status":
                    self._send_json(200, {
                        "username": state_ref.username,
                        "session_id": state_ref.session_id,
                        "status": state_ref.status,
                        "last_heartbeat": state_ref.last_heartbeat,
                    })

                # Route 2: Lấy danh sách peers đang online
                elif parsed.path == "/api/peers":
                    if not state_ref.session_id:
                        self._send_json(401, {"error": "Vui lòng đăng nhập"})
                        return
                    with state_ref._lock:
                        online_peers = list(state_ref.peers.values())
                    if engine_ref and engine_ref.using_central_server:
                        directory = engine_ref.request_user_directory()
                        online_by_name = {
                            peer["username"].casefold(): peer
                            for peer in online_peers
                        }
                        peers_data = []
                        for account in directory:
                            peer = online_by_name.get(account["username"].casefold())
                            ip = peer["ip"] if peer else account["ip"]
                            port = peer["port"] if peer else account["port"]
                            peers_data.append({
                                "username": account["username"],
                                "session_id": account["session_id"],
                                "online": account["online"],
                                "ip": ip,
                                "port": port,
                                "private_stream_id": private_stream_id(
                                    state_ref.session_id,
                                    account["session_id"],
                                ),
                            })
                    else:
                        peers_data = [
                            {
                                **peer,
                                "online": True,
                                "private_stream_id": private_stream_id(
                                    state_ref.session_id,
                                    peer["session_id"],
                                ),
                            }
                            for peer in online_peers
                        ]
                    self._send_json(200, peers_data)

                # Route 3: Lấy tin nhắn theo Stream ID
                elif parsed.path == "/api/messages":
                    if not state_ref.session_id:
                        self._send_json(401, {"error": "Vui lòng đăng nhập"})
                        return
                    self._expire_file_downloads()
                    stream_id = int(query_params.get("stream_id", [0])[0])
                    peer_username = query_params.get("peer_username", [""])[0].casefold()
                    with state_ref._lock:
                        if peer_username:
                            selected_messages = sorted(
                                (
                                    message
                                    for stream_messages in state_ref.streams.values()
                                    for message in stream_messages
                                    if message.get("conversation_key") == peer_username
                                ),
                                key=lambda message: (
                                    message["timestamp"],
                                    int(message.get("_event_order", 0)),
                                    str(message.get("_event_id", "")),
                                ),
                            )
                        else:
                            selected_messages = sorted(
                                state_ref.streams.get(stream_id, []),
                                key=lambda message: (
                                    message["timestamp"],
                                    int(message.get("_event_order", 0)),
                                    str(message.get("_event_id", "")),
                                ),
                            )
                        messages = [
                            {
                                key: value
                                for key, value in message.items()
                                if key != "message_id" and not key.startswith("_")
                            }
                            for message in selected_messages
                        ]
                    self._send_json(200, {"stream_id": stream_id, "messages": messages})

                # Route 4: Lấy danh sách tiến trình truyền/nhận file
                elif parsed.path == "/api/files":
                    if not state_ref.session_id:
                        self._send_json(401, {"error": "Vui lòng đăng nhập"})
                        return
                    with state_ref._lock:
                        transfers = list(state_ref.file_transfers.values())
                    self._send_json(200, transfers)

                # Route 5: Snapshot tổng hợp
                elif parsed.path == "/api/state":
                    if not state_ref.session_id:
                        self._send_json(401, {"error": "Vui lòng đăng nhập"})
                        return
                    self._send_json(200, state_ref.to_json_snapshot())

                # Mặc định: Phục vụ HTML, CSS, JS tĩnh
                else:
                    if parsed.path in ["/", "/index.html"]:
                        self.path = "/templates/index.html"
                    elif parsed.path.startswith("/css/"):
                        self.path = f"/static{parsed.path}"
                    elif parsed.path.startswith("/js/"):
                        self.path = f"/static{parsed.path}"
                    super().do_GET()

            def _do_POST(self):
                parsed = urllib.parse.urlparse(self.path)
                is_upload = parsed.path == "/api/send_file" and self.headers.get(
                    "Content-Type", ""
                ).startswith("application/octet-stream")
                content_len = int(self.headers.get("Content-Length", 0))
                limit = MAX_UPLOAD_BODY if is_upload else MAX_JSON_BODY
                if not 0 <= content_len <= limit:
                    self._send_json(413, {"error": "Dữ liệu quá lớn"})
                    return
                post_body = self.rfile.read(content_len)

                if parsed.path == "/api/send_file" and self.headers.get(
                    "Content-Type", ""
                ).startswith("application/octet-stream"):
                    if not state_ref.session_id:
                        self._send_json(401, {"error": "Vui lòng đăng nhập"})
                        return
                    query_params = urllib.parse.parse_qs(parsed.query)
                    recipient_ip = query_params.get("recipient_ip", [""])[0]
                    recipient_username = query_params.get("recipient_username", [""])[0]
                    try:
                        recipient_port = int(query_params.get("recipient_port", ["0"])[0])
                        chat_stream_id = int(query_params.get("stream_id", ["0"])[0])
                    except ValueError:
                        self._send_json(400, {"error": "Thông tin người nhận hoặc kênh không hợp lệ"})
                        return

                    raw_filename = urllib.parse.unquote(self.headers.get("X-File-Name", ""))
                    filename = os.path.basename(raw_filename.replace("\\", "/"))
                    if not filename or filename in {".", ".."}:
                        self._send_json(400, {"error": "Tên file không hợp lệ"})
                        return
                    if chat_stream_id != 0 and not recipient_username and (
                        not recipient_ip or not 1 <= recipient_port <= 65535
                    ):
                        self._send_json(400, {"error": "Vui lòng chọn người nhận file hợp lệ"})
                        return
                    if not 0 <= chat_stream_id <= 0xFFFFFFFF:
                        self._send_json(400, {"error": "Kênh chat không hợp lệ"})
                        return
                    if not engine_ref:
                        self._send_json(503, {"error": "Socket Engine chưa sẵn sàng"})
                        return
                    with state_ref._lock:
                        online_peers = list(state_ref.peers.values())
                    offline_recipients = []
                    conversation_key = None
                    if engine_ref.using_central_server:
                        directory = engine_ref.request_user_directory()
                        if chat_stream_id == 0:
                            offline_recipients = [
                                {"username": user["username"], "stream_id": 0}
                                for user in directory
                                if not user["online"]
                            ]
                        else:
                            account = next(
                                (
                                    user for user in directory
                                    if user["username"].casefold()
                                    == recipient_username.casefold()
                                ),
                                None,
                            )
                            peer = next(
                                (
                                    item for item in online_peers
                                    if (
                                        item["username"].casefold()
                                        == recipient_username.casefold()
                                        if recipient_username
                                        else item["ip"] == recipient_ip
                                        and item["port"] == recipient_port
                                    )
                                ),
                                None,
                            )
                            if peer:
                                online_peers = [peer]
                                conversation_key = peer["username"]
                            elif account:
                                offline_recipients = [{
                                    "username": account["username"],
                                    "stream_id": chat_stream_id,
                                }]
                                conversation_key = account["username"]
                            else:
                                self._send_json(404, {
                                    "error": "Không tìm thấy tài khoản người nhận trên server"
                                })
                                return
                    elif chat_stream_id != 0:
                        peer = next(
                            (
                                item for item in online_peers
                                if item["ip"] == recipient_ip
                                and item["port"] == recipient_port
                            ),
                            None,
                        )
                        if peer:
                            online_peers = [peer]
                            conversation_key = peer["username"]
                        else:
                            self._send_json(404, {"error": "Người nhận không còn online"})
                            return
                    if not online_peers and not offline_recipients:
                        self._send_json(400, {"error": "Không có người nhận để gửi file"})
                        return

                    transfer_id = uuid.uuid4().hex
                    upload_dir = os.path.abspath(
                        os.path.join(os.path.dirname(__file__), "..", "storage", "uploads")
                    )
                    os.makedirs(upload_dir, exist_ok=True)
                    filepath = os.path.join(upload_dir, f"{transfer_id}_{filename}")
                    filesize = len(post_body)
                    file_sha256 = hashlib.sha256(post_body).hexdigest()
                    with open(filepath, "wb") as uploaded_file:
                        uploaded_file.write(post_body)
                    del post_body

                    event_id, event_timestamp, event_order = state_ref.next_event_metadata()
                    server_synced = bool(engine_ref.using_central_server)
                    state_ref.add_file_message(
                        chat_stream_id,
                        f"{state_ref.username} (You)",
                        filename,
                        transfer_id,
                        "Đang gửi",
                        filesize,
                        conversation_key,
                        event_id=event_id,
                        timestamp=event_timestamp,
                        event_order=event_order,
                        server_synced=server_synced,
                        sha256=file_sha256,
                    )
                    state_ref.update_file_progress(
                        file_id=transfer_id,
                        filename=filename,
                        total_size=filesize,
                        received_chunks=0,
                        total_chunks=(filesize + 1349) // 1350,
                        status="SENDING",
                    )

                    def transfer_file():
                        try:
                            def send_to_recipient(recipient):
                                file_stream_id = secrets.randbelow(0xFFFFFFFE) + 2
                                engine_ref.send_file(
                                    recipient["ip"],
                                    recipient["port"],
                                    file_stream_id,
                                    filepath,
                                    chat_stream_id=chat_stream_id,
                                    transfer_id=transfer_id,
                                    filename=filename,
                                    target_session_id=(
                                        recipient["session_id"]
                                        if engine_ref.using_central_server
                                        else None
                                    ),
                                    event_id=event_id,
                                    event_timestamp=event_timestamp,
                                    event_order=event_order,
                                )
                            transfer_errors = []
                            if offline_recipients:
                                engine_ref.queue_offline_file(
                                    filepath,
                                    filename,
                                    offline_recipients,
                                    file_sha256,
                                    event_id,
                                    event_timestamp,
                                    event_order,
                                )
                            if online_peers:
                                with ThreadPoolExecutor(
                                    max_workers=min(len(online_peers), 32)
                                ) as executor:
                                    futures = [
                                        executor.submit(send_to_recipient, recipient)
                                        for recipient in online_peers
                                    ]
                                    for future in as_completed(futures):
                                        try:
                                            future.result()
                                        except Exception as exc:
                                            transfer_errors.append(exc)
                            if transfer_errors:
                                raise RuntimeError(
                                    "File transfer failed for one or more recipients: "
                                    + "; ".join(str(error) for error in transfer_errors)
                                )
                            state_ref.update_file_progress(
                                file_id=transfer_id,
                                filename=filename,
                                total_size=filesize,
                                received_chunks=(filesize + 1349) // 1350,
                                total_chunks=(filesize + 1349) // 1350,
                                status="SENT",
                            )
                            if offline_recipients and not online_peers:
                                state_ref.update_file_message(
                                    transfer_id,
                                    "Đã gửi (chờ nhận)",
                                )
                        except Exception as exc:
                            state_ref.update_file_message(transfer_id, "Gửi thất bại")
                            state_ref.update_file_progress(
                                file_id=transfer_id,
                                filename=filename,
                                total_size=filesize,
                                received_chunks=0,
                                total_chunks=(filesize + 1349) // 1350,
                                status="ERROR",
                            )
                            print(f"[!] File transfer failed for {filename}: {exc}")
                        finally:
                            try:
                                os.remove(filepath)
                            except OSError as exc:
                                print(f"[!] Could not remove temporary upload {filepath}: {exc}")

                    import threading
                    threading.Thread(target=transfer_file, daemon=True).start()
                    self._send_json(200, {"success": True, "transfer_id": transfer_id})
                    return

                data = json.loads(post_body.decode("utf-8")) if post_body else {}
                if not isinstance(data, dict):
                    raise ValueError("Body phải là JSON object")

                # Action 1: Đăng nhập / Bắt đầu kết nối
                if parsed.path == "/api/login":
                    if state_ref.session_id:
                        self._send_json(409, {"error": "Vui lòng đăng xuất trước khi đổi tài khoản"})
                        return
                    username = data.get("username", "")
                    if not isinstance(username, str):
                        self._send_json(400, {"error": "Username phải là chuỗi"})
                        return
                    username = username.strip()
                    if not username:
                        self._send_json(400, {"error": "Username không được để trống"})
                        return

                    password = data.get("password")
                    if not isinstance(password, str) or not 8 <= len(password) <= 256:
                        self._send_json(400, {"error": "Mật khẩu phải có từ 8 đến 256 ký tự"})
                        return

                    selected_host = data.get("server_host", "")
                    selected_name = data.get("server_name", "")
                    if not isinstance(selected_host, str) or not isinstance(selected_name, str):
                        self._send_json(400, {"error": "Tên và địa chỉ server phải là chuỗi"})
                        return
                    selected_host = selected_host.strip()
                    selected_name = selected_name.strip()
                    tcp_port_value = data.get("tcp_port", 0)
                    relay_port_value = data.get("relay_port", 0)
                    if isinstance(tcp_port_value, bool) or isinstance(relay_port_value, bool):
                        self._send_json(400, {"error": "Cổng server không hợp lệ"})
                        return
                    try:
                        selected_tcp_port = int(tcp_port_value)
                        selected_relay_port = int(relay_port_value)
                    except (TypeError, ValueError):
                        self._send_json(400, {"error": "Cổng server không hợp lệ"})
                        return

                    if selected_host:
                        if not (
                            1 <= selected_tcp_port <= 65535
                            and 1 <= selected_relay_port <= 65535
                        ):
                            self._send_json(400, {"error": "Cổng server phải nằm trong khoảng 1-65535"})
                            return
                        if not engine_ref:
                            self._send_json(503, {"error": "Socket Engine chưa sẵn sàng"})
                            return
                        try:
                            engine_ref.configure_central_server(
                                selected_host,
                                selected_tcp_port,
                                selected_relay_port,
                                selected_name,
                            )
                            sess_id = engine_ref.login(username, password)
                        except PermissionError as exc:
                            self._send_json(409, {"error": str(exc)})
                            return
                        except (OSError, ConnectionError, RuntimeError, ValueError) as exc:
                            self._send_json(502, {"error": f"Không thể đăng nhập vào server LAN: {exc}"})
                            return
                        selected_name = engine_ref.server_name
                    else:
                        if engine_ref and engine_ref.server_host:
                            engine_ref.configure_p2p()
                        # Giữ chế độ P2P cũ khi không cấu hình server trung tâm.
                        sess_id = secrets.randbelow(90000) + 10000
                        state_ref.set_online(username, sess_id)
                        if engine_ref and hasattr(engine_ref, "discovery"):
                            engine_ref.discovery.send_broadcast_announce()

                    history_server = (
                        f"{selected_name.casefold()}|{selected_host.casefold()}|"
                        f"{selected_tcp_port}|{selected_relay_port}"
                    )
                    state_ref.configure_history(
                        username,
                        history_server,
                        history_path_ref,
                    )
                    if selected_host:
                        server_id = (
                            f"{selected_host.casefold()}|"
                            f"{selected_tcp_port}|{selected_relay_port}"
                        )
                        with sqlite3.connect(history_path_ref) as database:
                            database.execute(
                                "CREATE TABLE IF NOT EXISTS known_servers ("
                                "server_id TEXT PRIMARY KEY, server_name TEXT NOT NULL, "
                                "server_host TEXT NOT NULL, tcp_port INTEGER NOT NULL, "
                                "relay_port INTEGER NOT NULL, last_used_at REAL NOT NULL)"
                            )
                            database.execute(
                                "INSERT OR REPLACE INTO known_servers "
                                "(server_id, server_name, server_host, tcp_port, "
                                "relay_port, last_used_at) VALUES (?, ?, ?, ?, ?, ?)",
                                (
                                    server_id,
                                    selected_name,
                                    selected_host,
                                    selected_tcp_port,
                                    selected_relay_port,
                                    time.time(),
                                ),
                            )
                    mailbox_sync_error = None
                    if selected_host and engine_ref:
                        try:
                            engine_ref.sync_offline_mailbox()
                        except (OSError, ConnectionError, TimeoutError, ValueError) as exc:
                            mailbox_sync_error = str(exc)
                            print(f"[!] Could not sync offline mailbox: {exc}")
                    self._send_json(200, {
                        "success": True,
                        "session_id": sess_id,
                        "account_created": bool(
                            engine_ref
                            and engine_ref.coordinator
                            and engine_ref.coordinator.account_created
                        ),
                        "server_name": selected_name,
                        "mailbox_sync_error": mailbox_sync_error,
                    })

                elif parsed.path == "/api/logout":
                    if engine_ref and hasattr(engine_ref, "logout"):
                        engine_ref.logout()
                    else:
                        state_ref.clear_session_data()
                    self._send_json(200, {"success": True})

                # Action 2: Gửi tin nhắn qua UDP
                elif parsed.path == "/api/send_message":
                    if not state_ref.session_id:
                        self._send_json(401, {"error": "Vui lòng đăng nhập"})
                        return
                    stream_id = int(data.get("stream_id", 0))
                    text = data.get("content", "").strip()
                    dest_ip = data.get("recipient_ip", "")
                    dest_port = int(data.get("recipient_port", 0))
                    recipient_username = data.get("recipient_username", "")

                    if not text:
                        self._send_json(400, {"error": "Nội dung tin nhắn trống"})
                        return
                    if stream_id != 0 and not recipient_username and (
                        not dest_ip or not dest_port
                    ):
                        self._send_json(400, {"error": "Hãy chọn người nhận cho kênh riêng"})
                        return

                    send_errors = []
                    conversation_key = None
                    event_id, event_timestamp, event_order = state_ref.next_event_metadata()
                    server_synced = bool(engine_ref and engine_ref.using_central_server)
                    if engine_ref:
                        if engine_ref.using_central_server:
                            try:
                                directory = engine_ref.request_user_directory()
                                offline_recipients = [
                                    {
                                        "username": user["username"],
                                        "stream_id": stream_id,
                                    }
                                    for user in directory
                                    if not user["online"]
                                ] if stream_id == 0 else []
                                target_session_id = 0
                                if stream_id != 0:
                                    with state_ref._lock:
                                        recipient = next(
                                            (
                                                peer for peer in state_ref.peers.values()
                                                if (
                                                    peer["username"].casefold()
                                                    == recipient_username.casefold()
                                                    if recipient_username
                                                    else peer["ip"] == dest_ip
                                                    and peer["port"] == dest_port
                                                )
                                            ),
                                            None,
                                        )
                                    if recipient:
                                        target_session_id = recipient["session_id"]
                                        dest_ip = recipient["ip"]
                                        dest_port = recipient["port"]
                                        conversation_key = recipient["username"]
                                        engine_ref.send_message(
                                            dest_ip,
                                            dest_port,
                                            stream_id,
                                            text,
                                            target_session_id=target_session_id,
                                            event_id=event_id,
                                            event_timestamp=event_timestamp,
                                            event_order=event_order,
                                        )
                                    else:
                                        account = next(
                                            (
                                                user for user in directory
                                                if user["username"].casefold()
                                                == str(recipient_username).casefold()
                                            ),
                                            None,
                                        )
                                        if not account:
                                            self._send_json(404, {
                                                "error": "Không tìm thấy tài khoản người nhận trên server"
                                            })
                                            return
                                        conversation_key = account["username"]
                                        offline_recipients = [{
                                            "username": account["username"],
                                            "stream_id": stream_id,
                                        }]
                                if offline_recipients:
                                    engine_ref.queue_offline_message(
                                        offline_recipients,
                                        text,
                                        event_id,
                                        event_timestamp,
                                        event_order,
                                    )
                                if stream_id == 0:
                                    engine_ref.send_message(
                                        "",
                                        0,
                                        0,
                                        text,
                                        target_session_id=0,
                                        event_id=event_id,
                                        event_timestamp=event_timestamp,
                                        event_order=event_order,
                                    )
                            except (OSError, ConnectionError, RuntimeError, ValueError) as exc:
                                send_errors.append(str(exc))
                        elif dest_ip and dest_port:
                            with state_ref._lock:
                                conversation_key = next(
                                    (
                                        peer["username"]
                                        for peer in state_ref.peers.values()
                                        if peer["ip"] == dest_ip and peer["port"] == dest_port
                                    ),
                                    None,
                                )
                            try:
                                engine_ref.send_message(
                                    dest_ip,
                                    dest_port,
                                    stream_id,
                                    text,
                                    event_id=event_id,
                                    event_timestamp=event_timestamp,
                                    event_order=event_order,
                                )
                            except OSError as exc:
                                send_errors.append(str(exc))
                        else:
                            with state_ref._lock:
                                peers_list = list(state_ref.peers.values())
                            for p in peers_list:
                                try:
                                    engine_ref.send_message(
                                        p["ip"],
                                        p["port"],
                                        stream_id,
                                        text,
                                        event_id=event_id,
                                        event_timestamp=event_timestamp,
                                        event_order=event_order,
                                    )
                                except OSError as exc:
                                    send_errors.append(f"{p['username']}: {exc}")

                    # Ghi tin nhắn của chính mình vào Stream
                    state_ref.add_message(
                        stream_id,
                        f"{state_ref.username} (You)",
                        text,
                        conversation_key=conversation_key if stream_id != 0 else None,
                        event_id=event_id,
                        timestamp=event_timestamp,
                        event_order=event_order,
                        server_synced=server_synced,
                    )
                    if send_errors:
                        self._send_json(502, {
                            "error": "Không gửi được tới một hoặc nhiều peer",
                            "details": send_errors,
                        })
                        return
                    self._send_json(200, {"success": True})

                else:
                    self._send_json(404, {"error": "Endpoint không tồn tại"})

            def _send_json(self, status_code: int, data: any):
                response = json.dumps(data).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(response)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(response)

            def _expire_file_downloads(self):
                expired = state_ref.expire_file_messages()
                download_dir = (
                    engine_ref.stream_manager.download_dir
                    if engine_ref
                    else os.path.join(os.getcwd(), "storage", "downloads")
                )
                for expired_filename in expired:
                    filename = os.path.basename(expired_filename.replace("\\", "/"))
                    filepath = os.path.join(download_dir, filename)
                    if os.path.isfile(filepath):
                        try:
                            os.remove(filepath)
                        except OSError as exc:
                            print(f"[!] Could not remove expired download {filepath}: {exc}")

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), RequestHandler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self.httpd.serve_forever()

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
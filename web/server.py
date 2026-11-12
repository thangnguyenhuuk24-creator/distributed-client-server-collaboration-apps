import http.server
import json
import os
import secrets
import secrets
import sys
import urllib.parse
import uuid
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.state import ClientState, private_stream_id

MAX_JSON_BODY = 1 * 1024 * 1024        # 1 MB cho tin nhắn, login...
MAX_UPLOAD_BODY = 200 * 1024 * 1024    # 200 MB cho file upload


class LocalAPIServer:
    """HTTP Server nội bộ điều phối giao tiếp giữa Web UI và ClientState trong RAM."""

    def __init__(self, state: ClientState, socket_engine=None, port: int = 8080):
        self.state = state
        self.socket_engine = socket_engine
        self.port = port
        self.httpd: Optional[http.server.ThreadingHTTPServer] = None

    def start(self):
        # Lưu tham chiếu để RequestHandler truy cập
        state_ref = self.state
        engine_ref = self.socket_engine
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

                if parsed.path.startswith("/api/downloads/"):
                    requested_name = urllib.parse.unquote(parsed.path[len("/api/downloads/"):])
                    filename = os.path.basename(requested_name.replace("\\", "/"))
                    download_dir = (
                        engine_ref.stream_manager.download_dir
                        if engine_ref
                        else os.path.join(os.getcwd(), "storage", "downloads")
                    )
                    filepath = os.path.join(download_dir, filename)
                    if not filename or not os.path.isfile(filepath):
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
                    with state_ref._lock:
                        peers_data = [
                            {
                                **peer,
                                "private_stream_id": private_stream_id(
                                    state_ref.session_id,
                                    peer["session_id"],
                                ),
                            }
                            for peer in state_ref.peers.values()
                        ]
                    self._send_json(200, peers_data)

                # Route 3: Lấy tin nhắn theo Stream ID
                elif parsed.path == "/api/messages":
                    stream_id = int(query_params.get("stream_id", [0])[0])
                    with state_ref._lock:
                        messages = state_ref.streams.get(stream_id, [])
                    self._send_json(200, {"stream_id": stream_id, "messages": messages})

                # Route 4: Lấy danh sách tiến trình truyền/nhận file
                elif parsed.path == "/api/files":
                    with state_ref._lock:
                        transfers = list(state_ref.file_transfers.values())
                    self._send_json(200, transfers)

                # Route 5: Snapshot tổng hợp
                elif parsed.path == "/api/state":
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
                    query_params = urllib.parse.parse_qs(parsed.query)
                    recipient_ip = query_params.get("recipient_ip", [""])[0]
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
                    if chat_stream_id != 0 and (
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
                    if chat_stream_id == 0:
                        with state_ref._lock:
                            if not state_ref.peers:
                                self._send_json(400, {"error": "Không có người dùng online để nhận file"})
                                return

                    transfer_id = uuid.uuid4().hex
                    upload_dir = os.path.abspath(
                        os.path.join(os.path.dirname(__file__), "..", "storage", "uploads")
                    )
                    os.makedirs(upload_dir, exist_ok=True)
                    filepath = os.path.join(upload_dir, f"{transfer_id}_{filename}")
                    with open(filepath, "wb") as uploaded_file:
                        uploaded_file.write(post_body)

                    state_ref.add_file_message(
                        chat_stream_id,
                        f"{state_ref.username} (You)",
                        filename,
                        transfer_id,
                        "Đang gửi",
                    )
                    state_ref.update_file_progress(
                        file_id=transfer_id,
                        filename=filename,
                        total_size=len(post_body),
                        received_chunks=0,
                        total_chunks=(len(post_body) + 1349) // 1350,
                        status="SENDING",
                    )

                    def transfer_file():
                        try:
                            if chat_stream_id == 0:
                                with state_ref._lock:
                                    recipients = list(state_ref.peers.values())
                            else:
                                with state_ref._lock:
                                    recipient = next(
                                        (
                                            peer for peer in state_ref.peers.values()
                                            if peer["ip"] == recipient_ip
                                            and peer["port"] == recipient_port
                                        ),
                                        None,
                                    )
                                if recipient is None:
                                    raise RuntimeError("Người nhận không còn online")
                                recipients = [recipient]

                            if not recipients:
                                raise RuntimeError("Không có người nhận đang online")

                            for recipient in recipients:
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
                                        if getattr(engine_ref, "coordinator", None)
                                        else None
                                    ),
                                )
                            state_ref.update_file_progress(
                                file_id=transfer_id,
                                filename=filename,
                                total_size=len(post_body),
                                received_chunks=(len(post_body) + 1349) // 1350,
                                total_chunks=(len(post_body) + 1349) // 1350,
                                status="SENT",
                            )
                        except Exception as exc:
                            state_ref.update_file_message(transfer_id, "Gửi thất bại")
                            state_ref.update_file_progress(
                                file_id=transfer_id,
                                filename=filename,
                                total_size=len(post_body),
                                received_chunks=0,
                                total_chunks=(len(post_body) + 1349) // 1350,
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
                    username = data.get("username", "")
                    if not isinstance(username, str):
                        self._send_json(400, {"error": "Username phải là chuỗi"})
                        return
                    username = username.strip()
                    if not username:
                        self._send_json(400, {"error": "Username không được để trống"})
                        return

                    if engine_ref and getattr(engine_ref, "coordinator", None):
                        try:
                            sess_id = engine_ref.login(username)
                        except PermissionError as exc:
                            self._send_json(409, {"error": str(exc)})
                            return
                        except (OSError, ConnectionError, RuntimeError, ValueError) as exc:
                            self._send_json(502, {"error": f"Không thể đăng nhập vào server LAN: {exc}"})
                            return
                    else:
                        # Giữ chế độ P2P cũ khi không cấu hình server trung tâm.
                        sess_id = secrets.randbelow(90000) + 10000
                        state_ref.set_online(username, sess_id)
                        if engine_ref and hasattr(engine_ref, "discovery"):
                            engine_ref.discovery.send_broadcast_announce()

                    self._send_json(200, {"success": True, "session_id": sess_id})

                # Action 2: Gửi tin nhắn qua UDP
                elif parsed.path == "/api/send_message":
                    stream_id = int(data.get("stream_id", 0))
                    text = data.get("content", "").strip()
                    dest_ip = data.get("recipient_ip", "")
                    dest_port = int(data.get("recipient_port", 0))

                    if not text:
                        self._send_json(400, {"error": "Nội dung tin nhắn trống"})
                        return
                    if stream_id != 0 and (not dest_ip or not dest_port):
                        self._send_json(400, {"error": "Hãy chọn người nhận cho kênh riêng"})
                        return

                    send_errors = []
                    if engine_ref:
                        if getattr(engine_ref, "coordinator", None):
                            try:
                                target_session_id = 0
                                if stream_id != 0:
                                    with state_ref._lock:
                                        recipient = next(
                                            (
                                                peer for peer in state_ref.peers.values()
                                                if peer["ip"] == dest_ip
                                                and peer["port"] == dest_port
                                            ),
                                            None,
                                        )
                                    if recipient is None:
                                        self._send_json(400, {"error": "Người nhận không còn online"})
                                        return
                                    target_session_id = recipient["session_id"]
                                engine_ref.send_message(
                                    dest_ip,
                                    dest_port,
                                    stream_id,
                                    text,
                                    target_session_id=target_session_id,
                                )
                            except (OSError, ConnectionError, RuntimeError, ValueError) as exc:
                                send_errors.append(str(exc))
                        elif dest_ip and dest_port:
                            try:
                                engine_ref.send_message(dest_ip, dest_port, stream_id, text)
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
                                    )
                                except OSError as exc:
                                    send_errors.append(f"{p['username']}: {exc}")

                    # Ghi tin nhắn của chính mình vào Stream
                    state_ref.add_message(stream_id, f"{state_ref.username} (You)", text)
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

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), RequestHandler)
        self.httpd.daemon_threads = True
        self.httpd.serve_forever()

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
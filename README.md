# Distributed and Streaming-based Client-Server Collaboration Application

Ứng dụng cộng tác và truyền thông trên mạng cục bộ (LAN / Air-gapped Network) kết hợp kiến trúc **Lai (Hybrid Architecture)**:
- **TCP:** Quản lý phiên, xác thực, danh bạ trực tuyến và đồng bộ trạng thái đồng thời.
- **UDP Streaming qua Central Server:** Chuyển tiếp tin nhắn và các chunk file qua UDP Relay với cơ chế kiểm soát tắc nghẽn CUBIC (RFC 8312), SACK, Sliding Window và mã hóa AES-128-CBC. Chế độ không cấu hình server trung tâm vẫn hỗ trợ truyền trực tiếp giữa các peer.

---

## Mục lục

- [Tính năng chính](#tính-năng-chính)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Yêu cầu môi trường](#yêu-cầu-môi-trường)
- [Cài đặt](#cài-đặt)
- [Hướng dẫn chạy ứng dụng (Khuyến nghị chấm điểm)](#hướng-dẫn-chạy-ứng-dụng-khuyến-nghị-chấm-điểm)
  - [Kịch bản 1: Kiểm thử trên cùng một máy (Localhost Demo)](#kịch-bản-1-kiểm-thử-trên-cùng-một-máy-localhost-demo)
  - [Kịch bản 2: Kiểm thử trên nhiều máy trong mạng LAN](#kịch-bản-2-kiểm-thử-trên-nhiều-máy-trong-mạng-lan)
  - [Tham số dòng lệnh](#tham-số-dòng-lệnh)
- [Kiểm thử tự động](#lệnh-chạy-unit-tests-tự-động)
- [Hạn chế đã biết](#hạn-chế-đã-biết)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Tài liệu tham khảo](#tài-liệu-tham-khảo)

---

## Tính năng chính

- **Quản lý phiên và trạng thái trực tuyến:** Máy khách đăng nhập qua kết nối TCP duy trì lâu dài. Máy chủ cấp `session_id`, quản lý danh sách người dùng trực tuyến và cập nhật danh sách khi có máy khách kết nối hoặc ngắt kết nối.
- **Trò chuyện qua UDP:** Khi sử dụng máy chủ trung tâm, tin nhắn nhóm và tin nhắn riêng được chuyển tiếp qua UDP Relay. Nếu không cấu hình máy chủ trung tâm, các máy khách có thể tìm thấy nhau trong mạng LAN và trao đổi trực tiếp.
- **Tách luồng dữ liệu:** Tin nhắn và dữ liệu truyền tệp được phân loại theo `stream_id`, với trạng thái thứ tự và truyền lại riêng cho từng luồng. Cách tổ chức này giúp hạn chế việc một luồng truyền làm chậm các luồng khác.
- **Truyền tệp có kiểm soát lỗi:** Tệp được chia thành các khối dữ liệu tối đa 1350 byte. Xác nhận tích lũy, xác nhận chọn lọc (SACK), cửa sổ trượt và truyền lại giúp xử lý mất gói. Sau khi ghép tệp, hệ thống đối chiếu mã SHA-256.
- **Điều khiển tốc độ gửi:** Bộ gửi UDP sử dụng cửa sổ tắc nghẽn giới hạn và một biến thể rút gọn dựa trên CUBIC, có xét đến vùng tăng trưởng thân thiện với TCP. Đây là cơ chế dành cho UDP, không phải triển khai đầy đủ CUBIC trên TCP.
- **Mã hóa dữ liệu:** Trong cấu hình thông thường, dữ liệu trò chuyện và truyền tệp được mã hóa AES-128-CBC với đệm PKCS#7 và IV ngẫu nhiên 16 byte. Nếu thiếu thư viện `cryptography`, chương trình mặc định báo lỗi; chỉ khi đồng thời bật `ALLOW_INSECURE_XOR=1` mới sử dụng XOR dự phòng không an toàn.
- **Giao diện và giám sát:** Ứng dụng cung cấp giao diện web cục bộ cho người dùng và bảng điều khiển quản trị máy chủ. Bảng điều khiển hiển thị trạng thái, người dùng, lưu lượng, tiến độ truyền tệp và nhật ký; đồng thời hỗ trợ một số thao tác như khởi động hoặc dừng máy chủ, ngắt kết nối máy khách.
- **Xử lý đồng thời và timeout:** Các kết nối TCP, việc nhận gói UDP và bộ định thời truyền lại được xử lý bằng các luồng thực thi riêng. Cơ chế timeout giúp phát hiện máy khách mất kết nối hoặc không phản hồi.

---

## Kiến trúc hệ thống

Hệ thống hoạt động theo mô hình phân tách điều khiển và dữ liệu:

| Thành phần | Giao thức | Cổng mặc định | Vai trò kỹ thuật |
| :--- | :--- | :--- | :--- |
| **Server TCP Control** | TCP | `9001` | Handshake, cấp Session ID, quản lý Presence/Roster, phát hiện ngắt kết nối. |
| **Server UDP Relay** | UDP | `9002` | Chuyển tiếp các packet chat, file và ACK mang tiền tố `RLY1`. |
| **Server Dashboard** | HTTP (Loopback) | `9003` | Giám sát trạng thái máy chủ nội bộ (chỉ bind `127.0.0.1`). |
| **Client UDP Engine** | UDP | `9010+` | Gửi/nhận chat và file, trao đổi ACK/SACK qua relay hoặc trực tiếp P2P khi không dùng server trung tâm. |
| **Client Local Web UI**| HTTP (Loopback) | `8080+` | Giao diện điều khiển cho người dùng. |


---

## Yêu cầu môi trường

- **Python:** Phiên bản `3.9` trở lên.
- **Thư viện phụ thuộc:** Đã liệt kê trong `requirements.txt` (bắt buộc package `cryptography` để chạy mã hóa AES-128-CBC).
- **Mạng:** Các máy tính trao đổi được gói tin TCP và UDP nội bộ trong cùng mạng LAN / VPN / Hotspot.
- **Tường lửa (Firewall):** Cho phép Inbound/Outbound trên các cổng TCP và UDP được cấu hình.

---

## Cài đặt

Mở terminal từ thư mục gốc của dự án:

### Trên Windows (PowerShell)
```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Trên macOS / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

---

## Hướng dẫn chạy ứng dụng

---

### Kịch bản 1: Kiểm thử trên cùng một máy (Localhost Demo)

Mở **3 cửa sổ terminal riêng biệt** tại thư mục gốc của dự án:

#### 1. Khởi chạy Server trung tâm (Terminal 1)
```bash
python main.py --server --host 0.0.0.0 --tcp-port 9001 --relay-port 9002
```
* Dashboard quản trị tự động kích hoạt tại: <http://127.0.0.1:9003>.

#### 2. Khởi chạy Client 1 - An (Terminal 2)
```bash
python main.py --server-host 127.0.0.1 --tcp-port 9001 --relay-port 9002 --udp-port 9010 --web-port 8080
```
* Mở trình duyệt vào: <http://127.0.0.1:8080> $\rightarrow$ Đăng nhập tên: `An`.

#### 3. Khởi chạy Client 2 - Binh (Terminal 3)
```bash
python main.py --server-host 127.0.0.1 --tcp-port 9001 --relay-port 9002 --udp-port 9011 --web-port 8081
```
* Mở trình duyệt thứ hai (hoặc cửa sổ ẩn danh) vào: <http://127.0.0.1:8081> $\rightarrow$ Đăng nhập tên: `Binh`.

---

### Kịch bản 2: Kiểm thử trên nhiều máy trong mạng LAN

Giả sử máy chủ trung tâm có địa chỉ IP LAN là `192.168.1.15`:

1. **Trên máy Server (`192.168.1.15`):**
   ```bash
   python main.py --server --host 0.0.0.0 --tcp-port 9001 --relay-port 9002
   ```
   *(Mở firewall cho cổng TCP `9001` và UDP `9002`)*.
2. **Trên máy Client Alice:**
   ```bash
   python main.py --server-host 192.168.1.15 --tcp-port 9001 --relay-port 9002 --udp-port 9000 --web-port 8080
   ```
   *(Truy cập `http://127.0.0.1:8080` trên máy Alice để đăng nhập)*.
3. **Trên máy Client Bob:**
   ```bash
   python main.py --server-host 192.168.1.15 --tcp-port 9001 --relay-port 9002 --udp-port 9000 --web-port 8080
   ```
   *(Truy cập `http://127.0.0.1:8080` trên máy Bob để đăng nhập)*.

---



---

## Tham số dòng lệnh

| Tham số | Mặc định | Ý nghĩa kỹ thuật |
| :--- | :---: | :--- |
| `--server` | `False` | Cờ kích hoạt tiến trình làm Central Server. |
| `--server-host` | `None` | Địa chỉ IP của Central Server; nếu cấu hình sẽ bật chế độ Client kết nối Server. |
| `--host` | `0.0.0.0` | Địa chỉ bind socket cục bộ. |
| `--tcp-port` | `9001` | Cổng TCP phục vụ đăng nhập, quản lý phiên và presence của Server. |
| `--relay-port` | `9002` | Cổng UDP tiếp nhận và chuyển tiếp các gói tin chat, file và ACK. |
| `--dashboard-port` | `9003` | Cổng HTTP Web Dashboard giám sát máy chủ (chỉ bind localhost). |
| `--udp-port` | `9000` | Cổng UDP của Client dùng để nhận chat/file relay hoặc truyền file P2P khi không dùng server trung tâm. |
| `--web-port` | `8080` | Cổng HTTP Web UI của Client. |
| `--discovery-port` | `9002` | Cổng UDP phát bản tin Discovery Broadcast (chỉ dùng trong chế độ P2P thuần). |

---

## Lệnh chạy Unit Tests tự động

Từ thư mục gốc dự án:
```bash
# Chạy toàn bộ 100 test, có dòng PASS/FAIL cho từng test và tổng kết
python -m test.run_tests

# Chạy riêng các yêu cầu step
python -m test.test_step2
python -m test.test_step3
python -m test.test_step4
python -m test.test_step5
python -m test.test_step6
python -m test.test_step7
python -m test.test_step8

# Chạy test các chức năng chuyên sâu
python -m test.test_central_dashboard
python -m test.test_retransmission
python -m test.test_e2e_network
python -m test.test_thread_safety

# Load test: 20 client đồng thời + file 1 MB
python tools/load_test.py --clients 20 --file-mb 1
```

---

## Cấu trúc dự án

```text
network_app/
├── core/
│   ├── __init__.py
│   ├── crypto.py                 # Mã hóa AES-128-CBC (PKCS7) & fallback XOR
│   ├── protocol.py               # Định dạng Packet Header 24B, checksum, SACK parser
│   └── state.py                  # Quản lý RAM state, danh sách peer và stream mapping
├── docs/
│   ├── Protocol_Spec.md          # Đặc tả chi tiết giao thức
│   └── assignment_net.pdf        # Đề bài yêu cầu kỹ thuật
├── network/
│   ├── __init__.py
│   ├── central_client.py         # Client TCP socket bắt tay và duy trì presence
│   ├── central_server.py         # Server TCP presence manager & UDP chat/file relay
│   ├── discovery.py              # UDP broadcast discovery cho chế độ P2P thuần
│   ├── reliability.py            # Cửa sổ trượt, TCP CUBIC (RFC 8312), SACK & RTO
│   ├── socket_engine.py          # UDP engine điều phối định tuyến gói tin
│   └── streaming.py              # Chunking tệp tin và tái ghép kiểm tra SHA-256
├── storage/
│   ├── downloads/                # File nhận được lưu tại đây
│   └── uploads/                  # Kho lưu trữ tệp tin tải lên tạm thời
├── test/
│   ├── __init__.py
│   ├── net_helpers.py            # Tiện ích mô phỏng mạng (mất gói) cho test
│   ├── test_e2e_network.py       # Test đầu-cuối: file 1MB, mất gói 10%, chat không bị HoL
│   ├── test_retransmission.py    # Test cửa sổ trượt, CUBIC, SACK, giới hạn retry
│   ├── run_tests.py              # Chạy toàn bộ test và in tổng pass/tổng test
│   ├── test_report.py            # Định dạng dòng PASS/FAIL và tổng kết theo step
│   ├── test_step2.py             # Test bắt tay TCP và đăng nhập
│   ├── test_step3.py             # Test xác thực và trạng thái người dùng
│   ├── test_step4.py             # Test kết nối/ngắt kết nối đồng thời
│   ├── test_step5.py             # Test định tuyến và broadcast tin nhắn
│   ├── test_step6.py             # Test truyền, chia chunk và ghép file UDP
│   ├── test_step7.py             # Test timeout, lỗi mạng và shutdown an toàn
│   ├── test_step8.py             # Test mã hóa AES
│   └── test_thread_safety.py     # Test tranh chấp tài nguyên trong ClientState
├── tools/
│   ├── load_test.py              # Load test nhiều client đồng thời
│   └── TCPCUBIC.cpp              # Tham khảo thuật toán CUBIC (không dùng khi chạy)
├── web/
│   ├── dashboard/                # Giao diện Web Dashboard cho Central Server
│   ├── static/                   # CSS và Vanilla JavaScript cho Client UI
│   ├── templates/                # HTML template giao diện người dùng
│   ├── central_dashboard.py      # HTTP server phục vụ Dashboard quản trị (:9003)
│   └── server.py                 # HTTP server phục vụ giao diện cục bộ Client
├── main.py                       # Điểm nhập khởi chạy thống nhất (CLI Router)
├── requirements.txt              # Danh sách thư viện bên ngoài (cryptography)
└── README.md                     # Tài liệu hướng dẫn sử dụng và kiểm thử
```

---

## Hạn chế đã biết

- Khóa AES dùng chung, không có trao đổi khóa; CBC không có MAC nên không chống sửa đổi có chủ đích.
- Bên gửi không được báo khi bên nhận lỗi ghép file (bên nhận hiển thị "Nhận thất bại").
- Stream ID chat riêng (CRC-32) và file (ngẫu nhiên) có thể va chạm với xác suất rất nhỏ.
- Client bị server loại nếu im lặng quá 15 giây (client gửi ping mỗi 3 giây); bên gửi file bỏ cuộc sau 12 lần gửi lại một gói.
- Web UI dùng `http.server` của thư viện chuẩn (không dùng framework).

---

## Tài liệu tham khảo

- [Đặc tả kỹ thuật giao thức (Protocol_Spec.md)](docs/Protocol_Spec.md)
- [Đề tài bài tập lớn (assignment_net.pdf)](docs/assignment_net.pdf)
- [RFC 8312: CUBIC Congestion Control for Fast Long-Distance Networks](https://datatracker.ietf.org/doc/html/rfc8312)
- [RFC 2018: TCP Selective Acknowledgement Options](https://datatracker.ietf.org/doc/html/rfc2018)
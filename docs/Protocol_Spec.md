# PROTOCOL SPECIFICATION & SYSTEM ARCHITECTURE
**Project:** Distributed and Streaming-based Client-Server Collaboration Application  
**Document Status:** Production Specification  

---

## 1. SYSTEM ARCHITECTURE OVERVIEW

Hệ thống áp dụng kiến trúc lai (**Hybrid Client-Server / Peer-to-Peer**) phân tách rõ ràng giữa mặt phẳng điều khiển (**Control Plane**) và mặt phẳng dữ liệu (**Data Plane**), kết hợp giao diện quản trị cục bộ tách biệt hoàn toàn với lõi xử lý mạng.

```
+-----------------------------------------------------------------------------------+
|                                CLIENT NODE ARCHITECTURE                           |
|                                                                                   |
|  +-----------------------+      In-Memory State      +-------------------------+  |
|  |   Web UI (Browser)    | <-----------------------> |    ClientState (RAM)    |  |
|  | (HTML / Vanilla JS)   |    Local HTTP Loopback    |   (Thread-Safe RLock)   |  |
|  +-----------------------+    (127.0.0.1:8080)       +-------------------------+  |
|                                                                    ^              |
|                                                                    |              |
|  +-----------------------------------------------------------------v-----------+  |
|  |                         SOCKET ENGINE (network/socket_engine.py)            |  |
|  |  +-------------------+   +-------------------+   +-----------------------+  |  |
|  |  |  CryptoEngine     |   | ReliabilityLayer  |   |   StreamManager       |  |  |
|  |  | (AES-128-CBC)     |   | (CUBIC + SACK)    |   | (Chunking & SHA-256)  |  |  |
|  |  +-------------------+   +-------------------+   +-----------------------+  |  |
|  +----------------------------------+-------------------------------+----------+  |
|                                     |                               |             |
+-------------------------------------|-------------------------------|-------------+
                                      |                               |
                   TCP Control Plane  |                               | UDP Data Plane
                   (Port 9001)        |                               | (Port 9002)
                                      v                               v
+-----------------------------------------------------------------------------------+
|                         CENTRAL LAN SERVER (network/central_server.py)            |
|                                                                                   |
|  +-----------------------+      Shared State         +-------------------------+  |
|  | TCP Presence & Auth   | <-----------------------> |  UDP Datagram Relay     |  |
|  | - Session Allocation  |                           |  - Unicast/Broadcast    |  |
|  | - Roster Broadcast    |                           |  - File/ACK Forwarding  |  |
|  | - Ping / Timeout      |                           |  - Metrics & Telemetry  |  |
|  +-----------------------+                           +-------------------------+  |
|                                     ^                                             |
|                                     | Loopback HTTP (127.0.0.1:9003)              |
|                                     v                                             |
|                        +-------------------------+                                |
|                        | CentralDashboardServer  |                                |
|                        +-------------------------+                                |
+-----------------------------------------------------------------------------------+
```
### 1.1 State Diagram
```mermaid
stateDiagram-v2
    [*] --> Tat_May: Khởi động ứng dụng Client
    
    Tat_May --> Tu_Kiem_Tra: Bật tiến trình / Khởi tạo Local Server (ThreadingHTTPServer)
    Tu_Kiem_Tra --> Cho_Ranh_Roi: Tải thành công ClientState (In-memory Thread-safe)
    Tu_Kiem_Tra --> Su_Co: Lỗi cấu hình / Thiếu thư viện cryptography
    
    Cho_Ranh_Roi --> Tat_May: Đóng cửa sổ trình duyệt / Tắt ứng dụng
    Cho_Ranh_Roi --> Bao_Tri: Kiểm tra trạng thái / Đồng bộ thủ công
    
    Bao_Tri --> Cho_Ranh_Roi: Hoàn tất kiểm tra
    Bao_Tri --> Su_Co: Lỗi ngoại lệ hệ thống
    Su_Co --> Bao_Tri: Khởi động lại luồng
    Su_Co --> Tat_May: Buộc dừng ứng dụng

    Cho_Ranh_Roi --> Dang_Ket_Noi: POST /api/login (Gửi username & udp_port qua TCP tới Central Server)
    
    state Dang_Ket_Noi {
        [*] --> Gui_Thong_Tin
        Gui_Thong_Tin --> Doi_Phan_Hoi: Kết nối TCP bền vững (Persistent Socket)
        Doi_Phan_Hoi --> [*]: Nhận về session_id (uint32) & Roster trực tuyến
    }

    Dang_Ket_Noi --> Trang_Thai_Loi: Sai định dạng / Từ chối / Hết thời gian chờ TCP
    Trang_Thai_Loi --> Cho_Ranh_Roi: Reset giao diện / Thử lại đăng nhập

    Dang_Ket_Noi --> Phien_Da_Ket_Noi: Xác thực TCP thành công

    state Phien_Da_Ket_Noi {
        [*] --> Trang_Thai_Hoat_Dong
        
        state Trang_Thai_Hoat_Dong {
            [*] --> San_Sang
            San_Sang --> Dang_Gui_Tin_Nhan: Gửi/Nhận Chat (Luồng 0 hoặc Luồng riêng 1+)
            Dang_Gui_Tin_Nhan --> San_Sang: Nhận gói tin ACK tích lũy / Payload AES-128-CBC
            
            San_Sang --> Dang_Truyen_File: Bắt đầu truyền file (RUDP + CUBIC + SACK)
            
            state Dang_Truyen_File {
                [*] --> Trong_Hang_Doi
                Trong_Hang_Doi --> Dang_Chuyen_Chunks: Chia nhỏ file thành các chunks qua UDP Socket
                Dang_Chuyen_Chunks --> Xu_Ly_Mat_Gói: Mất gói tin / Sắp xếp sai thứ tự (Out-of-order)
                Xu_Ly_Mat_Gói --> Dang_Chuyen_Chunks: Kích hoạt Fast Retransmit (SACK_LOSS_THRESHOLD >= 2 blocks) hoặc Timeout (RTO = 0.5s)
                Dang_Chuyen_Chunks --> Hoan_Tat_Truyyen: Kiểm tra toàn vẹn CRC32 & Ghi file thành công vào Storage
            }
            
            Dang_Truyen_File --> San_Sang: Tiến trình truyền file hoàn tất hoặc gặp lỗi
        }
    }

    Phien_Da_Ket_Noi --> Cho_Ranh_Roi: Mất kết nối TCP / Server Timeout / Bị đá khỏi phòng (Kicked)
    Phien_Da_Ket_Noi --> Su_Co: Đứt đường truyền mạng LAN / Lỗi Socket Engine nghiêm trọng
```

### 1.2 Module Coupling & Responsibilities
* **`core/protocol.py`:** Định nghĩa định dạng nhị phân của gói tin (`Packet`), tính toán checksum `CRC-32`, và mã hóa/giải mã các khối `SACK`.
* **`core/crypto.py`:** Cung cấp lớp bảo mật `CryptoEngine` sử dụng mã hóa đối xứng **AES-128-CBC** với vector khởi tạo ngẫu nhiên (`IV`) và đệm `PKCS7`.
* **`core/state.py`:** Lưu trữ trạng thái đồng bộ đa luồng (`ClientState`) đóng vai trò là cầu nối dữ liệu giữa `SocketEngine` và `LocalAPIServer`.
* **`network/central_client.py` & `network/central_server.py`:** Duy trì phiên kết nối TCP bền vững để xác thực người dùng, cấp phát `session_id`, đồng bộ danh sách peer trực tuyến (`roster`), và chuyển tiếp gói tin UDP (`RLY1`).
* **`network/reliability.py`:** Hiện thực hóa cơ chế truyền tin cậy trên UDP thông qua cửa sổ trượt (`SlidingWindowSender`), kiểm soát tắc nghẽn **TCP CUBIC**, xác nhận chọn lọc (**SACK**), và sắp xếp thứ tự nhận (`StreamReceiver`).
* **`network/streaming.py`:** Phân mảnh tập tin thành các chunk tối đa `1350 bytes` (`FileTransferManager`), kiểm tra toàn vẹn dữ liệu qua **SHA-256**, và tái lắp ráp tập tin (`StreamManager`).
* **`network/socket_engine.py`:** Bộ điều phối trung tâm (Orchestrator) kết nối tất cả các module trên để gửi/nhận gói tin bất đồng bộ.

---

## 2. CONTROL PLANE PROTOCOL (TCP — PORT 9001)

Mặt phẳng điều khiển sử dụng kết nối **TCP Persistent** với định dạng thông điệp **NDJSON (Newline-Delimited JSON)** kết thúc bằng ký tự `\n` (giới hạn tối đa `4096 bytes`/dòng).

### 2.1 Handshake & Authentication Flow
1. **Client $\rightarrow$ Server (`login`):**
   ```json
   {"type": "login", "username": "alice", "udp_port": 9000}
   ```
2. **Server $\rightarrow$ Client (`login_ack`):**
   * *Thành công:* Server cấp phát ngẫu nhiên một `session_id` 32-bit không trùng lặp ($1 \le \text{session\_id} \le \text{0xFFFFFFFF}$) và trả về danh sách các peer đang hoạt động.
     ```json
     {
       "type": "login_ack",
       "success": true,
       "session_id": 284910234,
       "peers": [
         {"username": "bob", "session_id": 918273645, "ip": "192.168.1.15", "port": 9000}
       ]
     }
     ```
   * *Thất bại:* Trả về `"success": false` kèm trường `"error"` (trùng tên người dùng, sai định dạng cổng UDP).

### 2.2 Presence, Heartbeat & Diagnostics
* **Roster Synchronization (`roster`):** Bất cứ khi nào có client đăng nhập mới, ngắt kết nối, hoặc bị quản trị viên kick, Server phát quảng bá danh sách cập nhật tới toàn bộ kết nối TCP hiện hành:
  ```json
  {"type": "roster", "users": [...]}
  ```
* **Keep-Alive Heartbeat (`ping`):** Client gửi định kỳ mỗi `3.0 giây`. Nếu Server không nhận được tín hiệu trong vòng `15.0 giây` (`client_timeout`), kết nối bị ngắt và xóa khỏi `roster`.
  ```json
  {"type": "ping"}
  ```
* **Security & Integrity Telemetry (`warning`):** Client báo cáo các bất thường về gói tin tới Server Dashboard (`invalid_packet`, `decrypt_failure`, `file_decode_failure`):
  ```json
  {"type": "warning", "category": "decrypt_failure"}
  ```

---

## 3. DATA PLANE BINARY PACKET FORMAT (UDP — PORT 9002)

Tất cả các gói tin trên mặt phẳng dữ liệu đều tuân thủ cấu trúc Header cố định **24 bytes** theo chuẩn thứ tự byte mạng (**Big-Endian / Network Byte Order**, format `!HBBIIIIHH`).

### 3.1 Binary Header Layout

```
+---------------+---------+---------+------------+-----------+----------+----------+-------------+------------+------------------+
|  Magic Number | Version |  Flags  | Session ID | Stream ID | Seq Num  | ACK Num  | Payload Len |  Checksum  |     Payload      |
|   (0x4E41)    |   (1)   | (1B bit)|  (uint32)  | (uint32)  | (uint32) | (uint32) |  (uint16)   |  (CRC-16)  |  (AES / SACK)    |
+---------------+---------+---------+------------+-----------+----------+----------+-------------+------------+------------------+
|<-- 2 Bytes -->|<--1B--->|<--1B--->|<-- 4B ---->|<-- 4B --->|<-- 4B -->|<-- 4B -->|<--- 2B --->|<--- 2B --->|<-- Variable B -->|
|<---------------------------------------------- HEADER (24 Bytes) ------------------------------------------->|<----- DATA ----->|
```

### 3.2 Header Field Specifications

| Field | Size | Type | Description |
| :--- | :--- | :--- | :--- |
| **Magic** | 2 Bytes | `uint16` | Hằng số nhận diện giao thức: `0x4E41` (`"NA"`). Gói tin sai Magic bị loại bỏ ngay lập tức. |
| **Version** | 1 Byte | `uint8` | Phiên bản giao thức hiện hành: `1`. |
| **Flags** | 1 Byte | `uint8` | Mặt nạ bit (Bitmask) xác định loại gói tin điều khiển hoặc dữ liệu. |
| **Session ID** | 4 Bytes | `uint32` | Mã định danh phiên của người gửi do Central Server cấp phát. |
| **Stream ID** | 4 Bytes | `uint32` | Mã định danh luồng độc lập nhằm loại bỏ **Head-of-Line (HoL) Blocking**. |
| **Seq Num** | 4 Bytes | `uint32` | Số thứ tự đơn điệu tăng dần trên từng `Stream ID` (bắt đầu từ `1`). |
| **ACK Num** | 4 Bytes | `uint32` | Số thứ tự tích lũy (Cumulative ACK) cho biết byte/gói tiếp theo bên nhận đang chờ. |
| **Payload Len** | 2 Bytes | `uint16` | Độ dài thực tế tính bằng byte của phần `Payload` theo sau Header. |
| **Checksum** | 2 Bytes | `uint16` | 16 bit thấp của mã băm `zlib.crc32(Header_Zero_CRC + Payload) & 0xFFFF`. |

### 3.3 Control Bitmask Flags

| Flag Name | Hex Value | Binary | Description |
| :--- | :--- | :--- | :--- |
| `FLAG_AUTH` | `0x01` | `00000001` | Gói tin xác thực phiên làm việc. |
| `FLAG_DISCOV` | `0x02` | `00000010` | Gói tin quảng bá khám phá peer nội mạng (`255.255.255.255`). |
| `FLAG_MSG` | `0x04` | `00000100` | Gói tin mang thông điệp văn bản (Chat message). |
| `FLAG_FILE` | `0x08` | `00001000` | Gói tin mang metadata khởi tạo tệp hoặc chunk dữ liệu nhị phân của tệp. |
| `FLAG_ACK` | `0x10` | `00010000` | Gói tin xác nhận đã nhận dữ liệu (kèm theo SACK blocks trong payload). |
| `FLAG_HEARTBEAT`| `0x20` | `00100000` | Gói tin duy trì kết nối giữa các peer. |
| `FLAG_SYN` | `0x40` | `01000000` | Khởi tạo đồng bộ luồng. |
| `FLAG_FIN` | `0x80` | `10000000` | Kết thúc luồng truyền dữ liệu. |

### 3.4 Server UDP Relay Envelope (`RLY1`)
Khi hoạt động ở chế độ Client-Server trung tâm, gói tin nhị phân `Packet` được bọc trong một phong bì định tuyến có tiền tố `RLY1` (4 bytes) kèm JSON metadata:
* **Client $\rightarrow$ Server (`relay`):**
  ```text
  RLY1{"type":"relay","sender_session_id":284910234,"target_session_id":918273645,"packet":"<Base64_Packet>"}
  ```
  *(Nếu truyền file, phong bì đính kèm thêm trường `"file"` chứa `transfer_id`, `filename`, `filesize`, `total_chunks`, `first_seq` và `"retry_count"` để Server giám sát tiến độ).*
* **Server $\rightarrow$ Target Client (`deliver`):**
  ```text
  RLY1{"type":"deliver","sender_session_id":284910234,"packet":"<Base64_Packet>"}
  ```

---

## 4. STREAM MULTIPLEXING & HoL BLOCKING ELIMINATION

Để ngăn chặn các luồng truyền tập tin dung lượng lớn làm nghẽn các tin nhắn thời gian thực, giao thức phân tách dữ liệu thành các **Stream ID** độc lập:

* **`Stream ID = 0` (Global Broadcast Chat):** Kênh trò chuyện chung toàn mạng.
* **`Stream ID = private_stream_id(A, B)` (Private Unicast Chat):** Kênh trò chuyện riêng tư giữa 2 peer, tính toán tất định bằng hàm băm:
  $$\text{StreamID} = \text{CRC32}(\text{"}\min(\text{ID}_A, \text{ID}_B)\text{:}\max(\text{ID}_A, \text{ID}_B)\text{"}) \pmod{2^{32}}$$
  *(Nếu kết quả $\le 1$, cộng thêm $2$ để tránh xung đột với kênh hệ thống).*
* **`Stream ID = Random(2, 0xFFFFFFFE)` (Ephemeral File Transfer Stream):** Mỗi lần truyền một tập tin mới sẽ khởi tạo một `Stream ID` ngẫu nhiên riêng biệt. Hàng đợi truyền lại (`SlidingWindowSender`) và bộ đệm sắp xếp (`StreamReceiver`) hoạt động độc lập theo khóa `(stream_id, peer_endpoint)`, đảm bảo tin nhắn chat luôn được xử lý ngay lập tức.

---

## 5. RELIABILITY, SACK & CONGESTION CONTROL

### 5.1 Selective Acknowledgment (SACK) Payload Format
Khi gửi gói tin có cờ `FLAG_ACK`, phần `Payload` chứa danh sách các khối dữ liệu đã nhận được không liên tục (Out-of-order) theo chuẩn **RFC 2018**:

```
+--------------+-------------+-------------------+-------------------+-----+-------------------+
| SACK Version | Block Count |   SACK Block #1   |   SACK Block #2   | ... |   SACK Block #N   |
|     (1)      |   (0..32)   |  [Left  | Right)  |  [Left  | Right)  |     |  [Left  | Right)  |
+--------------+-------------+-------------------+-------------------+-----+-------------------+
|<--- 1B ----->|<--- 1B ---->|<---- 4B + 4B ---->|<---- 4B + 4B ---->|     |<---- 4B + 4B ---->|
```

* Mỗi khối biểu diễn nửa khoảng `[left, right)` dưới dạng số nguyên không dấu 32-bit (`!II`), tuân thủ điều kiện: $1 \le \text{left}_1 < \text{right}_1 \le \text{left}_2 < \text{right}_2 \le \text{0xFFFFFFFF}$.
* **Fast Retransmit:** Nếu bên gửi nhận thấy có ít nhất `SACK_LOSS_THRESHOLD = 2` khối SACK nằm phía trên một gói tin chưa được xác nhận, gói tin đó được đánh dấu mất và truyền lại ngay lập tức mà không cần chờ hết thời gian `RTO = 0.5s`.

### 5.2 TCP CUBIC Congestion Control (RFC 8312)
Mỗi `SlidingWindowSender` duy trì một cửa sổ tắc nghẽn động (`cwnd`) khởi tạo bằng `4.0` gói tin (giới hạn tối đa `window_size = 8` gói):
* **Giai đoạn Slow Start ($\text{cwnd} < \text{ssthresh}$):** Tăng tuyến tính $\text{cwnd} \leftarrow \text{cwnd} + 1$ theo mỗi gói được xác nhận.
* **Giai đoạn Congestion Avoidance (CUBIC Function):**
  $$W(t) = C \cdot (t - K)^3 + W_{\max}$$
  Trong đó: $C = 0.4$, $\beta = 0.7$, $t$ là thời gian trôi qua kể từ lần mất gói gần nhất, và điểm uốn $K$ được tính bằng:
  $$K = \sqrt[3]{\frac{W_{\max} \cdot (1 - \beta)}{C}}$$
* **TCP-Friendly Region:** Tại mỗi bước cập nhật, thuật toán đối chiếu với mô hình TCP Reno ($W_{\text{tcp}}(t)$) và chọn $\max(W(t), W_{\text{tcp}}(t))$ để đảm bảo thông lượng công bằng.
* **Phản ứng khi mất gói (Loss Event):** Cập nhật $W_{\max} = \text{cwnd}$, giảm $\text{ssthresh} = \max(2.0, \text{cwnd} \cdot \beta)$, đặt lại $\text{cwnd} = \text{ssthresh}$, và khởi tạo chu kỳ CUBIC mới.

---

## 6. RELIABLE FILE TRANSFER LIFECYCLE

Quá trình truyền tập tin kích thước lớn qua UDP tuân thủ quy trình 3 bước khép kín để chống phân mảnh IP (IP Fragmentation) và đảm bảo toàn vẹn dữ liệu:

1. **Chunking & Integrity Hashing (`FileTransferManager`):**
   * Tập tin gốc được đọc tuần tự và cắt thành các mảnh kích thước tối đa **`1350 bytes`** (đảm bảo tổng kích thước gói tin sau khi cộng Header 22 bytes, AES IV/Padding 32 bytes và Relay JSON luôn nằm gọn dưới ngưỡng `MTU = 1500 bytes`).
   * Toàn bộ nội dung tệp được băm bằng thuật toán **SHA-256**.
2. **Metadata Initialization (`seq_num = 1`):**
   * Gói tin đầu tiên trên luồng truyền file mang cờ `FLAG_FILE` chứa chuỗi JSON metadata được mã hóa AES:
     ```json
     {
       "filename": "report.pdf",
       "filesize": 1288192,
       "total_chunks": 955,
       "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
       "transfer_id": "a1b2c3d4e5f6...",
       "chat_stream_id": 0
     }
     ```
3. **Chunk Transmission & Reassembly (`seq_num = 2 ... N+1`):**
   * Các chunk dữ liệu được mã hóa độc lập và đẩy vào cửa sổ trượt CUBIC.
   * Tại bên nhận (`StreamManager`), sau khi thu thập đủ `total_chunks` theo đúng thứ tự từ `StreamReceiver`, hệ thống ghép nối các byte và đối chiếu mã băm **SHA-256**. Nếu khớp, tệp được lưu vào thư mục `storage/downloads/` (tự động đánh số hậu tố `_1`, `_2` nếu trùng tên).

---

## 7. CRYPTOGRAPHIC SECURITY LAYER

Tất cả các trường `Payload` của gói tin `FLAG_MSG` và `FLAG_FILE` đều được mã hóa đầu-cuối (End-to-End Payload Encryption) trước khi đóng gói vào `Packet`:

* **Thuật toán:** **AES-128** (Advanced Encryption Standard với độ dài khóa 128-bit).
* **Chế độ vận hành:** **CBC** (Cipher Block Chaining) kết hợp đệm chuẩn **PKCS7** (block size 128-bit).
* **Cấu trúc Ciphertext trên đường truyền:**
  ```
  +------------------------+---------------------------------------+
  |  Random IV (16 Bytes)  |   AES-128-CBC Ciphertext (N * 16 B)   |
  +------------------------+---------------------------------------+
  ```
* Mỗi gói tin sử dụng một vector khởi tạo ngẫu nhiên `IV = os.urandom(16)` độc lập, ngăn chặn triệt để việc phân tích mẫu lưu lượng (Traffic Pattern Analysis) ngay cả khi người dùng gửi lặp lại cùng một nội dung tin nhắn.

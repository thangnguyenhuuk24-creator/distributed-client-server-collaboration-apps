# Báo cáo kỹ thuật - Ứng dụng cộng tác Client-Server trên UDP Streaming

Nov 13, 2026 · @tim

Ứng dụng dùng TCP cho đăng nhập và danh sách người online, còn chat và truyền file chạy trên giao thức tin cậy tự thiết kế trên UDP (cửa sổ trượt, SACK, CUBIC, AES-128-CBC), qua một Central Server và client có Web UI cục bộ. Môn: Network Application Development (CO3093/CO3094), HCMUT. Thành viên: \[điền họ tên - MSSV\].

## Giới thiệu

Bài làm đáp ứng đủ 5 tính năng của đề (đăng nhập và khám phá, chat chung và riêng, truyền file tin cậy, chịu lỗi, mã hóa) bằng Python 3.9+, chỉ dùng socket thô, `threading` và `http.server` của thư viện chuẩn. Không dùng framework HTTP hay WebSocket; thư viện ngoài duy nhất là `cryptography`.

| Tính năng | Cách hiện thực | Module chính |
| --- | --- | --- |
| Đăng nhập, danh sách online | TCP bền vững, thông điệp NDJSON, `session_id` 32 bit, roster đẩy tới mọi client | `central_server.py`, `central_client.py` |
| Chat chung và riêng | UDP qua relay, stream 0 cho chat chung, stream riêng cho từng cặp người dùng | `socket_engine.py` |
| Truyền file tin cậy | Chunk 1350 byte, cửa sổ trượt, SACK, CUBIC, kiểm tra SHA-256 khi ghép | `reliability.py`, `streaming.py` |
| Chịu lỗi | Ping 3 giây, timeout 15 giây, tối đa 12 lần gửi lại một gói, try/except quanh mọi thao tác socket | `central_server.py`, `reliability.py` |
| Mã hóa | AES-128-CBC, đệm PKCS7, IV ngẫu nhiên cho từng gói | `crypto.py` |
| Giao diện | Web UI JavaScript thuần tại `127.0.0.1:8080`; dashboard server tại `127.0.0.1:9003` | `web/` |

Cách chạy nhanh (chi tiết trong `README.md`):

```bash
pip install -r requirements.txt
python main.py --server --host 0.0.0.0 --tcp-port 9001 --relay-port 9002
python main.py --server-host <IP_SERVER> --tcp-port 9001 --relay-port 9002 --udp-port 9010 --web-port 8080
```

## 1. Kiến trúc hệ thống

Hệ thống theo mô hình lai: mỗi client nối tới một Central Server bằng hai kênh tách biệt, kênh điều khiển TCP (đăng nhập, danh sách online, ping) và kênh dữ liệu UDP (chat, file, ACK, đi qua relay của server). Bên trong client, ba lớp được tách rõ: Web UI, `ClientState` dùng chung trong RAM, và Socket Engine. Khi không đặt `--server-host`, client chạy chế độ P2P thuần với UDP broadcast discovery.

&#91;embedded content: kiến trúc tổng quan · 1 client chi tiết, 1 server\]

Client A và Client B cùng chạy một cấu trúc; chúng không gửi gói trực tiếp cho nhau mà đi qua UDP Relay của server.

| Lớp | Module | Vai trò |
| --- | --- | --- |
| Mạng | `network/socket_engine.py` | Điều phối: nhận UDP, giải mã, gửi ACK/SACK, vòng lặp gửi lại mỗi 50 ms |
| Mạng | `network/reliability.py` | `SlidingWindowSender` (cửa sổ trượt, CUBIC, SACK) và `StreamReceiver` (sắp thứ tự, khử trùng lặp) |
| Mạng | `network/central_server.py`, `central_client.py` | TCP đăng nhập, ping, roster; server đồng thời relay UDP |
| Xử lý dữ liệu | `core/protocol.py` | `Packet` header 24 byte, CRC, mã hóa và giải mã khối SACK |
| Xử lý dữ liệu | `core/crypto.py` | `CryptoEngine` AES-128-CBC |
| Xử lý dữ liệu | `network/streaming.py` | Cắt chunk 1350 byte, SHA-256, ghép file |
| Xử lý dữ liệu | `core/state.py` | `ClientState` an toàn đa luồng (RLock), cầu nối giữa mạng và UI |
| Giao diện | `web/server.py`, `web/static/js/app.js` | HTTP loopback và UI JavaScript thuần, polling mỗi 500 ms |
| Giao diện | `web/central_dashboard.py` | Dashboard quản trị server: uptime, user, throughput, tiến độ file, kick |

**Mô hình đồng thời.** Dùng `threading`. Server có 1 thread accept TCP, 1 thread relay UDP và 1 thread cho mỗi client đang kết nối. Client có thread nhận UDP, thread gửi lại, thread đọc roster từ TCP, thread ping, thread HTTP, và mỗi lần gửi file chạy trên một thread riêng. Dữ liệu dùng chung được bảo vệ bằng `RLock`; server sao chép danh sách người dùng trong khóa rồi mới gửi ra ngoài khóa, nên một client chậm không giữ khóa của cả server. `tools/load_test.py` chạy 20 client đồng thời để kiểm tra mức 10-20 client của đề.

## 2. Đặc tả giao thức

Giao thức có hai tầng: mặt phẳng điều khiển dùng JSON trên TCP, mặt phẳng dữ liệu dùng gói nhị phân tự thiết kế trên UDP với header cố định 24 byte.

### 2.1 Mặt phẳng điều khiển (TCP 9001)

Mỗi thông điệp là một dòng JSON kết thúc bằng `\n` (client gửi tối đa 4096 byte một dòng). Kết nối TCP được giữ suốt phiên.

| Thông điệp | Hướng | Trường | Ý nghĩa |
| --- | --- | --- | --- |
| `login` | Client → Server | `username` (1-64 ký tự), `udp_port` | Xin đăng nhập |
| `login_ack` | Server → Client | `success`, `session_id` (uint32), `peers[]`; hoặc `error` | Cấp phiên. Từ chối khi tên đã online (không phân biệt hoa thường), tên rỗng hoặc cổng UDP ngoài 1-65535 |
| `roster` | Server → Client | `users[]` | Phát lại danh sách online mỗi khi có người vào, ra hoặc bị kick |
| `ping` | Client → Server | không có | Gửi mỗi 3 giây; im lặng quá 15 giây thì server loại client |
| `warning` | Client → Server | `category`: `invalid_packet`, `decrypt_failure`, `file_decode_failure` | Báo bất thường lên dashboard |

### 2.2 Gói dữ liệu UDP: header 24 byte

Định dạng `struct` `!HBBIIIIHH` (big-endian, thứ tự byte mạng), theo sau là payload.

| Offset | Trường | Kích thước | Mô tả |
| --- | --- | --- | --- |
| 0 | Magic | 2 byte | `0x4E41`; sai Magic thì bỏ gói |
| 2 | Version | 1 byte | Hiện là `1` |
| 3 | Flags | 1 byte | Bitmask loại gói (bảng bên dưới) |
| 4 | Session ID | 4 byte | Phiên của người gửi, do server cấp |
| 8 | Stream ID | 4 byte | Luồng logic của gói (mục 2.4) |
| 12 | Seq Num | 4 byte | Số thứ tự gói trong stream, bắt đầu từ 1 |
| 16 | ACK Num | 4 byte | ACK tích lũy: số thứ tự gói tiếp theo bên nhận đang chờ |
| 20 | Payload Len | 2 byte | Độ dài payload tính bằng byte |
| 22 | Checksum | 2 byte | 16 bit thấp của CRC-32 trên (header với checksum = 0) + payload |

Bên nhận loại gói khi thiếu byte, sai Magic, `Payload Len` không khớp độ dài thực hoặc sai checksum.

| Flag | Giá trị | Công dụng |
| --- | --- | --- |
| `FLAG_MSG` | `0x04` | Tin nhắn chat (payload đã mã hóa) |
| `FLAG_FILE` | `0x08` | Metadata hoặc chunk của file (payload đã mã hóa) |
| `FLAG_ACK` | `0x10` | Xác nhận; payload chứa các khối SACK |
| `FLAG_DISCOV` | `0x02` | Broadcast khám phá peer, chỉ dùng ở chế độ P2P |
| `FLAG_HEARTBEAT` | `0x20` | Duy trì peer, chỉ dùng ở chế độ P2P |
| `FLAG_AUTH`, `FLAG_SYN`, `FLAG_FIN` | `0x01`, `0x40`, `0x80` | Đã định nghĩa trong header, dành cho mở rộng; luồng hiện tại đăng nhập qua TCP nên chưa dùng |

### 2.3 Phong bì relay `RLY1`

Trong chế độ có server, client không gửi thẳng tới peer mà bọc gói nhị phân (Base64) trong một datagram bắt đầu bằng `RLY1` và gửi tới UDP Relay.

```text
Client -> Server: RLY1{"type":"relay","sender_session_id":S,"target_session_id":T,"packet":"<base64>"}
Server -> Client: RLY1{"type":"deliver","sender_session_id":S,"packet":"<base64>"}
```

`target_session_id = 0` nghĩa là server phát tới mọi người còn lại; client hiện gửi một bản riêng cho từng peer thay vì dùng giá trị này. Với gói file, phong bì thêm `file` (`transfer_id`, `filename`, `filesize`, `total_chunks`, `first_seq`) và `retry_count` để dashboard theo dõi tiến độ và số lần gửi lại. Trước khi chuyển tiếp, server kiểm tra: IP và cổng gửi khớp với phiên đã đăng nhập, `session_id` trong gói khớp người gửi, tên file không chứa đường dẫn, file không quá 200 MB, `total_chunks` khớp kích thước, `seq` nằm trong khoảng đã khai báo và `retry_count` không quá 12.

### 2.4 Stream và chống Head-of-Line blocking

| Stream ID | Dùng cho |
| --- | --- |
| `0` | Chat chung |
| CRC-32 của chuỗi `"<id nhỏ>:<id lớn>"` (hai session ID), cộng 2 nếu kết quả ≤ 1 | Chat riêng giữa hai người; hai bên tự tính ra cùng giá trị |
| Ngẫu nhiên trong khoảng 2 đến 2³²−1 | Mỗi lần gửi một file |

Mỗi cặp (Stream ID, peer) có bộ gửi và bộ nhận riêng, với số thứ tự, cửa sổ trượt và `cwnd` riêng. Vì vậy chunk file bị mất chỉ làm chậm chính stream file đó; tin nhắn trên stream khác vẫn được giao ngay.

### 2.5 Độ tin cậy: số thứ tự, ACK và SACK

- **Số thứ tự:** tăng đơn điệu theo từng gói trong mỗi stream, bắt đầu từ 1.
- **Bên nhận** (`StreamReceiver`): chỉ nhận gói có `seq` trong `[expected, expected + 8)`, lưu gói đến sớm vào bộ đệm, loại gói trùng, giao dữ liệu lên tầng trên đúng thứ tự, rồi trả ACK tích lũy kèm các khối SACK.
- **Payload SACK:** `Version` (1 byte, giá trị 1), `Block Count` (1 byte, tối đa 32), sau đó mỗi khối là một nửa khoảng `[left, right)` gồm 2 số uint32 (8 byte), theo kiểu RFC 2018.
- **Bên gửi** (`SlidingWindowSender`): số gói chưa ACK không vượt `min(8, cwnd)`. RTO cố định 0,5 giây. Chỉ gửi lại gói chưa được SACK; gói có từ 2 khối SACK nằm phía trên mà vẫn chưa được xác nhận bị gửi lại ngay (fast retransmit) mà không chờ hết RTO.
- **Bỏ cuộc:** một gói bị gửi lại quá 12 lần thì stream bị đánh dấu thất bại, bên gửi báo lỗi thay vì treo vô hạn.

### 2.6 Điều khiển tắc nghẽn CUBIC

`cwnd` khởi đầu 4 gói, tối đa 8 gói (kích thước cửa sổ). Dưới `ssthresh` là slow start (tăng 1 gói cho mỗi gói được ACK); sau đó dùng hàm CUBIC theo RFC 8312 với C = 0,4 và β = 0,7:

```latex
W(t) = C\,(t - K)^3 + W_{max}, \qquad K = \sqrt[3]{\frac{W_{max}\,(1-\beta)}{C}}
```

Mỗi lần cập nhật, bên gửi so với mô hình TCP Reno (vùng TCP-friendly) và lấy giá trị lớn hơn. Khi có gói hết RTO, `cwnd` giảm đúng một lần cho mỗi đợt mất gói: `W_max = cwnd`, `ssthresh = max(2, 0,7 × cwnd)`, `cwnd = ssthresh`, và một chu kỳ CUBIC mới bắt đầu.

### 2.7 Mã hóa

Payload của `FLAG_MSG` và `FLAG_FILE` (kể cả metadata và từng chunk file) được mã hóa bằng **AES-128-CBC** với đệm PKCS7. Mỗi gói có IV ngẫu nhiên 16 byte (`os.urandom`) đặt ngay đầu bản mã, nên cùng một tin nhắn gửi hai lần cho ra hai bản mã khác nhau. Header và gói ACK/SACK không mã hóa. Khóa 16 byte lấy từ biến môi trường `CNET_SECRET_KEY` (mặc định là khóa tĩnh trong mã nguồn); mọi client phải dùng cùng khóa.

### 2.8 Truyền file

1. Bên gửi đọc file, cắt thành chunk tối đa **1350 byte** và tính SHA-256 của toàn file.
2. Gói đầu của stream file mang metadata JSON đã mã hóa: `filename`, `filesize`, `total_chunks`, `sha256`, `transfer_id`, `chat_stream_id`.
3. Các gói tiếp theo mang từng chunk đã mã hóa, đi qua cửa sổ trượt CUBIC của stream file.
4. Bên nhận ghép đủ `total_chunks` theo thứ tự, so SHA-256; nếu khớp thì lưu vào `storage/downloads/` (thêm hậu tố `_1`, `_2` khi trùng tên), nếu lệch thì báo "Nhận thất bại".

### 2.9 Sơ đồ trạng thái

Phiên của client đi qua ba trạng thái chính; việc truyền file có vòng trạng thái riêng bên trong trạng thái đã kết nối.

&#91;embedded content: trạng thái phiên client và vòng gửi file · 3 + 5 trạng thái\]

Mũi tên vòng phía trên là khi TCP đứt, quá 15 giây không ping hoặc bị kick trên dashboard; khi đó danh sách peer của client bị xóa. Nét đứt nối trạng thái Đã kết nối với vòng gửi file diễn ra bên trong nó.

## 3. Sequence diagram

Ba sơ đồ dưới đây đọc theo thời gian từ trên xuống: đăng nhập, vòng đời một tin nhắn và vòng đời một lần truyền file.

### 3.1 Đăng nhập và đồng bộ danh sách online

Client đăng nhập qua TCP; server cấp `session_id`, trả danh sách peer hiện có và phát `roster` mới cho mọi client khác.

&#91;embedded content: sequence diagram đăng nhập · 4 thành phần\]

### 3.2 Vòng đời một tin nhắn

Ví dụ A gửi tin riêng cho B, lần gửi đầu bị mất trên đường. Với chat chung, client gửi một bản riêng cho từng peer (mỗi peer có cửa sổ trượt và ACK riêng), nên một peer chậm không làm chậm những peer khác.

&#91;embedded content: sequence diagram tin nhắn · có mất gói và gửi lại\]

### 3.3 Vòng đời một lần truyền file

File dùng cùng cơ chế ACK/SACK nhưng chạy trên một stream ngẫu nhiên riêng, nên không chặn chat.

&#91;embedded content: sequence diagram truyền file · metadata, chunk, ACK, SHA-256\]

## 4. Challenges & Solutions

Năm khó khăn mạng chính khi hiện thực; mỗi khó khăn đều có test kiểm chứng trong thư mục `test/`.

**1. Mất gói, trùng gói và đảo thứ tự trên UDP.** *Vấn đề:* khi giả lập mất 10-15% gói, chat đến thiếu, trùng hoặc sai thứ tự, file bị hỏng. *Nguyên nhân:* UDP không bảo đảm giao nhận và thứ tự. *Giải pháp:* đánh số thứ tự gói theo từng stream, bên nhận trả ACK tích lũy kèm SACK, giữ gói đến sớm trong bộ đệm, loại gói trùng và chỉ giao lên trên khi đủ thứ tự; bên gửi chỉ gửi lại gói chưa được SACK, sau RTO 0,5 giây hoặc ngay khi thấy từ 2 khối SACK phía trên. *Kiểm chứng:* `test_private_chat_in_order_exactly_once_with_loss` (20 tin, mất 15%) và `test_file_survives_10_percent_packet_loss`.

**2. Head-of-Line blocking giữa chat và file.** *Vấn đề:* nếu dùng chung một chuỗi số thứ tự và một cửa sổ, một file lớn đang gửi lại gói sẽ chặn mọi tin nhắn phía sau. *Giải pháp:* tách dữ liệu thành các stream độc lập; mỗi cặp (Stream ID, peer) có bộ gửi, bộ nhận, cửa sổ trượt và `cwnd` riêng. *Kiểm chứng:* `test_chat_is_not_blocked_by_a_lossy_file_transfer` gửi 5 tin chat trong lúc file 400 KB vẫn đang truyền với 15% mất gói, và cả 5 tin đến đầy đủ.

**3. Kích thước gói và phân mảnh IP.** *Vấn đề:* datagram vượt MTU bị IP phân mảnh, mất một mảnh là mất cả gói. *Giải pháp:* giới hạn chunk 1350 byte; sau khi mã hóa (thêm IV 16 byte và đệm PKCS7) và thêm header 24 byte, gói gửi trực tiếp tối đa 1400 byte, dưới MTU 1500. *Hạn chế còn lại:* khi đi qua relay, gói được bọc Base64 và JSON nên phình lên khoảng 2 KB và vẫn có thể bị IP phân mảnh trên LAN thật; SACK bù lại bằng cách gửi lại, và hướng cải thiện là dùng phong bì nhị phân thay cho Base64.

**4. Tranh chấp dữ liệu giữa các luồng.** *Vấn đề:* luồng nhận UDP, luồng gửi lại, luồng HTTP và luồng ping cùng đọc ghi `ClientState`, bảng bộ gửi/bộ nhận và socket TCP. *Giải pháp:* `RLock` cho `ClientState` và các bảng bộ gửi/bộ nhận; mỗi socket TCP có `send_lock` riêng; giao diện chỉ đọc bản sao dữ liệu; khi phát `roster`, server sao chép danh sách người dùng trong khóa rồi gửi ngoài khóa, để một client chậm không giữ khóa của toàn server.

**5. Client rớt đột ngột và peer "ma".** *Vấn đề:* client tắt đột ngột không báo trước thì server vẫn giữ tên trong danh sách, còn bên gửi file chờ ACK mãi. *Giải pháp:* client ping mỗi 3 giây, server đặt timeout 15 giây trên socket TCP rồi loại client và phát `roster` mới; bên gửi bỏ cuộc sau 12 lần gửi lại một gói và báo lỗi thay vì treo; mọi vòng đọc socket đều bắt `OSError`, lỗi JSON và `ValueError` nên một client lỗi không làm sập server. *Kiểm chứng:* `test_11_silent_client_is_removed_after_timeout` và `test_sender_gives_up_when_receiver_disappears`.

## 5. Kiểm thử

Bộ kiểm thử gồm 41 test case (đếm trong mã nguồn) chạy với server thật và socket engine thật trên localhost; các test đầu-cuối chèn mất gói giả lập vào socket UDP của client.

| Kịch bản | File | Số test | Kiểm chứng |
| --- | --- | --- | --- |
| Bắt tay TCP, đăng nhập | `test/test_step2.py` | 11 | Nhận ACK, ghi IP và cổng UDP, từ chối tên rỗng, tên trùng, cổng sai, JSON hỏng; 10 client đăng nhập đồng thời; client im lặng bị loại sau timeout |
| Mã hóa | `test/test_step8.py` | 10 | Giải mã đúng văn bản tiếng Việt, dữ liệu nhị phân, payload lớn; IV ngẫu nhiên làm hai bản mã khác nhau; chuẩn hóa độ dài khóa |
| Độ tin cậy và CUBIC | `test/test_retransmission.py` | 11 | Cửa sổ trượt, ACK tích lũy, SACK, chỉ gửi lại gói thiếu, RTO, giảm `cwnd` khi mất gói, bỏ cuộc sau 12 lần |
| Đầu-cuối có mất gói | `test/test_e2e_network.py` | 6 | File 1 MB không mất gói; file 150 KB mất 10%; file qua relay mất 10%; chat riêng đúng thứ tự và đúng một lần khi mất 15%; chat không bị chặn bởi file; bên gửi bỏ cuộc khi bên nhận biến mất. Các file nhận được so SHA-256 với bản gốc |
| Dashboard server | `test/test_central_dashboard.py` | 3 | Trạng thái, xuất và xóa log, dừng và khởi động lại server, kick client |
| Tải 10-20 client | `tools/load_test.py` | - | 20 client đăng nhập cùng lúc, mọi client nhận đủ tin chat chung của những client còn lại, tùy chọn gửi thêm file 1 MB |

Lệnh chạy từ thư mục gốc của dự án:

```bash
python -m unittest discover -s test -t . -v
python tools/load_test.py --clients 20 --file-mb 1
```

Ngoài ra có kiểm thử thủ công theo `README.md`: một server và hai client trên localhost trong ba cửa sổ terminal, và nhiều máy trong cùng mạng LAN.

**Kết quả chạy:** \[dán ảnh chụp terminal sau khi chạy hai lệnh trên, và ảnh giao diện web hai client đang chat và gửi file\].

## 6. Kết luận, hạn chế và tài liệu tham khảo

Ứng dụng hoàn thành đủ 10 bước của rubric: giao thức tự thiết kế trên UDP có tin cậy, chống Head-of-Line blocking bằng stream, điều khiển tắc nghẽn CUBIC, mã hóa payload và giao diện web JavaScript thuần.

| Bước | Nội dung | Nơi hiện thực | Kiểm chứng |
| --- | --- | --- | --- |
| 1 | Thiết kế giao thức, sơ đồ trạng thái | Mục 2 của báo cáo, `docs/Protocol_Spec.md` | - |
| 2 | Bắt tay TCP | `central_server.py`, `central_client.py` | `test_step2.py` |
| 3 | Đăng nhập, theo dõi user, IP, cổng | `session_id`, roster, `get_users()` | `test_step2.py` |
| 4 | Đồng thời | Một thread cho mỗi client TCP, thread relay UDP | `load_test.py` (20 client) |
| 5 | Định tuyến chat chung và riêng | UDP Relay, stream 0 và stream riêng | `test_e2e_network.py` |
| 6 | Truyền file tin cậy | `streaming.py`, `reliability.py` | `test_e2e_network.py` |
| 7 | Chịu lỗi | Ping, timeout 15 giây, giới hạn 12 lần gửi lại, try/except | `test_step2.py`, `test_e2e_network.py` |
| 8 | Mã hóa | `crypto.py` (AES-128-CBC) | `test_step8.py` |
| 9 | Web UI JavaScript thuần | `web/static/js/app.js`: tin nhắn, thanh tiến độ file, danh sách online | Thử thủ công |
| 10 | Kiểm thử và chất lượng mã | `test/`, `tools/load_test.py` | Mục 5 |

**Hạn chế đã biết.**

- Khóa AES dùng chung và không có bước trao đổi khóa; CBC không kèm MAC nên không chống được việc sửa đổi có chủ đích.
- Phong bì relay dùng Base64 nên gói đi qua relay lớn khoảng 2 KB (mục 4, khó khăn 3).
- Bên gửi không được báo khi bên nhận ghép file thất bại; bên nhận hiển thị "Nhận thất bại".
- Stream ID của chat riêng (CRC-32) và của file (ngẫu nhiên) có thể va chạm với xác suất rất nhỏ.
- Web UI dùng `http.server` của thư viện chuẩn Python, không phải framework HTTP.

**Tài liệu tham khảo**

- [RFC 8312: CUBIC for Fast Long-Distance Networks](https://datatracker.ietf.org/doc/html/rfc8312)
- [RFC 2018: TCP Selective Acknowledgment Options](https://datatracker.ietf.org/doc/html/rfc2018)
- Mã nguồn và `README.md` của dự án; `docs/Protocol_Spec.md`.

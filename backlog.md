# Phân tích Yêu cầu Đồ án: Ứng dụng Cộng tác Client-Server qua UDP

Theo đề bài, hệ thống yêu cầu xây dựng một ứng dụng cộng tác client-server trên mạng LAN mà không dựa vào dịch vụ bên thứ ba (như Slack hay cloud storage). Giao thức tầng ứng dụng phải do sinh viên tự thiết kế, chạy trên nền UDP và đảm bảo độ tin cậy tuyệt đối cho luồng dữ liệu nhắn tin.

## 1. Năm tính năng bắt buộc

*   **Đăng nhập và khám phá:** Cho phép đăng nhập và hiển thị danh sách người dùng đang online.
*   **Nhắn tin:** Hỗ trợ chat chung (broadcast) và chat riêng (unicast).
*   **Truyền file tin cậy:** Đảm bảo gửi file mọi định dạng, mọi kích thước mà không bị hỏng hóc dữ liệu.
*   **Chịu lỗi (Fault Tolerance):** Xử lý an toàn các tình huống client ngắt kết nối đột ngột, timeout, hoặc đứt mạng mà không làm crash server.
*   **Mã hóa:** Mã hóa payload để chống nghe lén (ví dụ: thuật toán AES).

## 2. Các kỹ thuật bắt buộc sử dụng

*   Sử dụng **Socket thô (Raw Socket)** (Tuyệt đối cấm các framework như Flask, Django, WebSocket).
*   Áp dụng **Đa luồng (Multithreading)** hoặc **Bất đồng bộ (Async)** để phục vụ đồng thời tối thiểu 10–20 client.
*   Tự thiết kế cấu trúc gói tin (bao gồm: Header, Payload, Flags).
*   Implement cơ chế tin cậy: Số thứ tự (Sequence numbers), Cửa sổ trượt (Sliding window), ACK, và cơ chế chỉ gửi lại gói tin bị mất (Selective Retransmission).
*   Thuật toán điều khiển tắc nghẽn (Congestion control - ví dụ: CUBIC hoặc BBR).
*   Xử lý chống nghẽn đầu hàng (**Head-of-Line blocking**) giữa các luồng dữ liệu.
*   Giao diện web HTTP chỉ được sử dụng JavaScript ở phía client.

## 3. Đối chiếu yêu cầu với Code thực tế

| Yêu cầu từ đề bài | Triển khai trong Source Code |
| :--- | :--- |
| **Đăng nhập, danh sách online** | Dùng TCP trong `central_server.py` và `central_client.py` |
| **Chat chung và riêng** | Dùng UDP relay, stream 0 cho broadcast và stream riêng cho unicast |
| **Truyền file tin cậy** | Chia chunk 1350 byte, dùng SACK, check mã băm SHA-256 trong `socket_engine.py` |
| **Khả năng chịu lỗi** | Cơ chế Ping, timeout 15 giây, giới hạn gửi lại (retry limit) 12 lần |
| **Mã hóa dữ liệu** | Thuật toán AES-128-CBC triển khai tại `core/crypto.py` |
| **Chống Head-of-Line Blocking** | Mỗi hội thoại và file truyền tải được phân tách thành một stream riêng biệt |
| **Phục vụ 10–20 client** | Đã test trong `tools/load_test.py` với mô phỏng 20 client đồng thời |
| **Giao diện Web UI** | Setup qua `web/server.py` kết hợp với thư mục `static/` |

> **⚠️ Lưu ý quan trọng:** Đề bài cấm sử dụng “HTTP frameworks”. Hiện tại code đang sử dụng `http.server` thuộc thư viện chuẩn của Python (không bị tính là framework). Nhưng ... 

***************************************************************************

## 1. Source code: DoneDone

Toàn bộ script client và server.
README.md có hướng dẫn từng bước: cài thư viện, chạy server, kết nối client.

## 2. Báo cáo kỹ thuật (PDF) gồm 4 phần: (tham khảo ở docs/techical_Report.pdf)

Sơ đồ kiến trúc hệ thống (client và server tương tác thế nào).
Đặc tả giao thức (cấu trúc gói tin).
Sequence diagram: vòng đời của một tin nhắn và một lần truyền file.
Challenges & Solutions: phần tự đánh giá 1 trang về các khó khăn mạng và cách giải quyết.
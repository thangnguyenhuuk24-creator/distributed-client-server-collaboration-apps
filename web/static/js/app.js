document.addEventListener("DOMContentLoaded", () => {
    // Trạng thái cục bộ phía Web Client
    let currentStreamId = 0;
    let selectedPeer = null; // { ip, port, username }
    let isLoggedIn = false;
    let fileTransfers = new Map();
    let activeUpload = null;
    let lastRenderKey = "";
    let forceScrollBottom = true;
    let lastPeersKey = "";
    let polling = false;

    // DOM Elements
    const loginModal = document.getElementById("login-modal");
    const usernameInput = document.getElementById("username-input");
    const loginBtn = document.getElementById("login-btn");
    
    const displayUser = document.getElementById("display-user");
    const displaySession = document.getElementById("display-session");
    const peerList = document.getElementById("peer-list");
    const peerCount = document.getElementById("peer-count");
    
    const messagesContainer = document.getElementById("messages-container");
    const messageInput = document.getElementById("message-input");
    const sendMsgBtn = document.getElementById("send-msg-btn");
    
    const fileInput = document.getElementById("file-input");
    const sendFileBtn = document.getElementById("send-file-btn");
    const transferList = document.getElementById("transfer-list");
    const transferCount = document.getElementById("transfer-count");
    
    const streamTabs = document.querySelectorAll(".stream-tab");
    const activeStreamBadge = document.getElementById("active-stream-badge");
    const activeChatTitle = document.getElementById("active-chat-title");

    // 1. Xử lý Đăng nhập
    loginBtn.addEventListener("click", performLogin);
    usernameInput.addEventListener("keypress", (e) => {
        if (e.key === "Enter") performLogin();
    });

    async function performLogin() {
        const username = usernameInput.value.trim();
        if (!username) return alert("Vui lòng nhập tên tài khoản!");

        try {
            const res = await fetch("/api/login", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: jsonStringify({ username }),
            });
            const data = await res.json();
            if (data.success) {
                isLoggedIn = true;
                loginModal.style.display = "none";
                displayUser.textContent = username;
                displaySession.textContent = `Session: ${data.session_id}`;
                startPolling();
            } else {
                alert(data.error || "Đăng nhập thất bại");
            }
        } catch (err) {
            console.error("Lỗi đăng nhập:", err);
        }
    }

    // 2. Chuyển đổi Stream ID (Multiplexing Tabs)
    streamTabs.forEach((tab) => {
        tab.addEventListener("click", () => {
            streamTabs.forEach((t) => t.classList.remove("active"));
            tab.classList.add("active");
            const selectedTabStreamId = parseInt(tab.dataset.stream, 10);
            currentStreamId = selectedTabStreamId === 0
                ? 0
                : (selectedPeer ? selectedPeer.private_stream_id : 1);
            forceScrollBottom = true;
            activeStreamBadge.textContent = selectedTabStreamId === 0
                ? "Stream ID: 0"
                : "Kênh riêng";
            updateActiveChatTitle();
            fetchMessages();
        });
    });

    // 3. Gửi Tin Nhắn Văn Bản
    sendMsgBtn.addEventListener("click", sendMessage);
    messageInput.addEventListener("keypress", (e) => {
        if (e.key === "Enter") sendMessage();
    });

    async function sendMessage() {
        const content = messageInput.value.trim();
        if (!content) return;

        const recipient = currentStreamId === 0 ? null : selectedPeer;
        if (currentStreamId !== 0 && !recipient) {
            alert("Hãy chọn người nhận trong danh sách người dùng online trước.");
            return;
        }
        const payload = {
            stream_id: currentStreamId,
            content: content,
            recipient_ip: recipient ? recipient.ip : "",
            recipient_port: recipient ? recipient.port : 0,
        };

        try {
            const res = await fetch("/api/send_message", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: jsonStringify(payload),
            });
            const result = await res.json();
            if (!res.ok) {
                alert(result.error || "Không gửi được tin nhắn.");
                return;
            }
            messageInput.value = "";
            fetchMessages();
        } catch (err) {
            console.error("Lỗi gửi tin nhắn:", err);
        }
    }

    // 4. Gửi Tập Tin (Reliable File Transfer)
    sendFileBtn.addEventListener("click", async () => {
        const file = fileInput.files[0];
        if (!file) return alert("Hãy chọn file cần gửi.");
        if (currentStreamId !== 0 && !selectedPeer) {
            return alert("Vui lòng chọn người nhận từ danh sách online.");
        }

        try {
            const query = new URLSearchParams({
                stream_id: currentStreamId,
            });
            if (currentStreamId !== 0) {
                query.set("recipient_ip", selectedPeer.ip);
                query.set("recipient_port", selectedPeer.port);
            }
            activeUpload = { filename: file.name, progress: 0 };
            sendFileBtn.disabled = true;
            renderTransfers();
            const data = await uploadFile(file, query);
            if (!data.success) {
                alert(data.error || "Không gửi được file.");
                return;
            }
            fileInput.value = "";
            await fetchFileTransfers();
            fetchMessages();
        } catch (err) {
            console.error("Lỗi gửi file:", err);
            alert(`Không gửi được file: ${err.message}`);
        } finally {
            activeUpload = null;
            sendFileBtn.disabled = false;
            renderTransfers();
        }
    });

    function uploadFile(file, query) {
        return new Promise((resolve, reject) => {
            const request = new XMLHttpRequest();
            request.open("POST", `/api/send_file?${query}`);
            request.setRequestHeader("Content-Type", "application/octet-stream");
            request.setRequestHeader("X-File-Name", encodeURIComponent(file.name));
            request.setRequestHeader("Cache-Control", "no-store");
            request.upload.addEventListener("progress", (event) => {
                if (!event.lengthComputable || !activeUpload) return;
                activeUpload.progress = Math.round(event.loaded / event.total * 100);
                renderTransfers();
            });
            request.addEventListener("load", () => {
                let data;
                try {
                    data = JSON.parse(request.responseText);
                } catch (error) {
                    reject(new Error("Server trả về phản hồi không hợp lệ."));
                    return;
                }
                if (request.status < 200 || request.status >= 300) {
                    reject(new Error(data.error || `HTTP ${request.status}`));
                    return;
                }
                resolve(data);
            });
            request.addEventListener("error", () => reject(new Error("Lỗi kết nối tới server local.")));
            request.addEventListener("abort", () => reject(new Error("Đã hủy tải file lên server.")));
            request.send(file);
        });
    }

    // 5. Polling Dữ Liệu Thời Gian Thực
    function startPolling() {
        const poll = async () => {
            if (!isLoggedIn || polling) return;
            polling = true;
            try {
                await fetchPeers();
                await Promise.all([fetchFileTransfers(), fetchMessages()]);
            } finally {
                polling = false;
            }
        };
        poll();
        setInterval(poll, 500); // Refresh transfer progress twice per second.
    }

    async function fetchFileTransfers() {
        try {
            const res = await fetch("/api/files", { cache: "no-store" });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            if (!Array.isArray(data)) throw new Error("Phản hồi tiến độ file không hợp lệ");
            fileTransfers = new Map(
                data.map((transfer) => [transfer.file_id, transfer])
            );
            renderTransfers();
        } catch (err) {
            console.error("Lỗi lấy tiến độ truyền file:", err);
        }
    }

    function renderTransfers() {
        if (!transferList || !transferCount) return;
        const transfers = [...fileTransfers.values()].sort(
            (left, right) => right.updated_at - left.updated_at
        );
        transferCount.textContent = transfers.length + (activeUpload ? 1 : 0);
        const cards = transfers.map((transfer) => {
            const progress = Math.max(
                0,
                Math.min(100, Number(transfer.progress_percent) || 0)
            );
            const status = ({
                SENDING: "Đang gửi qua UDP",
                RECEIVING: "Đang nhận qua UDP",
                SENT: "Đã gửi",
                COMPLETED: "Đã nhận",
                ERROR: "Truyền thất bại",
            }[transfer.status] || "Đang chuẩn bị");
            return `
                <article class="transfer-card">
                    <strong>${escapeHTML(transfer.filename)}</strong>
                    <span class="transfer-status">${status}</span>
                    <div class="transfer-progress-track"
                         role="progressbar"
                         aria-label="Tiến độ truyền ${escapeHTML(transfer.filename)}"
                         aria-valuemin="0"
                         aria-valuemax="100"
                         aria-valuenow="${progress}">
                        <div class="transfer-progress-fill" style="width: ${progress}%"></div>
                    </div>
                    <span class="transfer-progress-label">${transfer.received_chunks}/${transfer.total_chunks} phần · ${progress}%</span>
                </article>`;
        });
        if (activeUpload) {
            const progress = Math.max(0, Math.min(100, activeUpload.progress));
            cards.unshift(`
                <article class="transfer-card">
                    <strong>${escapeHTML(activeUpload.filename)}</strong>
                    <span class="transfer-status">Đang tải lên server local</span>
                    <div class="transfer-progress-track"
                         role="progressbar"
                         aria-label="Đang tải lên ${escapeHTML(activeUpload.filename)}"
                         aria-valuemin="0"
                         aria-valuemax="100"
                         aria-valuenow="${progress}">
                        <div class="transfer-progress-fill" style="width: ${progress}%"></div>
                    </div>
                    <span class="transfer-progress-label">${progress}%</span>
                </article>`);
        }
        transferList.innerHTML = cards.join("")
            || '<p class="empty-hint">Chưa có file đang truyền.</p>';
    }

    async function fetchPeers() {
        try {
            const res = await fetch("/api/peers");
            const peers = await res.json();
            if (!Array.isArray(peers)) throw new Error("Danh sách peer không hợp lệ");
            if (selectedPeer) {
                const previousStreamId = selectedPeer.private_stream_id;
                selectedPeer = peers.find(
                    p => p.ip === selectedPeer.ip && p.port === selectedPeer.port
                ) || null;
                if (currentStreamId !== 0 && selectedPeer) {
                    currentStreamId = selectedPeer.private_stream_id;
                    if (previousStreamId !== currentStreamId) {
                        updateActiveChatTitle();
                    }
                } else if (currentStreamId !== 0) {
                    currentStreamId = 1;
                    updateActiveChatTitle();
                }
            }
            const peersKey = JSON.stringify([
                peers,
                selectedPeer ? `${selectedPeer.ip}:${selectedPeer.port}` : null,
            ]);
            if (peersKey === lastPeersKey) return;
            lastPeersKey = peersKey;
            peerCount.textContent = peers.length;

            peerList.innerHTML = "";
            peers.forEach((p) => {
                const li = document.createElement("li");
                li.className = `peer-item ${selectedPeer && selectedPeer.ip === p.ip && selectedPeer.port === p.port ? "active" : ""}`;
                li.innerHTML = `
                    <div class="peer-info">
                        <strong>${escapeHTML(p.username)}</strong>
                        <span class="peer-endpoint">${p.ip}:${p.port}</span>
                    </div>
                `;
                li.addEventListener("click", () => {
                    selectedPeer = p;
                    forceScrollBottom = true;
                    updateActiveChatTitle();
                    fetchPeers();
                    if (currentStreamId !== 0) fetchMessages();
                });
                peerList.appendChild(li);
            });
        } catch (err) {
            console.error("Lỗi lấy peers:", err);
        }
    }

    async function fetchMessages() {
        try {
            const requestedStreamId = currentStreamId;
            const res = await fetch(`/api/messages?stream_id=${requestedStreamId}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            if (requestedStreamId !== currentStreamId) return;
            const messages = data.messages || [];
            const renderKey = JSON.stringify([
                requestedStreamId,
                messages,
                messages
                    .filter((message) => message.type === "file")
                    .map((message) => fileTransfers.get(message.file_id)),
            ]);
            if (renderKey === lastRenderKey) return;
            lastRenderKey = renderKey;

            const previousScrollTop = messagesContainer.scrollTop;
            const wasNearBottom =
                messagesContainer.scrollHeight -
                previousScrollTop -
                messagesContainer.clientHeight < 80;
            messagesContainer.innerHTML = "";
            messages.forEach((msg) => {
                const div = document.createElement("div");
                const isSelf = msg.sender.includes("(You)");
                div.className = `msg-item ${isSelf ? "self" : ""}`;
                
                const timeStr = new Date(msg.timestamp * 1000).toLocaleTimeString();
                const transfer = msg.type === "file"
                    ? fileTransfers.get(msg.file_id)
                    : null;
                const progress = transfer
                    ? Math.max(0, Math.min(100, Number(transfer.progress_percent) || 0))
                    : 0;
                const receivedChunks = transfer
                    ? Number(transfer.received_chunks) || 0
                    : 0;
                const totalChunks = transfer
                    ? Number(transfer.total_chunks) || 0
                    : 0;
                const transferStatus = transfer
                    ? ({
                        SENDING: "Đang gửi",
                        RECEIVING: "Đang nhận",
                        SENT: "Đã gửi",
                        COMPLETED: "Đã nhận",
                        ERROR: "Gửi thất bại",
                    }[transfer.status] || msg.status || "Đang truyền")
                    : (msg.status || "Đang truyền");
                const body = msg.type === "file"
                    ? `<div class="file-message">
                           <strong>${escapeHTML(msg.filename)}</strong>
                           <span class="file-transfer-status">${escapeHTML(transferStatus)}</span>
                           <div class="file-transfer-progress">
                               <div class="progress-bar-bg"
                                    role="progressbar"
                                    aria-label="Tiến độ tệp ${escapeHTML(msg.filename)}"
                                    aria-valuemin="0"
                                    aria-valuemax="100"
                                    aria-valuenow="${progress}">
                                   <div class="progress-bar-fill" style="width: ${progress}%"></div>
                               </div>
                               <span class="file-progress-label">${transfer
                                   ? `${receivedChunks}/${totalChunks} phần · ${progress}%`
                                   : "Đang chờ cập nhật tiến độ"}</span>
                           </div>
                           ${msg.status === "Đã nhận"
                               ? `<a href="/api/downloads/${encodeURIComponent(msg.download_filename || msg.filename)}" download> Tải xuống</a>`
                               : ""}
                       </div>`
                    : `<div class="msg-body">${escapeHTML(msg.text)}</div>`;
                div.innerHTML = `
                    <div class="msg-meta">${escapeHTML(msg.sender)} • ${timeStr}</div>
                    ${body}
                `;
                messagesContainer.appendChild(div);
            });
            if (forceScrollBottom || wasNearBottom) {
                messagesContainer.scrollTop = messagesContainer.scrollHeight;
                forceScrollBottom = false;
            } else {
                messagesContainer.scrollTop = previousScrollTop;
            }
        } catch (err) {
            console.error("Lỗi lấy tin nhắn:", err);
        }
    }

    function updateActiveChatTitle() {
        if (currentStreamId === 0) {
            activeChatTitle.textContent = "Kênh Chung (Broadcast)";
        } else {
            currentStreamId = selectedPeer ? selectedPeer.private_stream_id : 1;
            activeChatTitle.textContent = selectedPeer
                ? `Kênh Riêng với ${selectedPeer.username}`
                : "Kênh Riêng — hãy chọn người nhận bên trái";
        }
    }

    function jsonStringify(obj) {
        return JSON.stringify(obj);
    }

    function escapeHTML(str) {
        return str.replace(/[&<>'"]/g, 
            tag => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[tag] || tag)
        );
    }
});
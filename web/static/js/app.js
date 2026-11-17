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
    let pollingTimer = null;
    let downloadInProgress = new Set();
    let serverChoices = [];
    let serverListLoaded = false;

    fetch("/api/servers", { cache: "no-store" })
        .then(async (response) => {
            const result = await response.json();
            if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
            if (!Array.isArray(result.servers)) {
                throw new Error("Danh sách server không hợp lệ");
            }
            serverChoices = result.servers;
            serverListLoaded = true;
            serverSelect.replaceChildren();
            if (serverChoices.length === 0) {
                const option = document.createElement("option");
                option.value = "";
                option.textContent = "Chưa có server được cấu hình";
                serverSelect.appendChild(option);
                serverSelect.disabled = true;
                loginBtn.disabled = true;
                return;
            }
            serverSelect.disabled = false;
            serverChoices.forEach((server) => {
                const option = document.createElement("option");
                option.value = server.id;
                option.textContent = server.label || server.name;
                serverSelect.appendChild(option);
            });
            serverSelect.value = result.selected_id || "";
            loginBtn.disabled = false;
        })
        .catch((error) => {
            console.error("Không lấy được cấu hình server:", error);
            serverSelect.innerHTML = '<option value="">Không tải được server</option>';
            loginBtn.disabled = false;
        });

    // DOM Elements
    const loginModal = document.getElementById("login-modal");
    const serverSelect = document.getElementById("server-select");
    const usernameInput = document.getElementById("username-input");
    const passwordInput = document.getElementById("password-input");
    const loginBtn = document.getElementById("login-btn");
    loginBtn.disabled = true;
    const logoutBtn = document.getElementById("logout-btn");
    
    const displayUser = document.getElementById("display-user");
    const displaySession = document.getElementById("display-session");
    const peerList = document.getElementById("peer-list");
    const peerCount = document.getElementById("peer-count");
    const offlinePeerList = document.getElementById("offline-peer-list");
    const offlinePeerCount = document.getElementById("offline-peer-count");
    
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
    passwordInput.addEventListener("keypress", (e) => {
        if (e.key === "Enter") performLogin();
    });
    logoutBtn.addEventListener("click", performLogout);

    async function performLogin() {
        const username = usernameInput.value.trim();
        const password = passwordInput.value;
        if (!username || !password) return alert("Vui lòng nhập username và mật khẩu.");
        if (!serverListLoaded) return alert("Không tải được danh sách server. Vui lòng tải lại trang.");
        const selectedServer = serverChoices.find(
            (server) => server.id === serverSelect.value
        );
        if (!selectedServer) return alert("Vui lòng chọn server.");

        try {
            const res = await fetch("/api/login", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: jsonStringify({
                    username,
                    password,
                    server_name: selectedServer.name,
                    server_host: selectedServer.host,
                    tcp_port: selectedServer.tcp_port,
                    relay_port: selectedServer.relay_port,
                }),
            });
            const data = await res.json();
            if (res.ok && data.success) {
                if (data.server_name) {
                    selectedServer.name = data.server_name;
                    serverSelect.options[serverSelect.selectedIndex].textContent =
                        data.server_name;
                }
                isLoggedIn = true;
                loginModal.style.display = "none";
                logoutBtn.hidden = false;
                displayUser.textContent = username;
                displaySession.textContent = `Session: ${data.session_id}`;
                if (data.mailbox_sync_error) {
                    alert(`Đăng nhập thành công nhưng chưa đồng bộ được tin nhắn offline: ${data.mailbox_sync_error}`);
                }
                lastRenderKey = "";
                lastPeersKey = "";
                startPolling();
            } else {
                alert(data.error || "Đăng nhập thất bại");
            }
        } catch (err) {
            console.error("Lỗi đăng nhập:", err);
        }
    }

    async function performLogout() {
        try {
            const response = await fetch("/api/logout", { method: "POST" });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            isLoggedIn = false;
            if (pollingTimer) {
                clearInterval(pollingTimer);
                pollingTimer = null;
            }
            logoutBtn.hidden = true;
            loginModal.style.display = "flex";
            displayUser.textContent = "Chưa kết nối";
            displaySession.textContent = "Session: --";
            peerList.innerHTML = "";
            offlinePeerList.innerHTML = "";
            messagesContainer.innerHTML = "";
            fileTransfers.clear();
            renderTransfers();
            selectedPeer = null;
            currentStreamId = 0;
            lastRenderKey = "";
            lastPeersKey = "";
        } catch (error) {
            alert(`Không thể đăng xuất: ${error.message}`);
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
            alert("Hãy chọn người nhận trong danh sách người dùng online hoặc offline.");
            return;
        }
        const payload = {
            stream_id: currentStreamId,
            content: content,
            recipient_ip: recipient ? recipient.ip : "",
            recipient_port: recipient ? recipient.port : 0,
            recipient_username: recipient ? recipient.username : "",
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
            return alert("Vui lòng chọn người nhận từ danh sách online hoặc offline.");
        }

        try {
            const query = new URLSearchParams({
                stream_id: currentStreamId,
            });
            if (currentStreamId !== 0) {
                query.set("recipient_ip", selectedPeer.ip);
                query.set("recipient_port", selectedPeer.port);
                query.set("recipient_username", selectedPeer.username);
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
        if (!pollingTimer) {
            pollingTimer = setInterval(poll, 500);
        }
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
            if (!Array.isArray(peers)) throw new Error("Danh sách người dùng không hợp lệ");
            if (selectedPeer) {
                const previousStreamId = selectedPeer.private_stream_id;
                selectedPeer = peers.find(
                    p => p.username.toLowerCase() === selectedPeer.username.toLowerCase()
                ) || null;
                if (currentStreamId !== 0 && selectedPeer) {
                    currentStreamId = selectedPeer.private_stream_id;
                    if (previousStreamId !== currentStreamId) updateActiveChatTitle();
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
            const onlinePeers = peers.filter((peer) => peer.online !== false);
            const offlinePeers = peers.filter((peer) => peer.online === false);
            peerCount.textContent = onlinePeers.length;
            offlinePeerCount.textContent = offlinePeers.length;

            function renderPeerItems(targetList, items, isOffline) {
                targetList.innerHTML = "";
                items.forEach((p) => {
                    const li = document.createElement("li");
                    li.className = `peer-item ${selectedPeer && selectedPeer.username.toLowerCase() === p.username.toLowerCase() ? "active" : ""}`;
                    li.innerHTML = `
                        <div class="peer-info">
                            <strong>${escapeHTML(p.username)}</strong>
                            <span class="${isOffline ? "peer-status" : "peer-endpoint"}">${isOffline ? "Offline" : `${escapeHTML(p.ip)}:${p.port}`}</span>
                        </div>
                    `;
                    li.addEventListener("click", () => {
                        selectedPeer = p;
                        if (isOffline) {
                            currentStreamId = p.private_stream_id;
                            streamTabs.forEach((tab) => {
                                tab.classList.toggle("active", tab.dataset.stream === "1");
                            });
                            activeStreamBadge.textContent = "Kênh riêng";
                        } else if (currentStreamId !== 0) {
                            currentStreamId = p.private_stream_id;
                        }
                        forceScrollBottom = true;
                        updateActiveChatTitle();
                        fetchPeers();
                        if (currentStreamId !== 0) fetchMessages();
                    });
                    targetList.appendChild(li);
                });
            }
            renderPeerItems(peerList, onlinePeers, false);
            renderPeerItems(offlinePeerList, offlinePeers, true);
        } catch (err) {
            console.error("Lỗi lấy peers:", err);
        }
    }

    async function fetchMessages() {
        try {
            const requestedStreamId = currentStreamId;
            if (requestedStreamId !== 0 && !selectedPeer) return;
            const peerQuery = requestedStreamId !== 0
                ? `&peer_username=${encodeURIComponent(selectedPeer.username)}`
                : "";
            const res = await fetch(
                `/api/messages?stream_id=${requestedStreamId}${peerQuery}`
            );
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
                const body = msg.type === "file" && (msg.expired || msg.status === "Hết hạn")
                    ? `<div class="file-message">
                           <strong>${escapeHTML(msg.filename)}</strong>
                           <span>${formatBytes(msg.filesize)}</span>
                           <span class="file-transfer-status">File đã hết hạn</span>
                       </div>`
                    : msg.type === "file"
                    ? `<div class="file-message">
                           <strong>${escapeHTML(msg.filename)}</strong>
                           <span>${formatBytes(msg.filesize)}</span>
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
                           ${msg.status === "Chờ nhận"
                               ? `<button class="btn secondary-btn offline-file-download" data-mail-id="${escapeHTML(msg.file_id)}">Tải file (còn tối đa 1 phút)</button>`
                               : ""}
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
                const offlineDownload = div.querySelector(".offline-file-download");
                if (offlineDownload) {
                    offlineDownload.addEventListener("click", async () => {
                        const mailId = offlineDownload.dataset.mailId;
                        if (downloadInProgress.has(mailId)) return;
                        downloadInProgress.add(mailId);
                        offlineDownload.disabled = true;
                        try {
                            const response = await fetch(
                                `/api/offline-files/${encodeURIComponent(mailId)}`
                            );
                            const result = await response.json();
                            if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
                            await fetchMessages();
                        } catch (error) {
                            alert(`Không tải được file offline: ${error.message}`);
                            await fetchMessages();
                        } finally {
                            downloadInProgress.delete(mailId);
                        }
                    });
                }
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

    function formatBytes(value) {
        const size = Math.max(0, Number(value) || 0);
        if (size < 1024) return `${size} B`;
        if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
        if (size < 1024 * 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)} MB`;
        return `${(size / (1024 * 1024 * 1024)).toFixed(1)} GB`;
    }

    function escapeHTML(str) {
        return str.replace(/[&<>'"]/g, 
            tag => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[tag] || tag)
        );
    }
});
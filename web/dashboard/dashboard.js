const $ = (id) => document.getElementById(id);

function formatDuration(totalSeconds) {
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return days
        ? `${days}d ${hours}h ${minutes}m`
        : `${hours}h ${minutes}m ${seconds}s`;
}

function formatBytes(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function makeCell(value, code = false) {
    const cell = document.createElement("td");
    if (code) {
        const node = document.createElement("code");
        node.textContent = value;
        cell.appendChild(node);
    } else {
        cell.textContent = value;
    }
    return cell;
}

function renderUsers(users) {
    const tbody = $("users-body");
    tbody.replaceChildren();
    $("users-label").textContent = users.length;
    if (!users.length) {
        const row = document.createElement("tr");
        const cell = makeCell("No clients connected");
        cell.colSpan = 8;
        cell.className = "empty-state";
        row.appendChild(cell);
        tbody.appendChild(row);
        return;
    }
    users.forEach((user) => {
        const row = document.createElement("tr");
        row.append(
            makeCell(user.username),
            makeCell(String(user.session_id), true),
            makeCell(user.ip),
            makeCell(String(user.tcp_port)),
            makeCell(String(user.udp_port)),
            makeCell("Online"),
            makeCell(formatDuration(user.connected_seconds)),
        );
        const action = document.createElement("td");
        const kick = document.createElement("button");
        kick.className = "button danger";
        kick.type = "button";
        kick.textContent = "Kick";
        kick.addEventListener("click", async () => {
            if (!window.confirm(`Disconnect ${user.username}?`)) return;
            await runAction("/api/clients/kick", { session_id: user.session_id });
        });
        action.appendChild(kick);
        row.appendChild(action);
        tbody.appendChild(row);
    });
}

function renderTransfers(transfers) {
    const tbody = $("transfers-body");
    tbody.replaceChildren();
    if (!transfers.length) {
        const row = document.createElement("tr");
        const cell = makeCell("No relayed file transfers yet.");
        cell.colSpan = 7;
        cell.className = "empty-state";
        row.appendChild(cell);
        tbody.appendChild(row);
        return;
    }
    transfers.sort((left, right) => right.updated_at - left.updated_at);
    transfers.forEach((transfer) => {
        const row = document.createElement("tr");
        const percent = transfer.total_chunks
            ? Math.min(100, transfer.chunks_acked / transfer.total_chunks * 100)
            : 100;
        row.append(
            makeCell(transfer.filename),
            makeCell(formatBytes(transfer.filesize)),
            makeCell(`${transfer.sender} → ${transfer.recipient}`),
            makeCell(`${transfer.chunks_forwarded}/${transfer.total_chunks} · ${transfer.chunks_acked} ACK`),
            makeCell(`${percent.toFixed(1)}%`),
            makeCell(String(transfer.retransmissions)),
            makeCell(transfer.status),
        );
        tbody.appendChild(row);
    });
}

function describeEvent(event) {
    switch (event.type) {
        case "tcp_connected":
            return ["Handshake started", `${event.ip}:${event.tcp_port}`];
        case "login_accepted":
            return ["Authentication succeeded", `${event.username} · ${event.ip} · UDP ${event.udp_port}`];
        case "login_rejected":
            return ["Authentication failed", `${event.username || event.ip} · ${event.reason}`];
        case "client_disconnected":
            return ["Client disconnected", `${event.username} · ${event.ip}`];
        case "client_timeout":
            return ["Client timed out", `${event.username} · ${event.ip}`];
        case "client_kicked":
            return ["Client kicked", `${event.username} · ${event.ip}`];
        case "network_warning":
            return ["Network / security warning", `${event.ip}:${event.port} · ${event.reason}`];
        case "client_warning":
            return ["Client reported a security warning", `${event.username} · ${event.category}`];
        case "login_timeout":
            return ["Handshake timed out", event.ip];
        case "server_started":
            return ["Server started", `TCP ${event.tcp_port} · UDP ${event.relay_port}`];
        case "server_stopped":
            return ["Server stopped", "Listeners closed"];
        case "chat_relayed":
            return ["Chat relayed", `${event.sender} → ${event.recipient} · stream ${event.stream_id} · ${event.bytes} B`];
        case "file_relayed":
            return ["File packet relayed", `${event.sender} → ${event.recipient} · stream ${event.stream_id} · ${event.bytes} B`];
        case "ack_relayed":
            return ["ACK relayed", `${event.sender} → ${event.recipient} · stream ${event.stream_id}`];
        case "relay_dropped":
            return ["Relay delivery failed", `session ${event.sender_session_id} → ${event.target_session_id}`];
        default:
            return [event.type, ""];
    }
}

function renderEvents(events) {
    const container = $("events-list");
    container.replaceChildren();
    if (!events.length) {
        const empty = document.createElement("p");
        empty.className = "empty-state";
        empty.textContent = "No activity yet.";
        container.appendChild(empty);
        return;
    }
    events.forEach((event) => {
        const row = document.createElement("div");
        row.className = "event-row";
        const time = document.createElement("time");
        time.className = "event-time";
        time.textContent = new Date(event.timestamp * 1000).toLocaleTimeString();
        const content = document.createElement("div");
        const title = document.createElement("p");
        title.className = "event-title";
        const detail = document.createElement("p");
        detail.className = "event-detail";
        [title.textContent, detail.textContent] = describeEvent(event);
        content.append(title, detail);
        row.append(time, content);
        container.appendChild(row);
    });
}

async function refreshDashboard() {
    try {
        const response = await fetch("/api/dashboard", { cache: "no-store" });
        if (!response.ok) throw new Error(`Dashboard API returned ${response.status}`);
        const data = await response.json();
        const metrics = data.metrics;
        $("server-status").textContent = data.status.toUpperCase();
        $("server-status").previousElementSibling.classList.toggle("offline", data.status !== "online");
        $("server-toggle").textContent = data.status === "online" ? "Stop server" : "Start server";
        $("server-toggle").disabled = false;
        $("last-updated").textContent = new Date(data.updated_at * 1000).toLocaleTimeString();
        $("uptime").textContent = formatDuration(data.uptime_seconds);
        $("online-count").textContent = data.online_count;
        $("successful-logins").textContent = metrics.successful_logins;
        $("data-relays").textContent = `${metrics.chat_relays} / ${metrics.file_relays}`;
        $("chat-types").textContent = `${metrics.global_chat_relays} / ${metrics.private_chat_relays}`;
        $("throughput").textContent = `${formatBytes(data.throughput.bytes_per_second)}/s`;
        $("packet-throughput").textContent =
            `${data.throughput.packets_per_second} packets/s · ` +
            `${data.throughput.file_packets_per_second} file packets/s`;
        $("relay-bytes").textContent = formatBytes(metrics.relay_bytes);
        $("relay-drops").textContent = `${metrics.relay_drops} dropped · ${metrics.ack_relays} ACK relays`;
        $("tcp-endpoint").textContent = `${data.listeners.tcp.address}:${data.listeners.tcp.port}`;
        $("udp-endpoint").textContent = `${data.listeners.udp_relay.address}:${data.listeners.udp_relay.port}`;
        $("tcp-address").textContent = `${data.listeners.tcp.address}:${data.listeners.tcp.port}`;
        $("udp-address").textContent = `${data.listeners.udp_relay.address}:${data.listeners.udp_relay.port}`;
        renderUsers(data.users);
        renderTransfers(data.file_transfers);
        renderEvents(data.events);
    } catch (error) {
        $("server-status").textContent = "CONNECTION ERROR";
        $("server-status").previousElementSibling.classList.add("offline");
        $("server-toggle").disabled = true;
        console.error("Could not refresh server dashboard:", error);
    }
}

async function runAction(path, body) {
    try {
        const response = await fetch(path, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
        await refreshDashboard();
    } catch (error) {
        window.alert(error.message);
    }
}

$("server-toggle").addEventListener("click", async () => {
    const online = $("server-status").textContent === "ONLINE";
    if (online && !window.confirm("Stop the server and disconnect all clients?")) return;
    $("server-toggle").disabled = true;
    await runAction(online ? "/api/server/stop" : "/api/server/start");
});

$("clear-logs").addEventListener("click", async () => {
    await runAction("/api/logs/clear");
});

refreshDashboard();
window.setInterval(refreshDashboard, 2000);

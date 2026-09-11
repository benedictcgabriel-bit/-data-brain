// API Base URL - handles both direct port 8000 or Nginx proxy /api
const API_BASE = window.location.port === "3000" ? "/api" : "http://localhost:8000";

let selectedFile = null;
let currentJobId = null;
let statusPollInterval = null;

// DOM Elements
const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("fileInput");
const btnSelectFile = document.getElementById("btnSelectFile");
const btnUpload = document.getElementById("btnUpload");
const previewWrapper = document.getElementById("previewWrapper");
const previewHead = document.getElementById("previewHead");
const previewBody = document.getElementById("previewBody");

const statusCard = document.getElementById("statusCard");
const lblFilename = document.getElementById("lblFilename");
const badgeStatus = document.getElementById("badgeStatus");
const progressBar = document.getElementById("progressBar");
const lblJobId = document.getElementById("lblJobId");
const lblProgressCounts = document.getElementById("lblProgressCounts");
const lblRowsLoaded = document.getElementById("lblRowsLoaded");
const lblRowsFailed = document.getElementById("lblRowsFailed");

const chatHistory = document.getElementById("chatHistory");
const chatInput = document.getElementById("chatInput");
const btnSendChat = document.getElementById("btnSendChat");

const dotKafka = document.getElementById("dotKafka");
const lblKafka = document.getElementById("lblKafka");
const dotNeo4j = document.getElementById("dotNeo4j");
const lblNeo4j = document.getElementById("lblNeo4j");
const dotApi = document.getElementById("dotApi");
const lblApi = document.getElementById("lblApi");

// ----------------------------------------------------
// 1. Health Probe Polling
// ----------------------------------------------------
async function checkHealth() {
  try {
    const res = await fetch(`${API_BASE}/health`);
    const data = await res.json();
    
    if (data.kafka_connected) {
      dotKafka.className = "dot online";
      lblKafka.textContent = "CONNECTED";
    } else {
      dotKafka.className = "dot offline";
      lblKafka.textContent = "DISCONNECTED";
    }

    if (data.neo4j_connected) {
      dotNeo4j.className = "dot online";
      lblNeo4j.textContent = "CONNECTED";
    } else {
      dotNeo4j.className = "dot offline";
      lblNeo4j.textContent = "DISCONNECTED";
    }

    if (data.status === "ok") {
      dotApi.className = "dot online";
      lblApi.textContent = "OPERATIONAL";
    } else {
      dotApi.className = "dot offline";
      lblApi.textContent = "DEGRADED";
    }
  } catch (err) {
    dotKafka.className = "dot offline";
    dotNeo4j.className = "dot offline";
    dotApi.className = "dot offline";
    lblKafka.textContent = "OFFLINE";
    lblNeo4j.textContent = "OFFLINE";
    lblApi.textContent = "UNREACHABLE";
  }
}

setInterval(checkHealth, 5000);
checkHealth();

// ----------------------------------------------------
// 2. File Selection & Dropzone
// ----------------------------------------------------
btnSelectFile.addEventListener("click", () => fileInput.click());

fileInput.addEventListener("change", (e) => {
  if (e.target.files.length > 0) {
    handleFile(e.target.files[0]);
  }
});

dropzone.addEventListener("dragover", (e) => {
  e.preventDefault();
  dropzone.classList.add("dragover");
});

dropzone.addEventListener("dragleave", () => {
  dropzone.classList.remove("dragover");
});

dropzone.addEventListener("drop", (e) => {
  e.preventDefault();
  dropzone.classList.remove("dragover");
  if (e.dataTransfer.files.length > 0) {
    handleFile(e.dataTransfer.files[0]);
  }
});

function handleFile(file) {
  if (!file.name.toLowerCase().endsWith(".csv")) {
    alert("Please upload a CSV file (.csv).");
    return;
  }
  selectedFile = file;
  btnUpload.disabled = false;
  dropzone.querySelector(".dropzone-text").innerHTML = `Selected: <strong>${escapeHtml(file.name)}</strong> (${(file.size / 1024).toFixed(1)} KB)`;
  
  // Client-side CSV Preview
  const reader = new FileReader();
  reader.onload = function(e) {
    parseAndRenderPreview(e.target.result);
  };
  reader.readAsText(file);
}

function parseAndRenderPreview(csvText) {
  const lines = csvText.split(/\r\n|\n/).filter(line => line.trim() !== "");
  if (lines.length === 0) {
    previewWrapper.style.display = "none";
    return;
  }

  // Parse header
  const headers = lines[0].split(",").map(h => h.trim().replace(/^["']|["']$/g, ""));
  previewHead.innerHTML = headers.map(h => `<th>${escapeHtml(h)}</th>`).join("");

  // Parse first 5 data rows
  const previewRows = lines.slice(1, 6);
  previewBody.innerHTML = previewRows.map(rowLine => {
    const cols = rowLine.split(",").map(c => c.trim().replace(/^["']|["']$/g, ""));
    return `<tr>${cols.map(c => `<td>${escapeHtml(c)}</td>`).join("")}</tr>`;
  }).join("");

  previewWrapper.style.display = "block";
}

// ----------------------------------------------------
// 3. Upload & Ingestion Progress
// ----------------------------------------------------
btnUpload.addEventListener("click", async () => {
  if (!selectedFile) return;

  const formData = new FormData();
  formData.append("file", selectedFile);

  btnUpload.disabled = true;
  btnUpload.textContent = "Uploading...";

  try {
    const res = await fetch(`${API_BASE}/ingest`, {
      method: "POST",
      body: formData,
    });

    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || "Upload failed");
    }

    currentJobId = data.job_id;
    lblFilename.textContent = selectedFile.name;
    lblJobId.textContent = currentJobId;
    statusCard.style.display = "block";

    updateStatusUI({
      status: data.status,
      rows_total: data.rows_received,
      rows_loaded: 0,
      rows_failed: 0,
    });

    // Start polling status
    if (statusPollInterval) clearInterval(statusPollInterval);
    statusPollInterval = setInterval(pollJobStatus, 600);

  } catch (err) {
    alert(`Ingest error: ${err.message}`);
  } finally {
    btnUpload.disabled = false;
    btnUpload.innerHTML = `
      <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"></path></svg>
      Upload to Kafka
    `;
  }
});

async function pollJobStatus() {
  if (!currentJobId) return;
  try {
    const res = await fetch(`${API_BASE}/status?job_id=${currentJobId}`);
    if (!res.ok) return;
    const data = await res.json();
    updateStatusUI(data);

    if (data.status === "complete" || data.status === "failed") {
      clearInterval(statusPollInterval);
      statusPollInterval = null;
    }
  } catch (err) {
    console.error("Status poll error:", err);
  }
}

function updateStatusUI(data) {
  badgeStatus.className = `badge badge-${data.status}`;
  badgeStatus.textContent = data.status.toUpperCase();

  const total = data.rows_total || 0;
  const loaded = data.rows_loaded || 0;
  const failed = data.rows_failed || 0;
  const processed = loaded + failed;

  lblProgressCounts.textContent = `${processed} / ${total}`;
  lblRowsLoaded.textContent = loaded;
  lblRowsFailed.textContent = failed;

  const pct = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 100;
  progressBar.style.width = `${pct}%`;
}

// ----------------------------------------------------
// 4. Grounded Chat
// ----------------------------------------------------
btnSendChat.addEventListener("click", () => sendChat());
chatInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") sendChat();
});

function askPreset(q) {
  chatInput.value = q;
  sendChat();
}

async function sendChat() {
  const query = chatInput.value.trim();
  if (!query) return;

  // Append user message
  appendUserMessage(query);
  chatInput.value = "";
  btnSendChat.disabled = true;

  try {
    const res = await fetch(`${API_BASE}/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question: query }),
    });

    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || "Query error");
    }

    appendBotResponse(data);
  } catch (err) {
    appendBotResponse({
      answer: `Error: ${err.message}`,
      cypher: "NONE",
      result: [],
      grounded: false,
    });
  } finally {
    btnSendChat.disabled = false;
    chatInput.focus();
  }
}

function appendUserMessage(text) {
  const msg = document.createElement("div");
  msg.className = "chat-msg msg-user";
  msg.textContent = text;
  chatHistory.appendChild(msg);
  chatHistory.scrollTop = chatHistory.scrollHeight;
}

function appendBotResponse(data) {
  const msg = document.createElement("div");
  msg.className = "chat-msg msg-bot";

  const isGrounded = !!data.grounded;
  const badgeClass = isGrounded ? "grounded-badge true" : "grounded-badge false";
  const badgeText = isGrounded ? "✓ GROUNDED (FROM GRAPH)" : "✗ NOT GROUNDED (UNSUPPORTED/NO DATA)";

  msg.innerHTML = `
    <span class="${badgeClass}">${badgeText}</span>
    <div class="bot-answer whitespace-pre-line">${escapeHtml(data.answer)}</div>
  `;

  chatHistory.appendChild(msg);
  chatHistory.scrollTop = chatHistory.scrollHeight;
}

function escapeHtml(str) {
  if (typeof str !== "string") str = String(str);
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

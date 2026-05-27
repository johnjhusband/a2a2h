// A2A2H PWA frontend — chat client.
// On first visit with ?token=… on the URL, save token to localStorage and
// strip it from the URL. All subsequent API calls include it as Bearer.
// Live message stream via Server-Sent Events. New messages render with
// sender-coloured borders; A2A traffic shown distinctly (kind="a2a_*").

const STORAGE_KEY = "a2a2h_pwa_token";
const $status = document.getElementById("status");
const $messages = document.getElementById("messages");
const $composer = document.getElementById("composer");
const $input = document.getElementById("input");
const $enablePush = document.getElementById("enable-push");

function token() { return localStorage.getItem(STORAGE_KEY) || ""; }

function captureTokenFromUrl() {
  const url = new URL(window.location.href);
  const t = url.searchParams.get("token");
  if (t) {
    localStorage.setItem(STORAGE_KEY, t);
    url.searchParams.delete("token");
    window.history.replaceState({}, "", url.toString());
  }
}

function authHeaders(extra = {}) {
  const h = { "Content-Type": "application/json", ...extra };
  const t = token();
  if (t) h["Authorization"] = `Bearer ${t}`;
  return h;
}

function setStatus(text, isError = false) {
  $status.textContent = text;
  $status.classList.toggle("warn", isError);
}

function senderLabel(s) {
  return ({
    john: "you",
    openclaw: "OpenClaw",
    hermes: "Hermes",
    system: "system",
  })[s] || s;
}

function tsLabel(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function appendMessage(m) {
  // a2a_* rows (JSON envelopes for agent-to-agent traffic) are rendered into the DOM
  // but hidden by CSS unless the user enables the A2A toggle in the topbar.
  // The toggle controls a body class; CSS does the actual hiding (style.css).
  const el = document.createElement("div");
  el.className = "msg " + m.sender + (m.kind && m.kind.startsWith("a2a_") ? " a2a" : "");
  el.dataset.id = m.id;

  const meta = document.createElement("div");
  meta.className = "msg-meta";
  const s = document.createElement("span");
  s.className = "sender " + m.sender;
  s.textContent = senderLabel(m.sender);
  meta.appendChild(s);
  if (m.recipient) {
    const r = document.createElement("span");
    r.textContent = "→ " + senderLabel(m.recipient);
    meta.appendChild(r);
  }
  if (m.kind && m.kind !== "chat") {
    const k = document.createElement("span");
    k.textContent = m.kind;
    meta.appendChild(k);
  }
  const t = document.createElement("span");
  t.textContent = tsLabel(m.ts);
  meta.appendChild(t);

  const content = document.createElement("div");
  content.className = "msg-content";
  // For a2a_request/response, pretty-print the JSON; for chat, plain text
  let body = m.content;
  if (m.kind && m.kind.startsWith("a2a_")) {
    try { body = JSON.stringify(JSON.parse(body), null, 2); } catch (e) {}
  }
  content.textContent = body;

  el.appendChild(meta);
  el.appendChild(content);
  $messages.appendChild(el);
  $messages.scrollTop = $messages.scrollHeight;
}

let lastSeenId = 0;
async function loadHistory() {
  try {
    const r = await fetch(`/api/messages?since_id=0`, { headers: authHeaders() });
    if (r.status === 401) { setStatus("unauthorized — refresh URL with ?token=", true); return; }
    if (!r.ok) { setStatus(`history HTTP ${r.status}`, true); return; }
    const data = await r.json();
    (data.messages || []).forEach(m => { appendMessage(m); lastSeenId = Math.max(lastSeenId, m.id); });
  } catch (e) { setStatus("history fetch failed: " + e.message, true); }
}

function openStream() {
  if (!token()) { setStatus("no token — visit with ?token=…", true); return; }
  // EventSource doesn't support custom headers, so we pass token as query.
  const es = new EventSource(`/api/stream?token=${encodeURIComponent(token())}`);
  es.onopen = () => setStatus("connected");
  es.onerror = () => { setStatus("stream disconnected — reconnecting…", true); es.close(); setTimeout(openStream, 3000); };
  es.onmessage = (ev) => {
    try {
      const m = JSON.parse(ev.data);
      if (m.id <= lastSeenId) return;
      appendMessage(m); lastSeenId = m.id;
    } catch (e) {}
  };
}

$composer.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const text = $input.value.trim();
  if (!text) return;
  $input.value = "";
  try {
    const r = await fetch("/api/messages", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ text }),
    });
    if (!r.ok) {
      const err = await r.text();
      setStatus(`send failed: ${r.status} ${err}`, true);
    }
  } catch (e) { setStatus("send error: " + e.message, true); }
});

// PWA registration
if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/service-worker.js").catch(e => console.warn("SW reg failed", e));
}

// Push subscribe (best-effort — works only on supported Android browsers
// with VAPID keys configured server-side)
async function enablePush() {
  try {
    if (!("Notification" in window) || !("serviceWorker" in navigator) || !("PushManager" in window)) {
      setStatus("push not supported in this browser", true); return;
    }
    const perm = await Notification.requestPermission();
    if (perm !== "granted") { setStatus("push permission denied", true); return; }
    const reg = await navigator.serviceWorker.ready;
    const vapidResp = await fetch("/api/push/vapid_public_key", { headers: authHeaders() });
    const { public_key } = await vapidResp.json();
    if (!public_key) { setStatus("server has no VAPID keys yet", true); return; }
    const sub = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8(public_key),
    });
    await fetch("/api/push/subscribe", {
      method: "POST", headers: authHeaders(),
      body: JSON.stringify({ subscription: sub.toJSON() }),
    });
    setStatus("push enabled");
    $enablePush.hidden = true;
  } catch (e) { setStatus("push enable failed: " + e.message, true); }
}

function urlBase64ToUint8(base64) {
  const padding = "=".repeat((4 - (base64.length % 4)) % 4);
  const b64 = (base64 + padding).replace(/-/g, "+").replace(/_/g, "/");
  const raw = atob(b64);
  const arr = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) arr[i] = raw.charCodeAt(i);
  return arr;
}

$enablePush.addEventListener("click", enablePush);

// ─── Toggles ─────────────────────────────────────────────────────────────
// Two independent body classes drive visibility (style.css does the hiding):
//   .show-a2a   — render a2a_request / a2a_response rows at all
//   .show-json  — within visible a2a rows, also render the JSON body
// State persists in localStorage so the user's preference survives reloads.
const $toggleA2A = document.getElementById("toggle-a2a");
const $toggleJSON = document.getElementById("toggle-json");

function applyToggle(name, on) {
  document.body.classList.toggle("show-" + name, on);
  localStorage.setItem("pwa-show-" + name, on ? "1" : "0");
}

function initToggle($el, name) {
  const on = localStorage.getItem("pwa-show-" + name) === "1";
  $el.checked = on;
  applyToggle(name, on);
  $el.addEventListener("change", () => applyToggle(name, $el.checked));
}

initToggle($toggleA2A, "a2a");
initToggle($toggleJSON, "json");

// Boot
captureTokenFromUrl();
if (!token()) { setStatus("no token saved — visit with ?token=… once", true); }
else {
  loadHistory().then(openStream);
  if ("Notification" in window && Notification.permission === "default") $enablePush.hidden = false;
}

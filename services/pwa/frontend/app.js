// A2A2H PWA frontend — chat client.
// Auth uses a Secure/HttpOnly session cookie set by the backend bootstrap redirect.
// API calls rely on same-origin cookies; no token is stored in JavaScript.
// Live message stream via Server-Sent Events. New messages render with
// sender-coloured borders; A2A traffic shown distinctly (kind="a2a_*").

const $status = document.getElementById("status");
const $messages = document.getElementById("messages");
const $composer = document.getElementById("composer");
const $input = document.getElementById("input");
const $enablePush = document.getElementById("enable-push");

function authHeaders(extra = {}) {
  return { "Content-Type": "application/json", ...extra };
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

function summarizeText(value, max = 280) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return text.length > max ? text.slice(0, max - 1).trimEnd() + "…" : text;
}

function a2aSummary(m) {
  let payload = {};
  try { payload = JSON.parse(m.content || "{}"); } catch (e) {}
  const capability = payload.capability || payload.task || payload.task_id || "agent coordination";
  const findings = payload.findings || payload.summary || payload.result || payload.error || payload.status || payload.success_criteria || "coordination event recorded";
  let raw = m.content || "";
  try { raw = JSON.stringify(payload, null, 2); } catch (e) {}
  return { capability: summarizeText(capability, 160), findings: summarizeText(findings, 360), raw };
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
  if (m.kind && m.kind.startsWith("a2a_")) {
    const summary = a2aSummary(m);
    const capability = document.createElement("div");
    capability.className = "a2a-capability";
    capability.textContent = "Capability: " + summary.capability;
    const findings = document.createElement("div");
    findings.className = "a2a-findings";
    findings.textContent = summary.findings;
    const raw = document.createElement("details");
    raw.className = "a2a-raw";
    const rawLabel = document.createElement("summary");
    rawLabel.textContent = "Raw JSON";
    const pre = document.createElement("pre");
    pre.textContent = summary.raw;
    raw.appendChild(rawLabel);
    raw.appendChild(pre);
    content.appendChild(capability);
    content.appendChild(findings);
    content.appendChild(raw);
  } else {
    content.textContent = m.content;
  }

  el.appendChild(meta);
  el.appendChild(content);
  $messages.appendChild(el);
  $messages.scrollTop = $messages.scrollHeight;
}

let lastSeenId = 0;
let historyInFlight = false;
async function loadHistory({ replace = false } = {}) {
  if (historyInFlight) return;
  historyInFlight = true;
  try {
    const r = await fetch(`/api/messages?since_id=0`, { headers: authHeaders() });
    if (r.status === 401) { setStatus("unauthorized — open the private login URL again", true); return; }
    if (!r.ok) { setStatus(`history HTTP ${r.status}`, true); return; }
    const data = await r.json();
    if (replace) {
      $messages.textContent = "";
      lastSeenId = 0;
    }
    (data.messages || []).forEach(m => {
      if (!replace && m.id <= lastSeenId) return;
      appendMessage(m);
      lastSeenId = Math.max(lastSeenId, m.id);
    });
    setStatus("history synced");
  } catch (e) { setStatus("history fetch failed: " + e.message, true); }
  finally { historyInFlight = false; }
}

function reloadFullHistory() {
  // Foregrounded mobile PWAs can have a dead EventSource and stale in-memory
  // DOM. Re-read the canonical chat.db mirror through /api/messages?since_id=0.
  return loadHistory({ replace: true });
}

function openStream() {
  const es = new EventSource("/api/stream");
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

// Push subscribe + self-test (best-effort — works only on browsers that support
// Web Push with VAPID keys configured server-side). The button stays visible so
// John can verify the path on demand instead of guessing whether push works.
async function enablePush() {
  try {
    if (!("Notification" in window) || !("serviceWorker" in navigator) || !("PushManager" in window)) {
      setStatus("push not supported in this browser", true); return;
    }
    setStatus("checking push…");
    const perm = await Notification.requestPermission();
    if (perm !== "granted") { setStatus("push permission denied", true); return; }
    const reg = await navigator.serviceWorker.ready;
    const vapidResp = await fetch("/api/push/vapid_public_key", { headers: authHeaders() });
    const { public_key } = await vapidResp.json();
    if (!public_key) { setStatus("server has no VAPID keys yet", true); return; }
    const sub = await reg.pushManager.getSubscription() || await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8(public_key),
    });
    const subscribeResp = await fetch("/api/push/subscribe", {
      method: "POST", headers: authHeaders(),
      body: JSON.stringify({ subscription: sub.toJSON() }),
    });
    if (!subscribeResp.ok) { setStatus(`push subscribe failed: ${subscribeResp.status}`, true); return; }
    const testResp = await fetch("/api/push/test", { method: "POST", headers: authHeaders(), body: "{}" });
    if (!testResp.ok) { setStatus(`push test failed: ${testResp.status}`, true); return; }
    const test = await testResp.json();
    if (test.attempted > 0 && test.failed === 0) setStatus("push test sent — watch for the notification");
    else if (test.attempted > 0) setStatus(`push test attempted with ${test.failed} failure(s)`, true);
    else setStatus("push subscribed, but server did not attempt delivery", true);
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

// iOS/Android can kill EventSource while a PWA is backgrounded. On foreground,
// reload the canonical chat.db history so missed messages appear even if the
// live stream never delivered them to this page instance.
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") reloadFullHistory();
});
window.addEventListener("focus", reloadFullHistory);

// ─── Toggles ─────────────────────────────────────────────────────────────
// The coordination toggle adds .show-a2a to the body. A2A rows stay hidden by
// default; when shown, each row includes a short summary plus collapsible JSON.
const $toggleA2A = document.getElementById("toggle-a2a");

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

// Boot
loadHistory().then(openStream);

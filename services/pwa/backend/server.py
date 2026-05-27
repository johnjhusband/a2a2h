#!/usr/bin/env python3
"""
PWA backend — bridges the browser to OpenClaw and Hermes.

Serves the PWA frontend at /, plus a JSON API:

  GET  /api/messages?since_id=N  → message history
  POST /api/messages              → send message from human user (routes by @-mention)
  GET  /api/stream                → Server-Sent Events stream of new messages
  POST /api/push/subscribe        → register browser for Web Push (subscription stored)
  GET  /api/push/vapid_public_key → return server's VAPID public key
  GET  /api/health                → health check

Auth: HttpOnly session cookie. Token is `PWA_AUTH_TOKEN` set at install
time. A first visit may include `?token=...` only on the PWA shell route; the
server immediately exchanges it for a Secure/HttpOnly cookie and redirects to
a clean URL. API endpoints never accept URL query tokens.

Routing of the human user's outbound messages:
  - Starts with "@hermes " (case-insensitive) → POST to Hermes A2A sidecar
  - Starts with "@openclaw " → run the real OpenClaw agent via the gateway-backed
    CLI session (not a raw model/capability fallback)
  - Starts with "@both " → coordinated two-step flow: OpenClaw strategy first,
    then Hermes implementation after a scoped handoff. This intentionally avoids
    uncontrolled parallel replies from both agents.
  - No @-mention → small content-aware router: Hermes-targeted language goes to
    Hermes, greetings/both-hemisphere prompts use the same coordinated @both
    flow, everything else defaults to OpenClaw as orchestrator.

Implementation: pure stdlib http.server + threading. SSE via long-running
generator response. Long-job intent is handed to a detached repo-backed runner
because OpenClaw TaskFlow writes are currently an internal plugin/runtime API,
not a first-class CLI/HTTP primitive available to this Python bridge. Push
notifications stored as DB rows for now (sending push requires a small library —
wired via `pywebpush` if installed in the same venv as this server, otherwise
gracefully degrades to no-push).
"""
from __future__ import annotations
import ast
import hmac
import json
import os
import queue
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

# Path is /opt/a2a2h/services/pwa/backend/server.py — add /opt/a2a2h/services so
# we can import the `chat` package alongside the sidecars (matching their style).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from chat.db import append, tail, log_a2a_request, log_a2a_response, clone_chat_isolation_error  # noqa: E402

# ─── Config ─────────────────────────────────────────────────────────────────

PORT = int(os.environ.get("PWA_PORT", "8088"))
BIND = os.environ.get("PWA_BIND", "127.0.0.1")  # Caddy reverse-proxies to this
PWA_AUTH_TOKEN = os.environ.get("PWA_AUTH_TOKEN", "")
FRONTEND_DIR = Path(os.environ.get("PWA_FRONTEND", str(Path(__file__).resolve().parent.parent / "frontend")))

HERMES_A2A_URL = os.environ.get("HERMES_A2A_URL", "http://127.0.0.1:8643/a2a/")
HERMES_A2A_TOKEN = os.environ.get("HERMES_A2A_TOKEN", "")
HERMES_SEND_TIMEOUT_S = int(os.environ.get("HERMES_SEND_TIMEOUT_S", "660"))

OPENCLAW_MODEL = os.environ.get("OPENCLAW_MODEL", "openai-codex/gpt-5.5")
# Stable session id keeps OpenClaw's prompt cache warm across PWA turns and preserves
# conversation continuity. Single-user assumption; if multi-user comes, derive per user.
OPENCLAW_SESSION_ID = os.environ.get("OPENCLAW_SESSION_ID", "pwa-john-main")
# OpenClaw often has to inspect the A2A2H repo or delegate work. The PWA already
# returns 202 immediately and delivers via SSE, so give the background worker a
# long-task budget instead of killing real A2A2H work at the old interactive limit.
OPENCLAW_AGENT_TIMEOUT_S = int(os.environ.get("OPENCLAW_AGENT_TIMEOUT_S", "900"))
OPENCLAW_SUBPROCESS_TIMEOUT_S = int(os.environ.get("OPENCLAW_SUBPROCESS_TIMEOUT_S", str(OPENCLAW_AGENT_TIMEOUT_S + 30)))

HUMAN_CHAT_STYLE = (
    "Audience: a human user in the PWA chat. Reply in plain conversational English. "
    "Do not return JSON, YAML, markdown tables, schema blocks, or agent findings. "
    "Avoid bullet lists unless John explicitly asks for a list. If you used tools or "
    "delegated work, summarize the result naturally."
)

VAPID_PUBLIC_KEY_FILE = Path(os.environ.get("VAPID_PUBLIC_KEY_FILE", "/opt/a2a2h/.vapid/public.pem"))
VAPID_PRIVATE_KEY_FILE = Path(os.environ.get("VAPID_PRIVATE_KEY_FILE", "/opt/a2a2h/.vapid/private.pem"))
VAPID_EMAIL = os.environ.get("VAPID_EMAIL", "mailto:admin@example.com")

PWA_JOB_PAYLOAD_DIR = Path(os.environ.get("PWA_JOB_PAYLOAD_DIR", "/opt/a2a2h/.cache/pwa-jobs/payloads"))
PWA_JOB_RUNNER = Path(os.environ.get("PWA_JOB_RUNNER", str(Path(__file__).resolve().parent / "job_runner.py")))
A2A2H_ROOT = os.environ.get("A2A2H_ROOT", "/opt/a2a2h")
A2A2H_INSTANCE_ID = os.environ.get("A2A2H_INSTANCE_ID", "production")
CHAT_DB_PATH = os.environ.get("CHAT_DB", "/opt/a2a2h/chat.db")


def _clone_chat_isolation_error(*, instance_id: str, chat_db: str, a2a2h_root: str) -> str | None:
    return clone_chat_isolation_error(instance_id=instance_id, chat_db=chat_db, a2a2h_root=a2a2h_root)


def _assert_clone_chat_isolation() -> None:
    error = _clone_chat_isolation_error(instance_id=A2A2H_INSTANCE_ID, chat_db=CHAT_DB_PATH, a2a2h_root=A2A2H_ROOT)
    if error:
        raise RuntimeError(error)


_assert_clone_chat_isolation()

LONG_JOB_RE = re.compile(
    r"\b(implement|build|create|add|wire|install|upgrade|deploy|research|audit|investigate|diagnos(?:e|is)|debug|fix|repair|patch|refactor|run\s+tests?|test|document|analy[sz]e|background|long[-\s]?running|report\s+back|when\s+(you('|’)re|you\s+are)\s+done)\b",
    re.IGNORECASE,
)
MULTI_STEP_RE = re.compile(r"\b(first|then|after\s+that|finally|multi[-\s]?step|end[-\s]?to[-\s]?end)\b", re.IGNORECASE)


# ─── SSE broadcaster ────────────────────────────────────────────────────────

class SSEBroadcaster:
    """Fan-out new chat messages to all connected SSE clients."""
    def __init__(self):
        self._subs: list[queue.Queue] = []
        self._lock = threading.Lock()
        self._poller = threading.Thread(target=self._poll, daemon=True)
        self._last_id = 0
        self._poller.start()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=100)
        with self._lock:
            self._subs.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            try:
                self._subs.remove(q)
            except ValueError:
                pass

    def _broadcast(self, msg: dict) -> None:
        with self._lock:
            dead = []
            for q in self._subs:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                try:
                    self._subs.remove(q)
                except ValueError:
                    pass

    def _poll(self) -> None:
        # Re-init last_id from DB so we don't replay old messages
        try:
            existing = tail(0, 1)
            if existing:
                self._last_id = existing[-1]["id"]
        except Exception:
            pass
        while True:
            try:
                rows = tail(self._last_id, 200)
                for row in rows:
                    self._broadcast(row)
                    self._last_id = max(self._last_id, row["id"])
            except Exception:
                pass
            time.sleep(0.5)

BROADCASTER = SSEBroadcaster()

# ─── Routing logic ──────────────────────────────────────────────────────────

MENTION_RE = re.compile(r"^\s*@(openclaw|hermes|both)\b\s*", re.IGNORECASE)
MENTION_ANY_RE = re.compile(r"@(openclaw|hermes|both)\b", re.IGNORECASE)
GREETING_RE = re.compile(r"^\s*(hi|hello|hey|yo|gm|good\s+(morning|afternoon|evening))\b[\s!.?]*$", re.IGNORECASE)
HERMES_ADDRESS_RE = re.compile(r"\b(hermes|right\s+hemisphere)\b", re.IGNORECASE)
OPENCLAW_ADDRESS_RE = re.compile(r"\b(openclaw|left\s+hemisphere)\b", re.IGNORECASE)
BOTH_ADDRESS_RE = re.compile(r"\b(both\s+(of\s+you|agents|hemispheres)|you\s+both|openclaw\s+and\s+hermes|hermes\s+and\s+openclaw)\b", re.IGNORECASE)
OPENCLAW_ORCHESTRATION_RE = re.compile(
    r"\b(fix|debug|repair|diagnos(e|is)|investigate|patch|restart|deploy|wire|route|why\s+(did|is|are|was|were)|what\s+happened)\b",
    re.IGNORECASE,
)

_HUMAN_FIELD_PRIORITY = (
    "reply", "answer", "message", "response", "simplified_response", "final",
    "summary", "findings", "content", "text",
)


def _strip_json_fence(text: str) -> str:
    """Return fenced JSON content without markdown fences when present."""
    stripped = text.strip()
    fence = re.fullmatch(r"```(?:json|JSON)?\s*(.*?)\s*```", stripped, re.DOTALL)
    if fence:
        return fence.group(1).strip()
    return stripped


def _select_human_value(value: Any) -> Any:
    """Recursively choose the most human-facing value from model JSON output."""
    if isinstance(value, dict):
        for key in _HUMAN_FIELD_PRIORITY:
            if key in value and value[key] not in (None, "", [], {}):
                return _select_human_value(value[key])
        # Common OpenAI-ish shape, if a raw provider response leaks through.
        choices = value.get("choices")
        if isinstance(choices, list) and choices:
            return _select_human_value(choices[0])
        if len(value) == 1:
            return _select_human_value(next(iter(value.values())))
        return value
    if isinstance(value, list):
        if len(value) == 1:
            return _select_human_value(value[0])
        return [_select_human_value(item) for item in value]
    return value


def _render_human_value(value: Any) -> str:
    """Render an unwrapped JSON value as concise chat text."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)) or value is None:
        return "" if value is None else str(value)
    if isinstance(value, list):
        parts = [_render_human_value(item) for item in value]
        parts = [part for part in parts if part]
        if not parts:
            return ""
        if all("\n" not in part and len(part) < 160 for part in parts):
            return " ".join(parts)
        return "\n".join(parts)
    if isinstance(value, dict):
        rendered_parts: list[str] = []
        for key in _HUMAN_FIELD_PRIORITY:
            if key in value:
                part = _render_human_value(_select_human_value(value[key]))
                if part:
                    rendered_parts.append(part)
        if rendered_parts:
            return " ".join(dict.fromkeys(rendered_parts))
        simple_items = []
        for key, item in value.items():
            if isinstance(item, (str, int, float, bool)) and str(item).strip():
                simple_items.append(f"{key}: {item}")
        if simple_items and len(simple_items) <= 3:
            return "; ".join(simple_items)
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def _humanize_chat_content(text: str) -> str:
    """Safety net for kind='chat': unwrap obvious JSON/dict replies for John."""
    stripped = (text or "").strip()
    if not stripped:
        return ""
    candidate = _strip_json_fence(stripped)
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(candidate)
        except (ValueError, SyntaxError):
            return stripped
        if not isinstance(parsed, (dict, list, str, int, float, bool, type(None))):
            return stripped
    selected = _select_human_value(parsed)
    rendered = _render_human_value(selected).strip()
    return rendered or stripped

def parse_mention(text: str) -> tuple[str, str]:
    """Return (target, stripped_text). Target is 'openclaw', 'hermes', or 'both'."""
    m = MENTION_RE.match(text)
    if m:
        target = m.group(1).lower()
        stripped = text[m.end():].strip()
        mentioned = {item.lower() for item in MENTION_ANY_RE.findall(text)}
        # John may explicitly sequence the two hemispheres in one turn, e.g.
        # "@openclaw start with strategy; @hermes implement after". Treat that
        # as coordinated @both, never as two uncontrolled parallel deliveries.
        if target != "both" and {"openclaw", "hermes"}.issubset(mentioned):
            return "both", stripped
        return target, stripped
    # Content-aware no-mention routing. Keep this deterministic and conservative:
    # OpenClaw remains the default orchestrator for repairs, decisions, and ambiguous
    # work; Hermes receives messages that are clearly addressed to Hermes; greetings
    # are sent to both so John can immediately see whether both routes are alive.
    if GREETING_RE.match(text) or BOTH_ADDRESS_RE.search(text):
        return "both", text
    if HERMES_ADDRESS_RE.search(text):
        if OPENCLAW_ADDRESS_RE.search(text) or OPENCLAW_ORCHESTRATION_RE.search(text):
            return "openclaw", text
        return "hermes", text
    if OPENCLAW_ADDRESS_RE.search(text):
        return "openclaw", text
    return "openclaw", text  # default: OpenClaw is the left-hemisphere router/orchestrator


def _is_long_job_intent(text: str) -> bool:
    """Conservative heuristic for turns that should survive the PWA process.

    OpenClaw's internal TaskFlow API is not exposed to this Python bridge as a
    first-class CLI/HTTP write surface, so PWA long jobs use a detached local
    runner. Keep greetings and short Q&A on the existing lightweight path; route
    repo/infrastructure/research work to durable execution.
    """
    words = re.findall(r"\S+", text or "")
    if LONG_JOB_RE.search(text or "") and (len(words) >= 4 or MULTI_STEP_RE.search(text or "")):
        return True
    if len(words) >= 35 and (LONG_JOB_RE.search(text or "") or MULTI_STEP_RE.search(text or "")):
        return True
    return False


def _start_background_chat_job(*, target: str, message: str) -> tuple[bool, str, Optional[str]]:
    """Persist a PWA long-job payload and start the detached repo-backed runner."""
    job_id = f"pwa-bg-{uuid.uuid4().hex[:12]}"
    try:
        PWA_JOB_PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
        payload_path = PWA_JOB_PAYLOAD_DIR / f"{job_id}.json"
        payload_path.write_text(json.dumps({"job_id": job_id, "target": target, "message": message}))
        try:
            payload_path.chmod(0o600)
        except OSError:
            pass
        log_path = PWA_JOB_PAYLOAD_DIR.parent / f"{job_id}.log"
        log_fh = log_path.open("ab", buffering=0)
        try:
            subprocess.Popen(
                [sys.executable, str(PWA_JOB_RUNNER), "--job-id", job_id, "--payload", str(payload_path)],
                cwd="/opt/a2a2h",
                stdin=subprocess.DEVNULL,
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
                env={
                    **os.environ,
                    "HOME": os.environ.get("HOME", "/home/a2a2h"),
                    # Keep long jobs off the live PWA chat session so John can
                    # continue short turns while a background run is active.
                    "OPENCLAW_SESSION_ID": f"{OPENCLAW_SESSION_ID}-{job_id}",
                },
            )
        finally:
            log_fh.close()
        return True, job_id, None
    except Exception as exc:
        return False, job_id, repr(exc)

def send_to_hermes(
    text: str,
    task_id: Optional[str] = None,
    *,
    sender: str = "john",
    capability: str = "direct-instruction-from-user",
    inputs: Optional[dict] = None,
    success_criteria: str = "respond to John in plain conversational English, not JSON or structured findings",
) -> dict:
    """Send a message to Hermes via the A2A sidecar.

    Direct John turns use the human Hermes session. Coordinated @both handoffs use
    sender=openclaw and audience=agent so Hermes works as implementer, not as a
    second independent human-facing responder.
    """
    task_id = task_id or str(uuid.uuid4())
    payload_inputs = inputs if inputs is not None else {"message": text, "audience": "human", "response_style": HUMAN_CHAT_STYLE}
    body = json.dumps({
        "task_id": task_id,
        "sender": sender,
        "capability": capability,
        "inputs": payload_inputs,
        "success_criteria": success_criteria,
    }).encode("utf-8")
    req = urllib.request.Request(
        HERMES_A2A_URL, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {HERMES_A2A_TOKEN}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=HERMES_SEND_TIMEOUT_S) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return {"ok": True, "task_id": task_id, "findings": payload.get("findings", "")}
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read().decode("utf-8"))
        except Exception:
            payload = {}
        status = payload.get("status") if isinstance(payload, dict) else None
        if e.code == 504 or status == "timeout":
            return {"ok": False, "error": f"Hermes is still working or timed out after {HERMES_SEND_TIMEOUT_S}s; please retry or ask for a smaller step.", "task_id": task_id}
        detail = payload.get("error") if isinstance(payload, dict) else None
        return {"ok": False, "error": detail or f"Hermes HTTP {e.code}", "task_id": task_id}
    except Exception as e:
        return {"ok": False, "error": repr(e), "task_id": task_id}

def _extract_json_object(raw: str) -> Optional[dict]:
    """OpenClaw may print logs around --json output; parse the first JSON object."""
    decoder = json.JSONDecoder()
    starts = [i for i, ch in enumerate(raw) if ch == "{"]
    for idx in starts:
        try:
            candidate, _end = decoder.raw_decode(raw[idx:])
            if isinstance(candidate, dict):
                return candidate
        except json.JSONDecodeError:
            continue
    return None


def _extract_openclaw_reply(payload: dict) -> str:
    """Return the assistant-visible text from OpenClaw's `agent --json` payload.

    The 2026.5.7 structure is:
        {"status": "ok", "summary": "completed",
         "result": {"payloads": [{"text": "..."}], "meta": {...}}}

    Older versions had `payloads` and `meta` at the top level. Handle both.
    Falls back to a JSON dump only as a last resort.
    """
    # Look at result.* first (current shape), fall through to top-level (legacy).
    for root in (payload.get("result") if isinstance(payload.get("result"), dict) else None, payload):
        if not isinstance(root, dict):
            continue
        meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
        for key in ("finalAssistantVisibleText", "finalAssistantRawText"):
            value = meta.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        payloads = root.get("payloads")
        if isinstance(payloads, list):
            parts = []
            for item in payloads:
                if isinstance(item, dict) and isinstance(item.get("text"), str) and item["text"].strip():
                    parts.append(item["text"].strip())
            if parts:
                return "\n".join(parts)
    return json.dumps(payload)


def send_to_openclaw(text: str) -> dict:
    """Full agent turn through OpenClaw via the running gateway.

    Routes to `openclaw agent` (gateway transport) with a stable session id so
    SOUL.md / AGENTS.md / IDENTITY.md / skills / memory are loaded and the prompt
    cache stays warm across PWA turns. The earlier HTTP and model.run paths were
    removed — they were bare LLM completions, not agent runs (see A2A2H-DECISION-013
    follow-up: PWA OpenClaw routing fix).
    """
    human_message = f"{HUMAN_CHAT_STYLE}\n\nJohn says: {text}"
    # Don't pass --model: the gateway rejects per-caller model overrides with
    # "GatewayClientRequestError: provider/model overrides are not authorized
    # for this caller." The agent uses agents.defaults.model.primary from
    # openclaw.json (openai-codex/gpt-5.5) which is what we want anyway.
    cmd = [
        "openclaw", "agent",
        "--agent", "main",
        "--session-id", OPENCLAW_SESSION_ID,
        "--message", human_message,
        "--thinking", "off",
        "--json",
        "--timeout", str(OPENCLAW_AGENT_TIMEOUT_S),
    ]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=OPENCLAW_SUBPROCESS_TIMEOUT_S,
            cwd="/opt/a2a2h",
            env={**os.environ, "HOME": os.environ.get("HOME", "/home/a2a2h")},
        )
        combined = (r.stdout or "") + ("\n" + r.stderr if r.stderr else "")
        if r.returncode != 0:
            return {"ok": False, "error": combined.strip() or f"openclaw exited {r.returncode}"}
        payload = _extract_json_object(combined)
        if not payload:
            return {"ok": False, "error": f"openclaw returned no parseable JSON: {combined[-1000:]}"}
        return {"ok": True, "reply": _extract_openclaw_reply(payload), "session_id": OPENCLAW_SESSION_ID}
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "timeout": True,
            "error": (
                f"OpenClaw is still working or exceeded the PWA background budget "
                f"after {OPENCLAW_SUBPROCESS_TIMEOUT_S}s. Break the request into a "
                "smaller step or run it as an explicit long-running task."
            ),
        }
    except Exception as e:
        return {"ok": False, "error": repr(e)}


def send_coordinated_both(text: str) -> dict:
    """Run @both as OpenClaw strategy first, Hermes implementation second.

    This is deliberately sequential. Hermes receives OpenClaw's reply as the
    scoped handoff and returns structured implementation findings; it is not
    invoked in parallel and is not treated as an independent human-chat reply.
    """
    coordination_id = f"coord-{uuid.uuid4().hex[:12]}"
    append(sender="system", recipient="john", kind="system_event", correlation=coordination_id,
           content=json.dumps({"event": "coordinated_both_started", "owner": "openclaw", "phase": "strategy"}))

    openclaw_result = send_to_openclaw(
        "Coordinated @both request. You own strategy and routing authority. "
        "Reply first with the strategy/architecture and, if Hermes should implement, "
        "include a clear scoped handoff for Hermes. John says: " + text
    )
    if not openclaw_result.get("ok"):
        return {"ok": False, "coordination_id": coordination_id, "phase": "openclaw_strategy", "error": openclaw_result.get("error")}

    openclaw_reply = _humanize_chat_content(openclaw_result.get("reply", ""))
    append(sender="openclaw", recipient="john", kind="chat", correlation=coordination_id,
           content=openclaw_reply)

    append(sender="system", recipient="john", kind="system_event", correlation=coordination_id,
           content=json.dumps({"event": "coordinated_both_handoff", "owner": "hermes", "phase": "implementation"}))
    hermes_result = send_to_hermes(
        text,
        task_id=f"{coordination_id}-hermes",
        sender="openclaw",
        capability="coordinated-pwa-handoff-implementation",
        inputs={
            "message": text,
            "audience": "agent",
            "openclaw_strategy_and_handoff": openclaw_reply,
            "routing_contract": (
                "This is a coordinated @both flow. OpenClaw has spoken first and remains decider. "
                "Implement only the scoped handoff. Return bounded structured findings as data."
            ),
        },
        success_criteria="Hermes implements or validates the scoped handoff and returns bounded structured findings for OpenClaw/John.",
    )
    if hermes_result.get("ok"):
        append(sender="hermes", recipient="john", kind="chat", correlation=coordination_id,
               content=_humanize_chat_content(hermes_result.get("findings", "")))
    else:
        append(sender="system", recipient="john", kind="system_event", correlation=coordination_id,
               content=json.dumps({"event": "hermes_coordinated_handoff_failed", "error": hermes_result.get("error")}))

    return {"ok": True, "coordination_id": coordination_id, "openclaw": openclaw_result, "hermes": hermes_result}

# ─── HTTP handler ───────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    server_version = "CtoPWA/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    # Helpers
    def _json(self, status: int, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path: Path, content_type: str):
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            self._json(404, {"error": "not_found"}); return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    SESSION_COOKIE_NAME = "a2a2h_pwa_session"

    def _cookie_value(self, name: str) -> str:
        raw = self.headers.get("Cookie", "")
        for part in raw.split(";"):
            if "=" not in part:
                continue
            key, value = part.strip().split("=", 1)
            if key == name:
                return urllib.parse.unquote(value)
        return ""

    def _query_param(self, name: str) -> str:
        parsed = urllib.parse.urlsplit(self.path)
        values = urllib.parse.parse_qs(parsed.query, keep_blank_values=True).get(name, [])
        return values[0] if values else ""

    def _session_cookie_header(self) -> str:
        return (
            f"{self.SESSION_COOKIE_NAME}={urllib.parse.quote(PWA_AUTH_TOKEN, safe='')}; "
            "Path=/; HttpOnly; Secure; SameSite=Strict"
        )

    def _auth_ok(self) -> bool:
        if not PWA_AUTH_TOKEN:
            return True  # auth disabled (dev mode)
        cookie_token = self._cookie_value(self.SESSION_COOKIE_NAME)
        return bool(cookie_token) and hmac.compare_digest(cookie_token, PWA_AUTH_TOKEN)

    def _maybe_bootstrap_session(self, path: str) -> bool:
        """Exchange ?token= for a session cookie only on PWA shell routes."""
        if not PWA_AUTH_TOKEN or path not in ("/", "/index.html"):
            return False
        supplied = self._query_param("token")
        if not supplied:
            return False
        if not hmac.compare_digest(supplied, PWA_AUTH_TOKEN):
            return False

        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        clean_query = urllib.parse.urlencode([(k, v) for k, v in query if k != "token"])
        location = urllib.parse.urlunsplit(("", "", parsed.path or "/", clean_query, parsed.fragment))
        self.send_response(303)
        self.send_header("Location", location or "/")
        self.send_header("Set-Cookie", self._session_cookie_header())
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        return True

    # Routes
    # Paths served WITHOUT auth — the PWA shell must bootstrap before the JS
    # can attach the Bearer token. Static assets contain no secrets.
    _PUBLIC_GET_EXACT = ("/", "/index.html", "/manifest.json", "/service-worker.js", "/api/health")
    _PUBLIC_GET_PREFIX = ("/static/",)

    def _is_public_get(self, path: str) -> bool:
        if path in self._PUBLIC_GET_EXACT: return True
        return any(path.startswith(p) for p in self._PUBLIC_GET_PREFIX)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if self._maybe_bootstrap_session(path):
            return
        if path == "/api/health":
            return self._json(200, {"status": "ok", "service": "pwa-backend"})
        # Public paths skip auth; API paths still require it
        if not self._is_public_get(path) and not self._auth_ok():
            return self._json(401, {"error": "unauthorized"})
        if path == "/api/messages":
            qs = self.path.split("?", 1)[-1] if "?" in self.path else ""
            since_id = 0
            for kv in qs.split("&"):
                if kv.startswith("since_id="):
                    try: since_id = int(kv[len("since_id="):])
                    except ValueError: pass
            return self._json(200, {"messages": tail(since_id, 500)})
        if path == "/api/stream":
            return self._sse_stream()
        if path == "/api/push/vapid_public_key":
            if VAPID_PUBLIC_KEY_FILE.exists():
                return self._json(200, {"public_key": VAPID_PUBLIC_KEY_FILE.read_text().strip()})
            return self._json(200, {"public_key": None, "note": "VAPID keys not yet generated"})
        if path == "/" or path == "/index.html":
            return self._file(FRONTEND_DIR / "index.html", "text/html; charset=utf-8")
        if path == "/manifest.json":
            return self._file(FRONTEND_DIR / "manifest.json", "application/manifest+json")
        if path == "/service-worker.js":
            return self._file(FRONTEND_DIR / "service-worker.js", "application/javascript")
        if path.startswith("/static/"):
            sub = path[len("/static/"):].lstrip("/")
            target = (FRONTEND_DIR / sub).resolve()
            try: target.relative_to(FRONTEND_DIR.resolve())
            except ValueError: return self._json(403, {"error": "forbidden"})
            ct = "text/javascript" if sub.endswith(".js") else \
                 "text/css" if sub.endswith(".css") else \
                 "image/png" if sub.endswith(".png") else "application/octet-stream"
            return self._file(target, ct)
        return self._json(404, {"error": "not_found"})

    def do_POST(self):
        if not self._auth_ok():
            return self._json(401, {"error": "unauthorized"})
        path = self.path.split("?", 1)[0]
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return self._json(400, {"error": "invalid_json"})

        if path == "/api/messages":
            return self._handle_message_post(body)
        if path == "/api/push/subscribe":
            sub = body.get("subscription")
            if not sub:
                return self._json(400, {"error": "subscription_missing"})
            append(sender="system", kind="system_event",
                   content=json.dumps({"event": "push_subscribed", "endpoint_host":
                                       (sub.get("endpoint","") or "")[:60]}))
            # Persist subscription
            sub_dir = Path("/opt/a2a2h/.cache/push-subscriptions"); sub_dir.mkdir(parents=True, exist_ok=True)
            fname = sub_dir / (uuid.uuid4().hex + ".json")
            fname.write_text(json.dumps(sub))
            return self._json(200, {"ok": True})
        return self._json(404, {"error": "not_found"})

    def _handle_message_post(self, body: dict):
        text = (body.get("text") or "").strip()
        if not text:
            return self._json(400, {"error": "empty_message"})

        # Persist the human user's message for chat history
        target, stripped = parse_mention(text)
        append(sender="john", recipient=target, kind="chat", content=text)

        msg = stripped or text
        if _is_long_job_intent(msg):
            ok, job_id, error = _start_background_chat_job(target=target, message=msg)
            if ok:
                append(
                    sender="openclaw",
                    recipient="john",
                    kind="chat",
                    correlation=job_id,
                    content=(
                        f"I’m starting that as background job {job_id}. "
                        "I’ll post the final result here when it finishes."
                    ),
                )
                return self._json(202, {"accepted": True, "target": target, "background": True, "job_id": job_id})
            append(
                sender="system",
                recipient="john",
                kind="system_event",
                correlation=job_id,
                content=json.dumps({"event": "pwa_background_job_start_failed", "job_id": job_id, "error": error}),
            )
            # Fall through to the in-process worker if detached start fails.

        # Spawn worker so we can return 202 immediately and let SSE deliver the reply
        def worker():
            def deliver_hermes():
                r = send_to_hermes(msg)
                if r.get("ok"):
                    append(sender="hermes", recipient="john", kind="chat",
                           content=_humanize_chat_content(r.get("findings", "")))
                else:
                    append(sender="system", recipient="john", kind="system_event",
                           content=json.dumps({"event": "hermes_send_timeout" if "timed out" in (r.get("error") or "") else "hermes_send_failed", "error": r.get("error")}))

            def deliver_openclaw():
                r = send_to_openclaw(msg)
                if r.get("ok"):
                    append(sender="openclaw", recipient="john", kind="chat",
                           content=_humanize_chat_content(r.get("reply", "")))
                else:
                    append(sender="system", recipient="john", kind="system_event",
                           content=json.dumps({"event": "openclaw_send_timeout" if r.get("timeout") else "openclaw_send_failed", "error": r.get("error")}))

            if target == "hermes":
                deliver_hermes()
            elif target == "both":
                r = send_coordinated_both(msg)
                if not r.get("ok"):
                    append(sender="system", recipient="john", kind="system_event",
                           content=json.dumps({"event": "coordinated_both_failed", "phase": r.get("phase"), "error": r.get("error")}))
            else:  # openclaw or default
                deliver_openclaw()

        threading.Thread(target=worker, daemon=True).start()
        return self._json(202, {"accepted": True, "target": target})

    def _sse_stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        q = BROADCASTER.subscribe()
        try:
            # Send a comment to start the stream
            self.wfile.write(b": connected\n\n"); self.wfile.flush()
            while True:
                try:
                    msg = q.get(timeout=15)
                    data = json.dumps(msg)
                    self.wfile.write(f"data: {data}\n\n".encode()); self.wfile.flush()
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n"); self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            BROADCASTER.unsubscribe(q)

# ─── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    isolation_error = _clone_chat_isolation_error(
        instance_id=A2A2H_INSTANCE_ID,
        chat_db=CHAT_DB_PATH,
        a2a2h_root=A2A2H_ROOT,
    )
    if isolation_error:
        sys.stderr.write(f"FATAL: {isolation_error}\n")
        sys.exit(2)
    if not PWA_AUTH_TOKEN:
        sys.stderr.write("WARN: PWA_AUTH_TOKEN not set — running without auth (dev only)\n")
    append(sender="system", kind="system_event",
           content=f"pwa-backend starting on {BIND}:{PORT}")
    server = ThreadingHTTPServer((BIND, PORT), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()

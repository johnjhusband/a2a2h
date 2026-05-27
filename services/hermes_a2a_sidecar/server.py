#!/usr/bin/env python3
"""
Hermes A2A sidecar — translates A2A requests into Hermes API calls.

Hermes doesn't speak A2A natively (NousResearch/hermes-agent issue #514). This
sidecar listens on port 8643, accepts A2A-shaped JSON over HTTP, authenticates
the caller via HERMES_A2A_TOKEN bearer, then translates into a Hermes
OpenAI-compatible chat completion request (port 8642, authenticated with the
existing api_server.key). The Hermes response is wrapped back into A2A format
and returned.

Accepts requests from:
  - OpenClaw (via services/a2a_delegate/server.py MCP tool)
  - PWA backend (when John @-mentions Hermes directly)

All requests + responses are logged to the shared chat DB so the PWA renders
inter-hemisphere traffic alongside user-facing messages.

Run as systemd user service: a2a2h-hermes-a2a-sidecar.service
Listens loopback only by default. Public exposure goes through Caddy + PWA backend.

Implementation: pure stdlib http.server (no fastapi dependency — PEP 668-friendly).
"""
from __future__ import annotations
import json
import os
import sys
import time
import uuid
import socket
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from chat.db import log_a2a_request, log_a2a_response, append  # noqa: E402

PORT = int(os.environ.get("HERMES_A2A_PORT", "8643"))
HERMES_A2A_TOKEN = os.environ.get("HERMES_A2A_TOKEN", "")
HERMES_API_URL = os.environ.get("HERMES_API_URL", "http://127.0.0.1:8642/v1/chat/completions")
HERMES_API_KEY = os.environ.get("HERMES_API_SERVER_KEY", "")
HERMES_MODEL = os.environ.get("HERMES_MODEL", "openai-codex/gpt-5.5")
HERMES_TIMEOUT_S = int(os.environ.get("HERMES_TIMEOUT_S", "180"))

# Keep PWA human chat and agent-to-agent delegation in separate persistent
# Hermes API-server sessions. The Hermes API is still HTTP/stateless, but the
# API-server loads the transcript for X-Hermes-Session-Id from its state DB and
# returns the same session id on each turn, preserving short-term context while
# keeping prompt-cache prefixes stable.
HERMES_HUMAN_SESSION_ID = os.environ.get("HERMES_HUMAN_SESSION_ID", "pwa-john-hermes-main")
HERMES_HUMAN_SESSION_KEY = os.environ.get("HERMES_HUMAN_SESSION_KEY", "pwa:john:hermes")
HERMES_AGENT_SESSION_ID = os.environ.get("HERMES_AGENT_SESSION_ID", "a2a-openclaw-hermes-main")
HERMES_AGENT_SESSION_KEY = os.environ.get("HERMES_AGENT_SESSION_KEY", "a2a:openclaw:hermes")


def _build_hermes_messages(capability: str, inputs: dict, success_criteria: str, sender: str) -> list[dict]:
    """Compose the chat-completion-shaped messages Hermes will receive."""
    audience = (inputs.get("audience") if isinstance(inputs, dict) else None) or (
        "human" if sender == "john" else "agent"
    )
    if audience == "human" or sender == "john":
        response_style = inputs.get("response_style") if isinstance(inputs, dict) else ""
        message = inputs.get("message", "") if isinstance(inputs, dict) else ""
        context = {
            key: value for key, value in inputs.items()
            if key not in {"message", "audience", "response_style"}
        } if isinstance(inputs, dict) else {}
        system_prompt = (
            "You are Hermes, the right hemisphere of A2A2H, speaking directly to "
            "John in the PWA chat. Respond in plain conversational English. Do "
            "not return JSON, YAML, markdown schema blocks, or agent findings. "
            "Do not use an A2A structured-data contract for this reply. Keep it "
            "concise, natural, and useful."
        )
        if response_style:
            system_prompt = f"{system_prompt}\n\n{response_style}"
        user_parts = []
        if message:
            user_parts.append(f"John says: {message}")
        if context:
            user_parts.append(f"Context: {json.dumps(context, ensure_ascii=False)}")
        if capability:
            user_parts.append(f"Capability: {capability}")
        if success_criteria:
            user_parts.append(f"Success criteria: {success_criteria}")
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "\n\n".join(user_parts) or "Please respond to John."},
        ]

    user_payload = {
        "delegation_from": sender,
        "capability": capability,
        "inputs": inputs,
        "success_criteria": success_criteria,
        "contract": (
            "Execute the capability. Return STRUCTURED FINDINGS AS DATA. "
            "Do not issue commands. The decider is OpenClaw (or John, if "
            "sender=='john'). Your role per HERMES_ROLE.md is the right "
            "hemisphere — autonomic nervous system."
        ),
    }
    return [
        {"role": "system", "content": "You are Hermes, the right hemisphere of A2A2H. Respond per HERMES_ROLE.md."},
        {"role": "user", "content": json.dumps(user_payload)},
    ]


def _call_hermes(messages: list[dict], *, session_id: str, session_key: str) -> dict:
    body = json.dumps({"model": HERMES_MODEL, "messages": messages}).encode("utf-8")
    req = urllib.request.Request(
        HERMES_API_URL,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {HERMES_API_KEY}",
            "X-Hermes-Session-Id": session_id,
            "X-Hermes-Session-Key": session_key,
        },
    )
    with urllib.request.urlopen(req, timeout=HERMES_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _extract_text(hermes_resp: dict) -> str:
    try:
        return hermes_resp["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return json.dumps(hermes_resp)


class Handler(BaseHTTPRequestHandler):
    server_version = "A2A2HHermesA2ASidecar/1.0"

    def log_message(self, fmt, *args):
        # Default goes to stderr; keep it for journalctl.
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def _send_json(self, status: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            # Client timed out or disconnected before we finished the response.
            # This should not poison the sidecar process or obscure the real failure.
            pass

    def _unauth(self):
        append(sender="system", recipient="user", kind="system_event",
               content=json.dumps({"event": "hermes_a2a_unauthorized", "path": self.path}))
        self._send_json(401, {"error": "unauthorized"})

    def _check_auth(self) -> bool:
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return False
        return auth[len("Bearer "):] == HERMES_A2A_TOKEN

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "service": "hermes-a2a-sidecar"})
            return
        self._send_json(404, {"error": "not_found"})

    def do_POST(self):
        if not self.path.startswith("/a2a"):
            self._send_json(404, {"error": "not_found"})
            return
        if not self._check_auth():
            self._unauth()
            return

        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            self._send_json(400, {"error": "invalid_json"})
            return

        task_id = req.get("task_id") or str(uuid.uuid4())
        sender = req.get("sender", "unknown")
        capability = req.get("capability", "")
        inputs = req.get("inputs") or {}
        success_criteria = req.get("success_criteria", "")

        log_a2a_request(task_id=task_id, sender=sender, recipient="hermes",
                        payload={"capability": capability, "inputs": inputs,
                                 "success_criteria": success_criteria})

        try:
            messages = _build_hermes_messages(capability, inputs, success_criteria, sender)
            if sender == "john" or (isinstance(inputs, dict) and inputs.get("audience") == "human"):
                session_id = HERMES_HUMAN_SESSION_ID
                session_key = HERMES_HUMAN_SESSION_KEY
            else:
                # Agent-to-agent delegations are work units, not an ongoing chat.
                # Reusing one persistent Hermes API session caused the transcript
                # to grow past the compression threshold (~250k tokens), which in
                # turn made short health/delegation calls time out during preflight
                # context summarization. Keep the configured prefix for traceability
                # but isolate each task in its own session so delegation latency does
                # not degrade over time.
                compact_task_id = task_id.replace("-", "")
                # Hermes/Codex may derive prompt-cache identity from either the
                # session id or session key, and Codex caps that value at 64 chars.
                # Use compact task-scoped values for both fields.
                session_id = f"a2a-{compact_task_id}"
                session_key = f"a2a:{compact_task_id}"
            hermes_resp = _call_hermes(messages, session_id=session_id, session_key=session_key)
            findings_text = _extract_text(hermes_resp)
        except urllib.error.HTTPError as e:
            err = {"task_id": task_id, "status": "error",
                   "error": f"Hermes HTTP {e.code}: {(e.read().decode('utf-8','replace') if e.fp else '')[:500]}"}
            log_a2a_response(task_id=task_id, sender="hermes", recipient=sender, payload=err)
            self._send_json(502, err)
            return
        except urllib.error.URLError as e:
            err = {"task_id": task_id, "status": "error", "error": f"Hermes unreachable: {e.reason}"}
            log_a2a_response(task_id=task_id, sender="hermes", recipient=sender, payload=err)
            self._send_json(503, err)
            return
        except (TimeoutError, socket.timeout) as e:
            err = {"task_id": task_id, "status": "timeout", "error": f"Hermes timed out after {HERMES_TIMEOUT_S}s"}
            log_a2a_response(task_id=task_id, sender="hermes", recipient=sender, payload=err)
            self._send_json(504, err)
            return
        except Exception as e:
            err = {"task_id": task_id, "status": "error", "error": repr(e)}
            log_a2a_response(task_id=task_id, sender="hermes", recipient=sender, payload=err)
            self._send_json(500, err)
            return

        out = {"task_id": task_id, "status": "ok", "findings": findings_text, "session_id": session_id}
        log_a2a_response(task_id=task_id, sender="hermes", recipient=sender, payload=out)
        self._send_json(200, out)


def main() -> None:
    if not HERMES_A2A_TOKEN:
        sys.stderr.write("FATAL: HERMES_A2A_TOKEN not set in environment\n")
        sys.exit(2)
    if not HERMES_API_KEY:
        sys.stderr.write("FATAL: HERMES_API_SERVER_KEY not set in environment\n")
        sys.exit(2)
    append(sender="system", content=f"hermes-a2a-sidecar starting on 127.0.0.1:{PORT}",
           kind="system_event")
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()

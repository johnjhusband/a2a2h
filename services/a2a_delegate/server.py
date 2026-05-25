#!/usr/bin/env python3
"""
OpenClaw MCP server — exposes one tool: `a2a_delegate`.

OpenClaw discovers this server via openclaw.json `mcp.servers.a2a-delegate`,
spawns it as a stdio subprocess, and lists its tool to the agent's prompt.
When OpenClaw decides to delegate work to Hermes, it calls the tool with
{target, capability, inputs, success_criteria}. The tool:
  1. Logs the request to the shared chat DB (observability — John sees it).
  2. POSTs to the Hermes A2A sidecar (http://127.0.0.1:8643/a2a/).
  3. Logs Hermes's response.
  4. Returns the findings to OpenClaw.

Stdio MCP protocol: JSON-RPC 2.0 messages, one per line. Methods used:
  - initialize         : handshake
  - tools/list         : declare a2a_delegate
  - tools/call         : execute the delegation

This module is intentionally dependency-light — pure stdlib + a hand-written
JSON-RPC loop. No `pip install mcp` required (PEP 668 friction avoided).
"""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.request
import urllib.error
import uuid
from pathlib import Path

# Make the shared chat module importable when this script runs from systemd
# or as an MCP subprocess (working dir not guaranteed).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from chat.db import log_a2a_request, log_a2a_response  # noqa: E402

HERMES_A2A_URL = os.environ.get("HERMES_A2A_URL", "http://127.0.0.1:8643/a2a/")
HERMES_A2A_TOKEN = os.environ.get("HERMES_A2A_TOKEN", "")
DELEGATE_TIMEOUT_S = int(os.environ.get("DELEGATE_TIMEOUT_S", "180"))

PROTOCOL_VERSION = "2025-06-18"  # MCP spec version we implement

TOOL_SCHEMA = {
    "name": "a2a_delegate",
    "description": (
        "Delegate work to Hermes (right hemisphere — autonomic nervous system). "
        "Hermes executes skills, runs tool chains, gathers data from sources, "
        "does long-horizon work, and returns STRUCTURED FINDINGS AS DATA. "
        "Hermes never returns commands; OpenClaw retains decision authority. "
        "Use this when a task requires action in the world, not when you're "
        "reasoning, planning, or composing a user-facing reply."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "capability": {
                "type": "string",
                "description": "What Hermes should do. Free-form, but be specific.",
            },
            "inputs": {
                "type": "object",
                "description": "Structured inputs Hermes needs (URLs, queries, parameters).",
                "additionalProperties": True,
            },
            "success_criteria": {
                "type": "string",
                "description": "How Hermes should know it's done. What does success look like?",
            },
        },
        "required": ["capability", "success_criteria"],
    },
}

def _send(msg: dict) -> None:
    """Write a JSON-RPC message to stdout (one line)."""
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()

def _err(req_id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}

def _ok(req_id, result) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}

def _do_a2a_call(task_id: str, capability: str, inputs: dict, success_criteria: str) -> dict:
    """POST to Hermes A2A sidecar, return parsed findings."""
    body = json.dumps({
        "task_id": task_id,
        "sender": "openclaw",
        "capability": capability,
        "inputs": inputs,
        "success_criteria": success_criteria,
    }).encode("utf-8")
    req = urllib.request.Request(
        HERMES_A2A_URL,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {HERMES_A2A_TOKEN}",
        },
    )
    log_a2a_request(task_id=task_id, sender="openclaw", recipient="hermes",
                    payload={"capability": capability, "inputs": inputs,
                             "success_criteria": success_criteria})
    with urllib.request.urlopen(req, timeout=DELEGATE_TIMEOUT_S) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    log_a2a_response(task_id=task_id, sender="hermes", recipient="openclaw", payload=payload)
    return payload

def handle_tools_call(params: dict) -> dict:
    name = params.get("name")
    args = params.get("arguments") or {}
    if name != "a2a_delegate":
        return {"isError": True, "content": [{"type": "text", "text": f"unknown tool: {name}"}]}

    task_id = str(uuid.uuid4())
    capability = args.get("capability") or ""
    inputs = args.get("inputs") or {}
    success_criteria = args.get("success_criteria") or ""

    try:
        payload = _do_a2a_call(task_id, capability, inputs, success_criteria)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace") if e.fp else ""
        return {"isError": True, "content": [{"type": "text",
                "text": f"Hermes returned HTTP {e.code}: {body[:500]}"}]}
    except urllib.error.URLError as e:
        return {"isError": True, "content": [{"type": "text",
                "text": f"Hermes unreachable: {e.reason}"}]}
    except Exception as e:
        return {"isError": True, "content": [{"type": "text",
                "text": f"Delegation error: {e!r}"}]}

    text = json.dumps(payload, indent=2)
    return {"content": [{"type": "text", "text": text}]}

def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = req.get("method")
        req_id = req.get("id")
        params = req.get("params") or {}

        if method == "initialize":
            _send(_ok(req_id, {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "a2a-delegate", "version": "1.0.0"},
            }))
        elif method == "tools/list":
            _send(_ok(req_id, {"tools": [TOOL_SCHEMA]}))
        elif method == "tools/call":
            result = handle_tools_call(params)
            _send(_ok(req_id, result))
        elif method == "notifications/initialized":
            pass  # client done initializing; no response required for notifications
        else:
            if req_id is not None:
                _send(_err(req_id, -32601, f"method not implemented: {method}"))

if __name__ == "__main__":
    main()

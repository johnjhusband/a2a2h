#!/bin/bash
# Keep the three live sessions warm:
#   - pwa-john-main          (OpenClaw left-hemisphere chat with John)
#   - pwa-john-hermes-main   (Hermes right-hemisphere chat with John)
#   - a2a-openclaw-hermes-main (inter-hemisphere A2A bridge)
# Each ping costs essentially nothing because the entire prefix is cached and
# we only send a one-word prompt. The point is to keep OpenAIs prompt cache
# and Hermes server-side session state from going cold so subsequent real
# messages do not pay another ~45K bootstrap re-warm.
# Wired by scripts/install-a2a2h.sh and run via systemd timer cache-keepalive.timer.
set -e
PATH=$HOME/.local/bin:$HOME/.hermes/hermes-agent/venv/bin:$PATH
TS=$(date -Iseconds)

# OpenClaw pwa-john-main keep-alive (uses openclaw agent --session-id reuse)
timeout 90 openclaw agent --agent main --session-id pwa-john-main \
  --message "[cache-keepalive at $TS — respond with one word: ok]" \
  --thinking off --json --timeout 60 >/dev/null 2>&1 || echo "openclaw ping failed"

# Hermes human session — POST through the sidecar so we exercise the real path.
# Keep bearer values out of shell variables, command arguments, and journald.
# The Python helper reads the local env file in-process and sends the Authorization
# header through urllib instead of exposing it in a curl -H process argument.
KEEPALIVE_TS="$TS" python3 - <<'PY' >/dev/null 2>&1 || echo "hermes ping failed"
import json
import os
import time
import urllib.request
import uuid

TOKEN_NAME = "HERMES_A2A_TOKEN"

def read_env_value(path: str, name: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key == name:
                    return value.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return os.environ.get(name, "")

token = read_env_value("/opt/a2a2h/.env", TOKEN_NAME)
if not token:
    raise SystemExit(f"{TOKEN_NAME} missing")

ts = os.environ.get("KEEPALIVE_TS") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
payload = {
    "task_id": str(uuid.uuid4()),
    "sender": "keepalive",
    "capability": "keepalive-ping",
    "inputs": {
        "message": f"keep-alive at {ts} — reply with single word: ok",
        "audience": "agent",
        "response_style": "one-word reply",
    },
    "success_criteria": "any response",
}
req = urllib.request.Request(
    "http://127.0.0.1:8643/a2a/",
    data=json.dumps(payload).encode("utf-8"),
    method="POST",
    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
)
with urllib.request.urlopen(req, timeout=90) as resp:
    resp.read()
PY

exit 0

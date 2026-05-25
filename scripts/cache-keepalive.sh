#!/bin/bash
# Keep the three live sessions warm:
#   - pwa-john-main          (OpenClaw left-hemisphere chat with John)
#   - pwa-john-hermes-main   (Hermes right-hemisphere chat with John)
#   - a2a-openclaw-hermes-main (inter-hemisphere A2A bridge)
# Each ping costs essentially nothing because the entire prefix is cached and
# we only send a one-word prompt. The point is to keep OpenAIs prompt cache
# and Hermes server-side session state from going cold so subsequent real
# messages do not pay another ~45K bootstrap re-warm.
# Wired by scripts/install-cto.sh and run via systemd timer cache-keepalive.timer.
set -e
PATH=$HOME/.local/bin:$HOME/.hermes/hermes-agent/venv/bin:$PATH
TS=$(date -Iseconds)

# OpenClaw pwa-john-main keep-alive (uses openclaw agent --session-id reuse)
timeout 90 openclaw agent --agent main --session-id pwa-john-main \
  --message "[cache-keepalive at $TS — respond with one word: ok]" \
  --thinking off --json --timeout 60 >/dev/null 2>&1 || echo "openclaw ping failed"

# Hermes human session — POST through the sidecar so we exercise the real path
PWA_TOKEN=$(grep "^PWA_AUTH_TOKEN=" /opt/cto/.env | cut -d= -f2)
HERMES_A2A_TOKEN=$(grep "^HERMES_A2A_TOKEN=" /opt/cto/.env | cut -d= -f2)
curl -sS --max-time 30 -X POST http://127.0.0.1:8643/a2a/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $HERMES_A2A_TOKEN" \
  -d "{\"task_id\":\"$(uuidgen)\",\"sender\":\"keepalive\",\"capability\":\"keepalive-ping\",\"inputs\":{\"message\":\"keep-alive at $TS — reply with single word: ok\",\"audience\":\"agent\",\"response_style\":\"one-word reply\"},\"success_criteria\":\"any response\"}" \
  >/dev/null 2>&1 || echo "hermes ping failed"

exit 0

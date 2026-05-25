# A2A2H — Agent-to-Agent-to-Human Bridge

A small, opinionated chat bridge that puts a human in front of a multi-agent system over the [A2A protocol](https://a2aproject.dev/).

A2A2H is the "H" — the human-facing surface — for a topology where one or more A2A-speaking agents run on a server and a single human (you) talks to them from a phone or browser. It bundles:

- A **mobile-first PWA** (chat UI + service worker + Web Push) you can install on a phone home screen
- A **stdlib Python backend** that mediates between the PWA and the agents, persisting transcripts to SQLite
- An **A2A↔Hermes sidecar** that translates the canonical A2A `{task_id, capability, inputs, success_criteria}` envelope into Hermes' OpenAI-compatible chat API
- An **MCP server (`a2a_delegate`)** the left-hemisphere agent (OpenClaw) mounts so it can delegate work to Hermes over A2A
- A **cache keep-alive** systemd timer that pings live sessions every 30 min so OpenAI's prompt cache stays warm and you don't re-pay bootstrap tokens on every idle gap

This was extracted from a private CTO project where it sits between [OpenClaw](https://docs.openclaw.ai) (left hemisphere / decider) and [Hermes Agent](https://hermes-agent.nousresearch.com) (right hemisphere / executor). It runs in production on a headless Hetzner VPS, fronted by Caddy at a single HTTPS endpoint.

## Why this exists

The "missing piece" of a multi-agent system is usually not the agents themselves — those are mature open-source frameworks now — but the everyday surface a human uses to talk to them. Slack/Telegram bots are noisy and lossy. A web chat that knows about the A2A envelope, persists transcripts, hides JSON envelopes by default but lets you toggle them on for observability, and stays cache-warm so cost stays predictable — that's the gap.

## What you see vs what the agents see

The chat UI shows you the prose, not the protocol. Toggles in the topbar let you peek under the hood:

- **A2A toggle** — render the agent-to-agent `a2a_request` / `a2a_response` JSON envelopes inline so you can watch hemisphere-to-hemisphere negotiation.
- **JSON toggle** — when A2A is on, also pretty-print the JSON contents (off = just the metadata: who→who, kind, time).

Default is both off. State persists in localStorage.

## Architecture (at a glance)

```
                          ┌────────────────────────┐
                          │  Browser PWA           │
                          │  /index.html /app.js   │
                          └───────────┬────────────┘
                                      │ HTTPS + SSE
                                      ▼
                          ┌────────────────────────┐
                          │  Caddy reverse proxy   │
                          └───────────┬────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │  PWA backend (server.py)    │
                       │  routes by @-mention or     │
                       │  defaults to left hemisphere │
                       └───┬─────────────────────┬───┘
                           │                     │
        @openclaw / default│                     │@hermes
                           │                     │
                           ▼                     ▼
                ┌────────────────────┐  ┌────────────────────┐
                │  openclaw agent    │  │  hermes_a2a_sidecar│
                │  (CLI, --session-id│  │  POST /a2a/        │
                │  reuse)            │  │  → Hermes HTTP API │
                └─────────┬──────────┘  └──────────┬─────────┘
                          │                        │
                          ▼                        ▼
                    OpenClaw gateway          Hermes gateway
                    (left hemisphere)         (right hemisphere)
                              │       ▲
                              │ A2A   │ A2A response
                              ▼       │
                         a2a_delegate (MCP)
```

All chat traffic — human input, agent replies, and inter-hemisphere A2A — is persisted to a single SQLite table (`messages`) in `chat.db`. The PWA streams new rows live over Server-Sent Events.

## Layout

```
A2A2H/
├── services/
│   ├── pwa/
│   │   ├── frontend/         # PWA shell (index.html, app.js, style.css, manifest, service-worker)
│   │   ├── backend/server.py # stdlib HTTP+SSE bridge, routes @-mentions, persists chat
│   │   └── caddy/Caddyfile   # HTTPS termination + reverse proxy
│   ├── chat/db.py            # SQLite schema + append/tail helpers (used by every service)
│   ├── hermes_a2a_sidecar/   # A2A → Hermes API server translator
│   └── a2a_delegate/         # MCP server so OpenClaw can delegate to Hermes via A2A
├── scripts/
│   ├── cache-keepalive.sh    # pings live sessions every 30 min
│   └── systemd/              # systemd user units for the keep-alive timer
└── README.md
```

## Status

**Extracted 2026-05-25** from the upstream private CTO project. v0.1.

This release is the working VPS deployment — paths (`/opt/cto`, session ids like `pwa-john-main`, the `cto@husband.llc` Codex account) are still hardcoded in places. Treat it as a reference implementation, not a polished library. The cleanest place to start a fork is `services/pwa/backend/server.py` — that's the file that decides routing and persistence.

## Auth model

- **PWA → backend:** `PWA_AUTH_TOKEN` bearer (passed via `?token=…` on first visit, then localStorage on subsequent loads)
- **backend → Hermes sidecar:** `HERMES_A2A_TOKEN` bearer
- **sidecar → Hermes API:** `HERMES_API_SERVER_KEY` bearer + `X-Hermes-Session-Id` for session continuity
- **OpenClaw side:** the OpenClaw CLI talks to its own gateway via `~/.openclaw/openclaw.json` `gateway.auth.token`

All tokens are generated once per install and live in `/opt/cto/.env` (or wherever your deployment puts the shared env file). None should ship in this repo.

## Dependencies (external)

A2A2H is the bridge; the agents themselves live elsewhere:

- [OpenClaw](https://docs.openclaw.ai) ≥ 2026.5.7 (left hemisphere — the decider)
- [Hermes Agent](https://hermes-agent.nousresearch.com) ≥ 0.13.0 (right hemisphere — the doer)
- Codex CLI (`@openai/codex`) — drives the device-code OAuth flow that backs both agents' LLM calls
- Python 3.11+ stdlib only on the bridge (no third-party Python deps in `services/`)
- Caddy 2.x for the HTTPS edge
- A web push library (`pywebpush`) optional — the PWA backend gracefully degrades to no-push if it's missing

## License

Not chosen yet — talk to the owner.

"""
Shared chat persistence for A2A2H.

Schema is intentionally minimal — every message is one row. Senders are agents
or "john". Kinds distinguish chat messages from A2A protocol traffic so the PWA
frontend can render them with distinct affordances (different icons/colors)
without losing observability per the v1.1 chat model.

This module is imported by:
  - services/a2a_delegate/server.py   (OpenClaw's MCP tool logs delegations)
  - services/hermes_a2a_sidecar/server.py (logs incoming delegations + responses)
  - services/pwa/backend/server.py    (reads + tails for WebSocket streaming)

The DB file is single-writer-multi-reader; SQLite WAL mode handles concurrency.
"""
from __future__ import annotations
import os
import sqlite3
import time
import json
from typing import Optional, Iterable
from contextlib import contextmanager

CHAT_DB_PATH = os.environ.get("CHAT_DB", "/opt/a2a2h/chat.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,
    sender      TEXT    NOT NULL,    -- 'john' | 'openclaw' | 'hermes' | 'system'
    recipient   TEXT,                 -- target sender if directed; null = broadcast/observable
    kind        TEXT    NOT NULL,    -- 'chat' | 'a2a_request' | 'a2a_response' | 'system_event'
    correlation TEXT,                 -- task_id for A2A traffic; links request<->response
    content     TEXT    NOT NULL     -- body; JSON-encoded for a2a_*, plain text for chat
);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
CREATE INDEX IF NOT EXISTS idx_messages_correlation ON messages(correlation);
"""

def _init(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError as exc:
        # WAL improves concurrent readers, but a short-lived test or helper can
        # briefly hold a SQLite lock while a fresh process initializes the same
        # DB. Do not fail initialization solely because the WAL toggle could not
        # acquire the lock; the schema creation below is still idempotent and the
        # connection remains usable with SQLite's current journal mode.
        if "locked" not in str(exc).lower():
            raise
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)

@contextmanager
def connection(path: str = CHAT_DB_PATH):
    new = not os.path.exists(path)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        if new:
            _init(conn)
        else:
            # cheap: ensure schema present on existing DBs too (idempotent)
            conn.executescript(SCHEMA)
        yield conn
    finally:
        conn.close()

def append(
    sender: str,
    content: str,
    *,
    recipient: Optional[str] = None,
    kind: str = "chat",
    correlation: Optional[str] = None,
    path: str = CHAT_DB_PATH,
) -> int:
    """Append one message. Returns inserted row id."""
    with connection(path) as conn:
        cur = conn.execute(
            "INSERT INTO messages (ts, sender, recipient, kind, correlation, content) VALUES (?, ?, ?, ?, ?, ?)",
            (time.time(), sender, recipient, kind, correlation, content),
        )
        return int(cur.lastrowid or 0)

def tail(since_id: int = 0, limit: int = 200, path: str = CHAT_DB_PATH) -> list[dict]:
    """Return up to `limit` messages with id > since_id, oldest first.

    Implements true tail semantics: when the table has more than `limit`
    matching rows, this returns the LATEST `limit` rows (in oldest-first
    order), not the earliest. Critical for PWA first-load — clients pass
    since_id=0 expecting recent history, not msgs 1-N from days ago.
    """
    with connection(path) as conn:
        rows = conn.execute(
            "SELECT id, ts, sender, recipient, kind, correlation, content "
            "FROM messages WHERE id > ? ORDER BY id DESC LIMIT ?",
            (since_id, limit),
        ).fetchall()
    rows = list(reversed(rows))
    return [
        {"id": r[0], "ts": r[1], "sender": r[2], "recipient": r[3],
         "kind": r[4], "correlation": r[5], "content": r[6]}
        for r in rows
    ]

def log_a2a_request(*, task_id: str, sender: str, recipient: str, payload: dict, path: str = CHAT_DB_PATH) -> int:
    return append(sender=sender, recipient=recipient, kind="a2a_request",
                  correlation=task_id, content=json.dumps(payload), path=path)

def log_a2a_response(*, task_id: str, sender: str, recipient: str, payload: dict, path: str = CHAT_DB_PATH) -> int:
    return append(sender=sender, recipient=recipient, kind="a2a_response",
                  correlation=task_id, content=json.dumps(payload), path=path)

if __name__ == "__main__":
    # smoke test
    with connection() as c:
        c.executescript(SCHEMA)
    print(f"chat DB ready at {CHAT_DB_PATH}")

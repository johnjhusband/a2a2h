#!/usr/bin/env python3
"""Detached PWA chat job runner.

The OpenClaw TaskFlow API is exposed inside plugin/runtime tool context, not as
an authenticated CLI/HTTP primitive this stdlib Python bridge can safely call.
For PWA-originated long chat turns we therefore use the smallest durable local
runner: persist job state in /opt/a2a2h/.cache/pwa-jobs.sqlite, read the request
payload from a 0600 JSON file, execute the existing OpenClaw/Hermes route, and
append the terminal result to chat.db for SSE delivery.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVICES_ROOT = REPO_ROOT / "services"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SERVICES_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICES_ROOT))

from chat.db import append  # noqa: E402
from services.pwa.backend.server import (  # noqa: E402
    _humanize_chat_content,
    append_agent_reply,
    send_coordinated_both,
    send_to_hermes,
    send_to_openclaw,
)

JOB_DB_PATH = Path(os.environ.get("PWA_JOB_DB", "/opt/a2a2h/.cache/pwa-jobs.sqlite"))
JOB_SCHEMA = """
CREATE TABLE IF NOT EXISTS pwa_jobs (
    job_id      TEXT PRIMARY KEY,
    target      TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL,
    started_at  REAL,
    ended_at    REAL,
    payload_path TEXT,
    summary     TEXT,
    error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_pwa_jobs_status ON pwa_jobs(status);
CREATE INDEX IF NOT EXISTS idx_pwa_jobs_updated ON pwa_jobs(updated_at);
"""


def _connect() -> sqlite3.Connection:
    JOB_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(JOB_DB_PATH), timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(JOB_SCHEMA)
    return conn


def ensure_job(job_id: str, target: str, payload_path: str) -> None:
    now = time.time()
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO pwa_jobs
              (job_id, target, status, created_at, updated_at, payload_path)
            VALUES (?, ?, 'queued', ?, ?, ?)
            """,
            (job_id, target, now, now, payload_path),
        )


def mark(job_id: str, status: str, *, summary: str | None = None, error: str | None = None) -> None:
    now = time.time()
    fields = ["status = ?", "updated_at = ?"]
    values: list[object] = [status, now]
    if status == "running":
        fields.append("started_at = COALESCE(started_at, ?)")
        values.append(now)
    if status in {"succeeded", "failed"}:
        fields.append("ended_at = ?")
        values.append(now)
    if summary is not None:
        fields.append("summary = ?")
        values.append(summary[:4000])
    if error is not None:
        fields.append("error = ?")
        values.append(error[:4000])
    values.append(job_id)
    with _connect() as conn:
        conn.execute(f"UPDATE pwa_jobs SET {', '.join(fields)} WHERE job_id = ?", values)


def _deliver_one(target: str, message: str) -> tuple[bool, str]:
    if target == "hermes":
        result = send_to_hermes(message, task_id=f"pwa-bg-{int(time.time())}")
        if result.get("ok"):
            reply = _humanize_chat_content(result.get("findings", ""))
            append_agent_reply(sender="hermes", recipient="john", kind="chat", content=reply)
            return True, reply
        return False, result.get("error") or "Hermes background job failed"

    result = send_to_openclaw(message)
    if result.get("ok"):
        reply = _humanize_chat_content(result.get("reply", ""))
        append_agent_reply(sender="openclaw", recipient="john", kind="chat", content=reply)
        return True, reply
    return False, result.get("error") or "OpenClaw background job failed"


def run_job(job_id: str, payload_path: Path) -> int:
    payload = json.loads(payload_path.read_text())
    target = payload.get("target") or "openclaw"
    message = payload.get("message") or ""
    ensure_job(job_id, target, str(payload_path))
    mark(job_id, "running")

    try:
        if target == "both":
            result = send_coordinated_both(message)
            ok = bool(result.get("ok"))
            summary = result.get("coordination_id") or json.dumps(result, ensure_ascii=False)[:1000]
            if not ok:
                summary = result.get("error") or summary
        else:
            ok, summary = _deliver_one(target, message)

        if ok:
            mark(job_id, "succeeded", summary=summary)
            return 0

        append(
            sender="system",
            recipient="john",
            kind="system_event",
            correlation=job_id,
            content=json.dumps({"event": "pwa_background_job_failed", "job_id": job_id, "error": summary}),
        )
        mark(job_id, "failed", error=summary)
        return 1
    except Exception as exc:  # keep detached failures visible in chat.db
        detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-4000:]
        append(
            sender="system",
            recipient="john",
            kind="system_event",
            correlation=job_id,
            content=json.dumps({"event": "pwa_background_job_crashed", "job_id": job_id, "error": repr(exc)}),
        )
        mark(job_id, "failed", error=detail)
        return 1
    finally:
        try:
            payload_path.unlink()
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one detached PWA chat job")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--payload", required=True)
    args = parser.parse_args()
    return run_job(args.job_id, Path(args.payload))


if __name__ == "__main__":
    raise SystemExit(main())

"""Durable SQLite-backed queue for scheduler jobs."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from marketwatcher.logging_config import get_logger

logger = get_logger("scheduler-queue")

QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS scheduler_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    job_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    scheduled_for_utc TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_attempt_utc TEXT NOT NULL,
    lease_until_utc TEXT,
    started_at_utc TEXT,
    finished_at_utc TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    last_error TEXT,
    UNIQUE(job_id, scheduled_for_utc)
);

CREATE INDEX IF NOT EXISTS idx_scheduler_queue_status_next_attempt
ON scheduler_queue(status, next_attempt_utc);

CREATE INDEX IF NOT EXISTS idx_scheduler_queue_scheduled_for
ON scheduler_queue(scheduled_for_utc);

CREATE TABLE IF NOT EXISTS scheduler_watermarks (
    job_id TEXT PRIMARY KEY,
    last_slot_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scheduler_meta (
    meta_key TEXT PRIMARY KEY,
    meta_value TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class QueuedJob:
    """A claimed job from the durable queue."""

    queue_id: int
    job_id: str
    job_type: str
    payload: dict[str, Any]
    scheduled_for_utc: datetime
    attempt_count: int
    max_attempts: int


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat()


def _from_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


class SchedulerQueue:
    """Persistent queue + watermark storage for scheduler execution."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(QUEUE_SCHEMA)
        self.conn.commit()

    def get_watermark(self, job_id: str) -> datetime | None:
        row = self.conn.execute(
            "SELECT last_slot_utc FROM scheduler_watermarks WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        return _from_iso_utc(str(row["last_slot_utc"]))

    def set_watermark(self, job_id: str, last_slot_utc: datetime) -> None:
        now_iso = _to_iso_utc(_utc_now())
        self.conn.execute(
            """
            INSERT INTO scheduler_watermarks(job_id, last_slot_utc, updated_at_utc)
            VALUES (?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE
            SET last_slot_utc = excluded.last_slot_utc,
                updated_at_utc = excluded.updated_at_utc
            """,
            (job_id, _to_iso_utc(last_slot_utc), now_iso),
        )
        self.conn.commit()

    def clear_watermarks(self) -> None:
        self.conn.execute("DELETE FROM scheduler_watermarks")
        self.conn.commit()

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT meta_value FROM scheduler_meta WHERE meta_key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return str(row["meta_value"])

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            """
            INSERT INTO scheduler_meta(meta_key, meta_value)
            VALUES (?, ?)
            ON CONFLICT(meta_key) DO UPDATE
            SET meta_value = excluded.meta_value
            """,
            (key, value),
        )
        self.conn.commit()

    def enqueue(
        self,
        *,
        job_id: str,
        job_type: str,
        payload: dict[str, Any],
        scheduled_for_utc: datetime,
        max_attempts: int = 3,
    ) -> bool:
        now_iso = _to_iso_utc(_utc_now())
        scheduled_iso = _to_iso_utc(scheduled_for_utc)
        cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO scheduler_queue (
                job_id, job_type, payload_json, scheduled_for_utc, status,
                attempt_count, max_attempts, next_attempt_utc, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?)
            """,
            (
                job_id,
                job_type,
                json.dumps(payload, separators=(",", ":")),
                scheduled_iso,
                max_attempts,
                scheduled_iso,
                now_iso,
                now_iso,
            ),
        )
        self.conn.commit()
        return cursor.rowcount == 1

    def recover_expired_leases(self) -> int:
        now_iso = _to_iso_utc(_utc_now())
        cursor = self.conn.execute(
            """
            UPDATE scheduler_queue
            SET status = 'pending',
                next_attempt_utc = ?,
                lease_until_utc = NULL,
                updated_at_utc = ?
            WHERE status = 'running'
              AND lease_until_utc IS NOT NULL
              AND lease_until_utc <= ?
            """,
            (now_iso, now_iso, now_iso),
        )
        self.conn.commit()
        return cursor.rowcount

    def claim_next_due(self, *, lease_seconds: int = 1800) -> QueuedJob | None:
        now = _utc_now()
        now_iso = _to_iso_utc(now)
        lease_until_iso = _to_iso_utc(now + timedelta(seconds=lease_seconds))

        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        row = cursor.execute(
            """
            SELECT id, job_id, job_type, payload_json, scheduled_for_utc, attempt_count, max_attempts
            FROM scheduler_queue
            WHERE status = 'pending'
              AND next_attempt_utc <= ?
              AND scheduled_for_utc <= ?
            ORDER BY next_attempt_utc ASC, scheduled_for_utc ASC, id ASC
            LIMIT 1
            """,
            (now_iso, now_iso),
        ).fetchone()
        if row is None:
            self.conn.commit()
            return None

        updated = cursor.execute(
            """
            UPDATE scheduler_queue
            SET status = 'running',
                lease_until_utc = ?,
                started_at_utc = COALESCE(started_at_utc, ?),
                updated_at_utc = ?
            WHERE id = ?
              AND status = 'pending'
            """,
            (lease_until_iso, now_iso, now_iso, int(row["id"])),
        ).rowcount
        self.conn.commit()
        if updated != 1:
            return None

        payload: dict[str, Any] = json.loads(str(row["payload_json"]))
        return QueuedJob(
            queue_id=int(row["id"]),
            job_id=str(row["job_id"]),
            job_type=str(row["job_type"]),
            payload=payload,
            scheduled_for_utc=_from_iso_utc(str(row["scheduled_for_utc"])),
            attempt_count=int(row["attempt_count"]),
            max_attempts=int(row["max_attempts"]),
        )

    def mark_done(self, queue_id: int) -> None:
        now_iso = _to_iso_utc(_utc_now())
        self.conn.execute(
            """
            UPDATE scheduler_queue
            SET status = 'done',
                lease_until_utc = NULL,
                finished_at_utc = ?,
                updated_at_utc = ?,
                last_error = NULL
            WHERE id = ?
            """,
            (now_iso, now_iso, queue_id),
        )
        self.conn.commit()

    def mark_failed(self, queued_job: QueuedJob, error_message: str, retry_delay_seconds: int) -> tuple[bool, int]:
        """Mark a failed attempt.

        Returns:
            (will_retry, next_attempt_number)
        """
        now = _utc_now()
        now_iso = _to_iso_utc(now)
        next_attempt_number = queued_job.attempt_count + 1
        if next_attempt_number >= queued_job.max_attempts:
            self.conn.execute(
                """
                UPDATE scheduler_queue
                SET status = 'dead',
                    attempt_count = ?,
                    lease_until_utc = NULL,
                    finished_at_utc = ?,
                    updated_at_utc = ?,
                    last_error = ?
                WHERE id = ?
                """,
                (
                    next_attempt_number,
                    now_iso,
                    now_iso,
                    error_message[:1000],
                    queued_job.queue_id,
                ),
            )
            self.conn.commit()
            return (False, next_attempt_number)

        next_attempt_iso = _to_iso_utc(now + timedelta(seconds=retry_delay_seconds))
        self.conn.execute(
            """
            UPDATE scheduler_queue
            SET status = 'pending',
                attempt_count = ?,
                next_attempt_utc = ?,
                lease_until_utc = NULL,
                updated_at_utc = ?,
                last_error = ?
            WHERE id = ?
            """,
            (
                next_attempt_number,
                next_attempt_iso,
                now_iso,
                error_message[:1000],
                queued_job.queue_id,
            ),
        )
        self.conn.commit()
        return (True, next_attempt_number)

    def next_pending_eta(self) -> datetime | None:
        row = self.conn.execute(
            """
            SELECT next_attempt_utc
            FROM scheduler_queue
            WHERE status = 'pending'
            ORDER BY next_attempt_utc ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        return _from_iso_utc(str(row["next_attempt_utc"]))

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> SchedulerQueue:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

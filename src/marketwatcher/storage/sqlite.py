"""SQLite storage for market data snapshots.

Handles:
- Schema initialization
- Metric and category snapshot persistence
- Historical data queries for delta calculations
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Generator

from marketwatcher.logging_config import get_logger
from marketwatcher.models import CategorySnapshot, MetricSnapshot, RunLog

logger = get_logger("storage")

SCHEMA = """
-- Metric snapshots table
CREATE TABLE IF NOT EXISTS metric_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_key TEXT NOT NULL,
    value REAL NOT NULL,
    as_of_utc TEXT,
    as_of_date TEXT,
    collected_at_utc TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coingecko',
    UNIQUE(metric_key, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_metric_snapshots_key_date
ON metric_snapshots(metric_key, as_of_date);

-- Category snapshots table
CREATE TABLE IF NOT EXISTS category_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id TEXT NOT NULL,
    category_name TEXT NOT NULL,
    market_cap_usd REAL NOT NULL,
    pct_change_24h REAL,
    as_of_utc TEXT,
    as_of_date TEXT,
    collected_at_utc TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coingecko'
);

CREATE INDEX IF NOT EXISTS idx_category_snapshots_date
ON category_snapshots(as_of_date);

CREATE INDEX IF NOT EXISTS idx_category_snapshots_id_date
ON category_snapshots(category_id, as_of_date);

-- Run log table
CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL DEFAULT 'ok',
    error_summary TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_log_started
ON run_log(started_at_utc);
"""


class Storage:
    """SQLite storage handler."""

    def __init__(self, db_path: str | Path):
        """Initialize storage with database path."""
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        """Get database connection."""
        if self._conn is None:
            # Ensure parent directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            self._conn = sqlite3.connect(
                self.db_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
            self._conn.row_factory = sqlite3.Row
            self._init_schema()
        return self._conn

    def _init_schema(self) -> None:
        """Initialize database schema."""
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        logger.info(f"Database initialized at {self.db_path}")

    def save_metric_snapshot(self, snapshot: MetricSnapshot) -> int:
        """Save a metric snapshot."""
        as_of_date = snapshot.as_of_utc.strftime("%Y-%m-%d") if snapshot.as_of_utc else None
        cursor = self.conn.execute(
            """
            INSERT OR REPLACE INTO metric_snapshots
            (metric_key, value, as_of_utc, as_of_date, collected_at_utc, source)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.metric_key,
                snapshot.value,
                snapshot.as_of_utc.isoformat() if snapshot.as_of_utc else None,
                as_of_date,
                snapshot.collected_at_utc.isoformat() if snapshot.collected_at_utc else datetime.now(timezone.utc).isoformat(),
                snapshot.source,
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_latest_metric(self, metric_key: str) -> MetricSnapshot | None:
        """Get the most recent snapshot for a metric."""
        cursor = self.conn.execute(
            """
            SELECT * FROM metric_snapshots
            WHERE metric_key = ?
            ORDER BY as_of_utc DESC
            LIMIT 1
            """,
            (metric_key,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_metric_snapshot(row)

    def get_metric_history(
        self, metric_key: str, days: int
    ) -> list[MetricSnapshot]:
        """Get metric history for the last N days."""
        cursor = self.conn.execute(
            """
            SELECT * FROM metric_snapshots
            WHERE metric_key = ?
            AND as_of_date >= date('now', '-' || ? || ' days')
            ORDER BY as_of_utc ASC
            """,
            (metric_key, days),
        )
        return [self._row_to_metric_snapshot(row) for row in cursor.fetchall()]

    def save_category_snapshot(self, snapshot: CategorySnapshot) -> int:
        """Save a category snapshot."""
        as_of_date = snapshot.as_of_utc.strftime("%Y-%m-%d") if snapshot.as_of_utc else None
        cursor = self.conn.execute(
            """
            INSERT INTO category_snapshots
            (category_id, category_name, market_cap_usd, pct_change_24h, as_of_utc, as_of_date, collected_at_utc, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.category_id,
                snapshot.category_name,
                snapshot.market_cap_usd,
                snapshot.pct_change_24h,
                snapshot.as_of_utc.isoformat() if snapshot.as_of_utc else None,
                as_of_date,
                snapshot.collected_at_utc.isoformat() if snapshot.collected_at_utc else datetime.now(timezone.utc).isoformat(),
                snapshot.source,
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_latest_categories(self) -> list[CategorySnapshot]:
        """Get the most recent category snapshots."""
        cursor = self.conn.execute(
            """
            SELECT c1.* FROM category_snapshots c1
            INNER JOIN (
                SELECT category_id, MAX(as_of_utc) as max_date
                FROM category_snapshots
                GROUP BY category_id
            ) c2 ON c1.category_id = c2.category_id AND c1.as_of_utc = c2.max_date
            """
        )
        return [self._row_to_category_snapshot(row) for row in cursor.fetchall()]

    def get_category_history(
        self, category_id: str, days: int
    ) -> list[CategorySnapshot]:
        """Get category history for the last N days."""
        cursor = self.conn.execute(
            """
            SELECT * FROM category_snapshots
            WHERE category_id = ?
            AND as_of_date >= date('now', '-' || ? || ' days')
            ORDER BY as_of_utc ASC
            """,
            (category_id, days),
        )
        return [self._row_to_category_snapshot(row) for row in cursor.fetchall()]

    def log_run(
        self,
        run_type: str,
        status: str = "ok",
        error_summary: str | None = None,
    ) -> int:
        """Log a run operation."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """
            INSERT INTO run_log (run_type, started_at_utc, finished_at_utc, status, error_summary)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_type, now, now, status, error_summary),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_last_run(self) -> RunLog | None:
        """Get the most recent run log."""
        cursor = self.conn.execute(
            """
            SELECT * FROM run_log
            ORDER BY started_at_utc DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_run_log(row)

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _row_to_metric_snapshot(self, row: sqlite3.Row) -> MetricSnapshot:
        """Convert database row to MetricSnapshot."""
        return MetricSnapshot(
            id=row["id"],
            metric_key=row["metric_key"],
            value=row["value"],
            as_of_utc=datetime.fromisoformat(row["as_of_utc"]) if row["as_of_utc"] else None,
            collected_at_utc=datetime.fromisoformat(row["collected_at_utc"]),
            source=row["source"],
        )

    def _row_to_category_snapshot(self, row: sqlite3.Row) -> CategorySnapshot:
        """Convert database row to CategorySnapshot."""
        return CategorySnapshot(
            id=row["id"],
            category_id=row["category_id"],
            category_name=row["category_name"],
            market_cap_usd=row["market_cap_usd"],
            pct_change_24h=row["pct_change_24h"],
            as_of_utc=datetime.fromisoformat(row["as_of_utc"]) if row["as_of_utc"] else None,
            collected_at_utc=datetime.fromisoformat(row["collected_at_utc"]),
            source=row["source"],
        )

    def _row_to_run_log(self, row: sqlite3.Row) -> RunLog:
        """Convert database row to RunLog."""
        return RunLog(
            id=row["id"],
            run_type=row["run_type"],
            started_at_utc=datetime.fromisoformat(row["started_at_utc"]),
            finished_at_utc=datetime.fromisoformat(row["finished_at_utc"]) if row["finished_at_utc"] else None,
            status=row["status"],
            error_summary=row["error_summary"],
        )

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

"""JSONL logger for on-chain data."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from marketwatcher.logging_config import get_logger

logger = get_logger("jsonl")


class JsonlLogger:
    """JSONL logger for storing on-chain fetch history."""

    def __init__(self, log_path: str | Path = "onchain_history.jsonl"):
        self.log_path = Path(log_path)
        # Ensure directory exists
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, data: dict[str, Any]) -> None:
        """Log a record to the JSONL file.

        Args:
            data: Data to log (will be merged with timestamp)
        """
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **data,
        }

        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.debug(f"Logged record to {self.log_path}")

    def read(self, limit: int = 100) -> list[dict]:
        """Read recent records from the JSONL file.

        Args:
            limit: Number of recent records to read

        Returns:
            List of log records
        """
        if not self.log_path.exists():
            return []

        records = []
        with open(self.log_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        return records[-limit:]

    def get_unique_tokens(self) -> set[str]:
        """Get all unique tokens seen across all logs.

        Returns:
            Set of unique token symbols
        """
        tokens = set()
        for record in self.read(limit=10000):
            if "tokens" in record:
                for token in record.get("tokens", []):
                    tokens.add(token.get("symbol", ""))
        return tokens

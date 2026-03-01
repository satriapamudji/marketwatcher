"""Logging configuration for MarketWatcher.

Supports:
- Console output (stderr)
- JSONL file logging (one JSON object per line)
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class JsonlFormatter(logging.Formatter):
    """Format log records as JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        """Format record as JSON."""
        data: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)

        if record.args:
            data["args"] = str(record.args)

        return json.dumps(data, ensure_ascii=False)


class JsonlHandler(logging.Handler):
    """File handler that writes JSONL format."""

    def __init__(self, filepath: Path):
        super().__init__()
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.filepath, "a", encoding="utf-8")

    def emit(self, record: logging.LogRecord) -> None:
        """Write JSONL record."""
        try:
            line = JsonlFormatter().format(record)
            self._file.write(line + "\n")
            self._file.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        """Close file handle."""
        if hasattr(self, "_file"):
            self._file.close()
        super().close()


def setup_logging(
    level: str = "INFO",
    jsonl_path: Path | str | None = None,
    console: bool = True,
) -> logging.Logger:
    """Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        jsonl_path: Path to JSONL log file
        console: Whether to output to console

    Returns:
        Configured logger instance
    """
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    # Configure root logger
    logger = logging.getLogger("marketwatcher")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers
    logger.handlers.clear()

    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        console_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(console_handler)

    # JSONL file handler
    if jsonl_path:
        jsonl_handler = JsonlHandler(Path(jsonl_path))
        jsonl_handler.setLevel(logging.DEBUG)
        logger.addHandler(jsonl_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger for a specific module."""
    return logging.getLogger(f"marketwatcher.{name}")

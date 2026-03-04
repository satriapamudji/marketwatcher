"""Timezone parsing helpers for scheduler-facing settings."""

from __future__ import annotations

import re
from datetime import timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

UTC_OFFSET_RE = re.compile(r"^UTC\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?$", re.IGNORECASE)


def parse_timezone(value: str) -> tzinfo:
    """Parse timezone labels like 'UTC', 'UTC+8', 'UTC+08:00', or IANA names."""
    raw = (value or "UTC").strip()
    if raw.upper() == "UTC":
        return timezone.utc

    match = UTC_OFFSET_RE.match(raw)
    if match:
        sign = 1 if match.group(1) == "+" else -1
        hours = int(match.group(2))
        minutes = int(match.group(3) or "0")
        if hours > 14 or minutes > 59:
            raise ValueError(f"Invalid UTC offset: {value}")
        delta = timedelta(hours=hours, minutes=minutes)
        return timezone(sign * delta)

    try:
        return ZoneInfo(raw)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone: {value}") from exc


def normalize_timezone_label(value: str) -> str:
    """Return normalized label for user-facing display."""
    raw = (value or "UTC").strip()
    if raw.upper() == "UTC":
        return "UTC"

    match = UTC_OFFSET_RE.match(raw)
    if match:
        sign = match.group(1)
        hours = int(match.group(2))
        minutes = int(match.group(3) or "0")
        return f"UTC{sign}{hours:02d}:{minutes:02d}"

    return raw

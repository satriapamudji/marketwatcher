"""Data models for MarketWatcher."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


@dataclass
class MetricSnapshot:
    """A single snapshot of a market metric."""

    id: int | None = None
    metric_key: str = ""  # e.g., "global_mcap_usd", "btc_dominance_pct"
    value: float = 0.0
    as_of_utc: datetime | None = None
    collected_at_utc: datetime | None = None
    source: str = "coingecko"


@dataclass
class CategorySnapshot:
    """A snapshot of a crypto category."""

    id: int | None = None
    category_id: str = ""
    category_name: str = ""
    market_cap_usd: float = 0.0
    pct_change_24h: float | None = None
    as_of_utc: datetime | None = None
    collected_at_utc: datetime | None = None
    source: str = "coingecko"


@dataclass
class RunLog:
    """Log entry for a run operation."""

    id: int | None = None
    run_type: Literal["fetch", "send", "full"] = "full"
    started_at_utc: datetime | None = None
    finished_at_utc: datetime | None = None
    status: Literal["ok", "partial", "err"] = "ok"
    error_summary: str | None = None


@dataclass
class MarketMetrics:
    """Computed market metrics for a report."""

    global_mcap: float
    btc_dominance: float

    # Percentage changes
    global_mcap_1d_change: float | None = None
    global_mcap_7d_change: float | None = None
    global_mcap_14d_change: float | None = None

    btc_dominance_1d_change: float | None = None
    btc_dominance_7d_change: float | None = None
    btc_dominance_14d_change: float | None = None


@dataclass
class CategoryRank:
    """A category with its ranking info."""

    category_id: str
    category_name: str
    market_cap_usd: float
    pct_change_24h: float


@dataclass
class MarketSummary:
    """Complete market summary report data."""

    timestamp: datetime
    metrics: MarketMetrics
    top_gainers: list[CategoryRank] = field(default_factory=list)
    top_losers: list[CategoryRank] = field(default_factory=list)

    # Formatted fields for template
    formatted_global_mcap: str = ""
    formatted_btc_dominance: str = ""
    formatted_global_mcap_1d: str = "N/A"
    formatted_global_mcap_7d: str = "N/A"
    formatted_global_mcap_14d: str = "N/A"
    formatted_btc_dominance_1d: str = "N/A"
    formatted_btc_dominance_7d: str = "N/A"
    formatted_btc_dominance_14d: str = "N/A"

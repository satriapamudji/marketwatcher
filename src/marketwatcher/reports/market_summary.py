"""Market summary report builder.

Computes:
- 1D, 7D, 14D percentage changes from historical data
- Category rankings (top gainers/losers)
"""

from datetime import datetime, timezone
from typing import Any

from marketwatcher.logging_config import get_logger
from marketwatcher.models import CategoryRank, MarketMetrics, MarketSummary
from marketwatcher.config import ReportConfig
from marketwatcher.storage.sqlite import Storage

logger = get_logger("reports")


def compute_daily_change(current: float, previous: float) -> float | None:
    """Compute daily percentage change.

    Args:
        current: Current value
        previous: Previous value

    Returns:
        Percentage change, or None if previous is zero/invalid
    """
    if previous is None or previous == 0:
        return None
    return ((current / previous) - 1) * 100


def compute_average_change(changes: list[float | None]) -> float | None:
    """Compute average of non-None daily changes.

    Args:
        changes: List of daily percentage changes

    Returns:
        Average change, or None if no valid changes
    """
    valid = [c for c in changes if c is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def compute_delta_from_history(
    storage: Storage,
    metric_key: str,
    days: int,
    min_changes: int = 1,
) -> float | None:
    """Compute average daily change over N days from stored history.

    Args:
        storage: Storage instance
        metric_key: Metric to compute delta for
        days: Number of days to look back
        min_changes: Minimum number of daily changes required

    Returns:
        Average daily change percentage, or None if insufficient data
    """
    history = storage.get_metric_history(metric_key, days + 1)

    if len(history) < 2:
        return None

    # Sort by time ascending
    history.sort(key=lambda x: x.as_of_utc or datetime.min)

    # Compute daily changes
    daily_changes = []
    for i in range(1, len(history)):
        prev = history[i - 1]
        curr = history[i]

        if prev.value and curr.value and prev.as_of_utc and curr.as_of_utc:
            change = compute_daily_change(curr.value, prev.value)
            if change is not None:
                daily_changes.append(change)

    # Require minimum number of changes
    if len(daily_changes) < min_changes:
        return None

    return compute_average_change(daily_changes)


def build_market_summary(
    storage: Storage,
    config: ReportConfig,
    timestamp: datetime | None = None,
    api_fallback: dict | None = None,
) -> MarketSummary:
    """Build complete market summary from stored data.

    Args:
        storage: Storage with persisted snapshots
        config: Report configuration
        timestamp: Optional timestamp (defaults to now)
        api_fallback: Optional dict with API data for fallback (from current fetch)

    Returns:
        Complete MarketSummary ready for rendering
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    logger.info("Building market summary report")

    # Get latest metrics
    latest_mcap = storage.get_latest_metric("global_mcap_usd")
    latest_btc_dom = storage.get_latest_metric("btc_dominance_pct")

    if latest_mcap is None or latest_btc_dom is None:
        raise ValueError("No metric data found. Run 'fetch' first.")

    # Build metrics object
    metrics = MarketMetrics(
        global_mcap=latest_mcap.value,
        btc_dominance=latest_btc_dom.value,
    )

    # Compute deltas - use stored history if available, fallback to API data
    history_1d = compute_delta_from_history(storage, "global_mcap_usd", 1, min_changes=1)
    if history_1d is not None:
        metrics.global_mcap_1d_change = history_1d
    elif api_fallback and "market_cap_change_24h" in api_fallback:
        metrics.global_mcap_1d_change = api_fallback.get("market_cap_change_24h")

    history_7d = compute_delta_from_history(storage, "global_mcap_usd", 7, min_changes=7)
    if history_7d is not None:
        metrics.global_mcap_7d_change = history_7d

    history_14d = compute_delta_from_history(storage, "global_mcap_usd", 14, min_changes=14)
    if history_14d is not None:
        metrics.global_mcap_14d_change = history_14d

    # BTC dominance changes
    metrics.btc_dominance_1d_change = compute_delta_from_history(storage, "btc_dominance_pct", 1, min_changes=1)
    metrics.btc_dominance_7d_change = compute_delta_from_history(storage, "btc_dominance_pct", 7, min_changes=7)
    metrics.btc_dominance_14d_change = compute_delta_from_history(storage, "btc_dominance_pct", 14, min_changes=14)

    # Get category rankings
    categories = storage.get_latest_categories()

    # Filter valid categories (have market cap and 24h change)
    valid_cats = [
        c for c in categories
        if c.market_cap_usd and c.pct_change_24h is not None
    ]

    # Sort for gainers and losers
    gainers = sorted(valid_cats, key=lambda x: x.pct_change_24h or 0, reverse=True)
    losers = sorted(valid_cats, key=lambda x: x.pct_change_24h or 0)

    # Build category ranks
    top_gainers = [
        CategoryRank(
            category_id=c.category_id,
            category_name=c.category_name,
            market_cap_usd=c.market_cap_usd,
            pct_change_24h=c.pct_change_24h or 0,
        )
        for c in gainers[:config.top_gainers_count]
    ]

    top_losers = [
        CategoryRank(
            category_id=c.category_id,
            category_name=c.category_name,
            market_cap_usd=c.market_cap_usd,
            pct_change_24h=c.pct_change_24h or 0,
        )
        for c in losers[:config.top_losers_count]
    ]

    # Build summary
    summary = MarketSummary(
        timestamp=timestamp,
        metrics=metrics,
        top_gainers=top_gainers,
        top_losers=top_losers,
    )

    # Format values
    summary.formatted_global_mcap = format_market_cap(metrics.global_mcap, config)
    summary.formatted_btc_dominance = format_percentage(metrics.btc_dominance, config.decimals)

    summary.formatted_global_mcap_1d = format_change(metrics.global_mcap_1d_change, config.decimals)
    summary.formatted_global_mcap_7d = format_change(metrics.global_mcap_7d_change, config.decimals)
    summary.formatted_global_mcap_14d = format_change(metrics.global_mcap_14d_change, config.decimals)

    summary.formatted_btc_dominance_1d = format_change(metrics.btc_dominance_1d_change, config.decimals)
    summary.formatted_btc_dominance_7d = format_change(metrics.btc_dominance_7d_change, config.decimals)
    summary.formatted_btc_dominance_14d = format_change(metrics.btc_dominance_14d_change, config.decimals)

    logger.info("Market summary built successfully")
    return summary


def format_market_cap(value: float, config: ReportConfig) -> str:
    """Format market cap with abbreviation."""
    if config.use_abbreviations:
        return format_abbreviated(value)
    return f"${value:,.0f}"


def format_abbreviated(value: float) -> str:
    """Format number with K/M/B/T abbreviation."""
    if value >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    elif value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.2f}K"
    else:
        return f"${value:.2f}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """Format percentage."""
    return f"{value:.{decimals}f}%"


def format_change(value: float | None, decimals: int = 2) -> str:
    """Format percentage change (with sign)."""
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{decimals}f}%"

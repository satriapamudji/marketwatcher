"""Alert engine for watchlist price/percentage threshold monitoring.

Compares live token data from a watchlist report against configured
thresholds (per-token or watchlist-level defaults).
"""

from dataclasses import dataclass
from typing import Any

from marketwatcher.logging_config import get_logger

logger = get_logger("alerts")


@dataclass
class Alert:
    """A triggered alert."""

    symbol: str
    alert_type: str  # "price_above" | "price_below" | "pct_change"
    current_value: float
    threshold: float
    watchlist_id: str
    watchlist_name: str = ""


def check_alerts(watchlist: dict, report_data: dict[str, Any]) -> list[Alert]:
    """Compare report token data against alert thresholds.

    Args:
        watchlist: The watchlist dict (with tokens that may have alert_above/below/pct fields)
        report_data: Output of build_watchlist_report() — has 'tokens' list with price_raw, change_24h_raw

    Returns:
        List of triggered Alert objects.
    """
    wl_id = watchlist.get("id", "main")
    wl_name = watchlist.get("name", "Main")
    wl_default_pct = watchlist.get("alert_pct")

    # Build lookup: symbol -> token config (with alert fields)
    token_config = {}
    for t in watchlist.get("tokens", []):
        sym = t.get("symbol", "").upper()
        if sym:
            token_config[sym] = t

    triggered: list[Alert] = []

    for token_data in report_data.get("tokens", []):
        symbol = token_data.get("symbol", "").upper()
        cfg = token_config.get(symbol, {})

        price = token_data.get("price_raw", 0)
        change = token_data.get("change_24h_raw")

        # Price above threshold
        alert_above = cfg.get("alert_above")
        if alert_above is not None and price and price >= alert_above:
            triggered.append(Alert(
                symbol=symbol,
                alert_type="price_above",
                current_value=price,
                threshold=alert_above,
                watchlist_id=wl_id,
                watchlist_name=wl_name,
            ))

        # Price below threshold
        alert_below = cfg.get("alert_below")
        if alert_below is not None and price and price <= alert_below:
            triggered.append(Alert(
                symbol=symbol,
                alert_type="price_below",
                current_value=price,
                threshold=alert_below,
                watchlist_id=wl_id,
                watchlist_name=wl_name,
            ))

        # Percentage change threshold (per-token overrides watchlist default)
        pct_threshold = cfg.get("alert_pct", wl_default_pct)
        if pct_threshold is not None and change is not None:
            if abs(change) >= pct_threshold:
                triggered.append(Alert(
                    symbol=symbol,
                    alert_type="pct_change",
                    current_value=change,
                    threshold=pct_threshold,
                    watchlist_id=wl_id,
                    watchlist_name=wl_name,
                ))

    if triggered:
        logger.info(f"Triggered {len(triggered)} alert(s) for {wl_name}")
    return triggered


def format_alert(alert: Alert) -> str:
    """Render a single alert as an HTML Telegram message."""
    if alert.alert_type == "price_above":
        emoji = "\u26a0\ufe0f"  # warning sign
        return (
            f"{emoji} <b>{alert.symbol}</b> above ${alert.threshold:,.2f}\n"
            f"Current: <b>${alert.current_value:,.2f}</b>"
        )
    elif alert.alert_type == "price_below":
        emoji = "\u26a0\ufe0f"
        return (
            f"{emoji} <b>{alert.symbol}</b> below ${alert.threshold:,.2f}\n"
            f"Current: <b>${alert.current_value:,.2f}</b>"
        )
    elif alert.alert_type == "pct_change":
        sign = "+" if alert.current_value >= 0 else ""
        emoji = "\U0001f4c8" if alert.current_value >= 0 else "\U0001f4c9"  # chart up/down
        return (
            f"{emoji} <b>{alert.symbol}</b> moved {sign}{alert.current_value:.1f}% (24h)\n"
            f"Threshold: \u00b1{alert.threshold:.1f}%"
        )
    return f"Alert: {alert.symbol} ({alert.alert_type})"


def format_alerts_batch(alerts: list[Alert]) -> str:
    """Format multiple alerts into a single message."""
    if not alerts:
        return ""

    wl_name = alerts[0].watchlist_name or alerts[0].watchlist_id
    lines = [f"<b><u>Alerts: {wl_name}</u></b>\n"]
    for alert in alerts:
        lines.append(format_alert(alert))
        lines.append("")  # blank line between alerts

    return "\n".join(lines).strip()

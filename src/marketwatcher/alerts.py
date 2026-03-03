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
    timeframe: str = "24h"


def watchlist_has_alerts(watchlist: dict) -> bool:
    """Check if a watchlist has any alert thresholds configured."""
    if watchlist.get("alert_pct"):
        return True
    for t in watchlist.get("tokens", []):
        if t.get("alert_above") or t.get("alert_below") or t.get("alert_pct"):
            return True
    return False


def fetch_alert_prices(
    watchlist: dict,
    cg_provider: Any,
    gt_provider: Any,
) -> dict[str, Any]:
    """Lightweight price fetch for alert checking (no full report build).

    CEX tokens: single batch call via /simple/price.
    DEX tokens: per-token pool fetch (returns 1h/6h/24h changes).

    Returns:
        Dict matching check_alerts() report_data format: {"tokens": [...]}.
    """
    cex_tokens = []
    dex_tokens = []
    for t in watchlist.get("tokens", []):
        token_type = t.get("type", "cex")
        if token_type == "dex":
            dex_tokens.append(t)
        else:
            cex_tokens.append(t)

    tokens_data: list[dict[str, Any]] = []

    # CEX: batch fetch via /simple/price (1 API call for all)
    if cex_tokens:
        coin_ids = [t["coingecko_id"] for t in cex_tokens if t.get("coingecko_id")]
        prices = cg_provider.get_simple_prices(coin_ids)

        for t in cex_tokens:
            cg_id = t.get("coingecko_id", "")
            coin_data = prices.get(cg_id, {})
            tokens_data.append({
                "symbol": t.get("symbol", "").upper(),
                "price_raw": coin_data.get("price", 0),
                "change_24h_raw": coin_data.get("change_24h"),
            })

    # DEX: per-token pool fetch (has 1h/6h/24h)
    for t in dex_tokens:
        chain = t.get("chain", "")
        address = t.get("address", "")
        if not chain or not address:
            continue
        try:
            pools = gt_provider.get_token_pools(chain, address, limit=1)
            if pools:
                pool = pools[0]
                tokens_data.append({
                    "symbol": t.get("symbol", "").upper(),
                    "price_raw": pool.price_usd or 0,
                    "change_24h_raw": pool.price_change_24h,
                    "change_h6_raw": pool.price_change_h6,
                    "change_h1_raw": pool.price_change_h1,
                })
            else:
                logger.warning(f"No pool data for {t.get('symbol')} on {chain}")
        except Exception as e:
            logger.error(f"Error fetching DEX price for {t.get('symbol')}: {e}")

    return {"tokens": tokens_data}


def check_alerts(watchlist: dict, report_data: dict[str, Any]) -> list[Alert]:
    """Compare report token data against alert thresholds.

    Args:
        watchlist: The watchlist dict (with tokens that may have alert_above/below/pct fields)
        report_data: Has 'tokens' list with price_raw, change_24h_raw, and optionally change_h1_raw/change_h6_raw

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
        if pct_threshold is not None:
            # Check 24h
            if change is not None and abs(change) >= pct_threshold:
                triggered.append(Alert(
                    symbol=symbol,
                    alert_type="pct_change",
                    current_value=change,
                    threshold=pct_threshold,
                    watchlist_id=wl_id,
                    watchlist_name=wl_name,
                    timeframe="24h",
                ))

            # Check 6h (DEX tokens only — field absent for CEX)
            change_h6 = token_data.get("change_h6_raw")
            if change_h6 is not None and abs(change_h6) >= pct_threshold:
                triggered.append(Alert(
                    symbol=symbol,
                    alert_type="pct_change",
                    current_value=change_h6,
                    threshold=pct_threshold,
                    watchlist_id=wl_id,
                    watchlist_name=wl_name,
                    timeframe="6h",
                ))

            # Check 1h (DEX tokens only)
            change_h1 = token_data.get("change_h1_raw")
            if change_h1 is not None and abs(change_h1) >= pct_threshold:
                triggered.append(Alert(
                    symbol=symbol,
                    alert_type="pct_change",
                    current_value=change_h1,
                    threshold=pct_threshold,
                    watchlist_id=wl_id,
                    watchlist_name=wl_name,
                    timeframe="1h",
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
            f"{emoji} <b>{alert.symbol}</b> moved {sign}{alert.current_value:.1f}% ({alert.timeframe})\n"
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

"""Alert engine for watchlist price/percentage threshold monitoring.

Compares live token data from a watchlist report against configured
thresholds (per-token or watchlist-level defaults).
"""

from datetime import datetime, timezone
from dataclasses import dataclass
import html
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
    direction: str = ""  # "up" | "down" for pct alerts
    current_price: float | None = None
    reference_price: float | None = None


def watchlist_has_alerts(watchlist: dict) -> bool:
    """Check if a watchlist has any alert thresholds configured."""
    if any(
        watchlist.get(key) is not None
        for key in ("alert_pct", "alert_pct_up", "alert_pct_down")
    ):
        return True
    for t in watchlist.get("tokens", []):
        if any(
            t.get(key) is not None
            for key in ("alert_above", "alert_below", "alert_pct", "alert_pct_up", "alert_pct_down")
        ):
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
    wl_default_pct = _normalize_threshold(watchlist.get("alert_pct"))
    wl_default_pct_up = _normalize_threshold(watchlist.get("alert_pct_up"))
    wl_default_pct_down = _normalize_threshold(watchlist.get("alert_pct_down"))

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
                current_price=price,
                reference_price=alert_above,
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
                current_price=price,
                reference_price=alert_below,
            ))

        pct_up, pct_down = _resolve_pct_thresholds(
            cfg,
            wl_default_pct,
            wl_default_pct_up,
            wl_default_pct_down,
        )
        if pct_up is not None or pct_down is not None:
            current_price = price if price else None
            _append_pct_alerts(
                triggered,
                symbol=symbol,
                watchlist_id=wl_id,
                watchlist_name=wl_name,
                change_value=change,
                timeframe="24h",
                up_threshold=pct_up,
                down_threshold=pct_down,
                current_price=current_price,
            )
            _append_pct_alerts(
                triggered,
                symbol=symbol,
                watchlist_id=wl_id,
                watchlist_name=wl_name,
                change_value=token_data.get("change_h6_raw"),
                timeframe="6h",
                up_threshold=pct_up,
                down_threshold=pct_down,
                current_price=current_price,
            )
            _append_pct_alerts(
                triggered,
                symbol=symbol,
                watchlist_id=wl_id,
                watchlist_name=wl_name,
                change_value=token_data.get("change_h1_raw"),
                timeframe="1h",
                up_threshold=pct_up,
                down_threshold=pct_down,
                current_price=current_price,
            )

    if triggered:
        logger.info(f"Triggered {len(triggered)} alert(s) for {wl_name}")
    return triggered


def _estimate_reference_price(current_price: float | None, pct_change: float | None) -> float | None:
    """Estimate earlier price from current and percent change."""
    if current_price is None or pct_change is None:
        return None
    denominator = 1.0 + (pct_change / 100.0)
    if abs(denominator) < 1e-9:
        return None
    return current_price / denominator


def _normalize_threshold(value: Any) -> float | None:
    """Convert threshold inputs to positive floats."""
    if value is None:
        return None
    try:
        return abs(float(value))
    except (TypeError, ValueError):
        return None


def _resolve_pct_thresholds(
    token_cfg: dict[str, Any],
    wl_default_pct: float | None,
    wl_default_pct_up: float | None,
    wl_default_pct_down: float | None,
) -> tuple[float | None, float | None]:
    """Resolve per-token up/down percent thresholds with backward compatibility."""
    token_pct = _normalize_threshold(token_cfg.get("alert_pct"))
    token_up = _normalize_threshold(token_cfg.get("alert_pct_up"))
    token_down = _normalize_threshold(token_cfg.get("alert_pct_down"))

    up_threshold = token_up
    if up_threshold is None:
        up_threshold = token_pct
    if up_threshold is None:
        up_threshold = wl_default_pct_up
    if up_threshold is None:
        up_threshold = wl_default_pct

    down_threshold = token_down
    if down_threshold is None:
        down_threshold = token_pct
    if down_threshold is None:
        down_threshold = wl_default_pct_down
    if down_threshold is None:
        down_threshold = wl_default_pct

    return (up_threshold, down_threshold)


def _append_pct_alerts(
    triggered: list[Alert],
    *,
    symbol: str,
    watchlist_id: str,
    watchlist_name: str,
    change_value: float | None,
    timeframe: str,
    up_threshold: float | None,
    down_threshold: float | None,
    current_price: float | None,
) -> None:
    """Append directional pct alerts for one timeframe value."""
    if change_value is None:
        return

    if up_threshold is not None and change_value >= up_threshold:
        triggered.append(Alert(
            symbol=symbol,
            alert_type="pct_change",
            current_value=change_value,
            threshold=up_threshold,
            watchlist_id=watchlist_id,
            watchlist_name=watchlist_name,
            timeframe=timeframe,
            direction="up",
            current_price=current_price,
            reference_price=_estimate_reference_price(current_price, change_value),
        ))

    if down_threshold is not None and change_value <= -down_threshold:
        triggered.append(Alert(
            symbol=symbol,
            alert_type="pct_change",
            current_value=change_value,
            threshold=down_threshold,
            watchlist_id=watchlist_id,
            watchlist_name=watchlist_name,
            timeframe=timeframe,
            direction="down",
            current_price=current_price,
            reference_price=_estimate_reference_price(current_price, change_value),
        ))


def _fmt_usd(value: float | None) -> str:
    """Format USD with readable precision across large/small caps."""
    if value is None:
        return "n/a"
    abs_value = abs(value)
    if abs_value >= 1000:
        return f"${value:,.2f}"
    if abs_value >= 1:
        return f"${value:,.4f}"
    if abs_value >= 0.01:
        return f"${value:,.5f}"
    return f"${value:,.8f}"


def _fmt_pct(value: float, digits: int = 2) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{digits}f}%"


def _format_price_move(reference: float | None, current: float | None) -> str:
    if reference is None or current is None:
        return "Price: n/a"
    return f"Price: {_fmt_usd(reference)} \u2192 <b>{_fmt_usd(current)}</b>"


def format_alert(alert: Alert) -> str:
    """Render a single alert as an HTML Telegram message."""
    symbol = html.escape(alert.symbol)

    if alert.alert_type == "price_above":
        delta = alert.current_value - alert.threshold
        delta_pct = (delta / alert.threshold * 100.0) if alert.threshold else 0.0
        return (
            f"\U0001f6a8 <b>{symbol}</b> broke above target\n"
            f"\u2022 {_format_price_move(alert.threshold, alert.current_price)}\n"
            f"\u2022 Breach: +{_fmt_usd(delta)} ({_fmt_pct(delta_pct)})"
        )
    elif alert.alert_type == "price_below":
        delta = alert.threshold - alert.current_value
        delta_pct = (delta / alert.threshold * 100.0) if alert.threshold else 0.0
        return (
            f"\U0001f6a8 <b>{symbol}</b> dropped below floor\n"
            f"\u2022 {_format_price_move(alert.threshold, alert.current_price)}\n"
            f"\u2022 Breach: -{_fmt_usd(delta)} ({_fmt_pct(-delta_pct)})"
        )
    elif alert.alert_type == "pct_change":
        move_icon = "\U0001f4c8" if alert.current_value >= 0 else "\U0001f4c9"
        if alert.direction == "up":
            trigger_label = "upside momentum trigger"
            threshold_txt = f"+{alert.threshold:.1f}%"
        elif alert.direction == "down":
            trigger_label = "downside momentum trigger"
            threshold_txt = f"-{alert.threshold:.1f}%"
        else:
            trigger_label = "momentum trigger"
            threshold_txt = f"\u00b1{alert.threshold:.1f}%"
        return (
            f"{move_icon} <b>{symbol}</b> {alert.timeframe} {trigger_label}\n"
            f"\u2022 Move: <b>{_fmt_pct(alert.current_value)}</b> (threshold {threshold_txt})\n"
            f"\u2022 {_format_price_move(alert.reference_price, alert.current_price)}"
        )
    return f"Alert: {symbol} ({alert.alert_type})"


def format_alerts_batch(alerts: list[Alert]) -> str:
    """Format multiple alerts into a single message."""
    if not alerts:
        return ""

    wl_name = alerts[0].watchlist_name or alerts[0].watchlist_id
    safe_name = html.escape(wl_name)
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    type_counts = {"price_above": 0, "price_below": 0, "pct_change": 0}
    for alert in alerts:
        type_counts[alert.alert_type] = type_counts.get(alert.alert_type, 0) + 1

    pct_up = sum(
        1 for a in alerts
        if a.alert_type == "pct_change" and a.direction == "up"
    )
    pct_down = sum(
        1 for a in alerts
        if a.alert_type == "pct_change" and a.direction == "down"
    )

    summary_parts = []
    if type_counts["price_above"] or type_counts["price_below"]:
        summary_parts.append(
            f"Price levels: {type_counts['price_above'] + type_counts['price_below']}"
        )
    if type_counts["pct_change"]:
        summary_parts.append(f"Momentum: {pct_up} up / {pct_down} down")
    summary = " | ".join(summary_parts) if summary_parts else "Signals"

    lines = [
        f"<b>\U0001f6a8 Watchlist Alerts: {safe_name}</b>",
        f"<i>{len(alerts)} trigger(s) \u00b7 {summary} \u00b7 {now_utc}</i>",
        "",
    ]

    for alert in sorted(alerts, key=lambda a: (a.symbol, a.alert_type, a.direction, a.timeframe)):
        lines.append(format_alert(alert))
        lines.append("")  # blank line between alerts

    return "\n".join(lines).strip()

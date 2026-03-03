"""Global macro report builder.

Fetches macro data and builds a report dict ready for the formatter.
"""

from datetime import datetime, timezone
from typing import Any

from marketwatcher.logging_config import get_logger
from marketwatcher.providers.macro import MacroQuote, fetch_macro_data

logger = get_logger("reports.macro")


def _fmt_price(value: float, decimals: int = 2) -> str:
    """Format price with thousands separator."""
    return f"{value:,.{decimals}f}"


def _fmt_change(pct: float | None) -> str:
    """Format change percentage with sign."""
    if pct is None:
        return "N/A"
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


def _fmt_yield_change(current: float, previous: float) -> str:
    """Format yield change in basis points."""
    diff = current - previous
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.2f}"


def build_macro_report(timestamp: datetime | None = None) -> dict[str, Any]:
    """Build the global macro report.

    Returns:
        Dict with all formatted sections ready for rendering.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    logger.info("Building macro report")
    quotes = fetch_macro_data()

    def q(key: str) -> MacroQuote | None:
        return quotes.get(key)

    # Dollar
    dxy = q("dxy")

    # Rates
    fed_funds = q("fed_funds")
    us_5y = q("us_5y")
    us_10y = q("us_10y")
    us_30y = q("us_30y")

    # Curve spread (5s10s)
    spread_5s10s = None
    if us_5y and us_10y:
        spread_5s10s = us_10y.price - us_5y.price

    # Equities
    sp500 = q("sp500")
    nasdaq = q("nasdaq")
    vix = q("vix")

    # Commodities
    gold = q("gold")
    oil = q("oil")
    copper = q("copper")

    # FX
    usdjpy = q("usdjpy")
    eurusd = q("eurusd")

    report: dict[str, Any] = {
        "timestamp": timestamp,
        # Dollar
        "dxy_price": _fmt_price(dxy.price) if dxy else "N/A",
        "dxy_change": _fmt_change(dxy.change_pct) if dxy else "N/A",
        # Rates (yields shown as %, changes in absolute terms not %)
        "fed_funds_rate": f"{fed_funds.price:.2f}%" if fed_funds else "N/A",
        "us_5y_yield": f"{us_5y.price:.2f}%" if us_5y else "N/A",
        "us_5y_change": _fmt_yield_change(us_5y.price, us_5y.prev_close) if us_5y else "N/A",
        "us_10y_yield": f"{us_10y.price:.2f}%" if us_10y else "N/A",
        "us_10y_change": _fmt_yield_change(us_10y.price, us_10y.prev_close) if us_10y else "N/A",
        "us_30y_yield": f"{us_30y.price:.2f}%" if us_30y else "N/A",
        "us_30y_change": _fmt_yield_change(us_30y.price, us_30y.prev_close) if us_30y else "N/A",
        "spread_5s10s": f"{spread_5s10s:+.2f}" if spread_5s10s is not None else "N/A",
        "curve_status": "inverted" if spread_5s10s is not None and spread_5s10s < 0 else "normal",
        # Equities
        "sp500_price": _fmt_price(sp500.price) if sp500 else "N/A",
        "sp500_change": _fmt_change(sp500.change_pct) if sp500 else "N/A",
        "nasdaq_price": _fmt_price(nasdaq.price) if nasdaq else "N/A",
        "nasdaq_change": _fmt_change(nasdaq.change_pct) if nasdaq else "N/A",
        "vix_price": f"{vix.price:.2f}" if vix else "N/A",
        "vix_change": _fmt_change(vix.change_pct) if vix else "N/A",
        # Commodities
        "gold_price": _fmt_price(gold.price) if gold else "N/A",
        "gold_change": _fmt_change(gold.change_pct) if gold else "N/A",
        "oil_price": _fmt_price(oil.price) if oil else "N/A",
        "oil_change": _fmt_change(oil.change_pct) if oil else "N/A",
        "copper_price": _fmt_price(copper.price) if copper else "N/A",
        "copper_change": _fmt_change(copper.change_pct) if copper else "N/A",
        # FX
        "usdjpy_price": _fmt_price(usdjpy.price) if usdjpy else "N/A",
        "usdjpy_change": _fmt_change(usdjpy.change_pct) if usdjpy else "N/A",
        "eurusd_price": _fmt_price(eurusd.price, 4) if eurusd else "N/A",
        "eurusd_change": _fmt_change(eurusd.change_pct) if eurusd else "N/A",
    }

    logger.info("Macro report built successfully")
    return report

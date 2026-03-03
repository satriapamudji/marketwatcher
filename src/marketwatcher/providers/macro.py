"""Global macro data provider via Yahoo Finance (yfinance).

Fetches:
- Dollar (DXY)
- Rates (Fed Funds proxy, 5Y, 10Y, 30Y yields)
- Equities (S&P 500, NASDAQ, VIX)
- Commodities (Gold, Oil, Copper)
- FX (USD/JPY, EUR/USD)
"""

from dataclasses import dataclass
from typing import Any

from marketwatcher.logging_config import get_logger

logger = get_logger("macro")

# Ticker definitions
MACRO_TICKERS = {
    # Dollar
    "dxy": "DX-Y.NYB",
    # Rates (yields in %)
    "us_5y": "^FVX",
    "us_10y": "^TNX",
    "us_30y": "^TYX",
    "fed_funds": "^IRX",  # 13-week T-bill as proxy
    # Equities
    "sp500": "^GSPC",
    "nasdaq": "^IXIC",
    "vix": "^VIX",
    # Commodities
    "gold": "GC=F",
    "oil": "CL=F",
    "copper": "HG=F",
    # FX
    "usdjpy": "JPY=X",
    "eurusd": "EURUSD=X",
}


@dataclass
class MacroQuote:
    """A single macro instrument quote."""

    symbol: str
    price: float
    prev_close: float
    change_pct: float | None = None

    def __post_init__(self):
        if self.change_pct is None and self.prev_close:
            self.change_pct = ((self.price / self.prev_close) - 1) * 100


def fetch_macro_data() -> dict[str, MacroQuote]:
    """Fetch all macro data in a single batch call.

    Returns:
        Dict keyed by our internal names (dxy, sp500, etc.) -> MacroQuote.
    """
    import yfinance as yf

    logger.info("Fetching macro data via yfinance")

    ticker_str = " ".join(MACRO_TICKERS.values())
    tickers = yf.Tickers(ticker_str)

    # Reverse lookup: yahoo symbol -> our key
    reverse = {v: k for k, v in MACRO_TICKERS.items()}

    results: dict[str, MacroQuote] = {}

    for yahoo_sym, ticker_obj in tickers.tickers.items():
        key = reverse.get(yahoo_sym)
        if not key:
            continue

        try:
            info = ticker_obj.fast_info
            price = info.last_price
            prev = info.previous_close

            if price is None or prev is None:
                logger.warning(f"No price data for {yahoo_sym}")
                continue

            # Handle NaN
            import math
            if math.isnan(price) or math.isnan(prev):
                logger.warning(f"NaN price for {yahoo_sym}")
                continue

            results[key] = MacroQuote(
                symbol=yahoo_sym,
                price=float(price),
                prev_close=float(prev),
            )
        except Exception as e:
            logger.error(f"Failed to fetch {yahoo_sym}: {e}")

    logger.info(f"Fetched {len(results)}/{len(MACRO_TICKERS)} macro quotes")
    return results

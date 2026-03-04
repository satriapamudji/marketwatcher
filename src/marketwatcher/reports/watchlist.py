"""Watchlist report builder."""

from datetime import datetime, timezone
from typing import Any

from marketwatcher.config import ReportConfig
from marketwatcher.logging_config import get_logger

logger = get_logger("reports")


def _format_price(value: float) -> str:
    if value >= 1:
        return f"${value:,.2f}"
    elif value >= 0.01:
        return f"${value:.4f}"
    else:
        return f"${value:.8f}"


def _format_usd(value: float) -> str:
    if value >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    elif value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.0f}"


def _format_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


def build_watchlist_report(
    watchlist: dict,
    coingecko_provider: Any,
    geckoterminal_provider: Any,
    config: ReportConfig,
) -> dict[str, Any]:
    """Build watchlist report by fetching data for each token."""
    logger.info(f"Building watchlist report: {watchlist.get('name', 'main')}")

    tokens_data = []
    for token_entry in watchlist.get("tokens", []):
        symbol = token_entry.get("symbol", "???")
        token_type = token_entry.get("type", "cex")

        try:
            if token_type == "cex":
                cg_id = token_entry.get("coingecko_id", "")
                if not cg_id:
                    logger.warning(f"No coingecko_id for {symbol}, skipping")
                    continue

                coin = coingecko_provider.get_coin(cg_id)
                tokens_data.append({
                    "symbol": coin.get("symbol", symbol),
                    "name": coin.get("name", ""),
                    "token_url": f"https://www.coingecko.com/en/coins/{cg_id}",
                    "price": _format_price(coin.get("price", 0)),
                    "price_raw": coin.get("price", 0),
                    "mcap": _format_usd(coin.get("mcap", 0)),
                    "mcap_label": "MCAP",
                    "change_24h": _format_pct(coin.get("change_24h")),
                    "change_24h_raw": coin.get("change_24h"),
                    "change_7d": _format_pct(coin.get("change_7d")),
                    "type": "cex",
                })

            elif token_type == "dex":
                chain = token_entry.get("chain", "")
                address = token_entry.get("address", "")
                if not chain or not address:
                    logger.warning(f"Missing chain/address for {symbol}, skipping")
                    continue

                pools = geckoterminal_provider.get_token_pools(chain, address, limit=1)
                if not pools:
                    logger.warning(f"No pools found for {symbol} on {chain}")
                    tokens_data.append({
                        "symbol": symbol,
                        "name": "",
                        "token_url": f"https://www.geckoterminal.com/{chain}/tokens/{address}",
                        "price": "N/A",
                        "price_raw": 0,
                        "mcap": "N/A",
                        "mcap_label": "FDV",
                        "change_24h": "N/A",
                        "change_24h_raw": None,
                        "change_7d": "N/A",
                        "type": "dex",
                    })
                    continue

                pool = pools[0]
                mcap_val = pool.market_cap if pool.market_cap is not None else pool.fdv
                price_val = pool.price_usd
                tokens_data.append({
                    "symbol": symbol,
                    "name": pool.name,
                    "token_url": f"https://www.geckoterminal.com/{chain}/tokens/{address}",
                    "price": _format_price(price_val) if price_val else "N/A",
                    "price_raw": price_val or 0,
                    "mcap": _format_usd(mcap_val) if mcap_val else "N/A",
                    "mcap_label": "MCAP" if pool.market_cap is not None else "FDV",
                    "change_24h": _format_pct(pool.price_change_24h),
                    "change_24h_raw": pool.price_change_24h,
                    "change_7d": "N/A",
                    "volume": _format_usd(pool.volume_24h),
                    "liquidity": _format_usd(pool.liquidity),
                    "type": "dex",
                })

        except Exception as e:
            logger.warning(f"Failed to fetch data for {symbol}: {e}")
            tokens_data.append({
                "symbol": symbol,
                "name": "",
                "token_url": (
                    f"https://www.coingecko.com/en/coins/{token_entry.get('coingecko_id', '')}"
                    if token_type == "cex" and token_entry.get("coingecko_id")
                    else f"https://www.geckoterminal.com/{token_entry.get('chain', '')}/tokens/{token_entry.get('address', '')}"
                ),
                "price": "ERR",
                "price_raw": 0,
                "mcap": "N/A",
                "mcap_label": "MCAP",
                "change_24h": "N/A",
                "change_24h_raw": None,
                "change_7d": "N/A",
                "type": token_type,
            })

    divider_style = getattr(config, "divider_style", None)
    divider_line = (divider_style * 10) if divider_style else "--------------------------------"

    return {
        "timestamp": datetime.now(timezone.utc),
        "watchlist_name": watchlist.get("name", "Main"),
        "tokens": tokens_data,
        "divider_line": divider_line,
    }

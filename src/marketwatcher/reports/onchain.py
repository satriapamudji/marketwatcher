"""On-chain report builder for GeckoTerminal data."""

import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from marketwatcher.logging_config import get_logger
from marketwatcher.providers.geckoterminal import GeckoTerminalProvider, PoolData
from marketwatcher.config import ReportConfig, OnchainConfig

logger = get_logger("reports")


NATIVE_SYMBOLS_BY_NETWORK: dict[str, set[str]] = {
    "solana": {"SOL", "WSOL"},
    "base": {"ETH", "WETH"},
    "ethereum": {"ETH", "WETH"},
    "eth": {"ETH", "WETH"},
    "arbitrum": {"ETH", "WETH"},
    "optimism": {"ETH", "WETH"},
    "linea": {"ETH", "WETH"},
    "scroll": {"ETH", "WETH"},
    "blast": {"ETH", "WETH"},
    "mantle": {"MNT", "WMNT"},
    "bsc": {"BNB", "WBNB"},
    "polygon": {"MATIC", "WMATIC", "POL", "WPOL"},
    "polygon_pos": {"MATIC", "WMATIC", "POL", "WPOL"},
    "avax": {"AVAX", "WAVAX"},
    "aptos": {"APT"},
    "sei": {"SEI", "WSEI"},
    "sei-network": {"SEI", "WSEI"},
    "ton": {"TON"},
}


def format_volume_usd(value: float) -> str:
    """Format volume with abbreviation."""
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.1f}K"
    else:
        return f"${value:.0f}"


def format_pct(value: float | None) -> str:
    """Format percentage with sign."""
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


def should_exclude_token(symbol: str, name: str, config: OnchainConfig) -> bool:
    """Check if a token should be excluded based on filters.

    Args:
        symbol: Token symbol
        name: Token name
        config: Onchain config with filters

    Returns:
        True if token should be excluded
    """
    # Check whitelist first
    if config.top_tokens:
        if symbol.upper() in [t.upper() for t in config.top_tokens]:
            return False  # Always include top tokens

    if symbol and symbol.upper() in {s.upper() for s in config.symbol_exclude_list}:
        return True

    # Check exclude patterns
    for pattern in config.name_exclude_patterns:
        try:
            # Check both symbol and name
            if re.search(pattern, symbol, re.IGNORECASE):
                logger.debug(f"Excluding {symbol} (pattern: {pattern})")
                return True
            if re.search(pattern, name, re.IGNORECASE):
                logger.debug(f"Excluding {name} (pattern: {pattern})")
                return True
        except re.error:
            pass  # Skip invalid patterns

    return False


def is_native_token(symbol: str, network: str, config: OnchainConfig) -> bool:
    """Return True if symbol is the chain native/wrapped native token."""
    if not config.exclude_native_tokens or not symbol:
        return False
    normalized = network.lower()
    return symbol.upper() in NATIVE_SYMBOLS_BY_NETWORK.get(normalized, set())


def shorten_ca(address: str, head: int = 6, tail: int = 4) -> str:
    """Shorten contract address for Telegram display."""
    if not address:
        return ""
    if len(address) <= head + tail + 3:
        return address
    return f"{address[:head]}...{address[-tail:]}"


def aggregate_tokens(pools: list[PoolData], config: OnchainConfig, network: str) -> list[dict]:
    """Aggregate pools by base token.

    Args:
        pools: List of pools
        config: Onchain config

    Returns:
        List of aggregated token data
    """
    token_data = defaultdict(lambda: {"volume": 0, "pools": 0, "liquidity": 0})

    for pool in pools:
        # Skip pools with low liquidity
        if pool.liquidity < config.min_liquidity_usd:
            continue

        # Skip pools with low volume
        if pool.volume_24h < config.min_volume_usd:
            continue

        # Skip excluded tokens
        if should_exclude_token(pool.base_token, pool.name, config):
            continue

        if is_native_token(pool.base_token, network, config):
            continue

        symbol = pool.base_token.upper()
        if not symbol:
            continue

        token_data[symbol]["volume"] += pool.volume_24h
        token_data[symbol]["liquidity"] = max(token_data[symbol]["liquidity"], pool.liquidity)
        token_data[symbol]["pools"] += 1
        token_data[symbol]["symbol"] = pool.base_token
        token_data[symbol]["name"] = pool.name
        token_data[symbol]["token_address"] = pool.base_token_address

    # Convert to list and sort by volume
    tokens = [
        {
            "symbol": data["symbol"],
            "name": data["name"],
            "volume_24h": data["volume"],
            "liquidity": data["liquidity"],
            "pool_count": data["pools"],
            "token_address": data.get("token_address", ""),
        }
        for symbol, data in token_data.items()
    ]

    return sorted(tokens, key=lambda x: x["volume_24h"], reverse=True)


def build_onchain_report(
    provider: GeckoTerminalProvider,
    network: str,
    config: ReportConfig,
    onchain_config: OnchainConfig | None = None,
    limit: int = 15,
    log_jsonl: bool = True,
) -> dict[str, Any]:
    """Build on-chain report for a network.

    Args:
        provider: GeckoTerminal provider
        network: Network to fetch (e.g., "solana", "base")
        config: Report configuration
        onchain_config: Onchain filters (optional)
        limit: Number of tokens/pools to include
        log_jsonl: Whether to log to JSONL

    Returns:
        Dict with report data ready for rendering
    """
    from marketwatcher.storage.jsonl import JsonlLogger

    logger.info(f"Building on-chain report for {network}")

    if onchain_config is None:
        onchain_config = OnchainConfig()

    gainers_count = max(1, int(onchain_config.top_gainers_count))
    losers_count = max(1, int(onchain_config.top_losers_count))
    movers_candidate_count = max(
        int(onchain_config.movers_candidate_count),
        gainers_count,
        losers_count,
    )

    # Get top pools by volume (overfetch before local filtering/deduping)
    pool_fetch_limit = max(20, int(onchain_config.pool_fetch_limit))
    pools = provider.get_pools(network, limit=pool_fetch_limit, sort_by="h24_volume_usd_desc")

    # Get network display name
    network_name = provider.get_network_name(network)

    # Aggregate by token
    tokens = aggregate_tokens(pools, onchain_config, network)

    # Take top N
    top_tokens = tokens[:limit]

    # Log to JSONL if enabled
    if log_jsonl:
        try:
            jsonl = JsonlLogger()
            jsonl.log({
                "network": network,
                "token_count": len(top_tokens),
                "tokens": [
                    {
                        "symbol": t["symbol"],
                        "volume_24h": t["volume_24h"],
                        "liquidity": t["liquidity"],
                    }
                    for t in top_tokens
                ],
            })
        except Exception as e:
            logger.warning(f"Failed to log to JSONL: {e}")

    # Format for display
    formatted_tokens = []
    for token in top_tokens:
        formatted_tokens.append({
            "symbol": token["symbol"],
            "name": token["name"],
            "token_address": token.get("token_address", ""),
            "token_address_short": shorten_ca(token.get("token_address", "")),
            "volume_24h": format_volume_usd(token["volume_24h"]),
            "volume_raw": token["volume_24h"],
            "liquidity": format_volume_usd(token["liquidity"]),
            "pool_count": token["pool_count"],
        })

    # Get top gainers/losers from individual pools (filtered)
    filtered_pools = [
        p for p in pools
        if p.liquidity >= onchain_config.min_liquidity_usd
        and p.volume_24h >= onchain_config.min_volume_usd
        and not should_exclude_token(p.base_token, p.name, onchain_config)
        and not is_native_token(p.base_token, network, onchain_config)
    ]

    # Deduplicate by token CA (or symbol fallback) so the lists read like token movers, not pool spam.
    seen_keys: set[str] = set()
    deduped_pools: list[PoolData] = []
    for pool in filtered_pools:
        dedupe_key = pool.base_token_address or pool.base_token.upper() or pool.pool_address
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped_pools.append(pool)

    positive_pools = [p for p in deduped_pools if p.price_change_24h is not None and p.price_change_24h > 0]
    negative_pools = [p for p in deduped_pools if p.price_change_24h is not None and p.price_change_24h < 0]

    gainers_candidates = sorted(
        positive_pools,
        key=lambda x: x.price_change_24h or -999,
        reverse=True,
    )[:movers_candidate_count]
    losers_candidates = sorted(
        negative_pools,
        key=lambda x: x.price_change_24h or 999,
    )[:movers_candidate_count]

    formatted_gainers = []
    for pool in gainers_candidates:
        formatted_gainers.append({
            "name": pool.name,
            "symbol": pool.base_token,
            "address": pool.pool_address,
            "token_address": pool.base_token_address,
            "token_address_short": shorten_ca(pool.base_token_address),
            "change": format_pct(pool.price_change_24h),
            "volume": format_volume_usd(pool.volume_24h),
            "liquidity": format_volume_usd(pool.liquidity),
            "mcap_label": "MCAP" if pool.market_cap is not None else "FDV",
            "mcap": format_volume_usd(pool.market_cap if pool.market_cap is not None else pool.fdv),
        })
        if len(formatted_gainers) >= gainers_count:
            break

    formatted_losers = []
    for pool in losers_candidates:
        formatted_losers.append({
            "name": pool.name,
            "symbol": pool.base_token,
            "address": pool.pool_address,
            "token_address": pool.base_token_address,
            "token_address_short": shorten_ca(pool.base_token_address),
            "change": format_pct(pool.price_change_24h),
            "volume": format_volume_usd(pool.volume_24h),
            "liquidity": format_volume_usd(pool.liquidity),
            "mcap_label": "MCAP" if pool.market_cap is not None else "FDV",
            "mcap": format_volume_usd(pool.market_cap if pool.market_cap is not None else pool.fdv),
        })
        if len(formatted_losers) >= losers_count:
            break

    return {
        "network": network,
        "network_name": network_name,
        "timestamp": datetime.now(timezone.utc),
        "tokens": formatted_tokens,
        "top_gainers": formatted_gainers,
        "top_losers": formatted_losers,
        "top_gainers_count": gainers_count,
        "top_losers_count": losers_count,
        "token_count": len(formatted_tokens),
    }

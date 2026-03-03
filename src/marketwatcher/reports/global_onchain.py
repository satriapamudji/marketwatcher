"""Global on-chain report builder using DefiLlama data.

Produces a DeFi overview with:
- Total DeFi TVL + 1D/7D/14D changes
- 24h DEX volume
- Stablecoin supply
- TVL by chain (top N)
- TVL chain gainers/losers (7D)
"""

from datetime import datetime, timezone
from typing import Any

from marketwatcher.config import ReportConfig
from marketwatcher.logging_config import get_logger
from marketwatcher.providers.defillama import DefiLlamaProvider

logger = get_logger("reports")


def _format_usd(value: float) -> str:
    """Format USD value with abbreviation."""
    if value >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    elif value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.1f}K"
    else:
        return f"${value:.0f}"


def _format_pct(value: float | None) -> str:
    """Format percentage with sign."""
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


def _tvl_at_days_ago(history: list[dict], days: int) -> float | None:
    """Get TVL value from N days ago in a historical series.

    History entries have {date: unix_timestamp, tvl: float}.
    """
    if not history:
        return None

    # History is chronological; grab from the end
    target_idx = len(history) - 1 - days
    if target_idx < 0:
        return None

    return history[target_idx].get("tvl")


def _pct_change(current: float, previous: float | None) -> float | None:
    """Calculate percentage change."""
    if previous is None or previous == 0:
        return None
    return ((current - previous) / previous) * 100


def build_global_onchain_report(
    provider: DefiLlamaProvider,
    config: ReportConfig,
    top_chains_count: int = 5,
    tvl_movers_count: int = 5,
) -> dict[str, Any]:
    """Build global on-chain report.

    Args:
        provider: DefiLlama provider instance
        config: Report configuration
        top_chains_count: Number of top chains by TVL to show
        tvl_movers_count: Number of TVL gainers/losers to show

    Returns:
        Dict with report data ready for rendering
    """
    logger.info("Building global on-chain report")

    # 1. Get current chain TVL data
    chains = provider.get_chains_tvl()
    total_tvl = sum(c["tvl"] for c in chains)

    # 2. Get global TVL history for total DeFi TVL deltas
    global_history = provider.get_global_tvl_history()
    current_global_tvl = global_history[-1]["tvl"] if global_history else total_tvl

    tvl_1d_prev = _tvl_at_days_ago(global_history, 1)
    tvl_7d_prev = _tvl_at_days_ago(global_history, 7)
    tvl_14d_prev = _tvl_at_days_ago(global_history, 14)

    tvl_1d_change = _pct_change(current_global_tvl, tvl_1d_prev)
    tvl_7d_change = _pct_change(current_global_tvl, tvl_7d_prev)
    tvl_14d_change = _pct_change(current_global_tvl, tvl_14d_prev)

    # 3. Get DEX overview
    dex = provider.get_dex_overview()

    # 4. Get stablecoin data
    stablecoin_chains = provider.get_stablecoin_chains()
    total_stablecoin = sum(c["stablecoin_mcap"] for c in stablecoin_chains)

    # 5. Top chains by TVL with dominance
    top_chains = []
    for chain in chains[:top_chains_count]:
        dominance = (chain["tvl"] / total_tvl * 100) if total_tvl > 0 else 0
        top_chains.append({
            "name": chain["name"],
            "tvl": _format_usd(chain["tvl"]),
            "tvl_raw": chain["tvl"],
            "dominance": f"{dominance:.1f}%",
        })

    # 6. Get 7D TVL changes per chain for gainers/losers
    #    Fetch history for a broader set of chains to find movers
    chain_changes: list[dict] = []
    candidate_chains = chains[:30]  # Top 30 by TVL for mover candidates

    for chain in candidate_chains:
        try:
            history = provider.get_historical_tvl(chain["name"])
            if not history or len(history) < 8:
                continue

            current = history[-1]["tvl"]
            prev_7d = _tvl_at_days_ago(history, 7)
            change = _pct_change(current, prev_7d)

            if change is not None:
                chain_changes.append({
                    "name": chain["name"],
                    "change_raw": change,
                    "change": _format_pct(change),
                    "tvl": _format_usd(current),
                })
        except Exception as e:
            logger.warning(f"Failed to get history for {chain['name']}: {e}")
            continue

    # Sort for gainers (biggest positive) and losers (biggest negative)
    gainers = sorted(
        [c for c in chain_changes if c["change_raw"] > 0],
        key=lambda x: x["change_raw"],
        reverse=True,
    )[:tvl_movers_count]

    losers = sorted(
        [c for c in chain_changes if c["change_raw"] < 0],
        key=lambda x: x["change_raw"],
    )[:tvl_movers_count]

    divider_style = getattr(config, "divider_style", None)
    divider_line = (divider_style * 10) if divider_style else "--------------------------------"

    return {
        "timestamp": datetime.now(timezone.utc),
        "total_tvl": _format_usd(current_global_tvl),
        "total_tvl_raw": current_global_tvl,
        "tvl_1d": _format_pct(tvl_1d_change),
        "tvl_7d": _format_pct(tvl_7d_change),
        "tvl_14d": _format_pct(tvl_14d_change),
        "dex_volume_24h": _format_usd(dex["total_24h"]),
        "dex_change_1d": _format_pct(dex.get("change_1d")),
        "dex_change_7d": _format_pct(dex.get("change_7d")),
        "stablecoin_supply": _format_usd(total_stablecoin),
        "top_chains": top_chains,
        "tvl_gainers": gainers,
        "tvl_losers": losers,
        "divider_line": divider_line,
    }

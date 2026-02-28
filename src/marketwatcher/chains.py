"""Chain list management for GeckoTerminal.

Fetches and caches the list of available chains from GeckoTerminal API.
Cache is refreshed every 14 days.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from marketwatcher.logging_config import get_logger

logger = get_logger("chains")

# Cache file path (relative to project root)
CACHE_FILENAME = "chains.json"
CACHE_MAX_AGE_DAYS = 14

# Popular chains shown first in the UI
POPULAR_CHAINS = [
    ("solana", "Solana"),
    ("base", "Base"),
    ("eth", "Ethereum"),
    ("arbitrum", "Arbitrum"),
    ("polygon_pos", "Polygon"),
    ("bsc", "BNB Chain"),
    ("avax", "Avalanche"),
    ("optimism", "Optimism"),
]


def _get_cache_path() -> Path:
    """Get the cache file path."""
    # Find project root (where config/ lives)
    import marketwatcher
    module_path = Path(marketwatcher.__file__).resolve()
    project_root = module_path.parent.parent.parent
    return project_root / "config" / CACHE_FILENAME


def _fetch_chains_from_api() -> list[dict[str, str]]:
    """Fetch chains from GeckoTerminal API."""
    url = "https://api.geckoterminal.com/api/v2/networks"

    try:
        with httpx.Client(timeout=30) as client:
            response = client.get(url)
            response.raise_for_status()
            data = response.json()

        chains = []
        for item in data.get("data", []):
            attrs = item.get("attributes", {})
            chain_id = item.get("id", "")
            name = attrs.get("name", chain_id)
            if chain_id:
                chains.append({
                    "id": chain_id,
                    "name": name,
                })

        logger.info(f"Fetched {len(chains)} chains from GeckoTerminal")
        return chains

    except Exception as e:
        logger.error(f"Failed to fetch chains: {e}")
        raise


def _load_cache() -> dict[str, Any] | None:
    """Load cached chains data."""
    cache_path = _get_cache_path()
    if not cache_path.exists():
        return None

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load chains cache: {e}")
        return None


def _save_cache(chains: list[dict[str, str]]) -> None:
    """Save chains to cache."""
    cache_path = _get_cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "chains": chains,
    }

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Saved {len(chains)} chains to cache")


def _is_cache_stale(cache_data: dict[str, Any]) -> bool:
    """Check if cache is older than CACHE_MAX_AGE_DAYS."""
    try:
        fetched_at = datetime.fromisoformat(cache_data["fetched_at"])
        age = (datetime.now(timezone.utc) - fetched_at).days
        return age >= CACHE_MAX_AGE_DAYS
    except Exception:
        return True


def get_chains(force_refresh: bool = False) -> list[dict[str, str]]:
    """Get list of chains, using cache if fresh.

    Args:
        force_refresh: If True, always fetch from API

    Returns:
        List of {"id": ..., "name": ...} dicts
    """
    # Try cache first
    if not force_refresh:
        cache = _load_cache()
        if cache and not _is_cache_stale(cache):
            return cache.get("chains", [])

    # Fetch from API
    try:
        chains = _fetch_chains_from_api()
        _save_cache(chains)
        return chains
    except Exception:
        # If API fails, return cached data even if stale
        if cache:
            logger.warning("Using stale cache due to API failure")
            return cache.get("chains", [])
        # Fall back to popular chains
        logger.warning("Using fallback chain list")
        return [{"id": cid, "name": name} for cid, name in POPULAR_CHAINS]


def refresh_chains() -> bool:
    """Force refresh the chain cache.

    Returns:
        True if successful
    """
    try:
        chains = _fetch_chains_from_api()
        _save_cache(chains)
        return True
    except Exception as e:
        logger.error(f"Failed to refresh chains: {e}")
        return False


def get_chain_ids() -> list[str]:
    """Get list of chain IDs only."""
    return [c["id"] for c in get_chains()]


def find_chain(query: str) -> list[dict[str, str]]:
    """Search for chains matching query.

    Args:
        query: Search string (matches ID or name)

    Returns:
        List of matching chains, sorted by relevance
    """
    chains = get_chains()
    query = query.lower().strip()

    if not query:
        # Return popular chains first, then rest
        popular_ids = {c[0] for c in POPULAR_CHAINS}
        result = [{"id": c[0], "name": c[1]} for c in POPULAR_CHAINS]
        result.extend(c for c in chains if c["id"] not in popular_ids)
        return result

    # Filter by query
    matches = []
    for chain in chains:
        cid = chain["id"].lower()
        name = chain["name"].lower()

        # Exact match on ID
        if cid == query:
            return [chain]

        # Partial matches
        if query in cid or query in name:
            matches.append(chain)

    return matches


def get_cache_age_days() -> int | None:
    """Get age of cache in days, or None if no cache."""
    cache = _load_cache()
    if not cache:
        return None

    try:
        fetched_at = datetime.fromisoformat(cache["fetched_at"])
        return (datetime.now(timezone.utc) - fetched_at).days
    except Exception:
        return None

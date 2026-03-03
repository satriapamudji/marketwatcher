"""Watchlist storage for user-managed token lists.

Persists watchlists to config/watchlists.yaml.
Each watchlist has an id, name, and list of token entries.

Token entry fields:
- symbol (required): display symbol e.g. "BTC"
- type: "cex" (CoinGecko) or "dex" (GeckoTerminal)
- coingecko_id: for CEX tokens e.g. "bitcoin"
- chain + address: for DEX tokens (contract address on a chain)

Alert fields (optional, per-token):
- alert_above: float, USD price ceiling
- alert_below: float, USD price floor
- alert_pct: float, ±% 24h change threshold

Watchlist-level alert defaults:
- alert_pct: float, default ±% threshold for all tokens
- alert_chat_id: str, optional separate Telegram channel for alerts
"""

from pathlib import Path
from typing import Any

import yaml

from marketwatcher.logging_config import get_logger

logger = get_logger("watchlist")

DEFAULT_WATCHLIST = {
    "watchlists": [
        {
            "id": "main",
            "name": "Main",
            "tokens": [],
        }
    ]
}


def _watchlists_path(config_dir: Path | None = None) -> Path:
    if config_dir is None:
        config_dir = Path(__file__).parent.parent.parent / "config"
    return config_dir / "watchlists.yaml"


def load_watchlists(config_dir: Path | None = None) -> dict[str, Any]:
    """Load watchlists from YAML. Creates default file if missing."""
    path = _watchlists_path(config_dir)

    if not path.exists():
        save_watchlists(DEFAULT_WATCHLIST, config_dir)
        return DEFAULT_WATCHLIST

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if "watchlists" not in data:
        data["watchlists"] = DEFAULT_WATCHLIST["watchlists"]

    return data


def save_watchlists(data: dict[str, Any], config_dir: Path | None = None) -> None:
    """Save watchlists to YAML."""
    path = _watchlists_path(config_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)

    logger.info(f"Saved watchlists to {path}")


def get_watchlist(watchlist_id: str = "main", config_dir: Path | None = None) -> dict:
    """Get a specific watchlist by ID.

    Returns dict with 'id', 'name', 'tokens' keys.
    Creates default watchlist if not found.
    """
    data = load_watchlists(config_dir)

    for wl in data.get("watchlists", []):
        if wl.get("id") == watchlist_id:
            return wl

    # Create the watchlist if it doesn't exist
    new_wl = {"id": watchlist_id, "name": watchlist_id.title(), "tokens": []}
    data["watchlists"].append(new_wl)
    save_watchlists(data, config_dir)
    return new_wl


def list_watchlists(config_dir: Path | None = None) -> list[dict]:
    """List all watchlists (id and name only)."""
    data = load_watchlists(config_dir)
    return [{"id": wl["id"], "name": wl["name"], "count": len(wl.get("tokens", []))}
            for wl in data.get("watchlists", [])]


def create_watchlist(
    watchlist_id: str,
    name: str = "",
    config_dir: Path | None = None,
) -> bool:
    """Create a new watchlist. Returns True if created, False if ID already exists."""
    data = load_watchlists(config_dir)
    for wl in data.get("watchlists", []):
        if wl.get("id") == watchlist_id:
            return False
    data.setdefault("watchlists", []).append({
        "id": watchlist_id,
        "name": name or watchlist_id.title(),
        "tokens": [],
    })
    save_watchlists(data, config_dir)
    logger.info(f"Created watchlist {watchlist_id}")
    return True


def delete_watchlist(
    watchlist_id: str,
    config_dir: Path | None = None,
) -> bool:
    """Delete a watchlist. Returns True if deleted."""
    data = load_watchlists(config_dir)
    original = data.get("watchlists", [])
    data["watchlists"] = [wl for wl in original if wl.get("id") != watchlist_id]
    if len(data["watchlists"]) < len(original):
        save_watchlists(data, config_dir)
        logger.info(f"Deleted watchlist {watchlist_id}")
        return True
    return False


def add_token(
    watchlist_id: str,
    symbol: str,
    token_type: str = "cex",
    coingecko_id: str = "",
    chain: str = "",
    address: str = "",
    config_dir: Path | None = None,
) -> bool:
    """Add a token to a watchlist. Returns True if added, False if already exists."""
    data = load_watchlists(config_dir)

    # Find or create watchlist
    target = None
    for wl in data.get("watchlists", []):
        if wl.get("id") == watchlist_id:
            target = wl
            break

    if target is None:
        target = {"id": watchlist_id, "name": watchlist_id.title(), "tokens": []}
        data.setdefault("watchlists", []).append(target)

    tokens = target.setdefault("tokens", [])

    # Check for duplicate
    for t in tokens:
        if t.get("symbol", "").upper() == symbol.upper():
            logger.warning(f"{symbol} already in watchlist {watchlist_id}")
            return False

    entry: dict[str, str] = {"symbol": symbol.upper(), "type": token_type}
    if token_type == "cex" and coingecko_id:
        entry["coingecko_id"] = coingecko_id
    elif token_type == "dex":
        entry["chain"] = chain
        entry["address"] = address

    tokens.append(entry)
    save_watchlists(data, config_dir)
    logger.info(f"Added {symbol} to watchlist {watchlist_id}")
    return True


def remove_token(
    watchlist_id: str,
    symbol: str,
    config_dir: Path | None = None,
) -> bool:
    """Remove a token from a watchlist. Returns True if removed."""
    data = load_watchlists(config_dir)

    for wl in data.get("watchlists", []):
        if wl.get("id") == watchlist_id:
            tokens = wl.get("tokens", [])
            original_len = len(tokens)
            wl["tokens"] = [t for t in tokens if t.get("symbol", "").upper() != symbol.upper()]

            if len(wl["tokens"]) < original_len:
                save_watchlists(data, config_dir)
                logger.info(f"Removed {symbol} from watchlist {watchlist_id}")
                return True

    logger.warning(f"{symbol} not found in watchlist {watchlist_id}")
    return False


def set_token_alerts(
    watchlist_id: str,
    symbol: str,
    alert_above: float | None = None,
    alert_below: float | None = None,
    alert_pct: float | None = None,
    config_dir: Path | None = None,
) -> bool:
    """Set alert thresholds on a token. Returns True if token found and updated."""
    data = load_watchlists(config_dir)

    for wl in data.get("watchlists", []):
        if wl.get("id") != watchlist_id:
            continue
        for token in wl.get("tokens", []):
            if token.get("symbol", "").upper() != symbol.upper():
                continue
            # Set or remove each threshold
            if alert_above is not None:
                token["alert_above"] = alert_above
            if alert_below is not None:
                token["alert_below"] = alert_below
            if alert_pct is not None:
                token["alert_pct"] = alert_pct
            save_watchlists(data, config_dir)
            logger.info(f"Set alerts for {symbol} in {watchlist_id}")
            return True

    logger.warning(f"{symbol} not found in watchlist {watchlist_id}")
    return False


def clear_token_alerts(
    watchlist_id: str,
    symbol: str,
    config_dir: Path | None = None,
) -> bool:
    """Remove all alert thresholds from a token. Returns True if found."""
    data = load_watchlists(config_dir)

    for wl in data.get("watchlists", []):
        if wl.get("id") != watchlist_id:
            continue
        for token in wl.get("tokens", []):
            if token.get("symbol", "").upper() != symbol.upper():
                continue
            token.pop("alert_above", None)
            token.pop("alert_below", None)
            token.pop("alert_pct", None)
            save_watchlists(data, config_dir)
            logger.info(f"Cleared alerts for {symbol} in {watchlist_id}")
            return True

    return False


def set_watchlist_alerts(
    watchlist_id: str,
    alert_pct: float | None = None,
    alert_chat_id: str | None = None,
    config_dir: Path | None = None,
) -> bool:
    """Set watchlist-level alert defaults. Returns True if watchlist found."""
    data = load_watchlists(config_dir)

    for wl in data.get("watchlists", []):
        if wl.get("id") != watchlist_id:
            continue
        if alert_pct is not None:
            wl["alert_pct"] = alert_pct
        if alert_chat_id is not None:
            if alert_chat_id:
                wl["alert_chat_id"] = alert_chat_id
            else:
                wl.pop("alert_chat_id", None)
        save_watchlists(data, config_dir)
        logger.info(f"Set watchlist alerts for {watchlist_id}")
        return True

    return False

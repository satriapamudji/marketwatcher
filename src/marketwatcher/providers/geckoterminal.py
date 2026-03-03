"""GeckoTerminal API provider for on-chain data.

Handles:
- Network/pool listings
- Trending/new pools by chain
- Rate limiting and caching
"""

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from marketwatcher.logging_config import get_logger

logger = get_logger("geckoterminal")


# Known network IDs
NETWORKS = {
    "solana": "solana",
    "ethereum": "eth",
    "base": "base",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "bsc": "bsc",
    "polygon": "polygon_pos",
    "avax": "avax",
    "aptos": "aptos",
    "sei": "sei-network",
    "ton": "ton",
    "scroll": "scroll",
    "linea": "linea",
    "mantle": "mantle",
    "blast": "merlin-chain",
}


@dataclass
class PoolData:
    """On-chain pool data."""

    pool_address: str
    name: str
    base_token: str
    quote_token: str
    base_token_address: str = ""  # Contract address
    volume_24h: float = 0
    liquidity: float = 0
    market_cap: float | None = None
    price_change_24h: float | None = None
    fdv: float = 0
    dex: str = ""
    network: str = ""
    price_usd: float | None = None
    pool_created_at: str | None = None


class GeckoTerminalProvider:
    """GeckoTerminal API client."""

    base_url = "https://api.geckoterminal.com/api/v2"
    cache_ttl = 300
    timeout = 30

    def __init__(self, cache_ttl: int = 300, timeout: int = 30):
        self.cache_ttl = cache_ttl
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout)
        self._cache: dict[str, tuple[datetime, Any]] = {}

    def _get_cached(self, key: str) -> Any | None:
        """Get cached response if fresh."""
        if key in self._cache:
            cached_time, cached_data = self._cache[key]
            age = (datetime.now(timezone.utc) - cached_time).total_seconds()
            if age < self.cache_ttl:
                logger.debug(f"Cache hit for {key} (age: {age:.1f}s)")
                return cached_data
            else:
                del self._cache[key]
        return None

    def _set_cached(self, key: str, data: Any) -> None:
        """Cache a response."""
        self._cache[key] = (datetime.now(timezone.utc), data)

    def _request(self, endpoint: str, params: dict | None = None) -> dict:
        """Make HTTP request."""
        cache_key = f"{endpoint}:{str(params)}"

        # Check cache
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            age = (datetime.now(timezone.utc) - cached_time).total_seconds()
            if age < self.cache_ttl:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_data

        # Rate limit - be respectful
        time.sleep(1.0)

        url = f"{self.base_url}{endpoint}"
        logger.debug(f"Requesting {url} with params {params}")

        try:
            response = self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Cache the response
            self._cache[cache_key] = (datetime.now(timezone.utc), data)
            return data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e}")
            raise
        except httpx.TimeoutException:
            logger.error("Request timed out")
            raise

    def get_pools(
        self,
        network: str,
        limit: int = 10,
        sort_by: str = "h24_volume_usd_desc",
        include: str = "base_token,quote_token,dex",
        max_pages: int = 2,
    ) -> list[PoolData]:
        """Get pools for a network.

        Args:
            network: Network ID (e.g., "solana", "eth", "base")
            limit: Number of pools to return
            sort_by: Sort order (h24_volume_usd_desc, h24_tx_count_desc)
            include: Related data to include

        Returns:
            List of PoolData objects
        """
        # Map network name to ID
        network_id = NETWORKS.get(network.lower(), network)

        params = {
            "page": 1,
            "limit": min(limit, 100),
            "order_by": sort_by,
            "include": include,
        }

        logger.info(f"Fetching top pools for {network}")
        pools: list[PoolData] = []
        seen_pool_addresses: set[str] = set()
        max_pages = max(1, int(max_pages))

        for page in range(1, max_pages + 1):
            params["page"] = page
            data = self._request(f"/networks/{network_id}/pools", params)
            page_items = data.get("data", [])
            if not page_items:
                break

            # Build token lookup from included
            token_lookup = {}
            for item in data.get("included", []):
                if item.get("type") in ("Token", "token"):
                    attrs = item.get("attributes", {})
                    token_lookup[item.get("id")] = {
                        "symbol": attrs.get("symbol", ""),
                        "name": attrs.get("name", ""),
                        "address": item.get("id", ""),
                    }

            # Parse pools
            for pool in page_items:
                attrs = pool.get("attributes", {})

                # Get token symbols from relationships
                rels = pool.get("relationships", {})
                base_id = rels.get("base_token", {}).get("data", {}).get("id")
                quote_id = rels.get("quote_token", {}).get("data", {}).get("id")

                base_token = token_lookup.get(base_id, {}).get("symbol", "")
                quote_token = token_lookup.get(quote_id, {}).get("symbol", "")
                # Strip network prefix (e.g., "solana_7nePAc..." -> "7nePAc...")
                full_addr = token_lookup.get(base_id, {}).get("address", "")
                base_token_address = full_addr.split("_", 1)[1] if "_" in full_addr else full_addr

                # Extract volume (nested dict)
                volume_data = attrs.get("volume_usd", {})
                volume_24h = float(volume_data.get("h24", 0) or 0)

                # Extract price change (nested dict)
                price_change_data = attrs.get("price_change_percentage", {})
                price_change_24h = None
                if isinstance(price_change_data, dict):
                    try:
                        price_change_24h = float(price_change_data.get("h24", 0) or 0)
                    except (ValueError, TypeError):
                        price_change_24h = None

                full_pool_id = pool.get("id", "")
                pool_address = full_pool_id.split("_", 1)[1] if "_" in full_pool_id else full_pool_id
                if not pool_address or pool_address in seen_pool_addresses:
                    continue
                seen_pool_addresses.add(pool_address)

                pools.append(PoolData(
                    pool_address=pool_address,
                    name=attrs.get("name", f"{base_token}/{quote_token}"),
                    base_token=base_token,
                    quote_token=quote_token,
                    base_token_address=base_token_address,
                    volume_24h=volume_24h,
                    liquidity=float(attrs.get("reserve_in_usd", 0) or 0),
                    market_cap=float(attrs.get("market_cap_usd")) if attrs.get("market_cap_usd") is not None else None,
                    price_change_24h=price_change_24h,
                    fdv=float(attrs.get("fdv_usd", 0) or 0),
                    dex=attrs.get("name", "").split("/")[-1].strip() if "/" in attrs.get("name", "") else "",
                    network=network,
                    pool_created_at=attrs.get("pool_created_at"),
                ))

                if len(pools) >= limit:
                    return pools[:limit]

        return pools[:limit]

    def search_token(self, address: str) -> dict | None:
        """Search for a token by contract address across all chains.

        Returns dict with 'symbol', 'name', 'chain', 'address' or None if not found.
        """
        logger.info(f"Searching for token {address[:10]}...")
        time.sleep(0.5)

        url = f"{self.base_url}/search/pools"
        params = {"query": address, "include": "base_token"}

        try:
            response = self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"Token search failed: {e}")
            return None

        # Find the matching token in included data
        addr_lower = address.lower()
        for item in data.get("included", []):
            if item.get("type") != "token":
                continue
            token_addr = item.get("attributes", {}).get("address", "")
            if token_addr.lower() == addr_lower:
                token_id = item.get("id", "")  # e.g. "base_0xacfe..."
                chain = token_id.split("_")[0] if "_" in token_id else ""
                return {
                    "symbol": item["attributes"].get("symbol", ""),
                    "name": item["attributes"].get("name", ""),
                    "chain": chain,
                    "address": token_addr,
                }

        # Fallback: parse from pool data if token not in included
        pools = data.get("data", [])
        if pools:
            pool_id = pools[0].get("id", "")
            chain = pool_id.split("_")[0] if "_" in pool_id else ""
            pool_name = pools[0].get("attributes", {}).get("name", "")
            symbol = pool_name.split("/")[0].strip() if "/" in pool_name else pool_name
            return {
                "symbol": symbol,
                "name": "",
                "chain": chain,
                "address": address,
            }

        return None

    def get_network_name(self, network: str) -> str:
        """Get display name for a network."""
        # Just return the input as-is for now - API rate limits
        # Could enhance later to fetch from API
        return network.capitalize()

    def get_token_pools(self, network: str, address: str, limit: int = 5) -> list['PoolData']:
        """Get top pools for a specific token address on a chain.

        Args:
            network: Network ID (e.g. "solana", "base")
            address: Token contract address

        Returns:
            List of PoolData for pools containing this token
        """
        net_id = NETWORKS.get(network, network)
        logger.info(f"Fetching pools for token {address[:10]}... on {net_id}")

        endpoint = f"/networks/{net_id}/tokens/{address}/pools"
        cache_key = f"{endpoint}:top{limit}"

        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        time.sleep(0.5)
        url = f"{self.base_url}{endpoint}"
        params = {"page": "1"}

        try:
            response = self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch token pools: {e}")
            return []

        pools = []
        for item in data.get("data", [])[:limit]:
            attrs = item.get("attributes", {})
            name = attrs.get("name", "")
            base_token = name.split("/")[0].strip() if "/" in name else name

            price_change = None
            price_changes = attrs.get("price_change_percentage", {})
            if isinstance(price_changes, dict):
                raw = price_changes.get("h24")
                if raw is not None:
                    try:
                        price_change = float(raw)
                    except (ValueError, TypeError):
                        pass

            volume = 0
            vol_data = attrs.get("volume_usd", {})
            if isinstance(vol_data, dict):
                try:
                    volume = float(vol_data.get("h24", 0) or 0)
                except (ValueError, TypeError):
                    pass

            try:
                liquidity = float(attrs.get("reserve_in_usd", 0) or 0)
            except (ValueError, TypeError):
                liquidity = 0

            fdv = 0
            try:
                fdv = float(attrs.get("fdv_usd", 0) or 0)
            except (ValueError, TypeError):
                pass

            market_cap = None
            try:
                mc = attrs.get("market_cap_usd")
                if mc is not None:
                    market_cap = float(mc)
            except (ValueError, TypeError):
                pass

            # Extract base token price
            price_usd = None
            try:
                raw_price = attrs.get("base_token_price_usd")
                if raw_price is not None:
                    price_usd = float(raw_price)
            except (ValueError, TypeError):
                pass

            pool = PoolData(
                pool_address=item.get("id", "").split("_")[-1] if "_" in item.get("id", "") else item.get("id", ""),
                name=name,
                base_token=base_token,
                quote_token=name.split("/")[1].strip() if "/" in name else "",
                base_token_address=address,
                volume_24h=volume,
                liquidity=liquidity,
                market_cap=market_cap,
                price_change_24h=price_change,
                fdv=fdv,
                price_usd=price_usd,
                network=net_id,
            )
            pools.append(pool)

        self._set_cached(cache_key, pools)
        return pools

    def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

"""DefiLlama API provider for on-chain DeFi data.

Free API, no key required.
Docs: https://defillama.com/docs/api

Endpoints used:
- /v2/chains              → Current TVL per chain
- /v2/historicalChainTvl  → Historical total DeFi TVL (daily)
- /v2/historicalChainTvl/{chain} → Historical TVL for a specific chain
- /overview/dexs          → Aggregated DEX volumes
- stablecoins.llama.fi/stablecoinchains → Stablecoin supply per chain
"""

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from marketwatcher.logging_config import get_logger

logger = get_logger("defillama")

STABLECOINS_BASE = "https://stablecoins.llama.fi"


@dataclass
class DefiLlamaProvider:
    """DefiLlama API client."""

    base_url: str = "https://api.llama.fi"
    cache_ttl: int = 300
    timeout: int = 30
    retry_count: int = 3
    backoff_factor: float = 1.5

    _client: httpx.Client | None = None
    _cache: dict[str, tuple[datetime, Any]] | None = None

    def __post_init__(self):
        self._cache = {}

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=self.timeout,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "MarketWatcher/0.1.0",
                },
            )
        return self._client

    def _get_cached(self, key: str) -> Any | None:
        if self._cache is None:
            return None
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
        if self._cache is not None:
            self._cache[key] = (datetime.now(timezone.utc), data)

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _request(self, url: str, params: dict | None = None) -> Any:
        """Make HTTP request with retry logic."""
        cache_key = f"{url}:{str(params)}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        logger.debug(f"Requesting {url}")
        time.sleep(0.5)  # Light rate limit

        response = self.client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        self._set_cached(cache_key, data)
        return data

    def get_chains_tvl(self) -> list[dict[str, Any]]:
        """Get current TVL for all chains.

        Returns list of {name, tvl, tokenSymbol, gecko_id, chainId}.
        """
        logger.info("Fetching chains TVL")
        data = self._request(f"{self.base_url}/v2/chains")

        chains = []
        for entry in data:
            tvl = entry.get("tvl", 0)
            if tvl and tvl > 0:
                chains.append({
                    "name": entry.get("name", ""),
                    "tvl": tvl,
                    "token_symbol": entry.get("tokenSymbol"),
                    "gecko_id": entry.get("gecko_id"),
                })

        chains.sort(key=lambda x: x["tvl"], reverse=True)
        logger.info(f"Got TVL for {len(chains)} chains")
        return chains

    def get_historical_tvl(self, chain: str) -> list[dict[str, Any]]:
        """Get historical daily TVL for a specific chain.

        Returns list of {date (unix timestamp), tvl}.
        """
        logger.info(f"Fetching historical TVL for {chain}")
        data = self._request(f"{self.base_url}/v2/historicalChainTvl/{chain}")

        points = []
        for entry in data:
            points.append({
                "date": entry.get("date", 0),
                "tvl": entry.get("tvl", 0),
            })

        return points

    def get_global_tvl_history(self) -> list[dict[str, Any]]:
        """Get historical total DeFi TVL across all chains.

        Returns list of {date (unix timestamp), tvl}.
        """
        logger.info("Fetching global TVL history")
        data = self._request(f"{self.base_url}/v2/historicalChainTvl")

        points = []
        for entry in data:
            points.append({
                "date": entry.get("date", 0),
                "tvl": entry.get("tvl", 0),
            })

        return points

    def get_dex_overview(self) -> dict[str, Any]:
        """Get aggregated DEX volume overview.

        Returns dict with totalDataChart, total24h, total48hto24h, etc.
        """
        logger.info("Fetching DEX overview")
        data = self._request(f"{self.base_url}/overview/dexs")

        return {
            "total_24h": data.get("total24h", 0),
            "total_48h_to_24h": data.get("total48hto24h", 0),
            "total_7d": data.get("total7d", 0),
            "total_30d": data.get("total30d", 0),
            "change_1d": data.get("change_1d"),
            "change_7d": data.get("change_7d"),
            "change_1m": data.get("change_1m"),
        }

    def get_stablecoin_chains(self) -> list[dict[str, Any]]:
        """Get stablecoin supply per chain.

        Returns list of {name, totalCirculatingUSD}.
        """
        logger.info("Fetching stablecoin chains")
        data = self._request(f"{STABLECOINS_BASE}/stablecoinchains")

        chains = []
        for entry in data:
            circulating = entry.get("totalCirculatingUSD", {})
            usd_value = circulating.get("peggedUSD", 0) if isinstance(circulating, dict) else 0
            if usd_value and usd_value > 0:
                chains.append({
                    "name": entry.get("name", ""),
                    "stablecoin_mcap": usd_value,
                    "gecko_id": entry.get("gecko_id"),
                })

        chains.sort(key=lambda x: x["stablecoin_mcap"], reverse=True)
        logger.info(f"Got stablecoin data for {len(chains)} chains")
        return chains

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

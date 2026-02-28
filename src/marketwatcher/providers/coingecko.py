"""CoinGecko API provider for market data.

Handles:
- HTTP client with timeouts and retries
- Rate limiting
- Response parsing
- Cache management
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

logger = get_logger("coingecko")


@dataclass
class CoinGeckoProvider:
    """CoinGecko API client."""

    base_url: str = "https://api.coingecko.com/api/v3"
    cache_ttl: int = 300
    timeout: int = 30
    retry_count: int = 3
    backoff_factor: float = 1.5

    _client: httpx.Client | None = None
    _cache: dict[str, tuple[datetime, Any]] | None = None

    def __post_init__(self):
        """Initialize client and cache."""
        self._cache = {}

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "MarketWatcher/0.1.0",
                },
            )
        return self._client

    def _get_cached(self, key: str) -> Any | None:
        """Get cached response if fresh."""
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
        """Cache a response."""
        if self._cache is not None:
            self._cache[key] = (datetime.now(timezone.utc), data)

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _request(self, endpoint: str, params: dict | None = None) -> dict:
        """Make HTTP request with retry logic."""
        # Check cache first
        cache_key = f"{endpoint}:{str(params)}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Rate limiting - simple sleep before request
        logger.debug(f"Requesting {endpoint} with params {params}")
        time.sleep(1.1)  # Conservative rate limit

        try:
            response = self.client.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            # Cache the response
            self._set_cached(cache_key, data)
            return data

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limited by CoinGecko, waiting...")
                time.sleep(5)  # Extra backoff
            raise
        except httpx.TimeoutException:
            logger.warning("Request timed out, retrying...")
            raise

    def get_global_metrics(self) -> dict[str, float]:
        """Fetch global market metrics.

        Returns:
            Dict with:
            - total_market_cap: Total market cap in USD
            - btc_dominance: BTC dominance percentage
            - total_volume: Total 24h volume
            - market_cap_change_24h: 24h change percentage
        """
        logger.info("Fetching global metrics")
        data = self._request("/global")

        global_data = data.get("data", {})

        # Get market cap changes - field name is market_cap_change_percentage_24h_usd
        market_cap_change_24h = global_data.get("market_cap_change_percentage_24h_usd")

        return {
            "total_market_cap": global_data.get("total_market_cap", {}).get("usd", 0),
            "btc_dominance": global_data.get("market_cap_percentage", {}).get("btc", 0),
            "total_volume": global_data.get("total_volume", {}).get("usd", 0),
            "market_cap_change_24h": market_cap_change_24h,
        }

    def get_categories(self) -> list[dict[str, Any]]:
        """Fetch all categories with market data.

        Returns:
            List of category dicts with:
            - id: Category ID
            - name: Category name
            - market_cap_usd: Market cap in USD
            - pct_change_24h: 24h percentage change
        """
        logger.info("Fetching categories")
        data = self._request("/coins/categories")

        # Debug: log first item keys
        if data:
            logger.debug(f"First category keys: {list(data[0].keys())[:10]}")

        categories = []
        for cat in data:
            # Skip categories without market cap data - check various possible field names
            market_cap = cat.get("market_cap_usd") or cat.get("market_cap") or cat.get("market_cap_usd", 0)
            if market_cap is None or market_cap == 0:
                continue

            categories.append({
                "id": cat.get("id", ""),
                "name": cat.get("name", "Unknown"),
                "market_cap_usd": market_cap,
                "pct_change_24h": cat.get("market_cap_change_24h") or cat.get("market_cap_change_percentage_24h"),
            })

        # Sort by market cap descending
        categories.sort(key=lambda x: x["market_cap_usd"], reverse=True)

        logger.info(f"Found {len(categories)} categories with market data")
        return categories

    def close(self) -> None:
        """Close HTTP client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

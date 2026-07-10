"""Fakeable price provider abstraction for paper mark-to-market."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import httpx


DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint_address}"


@dataclass
class PriceResult:
    price_sol: float | None
    reason: str = "price_unavailable"


class PriceProvider(ABC):
    @property
    def name(self) -> str:
        return "base"

    @abstractmethod
    async def get_current_price(self, mint_address: str) -> float | None:
        ...

    async def get_price_with_diagnostic(self, mint_address: str) -> PriceResult:
        price = await self.get_current_price(mint_address)
        if price is not None:
            return PriceResult(price_sol=price, reason="ok")
        return PriceResult(price_sol=None, reason="price_unavailable")


class UnavailablePriceProvider(PriceProvider):
    @property
    def name(self) -> str:
        return "unavailable"

    async def get_current_price(self, mint_address: str) -> float | None:
        return None


class FakePriceProvider(PriceProvider):
    @property
    def name(self) -> str:
        return "fake"

    def __init__(self, prices: dict[str, float] | None = None) -> None:
        self._prices = prices or {}

    async def get_current_price(self, mint_address: str) -> float | None:
        return self._prices.get(mint_address)


class DexScreenerPriceProvider(PriceProvider):
    @property
    def name(self) -> str:
        return "live"

    def __init__(
        self,
        token_url: str = DEXSCREENER_TOKEN_URL,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._token_url = token_url
        self._client = http_client

    async def get_current_price(self, mint_address: str) -> float | None:
        result = await self.get_price_with_diagnostic(mint_address)
        return result.price_sol

    async def get_price_with_diagnostic(self, mint_address: str) -> PriceResult:
        client = self._client or httpx.AsyncClient(timeout=10.0)
        try:
            response = await client.get(self._token_url.format(mint_address=mint_address))
            response.raise_for_status()
            data = response.json()
        except httpx.TimeoutException:
            return PriceResult(None, "provider_timeout")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                return PriceResult(None, "rate_limited")
            return PriceResult(None, "provider_error")
        except httpx.HTTPError:
            return PriceResult(None, "provider_error")
        except ValueError:
            return PriceResult(None, "malformed_response")
        finally:
            if self._client is None:
                await client.aclose()

        if not isinstance(data, dict):
            return PriceResult(None, "malformed_response")

        pairs = data.get("pairs")
        if not isinstance(pairs, list) or not pairs:
            return PriceResult(None, "no_pairs")

        solana_pairs = [p for p in pairs if isinstance(p, dict) and p.get("chainId") == "solana"]
        if not solana_pairs:
            return PriceResult(None, "no_solana_pairs")

        best_pair = max(
            solana_pairs,
            key=lambda p: _safe_float(p.get("liquidity"), "usd", 0),
        )

        raw = best_pair.get("priceNative")
        if raw is None:
            return PriceResult(None, "missing_native_price")
        try:
            price = float(raw)
        except (TypeError, ValueError):
            return PriceResult(None, "malformed_price")

        if price <= 0:
            return PriceResult(None, "zero_price")

        return PriceResult(price, "live_dexscreener")


def _safe_float(data: object, key: str, default: float = 0.0) -> float:
    if not isinstance(data, dict):
        return default
    try:
        value = data.get(key)
        if value is not None:
            return float(value)
    except (TypeError, ValueError):
        pass
    return default

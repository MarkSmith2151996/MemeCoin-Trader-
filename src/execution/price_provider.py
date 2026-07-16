"""Fakeable price provider abstraction for paper mark-to-market."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import math

import httpx


DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint_address}"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"


@dataclass
class PriceResult:
    price_sol: float | None
    reason: str = "price_unavailable"
    liquidity_usd: float | None = None


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

        solana_pairs = [pair for pair in pairs if isinstance(pair, dict) and pair.get("chainId") == "solana"]
        if not solana_pairs:
            return PriceResult(None, "no_solana_pairs")

        sol_pairs = [
            pair
            for pair in solana_pairs
            if _is_requested_mint_sol_pair(pair, mint_address)
        ]
        if not sol_pairs:
            return PriceResult(None, "no_requested_mint_sol_pair")

        best_pair = max(
            sol_pairs,
            key=lambda p: _safe_float(p.get("liquidity"), "usd", 0),
        )

        raw = best_pair.get("priceNative")
        if raw is None:
            return PriceResult(None, "missing_native_price")
        try:
            price = float(raw)
        except (TypeError, ValueError):
            return PriceResult(None, "malformed_price")

        if not math.isfinite(price) or price <= 0:
            return PriceResult(None, "invalid_price")

        liquidity_usd = _safe_float(best_pair.get("liquidity"), "usd", default=-1.0)
        return PriceResult(price, "live_dexscreener", liquidity_usd=liquidity_usd if liquidity_usd >= 0 else None)


def _safe_float(data: object, key: str, default: float = 0.0) -> float:
    if not isinstance(data, dict):
        return default
    try:
        value = data.get(key)
        if value is not None:
            parsed = float(value)
            return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError):
        pass
    return default


class JupiterPriceProvider(PriceProvider):
    @property
    def name(self) -> str:
        return "jupiter"

    def __init__(self, client: "JupiterClient | None" = None, reference_sol: float = 0.01) -> None:
        from src.chain.jupiter import JupiterClient
        self._client = client or JupiterClient()
        self._reference_sol = reference_sol

    async def get_current_price(self, mint_address: str) -> float | None:
        from src.core.models import Side
        try:
            quote = await self._client.get_quote(mint_address, Side.BUY, self._reference_sol)
            return quote.price_sol
        except Exception:
            return None

    async def close(self) -> None:
        await self._client.close()


def _is_requested_mint_sol_pair(pair: dict[str, object], mint_address: str) -> bool:
    """Accept only requested-mint / canonical wrapped-SOL base-oriented pairs."""
    base_token = pair.get("baseToken")
    quote_token = pair.get("quoteToken")
    if not isinstance(base_token, dict) or not isinstance(quote_token, dict):
        return False
    return (
        base_token.get("address") == mint_address
        and quote_token.get("address") == WRAPPED_SOL_MINT
    )

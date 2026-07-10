"""Liquidity enrichment helpers plus liquidity, age, and buyer checks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import httpx

from src.core.config import RiskConfig
from src.core.models import CheckResult, TokenInfo

DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint_address}"
JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
SOL_MINT = "So11111111111111111111111111111111111111112"


class LiquidityProbe:
    def __init__(
        self,
        *,
        token_url_template: str = DEXSCREENER_TOKEN_URL,
        jupiter_quote_url: str = JUPITER_QUOTE_URL,
        timeout_s: float = 15.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._token_url_template = token_url_template
        self._jupiter_quote_url = jupiter_quote_url
        self._timeout_s = timeout_s
        self._client = client

    async def get_pool_info(self, mint_address: str) -> dict[str, object]:
        diagnostics: dict[str, object] = {
            "dexscreener_attempted": True,
            "dexscreener_liquidity_sol": None,
            "dexscreener_liquidity_usd": None,
            "dexscreener_status": "missing",
            "jupiter_attempted": False,
            "jupiter_liquidity_sol": None,
            "jupiter_liquidity_usd": None,
            "jupiter_status": None,
        }
        result = await self._get_dexscreener(mint_address)
        if result is not None and result.get("pool_liquidity_sol") is not None:
            diagnostics.update(
                {
                    "dexscreener_liquidity_sol": result.get("pool_liquidity_sol"),
                    "dexscreener_liquidity_usd": result.get("pool_liquidity_usd"),
                    "dexscreener_status": "ok",
                }
            )
            return {**diagnostics, **result}

        if result is not None:
            diagnostics.update(
                {
                    "dexscreener_liquidity_sol": result.get("pool_liquidity_sol"),
                    "dexscreener_liquidity_usd": result.get("pool_liquidity_usd"),
                    "dexscreener_status": str(result.get("status") or "missing"),
                }
            )

        diagnostics["jupiter_attempted"] = True
        result = await self._get_jupiter_liquidity_probe(mint_address)
        if result is not None and result.get("pool_liquidity_sol") is not None:
            diagnostics.update(
                {
                    "jupiter_liquidity_sol": result.get("pool_liquidity_sol"),
                    "jupiter_liquidity_usd": result.get("pool_liquidity_usd"),
                    "jupiter_status": "ok",
                }
            )
            return {**diagnostics, **result}

        if result is not None:
            diagnostics.update(
                {
                    "jupiter_liquidity_sol": result.get("pool_liquidity_sol"),
                    "jupiter_liquidity_usd": result.get("pool_liquidity_usd"),
                    "jupiter_status": str(result.get("status") or "missing"),
                }
            )

        return {**diagnostics, "pool_liquidity_sol": None, "pool_liquidity_usd": None, "source": "none", "status": "missing"}

    async def _get_dexscreener(self, mint_address: str) -> dict[str, object] | None:
        payload = await self._get_json(self._token_url_template.format(mint_address=mint_address))
        if not isinstance(payload, Mapping):
            return {"pool_liquidity_sol": None, "pool_liquidity_usd": None, "source": "dexscreener", "status": "provider_missing"}

        pairs = payload.get("pairs")
        if not isinstance(pairs, Sequence) or isinstance(pairs, (str, bytes, bytearray)):
            return {"pool_liquidity_sol": None, "pool_liquidity_usd": None, "source": "dexscreener", "status": "no_pairs"}

        best_liquidity: float | None = None
        best_liquidity_usd: float | None = None
        for pair in pairs:
            if not isinstance(pair, Mapping) or pair.get("chainId") != "solana":
                continue
            liquidity_sol = _extract_pair_liquidity_sol(pair)
            liquidity_usd = _extract_pair_liquidity_usd(pair)
            if liquidity_sol is None:
                continue
            if best_liquidity is None or liquidity_sol > best_liquidity:
                best_liquidity = liquidity_sol
                best_liquidity_usd = liquidity_usd

        if best_liquidity is None:
            return {"pool_liquidity_sol": None, "pool_liquidity_usd": None, "source": "dexscreener", "status": "no_solana_liquidity"}
        return {"pool_liquidity_sol": best_liquidity, "pool_liquidity_usd": best_liquidity_usd, "source": "dexscreener", "status": "ok"}

    async def _get_jupiter_liquidity_probe(self, mint_address: str) -> dict[str, object] | None:
        url = (
            f"{self._jupiter_quote_url}?inputMint={mint_address}&outputMint={SOL_MINT}"
            "&amount=1000000&slippageBps=1000"
        )
        payload = await self._get_json(url)
        if not isinstance(payload, Mapping):
            return {"pool_liquidity_sol": None, "pool_liquidity_usd": None, "source": "jupiter_fallback", "status": "provider_missing"}

        route_plan = payload.get("routePlan")
        if not isinstance(route_plan, Sequence) or isinstance(route_plan, (str, bytes, bytearray)) or not route_plan:
            return {"pool_liquidity_sol": None, "pool_liquidity_usd": None, "source": "jupiter_fallback", "status": "no_route"}

        explicit_liquidity = _extract_jupiter_liquidity_sol(payload)
        if explicit_liquidity is not None:
            return {"pool_liquidity_sol": explicit_liquidity, "pool_liquidity_usd": None, "source": "jupiter_fallback", "status": "ok"}

        # A quote's output amount is execution pricing, not total pool liquidity.
        return {
            "pool_liquidity_sol": None,
            "pool_liquidity_usd": None,
            "source": "jupiter_fallback",
            "status": "explicit_liquidity_unavailable",
        }

    async def _get_json(self, url: str) -> object | None:
        client = self._client
        if client is None:
            client = httpx.AsyncClient(timeout=self._timeout_s)
            self._client = client
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError):
            return None


def _extract_pair_liquidity_sol(pair: Mapping[str, object]) -> float | None:
    direct_value = _coerce_float(
        pair.get("poolLiquiditySol")
        or pair.get("liquiditySol")
        or pair.get("solLiquidity")
    )
    if direct_value is not None:
        return direct_value

    liquidity = pair.get("liquidity")
    if not isinstance(liquidity, Mapping):
        return None

    base_token = pair.get("baseToken")
    if isinstance(base_token, Mapping) and base_token.get("address") == SOL_MINT:
        return _coerce_float(liquidity.get("base"))

    quote_token = pair.get("quoteToken")
    if isinstance(quote_token, Mapping) and quote_token.get("address") == SOL_MINT:
        return _coerce_float(liquidity.get("quote"))

    return None


def _extract_pair_liquidity_usd(pair: Mapping[str, object]) -> float | None:
    liquidity = pair.get("liquidity")
    if not isinstance(liquidity, Mapping):
        return None
    return _coerce_float(liquidity.get("usd"))


def _extract_jupiter_liquidity_sol(payload: Mapping[str, object]) -> float | None:
    direct_value = _coerce_float(
        payload.get("poolLiquiditySol")
        or payload.get("liquiditySol")
        or payload.get("solLiquidity")
    )
    if direct_value is not None:
        return direct_value

    route_plan = payload.get("routePlan")
    if not isinstance(route_plan, Sequence) or isinstance(route_plan, (str, bytes, bytearray)):
        return None

    for step in route_plan:
        if not isinstance(step, Mapping):
            continue
        swap_info = step.get("swapInfo")
        if not isinstance(swap_info, Mapping):
            continue
        liquidity_sol = _coerce_float(
            swap_info.get("poolLiquiditySol")
            or swap_info.get("liquiditySol")
            or swap_info.get("solLiquidity")
        )
        if liquidity_sol is not None:
            return liquidity_sol
    return None


def _coerce_float(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def check_liquidity(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.liquidity_sol is None:
        return CheckResult.UNKNOWN
    if token.liquidity_sol < config.min_liquidity_sol:
        return CheckResult.FAIL
    return CheckResult.PASS


def check_age(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.age_minutes is None:
        return CheckResult.UNKNOWN
    if token.age_minutes < config.min_age_minutes:
        return CheckResult.FAIL
    return CheckResult.PASS


def check_unique_buyers(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.unique_buyers is None:
        return CheckResult.UNKNOWN
    if token.unique_buyers < config.min_unique_buyers:
        return CheckResult.FAIL
    return CheckResult.PASS

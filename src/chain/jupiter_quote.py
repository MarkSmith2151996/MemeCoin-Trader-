"""Shadow-mode Jupiter V2 quote telemetry client.

Pure read-only quoting against the legacy Jupiter V2 ``/quote`` endpoint.
No wallet, no signing, no transaction building. Used to compare what the
paper runner prices via DexScreener against what Jupiter would actually
execute, so real slippage can be measured before any live trading.

The client is fail-open by design: every failure (Jupiter down, no route,
rate limit, malformed payload) is logged and returns ``None`` so the caller's
trading loop is never affected.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from src.chain.jupiter import LAMPORTS_PER_SOL, SOL_MINT
from src.core.models import Side

log = logging.getLogger("jupiter_quote_shadow")

_DEFAULT_SOLANA_RPC = "https://api.mainnet-beta.solana.com"
_DEFAULT_SLIPPAGE_BPS = 300
_FALLBACK_DECIMALS = 9  # pump.fun mints are always 9 decimals


@dataclass(frozen=True, slots=True)
class JupiterQuoteV2:
    """Normalized result of one Jupiter V2 shadow quote."""

    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    price_impact_pct: float
    route_plan: tuple[dict[str, object], ...]
    quoted_at: str
    token_decimals: int
    price_sol: float | None


class JupiterV2QuoteClient:
    """Read-only Jupiter V2 quote client with best-effort error handling."""

    def __init__(
        self,
        base_url: str = "https://quote-api.jup.ag",
        solana_rpc_url: str | None = None,
        backup_solana_rpc_url: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        timeout_s: float = 10.0,
        min_interval_s: float = 0.25,
        max_concurrent: int = 2,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._solana_rpc_url = (
            solana_rpc_url
            or os.environ.get("QUICKNODE_RPC_URL")
            or os.environ.get("PRIMARY_RPC_URL")
            or _DEFAULT_SOLANA_RPC
        )
        self._backup_solana_rpc_url = (
            backup_solana_rpc_url
            or os.environ.get("BACKUP_RPC_URL")
            or os.environ.get("HELIUS_RPC_URL")
            or None
        )
        self._client = http_client or httpx.AsyncClient(timeout=timeout_s)
        self._decimals_cache: dict[str, int] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._min_interval_s = min_interval_s
        self._last_request_at = 0.0

    async def get_token_decimals(self, mint: str) -> int:
        """Cached best-effort token decimals lookup via public Solana RPC.

        Falls back to 9 (the pump.fun standard) when the RPC is unreachable so
        a shadow quote is never blocked by the decimals probe.
        """
        if mint in self._decimals_cache:
            return self._decimals_cache[mint]
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenSupply",
            "params": [mint],
        }
        try:
            response = await self._rpc_post(payload)
            data = response.json()
            decimals = int(data["result"]["value"]["decimals"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            log.warning(
                "SHADOW decimals lookup failed for %s — fallback %d: %s",
                mint[:16], _FALLBACK_DECIMALS, exc,
            )
            decimals = _FALLBACK_DECIMALS
        self._decimals_cache[mint] = decimals
        return decimals

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_lamports: int,
        slippage_bps: int = _DEFAULT_SLIPPAGE_BPS,
    ) -> JupiterQuoteV2 | None:
        """Fire one Jupiter V2 quote for the given mint/side.

        ``amount_lamports`` is denominated in the input token's base units
        (lamports for SOL, token lamports for the mint). Returns ``None`` on
        any failure — never raises — so callers can treat the shadow quote as
        optional telemetry.
        """
        if not isinstance(side, Side):
            log.warning("SHADOW invalid side %r for mint=%s", side, mint_address[:16])
            return None
        if amount_lamports <= 0:
            log.warning(
                "SHADOW non-positive amount %d for mint=%s", amount_lamports, mint_address[:16],
            )
            return None

        decimals = await self.get_token_decimals(mint_address)
        if side == Side.BUY:
            input_mint = SOL_MINT
            output_mint = mint_address
        else:
            input_mint = mint_address
            output_mint = SOL_MINT

        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_lamports),
            "slippageBps": str(slippage_bps),
        }

        async with self._semaphore:
            await self._throttle()
            try:
                response = await self._client.get(f"{self._base_url}/v2/quote", params=params)
                self._last_request_at = time.monotonic()
            except httpx.HTTPError as exc:
                log.warning(
                    "SHADOW quote request failed mint=%s side=%s: %s",
                    mint_address[:16], side.value, exc,
                )
                return None

            if response.status_code == 429:
                log.warning(
                    "SHADOW rate limited (429) mint=%s side=%s — skipping quote",
                    mint_address[:16], side.value,
                )
                return None
            if response.status_code != 200:
                log.warning(
                    "SHADOW quote failed HTTP %d mint=%s side=%s: %s",
                    response.status_code, mint_address[:16], side.value, response.text[:200],
                )
                return None

            try:
                data = response.json()
                in_amount = int(data["inAmount"])
                out_amount = int(data["outAmount"])
                price_impact_pct = float(data.get("priceImpactPct", 0.0))
                if isinstance(data.get("routePlan"), list):
                    route_plan = tuple(data["routePlan"])
                else:
                    route_plan = ()
            except (ValueError, KeyError, TypeError) as exc:
                log.warning(
                    "SHADOW malformed quote response mint=%s side=%s: %s",
                    mint_address[:16], side.value, exc,
                )
                return None

        price_sol = self._derive_price_sol(side, in_amount, out_amount, decimals)
        return JupiterQuoteV2(
            input_mint=input_mint,
            output_mint=output_mint,
            in_amount=in_amount,
            out_amount=out_amount,
            price_impact_pct=price_impact_pct,
            route_plan=route_plan,
            quoted_at=datetime.now(UTC).isoformat(),
            token_decimals=decimals,
            price_sol=price_sol,
        )

    async def _rpc_post(self, payload: dict[str, object]) -> httpx.Response:
        urls = [self._solana_rpc_url]
        if self._backup_solana_rpc_url and self._backup_solana_rpc_url != self._solana_rpc_url:
            urls.append(self._backup_solana_rpc_url)
        last_error: httpx.HTTPError | None = None
        for index, url in enumerate(urls):
            try:
                response = await self._client.post(url, json=payload)
                response.raise_for_status()
                return response
            except httpx.HTTPError as exc:
                last_error = exc
                if index + 1 < len(urls):
                    log.warning("SHADOW primary RPC failed; trying configured backup")
        assert last_error is not None
        raise last_error

    def _derive_price_sol(
        self,
        side: Side,
        in_amount: int,
        out_amount: int,
        decimals: int,
    ) -> float | None:
        """Derive the SOL-per-token price the route implies."""
        if side == Side.BUY:
            token_amount = out_amount / (10**decimals)
            sol_amount = in_amount / LAMPORTS_PER_SOL
        else:
            token_amount = in_amount / (10**decimals)
            sol_amount = out_amount / LAMPORTS_PER_SOL
        if token_amount <= 0:
            return None
        return sol_amount / token_amount

    async def _throttle(self) -> None:
        """Space quote requests by at least ``min_interval_s`` to respect rate limits."""
        elapsed = time.monotonic() - self._last_request_at
        if self._min_interval_s > 0 and elapsed < self._min_interval_s:
            await asyncio.sleep(self._min_interval_s - elapsed)

    async def close(self) -> None:
        await self._client.aclose()

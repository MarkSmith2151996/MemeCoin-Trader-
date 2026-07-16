"""Jupiter quote/swap integration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx

from src.core.models import Side, SwapQuote

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000
_QUOTE_TTL_SECONDS = 30
_DEFAULT_SOLANA_RPC = "https://api.mainnet-beta.solana.com"


class JupiterClient:
    def __init__(
        self,
        base_url: str = "https://quote-api.jup.ag",
        solana_rpc_url: str = _DEFAULT_SOLANA_RPC,
        http_client: httpx.AsyncClient | None = None,
        timeout_s: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.solana_rpc_url = solana_rpc_url
        self._client = http_client or httpx.AsyncClient(timeout=timeout_s)
        self._decimals_cache: dict[str, int] = {}

    async def _get_token_decimals(self, mint: str) -> int:
        """Fetch and cache token decimals via public Solana RPC getTokenSupply."""
        if mint in self._decimals_cache:
            return self._decimals_cache[mint]
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenSupply",
            "params": [mint],
        }
        response = await self._client.post(self.solana_rpc_url, json=payload)
        response.raise_for_status()
        data = response.json()
        decimals: int = data["result"]["value"]["decimals"]
        self._decimals_cache[mint] = decimals
        return decimals

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        decimals = await self._get_token_decimals(mint_address)

        if side == Side.BUY:
            input_mint = SOL_MINT
            output_mint = mint_address
            amount_raw = int(amount_sol * LAMPORTS_PER_SOL)
        else:
            input_mint = mint_address
            output_mint = SOL_MINT
            amount_raw = int(amount_sol * LAMPORTS_PER_SOL)

        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_raw),
            "slippageBps": str(slippage_bps),
        }

        response = await self._client.get(f"{self.base_url}/v6/quote", params=params)
        response.raise_for_status()
        data = response.json()

        in_amount = int(data["inAmount"])
        out_amount = int(data["outAmount"])
        price_impact_pct = float(data.get("priceImpactPct", 0.0))
        expires_at = datetime.now(UTC) + timedelta(seconds=_QUOTE_TTL_SECONDS)

        if side == Side.BUY:
            token_amount = out_amount / (10**decimals)
            sol_amount = in_amount / LAMPORTS_PER_SOL
            price_sol = sol_amount / token_amount if token_amount > 0 else None
            estimated_out = token_amount
        else:
            sol_out = out_amount / LAMPORTS_PER_SOL
            token_in = in_amount / (10**decimals)
            price_sol = sol_out / token_in if token_in > 0 else None
            estimated_out = sol_out

        return SwapQuote(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            estimated_out_amount=estimated_out,
            price_sol=price_sol,
            price_impact_pct=price_impact_pct,
            slippage_bps=slippage_bps,
            provider="jupiter",
            expires_at=expires_at,
        )

    async def close(self) -> None:
        await self._client.aclose()

"""Jupiter quote/swap integration placeholders."""

from __future__ import annotations

from src.core.models import Side, SwapQuote


class JupiterClient:
    def __init__(self, base_url: str = "https://quote-api.jup.ag") -> None:
        self.base_url = base_url.rstrip("/")

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        raise NotImplementedError("Jupiter quote integration is a Phase 2 task")

    async def close(self) -> None:
        return None

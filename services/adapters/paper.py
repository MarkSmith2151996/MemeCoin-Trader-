"""Paper adapter with an executor-owned current candidate mark."""

from __future__ import annotations

from src.execution.paper import PaperExecutionAdapter as BasePaperExecutionAdapter


class PaperExecutionAdapter(BasePaperExecutionAdapter):
    """Allow the database candidate price to drive deterministic paper fills."""

    def set_price(self, mint_address: str, price_sol: float) -> None:
        if price_sol <= 0:
            raise ValueError("paper candidate price must be positive")
        self._price_lookup[mint_address] = price_sol

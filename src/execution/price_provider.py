"""Fakeable price provider abstraction for paper mark-to-market."""

from __future__ import annotations

from abc import ABC, abstractmethod


class PriceProvider(ABC):
    @abstractmethod
    async def get_current_price(self, mint_address: str) -> float | None:
        ...


class UnavailablePriceProvider(PriceProvider):
    async def get_current_price(self, mint_address: str) -> float | None:
        return None


class FakePriceProvider(PriceProvider):
    def __init__(self, prices: dict[str, float] | None = None) -> None:
        self._prices = prices or {}

    async def get_current_price(self, mint_address: str) -> float | None:
        return self._prices.get(mint_address)

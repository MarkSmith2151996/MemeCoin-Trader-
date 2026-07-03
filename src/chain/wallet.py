"""Wallet loading boundary for future live trading."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TradingWallet:
    public_key: str
    private_key: str


def load_wallet(private_key: str | None) -> TradingWallet | None:
    if not private_key:
        return None
    return TradingWallet(public_key="TODO_DERIVE_PUBLIC_KEY", private_key=private_key)

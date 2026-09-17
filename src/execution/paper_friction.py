"""Deterministic paper-fill friction calculations for the V2 executor."""

from __future__ import annotations

from dataclasses import dataclass

from src.core.config import PaperFrictionConfig


@dataclass(frozen=True, slots=True)
class PaperEntryFill:
    token_amount: float
    quoted_token_amount: float
    slippage_pct: float
    dex_fee_sol: float


@dataclass(frozen=True, slots=True)
class PaperExitFill:
    sol_amount: float
    slippage_pct: float
    dex_fee_sol: float


def pool_slippage_pct(position_sol: float, pool_sol: float) -> float:
    """Return adverse constant-product impact for one paper-execution leg."""

    if position_sol <= 0 or pool_sol <= 0:
        return 0.0
    return position_sol / (2 * pool_sol)


def apply_entry_friction(
    quoted_token_amount: float,
    position_sol: float,
    pool_sol: float,
    config: PaperFrictionConfig,
) -> PaperEntryFill:
    slippage_pct = pool_slippage_pct(position_sol, pool_sol)
    token_amount = quoted_token_amount * (1 - slippage_pct) * (1 - config.dex_fee_pct)
    return PaperEntryFill(
        token_amount=token_amount,
        quoted_token_amount=quoted_token_amount,
        slippage_pct=slippage_pct,
        dex_fee_sol=position_sol * config.dex_fee_pct,
    )


def apply_exit_friction(
    quoted_sol_amount: float,
    position_sol: float,
    pool_sol: float,
    config: PaperFrictionConfig,
) -> PaperExitFill:
    slippage_pct = pool_slippage_pct(position_sol, pool_sol)
    sol_amount = quoted_sol_amount * (1 - slippage_pct) * (1 - config.dex_fee_pct)
    return PaperExitFill(
        sol_amount=sol_amount,
        slippage_pct=slippage_pct,
        dex_fee_sol=quoted_sol_amount * (1 - slippage_pct) * config.dex_fee_pct,
    )

"""Diagnostic startup position reconciliation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from src.core.models import Position
from src.strategy.position_manager import PositionManager


class SupportsWalletHoldingsLookup(Protocol):
    async def __call__(self) -> dict[str, float] | None: ...


@dataclass(frozen=True, slots=True)
class PositionReconciliationMismatch:
    kind: str
    mint_address: str
    local_token_amount: float | None = None
    wallet_token_amount: float | None = None


@dataclass(frozen=True, slots=True)
class PositionReconciliationReport:
    ok: bool
    diagnostics: tuple[str, ...]
    mismatches: tuple[PositionReconciliationMismatch, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "diagnostics": list(self.diagnostics),
            "mismatches": [
                {
                    "kind": mismatch.kind,
                    "mint_address": mismatch.mint_address,
                    "local_token_amount": mismatch.local_token_amount,
                    "wallet_token_amount": mismatch.wallet_token_amount,
                }
                for mismatch in self.mismatches
            ],
        }


async def reconcile_positions(
    position_manager: PositionManager,
    wallet_holdings_lookup: SupportsWalletHoldingsLookup | None,
    *,
    material_balance_ratio: float = 0.05,
) -> PositionReconciliationReport:
    live_positions = await position_manager.get_all_open(mode="live")
    if not live_positions:
        return PositionReconciliationReport(
            ok=True,
            diagnostics=("no_live_positions_to_reconcile",),
            mismatches=(),
        )

    if wallet_holdings_lookup is None:
        return PositionReconciliationReport(
            ok=False,
            diagnostics=("wallet_holdings_lookup_unavailable",),
            mismatches=(),
        )

    try:
        wallet_holdings = await wallet_holdings_lookup()
    except Exception:
        return PositionReconciliationReport(
            ok=False,
            diagnostics=("wallet_holdings_lookup_failed",),
            mismatches=(),
        )

    if wallet_holdings is None:
        return PositionReconciliationReport(
            ok=False,
            diagnostics=("wallet_holdings_unknown",),
            mismatches=(),
        )

    local_by_mint = {position.mint_address: position for position in live_positions}
    wallet_by_mint = {mint: amount for mint, amount in wallet_holdings.items() if amount > 0}

    mismatches: list[PositionReconciliationMismatch] = []

    for mint, amount in wallet_by_mint.items():
        local_position = local_by_mint.get(mint)
        if local_position is None:
            mismatches.append(
                PositionReconciliationMismatch(
                    kind="wallet_only_holding",
                    mint_address=mint,
                    wallet_token_amount=amount,
                )
            )

    for mint, position in local_by_mint.items():
        wallet_amount = wallet_by_mint.get(mint)
        if wallet_amount is None or wallet_amount <= 0:
            mismatches.append(
                PositionReconciliationMismatch(
                    kind="local_only_position",
                    mint_address=mint,
                    local_token_amount=position.token_amount,
                )
            )
            continue

        if _is_material_balance_mismatch(position, wallet_amount, material_balance_ratio):
            mismatches.append(
                PositionReconciliationMismatch(
                    kind="balance_mismatch",
                    mint_address=mint,
                    local_token_amount=position.token_amount,
                    wallet_token_amount=wallet_amount,
                )
            )

    if mismatches:
        return PositionReconciliationReport(
            ok=False,
            diagnostics=("position_reconciliation_mismatch",),
            mismatches=tuple(mismatches),
        )

    return PositionReconciliationReport(
        ok=True,
        diagnostics=("position_reconciliation_passed",),
        mismatches=(),
    )


def _is_material_balance_mismatch(
    position: Position,
    wallet_amount: float,
    material_balance_ratio: float,
) -> bool:
    baseline = max(position.token_amount, wallet_amount, 1e-9)
    return abs(position.token_amount - wallet_amount) / baseline > material_balance_ratio

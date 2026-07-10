"""Paper-only PnL mark-to-market calculator."""

from __future__ import annotations

from dataclasses import dataclass, field

from src.core.models import PaperFillQuality, Position, PositionStatus
from src.execution.price_provider import PriceProvider, UnavailablePriceProvider
from src.strategy.position_manager import PositionManager


@dataclass
class PaperPnLPosition:
    mint_address: str
    entry_price_sol: float
    amount_sol: float
    token_amount: float
    status: PositionStatus
    fill_quality: PaperFillQuality
    close_price_sol: float | None = None
    mark_price_sol: float | None = None
    realized_pnl_sol: float = 0.0
    unrealized_pnl_sol: float | None = None
    unrealized_pnl_pct: float | None = None
    mark_unavailable: bool = False
    mark_reason: str = "price_unavailable"
    pnl_confidence: str = "low_confidence"


@dataclass
class PaperPnLSummary:
    total_positions: int = 0
    open_positions: int = 0
    closed_positions: int = 0
    total_sol_deployed: float = 0.0
    realized_pnl_sol: float = 0.0
    unrealized_pnl_sol: float | None = 0.0
    mark_unavailable_count: int = 0
    unrealized_incomplete: bool = False
    positions: list[PaperPnLPosition] = field(default_factory=list)
    marks_mode: str = "unavailable"
    fill_quality_counts: dict[str, int] = field(default_factory=dict)
    usable_mark_count: int = 0
    unusable_mark_count: int = 0
    mark_reason_counts: dict[str, int] = field(default_factory=dict)
    report_confidence: str = "low_confidence"


class PaperPnLCalculator:
    def __init__(self, manager: PositionManager, price_provider: PriceProvider | None = None) -> None:
        self._manager = manager
        self._price_provider = price_provider

    async def compute_summary(self) -> PaperPnLSummary:
        all_positions = await self._manager.get_all_open()
        closed_positions = await self._get_closed_positions()
        paper_positions = [p for p in all_positions if p.mode == "paper"]
        all_paper = paper_positions + closed_positions

        summary = PaperPnLSummary()
        summary.total_positions = len(all_paper)
        summary.open_positions = len(paper_positions)
        summary.closed_positions = len(closed_positions)

        for pos in paper_positions:
            summary.total_sol_deployed += pos.amount_sol * pos.remaining_sell_pct

        summary.realized_pnl_sol = round(sum(p.realized_pnl_sol for p in closed_positions), 9)

        summary.marks_mode = "live" if (self._price_provider is not None and not isinstance(self._price_provider, UnavailablePriceProvider)) else "unavailable"

        unrealized_total = 0.0
        any_mark_unavailable = False
        mark_unavailable_count = 0
        usable_mark_count = 0
        mark_reason_counts: dict[str, int] = {}

        for pos in all_paper:
            pnl_pos = PaperPnLPosition(
                mint_address=pos.mint_address,
                entry_price_sol=pos.entry_price_sol,
                amount_sol=pos.amount_sol,
                token_amount=pos.token_amount,
                status=pos.status,
                fill_quality=pos.fill_quality,
                close_price_sol=pos.close_price_sol,
                realized_pnl_sol=pos.realized_pnl_sol,
            )

            if pos.status == PositionStatus.OPEN:
                summary.fill_quality_counts[pos.fill_quality.value] = summary.fill_quality_counts.get(pos.fill_quality.value, 0) + 1

            if pos.status == PositionStatus.CLOSED:
                pnl_pos.pnl_confidence = _confidence_for_fill_quality(pos.fill_quality)
                summary.positions.append(pnl_pos)
                continue

            if self._price_provider is not None:
                result = await self._price_provider.get_price_with_diagnostic(pos.mint_address)
                mark = result.price_sol
                pnl_pos.mark_reason = result.reason
            else:
                mark = None
                pnl_pos.mark_reason = "price_unavailable"

            if pos.fill_quality == PaperFillQuality.LEGACY_UNKNOWN:
                pnl_pos.mark_price_sol = mark if mark is not None and mark > 0 else None
                pnl_pos.mark_unavailable = True
                pnl_pos.mark_reason = "legacy_low_confidence"
                pnl_pos.pnl_confidence = "low_confidence"
                any_mark_unavailable = True
                mark_unavailable_count += 1
            elif pos.fill_quality == PaperFillQuality.UNPRICED:
                pnl_pos.mark_price_sol = mark if mark is not None and mark > 0 else None
                pnl_pos.mark_unavailable = True
                pnl_pos.mark_reason = "unpriced_entry"
                pnl_pos.pnl_confidence = "unavailable"
                any_mark_unavailable = True
                mark_unavailable_count += 1
            elif mark is not None and mark > 0 and pos.token_amount > 0:
                pnl_pos.mark_price_sol = mark
                pnl_pos.unrealized_pnl_sol = round(pos.token_amount * mark - pos.amount_sol, 9)
                pnl_pos.unrealized_pnl_pct = (
                    round(((mark - pos.entry_price_sol) / pos.entry_price_sol) * 100, 2)
                    if pos.entry_price_sol > 0
                    else None
                )
                pnl_pos.pnl_confidence = "high_confidence"
                usable_mark_count += 1
                unrealized_total += pnl_pos.unrealized_pnl_sol
            else:
                pnl_pos.mark_unavailable = True
                pnl_pos.pnl_confidence = "partial"
                any_mark_unavailable = True
                mark_unavailable_count += 1

            if pnl_pos.mark_unavailable:
                mark_reason_counts[pnl_pos.mark_reason] = mark_reason_counts.get(pnl_pos.mark_reason, 0) + 1

            summary.positions.append(pnl_pos)

        summary.mark_unavailable_count = mark_unavailable_count
        summary.usable_mark_count = usable_mark_count
        summary.unusable_mark_count = mark_unavailable_count
        summary.mark_reason_counts = mark_reason_counts
        if any_mark_unavailable:
            summary.unrealized_incomplete = True
            summary.unrealized_pnl_sol = None
        else:
            summary.unrealized_pnl_sol = round(unrealized_total, 9)

        summary.report_confidence = _classify_report_confidence(summary)

        summary.total_sol_deployed = round(summary.total_sol_deployed, 6)
        return summary

    async def _get_closed_positions(self) -> list[Position]:
        if self._manager.db is None or not self._manager.use_persisted_positions:
            return [p for p in self._manager._cache.values() if p.status == PositionStatus.CLOSED and p.mode == "paper"]

        import aiosqlite

        async with aiosqlite.connect(self._manager.db) as conn:
            cursor = await conn.execute(
                "SELECT partial_exits_json FROM positions WHERE status = ?",
                (PositionStatus.CLOSED.value,),
            )
            rows = await cursor.fetchall()

        positions = [Position.model_validate_json(row[0]) for row in rows]
        return [p for p in positions if p.mode == "paper"]


def _confidence_for_fill_quality(fill_quality: PaperFillQuality) -> str:
    if fill_quality == PaperFillQuality.PRICED_QUOTE:
        return "high_confidence"
    if fill_quality == PaperFillQuality.UNPRICED:
        return "unavailable"
    return "low_confidence"


def _classify_report_confidence(summary: PaperPnLSummary) -> str:
    if summary.open_positions == 0:
        return "high_confidence"
    if summary.usable_mark_count == summary.open_positions and summary.unusable_mark_count == 0:
        return "high_confidence"
    if summary.usable_mark_count > 0:
        return "partial"
    return "low_confidence"

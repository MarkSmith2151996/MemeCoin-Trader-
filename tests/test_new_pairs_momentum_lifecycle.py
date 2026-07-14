"""Focused coverage for the explicit New Pairs momentum lifecycle."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite

import src.cli as cli_module
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
from src.execution.price_provider import PriceProvider, PriceResult
from src.risk.paper_minimum import evaluate_paper_minimum_evidence
from src.risk.paper_momentum import evaluate_paper_new_pairs_momentum_evidence
from src.risk.scorer import _dexscreener_pair_created_at


class SequencedPriceProvider(PriceProvider):
    def __init__(self, prices: list[float | None]) -> None:
        self._prices = iter(prices)

    async def get_current_price(self, mint_address: str) -> float | None:
        return (await self.get_price_with_diagnostic(mint_address)).price_sol

    async def get_price_with_diagnostic(self, mint_address: str) -> PriceResult:
        price = next(self._prices)
        return PriceResult(price, "live_dexscreener" if price is not None else "no_requested_mint_sol_pair")


def _signal() -> Signal:
    return Signal(
        source=SignalSource.ONCHAIN,
        type=SignalType.NEW_POOL,
        mint_address="MomentumLifecycleMint11111111111111111111111",
        observed_at=datetime.now(UTC),
        payload={
            "provider": "dexscreener",
            "pair_address": "pair-address",
            "pair_created_at": datetime.now(UTC).isoformat(),
            "ui_age": "38s",
            "ui_age_minutes": 38 / 60,
            "ui_max_age_minutes": 60.0,
            "ui_age_source": "dexscreener_new_pairs_rendered_row",
        },
    )


def _unknown_age_assessment() -> RiskAssessment:
    return RiskAssessment(
        token=TokenInfo(
            mint_address="MomentumLifecycleMint11111111111111111111111",
            top10_holder_pct=80.0,
        ),
        liquidity_check=CheckResult.PASS,
        top10_holder_check=CheckResult.FAIL,
        creator_holding_check=CheckResult.UNKNOWN,
        age_check=CheckResult.UNKNOWN,
        unique_buyers_check=CheckResult.UNKNOWN,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=CheckResult.UNKNOWN,
    )


def test_ui_fresh_fallback_is_limited_to_momentum_lane() -> None:
    signal = _signal()
    assessment = _unknown_age_assessment()
    before = assessment.model_dump()

    momentum = evaluate_paper_new_pairs_momentum_evidence(
        signal,
        assessment,
        top10_holder_pct=80.0,
        ui_age_minutes=38 / 60,
        ui_max_age_minutes=60.0,
    )
    strict_default = evaluate_paper_minimum_evidence(signal, assessment)

    assert momentum.eligible is True
    assert "paper_momentum_age_ui_observed_fresh" in momentum.reason_labels
    assert strict_default.eligible is False
    assert "paper_minimum_blocked_top_holders" in strict_default.reason_labels
    assert assessment.model_dump() == before
    assert assessment.age_check == CheckResult.UNKNOWN


def test_missing_or_future_provider_age_does_not_block_ui_fresh_momentum() -> None:
    assessment = _unknown_age_assessment()
    for pair_created_at in (None, (datetime.now(UTC) + timedelta(minutes=1)).isoformat()):
        signal = _signal().model_copy(update={"payload": {**_signal().payload, "pair_created_at": pair_created_at}})
        assert _dexscreener_pair_created_at(signal.payload) is None
        result = evaluate_paper_new_pairs_momentum_evidence(
            signal,
            assessment,
            top10_holder_pct=80.0,
            ui_age_minutes=2.0,
            ui_max_age_minutes=60.0,
        )
        assert result.eligible is True
        assert "paper_momentum_age_ui_observed_fresh" in result.reason_labels


def test_momentum_lifecycle_uses_90_percent_gate_and_closes_paper_trade(tmp_path: Path) -> None:
    async def assessor(_signal: Signal) -> RiskAssessment:
        return _unknown_age_assessment()

    async def run() -> None:
        db_path = tmp_path / "momentum.db"
        summary = await cli_module.run_new_pairs_momentum_lifecycle(
            [_signal()],
            ui_rows=1,
            confirm=True,
            db_path=db_path,
            assessor=assessor,
            price_provider=SequencedPriceProvider([0.00001, 0.000012]),
        )

        assert summary.momentum_passes == 1
        assert summary.ui_age_fallbacks == 1
        assert summary.quote_available == summary.entries == summary.closes == 1
        assert summary.outcome == "closed"
        assert summary.trades[0].realized_pnl_sol == 0.002
        async with aiosqlite.connect(db_path) as db:
            trades = (await (await db.execute("SELECT COUNT(*) FROM trades")).fetchone())[0]
            open_positions = (await (await db.execute("SELECT COUNT(*) FROM positions WHERE status != 'CLOSED'")).fetchone())[0]
        assert (trades, open_positions) == (1, 0)

    asyncio.run(run())

"""V2 paper execution friction: fills, failures, and net PnL."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from services.adapters.paper import PaperExecutionAdapter
from services.executor import StrategyExecutor
from src.core.config import PaperFrictionConfig
from src.execution.paper_friction import apply_entry_friction, apply_exit_friction
from src.execution.price_provider import PriceResult


class Entry:
    allowed = True
    rejection_reason = None

    def __init__(self, created: list[tuple[dict, dict]]) -> None:
        self._created = created

    async def create_position(self, position: dict, trade: dict) -> None:
        self._created.append((position, trade))


class Store:
    def __init__(self) -> None:
        self.created: list[tuple[dict, dict]] = []
        self.closed: list[dict] = []

    @asynccontextmanager
    async def entry_transaction(self, _mint: str):
        yield Entry(self.created)

    async def close_position(self, position, trade, **values) -> None:
        self.closed.append({"position": dict(position), "trade": trade, **values})

    async def refresh_daily_stats(self, _strategy: str) -> None:
        pass


class MarkProvider:
    name = "jupiter"

    async def get_price_with_diagnostic(self, _mint: str) -> PriceResult:
        return PriceResult(0.01, "live_jupiter")


def _candidate(mint: str = "mint") -> dict[str, object]:
    return {
        "id": 1,
        "mint_address": mint,
        "price_sol": 0.01,
        "source": "jupiter_recent",
        "pool_type": "graduated",
        "pool_sol": 200.0,
    }


def _executor(
    tmp_path: Path,
    store: Store,
    *,
    random_float,
) -> StrategyExecutor:
    return StrategyExecutor(
        store,
        PaperExecutionAdapter(),
        heartbeat_path=tmp_path / "heartbeat",
        halt_path=tmp_path / "halt",
        mark_provider=MarkProvider(),
        paper_friction=PaperFrictionConfig(exit_retry_delay_seconds=0),
        random_float=random_float,
    )


def test_pool_relative_entry_and_exit_friction_matches_constant_product_model() -> None:
    config = PaperFrictionConfig()
    entry = apply_entry_friction(100.0, 1.0, 200.0, config)
    exit_fill = apply_exit_friction(entry.token_amount * 0.02, 1.0, 200.0, config)

    assert entry.slippage_pct == pytest.approx(0.0025)
    assert entry.token_amount == pytest.approx(100.0 * 0.9975 * 0.9975)
    assert entry.dex_fee_sol == pytest.approx(0.0025)
    assert exit_fill.slippage_pct == pytest.approx(0.0025)
    assert exit_fill.sol_amount == pytest.approx(entry.token_amount * 0.02 * 0.9975 * 0.9975)


def test_entry_failure_retries_three_times_then_abandons(tmp_path: Path) -> None:
    async def run() -> None:
        store = Store()
        executor = _executor(tmp_path, store, random_float=lambda: 0.1)

        for _ in range(4):
            await executor._enter(_candidate())

        assert store.created == []
        assert executor._entry_attempts["mint"] == 3
        assert "mint" in executor._entry_failures_exhausted

    asyncio.run(run())


def test_paper_close_records_gross_net_and_all_friction_fields(tmp_path: Path) -> None:
    async def run() -> None:
        store = Store()
        executor = _executor(tmp_path, store, random_float=lambda: 0.99)
        executor._exits = {
            "trailing_stop_pct": 2,
            "trailing_arm_pct": 2,
            "hard_stop_pct": 8,
            "take_profit_pct": 150,
            "time_stop_minutes": 7,
        }
        position = {
            "id": "position-1",
            "mint_address": "mint",
            "entry_price_sol": 0.01,
            "amount_sol": 1.0,
            "token_amount": 100.0 * 0.9975 * 0.9975,
            "quoted_token_amount": 100.0,
            "entry_pool_sol": 200.0,
            "slippage_entry_pct": 0.0025,
            "dex_fee_entry_sol": 0.0025,
            "priority_fee_total_sol": 0.006,
            "entry_attempts": 2,
            "opened_at": datetime.now(UTC) - timedelta(seconds=422),
        }
        executor._positions["mint"] = position

        await executor._close_from_monitor(
            position,
            0.02,
            "time_stop",
            mark_source="jupiter",
            mark_timestamp=datetime.now(UTC),
            mark_diagnostic="live_jupiter",
            lock_held=False,
        )

        assert len(store.closed) == 1
        closed = store.closed[0]
        fields = closed["position"]
        assert fields["gross_pnl_sol"] == pytest.approx(1.0)
        assert fields["net_pnl_sol"] < fields["gross_pnl_sol"]
        assert fields["friction_total_sol"] == pytest.approx(
            fields["gross_pnl_sol"] - fields["net_pnl_sol"],
        )
        assert fields["slippage_exit_pct"] == pytest.approx(0.0025)
        assert fields["dex_fee_exit_sol"] > 0
        assert fields["exit_delay_seconds"] == pytest.approx(2.0, abs=0.5)
        assert closed["trade"]["metadata"]["priority_fee_total_sol"] == pytest.approx(0.006)

    asyncio.run(run())


def test_exit_failure_waits_for_one_retry_before_closing(tmp_path: Path) -> None:
    async def run() -> None:
        values = iter((0.1, 0.99))
        store = Store()
        executor = _executor(tmp_path, store, random_float=lambda: next(values))
        executor._exits = {
            "trailing_stop_pct": 2,
            "trailing_arm_pct": 2,
            "hard_stop_pct": 8,
            "take_profit_pct": 150,
            "time_stop_minutes": 7,
        }
        position = {
            "id": "position-2",
            "mint_address": "retry-mint",
            "entry_price_sol": 0.01,
            "amount_sol": 1.0,
            "token_amount": 99.500625,
            "quoted_token_amount": 100.0,
            "entry_pool_sol": 200.0,
            "priority_fee_total_sol": 0.006,
            "opened_at": datetime.now(UTC) - timedelta(seconds=422),
        }
        executor._positions["retry-mint"] = position

        await executor._close_from_monitor(
            position, 0.02, "time_stop", mark_source="jupiter",
            mark_timestamp=datetime.now(UTC), mark_diagnostic="live_jupiter", lock_held=False,
        )
        assert store.closed == []
        await executor._close_from_monitor(
            position, 0.02, "time_stop", mark_source="jupiter",
            mark_timestamp=datetime.now(UTC), mark_diagnostic="live_jupiter", lock_held=False,
        )
        assert len(store.closed) == 1

    asyncio.run(run())

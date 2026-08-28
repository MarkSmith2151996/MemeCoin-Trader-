"""V2 executor exit lifecycle coverage using an in-memory store boundary."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from services.executor import StrategyExecutor
from src.core.models import Side, Trade

GATES = {
    "mcap_floor": 5100,
    "mcap_ceiling": 50000,
    "min_age_seconds": 22,
    "max_age_seconds": 1320,
    "age_offset_seconds": 39,
    "txn_count_adjustment": 1.24,
    "min_volume_usd": 500,
    "min_volume_to_mcap_ratio": 0.005,
    "max_volume_to_mcap_ratio": 50,
    "min_buy_sell_ratio": 0.5,
    "min_pool_sol_bonding": 5,
    "min_pool_sol_graduated": 5,
    "creator_holdings_max": 0,
    "max_top_holder_pct": 100,
    "score_threshold_bonding": 40,
    "score_threshold_graduated": 40,
    "blocked_weekdays": [2],
    "blocked_hours_utc": [0, 19, 20, 21],
    "max_open": 5,
}
EXITS = {
    "trailing_stop_pct": 2,
    "trailing_arm_pct": 2,
    "hard_stop_pct": 8,
    "take_profit_pct": 150,
    "time_stop_minutes": 10,
}


class FakeStore:
    def __init__(self, position: dict[str, object]) -> None:
        self.position = position
        self.marks: list[tuple[str, float, bool]] = []
        self.closed: list[dict[str, object]] = []
        self.evaluations: list[dict[str, object]] = []

    async def fetch(self, query: str, *_args: object) -> list[dict[str, object]]:
        if "gate_config" in query:
            return [{"gate_name": name, "gate_value": value} for name, value in GATES.items()]
        return []

    async def list_open_positions(self, _strategy: str, _mode: str) -> list[dict[str, object]]:
        return [self.position]

    async def load_exit_config(self, _strategy: str) -> dict[str, float]:
        return EXITS

    async def update_position_mark(self, position_id: str, peak: float, armed: bool) -> None:
        self.marks.append((position_id, peak, armed))

    async def record_exit_evaluation(
        self, position_id, mint_address, **evaluation
    ) -> None:
        self.evaluations.append(
            {"position_id": position_id, "mint_address": mint_address, **evaluation}
        )

    async def close_position(
        self, position, trade, *, close_price_sol, close_reason, realized_pnl_sol
    ) -> None:
        self.closed.append(
            {
                "id": str(position["id"]),
                "close_price_sol": close_price_sol,
                "close_reason": close_reason,
                "realized_pnl_sol": realized_pnl_sol,
                "trade": trade,
            }
        )

    async def refresh_daily_stats(self, _strategy: str) -> None:
        pass

    async def create_position(self, _position, _trade) -> None:
        raise AssertionError("entry is not expected in an exit test")

    @asynccontextmanager
    async def entry_transaction(self, _mint_address):
        raise AssertionError("entry is not expected in an exit test")
        yield

    async def record_runtime_event(self, *_args, **_kwargs) -> None:
        pass


class FakeAdapter:
    mode = "paper"

    async def execute_swap(self, mint: str, side: Side, amount: float, _slippage: int) -> Trade:
        assert side == Side.SELL
        return Trade(
            mint_address=mint,
            side=side,
            amount_sol=amount * 0.000099,
            price_sol=0.000099,
            mode="paper",
        )

    async def close(self) -> None:
        pass


class FakeLiveMonitorAdapter(FakeAdapter):
    mode = "live"

    def __init__(self) -> None:
        self.sells = 0

    async def get_wallet_holdings(self) -> dict[str, float]:
        return {"live-mint": 100.0}

    def circuit_breaker_tripped(self) -> bool:
        return False

    async def sell(self, mint: str, token_amount: float, slippage_bps: int) -> Trade:
        self.sells += 1
        return Trade(
            mint_address=mint,
            side=Side.SELL,
            amount_sol=0.009,
            token_amount=token_amount,
            price_sol=0.00009,
            slippage_bps=slippage_bps,
            mode="live",
            status="confirmed",
        )

    async def verify_token_balance_cleared(self, _mint: str) -> float:
        return 0.0


class FakeJupiterPriceProvider:
    name = "jupiter"

    def __init__(self, prices: dict[str, float]) -> None:
        self.prices = prices
        self.calls: list[str] = []

    async def get_current_price(self, mint_address: str) -> float | None:
        self.calls.append(mint_address)
        return self.prices.get(mint_address)


def test_trailing_exit_uses_persisted_peak_and_arm_state(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-1",
            "mint_address": "mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.01,
            "token_amount": 100,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC),
        }
        store = FakeStore(position)
        executor = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        await executor.start()
        await executor.handle_price("mint", 0.000102)

        assert store.marks == [("position-1", 0.000102, True)]
        assert executor._positions["mint"]["peak_price_sol"] == 0.000102
        assert executor._positions["mint"]["trailing_armed"] is True
        await executor.handle_price("mint", 0.000099)
        assert len(store.closed) == 1
        assert store.closed[0]["close_price_sol"] == pytest.approx(0.00009996)
        assert store.closed[0]["close_reason"] == "trailing_stop"

    asyncio.run(run())


def test_no_price_time_stop_closes_at_entry_with_correct_paper_proceeds(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-time-stop",
            "mint_address": "no-price-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.01,
            "token_amount": 100,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC) - timedelta(minutes=11),
        }
        store = FakeStore(position)
        executor = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            mark_provider=FakeJupiterPriceProvider({}),
        )
        await executor.start()
        await executor._monitor_positions_once()

        assert len(store.closed) == 1
        closed = store.closed[0]
        assert closed["close_reason"] == "time_stop"
        assert closed["close_price_sol"] == 0.0001
        assert closed["realized_pnl_sol"] == 0.0
        trade = closed["trade"]
        assert trade["amount_sol"] == 0.01
        assert trade["token_amount"] == 100
        assert trade["metadata"]["trigger_price_sol"] == 0.0001
        assert store.evaluations[-1]["usable"] is False

    asyncio.run(run())


def test_mark_sla_closes_after_120_seconds_without_valid_mark(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-sla",
            "mint_address": "sla-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.01,
            "token_amount": 100,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC),
        }
        store = FakeStore(position)
        executor = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            mark_provider=FakeJupiterPriceProvider({}),
        )
        await executor.start()
        executor._last_valid_mark_at["sla-mint"] = time.monotonic() - 121
        await executor._monitor_positions_once()

        assert len(store.closed) == 1
        assert store.closed[0]["close_reason"] == "mark_sla_timeout"
        assert store.closed[0]["close_price_sol"] == 0.0001
        assert store.closed[0]["realized_pnl_sol"] == 0.0

    asyncio.run(run())


def test_paper_hard_stop_uses_stop_level_for_pnl_and_sell_proceeds(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-hard-stop",
            "mint_address": "hard-stop-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.02,
            "token_amount": 200,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC),
        }
        store = FakeStore(position)
        executor = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        await executor.start()
        await executor.handle_price("hard-stop-mint", 0.00007)

        closed = store.closed[0]
        assert closed["close_price_sol"] == pytest.approx(0.000092)
        assert closed["realized_pnl_sol"] == pytest.approx(-0.0016)
        trade = closed["trade"]
        assert trade["amount_sol"] == pytest.approx(0.0184)
        assert trade["price_sol"] == pytest.approx(0.000092)
        assert trade["metadata"]["mark_source"] == "pumpportal"
        assert trade["metadata"]["trigger_price_sol"] == 0.00007

    asyncio.run(run())


def test_concurrent_marks_for_one_mint_produce_one_close(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-lock",
            "mint_address": "locked-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.02,
            "token_amount": 200,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC),
        }
        store = FakeStore(position)
        executor = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        await executor.start()
        await asyncio.gather(
            executor.handle_price("locked-mint", 0.00009),
            executor.handle_price("locked-mint", 0.000089),
        )

        assert len(store.closed) == 1

    asyncio.run(run())


def test_quiet_position_uses_jupiter_mark_for_exit_evaluation(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-quiet",
            "mint_address": "quiet-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.01,
            "token_amount": 100,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC),
        }
        store = FakeStore(position)
        mark_provider = FakeJupiterPriceProvider({"quiet-mint": 0.000102})
        executor = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            mark_provider=mark_provider,
        )
        await executor.start()
        await executor._refresh_quiet_position_marks()
        await executor._monitor_positions_once()

        assert mark_provider.calls == ["quiet-mint"]
        assert executor._positions["quiet-mint"]["peak_price_sol"] == 0.000102
        assert store.marks == [("position-quiet", 0.000102, True)]
        assert store.evaluations == []
        assert store.closed == []

    asyncio.run(run())


def test_paper_take_profit_uses_configured_level_not_crashed_price(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-take-profit",
            "mint_address": "take-profit-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.02,
            "token_amount": 200,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC),
        }
        store = FakeStore(position)
        executor = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        await executor.start()
        await executor.handle_price("take-profit-mint", 0.0003)

        closed = store.closed[0]
        assert closed["close_reason"] == "take_profit"
        assert closed["close_price_sol"] == pytest.approx(0.00025)
        assert closed["realized_pnl_sol"] == pytest.approx(0.03)
        assert closed["trade"]["amount_sol"] == pytest.approx(0.05)

    asyncio.run(run())


def test_live_monitor_only_mode_hydrates_and_closes_open_position(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "live-position",
            "mint_address": "live-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.01,
            "token_amount": 100,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC),
        }
        store = FakeStore(position)
        adapter = FakeLiveMonitorAdapter()

        async def blocked_entries() -> tuple[str, ...]:
            return ("live_trading_env_not_enabled",)

        executor = StrategyExecutor(
            store,
            adapter,
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            entry_arming_check=blocked_entries,
        )
        await executor.start()
        await executor.run_cycle()
        await executor.handle_price("live-mint", 0.00009)

        assert executor._monitor_only is True
        assert adapter.sells == 1
        assert store.closed[0]["close_reason"] == "hard_stop"

    asyncio.run(run())

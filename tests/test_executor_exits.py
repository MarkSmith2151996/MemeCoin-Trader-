"""V2 executor exit lifecycle coverage using an in-memory store boundary."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from services.executor import StrategyExecutor
from src.core.models import Side, Trade

GATES = {
    "mcap_floor": 5100,
    "min_age_seconds": 22,
    "max_age_seconds": 1320,
    "min_volume_usd": 500,
    "min_buy_sell_ratio": 0.5,
    "min_pool_sol_bonding": 5,
    "min_pool_sol_graduated": 5,
    "creator_holdings_max": 0,
    "score_threshold_bonding": 40,
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
        self.closed: list[tuple[str, float]] = []

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

    async def close_position(
        self, position, _trade, *, close_price_sol, close_reason, realized_pnl_sol
    ) -> None:
        del close_reason, realized_pnl_sol
        self.closed.append((str(position["id"]), close_price_sol))

    async def refresh_daily_stats(self, _strategy: str) -> None:
        pass

    async def create_position(self, _position, _trade) -> None:
        raise AssertionError("entry is not expected in an exit test")

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
        await executor.handle_price("mint", 0.000099)
        assert store.closed == [("position-1", 0.000099)]

    asyncio.run(run())

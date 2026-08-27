"""V2 startup must hydrate persisted trailing state before evaluating marks."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from services.executor import StrategyExecutor
from tests.test_executor_exits import FakeAdapter, FakeStore


def test_restart_keeps_persisted_peak_without_rewriting_a_lower_mark(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-2",
            "mint_address": "mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.01,
            "token_amount": 100,
            "peak_price_sol": 0.000102,
            "trailing_armed": True,
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
        await executor.handle_price("mint", 0.000101)

        assert store.marks == []
        assert store.closed == []

    asyncio.run(run())

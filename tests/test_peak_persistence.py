"""V2 startup must hydrate persisted trailing state before evaluating marks."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

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


def test_new_peak_is_persisted_before_restart_and_drives_trailing_exit(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "position-restart",
            "mint_address": "restart-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.01,
            "token_amount": 100,
            "peak_price_sol": 0.0001,
            "trailing_armed": False,
            "opened_at": datetime.now(UTC),
        }
        store = FakeStore(position)
        first = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat-1",
            halt_path=tmp_path / "halt-1",
        )
        await first.start()
        await first.handle_price("restart-mint", 0.00011)

        assert store.marks == [("position-restart", 0.00011, True)]
        # Simulate the Hive row read by a freshly started process.
        position["peak_price_sol"] = store.marks[-1][1]
        position["trailing_armed"] = store.marks[-1][2]

        restarted = StrategyExecutor(
            store,
            FakeAdapter(),
            heartbeat_path=tmp_path / "heartbeat-2",
            halt_path=tmp_path / "halt-2",
        )
        await restarted.start()
        await restarted.handle_price("restart-mint", 0.000105)

        assert store.closed[0]["close_reason"] == "trailing_stop"
        assert store.closed[0]["close_price_sol"] == pytest.approx(0.0001078)

    asyncio.run(run())

"""V2 entry parity coverage for fresh marks and atomic loss rechecks."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from services.adapters.paper import PaperExecutionAdapter
from services.executor import StrategyExecutor, _entry_slippage_bps
from src.execution.price_provider import PriceResult


class FakeEntry:
    def __init__(self, rejection_reason: str | None = None) -> None:
        self.rejection_reason = rejection_reason
        self.created: list[tuple[dict, dict]] = []

    @property
    def allowed(self) -> bool:
        return self.rejection_reason is None

    async def create_position(self, position: dict, trade: dict) -> None:
        self.created.append((position, trade))


class FakeStore:
    def __init__(self, rejection_reason: str | None = None) -> None:
        self.entry = FakeEntry(rejection_reason)

    @asynccontextmanager
    async def entry_transaction(self, _mint_address: str):
        yield self.entry


class MarkProvider:
    name = "jupiter"

    def __init__(self, price: float | None, reason: str = "live_jupiter") -> None:
        self.result = PriceResult(price, reason)
        self.calls: list[str] = []

    async def get_price_with_diagnostic(self, mint_address: str) -> PriceResult:
        self.calls.append(mint_address)
        return self.result


def candidate(price_sol: float | None = 0.00009) -> dict[str, object]:
    return {
        "id": 1,
        "mint_address": "mint",
        "price_sol": price_sol,
        "source": "jupiter_recent",
        "pool_type": "graduated",
        "pool_sol": 10,
    }


def executor(
    tmp_path: Path,
    store: FakeStore,
    mark_provider: MarkProvider,
) -> StrategyExecutor:
    return StrategyExecutor(
        store,
        PaperExecutionAdapter(),
        heartbeat_path=tmp_path / "heartbeat",
        halt_path=tmp_path / "halt",
        mark_provider=mark_provider,
    )


def test_paper_entry_uses_fresh_price_v3_mark_not_discovery_price(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def run() -> None:
        monkeypatch.setenv("POSITION_SIZE_SOL", "0.02")
        store = FakeStore()
        marks = MarkProvider(0.0001)
        await executor(tmp_path, store, marks)._enter(candidate())

        assert marks.calls == ["mint"]
        position, trade = store.entry.created[0]
        assert position["entry_price_sol"] == 0.0001
        assert position["amount_sol"] == 0.02
        assert trade["price_sol"] == 0.0001
        assert trade["metadata"]["discovery_price_sol"] == 0.00009
        assert trade["metadata"]["entry_mark_source"] == "jupiter"
        assert datetime.fromisoformat(trade["metadata"]["entry_mark_timestamp"]).tzinfo == UTC

    asyncio.run(run())


def test_invalid_price_v3_mark_skips_candidate_without_fill(tmp_path: Path) -> None:
    async def run() -> None:
        store = FakeStore()
        marks = MarkProvider(None, "no_price")
        await executor(tmp_path, store, marks)._enter(candidate())

        assert marks.calls == ["mint"]
        assert store.entry.created == []

    asyncio.run(run())


def test_atomic_recent_hard_stop_recheck_happens_before_mark_or_fill(tmp_path: Path) -> None:
    async def run() -> None:
        store = FakeStore("recent_hard_stop")
        marks = MarkProvider(0.0001)
        await executor(tmp_path, store, marks)._enter(candidate())

        assert marks.calls == []
        assert store.entry.created == []

    asyncio.run(run())


def test_entry_slippage_matches_v1_pool_tiers() -> None:
    assert _entry_slippage_bps(None) is None
    assert _entry_slippage_bps(4.999) is None
    assert _entry_slippage_bps(5) == 300
    assert _entry_slippage_bps(20) == 300
    assert _entry_slippage_bps(20.001) == 100

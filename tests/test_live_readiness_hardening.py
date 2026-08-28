"""MT-677 V2 readiness hardening coverage without wallet or Hive side effects."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

import services.executor as executor_module
from services.executor import (
    FailClosed,
    OrphanedLiveStateFailClosed,
    StrategyExecutor,
)
from src.core.models import Side, Trade
from src.execution.price_provider import PriceResult

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


def _position(mint: str = "open-mint") -> dict[str, object]:
    return {
        "id": f"{mint}-position",
        "mint_address": mint,
        "entry_price_sol": 0.0001,
        "amount_sol": 0.01,
        "token_amount": 100.0,
        "peak_price_sol": 0.0001,
        "trailing_armed": False,
        "opened_at": datetime.now(UTC),
        "strategy": "BT",
    }


class Store:
    def __init__(self, position: dict[str, object] | None = None) -> None:
        self.position = position
        self.quarantined: list[dict[str, object]] = []
        self.events: list[tuple[str, str, dict | None]] = []
        self.closed: list[dict[str, object]] = []
        self.exit_evaluations: list[dict[str, object]] = []

    async def fetch(self, query: str, *_args: object) -> list[dict[str, object]]:
        if "gate_config" in query:
            return [{"gate_name": name, "gate_value": value} for name, value in GATES.items()]
        return []

    async def load_exit_config(self, _strategy: str) -> dict[str, float]:
        return EXITS

    async def list_open_positions(self, _strategy: str, _mode: str) -> list[dict[str, object]]:
        return [self.position] if self.position is not None else []

    async def list_open_live_positions(self) -> list[dict[str, object]]:
        return []

    async def list_quarantined_live_positions(self) -> list[dict[str, object]]:
        return self.quarantined

    async def update_position_mark(self, *_args: object) -> None:
        pass

    async def record_exit_evaluation(self, _position_id: str, _mint: str, **values: object) -> None:
        self.exit_evaluations.append(values)

    async def close_position(self, position: dict, trade: dict, **values: object) -> None:
        self.closed.append({"position": position, "trade": trade, **values})

    async def quarantine_position(self, position: dict[str, object], reason: str) -> None:
        self.quarantined.append({**position, "status": "quarantined", "reason": reason})

    async def record_runtime_event(
        self,
        event_type: str,
        reason: str,
        details: dict | None = None,
    ) -> None:
        self.events.append((event_type, reason, details))

    async def refresh_daily_stats(self, _strategy: str) -> None:
        pass


class Alerts:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str]] = []

    async def send(self, level: str, title: str, message: str) -> None:
        self.messages.append((level, title, message))


class MarkProvider:
    name = "jupiter"

    def __init__(self, prices: dict[str, float | None]) -> None:
        self.prices = prices

    async def get_price_with_diagnostic(self, mint: str) -> PriceResult:
        price = self.prices.get(mint)
        return PriceResult(price, "ok" if price is not None else "unavailable")


class LiveAdapter:
    mode = "live"

    def __init__(
        self,
        holdings: dict[str, float],
        accounts: list[dict[str, object]],
        *,
        sell_failures: int = 0,
    ) -> None:
        self.holdings = holdings
        self.accounts = accounts
        self.sell_failures = sell_failures
        self.slippage_attempts: list[int] = []

    async def get_wallet_holdings(self) -> dict[str, float]:
        return self.holdings

    async def get_token_accounts(self) -> list[dict[str, object]]:
        return self.accounts

    def circuit_breaker_tripped(self) -> bool:
        return False

    async def sell(self, mint: str, token_amount: float, slippage_bps: int) -> Trade:
        self.slippage_attempts.append(slippage_bps)
        if len(self.slippage_attempts) <= self.sell_failures:
            raise RuntimeError(f"sell rejected at {slippage_bps}bps")
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

    async def close(self) -> None:
        pass


def _account(mint: str, raw_amount: int, decimals: int = 6) -> dict[str, object]:
    return {"mint": mint, "raw_amount": raw_amount, "decimals": decimals}


def test_reconciliation_ignores_shared_clearance_dust_and_quarantined_mints(tmp_path: Path) -> None:
    async def run() -> None:
        store = Store()
        store.quarantined = [_position("quarantined-mint")]
        adapter = LiveAdapter(
            {
                "open-mint": 100.0,
                "dust-mint": 0.00001,
                "quarantined-mint": 50.0,
            },
            [
                _account("open-mint", 100_000_000),
                _account("dust-mint", 10),
                _account("quarantined-mint", 50_000_000),
            ],
        )
        executor = StrategyExecutor(
            store,
            adapter,
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            mark_provider=MarkProvider({"dust-mint": 0.01}),
        )
        executor._positions = {"open-mint": _position("open-mint")}

        assert await executor._reconcile_startup() is True
        assert executor._monitor_only is False
        assert store.events == []

    asyncio.run(run())


def test_reconciliation_wallet_only_value_sets_monitor_only_not_fatal(tmp_path: Path) -> None:
    async def run() -> None:
        store = Store()
        alerts = Alerts()
        adapter = LiveAdapter(
            {"open-mint": 100.0, "airdrop-mint": 1.0},
            [_account("open-mint", 100_000_000), _account("airdrop-mint", 1_000_000)],
        )
        executor = StrategyExecutor(
            store,
            adapter,
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            mark_provider=MarkProvider({"airdrop-mint": 0.01}),
            alert_manager=alerts,
        )
        executor._positions = {"open-mint": _position("open-mint")}

        assert await executor._reconcile_startup() is True
        assert executor._monitor_only is True
        assert store.events[0][0] == "wallet_only_holdings_monitor_only"
        assert alerts.messages[0][0] == "critical"

    asyncio.run(run())


def test_reconciliation_tracked_mismatch_remains_fatal(tmp_path: Path) -> None:
    async def run() -> None:
        adapter = LiveAdapter({"open-mint": 99.0}, [_account("open-mint", 99_000_000)])
        executor = StrategyExecutor(
            Store(),
            adapter,
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        executor._positions = {"open-mint": _position("open-mint")}

        with pytest.raises(FailClosed, match="reconciliation mismatch"):
            await executor._reconcile_startup()

    asyncio.run(run())


def test_sell_failure_quarantines_position_without_stopping_monitor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def run() -> None:
        monkeypatch.setattr(executor_module, "LIVE_EXIT_RETRY_BACKOFF_SECONDS", 0)
        position = _position()
        store = Store(position)
        alerts = Alerts()
        adapter = LiveAdapter(
            {"open-mint": 100.0},
            [_account("open-mint", 100_000_000)],
            sell_failures=3,
        )
        executor = StrategyExecutor(
            store,
            adapter,
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            alert_manager=alerts,
        )
        executor._positions = {"open-mint": position}
        executor._exits = EXITS

        await executor.handle_price("open-mint", 0.00009)
        await executor._monitor_positions_once()

        assert adapter.slippage_attempts == [300, 500, 1000]
        assert store.quarantined[0]["status"] == "quarantined"
        assert store.events[0][0] == "position_quarantined"
        assert executor._positions == {}
        assert executor._fatal_reason is None
        assert alerts.messages[0][0] == "critical"

    asyncio.run(run())


def test_feed_stale_uses_fallback_marks_for_hard_stop_then_recovers(tmp_path: Path) -> None:
    async def run() -> None:
        position = _position()
        store = Store(position)
        adapter = LiveAdapter({"open-mint": 100.0}, [_account("open-mint", 100_000_000)])
        executor = StrategyExecutor(
            store,
            adapter,
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            mark_provider=MarkProvider({"open-mint": 0.00009}),
        )
        executor._positions = {"open-mint": position}
        executor._exits = EXITS

        await executor._on_feed_stale()
        await executor._refresh_quiet_position_marks()
        await executor._monitor_positions_once()
        await executor._on_feed_recovered()

        assert store.closed[0]["close_reason"] == "hard_stop"
        assert executor._feed_stale_since is None

    asyncio.run(run())


def test_feed_stale_grace_expiry_escalates_only_after_grace(tmp_path: Path) -> None:
    executor = StrategyExecutor(
        Store(),
        LiveAdapter({"open-mint": 100.0}, [_account("open-mint", 100_000_000)]),
        heartbeat_path=tmp_path / "heartbeat",
        halt_path=tmp_path / "halt",
    )
    executor._positions = {"open-mint": _position()}
    executor._feed_stale_since = time.monotonic() - executor_module.FEED_STALE_GRACE_SECONDS - 1

    executor._check_feed_stale_grace()

    assert executor._fatal_reason == "PumpPortal global feed remained stale for 90s"


def test_paper_startup_refuses_unsettled_live_state_unless_overridden(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def live_state() -> tuple[str, ...]:
        return ("1 unsettled live Hive position(s): live-mint",)

    async def run() -> None:
        executor = StrategyExecutor(
            Store(),
            type("PaperAdapter", (), {"mode": "paper"})(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
            paper_live_state_check=live_state,
        )
        with pytest.raises(OrphanedLiveStateFailClosed, match="paper startup refused"):
            await executor.start()

        monkeypatch.setenv("MEMECOIN_ALLOW_ORPHANED_LIVE_STATE", "true")
        overridden = StrategyExecutor(
            Store(),
            type("PaperAdapter", (), {"mode": "paper"})(),
            heartbeat_path=tmp_path / "heartbeat-override",
            halt_path=tmp_path / "halt-override",
            paper_live_state_check=live_state,
        )
        await overridden.start()

    asyncio.run(run())

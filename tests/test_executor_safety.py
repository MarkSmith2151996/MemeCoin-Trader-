"""V2 process, live-guardrail, and wallet-clear safety coverage."""

from __future__ import annotations

import asyncio
import inspect
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from services.executor import FailClosed, StrategyExecutor
from src.core.models import Side, Trade
from src.execution.live_daily_caps import DailyLiveState


class DailyStateStore:
    def __init__(self, state: DailyLiveState) -> None:
        self.state = state
        self.closed: list[dict] = []

    async def load_daily_live_state(self) -> DailyLiveState:
        return self.state

    async def close_position(self, position, trade, **values) -> None:
        self.closed.append({"position": position, "trade": trade, **values})

    async def refresh_daily_stats(self, _strategy: str) -> None:
        pass


class LiveAdapter:
    mode = "live"

    def __init__(self, *, balance: float = 1.0, clear: bool = True) -> None:
        self.balance = balance
        self.clear = clear
        self.sells = 0
        self.verify_calls = 0

    async def get_sol_balance(self) -> float:
        return self.balance

    async def sell(self, mint: str, token_amount: float, slippage_bps: int) -> Trade:
        self.sells += 1
        return Trade(
            mint_address=mint,
            side=Side.SELL,
            amount_sol=0.021,
            token_amount=token_amount,
            price_sol=0.000105,
            slippage_bps=slippage_bps,
            mode="live",
            status="confirmed",
        )

    async def verify_token_balance_cleared(self, _mint: str) -> float:
        self.verify_calls += 1
        if not self.clear:
            raise RuntimeError("tokens remain")
        return 0.0


def _arm_live_env(monkeypatch) -> None:
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_CONFIRMATION_PHRASE", "I_UNDERSTAND_THIS_CAN_LOSE_REAL_SOL")
    monkeypatch.setenv("LIVE_KILL_SWITCH", "false")
    monkeypatch.setenv("MAX_LIVE_TRADE_SOL", "0.01")
    monkeypatch.setenv("MAX_LIVE_DAILY_TRADES", "3")
    monkeypatch.setenv("MAX_LIVE_DAILY_LOSS_SOL", "0.05")
    monkeypatch.setenv("MIN_LIVE_WALLET_BALANCE_SOL", "0.05")


def test_live_entry_guardrails_allow_valid_current_state(monkeypatch, tmp_path: Path) -> None:
    async def run() -> None:
        _arm_live_env(monkeypatch)
        store = DailyStateStore(
            DailyLiveState(datetime.now(UTC).date(), 0, 0.0),
        )
        executor = StrategyExecutor(
            store,
            LiveAdapter(balance=0.1),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        await executor._check_live_entry_guardrails(0.005)

    asyncio.run(run())


def test_live_entry_guardrails_fail_closed_when_not_armed(monkeypatch, tmp_path: Path) -> None:
    async def run() -> None:
        _arm_live_env(monkeypatch)
        monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")
        executor = StrategyExecutor(
            DailyStateStore(DailyLiveState(datetime.now(UTC).date(), 0, 0.0)),
            LiveAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        with pytest.raises(FailClosed, match="live_trading_env_not_enabled"):
            await executor._check_live_entry_guardrails(0.005)

    asyncio.run(run())


def test_live_entry_guardrails_enforce_daily_trade_cap(monkeypatch, tmp_path: Path) -> None:
    async def run() -> None:
        _arm_live_env(monkeypatch)
        executor = StrategyExecutor(
            DailyStateStore(DailyLiveState(datetime.now(UTC).date(), 3, 0.0)),
            LiveAdapter(),
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        with pytest.raises(FailClosed, match="daily_live_trade_cap_exhausted"):
            await executor._check_live_entry_guardrails(0.005)

    asyncio.run(run())


def test_live_entry_guardrails_enforce_daily_loss_and_wallet_reserve(
    monkeypatch,
    tmp_path: Path,
) -> None:
    async def run() -> None:
        _arm_live_env(monkeypatch)
        loss_blocked = StrategyExecutor(
            DailyStateStore(DailyLiveState(datetime.now(UTC).date(), 0, 0.05)),
            LiveAdapter(),
            heartbeat_path=tmp_path / "heartbeat-loss",
            halt_path=tmp_path / "halt-loss",
        )
        with pytest.raises(FailClosed, match="daily_live_loss_cap_exhausted"):
            await loss_blocked._check_live_entry_guardrails(0.005)

        balance_blocked = StrategyExecutor(
            DailyStateStore(DailyLiveState(datetime.now(UTC).date(), 0, 0.0)),
            LiveAdapter(balance=0.05),
            heartbeat_path=tmp_path / "heartbeat-balance",
            halt_path=tmp_path / "halt-balance",
        )
        with pytest.raises(FailClosed, match="wallet balance insufficient"):
            await balance_blocked._check_live_entry_guardrails(0.005)

    asyncio.run(run())


def test_live_close_keeps_position_open_when_wallet_does_not_clear(tmp_path: Path) -> None:
    async def run() -> None:
        position = {
            "id": "live-position",
            "mint_address": "live-mint",
            "entry_price_sol": 0.0001,
            "amount_sol": 0.02,
            "token_amount": 200,
            "opened_at": datetime.now(UTC),
        }
        store = DailyStateStore(DailyLiveState(datetime.now(UTC).date(), 0, 0.0))
        adapter = LiveAdapter(clear=False)
        executor = StrategyExecutor(
            store,
            adapter,
            heartbeat_path=tmp_path / "heartbeat",
            halt_path=tmp_path / "halt",
        )
        executor._positions["live-mint"] = position

        with pytest.raises(RuntimeError, match="tokens remain"):
            await executor._close_position(position, 0.000105, "trailing_stop")

        assert adapter.sells == 1
        assert adapter.verify_calls == 1
        assert store.closed == []
        assert "live-mint" in executor._positions

    asyncio.run(run())


def test_second_v2_executor_process_is_rejected(tmp_path: Path) -> None:
    lock_path = tmp_path / "memecoin_executor.lock"
    acquire = (
        "from pathlib import Path; "
        "from services.executor import _acquire_singleton_lock; "
        "_acquire_singleton_lock(Path(sys.argv[1]))"
    )
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            f"import sys,time; {acquire}; print('locked',flush=True); time.sleep(30)",
            str(lock_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "locked"
        second = subprocess.run(
            [sys.executable, "-c", f"import sys; {acquire}", str(lock_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        holder.terminate()
        holder.wait(timeout=5)

    assert second.returncode == 42
    assert "another memecoin executor instance is running" in second.stderr


def test_executor_cycle_does_not_wrap_transactions_in_wait_for() -> None:
    assert "asyncio.wait_for" not in inspect.getsource(StrategyExecutor.run)

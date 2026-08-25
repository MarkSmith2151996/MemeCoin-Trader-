"""MT-516: focused coverage for entry quality gates.

Covers the six improvements from the MT-515 analysis across Strategy A
(scripts/run_paper_loop.py) and Strategy B (scripts/run_strategy_b.py):
  1. Hard stop closes at the trigger price (not the deep mark)
  2. Trailing stop only arms after peak > entry + 2%
  3. Time-of-day gates (blocked UTC hours) with candidate_log rows
  4. Post-entry confirmation: early_exit_no_green at 90s without +1%
  5. No re-entry on losing mints: Strategy A applies a 2h cooldown
     (`has_recent_losing_close`), Strategy B keeps the permanent ban
  6. Concurrent position caps enforced at entry

All tests are offline — no real network or RugCheck calls.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import scripts.run_paper_loop as paper_loop
import scripts.run_strategy_b as strategy_b
from src.core.database import has_losing_close, has_recent_losing_close, init_db, record_entry_skip
from src.core.models import Position, Side, Trade

# ── Fixtures / fakes ─────────────────────────────────────────────────

class FakePrice:
    def __init__(self, price: float) -> None:
        self._price = price

    async def get_current_price(self, mint: str) -> float:
        return self._price


class FakeAdapter:
    mode = "paper"

    def __init__(self) -> None:
        self.sizes: list[float] = []
        self.slippages: list[int] = []

    async def execute_swap(
        self, mint: str, side: Side, size_sol: float, slippage_bps: int = 300,
    ) -> Trade:
        self.sizes.append(size_sol)
        self.slippages.append(slippage_bps)
        return Trade(
            mint_address=mint,
            side=side,
            amount_sol=size_sol,
            token_amount=1_000.0,
            price_sol=1.0,
        )


class FailingLiveAdapter:
    mode = "live"

    async def sell(self, mint: str, token_amount: float, slippage_bps: int = 300) -> Trade:
        raise RuntimeError("confirmed sell left tokens in wallet")


class EmptyBalanceLiveAdapter(FailingLiveAdapter):
    def __init__(self, balance: float | None) -> None:
        self.balance = balance

    async def get_token_balance(self, mint: str) -> float | None:
        return self.balance


class SlippageThenSuccessLiveAdapter:
    mode = "live"

    def __init__(self) -> None:
        self.slippages: list[int] = []

    async def sell(self, mint: str, token_amount: float, slippage_bps: int = 300) -> Trade:
        self.slippages.append(slippage_bps)
        if slippage_bps == 300:
            raise RuntimeError("Jupiter swap failed: custom program error: 0x1771")
        return Trade(
            mint_address=mint,
            side=Side.SELL,
            amount_sol=0.04,
            token_amount=token_amount,
            price_sol=0.00004,
            slippage_bps=slippage_bps,
            mode="live",
            status="confirmed",
        )


class RecoveringLiveAdapter:
    mode = "live"

    def __init__(self, *, sell_fails: bool = False) -> None:
        self.sell_fails = sell_fails
        self.sell_calls: list[tuple[str, float, int]] = []
        self.breaker_trips: list[dict] = []

    async def execute_swap(
        self, mint: str, side: Side, size_sol: float, slippage_bps: int = 300,
    ) -> Trade:
        return Trade(
            mint_address=mint,
            side=side,
            amount_sol=size_sol,
            token_amount=1_000.0,
            price_sol=0.00005,
            slippage_bps=slippage_bps,
            tx_signature="live-buy-signature",
            mode="live",
            status="confirmed",
        )

    async def sell(
        self, mint: str, token_amount: float, slippage_bps: int = 300,
    ) -> Trade:
        self.sell_calls.append((mint, token_amount, slippage_bps))
        if self.sell_fails:
            raise RuntimeError("sell-back failed")
        return Trade(
            mint_address=mint,
            side=Side.SELL,
            amount_sol=0.049,
            token_amount=token_amount,
            price_sol=0.000049,
            slippage_bps=slippage_bps,
            tx_signature="recovery-sell-signature",
            mode="live",
            status="confirmed",
        )

    def trip_circuit_breaker(self, **kwargs) -> None:
        self.breaker_trips.append(kwargs)


class PreSoldLiveAdapter:
    mode = "live"

    def __init__(self) -> None:
        self.sell_calls = 0

    async def get_token_balance(self, mint: str) -> float:
        return 0.0

    async def sell(self, mint: str, token_amount: float, slippage_bps: int = 300) -> Trade:
        self.sell_calls += 1
        raise AssertionError("sell should not run for an empty wallet balance")


class LiveSafetyAdapter:
    mode = "live"

    def __init__(self, holdings: dict[str, float], sol_balance: float) -> None:
        self.holdings = holdings
        self.sol_balance = sol_balance
        self.breaker_trips: list[dict] = []

    async def get_wallet_holdings(self) -> dict[str, float]:
        return self.holdings

    async def get_sol_balance(self) -> float:
        return self.sol_balance

    def trip_circuit_breaker(self, **kwargs) -> None:
        self.breaker_trips.append(kwargs)


class FakeManager:
    def __init__(self, open_positions: list[Position] | None = None) -> None:
        self._open = list(open_positions or [])
        self.closed_with: list[tuple[str, float, float | None]] = []
        self.open_position_id = "pos-1"

    async def get_all_open(
        self, *, include_archived: bool = False, mode: str | None = None,
    ) -> list[Position]:
        return list(self._open)

    async def get_position(self, mint: str, *, mode: str | None = None) -> Position | None:
        return None

    async def open_position(self, trade: Trade, signal) -> Position:
        return Position(
            id=self.open_position_id,
            mint_address=trade.mint_address,
            entry_trade_id=trade.id,
            amount_sol=trade.amount_sol,
            token_amount=1_000.0,
            entry_price_sol=1.0,
        )

    async def close_position(
        self,
        mint: str,
        exit_price_sol: float | None = None,
        *,
        mode: str | None = None,
        peak_price_sol: float | None = None,
    ) -> Position | None:
        self.closed_with.append((mint, exit_price_sol, peak_price_sol))
        return None


def make_position(
    mint: str = "Mint1",
    entry: float = 1.0,
    opened_minutes_ago: float = 5.0,
) -> Position:
    return Position(
        id=f"{mint}-pos",
        mint_address=mint,
        entry_trade_id=f"{mint}-trade",
        amount_sol=0.05,
        token_amount=1_000.0,
        entry_price_sol=entry,
        opened_at=datetime.now(UTC) - timedelta(minutes=opened_minutes_ago),
    )


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "trades.db"
    asyncio.run(init_db(path))
    return path


def seed_closed_position(
    db_path: Path, mint: str, pnl: float, *, strategy: str = "A", closed_at: str | None = None,
) -> None:
    with sqlite3.connect(db_path) as db:
        db.execute(
            """INSERT INTO positions (
                id, mint_address, entry_trade_id, amount_sol, token_amount, entry_price_sol,
                status, opened_at, closed_at, realized_pnl_sol, partial_exits_json, strategy
            ) VALUES (?, ?, 't', 1, 1, 1, 'CLOSED', 'now', ?, ?, '{}', ?)""",
            (f"{mint}-seed", mint, closed_at, pnl, strategy),
        )
        db.commit()


def candidate_log_rows(db_path: Path, strategy: str, gate: str) -> list[str]:
    with sqlite3.connect(db_path) as db:
        rows = db.execute(
            "SELECT gates_failed FROM candidate_log WHERE strategy = ?",
            (strategy,),
        ).fetchall()
    reasons = []
    for (payload,) in rows:
        parsed = json.loads(payload)
        if parsed.get("gate") == gate:
            reasons.append(parsed.get("reason"))
    return reasons


def sell_trades(db_path: Path) -> list[tuple[float, str]]:
    with sqlite3.connect(db_path) as db:
        rows = db.execute(
            "SELECT price_sol, metadata_json FROM trades WHERE side = 'SELL'",
        ).fetchall()
    return [
        (price, json.loads(md).get("metadata", {}).get("close_reason"))
        for price, md in rows
    ]


# ── 1. Hard stop closes at trigger price ─────────────────────────────

def test_strategy_a_hard_stop_closes_at_trigger_price(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0)])
    paper_loop.peak_prices.clear()
    danger = asyncio.run(paper_loop.monitor_positions(manager, FakePrice(0.85), db))
    trigger = 1.0 - paper_loop.HARD_STOP_PCT / 100
    assert manager.closed_with == [("Mint1", trigger, 1.0)]
    assert sell_trades(db) == [(trigger, "hard_stop")]
    assert danger is False


def test_strategy_b_hard_stop_closes_at_trigger_price(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0)])
    strategy_b.peak_prices.clear()
    danger = asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.65), db))
    trigger = strategy_b.HARD_STOP_MULTIPLIER
    assert manager.closed_with == [("Mint1", trigger, 1.0)]
    assert sell_trades(db) == [(trigger, "hard_stop")]
    assert danger is False


def test_strategy_b_monitor_silently_skips_mint_while_close_is_in_progress(
    db: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    manager = FakeManager([make_position(entry=1.0)])
    started = asyncio.Event()
    release = asyncio.Event()
    close_calls = 0

    async def slow_close(*args, **kwargs) -> Trade:
        nonlocal close_calls
        close_calls += 1
        started.set()
        await release.wait()
        return Trade(
            mint_address="Mint1",
            side=Side.SELL,
            amount_sol=0.92,
            token_amount=1_000.0,
            price_sol=0.92,
            mode="paper",
            status="simulated",
        )

    monkeypatch.setattr(strategy_b, "_adapter_close", slow_close)
    strategy_b.peak_prices.clear()
    strategy_b._selling_in_progress.clear()

    async def verify() -> None:
        first_monitor = asyncio.create_task(
            strategy_b.monitor_positions(manager, FakePrice(0.65), db),
        )
        await started.wait()
        await strategy_b.monitor_positions(manager, FakePrice(0.65), db)
        release.set()
        await first_monitor

    with caplog.at_level("ERROR"):
        asyncio.run(verify())

    assert close_calls == 1
    assert "CLOSE FAILED" not in caplog.text
    assert not strategy_b._selling_in_progress


def test_strategy_b_monitor_retries_after_failed_close(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = FakeManager([make_position(entry=1.0)])
    close_calls = 0

    async def failed_close(*args, **kwargs) -> None:
        nonlocal close_calls
        close_calls += 1
        return None

    monkeypatch.setattr(strategy_b, "_adapter_close", failed_close)
    strategy_b.peak_prices.clear()
    strategy_b._selling_in_progress.clear()

    asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.65), db))
    asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.65), db))

    assert close_calls == 2
    assert not strategy_b._selling_in_progress


def test_strategy_a_danger_zone_triggers_fast_polling(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=0.5)])
    paper_loop.peak_prices.clear()
    danger = asyncio.run(paper_loop.monitor_positions(manager, FakePrice(0.93), db))
    assert danger is True
    assert manager.closed_with == []


def test_strategy_b_danger_zone_triggers_fast_polling(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=0.5)])
    strategy_b.peak_prices.clear()
    danger = asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.93), db))
    assert danger is True
    assert manager.closed_with == []


# ── 2. Trailing stop arm requirement ─────────────────────────────────

def test_trailing_stop_not_armed_below_entry_plus_2pct(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=0.5)])
    paper_loop.peak_prices.clear()
    paper_loop.peak_prices["Mint1"] = 1.01  # green-ish but never +2%
    danger = asyncio.run(paper_loop.monitor_positions(manager, FakePrice(0.94), db))
    assert manager.closed_with == []  # trailing must NOT fire
    assert sell_trades(db) == []
    assert danger is True


def test_trailing_stop_arms_above_entry_plus_2pct(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0)])
    paper_loop.peak_prices.clear()
    paper_loop.peak_prices["Mint1"] = 1.03
    danger = asyncio.run(paper_loop.monitor_positions(manager, FakePrice(0.98), db))
    assert manager.closed_with == [("Mint1", 0.98, 1.03)]
    assert sell_trades(db) == [(0.98, "trailing_stop")]


def test_strategy_b_trailing_stop_matches_backtest_stop_price(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=0.5)])
    strategy_b.peak_prices.clear()
    strategy_b.peak_prices["Mint1"] = 1.02
    danger = asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.999), db))
    assert manager.closed_with == [("Mint1", 0.9996, 1.02)]
    assert sell_trades(db) == [(0.9996, "trailing_stop")]
    assert danger is False


# ── 4. Post-entry confirmation exit ──────────────────────────────────

def test_early_exit_when_never_green_within_90s(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=2.0)])
    paper_loop.peak_prices.clear()
    danger = asyncio.run(paper_loop.monitor_positions(manager, FakePrice(0.99), db))
    assert manager.closed_with == [("Mint1", 0.99, 1.0)]
    assert sell_trades(db) == [(0.99, "early_exit_no_green")]
    assert danger is False


def test_no_early_exit_before_90s(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=1.0)])
    paper_loop.peak_prices.clear()
    danger = asyncio.run(paper_loop.monitor_positions(manager, FakePrice(0.99), db))
    assert manager.closed_with == []
    assert sell_trades(db) == []
    assert danger is False


def test_no_early_exit_when_position_went_green(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=2.0)])
    paper_loop.peak_prices.clear()
    paper_loop.peak_prices["Mint1"] = 1.03
    danger = asyncio.run(paper_loop.monitor_positions(manager, FakePrice(1.01), db))
    assert manager.closed_with == []
    assert sell_trades(db) == []
    assert danger is False


def test_strategy_b_does_not_early_exit_before_backtest_time_stop(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=2.0)])
    strategy_b.peak_prices.clear()
    danger = asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.99), db))
    assert manager.closed_with == []
    assert sell_trades(db) == []
    assert danger is False


def test_strategy_b_take_profit_matches_backtest_multiplier(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0)])
    strategy_b.peak_prices.clear()
    trigger = strategy_b.TAKE_PROFIT_MULTIPLIER
    danger = asyncio.run(strategy_b.monitor_positions(manager, FakePrice(trigger + 0.01), db))
    assert manager.closed_with == [("Mint1", trigger, trigger + 0.01)]
    assert sell_trades(db) == [(trigger, "take_profit")]
    assert danger is False


def test_strategy_b_keeps_live_position_open_when_sell_verification_fails(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")
    manager = FakeManager([make_position(entry=1.0)])
    strategy_b.peak_prices.clear()

    asyncio.run(
        strategy_b.monitor_positions(
            manager,
            FakePrice(strategy_b.TAKE_PROFIT_MULTIPLIER + 0.01),
            db,
            adapter=FailingLiveAdapter(),
        ),
    )

    assert manager.closed_with == []
    assert sell_trades(db) == []


def test_strategy_b_retries_slippage_sell_once_at_500_bps(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")
    adapter = SlippageThenSuccessLiveAdapter()

    trade = asyncio.run(
        strategy_b._adapter_close(
            make_position(), 1.0, "hard_stop", db, adapter,
        ),
    )

    assert trade is not None
    assert adapter.slippages == [300, 500]
    assert sell_trades(db) == [(0.00004, "hard_stop")]


def test_strategy_b_abandons_live_position_without_wallet_tokens(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")
    manager = FakeManager([make_position(entry=1.0)])
    strategy_b.peak_prices.clear()

    asyncio.run(
        strategy_b.monitor_positions(
            manager,
            FakePrice(strategy_b.TAKE_PROFIT_MULTIPLIER + 0.01),
            db,
            adapter=EmptyBalanceLiveAdapter(0.0),
        ),
    )

    assert manager.closed_with == [("Mint1", 0, strategy_b.TAKE_PROFIT_MULTIPLIER + 0.01)]
    with sqlite3.connect(db) as db_conn:
        row = db_conn.execute(
            "SELECT amount_sol, price_sol, status FROM trades WHERE side = 'SELL'",
        ).fetchone()
    assert row == (0.0, 0.0, "abandoned")


def test_strategy_b_unknown_wallet_balance_never_closes_live_position(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")
    manager = FakeManager([make_position(entry=1.0)])

    asyncio.run(
        strategy_b.monitor_positions(
            manager,
            FakePrice(strategy_b.TAKE_PROFIT_MULTIPLIER + 0.01),
            db,
            adapter=EmptyBalanceLiveAdapter(None),
        ),
    )

    assert manager.closed_with == []
    assert sell_trades(db) == []


def test_strategy_b_precheck_skips_sell_when_wallet_tokens_are_gone(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")
    adapter = PreSoldLiveAdapter()

    trade = asyncio.run(
        strategy_b._adapter_close(make_position(), 1.0, "hard_stop", db, adapter),
    )

    assert trade is not None
    assert trade.status == "abandoned"
    assert trade.metadata["pre_sell_balance_check"] is True
    assert adapter.sell_calls == 0


def test_strategy_b_live_record_failure_sells_back_and_trips_breaker(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "BLOCKED_UTC_HOURS", frozenset())
    monkeypatch.setattr(strategy_b, "BLOCKED_WEEKDAYS", frozenset())
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")

    async def fail_record(*args, **kwargs) -> None:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(strategy_b, "record_trade", fail_record)
    adapter = RecoveringLiveAdapter()

    result = asyncio.run(
        strategy_b.try_enter(
            "LiveRecordFailure", "LIVE", FakePrice(0.00005), adapter,
            FakeManager(), db, pool_sol=100.0,
        ),
    )

    assert result is None
    assert adapter.sell_calls == [("LiveRecordFailure", 1_000.0, 500)]
    assert adapter.breaker_trips[0]["reason"] == "database_failure"
    assert adapter.breaker_trips[0]["signature_attempt"] == "live-buy-signature"


def test_strategy_b_live_open_failure_attempts_sell_back_even_when_it_fails(
    db: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(strategy_b, "BLOCKED_UTC_HOURS", frozenset())
    monkeypatch.setattr(strategy_b, "BLOCKED_WEEKDAYS", frozenset())
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")
    manager = FakeManager()

    async def fail_open(trade: Trade, signal) -> Position:
        raise RuntimeError("position insert failed")

    manager.open_position = fail_open
    adapter = RecoveringLiveAdapter(sell_fails=True)

    with caplog.at_level("CRITICAL"):
        result = asyncio.run(
            strategy_b.try_enter(
                "LiveOpenFailure", "LIVE", FakePrice(0.00005), adapter,
                manager, db, pool_sol=100.0,
            ),
        )

    assert result is None
    assert adapter.sell_calls == [("LiveOpenFailure", 1_000.0, 500)]
    assert adapter.breaker_trips[0]["reason"] == "database_failure"
    assert "manual recovery required" in caplog.text


def test_strategy_b_live_safety_logs_orphan_and_phantom_and_trips(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = FakeManager([make_position("PhantomMint")])
    adapter = LiveSafetyAdapter({"OrphanMint": 42.0}, sol_balance=1.0)

    with caplog.at_level("WARNING"):
        asyncio.run(strategy_b._run_live_safety_checks(manager, adapter, 0.05))

    assert "orphan token detected mint=OrphanMint balance=42.0" in caplog.text
    assert "phantom position mint=PhantomMint" in caplog.text
    assert adapter.breaker_trips[0]["reason"] == "position_reconciliation"


def test_strategy_b_live_safety_trips_on_low_sol_balance(
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter = LiveSafetyAdapter({}, sol_balance=0.049)

    with caplog.at_level("INFO"):
        asyncio.run(strategy_b._run_live_safety_checks(FakeManager(), adapter, 0.05))

    assert "WALLET_BALANCE sol=0.0490" in caplog.text
    assert adapter.breaker_trips[0]["reason"] == "low_wallet_balance"


# ── 3. Time-of-day gates ─────────────────────────────────────────────

def _patched_hours() -> frozenset[int]:
    return frozenset({datetime.now(UTC).hour})


def test_strategy_a_time_gate_blocks_and_logs(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(paper_loop, "BLOCKED_UTC_HOURS", _patched_hours())
    manager = FakeManager()
    ok = asyncio.run(
        paper_loop.try_enter("Mint1", FakePrice(1.0), FakeAdapter(), manager, db, ticker="TST"),
    )
    assert ok is False
    assert candidate_log_rows(db, "A", "time_gate")
    assert manager.closed_with == []


def test_strategy_b_time_gate_blocks_and_logs(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy_b, "BLOCKED_UTC_HOURS", _patched_hours())
    manager = FakeManager()
    result = asyncio.run(
        strategy_b.try_enter("Mint1", "TST", FakePrice(1.0), FakeAdapter(), manager, db),
    )
    assert result is None
    assert candidate_log_rows(db, "B", "time_gate")


def test_strategy_b_monitor_uses_cached_price_without_provider_lookup(db: Path) -> None:
    class UnexpectedProviderLookup:
        async def get_current_price(self, mint: str) -> float:
            raise AssertionError(f"provider lookup should not run for cached {mint}")

    manager = FakeManager([make_position(entry=1.0)])
    danger = asyncio.run(
        strategy_b.monitor_positions(
            manager,
            UnexpectedProviderLookup(),
            db,
            price_overrides={"Mint1": 0.94},
            allow_provider_lookup=False,
        ),
    )
    assert danger is True


def test_strategy_b_live_monitor_rejects_missing_adapter(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")

    with pytest.raises(RuntimeError, match="non-null live adapter"):
        asyncio.run(strategy_b.monitor_positions(FakeManager(), FakePrice(1.0), db))


def test_strategy_b_live_close_persists_actual_fill_and_sells_wallet_balance(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")

    class FilledLiveAdapter:
        mode = "live"

        def __init__(self) -> None:
            self.sell_amounts: list[float] = []

        async def get_token_balance(self, mint: str) -> float:
            return 1_250.0

        async def sell(self, mint: str, token_amount: float, slippage_bps: int = 300) -> Trade:
            self.sell_amounts.append(token_amount)
            return Trade(
                mint_address=mint,
                side=Side.SELL,
                amount_sol=0.04,
                token_amount=token_amount,
                price_sol=0.00004,
                slippage_bps=slippage_bps,
                mode="live",
                status="confirmed",
            )

    adapter = FilledLiveAdapter()
    manager = FakeManager([make_position(entry=0.00005)])
    strategy_b.peak_prices.clear()

    asyncio.run(
        strategy_b.monitor_positions(
            manager,
            FakePrice(0.00001),
            db,
            adapter=adapter,
        ),
    )

    assert adapter.sell_amounts == [1_250.0]
    assert manager.closed_with == [("Mint1", 0.00004, 0.00005)]
    with sqlite3.connect(db) as db_conn:
        price, metadata_json = db_conn.execute(
            "SELECT price_sol, metadata_json FROM trades WHERE side = 'SELL'",
        ).fetchone()
    assert price == pytest.approx(0.00004)
    assert json.loads(metadata_json)["metadata"]["trigger_price_sol"] == pytest.approx(0.000046)


def test_strategy_b_confirmed_unpriced_live_sell_closes_once_at_zero(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy_b, "EXECUTION_MODE", "live")

    class ConfirmedUnpricedAdapter:
        mode = "live"

        def __init__(self) -> None:
            self.sell_calls = 0

        async def get_token_balance(self, mint: str) -> float:
            return 1_000.0

        async def sell(self, mint: str, token_amount: float, slippage_bps: int = 300) -> Trade:
            self.sell_calls += 1
            return Trade(
                mint_address=mint,
                side=Side.SELL,
                amount_sol=0,
                token_amount=token_amount,
                price_sol=0,
                slippage_bps=slippage_bps,
                tx_signature="confirmed-unpriced-signature",
                mode="live",
                status="confirmed_unpriced",
                metadata={"token_balance_after": 0.0, "fill_reconciled": False},
            )

    adapter = ConfirmedUnpricedAdapter()
    manager = FakeManager([make_position(entry=1.0)])
    strategy_b.peak_prices.clear()

    asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.5), db, adapter=adapter))

    assert adapter.sell_calls == 1
    assert manager.closed_with == [("Mint1", 0.0, 1.0)]
    assert sell_trades(db) == [(0.0, "hard_stop")]


# ── 5. Loss-ban expiry ───────────────────────────────────────────────

def test_has_losing_close_helpers(db: Path) -> None:
    assert asyncio.run(has_losing_close(db, "UnknownMint")) is False
    seed_closed_position(db, "Loser", pnl=-0.002)
    assert asyncio.run(has_losing_close(db, "Loser")) is True
    seed_closed_position(db, "Winner", pnl=+0.1)
    assert asyncio.run(has_losing_close(db, "Winner")) is False


def test_has_losing_close_checks_all_strategies(db: Path) -> None:
    seed_closed_position(db, "BLoser", pnl=-0.005, strategy="B")
    assert asyncio.run(has_losing_close(db, "BLoser")) is True


def test_has_losing_close_ignores_open_positions(db: Path) -> None:
    with sqlite3.connect(db) as db_conn:
        db_conn.execute(
            """INSERT INTO positions (
                id, mint_address, entry_trade_id, amount_sol, token_amount, entry_price_sol,
                status, opened_at, realized_pnl_sol, partial_exits_json, strategy
            ) VALUES ('open-seed', 'OpenLoser', 't', 1, 1, 1, 'OPEN', 'now', -0.01, '{}', 'A')""",
        )
        db_conn.commit()
    assert asyncio.run(has_losing_close(db, "OpenLoser")) is False


def test_has_recent_losing_close_cooldown_window(db: Path) -> None:
    assert asyncio.run(has_recent_losing_close(db, "UnknownMint")) is False
    now = datetime.now(UTC)
    seed_closed_position(db, "FreshLoser", pnl=-0.002, closed_at=now.isoformat())
    seed_closed_position(db, "OldLoser", pnl=-0.002,
                         closed_at=(now - timedelta(hours=3)).isoformat())
    seed_closed_position(db, "Winner", pnl=+0.1, closed_at=now.isoformat())
    seed_closed_position(db, "NoTimestamp", pnl=-0.002, closed_at=None)
    assert asyncio.run(has_recent_losing_close(db, "FreshLoser")) is True
    assert asyncio.run(has_recent_losing_close(db, "OldLoser")) is False
    assert asyncio.run(has_recent_losing_close(db, "Winner")) is False
    assert asyncio.run(has_recent_losing_close(db, "NoTimestamp")) is False


def test_has_recent_losing_close_honors_custom_cooldown(db: Path) -> None:
    now = datetime.now(UTC)
    seed_closed_position(db, "TwoHourOld", pnl=-0.002,
                         closed_at=(now - timedelta(hours=2, minutes=1)).isoformat())
    assert asyncio.run(has_recent_losing_close(db, "TwoHourOld", cooldown_minutes=120)) is False
    assert asyncio.run(has_recent_losing_close(db, "TwoHourOld", cooldown_minutes=180)) is True


def test_strategy_a_repeat_loser_fresh_loss_blocked(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(paper_loop, "BLOCKED_UTC_HOURS", frozenset())
    seed_closed_position(db, "Loser", pnl=-0.002, closed_at=datetime.now(UTC).isoformat())
    manager = FakeManager()
    ok = asyncio.run(
        paper_loop.try_enter("Loser", FakePrice(1.0), FakeAdapter(), manager, db, ticker="TST"),
    )
    assert ok is False
    assert candidate_log_rows(db, "A", "repeat_loser")
    assert manager.closed_with == []


def test_strategy_a_repeat_loser_allowed_after_cooldown(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_closed_position(db, "OldLoser", pnl=-0.002,
                         closed_at=(datetime.now(UTC) - timedelta(hours=3)).isoformat())
    monkeypatch.setattr(paper_loop, "RUGCHECK_ENABLED", False)
    monkeypatch.setattr(paper_loop, "BLOCKED_UTC_HOURS", frozenset())

    async def no_metadata(*args, **kwargs) -> dict:
        return {}

    monkeypatch.setattr(paper_loop, "fetch_entry_metadata", no_metadata)
    adapter = FakeAdapter()
    manager = FakeManager()
    ok = asyncio.run(
        paper_loop.try_enter("OldLoser", FakePrice(1.0), adapter, manager, db, ticker="TST"),
    )
    assert ok is True
    assert candidate_log_rows(db, "A", "repeat_loser") == []
    expected_size = paper_loop.POSITION_SIZE_SOL
    if datetime.now(UTC).weekday() == 5:
        expected_size *= paper_loop.SATURDAY_SIZE_MULTIPLIER
    assert adapter.sizes == [expected_size]


def test_strategy_b_repeat_loser_blocked_within_ttl(
    db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # MT-593: Wednesday is blocked again — freeze the weekday gate open so the
    # repeat_loser gate (the gate under test) is the one that rejects.
    monkeypatch.setattr(strategy_b, "BLOCKED_WEEKDAYS", frozenset())
    monkeypatch.setattr(strategy_b, "BLOCKED_UTC_HOURS", frozenset())
    seed_closed_position(db, "Loser", pnl=-0.002, closed_at=datetime.now(UTC).isoformat())
    manager = FakeManager()
    result = asyncio.run(
        strategy_b.try_enter("Loser", "TST", FakePrice(1.0), FakeAdapter(), manager, db),
    )
    assert result is None
    assert candidate_log_rows(db, "B", "repeat_loser")


def test_strategy_b_loss_ban_expires_after_ttl(db: Path) -> None:
    seed_closed_position(
        db,
        "OldLoser",
        pnl=-0.002,
        closed_at=(datetime.now(UTC) - timedelta(hours=25)).isoformat(),
    )
    assert asyncio.run(
        has_recent_losing_close(
            db,
            "OldLoser",
            cooldown_minutes=strategy_b.LOSS_BAN_TTL_HOURS * 60,
        ),
    ) is False


# ── 6. Position caps ─────────────────────────────────────────────────

def test_strategy_a_cap_blocks_entry_at_limit(db: Path) -> None:
    manager = FakeManager(
        [make_position("A"), make_position("B"), make_position("C"), make_position("D")],
    )
    ok = asyncio.run(
        paper_loop.try_enter("New", FakePrice(1.0), FakeAdapter(), manager, db, ticker="TST"),
    )
    assert ok is False
    assert manager.closed_with == []


def test_strategy_a_pending_entries_reserve_capacity(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(paper_loop, "BLOCKED_UTC_HOURS", frozenset())
    paper_loop.pending_entries.clear()
    paper_loop.pending_entries["Pending1"] = {
        "price": 1.0, "time": 0.0, "ticker": "P1", "size_multiplier": 1.0,
    }
    manager = FakeManager([make_position("A"), make_position("B"), make_position("C")])
    ok = asyncio.run(
        paper_loop.try_enter("New", FakePrice(1.0), FakeAdapter(), manager, db, ticker="TST"),
    )
    assert ok is False
    paper_loop.pending_entries.clear()


def test_strategy_b_cap_blocks_entry_at_limit(db: Path) -> None:
    manager = FakeManager(
        [make_position(f"P{i}") for i in range(5)],
    )
    result = asyncio.run(
        strategy_b.try_enter("New", "TST", FakePrice(1.0), FakeAdapter(), manager, db),
    )
    assert result is None


# ── Saturday halving ─────────────────────────────────────────────────

def _next_saturday() -> datetime:
    today = datetime.now(UTC).date()
    days_ahead = (5 - today.weekday()) % 7
    return datetime.combine(today + timedelta(days=days_ahead), datetime.min.time(), tzinfo=UTC)


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: N805 - datetime API
        fixed = _next_saturday()
        return cls(
            fixed.year, fixed.month, fixed.day, fixed.hour, fixed.minute, tzinfo=fixed.tzinfo,
        )


def test_strategy_a_saturday_halves_position_size(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _FixedDatetime.now().weekday() == 5
    monkeypatch.setattr("datetime.datetime", _FixedDatetime)
    monkeypatch.setattr(paper_loop, "RUGCHECK_ENABLED", False)
    monkeypatch.setattr(paper_loop, "BLOCKED_UTC_HOURS", frozenset())
    async def no_metadata(*args, **kwargs) -> dict:
        return {}
    monkeypatch.setattr(paper_loop, "fetch_entry_metadata", no_metadata)
    adapter = FakeAdapter()
    manager = FakeManager()
    ok = asyncio.run(
        paper_loop.try_enter("SatMint", FakePrice(1.0), adapter, manager, db, ticker="TST"),
    )
    assert ok is True
    assert adapter.sizes == [paper_loop.POSITION_SIZE_SOL * paper_loop.SATURDAY_SIZE_MULTIPLIER]


def test_strategy_b_uses_flat_position_size_on_saturday(
    db: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _FixedDatetime.now().weekday() == 5
    monkeypatch.setattr(strategy_b, "datetime", _FixedDatetime)
    monkeypatch.setattr(strategy_b, "BLOCKED_UTC_HOURS", frozenset())
    adapter = FakeAdapter()
    manager = FakeManager()
    result = asyncio.run(
        strategy_b.try_enter(
            "SatMint", "TST", FakePrice(1.0), adapter, manager, db, pool_sol=100.0,
        ),
    )
    assert result == "pos-1"
    assert adapter.sizes == [strategy_b.POSITION_SIZE_SOL]
    # MT-588/MT-590: thick pool (>20 SOL) -> 1% (100 bps) tiered slippage.
    assert adapter.slippages == [strategy_b.SLIPPAGE_BPS_THICK_POOL]


def test_strategy_b_skips_entry_when_pool_too_thin(db: Path) -> None:
    adapter = FakeAdapter()
    manager = FakeManager()
    result = asyncio.run(
        strategy_b.try_enter(
            "ThinMint", "TST", FakePrice(1.0), adapter, manager, db, pool_sol=4.0,
        ),
    )
    assert result is None
    assert adapter.sizes == []


# ── DB helper: record_entry_skip ─────────────────────────────────────

def test_record_entry_skip_persists_gate_and_reason(db: Path) -> None:
    row_id = asyncio.run(record_entry_skip(
        db, strategy="A", mint_address="SkipMint", ticker="TST",
        gate="time_gate", reason="utc_hour=19",
    ))
    assert row_id > 0
    with sqlite3.connect(db) as db_conn:
        row = db_conn.execute(
            "SELECT strategy, mint_address, ticker, entered FROM candidate_log WHERE id = ?",
            (row_id,),
        ).fetchone()
        payload = db_conn.execute(
            "SELECT gates_failed FROM candidate_log WHERE id = ?",
            (row_id,),
        ).fetchone()[0]
    assert row == ("A", "SkipMint", "TST", 0)
    assert json.loads(payload) == {"gate": "time_gate", "reason": "utc_hour=19"}

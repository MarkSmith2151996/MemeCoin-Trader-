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
    trigger = 1.0 - strategy_b.HARD_STOP_PCT / 100
    assert manager.closed_with == [("Mint1", trigger, 1.0)]
    assert sell_trades(db) == [(trigger, "hard_stop")]
    assert danger is False


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


def test_strategy_b_trailing_stop_uses_tuned_threshold(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=0.5)])
    strategy_b.peak_prices.clear()
    strategy_b.peak_prices["Mint1"] = 1.05
    danger = asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.99), db))
    assert manager.closed_with == [("Mint1", 0.99, 1.05)]
    assert sell_trades(db) == [(0.99, "trailing_stop")]
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


def test_strategy_b_early_exit_when_never_green_within_90s(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0, opened_minutes_ago=2.0)])
    strategy_b.peak_prices.clear()
    danger = asyncio.run(strategy_b.monitor_positions(manager, FakePrice(0.99), db))
    assert manager.closed_with == [("Mint1", 0.99, 1.0)]
    assert sell_trades(db) == [(0.99, "early_exit_no_green")]
    assert danger is False


def test_strategy_b_take_profit_uses_tuned_threshold(db: Path) -> None:
    manager = FakeManager([make_position(entry=1.0)])
    strategy_b.peak_prices.clear()
    trigger = 1.0 + strategy_b.TAKE_PROFIT_PCT / 100
    danger = asyncio.run(strategy_b.monitor_positions(manager, FakePrice(trigger + 0.01), db))
    assert manager.closed_with == [("Mint1", trigger, trigger + 0.01)]
    assert sell_trades(db) == [(trigger, "take_profit")]
    assert danger is False


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


# ── 5. No re-entry on losing mints ───────────────────────────────────

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


def test_strategy_a_repeat_loser_fresh_loss_blocked(db: Path) -> None:
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
    expected_size = paper_loop.PAPER_SIZE_SOL
    if datetime.now(UTC).weekday() == 5:
        expected_size *= paper_loop.SATURDAY_SIZE_MULTIPLIER
    assert adapter.sizes == [expected_size]


def test_strategy_b_repeat_loser_blocked(db: Path) -> None:
    seed_closed_position(db, "Loser", pnl=-0.002)
    manager = FakeManager()
    result = asyncio.run(
        strategy_b.try_enter("Loser", "TST", FakePrice(1.0), FakeAdapter(), manager, db),
    )
    assert result is None
    assert candidate_log_rows(db, "B", "repeat_loser")


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
    assert adapter.sizes == [paper_loop.PAPER_SIZE_SOL * paper_loop.SATURDAY_SIZE_MULTIPLIER]


def test_strategy_b_saturday_halves_position_size(
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
    assert adapter.sizes == [strategy_b.PAPER_SIZE_SOL * strategy_b.SATURDAY_SIZE_MULTIPLIER]
    # MT-588: thick pool (>50 SOL) -> 1% (100 bps) tiered slippage.
    assert adapter.slippages == [strategy_b.SLIPPAGE_BPS_THICK_POOL]


def test_strategy_b_skips_entry_when_pool_too_thin(db: Path) -> None:
    adapter = FakeAdapter()
    manager = FakeManager()
    result = asyncio.run(
        strategy_b.try_enter(
            "ThinMint", "TST", FakePrice(1.0), adapter, manager, db, pool_sol=15.0,
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

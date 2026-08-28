"""Focused semantics tests for the detached MT-678 replay."""

from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import capacity_sweep_bt_v2 as bt  # noqa: E402


def config() -> bt.LiveConfig:
    return bt.LiveConfig(
        gates={},
        exits={
            "hard_stop_pct": 8,
            "take_profit_pct": 150,
            "trailing_arm_pct": 2,
            "trailing_stop_pct": 2,
            "time_stop_minutes": 10,
        },
        position_size_sol=0.02,
        max_open=5,
        captured_at="2026-08-28T00:00:00+00:00",
    )


def test_age_tiers_match_v2_collector_boundaries() -> None:
    ages = (0, 59, 60, 179, 180, 299, 300, 599, 600)
    assert [bt.age_tier_min_transactions(age) for age in ages] == [
        3,
        3,
        5,
        5,
        8,
        8,
        12,
        12,
        16,
    ]


def test_hard_stop_fills_on_next_bar_not_stop_level() -> None:
    series = {
        "time": np.array([0, 45_000, 50_000, 55_000], dtype=np.int64),
        "open": np.array([1.0, 2.0, 0.91, 0.80]),
        "close": np.array([1.0, 1.0, 0.90, 0.79]),
        "pool": np.array([10.0, 10.0, 10.0, 10.0]),
    }
    trade = bt.build_trade(bt.Candidate("mint", 0, 1, 90), series, config())

    assert trade is not None
    assert trade.entry_time == 45_000
    assert trade.exit_reason == "hard_stop"
    assert trade.exit_time == 55_000
    assert trade.exit_price == pytest.approx(0.79)
    assert trade.raw_pnl_sol == pytest.approx(-0.0042)
    assert trade.net_pnl_sol < trade.raw_pnl_sol


def test_hard_stop_ban_expires_after_twenty_four_hours() -> None:
    state = bt.ReplayState("test", max_open=5)
    state.hard_stop_ban_until["mint"] = 86_400_000

    assert not bt.eligible(bt.Candidate("mint", 86_399_999, 1, 1), state)
    assert bt.eligible(bt.Candidate("mint", 86_400_000, 1, 1), state)


def test_pool_reserve_bounds_mark_and_costed_liquidation_proceeds() -> None:
    trade = bt.ReplayTrade(
        mint="bad-mark",
        entry_time=0,
        entry_price=1e-7,
        exit_time=5_000,
        exit_price=1_000.0,
        exit_reason="take_profit",
        entry_pool_sol=10.0,
        exit_pool_sol=0.01,
        position_size_sol=0.02,
    )

    assert trade.raw_pnl_sol == pytest.approx(-0.01)
    assert trade.net_pnl_sol == pytest.approx(-0.0208)


def test_visibility_sampler_returns_distinct_weighted_mints() -> None:
    sample = bt.VisibilityModel._sample(
        random.Random(7),
        [(f"mint-{index}", float(index + 1)) for index in range(40)],
    )

    assert len(sample) == bt.POLL_SIZE
    assert len(set(sample)) == bt.POLL_SIZE


def test_report_renders_percentages_and_exit_breakdowns() -> None:
    trade = bt.ReplayTrade(
        mint="mint",
        entry_time=0,
        entry_price=1.0,
        exit_time=5_000,
        exit_price=3.0,
        exit_reason="take_profit",
        entry_pool_sol=10.0,
        exit_pool_sol=10.0,
        position_size_sol=0.02,
    )
    states = [
        bt.ReplayState("perfect_visibility", 5, trades=[trade]),
        bt.ReplayState("realistic_visibility", 5, trades=[trade]),
    ]
    visibility = bt.VisibilityModel()
    visibility.daily_stats["2026-04-18"] = {
        "polls": 1,
        "universe_mints": 1,
        "born_mints": 1,
        "born_discovered": 1,
        "newly_discovered": 1,
        "median_lag_s": 5.0,
        "p90_lag_s": 5.0,
    }

    report = bt.build_report(
        dates=["2026-04-18"],
        config=config(),
        summaries=[bt.summary(state, ["2026-04-18"]) for state in states],
        states=states,
        visibility=visibility,
    )

    assert "| raw win rate | 100.00% | 100.00% |" in report
    assert "| take_profit | 1 |" in report

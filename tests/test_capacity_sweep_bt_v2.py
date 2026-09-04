"""Focused semantics tests for the detached MT-678 replay."""

from __future__ import annotations

import asyncio
import random
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import capacity_sweep_bt_v2 as bt  # noqa: E402


def config() -> bt.LiveConfig:
    return bt.LiveConfig(
        gates={
            "mcap_floor": 5_100,
            "min_pool_sol_bonding": 5,
            "min_pool_sol_graduated": 5,
        },
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


def test_hard_stop_waits_for_the_configured_delay() -> None:
    series = {
        "time": np.array(
            [0, 45_000, 50_000, 55_000, 60_000, 65_000, 70_000, 75_000, 80_000],
            dtype=np.int64,
        ),
        "open": np.array([1.0, 1.0, 0.91, 0.90, 0.90, 0.90, 0.90, 0.90, 0.85]),
        "close": np.array([1.0, 1.0, 0.90, 0.89, 0.89, 0.89, 0.89, 0.89, 0.84]),
        "pool": np.array([10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]),
    }
    delayed_config = replace(config(), hard_stop_delay_seconds=30)

    trade = bt.build_trade(bt.Candidate("mint", 0, 1, 90), series, delayed_config)

    assert trade is not None
    assert trade.exit_reason == "hard_stop"
    assert trade.exit_time == 80_000


def test_series_end_without_exit_closes_as_data_end() -> None:
    series = {
        "time": np.array([0, 45_000, 50_000, 55_000], dtype=np.int64),
        "open": np.array([1.0, 1.0, 1.0, 1.0]),
        "close": np.array([1.0, 1.0, 1.0, 1.0]),
        "pool": np.array([10.0, 10.0, 10.0, 10.0]),
    }
    trade = bt.build_trade(bt.Candidate("mint", 0, 1, 90), series, config())

    assert trade is not None
    assert trade.exit_reason == "data_end"
    assert trade.exit_time == 55_000
    assert trade.exit_price == 1.0
    assert trade.trigger_price is None


def test_trigger_on_last_bar_closes_as_data_end() -> None:
    series = {
        "time": np.array([0, 45_000, 50_000], dtype=np.int64),
        "open": np.array([1.0, 1.0, 0.80]),
        "close": np.array([1.0, 1.0, 0.80]),
        "pool": np.array([10.0, 10.0, 10.0]),
    }
    trade = bt.build_trade(bt.Candidate("mint", 0, 1, 90), series, config())

    assert trade is not None
    assert trade.exit_reason == "data_end"
    assert trade.exit_time == 50_000
    assert trade.exit_price == 0.80


def test_bar_coverage_gap_closes_as_data_end() -> None:
    series = {
        "time": np.array([0, 45_000, 50_000, 70_000], dtype=np.int64),
        "open": np.array([1.0, 1.0, 1.0, 1.0]),
        "close": np.array([1.0, 1.0, 1.0, 1.0]),
        "pool": np.array([10.0, 10.0, 10.0, 10.0]),
    }
    trade = bt.build_trade(bt.Candidate("mint", 0, 1, 90), series, config())

    assert trade is not None
    assert trade.exit_reason == "data_end"
    assert trade.exit_time == 50_000


def test_exit_fill_gap_closes_as_data_end() -> None:
    series = {
        "time": np.array([0, 45_000, 50_000, 70_000], dtype=np.int64),
        "open": np.array([1.0, 1.0, 0.80, 0.80]),
        "close": np.array([1.0, 1.0, 0.80, 0.80]),
        "pool": np.array([10.0, 10.0, 10.0, 10.0]),
    }
    trade = bt.build_trade(bt.Candidate("mint", 0, 1, 90), series, config())

    assert trade is not None
    assert trade.exit_reason == "data_end"
    assert trade.exit_time == 50_000


def test_stale_entry_rejected_when_bar_overshoots() -> None:
    series = {
        "time": np.array([0, 100_000, 105_000, 110_000], dtype=np.int64),
        "open": np.array([1.0, 1.0, 1.0, 1.0]),
        "close": np.array([1.0, 1.0, 1.0, 1.0]),
        "pool": np.array([10.0, 10.0, 10.0, 10.0]),
    }
    trade = bt.build_trade(bt.Candidate("mint", 0, 1, 90), series, config())

    assert trade is None


def test_repeat_loser_ban_applies_to_any_net_losing_exit() -> None:
    losing = bt.ReplayTrade(
        mint="loser",
        entry_time=0,
        entry_price=1.0,
        exit_time=5_000,
        exit_price=0.5,
        exit_reason="trailing_stop",
        entry_pool_sol=100.0,
        exit_pool_sol=100.0,
        position_size_sol=0.02,
    )
    winning = bt.ReplayTrade(
        mint="winner",
        entry_time=0,
        entry_price=1.0,
        exit_time=5_000,
        exit_price=2.0,
        exit_reason="take_profit",
        entry_pool_sol=100.0,
        exit_pool_sol=100.0,
        position_size_sol=0.02,
    )
    state = bt.ReplayState("test", max_open=5)
    state.positions.append(bt.ScheduledPosition("loser", losing))
    state.positions.append(bt.ScheduledPosition("winner", winning))

    bt.settle(state, 10_000)

    assert losing.net_pnl_sol < 0
    assert "loser" in state.repeat_loser_ban_until
    assert "winner" not in state.repeat_loser_ban_until


def test_exit_price_cap_has_symmetric_downside_floor() -> None:
    trade = bt.ReplayTrade(
        mint="inverse-artifact",
        entry_time=0,
        entry_price=1.0,
        exit_time=5_000,
        exit_price=0.0001,
        exit_reason="hard_stop",
        entry_pool_sol=100.0,
        exit_pool_sol=100.0,
        position_size_sol=0.02,
        trigger_price=1.0,
    )

    assert trade.exit_price_for_cap(100.0) == pytest.approx(0.01)
    assert trade.exit_price_for_cap(None) == pytest.approx(0.0001)


def test_trade_retains_candidate_entry_characteristics() -> None:
    series = {
        "time": np.array([0, 45_000, 50_000, 55_000], dtype=np.int64),
        "open": np.array([1.0, 2.0, 0.91, 0.80]),
        "close": np.array([1.0, 1.0, 0.90, 0.79]),
        "pool": np.array([10.0, 10.0, 10.0, 10.0]),
        "pool_label": np.array(["pump"] * 4),
        "graduated": np.array([False] * 4),
    }
    candidate = bt.Candidate(
        mint="mint",
        scan_time=0,
        ordinal=1,
        strength_score=90.0,
        buy_sell_ratio_at_entry=2.5,
        age_seconds_at_entry=120.0,
        volume_usd_at_entry=6_000.0,
        txn_count_at_entry=12,
        volume_to_mcap_ratio_at_entry=0.12,
    )

    trade = bt.build_trade(candidate, series, config())

    assert trade is not None
    assert trade.score_at_entry == 90.0
    assert trade.buy_sell_ratio_at_entry == 2.5
    assert trade.age_seconds_at_entry == 120.0
    assert trade.volume_usd_at_entry == 6_000.0
    assert trade.txn_count_at_entry == 12
    assert trade.volume_to_mcap_ratio_at_entry == 0.12
    assert trade.entry_pool_type == "bonding"


def test_gate_candidate_captures_entry_characteristics() -> None:
    candidate_config = replace(
        config(),
        gates={
            **config().gates,
            "mcap_ceiling": 50_000,
            "min_age_seconds": 0,
            "max_age_seconds": 1_320,
            "age_offset_seconds": 39,
            "txn_count_adjustment": 1,
            "min_volume_usd": 100,
            "min_volume_to_mcap_ratio": 0.005,
            "max_volume_to_mcap_ratio": 50,
            "min_buy_sell_ratio": 0.5,
            "creator_holdings_max": 0,
            "score_threshold_bonding": 0,
            "score_threshold_graduated": 0,
            "blocked_weekdays": [],
            "blocked_hours_utc": [],
        },
    )
    candidate = bt.candidate_from_row(
        (
            "mint",
            1_000,
            1,
            3.0,
            1.0,
            10,
            100.0,
            10_000.0,
            10.0,
            "pump",
            False,
            0.0,
            1.0,
        ),
        sol_usd=100.0,
        carry={},
        config=candidate_config,
    )

    assert candidate is not None
    assert candidate.buy_sell_ratio_at_entry == 3.0
    assert candidate.age_seconds_at_entry == 139.0
    assert candidate.volume_usd_at_entry == 400.0
    assert candidate.txn_count_at_entry == 10
    assert candidate.volume_to_mcap_ratio_at_entry == 0.04
    assert (
        bt.candidate_from_row(
            (
                "mint",
                1_000,
                1,
                3.0,
                1.0,
                10,
                100.0,
                10_000.0,
                10.0,
                "pump",
                False,
                0.0,
                1.0,
            ),
            sol_usd=100.0,
            carry={},
            config=candidate_config,
            graduated_only=True,
        )
        is None
    )


def test_trade_csv_appends_entry_characteristics() -> None:
    assert bt.TRADE_CSV_FIELDS[-7:] == (
        "score_at_entry",
        "buy_sell_ratio_at_entry",
        "age_seconds_at_entry",
        "volume_usd_at_entry",
        "txn_count_at_entry",
        "pool_type_at_entry",
        "volume_to_mcap_ratio_at_entry",
    )


def test_cli_overrides_update_effective_config_and_header() -> None:
    args = bt.parse_args(
        [
            "--mcap-floor",
            "10000",
            "--min-pool-sol",
            "50",
            "--hard-stop-pct",
            "12",
            "--time-stop-minutes",
            "20",
            "--hard-stop-delay-seconds",
            "30",
        ],
    )
    effective = bt.apply_cli_overrides(config(), args)
    header = bt.replay_header(["2026-04-18"], effective)

    assert effective.gates["mcap_floor"] == 10_000
    assert effective.gates["min_pool_sol_bonding"] == 50
    assert effective.gates["min_pool_sol_graduated"] == 50
    assert effective.exits["hard_stop_pct"] == 12
    assert effective.exits["time_stop_minutes"] == 20
    assert effective.hard_stop_delay_seconds == 30
    assert "mcap_floor=10000" in header
    assert "min_pool_sol_bonding=50" in header
    assert "min_pool_sol_graduated=50" in header
    assert "hard_stop_pct=12" in header
    assert "hard_stop_delay_seconds=30" in header
    assert "--mcap-floor=10000" in header
    assert "--min-pool-sol=50" in header
    assert "--hard-stop-pct=12" in header
    assert "--time-stop-minutes=20" in header
    assert "--hard-stop-delay-seconds=30" in header


def test_complete_cli_config_skips_hive_read(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("MEMECOIN_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(bt, "load_dotenv", lambda *_args, **_kwargs: None)
    args = bt.parse_args(
        [
            "--mcap-floor", "5100", "--mcap-ceiling", "50000", "--min-age-seconds", "0",
            "--max-age-seconds", "1320", "--age-offset-seconds", "39",
            "--txn-count-adjustment", "1",
            "--min-volume-usd", "100", "--min-volume-to-mcap-ratio", "0.005",
            "--max-volume-to-mcap-ratio", "50", "--min-buy-sell-ratio", "0.5",
            "--min-pool-sol", "5",
            "--creator-holdings-max", "0",
            "--score-threshold-bonding", "40", "--score-threshold-graduated", "40",
            "--blocked-weekdays", "2", "--blocked-hours-utc", "0", "7", "--max-open", "5",
            "--position-size-sol", "0.02", "--trailing-stop-pct", "2", "--trailing-arm-pct", "2",
            "--hard-stop-pct", "8", "--take-profit-pct", "150", "--time-stop-minutes", "10",
        ],
    )

    effective = asyncio.run(bt.load_effective_config(args))

    assert effective.position_size_sol == 0.02
    assert effective.max_open == 5
    assert effective.gates["blocked_hours_utc"] == [0, 7]
    assert "All config provided via CLI - skipping Hive read" in capsys.readouterr().out


def test_partial_cli_config_without_hive_dsn_lists_missing_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MEMECOIN_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(bt, "load_dotenv", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match=r"missing CLI args: .*--mcap-ceiling"):
        asyncio.run(bt.load_effective_config(bt.parse_args(["--mcap-floor", "5100"])))


def test_partial_cli_config_uses_hive_and_overrides_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_load_live_config(
        dsn: str | None = None,
        *,
        position_size_sol: float | None = None,
        max_open_override: int | None = None,
    ) -> bt.LiveConfig:
        assert dsn == "postgresql://example"
        assert position_size_sol == 0.03
        assert max_open_override is None
        return config()

    monkeypatch.setenv("MEMECOIN_POSTGRES_DSN", "postgresql://example")
    monkeypatch.setattr(bt, "load_live_config", fake_load_live_config)

    effective = asyncio.run(
        bt.load_effective_config(
            bt.parse_args(
                [
                    "--mcap-floor",
                    "10000",
                    "--position-size-sol",
                    "0.03",
                    "--trailing-stop-pct",
                    "5",
                ],
            ),
        ),
    )

    assert effective.gates["mcap_floor"] == 10_000
    assert effective.position_size_sol == 0.03
    assert effective.exits["trailing_stop_pct"] == 5


def test_graduated_only_cli_flag_parses_and_is_logged() -> None:
    args = bt.parse_args(["--graduated-only"])
    header = bt.replay_header(["2026-04-18"], config(), graduated_only=args.graduated_only)

    assert args.graduated_only is True
    assert "graduated_only=True" in header
    assert "--graduated-only" in header


def test_cli_defaults_preserve_hive_values() -> None:
    original = config()
    effective = bt.apply_cli_overrides(original, bt.parse_args([]))

    assert effective.gates == original.gates
    assert effective.exits == original.exits
    assert effective.hard_stop_delay_seconds == 0
    assert effective.overrides == {}
    assert bt.parse_args([]).time_stop_minutes is None
    assert bt.parse_args([]).graduated_only is False


def test_root_derives_output_and_progress_paths() -> None:
    root, output_dir, progress_log, repo_report = bt.resolve_replay_paths(
        bt.parse_args(["--root", "/tmp/test"]),
    )

    assert root == Path("/tmp/test")
    assert output_dir == Path("/tmp/test/results/capacity_sweep_bt_v2_feefix")
    assert progress_log == Path("/tmp/test/results/capacity_sweep_bt_v2_feefix.progress.log")
    assert repo_report == bt.DEFAULT_REPO_REPORT


def test_path_defaults_preserve_windows_archive_locations() -> None:
    root, output_dir, progress_log, repo_report = bt.resolve_replay_paths(bt.parse_args([]))

    assert root == bt.DEFAULT_ROOT
    assert output_dir == bt.DEFAULT_OUT_DIR
    assert progress_log == bt.DEFAULT_PROGRESS_LOG
    assert repo_report == bt.DEFAULT_REPO_REPORT


def test_explicit_output_and_progress_paths_override_root() -> None:
    root, output_dir, progress_log, repo_report = bt.resolve_replay_paths(
        bt.parse_args(
            [
                "--root",
                "/tmp/test",
                "--output-dir",
                "/tmp/output",
                "--progress-log",
                "/tmp/progress.log",
                "--repo-report",
                "/tmp/report.md",
            ],
        ),
    )

    assert root == Path("/tmp/test")
    assert output_dir == Path("/tmp/output")
    assert progress_log == Path("/tmp/progress.log")
    assert repo_report == Path("/tmp/report.md")


def test_missing_requested_day_skips_without_loading_live_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "derived" / "enriched").mkdir(parents=True)
    (tmp_path / "derived" / "enriched" / "2026-07-29.parquet").touch()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "capacity_sweep_bt_v2.py",
            "--root",
            str(tmp_path),
            "--start",
            "2026-07-30",
            "--end",
            "2026-07-30",
        ],
    )

    bt.main()

    output = capsys.readouterr().out
    assert (
        "Skipping replay: no enriched Parquet files in requested range "
        "2026-07-30 through 2026-07-30"
    ) in output


def test_repeat_loser_ban_expires_after_twenty_four_hours() -> None:
    state = bt.ReplayState("test", max_open=5)
    state.repeat_loser_ban_until["mint"] = 86_400_000

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
    assert trade.net_pnl_sol == pytest.approx(-0.0204)


def test_pool_aware_fees_and_trigger_relative_exit_cap() -> None:
    trade = bt.ReplayTrade(
        mint="migration",
        entry_time=0,
        entry_price=1.0,
        exit_time=5_000,
        exit_price=10.0,
        exit_reason="take_profit",
        entry_pool_sol=100.0,
        exit_pool_sol=100.0,
        position_size_sol=0.02,
        trigger_price=2.5,
        entry_pool_type="bonding",
        exit_pool_type="graduated",
    )
    all_graduated = replace(trade, entry_pool_type="graduated")

    assert trade.exit_price_for_cap(1.5) == pytest.approx(3.75)
    assert trade.net_pnl_sol < all_graduated.net_pnl_sol


def test_visibility_sampler_returns_distinct_weighted_mints() -> None:
    sample = bt.VisibilityModel._sample(
        random.Random(7),
        [(f"mint-{index}", float(index + 1)) for index in range(40)],
    )

    assert len(sample) == bt.POLL_SIZE
    assert len(set(sample)) == bt.POLL_SIZE


def test_backtest_priority_fee_proxy_uses_conservative_floor() -> None:
    assert bt.PRIORITY_FEE_PER_LEG == pytest.approx(0.0002)


def test_duckdb_connection_has_a_bounded_memory_limit(tmp_path: Path) -> None:
    connection = bt.open_duckdb(tmp_path)
    try:
        memory_limit = connection.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        assert memory_limit == "1.8 GiB"  # DuckDB renders the configured decimal 2GB in GiB.
    finally:
        connection.close()


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
    ratios = bt.PriceRatioCaps(p99=1.5, p999=2.0, observations=10)
    caps = bt.exit_caps(ratios)
    summaries_by_cap = {
        cap.name: [
            bt.summary(state, ["2026-04-18"], price_ratio_bound=cap.price_ratio_bound)
            for state in states
        ]
        for cap in caps
    }

    report = bt.build_report(
        dates=["2026-04-18"],
        config=config(),
        price_ratios=ratios,
        caps=caps,
        summaries_by_cap=summaries_by_cap,
        floor_summaries=[
            bt.summary(
                state,
                ["2026-04-18"],
                price_ratio_bound=ratios.p999,
                exclude_take_profit=True,
            )
            for state in states
        ],
        fee_sensitivity_rows={
            state.scenario: bt.fee_sensitivity(state, ["2026-04-18"], price_ratio_bound=ratios.p999)
            for state in states
        },
        states=states,
        visibility=visibility,
    )

    assert "| raw win rate | 100.00% | 100.00% |" in report
    assert "| take_profit | 1 |" in report
    assert "p99.9 cap (2.000000x)" in report


def test_floor_impact_counts_floored_trades_and_pnl_delta() -> None:
    floored = bt.ReplayTrade(
        mint="inverse-artifact",
        entry_time=0,
        entry_price=1.0,
        exit_time=5_000,
        exit_price=0.0001,
        exit_reason="hard_stop",
        entry_pool_sol=100.0,
        exit_pool_sol=100.0,
        position_size_sol=0.02,
        trigger_price=1.0,
    )
    clean = bt.ReplayTrade(
        mint="clean",
        entry_time=0,
        entry_price=1.0,
        exit_time=5_000,
        exit_price=1.5,
        exit_reason="take_profit",
        entry_pool_sol=100.0,
        exit_pool_sol=100.0,
        position_size_sol=0.02,
        trigger_price=1.2,
    )

    count, delta = bt.floor_impact([floored, clean], 100.0)

    assert count == 1
    assert delta > 0
    assert bt.floor_impact([floored, clean], None) == (0, 0.0)


def test_price_ratio_measure_subrange_restricts_measurement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    enriched = tmp_path / "derived" / "enriched"
    enriched.mkdir(parents=True)
    for date in ("2026-04-18", "2026-04-19", "2026-04-20"):
        (enriched / f"{date}.parquet").touch()
    captured: dict[str, list[str]] = {}

    def fake_measure(root: Path, measure_dates: list[str]) -> bt.PriceRatioCaps:
        captured["dates"] = measure_dates
        return bt.PriceRatioCaps(1.5, 2.0, 10)

    monkeypatch.setattr(bt, "measure_price_ratio_caps", fake_measure)
    replay_dates = ["2026-04-18", "2026-04-19", "2026-04-20"]

    args = bt.parse_args(
        ["--price-ratio-measure-start", "2026-04-18", "--price-ratio-measure-end", "2026-04-19"],
    )
    caps, reused = bt.price_ratio_caps_from_args(args, tmp_path, replay_dates)

    assert reused is False
    assert captured["dates"] == ["2026-04-18", "2026-04-19"]

    captured.clear()
    caps, reused = bt.price_ratio_caps_from_args(bt.parse_args([]), tmp_path, replay_dates)

    assert reused is False
    assert captured["dates"] == replay_dates

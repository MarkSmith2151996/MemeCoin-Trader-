"""Read-only V2 terminal dashboard presentation coverage."""

from __future__ import annotations

from datetime import UTC, datetime

from rich.console import Console

from scripts.v2_dashboard import (
    ACTIVITY_SQL,
    GATE_SQL,
    OPEN_POSITIONS_SQL,
    RECENT_TRADES_SQL,
    RUNTIME_EVENT_SQL,
    SUMMARY_SQL,
    HiveDashboardSnapshot,
    build_dashboard,
    executor_heartbeat,
)


def test_dashboard_renders_hive_summary_and_activity() -> None:
    now = datetime(2026, 8, 29, 12, tzinfo=UTC)
    snapshot = HiveDashboardSnapshot(
        captured_at=now,
        summary={
            "open_positions": 1,
            "open_exposure_sol": 0.02,
            "today_closed_count": 2,
            "today_pnl_sol": 0.003,
            "all_time_pnl_sol": -0.01,
        },
        gates=[{"gate_name": "mcap_floor", "gate_value": 5100, "updated_at": now}],
        positions=[
            {
                "mint_address": "open-position-mint",
                "mode": "paper",
                "strategy": "BT",
                "amount_sol": 0.02,
                "entry_price_sol": 0.000001,
                "opened_at": now,
            }
        ],
        trades=[
            {
                "side": "BUY",
                "mint_address": "recent-entry-mint",
                "amount_sol": 0.02,
                "executed_at": now,
                "close_reason": None,
                "realized_pnl_sol": None,
            }
        ],
        activity={"candidate_observations_5m": 12, "candidate_mints_5m": 4},
        heartbeat="healthy (1.0s ago)",
    )
    console = Console(record=True, width=160)
    console.print(build_dashboard(snapshot))
    output = console.export_text()

    assert "V2 HIVE DASHBOARD (READ-ONLY)" in output
    assert "Live Loop Status" in output
    assert "Gate Stats" in output
    assert "Recent Entries / Exits" in output
    assert "mcap_floor" in output


def test_dashboard_queries_are_select_only() -> None:
    for query in (
        SUMMARY_SQL,
        GATE_SQL,
        RECENT_TRADES_SQL,
        OPEN_POSITIONS_SQL,
        ACTIVITY_SQL,
        RUNTIME_EVENT_SQL,
    ):
        assert query.lstrip().upper().startswith("SELECT")


def test_executor_heartbeat_reports_fresh_and_invalid(tmp_path) -> None:
    now = datetime(2026, 8, 29, 12, tzinfo=UTC)
    heartbeat = tmp_path / "heartbeat"
    heartbeat.write_text('{"last_cycle":"2026-08-29T11:59:50+00:00"}')

    assert executor_heartbeat(heartbeat, now=now) == "healthy (10.0s ago)"
    heartbeat.write_text("not-json")
    assert executor_heartbeat(heartbeat, now=now) == "missing"

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType
from src.signals.base import SignalSource as SignalSourceBase


runner = CliRunner()


class FakeSignalSource(SignalSourceBase):
    def __init__(self, signals: list[Signal]) -> None:
        self._signals = signals
        self.started = False
        self.stopped = False

    @property
    def name(self) -> str:
        return "fake_launch"

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def poll(self) -> list[Signal]:
        return self._signals


def _launch_signal(mint_address: str) -> Signal:
    return Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        observed_at=datetime.now(UTC),
    )


def _assessment(**updates: CheckResult) -> RiskAssessment:
    checks = {
        "liquidity_check": CheckResult.PASS,
        "top10_holder_check": CheckResult.PASS,
        "creator_holding_check": CheckResult.UNKNOWN,
        "age_check": CheckResult.FAIL,
        "unique_buyers_check": CheckResult.UNKNOWN,
        "mint_authority_check": CheckResult.PASS,
        "freeze_authority_check": CheckResult.PASS,
        "honeypot_check": CheckResult.UNKNOWN,
    }
    checks.update(updates)
    return RiskAssessment(**checks)


def test_dry_run_collects_unique_candidates_and_reports_eligibility() -> None:
    signal = _launch_signal("PaperMinimumCliMint111111111111111111111111")
    source = FakeSignalSource([signal, signal.model_copy(deep=True)])

    async def assessor(_signal: Signal) -> RiskAssessment:
        return _assessment()

    summary = asyncio.run(
        cli_module.run_paper_minimum_dry_run(
            max_candidates=5,
            sources=[source],
            assessor=assessor,
        )
    )

    assert source.started is True
    assert source.stopped is True
    assert summary.signals_collected == 2
    assert summary.unique_mints == 1
    assert summary.strict_passes == 0
    assert summary.paper_minimum_eligible == 1
    assert summary.deferred_labels["paper_minimum_deferred_age_launch_research"] == 1


def test_dry_run_blocks_unknown_required_evidence() -> None:
    source = FakeSignalSource([_launch_signal("PaperMinimumCliUnknown111111111111111111111")])

    async def assessor(_signal: Signal) -> RiskAssessment:
        return _assessment(mint_authority_check=CheckResult.UNKNOWN)

    summary = asyncio.run(
        cli_module.run_paper_minimum_dry_run(sources=[source], assessor=assessor)
    )

    assert summary.paper_minimum_eligible == 0
    assert summary.blocked_labels["paper_minimum_blocked_authority"] == 1


def test_dry_run_does_not_persist_observations_by_default(tmp_path: Path) -> None:
    db = tmp_path / "default-no-persistence.db"
    asyncio.run(cli_module.init_db(db))
    source = FakeSignalSource([_launch_signal("PaperMinimumDefault111111111111111111111")])

    async def assessor(_signal: Signal) -> RiskAssessment:
        return _assessment()

    summary = asyncio.run(
        cli_module.run_paper_minimum_dry_run(sources=[source], assessor=assessor, db_path=db)
    )

    with sqlite3.connect(db) as connection:
        count = connection.execute("SELECT COUNT(*) FROM live_candidate_observations").fetchone()[0]
    assert summary.observations_recorded == 0
    assert count == 0


def test_dry_run_persists_sanitized_observation_when_explicit(tmp_path: Path) -> None:
    db = tmp_path / "explicit-observation.db"
    source = FakeSignalSource([_launch_signal("PaperMinimumRecorded111111111111111111111")])

    async def assessor(_signal: Signal) -> RiskAssessment:
        return _assessment()

    summary = asyncio.run(
        cli_module.run_paper_minimum_dry_run(
            sources=[source],
            assessor=assessor,
            record_observations=True,
            db_path=db,
        )
    )

    with sqlite3.connect(db) as connection:
        row = connection.execute(
            "SELECT strict_labels_json, paper_minimum_labels_json, source_names_json FROM live_candidate_observations"
        ).fetchone()
        trades = connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    assert summary.observations_recorded == summary.new_mints == 1
    assert summary.repeat_mints == 0
    assert summary.observation_labels == (("PaperMinimumRecorded111111111111111111111", "new_mint"),)
    assert "liquidity_check_pass" in json.loads(row[0])
    assert set(json.loads(row[1])) == {
        "paper_minimum_pass",
        "paper_minimum_deferred_honeypot_unknown",
        "paper_minimum_deferred_creator_unknown",
        "paper_minimum_deferred_unique_buyers_unknown",
        "paper_minimum_deferred_age_launch_research",
    }
    assert json.loads(row[2]) == ["pump_fun"]
    assert trades == 0


def test_explicit_second_scan_reports_repeat_mint(tmp_path: Path) -> None:
    db = tmp_path / "repeat-observation.db"

    async def assessor(_signal: Signal) -> RiskAssessment:
        return _assessment()

    first = asyncio.run(
        cli_module.run_paper_minimum_dry_run(
            sources=[FakeSignalSource([_launch_signal("PaperMinimumRepeat111111111111111111111")])],
            assessor=assessor,
            record_observations=True,
            db_path=db,
        )
    )
    second = asyncio.run(
        cli_module.run_paper_minimum_dry_run(
            sources=[FakeSignalSource([_launch_signal("PaperMinimumRepeat111111111111111111111")])],
            assessor=assessor,
            record_observations=True,
            db_path=db,
        )
    )

    assert first.new_mints == 1
    assert second.new_mints == 0
    assert second.repeat_mints == 1
    assert second.observation_labels == (("PaperMinimumRepeat111111111111111111111", "repeat_prior_run"),)


def test_cli_command_writes_explicit_report_path(tmp_path: Path, monkeypatch) -> None:
    summary = cli_module.PaperMinimumDryRunSummary(
        signals_collected=1,
        unique_mints=1,
        strict_passes=0,
        paper_minimum_eligible=1,
        source_signal_counts={"fake_launch": 1},
        source_failures={},
        blocked_labels={},
        deferred_labels={"paper_minimum_deferred_age_launch_research": 1},
    )

    async def fake_run(**_kwargs: object) -> cli_module.PaperMinimumDryRunSummary:
        return summary

    monkeypatch.setattr(cli_module, "run_paper_minimum_dry_run", fake_run)
    report_path = tmp_path / "dry-run.md"

    result = runner.invoke(
        cli_module.app,
        ["paper-minimum-dry-run", "--max-candidates", "5", "--report-path", str(report_path)],
    )

    assert result.exit_code == 0
    assert "paper_minimum_eligible=1" in result.stdout
    assert "no paper trade" in report_path.read_text(encoding="utf-8").lower()

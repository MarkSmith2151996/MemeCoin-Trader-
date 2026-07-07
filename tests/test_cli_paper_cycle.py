import asyncio
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource as SignalSourceEnum, SignalType, TokenInfo
from src.monitoring.dashboard import load_open_positions, load_recent_trades
from src.signals.base import SignalSource


runner = CliRunner()


class FakeSignalSource(SignalSource):
    def __init__(self, batches: list[list[Signal]]) -> None:
        self._batches = list(batches)
        self.started = False
        self.stopped = False
        self.poll_calls = 0

    @property
    def name(self) -> str:
        return "fake"

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def poll(self) -> list[Signal]:
        self.poll_calls += 1
        if self._batches:
            return self._batches.pop(0)
        return []


def build_assessment(mint_address: str, *, passes: bool) -> RiskAssessment:
    check_value = CheckResult.PASS if passes else CheckResult.FAIL
    reasons = [] if passes else ["honeypot_check failed"]
    return RiskAssessment(
        token=TokenInfo(
            mint_address=mint_address,
            liquidity_sol=100.0,
            unique_buyers=250,
            top10_holder_pct=12.0,
            creator_holding_pct=2.0,
            mint_authority_revoked=True,
            freeze_authority_revoked=True,
        ),
        liquidity_check=CheckResult.PASS,
        top10_holder_check=CheckResult.PASS,
        creator_holding_check=CheckResult.PASS,
        age_check=CheckResult.PASS,
        unique_buyers_check=CheckResult.PASS,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=check_value,
        score=100.0 if passes else 70.0,
        reasons=reasons,
    )


def build_signal(mint_address: str, *, passes: bool, message: str | None = None) -> Signal:
    return Signal(
        source=SignalSourceEnum.MANUAL,
        type=SignalType.BUY,
        mint_address=mint_address,
        confidence=0.8,
        message=message,
        payload={"risk_assessment": build_assessment(mint_address, passes=passes)},
    )


def test_paper_cycle_persists_accepted_and_rejected_fake_signals(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "paper-cycle.db"
        source = FakeSignalSource(
            [
                [
                    build_signal("accepted-mint", passes=True),
                    build_signal("rejected-mint", passes=False),
                ]
            ]
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=2,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source],
            poll_interval_s=0.0,
        )

        assert summary.execution_mode == "paper"
        assert summary.signals_collected == 2
        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 1
        assert summary.trades_persisted == 1
        assert summary.open_positions == 1
        assert summary.termination_reason == "max_signals"
        assert source.started is True
        assert source.stopped is True

        trades = load_recent_trades(db_path, limit=5)
        positions = load_open_positions(db_path)

        assert len(trades) == 1
        assert trades[0].mint_address == "accepted-mint"
        assert trades[0].mode == "paper"
        assert len(positions) == 1
        assert positions[0].mint_address == "accepted-mint"

    asyncio.run(run())


def test_paper_cycle_forces_paper_execution_when_settings_request_live(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "live-request.db"
        live_settings = cli_module.load_settings().model_copy(
            update={"execution": cli_module.load_settings().execution.model_copy(update={"mode": "live"})}
        )
        source = FakeSignalSource([[build_signal("live-mode-mint", passes=True)]])

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            settings=live_settings,
            sources=[source],
            poll_interval_s=0.0,
        )

        trades = load_recent_trades(db_path, limit=5)

        assert summary.execution_mode == "paper"
        assert len(trades) == 1
        assert trades[0].mode == "paper"

    asyncio.run(run())


def test_paper_cycle_stops_at_max_signals_bound(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "bounded.db"
        source = FakeSignalSource(
            [[build_signal("mint-1", passes=True), build_signal("mint-2", passes=True), build_signal("mint-3", passes=True)]]
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source],
            poll_interval_s=0.0,
        )

        trades = load_recent_trades(db_path, limit=5)

        assert summary.signals_collected == 1
        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 0
        assert summary.trades_persisted == 1
        assert summary.termination_reason == "max_signals"
        assert len(trades) == 1
        assert trades[0].mint_address == "mint-1"

    asyncio.run(run())


def test_paper_cycle_times_out_cleanly_without_signals(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "timeout.db"
        source = FakeSignalSource([[]])

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=5,
            timeout_seconds=0.0,
            db_path=db_path,
            sources=[source],
            poll_interval_s=0.0,
        )

        assert summary.signals_collected == 0
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 0
        assert summary.trades_persisted == 0
        assert summary.open_positions == 0
        assert summary.termination_reason == "timeout"
        assert source.started is True
        assert source.stopped is True

    asyncio.run(run())


def test_paper_cycle_cli_prints_safe_summary(tmp_path: Path, monkeypatch) -> None:
    signal = build_signal(
        "cli-secret-mint",
        passes=True,
        message="super secret alpha message that should not be printed",
    )
    monkeypatch.setattr(cli_module, "build_signal_sources", lambda: [FakeSignalSource([[signal]])])

    result = runner.invoke(
        cli_module.app,
        [
            "paper-cycle",
            "--max-signals",
            "1",
            "--timeout-seconds",
            "0.1",
            "--db-path",
            str(tmp_path / "cli.db"),
        ],
    )

    assert result.exit_code == 0
    assert "execution_mode=paper" in result.stdout
    assert "signals_collected=1" in result.stdout
    assert "signals_accepted=1" in result.stdout
    assert "termination_reason=max_signals" in result.stdout
    assert "cli-secret-mint" not in result.stdout
    assert "super secret alpha message" not in result.stdout

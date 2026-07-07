import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import RiskConfig
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource as SignalSourceEnum, SignalType, TokenInfo
from src.monitoring.dashboard import load_open_positions, load_recent_trades
from src.risk.scorer import DiscoveryRiskScorer, HolderLookupResult
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


class ExplodingRiskScorer:
    async def assess_signal(self, signal: Signal) -> RiskAssessment:
        raise RuntimeError("boom")


class FakeHolderLookup:
    def __init__(self, result: HolderLookupResult | None = None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error

    async def fetch(self, mint_address: str) -> HolderLookupResult | None:
        if self._error is not None:
            raise self._error
        return self._result


def build_assessment(mint_address: str, *, passes: bool, failed_field: str = "honeypot_check") -> RiskAssessment:
    checks = {
        "liquidity_check": CheckResult.PASS,
        "top10_holder_check": CheckResult.PASS,
        "creator_holding_check": CheckResult.PASS,
        "age_check": CheckResult.PASS,
        "unique_buyers_check": CheckResult.PASS,
        "mint_authority_check": CheckResult.PASS,
        "freeze_authority_check": CheckResult.PASS,
        "honeypot_check": CheckResult.PASS,
    }
    if not passes:
        checks[failed_field] = CheckResult.FAIL
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
        liquidity_check=checks["liquidity_check"],
        top10_holder_check=checks["top10_holder_check"],
        creator_holding_check=checks["creator_holding_check"],
        age_check=checks["age_check"],
        unique_buyers_check=checks["unique_buyers_check"],
        mint_authority_check=checks["mint_authority_check"],
        freeze_authority_check=checks["freeze_authority_check"],
        honeypot_check=checks["honeypot_check"],
        score=100.0 if passes else 70.0,
        reasons=[] if passes else [f"{failed_field} failed"],
    )


def build_signal(
    mint_address: str,
    *,
    passes: bool,
    confidence: float = 0.8,
    failed_field: str = "honeypot_check",
    include_assessment: bool = True,
    message: str | None = None,
) -> Signal:
    payload = (
        {"risk_assessment": build_assessment(mint_address, passes=passes, failed_field=failed_field)}
        if include_assessment
        else {}
    )
    return Signal(
        source=SignalSourceEnum.MANUAL,
        type=SignalType.BUY,
        mint_address=mint_address,
        confidence=confidence,
        message=message,
        payload=payload,
    )


def build_enriched_pump_fun_signal(
    mint_address: str,
    *,
    created_at: datetime,
    liquidity_sol: float = 30.1,
    unique_buyers: int = 25,
    top10_holder_pct: float = 30.0,
    creator_holding_pct: float = 5.0,
) -> Signal:
    return Signal(
        source=SignalSourceEnum.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        confidence=0.85,
        payload={
            "symbol": "PUMP",
            "vSolInBondingCurve": liquidity_sol,
            "uniqueBuyers": unique_buyers,
            "top10HolderPct": top10_holder_pct,
            "creatorHoldingPct": creator_holding_pct,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": created_at.isoformat(),
        },
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
        assert summary.risk_profile == "strict"
        assert summary.signals_collected == 2
        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 1
        assert summary.trades_persisted == 1
        assert summary.open_positions == 1
        assert summary.rejection_reasons == {"honeypot_check_failed": 1}
        assert summary.holder_lookup_outcomes == {}
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


def test_paper_cycle_discovery_mode_stays_paper_when_settings_request_live(tmp_path: Path) -> None:
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
            risk_profile="discovery",
            settings=live_settings,
            sources=[source],
            poll_interval_s=0.0,
        )

        trades = load_recent_trades(db_path, limit=5)

        assert summary.execution_mode == "paper"
        assert summary.risk_profile == "discovery"
        assert summary.holder_lookup_outcomes == {}
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
        assert summary.risk_profile == "strict"
        assert summary.rejection_reasons == {}
        assert summary.holder_lookup_outcomes == {}
        assert summary.termination_reason == "max_signals"
        assert len(trades) == 1
        assert trades[0].mint_address == "mint-1"

    asyncio.run(run())


def test_paper_cycle_reports_stable_rejection_reason_counts(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "rejections.db"
        source = FakeSignalSource(
            [
                [
                    build_signal("risk-1", passes=False, failed_field="honeypot_check"),
                    build_signal("risk-2", passes=False, failed_field="honeypot_check"),
                    build_signal("zero-size", passes=True, confidence=0.0),
                ]
            ]
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=3,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source],
            poll_interval_s=0.0,
        )

        assert summary.signals_collected == 3
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 3
        assert summary.trades_persisted == 0
        assert summary.rejection_reasons == {
            "honeypot_check_failed": 2,
            "position_size_zero": 1,
        }
        assert summary.holder_lookup_outcomes == {}

    asyncio.run(run())


def test_paper_cycle_strict_mode_rejects_too_new_tokens_with_age_check_failed(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "strict-age.db"
        source = FakeSignalSource(
            [
                [
                    build_enriched_pump_fun_signal(
                        "strict-age-mint",
                        created_at=datetime.now(UTC),
                    )
                ]
            ]
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source],
            poll_interval_s=0.0,
        )

        assert summary.signals_collected == 1
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 1
        assert summary.risk_profile == "strict"
        assert summary.rejection_reasons == {"age_check_failed": 1}
        assert summary.holder_lookup_outcomes == {}

    asyncio.run(run())


def test_paper_cycle_discovery_mode_relaxes_only_age_blocker(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "discovery-age.db"
        source = FakeSignalSource(
            [
                [
                    build_enriched_pump_fun_signal(
                        "discovery-age-mint",
                        created_at=datetime.now(UTC),
                    )
                ]
            ]
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[source],
            poll_interval_s=0.0,
        )

        assert summary.execution_mode == "paper"
        assert summary.risk_profile == "discovery"
        assert summary.signals_collected == 1
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 1
        assert summary.rejection_reasons == {"honeypot_check_unknown": 1}
        assert summary.holder_lookup_outcomes == {}

    asyncio.run(run())


def test_paper_cycle_discovery_mode_still_respects_other_blockers(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "discovery-liquidity.db"
        source = FakeSignalSource(
            [
                [
                    build_enriched_pump_fun_signal(
                        "discovery-liquidity-mint",
                        created_at=datetime.now(UTC) - timedelta(minutes=10),
                        liquidity_sol=5.0,
                    )
                ]
            ]
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[source],
            poll_interval_s=0.0,
        )

        assert summary.execution_mode == "paper"
        assert summary.risk_profile == "discovery"
        assert summary.signals_collected == 1
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 1
        assert summary.rejection_reasons == {"liquidity_check_failed": 1}
        assert summary.holder_lookup_outcomes == {}

    asyncio.run(run())


def test_paper_cycle_discovery_mode_keeps_holder_unknown_when_payload_lacks_holder_fields(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "discovery-holder-unknown.db"
        source = FakeSignalSource(
            [
                [
                    Signal(
                        source=SignalSourceEnum.PUMP_FUN,
                        type=SignalType.NEW_POOL,
                        mint_address="discovery-holder-unknown-mint",
                        confidence=0.85,
                        payload={
                            "symbol": "PUMP",
                            "vSolInBondingCurve": 30.1,
                            "uniqueBuyers": 25,
                            "mintAuthorityRevoked": True,
                            "freezeAuthorityRevoked": True,
                            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
                        },
                    )
                ]
            ]
        )

        scorer = DiscoveryRiskScorer(
            RiskConfig(min_age_minutes=0),
            holder_lookup=FakeHolderLookup(error=RuntimeError("rpc unavailable")),
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[source],
            risk_scorer=scorer,
            poll_interval_s=0.0,
        )

        assert summary.execution_mode == "paper"
        assert summary.risk_profile == "discovery"
        assert summary.signals_collected == 1
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 1
        assert summary.rejection_reasons == {"top10_holder_check_unknown": 1}
        assert summary.holder_lookup_outcomes == {"holder_lookup_failed_provider": 1}

    asyncio.run(run())


def test_paper_cycle_discovery_mode_uses_holder_lookup_to_move_past_unknown(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "discovery-holder-lookup.db"
        source = FakeSignalSource(
            [
                [
                    Signal(
                        source=SignalSourceEnum.PUMP_FUN,
                        type=SignalType.NEW_POOL,
                        mint_address="discovery-holder-lookup-mint",
                        confidence=0.85,
                        payload={
                            "symbol": "PUMP",
                            "vSolInBondingCurve": 30.1,
                            "uniqueBuyers": 25,
                            "creatorHoldingPct": 5.0,
                            "mintAuthorityRevoked": True,
                            "freezeAuthorityRevoked": True,
                            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
                        },
                    )
                ]
            ]
        )
        scorer = DiscoveryRiskScorer(
            RiskConfig(min_age_minutes=0),
            holder_lookup=FakeHolderLookup(HolderLookupResult(top10_holder_pct=30.0)),
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[source],
            risk_scorer=scorer,
            poll_interval_s=0.0,
        )

        assert summary.execution_mode == "paper"
        assert summary.risk_profile == "discovery"
        assert summary.signals_collected == 1
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 1
        assert summary.rejection_reasons == {"honeypot_check_unknown": 1}
        assert summary.holder_lookup_outcomes == {"holder_lookup_succeeded": 1}

    asyncio.run(run())


def test_paper_cycle_discovery_mode_holder_lookup_failure_falls_back_to_unknown(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "discovery-holder-failure.db"
        source = FakeSignalSource(
            [
                [
                    Signal(
                        source=SignalSourceEnum.PUMP_FUN,
                        type=SignalType.NEW_POOL,
                        mint_address="discovery-holder-failure-mint",
                        confidence=0.85,
                        payload={
                            "symbol": "PUMP",
                            "vSolInBondingCurve": 30.1,
                            "uniqueBuyers": 25,
                            "mintAuthorityRevoked": True,
                            "freezeAuthorityRevoked": True,
                            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
                        },
                    )
                ]
            ]
        )
        scorer = DiscoveryRiskScorer(
            RiskConfig(min_age_minutes=0),
            holder_lookup=FakeHolderLookup(error=RuntimeError("rpc unavailable")),
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[source],
            risk_scorer=scorer,
            poll_interval_s=0.0,
        )

        assert summary.execution_mode == "paper"
        assert summary.risk_profile == "discovery"
        assert summary.signals_collected == 1
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 1
        assert summary.rejection_reasons == {"top10_holder_check_unknown": 1}
        assert summary.holder_lookup_outcomes == {"holder_lookup_failed_provider": 1}

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
        assert summary.holder_lookup_outcomes == {}
        assert summary.termination_reason == "timeout"
        assert source.started is True
        assert source.stopped is True

    asyncio.run(run())


def test_paper_cycle_counts_ambiguous_failures_as_unknown_or_other(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "unknown.db"
        source = FakeSignalSource([[build_signal("unknown-mint", passes=True, include_assessment=False)]])

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source],
            risk_scorer=ExplodingRiskScorer(),
            poll_interval_s=0.0,
        )

        assert summary.signals_collected == 1
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 1
        assert summary.trades_persisted == 0
        assert summary.rejection_reasons == {"unknown_or_other": 1}
        assert summary.holder_lookup_outcomes == {}

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
    assert "risk_profile=strict" in result.stdout
    assert "signals_collected=1" in result.stdout
    assert "signals_accepted=1" in result.stdout
    assert "termination_reason=max_signals" in result.stdout
    assert "rejection_reasons" not in result.stdout
    assert "cli-secret-mint" not in result.stdout
    assert "super secret alpha message" not in result.stdout


def test_paper_cycle_cli_prints_discovery_risk_profile(tmp_path: Path, monkeypatch) -> None:
    signal = build_enriched_pump_fun_signal(
        "discovery-cli-mint",
        created_at=datetime.now(UTC),
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
            "--mode",
            "discovery",
            "--db-path",
            str(tmp_path / "discovery-cli.db"),
        ],
    )

    assert result.exit_code == 0
    assert "execution_mode=paper" in result.stdout
    assert "risk_profile=discovery" in result.stdout
    assert "age_check_failed" not in result.stdout

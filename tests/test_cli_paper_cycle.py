import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import RiskConfig
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource as SignalSourceEnum, SignalType, TokenInfo
from src.monitoring.dashboard import load_open_positions, load_recent_trades
from src.risk.rugcheck import RugCheckResult
from src.risk.scorer import DiscoveryRiskScorer, HolderLookupResult
from src.signals.base import SignalSource


runner = CliRunner()


class FakeSignalSource(SignalSource):
    def __init__(self, batches: list[list[Signal]], *, name: str = "fake", poll_error: Exception | None = None) -> None:
        self._batches = list(batches)
        self._name = name
        self._poll_error = poll_error
        self.started = False
        self.stopped = False
        self.poll_calls = 0

    @property
    def name(self) -> str:
        return self._name

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def poll(self) -> list[Signal]:
        self.poll_calls += 1
        if self._poll_error is not None:
            raise self._poll_error
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


class FakeRugCheckClient:
    def __init__(self, result: RugCheckResult | None = None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error

    async def fetch_report(self, mint_address: str) -> RugCheckResult:
        if self._error is not None:
            raise self._error
        if self._result is not None:
            return self._result
        return RugCheckResult(mint_address=mint_address, provider_status="timeout", error="timed out")


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
        assert summary.sources_polled == ["fake"]
        assert summary.source_signal_counts == {"fake": 2}
        assert summary.source_failures == {}
        assert summary.composite_opportunities == 0
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
        assert summary.sources_polled == ["fake"]
        assert summary.holder_lookup_outcomes == {}
        assert len(trades) == 1
        assert trades[0].mode == "paper"
        assert "candidate_snapshot" in trades[0].metadata
        assert trades[0].metadata["candidate_snapshot"]["action_outcome"] == "traded"

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
        assert summary.sources_polled == ["fake"]
        assert summary.source_signal_counts == {"fake": 3}
        assert summary.source_failures == {}
        assert summary.composite_opportunities == 0
        assert summary.rejection_reasons == {}
        assert summary.holder_lookup_outcomes == {}
        assert summary.termination_reason == "max_signals"
        assert len(trades) == 1
        assert trades[0].mint_address in {"mint-1", "mint-2", "mint-3"}

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
        assert summary.sources_polled == ["fake"]
        assert summary.source_signal_counts == {"fake": 3}
        assert summary.source_failures == {}
        assert summary.composite_opportunities == 0
        assert summary.rejection_reasons == {
            "honeypot_check_failed": 2,
            "position_size_zero": 1,
        }
        assert summary.holder_lookup_outcomes == {}

    asyncio.run(run())


def test_paper_cycle_capacity_blockers_report_current_open_positions(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "capacity-blocked.db"
        constrained_settings = cli_module.load_settings().model_copy(
            update={
                "position": cli_module.load_settings().position.model_copy(
                    update={"max_open_positions": 1}
                )
            }
        )

        first_summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            settings=constrained_settings,
            sources=[FakeSignalSource([[build_signal("capacity-seed", passes=True)]])],
            poll_interval_s=0.0,
        )
        blocked_summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            settings=constrained_settings,
            sources=[FakeSignalSource([[build_signal("capacity-blocked", passes=True)]])],
            poll_interval_s=0.0,
        )

        safe_lines = blocked_summary.safe_lines()

        assert first_summary.open_positions == 1
        assert blocked_summary.signals_accepted == 0
        assert blocked_summary.signals_rejected == 1
        assert blocked_summary.rejection_reasons == {"max_open_positions_reached": 1}
        assert blocked_summary.capacity_blocked_candidates == 1
        assert blocked_summary.starting_open_positions == 1
        assert blocked_summary.persisted_open_positions == 1
        assert blocked_summary.configured_max_open_positions == 1
        assert blocked_summary.rejected_candidate_diagnostics[0]["action_outcome"] == "capacity-blocked"
        assert blocked_summary.rejected_candidate_diagnostics[0]["rejection_reason"] == "max_open_positions_reached"
        assert "starting_open_positions=1" in safe_lines
        assert "persisted_open_positions=1" in safe_lines
        assert "configured_max_open_positions=1" in safe_lines
        assert "capacity_blocked_candidates=1" in safe_lines

    asyncio.run(run())


def test_paper_cycle_fresh_evaluation_session_ignores_old_paper_positions_only_for_session(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "fresh-eval.db"
        constrained_settings = cli_module.load_settings().model_copy(
            update={
                "execution": cli_module.load_settings().execution.model_copy(update={"mode": "live"}),
                "position": cli_module.load_settings().position.model_copy(
                    update={"max_open_positions": 1}
                ),
            }
        )

        await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            settings=constrained_settings,
            sources=[FakeSignalSource([[build_signal("persisted-seed", passes=True)]])],
            poll_interval_s=0.0,
        )

        fresh_summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            settings=constrained_settings,
            fresh_evaluation_session=True,
            sources=[FakeSignalSource([[build_signal("fresh-session-mint", passes=True)]])],
            poll_interval_s=0.0,
        )

        trades = load_recent_trades(db_path, limit=10)
        positions = load_open_positions(db_path)

        assert fresh_summary.execution_mode == "paper"
        assert fresh_summary.evaluation_session_scope == "fresh"
        assert fresh_summary.starting_open_positions == 1
        assert fresh_summary.persisted_open_positions == 1
        assert fresh_summary.open_positions == 1
        assert fresh_summary.trades_persisted == 1
        assert len(fresh_summary.accepted_candidate_diagnostics) == 1
        assert fresh_summary.accepted_candidate_diagnostics[0]["action_outcome"] == "traded"
        assert len(trades) == 2
        assert any(trade.mint_address == "fresh-session-mint" for trade in trades)
        assert len(positions) == 1
        assert positions[0].mint_address == "persisted-seed"

    asyncio.run(run())


def test_paper_cycle_aggregator_combines_multi_source_signals(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "aggregated.db"
        source_a = FakeSignalSource(
            [[build_signal("composite-mint", passes=True)]],
            name="pump_fun",
        )
        source_b = FakeSignalSource(
            [[
                Signal(
                    source=SignalSourceEnum.WHALE_TRACKER,
                    type=SignalType.BUY,
                    mint_address="composite-mint",
                    confidence=0.7,
                    payload={"risk_assessment": build_assessment("composite-mint", passes=True)},
                )
            ]],
            name="whale_tracker",
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=5,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source_a, source_b],
            poll_interval_s=0.0,
        )

        trades = load_recent_trades(db_path, limit=5)

        assert summary.signals_collected == 1
        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 0
        assert summary.composite_opportunities == 1
        assert summary.sources_polled == ["pump_fun", "whale_tracker"]
        assert summary.source_signal_counts == {"pump_fun": 1, "whale_tracker": 1}
        assert summary.source_failures == {}
        assert len(trades) == 1

    asyncio.run(run())


def test_discovery_candidate_snapshot_persists_attention_fields_without_raw_payload(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "discovery-snapshot.db"
        signal = build_signal("snapshot-mint", passes=True)
        signal = signal.model_copy(
            update={
                "source": SignalSourceEnum.PUMP_FUN,
                "type": SignalType.NEW_POOL,
            }
        )
        signal.payload.update(
            {
                "symbol": "SNAP",
                "name": "Snapshot Token",
                "attention_diagnostics": {
                    "attention_score": 79,
                    "attention_tier": "strong_watch",
                    "attention_reasons": ["launch-stage signal", "strong liquidity"],
                    "narrative_tags": ["fresh-launch", "pumpfun", "pumpfun-launch"],
                    "social_signal_state": "missing",
                    "metadata_completeness_state": "partial",
                },
                "holder_policy": {
                    "holder_policy_state": "pass",
                    "stage_hint": "new_pool",
                    "token_age_minutes": 0.2,
                },
                "creator_policy": {"creator_policy_state": "unknown_warning"},
                "unique_buyers_policy": {"unique_buyers_policy_state": "unknown_warning"},
                "authority_policy": {"authority_policy_state": "unknown_warning"},
                "honeypot_policy": {"honeypot_policy_state": "unknown_warning"},
                "liquidity_diagnostics": {
                    "selected_liquidity_sol": 30.1,
                    "selected_liquidity_usd": 900.0,
                    "liquidity_source": "signal_payload",
                    "liquidity_data_state": "known",
                },
                "holder_diagnostics": {
                    "selected_top10_holder_pct": 12.0,
                    "top10_holder_source": "signal_payload",
                },
            }
        )
        signal.payload["raw_data"] = {"secret": "do-not-store"}
        signal.payload["buyerWallets"] = ["BuyerWallet11111111111111111111111111111111"]

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([[signal]])],
            poll_interval_s=0.0,
        )

        trades = load_recent_trades(db_path, limit=5)
        snapshot = trades[0].metadata["candidate_snapshot"]

        assert summary.signals_accepted == 1
        assert len(summary.accepted_candidate_diagnostics) == 1
        assert snapshot["attention_score"] > 0
        assert snapshot["attention_tier"] in {"watch", "strong_watch", "candidate"}
        assert "pumpfun-launch" in snapshot["narrative_tags"]
        assert snapshot["action_outcome"] == "traded"
        assert "raw_data" not in snapshot
        assert "buyerWallets" not in snapshot
        assert "do-not-store" not in str(snapshot)
        assert "BuyerWallet11111111111111111111111111111111" not in str(snapshot)

    asyncio.run(run())


def test_strict_mode_keeps_candidate_snapshot_path_inactive_for_rejected_trade_set(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "strict-no-snapshot.db"
        source = FakeSignalSource(
            [[
                build_enriched_pump_fun_signal(
                    "strict-snapshot-mint",
                    created_at=datetime.now(UTC),
                )
            ]]
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source],
            poll_interval_s=0.0,
        )

        trades = load_recent_trades(db_path, limit=5)

        assert summary.risk_profile == "strict"
        assert summary.signals_accepted == 0
        assert summary.accepted_candidate_diagnostics == []
        assert trades == []

    asyncio.run(run())


def test_paper_cycle_aggregator_keeps_missing_twitter_credentials_non_fatal(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "twitter-missing.db"
        source = FakeSignalSource([[build_signal("mint-1", passes=True)]], name="manual")

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[cli_module.TwitterMonitor(bearer_token="", grok_api_key=""), source],
            poll_interval_s=0.0,
        )

        assert summary.signals_collected == 1
        assert summary.signals_accepted == 1
        assert summary.sources_polled == ["twitter", "manual"]
        assert summary.source_signal_counts == {"manual": 1, "twitter": 0}
        assert summary.source_failures == {}

    asyncio.run(run())


def test_paper_cycle_aggregator_source_failures_are_non_fatal(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "source-failure.db"
        healthy = FakeSignalSource([[build_signal("mint-1", passes=True)]], name="healthy")
        broken = FakeSignalSource([[]], name="broken", poll_error=RuntimeError("boom"))

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[healthy, broken],
            poll_interval_s=0.0,
        )

        assert summary.signals_collected == 1
        assert summary.signals_accepted == 1
        assert summary.sources_polled == ["healthy", "broken"]
        assert summary.source_signal_counts == {"healthy": 1}
        assert summary.source_failures == {"broken": 1}

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
        assert summary.sources_polled == ["fake"]
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
        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 0
        assert summary.rejection_reasons == {}
        assert summary.sources_polled == ["fake"]
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
        assert summary.sources_polled == ["fake"]
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
        assert summary.sources_polled == ["fake"]
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
            holder_policy_mode="discovery",
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
        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 0
        assert summary.rejection_reasons == {}
        assert summary.sources_polled == ["fake"]
        assert summary.holder_lookup_outcomes == {"holder_lookup_succeeded": 1}

    asyncio.run(run())


def test_paper_cycle_discovery_mode_uses_rugcheck_and_stays_paper(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "discovery-rugcheck.db"
        mint_address = "So11111111111111111111111111111111111111112"
        source = FakeSignalSource(
            [
                [
                    Signal(
                        source=SignalSourceEnum.PUMP_FUN,
                        type=SignalType.NEW_POOL,
                        mint_address=mint_address,
                        confidence=0.85,
                        payload={
                            "symbol": "PUMP",
                            "vSolInBondingCurve": 30.1,
                            "uniqueBuyers": 25,
                            "creatorHoldingPct": 5.0,
                            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
                        },
                    )
                ]
            ]
        )
        scorer = DiscoveryRiskScorer(
            RiskConfig(min_age_minutes=0),
            rugcheck_client=FakeRugCheckClient(
                RugCheckResult(
                    mint_address=mint_address,
                    found=True,
                    mint_authority_revoked=True,
                    freeze_authority_revoked=True,
                    top_holder_pct=30.0,
                    is_honeypot=False,
                    liquidity_locked=True,
                    liquidity_status="locked",
                    risk_level="low",
                    provider_status="ok",
                )
            ),
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

        trades = load_recent_trades(db_path, limit=5)

        assert summary.execution_mode == "paper"
        assert summary.risk_profile == "discovery"
        assert summary.signals_collected == 1
        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 0
        assert summary.rejection_reasons == {}
        assert summary.holder_lookup_outcomes["rugcheck_used"] == 1
        assert summary.holder_lookup_outcomes["rugcheck_used_honeypot_pass"] == 1
        assert len(trades) == 1
        assert trades[0].mode == "paper"

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
        assert summary.sources_polled == ["fake"]
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
        assert summary.sources_polled == ["fake"]
        assert summary.source_signal_counts == {}
        assert summary.source_failures == {}
        assert summary.composite_opportunities == 0
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
        assert summary.sources_polled == ["fake"]
        assert summary.rejection_reasons == {"unknown_or_other": 1}
        assert summary.holder_lookup_outcomes == {}

    asyncio.run(run())


def test_paper_cycle_cli_prints_safe_summary(tmp_path: Path, monkeypatch) -> None:
    signal = build_signal(
        "cli-secret-mint",
        passes=True,
        message="super secret alpha message that should not be printed",
    )
    signal.payload["buyerWallets"] = [
        "BuyerWallet11111111111111111111111111111111",
        "BuyerWallet22222222222222222222222222222222",
    ]
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
    assert "sources_polled=fake" in result.stdout
    assert "composite_opportunities=0" in result.stdout
    assert "termination_reason=max_signals" in result.stdout
    assert "rejection_reasons" not in result.stdout
    assert "cli-secret-mint" not in result.stdout
    assert "super secret alpha message" not in result.stdout
    assert "BuyerWallet11111111111111111111111111111111" not in result.stdout
    assert "BuyerWallet22222222222222222222222222222222" not in result.stdout


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
    assert "sources_polled=fake" in result.stdout
    assert "age_check_failed" not in result.stdout

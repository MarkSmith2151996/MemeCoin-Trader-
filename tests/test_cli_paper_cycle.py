import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import RiskConfig
from src.core.database import get_recent_paper_decisions, init_db, record_paper_decision, record_position
from src.core.models import CheckResult, PaperDecisionRecord, Position, RiskAssessment, Signal, SignalSource as SignalSourceEnum, SignalType, TokenInfo
from src.execution.price_provider import FakePriceProvider, UnavailablePriceProvider
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
        decisions = await get_recent_paper_decisions(db_path, limit=10)

        assert len(trades) == 1
        assert trades[0].mint_address == "accepted-mint"
        assert trades[0].mode == "paper"
        assert len(positions) == 1
        assert positions[0].mint_address == "accepted-mint"
        assert len(decisions) == 2
        assert {d.action_outcome for d in decisions} == {"traded", "rejected"}
        accepted = next(d for d in decisions if d.action_outcome == "traded")
        rejected = next(d for d in decisions if d.action_outcome == "rejected")
        assert accepted.mint_address == "accepted-mint"
        assert accepted.primary_reason == "traded"
        assert rejected.primary_reason == "honeypot_check_failed"
        assert json.loads(accepted.diagnostics_json)["trade_id"] == positions[0].entry_trade_id == trades[0].id
        assert json.loads(rejected.diagnostics_json)["trade_id"] is None

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
        decisions = await get_recent_paper_decisions(db_path, limit=10)

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
        blocked = next(d for d in decisions if d.mint_address == "capacity-blocked")
        assert blocked.action_outcome == "capacity-blocked"
        assert blocked.primary_reason == "max_open_positions_reached"

    asyncio.run(run())


def test_paper_cycle_persists_safe_skipped_reason(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "skipped-data.db"
        source = FakeSignalSource([[build_signal("skipped-mint", passes=True, confidence=0.0)]])

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source],
            poll_interval_s=0.0,
        )
        decisions = await get_recent_paper_decisions(db_path, limit=10)

        assert summary.signals_rejected == 1
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision.action_outcome == "skipped"
        assert decision.primary_reason == "position_size_zero"
        assert "private_key" not in decision.diagnostics_json.lower()
        assert "api_key" not in decision.diagnostics_json.lower()

    asyncio.run(run())


def test_paper_cycle_ignores_archived_paper_rows_for_capacity_counts(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "archived-capacity.db"
        await init_db(db_path)
        archived_position = Position(
            mint_address="archived-paper-mint",
            entry_trade_id="archived-paper-trade",
            amount_sol=1.0,
            token_amount=0.0,
            entry_price_sol=1.0,
            mode="paper",
            archived=True,
        )
        await record_position(db_path, archived_position)

        constrained_settings = cli_module.load_settings().model_copy(
            update={
                "position": cli_module.load_settings().position.model_copy(update={"max_open_positions": 1})
            }
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            settings=constrained_settings,
            sources=[FakeSignalSource([[build_signal("active-paper-mint", passes=True)]])],
            poll_interval_s=0.0,
        )

        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 0
        assert summary.starting_open_positions == 0
        assert summary.persisted_open_positions == 1
        assert summary.capacity_blocked_candidates == 0

    asyncio.run(run())


def test_paper_cycle_ignores_live_rows_for_paper_capacity(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "live-capacity.db"
        await init_db(db_path)
        archived_paper = Position(
            mint_address="archived-paper-live-mix",
            entry_trade_id="archived-paper-live-mix-trade",
            amount_sol=1.0,
            token_amount=0.0,
            entry_price_sol=1.0,
            mode="paper",
            archived=True,
        )
        live_position = Position(
            mint_address="live-position-mint",
            entry_trade_id="live-position-trade",
            amount_sol=1.0,
            token_amount=100000.0,
            entry_price_sol=0.00001,
            mode="live",
        )
        await record_position(db_path, archived_paper)
        await record_position(db_path, live_position)

        constrained_settings = cli_module.load_settings().model_copy(
            update={
                "position": cli_module.load_settings().position.model_copy(update={"max_open_positions": 1})
            }
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            settings=constrained_settings,
            sources=[FakeSignalSource([[build_signal("blocked-by-live", passes=True)]])],
            poll_interval_s=0.0,
        )

        assert summary.signals_accepted == 1
        assert summary.signals_rejected == 0
        assert summary.rejection_reasons == {}
        assert summary.starting_open_positions == 0
        assert summary.persisted_open_positions == 1
        assert summary.capacity_blocked_candidates == 0

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
        assert snapshot["narrative_quality_hint"]
        assert snapshot["theme_cluster_hint"]
        assert snapshot["name_quality_hint"]
        assert snapshot["source_context_hint"]
        assert snapshot["momentum_context_hint"]
        assert "raw_data" not in snapshot
        assert "buyerWallets" not in snapshot
        assert "do-not-store" not in str(snapshot)
        assert "BuyerWallet11111111111111111111111111111111" not in str(snapshot)

    asyncio.run(run())


def test_discovery_cli_safe_lines_include_ranked_candidate_summary(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "discovery-summary.db"
        accepted_signal = build_signal("summary-accepted", passes=True).model_copy(
            update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL}
        )
        accepted_signal.payload.update(
            {
                "symbol": "TOP",
                "attention_diagnostics": {
                    "attention_score": 79,
                    "attention_tier": "strong_watch",
                    "attention_reasons": ["launch-stage signal"],
                    "narrative_tags": ["fresh-launch", "pumpfun", "pumpfun-launch"],
                    "social_signal_state": "missing",
                    "metadata_completeness_state": "partial",
                },
                "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.1},
                "creator_policy": {"creator_policy_state": "unknown_warning"},
            }
        )
        blocked_signal = build_signal("summary-blocked", passes=True).model_copy(
            update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL}
        )
        blocked_signal.payload.update(
            {
                "symbol": "BLOCK",
                "attention_diagnostics": {
                    "attention_score": 79,
                    "attention_tier": "strong_watch",
                    "attention_reasons": ["launch-stage signal"],
                    "narrative_tags": ["fresh-launch", "pumpfun", "pumpfun-launch"],
                    "social_signal_state": "missing",
                    "metadata_completeness_state": "partial",
                },
                "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.1},
            }
        )
        constrained_settings = cli_module.load_settings().model_copy(
            update={
                "position": cli_module.load_settings().position.model_copy(update={"max_open_positions": 1})
            }
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=2,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            settings=constrained_settings,
            sources=[FakeSignalSource([[accepted_signal, blocked_signal]])],
            poll_interval_s=0.0,
        )

        lines = summary.safe_lines()
        joined = "\n".join(lines)

        assert "Top discovery candidates:" in joined
        assert "candidate_mode_counts:" in joined
        assert "launch=" in joined
        assert "TOP" in joined
        assert "BLOCK" in joined
        assert "launch |" in joined
        assert "capacity-blocked" in joined
        assert "cluster:" in joined or "distinct-theme" in joined
        assert "partial" in joined
        assert "Accepted discovery comparison:" in joined
        assert "Mode routing guidance:" in joined
        assert "launch: fast-path only; no AI required or consulted" in joined

    asyncio.run(run())


def test_discovery_edge_diagnostics_are_deterministic_safe_and_paper_only(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "edge-diagnostics.db"
        pump_signal = build_signal("edge-mint", passes=True).model_copy(
            update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL}
        )
        onchain_signal = build_signal("edge-mint", passes=True).model_copy(
            update={"source": SignalSourceEnum.ONCHAIN, "type": SignalType.BUY}
        )
        onchain_signal.payload.update(
            {
                "symbol": "EDGE",
                "api_key": "do-not-leak-edge-secret",
                "attention_diagnostics": {
                    "attention_score": 80,
                    "attention_tier": "strong_watch",
                    "social_signal_state": "present",
                },
            }
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([[pump_signal]]), FakeSignalSource([[onchain_signal]])],
            poll_interval_s=0.0,
        )
        diagnostic = summary.accepted_candidate_diagnostics[0]
        snapshot = load_recent_trades(db_path, limit=1)[0].metadata["candidate_snapshot"]
        repeat = cli_module._apply_discovery_edge_diagnostics([dict(diagnostic)], [])[0][0]
        output = "\n".join(summary.safe_lines())

        assert summary.execution_mode == "paper"
        assert diagnostic["edge_score"] == repeat["edge_score"]
        assert diagnostic["edge_breakdown"] == repeat["edge_breakdown"]
        assert "src=2/comp=" in diagnostic["edge_breakdown"]
        assert "approval=strict_rejected" in diagnostic["edge_breakdown"]
        assert snapshot["edge_score"] == diagnostic["edge_score"]
        assert snapshot["edge_breakdown"] == diagnostic["edge_breakdown"]
        assert "| edge |" in output
        assert "do-not-leak-edge-secret" not in str(snapshot)
        assert "do-not-leak-edge-secret" not in output

    asyncio.run(run())


def test_discovery_cli_safe_lines_show_migration_mode_as_diagnostic_only(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "migration-mode-summary.db"
        migration_signal = build_signal("migration-summary", passes=True).model_copy(
            update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.GRADUATION}
        )
        migration_signal.payload.update(
            {
                "symbol": "GRAD",
                "name": "Graduated Token",
                "txType": "migrate",
                "pool": "raydium",
                "attention_diagnostics": {
                    "attention_score": 79,
                    "attention_tier": "strong_watch",
                    "attention_reasons": ["migration-stage signal"],
                    "narrative_tags": ["pumpfun", "graduation"],
                    "social_signal_state": "missing",
                    "metadata_completeness_state": "partial",
                },
                "holder_policy": {"holder_policy_state": "pass", "stage_hint": "graduation", "token_age_minutes": 15.0},
                "liquidity_diagnostics": {"selected_liquidity_sol": 3000.0, "liquidity_source": "signal_payload", "liquidity_data_state": "known"},
                "holder_diagnostics": {"selected_top10_holder_pct": 5.0, "top10_holder_source": "signal_payload"},
            }
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([[migration_signal]])],
            poll_interval_s=0.0,
        )

        joined = "\n".join(summary.safe_lines())

        assert "migration=1" in joined
        assert "migration |" in joined
        assert "migration: diagnostic only for now; may later become AI-eligible" in joined
        assert summary.accepted_candidate_diagnostics[0]["candidate_mode"] == "migration"

    asyncio.run(run())


def test_discovery_theme_cluster_hints_detect_repeated_clone_names(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "theme-cluster.db"
        clone_a = build_signal("clone-a", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})
        clone_b = build_signal("clone-b", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})
        unique = build_signal("unique-c", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})
        for signal, symbol, liquidity in (
            (clone_a, "fatdog", 3000.0),
            (clone_b, "fatbull", 3100.0),
            (unique, "nebulon", 3200.0),
        ):
            signal.payload.update(
                {
                    "symbol": symbol,
                    "name": symbol,
                    "attention_diagnostics": {
                        "attention_score": 79,
                        "attention_tier": "strong_watch",
                        "attention_reasons": ["launch-stage signal"],
                        "narrative_tags": ["fresh-launch", "pumpfun", "pumpfun-launch"],
                        "social_signal_state": "missing",
                        "metadata_completeness_state": "partial",
                    },
                    "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.2},
                    "liquidity_diagnostics": {"selected_liquidity_sol": liquidity, "liquidity_source": "signal_payload", "liquidity_data_state": "known"},
                    "holder_diagnostics": {"selected_top10_holder_pct": 5.0, "top10_holder_source": "signal_payload"},
                }
            )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=3,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([[clone_a, clone_b, unique]])],
            poll_interval_s=0.0,
        )

        clone_hints = {item["symbol"]: item["theme_cluster_hint"] for item in summary.accepted_candidate_diagnostics}
        name_hints = {item["symbol"]: item["name_quality_hint"] for item in summary.accepted_candidate_diagnostics}

        assert clone_hints["fatdog"] != "cluster:liquid"
        assert clone_hints["fatbull"] != "cluster:liquid"
        assert name_hints["fatdog"] != "differentiated-name"
        assert name_hints["nebulon"] == "differentiated-name"

    asyncio.run(run())


def test_discovery_theme_cluster_hint_allows_generic_liquidity_fallback_when_no_better_theme(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "theme-liquidity-fallback.db"
        alpha = build_signal("alpha-a", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})
        beta = build_signal("beta-b", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})
        for signal, symbol in ((alpha, "aurora"), (beta, "nebula")):
            signal.payload.update(
                {
                    "symbol": symbol,
                    "name": symbol,
                    "attention_diagnostics": {
                        "attention_score": 79,
                        "attention_tier": "strong_watch",
                        "attention_reasons": ["launch-stage signal"],
                        "narrative_tags": ["fresh-launch", "liquid", "pumpfun-launch"],
                        "social_signal_state": "missing",
                        "metadata_completeness_state": "partial",
                    },
                    "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.2},
                    "liquidity_diagnostics": {"selected_liquidity_sol": 4000.0, "liquidity_source": "signal_payload", "liquidity_data_state": "known"},
                    "holder_diagnostics": {"selected_top10_holder_pct": 5.0, "top10_holder_source": "signal_payload"},
                }
            )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=2,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([[alpha, beta]])],
            poll_interval_s=0.0,
        )

        theme_hints = {item["symbol"]: item["theme_cluster_hint"] for item in summary.accepted_candidate_diagnostics}

        assert theme_hints["aurora"] == "cluster:liquid"
        assert theme_hints["nebula"] == "cluster:liquid"

    asyncio.run(run())


def test_discovery_ranking_penalty_demotes_weaker_pump_fun_identity_without_rejecting_it(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "weak-identity-ranking.db"
        weak = build_signal("weak-pump", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})
        clean = build_signal("clean-pump", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})
        for signal, symbol, name, liquidity in (
            (weak, "fatdog", "READ INSANE FOLLOWERS", 3300.0),
            (clean, "nebulon", "Nebulon", 3200.0),
        ):
            signal.payload.update(
                {
                    "symbol": symbol,
                    "name": name,
                    "attention_diagnostics": {
                        "attention_score": 79,
                        "attention_tier": "strong_watch",
                        "attention_reasons": ["launch-stage signal"],
                        "narrative_tags": ["fresh-launch", "pumpfun", "pumpfun-launch"],
                        "social_signal_state": "missing",
                        "metadata_completeness_state": "partial",
                    },
                    "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.2},
                    "liquidity_diagnostics": {"selected_liquidity_sol": liquidity, "liquidity_source": "signal_payload", "liquidity_data_state": "known"},
                    "holder_diagnostics": {"selected_top10_holder_pct": 5.0, "top10_holder_source": "signal_payload"},
                }
            )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=2,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([[weak, clean]])],
            poll_interval_s=0.0,
        )

        ranked = sorted(summary.accepted_candidate_diagnostics, key=cli_module._accepted_candidate_sort_key)

        assert summary.signals_accepted == 2
        assert ranked[0]["symbol"] == "nebulon"
        assert ranked[1]["symbol"] == "fatdog"
        assert ranked[1]["ranking_penalty_points"] > 0
        assert "weak_identity" in ranked[1]["ranking_penalty_reasons"]
        assert ranked[1]["ranking_attention_score"] < ranked[1]["attention_score"]

    asyncio.run(run())


def test_discovery_ranking_penalty_does_not_bury_strong_multi_source_candidate_for_partial_pump_metadata(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "multi-source-partial-metadata.db"
        pump_leg = build_signal("shared-mint", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})
        onchain_leg = build_signal("shared-mint", passes=True).model_copy(update={"source": SignalSourceEnum.ONCHAIN, "type": SignalType.BUY})
        standalone = build_signal("standalone-pump", passes=True).model_copy(update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL})

        pump_leg.payload.update(
            {
                "symbol": "ALPHA",
                "name": "Alpha",
                "attention_diagnostics": {
                    "metadata_completeness_state": "partial",
                    "attention_score": 75,
                    "attention_tier": "strong_watch",
                    "attention_reasons": ["launch-stage signal"],
                    "narrative_tags": ["fresh-launch", "pumpfun", "pumpfun-launch"],
                    "social_signal_state": "missing",
                },
                "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.2},
                "liquidity_diagnostics": {"selected_liquidity_sol": 3000.0, "liquidity_source": "signal_payload", "liquidity_data_state": "known"},
                "holder_diagnostics": {"selected_top10_holder_pct": 5.0, "top10_holder_source": "signal_payload"},
            }
        )
        onchain_leg.payload.update(
            {
                "symbol": "ALPHA",
                "name": "Alpha",
                "attention_diagnostics": {
                    "metadata_completeness_state": "rich",
                    "attention_score": 75,
                    "attention_tier": "strong_watch",
                    "attention_reasons": ["onchain confirmation"],
                    "narrative_tags": ["momentum"],
                    "social_signal_state": "missing",
                },
                "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.3},
                "liquidity_diagnostics": {"selected_liquidity_sol": 3200.0, "liquidity_source": "signal_payload", "liquidity_data_state": "known"},
                "holder_diagnostics": {"selected_top10_holder_pct": 4.0, "top10_holder_source": "signal_payload"},
            }
        )
        standalone.payload.update(
            {
                "symbol": "BETA",
                "name": "Beta",
                "attention_diagnostics": {
                    "metadata_completeness_state": "partial",
                    "attention_score": 74,
                    "attention_tier": "strong_watch",
                    "attention_reasons": ["launch-stage signal"],
                    "narrative_tags": ["fresh-launch", "pumpfun", "pumpfun-launch"],
                    "social_signal_state": "missing",
                },
                "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.2},
                "liquidity_diagnostics": {"selected_liquidity_sol": 3100.0, "liquidity_source": "signal_payload", "liquidity_data_state": "known"},
                "holder_diagnostics": {"selected_top10_holder_pct": 5.0, "top10_holder_source": "signal_payload"},
            }
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=2,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([[pump_leg, onchain_leg, standalone]])],
            poll_interval_s=0.0,
        )

        ranked = sorted(summary.accepted_candidate_diagnostics, key=cli_module._accepted_candidate_sort_key)
        alpha = next(item for item in ranked if item["symbol"] == "ALPHA")

        assert ranked[0]["symbol"] == "ALPHA"
        assert summary.composite_opportunities == 1
        assert alpha["source_context_hint"] == "multi-source:2"
        assert alpha["ranking_penalty_points"] < 8
        assert "partial_metadata" not in alpha["ranking_penalty_reasons"]

    asyncio.run(run())


def test_discovery_ranking_penalty_does_not_apply_to_non_pump_fun_candidates(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "non-pump-no-penalty.db"
        onchain = build_signal("onchain-only", passes=True).model_copy(update={"source": SignalSourceEnum.ONCHAIN, "type": SignalType.BUY})
        onchain.payload.update(
            {
                "symbol": "WATCH",
                "name": "Watch",
                "attention_diagnostics": {
                    "attention_score": 70,
                    "attention_tier": "candidate",
                    "attention_reasons": ["momentum signal"],
                    "narrative_tags": ["momentum"],
                    "social_signal_state": "missing",
                    "metadata_completeness_state": "partial",
                },
                "holder_policy": {"holder_policy_state": "pass", "stage_hint": "unknown", "token_age_minutes": 2.0},
                "liquidity_diagnostics": {"selected_liquidity_sol": 2500.0, "liquidity_source": "signal_payload", "liquidity_data_state": "known"},
                "holder_diagnostics": {"selected_top10_holder_pct": 5.0, "top10_holder_source": "signal_payload"},
            }
        )

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([[onchain]])],
            poll_interval_s=0.0,
        )

        assert summary.signals_accepted == 1
        assert summary.accepted_candidate_diagnostics[0]["source"] == "onchain"
        assert summary.accepted_candidate_diagnostics[0]["ranking_penalty_points"] == 0
        assert summary.accepted_candidate_diagnostics[0]["ranking_penalty_reasons"] == ()

    asyncio.run(run())


def test_discovery_safe_lines_include_bounded_grok_prompt_export(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "grok-prompt.db"
        signals: list[Signal] = []
        for index in range(6):
            signal = build_signal(f"grok-mint-{index}", passes=True).model_copy(
                update={"source": SignalSourceEnum.PUMP_FUN, "type": SignalType.NEW_POOL}
            )
            signal.payload.update(
                {
                    "symbol": f"GROK{index}",
                    "name": f"Grok Candidate {index}",
                    "raw_provider_payload": {"secret": "should-not-appear"},
                    "api_key": "should-not-appear",
                    "attention_diagnostics": {
                        "attention_score": 90 - index,
                        "attention_tier": "strong_watch",
                        "attention_reasons": ["launch-stage signal"],
                        "narrative_tags": ["fresh-launch", f"theme-{index}", "pumpfun-launch"],
                        "social_signal_state": "missing",
                        "metadata_completeness_state": "partial",
                    },
                    "holder_policy": {"holder_policy_state": "pass", "stage_hint": "new_pool", "token_age_minutes": 0.2 + index},
                    "liquidity_diagnostics": {
                        "selected_liquidity_sol": 2500.0 + index,
                        "liquidity_source": "signal_payload",
                        "liquidity_data_state": "known",
                    },
                    "holder_diagnostics": {"selected_top10_holder_pct": 5.0 + index, "top10_holder_source": "signal_payload"},
                }
            )
            signals.append(signal)

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=6,
            timeout_seconds=0.1,
            db_path=db_path,
            risk_profile="discovery",
            sources=[FakeSignalSource([signals])],
            poll_interval_s=0.0,
        )

        joined = "\n".join(summary.safe_lines())

        assert "Grok social check prompt (manual only):" in joined
        assert "Return ONLY valid JSON array entries with keys: mint, social_live_score, tweet_velocity, real_account_signal, bot_spam_risk, influencer_mentions, ticker_collision, narrative_summary, recommendation." in joined
        assert "name=Grok Candidate 0; symbol=GROK0; mint=grok-mint-0; source=pump_fun;" in joined
        assert "name=Grok Candidate 4; symbol=GROK4; mint=grok-mint-4; source=pump_fun;" in joined
        assert "name=Grok Candidate 5; symbol=GROK5; mint=grok-mint-5; source=pump_fun;" not in joined
        assert "wallet_cluster=pending/unavailable" in joined
        assert "social=missing" in joined
        assert "raw_provider_payload" not in joined
        assert "should-not-appear" not in joined

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


def test_paper_cycle_discovery_mode_ranks_unknown_honeypot_without_strict_approval(tmp_path: Path) -> None:
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
        assert summary.rejected_candidate_diagnostics[0]["risk_approval_state"] == "discovery_relaxed"
        assert "discovery_relaxed" in "\n".join(summary.safe_lines())
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


def test_paper_cycle_discovery_mode_keeps_unknown_honeypot_out_of_strict_approval(tmp_path: Path) -> None:
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
        assert summary.signals_accepted == 0
        assert summary.signals_rejected == 1
        assert summary.rejection_reasons == {"honeypot_check_unknown": 1}
        assert summary.rejected_candidate_diagnostics[0]["risk_approval_state"] == "discovery_relaxed"
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


def test_paper_soak_audit_includes_all_diagnostic_sections(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "paper-soak.db"
        sources = [
            FakeSignalSource(
                [
                    [
                        build_signal("accepted-1", passes=True),
                        build_signal("rejected-1", passes=False),
                    ]
                ]
            )
        ]

        audit = await cli_module.run_paper_soak(
            max_signals=2,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=sources,
        )

        lines = audit.lines()
        joined = "\n".join(lines)

        assert "Paper Soak Audit" in joined
        assert "Signals scanned:" in joined
        assert "Candidates accepted:" in joined
        assert "Candidates rejected:" in joined
        assert "Paper trades entered:" in joined
        assert "Eval session scope:        fresh" in joined
        assert "Skipped trades:" in joined
        assert "Guardrail diagnostics:" in joined
        assert "Circuit breaker (paper):" in joined
        assert "Health status:" in joined
        assert "Live readiness:" in joined
        assert "does not affect paper mode" in joined
        assert "Source failures:" in joined
        assert "Risk rejections:" in joined
        assert "honeypot_check_failed: 1" in joined
        assert "Portfolio/capacity blocks:" in joined
        assert "Missing/unknown data blocks:" in joined
        assert "Execution/adapter failures:" in joined

        assert audit.cycle.signals_collected == 2
        assert audit.cycle.signals_accepted == 1
        assert audit.cycle.signals_rejected == 1
        assert audit.cycle.trades_persisted == 1
        assert audit.health_ok is True
        assert "paper_mode_unaffected" in audit.guardrail_diagnostics
        assert "paper_mode_unaffected" in audit.circuit_breaker_diagnostics

    asyncio.run(run())


def test_paper_soak_circuit_breaker_paper_mode_unaffected(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "paper-soak-breaker.db"
        sources = [FakeSignalSource([[build_signal("breaker-mint", passes=True)]])]

        audit = await cli_module.run_paper_soak(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=sources,
        )

        assert "paper_mode_unaffected" in audit.circuit_breaker_diagnostics
        assert audit.cycle.trades_persisted == 1
        assert audit.cycle.execution_mode == "paper"

    asyncio.run(run())


def test_paper_soak_cli_prints_audit_sections(tmp_path: Path, monkeypatch) -> None:
    signal = build_signal("soak-cli-mint", passes=True)
    monkeypatch.setattr(cli_module, "build_signal_sources", lambda: [FakeSignalSource([[signal]])])

    result = runner.invoke(
        cli_module.app,
        [
            "paper-soak",
            "--max-signals",
            "1",
            "--timeout-seconds",
            "0.1",
            "--db-path",
            str(tmp_path / "soak-cli.db"),
        ],
    )

    assert result.exit_code == 0
    assert "Paper Soak Audit" in result.stdout
    assert "Signals scanned:" in result.stdout
    assert "Health status:" in result.stdout
    assert "Live readiness:" in result.stdout
    assert "Guardrail diagnostics:" in result.stdout
    assert "Circuit breaker (paper):" in result.stdout
    assert "paper_mode_unaffected" in result.stdout


def test_paper_soak_capacity_audit_details(tmp_path: Path) -> None:
    """Capacity blocks show configured max, current open, and blocked count."""
    async def run() -> None:
        db_path = tmp_path / "capacity-audit.db"
        constrained = cli_module.load_settings().model_copy(
            update={
                "position": cli_module.load_settings().position.model_copy(
                    update={"max_open_positions": 1}
                )
            }
        )
        first = await cli_module.run_bounded_paper_cycle(
            max_signals=1, timeout_seconds=0.1, db_path=db_path,
            settings=constrained,
            sources=[FakeSignalSource([[build_signal("seed", passes=True)]])],
            poll_interval_s=0.0,
        )
        blocked = await cli_module.run_bounded_paper_cycle(
            max_signals=1, timeout_seconds=0.1, db_path=db_path,
            settings=constrained,
            sources=[FakeSignalSource([[build_signal("blocked", passes=True)]])],
            poll_interval_s=0.0,
        )
        audit = cli_module.PaperSoakAudit(
            cycle=blocked,
            health_ok=True, health_message="ok",
            guardrail_diagnostics=("paper_mode_unaffected",),
            circuit_breaker_diagnostics=("paper_mode_unaffected",),
            readiness_checks=(),
        )
        lines = "\n".join(audit.lines())
        assert "Portfolio/capacity blocks:" in lines
        assert "max_open_positions_reached: 1" in lines
        assert "configured_max_open_positions=1" in lines
        assert "starting_open_positions=1" in lines
        assert "persisted_open_positions=1" in lines

    asyncio.run(run())


def test_paper_soak_persist_positions_flag(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "persist-positions.db"
        sources = [
            FakeSignalSource(
                [
                    [
                        build_signal("persist-1", passes=True),
                        build_signal("persist-2", passes=True),
                    ]
                ]
            )
        ]

        audit = await cli_module.run_paper_soak(
            max_signals=2,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=sources,
            persist_positions=True,
        )

        assert audit.cycle.evaluation_session_scope == "persisted"
        assert audit.cycle.trades_persisted == 2
        lines = "\n".join(audit.lines())
        assert "Eval session scope:        persisted" in lines

        position_manager = cli_module.PositionManager(db_path, cli_module.load_settings())
        open_positions = await position_manager.get_all_open()
        assert len(open_positions) == 2

    asyncio.run(run())


def test_paper_soak_default_is_fresh_session(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "default-fresh.db"
        sources = [
            FakeSignalSource(
                [
                    [
                        build_signal("fresh-mint", passes=True),
                    ]
                ]
            )
        ]

        audit = await cli_module.run_paper_soak(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=sources,
        )

        assert audit.cycle.evaluation_session_scope == "fresh"

        position_manager = cli_module.PositionManager(db_path, cli_module.load_settings())
        open_positions = await position_manager.get_all_open()
        assert len(open_positions) == 0

    asyncio.run(run())


def _seed_decision(db_path: Path, outcome: str, reason: str, source: str = "whale_tracker", mode: str = "unknown") -> None:
    async def run() -> None:
        await record_paper_decision(
            db_path,
            PaperDecisionRecord(
                mint_address="fake-mint",
                source=source,
                candidate_mode=mode,
                action_outcome=outcome,
                decision=outcome,
                primary_reason=reason,
            ),
        )
    asyncio.run(run())


def test_paper_decisions_empty_db_shows_no_data_warning(tmp_path: Path) -> None:
    db = tmp_path / "decisions-empty.db"
    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "No paper decision telemetry found" in result.stdout
    assert "Run `paper-soak` to generate" in result.stdout


def test_paper_decisions_summary_counts(tmp_path: Path) -> None:
    db = tmp_path / "decisions-counts.db"
    asyncio.run(init_db(db))
    _seed_decision(db, "rejected", "top10_holder_check_failed")
    _seed_decision(db, "rejected", "top10_holder_check_failed")
    _seed_decision(db, "rejected", "creator_holding_check_unknown")
    _seed_decision(db, "traded", "traded", source="onchain", mode="launch")
    _seed_decision(db, "capacity_blocked", "max_open_positions_reached", source="composite", mode="migration")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Summary (5 decisions)" in result.stdout
    assert "By outcome:" in result.stdout
    assert "rejected: 3" in result.stdout
    assert "traded: 1" in result.stdout
    assert "capacity_blocked: 1" in result.stdout
    assert "By rejection reason:" in result.stdout
    assert "top10_holder_check_failed: 2" in result.stdout
    assert "creator_holding_check_unknown: 1" in result.stdout
    assert "By signal source:" in result.stdout
    assert "whale_tracker: 3" in result.stdout
    assert "onchain: 1" in result.stdout
    assert "composite: 1" in result.stdout
    assert "By candidate mode:" in result.stdout
    assert "unknown: 3" in result.stdout
    assert "launch: 1" in result.stdout
    assert "migration: 1" in result.stdout
    assert "Accepted candidates (1)" in result.stdout
    assert "Recent rejected candidates (4)" in result.stdout


def test_paper_decisions_displays_persisted_discovery_edge_diagnostics(tmp_path: Path) -> None:
    db = tmp_path / "decisions-edge-display.db"
    asyncio.run(init_db(db))
    diagnostics = {
        "edge_score": 73,
        "edge_breakdown": "src=2/comp=0.84 mode=launch attn=79/present weak=-0 warn=-3 approval=discovery_relaxed",
        "attention_quality": "strong",
        "attention_detail": "mode=launch src=composite/2 social=present attn=79/active weak=-0 approval=discovery_relaxed warn=1",
    }
    asyncio.run(
        record_paper_decision(
            db,
            PaperDecisionRecord(
                mint_address="edge-mint",
                symbol="EDGE",
                source="composite",
                candidate_mode="launch",
                action_outcome="rejected",
                decision="rejected",
                primary_reason="top10_holder_check_failed",
                diagnostics_json=json.dumps(diagnostics),
            ),
        )
    )

    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db)])

    assert result.exit_code == 0
    assert "paper-only operator/research" in result.stdout
    assert "diagnostics; they do not affect strict risk" in result.stdout
    assert "edge=73" in result.stdout
    assert "detail=src=2/comp=0.84 mode=launch" in result.stdout
    assert "approval=discovery_relaxed" in result.stdout
    assert "attention=strong" in result.stdout
    assert "detail=mode=launch src=composite/2" in result.stdout


def test_paper_decisions_edge_fallback_preserves_strict_record_and_never_calls_live_buy(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db = tmp_path / "decisions-edge-fallback.db"
    asyncio.run(init_db(db))
    record = PaperDecisionRecord(
        mint_address="strict-mint",
        source="pump_fun",
        candidate_mode="launch",
        action_outcome="rejected",
        decision="rejected",
        primary_reason="honeypot_check_unknown",
        risk_profile="strict",
    )
    asyncio.run(record_paper_decision(db, record))
    live_buy_calls: list[object] = []

    async def unexpected_live_buy(*args, **kwargs):
        live_buy_calls.append((args, kwargs))
        raise AssertionError("paper-decisions must not execute live buys")

    monkeypatch.setattr(cli_module, "execute_guarded_live_buy", unexpected_live_buy)

    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db)])
    stored = asyncio.run(get_recent_paper_decisions(db, limit=1))[0]

    assert result.exit_code == 0
    assert "edge=not-recorded" in result.stdout
    assert "attention=not-recorded" in result.stdout
    assert stored.risk_profile == "strict"
    assert stored.action_outcome == "rejected"
    assert stored.primary_reason == "honeypot_check_unknown"
    assert live_buy_calls == []


def test_attention_quality_diagnostics_are_deterministic_and_persisted_paper_only() -> None:
    diagnostic = {
        "source": "composite",
        "source_count": 2,
        "candidate_mode": "launch",
        "social_signal_state": "present",
        "attention_score": 79,
        "attention_tier": "active",
        "ranking_penalty_points": 4,
        "risk_approval_state": "discovery_relaxed",
        "main_warnings": ("limited holder evidence",),
    }

    enriched = cli_module._apply_paper_attention_diagnostics([dict(diagnostic)], [])[0][0]
    repeat = cli_module._apply_paper_attention_diagnostics([dict(diagnostic)], [])[0][0]
    record = cli_module._paper_decision_record(
        enriched,
        cycle_id="paper-cycle",
        execution_mode="paper",
        risk_profile="discovery",
    )
    persisted = json.loads(record.diagnostics_json)

    assert enriched["attention_quality"] == "strong"
    assert enriched["attention_detail"] == (
        "mode=launch src=composite/2 social=present attn=79/active weak=-4 "
        "approval=discovery_relaxed warn=1"
    )
    assert repeat["attention_quality"] == enriched["attention_quality"]
    assert repeat["attention_detail"] == enriched["attention_detail"]
    assert persisted["attention_quality"] == enriched["attention_quality"]
    assert persisted["attention_detail"] == enriched["attention_detail"]


def test_paper_decisions_outcome_filter(tmp_path: Path) -> None:
    db = tmp_path / "decisions-filter.db"
    asyncio.run(init_db(db))
    _seed_decision(db, "traded", "traded")
    _seed_decision(db, "rejected", "top10_holder_check_failed")
    _seed_decision(db, "rejected", "creator_holding_check_unknown")
    _seed_decision(db, "capacity_blocked", "max_open_positions_reached")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--outcome", "rejected", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Summary (2 decisions)" in result.stdout
    assert "rejected: 2" in result.stdout
    assert "Accepted candidates" not in result.stdout


def test_paper_decisions_mode_filter(tmp_path: Path) -> None:
    db = tmp_path / "decisions-mode-filter.db"
    asyncio.run(init_db(db))
    _seed_decision(db, "rejected", "honeypot_check_failed", mode="launch")
    _seed_decision(db, "rejected", "top10_holder_check_failed", mode="unknown")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--mode", "launch", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Summary (1 decisions)" in result.stdout
    assert "honeypot_check_failed" in result.stdout


def test_paper_decisions_source_filter(tmp_path: Path) -> None:
    db = tmp_path / "decisions-source-filter.db"
    asyncio.run(init_db(db))
    _seed_decision(db, "rejected", "honeypot_check_failed", source="pump_fun")
    _seed_decision(db, "rejected", "top10_holder_check_failed", source="whale_tracker")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--source", "pump_fun", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Summary (1 decisions)" in result.stdout
    assert "pump_fun: 1" in result.stdout


def test_paper_decisions_limit(tmp_path: Path) -> None:
    db = tmp_path / "decisions-limit.db"
    asyncio.run(init_db(db))
    for _ in range(5):
        _seed_decision(db, "rejected", "top10_holder_check_failed")
    _seed_decision(db, "traded", "traded")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--limit", "3", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Summary (3 decisions)" in result.stdout


def test_paper_decisions_no_secrets_printed(tmp_path: Path) -> None:
    db = tmp_path / "decisions-secrets.db"
    asyncio.run(init_db(db))
    _seed_decision(db, "rejected", "top10_holder_check_failed")
    _seed_decision(db, "traded", "traded")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db)])
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "private_key" not in output
    assert "api-key=" not in output
    assert "rpc_url=" not in output


def test_paper_decisions_export_md_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "decisions-export-md.db"
    export_path = tmp_path / "export.md"
    asyncio.run(init_db(db))
    _seed_decision(db, "rejected", "top10_holder_check_failed")
    _seed_decision(db, "traded", "traded")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db), "--export-md", str(export_path)])
    assert result.exit_code == 0
    assert export_path.exists()
    content = export_path.read_text()
    assert "# Paper Decision Telemetry" in content
    assert "Summary (2 decisions)" in content
    assert "top10_holder_check_failed" in content
    assert "WARNING: Paper results are simulated" in content
    assert "private_key" not in content.lower()


def test_paper_decisions_export_json_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "decisions-export-json.db"
    export_path = tmp_path / "export.json"
    asyncio.run(init_db(db))
    _seed_decision(db, "rejected", "top10_holder_check_failed")
    _seed_decision(db, "traded", "traded")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db), "--export-json", str(export_path)])
    assert result.exit_code == 0
    assert export_path.exists()
    import json
    data = json.loads(export_path.read_text())
    assert data["total_decisions"] == 2
    assert data["mode"] == "paper"
    assert "top10_holder_check_failed" in str(data["summary"]["by_reason"])
    assert len(data["rejected_candidates"]) == 1
    assert len(data["accepted_candidates"]) == 1
    assert "private_key" not in json.dumps(data).lower()


def test_paper_decisions_export_empty_db(tmp_path: Path) -> None:
    db = tmp_path / "decisions-empty-export.db"
    export_path = tmp_path / "empty.md"
    asyncio.run(init_db(db))

    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db), "--export-md", str(export_path)])
    assert result.exit_code == 0
    assert export_path.exists()
    content = export_path.read_text()
    assert "No paper decision telemetry found" in content


def test_paper_decisions_export_does_not_mutate_db(tmp_path: Path) -> None:
    db = tmp_path / "decisions-no-mutate.db"
    export_path = tmp_path / "no-mutate.md"
    asyncio.run(init_db(db))
    _seed_decision(db, "rejected", "top10_holder_check_failed")

    result = runner.invoke(cli_module.app, ["paper-decisions", "--db-path", str(db), "--export-md", str(export_path)])
    assert result.exit_code == 0
    assert export_path.exists()
    decisions = asyncio.run(get_recent_paper_decisions(db, limit=10))
    assert len(decisions) == 1
    assert decisions[0].action_outcome == "rejected"


def test_snapshot_dry_run_rechecks_only_covered_rows_without_execution_or_state_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db = tmp_path / "snapshot-dry-run.db"
    asyncio.run(init_db(db))
    all_pass = {check_name: "pass" for check_name in cli_module._SNAPSHOT_CHECK_ORDER}
    replayable_pass = {
        "mint": "snapshot-pass-mint",
        "source": "manual",
        "rejection_reason": "prior_rejection",
        "check_results": all_pass,
    }
    replayable_reject = {
        "mint": "snapshot-reject-mint",
        "source": "manual",
        "rejection_reason": "top10_holder_check_failed",
        "failed_check": "top10_holder_check",
    }
    for mint, diagnostics in (
        ("snapshot-pass-mint", {"recheck_snapshot": replayable_pass, "attention_score": 91}),
        ("snapshot-reject-mint", {"recheck_snapshot": replayable_reject, "attention_score": 12}),
        ("legacy-mint", {}),
    ):
        asyncio.run(
            record_paper_decision(
                db,
                PaperDecisionRecord(
                    mint_address=mint,
                    source="manual",
                    action_outcome="rejected",
                    decision="rejected",
                    primary_reason="top10_holder_check_failed",
                    risk_score=42.0,
                    diagnostics_json=json.dumps(diagnostics, sort_keys=True),
                ),
            )
        )

    async def unexpected_execution(*args, **kwargs):
        raise AssertionError("snapshot dry-run must not execute a swap")

    async def unexpected_live_readiness(*args, **kwargs):
        raise AssertionError("snapshot dry-run must not evaluate live readiness")

    monkeypatch.setattr(cli_module.PaperExecutionAdapter, "execute_swap", unexpected_execution)
    monkeypatch.setattr(cli_module, "execute_guarded_live_buy", unexpected_execution)
    monkeypatch.setattr(cli_module, "evaluate_micro_live_readiness", unexpected_live_readiness)
    before = asyncio.run(get_recent_paper_decisions(db, limit=10))

    result = runner.invoke(cli_module.app, ["paper-recheck-snapshot-dry-run", "--db-path", str(db)])

    after = asyncio.run(get_recent_paper_decisions(db, limit=10))
    assert result.exit_code == 0
    assert "Replayable snapshots: 2" in result.stdout
    assert "Skipped missing snapshots: 1" in result.stdout
    assert "Dry-run pass: 1" in result.stdout
    assert "Dry-run reject: 1" in result.stdout
    assert "top10_holder_check" in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]
    assert load_recent_trades(db, limit=10) == []


def test_raw_safe_snapshot_captures_normalized_check_results_only() -> None:
    snapshot = cli_module._raw_safe_recheck_snapshot(
        {
            "action_outcome": "rejected",
            "mint": "safe-snapshot-mint",
            "source": "manual",
            "rejection_reason": "top10_holder_check_failed",
            "risk_check_results": {
                "top10_holder_check": {"result": "FAIL", "value": 99.0, "threshold": 50.0},
                "liquidity_check": {"result": "PASS", "value": 100.0, "threshold": 10.0},
                "unrelated": {"result": "PASS", "secret": "not copied"},
            },
        }
    )

    assert snapshot is not None
    assert snapshot["check_results"] == {
        "liquidity_check": "pass",
        "top10_holder_check": "fail",
    }
    assert "secret" not in json.dumps(snapshot)


def test_snapshot_risk_source_analysis_groups_rejections_without_mutating_state(tmp_path: Path) -> None:
    db = tmp_path / "snapshot-risk-sources.db"
    asyncio.run(init_db(db))
    snapshots = (
        {
            "mint": "holder-one",
            "source": "pump_fun",
            "candidate_mode": "launch",
            "rejection_reason": "top10_holder_check_failed",
            "failed_check": "top10_holder_check",
            "top10_holder_pct": 90.0,
            "top10_holder_threshold_pct": 50.0,
            "confidence": 0.8,
            "effective_score": 0.8,
            "metadata_completeness_state": "partial",
        },
        {
            "mint": "holder-two",
            "source": "pump_fun",
            "candidate_mode": "launch",
            "rejection_reason": "top10_holder_check_failed",
            "failed_check": "top10_holder_check",
            "top10_holder_pct": 99.0,
            "top10_holder_threshold_pct": 50.0,
        },
        {
            "mint": "creator-unknown",
            "source": "onchain",
            "candidate_mode": "migration",
            "rejection_reason": "creator_holding_check_unknown",
            "failed_check": "creator_holding_check_unknown",
        },
    )
    for index, snapshot in enumerate(snapshots):
        asyncio.run(
            record_paper_decision(
                db,
                PaperDecisionRecord(
                    mint_address=f"snapshot-{index}",
                    source="manual",
                    action_outcome="rejected",
                    decision="rejected",
                    primary_reason="rejected",
                    diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
                ),
            )
        )

    before = asyncio.run(get_recent_paper_decisions(db, limit=10))
    result = runner.invoke(cli_module.app, ["paper-recheck-snapshot-risk-sources", "--db-path", str(db)])
    after = asyncio.run(get_recent_paper_decisions(db, limit=10))

    assert result.exit_code == 0
    assert "Replayable rejected snapshots: 3" in result.stdout
    assert "source=pump_fun mode=launch blocker=top10_holder_check" in result.stdout
    assert "holder=severe_fail count=2 repeated" in result.stdout
    assert "source=onchain mode=migration blocker=creator_holding_check_unknown" in result.stdout
    assert "holder=unknown count=1" in result.stdout
    assert "Top10 holder failure clusters:" in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]


def test_snapshot_source_quality_summarizes_rejections_without_trading_state_changes(tmp_path: Path) -> None:
    db = tmp_path / "snapshot-source-quality.db"
    asyncio.run(init_db(db))
    for mint, source, mode, blocker, holder_pct in (
        ("whale-holder", "whale_tracker", "unknown", "top10_holder_check", 95.0),
        ("whale-creator", "whale_tracker", "unknown", "creator_holding_check_unknown", None),
        ("pump-holder", "pump_fun", "launch", "top10_holder_check", 55.0),
    ):
        snapshot = {
            "mint": mint,
            "source": source,
            "candidate_mode": mode,
            "rejection_reason": blocker,
            "failed_check": blocker,
            "top10_holder_pct": holder_pct,
            "top10_holder_threshold_pct": 50.0,
        }
        asyncio.run(
            record_paper_decision(
                db,
                PaperDecisionRecord(
                    mint_address=mint,
                    source="manual",
                    action_outcome="rejected",
                    decision="rejected",
                    primary_reason="rejected",
                    diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
                ),
            )
        )

    before = asyncio.run(get_recent_paper_decisions(db, limit=10))
    result = runner.invoke(cli_module.app, ["paper-recheck-snapshot-source-quality", "--db-path", str(db)])
    after = asyncio.run(get_recent_paper_decisions(db, limit=10))

    assert result.exit_code == 0
    assert "DIAGNOSTIC-ONLY" in result.stdout
    assert "source=whale_tracker mode=unknown rejected=2 unknown_data=1" in result.stdout
    assert "source=pump_fun mode=launch rejected=1 unknown_data=0" in result.stdout
    assert "holder_severity={'severe_fail': 1}" in result.stdout
    assert "holder_severity={'near_threshold': 1}" in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]


def test_source_quality_bucket_is_deterministic_and_diagnostic_only() -> None:
    assert cli_module._source_quality_bucket(0, severe_holders=0, unknown_data=0, near_threshold_holders=0) == "insufficient_data"
    assert cli_module._source_quality_bucket(1, severe_holders=1, unknown_data=0, near_threshold_holders=0) == "sparse_sample"
    assert cli_module._source_quality_bucket(10, severe_holders=5, unknown_data=0, near_threshold_holders=0) == "severe_holder_heavy"
    assert cli_module._source_quality_bucket(10, severe_holders=0, unknown_data=5, near_threshold_holders=0) == "unknown_data_heavy"
    assert cli_module._source_quality_bucket(10, severe_holders=0, unknown_data=0, near_threshold_holders=5) == "near_threshold_heavy"
    assert cli_module._source_quality_bucket(10, severe_holders=1, unknown_data=1, near_threshold_holders=1) == "mixed_risk"
    assert cli_module._source_investigation_flag("severe_holder_heavy") == "severe_risk_source"
    assert cli_module._source_investigation_flag("unknown_data_heavy") == "provider_data_gap"
    assert cli_module._source_investigation_flag("near_threshold_heavy") == "near_threshold_review"
    assert cli_module._source_investigation_flag("mixed_risk") == "investigate_source"


def test_snapshot_source_buckets_are_read_only(tmp_path: Path) -> None:
    db = tmp_path / "snapshot-source-buckets.db"
    asyncio.run(init_db(db))
    for mint, source, mode, blocker, holder_pct in (
        ("whale-one", "whale_tracker", "unknown", "top10_holder_check", 95.0),
        ("whale-two", "whale_tracker", "unknown", "top10_holder_check", 95.0),
        ("whale-three", "whale_tracker", "unknown", "top10_holder_check", 95.0),
        ("pump-one", "pump_fun", "launch", "creator_holding_check_unknown", None),
        ("pump-two", "pump_fun", "launch", "creator_holding_check_unknown", None),
        ("pump-three", "pump_fun", "launch", "creator_holding_check_unknown", None),
    ):
        snapshot = {
            "mint": mint,
            "source": source,
            "candidate_mode": mode,
            "rejection_reason": blocker,
            "failed_check": blocker,
            "top10_holder_pct": holder_pct,
            "top10_holder_threshold_pct": 50.0,
        }
        asyncio.run(
            record_paper_decision(
                db,
                PaperDecisionRecord(
                    mint_address=mint,
                    source="manual",
                    action_outcome="rejected",
                    decision="rejected",
                    primary_reason="rejected",
                    diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
                ),
            )
        )

    before = asyncio.run(get_recent_paper_decisions(db, limit=10))
    result = runner.invoke(cli_module.app, ["paper-recheck-snapshot-source-buckets", "--db-path", str(db)])
    after = asyncio.run(get_recent_paper_decisions(db, limit=10))

    assert result.exit_code == 0
    assert "DIAGNOSTIC-ONLY" in result.stdout
    assert "source=whale_tracker mode=unknown bucket=severe_holder_heavy rejected=3" in result.stdout
    assert "source=pump_fun mode=launch bucket=unknown_data_heavy rejected=3" in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]


def test_weak_source_preview_is_read_only_and_not_a_ranking_signal(tmp_path: Path) -> None:
    db = tmp_path / "weak-source-preview.db"
    asyncio.run(init_db(db))
    for mint, source, blocker, holder_pct in (
        ("whale-one", "whale_tracker", "top10_holder_check", 95.0),
        ("whale-two", "whale_tracker", "top10_holder_check", 95.0),
        ("whale-three", "whale_tracker", "top10_holder_check", 95.0),
        ("pump-one", "pump_fun", "creator_holding_check_unknown", None),
        ("pump-two", "pump_fun", "creator_holding_check_unknown", None),
        ("pump-three", "pump_fun", "creator_holding_check_unknown", None),
    ):
        snapshot = {
            "mint": mint,
            "source": source,
            "candidate_mode": "launch",
            "rejection_reason": blocker,
            "failed_check": blocker,
            "top10_holder_pct": holder_pct,
            "top10_holder_threshold_pct": 50.0,
        }
        asyncio.run(record_paper_decision(db, PaperDecisionRecord(
            mint_address=mint,
            source="manual",
            action_outcome="rejected",
            decision="rejected",
            primary_reason="rejected",
            diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
        )))

    before = asyncio.run(get_recent_paper_decisions(db, limit=10))
    result = runner.invoke(cli_module.app, ["paper-recheck-snapshot-weak-source-preview", "--db-path", str(db)])
    after = asyncio.run(get_recent_paper_decisions(db, limit=10))

    assert result.exit_code == 0
    assert "DIAGNOSTIC-ONLY PREVIEW" in result.stdout
    assert "source=whale_tracker mode=launch preview=severe_risk_source" in result.stdout
    assert "source=pump_fun mode=launch preview=provider_data_gap" in result.stdout
    assert "not ranking" in result.stdout
    assert "trading signals" in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]


def test_rejected_outcomes_preserve_missing_baseline_and_unavailable_marks(tmp_path: Path) -> None:
    db = tmp_path / "rejected-outcomes.db"
    asyncio.run(init_db(db))
    for mint, baseline in (("missing-baseline", None), ("unavailable-mark", 0.00001), ("marked", 0.00001)):
        snapshot = {
            "mint": mint,
            "source": "pump_fun",
            "candidate_mode": "launch",
            "rejection_reason": "top10_holder_check_failed",
            "failed_check": "top10_holder_check",
            "rejection_mark_price_sol": baseline,
            "rejection_liquidity_sol": 25.0,
        }
        asyncio.run(record_paper_decision(db, PaperDecisionRecord(
            mint_address=mint,
            source="manual",
            action_outcome="rejected",
            decision="rejected",
            primary_reason="rejected",
            diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
        )))

    records = asyncio.run(get_recent_paper_decisions(db, limit=10))
    outcomes, skipped = asyncio.run(cli_module.collect_rejected_candidate_outcomes(
        records,
        FakePriceProvider({"missing-baseline": 0.00002, "marked": 0.00002}),
        limit=3,
    ))
    by_mint = {outcome.mint: outcome for outcome in outcomes}

    assert len(outcomes) == 3
    assert skipped == 0
    assert by_mint["marked"].return_multiple == 2.0
    assert by_mint["missing-baseline"].current_mark_sol == 0.00002
    assert by_mint["missing-baseline"].rejection_mark_sol is None
    assert by_mint["missing-baseline"].return_multiple is None

    unavailable, _ = asyncio.run(cli_module.collect_rejected_candidate_outcomes(
        records,
        UnavailablePriceProvider(),
        limit=3,
    ))
    assert all(outcome.current_mark_sol is None for outcome in unavailable)
    assert all(outcome.return_multiple is None for outcome in unavailable)


def test_rejected_snapshot_persists_source_baseline_or_explicit_missing_reason(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "rejection-baseline.db"
        marked = build_signal("baseline-marked", passes=False)
        marked.payload["priceNative"] = "0.000012"
        missing = build_signal("baseline-missing", passes=False)
        await cli_module.run_bounded_paper_cycle(
            max_signals=2,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[FakeSignalSource([[marked, missing]])],
            poll_interval_s=0.0,
        )
        records = await get_recent_paper_decisions(db_path, limit=10)
        snapshots = {
            record.mint_address: json.loads(record.diagnostics_json)["recheck_snapshot"]
            for record in records
        }
        assert snapshots["baseline-marked"]["rejection_mark_price_sol"] == 0.000012
        assert snapshots["baseline-marked"]["rejection_mark_provider"] == "signal_payload"
        assert snapshots["baseline-marked"]["rejection_mark_missing_reason"] is None
        assert datetime.fromisoformat(snapshots["baseline-marked"]["rejection_mark_timestamp"])
        assert snapshots["baseline-missing"]["rejection_mark_price_sol"] is None
        assert snapshots["baseline-missing"]["rejection_mark_provider"] == "unavailable"
        assert snapshots["baseline-missing"]["rejection_mark_missing_reason"] == "source_price_missing"

    asyncio.run(run())


def test_baseline_field_coverage_records_field_names_without_raw_values(tmp_path: Path) -> None:
    coverage = cli_module._baseline_payload_field_coverage(
        {
            "priceNative": "0.000012",
            "initialSolAmount": 3.0,
            "provider": "pumpportal",
        },
        {"liquidity_usd": 12_000.0},
    )
    assert coverage == {
        "price_fields": ("payload.priceNative",),
        "liquidity_fields": ("metrics.liquidity_usd", "payload.initialSolAmount"),
        "provider": "pumpportal",
    }

    db = tmp_path / "baseline-coverage.db"
    asyncio.run(init_db(db))
    snapshot = {
        "mint": "coverage-mint",
        "source": "onchain",
        "rejection_reason": "creator_holding_check_unknown",
        "rejection_mark_price_sol": None,
        "rejection_mark_missing_reason": "source_price_missing",
        "rejection_baseline_payload_provider": "dexscreener",
        "rejection_baseline_price_fields": [],
        "rejection_baseline_liquidity_fields": ["metrics.liquidity_usd"],
    }
    asyncio.run(record_paper_decision(db, PaperDecisionRecord(
        mint_address="coverage-mint",
        source="manual",
        action_outcome="rejected",
        decision="rejected",
        primary_reason="rejected",
        diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
    )))
    before = asyncio.run(get_recent_paper_decisions(db, limit=10))
    result = runner.invoke(cli_module.app, ["paper-rejected-baseline-coverage", "--db-path", str(db)])
    after = asyncio.run(get_recent_paper_decisions(db, limit=10))
    assert result.exit_code == 0
    assert "baseline_missing" in result.stdout
    assert "missing_reason:source_price_missing" in result.stdout
    assert "payload_provider:dexscreener" in result.stdout
    assert "liquidity_field:metrics.liquidity_usd" in result.stdout
    assert "12000" not in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]


def test_rejected_outcomes_cli_is_bounded_and_read_only(tmp_path: Path) -> None:
    db = tmp_path / "rejected-outcomes-cli.db"
    asyncio.run(init_db(db))
    for index in range(3):
        snapshot = {
            "mint": f"outcome-{index}",
            "source": "pump_fun",
            "candidate_mode": "launch",
            "rejection_reason": "creator_holding_check_unknown",
            "failed_check": "creator_holding_check_unknown",
        }
        asyncio.run(record_paper_decision(db, PaperDecisionRecord(
            mint_address=f"outcome-{index}",
            source="manual",
            action_outcome="rejected",
            decision="rejected",
            primary_reason="rejected",
            diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
        )))

    before = asyncio.run(get_recent_paper_decisions(db, limit=10))
    result = runner.invoke(cli_module.app, ["paper-rejected-outcomes", "--limit", "2", "--db-path", str(db)])
    after = asyncio.run(get_recent_paper_decisions(db, limit=10))

    assert result.exit_code == 0
    assert "Rejected snapshots tracked: 2" in result.stdout
    assert "Missing baseline marks: 2" in result.stdout
    assert "Unvailable current marks" not in result.stdout
    assert "Unavailable current marks: 2" in result.stdout
    assert "missing_baseline" in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]


def test_rejected_outcome_labels_require_marks_and_keep_source_reason_context(tmp_path: Path) -> None:
    def outcome(
        *,
        blocker: str = "top10_holder_check",
        multiple: float | None = 1.0,
        liquidity_usd: float | None = None,
    ) -> cli_module.RejectedCandidateOutcome:
        return cli_module.RejectedCandidateOutcome(
            mint="label-mint",
            source="pump_fun",
            mode="launch",
            blocker=blocker,
            age_hours=1.0,
            rejection_mark_sol=0.00001 if multiple is not None else None,
            current_mark_sol=0.00001 * multiple if multiple is not None else None,
            mark_reason="live_dexscreener",
            return_multiple=multiple,
            liquidity_sol=None,
            current_liquidity_usd=liquidity_usd,
        )

    assert cli_module._rejected_outcome_label(outcome(multiple=None)) == "inconclusive"
    assert cli_module._rejected_outcome_label(outcome(multiple=0.4)) == "good_block_dumped"
    assert cli_module._rejected_outcome_label(outcome(multiple=1.0, liquidity_usd=0)) == "good_block_liquidity_failed"
    assert cli_module._rejected_outcome_label(outcome(multiple=2.1)) == "possible_too_strict_pumped"
    assert cli_module._rejected_outcome_label(outcome(blocker="creator_holding_unknown", multiple=2.1)) == "data_issue_possible"

    db = tmp_path / "rejected-outcome-labels.db"
    asyncio.run(init_db(db))
    snapshot = {
        "mint": "inconclusive-mint",
        "source": "pump_fun",
        "candidate_mode": "launch",
        "rejection_reason": "creator_holding_check_unknown",
        "failed_check": "creator_holding_check_unknown",
    }
    asyncio.run(record_paper_decision(db, PaperDecisionRecord(
        mint_address="inconclusive-mint",
        source="manual",
        action_outcome="rejected",
        decision="rejected",
        primary_reason="rejected",
        diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
    )))
    before = asyncio.run(get_recent_paper_decisions(db, limit=10))
    result = runner.invoke(cli_module.app, ["paper-rejected-outcome-labels", "--db-path", str(db)])
    after = asyncio.run(get_recent_paper_decisions(db, limit=10))

    assert result.exit_code == 0
    assert "Labels: {'inconclusive': 1}" in result.stdout
    assert "label=inconclusive blocker=creator_holding_check_unknown" in result.stdout
    assert "source=pump_fun" in result.stdout
    assert "count=1" in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]


def test_later_rejection_marks_only_fetch_baseline_covered_rows_and_stay_read_only(tmp_path: Path) -> None:
    class CountingPriceProvider(FakePriceProvider):
        def __init__(self) -> None:
            super().__init__({"covered": 0.00002})
            self.calls: list[str] = []

        async def get_current_price(self, mint_address: str) -> float | None:
            self.calls.append(mint_address)
            return await super().get_current_price(mint_address)

    db = tmp_path / "later-rejection-marks.db"
    asyncio.run(init_db(db))
    for mint, baseline in (("covered", 0.00001), ("missing", None)):
        snapshot = {
            "mint": mint,
            "source": "pump_fun",
            "candidate_mode": "launch",
            "rejection_reason": "top10_holder_check_failed",
            "failed_check": "top10_holder_check",
            "rejection_mark_price_sol": baseline,
        }
        asyncio.run(record_paper_decision(db, PaperDecisionRecord(
            mint_address=mint,
            source="manual",
            action_outcome="rejected",
            decision="rejected",
            primary_reason="rejected",
            diagnostics_json=json.dumps({"recheck_snapshot": snapshot}, sort_keys=True),
        )))

    records = asyncio.run(get_recent_paper_decisions(db, limit=10))
    provider = CountingPriceProvider()
    outcomes, missing_snapshot, missing_baseline = asyncio.run(
        cli_module.collect_later_rejection_marks(records, provider, limit=25)
    )

    assert missing_snapshot == 0
    assert missing_baseline == 1
    assert provider.calls == ["covered"]
    assert len(outcomes) == 1
    assert outcomes[0].current_mark_sol == 0.00002
    assert outcomes[0].later_mark_provider == "fake"
    assert outcomes[0].later_mark_timestamp is not None
    assert cli_module._rejected_outcome_label(outcomes[0]) == "possible_too_strict_pumped"

    before = asyncio.run(get_recent_paper_decisions(db, limit=10))
    result = runner.invoke(
        cli_module.app,
        ["paper-rejected-later-marks", "--marks", "unavailable", "--db-path", str(db)],
    )
    after = asyncio.run(get_recent_paper_decisions(db, limit=10))
    assert result.exit_code == 0
    assert "Baseline-covered candidates: 1" in result.stdout
    assert "Skipped missing baseline: 1" in result.stdout
    assert "status=price_unavailable" in result.stdout
    assert [record.model_dump_json() for record in after] == [record.model_dump_json() for record in before]

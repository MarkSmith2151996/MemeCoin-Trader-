"""Typer CLI entrypoint."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import uuid4

import aiosqlite

import typer
from rich.console import Console

from src.core.config import Settings, load_settings
from src.core.database import get_recent_paper_decisions, get_recent_soak_runs, init_db, record_paper_decision, record_soak_run, record_trade
from src.core.models import PaperDecisionRecord, PaperFillQuality, Position, PositionStatus, SoakRunRecord
from src.execution.base import ExecutionAdapter
from src.execution.live_buy import execute_guarded_live_buy
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_execution_config import evaluate_live_execution_config
from src.execution.live_exit import execute_guarded_live_exit
from src.execution.live_guardrails import evaluate_live_guardrails
from src.execution.env_readiness import evaluate_env_readiness
from src.execution.helius_providers import (
    try_create_balance_lookup,
    try_create_holdings_lookup,
    try_create_transaction_simulator,
)
from src.execution.live_readiness import evaluate_micro_live_readiness
from src.execution.jupiter_live import JupiterLiveExecutionAdapter
from src.execution.paper import PaperExecutionAdapter
from src.execution.paper_pnl import PaperPnLCalculator, PaperPnLSummary
from src.execution.price_provider import DexScreenerPriceProvider, UnavailablePriceProvider
from src.monitoring.dashboard import resolve_db_path
from src.monitoring.health import check_health
from src.risk.rugcheck import RugCheckClient
from src.risk.scorer import DiscoveryRiskScorer
from src.signals.aggregator import SignalAggregator
from src.signals.base import SignalSource
from src.signals.onchain import OnChainMonitor
from src.signals.pump_fun import build_monitor_from_env
from src.signals.twitter import TwitterMonitor
from src.signals.whale_tracker import WhaleWalletTracker
from src.strategy.decision_engine import DecisionEngine, RejectionRecord
from src.strategy.position_manager import PositionManager

app = typer.Typer(help="Memecoin Trader CLI")
console = Console()
logger = logging.getLogger(__name__)
SUPPORTED_RISK_PROFILES = {"strict", "discovery"}
MT038_REPORT_PATH = Path(
    "/mnt/c/Users/Big A/custodian-shared/memecoin-trader/diagnostic-reports/mt038-per-token-rejection-diagnostics.txt"
)


@dataclass(slots=True)
class PaperCycleSummary:
    execution_mode: str
    risk_profile: str
    max_signals: int
    timeout_seconds: float
    signals_collected: int
    signals_accepted: int
    signals_rejected: int
    trades_persisted: int
    open_positions: int
    sources_polled: list[str]
    source_signal_counts: dict[str, int]
    source_failures: dict[str, int]
    composite_opportunities: int
    rejection_reasons: dict[str, int]
    candidates_evaluated: int
    passed_risk_checks: int
    summary_rejection_reasons: dict[str, int]
    source_evaluated_counts: dict[str, int]
    source_pass_counts: dict[str, int]
    holder_lookup_outcomes: dict[str, int]
    termination_reason: str
    elapsed_seconds: float
    candidate_mode_counts: dict[str, int] = field(default_factory=dict)
    accepted_candidate_diagnostics: list[dict[str, object]] = field(default_factory=list)
    rejected_candidate_diagnostics: list[dict[str, object]] = field(default_factory=list)
    diagnostic_report_path: str | None = None
    evaluation_session_scope: str = "persisted"
    starting_open_positions: int = 0
    persisted_open_positions: int = 0
    configured_max_open_positions: int = 0
    capacity_blocked_candidates: int = 0

    def safe_lines(self) -> list[str]:
        lines = [
            f"execution_mode={self.execution_mode}",
            f"risk_profile={self.risk_profile}",
            f"max_signals={self.max_signals}",
            f"timeout_seconds={self.timeout_seconds:g}",
            f"signals_collected={self.signals_collected}",
            f"signals_accepted={self.signals_accepted}",
            f"signals_rejected={self.signals_rejected}",
            f"trades_persisted={self.trades_persisted}",
            f"open_positions={self.open_positions}",
            f"evaluation_session_scope={self.evaluation_session_scope}",
            f"starting_open_positions={self.starting_open_positions}",
            f"persisted_open_positions={self.persisted_open_positions}",
            f"configured_max_open_positions={self.configured_max_open_positions}",
            f"capacity_blocked_candidates={self.capacity_blocked_candidates}",
            f"sources_polled={','.join(self.sources_polled)}",
            f"composite_opportunities={self.composite_opportunities}",
            f"termination_reason={self.termination_reason}",
            f"elapsed_seconds={self.elapsed_seconds:.3f}",
        ]
        if self.source_signal_counts:
            lines.append("source_signal_counts:")
            lines.extend(f"  {source}={count}" for source, count in self.source_signal_counts.items())
        if self.source_failures:
            lines.append("source_failures:")
            lines.extend(f"  {source}={count}" for source, count in self.source_failures.items())
        if self.rejection_reasons:
            lines.append("rejection_reasons:")
            lines.extend(f"  {reason}={count}" for reason, count in self.rejection_reasons.items())
        if self.holder_lookup_outcomes:
            lines.append("holder_lookup_outcomes:")
            lines.extend(f"  {reason}={count}" for reason, count in self.holder_lookup_outcomes.items())
        if self.candidate_mode_counts:
            lines.append("candidate_mode_counts:")
            lines.extend(f"  {mode}={count}" for mode, count in self.candidate_mode_counts.items())
        lines.extend(self.summary_table_lines())
        lines.extend(self.discovery_candidate_summary_lines())
        lines.extend(self.discovery_comparison_lines())
        lines.extend(self.discovery_grok_prompt_lines())
        lines.extend(self.rejection_diagnostic_lines())
        return lines

    def summary_table_lines(self) -> list[str]:
        lines = [
            "═══ Paper Cycle Summary ═══",
            f"Signals collected:     {self.signals_collected}",
            f"Candidates evaluated:  {self.candidates_evaluated}",
            f"Passed risk checks:    {self.passed_risk_checks}",
            f"Rejected:              {self.candidates_evaluated - self.passed_risk_checks}",
        ]
        for reason, count in self.summary_rejection_reasons.items():
            lines.append(f"  - {reason}: {count}")
        lines.append("By source:")
        for source, count in self.source_evaluated_counts.items():
            passed = self.source_pass_counts.get(source, 0)
            lines.append(f"  - {source}: {count} ({passed} passed)")
        lines.extend(
            [
                f"Paper trades executed: {self.trades_persisted}",
                "═══════════════════════════",
            ]
        )
        return lines

    def rejection_diagnostic_lines(self) -> list[str]:
        if not self.rejected_candidate_diagnostics:
            return []

        lines = ["Rejected candidate diagnostics:"]
        lines.append("  # | symbol | mint | source | failed_check | attn | holder_policy | age_policy | creator_policy | buyer_policy | authority_policy | honeypot_policy | holder_source | top10_holder_pct | creator | liquidity | attention_hints")
        for diagnostic in self.rejected_candidate_diagnostics:
            lines.append(
                "  {rank} | {symbol} | {mint_short} | {source} | {failed_check} | {attn} | {holder_policy} | {age_policy} | {creator_policy} | {buyer_policy} | {authority_policy} | {honeypot_policy} | {holder_source} | {top10_holder_pct} | {creator} | {liquidity} | {attention_hints}".format(
                    rank=diagnostic.get("rank", "?"),
                    symbol=diagnostic.get("symbol", "unknown"),
                    mint_short=diagnostic.get("mint_short", "unknown"),
                    source=diagnostic.get("source", "unknown"),
                    failed_check=diagnostic.get("failed_check", "unknown"),
                    attn=_candidate_attention_display(diagnostic),
                    holder_policy=diagnostic.get("holder_policy_state", "unknown"),
                    age_policy=diagnostic.get("age_policy_state", "unknown"),
                    creator_policy=diagnostic.get("creator_policy_state", "unknown"),
                    buyer_policy=diagnostic.get("unique_buyers_policy_state", "unknown"),
                    authority_policy=diagnostic.get("authority_policy_state", "unknown"),
                    honeypot_policy=diagnostic.get("honeypot_policy_state", "unknown"),
                    holder_source=diagnostic.get("top10_holder_source", "unknown"),
                    top10_holder_pct=diagnostic.get("top10_holder_pct", "unknown"),
                    creator=diagnostic.get("creator_holding_display", "unknown"),
                    liquidity=diagnostic.get("liquidity_display", "unknown"),
                    attention_hints=diagnostic.get("attention_hints", "none"),
                )
            )
        return lines

    def discovery_candidate_summary_lines(self) -> list[str]:
        if self.risk_profile != "discovery":
            return []

        candidates = sorted(
            [*self.accepted_candidate_diagnostics, *self.rejected_candidate_diagnostics],
            key=_candidate_sort_key,
        )
        if not candidates:
            return []

        lines = ["Top discovery candidates:"]
        lines.append("  # | symbol | mint | source | mode | attn | edge | approval | outcome | reason | theme | meta")
        for diagnostic in candidates[:8]:
            lines.append(
                "  {rank} | {symbol} | {mint_short} | {source} | {mode} | {attn} | {edge} | {approval} | {outcome} | {reason} | {theme} | {meta}".format(
                    rank=diagnostic.get("rank", "?"),
                    symbol=diagnostic.get("symbol", "unknown"),
                    mint_short=diagnostic.get("mint_short", "unknown"),
                    source=diagnostic.get("source", "unknown"),
                    mode=diagnostic.get("candidate_mode", "unknown"),
                    attn=_candidate_attention_display(diagnostic),
                    edge=diagnostic.get("edge_score", "n/a"),
                    approval=diagnostic.get("risk_approval_state", "strict_rejected"),
                    outcome=diagnostic.get("action_outcome", diagnostic.get("decision", "unknown")),
                    reason=_candidate_summary_reason(diagnostic),
                    theme=_candidate_summary_theme(diagnostic),
                    meta=diagnostic.get("metadata_completeness_state", "unknown"),
                )
            )
        lines.extend(
            [
                "Mode routing guidance:",
                "  - launch: fast-path only; no AI required or consulted",
                "  - migration: diagnostic only for now; may later become AI-eligible",
                "  - unknown: safe fallback; no AI/live routing changes",
            ]
        )
        return lines

    def discovery_comparison_lines(self) -> list[str]:
        if self.risk_profile != "discovery" or not self.accepted_candidate_diagnostics:
            return []

        candidates = sorted(self.accepted_candidate_diagnostics, key=_accepted_candidate_sort_key)
        lines = ["Accepted discovery comparison:"]
        lines.append("  # | symbol | mint | mode | attn | diff | note")
        for diagnostic in candidates[:5]:
            lines.append(
                "  {rank} | {symbol} | {mint_short} | {mode} | {attn} | {diff} | {note}".format(
                    rank=diagnostic.get("rank", "?"),
                    symbol=diagnostic.get("symbol", "unknown"),
                    mint_short=diagnostic.get("mint_short", "unknown"),
                    mode=diagnostic.get("candidate_mode", "unknown"),
                    attn=_candidate_attention_display(diagnostic),
                    diff=_accepted_candidate_diff(diagnostic),
                    note=_accepted_candidate_note(diagnostic),
                )
            )
        return lines

    def discovery_grok_prompt_lines(self) -> list[str]:
        if self.risk_profile != "discovery":
            return []

        candidates = sorted(
            [*self.accepted_candidate_diagnostics, *self.rejected_candidate_diagnostics],
            key=_candidate_sort_key,
        )
        if not candidates:
            return []

        lines = [
            "Grok social check prompt (manual only):",
            "  Review the following Solana memecoin discovery candidates using live social context only.",
            "  Return ONLY valid JSON array entries with keys: mint, social_live_score, tweet_velocity, real_account_signal, bot_spam_risk, influencer_mentions, ticker_collision, narrative_summary, recommendation.",
            "  This is operator-only paper-trading context. Do not assume these social checks override existing risk gates or trigger automatic buys.",
            "  Candidates:",
        ]
        for diagnostic in candidates[:5]:
            lines.append(f"  - {_grok_prompt_candidate_line(diagnostic)}")
        return lines


def build_signal_sources() -> list[SignalSource]:
    return [
        build_monitor_from_env(),
        WhaleWalletTracker(),
        OnChainMonitor(),
        TwitterMonitor(),
    ]


def build_signal_aggregator(sources: list[SignalSource], db_path: Path) -> SignalAggregator:
    return SignalAggregator(sources, db=db_path)


def force_paper_settings(settings: Settings) -> Settings:
    return settings.model_copy(
        update={"execution": settings.execution.model_copy(update={"mode": "paper"})}
    )


def apply_risk_profile(settings: Settings, risk_profile: str) -> Settings:
    normalized = normalize_risk_profile(risk_profile)
    return settings


def normalize_risk_profile(risk_profile: str) -> str:
    normalized = risk_profile.strip().lower()
    if normalized not in SUPPORTED_RISK_PROFILES:
        raise ValueError(f"Unsupported risk profile: {risk_profile}")
    return normalized


def build_paper_cycle_risk_scorer(risk_profile: str, settings: Settings) -> Any:
    normalized = normalize_risk_profile(risk_profile)
    return DiscoveryRiskScorer(
        settings.risk,
        rugcheck_client=RugCheckClient(),
        holder_policy_mode=normalized,
        enable_holder_lookup=normalized == "discovery",
    )


def extract_runtime_diagnostics(risk_scorer: Any) -> dict[str, int]:
    diagnostics_fn = getattr(risk_scorer, "diagnostics", None)
    if callable(diagnostics_fn):
        diagnostics = diagnostics_fn()
        if isinstance(diagnostics, dict):
            return {str(key): int(value) for key, value in diagnostics.items()}
    return {}


def extract_aggregator_diagnostics(aggregator: SignalAggregator) -> dict[str, object]:
    diagnostics = aggregator.diagnostics()
    if not isinstance(diagnostics, dict):
        return {}
    return diagnostics


def _count_rows(db_path: Path, table: str, where_clause: str | None = None, params: tuple[object, ...] = ()) -> int:
    query = f"SELECT COUNT(*) FROM {table}"
    if where_clause:
        query = f"{query} WHERE {where_clause}"
    with sqlite3.connect(db_path) as connection:
        row = connection.execute(query, params).fetchone()
    return int(row[0]) if row is not None else 0


async def _count_effective_open_positions(db_path: Path) -> int:
    """Count persisted open paper positions while honoring archived exclusions."""
    await init_db(db_path)
    inspector = PositionManager(db_path, load_settings())
    return len(await inspector.get_all_open(mode="paper"))


async def run_bounded_paper_cycle(
    max_signals: int,
    timeout_seconds: float,
    *,
    risk_profile: str = "strict",
    fresh_evaluation_session: bool = False,
    db_path: str | Path | None = None,
    settings: Settings | None = None,
    sources: list[SignalSource] | None = None,
    execution: ExecutionAdapter | None = None,
    risk_scorer: Any = None,
    poll_interval_s: float = 1.0,
) -> PaperCycleSummary:
    normalized_risk_profile = normalize_risk_profile(risk_profile)
    runtime_settings = apply_risk_profile(force_paper_settings(settings or load_settings()), normalized_risk_profile)
    runtime_db_path = resolve_db_path(db_path)
    signal_sources = list(sources) if sources is not None else build_signal_sources()
    aggregator = build_signal_aggregator(signal_sources, runtime_db_path)
    execution_adapter = execution or PaperExecutionAdapter(price_provider=DexScreenerPriceProvider())

    await init_db(runtime_db_path)
    initial_trade_count = _count_rows(runtime_db_path, "trades")
    initial_open_positions = await _count_effective_open_positions(runtime_db_path)

    position_manager = PositionManager(
        runtime_db_path,
        runtime_settings,
        use_persisted_positions=not fresh_evaluation_session,
        persist_positions=not fresh_evaluation_session,
    )

    engine = DecisionEngine(
        execution_adapter,
        risk_scorer or build_paper_cycle_risk_scorer(normalized_risk_profile, runtime_settings),
        position_manager,
        runtime_settings,
        db=runtime_db_path,
    )

    start_at = monotonic()
    signals_collected = 0
    signals_accepted = 0
    signals_rejected = 0
    source_signal_counts: Counter[str] = Counter()
    source_failures: Counter[str] = Counter()
    composite_opportunities = 0
    rejection_reasons: Counter[str] = Counter()
    evaluated_by_source: Counter[str] = Counter()
    passed_risk_by_source: Counter[str] = Counter()
    summary_rejection_reasons: Counter[str] = Counter()
    passed_risk_checks = 0
    accepted_candidate_diagnostics: list[dict[str, object]] = []
    rejected_candidate_diagnostics: list[dict[str, object]] = []
    accepted_trades_by_id: dict[str, Any] = {}
    termination_reason = "timeout"
    cycle_id = str(uuid4())

    await aggregator.start()
    try:
        while signals_collected < max_signals:
            if monotonic() - start_at >= timeout_seconds:
                break

            remaining_capacity = max_signals - signals_collected
            batch = await aggregator.poll_all()
            aggregator_diagnostics = extract_aggregator_diagnostics(aggregator)
            for source_name, count in aggregator_diagnostics.get("source_signal_counts", {}).items():
                source_signal_counts[str(source_name)] += int(count)
            for source_name, count in aggregator_diagnostics.get("source_failures", {}).items():
                source_failures[str(source_name)] += int(count)

            evaluated_batch = batch[:remaining_capacity]
            composite_opportunities += sum(
                1
                for signal in evaluated_batch
                if isinstance(signal.payload.get("source_count"), int) and signal.payload["source_count"] > 1
            )

            for signal in evaluated_batch:
                signals_collected += 1
                evaluated_by_source[str(signal.source.value).lower()] += 1
                try:
                    decision = await engine.evaluate_signal_with_diagnostics(signal)
                except Exception as exc:  # pragma: no cover - defensive against future decision/runtime failures
                    logger.warning("Failed to evaluate signal during paper-cycle: %s", exc)
                    signals_rejected += 1
                    rejection_reasons["unknown_or_other"] += 1
                    summary_rejection_reasons["unknown_or_other"] += 1
                    continue

                record = _extract_rejection_record(decision.metadata)
                if record is not None:
                    if record.outcome == "passed":
                        passed_risk_checks += 1
                        passed_risk_by_source[str(record.signal_source).lower()] += 1
                    elif record.failed_check is not None:
                        summary_rejection_reasons[_format_failed_check(record.failed_check)] += 1

                if decision.trade is None:
                    signals_rejected += 1
                    rejection_reasons[decision.rejection_reason or "unknown_or_other"] += 1
                    if record is None or record.failed_check is None:
                        summary_rejection_reasons[_format_rejection_reason(decision.rejection_reason)] += 1
                    rejected_candidate_diagnostics.append(
                        _build_rejected_candidate_diagnostic(
                            rank=signals_collected,
                            signal=signal,
                            decision=decision,
                            record=record,
                        )
                    )
                    continue

                signals_accepted += 1
                accepted_snapshot = _build_accepted_candidate_diagnostic(
                    rank=signals_collected,
                    signal=signal,
                    decision=decision,
                    record=record,
                )
                accepted_candidate_diagnostics.append(accepted_snapshot)
                if normalized_risk_profile == "discovery":
                    accepted_trades_by_id[str(decision.trade.id)] = decision.trade
                if record is None:
                    passed_risk_checks += 1
                    passed_risk_by_source[str(signal.source.value).lower()] += 1

            if signals_collected >= max_signals:
                termination_reason = "max_signals"
                break

            remaining_time = timeout_seconds - (monotonic() - start_at)
            if remaining_time <= 0:
                break
            await asyncio.sleep(min(max(poll_interval_s, 0.0), remaining_time))
    finally:
        await aggregator.stop()
        await execution_adapter.close()

    accepted_candidate_diagnostics, rejected_candidate_diagnostics = _apply_candidate_narrative_hints(
        accepted_candidate_diagnostics,
        rejected_candidate_diagnostics,
    )
    accepted_candidate_diagnostics, rejected_candidate_diagnostics = _apply_discovery_ranking_penalties(
        accepted_candidate_diagnostics,
        rejected_candidate_diagnostics,
    )
    if normalized_risk_profile == "discovery":
        accepted_candidate_diagnostics, rejected_candidate_diagnostics = _apply_discovery_edge_diagnostics(
            accepted_candidate_diagnostics,
            rejected_candidate_diagnostics,
        )
    await _persist_paper_decisions(
        runtime_db_path,
        cycle_id=cycle_id,
        execution_mode=runtime_settings.execution.mode,
        risk_profile=normalized_risk_profile,
        accepted=accepted_candidate_diagnostics,
        rejected=rejected_candidate_diagnostics,
    )
    candidate_mode_counts = _candidate_mode_counts(accepted_candidate_diagnostics, rejected_candidate_diagnostics)
    if normalized_risk_profile == "discovery":
        for diagnostic in accepted_candidate_diagnostics:
            trade_id = diagnostic.get("trade_id")
            if not isinstance(trade_id, str):
                continue
            trade = accepted_trades_by_id.get(trade_id)
            if trade is None:
                continue
            persisted_trade = trade.model_copy(
                update={
                    "metadata": {
                        **trade.metadata,
                        "candidate_snapshot": _compact_candidate_snapshot(diagnostic),
                    }
                }
            )
            await record_trade(runtime_db_path, persisted_trade)
            accepted_trades_by_id[trade_id] = persisted_trade

    holder_lookup_outcomes = extract_runtime_diagnostics(engine.risk_scorer)
    session_open_positions = len(await position_manager.get_all_open(mode="paper"))
    persisted_open_positions = await _count_effective_open_positions(runtime_db_path)

    return PaperCycleSummary(
        execution_mode=runtime_settings.execution.mode,
        risk_profile=normalized_risk_profile,
        max_signals=max_signals,
        timeout_seconds=timeout_seconds,
        signals_collected=signals_collected,
        signals_accepted=signals_accepted,
        signals_rejected=signals_rejected,
        trades_persisted=max(_count_rows(runtime_db_path, "trades") - initial_trade_count, 0),
        open_positions=session_open_positions,
        sources_polled=[source.name for source in signal_sources],
        source_signal_counts=dict(sorted(source_signal_counts.items())),
        source_failures=dict(sorted(source_failures.items())),
        composite_opportunities=composite_opportunities,
        rejection_reasons=dict(sorted(rejection_reasons.items())),
        candidates_evaluated=signals_accepted + signals_rejected,
        passed_risk_checks=passed_risk_checks,
        summary_rejection_reasons=dict(sorted(summary_rejection_reasons.items())),
        source_evaluated_counts=dict(sorted(evaluated_by_source.items())),
        source_pass_counts=dict(sorted(passed_risk_by_source.items())),
        holder_lookup_outcomes=holder_lookup_outcomes,
        candidate_mode_counts=candidate_mode_counts,
        termination_reason=termination_reason,
        elapsed_seconds=round(monotonic() - start_at, 3),
        accepted_candidate_diagnostics=accepted_candidate_diagnostics,
        rejected_candidate_diagnostics=rejected_candidate_diagnostics,
        evaluation_session_scope="fresh" if fresh_evaluation_session else "persisted",
        starting_open_positions=initial_open_positions,
        persisted_open_positions=persisted_open_positions,
        configured_max_open_positions=runtime_settings.position.max_open_positions,
        capacity_blocked_candidates=rejection_reasons.get("max_open_positions_reached", 0),
    )


async def _persist_paper_decisions(
    runtime_db_path: Path,
    *,
    cycle_id: str,
    execution_mode: str,
    risk_profile: str,
    accepted: list[dict[str, object]],
    rejected: list[dict[str, object]],
) -> None:
    for diagnostic in [*accepted, *rejected]:
        try:
            await record_paper_decision(
                runtime_db_path,
                _paper_decision_record(
                    diagnostic,
                    cycle_id=cycle_id,
                    execution_mode=execution_mode,
                    risk_profile=risk_profile,
                ),
            )
        except Exception as exc:  # pragma: no cover - defensive persistence fallback
            logger.warning("Failed to persist paper decision telemetry for %s: %s", diagnostic.get("mint", "unknown"), exc)


def _paper_decision_record(
    diagnostic: dict[str, object],
    *,
    cycle_id: str,
    execution_mode: str,
    risk_profile: str,
) -> PaperDecisionRecord:
    diagnostics_payload = {
        "rank": diagnostic.get("rank"),
        "failed_check": diagnostic.get("failed_check"),
        "rejection_reason": diagnostic.get("rejection_reason"),
        "holder_policy_state": diagnostic.get("holder_policy_state"),
        "creator_policy_state": diagnostic.get("creator_policy_state"),
        "unique_buyers_policy_state": diagnostic.get("unique_buyers_policy_state"),
        "authority_policy_state": diagnostic.get("authority_policy_state"),
        "honeypot_policy_state": diagnostic.get("honeypot_policy_state"),
        "risk_approval_state": diagnostic.get("risk_approval_state"),
        "edge_score": diagnostic.get("edge_score"),
        "edge_breakdown": diagnostic.get("edge_breakdown"),
        "metadata_completeness_state": diagnostic.get("metadata_completeness_state"),
        "social_signal_state": diagnostic.get("social_signal_state"),
        "source_count": diagnostic.get("source_count"),
        "sources": list(diagnostic.get("sources", ())) if isinstance(diagnostic.get("sources"), (list, tuple)) else [],
        "attention_reasons": list(diagnostic.get("attention_reasons", ())) if isinstance(diagnostic.get("attention_reasons"), (list, tuple)) else [],
        "narrative_tags": list(diagnostic.get("narrative_tags", ())) if isinstance(diagnostic.get("narrative_tags"), (list, tuple)) else [],
        "main_warnings": list(diagnostic.get("main_warnings", ())) if isinstance(diagnostic.get("main_warnings"), (list, tuple)) else [],
    }
    primary_reason = str(
        diagnostic.get("rejection_reason")
        or diagnostic.get("failed_check")
        or diagnostic.get("action_outcome")
        or diagnostic.get("decision")
        or "unknown"
    )
    source_count = diagnostic.get("source_count")
    return PaperDecisionRecord(
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        risk_profile=risk_profile,
        mint_address=str(diagnostic.get("mint") or diagnostic.get("mint_address") or ""),
        symbol=_stringish(diagnostic.get("symbol")) if isinstance(diagnostic.get("symbol"), str) else None,
        name=_stringish(diagnostic.get("name")) if isinstance(diagnostic.get("name"), str) else None,
        source=str(diagnostic.get("source") or "unknown"),
        source_count=int(source_count) if isinstance(source_count, int) else 1,
        candidate_mode=str(diagnostic.get("candidate_mode") or "unknown"),
        decision=str(diagnostic.get("decision") or "unknown"),
        action_outcome=str(diagnostic.get("action_outcome") or "unknown"),
        primary_reason=primary_reason,
        attention_score=int(diagnostic.get("attention_score") or 0) if isinstance(diagnostic.get("attention_score"), (int, float)) else 0,
        risk_score=float(diagnostic.get("risk_score")) if isinstance(diagnostic.get("risk_score"), (int, float)) else None,
        diagnostics_json=json.dumps(diagnostics_payload, sort_keys=True),
    )


def _paper_decision_edge_display(record: PaperDecisionRecord) -> str:
    """Format persisted discovery edge telemetry for paper operator review only."""
    try:
        diagnostics = json.loads(record.diagnostics_json)
    except (TypeError, json.JSONDecodeError):
        return "edge=not-recorded"
    if not isinstance(diagnostics, dict):
        return "edge=not-recorded"

    score = diagnostics.get("edge_score")
    breakdown = diagnostics.get("edge_breakdown")
    if not isinstance(score, (int, float)) or isinstance(score, bool):
        return "edge=not-recorded"
    if not isinstance(breakdown, str) or not breakdown.strip():
        return f"edge={score:g} detail=not-recorded"
    return f"edge={score:g} detail={breakdown.strip()}"


def _extract_rejection_record(metadata: dict[str, object]) -> RejectionRecord | None:
    raw_record = metadata.get("rejection_record")
    if isinstance(raw_record, RejectionRecord):
        return raw_record
    if isinstance(raw_record, dict):
        try:
            return RejectionRecord(**raw_record)
        except TypeError:
            return None
    return None


def _format_failed_check(failed_check: str) -> str:
    return failed_check.removesuffix("_check")


def _format_rejection_reason(rejection_reason: str | None) -> str:
    if not rejection_reason:
        return "unknown_or_other"
    if rejection_reason.endswith("_failed"):
        return rejection_reason.removesuffix("_failed").removesuffix("_check")
    if rejection_reason.endswith("_unknown"):
        return rejection_reason.removesuffix("_check_unknown") + "_unknown"
    return rejection_reason


def _build_rejected_candidate_diagnostic(
    *,
    rank: int,
    signal: Any,
    decision: Any,
    record: RejectionRecord | None,
) -> dict[str, object]:
    action_outcome = _decision_action_outcome(decision, record)
    payload = signal.payload if isinstance(signal.payload, dict) else {}
    token_section = payload.get("token") if isinstance(payload.get("token"), dict) else {}
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    social = payload.get("social_credibility") if isinstance(payload.get("social_credibility"), dict) else {}
    attention_diagnostics = payload.get("attention_diagnostics") if isinstance(payload.get("attention_diagnostics"), dict) else {}
    holder_diagnostics = payload.get("holder_diagnostics") if isinstance(payload.get("holder_diagnostics"), dict) else {}
    creator_diagnostics = payload.get("creator_diagnostics") if isinstance(payload.get("creator_diagnostics"), dict) else {}
    creator_policy = payload.get("creator_policy") if isinstance(payload.get("creator_policy"), dict) else {}
    unique_buyers_diagnostics = payload.get("unique_buyers_diagnostics") if isinstance(payload.get("unique_buyers_diagnostics"), dict) else {}
    unique_buyers_policy = payload.get("unique_buyers_policy") if isinstance(payload.get("unique_buyers_policy"), dict) else {}
    authority_diagnostics = payload.get("authority_diagnostics") if isinstance(payload.get("authority_diagnostics"), dict) else {}
    authority_policy = payload.get("authority_policy") if isinstance(payload.get("authority_policy"), dict) else {}
    honeypot_diagnostics = payload.get("honeypot_diagnostics") if isinstance(payload.get("honeypot_diagnostics"), dict) else {}
    honeypot_policy = payload.get("honeypot_policy") if isinstance(payload.get("honeypot_policy"), dict) else {}
    holder_policy = payload.get("holder_policy") if isinstance(payload.get("holder_policy"), dict) else {}
    age_policy = payload.get("age_policy") if isinstance(payload.get("age_policy"), dict) else {}
    liquidity_diagnostics = payload.get("liquidity_diagnostics") if isinstance(payload.get("liquidity_diagnostics"), dict) else {}
    top10_result = _check_result_value(record, "top10_holder_check")
    liquidity_result = _check_result_value(record, "liquidity_check")
    authority_result = _check_result_value(record, "mint_authority_check")
    honeypot_result = _check_result_value(record, "honeypot_check")
    unique_buyers_result = _check_result_value(record, "unique_buyers_check")
    market_cap = _first_present(
        token_section,
        payload,
        keys=("market_cap_usd", "marketCapUsd", "usdMarketCap", "marketCapUSD", "marketCapSol"),
    )
    volume = _first_present(metrics, payload, keys=("volume_m5", "volume_h1", "volume_h24", "volume"))
    buy_sell = _buy_sell_hint(metrics)
    attention_hints = _attention_hints(payload, social=social, market_cap=market_cap, volume=volume, buy_sell=buy_sell)
    symbol = _stringish(_first_present(token_section, payload, keys=("symbol", "ticker", "name"))) or "unknown"
    liquidity_value = _check_metric_value(record, "liquidity_check")
    top10_value = _check_metric_value(record, "top10_holder_check")

    return {
        "rank": rank,
        "mint": signal.mint_address,
        "mint_short": _short_mint(signal.mint_address),
        "symbol": symbol,
        "name": _stringish(_first_present(token_section, payload, keys=("name",))),
        "source": str(signal.source.value).lower(),
        "confidence": round(signal.confidence, 6),
        "weight": round(signal.weight, 6),
        "effective_score": round(min(signal.confidence * max(signal.weight, 0.0), 1.0), 6),
        "composite_score": payload.get("composite_score"),
        "decision": "rejected" if action_outcome == "rejected" else "skipped",
        "action_outcome": action_outcome,
        "failed_check": (record.failed_check if record is not None and record.failed_check is not None else _format_rejection_reason(decision.rejection_reason)),
        "rejection_reason": decision.rejection_reason or "unknown_or_other",
        "risk_score": record.risk_score if record is not None else None,
        "attention_score": attention_diagnostics.get("attention_score", 0),
        "attention_tier": attention_diagnostics.get("attention_tier", "ignore"),
        "attention_reasons": tuple(attention_diagnostics.get("attention_reasons", ())),
        "narrative_tags": tuple(attention_diagnostics.get("narrative_tags", ())),
        "candidate_mode": payload.get("candidate_mode", "unknown"),
        "social_signal_state": attention_diagnostics.get("social_signal_state", "missing"),
        "social_credibility_tier": social.get("highest_tier", "unknown"),
        "metadata_completeness_state": attention_diagnostics.get("metadata_completeness_state", "sparse"),
        "rugcheck_top10_holder_pct": holder_diagnostics.get("rugcheck_top10_holder_pct", "unknown"),
        "local_filtered_top10_holder_pct": holder_diagnostics.get("local_filtered_top10_holder_pct", "unknown"),
        "selected_top10_holder_pct": holder_diagnostics.get(
            "selected_top10_holder_pct",
            top10_value if top10_value is not None else "unknown",
        ),
        "top10_holder_pct": holder_diagnostics.get(
            "selected_top10_holder_pct",
            top10_value if top10_value is not None else "unknown",
        ),
        "top10_holder_source": _top10_holder_source_hint(payload, record),
        "bonding_curve_addresses": tuple(holder_diagnostics.get("bonding_curve_addresses", ())),
        "local_holder_raw_account_count": holder_diagnostics.get("local_holder_raw_account_count", 0),
        "local_holder_filtered_account_count": holder_diagnostics.get("local_holder_filtered_account_count", 0),
        "local_holder_retained_account_count": holder_diagnostics.get("local_holder_retained_account_count", 0),
        "local_holder_top_filtered_accounts": tuple(holder_diagnostics.get("local_holder_top_filtered_accounts", ())),
        "local_holder_top_retained_accounts": tuple(holder_diagnostics.get("local_holder_top_retained_accounts", ())),
        "creator_holding_pct": creator_diagnostics.get("creator_holding_pct", "unknown"),
        "creator_holding_source": creator_diagnostics.get("creator_holding_source", "unknown"),
        "creator_holding_state": creator_diagnostics.get("creator_holding_state", "unknown"),
        "creator_holding_unknown_reason": creator_diagnostics.get("creator_holding_unknown_reason"),
        "creator_holding_display": _creator_holding_display(creator_diagnostics),
        "creator_policy_state": creator_policy.get("creator_policy_state", "unknown"),
        "creator_policy_reason": creator_policy.get("creator_policy_reason", "unknown"),
        "creator_policy_context_used": bool(creator_policy.get("creator_policy_context_used")),
        "unique_buyers_count": unique_buyers_diagnostics.get("unique_buyers_count", "unknown"),
        "unique_buyers_source": unique_buyers_diagnostics.get("unique_buyers_source", "unknown"),
        "unique_buyers_state": unique_buyers_diagnostics.get("unique_buyers_state", "unknown"),
        "unique_buyers_unknown_reason": unique_buyers_diagnostics.get("unique_buyers_unknown_reason"),
        "unique_buyers_policy_state": unique_buyers_policy.get("unique_buyers_policy_state", "unknown"),
        "unique_buyers_policy_reason": unique_buyers_policy.get("unique_buyers_policy_reason", "unknown"),
        "unique_buyers_policy_context_used": bool(unique_buyers_policy.get("unique_buyers_policy_context_used")),
        "mint_authority_state": authority_diagnostics.get("mint_authority_state", "unknown"),
        "freeze_authority_state": authority_diagnostics.get("freeze_authority_state", "unknown"),
        "authority_source": authority_diagnostics.get("authority_source", "unknown"),
        "authority_unknown_reason": authority_diagnostics.get("authority_unknown_reason"),
        "authority_policy_state": authority_policy.get("authority_policy_state", "unknown"),
        "authority_policy_reason": authority_policy.get("authority_policy_reason", "unknown"),
        "authority_policy_context_used": bool(authority_policy.get("authority_policy_context_used")),
        "honeypot_state": honeypot_diagnostics.get("honeypot_state", "unknown"),
        "honeypot_source": honeypot_diagnostics.get("honeypot_source", "unknown"),
        "honeypot_unknown_reason": honeypot_diagnostics.get("honeypot_unknown_reason"),
        "honeypot_policy_state": honeypot_policy.get("honeypot_policy_state", "unknown"),
        "risk_approval_state": _risk_approval_state(
            record,
            holder_policy,
            age_policy,
            creator_policy,
            unique_buyers_policy,
            authority_policy,
            honeypot_policy,
        ),
        "honeypot_policy_reason": honeypot_policy.get("honeypot_policy_reason", "unknown"),
        "honeypot_policy_context_used": bool(honeypot_policy.get("honeypot_policy_context_used")),
        "holder_policy_state": holder_policy.get("holder_policy_state", "unknown"),
        "holder_policy_reason": holder_policy.get("holder_policy_reason", "unknown"),
        "token_age_minutes": holder_policy.get("token_age_minutes"),
        "stage_hint": holder_policy.get("stage_hint", "unknown"),
        "fresh_launch_context_used": bool(holder_policy.get("fresh_launch_context_used")),
        "age_policy_state": age_policy.get("age_policy_state", "unknown"),
        "age_policy_reason": age_policy.get("age_policy_reason", "unknown"),
        "age_policy_context_used": bool(age_policy.get("age_policy_context_used")),
        "age_policy_age_minutes": age_policy.get("token_age_minutes"),
        "age_policy_stage_hint": age_policy.get("stage_hint", "unknown"),
        "selected_liquidity_sol": liquidity_diagnostics.get("selected_liquidity_sol", liquidity_value if liquidity_value is not None else "unknown"),
        "selected_liquidity_usd": liquidity_diagnostics.get("selected_liquidity_usd", "unknown"),
        "liquidity_source": liquidity_diagnostics.get("liquidity_source", "unknown"),
        "liquidity_data_state": liquidity_diagnostics.get("liquidity_data_state", "unknown"),
        "liquidity_unknown_reason": liquidity_diagnostics.get("liquidity_unknown_reason"),
        "dexscreener_liquidity_sol": liquidity_diagnostics.get("dexscreener_liquidity_sol", "unknown"),
        "dexscreener_liquidity_usd": liquidity_diagnostics.get("dexscreener_liquidity_usd", "unknown"),
        "dexscreener_status": liquidity_diagnostics.get("dexscreener_status", "unknown"),
        "jupiter_liquidity_sol": liquidity_diagnostics.get("jupiter_liquidity_sol", "unknown"),
        "jupiter_liquidity_usd": liquidity_diagnostics.get("jupiter_liquidity_usd", "unknown"),
        "jupiter_status": liquidity_diagnostics.get("jupiter_status", "unknown"),
        "fallback_attempted": bool(liquidity_diagnostics.get("fallback_attempted")),
        "fallback_succeeded": bool(liquidity_diagnostics.get("fallback_succeeded")),
        "liquidity_value": liquidity_diagnostics.get("selected_liquidity_sol", liquidity_value if liquidity_value is not None else "unknown"),
        "liquidity_display": _liquidity_display(liquidity_result, liquidity_diagnostics.get("selected_liquidity_sol", liquidity_value)),
        "liquidity_state": liquidity_result or "unknown",
        "liquidity_check": liquidity_result or "unknown",
        "honeypot_check": honeypot_result or "unknown",
        "authority_check": authority_result or "unknown",
        "funding_check": unique_buyers_result or "unknown",
        "market_cap_hint": market_cap if market_cap is not None else "unknown",
        "volume_hint": volume if volume is not None else "unknown",
        "buy_sell_hint": buy_sell,
        "graduation_flag": str(signal.type.value).lower() == "graduation",
        "social_hint": _social_hint(social),
        "attention_hints": attention_hints,
        "creator_repeat_flag": bool(payload.get("creator_repeat_flag")),
        "pump_fun_identity_context": payload.get("pump_fun_identity_context") if isinstance(payload.get("pump_fun_identity_context"), dict) else {},
        "source_count": payload.get("source_count"),
        "sources": tuple(payload.get("sources", ())) if isinstance(payload.get("sources"), list) else tuple(payload.get("sources", ())) if isinstance(payload.get("sources"), tuple) else (),
        "main_warnings": _main_warnings(
            holder_policy_state=holder_policy.get("holder_policy_state", "unknown"),
            age_policy_state=age_policy.get("age_policy_state", "unknown"),
            creator_policy_state=creator_policy.get("creator_policy_state", "unknown"),
            unique_buyers_policy_state=unique_buyers_policy.get("unique_buyers_policy_state", "unknown"),
            authority_policy_state=authority_policy.get("authority_policy_state", "unknown"),
            honeypot_policy_state=honeypot_policy.get("honeypot_policy_state", "unknown"),
            rejection_reason=decision.rejection_reason,
            risk_reasons=(),
        ),
        "notes": _diagnostic_note(payload, record),
    }


def _build_accepted_candidate_diagnostic(
    *,
    rank: int,
    signal: Any,
    decision: Any,
    record: RejectionRecord | None,
) -> dict[str, object]:
    payload = signal.payload if isinstance(signal.payload, dict) else {}
    token_section = payload.get("token") if isinstance(payload.get("token"), dict) else {}
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    social = payload.get("social_credibility") if isinstance(payload.get("social_credibility"), dict) else {}
    attention_diagnostics = payload.get("attention_diagnostics") if isinstance(payload.get("attention_diagnostics"), dict) else {}
    holder_diagnostics = payload.get("holder_diagnostics") if isinstance(payload.get("holder_diagnostics"), dict) else {}
    creator_policy = payload.get("creator_policy") if isinstance(payload.get("creator_policy"), dict) else {}
    unique_buyers_policy = payload.get("unique_buyers_policy") if isinstance(payload.get("unique_buyers_policy"), dict) else {}
    authority_policy = payload.get("authority_policy") if isinstance(payload.get("authority_policy"), dict) else {}
    honeypot_policy = payload.get("honeypot_policy") if isinstance(payload.get("honeypot_policy"), dict) else {}
    holder_policy = payload.get("holder_policy") if isinstance(payload.get("holder_policy"), dict) else {}
    age_policy = payload.get("age_policy") if isinstance(payload.get("age_policy"), dict) else {}
    liquidity_diagnostics = payload.get("liquidity_diagnostics") if isinstance(payload.get("liquidity_diagnostics"), dict) else {}
    market_cap = _first_present(
        token_section,
        payload,
        keys=("market_cap_usd", "marketCapUsd", "usdMarketCap", "marketCapUSD", "marketCapSol"),
    )
    volume = _first_present(metrics, payload, keys=("volume_m5", "volume_h1", "volume_h24", "volume"))
    buy_sell = _buy_sell_hint(metrics)
    action_outcome = _decision_action_outcome(decision, record)
    risk_reasons = tuple(decision.trade.metadata.get("risk_reasons", ())) if getattr(decision, "trade", None) is not None else ()
    symbol = _stringish(_first_present(token_section, payload, keys=("symbol", "ticker", "name"))) or "unknown"

    return {
        "rank": rank,
        "mint": signal.mint_address,
        "mint_short": _short_mint(signal.mint_address),
        "symbol": symbol,
        "name": _stringish(_first_present(token_section, payload, keys=("name",))),
        "source": str(signal.source.value).lower(),
        "confidence": round(signal.confidence, 6),
        "weight": round(signal.weight, 6),
        "effective_score": round(min(signal.confidence * max(signal.weight, 0.0), 1.0), 6),
        "composite_score": payload.get("composite_score"),
        "decision": "accepted",
        "action_outcome": action_outcome,
        "trade_id": getattr(decision.trade, "id", None),
        "failed_check": record.failed_check if record is not None else None,
        "rejection_reason": decision.rejection_reason,
        "risk_score": record.risk_score if record is not None else None,
        "attention_score": attention_diagnostics.get("attention_score", 0),
        "attention_tier": attention_diagnostics.get("attention_tier", "ignore"),
        "attention_reasons": tuple(attention_diagnostics.get("attention_reasons", ())),
        "narrative_tags": tuple(attention_diagnostics.get("narrative_tags", ())),
        "candidate_mode": payload.get("candidate_mode", "unknown"),
        "social_signal_state": attention_diagnostics.get("social_signal_state", "missing"),
        "social_credibility_tier": social.get("highest_tier", "unknown"),
        "metadata_completeness_state": attention_diagnostics.get("metadata_completeness_state", "sparse"),
        "token_age_minutes": holder_policy.get("token_age_minutes", age_policy.get("token_age_minutes")),
        "stage_hint": holder_policy.get("stage_hint", age_policy.get("stage_hint", "unknown")),
        "selected_liquidity_sol": liquidity_diagnostics.get("selected_liquidity_sol"),
        "selected_liquidity_usd": liquidity_diagnostics.get("selected_liquidity_usd"),
        "liquidity_source": liquidity_diagnostics.get("liquidity_source", "unknown"),
        "liquidity_data_state": liquidity_diagnostics.get("liquidity_data_state", "unknown"),
        "liquidity_display": _liquidity_display("pass", liquidity_diagnostics.get("selected_liquidity_sol")),
        "top10_holder_source": _top10_holder_source_hint(payload, record),
        "top10_holder_pct": holder_diagnostics.get("selected_top10_holder_pct", _check_metric_value(record, "top10_holder_check")),
        "holder_policy_state": holder_policy.get("holder_policy_state", "unknown"),
        "creator_policy_state": creator_policy.get("creator_policy_state", "unknown"),
        "unique_buyers_policy_state": unique_buyers_policy.get("unique_buyers_policy_state", "unknown"),
        "authority_policy_state": authority_policy.get("authority_policy_state", "unknown"),
        "honeypot_policy_state": honeypot_policy.get("honeypot_policy_state", "unknown"),
        "risk_approval_state": _risk_approval_state(
            record,
            holder_policy,
            age_policy,
            creator_policy,
            unique_buyers_policy,
            authority_policy,
            honeypot_policy,
        ),
        "main_warnings": _main_warnings(
            holder_policy_state=holder_policy.get("holder_policy_state", "unknown"),
            age_policy_state=age_policy.get("age_policy_state", "unknown"),
            creator_policy_state=creator_policy.get("creator_policy_state", "unknown"),
            unique_buyers_policy_state=unique_buyers_policy.get("unique_buyers_policy_state", "unknown"),
            authority_policy_state=authority_policy.get("authority_policy_state", "unknown"),
            honeypot_policy_state=honeypot_policy.get("honeypot_policy_state", "unknown"),
            rejection_reason=decision.rejection_reason,
            risk_reasons=risk_reasons,
        ),
        "attention_hints": _attention_hints(payload, social=social, market_cap=market_cap, volume=volume, buy_sell=buy_sell),
        "pump_fun_identity_context": payload.get("pump_fun_identity_context") if isinstance(payload.get("pump_fun_identity_context"), dict) else {},
        "source_count": payload.get("source_count"),
        "sources": tuple(payload.get("sources", ())) if isinstance(payload.get("sources"), list) else tuple(payload.get("sources", ())) if isinstance(payload.get("sources"), tuple) else (),
    }


def _decision_action_outcome(decision: Any, record: RejectionRecord | None) -> str:
    if getattr(decision, "trade", None) is not None:
        return "traded"
    if decision.rejection_reason == "max_open_positions_reached":
        return "capacity-blocked"
    if record is not None and record.outcome == "passed":
        return "skipped"
    return "rejected"


def _main_warnings(
    *,
    holder_policy_state: object,
    age_policy_state: object,
    creator_policy_state: object,
    unique_buyers_policy_state: object,
    authority_policy_state: object,
    honeypot_policy_state: object,
    rejection_reason: str | None,
    risk_reasons: tuple[object, ...],
) -> tuple[str, ...]:
    warnings: list[str] = []
    for label, state in (
        ("holder_policy", holder_policy_state),
        ("age_policy", age_policy_state),
        ("creator_policy", creator_policy_state),
        ("unique_buyers_policy", unique_buyers_policy_state),
        ("authority_policy", authority_policy_state),
        ("honeypot_policy", honeypot_policy_state),
    ):
        if isinstance(state, str) and state not in {"pass", "age_pass", "known", "unknown", "none"}:
            warnings.append(f"{label}:{state}")
    if isinstance(rejection_reason, str) and rejection_reason:
        warnings.append(f"outcome:{rejection_reason}")
    for reason in risk_reasons:
        if isinstance(reason, str) and reason:
            warnings.append(f"risk:{reason}")
    return tuple(dict.fromkeys(warnings))


def _risk_approval_state(record: RejectionRecord | None, *policies: dict[str, object]) -> str:
    if any(
        value == "discovery_relaxed"
        for policy in policies
        for key, value in policy.items()
        if key.endswith("_state")
    ):
        return "discovery_relaxed"
    if record is not None and record.check_results and all(
        entry.get("result") == "pass"
        for entry in record.check_results.values()
        if isinstance(entry, dict)
    ):
        return "strict_approved"
    return "strict_rejected"


def _compact_candidate_snapshot(diagnostic: dict[str, object]) -> dict[str, object]:
    return {
        "symbol": diagnostic.get("symbol"),
        "name": diagnostic.get("name"),
        "mint": diagnostic.get("mint"),
        "mint_short": diagnostic.get("mint_short"),
        "source": diagnostic.get("source"),
        "confidence": diagnostic.get("confidence"),
        "weight": diagnostic.get("weight"),
        "effective_score": diagnostic.get("effective_score"),
        "composite_score": diagnostic.get("composite_score"),
        "attention_score": diagnostic.get("attention_score"),
        "attention_tier": diagnostic.get("attention_tier"),
        "attention_reasons": list(diagnostic.get("attention_reasons", ())),
        "narrative_tags": list(diagnostic.get("narrative_tags", ())),
        "candidate_mode": diagnostic.get("candidate_mode"),
        "social_signal_state": diagnostic.get("social_signal_state"),
        "social_credibility_tier": diagnostic.get("social_credibility_tier"),
        "metadata_completeness_state": diagnostic.get("metadata_completeness_state"),
        "token_age_minutes": diagnostic.get("token_age_minutes"),
        "stage_hint": diagnostic.get("stage_hint"),
        "liquidity_sol": diagnostic.get("selected_liquidity_sol"),
        "liquidity_usd": diagnostic.get("selected_liquidity_usd"),
        "liquidity_source": diagnostic.get("liquidity_source"),
        "liquidity_data_state": diagnostic.get("liquidity_data_state"),
        "holder_policy_state": diagnostic.get("holder_policy_state"),
        "holder_source": diagnostic.get("top10_holder_source"),
        "holder_top10_pct": diagnostic.get("top10_holder_pct"),
        "creator_policy_state": diagnostic.get("creator_policy_state"),
        "unique_buyers_policy_state": diagnostic.get("unique_buyers_policy_state"),
        "authority_policy_state": diagnostic.get("authority_policy_state"),
        "honeypot_policy_state": diagnostic.get("honeypot_policy_state"),
        "main_warnings": list(diagnostic.get("main_warnings", ())),
        "risk_approval_state": diagnostic.get("risk_approval_state"),
        "edge_score": diagnostic.get("edge_score"),
        "edge_breakdown": diagnostic.get("edge_breakdown"),
        "narrative_quality_hint": diagnostic.get("narrative_quality_hint"),
        "theme_cluster_hint": diagnostic.get("theme_cluster_hint"),
        "name_quality_hint": diagnostic.get("name_quality_hint"),
        "source_context_hint": diagnostic.get("source_context_hint"),
        "momentum_context_hint": diagnostic.get("momentum_context_hint"),
        "action_outcome": diagnostic.get("action_outcome"),
        "skip_or_rejection_reason": diagnostic.get("rejection_reason"),
    }


def _apply_candidate_narrative_hints(
    accepted: list[dict[str, object]],
    rejected: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    candidates = [*accepted, *rejected]
    token_counts: Counter[str] = Counter()
    for diagnostic in candidates:
        for token in _candidate_theme_tokens(diagnostic):
            token_counts[token] += 1

    def enrich(diagnostic: dict[str, object]) -> dict[str, object]:
        return {
            **diagnostic,
            "theme_cluster_hint": _theme_cluster_hint(diagnostic, token_counts),
            "name_quality_hint": _name_quality_hint(diagnostic, token_counts),
            "source_context_hint": _source_context_hint(diagnostic),
            "momentum_context_hint": _momentum_context_hint(diagnostic),
            "narrative_quality_hint": _narrative_quality_hint(diagnostic, token_counts),
        }

    return [enrich(item) for item in accepted], [enrich(item) for item in rejected]


def _apply_discovery_ranking_penalties(
    accepted: list[dict[str, object]],
    rejected: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    def enrich(diagnostic: dict[str, object]) -> dict[str, object]:
        penalty_points, reasons = _discovery_ranking_penalty(diagnostic)
        base_attention = diagnostic.get("attention_score", 0)
        attention_score = int(base_attention) if isinstance(base_attention, (int, float)) else 0
        return {
            **diagnostic,
            "ranking_penalty_points": penalty_points,
            "ranking_penalty_reasons": reasons,
            "ranking_attention_score": max(attention_score - penalty_points, 0),
        }

    return [enrich(item) for item in accepted], [enrich(item) for item in rejected]


def _apply_discovery_edge_diagnostics(
    accepted: list[dict[str, object]],
    rejected: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Attach display-only discovery context after all decisions and ranking inputs exist."""

    def enrich(diagnostic: dict[str, object]) -> dict[str, object]:
        source_count = diagnostic.get("source_count")
        source_count = source_count if isinstance(source_count, int) and source_count > 0 else 1
        composite_score = _coerce_numeric(diagnostic.get("composite_score"))
        effective_score = _coerce_numeric(diagnostic.get("effective_score")) or 0.0
        source_score = composite_score if composite_score is not None else effective_score
        attention_score = _coerce_numeric(diagnostic.get("attention_score")) or 0.0
        ranking_penalty = diagnostic.get("ranking_penalty_points")
        ranking_penalty = ranking_penalty if isinstance(ranking_penalty, int) else 0
        warnings_penalty = min(_warning_count(diagnostic) * 3, 15)
        approval = diagnostic.get("risk_approval_state", "strict_rejected")
        approval_adjustment = {
            "strict_approved": 5,
            "discovery_relaxed": 0,
            "strict_rejected": -10,
        }.get(approval, -10)
        social_state = diagnostic.get("social_signal_state", "missing")
        social_bonus = 4 if social_state not in {"missing", "unknown", None} else 0
        edge_score = round(
            max(
                0.0,
                min(
                    source_score * 45
                    + min(max(source_count - 1, 0), 2) * 7
                    + min(max(attention_score, 0.0), 100.0) * 0.35
                    + social_bonus
                    + approval_adjustment
                    - ranking_penalty
                    - warnings_penalty,
                    100.0,
                ),
            )
        )
        mode = diagnostic.get("candidate_mode", "unknown")
        breakdown = (
            f"src={source_count}/comp={source_score:.2f} mode={mode} "
            f"attn={int(attention_score)}/{social_state} weak=-{ranking_penalty} "
            f"warn=-{warnings_penalty} approval={approval}"
        )
        return {**diagnostic, "edge_score": edge_score, "edge_breakdown": breakdown}

    return [enrich(item) for item in accepted], [enrich(item) for item in rejected]


def _discovery_ranking_penalty(diagnostic: dict[str, object]) -> tuple[int, tuple[str, ...]]:
    identity_context = diagnostic.get("pump_fun_identity_context")
    if not isinstance(identity_context, dict) or not identity_context.get("has_pump_fun"):
        return 0, ()

    points = 0
    reasons: list[str] = []
    source_context = diagnostic.get("source_context_hint")
    single_source_launch = source_context == "single-source-launch"
    name_quality = diagnostic.get("name_quality_hint")
    theme_hint = diagnostic.get("theme_cluster_hint")

    if name_quality == "generic-clone-like":
        points += 4
        reasons.append("clone_cluster")
    elif name_quality == "theme-repeated":
        points += 3
        reasons.append("repeated_theme")

    if single_source_launch and theme_hint == "cluster:liquid":
        points += 2
        reasons.append("generic_theme")

    metadata_state = identity_context.get("metadata_state")
    if metadata_state == "sparse":
        points += 4
        reasons.append("sparse_metadata")
    elif metadata_state == "partial" and single_source_launch:
        points += 2
        reasons.append("partial_metadata")

    if identity_context.get("weak_identity_name"):
        points += 4
        reasons.append("weak_identity")

    bounded_points = min(points, 8)
    return bounded_points, tuple(dict.fromkeys(reasons))


def _candidate_sort_key(diagnostic: dict[str, object]) -> tuple[int, int, int]:
    ranking_attention = diagnostic.get("ranking_attention_score", diagnostic.get("attention_score", 0))
    return (
        -int(ranking_attention if isinstance(ranking_attention, (int, float)) else 0),
        -_attention_tier_rank(diagnostic.get("attention_tier")),
        int(diagnostic.get("rank", 10_000) if isinstance(diagnostic.get("rank"), int) else 10_000),
    )


def _attention_tier_rank(value: object) -> int:
    if not isinstance(value, str):
        return 0
    return {
        "strong_watch": 4,
        "candidate": 3,
        "watch": 2,
        "ignore": 1,
    }.get(value, 0)


def _candidate_summary_reason(diagnostic: dict[str, object]) -> str:
    warnings = diagnostic.get("main_warnings")
    if isinstance(warnings, (list, tuple)) and warnings:
        first_warning = warnings[0]
        if isinstance(first_warning, str) and first_warning:
            return first_warning
    rejection_reason = diagnostic.get("rejection_reason")
    if isinstance(rejection_reason, str) and rejection_reason:
        return rejection_reason
    failed_check = diagnostic.get("failed_check")
    if isinstance(failed_check, str) and failed_check:
        return failed_check
    return "none"


def _candidate_summary_tags(diagnostic: dict[str, object]) -> str:
    tags = diagnostic.get("narrative_tags")
    if isinstance(tags, (list, tuple)) and tags:
        safe_tags = [str(tag) for tag in tags[:3]]
        return ",".join(safe_tags)
    return "none"


def _candidate_summary_theme(diagnostic: dict[str, object]) -> str:
    theme_hint = diagnostic.get("theme_cluster_hint")
    if isinstance(theme_hint, str) and theme_hint:
        return theme_hint
    return _candidate_summary_tags(diagnostic)


def _accepted_candidate_sort_key(diagnostic: dict[str, object]) -> tuple[int, int, float, int, int]:
    holder_pct = _coerce_numeric(diagnostic.get("top10_holder_pct"))
    liquidity_sol = _coerce_numeric(diagnostic.get("selected_liquidity_sol"))
    warning_count = _warning_count(diagnostic)
    ranking_attention = diagnostic.get("ranking_attention_score", diagnostic.get("attention_score", 0))
    return (
        -int(ranking_attention if isinstance(ranking_attention, (int, float)) else 0),
        warning_count,
        holder_pct if holder_pct is not None else 10_000.0,
        -int(liquidity_sol) if liquidity_sol is not None else 0,
        int(diagnostic.get("rank", 10_000) if isinstance(diagnostic.get("rank"), int) else 10_000),
    )


def _accepted_candidate_diff(diagnostic: dict[str, object]) -> str:
    liquidity_sol = _coerce_numeric(diagnostic.get("selected_liquidity_sol"))
    holder_pct = _coerce_numeric(diagnostic.get("top10_holder_pct"))
    age_minutes = _coerce_numeric(diagnostic.get("token_age_minutes"))
    warning_count = _warning_count(diagnostic)
    social_state = diagnostic.get("social_signal_state", "unknown")
    meta_state = diagnostic.get("metadata_completeness_state", "unknown")
    parts = [f"warn={warning_count}"]
    if holder_pct is not None:
        parts.append(f"holder={holder_pct:.2f}%")
    else:
        parts.append("holder=?")
    if liquidity_sol is not None:
        parts.append(f"liq={liquidity_sol:.0f}")
    else:
        parts.append("liq=?")
    if age_minutes is not None:
        parts.append(f"age={age_minutes:.2f}m")
    else:
        parts.append("age=?")
    parts.append(f"social={social_state}")
    parts.append(f"meta={meta_state}")
    source_context = diagnostic.get("source_context_hint")
    if isinstance(source_context, str) and source_context:
        parts.append(source_context)
    confidence = _coerce_numeric(diagnostic.get("confidence"))
    weight = _coerce_numeric(diagnostic.get("weight"))
    if confidence is not None and weight is not None:
        parts.append(f"sig={confidence:.2f}x{weight:.2f}")
    penalty_points = diagnostic.get("ranking_penalty_points")
    penalty_reasons = diagnostic.get("ranking_penalty_reasons")
    if isinstance(penalty_points, int) and penalty_points > 0:
        if isinstance(penalty_reasons, (list, tuple)) and penalty_reasons:
            parts.append(f"pen={penalty_points}:{'+'.join(str(item) for item in penalty_reasons)}")
        else:
            parts.append(f"pen={penalty_points}")
    return ", ".join(parts)


def _accepted_candidate_note(diagnostic: dict[str, object]) -> str:
    holder_pct = _coerce_numeric(diagnostic.get("top10_holder_pct"))
    holder_source = diagnostic.get("top10_holder_source", "unknown")
    warning_count = _warning_count(diagnostic)
    meta_state = diagnostic.get("metadata_completeness_state", "unknown")
    parts: list[str] = []
    if holder_pct is not None and holder_pct >= 45.0:
        parts.append(f"near holder cutoff via {holder_source}")
    elif holder_pct is not None and holder_pct < 10.0:
        parts.append(f"clean holder profile via {holder_source}")
    if warning_count <= 2:
        parts.append("lighter warning stack")
    elif warning_count >= 5:
        parts.append("warning-heavy but passed")
    if meta_state == "partial":
        parts.append("metadata still partial")
    social_state = diagnostic.get("social_signal_state")
    if social_state == "missing":
        parts.append("social still missing")
    narrative_quality = diagnostic.get("narrative_quality_hint")
    if isinstance(narrative_quality, str) and narrative_quality:
        parts.append(f"narrative={narrative_quality}")
    penalty_reasons = diagnostic.get("ranking_penalty_reasons")
    if isinstance(penalty_reasons, (list, tuple)) and penalty_reasons:
        parts.append(f"rank-penalty={'+'.join(str(item) for item in penalty_reasons)}")
    return "; ".join(parts) if parts else "ranked by safe passer context"


def _grok_prompt_candidate_line(diagnostic: dict[str, object]) -> str:
    name = _stringish(diagnostic.get("name")) or "unknown"
    symbol = _stringish(diagnostic.get("symbol")) or "unknown"
    mint = _stringish(diagnostic.get("mint")) or "unknown"
    source = _stringish(diagnostic.get("source")) or "unknown"
    source_context = _stringish(diagnostic.get("source_context_hint")) or "unknown"
    stage_hint = _stringish(diagnostic.get("stage_hint")) or "unknown"
    attention = _candidate_attention_display(diagnostic)
    theme = _candidate_summary_theme(diagnostic)
    narrative = _stringish(diagnostic.get("narrative_quality_hint")) or "unknown"
    age = _candidate_age_display(diagnostic)
    warning_summary = _grok_warning_summary(diagnostic)
    return (
        f"rank={diagnostic.get('rank', '?')}; name={name}; symbol={symbol}; mint={mint}; "
        f"source={source}; source_context={source_context}; stage={stage_hint}; age={age}; "
        f"attention={attention}; theme={theme}; narrative={narrative}; warnings={warning_summary}; "
        f"wallet_cluster=pending/unavailable"
    )


def _candidate_age_display(diagnostic: dict[str, object]) -> str:
    age_minutes = _coerce_numeric(diagnostic.get("token_age_minutes"))
    if age_minutes is None:
        return "unknown"
    return f"{age_minutes:.2f}m"


def _grok_warning_summary(diagnostic: dict[str, object]) -> str:
    parts: list[str] = []
    holder_pct = _coerce_numeric(diagnostic.get("top10_holder_pct"))
    if holder_pct is not None:
        parts.append(f"holder={holder_pct:.2f}%")
    else:
        parts.append("holder=?")
    liquidity_sol = _coerce_numeric(diagnostic.get("selected_liquidity_sol"))
    if liquidity_sol is not None:
        parts.append(f"liq={liquidity_sol:.0f} SOL")
    else:
        parts.append("liq=?")
    social_state = diagnostic.get("social_signal_state")
    if social_state == "missing":
        parts.append("social=missing")
    warnings = diagnostic.get("main_warnings")
    if isinstance(warnings, (list, tuple)) and warnings:
        parts.append(f"warnings={','.join(str(item) for item in warnings[:3])}")
    else:
        parts.append("warnings=none")
    return ", ".join(parts)


def _warning_count(diagnostic: dict[str, object]) -> int:
    warnings = diagnostic.get("main_warnings")
    if isinstance(warnings, (list, tuple)):
        return sum(1 for item in warnings if isinstance(item, str) and item)
    return 0


def _candidate_mode_counts(
    accepted: list[dict[str, object]],
    rejected: list[dict[str, object]],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for diagnostic in [*accepted, *rejected]:
        mode = diagnostic.get("candidate_mode")
        if isinstance(mode, str) and mode:
            counts[mode] += 1
    return dict(sorted(counts.items()))


def _candidate_attention_display(diagnostic: dict[str, object]) -> str:
    attention_score = diagnostic.get("ranking_attention_score", diagnostic.get("attention_score", 0))
    if not isinstance(attention_score, (int, float)):
        attention_score = 0
    base_attention = diagnostic.get("attention_score", 0)
    penalty_points = diagnostic.get("ranking_penalty_points", 0)
    suffix = ""
    if isinstance(base_attention, (int, float)) and isinstance(penalty_points, int) and penalty_points > 0:
        suffix = f"(-{penalty_points})"
    return f"{int(attention_score)}/{diagnostic.get('attention_tier', 'ignore')}{suffix}"


def _coerce_numeric(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _candidate_theme_tokens(diagnostic: dict[str, object]) -> tuple[str, ...]:
    values = list(_candidate_name_tokens(diagnostic))
    tags = diagnostic.get("narrative_tags")
    if isinstance(tags, (list, tuple)):
        values.extend(_safe_theme_tag(str(tag)) for tag in tags[:3])

    tokens: list[str] = []
    for value in values:
        if not value:
            continue
        compact = "".join(character if character.isalnum() else " " for character in value)
        for token in compact.split():
            if len(token) >= 3:
                tokens.append(token)
    return tuple(dict.fromkeys(tokens))


def _candidate_priority_theme_tokens(diagnostic: dict[str, object]) -> tuple[str, ...]:
    values = list(_candidate_name_tokens(diagnostic))
    tags = diagnostic.get("narrative_tags")
    if isinstance(tags, (list, tuple)):
        for tag in tags[:3]:
            normalized = _safe_theme_tag(str(tag))
            if normalized and normalized not in {"liquid", "deep-liquidity"}:
                values.append(normalized)

    tokens: list[str] = []
    for value in values:
        if not value:
            continue
        compact = "".join(character if character.isalnum() else " " for character in value)
        for token in compact.split():
            if len(token) >= 3:
                tokens.append(token)
    return tuple(dict.fromkeys(tokens))


def _candidate_name_tokens(diagnostic: dict[str, object]) -> tuple[str, ...]:
    generic_clone_tokens = {"dog", "cat", "bull", "rat", "weasel", "honeycomb", "fatdog", "fatbull"}
    tokens: list[str] = []
    for field_name in ("symbol", "name"):
        value = diagnostic.get(field_name)
        if not isinstance(value, str) or not value.strip():
            continue
        normalized = value.strip().lower()
        compact = "".join(character for character in normalized if character.isalnum())
        if len(compact) >= 3:
            tokens.append(compact)
        for clone_token in generic_clone_tokens:
            if clone_token in compact:
                tokens.append(clone_token)
                tokens.append("clone")
    return tuple(dict.fromkeys(tokens))


def _safe_theme_tag(tag: str) -> str:
    normalized = tag.strip().lower()
    if normalized in {"fresh-launch", "pumpfun", "pumpfun-launch", "fresh", "launch"}:
        return ""
    return normalized


def _theme_cluster_hint(diagnostic: dict[str, object], token_counts: Counter[str]) -> str:
    repeated_priority = [token for token in _candidate_priority_theme_tokens(diagnostic) if token_counts[token] > 1]
    if repeated_priority:
        return f"cluster:{repeated_priority[0]}"
    repeated = [token for token in _candidate_theme_tokens(diagnostic) if token_counts[token] > 1]
    if repeated:
        return f"cluster:{repeated[0]}"
    momentum_context = _momentum_context_hint(diagnostic)
    if momentum_context in {"deep-liquidity", "liquid"}:
        return "cluster:liquid"
    return "distinct-theme"


def _name_quality_hint(diagnostic: dict[str, object], token_counts: Counter[str]) -> str:
    repeated = [token for token in _candidate_name_tokens(diagnostic) if token_counts[token] > 1]
    generic_clone_tokens = {"dog", "cat", "bull", "rat", "weasel", "honeycomb", "fatdog", "fatbull", "clone"}
    if any(token in generic_clone_tokens for token in repeated):
        return "generic-clone-like"
    if repeated:
        return "theme-repeated"
    return "differentiated-name"


def _source_context_hint(diagnostic: dict[str, object]) -> str:
    source_count = diagnostic.get("source_count")
    sources = diagnostic.get("sources")
    if isinstance(source_count, int) and source_count > 1:
        return f"multi-source:{source_count}"
    if isinstance(sources, (list, tuple)) and len(sources) > 1:
        return f"multi-source:{len(sources)}"
    source = diagnostic.get("source")
    if source == "pump_fun":
        return "single-source-launch"
    if source == "onchain":
        return "onchain-context"
    if source == "whale_tracker":
        return "whale-context"
    return "single-source"


def _momentum_context_hint(diagnostic: dict[str, object]) -> str:
    buy_sell = diagnostic.get("buy_sell_hint")
    liquidity = _coerce_numeric(diagnostic.get("selected_liquidity_sol"))
    if isinstance(buy_sell, str) and buy_sell.startswith("b") and "/s" in buy_sell:
        buys_part, sells_part = buy_sell[1:].split("/s", 1)
        try:
            buys = int(buys_part)
            sells = int(sells_part)
        except ValueError:
            buys = sells = 0
        if buys > sells:
            return "buy-pressure"
    if liquidity is not None and liquidity >= 1000:
        return "deep-liquidity"
    if liquidity is not None and liquidity >= 50:
        return "liquid"
    return "limited-context"


def _narrative_quality_hint(diagnostic: dict[str, object], token_counts: Counter[str]) -> str:
    name_quality = _name_quality_hint(diagnostic, token_counts)
    source_context = _source_context_hint(diagnostic)
    momentum_context = _momentum_context_hint(diagnostic)
    if name_quality == "differentiated-name" and source_context.startswith("multi-source"):
        return f"differentiated/{momentum_context}/{source_context}"
    if name_quality == "differentiated-name":
        return f"differentiated/{momentum_context}"
    if name_quality == "generic-clone-like":
        return f"clone-cluster/{momentum_context}"
    return f"theme-repeated/{momentum_context}"


def _check_result_value(record: RejectionRecord | None, field_name: str) -> str | None:
    if record is None:
        return None
    entry = record.check_results.get(field_name)
    if not isinstance(entry, dict):
        return None
    result = entry.get("result")
    return str(result).lower() if isinstance(result, str) else None


def _check_metric_value(record: RejectionRecord | None, field_name: str) -> object | None:
    if record is None:
        return None
    entry = record.check_results.get(field_name)
    if not isinstance(entry, dict):
        return None
    return entry.get("value")


def _first_present(*mappings: dict[str, object], keys: tuple[str, ...]) -> object | None:
    for mapping in mappings:
        for key in keys:
            value = mapping.get(key)
            if value is not None:
                return value
    return None


def _stringish(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _short_mint(mint: str) -> str:
    if len(mint) <= 12:
        return mint
    return f"{mint[:4]}...{mint[-4:]}"


def _liquidity_display(result: str | None, value: object | None) -> str:
    if isinstance(value, (int, float)):
        return f"{float(value):.4f}"
    if result == "unknown":
        return "unknown"
    if result == "fail":
        return "fail"
    return "unknown"


def _buy_sell_hint(metrics: dict[str, object]) -> str:
    buys = metrics.get("buys_m5")
    sells = metrics.get("sells_m5")
    if isinstance(buys, (int, float)) and isinstance(sells, (int, float)):
        return f"b{int(buys)}/s{int(sells)}"
    return "unknown"


def _social_hint(social: dict[str, object]) -> str:
    if not social:
        return "unknown"
    tier = social.get("highest_tier")
    unique_accounts = social.get("unique_accounts")
    parts: list[str] = []
    if tier is not None:
        parts.append(f"tier={tier}")
    if unique_accounts is not None:
        parts.append(f"accounts={unique_accounts}")
    return ",".join(parts) if parts else "known"


def _creator_holding_display(creator_diagnostics: dict[str, object]) -> str:
    value = creator_diagnostics.get("creator_holding_pct")
    source = creator_diagnostics.get("creator_holding_source")
    state = creator_diagnostics.get("creator_holding_state")
    if isinstance(value, (int, float)):
        return f"{float(value):.4f}@{source}"
    if state == "unknown":
        return "unknown"
    return "unknown"


def _attention_hints(
    payload: dict[str, object],
    *,
    social: dict[str, object],
    market_cap: object | None,
    volume: object | None,
    buy_sell: str,
) -> str:
    hints: list[str] = []
    if payload.get("pool") == "raydium" or any(
        keyword in str(payload.get(field, "")).lower()
        for field in ("txType", "event", "type")
        for keyword in ("migrate", "graduat")
    ):
        hints.append("graduation")
    if isinstance(market_cap, (int, float)):
        hints.append(f"mc={float(market_cap):.2f}")
    if isinstance(volume, (int, float)):
        hints.append(f"vol={float(volume):.2f}")
    if buy_sell != "unknown":
        hints.append(buy_sell)
    social_summary = _social_hint(social)
    if social_summary != "unknown":
        hints.append(f"social:{social_summary}")
    return ", ".join(hints) if hints else "none"


def _top10_holder_source_hint(payload: dict[str, object], record: RejectionRecord | None) -> str:
    holder_diagnostics = payload.get("holder_diagnostics")
    if isinstance(holder_diagnostics, dict):
        source = holder_diagnostics.get("top10_holder_source")
        if isinstance(source, str) and source:
            return source
    if any(key in payload for key in ("top10HolderPct", "top10HolderPercent", "holderConcentrationTop10Pct")):
        return "signal_payload"
    if record is not None and _check_metric_value(record, "top10_holder_check") is not None:
        return "risk_enrichment_unknown"
    return "unknown"


def _diagnostic_note(payload: dict[str, object], record: RejectionRecord | None) -> str:
    notes: list[str] = []
    if record is not None and _check_result_value(record, "liquidity_check") == "unknown":
        notes.append("liquidity metric missing at evaluation time")
    if record is not None and _check_result_value(record, "top10_holder_check") == "fail":
        notes.append("holder concentration threshold failed")
    if payload.get("social_credibility") is None:
        notes.append("no social credibility metadata present")
    return "; ".join(notes) if notes else "none"


def build_rejection_diagnostic_report(summary: PaperCycleSummary) -> str:
    lines = [
        "MT-038 Per-Token Rejection Diagnostics",
        f"generated_at_utc: {datetime.now(UTC).isoformat()}",
        f"execution_mode: {summary.execution_mode}",
        f"risk_profile: {summary.risk_profile}",
        f"max_signals: {summary.max_signals}",
        f"timeout_seconds: {summary.timeout_seconds:g}",
        f"signals_collected: {summary.signals_collected}",
        f"candidates_evaluated: {summary.candidates_evaluated}",
        f"passed_risk_checks: {summary.passed_risk_checks}",
        f"rejected: {summary.candidates_evaluated - summary.passed_risk_checks}",
        f"paper_trades_executed: {summary.trades_persisted}",
        "",
        "Accepted/passed candidate snapshots:",
    ]
    if summary.accepted_candidate_diagnostics:
        for diagnostic in summary.accepted_candidate_diagnostics:
            lines.extend(
                [
                    "",
                    f"[{diagnostic.get('rank', '?')}] {diagnostic.get('symbol', 'unknown')} ({diagnostic.get('mint_short', 'unknown')})",
                    f"mint: {diagnostic.get('mint', 'unknown')}",
                    f"source: {diagnostic.get('source', 'unknown')}",
                    f"decision: {diagnostic.get('decision', 'unknown')}",
                    f"action_outcome: {diagnostic.get('action_outcome', 'unknown')}",
                    f"attention_score: {diagnostic.get('attention_score', 0)}",
                    f"attention_tier: {diagnostic.get('attention_tier', 'ignore')}",
                    f"attention_reasons: {diagnostic.get('attention_reasons', ())}",
                    f"narrative_tags: {diagnostic.get('narrative_tags', ())}",
                    f"social_signal_state: {diagnostic.get('social_signal_state', 'missing')}",
                    f"metadata_completeness_state: {diagnostic.get('metadata_completeness_state', 'sparse')}",
                    f"token_age_minutes: {diagnostic.get('token_age_minutes', 'unknown') if diagnostic.get('token_age_minutes') is not None else 'unknown'}",
                    f"stage_hint: {diagnostic.get('stage_hint', 'unknown')}",
                    f"selected_liquidity_sol: {diagnostic.get('selected_liquidity_sol', 'unknown')}",
                    f"selected_liquidity_usd: {diagnostic.get('selected_liquidity_usd', 'unknown')}",
                    f"liquidity_source: {diagnostic.get('liquidity_source', 'unknown')}",
                    f"liquidity_data_state: {diagnostic.get('liquidity_data_state', 'unknown')}",
                    f"top10_holder_pct: {diagnostic.get('top10_holder_pct', 'unknown')}",
                    f"top10_holder_source: {diagnostic.get('top10_holder_source', 'unknown')}",
                    f"holder_policy_state: {diagnostic.get('holder_policy_state', 'unknown')}",
                    f"creator_policy_state: {diagnostic.get('creator_policy_state', 'unknown')}",
                    f"unique_buyers_policy_state: {diagnostic.get('unique_buyers_policy_state', 'unknown')}",
                    f"authority_policy_state: {diagnostic.get('authority_policy_state', 'unknown')}",
                    f"honeypot_policy_state: {diagnostic.get('honeypot_policy_state', 'unknown')}",
                    f"main_warnings: {diagnostic.get('main_warnings', ())}",
                    f"narrative_quality_hint: {diagnostic.get('narrative_quality_hint', 'unknown')}",
                    f"theme_cluster_hint: {diagnostic.get('theme_cluster_hint', 'unknown')}",
                    f"name_quality_hint: {diagnostic.get('name_quality_hint', 'unknown')}",
                    f"source_context_hint: {diagnostic.get('source_context_hint', 'unknown')}",
                    f"momentum_context_hint: {diagnostic.get('momentum_context_hint', 'unknown')}",
                    f"skip_or_rejection_reason: {diagnostic.get('rejection_reason', 'none')}",
                    f"attention_hints: {diagnostic.get('attention_hints', 'unknown')}",
                ]
            )
    else:
        lines.extend(["", "- none"])

    discovery_summary_lines = summary.discovery_candidate_summary_lines()
    if discovery_summary_lines:
        lines.extend(["", *discovery_summary_lines])
    accepted_comparison_lines = summary.discovery_comparison_lines()
    if accepted_comparison_lines:
        lines.extend(["", *accepted_comparison_lines])
    grok_prompt_lines = summary.discovery_grok_prompt_lines()
    if grok_prompt_lines:
        lines.extend(["", *grok_prompt_lines])

    lines.extend([
        "",
        "Aggregate rejection reasons:",
    ])
    if summary.summary_rejection_reasons:
        lines.extend(f"- {reason}: {count}" for reason, count in summary.summary_rejection_reasons.items())
    else:
        lines.append("- none")

    lines.extend(["", "Per-candidate sections:"])
    for diagnostic in summary.rejected_candidate_diagnostics:
        lines.extend(
            [
                "",
                f"[{diagnostic.get('rank', '?')}] {diagnostic.get('symbol', 'unknown')} ({diagnostic.get('mint_short', 'unknown')})",
                f"mint: {diagnostic.get('mint', 'unknown')}",
                f"source: {diagnostic.get('source', 'unknown')}",
                f"decision: {diagnostic.get('decision', 'unknown')}",
                f"action_outcome: {diagnostic.get('action_outcome', 'unknown')}",
                f"failed_check: {diagnostic.get('failed_check', 'unknown')}",
                f"rejection_reason: {diagnostic.get('rejection_reason', 'unknown')}",
                f"risk_score: {diagnostic.get('risk_score', 'unknown') if diagnostic.get('risk_score') is not None else 'unknown'}",
                f"attention_score: {diagnostic.get('attention_score', 0)}",
                f"attention_tier: {diagnostic.get('attention_tier', 'ignore')}",
                f"attention_reasons: {diagnostic.get('attention_reasons', ())}",
                f"narrative_tags: {diagnostic.get('narrative_tags', ())}",
                f"social_signal_state: {diagnostic.get('social_signal_state', 'missing')}",
                f"metadata_completeness_state: {diagnostic.get('metadata_completeness_state', 'sparse')}",
                f"rugcheck_top10_holder_pct: {diagnostic.get('rugcheck_top10_holder_pct', 'unknown')}",
                f"local_filtered_top10_holder_pct: {diagnostic.get('local_filtered_top10_holder_pct', 'unknown')}",
                f"selected_top10_holder_pct: {diagnostic.get('selected_top10_holder_pct', 'unknown')}",
                f"top10_holder_pct: {diagnostic.get('top10_holder_pct', 'unknown')}",
                f"top10_holder_source: {diagnostic.get('top10_holder_source', 'unknown')}",
                f"bonding_curve_addresses: {diagnostic.get('bonding_curve_addresses', ())}",
                f"local_holder_raw_account_count: {diagnostic.get('local_holder_raw_account_count', 0)}",
                f"local_holder_filtered_account_count: {diagnostic.get('local_holder_filtered_account_count', 0)}",
                f"local_holder_retained_account_count: {diagnostic.get('local_holder_retained_account_count', 0)}",
                f"local_holder_top_filtered_accounts: {diagnostic.get('local_holder_top_filtered_accounts', ())}",
                f"local_holder_top_retained_accounts: {diagnostic.get('local_holder_top_retained_accounts', ())}",
                f"creator_holding_pct: {diagnostic.get('creator_holding_pct', 'unknown')}",
                f"creator_holding_source: {diagnostic.get('creator_holding_source', 'unknown')}",
                f"creator_holding_state: {diagnostic.get('creator_holding_state', 'unknown')}",
                f"creator_holding_unknown_reason: {diagnostic.get('creator_holding_unknown_reason', 'unknown')}",
                f"creator_policy_state: {diagnostic.get('creator_policy_state', 'unknown')}",
                f"creator_policy_reason: {diagnostic.get('creator_policy_reason', 'unknown')}",
                f"creator_policy_context_used: {diagnostic.get('creator_policy_context_used', False)}",
                f"unique_buyers_count: {diagnostic.get('unique_buyers_count', 'unknown')}",
                f"unique_buyers_source: {diagnostic.get('unique_buyers_source', 'unknown')}",
                f"unique_buyers_state: {diagnostic.get('unique_buyers_state', 'unknown')}",
                f"unique_buyers_unknown_reason: {diagnostic.get('unique_buyers_unknown_reason', 'unknown')}",
                f"unique_buyers_policy_state: {diagnostic.get('unique_buyers_policy_state', 'unknown')}",
                f"unique_buyers_policy_reason: {diagnostic.get('unique_buyers_policy_reason', 'unknown')}",
                f"unique_buyers_policy_context_used: {diagnostic.get('unique_buyers_policy_context_used', False)}",
                f"mint_authority_state: {diagnostic.get('mint_authority_state', 'unknown')}",
                f"freeze_authority_state: {diagnostic.get('freeze_authority_state', 'unknown')}",
                f"authority_source: {diagnostic.get('authority_source', 'unknown')}",
                f"authority_unknown_reason: {diagnostic.get('authority_unknown_reason', 'unknown')}",
                f"authority_policy_state: {diagnostic.get('authority_policy_state', 'unknown')}",
                f"authority_policy_reason: {diagnostic.get('authority_policy_reason', 'unknown')}",
                f"authority_policy_context_used: {diagnostic.get('authority_policy_context_used', False)}",
                f"honeypot_state: {diagnostic.get('honeypot_state', 'unknown')}",
                f"honeypot_source: {diagnostic.get('honeypot_source', 'unknown')}",
                f"honeypot_unknown_reason: {diagnostic.get('honeypot_unknown_reason', 'unknown')}",
                f"honeypot_policy_state: {diagnostic.get('honeypot_policy_state', 'unknown')}",
                f"honeypot_policy_reason: {diagnostic.get('honeypot_policy_reason', 'unknown')}",
                f"honeypot_policy_context_used: {diagnostic.get('honeypot_policy_context_used', False)}",
                f"holder_policy_state: {diagnostic.get('holder_policy_state', 'unknown')}",
                f"holder_policy_reason: {diagnostic.get('holder_policy_reason', 'unknown')}",
                f"token_age_minutes: {diagnostic.get('token_age_minutes', 'unknown') if diagnostic.get('token_age_minutes') is not None else 'unknown'}",
                f"stage_hint: {diagnostic.get('stage_hint', 'unknown')}",
                f"fresh_launch_context_used: {diagnostic.get('fresh_launch_context_used', False)}",
                f"age_policy_state: {diagnostic.get('age_policy_state', 'unknown')}",
                f"age_policy_reason: {diagnostic.get('age_policy_reason', 'unknown')}",
                f"age_policy_context_used: {diagnostic.get('age_policy_context_used', False)}",
                f"age_policy_age_minutes: {diagnostic.get('age_policy_age_minutes', 'unknown') if diagnostic.get('age_policy_age_minutes') is not None else 'unknown'}",
                f"age_policy_stage_hint: {diagnostic.get('age_policy_stage_hint', 'unknown')}",
                f"selected_liquidity_sol: {diagnostic.get('selected_liquidity_sol', 'unknown')}",
                f"selected_liquidity_usd: {diagnostic.get('selected_liquidity_usd', 'unknown')}",
                f"liquidity_source: {diagnostic.get('liquidity_source', 'unknown')}",
                f"liquidity_data_state: {diagnostic.get('liquidity_data_state', 'unknown')}",
                f"liquidity_unknown_reason: {diagnostic.get('liquidity_unknown_reason', 'unknown')}",
                f"dexscreener_liquidity_sol: {diagnostic.get('dexscreener_liquidity_sol', 'unknown')}",
                f"dexscreener_liquidity_usd: {diagnostic.get('dexscreener_liquidity_usd', 'unknown')}",
                f"dexscreener_status: {diagnostic.get('dexscreener_status', 'unknown')}",
                f"jupiter_liquidity_sol: {diagnostic.get('jupiter_liquidity_sol', 'unknown')}",
                f"jupiter_liquidity_usd: {diagnostic.get('jupiter_liquidity_usd', 'unknown')}",
                f"jupiter_status: {diagnostic.get('jupiter_status', 'unknown')}",
                f"fallback_attempted: {diagnostic.get('fallback_attempted', False)}",
                f"fallback_succeeded: {diagnostic.get('fallback_succeeded', False)}",
                f"liquidity: {diagnostic.get('liquidity_display', 'unknown')}",
                f"liquidity_state: {diagnostic.get('liquidity_state', 'unknown')}",
                f"honeypot_check: {diagnostic.get('honeypot_check', 'unknown')}",
                f"authority_check: {diagnostic.get('authority_check', 'unknown')}",
                f"funding_check: {diagnostic.get('funding_check', 'unknown')}",
                f"market_cap_hint: {diagnostic.get('market_cap_hint', 'unknown')}",
                f"volume_hint: {diagnostic.get('volume_hint', 'unknown')}",
                f"buy_sell_hint: {diagnostic.get('buy_sell_hint', 'unknown')}",
                f"graduation_flag: {diagnostic.get('graduation_flag', 'unknown')}",
                f"social_hint: {diagnostic.get('social_hint', 'unknown')}",
                f"attention_hints: {diagnostic.get('attention_hints', 'unknown')}",
                f"main_warnings: {diagnostic.get('main_warnings', ())}",
                f"notes: {diagnostic.get('notes', 'none')}",
            ]
        )

    rugcheck_holder_failures = sum(
        1
        for diagnostic in summary.rejected_candidate_diagnostics
        if diagnostic.get("failed_check") == "top10_holder_check"
        and diagnostic.get("top10_holder_source") == "risk_enrichment_unknown"
    )
    liquidity_failures = sum(
        1 for diagnostic in summary.rejected_candidate_diagnostics if diagnostic.get("liquidity_state") in {"fail", "unknown"}
    )
    has_attention_hints = any(
        diagnostic.get("attention_hints") not in {"none", "unknown"}
        for diagnostic in summary.rejected_candidate_diagnostics
    )
    lines.extend(
        [
            "",
            "Interpretation:",
            f"- RugCheck/local enrichment holder-driven failures: {rugcheck_holder_failures}",
            f"- Liquidity fail/unknown count among rejected candidates: {liquidity_failures}",
            f"- Signal payloads contain attention-scoring hints: {'yes' if has_attention_hints else 'no'}",
        ]
    )
    return "\n".join(lines) + "\n"


def write_rejection_diagnostic_report(summary: PaperCycleSummary, path: Path) -> None:
    report = build_rejection_diagnostic_report(summary)
    path.write_text(report, encoding="utf-8")
    summary.diagnostic_report_path = str(path)


@dataclass(slots=True)
class PaperSoakAudit:
    cycle: PaperCycleSummary
    health_ok: bool
    health_message: str
    guardrail_diagnostics: tuple[str, ...]
    circuit_breaker_diagnostics: tuple[str, ...]
    readiness_checks: tuple[dict[str, object], ...]

    def lines(self) -> list[str]:
        lines = [
            "═══ Paper Soak Audit ═══",
            f"Signals scanned:           {self.cycle.signals_collected}",
            f"Candidates accepted:       {self.cycle.signals_accepted}",
            f"Candidates rejected:       {self.cycle.candidates_evaluated - self.cycle.passed_risk_checks}",
            f"Paper trades entered:      {self.cycle.trades_persisted}",
            f"Eval session scope:        {self.cycle.evaluation_session_scope}",
            f"Skipped trades:            {self.cycle.signals_accepted - self.cycle.trades_persisted}",
            f"Guardrail diagnostics:     {','.join(self.guardrail_diagnostics) if self.guardrail_diagnostics else 'none'}",
            f"Circuit breaker (paper):   {','.join(self.circuit_breaker_diagnostics) if self.circuit_breaker_diagnostics else 'clear'}",
            f"Health status:             {'ok' if self.health_ok else self.health_message}",
        ]

        readiness_state = "ready" if all(check["ok"] for check in self.readiness_checks) else "not_ready"
        lines.append(f"Live readiness:            {readiness_state} (diagnostic only — does not affect paper mode)")
        for check in self.readiness_checks:
            diag = ",".join(check["diagnostics"]) if check["diagnostics"] else "none"
            lines.append(f"  {check['name']}: {'ok' if check['ok'] else 'not_ready'} ({diag})")

        if self.cycle.source_failures:
            lines.append("Source failures:")
            for source, count in self.cycle.source_failures.items():
                lines.append(f"  - {source}: {count}")
        else:
            lines.append("Source failures:           none")

        risk_reasons: dict[str, int] = {}
        capacity_reasons: dict[str, int] = {}
        unknown_reasons: dict[str, int] = {}
        for reason, count in (self.cycle.rejection_reasons or {}).items():
            if "_unknown" in reason:
                unknown_reasons[reason] = count
            elif "max_" in reason:
                capacity_reasons[reason] = count
            else:
                risk_reasons[reason] = count

        if risk_reasons:
            lines.append("Risk rejections:")
            for reason, count in sorted(risk_reasons.items(), key=lambda item: -item[1]):
                lines.append(f"  - {reason}: {count}")
        else:
            lines.append("Risk rejections:           none")

        if capacity_reasons:
            lines.append("Portfolio/capacity blocks:")
            for reason, count in sorted(capacity_reasons.items(), key=lambda item: -item[1]):
                if reason == "max_open_positions_reached":
                    lines.append(f"  - max_open_positions_reached: {count}")
                    lines.append(f"    configured_max_open_positions={self.cycle.configured_max_open_positions}")
                    lines.append(f"    starting_open_positions={self.cycle.starting_open_positions}")
                    lines.append(f"    persisted_open_positions={self.cycle.persisted_open_positions}")
                else:
                    lines.append(f"  - {reason}: {count}")
        else:
            lines.append("Portfolio/capacity blocks: none")

        if unknown_reasons:
            lines.append("Missing/unknown data blocks:")
            for reason, count in sorted(unknown_reasons.items(), key=lambda item: -item[1]):
                lines.append(f"  - {reason}: {count}")
        else:
            lines.append("Missing/unknown data blocks: none")

        execution_failures: dict[str, int] = {}
        for reason, count in (self.cycle.summary_rejection_reasons or {}).items():
            if "execution" in reason or "adapter" in reason or "unknown_or_other" in reason:
                execution_failures[reason] = count
        if execution_failures:
            lines.append("Execution/adapter failures:")
            for reason, count in sorted(execution_failures.items(), key=lambda item: -item[1]):
                lines.append(f"  - {reason}: {count}")
        else:
            lines.append("Execution/adapter failures: none")

        lines.append("═══════════════════════════")
        return lines


def _count_unexpected_failures(cycle: PaperCycleSummary) -> int:
    count = 0
    for reason, c in (cycle.summary_rejection_reasons or {}).items():
        if "execution" in reason or "adapter" in reason or "unknown_or_other" in reason:
            count += c
    return count


async def run_paper_soak(
    max_signals: int = 20,
    timeout_seconds: float = 60.0,
    *,
    risk_profile: str = "discovery",
    fresh_evaluation_session: bool = True,
    persist_positions: bool = False,
    db_path: str | Path | None = None,
    sources: list[SignalSource] | None = None,
) -> PaperSoakAudit:
    effective_fresh = fresh_evaluation_session and not persist_positions
    cycle = await run_bounded_paper_cycle(
        max_signals=max_signals,
        timeout_seconds=timeout_seconds,
        risk_profile=risk_profile,
        fresh_evaluation_session=effective_fresh,
        db_path=db_path,
        sources=sources,
    )

    settings = load_settings()
    health_status = check_health()

    guardrails = evaluate_live_guardrails(settings)
    guardrail_diagnostics = ("paper_mode_unaffected",) if settings.execution.mode != "live" else guardrails.diagnostics

    breaker = LiveCircuitBreaker()
    breaker_decision = breaker.status(execution_mode="paper")
    circuit_breaker_diagnostics = breaker_decision.diagnostics if not breaker_decision.allowed else ("paper_mode_unaffected",)

    readiness = await evaluate_micro_live_readiness(settings, circuit_breaker=breaker)
    readiness_checks = [
        {"name": check.name, "ok": check.ok, "diagnostics": list(check.diagnostics)}
        for check in readiness.checks
    ]

    audit = PaperSoakAudit(
        cycle=cycle,
        health_ok=health_status.ok,
        health_message=health_status.message,
        guardrail_diagnostics=guardrail_diagnostics,
        circuit_breaker_diagnostics=circuit_breaker_diagnostics,
        readiness_checks=readiness_checks,
    )

    runtime_db_path = resolve_db_path(db_path)
    await init_db(runtime_db_path)

    import json

    capacity_reasons: dict[str, int] = {}
    unknown_reasons: dict[str, int] = {}
    risk_reasons: dict[str, int] = {}
    for reason, count in (cycle.rejection_reasons or {}).items():
        if "_unknown" in reason:
            unknown_reasons[reason] = count
        elif "max_" in reason:
            capacity_reasons[reason] = count
        else:
            risk_reasons[reason] = count

    capacity_total = sum(capacity_reasons.values())
    unknown_total = sum(unknown_reasons.values())

    soak_record = SoakRunRecord(
        max_signals=cycle.max_signals,
        timeout_seconds=cycle.timeout_seconds,
        execution_mode=cycle.execution_mode,
        risk_profile=cycle.risk_profile,
        signals_collected=cycle.signals_collected,
        signals_accepted=cycle.signals_accepted,
        signals_rejected=cycle.signals_rejected,
        trades_persisted=cycle.trades_persisted,
        open_positions=cycle.open_positions,
        source_failures_json=json.dumps(cycle.source_failures),
        rejection_reasons_json=json.dumps(risk_reasons),
        capacity_blocked=capacity_total,
        unknown_data_blocks=unknown_total,
        unexpected_failures=_count_unexpected_failures(cycle),
        termination_reason=cycle.termination_reason,
        elapsed_seconds=cycle.elapsed_seconds,
        health_ok=health_status.ok,
        health_message=health_status.message,
        guardrail_diagnostics_json=json.dumps(list(guardrail_diagnostics)),
        circuit_breaker_diagnostics_json=json.dumps(list(circuit_breaker_diagnostics)),
        readiness_json=json.dumps(list(readiness_checks)),
    )
    await record_soak_run(runtime_db_path, soak_record)

    return audit


@app.command()
def health() -> None:
    status = check_health()
    console.print({"ok": status.ok, "message": status.message, "checked_at": status.checked_at})


@app.command("show-config")
def show_config() -> None:
    settings = load_settings()
    config_dump = settings.model_dump()
    live_guardrails = config_dump.get("live_guardrails")
    if isinstance(live_guardrails, dict) and "confirmation_phrase" in live_guardrails:
        live_guardrails["confirmation_phrase"] = "<redacted>"
    execution = config_dump.get("execution")
    if isinstance(execution, dict):
        for key in ("primary_rpc_url", "backup_rpc_url"):
            if execution.get(key):
                execution[key] = "<redacted>"
    config_dump["live_guardrails_diagnostics"] = evaluate_live_guardrails(settings).as_dict()
    config_dump["live_execution_config_diagnostics"] = evaluate_live_execution_config(settings).as_dict()
    console.print(config_dump)


@app.command("paper-cycle")
def paper_cycle(
    max_signals: int = typer.Option(5, min=1, help="Maximum number of signals to evaluate before stopping."),
    timeout_seconds: float = typer.Option(30.0, min=0.0, help="Maximum wall-clock runtime before stopping."),
    risk_profile: str = typer.Option("strict", "--mode", help="Risk profile: strict or discovery."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
    fresh_evaluation_session: bool = typer.Option(
        False,
        "--fresh-evaluation-session",
        help="Paper-only: ignore previously persisted paper positions for this bounded evaluation run.",
    ),
) -> None:
    try:
        normalized_risk_profile = normalize_risk_profile(risk_profile)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--mode") from exc

    summary = asyncio.run(
        run_bounded_paper_cycle(
            max_signals=max_signals,
            timeout_seconds=timeout_seconds,
            risk_profile=normalized_risk_profile,
            fresh_evaluation_session=fresh_evaluation_session,
            db_path=db_path,
        )
    )
    if not MT038_REPORT_PATH.parent.is_dir():
        raise RuntimeError(f"Shared diagnostic directory missing: {MT038_REPORT_PATH.parent}")
    write_rejection_diagnostic_report(summary, MT038_REPORT_PATH)
    for line in summary.safe_lines():
        console.print(line)


@app.command("paper-soak")
def paper_soak(
    max_signals: int = typer.Option(20, min=1, help="Maximum number of signals to evaluate before stopping."),
    timeout_seconds: float = typer.Option(60.0, min=0.0, help="Maximum wall-clock runtime before stopping."),
    persist_positions: bool = typer.Option(
        False,
        "--persist-positions",
        help="Persist paper positions so they appear in paper-state/paper-pnl reports.",
    ),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    audit = asyncio.run(
        run_paper_soak(
            max_signals=max_signals,
            timeout_seconds=timeout_seconds,
            persist_positions=persist_positions,
            db_path=db_path,
        )
    )
    for line in audit.lines():
        console.print(line)


def _preflight_explainer(
    settings: Settings,
    manager: PositionManager,
) -> list[str]:
    """Rich preflight explainer for blocked micro-live commands.

    Includes env-readiness summary, provider status, readiness detail,
    and actionable next steps. No secrets printed.
    """
    simulator = try_create_transaction_simulator()
    balance = try_create_balance_lookup()
    holdings = try_create_holdings_lookup()

    env = evaluate_env_readiness()

    env_present = sum(1 for i in env.items if i.present)
    env_total = len(env.items)
    env_missing_names = sorted(i.name for i in env.items if not i.present)

    helius_key = "present" if any(i.present for i in env.items if i.name == "HELIUS_API_KEY") else "MISSING"
    pub_key = "present" if any(i.present for i in env.items if i.name == "TRADING_WALLET_PUBLIC_KEY") else "MISSING"
    priv_key = "present" if any(i.present for i in env.items if i.name == "TRADING_WALLET_PRIVATE_KEY") else "MISSING"

    report = asyncio.run(
        evaluate_micro_live_readiness(
            settings,
            position_manager=manager,
            wallet_balance_lookup=balance,
            transaction_simulator=simulator,
            wallet_holdings_lookup=holdings,
            circuit_breaker=LiveCircuitBreaker(),
        )
    )

    sim_status = "available" if simulator is not None else "unavailable"
    bal_status = "available" if balance is not None else "unavailable"
    hold_status = "available" if holdings is not None else "unavailable"

    lines = [
        "═══ Preflight Explainer ═══",
        f"Execution mode:           {settings.execution.mode}",
        f"Live trading:             {'ENABLED' if settings.execution.mode == 'live' else 'DISABLED'}",
        "",
        "--- Env Readiness ---",
        f"  Vars present:           {env_present}/{env_total}",
        f"  HELIUS_API_KEY:         {helius_key}",
        f"  TRADING_WALLET_PUBLIC_KEY: {pub_key}",
        f"  TRADING_WALLET_PRIVATE_KEY:{priv_key}",
        "",
        "--- Provider Status ---",
        f"  transaction_simulator:  {sim_status}",
        f"  wallet_balance_lookup:  {bal_status}",
        f"  wallet_holdings_lookup: {hold_status}",
        "",
        "--- Live Readiness ---",
    ]

    for check in report.checks:
        state = "ok" if check.ok else "BLOCKED"
        diag = ",".join(check.diagnostics) if check.diagnostics else "none"
        line = f"  {check.name}: {state} ({diag})"
        if check.recommended_env:
            line += f" needs={','.join(sorted(check.recommended_env))}"
        lines.append(line)

    if not report.ready or settings.execution.mode != "live":
        lines.append("")
        lines.append("--- Blocking Reasons ---")
        blockers: list[str] = []
        if settings.execution.mode != "live":
            blockers.append("execution_mode_not_live — set LIVE_TRADING_ENABLED=true and execution.mode=live")
        for check in report.checks:
            if not check.ok:
                for diag in check.diagnostics:
                    if diag not in blockers:
                        blockers.append(f"{diag}")
        for blocker in blockers:
            lines.append(f"  - {blocker}")

        lines.append("")
        lines.append("--- Missing Arming Gates ---")
        missing_gates: list[str] = []
        if any(i.name == "TRADING_WALLET_PUBLIC_KEY" and not i.present for i in env.items):
            missing_gates.append("TRADING_WALLET_PUBLIC_KEY — needed for balance/holdings checks")
        if any(i.name == "TRADING_WALLET_PRIVATE_KEY" and not i.present for i in env.items):
            missing_gates.append("TRADING_WALLET_PRIVATE_KEY — needed for signing (add only at smoke time)")
        if any(i.name == "LIVE_TRADING_ENABLED" and not i.present for i in env.items):
            missing_gates.append("LIVE_TRADING_ENABLED=true — arms live execution")
        if any(i.name == "LIVE_CONFIRMATION_PHRASE" and not i.present for i in env.items):
            missing_gates.append("LIVE_CONFIRMATION_PHRASE — required by guardrails")
        if any(i.name == "LIVE_KILL_SWITCH" and not i.present for i in env.items):
            missing_gates.append("LIVE_KILL_SWITCH=false — disable kill switch for live")
        if any(i.name == "MAX_LIVE_TRADE_SOL" and not i.present for i in env.items):
            missing_gates.append("MAX_LIVE_TRADE_SOL — tiny per-trade cap (e.g. 0.005)")
        if any(i.name == "MAX_LIVE_DAILY_TRADES" and not i.present for i in env.items):
            missing_gates.append("MAX_LIVE_DAILY_TRADES — daily count cap (e.g. 1)")
        if any(i.name == "MAX_LIVE_DAILY_LOSS_SOL" and not i.present for i in env.items):
            missing_gates.append("MAX_LIVE_DAILY_LOSS_SOL — daily loss limit (e.g. 0.02)")
        if any(i.name == "PRIMARY_RPC_URL" and not i.present for i in env.items):
            missing_gates.append("PRIMARY_RPC_URL — execution RPC endpoint")
        if any(i.name == "BACKUP_RPC_URL" and not i.present for i in env.items):
            missing_gates.append("BACKUP_RPC_URL — optional failover RPC")

        if missing_gates:
            for gate in missing_gates:
                lines.append(f"  - {gate}")
        else:
            lines.append("  (all env vars present — check readiness diagnostics above)")

        lines.append("")
        lines.append("--- Operator Next Steps ---")
        if env_present < env_total:
            lines.append(f"  1. Add {env_total - env_present} missing env var(s) to .env: {', '.join(env_missing_names)}")
        if settings.execution.mode != "live":
            lines.append("  2. Set LIVE_TRADING_ENABLED=true and execution.mode=live in .env")
        if not report.ready:
            lines.append("  3. Re-run live-readiness and confirm all checks ok")
        lines.append(f"  4. See docs/MICRO_LIVE_RUNBOOK.md for the full smoke procedure")
        lines.append(f"  5. See docs/WALLET_SETUP.md for safe disposable wallet creation")

    lines.append("═══════════════════════════")
    return lines


def _run_dry_report(
    settings: Settings,
    manager: PositionManager,
) -> list[str]:
    """Legacy wrapper — delegates to _preflight_explainer."""
    return _preflight_explainer(settings, manager)


@app.command("env-readiness")
def env_readiness() -> None:
    report = evaluate_env_readiness()
    for line in report.lines():
        console.print(line)


def _build_live_readiness_report(runtime_db_path: str | Path, settings: Settings):
    asyncio.run(init_db(runtime_db_path))
    manager = PositionManager(runtime_db_path, settings)
    simulator = try_create_transaction_simulator()
    balance = try_create_balance_lookup()
    holdings = try_create_holdings_lookup()
    return asyncio.run(
        evaluate_micro_live_readiness(
            settings,
            position_manager=manager,
            wallet_balance_lookup=balance,
            transaction_simulator=simulator,
            wallet_holdings_lookup=holdings,
            circuit_breaker=LiveCircuitBreaker(),
        )
    )


@app.command("live-readiness")
def live_readiness(
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    settings = load_settings()
    runtime_db_path = resolve_db_path(db_path)
    report = _build_live_readiness_report(runtime_db_path, settings)
    for line in report.lines():
        console.print(line)


@app.command("live-exit")
def live_exit(
    mint: str = typer.Option(..., "--mint", help="Mint address of the existing position to exit."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Dry-run mode: report readiness only, do not execute."),
) -> None:
    settings = load_settings()
    runtime_db_path = resolve_db_path(db_path)
    asyncio.run(init_db(runtime_db_path))
    manager = PositionManager(runtime_db_path, settings)
    if dry_run:
        for line in _run_dry_report(settings, manager):
            console.print(line)
        return
    adapter = JupiterLiveExecutionAdapter(settings=settings, circuit_breaker=LiveCircuitBreaker())
    result = asyncio.run(
        execute_guarded_live_exit(
            settings=settings,
            mint_address=mint,
            position_manager=manager,
            adapter=adapter,
            exit_transaction_builder=None,
            wallet_holdings_lookup=None,
            wallet_balance_lookup=None,
            transaction_simulator=None,
            circuit_breaker=LiveCircuitBreaker(),
        )
    )
    if result.ok:
        console.print({"ok": result.ok, "diagnostics": list(result.diagnostics), "provider": result.provider, "tx_signature": result.tx_signature})
    else:
        for line in _preflight_explainer(settings, manager):
            console.print(line)


@app.command("live-buy")
def live_buy(
    mint: str = typer.Option(..., "--mint", help="Mint address to buy."),
    amount_sol: float = typer.Option(..., "--amount-sol", min=0.000001, help="Requested SOL size."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Dry-run mode: report readiness only, do not execute."),
) -> None:
    settings = load_settings()
    runtime_db_path = resolve_db_path(db_path)
    asyncio.run(init_db(runtime_db_path))
    manager = PositionManager(runtime_db_path, settings)
    if dry_run:
        for line in _run_dry_report(settings, manager):
            console.print(line)
        return
    adapter = JupiterLiveExecutionAdapter(settings=settings, circuit_breaker=LiveCircuitBreaker())
    result = asyncio.run(
        execute_guarded_live_buy(
            settings=settings,
            mint_address=mint,
            amount_sol=amount_sol,
            position_manager=manager,
            adapter=adapter,
            buy_transaction_builder=None,
            wallet_holdings_lookup=None,
            wallet_balance_lookup=None,
            transaction_simulator=None,
            circuit_breaker=LiveCircuitBreaker(),
        )
    )
    if result.ok:
        console.print({"ok": result.ok, "diagnostics": list(result.diagnostics), "provider": result.provider, "tx_signature": result.tx_signature})
    else:
        for line in _preflight_explainer(settings, manager):
            console.print(line)


@app.command("paper-state")
def paper_state(
    cleanup: bool = typer.Option(False, "--cleanup", help="Close all open paper positions."),
    legacy: bool = typer.Option(False, "--legacy", help="List active legacy paper positions that are archive candidates."),
    archive_legacy: bool = typer.Option(False, "--archive-legacy", help="Archive active legacy paper positions from current paper reports."),
    confirm: bool = typer.Option(False, "--confirm", help="Required confirmation for cleanup."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    """Inspect or clean up paper-mode positions. Read-only by default."""
    settings = load_settings()
    runtime_db_path = resolve_db_path(db_path)
    asyncio.run(init_db(runtime_db_path))
    manager = PositionManager(runtime_db_path, settings)

    if cleanup and archive_legacy:
        console.print("[red]Choose either --cleanup or --archive-legacy, not both.[/red]")
        raise typer.Exit(code=1)

    if cleanup:
        if not confirm:
            asyncio.run(_print_paper_state(manager))
            console.print("\n[yellow]Use --confirm to close all paper positions. This does not affect live positions.[/yellow]")
            return

        count = asyncio.run(manager.close_paper_positions())
        remaining = asyncio.run(manager.get_all_open())
        live_count = sum(1 for p in remaining if p.mode == "live")
        console.print(f"Closed {count} paper position(s). {live_count} live position(s) untouched.")
        return

    if archive_legacy:
        if not confirm:
            asyncio.run(_print_paper_state(manager))
            asyncio.run(_print_legacy_paper_positions(manager))
            console.print("\n[yellow]Use --confirm with --archive-legacy to archive active legacy paper positions. Live positions are never touched.[/yellow]")
            return

        archived_count = asyncio.run(manager.archive_legacy_paper_positions())
        archived_total = len(asyncio.run(manager.get_archived_paper_positions()))
        live_count = sum(1 for p in asyncio.run(manager.get_all_open()) if p.mode == "live")
        console.print(
            f"Archived {archived_count} legacy paper position(s). "
            f"Archived total excluded from current reports: {archived_total}. "
            f"{live_count} live position(s) untouched."
        )
        console.print("[yellow]Paper archive is simulated bookkeeping. No live positions were changed.[/yellow]")
        return

    if legacy:
        asyncio.run(_print_paper_state(manager))
        asyncio.run(_print_legacy_paper_positions(manager))
        return

    asyncio.run(_print_paper_state(manager))


async def _print_paper_state(manager: PositionManager) -> None:
    """Print paper positions table to console."""
    all_open = await manager.get_all_open()
    paper_positions = [p for p in all_open if p.mode == "paper"]
    live_positions = [p for p in all_open if p.mode == "live"]
    archived_paper_positions = await manager.get_archived_paper_positions()

    console.print(f"Open paper positions: {len(paper_positions)}")
    console.print(f"Open live positions:  {len(live_positions)}")
    console.print(f"Total open positions: {len(all_open)}")
    console.print(f"Archived paper positions excluded: {len(archived_paper_positions)}")

    if paper_positions:
        console.print("\n--- Paper Positions ---")
        for pos in sorted(paper_positions, key=lambda p: p.opened_at):
            age_label = _position_age_label(pos)
            console.print(
                f"  mint={pos.mint_address[:16]}  "
                f"sol={pos.amount_sol:.4f}  "
                f"tokens={pos.token_amount:.0f}  "
                f"price={pos.entry_price_sol:.8f}  "
                f"quality={pos.fill_quality.value}  "
                f"age={age_label}  "
                f"opened={pos.opened_at.strftime('%H:%M:%S')}"
            )


async def _print_legacy_paper_positions(manager: PositionManager) -> None:
    """Print active legacy paper positions eligible for archive."""
    legacy_positions = await manager.get_legacy_paper_positions()
    console.print(f"\nLegacy paper positions eligible for archive: {len(legacy_positions)}")

    if not legacy_positions:
        console.print("  [yellow](no active legacy paper positions)[/yellow]")
        return

    for pos in sorted(legacy_positions, key=lambda p: p.opened_at):
        console.print(
            f"  mint={pos.mint_address[:16]}  "
            f"sol={pos.amount_sol:.4f}  "
            f"tokens={pos.token_amount:.0f}  "
            f"price={pos.entry_price_sol:.8f}  "
            f"quality={pos.fill_quality.value}"
        )


def _fill_quality_label(fill_quality: PaperFillQuality) -> str:
    if fill_quality == PaperFillQuality.PRICED_QUOTE:
        return "reliable"
    if fill_quality == PaperFillQuality.UNPRICED:
        return "unpriced"
    return "legacy/unknown"


def _fill_quality_confidence(fill_quality: PaperFillQuality) -> str:
    if fill_quality == PaperFillQuality.PRICED_QUOTE:
        return "high_confidence"
    if fill_quality == PaperFillQuality.UNPRICED:
        return "unavailable"
    return "low_confidence"


def _position_age_label(position: Position) -> str:
    if position.fill_quality == PaperFillQuality.UNPRICED or position.entry_price_sol <= 0:
        return "missing_mark"
    if position.fill_quality == PaperFillQuality.LEGACY_UNKNOWN:
        return "low_confidence_fill"
    age = datetime.now(UTC) - position.opened_at
    if age < timedelta(hours=1):
        return "fresh"
    if age < timedelta(hours=24):
        return "aging"
    return "stale"


def _close_suggestion(position: Position) -> str:
    label = _position_age_label(position)
    if label == "stale" or label == "aging":
        return f"Suggestion: review with `paper-close --preview --mint {position.mint_address[:16]}...`"
    if label == "missing_mark":
        return f"Suggestion: entry was unpriced — simulated PnL unavailable for `paper-close --preview`"
    return ""


def _report_confidence_label(confidence: str) -> str:
    if confidence == "high_confidence":
        return "[green]high_confidence[/green]"
    if confidence == "partial":
        return "[yellow]partial[/yellow]"
    return "[red]low_confidence[/red]"


def _print_mark_coverage(summary: PaperPnLSummary) -> None:
    console.print("\n--- Mark Coverage ---")
    console.print(f"  Open positions considered: {summary.open_positions}")
    console.print(f"  Usable marks: {summary.usable_mark_count}")
    console.print(f"  Without usable marks: {summary.unusable_mark_count}")
    for reason, count in sorted(summary.mark_reason_counts.items()):
        console.print(f"  {reason}: {count}")
    console.print(f"  Report confidence: {_report_confidence_label(summary.report_confidence)}")


def _paper_report_hints(summary: PaperPnLSummary, capacity_blocked: int = 0) -> list[str]:
    hints: list[str] = []
    if summary.open_positions == 0:
        hints.append("No open paper positions: run `paper-soak --max-signals 50` to generate a fresh bounded paper session.")
    if summary.mark_reason_counts.get("legacy_low_confidence", 0) > 0:
        hints.append("Legacy rows dominate current paper data: run `paper-state --legacy` and use `paper-state --archive-legacy --confirm` if you want to exclude them from current reports.")
    if summary.mark_reason_counts.get("unpriced_entry", 0) > 0:
        hints.append("Some paper entries were unpriced at fill time: unrealized PnL stays unavailable until future fills have quote data.")
    if any(summary.mark_reason_counts.get(reason, 0) > 0 for reason in ("no_pairs", "no_solana_pairs")):
        hints.append("DexScreener mark coverage is missing for some mints: these may be fake/mock mints or not yet indexed on Solana pairs.")
    if capacity_blocked > 0:
        hints.append("Recent paper-soak runs hit capacity blocks: inspect `paper-state` and close or archive stale paper positions before the next soak.")
    stale_count = sum(1 for p in summary.positions if _position_age_label(p) in ("stale", "aging"))
    if stale_count > 0:
        hints.append(f"{stale_count} paper position(s) are stale or aging: use `paper-close --preview --mint <addr>` to review simulated close PnL before closing.")
    if not hints and summary.report_confidence == "high_confidence":
        hints.append("Current paper PnL coverage is fully priced for active positions.")
    return hints


def _format_pnl_summary(summary: PaperPnLSummary) -> None:
    marks_label = "[green]live[/green]" if summary.marks_mode == "live" else "[yellow]unavailable[/yellow]"
    summary_line = (
        f"[bold]Paper PnL Summary[/bold]\n"
        f"  Marks: {marks_label}\n"
        f"  Total paper positions: {summary.total_positions}\n"
        f"  Open: {summary.open_positions}  |  Closed: {summary.closed_positions}\n"
        f"  Total SOL deployed (open): {summary.total_sol_deployed:.6f}\n"
        f"  Realized PnL: {_fmt_pnl(summary.realized_pnl_sol)}\n"
    )

    if summary.unrealized_incomplete:
        summary_line += (
            f"  Unrealized PnL: [yellow]mark_unavailable "
            f"({summary.mark_unavailable_count} position(s) without mark)[/yellow]\n"
        )
    else:
        summary_line += f"  Unrealized PnL: {_fmt_pnl(summary.unrealized_pnl_sol or 0.0)}\n"

    summary_line += "\n[yellow]WARNING: Paper PnL is simulated. Not real profit or loss.[/yellow]"
    console.print(summary_line)

    if summary.fill_quality_counts:
        console.print("\n--- Paper Fill Quality ---")
        for fill_quality in (
            PaperFillQuality.PRICED_QUOTE,
            PaperFillQuality.UNPRICED,
            PaperFillQuality.LEGACY_UNKNOWN,
        ):
            count = summary.fill_quality_counts.get(fill_quality.value, 0)
            console.print(f"  {fill_quality.value}: {count} ({_fill_quality_label(fill_quality)})")

    _print_mark_coverage(summary)

    hints = _paper_report_hints(summary)
    if hints:
        console.print("\n--- Actionable Hints ---")
        for hint in hints:
            console.print(f"  - {hint}")

    if summary.positions:
        console.print("\n--- Per-Position Detail ---")
        for pos in sorted(summary.positions, key=lambda p: p.mint_address):
            status_icon = "[green]OPEN[/green]" if pos.status == PositionStatus.OPEN else "[red]CLOSED[/red]"
            if pos.status == PositionStatus.CLOSED:
                pnl_str = _fmt_pnl(pos.realized_pnl_sol)
            elif pos.mark_unavailable:
                pnl_str = f"[yellow]unavailable ({pos.mark_reason})[/yellow]"
            else:
                pnl_str = _fmt_pnl(pos.unrealized_pnl_sol or 0.0)

            price_str = (
                f"entry={pos.entry_price_sol:.10f}"
                if pos.mark_price_sol is None
                else f"entry={pos.entry_price_sol:.10f} mark={pos.mark_price_sol:.10f}"
            )
            age_label = _position_age_label(pos)
            reason_str = f"  reason={pos.mark_reason}" if pos.mark_reason != "ok" and pos.mark_reason != "live_dexscreener" else ""
            confidence_str = f"  quality={pos.fill_quality.value} confidence={pos.pnl_confidence} age={age_label}"
            console.print(
                f"  mint={pos.mint_address[:16]}  {status_icon}  "
                f"sol={pos.amount_sol:.4f}  tokens={pos.token_amount:.0f}  "
                f"{price_str}  "
                f"pnl={pnl_str}{confidence_str}{reason_str}"
            )
            hint = _close_suggestion(pos)
            if hint:
                console.print(f"  {hint}")


def _display_soak_diagnostics(db_path: str | Path) -> None:
    import json

    try:
        runs = asyncio.run(get_recent_soak_runs(db_path, limit=3))
    except Exception:
        console.print("  [yellow](run 'paper-soak --max-signals 50' for current signal rejection stats)[/yellow]")
        return

    if not runs:
        console.print("  [yellow](run 'paper-soak --max-signals 50' for current signal rejection stats)[/yellow]")
        return

    for run in runs:
        started = run.started_at[:19] if run.started_at else "unknown"
        source_fails = json.loads(run.source_failures_json) if run.source_failures_json else {}
        source_fail_summary = ", ".join(f"{s}={c}" for s, c in source_fails.items()) if source_fails else "none"

        console.print(f"  Run at {started} ({run.risk_profile}, {run.execution_mode})")
        console.print(f"    Signals: {run.signals_collected} collected, {run.signals_accepted} accepted, {run.signals_rejected} rejected")
        console.print(f"    Trades entered: {run.trades_persisted}  |  Termination: {run.termination_reason} ({run.elapsed_seconds:.1f}s)")
        console.print(f"    Risk rejections: {sum(json.loads(run.rejection_reasons_json or '{}').values())}")
        console.print(f"    Capacity blocks: {run.capacity_blocked}  |  Unknown-data blocks: {run.unknown_data_blocks}")
        console.print(f"    Unexpected failures: {run.unexpected_failures}  |  Source failures: {source_fail_summary}")
        console.print("")


def _fmt_pnl(value: float | None) -> str:
    if value is None:
        return "[yellow]N/A[/yellow]"
    if value >= 0:
        return f"[green]+{value:.6f} SOL[/green]"
    return f"[red]{value:.6f} SOL[/red]"


@app.command("paper-pnl")
def paper_pnl(
    marks: str = typer.Option("unavailable", "--marks", help="Mark price source: 'unavailable' (default, no network) or 'live' (DexScreener read-only)."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    """Show paper PnL summary with per-position detail."""
    settings = load_settings()
    runtime_db_path = resolve_db_path(db_path)
    asyncio.run(init_db(runtime_db_path))
    manager = PositionManager(runtime_db_path, settings)

    if marks not in {"live", "unavailable"}:
        raise typer.BadParameter("must be 'unavailable' or 'live'", param_hint="--marks")
    if marks == "live":
        provider = DexScreenerPriceProvider()
    else:
        provider = UnavailablePriceProvider()

    calculator = PaperPnLCalculator(manager, price_provider=provider)
    summary = asyncio.run(calculator.compute_summary())
    _format_pnl_summary(summary)


def _resolve_close_price(
    mint_address: str,
    manual_price: float | None,
    use_mark: bool,
    position: Position | None = None,
) -> tuple[float | None, str]:
    """Resolve exit price and return (price_sol, source).

    Source is 'manual', 'live_mark', or 'unavailable'.
    """
    if _is_valid_price(manual_price):
        return manual_price, "manual"
    if use_mark:
        provider = DexScreenerPriceProvider()
        try:
            mark = asyncio.run(provider.get_current_price(mint_address))
            if _is_valid_price(mark):
                return mark, "live_mark"
        except Exception:
            pass
    if position is not None and _is_valid_price(position.close_price_sol):
        return position.close_price_sol, "manual"
    return None, "unavailable"


def _print_paper_close_preview_position(
    pos: Position,
    exit_price: float | None,
    source: str,
) -> float:
    """Print single position preview line. Returns estimated PnL."""
    pnl = None
    confidence = _fill_quality_confidence(pos.fill_quality)
    quality_label = pos.fill_quality.value
    if _is_valid_price(exit_price) and pos.remaining_token_amount > 0:
        pnl = round(
            pos.remaining_token_amount * exit_price
            - pos.amount_sol * pos.remaining_sell_pct,
            9,
        )

    if pos.fill_quality == PaperFillQuality.UNPRICED:
        pnl = None

    src_label = f"({source})" if source != "unavailable" else ""
    pnl_str = _fmt_pnl(pnl) if pnl is not None else "[yellow]N/A[/yellow]"
    price_str = f"{exit_price:.10f} {src_label}" if exit_price is not None else "[yellow]unavailable[/yellow]"
    entry_str = f"{pos.entry_price_sol:.10f}"

    console.print(
        f"  {pos.mint_address[:16]}  "
        f"Entry={entry_str}  "
        f"Exit={price_str}  "
        f"Est.PnL={pnl_str}  quality={quality_label} confidence={confidence}"
    )
    return pnl if pnl is not None else 0.0


@app.command("paper-close")
def paper_close(
    mint: str = typer.Option("", "--mint", help="Mint address to close."),
    price: float | None = typer.Option(None, "--price", help="Manual exit price in SOL."),
    use_mark: bool = typer.Option(False, "--use-mark", help="Attempt to use a live DexScreener mark price if no --price given."),
    close_all: bool = typer.Option(False, "--all", help="Close all open paper positions."),
    confirm: bool = typer.Option(False, "--confirm", help="Required confirmation for --all."),
    preview: bool = typer.Option(False, "--preview", help="Preview close without mutating DB."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    """Close a paper position by mint address or close all with --all --confirm.

    Paper close is simulated. Not real profit or loss.
    """
    settings = load_settings()
    runtime_db_path = resolve_db_path(db_path)
    asyncio.run(init_db(runtime_db_path))
    manager = PositionManager(runtime_db_path, settings)

    if close_all:
        paper_positions = asyncio.run(manager.get_paper_positions())
        live_positions = [p for p in asyncio.run(manager.get_all_open()) if p.mode == "live"]

        if preview:
            console.print(f"[bold]Paper Close Preview[/bold]")
            console.print(f"  Paper positions to close: {len(paper_positions)}")
            console.print(f"  Live positions (untouched): {len(live_positions)}")
            total_pnl = 0.0
            for pos in paper_positions:
                exit_price, source = _resolve_close_price(pos.mint_address, price, use_mark, pos)
                total_pnl += _print_paper_close_preview_position(pos, exit_price, source)
            console.print(f"\n  Estimated total realized PnL: {_fmt_pnl(total_pnl)}")
            console.print("\n[yellow]Preview only — no positions closed. Paper close is simulated.[/yellow]")
            return

        if not confirm:
            console.print(f"Paper positions to close: {len(paper_positions)}")
            console.print(f"Live positions (untouched): {len(live_positions)}")
            console.print("\n[yellow]Use --confirm to close all paper positions. Live positions are never closed.[/yellow]")
            return

        closed_count = 0
        skipped_count = 0
        price_provider = DexScreenerPriceProvider() if use_mark else None
        for pos in paper_positions:
            exit_price = price
            if exit_price is None and use_mark and price_provider is not None:
                result = asyncio.run(price_provider.get_current_price(pos.mint_address))
                if _is_valid_price(result):
                    exit_price = result
            if not _is_valid_price(exit_price):
                skipped_count += 1
                continue
            result = asyncio.run(manager.close_position(pos.mint_address, exit_price_sol=exit_price, mode="paper"))
            if result is not None:
                closed_count += 1
        remaining = asyncio.run(manager.get_all_open())
        live_count = sum(1 for p in remaining if p.mode == "live")
        console.print(f"Closed {closed_count} paper position(s) with PnL. {live_count} live position(s) untouched.")
        if skipped_count:
            console.print(f"Skipped {skipped_count} paper position(s): no valid exit price available.")
        console.print("[yellow]Paper close is simulated. Not real profit or loss.[/yellow]")
        return

    if not mint:
        console.print("[red]Provide --mint <address> or --all --confirm[/red]")
        raise typer.Exit(code=1)

    position = asyncio.run(manager.get_position(mint, mode="paper"))
    if position is None:
        live_position = asyncio.run(manager.get_position(mint, mode="live"))
        if live_position is not None:
            console.print("[red]Refusing to close a live position via paper-close.[/red]")
            raise typer.Exit(code=1)
        console.print(f"[red]Position not found for mint: {mint}[/red]")
        raise typer.Exit(code=1)

    exit_price, source = _resolve_close_price(mint, price, use_mark, position)

    if preview:
        console.print(f"[bold]Paper Close Preview[/bold]")
        console.print(f"  Position: {mint[:16]}")
        console.print(f"  Entry: {position.entry_price_sol:.10f} SOL | Tokens: {position.token_amount:.0f}")
        console.print(f"  Fill quality: {position.fill_quality.value} ({_fill_quality_confidence(position.fill_quality)})")
        console.print(f"  Exit price: {exit_price:.10f} ({source})" if exit_price is not None else f"  Exit price: [yellow]unavailable ({source})[/yellow]")
        if exit_price is not None and position.fill_quality != PaperFillQuality.UNPRICED:
            pnl = round(position.token_amount * exit_price - position.amount_sol, 9)
            console.print(f"  Estimated realized PnL: {_fmt_pnl(pnl)}")
        elif position.fill_quality == PaperFillQuality.UNPRICED:
            console.print("  Estimated realized PnL: [yellow]N/A (unpriced entry)[/yellow]")
        console.print("\n[yellow]Preview only — no position closed. Paper close is simulated.[/yellow]")
        return

    if not _is_valid_price(exit_price):
        src_help = "Provide --price for manual, or --use-mark for a live DexScreener price."
        console.print(f"[red]No exit price available (source: {source}). {src_help}[/red]")
        raise typer.Exit(code=1)

    closed = asyncio.run(manager.close_position(mint, exit_price_sol=exit_price, mode="paper"))
    pnl_str = _fmt_pnl(closed.realized_pnl_sol if closed else 0.0)
    console.print(
        f"Closed paper position {mint[:16]} at price {exit_price:.10f} ({source}). "
        f"Realized PnL: {pnl_str}"
    )
    console.print("[yellow]Paper close is simulated. Not real profit or loss.[/yellow]")


def _is_valid_price(price: float | None) -> bool:
    return price is not None and math.isfinite(price) and price > 0


def _count_paper_trades(db_path: str | Path) -> int:
    """Count paper-mode trades in DB. Safe fallback to 0 on error."""
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT COUNT(*) FROM trades WHERE mode = ?", ("paper",))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row is not None else 0
    except Exception:
        return 0


@app.command("paper-report")
def paper_report(
    marks: str = typer.Option("unavailable", "--marks", help="Mark price source: 'unavailable' (default, no network) or 'live' (DexScreener read-only)."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    """Generate a daily paper trading report with PnL, positions, and diagnostics.

    Paper results are simulated. Not real profit or loss.
    """
    settings = load_settings()
    runtime_db_path = resolve_db_path(db_path)
    asyncio.run(init_db(runtime_db_path))
    manager = PositionManager(runtime_db_path, settings)

    if marks not in {"live", "unavailable"}:
        raise typer.BadParameter("must be 'unavailable' or 'live'", param_hint="--marks")
    if marks == "live":
        provider = DexScreenerPriceProvider()
    else:
        provider = UnavailablePriceProvider()

    calculator = PaperPnLCalculator(manager, price_provider=provider)
    pnl_summary = asyncio.run(calculator.compute_summary())

    all_open = asyncio.run(manager.get_all_open())
    paper_positions = [p for p in all_open if p.mode == "paper"]
    live_positions = [p for p in all_open if p.mode == "live"]
    archived_paper_positions = asyncio.run(manager.get_archived_paper_positions())

    total_trades = _count_paper_trades(runtime_db_path)
    latest_runs = asyncio.run(get_recent_soak_runs(runtime_db_path, limit=1))
    latest_capacity_blocked = latest_runs[0].capacity_blocked if latest_runs else 0

    best_trade: float | None = None
    worst_trade: float | None = None
    for p in pnl_summary.positions:
        if p.status == PositionStatus.CLOSED:
            pnl_val = p.realized_pnl_sol
            if best_trade is None or pnl_val > best_trade:
                best_trade = pnl_val
            if worst_trade is None or pnl_val < worst_trade:
                worst_trade = pnl_val

    marks_label = "[green]live[/green]" if pnl_summary.marks_mode == "live" else "[yellow]unavailable[/yellow]"

    console.print("[bold]Paper Trading Report[/bold]")
    console.print(f"  Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    console.print(f"  Mode: paper (simulated)")
    console.print(f"  Marks: {marks_label}")
    console.print("")

    console.print("[bold]Trades & Positions[/bold]")
    console.print(f"  Total paper trades entered: {total_trades}")
    console.print(f"  Open paper positions: {pnl_summary.open_positions}")
    console.print(f"  Closed paper positions: {pnl_summary.closed_positions}")
    console.print(f"  Total SOL deployed (open): {pnl_summary.total_sol_deployed:.6f}")
    console.print(f"  Realized PnL: {_fmt_pnl(pnl_summary.realized_pnl_sol)}")
    if pnl_summary.unrealized_pnl_sol is not None:
        console.print(f"  Unrealized PnL: {_fmt_pnl(pnl_summary.unrealized_pnl_sol)}")
    else:
        console.print(f"  Unrealized PnL: [yellow]mark_unavailable ({pnl_summary.mark_unavailable_count} position(s) without mark)[/yellow]")
    console.print(f"  Live positions (untouched): {len(live_positions)}")
    console.print(f"  Archived legacy paper positions excluded: {len(archived_paper_positions)}")
    console.print("")

    console.print("[bold]Paper Data Quality[/bold]")
    for fill_quality in (
        PaperFillQuality.PRICED_QUOTE,
        PaperFillQuality.UNPRICED,
        PaperFillQuality.LEGACY_UNKNOWN,
    ):
        count = pnl_summary.fill_quality_counts.get(fill_quality.value, 0)
        console.print(f"  {_fill_quality_label(fill_quality)} ({fill_quality.value}): {count}")
    console.print(f"  Report confidence: {_report_confidence_label(pnl_summary.report_confidence)}")
    console.print("")

    console.print("[bold]Mark Coverage[/bold]")
    console.print(f"  Open positions considered: {pnl_summary.open_positions}")
    console.print(f"  Usable marks: {pnl_summary.usable_mark_count}")
    console.print(f"  Without usable marks: {pnl_summary.unusable_mark_count}")
    for reason, count in sorted(pnl_summary.mark_reason_counts.items()):
        console.print(f"  {reason}: {count}")
    console.print("")

    console.print("[bold]Best/Worst Closed Trades[/bold]")
    if best_trade is not None:
        console.print(f"  Best closed trade: {_fmt_pnl(best_trade)}")
        console.print(f"  Worst closed trade: {_fmt_pnl(worst_trade)}")
    else:
        console.print("  [yellow](no closed trades yet)[/yellow]")
    console.print("")

    console.print("[bold]Recent Paper Positions[/bold]")
    if paper_positions:
        for pos in sorted(paper_positions, key=lambda p: p.opened_at, reverse=True)[:5]:
            status_str = "[green]OPEN[/green]"
            age_label = _position_age_label(pos)
            hint = _close_suggestion(pos)
            console.print(
                f"  {pos.mint_address[:16]}  {status_str}  "
                f"sol={pos.amount_sol:.4f}  tokens={pos.token_amount:.0f}  "
                f"entry={pos.entry_price_sol:.10f}  quality={pos.fill_quality.value}  age={age_label}"
            )
            if hint:
                console.print(f"    {hint}")
    else:
        console.print("  [yellow](no open paper positions)[/yellow]")
    console.print("")

    console.print("[bold]Paper-Soak Diagnostics[/bold]")
    _display_soak_diagnostics(runtime_db_path)
    console.print("")

    console.print("[bold]Actionable Hints[/bold]")
    for hint in _paper_report_hints(pnl_summary, capacity_blocked=latest_capacity_blocked):
        console.print(f"  - {hint}")
    console.print("")

    console.print("[bold]Live Readiness (diagnostic only — does not affect paper mode)[/bold]")
    try:
        report = _build_live_readiness_report(runtime_db_path, settings)
        for line in report.lines():
            console.print(f"  {line}")
    except Exception:
        console.print("  [yellow]unavailable[/yellow]")
    console.print("")

    console.print("[yellow]WARNING: Paper results are simulated. Not real profit or loss.[/yellow]")


@app.command("paper-decisions")
def paper_decisions(
    limit: int = typer.Option(50, "--limit", "-n", help="Max recent decisions to retrieve."),
    since_hours: int | None = typer.Option(None, "--since-hours", help="Only decisions newer than N hours."),
    outcome: str | None = typer.Option(None, "--outcome", "-o", help="Filter by outcome (e.g. accepted, risk_rejected, capacity_blocked, unknown_data, skipped)."),
    mode: str | None = typer.Option(None, "--mode", "-m", help="Filter by candidate mode (launch, migration, unknown)."),
    source: str | None = typer.Option(None, "--source", "-s", help="Filter by signal source (pump_fun, onchain, whale, twitter, composite)."),
    export_md: str | None = typer.Option(None, "--export-md", help="Export to markdown file path."),
    export_json: str | None = typer.Option(None, "--export-json", help="Export to JSON file path."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    """Review recent paper decision telemetry — outcomes, rejections, sources.

    Paper results are simulated. Not real trading advice.
    """
    runtime_db_path = resolve_db_path(db_path)
    asyncio.run(init_db(runtime_db_path))
    decisions = asyncio.run(get_recent_paper_decisions(runtime_db_path, limit=limit))

    if since_hours is not None:
        cutoff = datetime.now(UTC) - timedelta(hours=since_hours)
        cutoff_str = cutoff.isoformat()
        decisions = [d for d in decisions if d.recorded_at >= cutoff_str]

    if outcome:
        decisions = [d for d in decisions if d.action_outcome == outcome]
    if mode:
        decisions = [d for d in decisions if d.candidate_mode == mode]
    if source:
        decisions = [d for d in decisions if d.source == source]

    now_str = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S') + " UTC"

    outcome_counts = Counter(d.action_outcome for d in decisions)
    reason_counts = Counter(d.primary_reason for d in decisions)
    source_counts = Counter(d.source for d in decisions)
    mode_counts = Counter(d.candidate_mode for d in decisions)
    accepted = [d for d in decisions if d.action_outcome in ("accepted", "traded")]
    rejected = [d for d in decisions if d.action_outcome not in ("accepted", "traded")]

    # Build candidate rows for export
    candidate_rows = []
    for d in decisions:
        label = d.symbol or d.name or (d.mint_address[:16] if d.mint_address else "unknown")
        candidate_rows.append({
            "label": label,
            "mint": d.mint_address,
            "source": d.source,
            "mode": d.candidate_mode,
            "outcome": d.action_outcome,
            "reason": d.primary_reason,
            "score": d.attention_score or 0,
            "risk_score": d.risk_score,
            "recorded_at": d.recorded_at,
        })

    # Export to markdown
    if export_md:
        export_path = Path(export_md)
        lines = [
            "# Paper Decision Telemetry",
            f"Generated: {now_str}",
            "Mode: paper (simulated)",
            "",
            "**This is safe simulated paper telemetry. Not trading advice.**",
            "",
        ]
        if not decisions:
            lines.append("No paper decision telemetry found. Run `paper-soak` to generate candidate decisions.")
        else:
            lines.append(f"## Summary ({len(decisions)} decisions)")
            lines.append("")
            lines.append("### By outcome")
            for oc, count in outcome_counts.most_common():
                lines.append(f"- {oc}: {count}")
            lines.append("")
            lines.append("### By rejection reason (top 10)")
            for rc, count in reason_counts.most_common(10):
                lines.append(f"- {rc}: {count}")
            lines.append("")
            lines.append("### By signal source")
            for sc, count in source_counts.most_common():
                lines.append(f"- {sc}: {count}")
            lines.append("")
            lines.append("### By candidate mode")
            for mc, count in mode_counts.most_common():
                lines.append(f"- {mc}: {count}")
            lines.append("")

            if accepted:
                lines.append(f"## Accepted candidates ({len(accepted)})")
                lines.append("")
                lines.append("| Label | Source | Mode | Score | Recorded At |")
                lines.append("|-------|--------|------|-------|-------------|")
                for d in accepted:
                    label = d.symbol or d.name or (d.mint_address[:16] if d.mint_address else "unknown")
                    lines.append(f"| {label} | {d.source} | {d.candidate_mode} | {d.attention_score or 0} | {d.recorded_at} |")
                lines.append("")

            if rejected:
                lines.append(f"## Rejected candidates ({len(rejected)})")
                lines.append("")
                lines.append("| Label | Reason | Source | Mode | Risk Score | Recorded At |")
                lines.append("|-------|--------|--------|------|------------|-------------|")
                for d in rejected:
                    label = d.symbol or d.name or (d.mint_address[:16] if d.mint_address else "unknown")
                    rs = f"{d.risk_score}" if d.risk_score is not None else "N/A"
                    lines.append(f"| {label} | {d.primary_reason} | {d.source} | {d.candidate_mode} | {rs} | {d.recorded_at} |")
                lines.append("")

        lines.append("---")
        lines.append("*WARNING: Paper results are simulated. Not real profit or loss.*")
        export_path.write_text("\n".join(lines) + "\n")
        console.print(f"[green]Exported markdown to {export_path}[/green]")

    # Export to JSON
    if export_json:
        export_path = Path(export_json)
        payload = {
            "generated_at": now_str,
            "mode": "paper",
            "total_decisions": len(decisions),
            "summary": {
                "by_outcome": dict(outcome_counts.most_common()),
                "by_reason": dict(reason_counts.most_common(10)),
                "by_source": dict(source_counts.most_common()),
                "by_mode": dict(mode_counts.most_common()),
            },
            "accepted_candidates": [
                {
                    "label": d.symbol or d.name or (d.mint_address[:16] if d.mint_address else "unknown"),
                    "source": d.source,
                    "mode": d.candidate_mode,
                    "outcome": d.action_outcome,
                    "reason": d.primary_reason,
                    "score": d.attention_score or 0,
                    "risk_score": d.risk_score,
                    "recorded_at": d.recorded_at,
                }
                for d in accepted
            ],
            "rejected_candidates": [
                {
                    "label": d.symbol or d.name or (d.mint_address[:16] if d.mint_address else "unknown"),
                    "source": d.source,
                    "mode": d.candidate_mode,
                    "outcome": d.action_outcome,
                    "reason": d.primary_reason,
                    "score": d.attention_score or 0,
                    "risk_score": d.risk_score,
                    "recorded_at": d.recorded_at,
                }
                for d in rejected
            ],
        }
        export_path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
        console.print(f"[green]Exported JSON to {export_path}[/green]")

    # Console output (skip if only exporting)
    if not export_md and not export_json:
        console.print("[bold]Paper Decision Telemetry[/bold]")
        console.print(f"  Generated: {now_str}")
        console.print(f"  Mode: paper (simulated)")
        console.print(
            "  Discovery edge is an operator diagnostic only; it does not affect strict risk, "
            "ranking, sizing, or execution."
        )
        console.print("")

        if not decisions:
            console.print("  [yellow]No paper decision telemetry found.[/yellow]")
            console.print("  [yellow]Run `paper-soak` to generate candidate decisions.[/yellow]")
            console.print("")
            console.print("[yellow]WARNING: Paper results are simulated. Not real profit or loss.[/yellow]")
            return

        console.print(f"[bold]Summary ({len(decisions)} decisions)[/bold]")
        console.print("  [bold]By outcome:[/bold]")
        for oc, count in outcome_counts.most_common():
            console.print(f"    {oc}: {count}")
        console.print("  [bold]By rejection reason:[/bold]")
        for rc, count in reason_counts.most_common(10):
            console.print(f"    {rc}: {count}")
        console.print("  [bold]By signal source:[/bold]")
        for sc, count in source_counts.most_common():
            console.print(f"    {sc}: {count}")
        console.print("  [bold]By candidate mode:[/bold]")
        for mc, count in mode_counts.most_common():
            console.print(f"    {mc}: {count}")
        console.print("")

        if accepted:
            console.print(f"[bold]Accepted candidates ({len(accepted)})[/bold]")
            for d in accepted[:5]:
                label = d.symbol or d.name or d.mint_address[:16] if d.mint_address else "unknown"
                console.print(
                    f"  {label}  source={d.source}  mode={d.candidate_mode}  "
                    f"score={d.attention_score}  {_paper_decision_edge_display(d)}"
                )
            if len(accepted) > 5:
                console.print(f"  ... and {len(accepted) - 5} more")
            console.print("")

        if rejected:
            console.print(f"[bold]Recent rejected candidates ({len(rejected)})[/bold]")
            for d in rejected[:5]:
                label = d.symbol or d.name or d.mint_address[:16] if d.mint_address else "unknown"
                console.print(
                    f"  {label}  reason={d.primary_reason}  source={d.source}  "
                    f"mode={d.candidate_mode}  {_paper_decision_edge_display(d)}"
                )
            if len(rejected) > 5:
                console.print(f"  ... and {len(rejected) - 5} more")

        console.print("")
        console.print("[yellow]WARNING: Paper results are simulated. Not real profit or loss.[/yellow]")


if __name__ == "__main__":
    app()

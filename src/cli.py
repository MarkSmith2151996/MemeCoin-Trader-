"""Typer CLI entrypoint."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

import typer
from rich.console import Console

from src.core.config import Settings, load_settings
from src.core.database import init_db, record_trade
from src.execution.base import ExecutionAdapter
from src.execution.live_buy import execute_guarded_live_buy
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_execution_config import evaluate_live_execution_config
from src.execution.live_exit import execute_guarded_live_exit
from src.execution.live_guardrails import evaluate_live_guardrails
from src.execution.env_readiness import evaluate_env_readiness
from src.execution.helius_providers import (
    try_create_balance_lookup,
    try_create_transaction_simulator,
)
from src.execution.live_readiness import evaluate_micro_live_readiness
from src.execution.jupiter_live import JupiterLiveExecutionAdapter
from src.execution.paper import PaperExecutionAdapter
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
        lines.append("  # | symbol | mint | source | mode | attn | outcome | reason | theme | meta")
        for diagnostic in candidates[:8]:
            lines.append(
                "  {rank} | {symbol} | {mint_short} | {source} | {mode} | {attn} | {outcome} | {reason} | {theme} | {meta}".format(
                    rank=diagnostic.get("rank", "?"),
                    symbol=diagnostic.get("symbol", "unknown"),
                    mint_short=diagnostic.get("mint_short", "unknown"),
                    source=diagnostic.get("source", "unknown"),
                    mode=diagnostic.get("candidate_mode", "unknown"),
                    attn=_candidate_attention_display(diagnostic),
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
    execution_adapter = execution or PaperExecutionAdapter()

    await init_db(runtime_db_path)
    initial_trade_count = _count_rows(runtime_db_path, "trades")
    initial_open_positions = _count_rows(runtime_db_path, "positions", "status != ?", ("CLOSED",))

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
    session_open_positions = len(await position_manager.get_all_open())
    persisted_open_positions = _count_rows(runtime_db_path, "positions", "status != ?", ("CLOSED",))

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
        "attention_score": diagnostic.get("attention_score"),
        "attention_tier": diagnostic.get("attention_tier"),
        "attention_reasons": list(diagnostic.get("attention_reasons", ())),
        "narrative_tags": list(diagnostic.get("narrative_tags", ())),
        "candidate_mode": diagnostic.get("candidate_mode"),
        "social_signal_state": diagnostic.get("social_signal_state"),
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
            f"Skipped trades:            {self.cycle.signals_accepted - self.cycle.trades_persisted}",
            f"Blocked trades (capacity): {self.cycle.capacity_blocked_candidates}",
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

        if self.cycle.rejection_reasons:
            lines.append("Risk rejection breakdown:")
            for reason, count in sorted(self.cycle.rejection_reasons.items(), key=lambda item: -item[1]):
                lines.append(f"  - {reason}: {count}")
        else:
            lines.append("Risk rejection breakdown:  none")

        if self.cycle.summary_rejection_reasons:
            stale = {r: c for r, c in self.cycle.summary_rejection_reasons.items() if "_unknown" in r}
            if stale:
                lines.append("Stale data warnings:")
                for reason, count in stale.items():
                    lines.append(f"  - {reason}: {count} candidates had unavailable data")
            else:
                lines.append("Stale data warnings:       none")
        else:
            lines.append("Stale data warnings:       none")

        lines.append("═══════════════════════════")
        return lines


async def run_paper_soak(
    max_signals: int = 20,
    timeout_seconds: float = 60.0,
    *,
    risk_profile: str = "discovery",
    fresh_evaluation_session: bool = True,
    db_path: str | Path | None = None,
    sources: list[SignalSource] | None = None,
) -> PaperSoakAudit:
    cycle = await run_bounded_paper_cycle(
        max_signals=max_signals,
        timeout_seconds=timeout_seconds,
        risk_profile=risk_profile,
        fresh_evaluation_session=fresh_evaluation_session,
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

    return PaperSoakAudit(
        cycle=cycle,
        health_ok=health_status.ok,
        health_message=health_status.message,
        guardrail_diagnostics=guardrail_diagnostics,
        circuit_breaker_diagnostics=circuit_breaker_diagnostics,
        readiness_checks=readiness_checks,
    )


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
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    audit = asyncio.run(
        run_paper_soak(
            max_signals=max_signals,
            timeout_seconds=timeout_seconds,
            db_path=db_path,
        )
    )
    for line in audit.lines():
        console.print(line)


@app.command("env-readiness")
def env_readiness() -> None:
    report = evaluate_env_readiness()
    for line in report.lines():
        console.print(line)


@app.command("live-readiness")
def live_readiness(
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    settings = load_settings()
    manager = PositionManager(resolve_db_path(db_path), settings)
    simulator = try_create_transaction_simulator()
    balance = try_create_balance_lookup()
    report = asyncio.run(
        evaluate_micro_live_readiness(
            settings,
            position_manager=manager,
            wallet_balance_lookup=balance,
            transaction_simulator=simulator,
            circuit_breaker=LiveCircuitBreaker(),
        )
    )
    for line in report.lines():
        console.print(line)


@app.command("live-exit")
def live_exit(
    mint: str = typer.Option(..., "--mint", help="Mint address of the existing position to exit."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    settings = load_settings()
    manager = PositionManager(resolve_db_path(db_path), settings)
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
    console.print({"ok": result.ok, "diagnostics": list(result.diagnostics), "provider": result.provider, "tx_signature": result.tx_signature})


@app.command("live-buy")
def live_buy(
    mint: str = typer.Option(..., "--mint", help="Mint address to buy."),
    amount_sol: float = typer.Option(..., "--amount-sol", min=0.000001, help="Requested SOL size."),
    db_path: str | None = typer.Option(None, help="Optional SQLite path override."),
) -> None:
    settings = load_settings()
    manager = PositionManager(resolve_db_path(db_path), settings)
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
    console.print({"ok": result.ok, "diagnostics": list(result.diagnostics), "provider": result.provider, "tx_signature": result.tx_signature})


if __name__ == "__main__":
    app()

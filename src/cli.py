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
from src.core.database import init_db
from src.execution.base import ExecutionAdapter
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
        lines.extend(self.summary_table_lines())
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
                    attn=f"{diagnostic.get('attention_score', 0)}/{diagnostic.get('attention_tier', 'ignore')}",
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
    rejected_candidate_diagnostics: list[dict[str, object]] = []
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
        termination_reason=termination_reason,
        elapsed_seconds=round(monotonic() - start_at, 3),
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
        "decision": "rejected",
        "failed_check": (record.failed_check if record is not None and record.failed_check is not None else _format_rejection_reason(decision.rejection_reason)),
        "rejection_reason": decision.rejection_reason or "unknown_or_other",
        "risk_score": record.risk_score if record is not None else None,
        "attention_score": attention_diagnostics.get("attention_score", 0),
        "attention_tier": attention_diagnostics.get("attention_tier", "ignore"),
        "attention_reasons": tuple(attention_diagnostics.get("attention_reasons", ())),
        "narrative_tags": tuple(attention_diagnostics.get("narrative_tags", ())),
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
        "notes": _diagnostic_note(payload, record),
    }


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
        "Aggregate rejection reasons:",
    ]
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


@app.command()
def health() -> None:
    status = check_health()
    console.print({"ok": status.ok, "message": status.message, "checked_at": status.checked_at})


@app.command("show-config")
def show_config() -> None:
    settings = load_settings()
    console.print(settings.model_dump())


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


if __name__ == "__main__":
    app()

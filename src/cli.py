"""Typer CLI entrypoint."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections import Counter
from dataclasses import dataclass
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
from src.risk.scorer import DiscoveryRiskScorer, assess_signal
from src.signals.base import SignalSource
from src.signals.pump_fun import build_monitor_from_env
from src.signals.whale_tracker import WhaleWalletTracker
from src.strategy.decision_engine import DecisionEngine
from src.strategy.position_manager import PositionManager

app = typer.Typer(help="Memecoin Trader CLI")
console = Console()
logger = logging.getLogger(__name__)
SUPPORTED_RISK_PROFILES = {"strict", "discovery"}


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
    rejection_reasons: dict[str, int]
    termination_reason: str
    elapsed_seconds: float

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
            f"termination_reason={self.termination_reason}",
            f"elapsed_seconds={self.elapsed_seconds:.3f}",
        ]
        if self.rejection_reasons:
            lines.append("rejection_reasons:")
            lines.extend(f"  {reason}={count}" for reason, count in self.rejection_reasons.items())
        return lines


def build_signal_sources() -> list[SignalSource]:
    return [build_monitor_from_env(), WhaleWalletTracker()]


def force_paper_settings(settings: Settings) -> Settings:
    return settings.model_copy(
        update={"execution": settings.execution.model_copy(update={"mode": "paper"})}
    )


def apply_risk_profile(settings: Settings, risk_profile: str) -> Settings:
    normalized = normalize_risk_profile(risk_profile)
    if normalized == "discovery":
        return settings.model_copy(
            update={"risk": settings.risk.model_copy(update={"min_age_minutes": 0})}
        )
    return settings


def normalize_risk_profile(risk_profile: str) -> str:
    normalized = risk_profile.strip().lower()
    if normalized not in SUPPORTED_RISK_PROFILES:
        raise ValueError(f"Unsupported risk profile: {risk_profile}")
    return normalized


def build_paper_cycle_risk_scorer(risk_profile: str, settings: Settings) -> Any:
    normalized = normalize_risk_profile(risk_profile)
    if normalized == "discovery":
        return DiscoveryRiskScorer(settings.risk)
    return assess_signal


def _count_rows(db_path: Path, table: str, where_clause: str | None = None, params: tuple[object, ...] = ()) -> int:
    query = f"SELECT COUNT(*) FROM {table}"
    if where_clause:
        query = f"{query} WHERE {where_clause}"
    with sqlite3.connect(db_path) as connection:
        row = connection.execute(query, params).fetchone()
    return int(row[0]) if row is not None else 0


async def _start_signal_sources(sources: list[SignalSource], rejection_reasons: Counter[str]) -> None:
    for source in sources:
        try:
            await source.start()
        except Exception as exc:  # pragma: no cover - defensive against provider/network errors
            rejection_reasons["provider_warning"] += 1
            logger.warning("Failed to start signal source %s: %s", source.name, exc)


async def _stop_signal_sources(sources: list[SignalSource], rejection_reasons: Counter[str]) -> None:
    for source in sources:
        try:
            await source.stop()
        except Exception as exc:  # pragma: no cover - defensive against provider/network errors
            rejection_reasons["provider_warning"] += 1
            logger.warning("Failed to stop signal source %s: %s", source.name, exc)


async def _poll_signal_sources(sources: list[SignalSource], rejection_reasons: Counter[str]) -> list[Any]:
    signals: list[Any] = []
    for source in sources:
        try:
            signals.extend(await source.poll())
        except Exception as exc:  # pragma: no cover - defensive against provider/network errors
            rejection_reasons["provider_warning"] += 1
            logger.warning("Failed to poll signal source %s: %s", source.name, exc)
    return signals


async def run_bounded_paper_cycle(
    max_signals: int,
    timeout_seconds: float,
    *,
    risk_profile: str = "strict",
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
    execution_adapter = execution or PaperExecutionAdapter()

    await init_db(runtime_db_path)
    initial_trade_count = _count_rows(runtime_db_path, "trades")

    engine = DecisionEngine(
        execution_adapter,
        risk_scorer or build_paper_cycle_risk_scorer(normalized_risk_profile, runtime_settings),
        PositionManager(runtime_db_path, runtime_settings),
        runtime_settings,
        db=runtime_db_path,
    )

    start_at = monotonic()
    signals_collected = 0
    signals_accepted = 0
    signals_rejected = 0
    rejection_reasons: Counter[str] = Counter()
    termination_reason = "timeout"

    await _start_signal_sources(signal_sources, rejection_reasons)
    try:
        while signals_collected < max_signals:
            if monotonic() - start_at >= timeout_seconds:
                break

            remaining_capacity = max_signals - signals_collected
            batch = await _poll_signal_sources(signal_sources, rejection_reasons)
            for signal in batch[:remaining_capacity]:
                signals_collected += 1
                try:
                    decision = await engine.evaluate_signal_with_diagnostics(signal)
                except Exception as exc:  # pragma: no cover - defensive against future decision/runtime failures
                    logger.warning("Failed to evaluate signal during paper-cycle: %s", exc)
                    signals_rejected += 1
                    rejection_reasons["unknown_or_other"] += 1
                    continue

                if decision.trade is None:
                    signals_rejected += 1
                    rejection_reasons[decision.rejection_reason or "unknown_or_other"] += 1
                    continue

                signals_accepted += 1

            if signals_collected >= max_signals:
                termination_reason = "max_signals"
                break

            remaining_time = timeout_seconds - (monotonic() - start_at)
            if remaining_time <= 0:
                break
            await asyncio.sleep(min(max(poll_interval_s, 0.0), remaining_time))
    finally:
        await _stop_signal_sources(signal_sources, rejection_reasons)
        await execution_adapter.close()

    return PaperCycleSummary(
        execution_mode=runtime_settings.execution.mode,
        risk_profile=normalized_risk_profile,
        max_signals=max_signals,
        timeout_seconds=timeout_seconds,
        signals_collected=signals_collected,
        signals_accepted=signals_accepted,
        signals_rejected=signals_rejected,
        trades_persisted=max(_count_rows(runtime_db_path, "trades") - initial_trade_count, 0),
        open_positions=_count_rows(runtime_db_path, "positions", "status != ?", ("CLOSED",)),
        rejection_reasons=dict(sorted(rejection_reasons.items())),
        termination_reason=termination_reason,
        elapsed_seconds=round(monotonic() - start_at, 3),
    )


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
            db_path=db_path,
        )
    )
    for line in summary.safe_lines():
        console.print(line)


if __name__ == "__main__":
    app()

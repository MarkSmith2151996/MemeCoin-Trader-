"""Read-only replay of position-linked price snapshots under exit parameters."""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import product
from pathlib import Path
from statistics import fmean, pstdev


@dataclass(frozen=True, slots=True)
class BacktestParameters:
    """Percentage-based exit controls used to replay one recorded price path."""

    trailing_stop_pct: float = 3.0
    take_profit_pct: float = 60.0
    hard_stop_pct: float = 8.0
    trailing_arm_pct: float = 2.0
    early_exit_timeout_s: float = 90.0
    early_exit_green_pct: float = 2.0

    def __post_init__(self) -> None:
        if self.trailing_stop_pct <= 0:
            raise ValueError("trailing_stop_pct must be positive")
        if self.take_profit_pct <= 0:
            raise ValueError("take_profit_pct must be positive")
        if self.hard_stop_pct <= 0:
            raise ValueError("hard_stop_pct must be positive")
        if self.trailing_arm_pct < 0:
            raise ValueError("trailing_arm_pct cannot be negative")
        if self.early_exit_timeout_s < 0:
            raise ValueError("early_exit_timeout_s cannot be negative")
        if self.early_exit_green_pct < 0:
            raise ValueError("early_exit_green_pct cannot be negative")


@dataclass(frozen=True, slots=True)
class SnapshotPoint:
    """One valid observed mark elapsed from the position's open time."""

    elapsed_seconds: float
    price_sol: float


@dataclass(frozen=True, slots=True)
class ClosedPosition:
    """A closed position eligible for snapshot-backed replay."""

    id: str
    strategy: str
    entry_price_sol: float
    amount_sol: float
    actual_pnl_sol: float
    snapshots: tuple[SnapshotPoint, ...]


@dataclass(frozen=True, slots=True)
class SimulatedExit:
    """The first configured exit that would have closed one position."""

    position_id: str
    exit_reason: str
    exit_price_sol: float
    pnl_sol: float


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_closed_positions(db_path: str | Path) -> list[ClosedPosition]:
    """Load closed positions that have valid position-linked price snapshots.

    The SQLite URI explicitly opens the database read-only, so reporting cannot
    contend with or alter either running strategy's state.
    """

    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"Database not found: {path}")

    conn = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    try:
        positions = conn.execute(
            """
            SELECT id, COALESCE(strategy, 'A'), opened_at, entry_price_sol,
                   amount_sol, realized_pnl_sol
            FROM positions
            WHERE status = 'CLOSED'
              AND entry_price_sol > 0
              AND amount_sol > 0
            ORDER BY opened_at, id
            """,
        ).fetchall()
        marks = conn.execute(
            """
            SELECT position_id, observed_at, price_sol
            FROM price_snapshots
            WHERE position_id IS NOT NULL AND price_sol > 0
            ORDER BY position_id, observed_at, id
            """,
        ).fetchall()
    finally:
        conn.close()

    marks_by_position: dict[str, list[tuple[str, float]]] = {}
    for position_id, observed_at, price_sol in marks:
        price = float(price_sol)
        if math.isfinite(price) and price > 0:
            marks_by_position.setdefault(str(position_id), []).append((str(observed_at), price))

    records: list[ClosedPosition] = []
    for position_id, strategy, opened_at, entry_price, amount, actual_pnl in positions:
        recorded_marks = marks_by_position.get(str(position_id))
        if not recorded_marks:
            continue
        opened = _parse_timestamp(str(opened_at))
        snapshots = tuple(
            SnapshotPoint(
                elapsed_seconds=(observed_at_dt - opened).total_seconds(),
                price_sol=price,
            )
            for observed_at, price in recorded_marks
            if (observed_at_dt := _parse_timestamp(observed_at)) >= opened
        )
        if snapshots:
            records.append(
                ClosedPosition(
                    id=str(position_id),
                    strategy=str(strategy),
                    entry_price_sol=float(entry_price),
                    amount_sol=float(amount),
                    actual_pnl_sol=float(actual_pnl),
                    snapshots=snapshots,
                ),
            )
    return records


def simulate_exit(position: ClosedPosition, parameters: BacktestParameters) -> SimulatedExit:
    """Replay marks in order and return the first triggered full-position exit."""

    entry = position.entry_price_sol
    peak = entry
    for snapshot in position.snapshots:
        current = snapshot.price_sol
        peak = max(peak, current)
        if current <= entry * (1 - parameters.hard_stop_pct / 100):
            return _exit(position, "hard_stop", entry * (1 - parameters.hard_stop_pct / 100))
        if current >= entry * (1 + parameters.take_profit_pct / 100):
            return _exit(position, "take_profit", entry * (1 + parameters.take_profit_pct / 100))
        if (
            peak > entry * (1 + parameters.trailing_arm_pct / 100)
            and (peak - current) / peak >= parameters.trailing_stop_pct / 100
        ):
            return _exit(position, "trailing_stop", current)
        if (
            snapshot.elapsed_seconds >= parameters.early_exit_timeout_s
            and peak <= entry * (1 + parameters.early_exit_green_pct / 100)
        ):
            return _exit(position, "early_exit_no_green", current)

    # Snapshot collection stops at close; retain the final recorded mark rather
    # than inventing a later exit price when no configured exit was observed.
    return _exit(position, "end_of_snapshots", position.snapshots[-1].price_sol)


def _exit(position: ClosedPosition, reason: str, price: float) -> SimulatedExit:
    return SimulatedExit(
        position_id=position.id,
        exit_reason=reason,
        exit_price_sol=price,
        pnl_sol=position.amount_sol * (price / position.entry_price_sol - 1),
    )


def summarize_backtest(
    positions: Iterable[ClosedPosition], parameters: BacktestParameters,
) -> dict[str, object]:
    """Return simulated metrics alongside the persisted realized PnL baseline."""

    records = list(positions)
    exits = [simulate_exit(position, parameters) for position in records]
    simulated_pnls = [exit.pnl_sol for exit in exits]
    actual_pnls = [position.actual_pnl_sol for position in records]
    reason_counts: dict[str, int] = {}
    for exit in exits:
        reason_counts[exit.exit_reason] = reason_counts.get(exit.exit_reason, 0) + 1
    simulated = _metrics(simulated_pnls)
    actual = _metrics(actual_pnls)
    return {
        "parameters": asdict(parameters),
        "eligible_positions": len(records),
        "simulated": simulated,
        "actual": actual,
        "pnl_vs_actual_sol": round(float(simulated["pnl_sol"]) - float(actual["pnl_sol"]), 8),
        "exit_reasons": reason_counts,
    }


def grid_search(
    positions: Iterable[ClosedPosition],
    parameter_ranges: Mapping[str, Iterable[float]],
    *,
    base_parameters: BacktestParameters | None = None,
) -> list[dict[str, object]]:
    """Evaluate every supplied parameter combination, ranked by simulated PnL."""

    base = base_parameters or BacktestParameters()
    valid_fields = set(asdict(base))
    unknown = set(parameter_ranges) - valid_fields
    if unknown:
        raise ValueError(f"Unknown backtest parameters: {', '.join(sorted(unknown))}")
    names = list(parameter_ranges)
    values = [tuple(parameter_ranges[name]) for name in names]
    if any(not value for value in values):
        raise ValueError("Parameter ranges cannot be empty")

    records = list(positions)
    results: list[dict[str, object]] = []
    for combination in product(*values):
        params = BacktestParameters(**(asdict(base) | dict(zip(names, combination, strict=True))))
        results.append(summarize_backtest(records, params))
    return sorted(results, key=lambda result: float(result["simulated"]["pnl_sol"]), reverse=True)


def _metrics(pnls: list[float]) -> dict[str, float | int]:
    if not pnls:
        return {
            "trades": 0,
            "pnl_sol": 0.0,
            "win_rate": 0.0,
            "sharpe": 0.0,
            "max_drawdown_sol": 0.0,
        }
    equity = high_watermark = max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        high_watermark = max(high_watermark, equity)
        max_drawdown = min(max_drawdown, equity - high_watermark)
    deviation = pstdev(pnls)
    sharpe = 0.0 if deviation == 0 else fmean(pnls) / deviation * math.sqrt(len(pnls))
    return {
        "trades": len(pnls),
        "pnl_sol": round(sum(pnls), 8),
        "win_rate": round(sum(pnl > 0 for pnl in pnls) / len(pnls), 6),
        "sharpe": round(sharpe, 6),
        "max_drawdown_sol": round(max_drawdown, 8),
    }

#!/usr/bin/env python3
"""Stress a fixed V2 blind-trade log against offline PumpApi rug labels.

This script never imports or runs the replay engine. It replaces only the
reported PnL of trades whose label falls strictly after entry and before the
blind exit, keeping the accepted trade set fixed.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


DEFAULT_LABELS = Path("/mnt/d/pumpapi-replay/results/mt712/rug_labels.csv")
DEFAULT_RESULTS = Path.home() / "workspace/data/results/mt731"
WINDOW_START_MS = int(datetime(2026, 7, 22, tzinfo=UTC).timestamp() * 1000)
WINDOW_END_MS = int(datetime(2026, 8, 22, tzinfo=UTC).timestamp() * 1000)
PRIORITY_FEE_PER_LEG_SOL = 0.0002


@dataclass(frozen=True, slots=True)
class RugLabel:
    timestamp_ms: int
    pool_sol_after: float


@dataclass(frozen=True, slots=True)
class Trade:
    mint: str
    entry_time_ms: int
    exit_time_ms: int
    position_size_sol: float
    exit_fee_pct: float
    blind_gross_proceeds_sol: float

    @property
    def blind_pnl_sol(self) -> float:
        return net_pnl(self.position_size_sol, self.exit_fee_pct, self.blind_gross_proceeds_sol)


@dataclass(frozen=True, slots=True)
class Casualty:
    trade: Trade
    label: RugLabel


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--trades",
        type=Path,
        required=True,
        help="Fixed MT-729 V2 trade CSV; do not provide an engine input archive.",
    )
    parser.add_argument("--rug-labels", type=Path, default=DEFAULT_LABELS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument(
        "--scenario",
        default="realistic_visibility",
        help="CSV scenario to analyze when the trade log contains a scenario column.",
    )
    parser.add_argument("--seeds", type=int, default=50)
    return parser.parse_args()


def net_pnl(position_size_sol: float, exit_fee_pct: float, gross_proceeds_sol: float) -> float:
    """Apply the fixed V2 exit fee and priority-fee convention to gross proceeds."""

    return (
        gross_proceeds_sol * (1.0 - exit_fee_pct)
        - position_size_sol
        - PRIORITY_FEE_PER_LEG_SOL * 2
    )


def required_field(fields: set[str], *names: str) -> str:
    for name in names:
        if name in fields:
            return name
    raise ValueError(f"Trade CSV is missing one of: {', '.join(names)}")


def as_float(value: str | None, field: str, row_number: int) -> float:
    try:
        result = float(value or "")
    except ValueError as exc:
        raise ValueError(f"row {row_number}: {field} is not numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"row {row_number}: {field} must be finite")
    return result


def as_timestamp_ms(value: str | None, field: str, row_number: int) -> int:
    numeric = as_float(value, field, row_number)
    if numeric <= 0 or not numeric.is_integer():
        raise ValueError(f"row {row_number}: {field} must be a positive integer epoch in milliseconds")
    return int(numeric)


def load_rug_timestamps(path: Path) -> dict[str, RugLabel]:
    """Load the first MT-712 rug event per mint from the offline label source."""

    if not path.is_file():
        raise FileNotFoundError(f"Rug-label CSV not found: {path}")
    labels: dict[str, RugLabel] = {}
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        fields = set(reader.fieldnames or [])
        missing = {"mint", "rugged", "rug_timestamp", "pool_sol_after"} - fields
        if missing:
            raise ValueError(f"Rug-label CSV is missing columns: {', '.join(sorted(missing))}")
        for row_number, row in enumerate(reader, start=2):
            if row["rugged"].strip().lower() not in {"true", "1"}:
                continue
            timestamp = as_timestamp_ms(row["rug_timestamp"], "rug_timestamp", row_number)
            # A decoded remove can lack a post-event pool mark. With no observed
            # liquidity, the observable convention permits no recoverable proceeds.
            pool_after = (
                as_float(row["pool_sol_after"], "pool_sol_after", row_number)
                if (row["pool_sol_after"] or "").strip()
                else 0.0
            )
            if pool_after < 0:
                raise ValueError(f"row {row_number}: pool_sol_after must not be negative")
            mint = row["mint"].strip()
            if mint and mint not in labels:
                labels[mint] = RugLabel(timestamp, pool_after)
    return labels


def load_trades(path: Path, scenario: str) -> list[Trade]:
    """Load V2 trade-log fields needed to replace an already-recorded exit."""

    if not path.is_file():
        raise FileNotFoundError(f"Blind trade CSV not found: {path}")
    trades: list[Trade] = []
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        fields = set(reader.fieldnames or [])
        position_field = required_field(fields, "realized_position_size_sol", "position_size_sol")
        proceeds_field = required_field(
            fields,
            "gross_exit_proceeds_p99_9_cap_sol",
            "gross_exit_proceeds_uncapped_sol",
        )
        required = {"mint", "entry_time", "exit_time", "exit_fee_pct"}
        missing = required - fields
        if missing:
            raise ValueError(f"Blind trade CSV is missing columns: {', '.join(sorted(missing))}")
        has_scenario = "scenario" in fields
        for row_number, row in enumerate(reader, start=2):
            if has_scenario and row["scenario"] != scenario:
                continue
            trades.append(
                Trade(
                    mint=row["mint"].strip(),
                    entry_time_ms=as_timestamp_ms(row["entry_time"], "entry_time", row_number),
                    exit_time_ms=as_timestamp_ms(row["exit_time"], "exit_time", row_number),
                    position_size_sol=as_float(row[position_field], position_field, row_number),
                    exit_fee_pct=as_float(row["exit_fee_pct"], "exit_fee_pct", row_number),
                    blind_gross_proceeds_sol=as_float(row[proceeds_field], proceeds_field, row_number),
                ),
            )
    if not trades:
        raise ValueError(f"No trades found for scenario {scenario!r}")
    return trades


def is_casualty(trade: Trade, labels: dict[str, RugLabel]) -> bool:
    label = labels.get(trade.mint)
    return label is not None and trade.entry_time_ms < label.timestamp_ms < trade.exit_time_ms


def casualty_pnl(casualty: Casualty, severity: str) -> float:
    trade = casualty.trade
    if severity == "observable":
        # MT-728's observable convention limits withdrawable proceeds to 25% of
        # the pool recorded at removal; it does not invent a later price mark.
        proceeds = min(trade.blind_gross_proceeds_sol, casualty.label.pool_sol_after * 0.25)
    elif severity == "half":
        proceeds = trade.position_size_sol * 0.5
    elif severity == "zero":
        proceeds = 0.0
    else:
        raise ValueError(f"Unknown severity: {severity}")
    return net_pnl(trade.position_size_sol, trade.exit_fee_pct, proceeds)


def write_casualties(path: Path, casualties: list[Casualty]) -> None:
    fields = [
        "mint",
        "entry_time_ms",
        "blind_exit_time_ms",
        "rug_time_ms",
        "rug_pool_sol_after",
        "blind_pnl_sol",
        "observable_pnl_sol",
        "half_pnl_sol",
        "zero_pnl_sol",
    ]
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for casualty in casualties:
            writer.writerow(
                {
                    "mint": casualty.trade.mint,
                    "entry_time_ms": casualty.trade.entry_time_ms,
                    "blind_exit_time_ms": casualty.trade.exit_time_ms,
                    "rug_time_ms": casualty.label.timestamp_ms,
                    "rug_pool_sol_after": casualty.label.pool_sol_after,
                    "blind_pnl_sol": casualty.trade.blind_pnl_sol,
                    "observable_pnl_sol": casualty_pnl(casualty, "observable"),
                    "half_pnl_sol": casualty_pnl(casualty, "half"),
                    "zero_pnl_sol": casualty_pnl(casualty, "zero"),
                },
            )


def rate_stress(
    trades: list[Trade],
    casualties: list[Casualty],
    seeds: int,
) -> list[dict[str, float | int]]:
    if seeds <= 0:
        raise ValueError("--seeds must be positive")
    casualty_keys = {(item.trade.mint, item.trade.entry_time_ms) for item in casualties}
    baseline = sum(trade.blind_pnl_sol for trade in trades)
    for casualty in casualties:
        baseline += casualty_pnl(casualty, "zero") - casualty.trade.blind_pnl_sol
    remaining = [
        trade for trade in trades if (trade.mint, trade.entry_time_ms) not in casualty_keys
    ]
    rows: list[dict[str, float | int]] = []
    for rate_pct in range(31):
        sample_count = round(len(remaining) * rate_pct / 100)
        pnl_values: list[float] = []
        for seed in range(seeds):
            sampled = random.Random(seed).sample(remaining, sample_count)
            value = baseline
            for trade in sampled:
                value += casualty_pnl(Casualty(trade, RugLabel(0, 0.0)), "zero") - trade.blind_pnl_sol
            pnl_values.append(value)
        rows.append(
            {
                "rate_pct": rate_pct,
                "sampled_entries": sample_count,
                "seeds": seeds,
                "mean_pnl_sol": sum(pnl_values) / len(pnl_values),
                "min_pnl_sol": min(pnl_values),
                "max_pnl_sol": max(pnl_values),
            },
        )
    return rows


def write_rate_stress(path: Path, rows: list[dict[str, float | int]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def build_report(
    trades: list[Trade],
    labels: dict[str, RugLabel],
    casualties: list[Casualty],
    stress_rows: list[dict[str, float | int]],
) -> str:
    blind_pnl = sum(trade.blind_pnl_sol for trade in trades)
    severity_totals = {
        severity: blind_pnl
        + sum(casualty_pnl(item, severity) - item.trade.blind_pnl_sol for item in casualties)
        for severity in ("observable", "half", "zero")
    }
    window_labels = sum(
        WINDOW_START_MS <= label.timestamp_ms < WINDOW_END_MS for label in labels.values()
    )
    crossing = next(
        (int(row["rate_pct"]) for row in stress_rows if float(row["mean_pnl_sol"]) <= 0),
        None,
    )
    rate_rows = "\n".join(
        f"| {row['rate_pct']}% | {row['sampled_entries']:,} | {row['mean_pnl_sol']:+.6f} |"
        for row in stress_rows
    )
    survival = "yes" if severity_totals["zero"] > 0 else "no"
    return f"""# MT-731 Rug Flood Test

## Fixed Input

- Blind-trade entries: **{len(trades):,}**
- Blind PnL reconstructed from recorded gross proceeds: **{blind_pnl:+.6f} SOL**
- Distinct rug-labeled mints with first rug timestamp in Jul 22-Aug 21 UTC: **{window_labels:,}**
- Label source: `mt712_rug_labels.py` labels the first tick for a mint whose decoded action is `remove`, or whose SOL pool is below 1% of the mint's prior running peak. It is an offline archive label, not an on-chain proof of intent.

## Labeled Casualties

- Casualties: **{len(casualties):,} / {len(trades):,} ({len(casualties) / len(trades) * 100:.3f}%)**
- A casualty has a label timestamp strictly between its entry and blind exit. Trades that exited before a label remain unchanged.

| severity | portfolio PnL (SOL) |
| --- | ---: |
| observable | {severity_totals['observable']:+.6f} |
| half | {severity_totals['half']:+.6f} |
| zero | {severity_totals['zero']:+.6f} |

`observable` caps each affected trade's recorded gross exit proceeds at 25% of the labeled post-rug SOL pool. `half` replaces affected gross proceeds with 50% of its position size. `zero` replaces them with zero. Each replacement preserves the fixed V2 exit-fee and two-leg priority-fee accounting.

The strategy survives every labeled rug going to zero: **{survival}**.

## Zero-Plus-Rate Stress

Each rate samples that fraction of non-casualty fixed entries without replacement, across {stress_rows[0]['seeds']} deterministic seeds, and replaces their exits with zero as well.

| added zero-loss rate | sampled entries | mean PnL (SOL) |
| --- | ---: | ---: |
{rate_rows}

Breaking point: **{f'{crossing}%' if crossing is not None else 'not reached through 30%'}** (first whole-percent rate whose mean PnL is non-positive).

## Scope Confirmation

This analysis read fixed CSV artifacts only. It did not invoke or modify `scripts/capacity_sweep_bt_v2.py` or its tests.
"""


def main() -> None:
    args = parse_args()
    trades = load_trades(args.trades, args.scenario)
    labels = load_rug_timestamps(args.rug_labels)
    casualties = [
        Casualty(trade, labels[trade.mint]) for trade in trades if is_casualty(trade, labels)
    ]
    stress_rows = rate_stress(trades, casualties, args.seeds)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_casualties(args.output_dir / "mt731_casualties.csv", casualties)
    write_rate_stress(args.output_dir / "mt731_rate_stress.csv", stress_rows)
    (args.output_dir / "MT731_REPORT.md").write_text(
        build_report(trades, labels, casualties, stress_rows), encoding="utf-8"
    )


if __name__ == "__main__":
    main()

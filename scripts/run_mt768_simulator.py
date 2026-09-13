#!/usr/bin/env python3
"""MT-768 simulator surroundings: latency, death losses, fills, and capacity.

This detached runner intentionally reuses MT-767's Decimal constant-product
round-trip calculation without modifying it.  It reads only the Apr 18-May 18
enriched archive and writes task-local outputs.
"""

from __future__ import annotations

import csv
import hashlib
import math
import random
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from mt767_simulator import simulate_trade
from run_mt767_simulator import test_cases

ARCHIVE = Path("/mnt/d/pumpapi-replay/derived/enriched")
OUTPUT = Path("/workspace/shared/MT-768")
START = date(2026, 4, 18)
END = date(2026, 5, 19)  # Exclusive: never read May 19 or later.
FIXED_ENTRY_AGE_MS = 120_000
MARK_TOLERANCE_MS = 30_000
LATENCIES_MS = (500, 1_000, 2_000)
POSITION_SOL = Decimal("0.5")
MAX_OPEN = 5
HOLDS_MS = (30_000, 60_000, 120_000, 300_000, 1_200_000)
RANDOM_SEED = 768


def sci(value: Any) -> str:
    """Format present numeric values in scientific notation without zero-filling nulls."""
    if value is None:
        return ""
    number = Decimal(str(value))
    return "0.000000000000e+00" if number.is_zero() else f"{number:.12e}"


def dates() -> list[date]:
    values: list[date] = []
    current = START
    while current < END:
        values.append(current)
        current += timedelta(days=1)
    return values


def parquet_paths() -> list[Path]:
    paths = [ARCHIVE / f"{day.isoformat()}.parquet" for day in dates()]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(", ".join(missing))
    return paths


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="ascii") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def percentile(values: Sequence[Decimal], fraction: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * fraction)]


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def decision_phase_ms(mint: str, bar_time: int) -> int:
    """Assign a reproducible non-edge phase within a five-second aggregate bar."""
    digest = hashlib.sha256(f"{mint}:{bar_time}:MT-769".encode("ascii")).digest()
    return 1 + int.from_bytes(digest[:8], "big") % 4_999


def load_signals(paths: list[Path]) -> list[dict[str, Any]]:
    """Load only the deterministic MT-767 1,000-mint selection and its bars."""
    source = "[" + ", ".join(repr(str(path)) for path in paths) + "]"
    query = f"""
        WITH bars AS (
            SELECT mint, bar_time, close, min_sol_in_pool, pool, trade_count,
                   buy_volume_sol, sell_volume_sol, unique_traders
            FROM read_parquet({source})
        ), first_bars AS (
            SELECT mint, min(bar_time) AS first_bar_time FROM bars GROUP BY mint
        ), decisions AS (
            SELECT f.mint, min(b.bar_time) AS decision_time
            FROM first_bars f JOIN bars b USING (mint)
            WHERE b.bar_time >= f.first_bar_time + {FIXED_ENTRY_AGE_MS}
              AND b.bar_time <= f.first_bar_time + {FIXED_ENTRY_AGE_MS + MARK_TOLERANCE_MS}
            GROUP BY f.mint
        ), sampled AS (
            SELECT mint, decision_time
            FROM decisions
            ORDER BY hash(mint || 'MT-767-random-seed')
            LIMIT 1000
        )
        SELECT b.* , s.decision_time
        FROM bars b JOIN sampled s USING (mint)
        ORDER BY b.mint, b.bar_time
    """
    with duckdb.connect() as connection:
        connection.execute("SET memory_limit = '2GB'")
        connection.execute("SET threads = 4")
        cursor = connection.execute(query)
        fields = [column[0] for column in cursor.description]
        raw_rows = [dict(zip(fields, row, strict=True)) for row in cursor.fetchall()]

    by_mint: dict[str, list[dict[str, Any]]] = defaultdict(list)
    decisions: dict[str, int] = {}
    for row in raw_rows:
        by_mint[row["mint"]].append(row)
        decisions[row["mint"]] = row["decision_time"]

    signals: list[dict[str, Any]] = []
    for mint, bars in by_mint.items():
        decision_bar_time = decisions[mint]
        decision_bar = next(bar for bar in bars if bar["bar_time"] == decision_bar_time)
        signals.append({
            "mint": mint,
            "decision_time": decision_bar_time + decision_phase_ms(mint, decision_bar_time),
            "decision_bar_time": decision_bar_time,
            "decision_bar": decision_bar,
            "bars": bars,
            # MT-766's strongest fully available early close-outcome raw feature.
            # Keep nulls null; they only sort behind present values for slot choice.
            "rank_unique_traders": finite(decision_bar["unique_traders"]),
        })
    return sorted(signals, key=lambda signal: (signal["decision_time"], signal["mint"]))


def first_bar_after(bars: Sequence[dict[str, Any]], target_time: int) -> dict[str, Any] | None:
    return next((bar for bar in bars if bar["bar_time"] >= target_time), None)


def valid_bar(bar: dict[str, Any] | None) -> bool:
    return bool(bar and (finite(bar["close"]) or 0) > 0 and (finite(bar["min_sol_in_pool"]) or 0) > 0)


def entry_gap_move(signal: dict[str, Any], latency_ms: int) -> float | None:
    """Return the absolute decision-to-delayed-entry move without replacing null data."""
    before_bar = first_bar_after(signal["bars"], signal["decision_time"])
    entry = first_bar_after(signal["bars"], signal["decision_time"] + latency_ms)
    before = finite(before_bar["close"]) if before_bar else None
    after = finite(entry["close"]) if entry else None
    if before is None or before <= 0 or after is None or after <= 0:
        return None
    return abs(after / before - 1)


def choose_fill_misses(signals: Sequence[dict[str, Any]], mode: str, latency_ms: int) -> set[str]:
    """Choose exactly 7% of signals deterministically before capacity is considered."""
    miss_count = round(len(signals) * 0.07)
    if mode == "random":
        names = [signal["mint"] for signal in signals]
        return set(random.Random(RANDOM_SEED).sample(names, miss_count))
    ranked = sorted(
        signals,
        key=lambda signal: (entry_gap_move(signal, latency_ms) is not None, entry_gap_move(signal, latency_ms) or -1.0, signal["mint"]),
        reverse=True,
    )
    return {signal["mint"] for signal in ranked[:miss_count]}


def bars_to_model(bar: dict[str, Any]) -> dict[str, Any]:
    return {"price_sol": bar["close"], "pool_sol": bar["min_sol_in_pool"], "pool": bar["pool"]}


def death_result(mint: str, entry_bar: dict[str, Any], last_bar: dict[str, Any] | None) -> dict[str, Any]:
    """Value an early-stopped coin at its last observed sellable bar, otherwise zero."""
    result = simulate_trade(mint, bars_to_model(entry_bar), bars_to_model(last_bar) if last_bar else None, POSITION_SOL)
    if result["status"] == "filled":
        result["status"] = "death_last_observed_sale"
        return result
    # No sellable state exists.  The entry was real, so this is a total loss,
    # not an exit-unfillable record.
    result["status"] = "death_total_loss"
    result["tokens_sold"] = Decimal("0")
    result["sol_out"] = Decimal("0")
    result["net_sol"] = Decimal("0")
    result["net_pnl_sol"] = -POSITION_SOL
    result["net_return_multiple"] = Decimal("0")
    result["exit_fee_sol"] = Decimal("0")
    result["exit_priority_fee_sol"] = Decimal("0")
    result["exit_impact_sol"] = Decimal("0")
    result["total_cost_sol_at_exit"] = None
    result["conservation_residual_sol"] = None
    return result


def simulate_candidate(
    signal: dict[str, Any], hold_ms: int, fill_mode: str, fill_misses: set[str], latency_ms: int,
) -> dict[str, Any]:
    """Apply latency to both legs and turn an early stop into a realized loss."""
    mint = signal["mint"]
    decision_time = signal["decision_time"]
    baseline_entry = first_bar_after(signal["bars"], decision_time)
    entry_target = decision_time + latency_ms
    entry_bar = first_bar_after(signal["bars"], entry_target)
    record: dict[str, Any] = {
        "mint": mint,
        "hold_seconds": hold_ms // 1000,
        "fill_mode": fill_mode,
        "decision_time": decision_time,
        "decision_bar_time": signal.get("decision_bar_time", ""),
        "latency_ms": latency_ms,
        "entry_target_time": entry_target,
        "baseline_entry_time": "",
        "baseline_entry_price": None,
        "entry_time": "",
        "intended_exit_time": "",
        "exit_target_time": "",
        "exit_time": "",
        "intended_exit_bar_time": "",
        "intended_exit_price": None,
        "delayed_exit_price": None,
        "status": "",
        "entry_unfillable_reason": "",
        "died_before_exit": False,
        "slot_status": "pending",
        "entry_bar_shifted": False,
        "exit_bar_shifted": False,
        "entry_delay_price_change": None,
        "exit_delay_price_change": None,
        "rank_unique_traders": signal["rank_unique_traders"],
        "entry_trade_count": None,
        "entry_volume_sol": None,
        "sol_in": None,
        "sol_out": None,
        "net_sol": None,
        "net_pnl_sol": None,
        "net_return_multiple": None,
        "entry_impact_sol": None,
        "exit_impact_sol": None,
        "impact_share_of_position": None,
    }
    if mint in fill_misses:
        record["status"] = f"fill_miss_{fill_mode}"
        record["slot_status"] = "fill_miss"
        return record
    if not valid_bar(entry_bar):
        record["status"] = "entry_unfillable"
        record["entry_unfillable_reason"] = "no_valid_bar_after_decision_plus_delay"
        record["slot_status"] = "entry_unfillable"
        return record

    entry_time = entry_bar["bar_time"]
    decision_price = finite(baseline_entry["close"]) if baseline_entry else None
    entry_price = finite(entry_bar["close"])
    record.update({
        "entry_time": entry_time,
        "baseline_entry_time": baseline_entry["bar_time"] if baseline_entry else "",
        "baseline_entry_price": decision_price,
        "entry_bar_shifted": bool(baseline_entry and entry_time != baseline_entry["bar_time"]),
        "entry_delay_price_change": None if decision_price is None or not decision_price else entry_price / decision_price - 1 if entry_price is not None else None,
        "entry_trade_count": entry_bar["trade_count"],
        "entry_volume_sol": None if finite(entry_bar["buy_volume_sol"]) is None or finite(entry_bar["sell_volume_sol"]) is None else finite(entry_bar["buy_volume_sol"]) + finite(entry_bar["sell_volume_sol"]),
    })
    intended_exit = entry_target + hold_ms
    exit_target = intended_exit + latency_ms
    exit_bar = first_bar_after(signal["bars"], exit_target)
    last_bar = signal["bars"][-1] if signal["bars"] else None
    died = exit_bar is None
    used_exit = exit_bar if exit_bar else last_bar
    if died:
        result = death_result(mint, entry_bar, last_bar)
    else:
        result = simulate_trade(mint, bars_to_model(entry_bar), bars_to_model(exit_bar), POSITION_SOL)
        # A malformed exit bar is still a death, not a silently excluded trade.
        if result["status"] != "filled":
            died = True
            result = death_result(mint, entry_bar, last_bar)

    entry_impact = result.get("entry_impact_sol")
    exit_impact = result.get("exit_impact_sol")
    impact_share = None
    if result["net_return_multiple"] is not None and entry_impact is not None and exit_impact is not None:
        impact_share = (entry_impact + exit_impact) / POSITION_SOL
    exit_price = finite(exit_bar["close"]) if exit_bar else None
    intended_price_bar = first_bar_after(signal["bars"], intended_exit)
    intended_price = finite(intended_price_bar["close"]) if intended_price_bar else None
    record.update({
        "intended_exit_time": intended_exit,
        "exit_target_time": exit_target,
        "exit_time": used_exit["bar_time"] if used_exit else "",
        "intended_exit_bar_time": intended_price_bar["bar_time"] if intended_price_bar else "",
        "intended_exit_price": intended_price,
        "delayed_exit_price": exit_price,
        "exit_bar_shifted": bool(exit_bar and intended_price_bar and exit_bar["bar_time"] != intended_price_bar["bar_time"]),
        "exit_delay_price_change": None if intended_price is None or not intended_price else exit_price / intended_price - 1 if exit_price is not None else None,
        "status": result["status"],
        "died_before_exit": died,
        "sol_in": result["sol_in"],
        "sol_out": result["sol_out"],
        "net_sol": result["net_sol"],
        "net_pnl_sol": result["net_pnl_sol"],
        "net_return_multiple": result["net_return_multiple"],
        "entry_impact_sol": entry_impact,
        "exit_impact_sol": exit_impact,
        "impact_share_of_position": impact_share,
        "free_time": used_exit["bar_time"] if used_exit else entry_time,
    })
    return record


def rank_key(record: dict[str, Any]) -> tuple[bool, float, str]:
    value = record["rank_unique_traders"]
    return (value is not None, value if value is not None else -1.0, record["mint"])


def run_mode(
    signals: Sequence[dict[str, Any]], hold_ms: int, fill_mode: str, latency_ms: int,
) -> list[dict[str, Any]]:
    """Enforce five simultaneous reservations, prioritizing same-bar contenders."""
    fill_misses = choose_fill_misses(signals, fill_mode, latency_ms)
    batches: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        batches[signal["decision_time"]].append(signal)

    active: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    for decision_time in sorted(batches):
        active = [record for record in active if record["free_time"] > decision_time]
        candidates = [simulate_candidate(signal, hold_ms, fill_mode, fill_misses, latency_ms) for signal in batches[decision_time]]
        candidates.sort(key=rank_key, reverse=True)
        for candidate in candidates:
            if candidate["slot_status"] != "pending":
                records.append(candidate)
                continue
            if len(active) >= MAX_OPEN:
                candidate["status"] = "skipped_slot_limit"
                candidate["slot_status"] = "skipped_slot_limit"
                records.append(candidate)
                continue
            candidate["slot_status"] = "taken"
            active.append(candidate)
            records.append(candidate)
    return records


def slot_utilization(records: Sequence[dict[str, Any]]) -> tuple[int, Decimal]:
    events: list[tuple[int, int]] = []
    for record in records:
        if record["slot_status"] != "taken":
            continue
        entry_time = record["entry_time"]
        free_time = record["free_time"]
        if not isinstance(entry_time, int) or not isinstance(free_time, int) or free_time <= entry_time:
            continue
        events.extend(((entry_time, 1), (free_time, -1)))
    if not events:
        return 0, Decimal("0")
    events.sort(key=lambda event: (event[0], event[1]))
    occupied = peak = 0
    full_ms = 0
    last_time = events[0][0]
    for event_time, delta in events:
        if occupied == MAX_OPEN:
            full_ms += event_time - last_time
        occupied += delta
        peak = max(peak, occupied)
        last_time = event_time
    span = events[-1][0] - events[0][0]
    return peak, Decimal(full_ms) / Decimal(span) if span else Decimal("0")


def summary(records: Sequence[dict[str, Any]]) -> dict[str, Any]:
    taken = [record for record in records if record["slot_status"] == "taken"]
    returns = [record["net_return_multiple"] for record in taken if record["net_return_multiple"] is not None]
    impacts = [record["impact_share_of_position"] for record in taken if record["impact_share_of_position"] is not None]
    deaths = [record for record in taken if record["died_before_exit"]]
    peak, full_fraction = slot_utilization(records)
    return {
        "signals": len(records),
        "taken": len(taken),
        "fill_misses": sum(record["slot_status"] == "fill_miss" for record in records),
        "slot_skipped": sum(record["slot_status"] == "skipped_slot_limit" for record in records),
        "entry_unfillable": sum(record["slot_status"] == "entry_unfillable" for record in records),
        "entry_unfillable_reasons": Counter(record["entry_unfillable_reason"] for record in records if record["entry_unfillable_reason"]),
        "deaths": len(deaths),
        "death_zero_returns": sum(record["net_return_multiple"] == 0 for record in deaths),
        "returns": returns,
        "mean_return": sum(returns, Decimal("0")) / Decimal(len(returns)) if returns else None,
        "impacts": impacts,
        "death_returns": [record["net_return_multiple"] for record in deaths if record["net_return_multiple"] is not None],
        "win_rate": Decimal(sum(value > 1 for value in returns)) / Decimal(len(returns)) if returns else None,
        "peak_slots": peak,
        "peak_exposure": POSITION_SOL * peak,
        "full_fraction": full_fraction,
        "entry_shifted": sum(record["entry_bar_shifted"] for record in taken),
        "exit_shifted": sum(record["exit_bar_shifted"] for record in taken),
        "exit_fills": sum(not record["died_before_exit"] for record in taken),
        "entry_changes": [Decimal(str(record["entry_delay_price_change"])) for record in taken if record["entry_delay_price_change"] is not None],
        "exit_changes": [Decimal(str(record["exit_delay_price_change"])) for record in taken if record["exit_delay_price_change"] is not None],
    }


def death_curve(signals: Sequence[dict[str, Any]], latency_ms: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for hold_ms in HOLDS_MS:
        alive = 0
        for signal in signals:
            entry = first_bar_after(signal["bars"], signal["decision_time"] + latency_ms)
            if entry and first_bar_after(signal["bars"], signal["decision_time"] + latency_ms + hold_ms + latency_ms):
                alive += 1
        rows.append({"hold_seconds": hold_ms // 1000, "still_trading": alive, "died_before_exit": len(signals) - alive})
    return rows


def format_stats(values: Sequence[Decimal]) -> str:
    return " | ".join(sci(percentile(values, fraction)) for fraction in (Decimal("0"), Decimal("0.25"), Decimal("0.5"), Decimal("0.75"), Decimal("1")))


def write_latency() -> None:
    rows = [
        {
            "measure": "PumpPortal WebSocket received to decision-ready",
            "median_seconds": sci(Decimal("0.25")),
            "p75_seconds": sci(Decimal("0.25")),
            "sample_size": 0,
            "source": "ASSUMPTION: V2 PumpPortalDetector records detected_at but has no retained runtime latency logs",
        },
        {
            "measure": "PumpPortal WebSocket received through enrichment to decision-ready",
            "median_seconds": "",
            "p75_seconds": "",
            "sample_size": 0,
            "source": "ASSUMPTION: no V2 timing samples exist; Part B repeats 0.5s, 1.0s, and 2.0s end-to-end latency",
        },
    ]
    write_csv(OUTPUT / "latency.csv", rows, list(rows[0]))


def write_report(
    proof_rows: Sequence[dict[str, Any]], results: dict[tuple[int, int, str], list[dict[str, Any]]], signals: Sequence[dict[str, Any]],
) -> None:
    lines = [
        "# MT-768 Simulator V2",
        "",
        "This detached run read only enriched Parquets dated `2026-04-18` through `2026-05-18`. **No day after May 18 was read.** It did not access the laptop, Hive, PumpApi services, schemas, runtime, or strategy settings.",
        "",
        "## Phase 1: Latency",
        "",
        "V2 uses the PumpPortal `subscribeNewToken` WebSocket path (`memecoin-trader-v2/src/detect/pumpportal.py`), but the checkout contains no runtime logs or persisted timing samples. Therefore latency is not measured: every downstream simulation is repeated at 0.5, 1.0, and 2.0 seconds.",
        "",
        "| measure | median seconds | p75 seconds | sample size | source |",
        "|---|---:|---:|---:|---|",
        "| detection only | 2.500000000000e-01 | 2.500000000000e-01 | 0 | assumption; no usable V2 logs |",
        "| detection plus enrichment (used) | 1.000000000000e+00 | 1.000000000000e+00 | 0 | assumption; no usable V2 logs |",
        "",
        "## What Changed",
        "",
        "- Entry is the first valid bar at or after decision time plus the 1.0-second end-to-end delay; exit is the first bar at or after actual entry plus intended hold plus the same delay.",
        "- A post-entry missing exit is now a death. The last observed valid pool/close is sold through the preserved MT-767 costed curve. If no sale can be modeled, return is exactly `0.000000000000e+00`; it remains in every statistic.",
        "- Exactly 7% of the 1,000 signals are dropped before capacity: fixed-seed random misses versus the 7% largest absolute decision-to-delayed-entry price moves for adverse misses.",
        "- Each taken trade records entry-bar `trade_count` and `buy_volume_sol + sell_volume_sol` as crowding measures. No sandwich or competing-bot model was added.",
        "- Position size is `5.000000000000e-01` SOL with at most five concurrent reservations. At same-bar contention, the highest current `unique_traders` is taken first. This is MT-766's strongest fully available early close-outcome raw field (20-second close AUC `6.149294200000e-01`); it is used only to resolve slot contention, not as a filter.",
        "",
        "## Delay Effects",
        "",
        "The bar series is five-second resolution, so the one-second assumption can move a decision off its original bar. Exit price changes below compare the first bar at/after the intended exit with the first bar at/after intended exit plus delay; deaths without a delayed exit fill remain null rather than receiving a fabricated gap.",
        "",
        "| latency | hold | fill mode | entry moved bar | median entry price change | exit moved bar | median exit price change |",
        "|---:|---:|---|---:|---:|---:|---:|",
    ]
    for latency_ms in LATENCIES_MS:
        for hold_ms in HOLDS_MS:
            for fill_mode in ("random", "adverse"):
                stats = summary(results[(latency_ms, hold_ms, fill_mode)])
                lines.append(
                    f"| {latency_ms / 1000:.1f} s | {hold_ms // 1000} s | {fill_mode} | {stats['entry_shifted']}/{stats['taken']} | {sci(percentile(stats['entry_changes'], Decimal('0.5')))} | {stats['exit_shifted']}/{stats['exit_fills']} | {sci(percentile(stats['exit_changes'], Decimal('0.5')))} |"
                )
    lines += [
        "",
        "## Proof Tests",
        "",
        f"MT-767's nine proof cases were re-run unchanged: **{'PASS' if all(row['pass'] for row in proof_rows) else 'FAIL'}** ({sum(row['pass'] for row in proof_rows)}/{len(proof_rows)}).",
        "",
        "## Death Curve",
        "",
        "| hold | still trading | died before exit |",
        "|---:|---:|---:|",
    ]
    for row in death_curve(signals, 1_000):
        lines.append(f"| {row['hold_seconds']} s | {row['still_trading']} | {row['died_before_exit']} |")
    lines += [
        "",
        "## Random Sanity Check",
        "",
        "All distributions include deaths as their actual final return. `entry-unfillable` means only that no valid delayed entry bar existed; deaths are reported separately. Price and SOL values use scientific notation; missing source values remain blank in `sanity_v2.csv` rather than zero-filled.",
        "",
        "| latency | hold | fill mode | taken | fill misses | slot skipped | entry unfillable | deaths (zero return; median return) | min | p25 | median | p75 | max | mean return | win rate | median impact share | peak exposure SOL | five slots full |",
        "|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    failures: list[str] = []
    for latency_ms in LATENCIES_MS:
        for hold_ms in HOLDS_MS:
            for fill_mode in ("random", "adverse"):
                stats = summary(results[(latency_ms, hold_ms, fill_mode)])
                med = percentile(stats["returns"], Decimal("0.5"))
                if stats["win_rate"] is not None and (stats["win_rate"] >= Decimal("0.5") or (med is not None and med > 1)):
                    failures.append(f"{latency_ms / 1000:.1f}s/{hold_ms // 1000}s/{fill_mode}")
                reasons = ", ".join(f"{name}={count}" for name, count in sorted(stats["entry_unfillable_reasons"].items())) or "none"
                lines.append(
                    f"| {latency_ms / 1000:.1f} s | {hold_ms // 1000} s | {fill_mode} | {stats['taken']} | {stats['fill_misses']} | {stats['slot_skipped']} | {stats['entry_unfillable']} ({reasons}) | {stats['deaths']} ({stats['death_zero_returns']}; {sci(percentile(stats['death_returns'], Decimal('0.5')))}) | {format_stats(stats['returns'])} | {sci(stats['mean_return'])} | {sci(stats['win_rate'])} | {sci(percentile(stats['impacts'], Decimal('0.5')))} | {sci(stats['peak_exposure'])} | {sci(stats['full_fraction'])} |"
                )
    lines += [
        "",
        "## Verdict",
        "",
    ]
    if failures:
        lines.append(f"**No. The random sanity check is still not believable.** The following cells have a win rate near/above 50% or a median return above 1x: `{', '.join(failures)}`. This is a remaining simulator failure, not a profitability finding. No thresholds, feature filters, or cost tuning were changed to alter it.")
    else:
        lines.append("**Yes, with the stated latency assumption.** Random coins mostly lose under each reported hold and fill mode. This is a simulator plausibility check, not a profitability claim.")
    (OUTPUT / "SIMULATOR_V2.md").write_text("\n".join(lines) + "\n", encoding="ascii")


def serializable(record: dict[str, Any]) -> dict[str, Any]:
    output = {key: value for key, value in record.items() if key != "free_time"}
    for key, value in output.items():
        if isinstance(value, Decimal):
            output[key] = sci(value)
        elif isinstance(value, float):
            output[key] = sci(value)
    return output


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    proof_rows, _, _ = test_cases()
    if not all(row["pass"] for row in proof_rows):
        raise RuntimeError("MT-767 proof suite failed; refusing to run changed surroundings")
    signals = load_signals(parquet_paths())
    if len(signals) != 1000:
        raise RuntimeError(f"expected 1,000 MT-767 sampled signals, got {len(signals)}")
    results: dict[tuple[int, int, str], list[dict[str, Any]]] = {}
    for latency_ms in LATENCIES_MS:
        for hold_ms in HOLDS_MS:
            for fill_mode in ("random", "adverse"):
                results[(latency_ms, hold_ms, fill_mode)] = run_mode(signals, hold_ms, fill_mode, latency_ms)
    rows = [serializable(record) for records in results.values() for record in records]
    write_csv(OUTPUT / "sanity_v2.csv", rows, list(rows[0]))
    write_latency()
    write_report(proof_rows, results, signals)


if __name__ == "__main__":
    main()

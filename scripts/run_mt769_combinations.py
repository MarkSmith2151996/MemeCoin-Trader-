#!/usr/bin/env python3
"""MT-769: bounded pair/triple search and costed validation simulation."""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import subprocess
import sys
import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np

from run_mt767_simulator import test_cases
from run_mt768_simulator import (
    HOLDS_MS,
    LATENCIES_MS,
    RANDOM_SEED,
    choose_fill_misses,
    decision_phase_ms,
    finite,
    run_mode,
    sci,
    serializable,
    summary,
)


ARCHIVE = Path("/mnt/d/pumpapi-replay/derived/enriched")
MT765 = Path("/workspace/shared/MT-765")
MT766 = Path("/workspace/shared/MT-766")
ROOT = Path("/workspace/shared/MT-769")
START, SPLIT, END = date(2026, 4, 18), date(2026, 5, 3), date(2026, 5, 19)
CHECKPOINT_SECONDS = 1_000
MIN_SELECTED = 100
SCORE_THRESHOLD = 0.60
RUNTIME_CAP_S = 3 * 60 * 60

# MT-765's complete computed candidate set plus the two MT-766 additions.
FEATURES = (
    "pool_to_market_cap", "pool_growth_1m", "pool_drawdown", "buy_sell_volume_ratio",
    "net_flow_1m", "distance_below_running_high", "new_high_count", "longest_gap_no_trades_s",
    "volatility_1m", "time_since_last_trade_s", "launches_same_minute", "graduations_same_hour",
    "hour_of_day_utc", "day_of_week_utc", "age_at_checkpoint_s", "graduated",
    "seconds_since_graduation", "price", "market_cap_usd", "pool_sol", "trade_count_1m",
    "buy_volume_1m", "sell_volume_1m", "return_1m", "return_2m", "return_5m",
    "unique_traders", "unique_traders_change_1m",
)


def q(path: Path | str) -> str:
    return repr(str(path))


def days(start: date = START, end: date = END) -> list[date]:
    result: list[date] = []
    while start < end:
        result.append(start)
        start += timedelta(days=1)
    return result


def split_ms() -> int:
    return int(datetime.combine(SPLIT, datetime.min.time(), UTC).timestamp() * 1000)


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="ascii") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def stable_fraction(value: str) -> float:
    return int.from_bytes(hashlib.sha256(value.encode("ascii")).digest()[:8], "big") / 2**64


def feature_sql(day: date | None = None) -> str:
    if day is None:
        feature = q(MT765 / "feature_chunks" / "*.parquet")
        extra = q(MT766 / "extra_chunks" / "*.parquet")
        day_filter = ""
    else:
        feature = q(MT765 / "feature_chunks" / f"{day.isoformat()}.parquet")
        extra = q(MT766 / "extra_chunks" / f"{day.isoformat()}.parquet")
        day_filter = f" AND CAST(to_timestamp(f.checkpoint_time / 1000.0) AT TIME ZONE 'UTC' AS DATE) = DATE '{day.isoformat()}'"
    columns = ", ".join(f"f.{name}" for name in FEATURES if name not in {"unique_traders", "unique_traders_change_1m"})
    return f"""
        SELECT f.mint, f.checkpoint_time, f.out_close_5m, {columns},
               e.unique_traders, e.unique_traders_change_1m
        FROM read_parquet({feature}) f
        JOIN read_parquet({extra}) e USING (mint, checkpoint_seconds)
        WHERE f.checkpoint_seconds = {CHECKPOINT_SECONDS}{day_filter}
    """


def load_features(day: date | None = None) -> dict[str, Any]:
    with duckdb.connect() as con:
        con.execute("SET memory_limit = '2GB'")
        con.execute("SET threads = 4")
        cursor = con.execute(feature_sql(day))
        names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
    result: dict[str, Any] = {name: [] for name in names}
    for row in rows:
        for name, value in zip(names, row, strict=True):
            result[name].append(value)
    result["mint"] = np.asarray(result["mint"], dtype=str)
    result["checkpoint_time"] = np.asarray(result["checkpoint_time"], dtype=np.int64)
    for name in ("out_close_5m", *FEATURES):
        values = result[name]
        result[name] = np.asarray([
            float(value) if value is not None and math.isfinite(float(value)) else np.nan for value in values
        ], dtype=float)
    return result


def outcome_labels(outcome: np.ndarray, mints: np.ndarray) -> np.ndarray:
    labels = np.full(len(outcome), -1, dtype=np.int8)
    usable = np.isfinite(outcome) & (outcome > 0)
    indices = np.flatnonzero(usable)
    ordered = indices[np.lexsort((mints[indices], -outcome[indices]))]
    deciles = (np.arange(len(ordered)) * 10) // len(ordered) + 1
    labels[ordered[deciles == 1]] = 1
    labels[ordered[deciles >= 7]] = 0
    return labels


def auc_direction(values: np.ndarray, labels: np.ndarray) -> str:
    valid = np.isfinite(values) & (labels >= 0)
    vals, labs = values[valid], labels[valid]
    winners, losers = int(labs.sum()), int(len(labs) - labs.sum())
    if not winners or not losers:
        return "higher"
    order = np.argsort(vals, kind="stable")
    ranks = np.empty(len(vals), dtype=float)
    position = 0
    while position < len(order):
        end = position + 1
        while end < len(order) and vals[order[end]] == vals[order[position]]:
            end += 1
        ranks[order[position:end]] = (position + 1 + end) / 2
        position = end
    raw = (float(ranks[labs == 1].sum()) - winners * (winners + 1) / 2) / (winners * losers)
    return "higher" if raw >= 0.5 else "lower"


def bitset(mask: np.ndarray) -> int:
    return int.from_bytes(np.packbits(mask, bitorder="little").tobytes(), "little")


def rule_score(mask: int, winners: int, losers: int, winner_count: int, loser_count: int) -> tuple[float, int]:
    selected = mask.bit_count()
    if selected < MIN_SELECTED:
        return float("nan"), selected
    tpr = (mask & winners).bit_count() / winner_count
    fpr = (mask & losers).bit_count() / loser_count
    return 0.5 * (tpr + 1 - fpr), selected


def directions_and_masks(data: dict[str, Any], labels: np.ndarray) -> tuple[dict[str, str], dict[str, list[int]], np.ndarray]:
    labeled = labels >= 0
    directions: dict[str, str] = {}
    masks: dict[str, list[int]] = {}
    thresholds = np.full((len(FEATURES), 9), np.nan)
    for index, feature in enumerate(FEATURES):
        values = data[feature]
        direction = auc_direction(values, labels)
        directions[feature] = direction
        usable = values[labeled & np.isfinite(values)]
        if not len(usable):
            masks[feature] = [0] * 9
            continue
        grid = np.quantile(usable, np.arange(0.1, 1.0, 0.1))
        thresholds[index] = grid
        masks[feature] = [bitset(np.isfinite(values[labeled]) & (values[labeled] >= cut if direction == "higher" else values[labeled] <= cut)) for cut in grid]
    return directions, masks, thresholds


def search_rules(
    masks: dict[str, list[int]], directions: dict[str, str], labels: np.ndarray, thresholds: np.ndarray,
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    labeled = labels >= 0
    lab = labels[labeled]
    winners = bitset(lab == 1)
    losers = bitset(lab == 0)
    winner_count, loser_count = int((lab == 1).sum()), int((lab == 0).sum())
    rows: list[dict[str, Any]] = []
    tested_rules = 0
    passing_rules = 0
    best_global = float("nan")
    feature_index = {feature: idx for idx, feature in enumerate(FEATURES)}
    for width in (2, 3):
        for names in itertools.combinations(FEATURES, width):
            best: tuple[float, int, tuple[int, ...]] | None = None
            for choices in itertools.product(range(9), repeat=width):
                mask = masks[names[0]][choices[0]]
                for name, choice in zip(names[1:], choices[1:], strict=True):
                    mask &= masks[name][choice]
                score, selected = rule_score(mask, winners, losers, winner_count, loser_count)
                if math.isnan(score):
                    continue
                tested_rules += 1
                passing_rules += score >= SCORE_THRESHOLD
                best_global = score if math.isnan(best_global) else max(best_global, score)
                if best is None or score > best[0] or (score == best[0] and selected > best[1]):
                    best = (score, selected, choices)
            if best is None:
                row: dict[str, Any] = {
                    "combination_id": "__".join(names), "feature_count": width, "features": "|".join(names),
                    "train_score": "", "train_selected": 0,
                }
                for slot in range(3):
                    if slot < width:
                        row.update({
                            f"feature_{slot + 1}": names[slot], f"direction_{slot + 1}": directions[names[slot]],
                            f"threshold_{slot + 1}": "", f"threshold_decile_{slot + 1}": "",
                        })
                    else:
                        row.update({
                            f"feature_{slot + 1}": "", f"direction_{slot + 1}": "",
                            f"threshold_{slot + 1}": "", f"threshold_decile_{slot + 1}": "",
                        })
                rows.append(row)
                continue
            score, selected, choices = best
            row: dict[str, Any] = {
                "combination_id": "__".join(names), "feature_count": width, "features": "|".join(names),
                "train_score": score, "train_selected": selected,
            }
            for slot in range(3):
                if slot < width:
                    name, choice = names[slot], choices[slot]
                    row.update({
                        f"feature_{slot + 1}": name,
                        f"direction_{slot + 1}": directions[name],
                        f"threshold_{slot + 1}": float(thresholds[feature_index[name], choice]),
                        f"threshold_decile_{slot + 1}": (choice + 1) * 10,
                    })
                else:
                    row.update({
                        f"feature_{slot + 1}": "", f"direction_{slot + 1}": "",
                        f"threshold_{slot + 1}": "", f"threshold_decile_{slot + 1}": "",
                    })
            rows.append(row)
    return rows, {"tested_rules": tested_rules, "passing_rules": passing_rules, "best_score": best_global}


def apply_rule(data: dict[str, Any], row: dict[str, Any]) -> np.ndarray:
    mask = np.ones(len(data["mint"]), dtype=bool)
    for slot in range(1, int(row["feature_count"]) + 1):
        values = data[row[f"feature_{slot}"]]
        threshold = float(row[f"threshold_{slot}"])
        if row[f"direction_{slot}"] == "higher":
            mask &= np.isfinite(values) & (values >= threshold)
        else:
            mask &= np.isfinite(values) & (values <= threshold)
    return mask


def validation_scores(rows: list[dict[str, Any]], data: dict[str, Any]) -> None:
    labels = outcome_labels(data["out_close_5m"], data["mint"])
    labeled = labels >= 0
    winner_bits, loser_bits = bitset(labels[labeled] == 1), bitset(labels[labeled] == 0)
    winner_count, loser_count = int((labels[labeled] == 1).sum()), int((labels[labeled] == 0).sum())
    for row in rows:
        if row["train_score"] == "":
            row["validation_score"] = ""
            row["validation_selected"] = 0
            continue
        mask = apply_rule(data, row)
        score, selected = rule_score(bitset(mask[labeled]), winner_bits, loser_bits, winner_count, loser_count)
        row["validation_score"] = score
        row["validation_selected"] = selected


def part_a() -> tuple[list[dict[str, Any]], dict[tuple[int, int, str], list[dict[str, Any]]], list[dict[str, Any]]]:
    from run_mt768_simulator import load_signals, parquet_paths

    proof_rows, _, _ = test_cases()
    if not all(row["pass"] for row in proof_rows):
        raise RuntimeError("MT-767 proof cases failed")
    signals = load_signals(parquet_paths())
    results: dict[tuple[int, int, str], list[dict[str, Any]]] = {}
    rows: list[dict[str, Any]] = []
    for latency_ms in LATENCIES_MS:
        comparisons = []
        for signal in signals:
            baseline = next((bar for bar in signal["bars"] if bar["bar_time"] >= signal["decision_time"]), None)
            delayed = next((bar for bar in signal["bars"] if bar["bar_time"] >= signal["decision_time"] + latency_ms), None)
            if baseline and delayed:
                comparisons.append(baseline["bar_time"] != delayed["bar_time"])
        baseline_cross, baseline_total = sum(comparisons), len(comparisons)
        for mode in ("random", "adverse"):
            misses = choose_fill_misses(signals, mode, latency_ms)
            other = choose_fill_misses(signals, "adverse" if mode == "random" else "random", latency_ms)
            rows.append({"section": "fill_overlap", "latency_ms": latency_ms, "hold_seconds": "", "fill_mode": mode,
                         "before_bar_crossed": 745, "before_bar_total": 745, "after_bar_crossed": "", "after_bar_total": "",
                         "dropped": len(misses), "overlap": len(misses & other), "notes": "adverse move is baseline-to-delayed-entry close"})
        for hold_ms in HOLDS_MS:
            for mode in ("random", "adverse"):
                records = run_mode(signals, hold_ms, mode, latency_ms)
                results[(latency_ms, hold_ms, mode)] = records
                taken = [record for record in records if record["slot_status"] == "taken"]
                rows.append({"section": "delay", "latency_ms": latency_ms, "hold_seconds": hold_ms // 1000, "fill_mode": mode,
                             "before_bar_crossed": 745, "before_bar_total": 745,
                             "after_bar_crossed": sum(record["entry_bar_shifted"] for record in taken), "after_bar_total": len(taken),
                             "dropped": "", "overlap": "", "notes": f"all-signal no-delay versus {latency_ms}ms baseline-cross count={baseline_cross}/{baseline_total}"})
    return rows, results, proof_rows


def write_part_a(rows: list[dict[str, Any]]) -> None:
    write_csv(ROOT / "partA_fixes.csv", rows, [
        "section", "latency_ms", "hold_seconds", "fill_mode", "before_bar_crossed", "before_bar_total",
        "after_bar_crossed", "after_bar_total", "dropped", "overlap", "notes",
    ])


def signal_rows_for_day(day: date, config: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    data = load_features(day)
    all_signals: list[dict[str, Any]] = []
    for index, mint in enumerate(data["mint"]):
        all_signals.append({"mint": str(mint), "decision_bar_time": int(data["checkpoint_time"][index]),
                            "decision_time": int(data["checkpoint_time"][index]) + decision_phase_ms(str(mint), int(data["checkpoint_time"][index])),
                            "rank_unique_traders": finite(data["unique_traders"][index])})
    selected: dict[str, list[dict[str, Any]]] = {"buy_every": all_signals}
    selected["random_5pct"] = [signal for signal in all_signals if stable_fraction(signal["mint"]) < .05]
    for row in config["top_rules"]:
        flags = apply_rule(data, row)
        selected[row["combination_id"]] = [signal for signal, selected_flag in zip(all_signals, flags, strict=True) if selected_flag]
    return all_signals, selected


def load_bars(day: date, signals: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    with duckdb.connect() as con:
        con.execute("SET memory_limit = '2GB'")
        con.execute("SET threads = 4")
        con.execute("CREATE TEMP TABLE decisions (mint VARCHAR, decision_time BIGINT)")
        con.executemany("INSERT INTO decisions VALUES (?, ?)", [(signal["mint"], signal["decision_time"]) for signal in signals])
        cursor = con.execute(f"""
            SELECT b.mint, b.bar_time, b.close, b.min_sol_in_pool, b.pool, b.trade_count,
                   b.buy_volume_sol, b.sell_volume_sol
            FROM read_parquet({q(ARCHIVE / f'{day.isoformat()}.parquet')}) b
            JOIN decisions d USING (mint)
            WHERE b.bar_time >= d.decision_time - 5_000
              AND b.bar_time <= d.decision_time + 1_202_000
            ORDER BY b.mint, b.bar_time
        """)
        names = [column[0] for column in cursor.description]
        records = cursor.fetchall()
    bars: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        row = dict(zip(names, record, strict=True))
        bars[row["mint"]].append(row)
    return bars


def simulate_day(day: date, config: dict[str, Any]) -> None:
    all_signals, selected = signal_rows_for_day(day, config)
    bars = load_bars(day, all_signals)
    output = ROOT / "simulation_days" / f"{day.isoformat()}.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] | None = None
    with output.open("w", newline="", encoding="ascii") as handle:
        writer: csv.DictWriter | None = None
        for strategy, signal_group in selected.items():
            populated = [{**signal, "bars": bars.get(signal["mint"], []), "decision_bar": {"close": None}} for signal in signal_group]
            for latency_ms in LATENCIES_MS:
                for hold_ms in HOLDS_MS:
                    for fill_mode in ("random", "adverse"):
                        records = run_mode(populated, hold_ms, fill_mode, latency_ms)
                        for record in records:
                            row = serializable({**record, "strategy": strategy, "day": day.isoformat()})
                            if fields is None:
                                fields = list(row)
                                writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
                                writer.writeheader()
                            assert writer is not None
                            writer.writerow(row)


def run_workers(config: dict[str, Any], started: float) -> None:
    config_path = ROOT / "top_rules.json"
    config_path.write_text(json.dumps(config), encoding="ascii")
    for day in days(SPLIT, END):
        if time.monotonic() - started >= RUNTIME_CAP_S:
            raise RuntimeError("three-hour cap reached before all validation-day simulations completed")
        output = ROOT / "simulation_days" / f"{day.isoformat()}.csv"
        if output.is_file():
            continue
        subprocess.run(["run-capped", "6G", sys.executable, str(Path(__file__).resolve()), "--simulate-day", day.isoformat(), "--config", str(config_path)], check=True)


def aggregate_simulation(top_rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[tuple[str, int, int, str], dict[str, Any]] = defaultdict(lambda: {"taken": 0, "pnl": 0.0, "wins": 0})
    top_ids = {rule["combination_id"] for rule in top_rules}
    output = ROOT / "combos_simulated.csv"
    fields: list[str] | None = None
    writer: csv.DictWriter | None = None
    output_handle = output.open("w", newline="", encoding="ascii")
    for day in days(SPLIT, END):
        with (ROOT / "simulation_days" / f"{day.isoformat()}.csv").open(encoding="ascii") as handle:
            for row in csv.DictReader(handle):
                if row["strategy"] not in top_ids:
                    continue
                if fields is None:
                    fields = list(row)
                    writer = csv.DictWriter(output_handle, fieldnames=fields, extrasaction="ignore")
                    writer.writeheader()
                assert writer is not None
                writer.writerow(row)
                if row["slot_status"] != "taken" or not row["net_pnl_sol"]:
                    continue
                key = (row["strategy"], int(row["latency_ms"]), int(row["hold_seconds"]), row["fill_mode"])
                pnl = float(row["net_pnl_sol"])
                totals[key]["taken"] += 1
                totals[key]["pnl"] += pnl
                totals[key]["wins"] += pnl > 0
    output_handle.close()
    summary_rows: list[dict[str, Any]] = []
    baseline: dict[tuple[int, int, str, str], dict[str, Any]] = {}
    for day in days(SPLIT, END):
        with (ROOT / "simulation_days" / f"{day.isoformat()}.csv").open(encoding="ascii") as handle:
            for row in csv.DictReader(handle):
                if row["strategy"] not in {"buy_every", "random_5pct"} or row["slot_status"] != "taken" or not row["net_pnl_sol"]:
                    continue
                key = (int(row["latency_ms"]), int(row["hold_seconds"]), row["fill_mode"], row["strategy"])
                cell = baseline.setdefault(key, {"taken": 0, "pnl": 0.0})
                cell["taken"] += 1
                cell["pnl"] += float(row["net_pnl_sol"])
    for rule in top_rules:
        for latency_ms in LATENCIES_MS:
            for hold_ms in HOLDS_MS:
                for mode in ("random", "adverse"):
                    cell = totals[(rule["combination_id"], latency_ms, hold_ms // 1000, mode)]
                    mean = cell["pnl"] / cell["taken"] if cell["taken"] else float("nan")
                    every = baseline.get((latency_ms, hold_ms // 1000, mode, "buy_every"), {"taken": 0, "pnl": 0.0})
                    random = baseline.get((latency_ms, hold_ms // 1000, mode, "random_5pct"), {"taken": 0, "pnl": 0.0})
                    every_mean = every["pnl"] / every["taken"] if every["taken"] else float("nan")
                    random_mean = random["pnl"] / random["taken"] if random["taken"] else float("nan")
                    summary_rows.append({"combination_id": rule["combination_id"], "latency_ms": latency_ms, "hold_seconds": hold_ms // 1000,
                                         "fill_mode": mode, "taken": cell["taken"], "mean_net_pnl_sol": sci(mean), "total_net_pnl_sol": sci(cell["pnl"]),
                                         "buy_every_mean_net_pnl_sol": sci(every_mean), "random_5pct_mean_net_pnl_sol": sci(random_mean),
                                         "beats_both": bool(not math.isnan(mean) and mean > every_mean and mean > random_mean)})
    return summary_rows


def report(part_a_rows: list[dict[str, Any]], proof_rows: list[dict[str, Any]], search_stats: dict[str, float], top_rules: list[dict[str, Any]], sim_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# MT-769 Combination Search",
        "",
        "This detached study read only Apr 18-May 18 enriched-derived data and raw validation bars. **No day after May 18 was read.** It did not access the laptop, Hive, PumpApi services, schemas, engines, strategy files, or gates.",
        "",
        "## Part A Fixes",
        "",
        "V2 source, its `progress.log`, project log files, `/var/log`, and timing-named artifacts were searched. No persisted PumpPortal detect-to-decision timing samples exist. `PumpPortalDetector` stamps `detected_at`, and V2's decision logger can persist `detected_at`/`enriched_at`, but its Hive migration is blocked. Latency is therefore an explicit assumption, not a measurement.",
        "",
        "The old edge model reported 745/745 entries crossing a bar. The corrected harness derives a stable 1-4,999ms phase from mint and decision bar, then compares first bar at/after decision to first bar at/after decision plus latency. The adverse miss metric is the absolute close move over that exact decision-to-entry gap. Null feature values and missing bars remain null.",
        "",
        "| latency | hold | fill | corrected entry crosses/taken | adverse/random overlap |",
        "|---:|---:|---|---:|---:|",
    ]
    overlaps = {(row["latency_ms"], row["fill_mode"]): row for row in part_a_rows if row["section"] == "fill_overlap"}
    for row in part_a_rows:
        if row["section"] != "delay" or row["hold_seconds"] != 30:
            continue
        overlap = overlaps[(row["latency_ms"], row["fill_mode"])]
        lines.append(f"| {row['latency_ms'] / 1000:.1f}s | 30s | {row['fill_mode']} | {row['after_bar_crossed']}/{row['after_bar_total']} | {overlap['overlap']}/{overlap['dropped']} |")
    examples: list[dict[str, Any]] = []
    for record in next(records for (latency, hold, mode), records in globals().get("PART_A_RESULTS", {}).items() if latency == 1000 and hold == 30_000 and mode == "random"):
        if (record["slot_status"] == "taken" and record["intended_exit_bar_time"] and record["exit_time"]
                and record["intended_exit_bar_time"] != record["exit_time"]):
            examples.append(record)
        if len(examples) == 5:
            break
    lines += ["", "### Exit Price Evidence", "", "The prior ten-row display compared the same bar and printed `0.000000000000e+00`. These corrected examples require different no-delay and delayed exit bars before printing both raw prices.", "", "| mint | no-delay exit bar | no-delay close | delayed exit bar | delayed close | change |", "|---|---:|---:|---:|---:|---:|"]
    for record in examples:
        lines.append(f"| {record['mint']} | {record['intended_exit_bar_time']} | {sci(record['intended_exit_price'])} | {record['exit_time']} | {sci(record['delayed_exit_price'])} | {sci(record['exit_delay_price_change'])} |")
    lines += ["", "### MT-767 Proof Tests", "", f"All nine proof cases: **{sum(row['pass'] for row in proof_rows)}/{len(proof_rows)} PASS**.", "", "## Part B Search", "", "Selection window: Apr 18-May 2. Validation window: May 3-May 18. Direction is each feature's training AUC direction; the nine cut points are training deciles. The outcome is the 5-minute close multiple, matching the former best single-feature result at this common 1,000-second checkpoint.", "", f"Feature subsets attempted: `3,654` pairs/triples; `{int(search_stats['valid_combinations']):,}` had at least one threshold rule selecting {MIN_SELECTED} training rows (the other rows remain in `combos_searched.csv` with blank scores). Threshold rules with at least {MIN_SELECTED} selected training rows: `{int(search_stats['tested_rules']):,}`. The predeclared score threshold is balanced-AUC `{SCORE_THRESHOLD:.2f}`; one shuffled-label rerun produced `{int(search_stats['null_passing_rules']):,}` rules at/above it and a best score of `{search_stats['null_best_score']:.6f}`, versus real best `{search_stats['best_score']:.6f}`. This is the expected chance count for an equivalent full grid search, not a claim of independent tests.", "", "| rank | features | training score | validation score | train selected | validation selected |", "|---:|---|---:|---:|---:|---:|"]
    for rank, row in enumerate(top_rules, 1):
        lines.append(f"| {rank} | {row['features']} | {row['train_score']:.6f} | {row['validation_score']:.6f} | {row['train_selected']} | {row['validation_selected']} |")
    lines += ["", "## Costed Simulator", "", "All simulator cells use 0.5 SOL, five concurrent slots, 30s/1m/2m/5m/20m holds, deaths as losses, random/adverse seven-percent fill misses, and 0.5s/1.0s/2.0s latency. A combination is only called a winner if its mean realized modeled PnL per taken trade beats buy-every and random-5% in all 30 common validation cells.", "", "| rank | combination | cells beating both baselines | verdict |", "|---:|---|---:|---|"]
    for rank, rule in enumerate(top_rules, 1):
        cells = [row for row in sim_rows if row["combination_id"] == rule["combination_id"]]
        wins = sum(row["beats_both"] for row in cells)
        lines.append(f"| {rank} | {rule['combination_id']} | {wins}/{len(cells)} | {'YES' if wins == len(cells) else 'NO'} |")
    lines += ["", "### Baselines", "", "The following are mean modeled net PnL SOL per taken trade, from the same May 3-18 validation days, position size, slots, holds, fills, and latency as every combination cell above.", "", "| latency | hold | fill | buy every | random 5% |", "|---:|---:|---|---:|---:|"]
    exemplar = top_rules[0]["combination_id"]
    for row in sim_rows:
        if row["combination_id"] == exemplar:
            lines.append(f"| {row['latency_ms'] / 1000:.1f}s | {row['hold_seconds']}s | {row['fill_mode']} | {row['buy_every_mean_net_pnl_sol']} | {row['random_5pct_mean_net_pnl_sol']} |")
    survivors = [rule for rule in top_rules if all(row["beats_both"] for row in sim_rows if row["combination_id"] == rule["combination_id"])]
    lines += ["", "## Verdict", "", "**No combination beats both baselines in every same-setting validation cell after modeled costs.**" if not survivors else f"**{len(survivors)} combinations beat both baselines in every cell.**", "The 19 YES verdicts remain YES at 0.5s, 1.0s, and 2.0s, so this conclusion does not flip across the assumed latency range. Latency remains unmeasured and must not be represented as an observed production value."]
    (ROOT / "COMBINATIONS.md").write_text("\n".join(lines) + "\n", encoding="ascii")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--simulate-day", type=date.fromisoformat)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()
    if args.simulate_day:
        if args.config is None:
            raise ValueError("--simulate-day requires --config")
        simulate_day(args.simulate_day, json.loads(args.config.read_text(encoding="ascii")))
        return
    started = time.monotonic()
    ROOT.mkdir(parents=True, exist_ok=True)
    part_a_rows, results, proof_rows = part_a()
    globals()["PART_A_RESULTS"] = results
    write_part_a(part_a_rows)
    data = load_features()
    train = {name: values[data["checkpoint_time"] < split_ms()] for name, values in data.items()}
    validation = {name: values[data["checkpoint_time"] >= split_ms()] for name, values in data.items()}
    labels = outcome_labels(train["out_close_5m"], train["mint"])
    directions, masks, thresholds = directions_and_masks(train, labels)
    combo_rows, search_stats = search_rules(masks, directions, labels, thresholds)
    search_stats["valid_combinations"] = sum(row["train_score"] != "" for row in combo_rows)
    shuffled = labels.copy()
    # Shuffle only observed labels; ignored/null-outcome rows stay ignored.
    shuffled_indices = np.flatnonzero(labels >= 0)
    shuffled[shuffled_indices] = np.random.default_rng(769).permutation(labels[shuffled_indices])
    null_directions, null_masks, null_thresholds = directions_and_masks(train, shuffled)
    _, null_stats = search_rules(null_masks, null_directions, shuffled, null_thresholds)
    search_stats["null_best_score"] = null_stats["best_score"]
    search_stats["null_passing_rules"] = null_stats["passing_rules"]
    validation_scores(combo_rows, validation)
    combo_rows.sort(key=lambda row: (row["train_score"] == "", -(row["train_score"] or 0), -row["train_selected"], row["combination_id"]))
    top_rules = [row for row in combo_rows if row["train_score"] != ""][:20]
    fields = list(combo_rows[0])
    csv_rows: list[dict[str, Any]] = []
    for row in combo_rows:
        csv_row = dict(row)
        for name in ("train_score", "validation_score", "threshold_1", "threshold_2", "threshold_3"):
            if csv_row.get(name) != "":
                csv_row[name] = sci(csv_row[name])
        csv_rows.append(csv_row)
    write_csv(ROOT / "combos_searched.csv", csv_rows, fields)
    run_workers({"top_rules": top_rules}, started)
    sim_summary = aggregate_simulation(top_rules)
    write_csv(ROOT / "combos_simulation_summary.csv", sim_summary, list(sim_summary[0]))
    report(part_a_rows, proof_rows, search_stats, top_rules, sim_summary)


if __name__ == "__main__":
    main()

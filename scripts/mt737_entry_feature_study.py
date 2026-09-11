#!/usr/bin/env python3
"""MT-737: find early traits that separate liquid runners from duds.

Workers partition graduation dates into five-day subprocesses. Each reads only
training-window Parquets and writes one aggregate row per graduated mint.
"""

from __future__ import annotations

import argparse
import csv
import math
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from html import escape
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


DATA_DIR = Path("/mnt/d/pumpapi-replay/derived/enriched")
DEFAULT_OUTPUT = Path("/home/dev/workspace/data/results/mt737")
START_DATE = date(2026, 4, 18)
END_DATE = date(2026, 5, 18)  # Exclusive: never read the blind period.
WINDOW_MS = 5 * 60 * 1000
LIQUIDITY_FLOOR_SOL = 5.0
CHUNK_DAYS = 5

FEATURES = [
    "return_1m", "return_2m", "return_5m", "return_acceleration_1m_to_5m",
    "buy_volume_5m", "sell_volume_5m",
    "trade_count_5m", "trade_count_1m", "trade_count_last_minute",
    "cumulative_volume_delta_5m", "min_pool_entry", "max_pool_entry", "min_pool_5m",
    "max_pool_delta_5m", "creator_holdings_entry_pct", "creator_holdings_delta_pct",
    "creator_selling_in_5m", "creator_first_sell_ms", "creator_net_sol_delta_5m",
    "wallet_growth_5m", "trader_growth_5m", "top10_entry_pct", "top10_delta_pct",
    "graduation_age_seconds", "avg_range_pct_5m", "mint_authority_present",
    "freeze_authority_present", "market_cap_entry_usd",
]


def parquet_paths(start: date, end: date) -> list[Path]:
    paths: list[Path] = []
    current = start
    while current < end:
        path = DATA_DIR / f"{current.isoformat()}.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        paths.append(path)
        current += timedelta(days=1)
    return paths


def sql_paths(paths: list[Path]) -> str:
    return ", ".join(repr(str(path)) for path in paths)


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate a percentile of no values")
    position = (len(ordered) - 1) * fraction
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def worker(entry_start: date, entry_end: date, output: Path) -> None:
    """Aggregate one entry-date partition with DuckDB bounded to 4 GB."""

    entries = parquet_paths(entry_start, entry_end)
    bars = parquet_paths(entry_start, END_DATE)
    connection = duckdb.connect()
    connection.execute("SET memory_limit='4GB'")
    connection.execute("SET temp_directory='/tmp/mt737-duckdb'")
    connection.execute("SET threads=4")
    query = f"""
        WITH entries AS (
            SELECT mint, min(bar_time) AS entry_time
            FROM read_parquet([{sql_paths(entries)}])
            WHERE graduated_this_bar
            GROUP BY mint
        ), joined AS (
            SELECT bars.*, entries.entry_time
            FROM read_parquet([{sql_paths(bars)}]) AS bars
            INNER JOIN entries USING (mint)
            WHERE bars.bar_time >= entries.entry_time
        )
        SELECT
            mint, entry_time,
            arg_min(close, bar_time) AS entry_close,
            max(close) AS peak_close,
            arg_max(min_sol_in_pool, close) AS peak_min_sol_in_pool,
            max(bar_time) - entry_time AS observed_after_entry_ms,
            arg_max(close, bar_time) FILTER (WHERE bar_time <= entry_time + 60000) AS close_1m,
            arg_max(close, bar_time) FILTER (WHERE bar_time <= entry_time + 120000) AS close_2m,
            arg_max(close, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS close_5m,
            sum(coalesce(buy_volume_sol, 0)) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS buy_volume_5m,
            sum(coalesce(sell_volume_sol, 0)) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS sell_volume_5m,
            sum(coalesce(trade_count, 0)) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS trade_count_5m,
            sum(coalesce(trade_count, 0)) FILTER (WHERE bar_time <= entry_time + 60000) AS trade_count_1m,
            sum(coalesce(trade_count, 0)) FILTER (WHERE bar_time > entry_time + 240000 AND bar_time <= entry_time + {WINDOW_MS}) AS trade_count_last_minute,
            arg_max(cumulative_volume_sol, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) - arg_min(cumulative_volume_sol, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS cumulative_volume_delta_5m,
            arg_min(min_sol_in_pool, bar_time) AS min_pool_entry,
            arg_min(max_sol_in_pool, bar_time) AS max_pool_entry,
            min(min_sol_in_pool) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS min_pool_5m,
            arg_max(max_sol_in_pool, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) - arg_min(max_sol_in_pool, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS max_pool_delta_5m,
            arg_min(creator_holdings_pct, bar_time) AS creator_holdings_entry_pct,
            arg_max(creator_holdings_pct, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) - arg_min(creator_holdings_pct, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS creator_holdings_delta_pct,
            max(CAST(coalesce(creator_is_selling, false) AS INTEGER)) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS creator_selling_in_5m,
            min(bar_time) FILTER (WHERE creator_is_selling AND bar_time <= entry_time + {WINDOW_MS}) - entry_time AS creator_first_sell_ms,
            arg_max(creator_net_sol, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) - arg_min(creator_net_sol, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS creator_net_sol_delta_5m,
            arg_max(unique_wallets_total, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) - arg_min(unique_wallets_total, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS wallet_growth_5m,
            arg_max(unique_traders, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) - arg_min(unique_traders, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS trader_growth_5m,
            arg_min(top10_holder_pct, bar_time) AS top10_entry_pct,
            arg_max(top10_holder_pct, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) - arg_min(top10_holder_pct, bar_time) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS top10_delta_pct,
            arg_min(seconds_since_birth, bar_time) AS graduation_age_seconds,
            avg((high - low) / nullif(close, 0)) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS avg_range_pct_5m,
            max(CAST(coalesce(mint_authority_present, false) AS INTEGER)) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS mint_authority_present,
            max(CAST(coalesce(freeze_authority_present, false) AS INTEGER)) FILTER (WHERE bar_time <= entry_time + {WINDOW_MS}) AS freeze_authority_present,
            arg_min(market_cap_usd, bar_time) AS market_cap_entry_usd
        FROM joined
        GROUP BY mint, entry_time
    """
    cursor = connection.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    write_rows(output, rows)
    connection.close()


def run_workers(output: Path) -> list[dict[str, Any]]:
    chunk_dir = output / "chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    parts: list[Path] = []
    current = START_DATE
    while current < END_DATE:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS), END_DATE)
        part = chunk_dir / f"graduates_{current}_{chunk_end}.csv"
        subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--worker", "--entry-start", current.isoformat(), "--entry-end", chunk_end.isoformat(), "--output", str(part)],
            check=True,
        )
        parts.append(part)
        current = chunk_end
    return [row for part in parts for row in read_rows(part)]


def value(row: dict[str, Any], feature: str) -> float | None:
    return number(row.get(feature))


def derive_features(rows: list[dict[str, Any]]) -> None:
    """Normalize close marks so momentum is comparable across token price scales."""

    for row in rows:
        entry = value(row, "entry_close")
        if entry is None or entry <= 0:
            continue
        for minutes in (1, 2, 5):
            close = value(row, f"close_{minutes}m")
            if close is not None and close > 0:
                row[f"return_{minutes}m"] = close / entry - 1.0
        one_minute = value(row, "return_1m")
        five_minutes = value(row, "return_5m")
        if one_minute is not None and five_minutes is not None:
            row["return_acceleration_1m_to_5m"] = five_minutes - one_minute


def tier_labels(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, float | int]]:
    eligible: list[dict[str, Any]] = []
    for row in rows:
        entry, peak, observed = value(row, "entry_close"), value(row, "peak_close"), value(row, "observed_after_entry_ms")
        if entry is None or peak is None or observed is None or entry <= 0 or peak <= 0 or observed < WINDOW_MS:
            continue
        row["max_pnl"] = peak / entry - 1.0
        eligible.append(row)
    raw_pnls = [float(row["max_pnl"]) for row in eligible]
    raw_runner_cut, raw_dud_cut = percentile(raw_pnls, 0.90), percentile(raw_pnls, 0.50)
    for row in eligible:
        row["raw_runner"] = row["max_pnl"] >= raw_runner_cut
        row["raw_dud"] = row["max_pnl"] <= raw_dud_cut
    raw_runners = [row for row in eligible if row["raw_runner"]]
    degraded = [row for row in raw_runners if (value(row, "peak_min_sol_in_pool") or 0.0) < LIQUIDITY_FLOOR_SOL]
    liquid = [row for row in eligible if (value(row, "peak_min_sol_in_pool") or 0.0) >= LIQUIDITY_FLOOR_SOL]
    liquid_mints = {str(row["mint"]) for row in liquid}
    liquid_pnls = [float(row["max_pnl"]) for row in liquid]
    screened_runner_cut, screened_dud_cut = percentile(liquid_pnls, 0.90), percentile(liquid_pnls, 0.50)
    for row in eligible:
        liquid_mint = str(row["mint"]) in liquid_mints
        row["runner"] = liquid_mint and row["max_pnl"] >= screened_runner_cut
        row["dud"] = liquid_mint and row["max_pnl"] <= screened_dud_cut
    metrics: dict[str, float | int] = {
        "graduation_entries": len(rows), "feature_complete_entries": len(eligible),
        "raw_runner_cut": raw_runner_cut, "raw_dud_cut": raw_dud_cut,
        "raw_runners": len(raw_runners), "raw_duds": sum(row["raw_dud"] for row in eligible),
        "degraded_raw_runners": len(degraded),
        "degraded_raw_runner_pct": len(degraded) * 100 / len(raw_runners) if raw_runners else 0.0,
        "screened_runner_cut": screened_runner_cut, "screened_dud_cut": screened_dud_cut,
        "screened_runners": sum(row["runner"] for row in eligible),
        "screened_duds": sum(row["dud"] for row in eligible),
        "liquid_entries": len(liquid),
    }
    return eligible, metrics


def auc_rank_biserial(runner: list[float], dud: list[float]) -> tuple[float | None, float | None]:
    if not runner or not dud:
        return None, None
    values = sorted((item, 1) for item in runner) + sorted((item, 0) for item in dud)
    values.sort(key=lambda item: item[0])
    rank_sum, index = 0.0, 0
    while index < len(values):
        end = index + 1
        while end < len(values) and values[end][0] == values[index][0]:
            end += 1
        average_rank = (index + 1 + end) / 2
        rank_sum += average_rank * sum(label for _, label in values[index:end])
        index = end
    auc = (rank_sum - len(runner) * (len(runner) + 1) / 2) / (len(runner) * len(dud))
    return auc, 2 * auc - 1


def rank_features(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    runners = [row for row in rows if row["runner"]]
    duds = [row for row in rows if row["dud"]]
    for feature in FEATURES:
        runner = [item for row in runners if (item := value(row, feature)) is not None]
        dud = [item for row in duds if (item := value(row, feature)) is not None]
        auc, effect = auc_rank_biserial(runner, dud)
        ranked.append({
            "feature": feature, "runner_n": len(runner), "dud_n": len(dud),
            "runner_median": median(runner) if runner else None, "runner_mean": mean(runner) if runner else None,
            "dud_median": median(dud) if dud else None, "dud_mean": mean(dud) if dud else None,
            "auc": auc, "rank_biserial": effect, "absolute_effect": abs(effect) if effect is not None else None,
            "rank_eligible": len(runner) >= 100 and len(dud) >= 100,
        })
    return sorted(
        ranked,
        key=lambda row: (row["rank_eligible"], row["absolute_effect"] if row["absolute_effect"] is not None else -1),
        reverse=True,
    )


def half_rankings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bounds = [("days_1_15", START_DATE, START_DATE + timedelta(days=15)), ("days_16_30", START_DATE + timedelta(days=15), END_DATE)]
    rank_maps: dict[str, dict[str, dict[str, Any]]] = {}
    for label, start, end in bounds:
        half = [row.copy() for row in rows if start <= datetime.fromtimestamp(float(row["entry_time"]) / 1000, UTC).date() < end]
        labeled, _ = tier_labels(half)
        ranked = rank_features(labeled)
        rank_maps[label] = {row["feature"]: {"rank": index + 1, "effect": row["rank_biserial"], "absolute": row["absolute_effect"]} for index, row in enumerate(ranked)}
    return [
        {
            "feature": feature,
            "days_1_15_rank": rank_maps["days_1_15"][feature]["rank"],
            "days_1_15_absolute_effect": rank_maps["days_1_15"][feature]["absolute"],
            "days_1_15_rank_biserial": rank_maps["days_1_15"][feature]["effect"],
            "days_16_30_rank": rank_maps["days_16_30"][feature]["rank"],
            "days_16_30_absolute_effect": rank_maps["days_16_30"][feature]["absolute"],
            "days_16_30_rank_biserial": rank_maps["days_16_30"][feature]["effect"],
        }
        for feature in FEATURES
    ]


def suggested_thresholds(rows: list[dict[str, Any]], ranked: list[dict[str, Any]], halves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stable = [row["feature"] for row in halves if row["days_1_15_rank"] <= 5 and row["days_16_30_rank"] <= 5]
    features = stable or [row["feature"] for row in ranked[:3]]
    effects = {row["feature"]: row["rank_biserial"] for row in ranked}
    results: list[dict[str, Any]] = []
    for feature in features:
        runner = [item for row in rows if row["runner"] and (item := value(row, feature)) is not None]
        dud = [item for row in rows if row["dud"] and (item := value(row, feature)) is not None]
        if not runner or not dud:
            continue
        direction = effects[feature] or 0.0
        combined = runner + dud
        best: tuple[float, float, int, int] | None = None
        for index in range(5, 96):
            threshold = percentile(combined, index / 100)
            runner_pass = sum(item >= threshold for item in runner) if direction >= 0 else sum(item < threshold for item in runner)
            dud_pass = sum(item >= threshold for item in dud) if direction >= 0 else sum(item < threshold for item in dud)
            score = runner_pass / len(runner) - dud_pass / len(dud)
            if best is None or score > best[0]:
                best = score, threshold, runner_pass, dud_pass
        assert best is not None
        results.append({"feature": feature, "rule": ">=" if direction >= 0 else "<", "threshold": best[1], "youden_j": best[0], "runner_pass": best[2], "runner_total": len(runner), "dud_pass": best[3], "dud_total": len(dud)})
    return results


def plot_distributions(rows: list[dict[str, Any]], ranked: list[dict[str, Any]], output: Path) -> list[str]:
    features = [row["feature"] for row in ranked if len({value(item, row["feature"]) for item in rows if value(item, row["feature"]) is not None}) > 5][:6]
    if not features:
        return []
    cells = ['<svg xmlns="http://www.w3.org/2000/svg" width="1500" height="820" viewBox="0 0 1500 820">', '<rect width="100%" height="100%" fill="white"/>', '<text x="30" y="32" font-family="sans-serif" font-size="22">MT-737: entry-window distributions (1st-99th percentile clipped)</text>']
    for index, feature in enumerate(features):
        runner = [item for row in rows if row["runner"] and (item := value(row, feature)) is not None]
        dud = [item for row in rows if row["dud"] and (item := value(row, feature)) is not None]
        low, high = percentile(runner + dud, 0.01), percentile(runner + dud, 0.99)
        if low == high:
            high = low + 1.0
        counts: list[list[int]] = []
        for values in (dud, runner):
            bins = [0] * 32
            for item in values:
                clipped = min(max(item, low), high)
                bins[min(31, int((clipped - low) / (high - low) * 32))] += 1
            counts.append(bins)
        max_count = max(max(counts[0]), max(counts[1]), 1)
        cell_x, cell_y = (index % 3) * 500 + 15, (index // 3) * 385 + 50
        chart_x, chart_y, chart_w, chart_h = cell_x + 45, cell_y + 45, 420, 260
        cells.extend([f'<text x="{cell_x}" y="{cell_y + 20}" font-family="sans-serif" font-size="16">{escape(feature)}</text>', f'<rect x="{chart_x}" y="{chart_y}" width="{chart_w}" height="{chart_h}" fill="none" stroke="#555"/>', f'<text x="{chart_x}" y="{chart_y + chart_h + 24}" font-family="sans-serif" font-size="12">{low:.4g}</text>', f'<text x="{chart_x + chart_w - 45}" y="{chart_y + chart_h + 24}" font-family="sans-serif" font-size="12">{high:.4g}</text>', f'<rect x="{cell_x + 45}" y="{cell_y + 330}" width="12" height="12" fill="#e1812c" fill-opacity="0.6"/><text x="{cell_x + 62}" y="{cell_y + 341}" font-family="sans-serif" font-size="12">duds</text>', f'<rect x="{cell_x + 120}" y="{cell_y + 330}" width="12" height="12" fill="#3274a1" fill-opacity="0.6"/><text x="{cell_x + 137}" y="{cell_y + 341}" font-family="sans-serif" font-size="12">screened runners</text>'])
        bin_width = chart_w / 32
        for bin_index in range(32):
            x = chart_x + bin_index * bin_width
            for side, (count, color) in enumerate(((counts[0][bin_index], "#e1812c"), (counts[1][bin_index], "#3274a1"))):
                bar_height = chart_h * count / max_count
                cells.append(f'<rect x="{x + side * bin_width / 2:.2f}" y="{chart_y + chart_h - bar_height:.2f}" width="{bin_width / 2:.2f}" height="{bar_height:.2f}" fill="{color}" fill-opacity="0.6"/>')
    cells.append("</svg>")
    path = output / "top_feature_distributions.svg"
    path.write_text("\n".join(cells) + "\n")
    return [path.name]


def markdown_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No rows."
    headers = list(rows[0])
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        cells = []
        for header in headers:
            item = row[header]
            cells.append(f"{item:.4f}" if isinstance(item, float) else str(item))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_report(output: Path, metrics: dict[str, float | int], ranked: list[dict[str, Any]], halves: list[dict[str, Any]], thresholds: list[dict[str, Any]], plots: list[str]) -> None:
    stable = [row["feature"] for row in halves if row["days_1_15_rank"] <= 5 and row["days_16_30_rank"] <= 5]
    report = [
        "# MT-737 Entry Feature Study", "", "## Scope and Entry Definition", "",
        f"Training files only: {START_DATE} through {END_DATE} exclusive (30 files). No May 18 or later data was read.",
        "The replay gate evaluates every bar from its age floor to its age ceiling; it does not require graduation. This study uses each mint's first `graduated_this_bar = true` bar as a reproducible discrete entry proxy.",
        f"Graduation entries: **{metrics['graduation_entries']:,}**. Feature-complete entries with a full five-minute early window: **{metrics['feature_complete_entries']:,}**.",
        "Features use only the entry bar through the next 300 seconds. Labels use each token's maximum close after entry within the same training files.", "", "## Tiers and Liquidity Screen", "",
        f"Raw top-decile runner cut: **{metrics['raw_runner_cut']:.4f}** max PnL; raw bottom-half dud cut: **{metrics['raw_dud_cut']:.4f}**.",
        f"Without screen: **{metrics['raw_runners']:,}** runners and **{metrics['raw_duds']:,}** duds.",
        f"**Important:** peak-bar `min_sol_in_pool >= {LIQUIDITY_FLOOR_SOL:g}` SOL is the liquidity screen. It removes **{metrics['degraded_raw_runners']:,}** raw runners (**{metrics['degraded_raw_runner_pct']:.1f}%** of raw runners) that peaked on degraded depth.",
        f"With screen: **{metrics['liquid_entries']:,}** eligible mints, **{metrics['screened_runners']:,}** runners at max-PnL **{metrics['screened_runner_cut']:.4f}**, and **{metrics['screened_duds']:,}** duds at max-PnL **{metrics['screened_dud_cut']:.4f}**.", "", "## Ranked Features", "", "Ranked by absolute rank-biserial effect; positive signs mean the feature is higher among runners. `rank_eligible` requires at least 100 observations in both tiers, so sparse fields remain reported but do not drive the ranking.", "", markdown_table(ranked), "", "## Half-Month Stability", "", markdown_table(halves), "", "Stable top-five features in both halves: " + (", ".join(stable) if stable else "none"), "", "## Descriptive Thresholds", "", "These maximize in-sample runner-minus-dud retention and are not validated filters.", "", markdown_table(thresholds), "", "## Conclusion", "", ("Yes: " + ", ".join(stable) + " have material, stable separation and are candidates for one fixed, blind-period filter test. Do not deploy or tune them further from this training result." if stable else "No stable top-five feature survived both halves; no filter is justified from this training result."), "", "## Files", "", "- `entry_features.csv`: one feature/outcome row per qualifying graduate", "- `feature_ranking.csv`: full-sample runner vs dud effects", "- `half_month_rankings.csv`: independent first/second-half rankings", "- `suggested_thresholds.csv`: descriptive in-sample thresholds", *[f"- `{plot}`" for plot in plots],
    ]
    (output / "MT737_REPORT.md").write_text("\n".join(report) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--entry-start", type=date.fromisoformat)
    parser.add_argument("--entry-end", type=date.fromisoformat)
    args = parser.parse_args()
    if args.worker:
        if not args.entry_start or not args.entry_end:
            parser.error("--worker requires --entry-start and --entry-end")
        worker(args.entry_start, args.entry_end, args.output)
        return
    args.output.mkdir(parents=True, exist_ok=True)
    raw = run_workers(args.output)
    derive_features(raw)
    rows, metrics = tier_labels(raw)
    ranked = rank_features(rows)
    halves = half_rankings(rows)
    thresholds = suggested_thresholds(rows, ranked, halves)
    write_rows(args.output / "entry_features.csv", rows)
    write_rows(args.output / "feature_ranking.csv", ranked)
    write_rows(args.output / "half_month_rankings.csv", halves)
    write_rows(args.output / "suggested_thresholds.csv", thresholds)
    plots = plot_distributions(rows, ranked, args.output)
    write_report(args.output, metrics, ranked, halves, thresholds, plots)


if __name__ == "__main__":
    main()

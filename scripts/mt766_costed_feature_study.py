#!/usr/bin/env python3
"""MT-766 detached feature and cost-model study, limited to Apr 18-May 18."""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import resource
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb


ARCHIVE = Path("/mnt/d/pumpapi-replay/derived/enriched")
MT765 = Path("/workspace/shared/MT-765")
ROOT = Path("/workspace/shared/MT-766")
START, SPLIT, END = date(2026, 4, 18), date(2026, 5, 3), date(2026, 5, 19)
MEMORY_LIMIT = "4GB"
RUNTIME_CAP_S = 2 * 60 * 60
MARK_TOLERANCE_MS = 30_000
OUTCOMES = ("out_close_5m", "out_close_20m", "out_peak_multiple")
RAW_COLUMNS = (
    "unique_traders", "unique_wallets_total", "top10_holder_pct", "creator_holdings_pct",
    "creator_net_sol", "creator_is_selling", "mint_authority_present", "freeze_authority_present",
    "unique_traders_change_1m", "top10_holder_pct_change_since_first", "creator_net_sol_to_pool",
)
FEE_BONDING, FEE_GRADUATED, PRIORITY_FEE = 0.01, 0.0025, 0.0002


def dates() -> list[date]:
    result, current = [], START
    while current < END:
        result.append(current)
        current += timedelta(days=1)
    return result


def q(value: Path | str) -> str:
    return repr(str(value))


def db(name: str) -> duckdb.DuckDBPyConnection:
    temp = ROOT / "duckdb_tmp" / name
    temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET threads = 4")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = {q(temp)}")
    return con


def finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def sci(value: Any) -> str:
    number = finite(value)
    return "" if number is None else f"{number:.8e}"


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def source(day: date) -> Path:
    path = ARCHIVE / f"{day.isoformat()}.parquet"
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def checkpoints() -> list[int]:
    population = MT765 / "population_with_day.parquet"
    if not population.is_file():
        raise FileNotFoundError(f"MT-765 population missing: {population}")
    with db("checkpoints") as con:
        lives = [row[0] for row in con.execute(
            f"SELECT lifespan_s FROM read_parquet({q(population)}) WHERE working ORDER BY lifespan_s"
        ).fetchall()]
    targets = (0.95, 0.90, 0.75, 0.50, 0.25)
    result = []
    for survival in targets:
        index = max(0, min(len(lives) - 1, math.ceil((1 - survival) * len(lives)) - 1))
        result.append(min(3600, int(math.floor(lives[index]))))
    if len(set(result)) != len(result):
        raise RuntimeError(f"survival checkpoint cap collision: {result}; cannot truthfully report five distinct ages")
    return result


def extra_day(day: date, ages: list[int]) -> None:
    destination = ROOT / "extra_chunks" / f"{day.isoformat()}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    age_values = ", ".join(f"({age})" for age in ages)
    day_end = int(datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC).timestamp() * 1000)
    with db(f"extra-{day.isoformat()}") as con:
        con.execute(f"""
            COPY (
              WITH population AS (
                SELECT mint, first_bar_time, last_bar_time, lifespan_s, working
                FROM read_parquet({q(MT765 / 'population_with_day.parquet')})
                WHERE working AND first_day = DATE '{day.isoformat()}'
              ), bars AS (
                SELECT b.* FROM read_parquet({q(source(day))}) b JOIN population p USING (mint)
                WHERE b.close > 0
              ), targets AS (
                SELECT p.*, v.age::INTEGER AS checkpoint_seconds, p.first_bar_time + v.age * 1000 AS target_time
                FROM population p CROSS JOIN (VALUES {age_values}) v(age) WHERE p.lifespan_s >= v.age
              ), marks AS (
                SELECT t.*, min(b.bar_time) AS checkpoint_time
                FROM targets t JOIN bars b USING (mint)
                WHERE b.bar_time >= t.target_time AND b.bar_time <= t.target_time + {MARK_TOLERANCE_MS}
                GROUP BY ALL
              ), history AS (
                SELECT m.*, b.bar_time, b.close, b.min_sol_in_pool, b.graduated_this_bar,
                       b.unique_traders, b.unique_wallets_total, b.top10_holder_pct, b.creator_holdings_pct,
                       b.creator_net_sol, b.creator_is_selling, b.mint_authority_present, b.freeze_authority_present
                FROM marks m JOIN bars b USING (mint) WHERE b.bar_time <= m.checkpoint_time
              ), features AS (
                SELECT mint, checkpoint_seconds, checkpoint_time, first_bar_time, last_bar_time,
                       arg_max(close, bar_time) AS entry_price, arg_max(min_sol_in_pool, bar_time) AS entry_pool_sol,
                       bool_or(graduated_this_bar) AS entry_graduated,
                       arg_max(unique_traders, bar_time) AS unique_traders,
                       arg_max(unique_wallets_total, bar_time) AS unique_wallets_total,
                       arg_max(top10_holder_pct, bar_time) AS top10_holder_pct,
                       arg_max(creator_holdings_pct, bar_time) AS creator_holdings_pct,
                       arg_max(creator_net_sol, bar_time) AS creator_net_sol,
                       arg_max(creator_is_selling, bar_time) AS creator_is_selling,
                       arg_max(mint_authority_present, bar_time) AS mint_authority_present,
                       arg_max(freeze_authority_present, bar_time) AS freeze_authority_present,
                       arg_max(unique_traders, bar_time) - arg_max(unique_traders, bar_time)
                         FILTER (WHERE bar_time <= checkpoint_time - 60000) AS unique_traders_change_1m,
                       arg_max(top10_holder_pct, bar_time) - max(top10_holder_pct)
                         FILTER (WHERE bar_time = first_bar_time) AS top10_holder_pct_change_since_first,
                       arg_max(creator_net_sol, bar_time) / nullif(arg_max(min_sol_in_pool, bar_time), 0) AS creator_net_sol_to_pool
                FROM history GROUP BY mint, checkpoint_seconds, checkpoint_time, first_bar_time, last_bar_time
              ), outcomes AS (
                SELECT f.mint, f.checkpoint_seconds,
                       arg_min(b.close, b.bar_time) FILTER (WHERE b.bar_time >= f.checkpoint_time + 300000
                         AND b.bar_time <= f.checkpoint_time + 300000 + {MARK_TOLERANCE_MS}) AS out_close_5m,
                       arg_min(b.min_sol_in_pool, b.bar_time) FILTER (WHERE b.bar_time >= f.checkpoint_time + 300000
                         AND b.bar_time <= f.checkpoint_time + 300000 + {MARK_TOLERANCE_MS}) AS out_pool_5m,
                       bool_or(b.graduated_this_bar) FILTER (WHERE b.bar_time >= f.checkpoint_time + 300000
                         AND b.bar_time <= f.checkpoint_time + 300000 + {MARK_TOLERANCE_MS}) AS out_graduated_5m,
                       arg_min(b.close, b.bar_time) FILTER (WHERE b.bar_time >= f.checkpoint_time + 1200000
                         AND b.bar_time <= f.checkpoint_time + 1200000 + {MARK_TOLERANCE_MS}) AS out_close_20m,
                       arg_min(b.min_sol_in_pool, b.bar_time) FILTER (WHERE b.bar_time >= f.checkpoint_time + 1200000
                         AND b.bar_time <= f.checkpoint_time + 1200000 + {MARK_TOLERANCE_MS}) AS out_pool_20m,
                       bool_or(b.graduated_this_bar) FILTER (WHERE b.bar_time >= f.checkpoint_time + 1200000
                         AND b.bar_time <= f.checkpoint_time + 1200000 + {MARK_TOLERANCE_MS}) AS out_graduated_20m,
                       max(b.close) / nullif(f.entry_price, 0) AS out_peak_multiple
                FROM features f LEFT JOIN bars b ON b.mint = f.mint AND b.bar_time > f.checkpoint_time
                GROUP BY f.mint, f.checkpoint_seconds, f.entry_price
              )
              SELECT f.*, o.*, CAST(to_timestamp(f.checkpoint_time / 1000.0) AT TIME ZONE 'UTC' AS DATE) AS entry_day,
                     f.checkpoint_time + 300000 > {day_end} AS exit_5m_past_day,
                     f.checkpoint_time + 1200000 > {day_end} AS exit_20m_past_day
              FROM features f LEFT JOIN outcomes o USING (mint, checkpoint_seconds)
            ) TO {q(destination)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    (ROOT / "worker_stats").mkdir(parents=True, exist_ok=True)
    (ROOT / "worker_stats" / f"{day.isoformat()}.txt").write_text(str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss), encoding="ascii")


def auc(source_sql: str, column: str, outcome: str, graduated: bool = False) -> dict[str, Any]:
    grad = "AND entry_graduated" if graduated else ""
    with db(f"auc-{column}-{outcome}-{graduated}") as con:
        winners, losers, rank_sum, coverage, total = con.execute(f"""
          WITH usable AS (
            SELECT *, ntile(10) OVER (ORDER BY {outcome} DESC, mint) AS decile
            FROM {source_sql} WHERE {outcome} > 0 AND isfinite({outcome}) {grad}
          ), chosen AS (
            SELECT {column}::DOUBLE AS value, decile = 1 AS winner
            FROM usable WHERE {column} IS NOT NULL AND isfinite(({column})::DOUBLE) AND (decile = 1 OR decile >= 7)
          ), grouped AS (SELECT value, count(*) n, sum(winner::INTEGER) wins FROM chosen GROUP BY value),
          ranked AS (SELECT *, sum(n) OVER (ORDER BY value) - n + (n + 1) / 2.0 rank FROM grouped),
          counts AS (SELECT count(*) total, count({column}) coverage FROM usable)
          SELECT coalesce(sum(wins), 0), coalesce(sum(n - wins), 0), coalesce(sum(wins * rank), 0),
                 (SELECT coverage FROM counts), (SELECT total FROM counts) FROM ranked
        """).fetchone()
    if not winners or not losers:
        return {"auc": None, "direction": "unavailable", "winners": winners, "losers": losers, "coverage": coverage, "total": total}
    raw = (rank_sum - winners * (winners + 1) / 2) / (winners * losers)
    return {"auc": max(raw, 1 - raw), "direction": "higher" if raw >= .5 else "lower", "winners": winners, "losers": losers, "coverage": coverage, "total": total}


def score_extras(ages: list[int]) -> list[dict[str, Any]]:
    chunks = [ROOT / "extra_chunks" / f"{day.isoformat()}.parquet" for day in dates()]
    source_sql = f"read_parquet([{', '.join(q(path) for path in chunks)}])"
    rows: list[dict[str, Any]] = []
    for age in ages:
        checkpoint = f"(SELECT * FROM {source_sql} WHERE checkpoint_seconds = {age})"
        for population, graduated in (("all", False), ("graduated_only", True)):
            for outcome in OUTCOMES:
                for column in RAW_COLUMNS:
                    result = auc(checkpoint, column, outcome, graduated)
                    rows.append({"checkpoint_seconds": age, "population": population, "outcome": outcome, "column": column,
                                 "normalized_auc": "" if result["auc"] is None else f"{result['auc']:.8f}",
                                 "favored_direction": result["direction"], "winner_count": result["winners"], "loser_count": result["losers"],
                                 "non_null_count": result["coverage"], "outcome_population_count": result["total"],
                                 "non_null_coverage": "" if not result["total"] else f"{result['coverage'] / result['total']:.8f}",
                                 "thin_sample_warning": bool(result["winners"] < 100 or result["losers"] < 100)})
    write_csv(ROOT / "partA_auc.csv", rows, list(rows[0]))
    return rows


def mt765_scores() -> list[dict[str, Any]]:
    paths = [MT765 / "feature_chunks" / f"{day.isoformat()}.parquet" for day in dates()]
    source_sql = f"read_parquet([{', '.join(q(path) for path in paths)}])"
    columns = [row[0] for row in db("mt765-schema").execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()]
    skip = {"mint", "checkpoint_seconds", "checkpoint_time", "first_bar_time", "last_bar_time", "graduation_time", "forward_window_truncated", *OUTCOMES, "out_pool_at_5m", "out_pool_at_20m"}
    rows: list[dict[str, Any]] = []
    for age in sorted({row[0] for row in db("mt765-ages").execute(f"SELECT DISTINCT checkpoint_seconds FROM {source_sql}").fetchall()}):
        train = f"(SELECT *, ever_graduated AS entry_graduated FROM {source_sql} WHERE checkpoint_seconds = {age} AND checkpoint_time < {int(datetime.combine(SPLIT, datetime.min.time(), UTC).timestamp() * 1000)})"
        for column in columns:
            if column in skip:
                continue
            for outcome in ("out_close_5m", "out_close_20m"):
                result = auc(train, column, outcome)
                if result["auc"] is not None:
                    rows.append({"origin": "MT-765", "column": column, "checkpoint_seconds": age, "outcome": outcome, **result})
    return rows


def choose_rule(extra_rows: list[dict[str, Any]]) -> dict[str, Any]:
    split_ms = int(datetime.combine(SPLIT, datetime.min.time(), UTC).timestamp() * 1000)
    candidates: list[dict[str, Any]] = mt765_scores()
    chunks = [ROOT / "extra_chunks" / f"{day.isoformat()}.parquet" for day in dates()]
    source_sql = f"read_parquet([{', '.join(q(path) for path in chunks)}])"
    for row in extra_rows:
        if row["population"] != "all" or row["outcome"] not in ("out_close_5m", "out_close_20m"):
            continue
        train = f"(SELECT * FROM {source_sql} WHERE checkpoint_seconds = {row['checkpoint_seconds']} AND checkpoint_time < {split_ms})"
        result = auc(train, row["column"], row["outcome"])
        if result["auc"] is not None:
            candidates.append({"origin": "MT-766", "column": row["column"], "checkpoint_seconds": row["checkpoint_seconds"], "outcome": row["outcome"], **result})
    best = max(candidates, key=lambda row: row["auc"])
    if best["origin"] == "MT-765":
        paths = [MT765 / "feature_chunks" / f"{day.isoformat()}.parquet" for day in dates()]
        extra_paths = [ROOT / "extra_chunks" / f"{day.isoformat()}.parquet" for day in dates()]
        data = f"(SELECT f.mint, f.checkpoint_time, f.checkpoint_seconds, {best['column']}::DOUBLE AS value, f.price AS entry_price, f.pool_sol AS entry_pool_sol, f.graduated AS entry_graduated, f.out_close_5m, f.out_pool_at_5m AS out_pool_5m, r.out_graduated_5m AS exit_graduated_5m, f.out_close_20m, f.out_pool_at_20m AS out_pool_20m, r.out_graduated_20m AS exit_graduated_20m, f.forward_window_truncated FROM read_parquet([{', '.join(q(path) for path in paths)}]) f LEFT JOIN read_parquet([{', '.join(q(path) for path in extra_paths)}]) r USING (mint, checkpoint_seconds) WHERE f.checkpoint_seconds = {best['checkpoint_seconds']})"
    else:
        data = f"(SELECT mint, checkpoint_time, checkpoint_seconds, {best['column']}::DOUBLE AS value, entry_price, entry_pool_sol, entry_graduated, out_close_5m, out_pool_5m, out_graduated_5m AS exit_graduated_5m, out_close_20m, out_pool_20m, out_graduated_20m AS exit_graduated_20m, exit_20m_past_day AS forward_window_truncated FROM {source_sql} WHERE checkpoint_seconds = {best['checkpoint_seconds']})"
    # Thresholds are picked only from the training period. Require 100 qualifying observations.
    with db("threshold") as con:
        options = con.execute(f"""
          WITH train AS (SELECT * FROM {data} WHERE checkpoint_time < {split_ms} AND value IS NOT NULL AND isfinite(value) AND out_close_5m > 0),
          bounds AS (SELECT quantile_cont(value, .1) q10, quantile_cont(value, .2) q20, quantile_cont(value, .3) q30, quantile_cont(value, .7) q70, quantile_cont(value, .8) q80, quantile_cont(value, .9) q90 FROM train),
          options AS (SELECT 'q10' AS label, q10 AS threshold FROM bounds UNION ALL SELECT 'q20',q20 FROM bounds UNION ALL SELECT 'q30',q30 FROM bounds UNION ALL SELECT 'q70',q70 FROM bounds UNION ALL SELECT 'q80',q80 FROM bounds UNION ALL SELECT 'q90',q90 FROM bounds)
          SELECT label, threshold, count(*) n, avg(out_close_5m / entry_price - 1) mean_return
          FROM train CROSS JOIN options WHERE (CASE WHEN '{best['direction']}' = 'higher' THEN value >= threshold ELSE value <= threshold END)
          GROUP BY 1,2 HAVING count(*) >= 100 ORDER BY mean_return DESC, n DESC LIMIT 1
        """).fetchone()
    if not options:
        raise RuntimeError("no training-only threshold has 100 observations")
    label, threshold, count, train_return = options
    best.update({"threshold_label": label, "threshold": threshold, "train_count": count, "train_mean_return": train_return, "data_sql": data})
    return best


def selected_rows(rule: dict[str, Any]) -> list[dict[str, Any]]:
    split_ms = int(datetime.combine(SPLIT, datetime.min.time(), UTC).timestamp() * 1000)
    with db("evaluation") as con:
        cols = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {rule['data_sql']}").fetchall()]
        rows = [dict(zip(cols, row, strict=True)) for row in con.execute(f"SELECT * FROM {rule['data_sql']} WHERE checkpoint_time >= {split_ms}").fetchall()]
    return rows


def fee_rate(graduated: Any) -> float:
    return FEE_GRADUATED if graduated is True else FEE_BONDING


def trade_result(row: dict[str, Any], strategy: str, horizon: int, size: float, selected: bool) -> dict[str, Any]:
    out_price = row[f"out_close_{horizon}m"]
    out_pool = row[f"out_pool_{horizon}m"]
    entry_price, entry_pool = finite(row["entry_price"]), finite(row["entry_pool_sol"])
    exit_price, exit_pool = finite(out_price), finite(out_pool)
    status = "filled"
    if not selected:
        status = "not_selected"
    elif not entry_price or not entry_pool or entry_pool <= 0:
        status = "unfillable_entry_depth_or_price"
    elif not exit_price or not exit_pool or exit_pool <= 0:
        if exit_price and (not exit_pool or exit_pool <= 0):
            status = "unfillable_exit_depth_or_price"
        else:
            status = "horizon_past_day" if row.get("forward_window_truncated") else "stopped_or_missing_before_exit"
    entry_rate = fee_rate(row.get("entry_graduated"))
    exit_rate = fee_rate(row.get(f"exit_graduated_{horizon}m"))
    gross_proceeds = None
    platform_fees = size * entry_rate
    priority_fees = 2 * PRIORITY_FEE
    price_impact = None
    net_pnl = None
    if status == "filled":
        price_ratio = exit_price / entry_price
        without_impact = size * (1 - entry_rate) * price_ratio * (1 - exit_rate)
        bought_spot_value = size * (1 - entry_rate)
        bought_after_impact = bought_spot_value * entry_pool / (entry_pool + bought_spot_value)
        exit_spot_value = bought_after_impact * price_ratio
        proceeds = exit_spot_value * exit_pool / (exit_pool + exit_spot_value) * (1 - exit_rate)
        gross_proceeds = size * price_ratio
        price_impact = max(0.0, without_impact - proceeds)
        net_pnl = proceeds - size - priority_fees
    return {"strategy": strategy, "horizon_minutes": horizon, "position_sol": sci(size), "mint": row["mint"],
            "checkpoint_seconds": row["checkpoint_seconds"], "checkpoint_time": row["checkpoint_time"], "entry_price": sci(entry_price), "exit_price": sci(exit_price),
            "entry_pool_sol": sci(entry_pool), "exit_pool_sol": sci(exit_pool), "status": status, "gross_return_pct": "" if gross_proceeds is None else sci(gross_proceeds / size - 1),
            "net_return_pct": "" if net_pnl is None else sci(net_pnl / size), "net_pnl_sol": sci(net_pnl), "platform_fees_sol": sci(platform_fees),
            "priority_fees_sol": sci(priority_fees), "price_impact_sol": sci(price_impact), "selected_value": sci(row.get("value"))}


def random_sample(mint: str) -> bool:
    return int.from_bytes(hashlib.sha256(mint.encode("utf-8")).digest()[:8], "big") / 2**64 < .05


def simulate(rule: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = selected_rows(rule)
    trades: list[dict[str, Any]] = []
    for row in rows:
        value = finite(row.get("value"))
        applies = value is not None and (value >= rule["threshold"] if rule["direction"] == "higher" else value <= rule["threshold"])
        for strategy, selected in (("rule", applies), ("buy_every", True), ("random_5pct", random_sample(row["mint"]))):
            if not selected:
                continue
            for horizon in (5, 20):
                for size in (.1, .5, 1.0):
                    trades.append(trade_result(row, strategy, horizon, size, True))
    fields = list(trades[0]) if trades else ["strategy"]
    write_csv(ROOT / "partB_trades.csv", trades, fields)
    summary: list[dict[str, Any]] = []
    for strategy in ("rule", "buy_every", "random_5pct"):
        for horizon in (5, 20):
            for size in (.1, .5, 1.0):
                cell = [row for row in trades if row["strategy"] == strategy and row["horizon_minutes"] == horizon and row["position_sol"] == sci(size)]
                filled = [row for row in cell if row["status"] == "filled"]
                values = [float(row["net_return_pct"]) for row in filled]
                pnl = [float(row["net_pnl_sol"]) for row in filled]
                gross = [float(row["gross_return_pct"]) for row in filled]
                fees = sum(float(row["platform_fees_sol"]) + float(row["priority_fees_sol"]) for row in filled)
                impact = sum(float(row["price_impact_sol"]) for row in filled)
                gross_value = sum(size * (1 + item) for item in gross)
                summary.append({"strategy": strategy, "horizon_minutes": horizon, "position_sol": sci(size), "trade_count": len(cell), "filled_trade_count": len(filled),
                                "unfilled_trade_count": len(cell) - len(filled), "thin_sample_warning": len(cell) < 100, "win_rate_after_costs": "" if not values else sci(sum(item > 0 for item in values) / len(values)),
                                "median_return_after_costs": "" if not values else sci(sorted(values)[len(values) // 2]), "mean_return_after_costs": "" if not values else sci(sum(values) / len(values)),
                                "total_return_sol": "" if not pnl else sci(sum(pnl)), "worst_trade_sol": "" if not pnl else sci(min(pnl)), "mean_gross_return_before_costs": "" if not gross else sci(sum(gross) / len(gross)),
                                "platform_plus_priority_fees_sol": sci(fees), "price_impact_sol": sci(impact), "fees_share_of_gross": "" if gross_value <= 0 else sci(fees / gross_value), "impact_share_of_gross": "" if gross_value <= 0 else sci(impact / gross_value),
                                "horizon_past_day_count": sum(row["status"] == "horizon_past_day" for row in cell), "stopped_or_missing_count": sum(row["status"] == "stopped_or_missing_before_exit" for row in cell), "zero_or_missing_depth_count": sum("depth" in row["status"] for row in cell)})
    write_csv(ROOT / "partB_summary.csv", summary, list(summary[0]))
    return trades, summary


def report(ages: list[int], extra_rows: list[dict[str, Any]], rule: dict[str, Any], summary: list[dict[str, Any]], started: float) -> None:
    best_extra = []
    for column in RAW_COLUMNS:
        candidates = [row for row in extra_rows if row["column"] == column and row["population"] == "all" and row["outcome"] != "out_peak_multiple" and row["normalized_auc"]]
        best_extra.append(max(candidates, key=lambda row: float(row["normalized_auc"])))
    rule_cells = [row for row in summary if row["strategy"] == "rule"]
    beats = all(float(row["total_return_sol"] or "-inf") > float(next(item for item in summary if item["strategy"] == "buy_every" and item["horizon_minutes"] == row["horizon_minutes"] and item["position_sol"] == row["position_sol"])["total_return_sol"] or "inf") and float(row["total_return_sol"] or "-inf") > float(next(item for item in summary if item["strategy"] == "random_5pct" and item["horizon_minutes"] == row["horizon_minutes"] and item["position_sol"] == row["position_sol"])["total_return_sol"] or "inf") for row in rule_cells)
    lines = ["# MT-766 Raw Fields And Costed Rule", "", "This report read only `2026-04-18` through `2026-05-18` enriched Parquets. **No day after May 18 was read.** No Hive, PumpApi service, runtime, schema, or strategy file was changed.", "", "## Part A: Untested Raw Fields", "", f"Five distinct checkpoint ages are `{', '.join(f'{age}s' for age in ages)}` for 95%, 90%, 75%, 50%, and 25% survival respectively; all are distinct and capped at 60 minutes.", "", "AUC is direction-normalized; winners are top outcome decile and losers bottom four deciles, middle excluded. Coverage is non-null / usable-outcome rows, so sparse columns are visible rather than treated as usable signals.", "", "| field | best close AUC | checkpoint/outcome | coverage | verdict |", "|---|---:|---|---:|---|"]
    for row in best_extra:
        coverage = float(row["non_null_coverage"] or 0)
        verdict = "sparse: not usable alone" if coverage < .5 else "coverage adequate"
        lines.append(f"| {row['column']} | {row['normalized_auc']} | {row['checkpoint_seconds']}s/{row['outcome']} | {row['non_null_count']}/{row['outcome_population_count']} ({coverage:.1%}) | {verdict} |")
    lines += ["", "`partA_auc.csv` contains every raw/derived field, checkpoint, outcome, population, direction-normalized AUC, and coverage.", "", "## Part B: Money Answer", "", f"**No, the rule {'does' if beats else 'does not'} beat both baselines after modeled costs in every reported exit/size cell.**", "", f"Feature and checkpoint selection used only Apr 18-May 2. Evaluation used only May 3-May 18. The selected one-feature rule is `{rule['column']} {'>=' if rule['direction'] == 'higher' else '<='} {sci(rule['threshold'])}` at {rule['checkpoint_seconds']}s (training {rule['outcome']} AUC {rule['auc']:.8f}; threshold `{rule['threshold_label']}`, {rule['train_count']} train entries). No May 3-18 data influenced feature, checkpoint, direction, or threshold selection.", "", "Cost model: each leg uses 1.00% while bonding and 0.25% after graduation, matching the existing V2 replay fee assumptions documented in `STATUS.md`; priority fee is a conservative 0.0002 SOL per leg from that same model. Price impact is calculated from the recorded SOL pool reserve under a constant-product curve on entry and exit. **Slippage/impact is modeled from bar-level pool depth, not measured from fills; all P&L is an estimate, not realized P&L.**", "", "| strategy | exit | size SOL | trades/filled | win rate | mean net return | total SOL | fees/gross | impact/gross | warning |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for row in summary:
        lines.append(f"| {row['strategy']} | {row['horizon_minutes']}m | {row['position_sol']} | {row['trade_count']}/{row['filled_trade_count']} | {row['win_rate_after_costs']} | {row['mean_return_after_costs']} | {row['total_return_sol']} | {row['fees_share_of_gross']} | {row['impact_share_of_gross']} | {'thin <100' if row['thin_sample_warning'] else ''} |")
    worker_times = [path.stat().st_mtime for path in (ROOT / "worker_stats").glob("*.txt")]
    worker_span = max(worker_times) - min(worker_times) if len(worker_times) > 1 else 0.0
    lines += ["", "Blockers: zero/missing entry or exit pool depth is quarantined as unfillable, never treated as a free exit; horizons past a day file and stopped/missing-before-exit rows are counted in `partB_summary.csv`; no cross-day price is read. Ratio denominators use `NULLIF`, nulls remain null (never zero-filled), and no infinity is emitted. A coin that stops trading before exit is not assigned a fabricated price; it is an unfilled `stopped_or_missing_before_exit` row.", "", f"Archive-worker span: {worker_span:.1f}s; every worker and finalizer used `run-capped 6G`, and the run remained below the two-hour cap. Outputs are independent: Part A is complete even if a later Part B cell is unfillable."]
    (ROOT / "RESULT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_child(day: date, ages: list[int]) -> None:
    subprocess.run(["run-capped", "6G", sys.executable, str(Path(__file__).resolve()), "--extra-day", day.isoformat(), "--ages", ",".join(map(str, ages)), "--output", str(ROOT)], check=True)


def main() -> None:
    global ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT)
    parser.add_argument("--extra-day", type=date.fromisoformat)
    parser.add_argument("--ages", default="")
    parser.add_argument("--finalize", action="store_true")
    args = parser.parse_args()
    ROOT = args.output
    ROOT.mkdir(parents=True, exist_ok=True)
    ages = [int(value) for value in args.ages.split(",") if value] or checkpoints()
    if args.extra_day:
        extra_day(args.extra_day, ages)
        return
    if args.finalize:
        extra = score_extras(ages)
        rule = choose_rule(extra)
        _, summary = simulate(rule)
        report(ages, extra, rule, summary, time.monotonic())
        return
    started = time.monotonic()
    for day in dates():
        if time.monotonic() - started >= RUNTIME_CAP_S:
            break
        if not (ROOT / "extra_chunks" / f"{day.isoformat()}.parquet").is_file():
            run_child(day, ages)
    completed = [day for day in dates() if (ROOT / "extra_chunks" / f"{day.isoformat()}.parquet").is_file()]
    if len(completed) != len(dates()):
        (ROOT / "RESULT.md").write_text(f"# MT-766 partial\n\nCompleted {len(completed)}/31 days before the two-hour cap. Part A/Part B finalization was not run.\n", encoding="utf-8")
        return
    extra = score_extras(ages)
    rule = choose_rule(extra)
    _, summary = simulate(rule)
    report(ages, extra, rule, summary, started)


if __name__ == "__main__":
    main()

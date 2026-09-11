"""Enrich finalized PumpApi OHLCV days with USD and wallet-concentration data."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sqlite3
import sys
import time
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq


ARCHIVE_ROOT = Path(r"D:\pumpapi-replay")
PRICE_FROM_UNIX = 1_713_398_400  # 2024-04-18 UTC; retained as the replay baseline.
POLL_SECONDS = 60
ROW_BATCH_SIZE = 100_000
ENRICHMENT_FIELDS = pa.schema(
    [
        ("market_cap_usd", pa.float64()),
        ("volume_usd", pa.float64()),
        ("creator_holdings_pct", pa.float64()),
        ("top10_holder_pct", pa.float64()),
        ("creator_net_sol", pa.float64()),
        ("creator_is_selling", pa.bool_()),
        ("unique_wallets_total", pa.int32()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ARCHIVE_ROOT)
    parser.add_argument("--once", action="store_true", help="Process currently complete days, then exit.")
    parser.add_argument("--date", help="Process one YYYY-MM-DD day only (for validation).")
    parser.add_argument(
        "--rebuild-state-through",
        help="Rebuild cumulative token state through YYYY-MM-DD without rewriting Parquet output.",
    )
    return parser.parse_args()


def configure_logging(derived_dir: Path) -> logging.Logger:
    log_dir = derived_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pumpapi_enrich")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler = RotatingFileHandler(log_dir / "enrich.log", maxBytes=50_000_000, backupCount=5, encoding="utf-8")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    return logger


def init_state(derived_dir: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(derived_dir / "enrichment_state.db")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS completed_days (date TEXT PRIMARY KEY, completed_at TEXT NOT NULL)"
    )
    connection.execute(
        """CREATE TABLE IF NOT EXISTS token_state (
            mint TEXT PRIMARY KEY,
            creator_wallet TEXT,
            supply REAL,
            balances_json TEXT NOT NULL,
            creator_net_sol REAL NOT NULL DEFAULT 0
        )"""
    )
    connection.commit()
    return connection


def utc_date(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).date().isoformat()


def load_local_prices(derived_dir: Path) -> dict[str, float]:
    parquet_path = derived_dir / "sol_prices.parquet"
    if parquet_path.exists():
        table = pq.read_table(parquet_path, columns=["date", "sol_usd"])
        return {
            str(date): float(price)
            for date, price in zip(table.column("date").to_pylist(), table.column("sol_usd").to_pylist(), strict=True)
            if price is not None and math.isfinite(float(price)) and float(price) > 0
        }

    csv_path = derived_dir / "sol_prices.csv"
    if not csv_path.exists():
        return {}
    prices: dict[str, float] = {}
    with csv_path.open(newline="", encoding="utf-8-sig") as source:
        for row in csv.DictReader(source):
            date = row.get("date") or row.get("Date")
            value = row.get("sol_usd") or row.get("Price") or row.get("price")
            if not date or not value:
                continue
            try:
                price = float(str(value).replace("$", "").replace(",", ""))
            except ValueError:
                continue
            if math.isfinite(price) and price > 0:
                prices[date[:10]] = price
    return prices


def refresh_sol_prices(derived_dir: Path, logger: logging.Logger) -> dict[str, float]:
    now = int(time.time())
    url = "https://api.coingecko.com/api/v3/coins/solana/market_chart/range"
    try:
        response = httpx.get(url, params={"vs_currency": "usd", "from": PRICE_FROM_UNIX, "to": now}, timeout=30)
        response.raise_for_status()
        payload = response.json()
        prices: dict[str, float] = {}
        for timestamp, value in payload.get("prices", []):
            try:
                price = float(value)
                date = utc_date(int(timestamp))
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(price) and price > 0:
                # CoinGecko may return multiple intraday observations; retain the latest daily point.
                prices[date] = price
        if not prices:
            raise ValueError("CoinGecko returned no usable SOL prices")
        table = pa.table({"date": sorted(prices), "sol_usd": [prices[date] for date in sorted(prices)]}, schema=pa.schema([("date", pa.string()), ("sol_usd", pa.float64())]))
        temporary = derived_dir / "sol_prices.tmp"
        pq.write_table(table, temporary, compression="snappy")
        os.replace(temporary, derived_dir / "sol_prices.parquet")
        logger.info("Refreshed %d SOL/USD daily prices through %s", len(prices), max(prices))
        return prices
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        fallback = load_local_prices(derived_dir)
        if fallback:
            logger.warning("CoinGecko refresh failed (%s); using %d local SOL prices", exc, len(fallback))
            return fallback
        raise RuntimeError("CoinGecko refresh failed and no derived/sol_prices.parquet or .csv fallback exists") from exc


def completed_days(connection: sqlite3.Connection) -> set[str]:
    return {row[0] for row in connection.execute("SELECT date FROM completed_days")}


def load_births(path: Path) -> dict[str, tuple[str | None, float | None]]:
    if not path.exists():
        return {}
    result: dict[str, tuple[str | None, float | None]] = {}
    for batch in pq.ParquetFile(path).iter_batches(columns=["mint", "creator_wallet", "supply", "timestamp"], batch_size=ROW_BATCH_SIZE):
        rows = zip(*(batch.column(index).to_pylist() for index in range(4)), strict=True)
        for mint, creator, supply, _timestamp in rows:
            if mint not in result:
                result[mint] = (creator, float(supply) if supply and float(supply) > 0 else None)
    return result


def grouped_rows(connection: duckdb.DuckDBPyConnection, query: str) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    current_mint: str | None = None
    rows: list[dict[str, Any]] = []
    reader = connection.execute(query).to_arrow_reader(ROW_BATCH_SIZE)
    for batch in reader:
        columns = {name: batch.column(index).to_pylist() for index, name in enumerate(batch.schema.names)}
        for index, mint in enumerate(columns["mint"]):
            if current_mint is not None and mint != current_mint:
                yield current_mint, rows
                rows = []
            current_mint = mint
            rows.append({name: values[index] for name, values in columns.items()})
    if current_mint is not None:
        yield current_mint, rows


def open_duckdb(derived_dir: Path) -> duckdb.DuckDBPyConnection:
    temporary_dir = derived_dir / ".enrich-duckdb-tmp"
    temporary_dir.mkdir(parents=True, exist_ok=True)
    escaped_directory = str(temporary_dir).replace("'", "''")
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '2GB'")
    connection.execute(f"SET temp_directory = '{escaped_directory}'")
    connection.execute("SET threads = 2")
    connection.execute("SET preserve_insertion_order = false")
    return connection


def load_token_state(connection: sqlite3.Connection, mint: str, birth: tuple[str | None, float | None]) -> dict[str, Any]:
    row = connection.execute(
        "SELECT creator_wallet, supply, balances_json, creator_net_sol FROM token_state WHERE mint = ?", (mint,)
    ).fetchone()
    if row:
        return {"creator": row[0], "supply": row[1], "balances": json.loads(row[2]), "creator_net_sol": row[3], "last_market_cap_sol": None}
    creator, supply = birth
    return {"creator": creator, "supply": supply, "balances": {}, "creator_net_sol": 0.0, "last_market_cap_sol": None}


def persist_token_state(connection: sqlite3.Connection, mint: str, state: dict[str, Any]) -> None:
    connection.execute(
        """INSERT INTO token_state(mint, creator_wallet, supply, balances_json, creator_net_sol)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(mint) DO UPDATE SET creator_wallet=excluded.creator_wallet, supply=excluded.supply,
           balances_json=excluded.balances_json, creator_net_sol=excluded.creator_net_sol""",
        (mint, state["creator"], state["supply"], json.dumps(state["balances"], separators=(",", ":")), state["creator_net_sol"]),
    )


def apply_ticks_until(state: dict[str, Any], ticks: list[dict[str, Any]], position: int, boundary: int) -> tuple[int, bool]:
    creator_sold = False
    while position < len(ticks) and ticks[position]["timestamp"] < boundary:
        tick = ticks[position]
        wallet, action = tick["tx_signer"], str(tick["action"] or "").lower()
        amount = tick["token_amount"]
        sol_amount = tick["sol_amount"]
        if wallet and amount is not None and math.isfinite(float(amount)):
            delta = float(amount) if action == "buy" else -float(amount) if action == "sell" else 0.0
            if delta:
                state["balances"][wallet] = state["balances"].get(wallet, 0.0) + delta
        if wallet and wallet == state["creator"] and sol_amount is not None and math.isfinite(float(sol_amount)):
            if action == "sell":
                state["creator_net_sol"] += float(sol_amount)
                creator_sold = True
            elif action == "buy":
                state["creator_net_sol"] -= float(sol_amount)
        market_cap_sol = tick["market_cap_sol"]
        if market_cap_sol is not None and math.isfinite(float(market_cap_sol)):
            state["last_market_cap_sol"] = float(market_cap_sol)
        position += 1
    return position, creator_sold


def wallet_metrics(state: dict[str, Any]) -> tuple[float | None, float | None, float, int]:
    supply = state["supply"]
    balances = state["balances"]
    if not supply or not math.isfinite(float(supply)) or float(supply) <= 0:
        return None, None, float(state["creator_net_sol"]), len(balances)
    creator_balance = max(0.0, float(balances.get(state["creator"], 0.0))) if state["creator"] else 0.0
    top_ten = sum(sorted((max(0.0, float(balance)) for balance in balances.values()), reverse=True)[:10])
    return creator_balance / float(supply) * 100, top_ten / float(supply) * 100, float(state["creator_net_sol"]), len(balances)


def enrich_day(derived_dir: Path, date: str, sol_usd: float, state_db: sqlite3.Connection, logger: logging.Logger) -> int:
    ticks_path, births_path, bars_path = (derived_dir / name / f"{date}.parquet" for name in ("ticks", "births", "ohlcv"))
    if not all(path.exists() for path in (ticks_path, births_path, bars_path)):
        raise FileNotFoundError(f"{date} is not complete: ticks, births, and ohlcv files are required")
    births = load_births(births_path)
    destination = derived_dir / "enriched" / f"{date}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp")
    temporary.unlink(missing_ok=True)
    duck = open_duckdb(derived_dir)
    tick_sql = str(ticks_path).replace("'", "''")
    bar_sql = str(bars_path).replace("'", "''")
    tick_groups = grouped_rows(duck, f"SELECT mint, timestamp, action, tx_signer, token_amount, sol_amount, market_cap_sol FROM read_parquet('{tick_sql}') ORDER BY mint, timestamp, signature")
    bar_groups = grouped_rows(duck, f"SELECT * FROM read_parquet('{bar_sql}') ORDER BY mint, bar_time")
    next_tick = next(tick_groups, None)
    writer: pq.ParquetWriter | None = None
    output: dict[str, list[Any]] | None = None
    rows_written = 0
    output_schema = pa.schema(list(pq.ParquetFile(bars_path).schema_arrow) + list(ENRICHMENT_FIELDS))

    try:
        for mint, bars in bar_groups:
            while next_tick is not None and next_tick[0] < mint:
                next_tick = next(tick_groups, None)
            ticks = next_tick[1] if next_tick is not None and next_tick[0] == mint else []
            token_state = load_token_state(state_db, mint, births.get(mint, (bars[0].get("creator_wallet"), None)))
            tick_position = 0
            for bar in bars:
                tick_position, creator_sold = apply_ticks_until(token_state, ticks, tick_position, int(bar["bar_time"]) + 5_000)
                creator_holdings, top_ten, creator_net_sol, wallet_count = wallet_metrics(token_state)
                if output is None:
                    output = {name: [] for name in bar}
                    output.update({field.name: [] for field in ENRICHMENT_FIELDS})
                    writer = pq.ParquetWriter(temporary, output_schema, compression="snappy")
                for name, value in bar.items():
                    output[name].append(value)
                market_cap_sol = token_state["last_market_cap_sol"]
                if market_cap_sol is None and token_state["supply"] and bar.get("close") is not None:
                    market_cap_sol = float(token_state["supply"]) * float(bar["close"])
                output["market_cap_usd"].append(market_cap_sol * sol_usd if market_cap_sol is not None else None)
                output["volume_usd"].append((float(bar["buy_volume_sol"] or 0) + float(bar["sell_volume_sol"] or 0)) * sol_usd)
                output["creator_holdings_pct"].append(creator_holdings)
                output["top10_holder_pct"].append(top_ten)
                output["creator_net_sol"].append(creator_net_sol)
                output["creator_is_selling"].append(creator_sold)
                output["unique_wallets_total"].append(wallet_count)
                if len(output["mint"]) >= ROW_BATCH_SIZE:
                    writer.write_table(pa.Table.from_pydict(output, schema=output_schema))
                    rows_written += len(output["mint"])
                    for values in output.values():
                        values.clear()
            persist_token_state(state_db, mint, token_state)
            if next_tick is not None and next_tick[0] == mint:
                next_tick = next(tick_groups, None)
        if writer is None or output is None:
            raise RuntimeError(f"No OHLCV bars found for {date}")
        if output["mint"]:
            writer.write_table(pa.Table.from_pydict(output, schema=output_schema))
        writer.close()
        writer = None
        os.replace(temporary, destination)
        state_db.execute("INSERT OR REPLACE INTO completed_days(date, completed_at) VALUES (?, ?)", (date, datetime.now(UTC).isoformat()))
        state_db.commit()
        logger.info("Enriched %s: %d rows at SOL/USD $%.2f", date, rows_written + len(output["mint"]), sol_usd)
        return rows_written + len(output["mint"])
    except Exception:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
        state_db.rollback()
        raise
    finally:
        duck.close()


def validate_day(derived_dir: Path, date: str, logger: logging.Logger) -> None:
    enriched = derived_dir / "enriched" / f"{date}.parquet"
    ohlcv = derived_dir / "ohlcv" / f"{date}.parquet"
    if pq.ParquetFile(enriched).metadata.num_rows != pq.ParquetFile(ohlcv).metadata.num_rows:
        raise RuntimeError(f"Row count mismatch for {date}")
    connection = duckdb.connect()
    path = str(enriched).replace("'", "''")
    births_path = str(derived_dir / "births" / f"{date}.parquet").replace("'", "''")
    sample = connection.execute(
        f"""SELECT e.mint, b.creator_wallet, b.initial_buy, e.seconds_since_birth,
                   e.creator_holdings_pct, e.top10_holder_pct
            FROM read_parquet('{path}') e
            JOIN read_parquet('{births_path}') b USING (mint)
            WHERE e.seconds_since_birth BETWEEN 0 AND 300
            ORDER BY e.seconds_since_birth LIMIT 60"""
    ).fetchall()
    coverage = connection.execute(f"SELECT count(*) FILTER (WHERE market_cap_usd IS NOT NULL), count(*) FROM read_parquet('{path}')").fetchone()
    connection.close()
    logger.info("Validation %s: USD market cap non-null %d/%d; first five creator bars=%s", date, coverage[0], coverage[1], sample[:5])


def pending_dates(derived_dir: Path, done: set[str], requested: str | None) -> list[str]:
    dates = [path.stem for path in sorted((derived_dir / "ohlcv").glob("*.parquet"))]
    if requested:
        dates = [date for date in dates if date == requested]
    return [date for date in dates if date not in done]


def rebuild_token_state(
    derived_dir: Path,
    through_date: str,
    state_db: sqlite3.Connection,
    logger: logging.Logger,
) -> None:
    datetime.fromisoformat(through_date)
    state_db.execute("DELETE FROM token_state")
    state_db.execute("DELETE FROM completed_days WHERE date > ?", (through_date,))
    state_db.commit()

    tick_paths = [
        path for path in sorted((derived_dir / "ticks").glob("*.parquet")) if path.stem <= through_date
    ]
    for index, ticks_path in enumerate(tick_paths, start=1):
        date = ticks_path.stem
        births = load_births(derived_dir / "births" / f"{date}.parquet")
        tick_sql = str(ticks_path).replace("'", "''")
        duck = open_duckdb(derived_dir)
        try:
            tick_groups = grouped_rows(
                duck,
                f"SELECT mint, timestamp, action, tx_signer, token_amount, sol_amount, market_cap_sol "
                f"FROM read_parquet('{tick_sql}') ORDER BY mint, timestamp, signature",
            )
            for mint, ticks in tick_groups:
                token_state = load_token_state(state_db, mint, births.get(mint, (None, None)))
                apply_ticks_until(token_state, ticks, 0, 2**63 - 1)
                persist_token_state(state_db, mint, token_state)
            state_db.commit()
        except Exception:
            state_db.rollback()
            raise
        finally:
            duck.close()
        logger.info("Rebuilt token state through %s [%d/%d]", date, index, len(tick_paths))


def run(args: argparse.Namespace) -> None:
    derived_dir = (args.root / "derived").resolve()
    logger = configure_logging(derived_dir)
    state_db = init_state(derived_dir)
    try:
        if args.rebuild_state_through:
            rebuild_token_state(derived_dir, args.rebuild_state_through, state_db, logger)
            return
        last_price_refresh: str | None = None
        while True:
            today = datetime.now(UTC).date().isoformat()
            prices = refresh_sol_prices(derived_dir, logger) if today != last_price_refresh else load_local_prices(derived_dir)
            last_price_refresh = today
            for date in pending_dates(derived_dir, completed_days(state_db), args.date):
                price = prices.get(date)
                if price is None:
                    logger.warning("Skipping %s: no SOL/USD price available", date)
                    continue
                enrich_day(derived_dir, date, price, state_db, logger)
                validate_day(derived_dir, date, logger)
            if args.once or args.date:
                return
            time.sleep(POLL_SECONDS)
    finally:
        state_db.close()


if __name__ == "__main__":
    try:
        run(parse_args())
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.exception("PumpApi enrichment stopped: %s", exc)
        raise

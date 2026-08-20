#!/usr/bin/env python3
"""Evaluate PumpPortal-collected tokens after their first two minutes.

The evaluator records hypothetical entry data only.  It contains no order
creation, transaction signing, or execution-adapter calls.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

import httpx
from solders.pubkey import Pubkey

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from src.chain.pumpfun import (  # noqa: E402
    WSOL_MINT,
    calculate_buy_amount,
    fetch_bonding_curve_state,
)

DATABASE_PATH = Path("data/realtime.db")
EVALUATION_MIN_AGE_S = 120
EVALUATION_MAX_AGE_S = 150
EVALUATION_INTERVAL_S = 5
FUNNEL_INTERVAL_S = 300
RPC_CALL_INTERVAL_S = 0.5
WOULD_BUY_SOL = 0.05
DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"

logger = logging.getLogger("evaluator")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS births (
    mint TEXT PRIMARY KEY,
    creator TEXT,
    name TEXT,
    symbol TEXT,
    created_at REAL NOT NULL,
    initial_buy_sol REAL,
    raw_data TEXT
);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL,
    side TEXT NOT NULL,
    sol_amount REAL,
    token_amount REAL,
    wallet TEXT,
    timestamp_ms INTEGER NOT NULL,
    raw_data TEXT,
    FOREIGN KEY (mint) REFERENCES births(mint)
);
CREATE INDEX IF NOT EXISTS idx_trades_mint_ts ON trades(mint, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_births_created ON births(created_at);
CREATE TABLE IF NOT EXISTS evaluations (
    mint TEXT PRIMARY KEY,
    evaluated_at REAL NOT NULL,
    birth_age_seconds REAL,
    trade_count_2m INTEGER,
    trade_count_1m INTEGER,
    unique_wallets_2m INTEGER,
    unique_wallets_1m INTEGER,
    buy_volume_sol_2m REAL,
    buy_volume_sol_1m REAL,
    buy_sell_ratio REAL,
    market_cap_sol REAL,
    passed_gates BOOLEAN,
    gate_failures TEXT,
    would_buy_price REAL,
    would_buy_tokens REAL,
    FOREIGN KEY (mint) REFERENCES births(mint)
);
"""

CurveReader = Callable[[str], Awaitable[tuple[float | None, float | None, float | None]]]


@dataclass(frozen=True)
class Evaluation:
    mint: str
    birth_age_seconds: float
    trade_count_2m: int
    trade_count_1m: int
    unique_wallets_2m: int
    unique_wallets_1m: int
    buy_volume_sol_2m: float
    buy_volume_sol_1m: float
    buy_sell_ratio: float
    failures: list[str]

    @property
    def passed(self) -> bool:
        return not self.failures


def gate_failures(metrics: Mapping[str, float | int]) -> list[str]:
    """Return every failed MT-581/582 two-minute characterization gate."""
    thresholds = (
        ("2m_trade_count", "trade_count_2m", 131),
        ("1m_trade_count", "trade_count_1m", 84),
        ("1m_unique_wallets", "unique_wallets_1m", 11),
        ("2m_unique_wallets", "unique_wallets_2m", 12),
        ("2m_buy_volume", "buy_volume_sol_2m", 37),
        ("1m_buy_volume", "buy_volume_sol_1m", 26.5),
        ("buy_sell_ratio", "buy_sell_ratio", 0.5),
    )
    return [name for name, field, minimum in thresholds if metrics[field] < minimum]


class Evaluator:
    """Read-only-from-trades evaluator with its own SQLite write connection."""

    def __init__(
        self,
        db_path: Path = DATABASE_PATH,
        *,
        rpc_url: str | None = None,
        curve_reader: CurveReader | None = None,
    ) -> None:
        self.db_path = db_path
        self.rpc_url = rpc_url or os.getenv("PRIMARY_RPC_URL") or DEFAULT_RPC_URL
        self.stop_event = asyncio.Event()
        self.db: sqlite3.Connection | None = None
        self._client: httpx.AsyncClient | None = None
        self._curve_reader = curve_reader
        self.window_started_at = time.time()
        self.window_births = 0
        self.window_evaluated = 0
        self.window_passed = 0
        self.window_failures: Counter[str] = Counter()

    def setup_database(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.db_path, timeout=5.0)
        self.db.executescript(SCHEMA)
        self.db.commit()

    def pending_births(self, now: float) -> list[sqlite3.Row]:
        if self.db is None:
            raise RuntimeError("database is not initialized")
        self.db.row_factory = sqlite3.Row
        return self.db.execute(
            "SELECT b.mint, b.created_at FROM births b "
            "LEFT JOIN evaluations e ON e.mint = b.mint "
            "WHERE e.mint IS NULL AND b.created_at >= ? AND b.created_at < ? "
            "ORDER BY b.created_at",
            (now - EVALUATION_MAX_AGE_S, now - EVALUATION_MIN_AGE_S),
        ).fetchall()

    def evaluate_metrics(self, mint: str, created_at: float, now: float) -> Evaluation:
        if self.db is None:
            raise RuntimeError("database is not initialized")
        start_ms = int(created_at * 1000)
        minute_ms = start_ms + 60_000
        two_minutes_ms = start_ms + 120_000
        row = self.db.execute(
            """SELECT
            COUNT(*) AS trade_count_2m,
            SUM(timestamp_ms < ?) AS trade_count_1m,
            COUNT(DISTINCT wallet) AS unique_wallets_2m,
            COUNT(DISTINCT CASE WHEN timestamp_ms < ? THEN wallet END) AS unique_wallets_1m,
            COALESCE(SUM(CASE WHEN side = 'buy' THEN sol_amount ELSE 0 END), 0)
                AS buy_volume_sol_2m,
            COALESCE(SUM(CASE WHEN side = 'buy' AND timestamp_ms < ? THEN sol_amount ELSE 0 END), 0)
                AS buy_volume_sol_1m,
            COALESCE(SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END), 0) AS buy_count
            FROM trades WHERE mint = ? AND timestamp_ms >= ? AND timestamp_ms < ?""",
            (minute_ms, minute_ms, minute_ms, mint, start_ms, two_minutes_ms),
        ).fetchone()
        trade_count = int(row["trade_count_2m"])
        metrics: dict[str, float | int] = {
            "trade_count_2m": trade_count,
            "trade_count_1m": int(row["trade_count_1m"] or 0),
            "unique_wallets_2m": int(row["unique_wallets_2m"] or 0),
            "unique_wallets_1m": int(row["unique_wallets_1m"] or 0),
            "buy_volume_sol_2m": float(row["buy_volume_sol_2m"] or 0),
            "buy_volume_sol_1m": float(row["buy_volume_sol_1m"] or 0),
            "buy_sell_ratio": int(row["buy_count"]) / trade_count if trade_count else 0.0,
        }
        return Evaluation(
            mint=mint,
            birth_age_seconds=now - created_at,
            failures=gate_failures(metrics),
            **metrics,
        )

    async def read_curve(self, mint: str) -> tuple[float | None, float | None, float | None]:
        """Return market cap, hypothetical price, and token output without trading."""
        if self._curve_reader is not None:
            return await self._curve_reader(mint)
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized")
        try:
            account = await fetch_bonding_curve_state(
                self.rpc_url, Pubkey.from_string(mint), http_client=self._client
            )
            if account.state.complete or account.state.quote_mint != WSOL_MINT:
                return None, None, None
            output_raw = calculate_buy_amount(
                int(WOULD_BUY_SOL * 1_000_000_000),
                account.state.virtual_sol_reserves,
                account.state.virtual_token_reserves,
            )
            output_tokens = output_raw / (10**account.token_decimals)
            if output_tokens <= 0:
                return None, None, None
            market_cap_sol = (
                account.state.virtual_sol_reserves
                / account.state.virtual_token_reserves
                * account.state.token_total_supply
                / 1_000_000_000
            )
            return market_cap_sol, WOULD_BUY_SOL / output_tokens, output_tokens
        except (ValueError, httpx.HTTPError) as exc:
            logger.warning("[EVALUATOR] curve unavailable for %s: %s", mint, exc)
            return None, None, None

    def persist(
        self,
        evaluation: Evaluation,
        now: float,
        curve: tuple[float | None, float | None, float | None],
    ) -> None:
        if self.db is None:
            raise RuntimeError("database is not initialized")
        market_cap_sol, would_buy_price, would_buy_tokens = curve
        self.db.execute(
            """INSERT OR IGNORE INTO evaluations
            (mint,evaluated_at,birth_age_seconds,trade_count_2m,trade_count_1m,
            unique_wallets_2m,unique_wallets_1m,buy_volume_sol_2m,buy_volume_sol_1m,
            buy_sell_ratio,market_cap_sol,passed_gates,gate_failures,would_buy_price,would_buy_tokens)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                evaluation.mint,
                now,
                evaluation.birth_age_seconds,
                evaluation.trade_count_2m,
                evaluation.trade_count_1m,
                evaluation.unique_wallets_2m,
                evaluation.unique_wallets_1m,
                evaluation.buy_volume_sol_2m,
                evaluation.buy_volume_sol_1m,
                evaluation.buy_sell_ratio,
                market_cap_sol,
                evaluation.passed,
                json.dumps(evaluation.failures),
                would_buy_price,
                would_buy_tokens,
            ),
        )
        self.db.commit()

    async def evaluate_once(self, now: float | None = None) -> int:
        current_time = time.time() if now is None else now
        evaluations = [
            self.evaluate_metrics(row["mint"], row["created_at"], current_time)
            for row in self.pending_births(current_time)
        ]
        for evaluation in evaluations:
            curve = (None, None, None)
            if evaluation.passed:
                curve = await self.read_curve(evaluation.mint)
                await asyncio.sleep(RPC_CALL_INTERVAL_S)
            self.persist(evaluation, current_time, curve)
            self.window_evaluated += 1
            self.window_passed += int(evaluation.passed)
            self.window_failures.update(evaluation.failures)
            logger.info(
                "[EVALUATOR] mint=%s pass=%s failures=%s trades=%s/%s wallets=%s/%s",
                evaluation.mint,
                evaluation.passed,
                ",".join(evaluation.failures) or "none",
                evaluation.trade_count_1m,
                evaluation.trade_count_2m,
                evaluation.unique_wallets_1m,
                evaluation.unique_wallets_2m,
            )
        return len(evaluations)

    def log_funnel(self) -> None:
        if self.db is None:
            return
        now = time.time()
        births = self.db.execute(
            "SELECT COUNT(*) FROM births WHERE created_at >= ?", (self.window_started_at,)
        ).fetchone()[0]
        pass_rate = (
            self.window_passed / self.window_evaluated * 100 if self.window_evaluated else 0.0
        )
        top_failure, count = (
            self.window_failures.most_common(1)[0] if self.window_failures else ("none", 0)
        )
        failure_rate = count / self.window_evaluated * 100 if self.window_evaluated else 0.0
        logger.info(
            "[EVALUATOR] %s | Window: last 5min | Births: %s | Evaluated: %s | "
            "Passed: %s (%.1f%%) | Top failure: %s (%.1f%%)",
            time.strftime("%Y-%m-%d %H:%M:%S"),
            births,
            self.window_evaluated,
            self.window_passed,
            pass_rate,
            top_failure,
            failure_rate,
        )
        self.window_started_at = now
        self.window_births = self.window_evaluated = self.window_passed = 0
        self.window_failures.clear()

    async def run(self) -> None:
        self.setup_database()
        self._client = httpx.AsyncClient(timeout=10.0)
        next_funnel_at = time.monotonic() + FUNNEL_INTERVAL_S
        try:
            while not self.stop_event.is_set():
                await self.evaluate_once()
                if time.monotonic() >= next_funnel_at:
                    self.log_funnel()
                    next_funnel_at = time.monotonic() + FUNNEL_INTERVAL_S
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=EVALUATION_INTERVAL_S)
                except TimeoutError:
                    pass
        finally:
            self.log_funnel()
            if self._client is not None:
                await self._client.aclose()
                self._client = None
            if self.db is not None:
                self.db.close()
                self.db = None


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    evaluator = Evaluator()
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(shutdown_signal, evaluator.stop_event.set)
    await evaluator.run()


if __name__ == "__main__":
    asyncio.run(main())

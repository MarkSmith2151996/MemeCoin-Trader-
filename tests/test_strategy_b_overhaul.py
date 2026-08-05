"""Focused offline coverage for Strategy B source, persistence, and tuning."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path

import httpx

from scripts.run_strategy_b import (
    MAX_TOP10_HOLDER_PCT,
    SOURCE_MAX_AGE_MINUTES,
    _age_holder_tier,
    _search_fresh_pair,
)
from src.core.database import init_db, mark_strategy_candidate_entered, record_strategy_candidate
from src.strategy.gate_tuner import GateThresholds, GateTuner


def test_search_source_discards_stale_pairs() -> None:
    now_ms = time.time() * 1000

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "pairs": [
                _pair(now_ms - 40 * 60_000, "stale"),
                _pair(now_ms - 5 * 60_000, "fresh"),
            ],
        })

    async def run() -> dict | None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await _search_fresh_pair("fresh", client)

    candidate = asyncio.run(run())
    assert candidate is not None
    assert candidate["ticker"] == "fresh"
    assert candidate["source_age_minutes"] <= SOURCE_MAX_AGE_MINUTES


def test_holder_gate_accepts_concentrated_fresh_tokens() -> None:
    assert MAX_TOP10_HOLDER_PCT == 90.0
    assert all(_age_holder_tier(age)[1] == 90.0 for age in (0.5, 3, 8, 20))


def test_candidate_tables_and_initial_gate_config(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    thresholds = GateThresholds()
    tuner = GateTuner(db_path, thresholds)
    asyncio.run(tuner.ensure_initial_config())

    candidate_id = asyncio.run(record_strategy_candidate(
        db_path, strategy="B", mint_address="mint", ticker="TST", age_minutes=5,
        mcap_usd=5_000, volume_usd=300, txns_buys=4, txns_sells=2,
        buy_sell_ratio=2, liquidity_usd=1_000, fdv=5_000, price_usd=0.01,
        price_change_5m=2, price_change_1h=3, rugcheck_result="pass",
        dev_holdings_pct=1, top10_holder_pct=10, gates_passed=["age"], gates_failed={},
    ))
    asyncio.run(mark_strategy_candidate_entered(db_path, candidate_id, "position"))

    with sqlite3.connect(db_path) as db:
        assert db.execute("SELECT strategy FROM positions LIMIT 1").description[0][0] == "strategy"
        row = db.execute("SELECT reason, config_json FROM gate_config").fetchone()
        assert row[0] == "initial"
        assert json.loads(row[1])["min_mcap_usd"] == 2_000
        assert db.execute("SELECT entered, position_id FROM candidate_log").fetchone() == (1, "position")


def test_tuner_caps_each_adjustment_at_25_percent(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    with sqlite3.connect(db_path) as db:
        for index in range(50):
            position_id = f"position-{index}"
            db.execute(
                """INSERT INTO positions (
                    id, mint_address, entry_trade_id, amount_sol, token_amount, entry_price_sol,
                    status, opened_at, realized_pnl_sol, partial_exits_json, strategy
                ) VALUES (?, 'mint', 'trade', 1, 1, 1, 'CLOSED', 'now', 1, '{}', 'B')""",
                (position_id,),
            )
            db.execute(
                """INSERT INTO candidate_log (
                    scan_time, strategy, mint_address, age_minutes, mcap_usd, volume_usd,
                    buy_sell_ratio, entered, position_id
                ) VALUES ('now', 'B', 'mint', 10, 100, 100, 0.1, 1, ?)""",
                (position_id,),
            )
        db.commit()

    thresholds = GateThresholds()
    tuner = GateTuner(db_path, thresholds)
    asyncio.run(tuner.ensure_initial_config())
    assert asyncio.run(tuner.maybe_tune()) is True
    assert thresholds.max_age_minutes == 22.5
    assert thresholds.min_mcap_usd == 1_500
    assert thresholds.min_volume_usd == 150
    assert thresholds.min_buy_sell_ratio == 0.3


def _pair(created_ms: float, ticker: str) -> dict:
    return {
        "chainId": "solana",
        "baseToken": {"address": ticker + "Mint", "symbol": ticker},
        "quoteToken": {"address": "So11111111111111111111111111111111111111112"},
        "pairCreatedAt": created_ms,
        "marketCap": 5_000,
        "volume": {"h1": 300},
        "txns": {"h1": {"buys": 4, "sells": 2}},
        "liquidity": {"usd": 1_000},
    }

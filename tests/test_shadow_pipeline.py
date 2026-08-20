"""Offline coverage for the PumpPortal collector and two-minute evaluator."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import evaluator as evaluator_module  # noqa: E402
from collector import (  # noqa: E402
    Collector,
    birth_from_payload,
    pumpportal_url,
    trade_from_payload,
)
from evaluator import Evaluator, gate_failures  # noqa: E402


class _WebSocket:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))


def test_pumpportal_url_uses_an_optional_escaped_api_key(monkeypatch) -> None:
    monkeypatch.delenv("PUMPPORTAL_API_KEY", raising=False)
    assert pumpportal_url() == "wss://pumpportal.fun/api/data"
    monkeypatch.setenv("PUMPPORTAL_API_KEY", "key with / punctuation")
    assert pumpportal_url().endswith("?api-key=key%20with%20%2F%20punctuation")


def test_collector_normalizes_and_scopes_token_trades(tmp_path: Path) -> None:
    now = time.time()
    birth = birth_from_payload(
        {
            "mint": "mint-one",
            "traderPublicKey": "creator",
            "name": "One",
            "symbol": "ONE",
            "initialBuy": 1.25,
            "createdTimestamp": int(now * 1000),
        },
        now,
    )
    trade = trade_from_payload(
        {
            "mint": "mint-one",
            "txType": "buy",
            "solAmount": 0.5,
            "tokenAmount": 1000,
            "traderPublicKey": "buyer",
            "timestamp": int(now * 1000),
        },
        now,
    )
    assert birth is not None
    assert trade is not None

    async def collect() -> _WebSocket:
        collector = Collector(tmp_path / "realtime.db")
        collector.setup_database()
        websocket = _WebSocket()
        await collector.handle_payload(
            websocket,
            {
                "mint": "mint-one",
                "name": "One",
                "symbol": "ONE",
                "createdTimestamp": int(now * 1000),
            },
        )
        await collector.handle_payload(
            websocket,
            {
                "mint": "mint-one",
                "txType": "buy",
                "solAmount": 0.5,
                "tokenAmount": 1000,
                "traderPublicKey": "buyer",
                "timestamp": int(now * 1000),
            },
        )
        assert collector.db is not None
        collector.db.commit()
        collector.db.close()
        return websocket

    websocket = asyncio.run(collect())
    assert websocket.sent == [{"method": "subscribeTokenTrade", "keys": ["mint-one"]}]
    connection = sqlite3.connect(tmp_path / "realtime.db")
    assert connection.execute("SELECT COUNT(*) FROM births").fetchone()[0] == 1
    assert connection.execute("SELECT side, sol_amount FROM trades").fetchone() == ("buy", 0.5)
    connection.close()


def test_gate_failures_reports_all_characterization_thresholds() -> None:
    failures = gate_failures(
        {
            "trade_count_2m": 130,
            "trade_count_1m": 83,
            "unique_wallets_2m": 11,
            "unique_wallets_1m": 10,
            "buy_volume_sol_2m": 36.9,
            "buy_volume_sol_1m": 26.4,
            "buy_sell_ratio": 0.49,
        }
    )
    assert failures == [
        "2m_trade_count",
        "1m_trade_count",
        "1m_unique_wallets",
        "2m_unique_wallets",
        "2m_buy_volume",
        "1m_buy_volume",
        "buy_sell_ratio",
    ]


def test_evaluator_handles_a_birth_with_no_trades(tmp_path: Path) -> None:
    now = time.time()
    evaluator = Evaluator(tmp_path / "realtime.db")
    evaluator.setup_database()
    assert evaluator.db is not None
    evaluator.db.execute(
        "INSERT INTO births (mint, created_at) VALUES (?, ?)", ("quiet", now - 121)
    )
    evaluator.db.commit()

    assert asyncio.run(evaluator.evaluate_once(now)) == 1
    row = evaluator.db.execute(
        "SELECT trade_count_1m, unique_wallets_2m, buy_volume_sol_2m, passed_gates FROM evaluations"
    ).fetchone()
    evaluator.db.close()
    assert tuple(row) == (0, 0, 0.0, 0)


def test_evaluator_persists_pass_with_hypothetical_curve_quote(tmp_path: Path) -> None:
    now = time.time()
    created_at = now - 121
    database = tmp_path / "realtime.db"
    evaluator = Evaluator(
        database,
        curve_reader=lambda _: asyncio.sleep(0, result=(42.0, 0.00001, 5_000.0)),
    )
    evaluator.setup_database()
    assert evaluator.db is not None
    evaluator.db.execute(
        "INSERT INTO births (mint, created_at) VALUES (?, ?)", ("passing-mint", created_at)
    )
    start_ms = int(created_at * 1000)
    for index in range(131):
        evaluator.db.execute(
            "INSERT INTO trades (mint,side,sol_amount,wallet,timestamp_ms) VALUES (?,?,?,?,?)",
            (
                "passing-mint",
                "buy" if index < 66 else "sell",
                0.6 if index < 66 else 0.1,
                f"wallet-{index % 12}",
                start_ms + (30_000 if index < 84 else 90_000),
            ),
        )
    evaluator.db.commit()

    previous_interval = evaluator_module.RPC_CALL_INTERVAL_S
    evaluator_module.RPC_CALL_INTERVAL_S = 0
    try:
        assert asyncio.run(evaluator.evaluate_once(now)) == 1
    finally:
        evaluator_module.RPC_CALL_INTERVAL_S = previous_interval

    row = evaluator.db.execute(
        "SELECT passed_gates, gate_failures, market_cap_sol, would_buy_price, would_buy_tokens "
        "FROM evaluations WHERE mint = 'passing-mint'"
    ).fetchone()
    evaluator.db.close()
    assert tuple(row) == (1, "[]", 42.0, 0.00001, 5_000.0)

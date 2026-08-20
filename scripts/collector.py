#!/usr/bin/env python3
"""Collect fresh Pump.fun births and their first five minutes of trades.

This is a data-only shadow-process.  It does not import or invoke any trading
or execution code.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sqlite3
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import quote

import websockets

DATABASE_PATH = Path("data/realtime.db")
PUMPPORTAL_URL = "wss://pumpportal.fun/api/data"
TRADE_SUBSCRIPTION_S = 5 * 60
HEARTBEAT_S = 60
RECONNECT_MAX_S = 30.0

logger = logging.getLogger("collector")

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
"""


def _number(payload: Mapping[str, object], *names: str) -> float | None:
    for name in names:
        value = payload.get(name)
        if isinstance(value, bool) or value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def pumpportal_url() -> str:
    """Add the optional metered-trade API key without exposing it in logs."""
    api_key = os.getenv("PUMPPORTAL_API_KEY", "").strip()
    if not api_key:
        return PUMPPORTAL_URL
    return f"{PUMPPORTAL_URL}?api-key={quote(api_key, safe='')}"


def _text(payload: Mapping[str, object], *names: str) -> str | None:
    for name in names:
        value = payload.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _timestamp_seconds(payload: Mapping[str, object], fallback: float) -> float:
    value = _number(payload, "createdTimestamp", "timestamp", "time", "created_at")
    if value is None or value <= 0:
        return fallback
    return value / 1000 if value > 10_000_000_000 else value


def _timestamp_ms(payload: Mapping[str, object], fallback: float) -> int:
    value = _number(payload, "timestamp", "timestampMs", "createdTimestamp", "time")
    if value is None or value <= 0:
        return int(fallback * 1000)
    return int(value if value > 10_000_000_000 else value * 1000)


def birth_from_payload(
    payload: Mapping[str, object], received_at: float
) -> dict[str, object] | None:
    """Normalize the documented PumpPortal new-token payload fields."""
    mint = _text(payload, "mint", "mintAddress")
    if mint is None:
        return None
    return {
        "mint": mint,
        "creator": _text(payload, "traderPublicKey", "creator", "creatorAddress"),
        "name": _text(payload, "name"),
        "symbol": _text(payload, "symbol", "ticker"),
        "created_at": _timestamp_seconds(payload, received_at),
        "initial_buy_sol": _number(payload, "initialBuy", "initialBuySol", "solAmount"),
        "raw_data": json.dumps(payload, separators=(",", ":"), default=str),
    }


def trade_from_payload(
    payload: Mapping[str, object], received_at: float
) -> dict[str, object] | None:
    """Normalize a PumpPortal token-trade payload, rejecting unknown sides."""
    mint = _text(payload, "mint", "mintAddress")
    raw_side = _text(payload, "txType", "side", "type")
    side = raw_side.lower() if raw_side else ""
    if mint is None or side not in {"buy", "sell"}:
        return None
    return {
        "mint": mint,
        "side": side,
        "sol_amount": _number(payload, "solAmount", "sol_amount", "amountSol"),
        "token_amount": _number(payload, "tokenAmount", "token_amount", "amount"),
        "wallet": _text(payload, "traderPublicKey", "wallet", "user"),
        "timestamp_ms": _timestamp_ms(payload, received_at),
        "raw_data": json.dumps(payload, separators=(",", ":"), default=str),
    }


class Collector:
    """PumpPortal collector that keeps trades scoped to newly born tokens."""

    def __init__(self, db_path: Path = DATABASE_PATH) -> None:
        self.db_path = db_path
        self.stop_event = asyncio.Event()
        self.db: sqlite3.Connection | None = None
        self.active_mints: dict[str, float] = {}
        self.total_births = 0
        self.total_trades = 0
        self.period_births = 0
        self.period_trades = 0
        self.unhandled_messages = 0
        self._last_commit = time.monotonic()

    def setup_database(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.db_path, timeout=5.0)
        self.db.executescript(SCHEMA)
        self.db.commit()

    def persist_birth(self, birth: Mapping[str, object]) -> bool:
        if self.db is None:
            raise RuntimeError("database is not initialized")
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO births "
            "(mint,creator,name,symbol,created_at,initial_buy_sol,raw_data) VALUES (?,?,?,?,?,?,?)",
            tuple(
                birth[name]
                for name in (
                    "mint",
                    "creator",
                    "name",
                    "symbol",
                    "created_at",
                    "initial_buy_sol",
                    "raw_data",
                )
            ),
        )
        inserted = cursor.rowcount == 1
        if inserted:
            self.total_births += 1
            self.period_births += 1
        self._commit_if_due()
        return inserted

    def persist_trade(self, trade: Mapping[str, object]) -> None:
        if self.db is None:
            raise RuntimeError("database is not initialized")
        self.db.execute(
            """INSERT INTO trades
            (mint,side,sol_amount,token_amount,wallet,timestamp_ms,raw_data)
            VALUES (?,?,?,?,?,?,?)""",
            tuple(
                trade[name]
                for name in (
                    "mint",
                    "side",
                    "sol_amount",
                    "token_amount",
                    "wallet",
                    "timestamp_ms",
                    "raw_data",
                )
            ),
        )
        self.total_trades += 1
        self.period_trades += 1
        self._commit_if_due()

    def _commit_if_due(self) -> None:
        if self.db is not None and time.monotonic() - self._last_commit >= 0.5:
            self.db.commit()
            self._last_commit = time.monotonic()

    async def handle_payload(self, websocket: Any, payload: Mapping[str, object]) -> None:
        received_at = time.time()
        trade = trade_from_payload(payload, received_at)
        if trade is not None:
            if str(trade["mint"]) in self.active_mints:
                self.persist_trade(trade)
            else:
                logger.debug("[COLLECTOR] ignored trade for inactive mint %s", trade["mint"])
            return

        birth = birth_from_payload(payload, received_at)
        if birth is not None:
            mint = str(birth["mint"])
            if self.persist_birth(birth):
                self.active_mints[mint] = received_at + TRADE_SUBSCRIPTION_S
                await websocket.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
            return
        self.unhandled_messages += 1
        if self.unhandled_messages <= 3:
            logger.info(
                "[COLLECTOR] PumpPortal subscription response: %s",
                payload.get("message", sorted(payload)),
            )

    async def unsubscribe_expired(self, websocket: Any) -> None:
        now = time.time()
        expired = [mint for mint, deadline in self.active_mints.items() if deadline <= now]
        for mint in expired:
            await websocket.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": [mint]}))
            del self.active_mints[mint]

    async def heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=HEARTBEAT_S)
            except TimeoutError:
                if self.db is not None:
                    self.db.commit()
                    self._last_commit = time.monotonic()
                size_mb = (
                    self.db_path.stat().st_size / (1024 * 1024) if self.db_path.exists() else 0.0
                )
                logger.info(
                    "[COLLECTOR] %s | births: %s/min | trades: %s/min | "
                    "DB size: %.1fMB | total: %s",
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    self.period_births,
                    self.period_trades,
                    size_mb,
                    self.total_births + self.total_trades,
                )
                self.period_births = 0
                self.period_trades = 0

    async def run(self, websocket_url: str | None = None) -> None:
        self.setup_database()
        websocket_url = websocket_url or pumpportal_url()
        if "?api-key=" not in websocket_url:
            logger.warning(
                "[COLLECTOR] PUMPPORTAL_API_KEY is absent; births will collect, "
                "but PumpPortal will reject metered token-trade subscriptions"
            )
        heartbeat = asyncio.create_task(self.heartbeat_loop())
        reconnect_delay = 1.0
        try:
            while not self.stop_event.is_set():
                try:
                    async with websockets.connect(
                        websocket_url, ping_interval=30, ping_timeout=30
                    ) as websocket:
                        logger.info("[COLLECTOR] connected to PumpPortal")
                        await websocket.send(json.dumps({"method": "subscribeNewToken"}))
                        reconnect_delay = 1.0
                        while not self.stop_event.is_set():
                            try:
                                raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                            except TimeoutError:
                                await self.unsubscribe_expired(websocket)
                                continue
                            try:
                                payload = json.loads(raw_message)
                            except json.JSONDecodeError:
                                logger.warning("[COLLECTOR] ignored malformed websocket message")
                                continue
                            if isinstance(payload, Mapping):
                                await self.handle_payload(websocket, payload)
                            await self.unsubscribe_expired(websocket)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "[COLLECTOR] disconnected; retrying in %.0fs: %s", reconnect_delay, exc
                    )
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=reconnect_delay)
                    except TimeoutError:
                        pass
                    reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            if self.db is not None:
                self.db.commit()
                self.db.close()
                self.db = None


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    collector = Collector()
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(shutdown_signal, collector.stop_event.set)
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())

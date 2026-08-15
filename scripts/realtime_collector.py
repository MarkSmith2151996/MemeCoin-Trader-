#!/usr/bin/env python3
"""Persist real-time Pump.fun program events from Helius into SQLite.

This process is intentionally independent of Strategy B. It captures raw launch,
trade, and migration events for a later evaluator process.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import os
import signal
import struct
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import aiosqlite
import httpx
import websockets
from dotenv import load_dotenv

PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
DATABASE_PATH = Path("data/realtime.db")
RECONNECT_MAX_S = 30.0
STATS_INTERVAL_S = 60.0
LAMPORTS_PER_SOL = 1_000_000_000

logger = logging.getLogger("realtime_collector")

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS token_births (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL, name TEXT, symbol TEXT, creator_wallet TEXT,
    bonding_curve TEXT, signature TEXT UNIQUE, sol_amount REAL,
    timestamp_ms INTEGER NOT NULL, detected_at_ms INTEGER NOT NULL,
    pool TEXT DEFAULT 'pump', raw_logs TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_births_mint ON token_births(mint);
CREATE INDEX IF NOT EXISTS idx_births_timestamp ON token_births(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_births_creator ON token_births(creator_wallet);
CREATE TABLE IF NOT EXISTS token_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL, action TEXT NOT NULL, sol_amount REAL, token_amount REAL,
    trader_wallet TEXT, signature TEXT UNIQUE, timestamp_ms INTEGER NOT NULL,
    detected_at_ms INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_trades_mint ON token_trades(mint);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON token_trades(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_trades_mint_time ON token_trades(mint, timestamp_ms);
CREATE TABLE IF NOT EXISTS token_graduations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL, destination_pool TEXT, signature TEXT UNIQUE,
    timestamp_ms INTEGER NOT NULL, detected_at_ms INTEGER NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_graduations_mint ON token_graduations(mint);
CREATE TABLE IF NOT EXISTS collector_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_start TEXT NOT NULL, period_end TEXT NOT NULL, births_count INTEGER DEFAULT 0,
    trades_count INTEGER DEFAULT 0, graduations_count INTEGER DEFAULT 0,
    reconnects INTEGER DEFAULT 0, parse_errors INTEGER DEFAULT 0
);
"""


@dataclass(frozen=True)
class PumpEvent:
    event_type: str
    mint: str
    timestamp_ms: int | None = None
    name: str | None = None
    symbol: str | None = None
    creator_wallet: str | None = None
    bonding_curve: str | None = None
    sol_amount: float | None = None
    token_amount: float | None = None
    trader_wallet: str | None = None
    destination_pool: str | None = None


class BorshReader:
    """Small decoder for the fixed prefix of Pump.fun Anchor events."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 8  # Anchor event discriminator

    def _take(self, size: int) -> bytes:
        if self.offset + size > len(self.payload):
            raise ValueError("truncated Anchor event")
        value = self.payload[self.offset : self.offset + size]
        self.offset += size
        return value

    def string(self) -> str:
        size = struct.unpack("<I", self._take(4))[0]
        return self._take(size).decode("utf-8", errors="replace")

    def pubkey(self) -> str:
        return _base58_encode(self._take(32))

    def u64(self) -> int:
        return struct.unpack("<Q", self._take(8))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self._take(8))[0]

    def boolean(self) -> bool:
        return self._take(1) != b"\0"


def _base58_encode(value: bytes) -> str:
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = int.from_bytes(value, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = alphabet[remainder] + encoded
    return "1" * (len(value) - len(value.lstrip(b"\0"))) + (encoded or "1")


def _discriminator(name: str) -> bytes:
    return hashlib.sha256(f"event:{name}".encode()).digest()[:8]


def decode_anchor_event(encoded_data: str) -> PumpEvent | None:
    """Decode Pump.fun's documented Anchor event prefixes from a log payload."""
    try:
        payload = base64.b64decode(encoded_data, validate=True)
    except (ValueError, TypeError):
        return None
    if len(payload) < 8:
        return None

    reader = BorshReader(payload)
    try:
        if payload[:8] == _discriminator("CreateEvent"):
            name, symbol = reader.string(), reader.string()
            reader.string()  # URI is not persisted by this collector.
            mint, bonding_curve, user, creator = (
                reader.pubkey(), reader.pubkey(), reader.pubkey(), reader.pubkey()
            )
            timestamp_ms = reader.i64() * 1000
            return PumpEvent(
                "create", mint, timestamp_ms, name, symbol, creator, bonding_curve,
                trader_wallet=user,
            )
        if payload[:8] == _discriminator("TradeEvent"):
            mint = reader.pubkey()
            sol_amount, token_amount, is_buy, user = (
                reader.u64(), reader.u64(), reader.boolean(), reader.pubkey()
            )
            timestamp_ms = reader.i64() * 1000
            return PumpEvent(
                "buy" if is_buy else "sell", mint, timestamp_ms,
                sol_amount=sol_amount / LAMPORTS_PER_SOL,
                token_amount=float(token_amount), trader_wallet=user,
            )
        if payload[:8] == _discriminator("CompleteEvent"):
            # CompleteEvent is the bonding-curve graduation event.
            user, mint, bonding_curve = reader.pubkey(), reader.pubkey(), reader.pubkey()
            timestamp_ms = reader.i64() * 1000
            return PumpEvent(
                "migrate", mint, timestamp_ms, trader_wallet=user,
                bonding_curve=bonding_curve, destination_pool="pump_swap",
            )
    except ValueError:
        return None
    return None


def parse_logs(logs: list[str]) -> list[PumpEvent]:
    """Return decoded events from Pump.fun's base64 ``Program data`` log records."""
    events: list[PumpEvent] = []
    for line in logs:
        if line.startswith("Program data: "):
            event = decode_anchor_event(line.removeprefix("Program data: "))
            if event is not None:
                events.append(event)
    return events


def helius_config() -> tuple[str, str]:
    """Resolve websocket and HTTP RPC endpoints without logging secret material."""
    load_dotenv()
    api_key = os.getenv("HELIUS_API_KEY", "").strip()
    rpc_url = os.getenv("HELIUS_RPC_URL", "").strip()
    if not api_key and rpc_url:
        api_key = parse_qs(urlparse(rpc_url).query).get("api-key", [""])[0]
    if not api_key:
        raise RuntimeError("HELIUS_API_KEY or HELIUS_RPC_URL with an api-key is required")
    http_url = rpc_url or f"https://mainnet.helius-rpc.com/?api-key={api_key}"
    return f"wss://mainnet.helius-rpc.com/?api-key={api_key}", http_url


class RealtimeCollector:
    def __init__(self, db_path: Path, http_rpc_url: str) -> None:
        self.db_path = db_path
        self.http_rpc_url = http_rpc_url
        self.stop_event = asyncio.Event()
        self.period_started_at = time.time()
        self.births = self.trades = self.graduations = self.reconnects = self.parse_errors = 0
        self.lags_ms: list[int] = []
        self._http_client: httpx.AsyncClient | None = None

    async def setup_database(self) -> aiosqlite.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        db = await aiosqlite.connect(self.db_path)
        await db.executescript(SCHEMA)
        await db.commit()
        return db

    async def fetch_transaction(
        self, client: httpx.AsyncClient, signature: str
    ) -> Mapping[str, Any] | None:
        response = await client.post(
            self.http_rpc_url,
            json={
                "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                "params": [
                    signature,
                    {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
                ],
            },
        )
        response.raise_for_status()
        result = response.json().get("result")
        return result if isinstance(result, Mapping) else None

    async def fallback_event(self, signature: str, event_type: str) -> PumpEvent | None:
        """Use the paid RPC lookup only for create/migrate instructions missing event data."""
        if self._http_client is None:
            return None
        try:
            transaction = await self.fetch_transaction(self._http_client, signature)
        except (httpx.HTTPError, ValueError) as error:
            logger.warning("Unable to enrich %s %s: %s", event_type, signature, error)
            return None
        if transaction is None:
            return None
        meta = transaction.get("meta")
        balances = meta.get("postTokenBalances") if isinstance(meta, Mapping) else None
        mint = next(
            (balance.get("mint") for balance in balances or []
             if isinstance(balance, Mapping) and isinstance(balance.get("mint"), str)),
            None,
        )
        if not isinstance(mint, str):
            return None
        message = transaction.get("transaction", {}).get("message", {})
        account_keys = message.get("accountKeys", []) if isinstance(message, Mapping) else []
        creator = next(
            (key.get("pubkey") if isinstance(key, Mapping) else key for key in account_keys
             if isinstance(key, (str, Mapping))),
            None,
        )
        block_time = transaction.get("blockTime")
        return PumpEvent(
            event_type,
            mint,
            int(block_time) * 1000 if isinstance(block_time, int) else None,
            creator_wallet=creator if isinstance(creator, str) else None,
            destination_pool="pump_swap" if event_type == "migrate" else None,
        )

    async def persist(self, db: aiosqlite.Connection, event: PumpEvent, signature: str,
                      detected_at_ms: int, logs: list[str]) -> None:
        timestamp_ms = event.timestamp_ms or detected_at_ms
        self.lags_ms.append(max(0, detected_at_ms - timestamp_ms))
        if event.event_type == "create":
            cursor = await db.execute(
                "INSERT OR IGNORE INTO token_births "
                "(mint,name,symbol,creator_wallet,bonding_curve,signature,sol_amount,"
                "timestamp_ms,detected_at_ms,raw_logs) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (event.mint, event.name, event.symbol, event.creator_wallet, event.bonding_curve,
                 signature, event.sol_amount, timestamp_ms, detected_at_ms, json.dumps(logs)),
            )
            self.births += cursor.rowcount
        elif event.event_type in {"buy", "sell"}:
            cursor = await db.execute(
                "INSERT OR IGNORE INTO token_trades "
                "(mint,action,sol_amount,token_amount,trader_wallet,signature,"
                "timestamp_ms,detected_at_ms) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (event.mint, event.event_type, event.sol_amount, event.token_amount,
                 event.trader_wallet, signature, timestamp_ms, detected_at_ms),
            )
            self.trades += cursor.rowcount
        elif event.event_type == "migrate":
            cursor = await db.execute(
                "INSERT OR IGNORE INTO token_graduations "
                "(mint,destination_pool,signature,timestamp_ms,detected_at_ms) VALUES (?,?,?,?,?)",
                (event.mint, event.destination_pool, signature, timestamp_ms, detected_at_ms),
            )
            self.graduations += cursor.rowcount
        await db.commit()

    async def handle_notification(
        self, db: aiosqlite.Connection, payload: Mapping[str, Any]
    ) -> None:
        value = payload.get("params", {}).get("result", {}).get("value", {})
        if not isinstance(value, Mapping):
            self.parse_errors += 1
            return
        signature, logs = value.get("signature"), value.get("logs")
        if (
            not isinstance(signature, str)
            or not isinstance(logs, list)
            or not all(isinstance(log, str) for log in logs)
        ):
            self.parse_errors += 1
            return
        detected_at_ms = time.time_ns() // 1_000_000
        events = parse_logs(logs)
        for instruction, event_type in (("Create", "create"), ("Migrate", "migrate")):
            has_instruction = any(f"Instruction: {instruction}" in log for log in logs)
            has_event = any(event.event_type == event_type for event in events)
            if has_instruction and not has_event:
                fallback_event = await self.fallback_event(signature, event_type)
                if fallback_event is not None:
                    events.append(fallback_event)
        if not events:
            return
        for event in events:
            await self.persist(db, event, signature, detected_at_ms, logs)

    async def log_stats(self, db: aiosqlite.Connection) -> None:
        ended_at = time.time()
        lag_average = sum(self.lags_ms) / len(self.lags_ms) if self.lags_ms else 0.0
        await db.execute(
            "INSERT INTO collector_stats (period_start,period_end,births_count,trades_count,"
            "graduations_count,reconnects,parse_errors) VALUES (?,?,?,?,?,?,?)",
            (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.period_started_at)),
             time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ended_at)), self.births,
             self.trades, self.graduations, self.reconnects, self.parse_errors),
        )
        await db.commit()
        logger.info("COLLECTOR births=%s trades=%s graduations=%s errors=%s lag_avg=%.1fms",
                    self.births, self.trades, self.graduations, self.parse_errors, lag_average)
        self.period_started_at, self.births, self.trades, self.graduations = ended_at, 0, 0, 0
        self.reconnects = self.parse_errors = 0
        self.lags_ms.clear()

    async def run(self, websocket_url: str) -> None:
        db = await self.setup_database()
        self._http_client = httpx.AsyncClient(timeout=10.0)
        stats_task = asyncio.create_task(self._stats_loop(db))
        reconnect_delay = 1.0
        try:
            while not self.stop_event.is_set():
                try:
                    async with websockets.connect(
                        websocket_url, ping_interval=30, ping_timeout=30
                    ) as websocket:
                        await websocket.send(json.dumps({
                            "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                            "params": [
                                {"mentions": [PUMP_FUN_PROGRAM_ID]},
                                {"commitment": "confirmed"},
                            ],
                        }))
                        reconnect_delay = 1.0
                        async for raw_message in websocket:
                            if self.stop_event.is_set():
                                break
                            message = json.loads(raw_message)
                            if message.get("method") == "logsNotification":
                                await self.handle_notification(db, message)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.reconnects += 1
                    logger.warning(
                        "WebSocket disconnected; reconnecting in %.0fs: %s", reconnect_delay, error
                    )
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=reconnect_delay)
                    except TimeoutError:
                        pass
                    reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)
        finally:
            stats_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stats_task
            await self.log_stats(db)
            await db.close()
            await self._http_client.aclose()
            self._http_client = None

    async def _stats_loop(self, db: aiosqlite.Connection) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=STATS_INTERVAL_S)
            except TimeoutError:
                await self.log_stats(db)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    websocket_url, http_rpc_url = helius_config()
    collector = RealtimeCollector(DATABASE_PATH, http_rpc_url)
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(shutdown_signal, collector.stop_event.set)
    await collector.run(websocket_url)


if __name__ == "__main__":
    asyncio.run(main())

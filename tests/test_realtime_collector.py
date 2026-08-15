"""Offline coverage for the standalone Pump.fun real-time collector."""

from __future__ import annotations

import asyncio
import base64
import sqlite3
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
from realtime_collector import (  # noqa: E402
    LAMPORTS_PER_SOL,
    PumpEvent,
    RealtimeCollector,
    _base58_encode,
    _discriminator,
    parse_logs,
)


def _string(value: str) -> bytes:
    encoded = value.encode()
    return struct.pack("<I", len(encoded)) + encoded


def _pubkey(value: int) -> bytes:
    return value.to_bytes(32, "little")


def _program_data(payload: bytes) -> str:
    return "Program data: " + base64.b64encode(payload).decode()


def test_parse_create_and_trade_anchor_events() -> None:
    create = (
        _discriminator("CreateEvent") + _string("Fresh Token") + _string("FRESH") + _string("uri")
        + _pubkey(1) + _pubkey(2) + _pubkey(3) + _pubkey(4) + struct.pack("<q", 1_700_000_000)
    )
    trade = (
        _discriminator("TradeEvent") + _pubkey(5) + struct.pack("<Q", 2 * LAMPORTS_PER_SOL)
        + struct.pack("<Q", 123) + b"\x01" + _pubkey(6) + struct.pack("<q", 1_700_000_001)
    )

    events = parse_logs([_program_data(create), _program_data(trade)])

    assert events[0].event_type == "create"
    assert events[0].name == "Fresh Token"
    assert events[0].mint == _base58_encode(_pubkey(1))
    assert events[1].event_type == "buy"
    assert events[1].sol_amount == 2.0
    assert events[1].token_amount == 123.0


def test_persist_ignores_duplicate_signature(tmp_path: Path) -> None:
    async def persist_twice() -> None:
        collector = RealtimeCollector(tmp_path / "realtime.db", "https://rpc.invalid")
        db = await collector.setup_database()
        event = PumpEvent("buy", "mint", timestamp_ms=int(time.time() * 1000), sol_amount=0.1)
        await collector.persist(db, event, "signature", int(time.time() * 1000), [])
        await collector.persist(db, event, "signature", int(time.time() * 1000), [])
        await db.close()

    asyncio.run(persist_twice())
    connection = sqlite3.connect(tmp_path / "realtime.db")
    assert connection.execute("SELECT COUNT(*) FROM token_trades").fetchone()[0] == 1
    connection.close()

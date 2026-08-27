"""Regression coverage for historical SQLite-to-Hive normalization."""

from __future__ import annotations

import asyncio
import importlib.util
import sqlite3
from pathlib import Path


def _migration_module():
    path = Path(__file__).parents[1] / "scripts" / "migrate_to_postgres.py"
    spec = importlib.util.spec_from_file_location("migrate_to_postgres", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_json_strips_nested_nul_characters() -> None:
    module = _migration_module()

    result = module._json('{"key\\u0000": ["before\\u0000after", {"nested": "\\u0000"}]}')

    assert result == {"key": ["beforeafter", {"nested": ""}]}
    assert module._strip_nuls({"ticker": "value\x00"}) == {"ticker": "value"}


def test_migration_retains_trade_without_position() -> None:
    module = _migration_module()
    sqlite = sqlite3.connect(":memory:")
    sqlite.row_factory = sqlite3.Row
    sqlite.execute(
        """
        CREATE TABLE positions (
            id TEXT, mint_address TEXT, mode TEXT, entry_trade_id TEXT, opened_at TEXT
        )
        """,
    )
    sqlite.execute(
        """
        CREATE TABLE trades (
            id TEXT, mint_address TEXT, mode TEXT, side TEXT, amount_sol REAL,
            token_amount REAL, price_sol REAL, slippage_bps INTEGER, tx_signature TEXT,
            executed_at TEXT, metadata_json TEXT
        )
        """,
    )
    sqlite.execute(
        "INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "legacy-trade",
            "mint",
            "paper",
            "buy",
            0.01,
            100.0,
            0.0001,
            0,
            None,
            "2026-01-01",
            "{}",
        ),
    )

    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        async def execute(self, _query: str, *args: object) -> None:
            self.calls.append(args)

    connection = Connection()
    count = asyncio.run(module._migrate_trades(sqlite, connection, {}))

    assert count == 1
    assert connection.calls[0][1] is None

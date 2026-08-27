"""PostgreSQL persistence boundary for the V2 services."""

from __future__ import annotations

import json
import os
from typing import Any
from uuid import uuid4

import asyncpg


def database_dsn() -> str:
    """Return the explicit Hive connection string required by V2 services."""

    dsn = os.getenv("MEMECOIN_POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("MEMECOIN_POSTGRES_DSN is required for Memecoin Trader V2")
    return dsn


async def _configure_connection(connection: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await connection.set_type_codec(
            type_name,
            schema="pg_catalog",
            encoder=json.dumps,
            decoder=json.loads,
            format="text",
        )


class MemecoinStore:
    """Small query layer shared by collector, strategy, and executor."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @classmethod
    async def connect(cls, dsn: str | None = None) -> MemecoinStore:
        pool = await asyncpg.create_pool(
            dsn or database_dsn(),
            min_size=1,
            max_size=int(os.getenv("MEMECOIN_PG_POOL_MAX", "5")),
            command_timeout=float(os.getenv("MEMECOIN_PG_COMMAND_TIMEOUT", "30")),
            init=_configure_connection,
        )
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        async with self._pool.acquire() as connection:
            return [dict(row) for row in await connection.fetch(query, *args)]

    async def insert_candidate(self, candidate: dict[str, Any]) -> int:
        columns = (
            "mint_address",
            "observed_at",
            "source",
            "age_seconds",
            "mcap_usd",
            "volume_usd",
            "txn_buys",
            "txn_sells",
            "buy_sell_ratio",
            "liquidity_usd",
            "fdv_usd",
            "price_sol",
            "price_usd",
            "pool_sol",
            "pool_type",
            "creator_holdings_pct",
            "unique_wallets",
            "price_change_5m",
            "price_change_1h",
            "strength_score",
            "raw_json",
        )
        values = [candidate.get(column) for column in columns]
        placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
        query = f"""
            INSERT INTO memecoin.candidates ({", ".join(columns)})
            VALUES ({placeholders})
            RETURNING id
        """
        async with self._pool.acquire() as connection:
            return int(await connection.fetchval(query, *values))

    async def list_open_positions(self, strategy: str, mode: str) -> list[dict[str, Any]]:
        return await self.fetch(
            """
            SELECT * FROM memecoin.positions
            WHERE strategy = $1 AND mode = $2 AND status = 'open'
            ORDER BY opened_at
            """,
            strategy,
            mode,
        )

    async def load_exit_config(self, strategy: str) -> dict[str, float]:
        rows = await self.fetch(
            """
            SELECT param_name, param_value
            FROM memecoin.exit_config
            WHERE strategy = $1
            """,
            strategy,
        )
        return {str(row["param_name"]): float(row["param_value"]) for row in rows}

    async def create_position(
        self,
        position: dict[str, Any],
        trade: dict[str, Any],
    ) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO memecoin.positions (
                        id, mint_address, entry_price_sol, amount_sol, token_amount,
                        peak_price_sol, trailing_armed, status, mode, strategy, opened_at,
                        candidate_id, fill_quality, tx_signature
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, 'open', $8, $9, $10, $11, $12, $13
                    )
                    """,
                    position["id"],
                    position["mint_address"],
                    position["entry_price_sol"],
                    position["amount_sol"],
                    position["token_amount"],
                    position["peak_price_sol"],
                    position["trailing_armed"],
                    position["mode"],
                    position["strategy"],
                    position["opened_at"],
                    position.get("candidate_id"),
                    position.get("fill_quality"),
                    position.get("tx_signature"),
                )
                await self._insert_trade(connection, trade, position["id"])

    async def update_position_mark(
        self,
        position_id: str,
        peak_price_sol: float,
        trailing_armed: bool,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE memecoin.positions
                SET peak_price_sol = GREATEST(COALESCE(peak_price_sol, 0), $2),
                    trailing_armed = trailing_armed OR $3
                WHERE id = $1 AND status = 'open'
                """,
                position_id,
                peak_price_sol,
                trailing_armed,
            )

    async def close_position(
        self,
        position: dict[str, Any],
        trade: dict[str, Any],
        *,
        close_price_sol: float,
        close_reason: str,
        realized_pnl_sol: float,
    ) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._insert_trade(connection, trade, str(position["id"]))
                await connection.execute(
                    """
                    UPDATE memecoin.positions
                    SET status = 'closed', closed_at = NOW(), close_reason = $2,
                        close_price_sol = $3, realized_pnl_sol = $4,
                        adjusted_pnl_sol = $4
                    WHERE id = $1 AND status = 'open'
                    """,
                    position["id"],
                    close_reason,
                    close_price_sol,
                    realized_pnl_sol,
                )

    async def record_runtime_event(
        self,
        event_type: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO memecoin.runtime_events (id, occurred_at, event_type, reason, details)
                VALUES ($1, NOW(), $2, $3, $4)
                """,
                str(uuid4()),
                event_type,
                reason,
                details or {},
            )

    async def refresh_daily_stats(self, strategy: str) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute("SELECT memecoin.refresh_daily_stats($1)", strategy)

    @staticmethod
    async def _insert_trade(
        connection: asyncpg.Connection,
        trade: dict[str, Any],
        position_id: str,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO memecoin.trades (
                id, position_id, mint_address, side, amount_sol, token_amount,
                price_sol, slippage_bps, tx_signature, mode, executed_at, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            trade["id"],
            position_id,
            trade["mint_address"],
            trade["side"],
            trade.get("amount_sol"),
            trade.get("token_amount"),
            trade.get("price_sol"),
            trade.get("slippage_bps"),
            trade.get("tx_signature"),
            trade.get("mode"),
            trade["executed_at"],
            trade.get("metadata") or {},
        )

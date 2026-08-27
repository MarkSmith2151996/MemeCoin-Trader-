"""V2 gate configuration and parameterized SQL query coverage."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from services.strategy import get_qualifying_candidates


class FakeStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.calls.append((query, args))
        if "gate_config" in query:
            return [
                {"gate_name": "mcap_floor", "gate_value": 5100},
                {"gate_name": "mcap_ceiling", "gate_value": 50000},
                {"gate_name": "min_age_seconds", "gate_value": 22},
                {"gate_name": "max_age_seconds", "gate_value": 1320},
                {"gate_name": "age_offset_seconds", "gate_value": 39},
                {"gate_name": "txn_count_adjustment", "gate_value": 1.24},
                {"gate_name": "min_volume_usd", "gate_value": 500},
                {"gate_name": "min_volume_to_mcap_ratio", "gate_value": 0.005},
                {"gate_name": "max_volume_to_mcap_ratio", "gate_value": 50},
                {"gate_name": "min_buy_sell_ratio", "gate_value": 0.5},
                {"gate_name": "min_pool_sol_bonding", "gate_value": 5},
                {"gate_name": "min_pool_sol_graduated", "gate_value": 5},
                {"gate_name": "creator_holdings_max", "gate_value": 0},
                {"gate_name": "max_top_holder_pct", "gate_value": 100},
                {"gate_name": "score_threshold_bonding", "gate_value": 40},
                {"gate_name": "score_threshold_graduated", "gate_value": 40},
                {"gate_name": "blocked_weekdays", "gate_value": [2]},
                {"gate_name": "blocked_hours_utc", "gate_value": [0, 19, 20, 21]},
                {"gate_name": "max_open", "gate_value": 5},
            ]
        return [{"id": 7, "mint_address": "qualifying-mint"}]


def test_strategy_query_uses_database_gates_and_bound_values() -> None:
    async def run() -> None:
        store = FakeStore()
        now = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        candidates = await get_qualifying_candidates(store, "BT", 60, now=now)

        assert candidates == [{"id": 7, "mint_address": "qualifying-mint"}]
        query, args = store.calls[-1]
        assert "memecoin.candidates" in query
        assert "memecoin.gate_config" not in query
        assert "NOT EXISTS" in query
        assert "close_reason = 'hard_stop'" in query
        assert "INTERVAL '24 hours'" in query
        assert "ROW_NUMBER() OVER" in query
        assert "c.mcap_usd <= $4" in query
        assert "c.mint_authority_revoked IS TRUE" in query
        assert "c.freeze_authority_revoked IS TRUE" in query
        assert "c.top_holder_pct IS NOT NULL" in query
        assert "c.price_sol > 0" in query
        assert "ORDER BY c.strength_score DESC" in query
        assert "$1::timestamptz" in query
        assert args[0] == now
        assert args[2:18] == (
            5100.0,
            50000.0,
            22.0,
            1320.0,
            39.0,
            500.0,
            0.5,
            5.0,
            5.0,
            40.0,
            40.0,
            0.0,
            1.24,
            0.005,
            50.0,
            100.0,
        )
        assert args[18:20] == ([2], [0, 19, 20, 21])
        assert args[-1] == 5

    asyncio.run(run())

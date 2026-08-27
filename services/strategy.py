"""Strategy-as-query entry selection over persisted candidate observations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol


class QueryStore(Protocol):
    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class GateConfig:
    strategy: str
    values: dict[str, Any]

    def number(self, name: str) -> float:
        value = self.values.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RuntimeError(f"Gate {self.strategy}.{name} must be numeric")
        return float(value)

    def integers(self, name: str) -> list[int]:
        value = self.values.get(name, [])
        if not isinstance(value, list) or any(isinstance(item, bool) for item in value):
            raise RuntimeError(f"Gate {self.strategy}.{name} must be an integer list")
        try:
            return [int(item) for item in value]
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Gate {self.strategy}.{name} must be an integer list") from exc


async def load_gates(store: QueryStore, strategy: str = "BT") -> GateConfig:
    """Load enabled gate rows for one strategy without any hard-coded thresholds."""

    rows = await store.fetch(
        """
        SELECT gate_name, gate_value
        FROM memecoin.gate_config
        WHERE strategy = $1 AND enabled = TRUE
        """,
        strategy,
    )
    values = {str(row["gate_name"]): row["gate_value"] for row in rows}
    required = {
        "mcap_floor",
        "min_age_seconds",
        "max_age_seconds",
        "min_volume_usd",
        "min_buy_sell_ratio",
        "min_pool_sol_bonding",
        "min_pool_sol_graduated",
        "creator_holdings_max",
        "score_threshold_bonding",
        "blocked_weekdays",
        "blocked_hours_utc",
        "max_open",
    }
    missing = sorted(required - values.keys())
    if missing:
        raise RuntimeError(f"Missing enabled gates for {strategy}: {', '.join(missing)}")
    return GateConfig(strategy=strategy, values=values)


async def get_qualifying_candidates(
    store: QueryStore,
    strategy: str = "BT",
    since_seconds: float = 60,
    *,
    mode: str = "paper",
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return newest candidate per mint that passes the persisted strategy gates."""

    if since_seconds <= 0:
        raise ValueError("since_seconds must be positive")
    gates = await load_gates(store, strategy)
    observed_at = now or datetime.now(UTC)
    return await store.fetch(
        """
        SELECT DISTINCT ON (c.mint_address) c.*
        FROM memecoin.candidates AS c
        WHERE c.observed_at >= $1::timestamptz - ($2::double precision * INTERVAL '1 second')
          AND c.mcap_usd >= $3
          AND c.age_seconds >= $4
          AND c.age_seconds <= $5
          AND c.volume_usd >= $6
          AND c.buy_sell_ratio >= $7
          AND (
              (c.pool_type = 'bonding' AND c.pool_sol >= $8)
              OR (c.pool_type = 'graduated' AND c.pool_sol >= $9)
          )
          AND c.strength_score >= $10
          AND (c.creator_holdings_pct IS NULL OR c.creator_holdings_pct <= $11)
          AND (EXTRACT(ISODOW FROM $1::timestamptz)::integer - 1) <> ALL($12::integer[])
          AND EXTRACT(HOUR FROM $1::timestamptz)::integer <> ALL($13::integer[])
          AND NOT EXISTS (
              SELECT 1
              FROM memecoin.positions AS p
              WHERE p.mint_address = c.mint_address AND p.status = 'open'
          )
          AND (
              SELECT COUNT(*)
              FROM memecoin.positions AS p
              WHERE p.status = 'open' AND p.strategy = $14 AND p.mode = $15
          ) < $16
        ORDER BY c.mint_address, c.observed_at DESC
        """,
        observed_at,
        since_seconds,
        gates.number("mcap_floor"),
        gates.number("min_age_seconds"),
        gates.number("max_age_seconds"),
        gates.number("min_volume_usd"),
        gates.number("min_buy_sell_ratio"),
        gates.number("min_pool_sol_bonding"),
        gates.number("min_pool_sol_graduated"),
        gates.number("score_threshold_bonding"),
        gates.number("creator_holdings_max"),
        gates.integers("blocked_weekdays"),
        gates.integers("blocked_hours_utc"),
        strategy,
        mode,
        int(gates.number("max_open")),
    )

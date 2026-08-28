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
        "mcap_ceiling",
        "min_age_seconds",
        "max_age_seconds",
        "age_offset_seconds",
        "txn_count_adjustment",
        "min_volume_usd",
        "min_volume_to_mcap_ratio",
        "max_volume_to_mcap_ratio",
        "min_buy_sell_ratio",
        "min_pool_sol_bonding",
        "min_pool_sol_graduated",
        "creator_holdings_max",
        "max_top_holder_pct",
        "score_threshold_bonding",
        "score_threshold_graduated",
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
        WITH newest AS (
            SELECT c.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY c.mint_address
                       ORDER BY c.observed_at DESC, c.id DESC
                   ) AS observation_rank
            FROM memecoin.candidates AS c
            WHERE c.observed_at >= $1::timestamptz
                - ($2::double precision * INTERVAL '1 second')
              AND c.source LIKE 'jupiter_%'
        )
        SELECT c.*
        FROM newest AS c
        WHERE c.observation_rank = 1
          AND c.mcap_usd >= $3
          AND c.mcap_usd <= $4
          AND c.age_seconds >= $5
          AND c.corrected_age_seconds <= $6
          AND c.corrected_age_seconds >= c.age_seconds + $7 - 0.001
          AND c.volume_usd >= $8
          AND c.buy_sell_ratio >= $9
          AND (
              (c.pool_type = 'bonding' AND c.pool_sol >= $10 AND c.strength_score >= $12)
              OR (
                  c.pool_type = 'graduated'
                  AND c.pool_sol >= $11
                  AND c.strength_score >= $13
              )
          )
          AND (c.creator_holdings_pct IS NULL OR c.creator_holdings_pct <= $14)
          AND (c.txn_buys + c.txn_sells) * $15 >= CASE
              WHEN c.corrected_age_seconds < 60 THEN 3
              WHEN c.corrected_age_seconds < 180 THEN 5
              WHEN c.corrected_age_seconds < 300 THEN 8
              WHEN c.corrected_age_seconds < 600 THEN 12
              ELSE 16
          END
          AND c.volume_usd / NULLIF(c.mcap_usd, 0) BETWEEN $16 AND $17
          AND c.mint_authority_revoked IS TRUE
          AND c.freeze_authority_revoked IS TRUE
          AND c.top_holder_pct IS NOT NULL
          AND c.top_holder_pct <= $18
          AND c.price_sol IS NOT NULL
          AND c.price_sol > 0
          AND (EXTRACT(ISODOW FROM $1::timestamptz)::integer - 1) <> ALL($19::integer[])
          AND EXTRACT(HOUR FROM $1::timestamptz)::integer <> ALL($20::integer[])
          AND NOT EXISTS (
              SELECT 1
              FROM memecoin.positions AS p
               WHERE p.mint_address = c.mint_address
                 AND p.status IN ('open', 'quarantined')
          )
          AND c.mint_address NOT IN (
              SELECT p.mint_address
              FROM memecoin.positions AS p
              WHERE p.status = 'closed'
                AND p.close_reason = 'hard_stop'
                AND p.closed_at > NOW() - INTERVAL '24 hours'
          )
          AND (
              SELECT COUNT(*)
              FROM memecoin.positions AS p
              WHERE p.status = 'open' AND p.strategy = $21 AND p.mode = $22
          ) < $23
        ORDER BY c.strength_score DESC, c.observed_at DESC, c.id DESC
        """,
        observed_at,
        since_seconds,
        gates.number("mcap_floor"),
        gates.number("mcap_ceiling"),
        gates.number("min_age_seconds"),
        gates.number("max_age_seconds"),
        gates.number("age_offset_seconds"),
        gates.number("min_volume_usd"),
        gates.number("min_buy_sell_ratio"),
        gates.number("min_pool_sol_bonding"),
        gates.number("min_pool_sol_graduated"),
        gates.number("score_threshold_bonding"),
        gates.number("score_threshold_graduated"),
        gates.number("creator_holdings_max"),
        gates.number("txn_count_adjustment"),
        gates.number("min_volume_to_mcap_ratio"),
        gates.number("max_volume_to_mcap_ratio"),
        gates.number("max_top_holder_pct"),
        gates.integers("blocked_weekdays"),
        gates.integers("blocked_hours_utc"),
        strategy,
        mode,
        int(gates.number("max_open")),
    )

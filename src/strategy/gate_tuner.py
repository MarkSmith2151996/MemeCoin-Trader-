"""Strategy gate persistence and bounded, data-driven threshold tuning."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite


@dataclass
class GateThresholds:
    """Mutable Strategy B entry thresholds updated without restarting the loop."""

    max_age_minutes: float = 30.0
    min_mcap_usd: float = 2_000.0
    min_volume_usd: float = 200.0
    min_buy_sell_ratio: float = 0.4

    def as_dict(self) -> dict[str, float]:
        return asdict(self)


class GateTuner:
    """Tune minimum gates from winning entries after bounded sample intervals."""

    def __init__(
        self,
        db_path: str | Path,
        thresholds: GateThresholds,
        *,
        strategy: str = "B",
        min_closed_trades: int = 50,
        tune_interval: int = 50,
        max_adjustment: float = 0.25,
    ) -> None:
        self.db_path = Path(db_path)
        self.thresholds = thresholds
        self.strategy = strategy
        self.min_closed_trades = min_closed_trades
        self.tune_interval = tune_interval
        self.max_adjustment = max_adjustment

    async def ensure_initial_config(self) -> None:
        """Record the live starting thresholds once per strategy database."""

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM gate_config WHERE strategy = ? LIMIT 1", (self.strategy,),
            )
            exists = await cursor.fetchone()
            await cursor.close()
            if exists is None:
                await db.execute(
                    """INSERT INTO gate_config
                       (strategy, updated_at, config_json, reason, sample_size, metrics_json)
                       VALUES (?, ?, ?, 'initial', 0, '{}')""",
                    (self.strategy, datetime.now(UTC).isoformat(), json.dumps(self.thresholds.as_dict())),
                )
                await db.commit()

    async def maybe_tune(self) -> bool:
        """Apply one bounded 20th-percentile update when a new closed-trade block exists."""

        async with aiosqlite.connect(self.db_path) as db:
            closed = await self._closed_trade_count(db)
            if closed < self.min_closed_trades:
                return False
            cursor = await db.execute(
                """SELECT COALESCE(MAX(sample_size), 0) FROM gate_config
                   WHERE strategy = ? AND reason = 'auto_tuned'""",
                (self.strategy,),
            )
            last_tuned = int((await cursor.fetchone())[0])
            await cursor.close()
            if closed - last_tuned < self.tune_interval:
                return False

            cursor = await db.execute(
                """SELECT c.age_minutes, c.mcap_usd, c.volume_usd, c.buy_sell_ratio
                   FROM candidate_log c
                   JOIN positions p ON p.id = c.position_id
                   WHERE c.strategy = ? AND c.entered = TRUE
                     AND p.strategy = ? AND p.status = 'CLOSED'
                     AND p.realized_pnl_sol > 0""",
                (self.strategy, self.strategy),
            )
            winners = await cursor.fetchall()
            await cursor.close()
            if not winners:
                return False

            proposed = self._proposed_thresholds(winners)
            before = self.thresholds.as_dict()
            for name, value in proposed.items():
                setattr(self.thresholds, name, value)
            metrics = {
                "closed_trades": closed,
                "winning_entries": len(winners),
                "previous_config": before,
                "percentile": 20,
            }
            await db.execute(
                """INSERT INTO gate_config
                   (strategy, updated_at, config_json, reason, sample_size, metrics_json)
                   VALUES (?, ?, ?, 'auto_tuned', ?, ?)""",
                (
                    self.strategy, datetime.now(UTC).isoformat(),
                    json.dumps(self.thresholds.as_dict()), closed, json.dumps(metrics),
                ),
            )
            await db.commit()
        return True

    async def _closed_trade_count(self, db: aiosqlite.Connection) -> int:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM positions WHERE strategy = ? AND status = 'CLOSED'",
            (self.strategy,),
        )
        result = int((await cursor.fetchone())[0])
        await cursor.close()
        return result

    def _proposed_thresholds(self, winners: list[tuple[object, ...]]) -> dict[str, float]:
        columns = {
            "max_age_minutes": 0,
            "min_mcap_usd": 1,
            "min_volume_usd": 2,
            "min_buy_sell_ratio": 3,
        }
        current = self.thresholds.as_dict()
        proposed: dict[str, float] = {}
        for name, index in columns.items():
            values = [float(row[index]) for row in winners if _finite_positive(row[index])]
            if not values:
                proposed[name] = current[name]
                continue
            target = _percentile(values, 20)
            lower = current[name] * (1 - self.max_adjustment)
            upper = current[name] * (1 + self.max_adjustment)
            proposed[name] = round(min(max(target, lower), upper), 6)
        return proposed


def _finite_positive(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile / 100
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)

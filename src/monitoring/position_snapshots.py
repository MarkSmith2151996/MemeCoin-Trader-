"""Non-blocking open-position price snapshot collection."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from pathlib import Path

from src.core.database import (
    prune_position_price_snapshots,
    record_position_price_snapshot,
)


SNAPSHOT_INTERVAL_S = 10
SNAPSHOT_RETENTION_DAYS = 7
log = logging.getLogger(__name__)


async def snapshot_open_positions(manager, mark_provider, db_path: Path) -> int:
    """Capture valid marks for the manager's open paper positions."""

    positions = await manager.get_all_open(mode="paper")
    prices = await asyncio.gather(
        *(mark_provider.get_current_price(position.mint_address) for position in positions),
        return_exceptions=True,
    )
    recorded = 0
    for position, result in zip(positions, prices, strict=True):
        if isinstance(result, Exception):
            log.debug("Snapshot price failed for %s: %s", position.mint_address[:16], result)
            continue
        if not isinstance(result, (int, float)) or not math.isfinite(result) or result <= 0:
            continue
        try:
            await record_position_price_snapshot(
                db_path,
                position_id=position.id,
                mint_address=position.mint_address,
                price_sol=float(result),
            )
            recorded += 1
        except Exception as exc:
            log.warning("Snapshot write failed for %s: %s", position.mint_address[:16], exc)
    await prune_position_price_snapshots(db_path, retention_days=SNAPSHOT_RETENTION_DAYS)
    return recorded


async def snapshot_loop(manager, mark_provider, db_path: Path) -> None:
    """Collect open-position marks every ten seconds independently of trading loops."""

    while True:
        cycle_start = time.monotonic()
        # A snapshot failure must never take down the trading runtime: the loop is
        # gathered with the strategy's main loop, so any uncaught exception here
        # would kill the whole process. Log and continue instead.
        try:
            await snapshot_open_positions(manager, mark_provider, db_path)
        except Exception as exc:  # noqa: BLE001 - isolation boundary for a background worker
            log.warning("Snapshot cycle failed (continuing): %s", exc)
        elapsed = time.monotonic() - cycle_start
        await asyncio.sleep(max(0.0, SNAPSHOT_INTERVAL_S - elapsed))

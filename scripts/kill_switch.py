#!/usr/bin/env python3
"""Kill switch (MT-546): halt live trading and liquidate every open position.

Run:
    python3 scripts/kill_switch.py

What it does, in order:
  1. Refuses to act unless EXECUTION_MODE in .env is ``live`` (live-mode only).
  2. Writes EXECUTION_MODE=paper into .env so the watchdog restarts Strategy B
     in paper mode — no new live trades fire after any restart.
  3. Trips the circuit breaker flag, so the currently running live process
     stops buying immediately (buys check the same flag).
  4. Sells every open live position through Jupiter at market with a wider
     slippage tolerance (500 bps), recording each fill and closing the
     position. Every sell attempt and result is logged to /tmp/kill_switch.log.

Can also be triggered from the Telegram bot ("kill switch") when it is running.
Sell failures never trip additional state — the breaker is already tripped by
the kill switch itself.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from src.core.config import load_settings  # noqa: E402
from src.core.database import init_db  # noqa: E402
from src.execution.live import LiveExecutionAdapter  # noqa: E402
from src.execution.safety_controls import (  # noqa: E402
    CircuitBreaker,
    KillSwitch,
    KillSwitchNotArmedError,
    read_execution_mode,
)
from src.strategy.position_manager import PositionManager  # noqa: E402

ENV_PATH = ROOT / ".env"
DB_PATH = ROOT / "data" / "trades.db"
BREAKER_PATH = ROOT / "data" / "circuit_breaker.json"

# The kill switch must be able to exit even on illiquid tokens: bump the
# price-impact gate to 100% (buys are blocked anyway once the breaker trips).
KILL_SWITCH_MAX_PRICE_IMPACT_PCT = 100.0


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("/tmp/kill_switch.log"),
        ],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def format_summary(summary) -> str:
    lines = [
        "KILL SWITCH EXECUTED",
        f"EXECUTION_MODE: {summary.mode_before} → {summary.mode_after}",
        f"Circuit breaker: {'tripped' if summary.breaker_tripped else 'NOT TRIPPED'}",
        f"Open live positions: {summary.positions_found}",
        f"Sold: {summary.sold}  Failed: {summary.failed}",
    ]
    if summary.details:
        lines.append("")
        lines.append("Per-position:")
        for detail in summary.details:
            lines.append(f"  {detail}")
    if summary.failed:
        lines.append("")
        lines.append(
            "Failed sells leave tokens in the wallet — re-run after investigating, "
            "or reset the breaker once you have verified wallet state.",
        )
    return "\n".join(lines)


async def main() -> int:
    parser = argparse.ArgumentParser(description="Halt live trading and liquidate all positions")
    args = parser.parse_args()
    del args

    setup_logging()
    load_dotenv(ENV_PATH)

    mode_before = read_execution_mode(ENV_PATH)
    print(f"Kill switch: EXECUTION_MODE is '{mode_before}'")
    if mode_before != "live":
        print("Nothing to do — kill switch is live-mode only (paper mode ignores it).")
        return 0

    breaker = CircuitBreaker(flag_path=BREAKER_PATH)
    adapter = LiveExecutionAdapter(max_price_impact_pct=KILL_SWITCH_MAX_PRICE_IMPACT_PCT)
    kill_switch = KillSwitch(
        env_path=ENV_PATH,
        breaker=breaker,
        adapter=adapter,
    )

    await init_db(DB_PATH)
    settings = load_settings(ROOT / "config" / "settings.yaml")
    manager = PositionManager(DB_PATH, settings, strategy="B")
    positions = await manager.get_all_open(mode="live")

    try:
        summary = await kill_switch.run(
            positions=positions,
            db_path=DB_PATH,
            manager=manager,
        )
    except KillSwitchNotArmedError as exc:
        print(f"Kill switch aborted: {exc}")
        return 0
    finally:
        await adapter.close()

    print()
    print(format_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

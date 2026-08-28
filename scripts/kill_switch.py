#!/usr/bin/env python3
"""V2 kill switch: halt live trading and liquidate every Hive live position.

Run:
    python3 scripts/kill_switch.py

What it does, in order:
  1. Refuses to act unless EXECUTION_MODE in .env is ``live`` (live-mode only).
   2. Sets LIVE_KILL_SWITCH=true and EXECUTION_MODE=paper in .env so restarts
      cannot re-arm live entries.
   3. Trips the shared permanent circuit breaker and writes the executor halt
      marker before any sell attempt.
   4. Queries memecoin.positions in Hive for every open live position, sells
      through the V2 direct-Pump/Jupiter router, verifies token clearance, and
      closes the Hive position with the confirmed fill.

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

from services.adapters.live import build_live_adapter  # noqa: E402
from services.store import MemecoinStore  # noqa: E402
from src.execution.safety_controls import (  # noqa: E402
    CircuitBreaker,
    KillSwitchNotArmedError,
    V2KillSwitch,
    read_execution_mode,
)
from src.monitoring.alerts import AlertManager  # noqa: E402

ENV_PATH = ROOT / ".env"


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

    store = await MemecoinStore.connect()
    adapter = build_live_adapter()
    kill_switch = V2KillSwitch(
        env_path=ENV_PATH,
        breaker=CircuitBreaker(),
        store=store,
        adapter=adapter,
        alert_manager=AlertManager.from_env(),
    )

    try:
        summary = await kill_switch.run()
    except KillSwitchNotArmedError as exc:
        print(f"Kill switch aborted: {exc}")
        return 0
    finally:
        await adapter.close()
        await store.close()

    print()
    print(format_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

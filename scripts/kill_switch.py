#!/usr/bin/env python3
"""V2 kill switch: halt live trading and liquidate every Hive live position.

Run:
    python3 scripts/kill_switch.py

What it does, in order:
   1. Stops the V2 executor service (or proves its singleton lock is free)
      before any sell can race the runtime.
   2. Detects unsettled live Hive rows and live wallet holdings regardless of
      the current EXECUTION_MODE, so an incomplete prior kill can be re-run.
   3. Sets LIVE_KILL_SWITCH=true and EXECUTION_MODE=paper in .env so restarts
      cannot re-arm live entries.
   4. Trips the shared permanent circuit breaker and writes the executor halt
      marker before any sell attempt.
   5. Queries memecoin.positions in Hive for every unsettled live position, sells
      through the V2 direct-Pump/Jupiter router, verifies token clearance, and
      closes the Hive position with the confirmed fill.

Can also be triggered from the Telegram bot ("kill switch") when it is running.
Sell failures never trip additional state — the breaker is already tripped by
the kill switch itself.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import logging
import shutil
import subprocess
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
EXECUTOR_UNIT = "memecoin-executor.service"
EXECUTOR_LOCK_PATH = Path("/tmp/memecoin_executor.lock")
log = logging.getLogger("kill_switch")


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
    parser.add_argument(
        "--force",
        action="store_true",
        help="allow liquidation when systemctl is unavailable and the executor lock is busy",
    )
    args = parser.parse_args()

    setup_logging()
    load_dotenv(ENV_PATH)

    mode_before = read_execution_mode(ENV_PATH)
    print(f"Kill switch: EXECUTION_MODE is '{mode_before}'")
    try:
        ensure_executor_stopped(force=args.force)
    except RuntimeError as exc:
        print(f"Kill switch aborted: {exc}")
        return 2

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


def ensure_executor_stopped(*, force: bool) -> None:
    """Stop the V2 service, or require explicit acknowledgement of a lock race."""

    systemctl = shutil.which("systemctl")
    if systemctl is not None:
        try:
            stopped = subprocess.run(
                ["sudo", "-n", systemctl, "stop", EXECUTOR_UNIT],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            active = subprocess.run(
                ["sudo", "-n", systemctl, "is-active", "--quiet", EXECUTOR_UNIT],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            log.warning("systemctl unavailable for kill switch: %s", exc)
        else:
            if stopped.returncode == 0 and active.returncode != 0:
                log.info("confirmed %s is stopped before liquidation", EXECUTOR_UNIT)
                return
            detail = (
                stopped.stderr or stopped.stdout or "systemctl could not stop executor"
            ).strip()
            if _systemctl_unavailable(detail):
                log.warning("systemctl unavailable for kill switch: %s", detail)
            else:
                raise RuntimeError(
                    "could not confirm "
                    f"{EXECUTOR_UNIT} is stopped with sudo -n: {detail or 'still active'}",
                )

    if _singleton_lock_is_free():
        log.warning("systemctl unavailable; V2 singleton lock is free before liquidation")
        return
    if force:
        log.critical("--force accepted while executor singleton lock is busy")
        return
    raise RuntimeError(
        "systemctl is unavailable and the V2 executor singleton lock is busy; "
        "refusing concurrent liquidation (use --force only after manual verification)",
    )


def _singleton_lock_is_free() -> bool:
    handle = EXECUTOR_LOCK_PATH.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return False
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()
    return True


def _systemctl_unavailable(detail: str) -> bool:
    normalized = detail.lower()
    return "not been booted" in normalized or "failed to connect to bus" in normalized


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

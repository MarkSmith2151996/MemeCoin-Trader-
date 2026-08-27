"""Paper-only integration verification for Strategy B's PumpPortal fail-closed path."""

from __future__ import annotations

import asyncio
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_strategy_b  # noqa: E402
from src.core.config import load_settings  # noqa: E402
from src.core.database import init_db  # noqa: E402
from src.core.models import Side, Trade  # noqa: E402
from src.execution import pumpportal_price  # noqa: E402
from src.strategy.position_manager import PositionManager  # noqa: E402

DISCONNECT_AFTER_S = 5.0
STALE_AFTER_S = 15.0


class _SilentSocket:
    def __init__(self, connection_number: int, feed: _MockPumpPortal) -> None:
        self._connection_number = connection_number
        self._feed = feed

    async def send(self, _message: str) -> None:
        if self._connection_number == 1:
            self._feed.schedule_disconnect()

    async def recv(self) -> str:
        if self._connection_number == 1:
            await self._feed.disconnect_ready.wait()
            raise ConnectionError("mock PumpPortal disconnect")
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _MockConnection:
    def __init__(self, socket: _SilentSocket) -> None:
        self._socket = socket

    async def __aenter__(self) -> _SilentSocket:
        return self._socket

    async def __aexit__(self, *_args: object) -> None:
        return None


class _MockPumpPortal:
    def __init__(self) -> None:
        self.connections = 0
        self.disconnected_at: float | None = None
        self.disconnect_ready = asyncio.Event()
        self._disconnect_scheduled = False

    def schedule_disconnect(self) -> None:
        if self._disconnect_scheduled:
            return
        self._disconnect_scheduled = True
        asyncio.create_task(self._disconnect_after_five_seconds())

    async def _disconnect_after_five_seconds(self) -> None:
        await asyncio.sleep(DISCONNECT_AFTER_S)
        self.disconnected_at = time.monotonic()
        self.disconnect_ready.set()

    def connect(self, *_args: object, **_kwargs: object) -> _MockConnection:
        self.connections += 1
        return _MockConnection(_SilentSocket(self.connections, self))


class _CapturingAlerts:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str]] = []

    async def send(self, level: str, title: str, message: str) -> None:
        self.messages.append((level, title, message))


async def _wait_forever(*_args: object, **_kwargs: object) -> None:
    await asyncio.Event().wait()


async def verify_fail_closed() -> None:
    with tempfile.TemporaryDirectory(prefix="strategy-b-fail-closed-") as directory:
        db_path = Path(directory) / "trades.db"
        halt_path = Path(directory) / "strategy_b_halted"
        await init_db(db_path)
        manager = PositionManager(db_path, load_settings(), strategy="B")
        await manager.open_position(
            Trade(
                mint_address="stale-mint",
                side=Side.BUY,
                amount_sol=0.01,
                token_amount=100.0,
                price_sol=0.0001,
                mode="paper",
            ),
            None,
        )

        mock_pumpportal = _MockPumpPortal()
        alerts = _CapturingAlerts()
        original_connect = pumpportal_price.websockets.connect
        original_scan_loop = run_strategy_b.scan_loop
        original_monitor_loop = run_strategy_b.monitor_loop
        original_snapshot_loop = run_strategy_b.snapshot_loop
        original_priority_fee_loop = run_strategy_b.priority_fee_loop
        original_halt_path = run_strategy_b.STRATEGY_B_HALT_PATH
        original_from_env = run_strategy_b.AlertManager.__dict__["from_env"]
        pumpportal_price.websockets.connect = mock_pumpportal.connect
        run_strategy_b.scan_loop = _wait_forever
        run_strategy_b.monitor_loop = _wait_forever
        run_strategy_b.snapshot_loop = _wait_forever
        run_strategy_b.priority_fee_loop = _wait_forever
        run_strategy_b.STRATEGY_B_HALT_PATH = halt_path
        run_strategy_b.AlertManager.from_env = classmethod(lambda _cls: alerts)
        started_at = time.monotonic()

        try:
            await asyncio.wait_for(
                run_strategy_b._run_runtime_until_stopped(
                    manager,
                    object(),
                    type("PaperAdapter", (), {"mode": "paper"})(),
                    db_path,
                    object(),
                    [],
                    0.0,
                ),
                timeout=25.0,
            )
        finally:
            pumpportal_price.websockets.connect = original_connect
            run_strategy_b.scan_loop = original_scan_loop
            run_strategy_b.monitor_loop = original_monitor_loop
            run_strategy_b.snapshot_loop = original_snapshot_loop
            run_strategy_b.priority_fee_loop = original_priority_fee_loop
            run_strategy_b.STRATEGY_B_HALT_PATH = original_halt_path
            run_strategy_b.AlertManager.from_env = original_from_env

        assert mock_pumpportal.disconnected_at is not None
        assert mock_pumpportal.disconnected_at - started_at >= DISCONNECT_AFTER_S
        assert time.monotonic() - started_at >= STALE_AFTER_S
        assert mock_pumpportal.connections >= 2
        assert await manager.get_all_open(mode="paper") == []
        assert len(alerts.messages) == 1
        assert alerts.messages[0][:2] == ("critical", "Strategy B emergency close")
        assert alerts.messages[0][2].startswith(
            "Reason: PumpPortal stale 15s\nPositions: stale-mint=0.0001 (closed)\nHalt: ",
        )
        assert halt_path.exists()
        with sqlite3.connect(db_path) as connection:
            event = connection.execute(
                "SELECT event_type, reason FROM runtime_events",
            ).fetchone()
        assert event == ("emergency_close_all", "PumpPortal stale 15s")


def main() -> None:
    asyncio.run(verify_fail_closed())
    print(
        "PASS: PumpPortal disconnected after 5s; the 15s stale grace triggered "
        "a paper emergency close, runtime event, Telegram attempt, and clean exit.",
    )


if __name__ == "__main__":
    main()

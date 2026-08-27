"""MT-633/MT-635 coverage for exit persistence, RPC selection, and price streaming."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import sqlite3
from pathlib import Path

from solders.keypair import Keypair

from scripts import health_monitor, run_strategy_b
from src.chain.jupiter_swap import JupiterSwapClient
from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import Position, PositionStatus, Trade
from src.execution import pumpportal_price
from src.execution.position_reconciliation import reconcile_positions
from src.execution.pumpportal_price import PumpPortalPriceFeed, _parse_price_update
from src.strategy.position_manager import PositionManager


def test_reconciliation_skips_sell_being_persisted(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "trades.db"
        await init_db(db_path)
        manager = PositionManager(db_path, load_settings(), strategy="B")
        position = Position(
            mint_address="mint-in-flight",
            entry_trade_id="entry",
            amount_sol=0.01,
            token_amount=100.0,
            entry_price_sol=0.0001,
            mode="live",
        )
        await manager.open_position(
            Trade(
                mint_address=position.mint_address,
                side="BUY",
                amount_sol=0.01,
                token_amount=100.0,
                price_sol=0.0001,
                mode="live",
            ),
            None,
        )

        report = await reconcile_positions(
            manager,
            lambda: _empty_holdings(),
            skip_mints={position.mint_address},
        )
        assert report.ok
        assert report.mismatches == ()

    asyncio.run(run())


async def _empty_holdings() -> dict[str, float]:
    return {}


def test_shutdown_waits_for_active_sell_persistence() -> None:
    async def run() -> None:
        run_strategy_b._selling_in_progress.add("mint-in-flight")

        async def finish_persisting() -> None:
            await asyncio.sleep(0.01)
            run_strategy_b._selling_in_progress.clear()

        await asyncio.gather(run_strategy_b._wait_for_inflight_sells(0.5), finish_persisting())

    asyncio.run(run())


def test_shutdown_blocks_new_entries_before_any_adapter_work() -> None:
    async def run() -> None:
        run_strategy_b._shutting_down = True
        try:
            result = await run_strategy_b.try_enter(
                "mint",
                "TST",
                None,
                None,
                None,
                Path("unused.db"),
            )
        finally:
            run_strategy_b._shutting_down = False
        assert result is None

    asyncio.run(run())


def test_single_trade_mode_is_part_of_the_scan_runtime() -> None:
    assert "single_trade_complete" in inspect.signature(run_strategy_b.scan_loop).parameters
    assert "--single-trade" in Path(run_strategy_b.__file__).read_text()


def test_positions_mode_migration_backfills_json_and_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    connection = sqlite3.connect(db_path)
    connection.execute(
        """CREATE TABLE positions (
            id TEXT PRIMARY KEY, mint_address TEXT, entry_trade_id TEXT, amount_sol REAL,
            token_amount REAL, entry_price_sol REAL, status TEXT, opened_at TEXT,
            closed_at TEXT, realized_pnl_sol REAL, partial_exits_json TEXT
        )""",
    )
    connection.execute(
        "INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "legacy",
            "mint",
            "entry",
            0.01,
            100,
            0.0001,
            PositionStatus.OPEN.value,
            "2026-01-01T00:00:00+00:00",
            None,
            0,
            '{"mode":"live"}',
        ),
    )
    connection.commit()
    connection.close()

    asyncio.run(init_db(db_path))
    connection = sqlite3.connect(db_path)
    mode = connection.execute("SELECT mode FROM positions WHERE id = 'legacy'").fetchone()[0]
    indexes = {row[1] for row in connection.execute("PRAGMA index_list(positions)")}
    connection.close()
    assert mode == "live"
    assert "idx_positions_mode_status" in indexes


def test_pumpportal_trade_updates_produce_safe_prices() -> None:
    assert _parse_price_update('{"mint":"mint","priceSol":0.00001}') == ("mint", 0.00001)
    assert _parse_price_update(
        '{"mint":"mint","solAmount":2,"tokenAmount":1000}',
    ) == ("mint", 0.002)
    assert _parse_price_update('{"mint":"mint","tokenAmount":0}') is None


def test_pumpportal_reconnects_after_disconnect(monkeypatch) -> None:
    async def run() -> None:
        connection_attempts = 0

        class DisconnectingSocket:
            async def send(self, _message: str) -> None:
                pass

            async def recv(self) -> str:
                raise ConnectionError("simulated disconnect")

        class Connection:
            async def __aenter__(self) -> DisconnectingSocket:
                return DisconnectingSocket()

            async def __aexit__(self, *_args) -> None:
                pass

        def connect(*_args, **_kwargs) -> Connection:
            nonlocal connection_attempts
            connection_attempts += 1
            return Connection()

        async def held_mints() -> set[str]:
            return {"mint"}

        async def on_price(_mint: str, _price: float) -> None:
            pass

        monkeypatch.setattr(pumpportal_price.websockets, "connect", connect)
        feed = PumpPortalPriceFeed(held_mints, on_price, reconnect_delay_s=0.001)
        task = asyncio.create_task(feed.run())
        try:
            for _ in range(100):
                if connection_attempts >= 2:
                    break
                await asyncio.sleep(0.005)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert connection_attempts >= 2

    asyncio.run(run())


def test_pumpportal_stale_price_activates_one_jupiter_fallback() -> None:
    async def run() -> None:
        stale_mints: list[str] = []
        stale_activated = asyncio.Event()

        class SilentSocket:
            def __init__(self) -> None:
                self.messages: list[str] = []

            async def send(self, message: str) -> None:
                self.messages.append(message)

            async def recv(self) -> str:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        async def held_mints() -> set[str]:
            return {"mint"}

        async def on_price(_mint: str, _price: float) -> None:
            pass

        async def on_stale(mint: str) -> None:
            stale_mints.append(mint)
            stale_activated.set()

        socket = SilentSocket()
        feed = PumpPortalPriceFeed(
            held_mints,
            on_price,
            on_stale,
            refresh_interval_s=0.001,
            stale_after_s=0.005,
        )
        task = asyncio.create_task(feed._run_connection(socket))
        try:
            await asyncio.wait_for(stale_activated.wait(), timeout=0.5)
            await asyncio.sleep(0.02)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert stale_mints == ["mint"]
        assert socket.messages == ['{"method": "subscribeTokenTrade", "keys": ["mint"]}']

    asyncio.run(run())


def test_startup_orphan_check_closes_wallet_empty_live_position(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "trades.db"
        await init_db(db_path)
        manager = PositionManager(db_path, load_settings(), strategy="B")
        await manager.open_position(
            Trade(
                mint_address="wallet-empty-mint",
                side="BUY",
                amount_sol=0.01,
                token_amount=100.0,
                price_sol=0.0001,
                mode="live",
            ),
            None,
        )

        class EmptyWalletAdapter:
            async def get_token_balance(self, _mint: str) -> float:
                return 0.0

        assert await run_strategy_b._close_abandoned_live_positions(
            manager,
            EmptyWalletAdapter(),
            db_path,
        ) == 1
        assert await manager.get_all_open(mode="live") == []
        with sqlite3.connect(db_path) as connection:
            status = connection.execute(
                "SELECT status FROM trades WHERE mint_address = ? "
                "ORDER BY executed_at DESC LIMIT 1",
                ("wallet-empty-mint",),
            ).fetchone()[0]
        assert status == "abandoned"

    asyncio.run(run())
    assert "_close_abandoned_live_positions(manager, adapter, db_path)" in inspect.getsource(
        run_strategy_b.main,
    )


def test_quicknode_is_selected_before_primary_rpc(monkeypatch) -> None:
    monkeypatch.setenv("QUICKNODE_RPC_URL", "https://quicknode.example")
    monkeypatch.setenv("PRIMARY_RPC_URL", "https://primary.example")
    client = JupiterSwapClient(keypair=Keypair(), api_key="test-key")
    assert client._solana_rpc_url == "https://quicknode.example"
    asyncio.run(client.close())


def test_startup_logs_quicknode_when_healthy(monkeypatch, caplog) -> None:
    class Response:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict[str, object]:
            return {"result": {"value": {"blockhash": "test"}}}

    class Client:
        def __init__(self, **_kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> None:
            pass

        async def post(self, url: str, *, json: dict[str, object]) -> Response:
            assert url == "https://quicknode.example"
            assert json["method"] == "getLatestBlockhash"
            return Response()

    monkeypatch.setenv("QUICKNODE_RPC_URL", "https://quicknode.example")
    monkeypatch.setattr(run_strategy_b.httpx, "AsyncClient", Client)
    with caplog.at_level(logging.INFO, logger="strategy_b"):
        asyncio.run(run_strategy_b._log_rpc_primary())
    assert "RPC_PRIMARY: QuickNode" in caplog.text


def test_pumpportal_runtime_starts_in_paper_mode(monkeypatch, tmp_path: Path) -> None:
    started = asyncio.Event()

    class FakeFeed:
        def __init__(self, held_mints, on_price, on_stale) -> None:
            self.held_mints = held_mints
            self.on_price = on_price
            self.on_stale = on_stale

        async def run(self) -> None:
            started.set()
            await asyncio.Event().wait()

    async def finish_scan(*_args, single_trade_complete=None, **_kwargs) -> None:
        assert single_trade_complete is not None
        single_trade_complete.set()

    async def wait_until_cancelled(*_args, **_kwargs) -> None:
        await asyncio.Event().wait()

    class PaperAdapter:
        mode = "paper"

    async def empty_hydration(*_args, **_kwargs) -> dict[str, float]:
        return {}

    monkeypatch.setattr(run_strategy_b, "PumpPortalPriceFeed", FakeFeed)
    monkeypatch.setattr(run_strategy_b, "_hydrate_peak_prices", empty_hydration)
    monkeypatch.setattr(run_strategy_b, "scan_loop", finish_scan)
    monkeypatch.setattr(run_strategy_b, "monitor_loop", wait_until_cancelled)
    monkeypatch.setattr(run_strategy_b, "snapshot_loop", wait_until_cancelled)
    monkeypatch.setattr(run_strategy_b, "priority_fee_loop", wait_until_cancelled)

    asyncio.run(
        run_strategy_b._run_runtime_until_stopped(
            object(),
            object(),
            PaperAdapter(),
            tmp_path / "trades.db",
            object(),
            [],
            0.0,
            single_trade=True,
        ),
    )
    assert started.is_set()


def test_pumpportal_stale_emergency_closes_paper_positions(monkeypatch, tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "trades.db"
        halt_path = tmp_path / "strategy_b_halted"
        await init_db(db_path)
        manager = PositionManager(db_path, load_settings(), strategy="B")
        await manager.open_position(
            Trade(
                mint_address="stale-mint",
                side="BUY",
                amount_sol=0.01,
                token_amount=100.0,
                price_sol=0.0001,
                mode="paper",
            ),
            None,
        )

        class FakeFeed:
            def __init__(self, _held_mints, _on_price, on_stale) -> None:
                self.on_stale = on_stale

            async def run(self) -> None:
                await self.on_stale("stale-mint")

        class PaperAdapter:
            mode = "paper"

        class FakeAlerts:
            messages: list[tuple[str, str, str]] = []

            async def send(self, level: str, title: str, message: str) -> None:
                self.messages.append((level, title, message))

        alerts = FakeAlerts()

        async def wait_forever(*_args, **_kwargs) -> None:
            await asyncio.Event().wait()

        monkeypatch.setattr(run_strategy_b, "PumpPortalPriceFeed", FakeFeed)
        monkeypatch.setattr(run_strategy_b, "STRATEGY_B_HALT_PATH", halt_path)
        monkeypatch.setattr(health_monitor, "STRATEGY_B_HALT_PATH", halt_path)
        monkeypatch.setattr(run_strategy_b, "scan_loop", wait_forever)
        monkeypatch.setattr(run_strategy_b, "monitor_loop", wait_forever)
        monkeypatch.setattr(run_strategy_b, "snapshot_loop", wait_forever)
        monkeypatch.setattr(run_strategy_b, "priority_fee_loop", wait_forever)
        monkeypatch.setattr(
            run_strategy_b.AlertManager,
            "from_env",
            classmethod(lambda _cls: alerts),
        )

        await run_strategy_b._run_runtime_until_stopped(
            manager,
            object(),
            PaperAdapter(),
            db_path,
            object(),
            [],
            0.0,
        )

        assert await manager.get_all_open(mode="paper") == []
        assert len(alerts.messages) == 1
        assert alerts.messages[0][:2] == ("critical", "Strategy B emergency close")
        assert alerts.messages[0][2].startswith(
            "Reason: PumpPortal stale 15s\nPositions: stale-mint=0.0001 (closed)\nHalt: ",
        )
        halt = json.loads(halt_path.read_text())
        assert halt["reason"] == "PumpPortal stale 15s"
        assert halt["halted_at"]

        restart_attempts: list[object] = []
        monitor = health_monitor.HealthMonitor(sleep=lambda _: None)
        monkeypatch.setattr(monitor, "restart_strategy_b", lambda: restart_attempts.append(True))
        assert not monitor.run_cycle()
        assert restart_attempts == []
        with sqlite3.connect(db_path) as connection:
            event = connection.execute(
                "SELECT event_type, reason FROM runtime_events",
            ).fetchone()
        assert event == ("emergency_close_all", "PumpPortal stale 15s")

    try:
        asyncio.run(run())
    finally:
        run_strategy_b._shutting_down = False
        run_strategy_b.peak_prices.clear()


def test_monitor_worker_crash_emergency_closes_paper_positions(monkeypatch, tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "trades.db"
        halt_path = tmp_path / "strategy_b_halted"
        await init_db(db_path)
        manager = PositionManager(db_path, load_settings(), strategy="B")
        await manager.open_position(
            Trade(
                mint_address="monitor-mint",
                side="BUY",
                amount_sol=0.01,
                token_amount=100.0,
                price_sol=0.0002,
                mode="paper",
            ),
            None,
        )

        class FakeFeed:
            def __init__(self, *_args) -> None:
                pass

            async def run(self) -> None:
                await asyncio.Event().wait()

        class PaperAdapter:
            mode = "paper"

        class FakeAlerts:
            async def send(self, *_args) -> None:
                pass

        async def crash_monitor(*_args, **_kwargs) -> None:
            raise RuntimeError("simulated monitor failure")

        async def wait_forever(*_args, **_kwargs) -> None:
            await asyncio.Event().wait()

        monkeypatch.setattr(run_strategy_b, "PumpPortalPriceFeed", FakeFeed)
        monkeypatch.setattr(run_strategy_b, "STRATEGY_B_HALT_PATH", halt_path)
        monkeypatch.setattr(run_strategy_b, "scan_loop", wait_forever)
        monkeypatch.setattr(run_strategy_b, "monitor_loop", crash_monitor)
        monkeypatch.setattr(run_strategy_b, "snapshot_loop", wait_forever)
        monkeypatch.setattr(run_strategy_b, "priority_fee_loop", wait_forever)
        monkeypatch.setattr(
            run_strategy_b.AlertManager,
            "from_env",
            classmethod(lambda _cls: FakeAlerts()),
        )

        await run_strategy_b._run_runtime_until_stopped(
            manager,
            object(),
            PaperAdapter(),
            db_path,
            object(),
            [],
            0.0,
        )

        assert await manager.get_all_open(mode="paper") == []
        assert json.loads(halt_path.read_text())["reason"] == (
            "price monitor worker crashed: simulated monitor failure"
        )
        with sqlite3.connect(db_path) as connection:
            reason = connection.execute("SELECT reason FROM runtime_events").fetchone()[0]
        assert reason == "price monitor worker crashed: simulated monitor failure"

    try:
        asyncio.run(run())
    finally:
        run_strategy_b._shutting_down = False
        run_strategy_b.peak_prices.clear()


def test_strategy_b_hydrates_persisted_peak_prices_after_restart(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "trades.db"
        await init_db(db_path)
        manager = PositionManager(db_path, load_settings(), strategy="B")
        await manager.open_position(
            Trade(
                mint_address="peak-mint",
                side="BUY",
                amount_sol=0.01,
                token_amount=100.0,
                price_sol=0.0001,
                mode="paper",
            ),
            None,
        )

        class FixedMark:
            async def get_current_price(self, _mint: str) -> float:
                return 0.0002

        class PaperAdapter:
            mode = "paper"

        run_strategy_b.peak_prices.clear()
        try:
            await run_strategy_b.monitor_positions(
                manager,
                FixedMark(),
                db_path,
                adapter=PaperAdapter(),
            )
            with sqlite3.connect(db_path) as connection:
                persisted_peak = connection.execute(
                    "SELECT peak_price_sol FROM positions WHERE mint_address = 'peak-mint'",
                ).fetchone()[0]
            assert persisted_peak == 0.0002

            restarted_manager = PositionManager(db_path, load_settings(), strategy="B")
            run_strategy_b.peak_prices.clear()
            assert await run_strategy_b._hydrate_peak_prices(restarted_manager, "paper") == {
                "peak-mint": 0.0002,
            }
        finally:
            run_strategy_b.peak_prices.clear()

    asyncio.run(run())


def test_emergency_close_writes_unpriced_paper_position_at_entry(tmp_path: Path, monkeypatch) -> None:
    async def run() -> None:
        db_path = tmp_path / "trades.db"
        await init_db(db_path)
        manager = PositionManager(db_path, load_settings(), strategy="B")
        await manager.open_position(
            Trade(
                mint_address="unpriced-mint",
                side="BUY",
                amount_sol=0.01,
                token_amount=100.0,
                price_sol=None,
                mode="paper",
            ),
            None,
        )

        class PaperAdapter:
            mode = "paper"

        class SilentAlerts:
            async def send(self, *_args) -> None:
                pass

        monkeypatch.setattr(run_strategy_b, "STRATEGY_B_HALT_PATH", tmp_path / "halted")
        monkeypatch.setattr(
            run_strategy_b.AlertManager,
            "from_env",
            classmethod(lambda _cls: SilentAlerts()),
        )

        details = await run_strategy_b._emergency_close_all(
            manager,
            PaperAdapter(),
            db_path,
            "test unpriced emergency",
        )

        assert details == [
            {"mint": "unpriced-mint", "status": "closed", "fill_price_sol": 0.0},
        ]
        with sqlite3.connect(db_path) as connection:
            close_price = connection.execute(
                "SELECT close_price_sol FROM positions WHERE mint_address = 'unpriced-mint'",
            ).fetchone()[0]
        assert close_price == 0.0

    try:
        asyncio.run(run())
    finally:
        run_strategy_b._shutting_down = False
        run_strategy_b.peak_prices.clear()


def test_confirmed_live_sell_cleanup_does_not_block_monitor(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "trades.db"
        await init_db(db_path)
        manager = PositionManager(db_path, load_settings(), strategy="B")
        await manager.open_position(
            Trade(
                mint_address="live-mint",
                side="BUY",
                amount_sol=0.01,
                token_amount=100.0,
                price_sol=0.0003,
                mode="live",
            ),
            None,
        )
        position = (await manager.get_all_open(mode="live"))[0]
        release_cleanup = asyncio.Event()

        class DelayedCleanupAdapter:
            mode = "live"

            async def get_token_balance(self, _mint: str) -> float:
                return 100.0

            async def sell(self, mint: str, amount: float, *, slippage_bps: int) -> Trade:
                return Trade(
                    mint_address=mint,
                    side="SELL",
                    amount_sol=0.005,
                    token_amount=amount,
                    price_sol=0.00025,
                    mode="live",
                    status="confirmed",
                )

            async def verify_token_balance_cleared(self, _mint: str) -> float:
                await release_cleanup.wait()
                return 0.0

        adapter = DelayedCleanupAdapter()
        previous_mode = run_strategy_b.EXECUTION_MODE
        run_strategy_b.EXECUTION_MODE = "live"
        try:
            result = await run_strategy_b._close_position(
                manager,
                position,
                0.00025,
                "hard_stop",
                db_path,
                adapter,
                peak_price_sol=None,
            )
            assert result is not None
            assert await manager.get_all_open(mode="live") != []
            assert "live-mint" in run_strategy_b._selling_in_progress

            release_cleanup.set()
            await run_strategy_b._wait_for_inflight_sells(timeout_s=1)
            assert await manager.get_all_open(mode="live") == []
        finally:
            run_strategy_b.EXECUTION_MODE = previous_mode

    asyncio.run(run())


def test_kill_script_and_watchdog_share_killswitch_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    assert "/tmp/memecoin_killswitch" in (root / "scripts" / "kill_loop.sh").read_text()
    assert "/tmp/memecoin_killswitch" in Path("/home/dev/watchdog_memecoin.sh").read_text()

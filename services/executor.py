"""Fail-closed V2 strategy executor backed entirely by Hive state."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.adapters.live import build_live_adapter
from services.adapters.paper import PaperExecutionAdapter
from services.store import MemecoinStore
from services.strategy import GateConfig, get_qualifying_candidates, load_gates
from src.core.models import Side
from src.execution.price_provider import JupiterPriceProvider, PriceProvider
from src.execution.pumpportal_price import PumpPortalPriceFeed
from src.monitoring.alerts import AlertManager

log = logging.getLogger("memecoin.executor")


class ExecutorStore(Protocol):
    async def list_open_positions(self, strategy: str, mode: str) -> list[dict[str, Any]]: ...

    async def load_exit_config(self, strategy: str) -> dict[str, float]: ...

    async def create_position(self, position: dict[str, Any], trade: dict[str, Any]) -> None: ...

    async def update_position_mark(
        self,
        position_id: str,
        peak_price_sol: float,
        trailing_armed: bool,
    ) -> None: ...

    async def close_position(
        self,
        position: dict[str, Any],
        trade: dict[str, Any],
        *,
        close_price_sol: float,
        close_reason: str,
        realized_pnl_sol: float,
    ) -> None: ...

    async def record_runtime_event(
        self,
        event_type: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None: ...

    async def refresh_daily_stats(self, strategy: str) -> None: ...


class FailClosed(RuntimeError):
    """Signals that the process must close positions and exit with status 42."""


class StrategyExecutor:
    """Run entry queries and price exits without retaining durable state in memory."""

    def __init__(
        self,
        store: ExecutorStore,
        adapter: Any,
        *,
        strategy: str = "BT",
        cycle_seconds: float = 1.0,
        heartbeat_path: Path | None = None,
        heartbeat_timeout_seconds: float = 30.0,
        halt_path: Path | None = None,
        mark_provider: PriceProvider | None = None,
        alert_manager: Any | None = None,
    ) -> None:
        self._store = store
        self._adapter = adapter
        self._strategy = strategy
        self._mode = str(adapter.mode)
        self._cycle_seconds = cycle_seconds
        self._heartbeat_path = heartbeat_path or Path("/tmp/memecoin-executor.heartbeat")
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._halt_path = halt_path or Path("/tmp/memecoin-executor-halted")
        self._mark_provider = mark_provider or JupiterPriceProvider()
        self._alerts = alert_manager or AlertManager.from_env()
        self._positions: dict[str, dict[str, Any]] = {}
        self._last_pumpportal_price_at: dict[str, float] = {}
        self._last_jupiter_fallback_at: dict[str, float] = {}
        self._gates: GateConfig | None = None
        self._exits: dict[str, float] = {}
        self._last_cycle_monotonic: float | None = None
        self._last_reconciliation_monotonic: float | None = None
        self._reconciliation_interval_seconds = float(
            os.getenv("MEMECOIN_RECONCILIATION_INTERVAL_SECONDS", "300"),
        )
        self._fatal_reason: str | None = None
        self._emergency_started = False

    async def start(self) -> None:
        """Hydrate persisted open state before accepting any new entry."""

        if self._halt_path.exists():
            raise FailClosed(
                f"manual review required: existing emergency halt marker {self._halt_path}",
            )
        self._gates = await load_gates(self._store, self._strategy)
        self._exits = await self._store.load_exit_config(self._strategy)
        self._validate_exit_config()
        rows = await self._store.list_open_positions(self._strategy, self._mode)
        self._positions = {str(row["mint_address"]): dict(row) for row in rows}
        await self._reconcile_startup()
        self._last_reconciliation_monotonic = time.monotonic()
        self._last_cycle_monotonic = time.monotonic()
        self._write_heartbeat()

    async def run(self) -> None:
        """Run until a failure requires permanent manual intervention."""

        feed_task: asyncio.Task[None] | None = None
        try:
            await self.start()
            feed = PumpPortalPriceFeed(
                self._held_mints,
                self._on_pumpportal_price,
                self._on_feed_stale,
            )
            feed_task = asyncio.create_task(feed.run(), name="memecoin-pumpportal-price-feed")
            while True:
                self._raise_if_fatal(feed_task)
                started = time.monotonic()
                await asyncio.wait_for(self.run_cycle(), timeout=self._heartbeat_timeout_seconds)
                self._last_cycle_monotonic = time.monotonic()
                self._write_heartbeat()
                await asyncio.sleep(max(0.0, self._cycle_seconds - (time.monotonic() - started)))
        except asyncio.CancelledError:
            await self._emergency_close_all("executor cancelled")
            raise
        except Exception as exc:
            await self._emergency_close_all(str(exc))
            raise SystemExit(42) from exc
        finally:
            if feed_task is not None:
                feed_task.cancel()
                await asyncio.gather(feed_task, return_exceptions=True)
            try:
                await self._adapter.close()
            except Exception as exc:
                log.error("adapter close failed: %s", exc)
            close_mark_provider = getattr(self._mark_provider, "close", None)
            if close_mark_provider is not None:
                try:
                    await close_mark_provider()
                except Exception as exc:
                    log.error("Jupiter mark provider close failed: %s", exc)

    async def run_cycle(self) -> None:
        """Evaluate the latest query result and enter only while capacity remains."""

        if self._last_cycle_monotonic is not None:
            elapsed = time.monotonic() - self._last_cycle_monotonic
            if elapsed > self._heartbeat_timeout_seconds:
                raise FailClosed(f"executor heartbeat stale for {elapsed:.1f}s")
        if self._fatal_reason:
            raise FailClosed(self._fatal_reason)
        if self._gates is None:
            raise RuntimeError("executor was not started")
        self._check_live_circuit_breaker()
        await self._refresh_quiet_position_marks()
        if (
            self._mode == "live"
            and self._last_reconciliation_monotonic is not None
            and time.monotonic() - self._last_reconciliation_monotonic
            >= self._reconciliation_interval_seconds
        ):
            await self._reconcile_startup()
            self._last_reconciliation_monotonic = time.monotonic()
        candidates = await get_qualifying_candidates(
            self._store,
            self._strategy,
            since_seconds=max(self._cycle_seconds * 2, 5),
            mode=self._mode,
        )
        max_open = int(self._gates.number("max_open"))
        for candidate in candidates:
            if len(self._positions) >= max_open:
                return
            mint = str(candidate["mint_address"])
            if mint in self._positions:
                continue
            await self._enter(candidate)

    async def handle_price(self, mint_address: str, price_sol: float) -> None:
        """Evaluate one streamed mark; exposed for deterministic paper verification."""

        await self._on_pumpportal_price(mint_address, price_sol)

    async def _held_mints(self) -> set[str]:
        return set(self._positions)

    async def _on_pumpportal_price(self, mint_address: str, price_sol: float) -> None:
        """Record a held-token PumpPortal mark before evaluating its exits."""
        if mint_address in self._positions:
            self._last_pumpportal_price_at[mint_address] = time.monotonic()
        await self._on_price(mint_address, price_sol)

    async def _on_price(self, mint_address: str, price_sol: float) -> None:
        try:
            if price_sol <= 0:
                raise ValueError(f"invalid non-positive price for {mint_address}")
            position = self._positions.get(mint_address)
            if position is None:
                return
            entry = float(position["entry_price_sol"])
            previous_peak = max(float(position.get("peak_price_sol") or entry), entry)
            peak = max(previous_peak, price_sol)
            arm_ratio = self._exits["trailing_arm_pct"] / 100
            # Decimal market prices make an exact percentage boundary susceptible
            # to binary rounding; never leave a configured threshold unarmed.
            armed = bool(position.get("trailing_armed")) or (peak / entry >= arm_ratio + 1 - 1e-12)
            if peak > previous_peak or armed != bool(position.get("trailing_armed")):
                await self._store.update_position_mark(str(position["id"]), peak, armed)
                position["peak_price_sol"] = peak
                position["trailing_armed"] = armed
            reason = self._exit_reason(position, price_sol)
            if reason is not None:
                await self._close_position(position, price_sol, reason)
        except Exception as exc:
            self._fatal_reason = f"price monitor failure: {exc}"
            raise

    async def _on_feed_stale(self) -> None:
        self._fatal_reason = "PumpPortal global feed stale >15s"

    async def _refresh_quiet_position_marks(self) -> None:
        """Use Jupiter marks when a held token has no recent PumpPortal trade."""
        now = time.monotonic()
        interval = max(self._cycle_seconds, 1.0)
        quiet_mints = [
            mint
            for mint in self._positions
            if now - self._last_pumpportal_price_at.get(mint, 0.0) >= interval
            and now - self._last_jupiter_fallback_at.get(mint, 0.0) >= interval
        ]
        if not quiet_mints:
            return

        async def refresh(mint: str) -> None:
            self._last_jupiter_fallback_at[mint] = now
            price_sol = await self._mark_provider.get_current_price(mint)
            if price_sol is not None:
                await self._on_price(mint, price_sol)

        await asyncio.gather(*(refresh(mint) for mint in quiet_mints))

    def _exit_reason(self, position: dict[str, Any], current_price: float) -> str | None:
        entry = float(position["entry_price_sol"])
        peak = max(float(position.get("peak_price_sol") or entry), entry)
        opened_at = position["opened_at"]
        if current_price <= entry * (1 - self._exits["hard_stop_pct"] / 100):
            return "hard_stop"
        if current_price >= entry * (1 + self._exits["take_profit_pct"] / 100):
            return "take_profit"
        if bool(position.get("trailing_armed")) and current_price <= peak * (
            1 - self._exits["trailing_stop_pct"] / 100
        ):
            return "trailing_stop"
        if isinstance(opened_at, datetime):
            age_minutes = (datetime.now(UTC) - opened_at).total_seconds() / 60
            if age_minutes >= self._exits["time_stop_minutes"]:
                return "time_stop"
        return None

    async def _enter(self, candidate: dict[str, Any]) -> None:
        mint = str(candidate["mint_address"])
        candidate_price = _positive_float(candidate.get("price_sol"))
        if candidate_price is None:
            raise FailClosed(f"qualifying candidate {mint} has no usable SOL price")
        if isinstance(self._adapter, PaperExecutionAdapter):
            self._adapter.set_price(mint, candidate_price)
        amount_sol = float(os.getenv("POSITION_SIZE_SOL", "0.02"))
        if amount_sol <= 0:
            raise RuntimeError("POSITION_SIZE_SOL must be positive")
        if (
            self._mode == "live"
            and candidate.get("pool_type") == "bonding"
            and hasattr(self._adapter, "buy_bonding_curve")
        ):
            trade = await self._adapter.buy_bonding_curve(mint, amount_sol, 300)
        else:
            trade = await self._adapter.execute_swap(mint, Side.BUY, amount_sol, 300)
        if (
            trade.token_amount is None
            or trade.token_amount <= 0
            or _positive_float(trade.price_sol) is None
        ):
            raise FailClosed(f"entry for {mint} returned an unpriced or zero-token fill")
        opened_at = datetime.now(UTC)
        position = {
            "id": str(uuid4()),
            "mint_address": mint,
            "entry_price_sol": float(trade.price_sol),
            "amount_sol": amount_sol,
            "token_amount": float(trade.token_amount),
            "peak_price_sol": float(trade.price_sol),
            "trailing_armed": False,
            "mode": self._mode,
            "strategy": self._strategy,
            "opened_at": opened_at,
            "candidate_id": candidate.get("id"),
            "fill_quality": "simulated" if self._mode == "paper" else "confirmed",
            "tx_signature": trade.tx_signature,
        }
        await self._store.create_position(position, _trade_record(trade))
        self._positions[mint] = position
        log.info("entered mint=%s position=%s", mint[:16], position["id"])

    async def _close_position(
        self,
        position: dict[str, Any],
        fallback_price_sol: float,
        reason: str,
    ) -> None:
        mint = str(position["mint_address"])
        token_amount = float(position["token_amount"])
        if hasattr(self._adapter, "sell"):
            trade = await self._adapter.sell(mint, token_amount, 300)
        else:
            trade = await self._adapter.execute_swap(mint, Side.SELL, token_amount, 300)
        close_price = _positive_float(trade.price_sol) or fallback_price_sol
        trade.token_amount = token_amount
        realized_pnl = token_amount * close_price - float(position["amount_sol"])
        await self._store.close_position(
            position,
            _trade_record(trade),
            close_price_sol=close_price,
            close_reason=reason,
            realized_pnl_sol=realized_pnl,
        )
        self._positions.pop(mint, None)
        self._last_pumpportal_price_at.pop(mint, None)
        self._last_jupiter_fallback_at.pop(mint, None)
        await self._store.refresh_daily_stats(self._strategy)
        log.info("closed mint=%s reason=%s", mint[:16], reason)

    async def _reconcile_startup(self) -> None:
        if self._mode != "live":
            return
        holdings_lookup = getattr(self._adapter, "get_wallet_holdings", None)
        if holdings_lookup is None:
            raise FailClosed("live adapter does not expose wallet holdings for reconciliation")
        holdings = await holdings_lookup()
        if holdings is None:
            raise FailClosed("live wallet holdings are unavailable for reconciliation")
        expected = {
            mint: float(position["token_amount"]) for mint, position in self._positions.items()
        }
        for mint, amount in expected.items():
            actual = float(holdings.get(mint, 0.0))
            if actual <= 0 or abs(actual - amount) > max(amount * 0.001, 1e-9):
                raise FailClosed(f"startup reconciliation mismatch for {mint}")
        wallet_only = {
            mint for mint, amount in holdings.items() if amount > 0 and mint not in expected
        }
        if wallet_only:
            raise FailClosed(
                f"startup reconciliation wallet-only holdings: {', '.join(sorted(wallet_only))}"
            )

    def _check_live_circuit_breaker(self) -> None:
        if self._mode != "live":
            return
        checker = getattr(self._adapter, "circuit_breaker_tripped", None)
        if checker is None:
            raise FailClosed("live adapter does not expose circuit-breaker status")
        if checker():
            raise FailClosed("live circuit breaker is tripped")

    def _raise_if_fatal(self, feed_task: asyncio.Task[None]) -> None:
        if self._fatal_reason:
            raise FailClosed(self._fatal_reason)
        if feed_task.done():
            exception = feed_task.exception()
            raise FailClosed(f"PumpPortal monitor stopped: {exception or 'unexpected completion'}")

    def _validate_exit_config(self) -> None:
        required = {
            "trailing_stop_pct",
            "trailing_arm_pct",
            "hard_stop_pct",
            "take_profit_pct",
            "time_stop_minutes",
        }
        missing = sorted(required - self._exits.keys())
        if missing:
            raise RuntimeError(
                f"Missing exit parameters for {self._strategy}: {', '.join(missing)}"
            )
        if any(value < 0 for value in self._exits.values()):
            raise RuntimeError("Exit parameters must not be negative")

    async def _emergency_close_all(self, reason: str) -> None:
        if self._emergency_started:
            return
        self._emergency_started = True
        self._write_halt(reason)
        failures: dict[str, str] = {}
        positions = list(self._positions.values())
        for position in positions:
            mint = str(position["mint_address"])
            fallback = float(position.get("peak_price_sol") or position["entry_price_sol"])
            try:
                await self._close_position(position, fallback, "emergency")
            except Exception as exc:
                failures[mint] = str(exc)
                log.critical("emergency close failed mint=%s: %s", mint[:16], exc)
        try:
            await self._store.record_runtime_event(
                "emergency_close_all",
                reason,
                {"close_failures": failures, "remaining_positions": sorted(self._positions)},
            )
        except Exception as exc:
            log.critical("failed to record emergency event: %s", exc)
        details = "; ".join(
            f"{mint[:16]}={'failed' if mint in failures else 'closed'}"
            for mint in sorted(str(position["mint_address"]) for position in positions)
        ) or "no open positions"
        try:
            await self._alerts.send(
                "critical",
                "Memecoin executor emergency close",
                f"Reason: {reason}\nPositions: {details}\nHalt: {self._halt_path}",
            )
        except Exception as exc:
            log.critical("emergency close alert failed: %s", exc)

    def _write_heartbeat(self) -> None:
        payload = json.dumps(
            {"last_cycle": datetime.now(UTC).isoformat(), "strategy": self._strategy}
        )
        temporary = self._heartbeat_path.with_suffix(".tmp")
        temporary.write_text(payload)
        temporary.replace(self._heartbeat_path)

    def _write_halt(self, reason: str) -> None:
        payload = json.dumps({"halted_at": datetime.now(UTC).isoformat(), "reason": reason})
        temporary = self._halt_path.with_suffix(".tmp")
        temporary.write_text(payload)
        temporary.replace(self._halt_path)


def _positive_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _trade_record(trade: Any) -> dict[str, Any]:
    return {
        "id": str(trade.id),
        "mint_address": str(trade.mint_address),
        "side": str(trade.side).lower(),
        "amount_sol": float(trade.amount_sol),
        "token_amount": float(trade.token_amount) if trade.token_amount is not None else None,
        "price_sol": float(trade.price_sol) if trade.price_sol is not None else None,
        "slippage_bps": int(trade.slippage_bps),
        "tx_signature": trade.tx_signature,
        "mode": str(trade.mode),
        "executed_at": trade.executed_at,
        "metadata": dict(trade.metadata),
    }


async def _run() -> None:
    load_dotenv()
    mode = os.getenv("EXECUTION_MODE", "paper").lower()
    if mode == "paper":
        adapter: Any = PaperExecutionAdapter()
    elif mode == "live":
        adapter = build_live_adapter()
    else:
        raise RuntimeError(f"EXECUTION_MODE must be paper or live, got {mode!r}")
    store = await MemecoinStore.connect()
    try:
        executor = StrategyExecutor(
            store,
            adapter,
            cycle_seconds=float(os.getenv("MEMECOIN_EXECUTOR_CYCLE_SECONDS", "1")),
        )
        await executor.run()
    finally:
        await store.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s")
    asyncio.run(_run())


if __name__ == "__main__":
    main()

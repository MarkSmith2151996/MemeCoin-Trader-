"""Fail-closed V2 strategy executor backed entirely by Hive state."""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import math
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
from src.core.config import Settings, load_settings
from src.core.models import Side, Trade
from src.execution.live_daily_caps import evaluate_daily_live_caps
from src.execution.live_guardrails import evaluate_live_guardrails
from src.execution.price_provider import JupiterPriceProvider, PriceProvider, PriceResult
from src.execution.pumpportal_price import PumpPortalPriceFeed
from src.monitoring.alerts import AlertManager

log = logging.getLogger("memecoin.executor")
EXECUTOR_LOCK_PATH = Path("/tmp/memecoin_executor.lock")
_executor_lock_handle: Any | None = None
MONITOR_INTERVAL_SECONDS = 0.1
JUPITER_FALLBACK_RPS = 7.0
JUPITER_FALLBACK_INTERVAL_SECONDS = 1 / JUPITER_FALLBACK_RPS
MIN_JUPITER_POSITION_INTERVAL_SECONDS = 0.25


class ExecutorStore(Protocol):
    async def list_open_positions(self, strategy: str, mode: str) -> list[dict[str, Any]]: ...

    async def load_exit_config(self, strategy: str) -> dict[str, float]: ...

    async def load_daily_live_state(self) -> Any: ...

    async def create_position(self, position: dict[str, Any], trade: dict[str, Any]) -> None: ...

    def entry_transaction(self, mint_address: str) -> Any: ...

    async def update_position_mark(
        self,
        position_id: str,
        peak_price_sol: float,
        trailing_armed: bool,
    ) -> None: ...

    async def record_exit_evaluation(
        self,
        position_id: str,
        mint_address: str,
        *,
        source: str,
        mark_timestamp: object,
        trigger_price_sol: float | None,
        usable: bool,
        diagnostic: str,
        exit_reason: str | None,
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
        self._cached_marks: dict[str, tuple[float | None, str, datetime, str]] = {}
        self._last_pumpportal_price_at: dict[str, float] = {}
        self._last_jupiter_fallback_at: dict[str, float] = {}
        self._last_jupiter_request_at = 0.0
        self._last_valid_mark_at: dict[str, float] = {}
        self._mark_sla_warned: set[str] = set()
        self._mint_locks: dict[str, asyncio.Lock] = {}
        self._gates: GateConfig | None = None
        self._exits: dict[str, float] = {}
        self._last_cycle_monotonic: float | None = None
        self._last_reconciliation_monotonic: float | None = None
        self._reconciliation_interval_seconds = float(
            os.getenv("MEMECOIN_RECONCILIATION_INTERVAL_SECONDS", "300"),
        )
        self._fatal_reason: str | None = None
        self._emergency_started = False
        self._live_entry_alerted = False

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
        started_at = time.monotonic()
        self._last_valid_mark_at = {mint: started_at for mint in self._positions}
        await self._reconcile_startup()
        self._last_reconciliation_monotonic = time.monotonic()
        self._last_cycle_monotonic = time.monotonic()
        self._write_heartbeat()

    async def run(self) -> None:
        """Run until a failure requires permanent manual intervention."""

        feed_task: asyncio.Task[None] | None = None
        monitor_task: asyncio.Task[None] | None = None
        refresh_task: asyncio.Task[None] | None = None
        try:
            await self.start()
            feed = PumpPortalPriceFeed(
                self._held_mints,
                self._on_pumpportal_price,
                self._on_feed_stale,
            )
            feed_task = asyncio.create_task(feed.run(), name="memecoin-pumpportal-price-feed")
            monitor_task = asyncio.create_task(
                self._monitor_loop(), name="memecoin-exit-monitor"
            )
            refresh_task = asyncio.create_task(
                self._price_refresh_loop(), name="memecoin-jupiter-price-refresh"
            )
            while True:
                self._raise_if_fatal(feed_task, monitor_task, refresh_task)
                started = time.monotonic()
                await self.run_cycle()
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
            tasks = [task for task in (feed_task, monitor_task, refresh_task) if task is not None]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
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

        if self._fatal_reason:
            raise FailClosed(self._fatal_reason)
        if self._gates is None:
            raise RuntimeError("executor was not started")
        self._check_live_circuit_breaker()
        if (
            self._mode == "live"
            and self._last_reconciliation_monotonic is not None
            and time.monotonic() - self._last_reconciliation_monotonic
            >= self._reconciliation_interval_seconds
        ):
            if await self._reconcile_startup():
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

        self._cache_mark(
            mint_address,
            price_sol,
            source="pumpportal",
            mark_timestamp=datetime.now(UTC),
            diagnostic="stream_mark",
        )
        await self._monitor_positions_once()

    async def _held_mints(self) -> set[str]:
        return set(self._positions)

    async def _on_pumpportal_price(self, mint_address: str, price_sol: float) -> None:
        """Record a held-token PumpPortal mark before evaluating its exits."""
        if mint_address not in self._positions:
            return
        self._last_pumpportal_price_at[mint_address] = time.monotonic()
        self._cache_mark(
            mint_address,
            price_sol,
            source="pumpportal",
            mark_timestamp=datetime.now(UTC),
            diagnostic="stream_mark",
        )

    async def _on_feed_stale(self) -> None:
        self._fatal_reason = "PumpPortal global feed stale >15s"

    def _cache_mark(
        self,
        mint_address: str,
        price_sol: float | None,
        *,
        source: str,
        mark_timestamp: datetime,
        diagnostic: str,
    ) -> None:
        """Accept price producers without performing database work on their path."""

        price = _positive_float(price_sol)
        self._cached_marks[mint_address] = (price, source, mark_timestamp, diagnostic)
        if price is not None:
            self._last_valid_mark_at[mint_address] = time.monotonic()
            self._mark_sla_warned.discard(mint_address)

    async def _monitor_loop(self) -> None:
        """Evaluate cached marks independently of the one-second discovery loop."""

        log.info("exit monitor started interval_ms=%d", int(MONITOR_INTERVAL_SECONDS * 1000))
        while True:
            started = time.monotonic()
            await self._monitor_positions_once()
            await asyncio.sleep(
                max(0.0, MONITOR_INTERVAL_SECONDS - (time.monotonic() - started))
            )

    async def _price_refresh_loop(self) -> None:
        """Refresh quiet holdings under the Jupiter budget; never evaluates exits."""

        while True:
            await self._refresh_quiet_position_marks()
            await asyncio.sleep(0.05)

    async def _refresh_quiet_position_marks(self) -> None:
        """Refresh one quiet mint at a time, preserving seven RPS for exit marks."""

        now = time.monotonic()
        if now - self._last_jupiter_request_at < JUPITER_FALLBACK_INTERVAL_SECONDS:
            return
        position_interval = max(
            MIN_JUPITER_POSITION_INTERVAL_SECONDS,
            len(self._positions) / JUPITER_FALLBACK_RPS,
        )
        quiet_mints = [
            mint
            for mint in self._positions
            if now - self._last_pumpportal_price_at.get(mint, 0.0)
            >= position_interval
            and now - self._last_jupiter_fallback_at.get(mint, 0.0)
            >= position_interval
        ]
        if not quiet_mints:
            return
        mint = min(quiet_mints, key=lambda item: self._last_jupiter_fallback_at.get(item, 0.0))
        self._last_jupiter_fallback_at[mint] = now
        self._last_jupiter_request_at = now
        mark = await self._get_mark(mint)
        self._cache_mark(
            mint,
            mark.price_sol,
            source=self._mark_provider.name,
            mark_timestamp=datetime.now(UTC),
            diagnostic=mark.reason,
        )

    async def _monitor_positions_once(self) -> None:
        """Perform only in-memory comparisons until an exit is triggered."""

        for mint in list(self._positions):
            async with self._lock_for(mint):
                position = self._positions.get(mint)
                if position is not None:
                    await self._monitor_position_locked(mint, position)

    async def _monitor_position_locked(
        self,
        mint_address: str,
        position: dict[str, Any],
    ) -> None:
        cached = self._cached_marks.get(mint_address)
        if cached is None or cached[0] is None:
            source, mark_timestamp, diagnostic = (
                cached[1:]
                if cached is not None
                else ("cache", datetime.now(UTC), "no_cached_mark")
            )
            await self._monitor_unmarked_position_locked(
                mint_address,
                position,
                source=source,
                mark_timestamp=mark_timestamp,
                diagnostic=diagnostic,
            )
            return

        current_price, source, mark_timestamp, diagnostic = cached
        entry = float(position["entry_price_sol"])
        previous_peak = max(float(position.get("peak_price_sol") or entry), entry)
        peak = max(previous_peak, current_price)
        arm_ratio = 1 + self._exits["trailing_arm_pct"] / 100
        armed = bool(position.get("trailing_armed")) or peak / entry >= arm_ratio - 1e-12
        position["peak_price_sol"] = peak
        position["trailing_armed"] = armed
        reason = self._exit_reason(position, current_price)
        if reason is None:
            return
        await self._store.record_exit_evaluation(
            str(position["id"]),
            mint_address,
            source=source,
            mark_timestamp=mark_timestamp,
            trigger_price_sol=current_price,
            usable=True,
            diagnostic=diagnostic,
            exit_reason=reason,
        )
        await self._close_position(
            position,
            current_price,
            reason,
            mark_source=source,
            mark_timestamp=mark_timestamp,
            mark_diagnostic=diagnostic,
            lock_held=True,
        )

    async def _monitor_unmarked_position_locked(
        self,
        mint_address: str,
        position: dict[str, Any],
        *,
        source: str,
        mark_timestamp: datetime,
        diagnostic: str,
    ) -> None:
        opened_at = position.get("opened_at")
        age_minutes = (
            (datetime.now(UTC) - opened_at).total_seconds() / 60
            if isinstance(opened_at, datetime)
            else 0.0
        )
        reason: str | None = None
        if age_minutes >= self._exits["time_stop_minutes"]:
            reason = "time_stop"
        else:
            no_mark_seconds = time.monotonic() - self._last_valid_mark_at.get(
                mint_address,
                time.monotonic(),
            )
            if no_mark_seconds >= 120:
                reason = "mark_sla_timeout"
            elif no_mark_seconds >= 60 and mint_address not in self._mark_sla_warned:
                self._mark_sla_warned.add(mint_address)
                log.warning(
                    "mark SLA warning mint=%s no valid mark for %.1fs",
                    mint_address[:16],
                    no_mark_seconds,
                )
        if reason is None:
            return
        await self._store.record_exit_evaluation(
            str(position["id"]),
            mint_address,
            source=source,
            mark_timestamp=mark_timestamp,
            trigger_price_sol=None,
            usable=False,
            diagnostic=diagnostic,
            exit_reason=reason,
        )
        await self._close_position(
            position,
            float(position["entry_price_sol"]),
            reason,
            mark_source=source,
            mark_timestamp=mark_timestamp,
            mark_diagnostic=diagnostic,
            lock_held=True,
        )

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
        async with self._lock_for(mint):
            if mint in self._positions:
                return
            await self._enter_locked(candidate)

    async def _enter_locked(self, candidate: dict[str, Any]) -> None:
        mint = str(candidate["mint_address"])
        slippage_bps = _entry_slippage_bps(candidate.get("pool_sol"))
        if slippage_bps is None:
            log.info("entry skipped mint=%s reason=pool_below_slippage_tier", mint[:16])
            return
        amount_sol = float(os.getenv("POSITION_SIZE_SOL", "0.02"))
        if amount_sol <= 0:
            raise RuntimeError("POSITION_SIZE_SOL must be positive")
        if self._mode == "live":
            await self._check_live_entry_guardrails(amount_sol)
        async with self._store.entry_transaction(mint) as entry:
            if not entry.allowed:
                log.info("entry skipped mint=%s reason=%s", mint[:16], entry.rejection_reason)
                return

            mark_timestamp: datetime | None = None
            entry_mark: PriceResult | None = None
            if isinstance(self._adapter, PaperExecutionAdapter):
                entry_mark = await self._get_mark(mint)
                mark_price = _positive_float(entry_mark.price_sol)
                if mark_price is None:
                    log.warning(
                        "entry skipped mint=%s no usable Price V3 mark reason=%s",
                        mint[:16],
                        entry_mark.reason,
                    )
                    return
                mark_timestamp = datetime.now(UTC)
                self._adapter.set_price(mint, mark_price)

            if (
                self._mode == "live"
                and candidate.get("pool_type") == "bonding"
                and hasattr(self._adapter, "buy_bonding_curve")
            ):
                trade = await self._adapter.buy_bonding_curve(mint, amount_sol, slippage_bps)
            else:
                trade = await self._adapter.execute_swap(
                    mint,
                    Side.BUY,
                    amount_sol,
                    slippage_bps,
                )
            if (
                trade.token_amount is None
                or trade.token_amount <= 0
                or _positive_float(trade.price_sol) is None
            ):
                raise FailClosed(f"entry for {mint} returned an unpriced or zero-token fill")
            trade.metadata = {
                **trade.metadata,
                "discovery_price_sol": candidate.get("price_sol"),
                "discovery_source": candidate.get("source"),
                "entry_mark_source": self._mark_provider.name if entry_mark else "confirmed_fill",
                "entry_mark_timestamp": mark_timestamp.isoformat() if mark_timestamp else None,
                "entry_mark_diagnostic": entry_mark.reason if entry_mark else None,
            }
            opened_at = datetime.now(UTC)
            position = {
                "id": str(uuid4()),
                "mint_address": mint,
                "entry_price_sol": float(trade.price_sol),
                "amount_sol": float(trade.amount_sol),
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
            await entry.create_position(position, _trade_record(trade))
            self._positions[mint] = position
            self._last_valid_mark_at[mint] = time.monotonic()
            log.info("entered mint=%s position=%s", mint[:16], position["id"])
            if self._mode == "live" and not self._live_entry_alerted:
                self._live_entry_alerted = True
                log.critical("FIRST LIVE ENTRY session mint=%s position=%s", mint, position["id"])
                try:
                    await self._alerts.send(
                        "critical",
                        "First live entry",
                        f"Mint: {mint}\nPosition: {position['id']}\nAmount: {trade.amount_sol} SOL",
                    )
                except Exception as exc:
                    log.error("first live entry alert failed: %s", exc)

    async def _close_position(
        self,
        position: dict[str, Any],
        fallback_price_sol: float,
        reason: str,
        *,
        mark_source: str = "unknown",
        mark_timestamp: datetime | None = None,
        mark_diagnostic: str = "unknown",
        lock_held: bool = False,
    ) -> None:
        mint = str(position["mint_address"])
        if not lock_held:
            async with self._lock_for(mint):
                current = self._positions.get(mint)
                if current is None or str(current["id"]) != str(position["id"]):
                    return
                await self._close_position(
                    current,
                    fallback_price_sol,
                    reason,
                    mark_source=mark_source,
                    mark_timestamp=mark_timestamp,
                    mark_diagnostic=mark_diagnostic,
                    lock_held=True,
                )
                return
        token_amount = float(position["token_amount"])
        trigger_timestamp = mark_timestamp or datetime.now(UTC)
        if self._mode == "paper":
            close_price = self._paper_exit_price(position, fallback_price_sol, reason)
            trade = Trade(
                mint_address=mint,
                side=Side.SELL,
                amount_sol=token_amount * close_price,
                token_amount=token_amount,
                price_sol=close_price,
                slippage_bps=300,
                mode="paper",
                status="simulated",
            )
        elif hasattr(self._adapter, "sell"):
            trade = await self._adapter.sell(mint, token_amount, 300)
            close_price = _positive_float(trade.price_sol)
            if close_price is None:
                raise FailClosed(f"live close for {mint} returned no actual fill price")
        else:
            trade = await self._adapter.execute_swap(mint, Side.SELL, token_amount, 300)
            close_price = _positive_float(trade.price_sol)
            if close_price is None:
                raise FailClosed(f"live close for {mint} returned no actual fill price")
        if self._mode == "live":
            verify_cleared = getattr(self._adapter, "verify_token_balance_cleared", None)
            if verify_cleared is None:
                raise FailClosed("live adapter cannot verify post-sell token balance")
            await verify_cleared(mint)
        trade.token_amount = token_amount
        trade.metadata = {
            **trade.metadata,
            "close_reason": reason,
            "trigger_price_sol": fallback_price_sol,
            "mark_source": mark_source,
            "mark_timestamp": trigger_timestamp.isoformat(),
            "mark_diagnostic": mark_diagnostic,
        }
        amount_sol = float(position["amount_sol"])
        entry_price = float(position["entry_price_sol"])
        realized_pnl = (
            amount_sol * ((close_price - entry_price) / entry_price)
            if self._mode == "paper"
            else float(trade.amount_sol) - amount_sol
        )
        await self._store.close_position(
            position,
            _trade_record(trade),
            close_price_sol=close_price,
            close_reason=reason,
            realized_pnl_sol=realized_pnl,
        )
        self._positions.pop(mint, None)
        self._cached_marks.pop(mint, None)
        self._last_pumpportal_price_at.pop(mint, None)
        self._last_jupiter_fallback_at.pop(mint, None)
        self._last_valid_mark_at.pop(mint, None)
        self._mark_sla_warned.discard(mint)
        await self._store.refresh_daily_stats(self._strategy)
        log.info("closed mint=%s reason=%s", mint[:16], reason)

    def _lock_for(self, mint_address: str) -> asyncio.Lock:
        lock = self._mint_locks.get(mint_address)
        if lock is None:
            lock = asyncio.Lock()
            self._mint_locks[mint_address] = lock
        return lock

    def _paper_exit_price(
        self,
        position: dict[str, Any],
        fallback_price_sol: float,
        reason: str,
    ) -> float:
        """Mirror fixed-level backtest fills while leaving live fills untouched."""

        entry = float(position["entry_price_sol"])
        if reason == "hard_stop":
            return entry * (1 - self._exits["hard_stop_pct"] / 100)
        if reason == "take_profit":
            return entry * (1 + self._exits["take_profit_pct"] / 100)
        if reason == "trailing_stop":
            peak = max(float(position.get("peak_price_sol") or entry), entry)
            return peak * (1 - self._exits["trailing_stop_pct"] / 100)
        return fallback_price_sol

    async def _check_live_entry_guardrails(self, amount_sol: float) -> None:
        settings = _live_settings()
        guardrails = evaluate_live_guardrails(settings, requested_trade_sol=amount_sol)
        if not guardrails.allowed:
            raise FailClosed(f"live guardrails blocked entry: {', '.join(guardrails.diagnostics)}")
        daily_state = await self._store.load_daily_live_state()
        daily_caps = evaluate_daily_live_caps(
            daily_state,
            max_daily_trades=guardrails.max_daily_trades,
            max_daily_loss_sol=guardrails.max_daily_loss_sol,
            today=datetime.now(UTC).date(),
        )
        if not daily_caps.allowed:
            raise FailClosed(f"live daily caps blocked entry: {', '.join(daily_caps.diagnostics)}")
        balance_lookup = getattr(self._adapter, "get_sol_balance", None)
        if balance_lookup is None:
            raise FailClosed("live adapter cannot verify wallet SOL balance")
        balance = await balance_lookup()
        required = amount_sol + settings.live_guardrails.min_wallet_balance_sol
        if balance is None or balance < required:
            raise FailClosed(
                f"live wallet balance insufficient: balance={balance} required={required}",
            )

    async def _get_mark(self, mint_address: str) -> PriceResult:
        diagnostic_lookup = getattr(self._mark_provider, "get_price_with_diagnostic", None)
        if diagnostic_lookup is not None:
            return await diagnostic_lookup(mint_address)
        price = await self._mark_provider.get_current_price(mint_address)
        return PriceResult(price, "ok" if price is not None else "price_unavailable")

    async def _reconcile_startup(self) -> bool:
        if self._mode != "live":
            return True
        locked_mints = [mint for mint, lock in self._mint_locks.items() if lock.locked()]
        if locked_mints:
            log.info(
                "live reconciliation deferred for in-flight mints: %s",
                ", ".join(sorted(mint[:16] for mint in locked_mints)),
            )
            return False
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
        return True

    def _check_live_circuit_breaker(self) -> None:
        if self._mode != "live":
            return
        checker = getattr(self._adapter, "circuit_breaker_tripped", None)
        if checker is None:
            raise FailClosed("live adapter does not expose circuit-breaker status")
        if checker():
            raise FailClosed("live circuit breaker is tripped")

    def _raise_if_fatal(
        self,
        feed_task: asyncio.Task[None],
        *background_tasks: asyncio.Task[None],
    ) -> None:
        if self._fatal_reason:
            raise FailClosed(self._fatal_reason)
        if feed_task.done():
            exception = feed_task.exception()
            raise FailClosed(f"PumpPortal monitor stopped: {exception or 'unexpected completion'}")
        for task in background_tasks:
            if task.done():
                exception = task.exception()
                raise FailClosed(
                    f"{task.get_name()} stopped: {exception or 'unexpected completion'}"
                )

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
    return number if math.isfinite(number) and number > 0 else None


def _entry_slippage_bps(pool_sol: object) -> int | None:
    pool = _positive_float(pool_sol)
    if pool is None or pool < 5:
        return None
    return 100 if pool > 20 else 300


def _live_settings() -> Settings:
    settings = load_settings()
    return settings.model_copy(
        update={"execution": settings.execution.model_copy(update={"mode": "live"})},
    )


def _acquire_singleton_lock(lock_path: Path | None = None) -> None:
    """Prevent concurrent V2 executors from sharing Hive and the wallet."""

    global _executor_lock_handle
    if _executor_lock_handle is not None:
        return
    handle = (lock_path or EXECUTOR_LOCK_PATH).open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        log.fatal("FATAL: another memecoin executor instance is running")
        raise SystemExit(42) from None
    _executor_lock_handle = handle


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
    store = await MemecoinStore.connect()
    try:
        if mode == "paper":
            adapter: Any = PaperExecutionAdapter()
        elif mode == "live":
            amount_sol = float(os.getenv("POSITION_SIZE_SOL", "0.02"))
            settings = _live_settings()
            guardrails = evaluate_live_guardrails(settings, requested_trade_sol=amount_sol)
            if not guardrails.allowed:
                raise RuntimeError(
                    f"live startup guardrails failed: {', '.join(guardrails.diagnostics)}",
                )
            daily_caps = evaluate_daily_live_caps(
                await store.load_daily_live_state(),
                max_daily_trades=guardrails.max_daily_trades,
                max_daily_loss_sol=guardrails.max_daily_loss_sol,
                today=datetime.now(UTC).date(),
            )
            if not daily_caps.allowed:
                raise RuntimeError(
                    f"live startup daily caps failed: {', '.join(daily_caps.diagnostics)}",
                )
            adapter = build_live_adapter()
            balance = await adapter.get_sol_balance()
            required = amount_sol + settings.live_guardrails.min_wallet_balance_sol
            if balance is None or balance < required:
                await adapter.close()
                raise RuntimeError(
                    "live startup wallet balance insufficient: "
                    f"balance={balance} required={required}",
                )
        else:
            raise RuntimeError(f"EXECUTION_MODE must be paper or live, got {mode!r}")
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
    _acquire_singleton_lock()
    asyncio.run(_run())


if __name__ == "__main__":
    main()

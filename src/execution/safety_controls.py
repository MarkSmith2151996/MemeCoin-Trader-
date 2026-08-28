"""Safety controls for live trading: circuit breaker + kill switch (MT-546).

Circuit breaker
---------------
A persistent, file-backed flag that trips when a live SELL fails to execute
(Jupiter swap error, transaction expired after retries, or confirmation
timeout). While tripped, the live adapter refuses new buys so no fresh capital
enters the market. Existing open positions continue to be managed — sells are
NOT blocked by the breaker, only buys. Manual reset via ``scripts/reset_breaker.py``.

Both mechanisms are live-mode only: the paper adapter never consults the
breaker, and the kill switch refuses to act unless ``EXECUTION_MODE`` is
``live``.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

log = logging.getLogger("safety_controls")

KILL_SWITCH_SLIPPAGE_BPS = 500
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BREAKER_PATH = REPOSITORY_ROOT / "data" / "circuit_breaker.json"
MANUAL_RESET_CONFIRMATION = "MANUAL_RESET"


@dataclass(frozen=True, slots=True)
class BreakerState:
    """Snapshot of the circuit breaker flag file."""

    tripped: bool
    reason: str | None = None
    mint: str | None = None
    signature_attempt: str | None = None
    error: str | None = None
    tripped_at: str | None = None
    last_error_at: str | None = None


def _atomic_write(path: Path, content: str) -> None:
    """Write via temp file + replace so concurrent readers never see a torn file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


class CircuitBreaker:
    """File-backed sell-failure trip flag shared across processes.

    The flag uses an absolute repository path so the V2 executor and standalone
    operator scripts always observe the same state. Missing state is clear;
    unreadable or corrupt existing state is tripped in live mode because its
    state is unknown. A trip remains latched until an explicit manual reset.
    """

    def __init__(
        self,
        *,
        flag_path: str | Path = DEFAULT_BREAKER_PATH,
        execution_mode: str = "live",
    ) -> None:
        self._path = Path(flag_path)
        self._execution_mode = execution_mode.strip().lower()

    def _read_state(self) -> BreakerState:
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return BreakerState(tripped=False)
        except OSError as exc:
            if self._execution_mode == "live":
                log.error("CIRCUIT BREAKER flag %s unreadable — treating as tripped", self._path)
                return BreakerState(tripped=True, reason="breaker_state_unreadable", error=str(exc))
            log.warning(
                "CIRCUIT BREAKER flag %s unreadable — paper mode treats it as clear",
                self._path,
            )
            return BreakerState(tripped=False)
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            log.error("CIRCUIT BREAKER flag %s corrupt — treating as tripped", self._path)
            return BreakerState(
                tripped=self._execution_mode == "live",
                reason="breaker_state_corrupt" if self._execution_mode == "live" else None,
            )
        return BreakerState(
            tripped=bool(data.get("tripped", False)),
            reason=data.get("reason"),
            mint=data.get("mint"),
            signature_attempt=data.get("signature_attempt"),
            error=data.get("error"),
            tripped_at=data.get("tripped_at"),
            last_error_at=data.get("last_error_at"),
        )

    @staticmethod
    def _payload(state: BreakerState) -> dict[str, object | None]:
        return {
            "tripped": state.tripped,
            "reason": state.reason,
            "mint": state.mint,
            "signature_attempt": state.signature_attempt,
            "error": state.error,
            "tripped_at": state.tripped_at,
            "last_error_at": state.last_error_at,
        }

    def status(self) -> BreakerState:
        return self._read_state()

    def is_tripped(self) -> bool:
        return self.status().tripped

    def trip(
        self,
        *,
        error: str,
        mint: str | None = None,
        signature_attempt: str | None = None,
        reason: str = "sell_failure",
    ) -> BreakerState:
        """Set the trip flag, preserving an existing trip if already set."""
        existing = self.status()
        if existing.tripped:
            updated = replace(existing, last_error_at=datetime.now(UTC).isoformat())
            _atomic_write(self._path, json.dumps(self._payload(updated), indent=2) + "\n")
            log.warning(
                "CIRCUIT BREAKER already tripped (reason=%s) — "
                "keeping original state and refreshing cooldown",
                existing.reason or "?",
            )
            return updated
        now = datetime.now(UTC).isoformat()
        state = BreakerState(
            tripped=True,
            reason=reason,
            mint=mint,
            signature_attempt=signature_attempt,
            error=error,
            tripped_at=now,
            last_error_at=now,
        )
        _atomic_write(self._path, json.dumps(self._payload(state), indent=2) + "\n")
        log.critical(
            "CIRCUIT BREAKER TRIPPED reason=%s mint=%s signature_attempt=%s error=%s",
            reason, mint or "-", signature_attempt or "-", error,
        )
        return state

    def reset(self, *, confirm: str) -> BreakerState:
        """Clear the trip flag only after an explicit manual confirmation."""

        if confirm != MANUAL_RESET_CONFIRMATION:
            raise ValueError(
                "circuit breaker reset requires "
                f"confirm={MANUAL_RESET_CONFIRMATION!r}",
            )
        state = self.status()
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.error("CIRCUIT BREAKER reset failed: %s", exc)
            raise
        if state.tripped:
            log.warning(
                "CIRCUIT BREAKER reset (reason=%s mint=%s) — new buys enabled again",
                state.reason or "?", state.mint or "-",
            )
        return state


# ── EXECUTION_MODE helpers ───────────────────────────────────────────

def read_execution_mode(env_path: str | Path) -> str:
    """Read ``EXECUTION_MODE`` from the .env file (missing → ``paper``)."""
    path = Path(env_path)
    if not path.is_file():
        return "paper"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return "paper"
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("EXECUTION_MODE=") and not stripped.startswith("#"):
            value = stripped.split("=", 1)[1].strip().strip('"').strip("'")
            if value:
                return value.lower()
    return "paper"


def set_execution_mode(env_path: str | Path, mode: str) -> None:
    """Set ``EXECUTION_MODE`` in the .env file, preserving all other lines.

    Replaces the existing key in place or appends it when absent. Atomic write.
    """
    path = Path(env_path)
    mode = mode.strip().lower()
    if path.is_file():
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise RuntimeError(f"cannot read {path}: {exc}") from exc
    else:
        lines = []
    replaced = False
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("EXECUTION_MODE=") and not stripped.startswith("#"):
            out.append(f"EXECUTION_MODE={mode}")
            replaced = True
        else:
            out.append(line)
    if not replaced:
        out.append(f"EXECUTION_MODE={mode}")
    try:
        _atomic_write(path, "\n".join(out) + "\n")
    except OSError as exc:
        raise RuntimeError(f"cannot write {path}: {exc}") from exc
    log.info("EXECUTION_MODE set to %s in %s", mode, path)


def set_env_value(env_path: str | Path, name: str, value: str) -> None:
    """Set one environment-file key while preserving all unrelated entries."""

    path = Path(env_path)
    if path.is_file():
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise RuntimeError(f"cannot read {path}: {exc}") from exc
    else:
        lines = []
    prefix = f"{name}="
    replaced = False
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(prefix) and not stripped.startswith("#"):
            out.append(f"{name}={value}")
            replaced = True
        else:
            out.append(line)
    if not replaced:
        out.append(f"{name}={value}")
    try:
        _atomic_write(path, "\n".join(out) + "\n")
    except OSError as exc:
        raise RuntimeError(f"cannot write {path}: {exc}") from exc
    log.info("%s set in %s", name, path)


# ── Kill switch ──────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class KillSwitchSummary:
    mode_before: str
    mode_after: str
    breaker_tripped: bool
    positions_found: int
    sold: int
    failed: int
    details: tuple[str, ...]


class KillSwitchNotArmedError(RuntimeError):
    """Raised when the kill switch runs while EXECUTION_MODE is not live."""


class KillSwitch:
    """Standalone liquidation path: halt live trading and sell everything.

    Order of operations:
    1. Refuse to run unless ``EXECUTION_MODE`` is ``live`` (live-mode only).
    2. Write ``EXECUTION_MODE=paper`` into the .env file so restarts stay paper.
    3. Trip the circuit breaker so the running live process stops new buys
       immediately (same flag the live adapter checks).
    4. Sell every open live position through the injected adapter at market
       with a wider slippage tolerance, recording fills and closing positions.
    """

    def __init__(
        self,
        *,
        env_path: str | Path = ".env",
        breaker: CircuitBreaker | None = None,
        adapter=None,
        slippage_bps: int = KILL_SWITCH_SLIPPAGE_BPS,
    ) -> None:
        self._env_path = Path(env_path)
        self._breaker = breaker if breaker is not None else CircuitBreaker()
        self._adapter = adapter
        self._slippage_bps = slippage_bps

    async def run(
        self,
        *,
        positions: list | None = None,
        db_path: str | Path | None = None,
        manager=None,
    ) -> KillSwitchSummary:
        """Execute the kill switch. Returns a summary; never raises on sell
        failures (every failure is logged and counted instead)."""
        mode_before = read_execution_mode(self._env_path)
        if mode_before != "live":
            raise KillSwitchNotArmedError(
                f"kill switch is live-mode only — EXECUTION_MODE is '{mode_before}'",
            )
        if self._adapter is None:
            raise RuntimeError("kill switch requires an execution adapter")
        if getattr(self._adapter, "mode", None) != "live":
            raise RuntimeError("kill switch requires a live execution adapter")

        set_execution_mode(self._env_path, "paper")
        self._breaker.trip(
            mint=None,
            error="kill switch triggered — live trading halted",
            reason="kill_switch",
        )

        positions = list(positions or ())
        sold = 0
        failed = 0
        details: list[str] = []
        for position in positions:
            token_amount = getattr(position, "remaining_token_amount", None)
            if token_amount is None or token_amount <= 0:
                token_amount = position.token_amount
            mint = position.mint_address
            try:
                trade = await self._adapter.sell(mint, token_amount, self._slippage_bps)
            except Exception as exc:
                failed += 1
                details.append(f"FAIL {mint[:16]}: {exc}")
                log.error(
                    "KILL SWITCH SELL FAILED mint=%s amount=%.8f: %s",
                    mint[:16], token_amount, exc,
                )
                continue
            sold += 1
            details.append(
                f"OK {mint[:16]} sig={trade.tx_signature or '-'} "
                f"price={trade.price_sol} sol={trade.amount_sol}",
            )
            log.info(
                "KILL SWITCH SELL OK mint=%s sig=%s price=%s sol_out=%s",
                mint[:16], trade.tx_signature or "-", trade.price_sol, trade.amount_sol,
            )
            if db_path is not None:
                try:
                    from src.core.database import record_trade

                    await record_trade(db_path, trade)
                except Exception as exc:
                    log.warning("KILL SWITCH record_trade failed mint=%s: %s", mint[:16], exc)
            if manager is not None:
                try:
                    await manager.close_position(
                        mint, exit_price_sol=trade.price_sol, mode="live",
                    )
                except Exception as exc:
                    log.warning("KILL SWITCH close_position failed mint=%s: %s", mint[:16], exc)

        summary = KillSwitchSummary(
            mode_before=mode_before,
            mode_after="paper",
            breaker_tripped=True,
            positions_found=len(positions),
            sold=sold,
            failed=failed,
            details=tuple(details),
        )
        log.info(
            "KILL SWITCH COMPLETE mode=%s→%s breaker=tripped positions=%d sold=%d failed=%d",
            mode_before, "paper", len(positions), sold, failed,
        )
        return summary


class V2KillSwitch:
    """Hive-backed live liquidation path for the V2 executor.

    The permanent breaker and halt marker are written before the first sell so
    the running executor stops entries even if a liquidation submission fails.
    Each position is closed in Hive only after the live adapter confirms the
    wallet no longer holds the token.
    """

    def __init__(
        self,
        *,
        store: Any,
        adapter: Any,
        env_path: str | Path = ".env",
        breaker: CircuitBreaker | None = None,
        halt_path: str | Path = "/tmp/memecoin-executor-halted",
        alert_manager: Any | None = None,
        slippage_bps: int = KILL_SWITCH_SLIPPAGE_BPS,
    ) -> None:
        self._store = store
        self._adapter = adapter
        self._env_path = Path(env_path)
        self._breaker = breaker if breaker is not None else CircuitBreaker()
        self._halt_path = Path(halt_path)
        self._alerts = alert_manager
        self._slippage_bps = slippage_bps

    async def run(self) -> KillSwitchSummary:
        """Liquidate every open live Hive position and leave entries disabled."""

        mode_before = read_execution_mode(self._env_path)
        if mode_before != "live":
            raise KillSwitchNotArmedError(
                f"kill switch is live-mode only — EXECUTION_MODE is '{mode_before}'",
            )
        if getattr(self._adapter, "mode", None) != "live":
            raise RuntimeError("kill switch requires a live execution adapter")

        # These latches must take effect before any external liquidation call.
        set_env_value(self._env_path, "LIVE_KILL_SWITCH", "true")
        set_execution_mode(self._env_path, "paper")
        self._breaker.trip(
            error="kill switch triggered — live trading halted",
            reason="kill_switch",
        )
        self._write_halt("kill switch triggered")

        positions = await self._store.list_open_live_positions()
        sold = 0
        failed = 0
        details: list[str] = []
        for position in positions:
            mint = str(position["mint_address"])
            token_amount = float(position.get("token_amount") or 0)
            if token_amount <= 0:
                failed += 1
                details.append(f"FAIL {mint[:16]}: no persisted token amount")
                continue
            try:
                trade = await self._adapter.sell(mint, token_amount, self._slippage_bps)
                verify_cleared = getattr(self._adapter, "verify_token_balance_cleared", None)
                if verify_cleared is None:
                    raise RuntimeError("live adapter cannot verify post-sell token balance")
                await verify_cleared(mint)
                close_price = float(getattr(trade, "price_sol", 0) or 0)
                if close_price <= 0:
                    raise RuntimeError("live sell returned no actual fill price")
                amount_sol = float(position["amount_sol"])
                realized_pnl = float(getattr(trade, "amount_sol", 0)) - amount_sol
                trade.metadata = {
                    **dict(getattr(trade, "metadata", {})),
                    "close_reason": "kill_switch",
                    "kill_switch": True,
                }
                await self._store.close_position(
                    position,
                    _trade_record(trade),
                    close_price_sol=close_price,
                    close_reason="kill_switch",
                    realized_pnl_sol=realized_pnl,
                )
                sold += 1
                details.append(f"OK {mint[:16]} sig={getattr(trade, 'tx_signature', None) or '-'}")
                log.info("V2 KILL SWITCH SELL OK mint=%s", mint[:16])
            except Exception as exc:  # Keep liquidating the remaining positions.
                failed += 1
                details.append(f"FAIL {mint[:16]}: {exc}")
                log.critical("V2 KILL SWITCH SELL FAILED mint=%s: %s", mint[:16], exc)

        if failed and self._alerts is not None:
            try:
                await self._alerts.send(
                    "critical",
                    "V2 kill switch incomplete",
                    "Remaining positions: " + "; ".join(
                        detail for detail in details if detail.startswith("FAIL ")
                    ),
                )
            except Exception as exc:
                log.critical("V2 kill switch alert failed: %s", exc)

        return KillSwitchSummary(
            mode_before=mode_before,
            mode_after="paper",
            breaker_tripped=True,
            positions_found=len(positions),
            sold=sold,
            failed=failed,
            details=tuple(details),
        )

    def _write_halt(self, reason: str) -> None:
        payload = json.dumps({"halted_at": datetime.now(UTC).isoformat(), "reason": reason})
        try:
            _atomic_write(self._halt_path, payload + "\n")
        except OSError as exc:
            log.critical("V2 kill switch could not write halt marker %s: %s", self._halt_path, exc)


def _trade_record(trade: Any) -> dict[str, object]:
    """Normalize a live fill for the V2 Hive trades table without importing services."""

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

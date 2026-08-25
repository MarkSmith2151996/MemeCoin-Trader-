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

log = logging.getLogger("safety_controls")

KILL_SWITCH_SLIPPAGE_BPS = 500
DEFAULT_BREAKER_PATH = Path("data/circuit_breaker.json")
DEFAULT_BREAKER_COOLDOWN_SECONDS = 30 * 60


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

    The flag lives in ``data/circuit_breaker.json`` so the Strategy B process
    and the standalone kill switch / reset scripts observe the same state.
    Missing and corrupt state is treated as clear. An unreadable existing flag
    is treated as tripped in live mode, because its state is unknown.
    """

    def __init__(
        self,
        *,
        flag_path: str | Path = DEFAULT_BREAKER_PATH,
        cooldown_seconds: int | None = None,
        execution_mode: str = "live",
    ) -> None:
        self._path = Path(flag_path)
        self._execution_mode = execution_mode.strip().lower()
        if cooldown_seconds is None:
            try:
                cooldown_seconds = int(
                    os.getenv(
                        "CIRCUIT_BREAKER_COOLDOWN_SECONDS",
                        str(DEFAULT_BREAKER_COOLDOWN_SECONDS),
                    ),
                )
            except ValueError:
                cooldown_seconds = DEFAULT_BREAKER_COOLDOWN_SECONDS
        self._cooldown_seconds = max(0, cooldown_seconds)

    def _read_state(self) -> BreakerState:
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return BreakerState(tripped=False)
        except OSError as exc:
            if self._execution_mode == "live":
                log.error("CIRCUIT BREAKER flag %s unreadable — treating as tripped", self._path)
                return BreakerState(tripped=True, reason="breaker_state_unreadable", error=str(exc))
            log.warning("CIRCUIT BREAKER flag %s unreadable — paper mode treats it as clear", self._path)
            return BreakerState(tripped=False)
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            log.error("CIRCUIT BREAKER flag %s corrupt — treating as clear", self._path)
            return BreakerState(tripped=False)
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

    def _auto_reset_if_due(self, state: BreakerState) -> bool:
        if not state.tripped or self._cooldown_seconds <= 0:
            return False
        timestamp = state.last_error_at or state.tripped_at
        if not timestamp:
            return False
        try:
            last_error_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return False
        if last_error_at.tzinfo is None:
            last_error_at = last_error_at.replace(tzinfo=UTC)
        # Status checks are non-blocking. The runtime's normal 100ms polling
        # observes elapsed time instead of sleeping through the cooldown.
        elapsed = (datetime.now(UTC) - last_error_at).total_seconds()
        if elapsed < self._cooldown_seconds:
            return False
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.error("CIRCUIT BREAKER auto-reset failed: %s", exc)
            return False
        log.warning(
            "CIRCUIT BREAKER AUTO-RESET after %ds without a new error "
            "(reason=%s mint=%s)",
            self._cooldown_seconds,
            state.reason or "?",
            state.mint or "-",
        )
        return True

    def status(self) -> BreakerState:
        state = self._read_state()
        if self._auto_reset_if_due(state):
            return BreakerState(tripped=False)
        return state

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

    def reset(self) -> BreakerState:
        """Clear the trip flag. Returns the pre-reset state."""
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

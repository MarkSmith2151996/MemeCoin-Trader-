"""Read-only sell simulation adapter for honeypot and sell-block detection."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field

import httpx

SimulationProvider = Callable[[str, "HoneypotSimulationRequest"], Awaitable[object]]

SUPPORTED_BACKENDS = {"helius", "blowfish"}


@dataclass(slots=True)
class HoneypotSimulationRequest:
    mint_address: str
    transaction_payload: str | bytes | None
    backend: str = "helius"
    parsed_instructions: Sequence[Mapping[str, object]] = field(default_factory=tuple)


@dataclass(slots=True)
class HoneypotSimulationResult:
    ok: bool
    sell_simulation_passed: bool
    blocked_reason: str | None = None
    transfer_hook_detected: bool = False
    suspicious_instruction_detected: bool = False
    provider_error: str | None = None
    provider_status: str = "unknown"
    backend: str = "helius"


class HoneypotSimulationAdapter:
    """Normalize provider-backed sell simulation into a small read-only result."""

    def __init__(
        self,
        *,
        provider: SimulationProvider,
        default_backend: str = "helius",
    ) -> None:
        self._provider = provider
        self._default_backend = default_backend.strip().lower() or "helius"

    async def simulate_sell(self, request: HoneypotSimulationRequest) -> HoneypotSimulationResult:
        backend = (request.backend or self._default_backend).strip().lower()
        if backend not in SUPPORTED_BACKENDS:
            return HoneypotSimulationResult(
                ok=False,
                sell_simulation_passed=False,
                blocked_reason="unsupported backend",
                provider_error="unsupported backend",
                provider_status="unsupported_backend",
                backend=backend,
                transfer_hook_detected=self.detect_transfer_hook(request.parsed_instructions),
                suspicious_instruction_detected=self.detect_suspicious_instruction(request.parsed_instructions),
            )

        if self._missing_transaction_payload(request.transaction_payload):
            return HoneypotSimulationResult(
                ok=False,
                sell_simulation_passed=False,
                blocked_reason="missing transaction payload",
                provider_error="missing transaction payload",
                provider_status="invalid_request",
                backend=backend,
                transfer_hook_detected=self.detect_transfer_hook(request.parsed_instructions),
                suspicious_instruction_detected=self.detect_suspicious_instruction(request.parsed_instructions),
            )

        transfer_hook_detected = self.detect_transfer_hook(request.parsed_instructions)
        suspicious_instruction_detected = self.detect_suspicious_instruction(request.parsed_instructions)

        try:
            payload = await self._provider(backend, request)
        except httpx.TimeoutException:
            return HoneypotSimulationResult(
                ok=False,
                sell_simulation_passed=False,
                blocked_reason="provider timeout",
                transfer_hook_detected=transfer_hook_detected,
                suspicious_instruction_detected=suspicious_instruction_detected,
                provider_error="request timed out",
                provider_status="timeout",
                backend=backend,
            )
        except Exception as exc:
            return HoneypotSimulationResult(
                ok=False,
                sell_simulation_passed=False,
                blocked_reason="provider error",
                transfer_hook_detected=transfer_hook_detected,
                suspicious_instruction_detected=suspicious_instruction_detected,
                provider_error=type(exc).__name__,
                provider_status="provider_error",
                backend=backend,
            )

        if not isinstance(payload, Mapping):
            return HoneypotSimulationResult(
                ok=False,
                sell_simulation_passed=False,
                blocked_reason="malformed response",
                transfer_hook_detected=transfer_hook_detected,
                suspicious_instruction_detected=suspicious_instruction_detected,
                provider_error="response payload was not an object",
                provider_status="malformed_payload",
                backend=backend,
            )

        return self._normalize_result(
            payload,
            backend=backend,
            transfer_hook_detected=transfer_hook_detected,
            suspicious_instruction_detected=suspicious_instruction_detected,
        )

    def detect_transfer_hook(self, parsed_instructions: Sequence[Mapping[str, object]]) -> bool:
        for instruction in parsed_instructions:
            flattened = self._flatten_instruction(instruction)
            if "token-2022" not in flattened and "token2022" not in flattened:
                continue
            if any(hint in flattened for hint in ("transferhook", "transfer hook", "executehook", "extraaccountmeta")):
                return True
        return False

    def detect_suspicious_instruction(self, parsed_instructions: Sequence[Mapping[str, object]]) -> bool:
        suspicious_hints = (
            "blacklist",
            "cooldown",
            "maxsell",
            "max sell",
            "sellblocked",
            "sell blocked",
            "tradingdisabled",
            "trading disabled",
            "whitelist only",
            "onlywhitelisted",
        )
        for instruction in parsed_instructions:
            flattened = self._flatten_instruction(instruction)
            if any(hint in flattened for hint in suspicious_hints):
                return True
        return False

    def _normalize_result(
        self,
        payload: Mapping[str, object],
        *,
        backend: str,
        transfer_hook_detected: bool,
        suspicious_instruction_detected: bool,
    ) -> HoneypotSimulationResult:
        success = self._extract_bool(payload, ["ok", "success", "result.success", "result.value.success"])
        if success is None:
            err_value = self._extract_value(payload, ["error", "result.err", "result.value.err", "result.error"])
            if err_value is not None:
                success = False

        if success is None:
            return HoneypotSimulationResult(
                ok=False,
                sell_simulation_passed=False,
                blocked_reason="malformed response",
                transfer_hook_detected=transfer_hook_detected,
                suspicious_instruction_detected=suspicious_instruction_detected,
                provider_error="missing success indicator",
                provider_status="malformed_payload",
                backend=backend,
            )

        blocked_reason = self._extract_str(
            payload,
            [
                "blockedReason",
                "blocked_reason",
                "reason",
                "error.message",
                "error",
                "result.err",
                "result.value.err",
                "result.reason",
            ],
        )

        if success:
            return HoneypotSimulationResult(
                ok=True,
                sell_simulation_passed=True,
                blocked_reason=blocked_reason,
                transfer_hook_detected=transfer_hook_detected,
                suspicious_instruction_detected=suspicious_instruction_detected,
                provider_status="ok",
                backend=backend,
            )

        return HoneypotSimulationResult(
            ok=True,
            sell_simulation_passed=False,
            blocked_reason=blocked_reason or "simulation blocked",
            transfer_hook_detected=transfer_hook_detected,
            suspicious_instruction_detected=suspicious_instruction_detected,
            provider_status="ok",
            backend=backend,
        )

    def _missing_transaction_payload(self, payload: str | bytes | None) -> bool:
        if payload is None:
            return True
        if isinstance(payload, str):
            return not payload.strip()
        if isinstance(payload, bytes):
            return len(payload) == 0
        return True

    def _extract_bool(self, payload: Mapping[str, object], paths: Sequence[str]) -> bool | None:
        for path in paths:
            value = self._extract_value(payload, [path])
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)) and value in (0, 1):
                return bool(value)
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "ok", "success", "passed"}:
                    return True
                if lowered in {"false", "failed", "blocked", "error"}:
                    return False
        return None

    def _extract_str(self, payload: Mapping[str, object], paths: Sequence[str]) -> str | None:
        value = self._extract_value(payload, paths)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    def _extract_value(self, payload: Mapping[str, object], paths: Sequence[str]) -> object:
        for path in paths:
            current: object = payload
            for segment in path.split("."):
                if not isinstance(current, Mapping):
                    current = None
                    break
                current = current.get(segment)
            if current is not None:
                return current
        return None

    def _flatten_instruction(self, instruction: Mapping[str, object]) -> str:
        parts: list[str] = []
        stack: list[object] = [instruction]
        while stack:
            current = stack.pop()
            if isinstance(current, Mapping):
                stack.extend(current.values())
                stack.extend(current.keys())
            elif isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray)):
                stack.extend(current)
            elif isinstance(current, str):
                parts.append(current.strip().lower())
        return " ".join(parts)

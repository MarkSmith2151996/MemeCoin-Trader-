"""Read-only RugCheck client for token safety report lookups."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field

import httpx

RUGCHECK_REPORT_URL = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"

ReportFetcher = Callable[[httpx.AsyncClient, str], Awaitable[httpx.Response]]


@dataclass(slots=True)
class RugCheckResult:
    mint_address: str
    found: bool = False
    mint_authority_revoked: bool | None = None
    freeze_authority_revoked: bool | None = None
    top_holder_pct: float | None = None
    liquidity_locked: bool | None = None
    liquidity_status: str | None = None
    is_honeypot: bool | None = None
    risk_score: float | None = None
    risk_level: str | None = None
    provider_status: str = "unknown"
    error: str | None = None
    raw: dict[str, object] = field(default_factory=dict)


async def _default_fetcher(client: httpx.AsyncClient, mint_address: str) -> httpx.Response:
    return await client.get(RUGCHECK_REPORT_URL.format(mint=mint_address))


class RugCheckClient:
    def __init__(
        self,
        *,
        timeout_s: float = 10.0,
        fetcher: ReportFetcher | None = None,
    ) -> None:
        self._timeout_s = timeout_s
        self._fetcher = fetcher or _default_fetcher

    async def fetch_report(self, mint_address: str) -> RugCheckResult:
        async with httpx.AsyncClient(timeout=self._timeout_s) as client:
            return await self._fetch_report_with_client(client, mint_address)

    async def _fetch_report_with_client(
        self,
        client: httpx.AsyncClient,
        mint_address: str,
    ) -> RugCheckResult:
        normalized_mint = mint_address.strip()
        if not normalized_mint:
            return RugCheckResult(
                mint_address="",
                provider_status="invalid_request",
                error="missing mint address",
            )

        try:
            response = await self._fetcher(client, normalized_mint)
        except httpx.TimeoutException:
            return RugCheckResult(
                mint_address=normalized_mint,
                provider_status="timeout",
                error="request timed out",
            )
        except Exception as exc:
            return RugCheckResult(
                mint_address=normalized_mint,
                provider_status="provider_error",
                error=type(exc).__name__,
            )

        if response.status_code != 200:
            return RugCheckResult(
                mint_address=normalized_mint,
                provider_status=f"http_{response.status_code}",
                error="non-200 response",
            )

        try:
            payload = response.json()
        except ValueError:
            return RugCheckResult(
                mint_address=normalized_mint,
                provider_status="malformed_json",
                error="response was not valid json",
            )

        if not isinstance(payload, Mapping):
            return RugCheckResult(
                mint_address=normalized_mint,
                provider_status="malformed_payload",
                error="response payload was not an object",
            )

        return self._normalize_report(normalized_mint, payload)

    def _normalize_report(self, mint_address: str, payload: Mapping[str, object]) -> RugCheckResult:
        return RugCheckResult(
            mint_address=mint_address,
            found=True,
            mint_authority_revoked=self._extract_authority_flag(payload, "mint"),
            freeze_authority_revoked=self._extract_authority_flag(payload, "freeze"),
            top_holder_pct=self._extract_top_holder_pct(payload),
            liquidity_locked=self._extract_bool(payload, ["liquidityLocked", "liquidity_locked"]),
            liquidity_status=self._extract_str(
                payload,
                ["liquidityStatus", "liquidity_status", "liquidity.status"],
            ),
            is_honeypot=self._extract_bool(
                payload,
                ["isHoneypot", "is_honeypot", "risks.isHoneypot", "verification.honeypot"],
            ),
            risk_score=self._extract_float(
                payload,
                ["riskScore", "risk_score", "score", "scams.riskScore"],
            ),
            risk_level=self._extract_str(
                payload,
                ["riskLevel", "risk_level", "verification.riskLevel", "scams.riskLevel"],
            ),
            provider_status="ok",
            raw=dict(payload),
        )

    def _extract_authority_flag(self, payload: Mapping[str, object], authority_name: str) -> bool | None:
        revoked = self._extract_bool(
            payload,
            [
                f"tokenMeta.{authority_name}AuthorityRevoked",
                f"token.{authority_name}AuthorityRevoked",
                f"authorities.{authority_name}AuthorityRevoked",
                f"{authority_name}AuthorityRevoked",
            ],
        )
        if revoked is not None:
            return revoked

        for path in (
            f"tokenMeta.{authority_name}Authority",
            f"token.{authority_name}Authority",
            f"authorities.{authority_name}Authority",
            f"{authority_name}Authority",
        ):
            present, value = self._extract_value_with_presence(payload, path)
            if not present:
                continue
            # RugCheck represents a missing mint/freeze authority with a present null field.
            if value is None:
                return True
            enabled = self._coerce_bool(value)
            if enabled is not None:
                return not enabled
        return None

    def _extract_top_holder_pct(self, payload: Mapping[str, object]) -> float | None:
        direct = self._extract_float(
            payload,
            [
                "topHolderPct",
                "top_holder_pct",
                "topHoldersPct",
                "holderAnalysis.topHolderPct",
                "holders.topHolderPct",
            ],
        )
        if direct is not None:
            return direct

        top_holders = self._extract_value(payload, "topHolders")
        if not isinstance(top_holders, Sequence) or isinstance(top_holders, (str, bytes, bytearray)):
            return None

        percentages: list[float] = []
        for holder in top_holders:
            if not isinstance(holder, Mapping):
                continue
            pct = self._coerce_float(holder.get("pct") or holder.get("percentage") or holder.get("share"))
            if pct is not None:
                percentages.append(pct)
        if not percentages:
            return None
        return sum(percentages)

    def _extract_bool(self, payload: Mapping[str, object], paths: Sequence[str]) -> bool | None:
        for path in paths:
            value = self._extract_value(payload, path)
            parsed = self._coerce_bool(value)
            if parsed is not None:
                return parsed
        return None

    def _coerce_bool(self, value: object) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "yes", "locked", "revoked"}:
                return True
            if lowered in {"false", "no", "unlocked", "active"}:
                return False
        return None

    def _extract_float(self, payload: Mapping[str, object], paths: Sequence[str]) -> float | None:
        for path in paths:
            value = self._extract_value(payload, path)
            parsed = self._coerce_float(value)
            if parsed is not None:
                return parsed
        return None

    def _extract_str(self, payload: Mapping[str, object], paths: Sequence[str]) -> str | None:
        for path in paths:
            value = self._extract_value(payload, path)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _extract_value(self, payload: Mapping[str, object], path: str) -> object:
        current: object = payload
        for segment in path.split("."):
            if not isinstance(current, Mapping):
                return None
            current = current.get(segment)
        return current

    def _extract_value_with_presence(self, payload: Mapping[str, object], path: str) -> tuple[bool, object]:
        current: object = payload
        for segment in path.split("."):
            if not isinstance(current, Mapping) or segment not in current:
                return False, None
            current = current[segment]
        return True, current

    def _coerce_float(self, value: object) -> float | None:
        if isinstance(value, bool) or value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            stripped = value.strip().rstrip("%")
            if not stripped:
                return None
            try:
                return float(stripped)
            except ValueError:
                return None
        return None

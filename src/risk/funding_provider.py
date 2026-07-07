"""Helius-backed inbound SOL funding provider for buyer funding analysis."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx
from dotenv import dotenv_values

from src.risk.funding_analysis import InboundTransfer

HELIUS_TRANSACTIONS_URL = "https://api.helius.xyz/v0/addresses/{wallet}/transactions"
LAMPORTS_PER_SOL = 1_000_000_000

EnhancedTransactionFetcher = Callable[[httpx.AsyncClient, str, str, int], Awaitable[httpx.Response]]


@dataclass(slots=True)
class FundingProviderLookupResult:
    """Sanitized provider lookup result for one wallet."""

    wallet_address: str
    transfers: list[InboundTransfer] = field(default_factory=list)
    provider_status: str = "unknown"
    error: str | None = None
    api_key_configured: bool = False
    ignored_transfer_count: int = 0


async def _default_fetcher(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
    limit: int,
) -> httpx.Response:
    return await client.get(
        HELIUS_TRANSACTIONS_URL.format(wallet=wallet_address),
        params={
            "api-key": api_key,
            "limit": limit,
        },
    )


class HeliusFundingProvider:
    """Fetch recent inbound SOL transfers for funding-source analysis."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        dotenv_path: str | Path | None = None,
        timeout_s: float = 10.0,
        lookback_limit: int = 25,
        fetcher: EnhancedTransactionFetcher | None = None,
    ) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        self._dotenv_path = Path(dotenv_path or repo_root / ".env")
        self._api_key = self._load_api_key() if api_key is None else api_key.strip()
        self._timeout_s = timeout_s
        self._lookback_limit = max(lookback_limit, 1)
        self._fetcher = fetcher or _default_fetcher

    async def get_recent_inbound_transfers(self, wallet: str) -> Sequence[InboundTransfer] | None:
        result = await self.lookup_wallet(wallet)
        if result.provider_status != "ok":
            return None
        return result.transfers

    async def lookup_wallet(self, wallet: str) -> FundingProviderLookupResult:
        normalized_wallet = wallet.strip()
        if not normalized_wallet:
            return FundingProviderLookupResult(
                wallet_address="",
                provider_status="invalid_request",
                error="missing wallet address",
                api_key_configured=bool(self._api_key),
            )

        if not self._api_key:
            return FundingProviderLookupResult(
                wallet_address=normalized_wallet,
                provider_status="missing_api_key",
                error="missing HELIUS_API_KEY",
                api_key_configured=False,
            )

        async with httpx.AsyncClient(timeout=self._timeout_s) as client:
            try:
                response = await self._fetcher(
                    client,
                    normalized_wallet,
                    self._api_key,
                    self._lookback_limit,
                )
            except httpx.TimeoutException:
                return FundingProviderLookupResult(
                    wallet_address=normalized_wallet,
                    provider_status="timeout",
                    error="request timed out",
                    api_key_configured=True,
                )
            except Exception as exc:
                return FundingProviderLookupResult(
                    wallet_address=normalized_wallet,
                    provider_status="provider_error",
                    error=type(exc).__name__,
                    api_key_configured=True,
                )

        if response.status_code != 200:
            return FundingProviderLookupResult(
                wallet_address=normalized_wallet,
                provider_status=f"http_{response.status_code}",
                error="non-200 response",
                api_key_configured=True,
            )

        try:
            payload = response.json()
        except ValueError:
            return FundingProviderLookupResult(
                wallet_address=normalized_wallet,
                provider_status="malformed_json",
                error="response was not valid json",
                api_key_configured=True,
            )

        if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes, bytearray)):
            return FundingProviderLookupResult(
                wallet_address=normalized_wallet,
                provider_status="malformed_payload",
                error="response payload was not a transaction list",
                api_key_configured=True,
            )

        transfers: list[InboundTransfer] = []
        ignored_transfer_count = 0
        for transaction in payload:
            normalized_transfers, ignored_count = self._normalize_transaction(normalized_wallet, transaction)
            transfers.extend(normalized_transfers)
            ignored_transfer_count += ignored_count

        return FundingProviderLookupResult(
            wallet_address=normalized_wallet,
            transfers=transfers,
            provider_status="ok",
            api_key_configured=True,
            ignored_transfer_count=ignored_transfer_count,
        )

    def _load_api_key(self) -> str:
        direct_value = os.getenv("HELIUS_API_KEY", "").strip()
        if direct_value:
            return direct_value
        if not self._dotenv_path.exists():
            return ""
        dotenv_value = dotenv_values(self._dotenv_path).get("HELIUS_API_KEY")
        if isinstance(dotenv_value, str):
            return dotenv_value.strip()
        return ""

    def _normalize_transaction(
        self,
        wallet_address: str,
        transaction: object,
    ) -> tuple[list[InboundTransfer], int]:
        if not isinstance(transaction, Mapping):
            return [], 1

        native_transfers = transaction.get("nativeTransfers")
        if not isinstance(native_transfers, Sequence) or isinstance(native_transfers, (str, bytes, bytearray)):
            return [], 1

        observed_at = self._coerce_timestamp(transaction.get("timestamp"))
        signature = self._extract_signature(transaction)
        transfers: list[InboundTransfer] = []
        ignored_transfer_count = 0
        for native_transfer in native_transfers:
            normalized_transfer = self._normalize_inbound_transfer(
                wallet_address,
                native_transfer,
                observed_at=observed_at,
                signature=signature,
            )
            if normalized_transfer is None:
                ignored_transfer_count += 1
                continue
            transfers.append(normalized_transfer)
        return transfers, ignored_transfer_count

    def _normalize_inbound_transfer(
        self,
        wallet_address: str,
        transfer: object,
        *,
        observed_at: datetime | None,
        signature: str | None,
    ) -> InboundTransfer | None:
        if not isinstance(transfer, Mapping):
            return None

        destination_candidates = self._normalize_candidates(
            transfer.get("toUserAccount"),
            transfer.get("toAccount"),
            transfer.get("destinationUserAccount"),
            transfer.get("destinationAccount"),
        )
        if wallet_address not in destination_candidates:
            return None

        source_candidates = self._normalize_candidates(
            transfer.get("fromUserAccount"),
            transfer.get("fromAccount"),
            transfer.get("sourceUserAccount"),
            transfer.get("sourceAccount"),
        )
        source_wallet = next((candidate for candidate in sorted(source_candidates) if candidate != wallet_address), None)
        if source_wallet is None:
            return None

        amount_sol = self._extract_sol_amount(transfer)
        if amount_sol is None or amount_sol <= 0:
            return None

        return InboundTransfer(
            source_wallet=source_wallet,
            observed_at=observed_at,
            amount_sol=amount_sol,
            signature=signature,
        )

    def _extract_signature(self, transaction: Mapping[str, object]) -> str | None:
        signature = transaction.get("signature")
        if isinstance(signature, str) and signature.strip():
            return signature.strip()

        signatures = transaction.get("signatures")
        if isinstance(signatures, Sequence) and not isinstance(signatures, (str, bytes, bytearray)):
            for candidate in signatures:
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        return None

    def _coerce_timestamp(self, value: object) -> datetime | None:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                return datetime.fromtimestamp(float(value), tz=UTC)
            except (OverflowError, OSError, ValueError):
                return None
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return None
            try:
                parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        return None

    def _extract_sol_amount(self, transfer: Mapping[str, object]) -> float | None:
        for key in ("amount", "lamports"):
            value = transfer.get(key)
            if isinstance(value, bool) or value is None:
                continue
            if isinstance(value, (int, float)):
                return float(value) / LAMPORTS_PER_SOL
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    continue
                try:
                    return float(stripped) / LAMPORTS_PER_SOL
                except ValueError:
                    continue
        return None

    def _normalize_candidates(self, *values: object) -> set[str]:
        normalized: set[str] = set()
        for value in values:
            normalized.update(self._flatten_strings(value))
        return normalized

    def _flatten_strings(self, value: object) -> set[str]:
        if isinstance(value, str):
            stripped = value.strip()
            return {stripped} if stripped else set()
        if isinstance(value, Mapping):
            normalized: set[str] = set()
            for nested_value in value.values():
                normalized.update(self._flatten_strings(nested_value))
            return normalized
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
            normalized: set[str] = set()
            for nested_value in value:
                normalized.update(self._flatten_strings(nested_value))
            return normalized
        return set()

"""Helius-backed whale wallet signal source."""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import httpx
import yaml

from src.core.models import Signal
from src.core.models import SignalSource as SignalSourceEnum
from src.core.models import SignalType
from src.signals.base import SignalSource


TIER_WEIGHTS: dict[str, float] = {
    "S": 1.0,
    "A": 0.85,
    "B": 0.7,
    "C": 0.55,
}


@dataclass(slots=True)
class TrackedWallet:
    address: str
    label: str
    tier: str
    enabled: bool = True


class WhaleWalletTracker(SignalSource):
    def __init__(
        self,
        wallets_config_path: str | Path | None = None,
        api_key: str | None = None,
        poll_limit: int = 25,
        timeout_s: float = 15.0,
    ) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        self._wallets_config_path = Path(wallets_config_path or repo_root / "config/wallets_to_track.yaml")
        self._api_key = api_key or os.getenv("HELIUS_API_KEY", "").strip()
        self._poll_limit = max(poll_limit, 1)
        self._timeout_s = timeout_s
        self._client: httpx.AsyncClient | None = None
        self._wallets: list[TrackedWallet] = []
        self._seen_signatures: set[str] = set()
        self._wallet_positions: dict[str, set[str]] = {}

    @property
    def name(self) -> str:
        return "whale_tracker"

    async def start(self) -> None:
        self._wallets = self._load_wallets()
        self._wallet_positions = {wallet.address: set() for wallet in self._wallets}
        if self._api_key and self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout_s)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def poll(self) -> list[Signal]:
        if not self._api_key:
            return []
        if not self._wallets:
            self._wallets = self._load_wallets()
            self._wallet_positions.setdefault("", set())
            self._wallet_positions.pop("", None)
            for wallet in self._wallets:
                self._wallet_positions.setdefault(wallet.address, set())
        if not self._wallets:
            return []
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout_s)

        signals: list[Signal] = []
        for wallet in self._wallets:
            if not wallet.enabled:
                continue
            transactions = await self._fetch_transactions(wallet.address)
            for transaction in transactions:
                signature = self._extract_signature(transaction)
                if not signature or signature in self._seen_signatures:
                    continue
                signal = self._build_signal(wallet, transaction)
                self._seen_signatures.add(signature)
                if signal is not None:
                    signals.append(signal)
        return signals

    def _load_wallets(self) -> list[TrackedWallet]:
        if not self._wallets_config_path.exists():
            return []

        data = yaml.safe_load(self._wallets_config_path.read_text(encoding="utf-8")) or {}
        wallets_data = data.get("wallets", [])
        wallets: list[TrackedWallet] = []
        for entry in wallets_data:
            if not isinstance(entry, dict):
                continue
            address = str(entry.get("address", "")).strip()
            label = str(entry.get("label", address or "wallet")).strip() or "wallet"
            tier = str(entry.get("tier", "C")).strip().upper() or "C"
            enabled = bool(entry.get("enabled", True))
            if not address or address.startswith("<placeholder"):
                continue
            wallets.append(TrackedWallet(address=address, label=label, tier=tier, enabled=enabled))
        return wallets

    async def _fetch_transactions(self, wallet_address: str) -> list[dict[str, object]]:
        if self._client is None:
            return []

        response = await self._client.get(
            f"https://api.helius.xyz/v0/addresses/{wallet_address}/transactions",
            params={"api-key": self._api_key, "limit": self._poll_limit},
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    def _build_signal(self, wallet: TrackedWallet, transaction: dict[str, object]) -> Signal | None:
        transfers = transaction.get("tokenTransfers")
        if not isinstance(transfers, list):
            return None

        for transfer in transfers:
            if not isinstance(transfer, dict):
                continue
            if not self._is_wallet_buy(wallet.address, transfer):
                continue
            mint_address = str(transfer.get("mint", "")).strip()
            if not mint_address:
                continue

            known_positions = self._wallet_positions.setdefault(wallet.address, set())
            is_new_position = mint_address not in known_positions
            known_positions.add(mint_address)
            amount = self._extract_token_amount(transfer)
            confidence = self._score_signal(wallet.tier, amount, is_new_position)
            message = self._build_message(wallet, mint_address, amount, is_new_position)

            payload = dict(transaction)
            payload.update(
                {
                    "tracked_wallet": {
                        "address": wallet.address,
                        "label": wallet.label,
                        "tier": wallet.tier,
                    },
                    "token_transfer": transfer,
                    "heuristics": {
                        "is_new_position": is_new_position,
                        "token_amount": amount,
                    },
                    "webhook_todo": "Polling-first implementation. Add webhook ingestion when a public callback endpoint exists.",
                }
            )

            return Signal(
                source=SignalSourceEnum.WHALE_TRACKER,
                type=SignalType.BUY,
                mint_address=mint_address,
                confidence=confidence,
                message=message,
                payload=payload,
            )
        return None

    def _is_wallet_buy(self, wallet_address: str, transfer: dict[str, object]) -> bool:
        destination_candidates = self._normalize_candidates(
            transfer.get("toUserAccount"),
            transfer.get("toTokenAccount"),
            transfer.get("destinationOwner"),
            transfer.get("destinationTokenAccount"),
        )
        if wallet_address not in destination_candidates:
            return False

        source_candidates = self._normalize_candidates(
            transfer.get("fromUserAccount"),
            transfer.get("fromTokenAccount"),
            transfer.get("sourceOwner"),
            transfer.get("sourceTokenAccount"),
        )
        if wallet_address in source_candidates:
            return False
        return True

    def _score_signal(self, tier: str, amount: float, is_new_position: bool) -> float:
        tier_weight = TIER_WEIGHTS.get(tier.upper(), 0.45)
        size_score = min(amount / 10_000, 0.2)
        new_position_bonus = 0.15 if is_new_position else 0.05
        return min(max(tier_weight * 0.65 + size_score + new_position_bonus, 0.0), 1.0)

    def _build_message(
        self,
        wallet: TrackedWallet,
        mint_address: str,
        amount: float,
        is_new_position: bool,
    ) -> str:
        action = "opened" if is_new_position else "accumulated"
        if amount > 0:
            return f"{wallet.label} {action} {mint_address} with ~{amount:.4f} tokens"
        return f"{wallet.label} {action} {mint_address}"

    def _extract_signature(self, transaction: dict[str, object]) -> str:
        signature = transaction.get("signature")
        if isinstance(signature, str):
            return signature
        signatures = transaction.get("signatures")
        if isinstance(signatures, list) and signatures:
            first_signature = signatures[0]
            if isinstance(first_signature, str):
                return first_signature
        return ""

    def _extract_token_amount(self, transfer: dict[str, object]) -> float:
        token_amount = transfer.get("tokenAmount")
        if isinstance(token_amount, dict):
            for key in ("uiAmount", "tokenAmount", "amount"):
                candidate = token_amount.get(key)
                try:
                    if candidate is not None:
                        return float(candidate)
                except (TypeError, ValueError):
                    continue
        try:
            if token_amount is not None:
                return float(token_amount)
        except (TypeError, ValueError):
            pass
        return 0.0

    def _normalize_candidates(self, *values: object) -> set[str]:
        normalized: set[str] = set()
        for value in values:
            normalized.update(self._flatten_strings(value))
        return normalized

    def _flatten_strings(self, value: object) -> set[str]:
        if isinstance(value, str):
            stripped = value.strip()
            return {stripped} if stripped else set()
        if isinstance(value, dict):
            normalized: set[str] = set()
            for nested_value in value.values():
                normalized.update(self._flatten_strings(nested_value))
            return normalized
        if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
            normalized: set[str] = set()
            for nested_value in value:
                normalized.update(self._flatten_strings(nested_value))
            return normalized
        return set()


class WhaleTrackerSignalSource(WhaleWalletTracker):
    """Backward-compatible class name for existing imports."""

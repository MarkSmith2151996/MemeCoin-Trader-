"""Helius-backed whale wallet signal source."""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import httpx
import yaml
from dotenv import dotenv_values

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
        dotenv_path: str | Path | None = None,
        api_key: str | None = None,
        poll_limit: int = 25,
        timeout_s: float = 15.0,
    ) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        self._wallets_config_path = Path(wallets_config_path or repo_root / "config/wallets_to_track.yaml")
        self._dotenv_path = Path(dotenv_path or repo_root / ".env")
        self._api_key = self._load_api_key() if api_key is None else api_key
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
            params={
                "api-key": self._api_key,
                "limit": self._poll_limit,
                "token-accounts": "balanceChanged",
            },
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

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


# ---------------------------------------------------------------------------
# Standalone signal-layer functions for whale tracking + launchpad coverage
# ---------------------------------------------------------------------------
# These functions are used by scripts/run_whale_tracker.py and can be imported
# independently by strategy loops. They load from config/tracked_wallets.json
# (the MT-490 wallet list) and use Helius, Birdeye, Solscan, and DexScreener.

import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger("whale_tracker.signals")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TRACKED_WALLETS_PATH = PROJECT_ROOT / "config" / "tracked_wallets.json"

HELIUS_API_KEY: str = (os.getenv("HELIUS_API_KEY") or "").strip()
BIRDEYE_API_KEY: str = (os.getenv("BIRDEYE_API_KEY") or "").strip()
SOLSCAN_API_KEY: str = (os.getenv("SOLSCAN_API_KEY") or "").strip()

BELIEVE_TOKEN_AUTHORITY = "5qWya6UjwWnGVhdSBL3hyZ7B45jbk6Byt1hwd7ohEGXE"
BELIEVE_PROGRAM = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"
MOONSHOT_PROGRAM = "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG"

WHALE_SIZE_MULTIPLIERS: dict[int, float] = {
    0: 1.0,
    1: 2.0,
    2: 4.0,
    3: 6.0,
}

DEXSCREENER_SEARCH = "https://api.dexscreener.com/latest/dex/search"


def load_tracked_wallets() -> list[dict[str, Any]]:
    """Load tracked wallets from config/tracked_wallets.json.

    Returns a list of wallet dicts with address, label, score,
    coins_early_on, total_trades, and unique_tokens_traded fields.
    Returns an empty list if the file is missing or invalid.
    """
    if not TRACKED_WALLETS_PATH.exists():
        _log.warning("Tracked wallets config not found: %s", TRACKED_WALLETS_PATH)
        return []
    try:
        raw = TRACKED_WALLETS_PATH.read_text(encoding="utf-8")
        wallets: list[dict[str, Any]] = json.loads(raw)
        if not isinstance(wallets, list):
            _log.warning("tracked_wallets.json root is not a list")
            return []
        return wallets
    except (json.JSONDecodeError, OSError) as exc:
        _log.warning("Failed to load tracked wallets: %s", exc)
        return []


async def get_recent_wallet_transactions(
    address: str,
    http: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    """Fetch recent swap transactions for a wallet via Helius enhanced API.

    Returns a list of dicts with signature, timestamp, token_mint,
    amount_sol, and source (DEX name).
    """
    if not HELIUS_API_KEY:
        _log.warning("HELIUS_API_KEY not set")
        return []

    url = f"https://api.helius.xyz/v0/addresses/{address}/transactions"
    try:
        response = await http.get(
            url,
            params={
                "api-key": HELIUS_API_KEY,
                "limit": 10,
                "type": "SWAP",
            },
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
        _log.warning("Helius HTTP %s for %s", exc.response.status_code, address)
        return []
    except (httpx.RequestError, ValueError) as exc:
        _log.warning("Helius request failed for %s: %s", address, exc)
        return []

    if not isinstance(payload, list):
        return []

    results: list[dict[str, Any]] = []
    for tx in payload:
        if not isinstance(tx, dict):
            continue
        signature = tx.get("signature") or ""
        timestamp = tx.get("timestamp")
        if not signature or not timestamp:
            continue

        token_mint = ""
        amount_sol = 0.0
        source = tx.get("source", "")
        token_transfers = tx.get("tokenTransfers")
        if isinstance(token_transfers, list):
            for ttf in token_transfers:
                if not isinstance(ttf, dict):
                    continue
                mint = (ttf.get("mint") or "").strip()
                if mint and len(mint) > 20:
                    token_mint = mint
                raw_amount = ttf.get("rawTokenAmount")
                if isinstance(raw_amount, dict):
                    try:
                        amount_sol = float(raw_amount.get("tokenAmount", 0))
                    except (TypeError, ValueError):
                        pass
                break

        results.append({
            "signature": str(signature),
            "timestamp": int(timestamp) if isinstance(timestamp, (int, float)) else 0,
            "token_mint": token_mint,
            "amount_sol": amount_sol,
            "source": str(source) if source else "unknown",
        })

    return results


async def enrich_wallet_pnl(
    address: str,
    http: httpx.AsyncClient,
) -> dict[str, Any]:
    """Fetch wallet PnL stats from Birdeye, falling back to Solscan.

    Returns {address, estimated_pnl_sol, win_rate, data_source}.
    All numeric fields default to None on failure.
    The ``debug_response`` key contains the raw response status + body
    when a provider is configured but returns non-2xx.
    """
    result: dict[str, Any] = {
        "address": address,
        "estimated_pnl_sol": None,
        "win_rate": None,
        "data_source": None,
    }

    # -- Try Birdeye --
    if BIRDEYE_API_KEY:
        tried_endpoints: list[str] = []
        for attempt_url, attempt_headers, label in [
            (
                f"https://public-api.birdeye.so/v1/wallet/token_list?wallet={address}&chain=solana",
                {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": "solana"},
                "v1/wallet/token_list (chain=solana)",
            ),
            (
                f"https://public-api.birdeye.so/v1/portfolio?address={address}",
                {"X-API-KEY": BIRDEYE_API_KEY},
                "v1/portfolio",
            ),
        ]:
            tried_endpoints.append(label)
            try:
                resp = await http.get(attempt_url, headers=attempt_headers)
                if resp.status_code == 200 and resp.is_success:
                    data = resp.json()
                    items = data.get("data", []) if isinstance(data, dict) else []
                    if isinstance(items, list):
                        total_pnl = 0.0
                        wins = 0
                        total = 0
                        for item in items:
                            if not isinstance(item, dict):
                                continue
                            pnl = item.get("realizedPnl")
                            if pnl is not None:
                                try:
                                    total_pnl += float(pnl)
                                    total += 1
                                    if float(pnl) > 0:
                                        wins += 1
                                except (TypeError, ValueError):
                                    pass
                        result["estimated_pnl_sol"] = round(total_pnl, 6)
                        result["win_rate"] = round(wins / total, 4) if total > 0 else None
                        result["data_source"] = "birdeye"
                    return result
                _log.warning(
                    "Birdeye %s HTTP %s for %s: %.200s",
                    label, resp.status_code, address,
                    resp.text[:200].replace("\n", " "),
                )
            except (httpx.RequestError, ValueError, TypeError) as exc:
                _log.warning("Birdeye %s failed for %s: %s", label, address, exc)

        result["debug_birdeye"] = tried_endpoints

    # -- Try Solscan --
    if SOLSCAN_API_KEY:
        tried_solscan: list[str] = []
        for attempt_headers, label in [
            ({"Authorization": f"Bearer {SOLSCAN_API_KEY}"}, "Bearer token"),
            ({"token": SOLSCAN_API_KEY}, "token header"),
        ]:
            tried_solscan.append(label)
            try:
                resp = await http.get(
                    f"https://pro-api.solscan.io/v2.0/account/token-accounts?address={address}&page=1&page_size=10",
                    headers=attempt_headers,
                )
                if resp.status_code == 200 and resp.is_success:
                    data = resp.json()
                    result["data_source"] = "solscan"
                    items = data.get("data", []) if isinstance(data, dict) else []
                    if isinstance(items, list):
                        total_pnl = 0.0
                        for item in items:
                            if not isinstance(item, dict):
                                continue
                            pnl = item.get("priceChange24h") or 0
                            try:
                                total_pnl += float(pnl)
                            except (TypeError, ValueError):
                                pass
                        if total_pnl != 0.0:
                            result["estimated_pnl_sol"] = round(total_pnl, 6)
                    return result
                _log.warning(
                    "Solscan %s HTTP %s for %s: %.200s",
                    label, resp.status_code, address,
                    resp.text[:200].replace("\n", " "),
                )
            except (httpx.RequestError, ValueError, TypeError) as exc:
                _log.warning("Solscan %s failed for %s: %s", label, address, exc)

        result["debug_solscan"] = tried_solscan

    result["data_source"] = "none"
    return result


async def check_fresh_coin_buys(
    wallets: list[dict[str, Any]],
    http: httpx.AsyncClient,
    max_age_minutes: int = 15,
) -> list[dict[str, Any]]:
    """Check tracked wallets for fresh coin buys.

    For each wallet, fetches recent transactions via Helius, then checks
    each bought token mint on DexScreener. Returns signals for coins
    younger than max_age_minutes with >$1K liquidity.

    Deduplicates by mint — multiple whales on the same coin are merged
    into one signal with whale_count and whale_addresses list.
    """
    if not HELIUS_API_KEY:
        _log.warning("HELIUS_API_KEY not set")
        return []

    signals_by_mint: dict[str, dict[str, Any]] = {}
    now = datetime.now(timezone.utc)

    for wallet in wallets:
        address = wallet.get("address", "")
        if not address:
            continue

        txs = await get_recent_wallet_transactions(address, http)
        for tx in txs:
            token_mint = tx.get("token_mint", "")
            if not token_mint:
                continue

            existing = signals_by_mint.get(token_mint)
            if existing:
                existing.setdefault("whale_addresses", []).append(address)
                existing.setdefault("whale_scores", []).append(wallet.get("score", 0))
                existing["whale_count"] = len(existing["whale_addresses"])
                existing["top_whale_score"] = max(existing["whale_scores"])
                continue

            try:
                dex_resp = await http.get(
                    DEXSCREENER_SEARCH,
                    params={"q": token_mint},
                    timeout=10.0,
                )
                if not dex_resp.is_success:
                    continue
                dex_data = dex_resp.json()
                pairs = dex_data.get("pairs") or []
                sol_pair = None
                for p in pairs:
                    if isinstance(p, dict) and p.get("chainId") == "solana":
                        sol_pair = p
                        break
                if not sol_pair:
                    continue

                created_ms = sol_pair.get("pairCreatedAt")
                if not created_ms:
                    continue
                age_min = (now.timestamp() - created_ms / 1000) / 60
                if age_min > max_age_minutes:
                    continue

                liquidity_usd = (sol_pair.get("liquidity") or {}).get("usd", 0)
                if not liquidity_usd or float(liquidity_usd) < 1000:
                    continue

                symbols = sol_pair.get("baseToken", {})
                signals_by_mint[token_mint] = {
                    "mint": token_mint,
                    "ticker": symbols.get("symbol", "?"),
                    "age_min": round(age_min, 1),
                    "mcap": sol_pair.get("marketCap") or sol_pair.get("fdv") or 0,
                    "liquidity": float(liquidity_usd),
                    "whale_addresses": [address],
                    "whale_scores": [wallet.get("score", 0)],
                    "whale_count": 1,
                    "top_whale_score": wallet.get("score", 0),
                    "timestamp": tx.get("timestamp", 0),
                }
            except (httpx.RequestError, ValueError, TypeError) as exc:
                _log.debug("DexScreener lookup failed for %s: %s", token_mint, exc)
                continue

        await asyncio.sleep(0.3)

    signals = list(signals_by_mint.values())
    signals.sort(key=lambda s: s.get("timestamp", 0), reverse=True)
    return signals


async def get_whale_signal(
    mint: str,
    wallets: list[dict[str, Any]],
    http: httpx.AsyncClient,
) -> dict[str, Any]:
    """Check if any tracked wallets have bought a given mint recently.

    Convenience function for strategy loops. Returns whale signal info
    including count, addresses, size multiplier, and top score.
    Used by:
        scripts/run_paper_loop.py (Strategy A)
        scripts/run_strategy_b.py (Strategy B)

    Result keys:
        whale_count, whale_addresses, size_multiplier, top_whale_score
    """
    result: dict[str, Any] = {
        "whale_count": 0,
        "whale_addresses": [],
        "size_multiplier": WHALE_SIZE_MULTIPLIERS.get(0, 1.0),
        "top_whale_score": 0,
    }

    if not HELIUS_API_KEY or not wallets or not mint:
        return result

    now = datetime.now(timezone.utc)
    matching_wallets: list[tuple[str, int]] = []

    for wallet in wallets:
        address = wallet.get("address", "")
        score = wallet.get("score", 0)
        if not address:
            continue

        txs = await get_recent_wallet_transactions(address, http)
        for tx in txs:
            if tx.get("token_mint", "") == mint:
                tx_ts = tx.get("timestamp", 0)
                age_min = (now.timestamp() - tx_ts) / 60 if tx_ts else 999
                if age_min <= 15:
                    matching_wallets.append((address, score))
                    break

        await asyncio.sleep(0.3)

    if matching_wallets:
        result["whale_count"] = len(matching_wallets)
        result["whale_addresses"] = [w[0] for w in matching_wallets]
        capped_count = min(len(matching_wallets), 3)
        result["size_multiplier"] = WHALE_SIZE_MULTIPLIERS.get(capped_count, 1.0)
        result["top_whale_score"] = max(w[1] for w in matching_wallets)

    return result


async def detect_new_launchpad_tokens(
    http: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    """Poll Helius for new token mints on Believe and Moonshot programs.

    Returns fresh token mints from the last 15 minutes in DexScreener-
    compatible format so Strategy B can consume them directly.

    Each result:
        {mint, ticker, source (believe/moonshot), age_min, mcap, liquidity,
         volume_h1, created_at, program}
    """
    if not HELIUS_API_KEY:
        _log.warning("HELIUS_API_KEY not set")
        return []

    now = datetime.now(timezone.utc)
    candidates: list[dict[str, Any]] = []

    programs = [
        ("believe", BELIEVE_TOKEN_AUTHORITY),
        ("moonshot", MOONSHOT_PROGRAM),
    ]

    for name, program_address in programs:
        try:
            payload = {
                "query": {
                    "accounts": [program_address],
                    "types": ["SWAP"],
                },
            }
            resp = await http.post(
                f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}",
                json=payload,
            )
            if not resp.is_success:
                _log.warning("Helius tx query failed for %s: HTTP %s", name, resp.status_code)
                continue

            data = resp.json()
            txs = data if isinstance(data, list) else []
            for tx in txs:
                if not isinstance(tx, dict):
                    continue

                timestamp = tx.get("timestamp")
                if not timestamp:
                    continue
                age_min = (now.timestamp() - int(timestamp)) / 60
                if age_min > 15:
                    continue

                token_transfers = tx.get("tokenTransfers")
                if not isinstance(token_transfers, list):
                    continue

                for ttf in token_transfers:
                    if not isinstance(ttf, dict):
                        continue
                    mint = (ttf.get("mint") or "").strip()
                    if not mint or len(mint) < 30:
                        continue

                    symbols = ttf.get("symbol", "") or "?"

                    candidates.append({
                        "mint": mint,
                        "ticker": str(symbols),
                        "source": name,
                        "age_min": round(age_min, 1),
                        "mcap": None,
                        "liquidity": None,
                        "volume_h1": None,
                        "created_at": datetime.fromtimestamp(
                            int(timestamp), tz=timezone.utc
                        ).isoformat(),
                        "program": program_address,
                    })
                    break

        except (httpx.RequestError, ValueError, TypeError) as exc:
            _log.warning("Helius query failed for %s program: %s", name, exc)
            continue

    return candidates

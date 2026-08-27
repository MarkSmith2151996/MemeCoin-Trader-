"""Jupiter and PumpPortal observations written unchanged into Hive candidates."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import websockets
from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.store import MemecoinStore

log = logging.getLogger("memecoin.data_collector")

JUPITER_API_BASE = "https://api.jup.ag/tokens/v2"
PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
AGE_OFFSET_SECONDS = 39.0
JUPITER_ENDPOINTS = (
    ("jupiter_toporganicscore", "/toporganicscore/5m", {"limit": 100}),
    ("jupiter_recent", "/recent", {"limit": 30}),
    ("jupiter_trending", "/toptrending/5m", {"limit": 100}),
)


def _finite_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _integer(value: object) -> int | None:
    number = _finite_float(value)
    return int(number) if number is not None else None


def _created_at(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _pool_type(token: dict[str, Any]) -> str:
    mint = str(token.get("id") or "").lower()
    pool_id = str((token.get("firstPool") or {}).get("id") or "").lower()
    return "bonding" if mint.endswith("pump") and (not pool_id or pool_id == mint) else "graduated"


def _age_adjusted_min_txns(age_seconds: float) -> int:
    age_minutes = age_seconds / 60
    if age_minutes < 1:
        return 3
    if age_minutes < 3:
        return 5
    if age_minutes < 5:
        return 8
    if age_minutes < 10:
        return 12
    return 16


def _strength_score(
    *,
    age_seconds: float | None,
    mcap_usd: float | None,
    volume_usd: float | None,
    buys: int | None,
    sells: int | None,
    buy_volume_usd: float | None,
    sell_volume_usd: float | None,
) -> float | None:
    if None in (
        age_seconds,
        mcap_usd,
        volume_usd,
        buys,
        sells,
        buy_volume_usd,
        sell_volume_usd,
    ) or mcap_usd <= 0:
        return None
    buy_volume_ratio = buy_volume_usd / max(sell_volume_usd, 1.0)
    volume_mcap_ratio = volume_usd / mcap_usd
    adjusted_transactions = int((buys + sells) * 1.24)
    score = (
        min(buy_volume_ratio / 2, 1) * 40
        + min(volume_mcap_ratio / 0.05, 1) * 30
        + min(adjusted_transactions / (4 * _age_adjusted_min_txns(age_seconds)), 1) * 15
        + min(volume_usd / 5000, 1) * 15
    )
    return round(score, 1)


def normalize_jupiter_token(
    token: dict[str, Any],
    source: str,
    *,
    sol_price_usd: float | None,
    observed_at: datetime | None = None,
) -> dict[str, Any] | None:
    """Turn one Jupiter response into the complete durable candidate record."""

    mint = token.get("id")
    if not isinstance(mint, str) or not mint:
        return None
    observed = observed_at or datetime.now(UTC)
    first_pool = token.get("firstPool") if isinstance(token.get("firstPool"), dict) else {}
    created = _created_at(first_pool.get("createdAt"))
    age_seconds = max((observed - created).total_seconds(), 0.0) if created else None
    stats_1h = token.get("stats1h") if isinstance(token.get("stats1h"), dict) else {}
    stats_5m = token.get("stats5m") if isinstance(token.get("stats5m"), dict) else {}
    audit = token.get("audit") if isinstance(token.get("audit"), dict) else {}
    buys = _integer(stats_1h.get("numBuys")) or 0
    sells = _integer(stats_1h.get("numSells")) or 0
    buy_volume = _finite_float(stats_1h.get("buyVolume")) or 0.0
    sell_volume = _finite_float(stats_1h.get("sellVolume")) or 0.0
    volume_usd = buy_volume + sell_volume
    liquidity_usd = _finite_float(token.get("liquidity"))
    raw_mcap = _finite_float(token.get("mcap")) or _finite_float(token.get("fdv"))
    pool_sol = liquidity_usd / sol_price_usd if liquidity_usd and sol_price_usd else None
    pool_mcap = pool_sol * sol_price_usd * 4.4 if pool_sol and sol_price_usd else None
    mcap_usd = pool_mcap if pool_mcap and raw_mcap and raw_mcap > pool_mcap * 1.5 else raw_mcap
    price_usd = _finite_float(token.get("usdPrice"))
    corrected_age_seconds = age_seconds + AGE_OFFSET_SECONDS if age_seconds is not None else None
    mint_authority_revoked = audit.get("mintAuthorityDisabled")
    freeze_authority_revoked = audit.get("freezeAuthorityDisabled")
    return {
        "mint_address": mint,
        "observed_at": observed,
        "source": source,
        "age_seconds": age_seconds,
        "corrected_age_seconds": corrected_age_seconds,
        "mcap_usd": mcap_usd,
        "volume_usd": volume_usd,
        "buy_volume_usd": buy_volume,
        "sell_volume_usd": sell_volume,
        "txn_buys": buys,
        "txn_sells": sells,
        "buy_sell_ratio": buy_volume / max(sell_volume, 1.0),
        "liquidity_usd": liquidity_usd,
        "fdv_usd": _finite_float(token.get("fdv")),
        "price_sol": price_usd / sol_price_usd if price_usd and sol_price_usd else None,
        "price_usd": price_usd,
        "pool_sol": pool_sol,
        "pool_type": _pool_type(token),
        "creator_holdings_pct": _finite_float(audit.get("devBalancePercentage")),
        "mint_authority_revoked": (
            mint_authority_revoked if isinstance(mint_authority_revoked, bool) else None
        ),
        "freeze_authority_revoked": (
            freeze_authority_revoked if isinstance(freeze_authority_revoked, bool) else None
        ),
        "top_holder_pct": _finite_float(audit.get("topHoldersPercentage")),
        "security_source": "jupiter_audit",
        "security_checked_at": observed,
        "unique_wallets": _integer(token.get("holderCount")),
        "price_change_5m": _finite_float(stats_5m.get("priceChange")),
        "price_change_1h": _finite_float(stats_1h.get("priceChange")),
        "strength_score": _strength_score(
            age_seconds=corrected_age_seconds,
            mcap_usd=mcap_usd,
            volume_usd=volume_usd,
            buys=buys,
            sells=sells,
            buy_volume_usd=buy_volume,
            sell_volume_usd=sell_volume,
        ),
        "raw_json": token,
    }


def normalize_pumpportal_token(
    payload: dict[str, Any],
    *,
    sol_price_usd: float | None,
    observed_at: datetime | None = None,
) -> dict[str, Any] | None:
    """Keep PumpPortal new-token payloads even when they lack gate fields."""

    mint = payload.get("mint")
    if not isinstance(mint, str) or not mint:
        return None
    observed = observed_at or datetime.now(UTC)
    pool_sol = _finite_float(payload.get("vSolInBondingCurve"))
    price_sol = _finite_float(payload.get("priceSol"))
    return {
        "mint_address": mint,
        "observed_at": observed,
        "source": "pumpportal",
        "age_seconds": 0.0,
        "corrected_age_seconds": None,
        "mcap_usd": (_finite_float(payload.get("marketCapSol")) or 0.0) * sol_price_usd
        if sol_price_usd
        else None,
        "volume_usd": None,
        "buy_volume_usd": None,
        "sell_volume_usd": None,
        "txn_buys": None,
        "txn_sells": None,
        "buy_sell_ratio": None,
        "liquidity_usd": pool_sol * sol_price_usd if pool_sol and sol_price_usd else None,
        "fdv_usd": None,
        "price_sol": price_sol,
        "price_usd": price_sol * sol_price_usd if price_sol and sol_price_usd else None,
        "pool_sol": pool_sol,
        "pool_type": "bonding",
        "creator_holdings_pct": None,
        "mint_authority_revoked": None,
        "freeze_authority_revoked": None,
        "top_holder_pct": None,
        "security_source": None,
        "security_checked_at": None,
        "unique_wallets": None,
        "price_change_5m": None,
        "price_change_1h": None,
        "strength_score": None,
        "raw_json": payload,
    }


class DataCollector:
    """Independent discovery process that never evaluates strategy gates."""

    def __init__(
        self,
        store: MemecoinStore,
        client: httpx.AsyncClient,
        *,
        api_key: str,
        interval_seconds: float = 1.0,
    ) -> None:
        self._store = store
        self._client = client
        self._api_key = api_key
        self._interval_seconds = interval_seconds
        self._sol_price_usd: float | None = None
        self._sol_price_at = 0.0

    async def run(self) -> None:
        await asyncio.gather(self._poll_jupiter(), self._listen_pumpportal())

    async def _poll_jupiter(self) -> None:
        while True:
            started = time.monotonic()
            sol_price_usd = await self._get_sol_price_usd()
            for source, endpoint, params in JUPITER_ENDPOINTS:
                response = await self._client.get(
                    f"{JUPITER_API_BASE}{endpoint}",
                    params=params,
                    headers={"x-api-key": self._api_key},
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise RuntimeError(f"Jupiter {endpoint} returned a non-list payload")
                for token in payload:
                    if not isinstance(token, dict):
                        continue
                    candidate = normalize_jupiter_token(
                        token,
                        source,
                        sol_price_usd=sol_price_usd,
                    )
                    if candidate is not None:
                        await self._store.insert_candidate(candidate)
                await asyncio.sleep(0.25)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, self._interval_seconds - elapsed))

    async def _listen_pumpportal(self) -> None:
        while True:
            try:
                async with websockets.connect(
                    PUMPPORTAL_WS_URL, ping_interval=20, ping_timeout=20
                ) as socket:
                    await socket.send(json.dumps({"method": "subscribeNewToken"}))
                    async for raw in socket:
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(payload, dict):
                            continue
                        candidate = normalize_pumpportal_token(
                            payload,
                            sol_price_usd=self._sol_price_usd,
                        )
                        if candidate is not None:
                            await self._store.insert_candidate(candidate)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("PumpPortal collector disconnected: %s", exc)
                await asyncio.sleep(2)

    async def _get_sol_price_usd(self) -> float | None:
        if time.monotonic() - self._sol_price_at < 60 and self._sol_price_usd is not None:
            return self._sol_price_usd
        response = await self._client.get(
            f"{JUPITER_API_BASE}/search",
            params={"query": WRAPPED_SOL_MINT},
            headers={"x-api-key": self._api_key},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            return self._sol_price_usd
        for token in payload:
            if isinstance(token, dict) and token.get("id") == WRAPPED_SOL_MINT:
                price = _finite_float(token.get("usdPrice"))
                if price and price > 0:
                    self._sol_price_usd = price
                    self._sol_price_at = time.monotonic()
                    return price
        return self._sol_price_usd


async def _run() -> None:
    load_dotenv()
    api_key = os.getenv("JUPITER_API_KEY")
    if not api_key:
        raise RuntimeError("JUPITER_API_KEY is required for memecoin-data")
    store = await MemecoinStore.connect()
    try:
        timeout = httpx.Timeout(15.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            collector = DataCollector(
                store,
                client,
                api_key=api_key,
                interval_seconds=float(os.getenv("MEMECOIN_COLLECT_INTERVAL_SECONDS", "1")),
            )
            await collector.run()
    finally:
        await store.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s")
    asyncio.run(_run())


if __name__ == "__main__":
    main()

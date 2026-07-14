"""Backfill price_snapshots table with DexScreener quotes for all known mints."""

from __future__ import annotations

import asyncio
import math
import time
from datetime import UTC, datetime

import httpx

from src.core.database import get_distinct_mints, init_db, record_price_snapshot

DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"
BATCH_DELAY_S = 0.3
MAX_CONCURRENT = 5


async def fetch_snapshot(client: httpx.AsyncClient, mint: str) -> dict | None:
    try:
        resp = await client.get(DEXSCREENER_TOKEN_URL.format(mint=mint))
        resp.raise_for_status()
        data = resp.json()
    except (httpx.HTTPError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    pairs = data.get("pairs")
    if not isinstance(pairs, list):
        return None
    solana_pairs = [p for p in pairs if isinstance(p, dict) and p.get("chainId") == "solana"]
    if not solana_pairs:
        return None
    best = max(solana_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
    return best


def extract_snapshot(mint: str, pair: dict) -> dict:
    volume = pair.get("volume", {}) or {}
    liquidity = pair.get("liquidity", {}) or {}
    price_native = pair.get("priceNative")
    price_usd = pair.get("priceUsd")
    fdv = pair.get("fdv")
    price_sol = None
    if isinstance(price_native, str):
        try:
            price_sol = float(price_native)
            if not math.isfinite(price_sol) or price_sol < 0:
                price_sol = None
        except (TypeError, ValueError):
            pass
    return {
        "mint_address": mint,
        "price_sol": price_sol,
        "price_usd": _safe_float(price_usd),
        "volume_h24": _safe_float(volume.get("h24")),
        "liquidity_usd": _safe_float(liquidity.get("usd")),
        "fdv_usd": _safe_float(fdv),
        "pair_address": pair.get("pairAddress"),
        "dex_id": pair.get("dexId"),
    }


def _safe_float(value: object) -> float | None:
    try:
        v = float(value) if value is not None else None
        return v if v is not None and math.isfinite(v) and v >= 0 else None
    except (TypeError, ValueError):
        return None


async def main() -> None:
    db_path = "data/trades.db"
    await init_db(db_path)
    mints = await get_distinct_mints(db_path)
    total = len(mints)
    print(f"Found {total} distinct mints. Starting backfill...", flush=True)

    sem = asyncio.Semaphore(MAX_CONCURRENT)
    snapshots: list[dict] = []
    errors = 0

    async with httpx.AsyncClient(timeout=10.0) as client:

        async def process(mint: str) -> None:
            nonlocal errors
            async with sem:
                pair = await fetch_snapshot(client, mint)
                if pair is None:
                    errors += 1
                    snapshots.append(
                        {
                            "mint_address": mint,
                            "price_sol": None,
                            "price_usd": None,
                            "volume_h24": None,
                            "liquidity_usd": None,
                            "fdv_usd": None,
                            "pair_address": None,
                            "dex_id": None,
                        }
                    )
                    return
                snapshots.append(extract_snapshot(mint, pair))

        tasks = []
        for i, mint in enumerate(mints):
            tasks.append(process(mint))
            if (i + 1) % 10 == 0 or i == total - 1:
                await asyncio.gather(*tasks)
                tasks = []
                print(f"  Progress: {i + 1}/{total}", flush=True)
                await asyncio.sleep(BATCH_DELAY_S)

    written = 0
    for snap in snapshots:
        await record_price_snapshot(
            db_path,
            mint_address=snap["mint_address"],
            price_sol=snap["price_sol"],
            price_usd=snap["price_usd"],
            volume_h24=snap["volume_h24"],
            liquidity_usd=snap["liquidity_usd"],
            fdv_usd=snap["fdv_usd"],
            pair_address=snap["pair_address"],
            dex_id=snap["dex_id"],
        )
        written += 1

    priced = sum(1 for s in snapshots if s["price_sol"] is not None)
    print(f"\nBackfill complete: {written} snapshots written, {priced} with prices, {errors} errors")


if __name__ == "__main__":
    asyncio.run(main())

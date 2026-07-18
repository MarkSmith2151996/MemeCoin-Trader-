"""Build tracked whale wallet list from historical Solana winners.

For BONK, WIF, and POPCAT, collect early buyer wallets via Helius
transactions API, score by how many of the three coins each wallet
was early on, enrich with recent trade stats, and save top 50
to config/tracked_wallets.json.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import httpx
from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[1]

COINS: dict[str, str] = {
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    "POPCAT": "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
}

KNOWN_NON_WALLET: set[str] = {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    "11111111111111111111111111111111",
    "SysvarRent111111111111111111111111111111111",
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
    "worm2oG2CBMhiETM1S3NNL8LYoK7XUtLxP1e3xKqLp",
    "namesLPneVptA9Z18rZ6sBCAc7GdWMGMM5hsu5XGqA",
    "cjg98GtCVRNDZoNzWGFD7d2CySqxBWpppXjxbEEbB1y",
    "JUP6LkbZbjSQQbyQtPJLFo6ep1T3Nx5p5KGJNJKNXk",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
    "675kPX9MHTjS2zt1L5bb3mFhbydJW8C1BdEZ8mFb6bq",
    "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2YgTcNEq4v1bP5",
    "CAMMCzo5YLJwav5VZ2Uqr2T2K2ZohBbeBcrYSu92i1M",
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
    "9W959DqEETiGZocYWCQPaC6N8R2L3q9qT3LgBAgWjq6",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ",
    "7yt1UZ1AmSnxNqCpfWrn85CprBkNuLLbWMdKmAP3L9r",
    "DujWh4bNKKKbMDvA1MvJ7yfECtE7CNdUNJFKjxmyUNUn",
    "12xt3WKKfGecCqLCqBENqqKbnXMiSm5Cah4c8ReBm5EE",
    "TREASrZ1B7dFjEHKNbM22SM3HZpc16CqBq3EKQj3kq",
}

TARGET_PER_COIN = 200
MAX_FINAL = 50
MAX_PAGES = 75
PAGE_LIMIT = 100
ENRICH_DELAY_S = 0.5
PAGINATION_DELAY_S = 0.25
HELIUS_TIMEOUT_S = 30.0


def load_api_key() -> str:
    direct = os.getenv("HELIUS_API_KEY", "").strip()
    if direct:
        return direct
    dotenv_path = REPO_ROOT / ".env"
    if dotenv_path.exists():
        return dotenv_values(dotenv_path).get("HELIUS_API_KEY", "").strip()
    return ""


def is_non_wallet(address: str) -> bool:
    return address in KNOWN_NON_WALLET


def extract_signature(tx: dict) -> str | None:
    sig = tx.get("signature")
    if isinstance(sig, str) and sig.strip():
        return sig.strip()
    sigs = tx.get("signatures")
    if isinstance(sigs, list) and sigs:
        first = sigs[0]
        if isinstance(first, str) and first.strip():
            return first.strip()
    return None


async def fetch_txs(
    client: httpx.AsyncClient, api_key: str, address: str, before: str | None = None
) -> list[dict]:
    params: dict[str, object] = {
        "api-key": api_key,
        "limit": PAGE_LIMIT,
    }
    if before:
        params["before"] = before

    response = await client.get(
        f"https://api.helius.xyz/v0/addresses/{address}/transactions",
        params=params,
        timeout=HELIUS_TIMEOUT_S,
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


async def collect_buyers(
    api_key: str, coin: str, mint: str
) -> list[tuple[str, int]]:
    """Collect earliest unique wallet addresses interacting with this token mint."""
    wallets: dict[str, int] = {}
    before: str | None = None

    async with httpx.AsyncClient() as client:
        for page in range(MAX_PAGES):
            try:
                txs = await fetch_txs(client, api_key, mint, before)
            except Exception as exc:
                print(f"  {coin}: page {page + 1} error: {exc}")
                break

            if not txs:
                print(f"  {coin}: no more transactions after page {page}")
                break

            for tx in txs:
                timestamp = tx.get("timestamp")
                if not isinstance(timestamp, (int, float)):
                    continue

                transfers = tx.get("tokenTransfers")
                if not isinstance(transfers, list):
                    continue

                for transfer in transfers:
                    if not isinstance(transfer, dict):
                        continue
                    if str(transfer.get("mint", "")) != mint:
                        continue

                    to_addr = str(transfer.get("toUserAccount", "")).strip()
                    from_addr = str(transfer.get("fromUserAccount", "")).strip()

                    if to_addr and not is_non_wallet(to_addr):
                        ts = int(timestamp)
                        if to_addr not in wallets or ts < wallets[to_addr]:
                            wallets[to_addr] = ts

                    if from_addr and not is_non_wallet(from_addr):
                        ts = int(timestamp)
                        if from_addr not in wallets or ts < wallets[from_addr]:
                            wallets[from_addr] = ts

            if len(wallets) >= TARGET_PER_COIN:
                print(f"  {coin}: collected {len(wallets)} unique wallets")
                break

            last_sig = extract_signature(txs[-1])
            if not last_sig:
                break
            before = last_sig
            await asyncio.sleep(PAGINATION_DELAY_S)

    sorted_wallets = sorted(wallets.items(), key=lambda x: x[1])
    print(f"  {coin}: {len(sorted_wallets)} total unique wallets")
    return sorted_wallets[:TARGET_PER_COIN]


async def enrich_wallet(
    client: httpx.AsyncClient, api_key: str, address: str
) -> tuple[int, int]:
    """Fetch recent Helius transactions for a wallet, return (total_trades, unique_tokens_traded)."""
    try:
        txs = await fetch_txs(client, api_key, address)
    except Exception:
        return 0, 0

    seen_mints: set[str] = set()
    total_swap_events = 0

    for tx in txs:
        transfers = tx.get("tokenTransfers")
        if not isinstance(transfers, list):
            continue
        for t in transfers:
            if not isinstance(t, dict):
                continue
            mint = str(t.get("mint", "")).strip()
            if not mint:
                continue
            total_swap_events += 1
            seen_mints.add(mint)

    total_trades = total_swap_events
    unique_tokens = len(seen_mints)
    return total_trades, unique_tokens


async def fetch_dexscreener_pairs(mint: str) -> list[dict]:
    """Fetch token pairs from DexScreener for creation timestamp."""
    url = f"https://api.dexscreener.com/token/v1/solana/{mint}"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
    except Exception:
        pass
    return []


async def main() -> None:
    api_key = load_api_key()
    if not api_key:
        print("ERROR: HELIUS_API_KEY not found")
        sys.exit(1)

    print("=" * 60)
    print("Building tracked whale wallet list")
    print("=" * 60)

    # Phase 0: Fetch DexScreener creation timestamps
    print("\n--- Fetching token creation info from DexScreener ---")
    creation_info: dict[str, int | None] = {}
    for coin_name, mint in COINS.items():
        pairs = await fetch_dexscreener_pairs(mint)
        created_at = None
        for pair in pairs:
            ts = pair.get("pairCreatedAt")
            if isinstance(ts, (int, float)) and ts > 0:
                created_at = int(ts / 1000)
                break
        creation_info[coin_name] = created_at
        if created_at:
            print(f"  {coin_name}: pool created at {created_at} ({created_at} unix)")
        else:
            print(f"  {coin_name}: creation timestamp not found via DexScreener")

    # Phase 1: Collect early buyers per coin
    all_wallets: dict[str, dict] = {}
    page_counts: dict[str, int] = {}

    for coin_name, mint in COINS.items():
        print(f"\n--- Collecting early buyers for {coin_name} ---")
        buyers = await collect_buyers(api_key, coin_name, mint)
        for address, timestamp in buyers:
            if address not in all_wallets:
                all_wallets[address] = {
                    "coins_early_on": [],
                    "first_buy_timestamps": {},
                }
            all_wallets[address]["coins_early_on"].append(coin_name)
            all_wallets[address]["first_buy_timestamps"][coin_name] = timestamp

    print(f"\nTotal unique wallets across all coins: {len(all_wallets)}")

    # Phase 2: Score and rank
    scored: list[tuple[str, int, float, list[str]]] = []
    for address, data in all_wallets.items():
        score = len(data["coins_early_on"])
        timestamps = list(data["first_buy_timestamps"].values())
        avg_time = sum(timestamps) / len(timestamps) if timestamps else float("inf")
        scored.append((address, score, avg_time, data["coins_early_on"]))

    scored.sort(key=lambda x: (-x[1], x[2]))
    top_wallets = scored[:MAX_FINAL]

    score_dist: dict[int, int] = defaultdict(int)
    for _, score, _, _ in top_wallets:
        score_dist[score] += 1

    print(f"\nTop {len(top_wallets)} wallets — score distribution:")
    for s in sorted(score_dist, reverse=True):
        label = "all 3" if s == 3 else str(s)
        print(f"  Score {s} ({label}): {score_dist[s]} wallets")
    for addr, score, _, coins in top_wallets:
        print(f"  {addr[:12]}...{addr[-6:]}  score={score}  coins={coins}")

    # Phase 3: Enrich top 50 with stats
    print(f"\n--- Enriching top {len(top_wallets)} wallets via Helius ---")
    enriched: list[dict] = []

    async with httpx.AsyncClient() as client:
        for i, (address, score, _, coins) in enumerate(top_wallets):
            label = f"smart_money_{i + 1}"
            trades, unique_tokens = await enrich_wallet(client, api_key, address)
            enriched.append({
                "address": address,
                "label": label,
                "coins_early_on": coins,
                "score": score,
                "total_trades": trades,
                "unique_tokens_traded": unique_tokens,
            })
            print(f"  {label}: {address[:12]}...{address[-6:]}  score={score}  trades={trades}  unique_tokens={unique_tokens}")
            await asyncio.sleep(ENRICH_DELAY_S)

    # Phase 4: Save
    output_path = REPO_ROOT / "config" / "tracked_wallets.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(enriched, indent=2) + "\n", encoding="utf-8")

    print(f"\n{'=' * 60}")
    print(f"Saved {len(enriched)} wallets to {output_path}")
    print(f"{'=' * 60}")
    print()
    print(f"Built whale list: {len(enriched)} wallets")
    for s in sorted(score_dist, reverse=True):
        label = "all 3" if s == 3 else str(s)
        print(f"  Score {s} (early on {label}): {score_dist[s]} wallets")
    print(f"\nTop 5 wallets:")
    for w in enriched[:5]:
        print(f"  {w['address']} ({w['label']})")


if __name__ == "__main__":
    asyncio.run(main())

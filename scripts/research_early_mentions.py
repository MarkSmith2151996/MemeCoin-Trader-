"""Research: winner vs loser early mentions using since_time/until_time operators.

Collects 50 winner and 50 loser Solana coins from DexScreener, queries Grok
x_search with exact launch-window timestamps, and computes the real
MIN_MENTIONS threshold for Strategy B.

Usage:
    python3 scripts/research_early_mentions.py

Outputs:
    research/mt484_output/winners.json
    research/mt484_output/losers.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.signals.grok_xsearch import (
    _post_with_retry, _extract_output_text, _parse_iso_timestamps,
    count_influencer_mentions, INFLUENCER_HANDLES,
)

load_dotenv()

DEXSCREENER_SEARCH = "https://api.dexscreener.com/latest/dex/search"
DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens"
DEXSCREENER_BOOSTS_TOP = "https://api.dexscreener.com/token-boosts/top/v1"
DEXSCREENER_BOOSTS_LATEST = "https://api.dexscreener.com/token-boosts/latest/v1"

OUTPUT_DIR = "research/mt484_output"

TARGET = 50

NOW = datetime.now(timezone.utc)

DEXSCREENER_PROFILES = "https://api.dexscreener.com/token-profiles/latest/v1"

SEARCH_TERMS = [
    "SOL", "pump", "raydium", "cat", "dog", "pepe", "moon", "ai", "trump",
    "coin", "bonk", "baby", "elon", "meme", "doge", "shib", "pig", "bird",
    "fish", "wolf", "dragon", "lion", "bear", "bull", "woof", "star", "rocket",
    "mario", "luna", "nova", "ape", "grok", "deep", "mind", "neo", "max",
    "btc", "eth", "xrp", "usdt", "usdc", "dai", "link", "uni", "aave",
    "mkr", "comp", "snx", "yfi", "crv", "bal", "lrc", "zrx", "ftt",
    "celo", "algo", "near", "avax", "matic", "atom", "dot", "ada", "xlm",
    "lite", "dash", "zcash", "iota", "eos", "trx", "waves", "ont", "vet",
    "icx", "omg", "bat", "zil", "fet", "ocean", "grt", "storj", "fil",
    "ankr", "theta", "tfuel", "band", "troy", "key", "sun", "jst", "btt",
    "win", "chat", "fun", "eat", "run", "fly", "top", "new", "big",
    "hot", "ice", "cap", "fan", "gun", "sea", "sky", "red", "blue",
    "woo", "wow", "wax", "web", "wtf", "xox", "yes", "yum", "zag",
    "zen", "zip", "zoo", "ace", "ark", "art", "ash", "axe", "bad",
    "bag", "ban", "bar", "bay", "bed", "bet", "bid", "bit", "bow",
    "box", "boy", "bud", "bug", "bus", "buy", "cab", "cam", "can",
    "cap", "car", "cat", "cow", "cry", "cub", "cup", "cut", "dad",
    "dam", "day", "dig", "dip", "doe", "dot", "dry", "dub", "dug",
    "dun", "duo", "dye", "ear", "eat", "eel", "egg", "elm", "emu",
    "end", "era", "eve", "eye", "fab", "fad", "fan", "far", "fat",
    "few", "fig", "fin", "fit", "fix", "fly", "fog", "for", "fox",
    "fry", "fun", "fur", "gag", "gal", "gap", "gel", "gem", "get",
    "gig", "gin", "gnu", "god", "got", "gum", "gun", "gut", "guy",
    "had", "ham", "has", "hat", "hen", "her", "hew", "hid", "him",
    "hip", "his", "hit", "hog", "hop", "hot", "how", "hub", "hue",
    "hug", "hum", "hut", "ice", "icy", "ill", "imp", "ink", "inn",
    "ion", "ire", "irk", "its", "ivy", "jab", "jag", "jam", "jar",
    "jaw", "jay", "jet", "jig", "job", "jog", "jot", "joy", "jug",
    "jut", "keg", "ken", "key", "kid", "kin", "kit", "lab", "lad",
    "lag", "lap", "law", "lay", "lea", "leg", "let", "lid", "lip",
    "lit", "log", "lot", "low", "lug", "mad", "man", "map", "mar",
    "mat", "maw", "max", "may", "men", "met", "mid", "mix", "mob",
    "mod", "mom", "mop", "mow", "mud", "mug", "net", "new", "nil",
    "nip", "nit", "nod", "nor", "not", "now", "nut", "oak", "oat",
    "odd", "ode", "off", "oil", "old", "one", "opt", "orb", "our",
    "out", "owe", "owl", "own", "pad", "pal", "pan", "pap", "par",
    "pat", "paw", "pay", "pea", "pen", "pep", "per", "pie", "pin",
    "pit", "pod", "pop", "pot", "pow", "pro", "pry", "pub", "pug",
    "pun", "pus", "put", "rag", "ram", "ran", "rap", "rat", "raw",
    "ray", "red", "ref", "rep", "rib", "rid", "rig", "rim", "rip",
    "rob", "rod", "roe", "rot", "row", "rub", "rug", "rum", "run",
    "rut", "rye", "sac", "sad", "sag", "sap", "sat", "saw", "say",
    "sea", "set", "sew", "she", "shy", "sin", "sip", "sir", "sit",
    "six", "ski", "sky", "sly", "sob", "sod", "son", "sop", "sot",
    "sow", "soy", "spa", "spy", "sty", "sub", "sue", "sum", "sun",
    "tab", "tad", "tag", "tan", "tap", "tar", "tax", "tea", "ten",
    "the", "tie", "tin", "tip", "toe", "ton", "too", "top", "tow",
    "toy", "try", "tub", "tug", "two", "urn", "use", "van", "vat",
    "vet", "vex", "via", "vie", "vim", "vow", "wad", "wag", "war",
    "was", "wax", "way", "web", "wed", "wet", "who", "why", "wig",
    "win", "wit", "woe", "wok", "won", "woo", "yam", "yap", "yaw",
    "yen", "yep", "yes", "yet", "yew", "you", "zap", "zed", "zen",
    "zig", "zip", "zit", "zoo",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("early_mentions")


def _classify_pair(pair: dict) -> dict | None:
    """Classify a single DexScreener pair dict. Returns info dict or None."""
    mint = (pair.get("baseToken") or {}).get("address")
    if not mint:
        return None
    created_ms = pair.get("pairCreatedAt")
    if not created_ms:
        return None

    launched_at = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
    age_hours = (NOW - launched_at).total_seconds() / 3600
    if age_hours < 2 or age_hours > 7 * 24:
        return None

    liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
    price_change = float(pair.get("priceChange", {}).get("h24", 0) or 0)
    mcap = float(pair.get("marketCap") or 0)
    symbol = pair.get("baseToken", {}).get("symbol", "?")
    ticker = symbol.upper()

    info = {
        "ticker": ticker,
        "mint": mint,
        "symbol": symbol,
        "launched_at": launched_at,
        "launched_at_iso": launched_at.isoformat(),
        "pairCreatedAt": created_ms,
        "market_cap": mcap,
        "liquidity_usd": liq,
        "volume_24h": float(pair.get("volume", {}).get("h24", 0) or 0),
        "price_change_pct": price_change,
        "age_hours": age_hours,
    }
    return info


def _collect_coins() -> tuple[list[dict], list[dict]]:
    """Collect winners and losers from DexScreener search + boosts.

    Uses search results inline to avoid per-mint rate limits.
    Boosts require a token detail lookup (with delay between batches).
    """
    seen: set[str] = set()
    winners: list[dict] = []
    losers: list[dict] = []
    boost_mints: set[str] = set()

    # Source 1: search results — full pair data inline
    for q in SEARCH_TERMS:
        try:
            resp = httpx.get(DEXSCREENER_SEARCH, params={"q": q}, timeout=8)
            if resp.status_code == 200:
                for p in resp.json().get("pairs", []):
                    if p.get("chainId") != "solana":
                        continue
                    mint = (p.get("baseToken") or {}).get("address")
                    if not mint or mint in seen:
                        continue
                    seen.add(mint)
                    info = _classify_pair(p)
                    if info is None:
                        continue
                    if info["price_change_pct"] > 80 and info["liquidity_usd"] > 5000 and info["market_cap"] >= 3000:
                        winners.append(info)
                    elif -80 <= info["price_change_pct"] <= -15 and info["liquidity_usd"] > 500 and info["market_cap"] >= 3000:
                        losers.append(info)
        except Exception:
            pass

    # Source 2: token profiles — new token listings with addresses
    try:
        resp = httpx.get(DEXSCREENER_PROFILES, timeout=10)
        if resp.status_code == 200:
            for profile in resp.json():
                if profile.get("chainId") != "solana":
                    continue
                addr = profile.get("tokenAddress")
                if addr and addr not in seen:
                    boost_mints.add(addr)
    except Exception as exc:
        log.warning("Token profiles fetch failed: %s", exc)

    # Source 3: boosts — tokenAddress only, need detail lookup in batches
    for url in (DEXSCREENER_BOOSTS_TOP, DEXSCREENER_BOOSTS_LATEST):
        try:
            resp = httpx.get(url, timeout=10)
            if resp.status_code == 200:
                for b in resp.json():
                    if b.get("chainId") == "solana":
                        addr = b.get("tokenAddress")
                        if addr and addr not in seen:
                            boost_mints.add(addr)
        except Exception as exc:
            log.warning("Boosts fetch failed: %s", exc)

    if boost_mints:
        log.info("Looking up %d boost mints (with delay)...", len(boost_mints))
        with httpx.Client(timeout=10) as client:
            for i, bm in enumerate(boost_mints):
                if len(winners) >= TARGET and len(losers) >= TARGET:
                    break
                try:
                    resp = client.get(f"{DEXSCREENER_TOKEN}/{bm}", timeout=8)
                    if resp.status_code == 200:
                        pairs = resp.json().get("pairs", [])
                        for p in pairs:
                            if p.get("chainId") != "solana":
                                continue
                            mint = (p.get("baseToken") or {}).get("address")
                            if not mint or mint in seen:
                                continue
                            seen.add(mint)
                            info = _classify_pair(p)
                            if info is None:
                                continue
                            if info["price_change_pct"] > 80 and info["liquidity_usd"] > 5000 and info["market_cap"] >= 3000:
                                winners.append(info)
                            elif -80 <= info["price_change_pct"] <= -15 and info["liquidity_usd"] > 500 and info["market_cap"] >= 3000:
                                losers.append(info)
                except Exception:
                    pass
                if i % 5 == 4:
                    import time
                    time.sleep(1)

    winners.sort(key=lambda w: w["price_change_pct"], reverse=True)
    losers.sort(key=lambda w: w["price_change_pct"])

    log.info("Winners: %d, Losers: %d (collected)", len(winners), len(losers))
    return winners[:TARGET], losers[:TARGET]


async def query_grok_mentions(
    ticker: str,
    mint: str,
    launch_unix: int,
    window_seconds: int,
) -> dict:
    """Query Grok x_search for mentions in [launch_unix, launch_unix + window_seconds].

    Tries $TICKER first, then mint address as fallback.
    Returns {"mentions": int, "timestamps": list[str], "earliest_min": float | None}.
    """
    window_end = launch_unix + window_seconds
    label = f"{window_seconds // 60}min"

    for query in [f"${ticker}", mint]:
        q = f'{query} since_time:{launch_unix} until_time:{window_end}'
        prompt = (
            f"Search X for: {q}\n"
            "List each mention with its approximate UTC timestamp (ISO format). "
            "Then report the total count of unique accounts that mentioned it."
        )
        payload = {
            "model": "grok-4.3",
            "input": [{"role": "user", "content": prompt}],
            "tools": [{"type": "x_search"}],
        }
        data = await _post_with_retry(ticker, mint, payload)
        if data is None:
            continue

        text = _extract_output_text(data)
        timestamps: list[datetime] = []
        if text:
            timestamps = _parse_iso_timestamps(text)
        if not timestamps:
            raw_json = json.dumps(data)
            timestamps = _parse_iso_timestamps(raw_json)

        if timestamps:
            in_window = [
                ts for ts in timestamps
                if launch_unix <= ts.timestamp() <= window_end
            ]
            earliest = min(in_window) if in_window else None
            earliest_min = (earliest.timestamp() - launch_unix) / 60.0 if earliest else None
            return {
                "mentions": len(in_window),
                "timestamps": [ts.isoformat() for ts in in_window],
                "earliest_min": earliest_min,
                "search_term": query,
                f"mentions_0_{label}": len(in_window),
            }

    return {
        "mentions": 0,
        "timestamps": [],
        "earliest_min": None,
        "search_term": None,
        f"mentions_0_{label}": 0,
    }


async def process_coin(info: dict) -> dict | None:
    ticker = info["ticker"]
    mint = info["mint"]
    launched_at = info["launched_at"]
    launch_unix = int(launched_at.timestamp())

    log.info(
        "  Grok %s (%s) — age=%.1fh, change=%+.1f%%",
        ticker, mint[:8], info["age_hours"], info["price_change_pct"],
    )

    r5 = await query_grok_mentions(ticker, mint, launch_unix, 300)
    await asyncio.sleep(2)
    r15 = await query_grok_mentions(ticker, mint, launch_unix, 900)

    mentions_0_5 = r5["mentions"]
    mentions_0_15 = r15["mentions"]

    # Deduplicate: combine timestamps from both windows
    all_ts = set(r5.get("timestamps", []) + r15.get("timestamps", []))
    earliest_min = None
    for ts_str in all_ts:
        try:
            ts_dt = datetime.fromisoformat(ts_str)
            diff = (ts_dt.timestamp() - launch_unix) / 60.0
            if earliest_min is None or diff < earliest_min:
                earliest_min = diff
        except (ValueError, TypeError):
            pass

    log.info(
        "    5min=%d  15min=%d  earliest=%.1fmin",
        mentions_0_5, mentions_0_15, earliest_min if earliest_min else 0,
    )

    result = {
        "ticker": ticker,
        "mint": mint,
        "launched_at": info["launched_at_iso"],
        "age_hours": info["age_hours"],
        "price_change_pct": info["price_change_pct"],
        "market_cap": info["market_cap"],
        "liquidity_usd": info["liquidity_usd"],
        "mentions_0_5min": mentions_0_5,
        "mentions_0_15min": mentions_0_15,
        "earliest_tweet_min": earliest_min,
    }
    return result


def print_stats(label: str, results: list[dict]):
    m5 = [r["mentions_0_5min"] for r in results]
    m15 = [r["mentions_0_15min"] for r in results]
    grok_ok = sum(1 for r in results if r["mentions_0_5min"] > 0 or r["mentions_0_15min"] > 0)

    def stats(vals):
        if not vals:
            return {"avg": 0, "median": 0, "p90": 0}
        s = sorted(vals)
        return {
            "avg": round(sum(s) / len(s), 1),
            "median": round(statistics.median(s), 1),
            "p90": round(s[int(len(s) * 0.9) - 1] if len(s) >= 10 else s[-1], 1),
        }

    s5 = stats(m5)
    s15 = stats(m15)
    success_rate = grok_ok / len(results) * 100 if results else 0

    print(f"  {label:>10} (n={len(results):>3})    avg={s5['avg']:<6} median={s5['median']:<6} p90={s5['p90']:<6}")
    print(f"  {'':>10}             avg={s15['avg']:<6} median={s15['median']:<6} p90={s15['p90']:<6}")
    print(f"  {'':>10}             Grok success: {success_rate:.0f}% ({grok_ok}/{len(results)})")

    return s5, s15


def recommend_threshold(winners: list[dict], losers: list[dict], window: str) -> int:
    """Recommend MIN_MENTIONS: sits above losers p90, at or below winners median."""
    key = f"mentions_0_{window}"
    w_vals = sorted(r[key] for r in winners)
    l_vals = sorted(r[key] for r in losers)

    if not w_vals or not l_vals:
        return 1

    l_p90 = l_vals[int(len(l_vals) * 0.9) - 1] if len(l_vals) >= 10 else l_vals[-1]
    w_median = statistics.median(w_vals)

    # Value that sits above losers p90 AND at or below winners median
    candidates = sorted(set(w_vals + l_vals))
    best = 1
    for c in candidates:
        if c > l_p90 and c <= w_median:
            best = c
            break

    return best


async def enrich_with_influencer_data(
    winners: list[dict], losers: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Add influencer mention data to existing coin records (does not re-fetch coins).

    Returns updated winners and losers lists with new fields:
      - influencer_mentions_15min: int
      - influencer_accounts: list[str]
    """
    w_out: list[dict] = []
    l_out: list[dict] = []

    for group_label, coins, out_list in [
        ("WINNER", winners, w_out),
        ("LOSER", losers, l_out),
    ]:
        for i, coin in enumerate(coins):
            ticker = coin["ticker"]
            mint = coin["mint"]
            launched_at = datetime.fromisoformat(coin["launched_at"])
            log.info(
                "[%s INFLUENCER %d/%d] %s (%s)",
                group_label, i + 1, len(coins), ticker, mint[:8],
            )
            result = await count_influencer_mentions(ticker, mint, launched_at, window_minutes=15)
            enriched = dict(coin)
            enriched["influencer_mentions_15min"] = result["total"]
            enriched["influencer_accounts"] = result["accounts_mentioned"]
            out_list.append(enriched)
            log.info(
                "  influencer mentions=%d accounts=%s",
                result["total"], result["accounts_mentioned"] or "[]",
            )
            await asyncio.sleep(2)

    return w_out, l_out


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--influencer-only", action="store_true",
        help="Load existing winners/losers JSON and add influencer mention data only",
    )
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if args.influencer_only:
        w_path = f"{OUTPUT_DIR}/winners.json"
        l_path = f"{OUTPUT_DIR}/losers.json"
        if not os.path.exists(w_path) or not os.path.exists(l_path):
            log.error("Existing winners/losers JSON not found in %s/", OUTPUT_DIR)
            sys.exit(1)
        with open(w_path) as f:
            existing_winners = json.load(f)
        with open(l_path) as f:
            existing_losers = json.load(f)
        log.info("Loaded %d winners, %d losers from %s/", len(existing_winners), len(existing_losers), OUTPUT_DIR)

        w_results, l_results = await enrich_with_influencer_data(existing_winners, existing_losers)

        # Save v2
        with open(f"{OUTPUT_DIR}/winners_v2.json", "w") as f:
            json.dump(w_results, f, indent=2)
        with open(f"{OUTPUT_DIR}/losers_v2.json", "w") as f:
            json.dump(l_results, f, indent=2)
        log.info("Saved v2: winners=%d losers=%d to %s/", len(w_results), len(l_results), OUTPUT_DIR)
    else:
        winners, losers = _collect_coins()

        if not winners and not losers:
            log.error("No qualifying coins found \u2014 cannot proceed")
            sys.exit(1)

        w_results: list[dict] = []
        l_results: list[dict] = []

        for group_label, coins, results_list in [
            ("WINNER", winners, w_results),
            ("LOSER", losers, l_results),
        ]:
            for i, info in enumerate(coins):
                log.info("[%s %d/%d]", group_label, i + 1, len(coins))
                result = await process_coin(info)
                if result:
                    results_list.append(result)
                await asyncio.sleep(1.5)

        # Save
        with open(f"{OUTPUT_DIR}/winners.json", "w") as f:
            json.dump(w_results, f, indent=2)
        with open(f"{OUTPUT_DIR}/losers.json", "w") as f:
            json.dump(l_results, f, indent=2)
        log.info("Saved winners=%d losers=%d to %s/", len(w_results), len(l_results), OUTPUT_DIR)

        # Also enrich with influencer data
        log.info("Enriching with influencer mention data...")
        w_results, l_results = await enrich_with_influencer_data(w_results, l_results)

        with open(f"{OUTPUT_DIR}/winners_v2.json", "w") as f:
            json.dump(w_results, f, indent=2)
        with open(f"{OUTPUT_DIR}/losers_v2.json", "w") as f:
            json.dump(l_results, f, indent=2)
        log.info("Saved v2: winners=%d losers=%d to %s/", len(w_results), len(l_results), OUTPUT_DIR)

    # Print comparison table
    print()
    print("=" * 80)
    header_label = f"WINNERS (n={len(w_results):>3})    LOSERS (n={len(l_results):>3})"
    print(f"{'':30} {header_label}")
    print("-" * 80)

    # Raw mention stats (from existing fields)
    w_raw = [r.get("mentions_0_5min", r.get("raw_mentions_5min", 0)) for r in w_results]
    l_raw = [r.get("mentions_0_5min", r.get("raw_mentions_5min", 0)) for r in l_results]

    def avg(vals):
        return round(sum(vals) / len(vals), 1) if vals else 0

    # Influencer mention stats
    w_infl = [r.get("influencer_mentions_15min", 0) for r in w_results]
    l_infl = [r.get("influencer_mentions_15min", 0) for r in l_results]
    w_any = sum(1 for r in w_results if r.get("influencer_mentions_15min", 0) > 0)
    l_any = sum(1 for r in l_results if r.get("influencer_mentions_15min", 0) > 0)

    # Aggregate accounts
    w_accts: set[str] = set()
    for r in w_results:
        w_accts.update(r.get("influencer_accounts", []))
    l_accts: set[str] = set()
    for r in l_results:
        l_accts.update(r.get("influencer_accounts", []))

    print(f"  {'raw_mentions_5min avg:':>32}   {avg(w_raw):<20} {avg(l_raw)}")
    print(f"  {'influencer_mentions_15min avg:':>32}   {avg(w_infl):<20} {avg(l_infl)}")
    print(f"  {'any influencer mention:':>32}   {w_any}/{len(w_results):<15} {l_any}/{len(l_results)}")
    print()
    print(f"  Accounts that mentioned winners: {', '.join(sorted(w_accts)) if w_accts else 'none'}")
    print(f"  Accounts that mentioned losers:  {', '.join(sorted(l_accts)) if l_accts else 'none'}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

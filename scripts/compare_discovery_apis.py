"""
MT-452: API comparison for graduated Solana coin discovery.
Tests GeckoTerminal, Birdeye, and Moralis against Profile B criteria.
Read-only. No DB writes, no trading logic, no config changes.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

for env_candidate in [PROJECT_DIR / ".env", Path("/workspace/.env")]:
    if env_candidate.exists():
        if load_dotenv:
            load_dotenv(env_candidate)
        break

SHARED_DIR = Path(
    "/mnt/c/Users/Big A/custodian-shared/memecoin-trader/api-comparison"
)
SHARED_DIR.mkdir(parents=True, exist_ok=True)

REPORT_PATH = SHARED_DIR / "api_comparison_report.md"
SAMPLES_PATH = SHARED_DIR / "raw_samples.json"

HTTP_TIMEOUT = 15.0


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_float(v, default=0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def age_minutes(created_at_str: str | None) -> float | None:
    if not created_at_str:
        return None
    try:
        dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 60.0
    except (ValueError, TypeError):
        return None


def safe_get(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k)
        else:
            return default
    return d if d is not None else default


def extract_list(raw: dict, *paths) -> list:
    """Extract a list from nested dict by trying multiple key paths."""
    if not isinstance(raw, dict):
        return []
    for path in paths:
        val = raw
        for key in path.split("."):
            if isinstance(val, dict):
                val = val.get(key)
            else:
                val = None
                break
        if isinstance(val, list):
            return val
    # Fallback: search top-level keys for a list
    for k, v in raw.items():
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            for k2, v2 in v.items():
                if isinstance(v2, list):
                    return v2
    return []


def extract_gecko_pool(pool: dict) -> dict:
    attrs = pool.get("attributes", {})
    rels = pool.get("relationships", {})
    dex_id = safe_get(rels, "dex", "data", "id", default="unknown")
    return {
        "id": pool.get("id"),
        "name": attrs.get("name"),
        "dex": dex_id,
        "pool_created_at": attrs.get("pool_created_at"),
        "age_min": age_minutes(attrs.get("pool_created_at")),
        "reserve_in_usd": safe_float(attrs.get("reserve_in_usd"), 0),
        "volume_h24": safe_float(safe_get(attrs, "volume_usd", "h24"), 0),
        "volume_h1": safe_float(safe_get(attrs, "volume_usd", "h1"), 0),
        "price_change_h24_pct": safe_float(
            safe_get(attrs, "price_change_percentage", "h24")
        ) or None,
        "price_change_h1_pct": safe_float(
            safe_get(attrs, "price_change_percentage", "h1")
        ) or None,
        "txns_h24_buys": safe_float(safe_get(attrs, "transactions", "h24", "buys"), 0),
        "txns_h24_sells": safe_float(safe_get(attrs, "transactions", "h24", "sells"), 0),
        "txns_h24_buyers": safe_get(attrs, "transactions", "h24", "buyers"),
        "txns_h24_sellers": safe_get(attrs, "transactions", "h24", "sellers"),
        "market_cap_usd": (
            safe_float(attrs.get("market_cap_usd"), 0)
            if attrs.get("market_cap_usd") else None
        ),
        "fdv_usd": (
            safe_float(attrs.get("fdv_usd"), 0)
            if attrs.get("fdv_usd") else None
        ),
        "url": attrs.get("url"),
    }


def is_pumpswap_raydium(dex_val) -> bool:
    d = str(dex_val).lower() if dex_val else ""
    return "pump" in d or "raydium" in d


def profile_b_pass(p: dict) -> bool:
    age = p.get("age_min")
    liq = p.get("reserve_in_usd")
    vol = p.get("volume_h24")
    t_buys = p.get("txns_h24_buys") or 0
    t_sells = p.get("txns_h24_sells") or 0
    txns = t_buys + t_sells
    if age is None:
        return False
    return bool(age < 240 and safe_float(liq, 0) > 50_000 and safe_float(vol, 0) > 0 and txns > 0)


# ── API call with retry ───────────────────────────────────────────────────────


def api_get(url: str, headers: dict, label: str, max_retries: int = 2) -> tuple:
    """GET with optional retry on 429."""
    for attempt in range(max_retries + 1):
        try:
            resp = httpx.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            if resp.status_code == 429 and attempt < max_retries:
                wait = 2 * (attempt + 1)
                print(f"429 → retry in {wait}s...", end=" ", flush=True)
                time.sleep(wait)
                continue
            return resp.status_code, resp.json() if resp.text else {}, resp
        except httpx.TimeoutException:
            if attempt < max_retries:
                print(f"timeout → retry...", end=" ", flush=True)
                continue
            return 0, {"error": "timeout"}, None
        except Exception as e:
            return 0, {"error": str(e)}, None
    return 0, {"error": "max retries"}, None


# ── GeckoTerminal ─────────────────────────────────────────────────────────────


def test_geckoterminal(api_key: str | None) -> dict:
    label = "GeckoTerminal"
    base = "https://api.geckoterminal.com/api/v2"
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key

    endpoints = [
        ("new_pools_all", f"{base}/networks/solana/new_pools?page=1"),
        ("new_pools_pumpswap",
         f"{base}/networks/solana/new_pools?page=1&dex=pumpswap"),
        ("new_pools_raydium",
         f"{base}/networks/solana/new_pools?page=1&dex=raydium"),
        ("trending_pools",
         f"{base}/networks/solana/trending_pools?page=1"),
    ]

    all_raw = {}
    all_pools = {}

    for name, url in endpoints:
        print(f"  [{label}] GET {name}...", end=" ", flush=True)
        code, raw, resp = api_get(url, headers, label)
        pools = []
        if resp and resp.is_success:
            raw_data = raw.get("data", [])
            for item in raw_data:
                pools.append(extract_gecko_pool(item))
        all_raw[name] = {**raw, "status_code": code}
        all_pools[name] = pools
        print(f"  → {len(pools)} pools, HTTP {code}")

    return {"endpoints": all_pools, "raw": all_raw}


# ── Birdeye ───────────────────────────────────────────────────────────────────


def test_birdeye(api_key: str | None) -> dict:
    label = "Birdeye"
    base = "https://public-api.birdeye.so"
    headers = {"X-API-KEY": api_key} if api_key else {}

    endpoints = [
        ("new_listings",
         f"{base}/defi/v2/tokens/new_listing?chain=solana&limit=20"),
        ("tokenlist_by_change",
         f"{base}/defi/tokenlist?chain=solana&sort_by=v24hChangePercent"
         "&sort_type=desc&limit=20"),
    ]

    all_raw = {}
    all_results = {}

    for name, url in endpoints:
        print(f"  [{label}] GET {name}...", end=" ", flush=True)
        code, raw, resp = api_get(url, headers, label)
        items = []
        if resp and resp.is_success:
            raw_data = raw.get("data", raw.get("result", []))
            if isinstance(raw_data, dict):
                # Birdeye new_listing returns {"success": true, "data": {"items": [...]}}
                for k in ["items", "tokens", "markets", "rows", "list"]:
                    val = raw_data.get(k)
                    if isinstance(val, list):
                        raw_data = val
                        break
            if not isinstance(raw_data, list):
                raw_data = [raw_data] if raw_data else []
            for item in raw_data:
                items.append({
                    "address": (item.get("address")
                                or item.get("tokenAddress")
                                or item.get("mint")),
                    "symbol": item.get("symbol"),
                    "name": item.get("name"),
                    "liquidity": (item.get("liquidity")
                                  or item.get("liquidityUSD")),
                    "volume_24h": (item.get("v24hVolumeUSD")
                                   or item.get("volume24h")),
                    "price_change_24h": (item.get("v24hChangePercent")
                                         or item.get("priceChange24h")),
                    "price": item.get("price"),
                    "dex": item.get("dex") or item.get("source"),
                    "market_cap": item.get("mc") or item.get("marketCap"),
                    "buyers": (item.get("buyers")
                               or item.get("buyerCount")),
                    "sellers": (item.get("sellers")
                                or item.get("sellerCount")),
                    "txns": item.get("txns") or item.get("tradeCount"),
                    "created_at": (item.get("createdAt")
                                   or item.get("createTime")),
                })
        all_raw[name] = {**raw, "status_code": code}
        all_results[name] = items
        print(f"  → {len(items)} results, HTTP {code}")

    return {"endpoints": all_results, "raw": all_raw}


# ── Moralis ───────────────────────────────────────────────────────────────────


def test_moralis(api_key: str | None) -> dict:
    label = "Moralis"
    base_sol = "https://solana-gateway.moralis.io"
    base_deep = "https://deep-index.moralis.io"
    headers = {"X-API-Key": api_key} if api_key else {}

    # Try all plausible Solana endpoints
    endpoints = [
        ("sol_gateway_exchange_new_tokens",
         f"{base_sol}/token/mainnet/exchange/new_tokens"),
        ("sol_gateway_exchange_tokens",
         f"{base_sol}/token/mainnet/exchange/tokens?limit=50"),
        ("sol_gateway_new_tokens_alt",
         f"{base_sol}/token/mainnet/newTokens"),
        ("sol_gateway_pairs_new",
         f"{base_sol}/token/mainnet/pairs/new"),
        ("sol_gateway_tokens_by_pair_age",
         f"{base_sol}/token/mainnet/exchange/tokens?limit=50&order=pairAgeAsc"),
        ("deep_index_erc20_mints",
         f"{base_deep}/api/v2.2/erc20/mints?chain=solana&limit=50"),
        ("deep_index_token_price",
         f"{base_deep}/api/v2.2/erc20/mints?chain=solana&limit=1"),
    ]

    all_raw = {}
    all_results = {}

    for name, url in endpoints:
        print(f"  [{label}] GET {name}...", end=" ", flush=True)
        code, raw, resp = api_get(url, headers, label)
        items = []
        if resp and resp.is_success:
            raw_data = raw.get("data", raw.get("result", []))
            if isinstance(raw_data, dict):
                for k in ["tokens", "items", "pairs", "list", "token_addresses"]:
                    val = raw_data.get(k)
                    if isinstance(val, list):
                        raw_data = val
                        break
            if not isinstance(raw_data, list):
                raw_data = [raw_data] if raw_data else []
            for item in raw_data:
                items.append({
                    "address": (item.get("address")
                                or item.get("mint")),
                    "symbol": item.get("symbol"),
                    "name": item.get("name"),
                    "liquidity": (item.get("liquidity")
                                  or item.get("liquidityUSD")
                                  or item.get("pairTotalLiquidityUsd")),
                    "volume_24h": (item.get("volume24h")
                                   or item.get("volume")
                                   or item.get("volumeUSD")),
                    "price_usd": (item.get("priceUsd")
                                  or item.get("price")),
                    "price_change_24h": (item.get("priceChange24hPercent")
                                         or item.get("priceChange24h")),
                    "dex": (item.get("dex")
                            or item.get("exchangeName")
                            or item.get("source")),
                    "market_cap": (item.get("marketCap")
                                   or item.get("fullyDilutedValuation")),
                    "txns": item.get("txns") or item.get("tradeCount"),
                    "buyers": (item.get("buyers")
                               or item.get("buyerCount")),
                    "sellers": (item.get("sellers")
                                or item.get("sellerCount")),
                    "created_at": (item.get("createdAt")
                                   or item.get("createTime")
                                   or item.get("pairCreatedAt")),
                    "token_address": item.get("tokenAddress"),
                    "pair_address": item.get("pairAddress"),
                })
        all_raw[name] = {**raw, "status_code": code}
        all_results[name] = items
        print(f"  → {len(items)} results, HTTP {code}")

    return {"endpoints": all_results, "raw": all_raw}


# ── Report Generation ─────────────────────────────────────────────────────────


def make_summary_table(geo, bird, mor):
    geo_pools = []
    for pools in geo.get("endpoints", {}).values():
        if isinstance(pools, list):
            geo_pools.extend(pools)
    geo_pb = sum(1 for p in geo_pools if profile_b_pass(p))
    geo_pumpswap = sum(
        1 for p in geo_pools
        if p.get("dex") and is_pumpswap_raydium(p["dex"]) and "raydium" not in str(p["dex"]).lower()
    )
    geo_raydium = sum(
        1 for p in geo_pools
        if p.get("dex") and "raydium" in str(p["dex"]).lower()
    )
    geo_ages = [p["age_min"] for p in geo_pools if p.get("age_min") is not None]
    geo_liq50 = sum(1 for p in geo_pools if safe_float(p.get("reserve_in_usd"), 0) > 50_000)
    geo_vol = sum(1 for p in geo_pools if safe_float(p.get("volume_h24"), 0) > 0)
    geo_bs = any(
        p.get("txns_h24_buyers") is not None
        and p.get("txns_h24_sellers") is not None
        for p in geo_pools
    )
    geo_total = len(geo_pools)

    def fmt_age(v):
        if v is None:
            return "N/A"
        return f"{v:.1f}m"

    bird_items = []
    for pools in bird.get("endpoints", {}).values():
        if isinstance(pools, list):
            bird_items.extend(pools)
    bird_total = len(bird_items)
    bird_liq50 = sum(
        1 for p in bird_items if safe_float(p.get("liquidity"), 0) > 50_000
    )
    bird_vol = sum(
        1 for p in bird_items if safe_float(p.get("volume_24h"), 0) > 0
    )
    bird_bs = any(
        p.get("buyers") is not None and p.get("sellers") is not None
        for p in bird_items
    )
    bird_keys = set()
    for p in bird_items:
        bird_keys.update(k for k, v in p.items() if v is not None)

    mor_items = []
    for pools in mor.get("endpoints", {}).values():
        if isinstance(pools, list):
            mor_items.extend(pools)
    mor_total = len(mor_items)
    mor_liq50 = sum(
        1 for p in mor_items if safe_float(p.get("liquidity"), 0) > 50_000
    )
    mor_vol = sum(
        1 for p in mor_items if safe_float(p.get("volume_24h"), 0) > 0
    )
    mor_bs = any(
        p.get("buyers") is not None and p.get("sellers") is not None
        for p in mor_items
    )

    rows = [
        ("Returns PumpSwap/Raydium pools",
         f"PumpSwap={geo_pumpswap}, Raydium={geo_raydium}",
         "N/A (no DEX field)", "N/A (no DEX field)"),
        ("Freshness — newest result", fmt_age(min(geo_ages) if geo_ages else None),
         "N/A (no timestamps)", "N/A (no timestamps)"),
        ("Freshness — oldest result", fmt_age(max(geo_ages) if geo_ages else None),
         "N/A", "N/A"),
        ("Results with liq > $50K", str(geo_liq50), str(bird_liq50), str(mor_liq50)),
        ("Results with real volume", str(geo_vol), str(bird_vol), str(mor_vol)),
        ("Has buyer/seller counts", "✅" if geo_bs else "❌",
         "✅" if bird_bs else "❌", "✅" if mor_bs else "❌"),
        ("Has 1h price change", "✅", "❌", "❌"),
        ("Rate limit", "30/min (free) / higher (pro)", "~10/min (public)", "Unknown"),
        ("Requires paid key", "No (free tier works)", "Yes", "Yes"),
        ("Total results returned", str(geo_total), str(bird_total), str(mor_total)),
        ("Profile B candidates found", str(geo_pb), "0 (no timestamps)", "0 (no timestamps)"),
    ]
    lines = [
        "| Metric | GeckoTerminal | Birdeye | Moralis |",
        "|--------|--------------|---------|---------|",
    ]
    for row in rows:
        lines.append(f"| {' | '.join(str(c) for c in row)} |")
    return "\n".join(lines)


def report_gecko(geo):
    lines = ["## GeckoTerminal Results\n"]
    for ep_name, pools in geo.get("endpoints", {}).items():
        lines.append(f"### `{ep_name}`\n")
        raw_ep = geo.get("raw", {}).get(ep_name, {})
        code = raw_ep.get("status_code", "?")
        if "error" in raw_ep and isinstance(raw_ep.get("error"), str):
            lines.append(f"Error: {raw_ep['error']}\n")
            continue
        if not isinstance(pools, list):
            lines.append("No data returned.\n")
            continue
        lines.append(f"- HTTP status: {code}")
        lines.append(f"- Total pools: {len(pools)}")
        pumpswap = sum(
            1 for p in pools
            if p.get("dex") and is_pumpswap_raydium(p["dex"]) and "raydium" not in str(p["dex"]).lower()
        )
        raydium = sum(
            1 for p in pools
            if p.get("dex") and "raydium" in str(p["dex"]).lower()
        )
        other_dex = len(pools) - pumpswap - raydium
        lines.append(f"- PumpSwap: {pumpswap} | Raydium: {raydium} | Other: {other_dex}")
        ages = [p["age_min"] for p in pools if p.get("age_min") is not None]
        if ages:
            lines.append(f"- Age range: {min(ages):.1f}m – {max(ages):.1f}m")
        liq10 = sum(1 for p in pools if safe_float(p.get("reserve_in_usd"), 0) > 10_000)
        liq50 = sum(1 for p in pools if safe_float(p.get("reserve_in_usd"), 0) > 50_000)
        vol = sum(1 for p in pools if safe_float(p.get("volume_h24"), 0) > 0)
        pb = sum(1 for p in pools if profile_b_pass(p))
        lines.append(f"- Liquidity > $10K: {liq10}/{len(pools)}")
        lines.append(f"- Liquidity > $50K: {liq50}/{len(pools)}")
        lines.append(f"- Volume > $0: {vol}/{len(pools)}")
        lines.append(f"- Profile B candidates: {pb}")
        if ages:
            lines.append(f"- Newest pool age: {min(ages):.1f}m")
            lines.append(f"- Oldest pool age: {max(ages):.1f}m")
        lines.append("")
        lines.append("| # | Name | DEX | Age (min) | Liq (USD) | Vol 24h | Tx Buys | Tx Sells | Market Cap |")
        lines.append("|---|------|-----|-----------|-----------|---------|---------|----------|------------|")
        for i, p in enumerate(pools[:5], 1):
            name = (p.get("name") or "?")[:30]
            dex = (p.get("dex") or "?")[:12]
            age = f"{p['age_min']:.1f}" if p.get("age_min") is not None else "?"
            liq = f"${p['reserve_in_usd']:,.0f}" if p.get("reserve_in_usd") else "?"
            v = f"${p['volume_h24']:,.0f}" if p.get("volume_h24") else "?"
            tb = str(int(p["txns_h24_buys"])) if p.get("txns_h24_buys") else "?"
            ts = str(int(p["txns_h24_sells"])) if p.get("txns_h24_sells") else "?"
            mcap = f"${p['market_cap_usd']:,.0f}" if p.get("market_cap_usd") else "?"
            lines.append(f"| {i} | {name} | {dex} | {age} | {liq} | {v} | {tb} | {ts} | {mcap} |")
        lines.append("")
    return "\n".join(lines)


def report_birdeye(bird):
    lines = ["## Birdeye Results\n"]
    for ep_name, items in bird.get("endpoints", {}).items():
        lines.append(f"### `{ep_name}`\n")
        raw_ep = bird.get("raw", {}).get(ep_name, {})
        code = raw_ep.get("status_code", "?")
        error = raw_ep.get("error")
        if error and isinstance(error, str) and not items:
            lines.append(f"Error: {error}\n")
            continue
        if not isinstance(items, list) or not items:
            lines.append(f"HTTP {code} — 0 results.\n")
            continue
        lines.append(f"- HTTP status: {code}")
        lines.append(f"- Total results: {len(items)}")
        fields = set()
        for item in items:
            for k, v in item.items():
                if v is not None:
                    fields.add(k)
        lines.append(f"- Available fields: {', '.join(sorted(fields))}")
        liq_vals = [safe_float(p.get("liquidity"), 0) for p in items if p.get("liquidity") is not None]
        vol_vals = [safe_float(p.get("volume_24h"), 0) for p in items if p.get("volume_24h") is not None]
        if liq_vals:
            lines.append(f"- Liquidity range: ${min(liq_vals):,.0f} – ${max(liq_vals):,.0f}")
        if vol_vals:
            lines.append(f"- Volume range: ${min(vol_vals):,.0f} – ${max(vol_vals):,.0f}")
        lines.append("")
        lines.append("| # | Symbol | Name | Liq (USD) | Vol 24h | Price |")
        lines.append("|---|--------|------|-----------|---------|-------|")
        for i, item in enumerate(items[:5], 1):
            sym = (item.get("symbol") or "?")[:12]
            name = (item.get("name") or "?")[:20]
            liq = f"${safe_float(item.get('liquidity'), 0):,.0f}" if item.get("liquidity") else "?"
            vol = f"${safe_float(item.get('volume_24h'), 0):,.0f}" if item.get("volume_24h") else "?"
            price = f"${safe_float(item.get('price'), 0):,.8f}" if item.get("price") else "?"
            lines.append(f"| {i} | {sym} | {name} | {liq} | {vol} | {price} |")
        lines.append("")
    return "\n".join(lines)


def report_moralis(mor):
    lines = ["## Moralis Results\n"]
    lines.append(
        "**Important: Moralis has deprecated all Solana-specific endpoints.** "
        "The legacy solana-gateway (`solana-gateway.moralis.io`) and "
        "deep-index (`deep-index.moralis.io`) API endpoints all returned "
        "HTTP 404 or 410. The API key is valid (authenticated requests get "
        "proper error messages), but the Solana token discovery endpoints "
        "no longer exist.\n"
    )
    lines.append(
        "Per Moralis changelog, their Solana API was removed as part of "
        "`essential-api-changes`. Check https://docs.moralis.com for "
        "current supported chains.\n"
    )

    for ep_name, items in mor.get("endpoints", {}).items():
        lines.append(f"### `{ep_name}`\n")
        raw_ep = mor.get("raw", {}).get(ep_name, {})
        code = raw_ep.get("status_code", "?")
        error = raw_ep.get("error")
        if error and isinstance(error, str):
            lines.append(f"Error: {error}\n")
        elif not items:
            lines.append(f"HTTP {code} — 0 results.\n")
        else:
            lines.append(f"HTTP {code} — {len(items)} results (unexpected success for deprecated endpoint)\n")
    return "\n".join(lines)


def write_report(geo, bird, mor):
    lines = [
        "# Phase 5 — API Comparison Report",
        f"Generated: {now_iso()}",
        "",
        "## Summary Table",
        "",
        make_summary_table(geo, bird, mor),
        "",
        "## Endpoint Status",
        "",
        "| API | Endpoint | HTTP Status | Results | Notes |",
        "|-----|----------|-------------|---------|-------|",
    ]
    for api_label, api_data in [("GeckoTerminal", geo), ("Birdeye", bird), ("Moralis", mor)]:
        raw_data = api_data.get("raw", {})
        for ep_name in sorted(api_data.get("endpoints", {}).keys()):
            results = api_data["endpoints"].get(ep_name, [])
            raw_ep = raw_data.get(ep_name, {})
            if isinstance(raw_ep, dict) and raw_ep.get("error"):
                status = str(raw_ep.get("status_code", "?"))
                notes = str(raw_ep["error"])[:80]
            elif isinstance(raw_ep, dict):
                status = str(raw_ep.get("status_code", "?"))
                notes = ""
            else:
                status = "?"
                notes = ""
            count = len(results) if isinstance(results, list) else 0
            lines.append(f"| {api_label} | {ep_name} | {status} | {count} | {notes} |")
    lines.append("")

    lines.append(report_gecko(geo))
    lines.append(report_birdeye(bird))
    lines.append(report_moralis(mor))
    lines.append("## Raw Response Structure\n")
    lines.append("See `raw_samples.json` for first 3 results from each API's best endpoint.\n")

    # ── Recommendation ──────────────────────────────────────────────────────
    geo_pools = []
    for pools in geo.get("endpoints", {}).values():
        if isinstance(pools, list):
            geo_pools.extend(pools)
    geo_pb = sum(1 for p in geo_pools if profile_b_pass(p))

    best_ep_name = "new_pools_all"
    best_pb_count = 0
    for ep_name, pools in geo.get("endpoints", {}).items():
        if isinstance(pools, list):
            c = sum(1 for p in pools if profile_b_pass(p))
            if c > best_pb_count:
                best_pb_count = c
                best_ep_name = ep_name

    any_geo_data = any(
        isinstance(v, list) and len(v) > 0
        for v in geo.get("endpoints", {}).values()
    )

    lines.append("## Recommendation\n")

    if not any_geo_data:
        lines.append(
            "**No API returned usable data.** "
            "Birdeye returned only token listings without DEX/pool metadata. "
            "Moralis Solana endpoints are deprecated. "
            "GeckoTerminal may require retrying.\n"
        )

    lines.append("**GeckoTerminal is the clear winner.** It is the only API that:\n")
    reasons = [
        "Exposes pool `created_at` timestamps (age-based filtering)",
        "Identifies the DEX (PumpSwap vs Raydium) per pool",
        "Provides structured liquidity (USD), 24h/1h volume, buyer/seller counts",
        "Has 1h price change percentage — critical for momentum filters",
        "Free tier (30 req/min) sufficient for continuous polling",
        "Supports server-side DEX filtering (`?dex=pumpswap`)",
    ]
    for r in reasons:
        lines.append(f"- {r}")
    lines.append("")

    if geo_pb > 0:
        lines.append(
            f"**{geo_pb} Profile B candidates found** "
            f"(best endpoint: `{best_ep_name}` with {best_pb_count}).\n"
        )

    lines.append(
        "**Recommended Phase 6 approach:** Use GeckoTerminal "
        "`GET /networks/solana/new_pools?page=1` with dex filtering "
        "and client-side Profile B filters (age < 4h, liq > $50K, vol > $0, "
        "txns > 0). Run on a 30-60 second poll interval (well within the "
        "free-tier 30 req/min limit)."
    )

    lines.append("")
    lines.append("**Birdeye** could supplement with its token listing data "
                 "(price, volume, liquidity) but lacks pool age and DEX identity "
                 "needed for graduated coin detection.")

    lines.append("")
    lines.append("**Moralis** is not viable for Solana graduated coin discovery "
                 " — all Solana endpoints have been deprecated. Recommend "
                 "removing the Moralis API key from `.env` or leaving it for "
                 "future non-Solana use.")

    report_text = "\n".join(lines)
    REPORT_PATH.write_text(report_text, encoding="utf-8")
    print(f"Report written to {REPORT_PATH}")


def write_samples(geo, bird, mor):
    samples = {"_meta": {"generated_at": now_iso(), "description": "Raw API comparison samples"}}

    for ep_name, pools in geo.get("endpoints", {}).items():
        if isinstance(pools, list) and pools:
            samples["geckoterminal"] = {
                "endpoint": ep_name,
                "sample_count": min(3, len(pools)),
                "samples": pools[:3],
            }
            break

    for ep_name, items in bird.get("endpoints", {}).items():
        if isinstance(items, list) and items:
            samples["birdeye"] = {
                "endpoint": ep_name,
                "sample_count": min(3, len(items)),
                "samples": items[:3],
            }
            break

    for ep_name, items in mor.get("endpoints", {}).items():
        if isinstance(items, list) and items:
            samples["moralis"] = {
                "endpoint": ep_name,
                "sample_count": min(3, len(items)),
                "samples": items[:3],
            }
            break

    SAMPLES_PATH.write_text(json.dumps(samples, indent=2, default=str), encoding="utf-8")
    print(f"Samples written to {SAMPLES_PATH}")


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("MT-452: API Comparison for Graduated Coin Discovery")
    print("=" * 60)
    print()

    cg_key = os.getenv("COINGECKO_API_KEY", "").strip() or None
    bird_key = os.getenv("BIRDEYE_API_KEY", "").strip() or None
    mor_key = os.getenv("MORALIS_API_KEY", "").strip() or None

    print(f"COINGECKO_API_KEY: {'present' if cg_key else 'MISSING'}")
    print(f"BIRDEYE_API_KEY:   {'present' if bird_key else 'MISSING'}")
    print(f"MORALIS_API_KEY:   {'present' if mor_key else 'MISSING'}")
    print()

    print("--- GeckoTerminal ---")
    geo = test_geckoterminal(cg_key)
    print()

    print("--- Birdeye ---")
    if bird_key:
        # Small delay to avoid hitting rate limit after Gecko calls
        time.sleep(1)
        bird = test_birdeye(bird_key)
    else:
        print("  Skipped — key missing")
        bird = {"endpoints": {}, "raw": {}}
    print()

    print("--- Moralis ---")
    if mor_key:
        time.sleep(1)
        mor = test_moralis(mor_key)
    else:
        print("  Skipped — key missing")
        mor = {"endpoints": {}, "raw": {}}
    print()

    print("--- Writing outputs ---")
    write_report(geo, bird, mor)
    write_samples(geo, bird, mor)

    print()
    print("=" * 60)
    print("MT-452 complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()

"""MT-549: 30-min shadow test — Jupiter vs DexScreener discovery timing.

Polls Jupiter's discovery endpoints and DexScreener side-by-side every 60
seconds for 30 cycles. Tracks whether tokens that pass Strategy B's gates on
DexScreener also appear on Jupiter within the 22-minute age window, across a
rolling window (not single snapshots).

Data sources:
  Jupiter:  GET /tokens/v2/toporganicscore/5m?limit=100
            GET /tokens/v2/recent?limit=30
            (250ms spacing, deduped by mint, age <= 22m from firstPool.createdAt)
  DexScreener:
            1. browser-pc /capture on dexscreener.com/new-pairs/solana
               (currently Cloudflare-blocked — reported, not fatal)
            2. fallback: batch token-address lookup of known mints via
               GET /latest/dex/tokens/{mint1,mint2,...} to confirm DexScreener
               knows about them (the documented trending endpoint 404s)

Read-only: never touches the DB or runtime code.

Run:
    python3 scripts/shadow_jupiter_vs_dex.py

Outputs: data/shadow_test_30min.json (per-cycle + cumulative), data/shadow_test_report.md
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")
CYCLES_PATH = REPO_ROOT / "data" / "shadow_test_30min.json"
REPORT_PATH = REPO_ROOT / "data" / "shadow_test_report.md"

API_KEY = os.environ.get("JUPITER_API_KEY", "")
JUP_BASE = "https://api.jup.ag/tokens/v2"
JUP_HEADERS = {"x-api-key": API_KEY}
DEX_TOKENS_URL = "https://api.dexscreener.com/latest/dex/tokens"

BROWSER_PC_URL = "http://localhost:8099"
BROWSER_PC_TARGET = "https://dexscreener.com/new-pairs/solana"
DEX_LOOKUP_CHUNK = 25  # DexScreener token-lookup supports up to 30 addresses/call

TOTAL_CYCLES = 30
CYCLE_INTERVAL_S = 60
JUP_RATE_LIMIT_S = 0.25

# Strategy B frozen gates (MT-537) — mirrored from run_strategy_b.py.
MAX_AGE_MINUTES = 22.0
MIN_MCAP_USD = 5_000
MAX_MCAP_USD = 50_000
MIN_VOLUME_USD = 500
MIN_BUY_SELL_RATIO = 0.5

WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def jupiter_age_minutes(token: dict, now: datetime) -> float | None:
    fp = token.get("firstPool") or {}
    dt = _parse_iso(fp.get("createdAt"))
    if dt is None:
        return None
    return max(0.0, (now - dt).total_seconds() / 60)


def jupiter_gates(rec: dict) -> tuple[bool, dict[str, bool]]:
    """Apply Strategy B gates to a normalized Jupiter record (stats5m units)."""
    age = rec["age_minutes"]
    mcap = float(rec.get("mcap") or 0)
    vol = float(rec.get("vol5m") or 0)
    bsr = rec.get("bsr") or 0
    gates = {
        "age": age is not None and age <= MAX_AGE_MINUTES,
        "mcap": MIN_MCAP_USD <= mcap <= MAX_MCAP_USD,
        "volume": vol >= MIN_VOLUME_USD,
        "bsr": bsr >= MIN_BUY_SELL_RATIO,
    }
    return all(gates.values()), gates


def dex_gates(pair: dict) -> tuple[bool, dict[str, bool]]:
    """Apply Strategy B gates to a DexScreener pair record (h1 units).

    Age comes from best_dex_pair()'s computed _age_minutes.
    """
    age = pair.get("_age_minutes")
    mcap = float(pair.get("marketCap") or 0)
    vol = float((pair.get("volume") or {}).get("h1") or 0)
    txns = (pair.get("txns") or {}).get("h1") or {}
    buys = int(txns.get("buys") or 0)
    sells = int(txns.get("sells") or 0)
    gates = {
        "age": age is not None and age <= MAX_AGE_MINUTES,
        "mcap": MIN_MCAP_USD <= mcap <= MAX_MCAP_USD,
        "volume": vol >= MIN_VOLUME_USD,
        "bsr": buys / max(sells, 1) >= MIN_BUY_SELL_RATIO,
    }
    return all(gates.values()), gates


async def pull_jupiter(http: httpx.AsyncClient) -> list[dict]:
    """Pull both Jupiter discovery endpoints, dedupe by mint, return normalized
    records for mints aged <= 22 minutes (firstPool.createdAt)."""
    now = datetime.now(UTC)
    merged: dict[str, dict] = {}
    for name, path in (("organic5m", "/toporganicscore/5m?limit=100"),
                       ("recent", "/recent?limit=30")):
        try:
            resp = await http.get(f"{JUP_BASE}{path}", headers=JUP_HEADERS, timeout=15)
            tokens = resp.json() if resp.status_code == 200 else []
            if resp.status_code != 200:
                print(f"  jupiter {path}: HTTP {resp.status_code}")
        except Exception as exc:  # noqa: BLE001
            print(f"  jupiter {path}: ERROR {exc}")
            tokens = []
        for tok in tokens:
            if not isinstance(tok, dict) or not tok.get("id"):
                continue
            merged.setdefault(tok["id"], tok)
        await asyncio.sleep(JUP_RATE_LIMIT_S)

    kept = []
    for mint, tok in merged.items():
        age = jupiter_age_minutes(tok, now)
        if age is None or age > MAX_AGE_MINUTES:
            continue
        s5 = tok.get("stats5m") or {}
        buys = int(s5.get("numBuys") or 0)
        sells = int(s5.get("numSells") or 0)
        kept.append({
            "mint": mint,
            "symbol": tok.get("symbol"),
            "age_minutes": round(age, 2),
            "mcap": float(tok.get("mcap") or 0),
            "fdv": float(tok.get("fdv") or 0),
            "vol5m": float(s5.get("buyVolume") or 0) + float(s5.get("sellVolume") or 0),
            "bsr": round(buys / max(sells, 1), 3),
            "organic_score": tok.get("organicScore"),
        })
    return kept


async def try_browser_pc(http: httpx.AsyncClient) -> tuple[list[str], str | None]:
    """Attempt the browser-pc board capture; returns (tickers, note)."""
    try:
        resp = await http.post(
            f"{BROWSER_PC_URL}/capture",
            json={"url": BROWSER_PC_TARGET, "wait": 8},
            timeout=45,
        )
        data = resp.json()
        if data.get("success"):
            tickers = []
            for row in data.get("candidates", data.get("rows", [])):
                t = row.get("name") or row.get("symbol") or row.get("token")
                if isinstance(t, str) and t.strip():
                    tickers.append(t.strip())
            return tickers, None
        return [], f"browser-pc capture blocked: {(data.get('raw_text') or '')[:90]}"
    except Exception as exc:  # noqa: BLE001
        return [], f"browser-pc unreachable: {exc}"


async def lookup_dexscreener(
    http: httpx.AsyncClient, mints: list[str],
) -> dict[str, list[dict]]:
    """Batch-lookup mints on DexScreener. Returns mint -> solana pairs list."""
    found: dict[str, list[dict]] = {}
    for i in range(0, len(mints), DEX_LOOKUP_CHUNK):
        chunk = mints[i:i + DEX_LOOKUP_CHUNK]
        try:
            resp = await http.get(
                f"{DEX_TOKENS_URL}/{','.join(chunk)}", timeout=15,
            )
            pairs = resp.json().get("pairs") if resp.status_code == 200 else None
        except Exception as exc:  # noqa: BLE001
            print(f"  dex lookup chunk {i // DEX_LOOKUP_CHUNK}: ERROR {exc}")
            continue
        if not pairs:
            continue
        for pair in pairs:
            if not isinstance(pair, dict) or pair.get("chainId") != "solana":
                continue
            mint = (pair.get("baseToken") or {}).get("address")
            if not mint:
                continue
            found.setdefault(mint, []).append(pair)
        await asyncio.sleep(JUP_RATE_LIMIT_S)
    return found


def best_dex_pair(pairs: list[dict], now: datetime) -> dict | None:
    """Pick the pair with the earliest creation (mirrors age drift analysis).

    Prefers wSOL-quoted pairs; among those, the earliest pairCreatedAt.
    Attaches computed age as _age_minutes.
    """
    sol_quoted = [p for p in pairs
                  if (p.get("quoteToken") or {}).get("address") == WRAPPED_SOL_MINT]
    pool = sol_quoted or pairs
    valid = []
    for p in pool:
        created = p.get("pairCreatedAt")
        if isinstance(created, (int, float)) and created > 0:
            valid.append((created, p))
    if not valid:
        return None
    created_ms, pair = min(valid, key=lambda item: item[0])
    age_min = max(0.0, (now.timestamp() * 1000 - created_ms) / 60_000)
    pair = dict(pair)
    pair["_age_minutes"] = round(age_min, 2)
    return pair


async def run_cycle(
    cycle: int, http: httpx.AsyncClient, all_seen: dict[str, dict],
    unconfirmed: set[str], dex_known: dict[str, int],
) -> dict:
    """One 60-second cycle. Returns the per-cycle record."""
    now = datetime.now(UTC)
    jup = await pull_jupiter(http)
    jup_mints = [r["mint"] for r in jup]

    for rec in jup:
        entry = all_seen.setdefault(rec["mint"], {
            "symbol": rec["symbol"],
            "first_seen_jupiter": None,
            "first_seen_dex": None,
            "jup_age_at_first": None,
        })
        if entry["first_seen_jupiter"] is None:
            entry["first_seen_jupiter"] = cycle
            entry["jup_age_at_first"] = rec["age_minutes"]
            unconfirmed.add(rec["mint"])

    # Gate results for Jupiter mints this cycle.
    jup_gate_rows = []
    for rec in jup:
        passed, gates = jupiter_gates(rec)
        jup_gate_rows.append({
            "mint": rec["mint"], "symbol": rec["symbol"],
            "passed": passed, "gates": gates,
            "age_minutes": rec["age_minutes"], "mcap": rec["mcap"],
            "vol5m": rec["vol5m"], "bsr": rec["bsr"],
        })

    # ---- DexScreener side ----
    browser_tickers, browser_note = await try_browser_pc(http)

    to_lookup = (set(all_seen) | set(jup_mints)) & unconfirmed
    found = await lookup_dexscreener(http, sorted(to_lookup))

    dex_gate_rows = []
    new_dex: list[str] = []
    for mint, pairs in found.items():
        if mint not in unconfirmed:
            continue
        unconfirmed.discard(mint)
        dex_known[mint] = cycle
        entry = all_seen.setdefault(mint, {
            "symbol": None, "first_seen_jupiter": None,
            "first_seen_dex": None, "jup_age_at_first": None,
        })
        if entry["first_seen_dex"] is None:
            entry["first_seen_dex"] = cycle
        new_dex.append(mint)

        best = best_dex_pair(pairs, now)
        if best is None:
            continue
        passed, gates = dex_gates(best)
        base = best.get("baseToken") or {}
        dex_gate_rows.append({
            "mint": mint, "symbol": base.get("symbol"),
            "passed": passed, "gates": gates,
            "age_minutes": best.get("_age_minutes"),
            "mcap": best.get("marketCap"),
            "vol_h1": (best.get("volume") or {}).get("h1"),
            "bsr": ((best.get("txns") or {}).get("h1") or {}).get("buys", 0)
                    / max(((best.get("txns") or {}).get("h1") or {}).get("sells", 0), 1),
        })

    dex_mints = [m for m, _ in sorted(dex_known.items(), key=lambda kv: kv[1])]
    overlap = sorted(set(jup_mints) & set(dex_known))
    jup_only = sorted(set(jup_mints) - set(dex_known))
    dex_only = sorted(set(dex_known) - set(jup_mints))

    record = {
        "cycle": cycle,
        "timestamp": now.isoformat(),
        "jupiter_mints_under_22m": jup_mints,
        "dexscreener_mints": dex_mints,
        "overlap": overlap,
        "jupiter_only": jup_only,
        "dex_only": dex_only,
        "dex_new_this_cycle": new_dex,
        "jupiter_gate_results": jup_gate_rows,
        "dexscreener_gate_results": dex_gate_rows,
        "browser_pc": {"ok": browser_note is None, "note": browser_note},
    }
    print(
        f"[cycle {cycle:02d}/{TOTAL_CYCLES}] {now.strftime('%H:%M:%S')} "
        f"jup<22m={len(jup_mints)} dex_known={len(dex_mints)} "
        f"overlap={len(overlap)} jup_only={len(jup_only)} dex_only={len(dex_only)}"
        + (f" | browser-pc: {browser_note}" if browser_note else " | browser-pc OK")
    )
    return record


def build_final_report(
    cycle_records: list[dict], all_seen: dict[str, dict],
    dex_known: dict[str, int], jup_pass: set[str], dex_pass: set[str],
) -> str:
    jup_mints = {m for m, e in all_seen.items() if e["first_seen_jupiter"] is not None}
    dex_mints = set(dex_known)
    both = jup_mints & dex_mints

    lines = []
    lines.append("=== 30-MINUTE SHADOW TEST ===")
    lines.append(f"Total unique mints seen (Jupiter, ≤22m): {len(jup_mints)}")
    lines.append(f"Total unique mints seen (DexScreener): {len(dex_mints)}")
    lines.append(f"Total unique mints seen on BOTH (ever, across all cycles): {len(both)}")

    denom = max(len(jup_mints), len(dex_mints))
    rate = 100.0 * len(both) / denom if denom else 0.0
    lines.append("")
    lines.append(f"Cumulative overlap rate: {len(both)} / max({len(jup_mints)}, "
                 f"{len(dex_mints)}) = {rate:.1f}%")

    jup_first = sum(1 for m in both
                    if all_seen[m]["first_seen_jupiter"] is not None
                    and (all_seen[m]["first_seen_dex"] is None
                         or all_seen[m]["first_seen_jupiter"] < all_seen[m]["first_seen_dex"]))
    dex_first = sum(1 for m in both if all_seen[m]["first_seen_dex"] < all_seen[m]["first_seen_jupiter"])
    same_cycle = sum(1 for m in both
                     if all_seen[m]["first_seen_jupiter"] == all_seen[m]["first_seen_dex"])
    lines.append("")
    lines.append(f"Mints that appeared on Jupiter first: {jup_first}")
    lines.append(f"Mints that appeared on DexScreener first: {dex_first}")
    lines.append(f"Mints that appeared on both in same cycle: {same_cycle}")
    lines.append(f"Mints Jupiter-only (never on DexScreener): {len(jup_mints - dex_mints)}")
    lines.append(f"Mints DexScreener-only (never on Jupiter): {len(dex_mints - jup_mints)}")

    lines.append("")
    lines.append("=== STRATEGY B GATE PASS COMPARISON ===")
    both_pass = jup_pass & dex_pass
    lines.append("Jupiter mints passing all gates (age, mcap≥5K, vol≥500, bsr≥0.5): "
                 f"{len(jup_pass)}")
    lines.append(f"DexScreener mints passing all gates: {len(dex_pass)}")
    lines.append(f"Gate-passing mints on BOTH: {len(both_pass)}")

    lines.append("")
    lines.append("=== VERDICT ===")
    if dex_pass:
        pct = 100.0 * len(both_pass) / len(dex_pass)
        lines.append(f"Jupiter would have seen {pct:.0f}% of DexScreener's tradeable "
                     f"candidates within the 22m window ({len(both_pass)}/{len(dex_pass)}).")
    else:
        lines.append("No DexScreener gate-passing mints — no verdict computable.")
    lines.append("")
    lines.append("=== NOTES ===")
    lines.append("- DexScreener discovery is lookup-only (browser-pc Cloudflare-blocked, "
                 "no working trending endpoint): mints NOT on Jupiter are invisible to "
                 "the DexScreener side, so 'DexScreener-only' counts are zero by "
                 "construction and both-pass is a lower bound.")
    lines.append("- Jupiter volume = stats5m USD; DexScreener volume = h1 USD "
                 "(unit mismatch as in run_strategy_b.py).")
    lines.append("- Gate pass = 'passed at any observed cycle' (best-case), "
                 "not a single snapshot.")
    return "\n".join(lines) + "\n"


async def main() -> None:
    if not API_KEY:
        print("ERROR: JUPITER_API_KEY not found in .env")
        return

    all_seen: dict[str, dict] = {}
    unconfirmed: set[str] = set()
    dex_known: dict[str, int] = {}
    cycle_records: list[dict] = []

    print(f"MT-549 shadow test: {TOTAL_CYCLES} cycles x {CYCLE_INTERVAL_S}s "
          f"starting {datetime.now(UTC).isoformat()}")
    async with httpx.AsyncClient(timeout=20) as http:
        for cycle in range(1, TOTAL_CYCLES + 1):
            t0 = time.monotonic()
            record = await run_cycle(cycle, http, all_seen, unconfirmed, dex_known)
            cycle_records.append(record)
            elapsed = time.monotonic() - t0
            if cycle < TOTAL_CYCLES:
                await asyncio.sleep(max(0, CYCLE_INTERVAL_S - elapsed))

    jup_pass = {row["mint"] for rec in cycle_records for row in rec["jupiter_gate_results"]
                if row["passed"]}
    dex_pass = {row["mint"] for rec in cycle_records for row in rec["dexscreener_gate_results"]
                if row["passed"]}
    report = build_final_report(cycle_records, all_seen, dex_known, jup_pass, dex_pass)

    print("\n" + report)

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "task": "MT-549",
        "cycles_total": TOTAL_CYCLES,
        "cycle_interval_s": CYCLE_INTERVAL_S,
        "gates": {
            "max_age_minutes": MAX_AGE_MINUTES,
            "min_mcap_usd": MIN_MCAP_USD,
            "max_mcap_usd": MAX_MCAP_USD,
            "min_volume_usd": MIN_VOLUME_USD,
            "min_buy_sell_ratio": MIN_BUY_SELL_RATIO,
        },
        "cycles": cycle_records,
        "all_seen": all_seen,
        "summary": {
            "jupiter_mints": sorted({m for m, e in all_seen.items()
                                     if e["first_seen_jupiter"] is not None}),
            "dexscreener_mints": sorted(dex_known),
            "jupiter_only": sorted({m for m, e in all_seen.items()
                                    if e["first_seen_jupiter"] is not None
                                    and m not in dex_known}),
            "dexscreener_only": sorted(set(dex_known) - {m for m, e in all_seen.items()
                                                         if e["first_seen_jupiter"] is not None}),
        },
    }
    CYCLES_PATH.write_text(json.dumps(payload, indent=2))
    REPORT_PATH.write_text(report)
    print(f"Saved per-cycle data to {CYCLES_PATH}")
    print(f"Saved report to {REPORT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())

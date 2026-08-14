"""MT-543: Compare Jupiter Tokens V2 candidate pools vs DexScreener (browser-pc).

Pulls /recent, /toporganicscore/5m, and /toptrending/5m from Jupiter,
dedupes by mint, runs the Strategy B gate filter (MT-537 frozen values),
then pulls the browser-pc DexScreener board and compares overlap.

DexScreener board sources (first that works):
  1. browser-pc /capture (http://localhost:8099) — live capture
  2. snapshot file data/dex_board_snapshot.json — captured via a headed
     browser that passed Cloudflare (same URL, dexscreener.com/new-pairs/solana)
  3. none — DexScreener section is marked skipped

Run:
    python3 scripts/compare_jupiter_vs_dex.py

Results are appended to data/jupiter_vs_dex_comparison.json.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_PATH = REPO_ROOT / "data" / "jupiter_vs_dex_comparison.json"
SNAPSHOT_PATH = REPO_ROOT / "data" / "dex_board_snapshot.json"
SNAPSHOT_MAX_AGE_S = 20 * 60  # accept snapshots up to 20 minutes old

API_KEY = os.environ.get("JUPITER_API_KEY", "")
BASE = "https://api.jup.ag/tokens/v2"
HEADERS = {"x-api-key": API_KEY}

BROWSER_PC_URL = "http://localhost:8099"
STRATEGY_B_DEXSCREENER_URL = "https://dexscreener.com/new-pairs/solana"
BROWSER_PC_WAIT_SECONDS = 8
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
MAX_SOURCE_ROWS = 30

# Strategy B frozen gates (MT-537) — mirrored from run_strategy_b.py.
MAX_AGE_MINUTES = 22.0
MIN_MCAP_USD = 5_000
MAX_MCAP_USD = 50_000  # actual upper mcap gate in run_strategy_b.py
MIN_VOLUME_USD = 500
# MT-543 spec value. NOTE: run_strategy_b.py GATES currently uses 0.5;
# the task spec explicitly sets 1.0 — flagged in output below.
MIN_BUY_SELL_RATIO = 1.0

JUPITER_ENDPOINTS = [
    ("recent", "/recent", "recent"),
    ("organic5m", "/toporganicscore/5m?limit=100", "organic"),
    ("trending5m", "/toptrending/5m?limit=100", "trending"),
]

RATE_LIMIT_S = 0.25


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def age_minutes_of(token: dict, now: datetime) -> float | None:
    """Age in minutes from firstPool.createdAt, None if missing."""
    fp = token.get("firstPool") or {}
    created = fp.get("createdAt")
    if not created:
        return None
    dt = _parse_iso(created)
    if dt is None:
        return None
    return max(0.0, (now - dt).total_seconds() / 60)


def normalize_jupiter(token: dict, now: datetime, source: str) -> dict:
    """Map a Jupiter token dict to a normalized comparison record."""
    s5 = token.get("stats5m") or {}
    buys = int(s5.get("numBuys") or 0)
    sells = int(s5.get("numSells") or 0)
    vol = float(s5.get("buyVolume") or 0) + float(s5.get("sellVolume") or 0)
    audit = token.get("audit") or {}
    return {
        "source": source,
        "mint": token.get("id"),
        "symbol": token.get("symbol"),
        "name": token.get("name"),
        "age_minutes": age_minutes_of(token, now),
        "mcap": float(token.get("mcap") or token.get("fdv") or 0),
        "fdv": float(token.get("fdv") or 0),
        "volume_5m_usd": vol,
        "num_buys": buys,
        "num_sells": sells,
        "buy_sell_ratio": buys / max(sells, 1),
        "organic_score": token.get("organicScore"),
        "mint_authority_disabled": audit.get("mintAuthorityDisabled"),
        "freeze_authority_disabled": audit.get("freezeAuthorityDisabled"),
        "is_sus": audit.get("isSus"),
        "holder_count": token.get("holderCount"),
        "liquidity": float(token.get("liquidity") or 0),
    }


def gate_check(rec: dict) -> tuple[bool, str, dict[str, bool]]:
    """Apply the Strategy B gate set (MT-537) to a normalized record.

    Returns (passed, reason, per-gate dict). Unit of volume passed in is
    source-specific (Jupiter stats5m USD vs DexScreener h1 USD) — both are
    matched against MIN_VOLUME_USD per the Strategy B constants.
    """
    gates = {"age": False, "mcap": False, "volume": False, "bsr": False}
    reasons = []

    age = rec.get("age_minutes")
    if age is None:
        return False, "no firstPool.createdAt", gates
    if age > MAX_AGE_MINUTES:
        reasons.append(f"age={age:.1f}m>{MAX_AGE_MINUTES:.0f}m")
    else:
        gates["age"] = True

    mcap = rec.get("mcap") or 0
    if mcap <= 0:
        reasons.append("no mcap")
    elif mcap < MIN_MCAP_USD:
        reasons.append(f"mcap=${mcap:,.0f}<${MIN_MCAP_USD:,}")
    elif mcap > MAX_MCAP_USD:
        reasons.append(f"mcap=${mcap:,.0f}>${MAX_MCAP_USD:,}")
    else:
        gates["mcap"] = True

    vol = rec.get("volume_5m_usd") or 0
    if vol < MIN_VOLUME_USD:
        reasons.append(f"vol=${vol:,.0f}<${MIN_VOLUME_USD:,}")
    else:
        gates["volume"] = True

    bsr = rec.get("buy_sell_ratio") or 0
    if bsr < MIN_BUY_SELL_RATIO:
        reasons.append(f"bsr={bsr:.2f}<{MIN_BUY_SELL_RATIO}")
    else:
        gates["bsr"] = True

    passed = all(gates.values())
    reason = "PASS" if passed else "FAIL " + " ".join(reasons)
    return passed, reason, gates


async def pull_jupiter(http: httpx.AsyncClient) -> list[dict]:
    """Pull all three Jupiter endpoints with rate limiting, dedupe by mint."""
    now = datetime.now(UTC)
    combined: dict[str, dict] = {}
    endpoint_stats: dict[str, int] = {}
    for name, path, _ in JUPITER_ENDPOINTS:
        try:
            resp = await http.get(f"{BASE}{path}", headers=HEADERS, timeout=15)
            tokens = resp.json() if resp.status_code == 200 else []
            endpoint_stats[name] = len(tokens)
            print(f"  {path}: HTTP {resp.status_code}, {len(tokens)} tokens")
        except Exception as exc:
            endpoint_stats[name] = 0
            print(f"  {path}: ERROR {exc}")
        for token in tokens:
            if not isinstance(token, dict) or not token.get("id"):
                continue
            rec = normalize_jupiter(token, now, name)
            if rec["mint"] in combined:
                existing = combined[rec["mint"]]
                if rec["age_minutes"] is not None and (
                    existing["age_minutes"] is None
                    or rec["age_minutes"] < existing["age_minutes"]
                ):
                    rec["source"] = existing["source"] + "+" + name
                    combined[rec["mint"]] = rec
            else:
                combined[rec["mint"]] = rec
        await asyncio.sleep(RATE_LIMIT_S)
    return list(combined.values())


def _normalize_dex_pair(pair: dict, ticker: str, age_min: float) -> dict | None:
    """Map a DexScreener API pair to a normalized record (h1 volume/txns)."""
    base = pair.get("baseToken") or {}
    mint = base.get("address")
    if not isinstance(mint, str) or not mint:
        return None
    txns = (pair.get("txns") or {}).get("h1") or {}
    buys = int(txns.get("buys") or 0)
    sells = int(txns.get("sells") or 0)
    return {
        "source": "dexscreener",
        "mint": mint,
        "symbol": base.get("symbol") or ticker,
        "name": ticker,
        "age_minutes": age_min,
        "mcap": float(pair.get("marketCap") or pair.get("fdv") or 0),
        "fdv": float(pair.get("fdv") or 0),
        "volume_5m_usd": float((pair.get("volume") or {}).get("h1") or 0),
        "num_buys": buys,
        "num_sells": sells,
        "buy_sell_ratio": buys / max(sells, 1),
        "organic_score": None,
        "mint_authority_disabled": None,
        "freeze_authority_disabled": None,
        "is_sus": None,
        "holder_count": None,
        "liquidity": float((pair.get("liquidity") or {}).get("usd") or 0),
    }


async def resolve_tickers(
    tickers: list[str], http: httpx.AsyncClient,
) -> list[dict]:
    """Resolve board tickers to fresh SOL pairs via the DexScreener search API.

    Mirrors run_strategy_b.py _search_fresh_pair: SOL-quoted, age <= 22m,
    lowest-age pair wins.
    """
    now_ms = time.time() * 1000
    candidates: list[dict] = []

    async def resolve(ticker: str) -> None:
        try:
            search = await http.get(
                "https://api.dexscreener.com/latest/dex/search",
                params={"q": ticker},
                timeout=10,
            )
            pairs = search.json().get("pairs") or []
        except Exception:
            return
        choices: list[tuple[float, dict]] = []
        for pair in pairs:
            if not isinstance(pair, dict) or pair.get("chainId") != "solana":
                continue
            if (pair.get("quoteToken") or {}).get("address") != WRAPPED_SOL_MINT:
                continue
            created_ms = pair.get("pairCreatedAt")
            if not isinstance(created_ms, (int, float)) or created_ms <= 0:
                continue
            age_min = max(0.0, (now_ms - created_ms) / 60_000)
            if age_min <= MAX_AGE_MINUTES:
                choices.append((age_min, pair))
        if not choices:
            return
        age_min, pair = min(choices, key=lambda item: item[0])
        rec = _normalize_dex_pair(pair, ticker, age_min)
        if rec is not None:
            candidates.append(rec)

    await asyncio.gather(*(resolve(t) for t in tickers[:MAX_SOURCE_ROWS]))
    return candidates


def load_snapshot_tickers() -> list[str] | None:
    """Read board tickers from the headed-browser snapshot file if fresh."""
    if not SNAPSHOT_PATH.exists():
        return None
    try:
        snap = json.loads(SNAPSHOT_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    captured = snap.get("captured_at") or snap.get("captured")
    if captured:
        try:
            parsed = datetime.fromisoformat(str(captured).replace("Z", "+00:00"))
            if (datetime.now(UTC) - parsed).total_seconds() > SNAPSHOT_MAX_AGE_S:
                return None
        except ValueError:
            return None
    tickers = []
    for row in snap.get("rows", []):
        sym = (row.get("symbol") or row.get("name") or "").strip()
        if sym:
            tickers.append(sym)
    return tickers or None


async def capture_dexscreener(http: httpx.AsyncClient) -> tuple[list[dict], str | None]:
    """Pull the DexScreener board and return (candidates, error).

    Source order: browser-pc /capture, then headed-browser snapshot file.
    Candidates are normalized records resolved through the DexScreener API,
    mirroring run_strategy_b.py's fetch_candidates pipeline.
    """
    tickers: list[str] = []
    source_note = ""
    try:
        resp = await http.post(
            f"{BROWSER_PC_URL}/capture",
            json={"url": STRATEGY_B_DEXSCREENER_URL, "wait": BROWSER_PC_WAIT_SECONDS},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            rows = data.get("candidates", data.get("rows", []))
            for row in rows:
                ticker = row.get("name") or row.get("symbol") or row.get("token")
                if isinstance(ticker, str) and ticker.strip():
                    tickers.append(ticker.strip())
            source_note = "browser-pc"
        else:
            raw = (data.get("raw_text") or "").strip()[:120]
            print(f"  browser-pc capture failed (blocked?): {raw}")
    except Exception as exc:
        print(f"  browser-pc unreachable: {exc}")

    if not tickers:
        snap_tickers = load_snapshot_tickers()
        if snap_tickers:
            tickers = snap_tickers
            source_note = "snapshot"
            print(f"  Falling back to headed-browser snapshot ({len(tickers)} tickers)")
        else:
            return [], "browser-pc capture blocked and no fresh snapshot available"

    candidates = await resolve_tickers(tickers, http)
    print(
        f"  board source={source_note} tickers={len(tickers)} "
        f"resolved fresh SOL pairs={len(candidates)}"
    )
    return candidates, None


def format_report(
    run_idx: int,
    jupiter_all: list[dict],
    jupiter_passed: list[dict],
    dex_candidates: list[dict],
    dex_passed: list[dict],
    dex_error: str | None,
    jupiter_by_source: dict[str, int],
) -> dict:
    """Print the per-run comparison report and return the record dict."""
    jup_by_mint = {c["mint"]: c for c in jupiter_passed}
    dex_by_mint = {c["mint"]: c for c in dex_passed}
    overlap = sorted(set(jup_by_mint) & set(dex_by_mint))
    jup_only = sorted(set(jup_by_mint) - set(dex_by_mint))
    dex_only = sorted(set(dex_by_mint) - set(jup_by_mint))

    print(f"\n=== RUN {run_idx} @ {datetime.now(UTC).isoformat()} ===")
    print("\n=== JUPITER POOL ===")
    for src, count in jupiter_by_source.items():
        print(f"  {src}: {count}")
    print(f"Total unique tokens across 3 endpoints: {len(jupiter_all)}")
    print(f"After Strategy B gates: {len(jupiter_passed)} passed")
    for c in sorted(jupiter_passed, key=lambda x: x["age_minutes"] or 999):
        print(
            f"  PASS {c['symbol']:12s} {c['mint'][:12]}... "
            f"age={c['age_minutes']:.0f}m mcap=${c['mcap']:,.0f} "
            f"vol5m=${c['volume_5m_usd']:,.0f} bsr={c['buy_sell_ratio']:.2f} "
            f"organic={c['organic_score'] if c['organic_score'] is not None else 'n/a'}"
        )

    print("\n=== DEXSCREENER POOL ===")
    if dex_error:
        print(f"  SKIPPED: {dex_error}")
    else:
        print(f"Total tokens from board: {len(dex_candidates)}")
        print(f"After Strategy B gates: {len(dex_passed)} passed")
        for c in sorted(dex_passed, key=lambda x: x["age_minutes"] or 999):
            print(
                f"  PASS {c['symbol']:12s} {c['mint'][:12]}... "
                f"age={c['age_minutes']:.0f}m mcap=${c['mcap']:,.0f} "
                f"vol(h1)=${c['volume_5m_usd']:,.0f} bsr={c['buy_sell_ratio']:.2f}"
            )

    print("\n=== OVERLAP ===")
    print(f"Tokens in BOTH: {len(overlap)}")
    for mint in overlap:
        j, d = jup_by_mint[mint], dex_by_mint[mint]
        print(f"  {j['symbol']:12s} {mint}")
    print(f"Jupiter-only: {len(jup_only)}")
    for mint in jup_only:
        c = jup_by_mint[mint]
        print(f"  {c['symbol']:12s} {mint}")
    print(f"DexScreener-only: {len(dex_only)}")
    for mint in dex_only:
        c = dex_by_mint[mint]
        print(f"  {c['symbol']:12s} {mint}")

    if dex_passed:
        pct = 100.0 * len(overlap) / len(dex_passed)
    elif not overlap:
        pct = 0.0
    else:
        pct = 100.0
    print("\n=== VERDICT ===")
    if not dex_passed and dex_error is None:
        print("  DexScreener pool empty — no overlap computable")
    elif not dex_passed:
        print("  DexScreener pull unavailable — Jupiter-only report")
    else:
        print(f"Overlap: {len(overlap)}/{len(dex_passed)} ({pct:.0f}%)")
        diff = len(jupiter_passed) - len(dex_passed)
        if diff > 0:
            print(f"Jupiter found {diff} more candidates than DexScreener")
        elif diff < 0:
            print(f"Jupiter found {-diff} fewer candidates than DexScreener")
        else:
            print("Jupiter and DexScreener found the same number of candidates")
    print(
        "NOTE: Jupiter volume unit = stats5m USD; DexScreener = h1 USD "
        "(per run_strategy_b.py). buy_sell_ratio gate = 1.0 per MT-543 spec "
        "(run_strategy_b.py GATES currently 0.5)."
    )

    return {
        "run_idx": run_idx,
        "captured_at": datetime.now(UTC).isoformat(),
        "jupiter": {
            "by_source": jupiter_by_source,
            "total_unique": len(jupiter_all),
            "gate_passed": jupiter_passed,
            "gate_pass_count": len(jupiter_passed),
        },
        "dexscreener": {
            "error": dex_error,
            "total": len(dex_candidates),
            "gate_pass_count": len(dex_passed),
            "gate_passed": dex_passed,
        },
        "overlap": {
            "both": overlap,
            "jupiter_only": jup_only,
            "dexscreener_only": dex_only,
            "overlap_pct": pct if dex_passed else None,
        },
    }


async def run_comparison(run_idx: int) -> dict:
    if not API_KEY:
        print("ERROR: JUPITER_API_KEY not set")
        sys.exit(1)

    async with httpx.AsyncClient(timeout=20) as http:
        print(f"\n[{datetime.now(UTC).isoformat()}] Pulling Jupiter...")
        jupiter_all = await pull_jupiter(http)
        jupiter_passed = []
        for rec in jupiter_all:
            ok, reason, _ = gate_check(rec)
            if ok:
                jupiter_passed.append(rec)
        jupiter_by_source: dict[str, int] = {}
        for rec in jupiter_all:
            jupiter_by_source[rec["source"]] = jupiter_by_source.get(rec["source"], 0) + 1

        print(f"Pulling DexScreener via browser-pc (wait={BROWSER_PC_WAIT_SECONDS}s)...")
        dex_candidates, dex_error = await capture_dexscreener(http)
        dex_passed = [c for c in dex_candidates if gate_check(c)[0]]
        print(
            f"  browser-pc candidates={len(dex_candidates)} "
            f"gate-passed={len(dex_passed)}"
            + (f" — {dex_error}" if dex_error else "")
        )

        record = format_report(
            run_idx, jupiter_all, jupiter_passed,
            dex_candidates, dex_passed, dex_error, jupiter_by_source,
        )
        return record


def print_cumulative_summary() -> None:
    """Print a summary across all runs recorded in the results file."""
    if not RESULTS_PATH.exists():
        return
    try:
        runs = json.loads(RESULTS_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        print("  (cannot read prior results)")
        return
    if not isinstance(runs, list):
        runs = [runs]

    overlap_pcts = []
    jup_only_all: set[str] = set()
    dex_only_all: set[str] = set()
    both_all: set[str] = set()
    print(f"\n=== FINAL SUMMARY ACROSS {len(runs)} RUN(S) ===")
    for r in runs:
        ov = r.get("overlap") or {}
        j = r.get("jupiter") or {}
        d = r.get("dexscreener") or {}
        if ov.get("overlap_pct") is not None:
            overlap_pcts.append(ov["overlap_pct"])
        jup_only_all.update(ov.get("jupiter_only", []))
        dex_only_all.update(ov.get("dexscreener_only", []))
        both_all.update(ov.get("both", []))
        print(
            f"  Run {r.get('run_idx')}: jupiter_pass={j.get('gate_pass_count')} "
            f"dex_pass={d.get('gate_pass_count')} "
            f"overlap_pct={ov.get('overlap_pct')}"
            + (f" dex_error={d.get('error')}" if d.get("error") else "")
        )
    if overlap_pcts:
        avg = sum(overlap_pcts) / len(overlap_pcts)
        print(f"Average overlap: {avg:.1f}%")
    print(f"Unique Jupiter-only coins (cumulative): {len(jup_only_all)}")
    print(f"Unique DexScreener-only coins (cumulative): {len(dex_only_all)}")
    print(f"Coins seen in both at least once (cumulative): {len(both_all)}")
    if len(jup_only_all) == 0 and len(dex_only_all) == 0:
        print("Jupiter consistently finds the SAME pool as DexScreener")
    elif len(jup_only_all) >= len(dex_only_all) and len(jup_only_all) > 0:
        print("Jupiter surfaces a SUPERSET of DexScreener's pool")
    else:
        print("Jupiter surfaces a SUBSET of DexScreener's pool")


def append_record(record: dict) -> None:
    if RESULTS_PATH.exists():
        try:
            runs = json.loads(RESULTS_PATH.read_text())
            if not isinstance(runs, list):
                runs = [runs]
        except (json.JSONDecodeError, OSError):
            runs = []
    else:
        runs = []
    runs.append(record)
    RESULTS_PATH.write_text(json.dumps(runs, indent=2))


async def main() -> None:
    run_idx = 1
    if RESULTS_PATH.exists():
        try:
            prior = json.loads(RESULTS_PATH.read_text())
            run_idx = max((r.get("run_idx", 0) for r in prior), default=0) + 1
        except (json.JSONDecodeError, OSError):
            pass
    record = await run_comparison(run_idx)
    append_record(record)
    print(f"\nSaved run {run_idx} to {RESULTS_PATH}")
    print_cumulative_summary()


if __name__ == "__main__":
    asyncio.run(main())

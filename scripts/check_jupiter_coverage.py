"""MT-545: Check Jupiter Tokens V2 coverage of Strategy B historical winners.

Answers: would switching the discovery layer to Jupiter miss the tokens that
actually made money? Checks whether Jupiter's search API returns each mint
that Strategy B traded, and records organicScore / liquidity / audit fields
for found tokens.

Read-only: queries data/trades.db (positions + candidate_log), never writes
to the DB, does not touch runtime code.

Run:
    python3 scripts/check_jupiter_coverage.py

Results saved to data/jupiter_coverage_check.json.
"""

from __future__ import annotations

import json
import os
import random
import time
from collections import Counter
from pathlib import Path

import httpx
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_PATH = REPO_ROOT / "data" / "jupiter_coverage_check.json"
DB_PATH = REPO_ROOT / "data" / "trades.db"

load_dotenv(REPO_ROOT / ".env")
API_KEY = os.environ.get("JUPITER_API_KEY", "")
BASE = "https://api.jup.ag/tokens/v2"
HEADERS = {"x-api-key": API_KEY}

RATE_LIMIT_S = 0.25
MAX_LOOKUPS = 150
WINNER_THRESHOLD_SOL = 0.01  # meaningful winners
LOSER_SAMPLE_SIZE = 50
WINNER_BUDGET = MAX_LOOKUPS - LOSER_SAMPLE_SIZE

SQL = """
SELECT
    p.id,
    p.mint_address,
    cl.ticker,
    p.realized_pnl_sol,
    p.opened_at,
    p.closed_at,
    cl.mcap_usd,
    cl.volume_usd
FROM positions p
LEFT JOIN candidate_log cl ON cl.position_id = p.id
WHERE p.strategy = 'B' AND p.status = 'CLOSED'
ORDER BY p.realized_pnl_sol DESC;
"""


def classify_pool(token: dict) -> str:
    """Best-effort DEX label for the launch pool.

    Jupiter's token record exposes firstPool.id and the launchpad label but
    not the pool's DEX program, so this maps from known launchpad labels and
    the pump.fun pool-id suffix. Unidentified pools are 'unknown'.
    """
    fp = token.get("firstPool") or {}
    pool_id = (fp.get("id") or "").lower()
    launchpad = (token.get("launchpad") or "").lower()
    if launchpad == "pump.fun" or pool_id.endswith("pump"):
        return "PumpSwap"
    if launchpad == "met-dbc":
        return "Meteora"
    if launchpad == "moonshot" or "moonshot" in pool_id:
        return "Moonshot"
    if launchpad:
        return f"launchpad:{launchpad}"
    return "unknown"


def fetch_mint(client: httpx.Client, mint: str) -> tuple[dict | None, str | None]:
    """Return (jupiter_token_dict, error) for a mint search."""
    url = f"{BASE}/search"
    for attempt in range(5):
        try:
            resp = client.get(url, params={"query": mint}, headers=HEADERS, timeout=15)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(2.0 ** attempt)  # 1s, 2s, 4s, 8s backoff
                continue
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list):
                return None, f"unexpected payload type {type(payload).__name__}"
            for token in payload:
                if str(token.get("id", "")).lower() == mint.lower():
                    return token, None
            return None, None  # searched, but no exact-id match
        except httpx.HTTPStatusError as e:
            return None, f"http {e.response.status_code}"
        except Exception as e:  # noqa: BLE001
            return None, f"{type(e).__name__}: {e}"
    return None, "failed after retries"


def summarize(token: dict) -> dict:
    audit = token.get("audit") or {}
    fp = token.get("firstPool") or {}
    return {
        "organicScore": token.get("organicScore"),
        "organicScoreLabel": token.get("organicScoreLabel"),
        "liquidity": token.get("liquidity"),
        "mcap": token.get("mcap"),
        "fdv": token.get("fdv"),
        "isSus": audit.get("isSus"),
        "mintAuthorityDisabled": audit.get("mintAuthorityDisabled"),
        "freezeAuthorityDisabled": audit.get("freezeAuthorityDisabled"),
        "firstPoolId": fp.get("id"),
        "firstPoolCreatedAt": fp.get("createdAt"),
        "firstPoolDex": classify_pool(token),
        "launchpad": token.get("launchpad"),
        "holderCount": token.get("holderCount"),
        "tokenProgram": token.get("tokenProgram"),
        "symbol": token.get("symbol"),
        "name": token.get("name"),
    }


def main() -> None:
    import sqlite3

    if not API_KEY:
        print("ERROR: JUPITER_API_KEY not found in .env")
        return

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(SQL)]
    con.close()
    print(f"Strategy B CLOSED positions: {len(rows)}")

    winners = [r for r in rows if r["realized_pnl_sol"] > 0]
    losers = [r for r in rows if r["realized_pnl_sol"] <= 0]
    meaningful = [r for r in winners if r["realized_pnl_sol"] > WINNER_THRESHOLD_SOL]

    total_winner_pnl = sum(r["realized_pnl_sol"] for r in winners)
    print(f"Winners (pnl>0): {len(winners)}  total pnl: {total_winner_pnl:.4f} SOL")
    print(f"Meaningful winners (pnl>{WINNER_THRESHOLD_SOL}): {len(meaningful)}")
    print(f"Losers (pnl<=0): {len(losers)}")

    # Dedupe by mint; keep the highest-pnl row for each mint.
    def by_mint(rs: list[dict]) -> list[dict]:
        best: dict[str, dict] = {}
        for r in rs:
            m = r["mint_address"]
            if m not in best or r["realized_pnl_sol"] > best[m]["realized_pnl_sol"]:
                best[m] = r
        return sorted(best.values(), key=lambda r: r["realized_pnl_sol"], reverse=True)

    winner_rows = by_mint(meaningful)[:WINNER_BUDGET]
    rng = random.Random(545)  # fixed seed for reproducibility
    loser_pool = by_mint(losers)
    loser_rows = rng.sample(loser_pool, min(LOSER_SAMPLE_SIZE, len(loser_pool)))
    if len(winner_rows) + len(loser_rows) > MAX_LOOKUPS:
        loser_rows = loser_rows[: MAX_LOOKUPS - len(winner_rows)]

    checked = winner_rows + loser_rows
    print(f"Checked: {len(checked)} mints "
          f"({len(winner_rows)} winners, {len(loser_rows)} losers, "
          f"budget {MAX_LOOKUPS})")

    results: dict[str, dict] = {}
    client = httpx.Client(timeout=20)
    try:
        for i, r in enumerate(checked):
            mint = r["mint_address"]
            token, err = fetch_mint(client, mint)
            if err:
                print(f"  [{i+1}/{len(checked)}] {mint} ERROR {err}")
            elif token is None:
                print(f"  [{i+1}/{len(checked)}] {mint} NOT FOUND")
            else:
                print(f"  [{i+1}/{len(checked)}] {mint} FOUND "
                      f"(score={token.get('organicScore')}, "
                      f"liq={round(token.get('liquidity') or 0)})")
            results[mint] = {
                "mint_address": mint,
                "position_id": r["id"],
                "ticker": r["ticker"],
                "realized_pnl_sol": r["realized_pnl_sol"],
                "opened_at": r["opened_at"],
                "closed_at": r["closed_at"],
                "entry_mcap_usd": r["mcap_usd"],
                "entry_volume_usd": r["volume_usd"],
                "checked_as": "winner" if r in winner_rows else "loser",
                "found": token is not None,
                "error": err,
                "jupiter": summarize(token) if token else None,
            }
            time.sleep(RATE_LIMIT_S)
    finally:
        client.close()

    # ---- Report ----
    wres = [v for v in results.values() if v["checked_as"] == "winner"]
    lres = [v for v in results.values() if v["checked_as"] == "loser"]
    w_found = [v for v in wres if v["found"]]
    w_missing = [v for v in wres if not v["found"] and not v["error"]]
    w_errored = [v for v in wres if v["error"]]
    l_found = [v for v in lres if v["found"]]
    l_errored = [v for v in lres if v["error"]]
    l_missing = [v for v in lres if not v["found"] and not v["error"]]

    w_pnl_covered = sum(v["realized_pnl_sol"] for v in w_found)
    w_pnl_missing = sum(v["realized_pnl_sol"] for v in w_missing)
    w_pnl_errored = sum(v["realized_pnl_sol"] for v in w_errored)

    print("\n=== JUPITER COVERAGE ===")
    print(f"Winners checked: {len(wres)}")
    print(f"Winners found on Jupiter: {len(w_found)} "
          f"({100*len(w_found)/max(1,len(wres)):.1f}%)")
    print(f"Winners errored (unresolved): {len(w_errored)}")
    print("Winners NOT on Jupiter:")
    for v in sorted(w_missing, key=lambda x: x["realized_pnl_sol"], reverse=True):
        print(f"  {v['mint_address']}  {v['ticker'] or '?'}  +{v['realized_pnl_sol']:.4f} SOL")
    print(f"Losers checked: {len(lres)}")
    print(f"Losers found on Jupiter: {len(l_found)} "
          f"({100*len(l_found)/max(1,len(lres)):.1f}%)")
    print(f"Losers errored (unresolved): {len(l_errored)}")
    print(f"Losers NOT on Jupiter: {len(l_missing)}")

    print("\n=== PNL IMPACT ===")
    print(f"Total winner PnL (all {len(winners)} positions): {total_winner_pnl:.4f} SOL")
    print(f"Winner PnL (Jupiter-covered, checked {len(w_found)}): "
          f"{w_pnl_covered:.4f} SOL")
    print(f"Winner PnL (Jupiter-missing, checked {len(w_missing)}): "
          f"{w_pnl_missing:.4f} SOL")
    if w_pnl_errored:
        print(f"Winner PnL (errored lookups, unchecked): {w_pnl_errored:.4f} SOL")
    checked_share = 100 * sum(v["realized_pnl_sol"] for v in wres) / max(1e-9, total_winner_pnl)
    print(f"% of profit at risk if switching: "
          f"{100*w_pnl_missing/max(1e-9,total_winner_pnl):.2f}% "
          f"(checked winners carry {checked_share:.1f}% of total winner pnl)")

    print("\n=== DEX DISTRIBUTION (winners, from Jupiter firstPool/launchpad) ===")
    dex_counter = Counter(v["jupiter"]["firstPoolDex"] for v in w_found)
    for dex, n in sorted(dex_counter.items(), key=lambda x: -x[1]):
        print(f"  {dex}: {n}")

    print("\n=== ORGANIC SCORE DISTRIBUTION ===")
    def avg_score(vs: list[dict]) -> float:
        scores = [v["jupiter"]["organicScore"] for v in vs
                  if v["jupiter"] and v["jupiter"]["organicScore"] is not None]
        return sum(scores) / len(scores) if scores else 0.0

    w_avg, l_avg = avg_score(w_found), avg_score(l_found)
    print(f"Winners avg organic score: {w_avg:.2f}")
    print(f"Losers avg organic score: {l_avg:.2f}")

    print("\n=== AUDIT FLAGS (winners found) ===")
    sus = sum(1 for v in w_found if v["jupiter"].get("isSus"))
    no_mint_auth = sum(1 for v in w_found
                       if v["jupiter"].get("mintAuthorityDisabled") is True)
    no_freeze_auth = sum(1 for v in w_found
                         if v["jupiter"].get("freezeAuthorityDisabled") is True)
    print(f"isSus=true: {sus}/{len(w_found)}")
    print(f"mintAuthorityDisabled: {no_mint_auth}/{len(w_found)}")
    print(f"freezeAuthorityDisabled: {no_freeze_auth}/{len(w_found)}")

    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "task": "MT-545",
        "method": "GET /tokens/v2/search?query={mint}, 250ms spacing, "
                  f"cap {MAX_LOOKUPS}",
        "totals": {
            "closed_positions": len(rows),
            "winners_pnl_gt_0": len(winners),
            "total_winner_pnl_sol": total_winner_pnl,
            "losers_pnl_le_0": len(losers),
            "meaningful_winners_gt_0.01": len(meaningful),
        },
        "summary": {
            "winners_checked": len(wres),
            "winners_found": len(w_found),
            "winners_not_found": len(w_missing),
            "winners_errored": len(w_errored),
            "losers_checked": len(lres),
            "losers_found": len(l_found),
            "losers_not_found": len(l_missing),
            "losers_errored": len(l_errored),
            "winner_pnl_covered_sol": w_pnl_covered,
            "winner_pnl_missing_sol": w_pnl_missing,
            "winner_pnl_errored_sol": w_pnl_errored,
            "pct_profit_at_risk": 100 * w_pnl_missing / max(1e-9, total_winner_pnl),
            "winners_avg_organic_score": w_avg,
            "losers_avg_organic_score": l_avg,
            "winners_dex_distribution": dict(dex_counter),
        },
        "results": results,
    }
    RESULTS_PATH.write_text(json.dumps(summary, indent=2))
    print(f"\nSaved {len(results)} per-mint results to {RESULTS_PATH}")


if __name__ == "__main__":
    main()

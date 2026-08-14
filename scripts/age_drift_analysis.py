"""MT-548: Quantify Jupiter vs DexScreener age drift for Strategy B mints.

Measures how much Jupiter's firstPool.createdAt diverges from DexScreener's
pairCreatedAt across real Strategy B trades, and checks whether Jupiter's
graduatedAt field is a better match. Produces the correct timestamp mapping
for the migration.

Read-only: queries data/trades.db (positions), never writes to the DB, does
not touch runtime code.

Run:
    python3 scripts/age_drift_analysis.py

Results saved to data/age_drift_analysis.json and data/age_drift_report.md.
"""

from __future__ import annotations

import json
import os
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_PATH = REPO_ROOT / "data" / "age_drift_analysis.json"
REPORT_PATH = REPO_ROOT / "data" / "age_drift_report.md"
DB_PATH = REPO_ROOT / "data" / "trades.db"

load_dotenv(REPO_ROOT / ".env")
API_KEY = os.environ.get("JUPITER_API_KEY", "")
JUP_BASE = "https://api.jup.ag/tokens/v2"
JUP_HEADERS = {"x-api-key": API_KEY}
DEX_URL = "https://api.dexscreener.com/latest/dex/search"

RATE_LIMIT_S = 0.25
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"

SQL = """
SELECT DISTINCT mint_address FROM positions
WHERE strategy = 'B' AND status = 'CLOSED'
ORDER BY opened_at DESC LIMIT 50;
"""


def to_epoch_ms(value) -> int | None:
    """Normalize a timestamp (ISO-8601 str, ms epoch, or s epoch) to ms epoch."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    if isinstance(value, (int, float)) and value > 0:
        if value > 1e12:  # already ms
            return int(value)
        if value > 1e9:  # seconds
            return int(value * 1000)
    return None


def fetch_jupiter(client: httpx.Client, mint: str) -> tuple[dict | None, str | None]:
    """Return (token_dict, error) for a Jupiter tokens v2 search by mint."""
    for attempt in range(5):
        try:
            resp = client.get(
                f"{JUP_BASE}/search", params={"query": mint}, headers=JUP_HEADERS, timeout=15
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(2.0 ** attempt)
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


def fetch_dexscreener(client: httpx.Client, mint: str) -> tuple[int | None, str | None]:
    """Return (pairCreatedAt_ms, error) for the earliest SOL-quoted Solana pair.

    Same pair-selection logic as _search_fresh_pair(): chain solana, quote
    token is wSOL; here we pick the earliest creation rather than the
    freshest (historical mints, no age window).
    """
    try:
        resp = client.get(DEX_URL, params={"q": mint}, timeout=15)
        resp.raise_for_status()
        pairs = resp.json().get("pairs") or []
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {e}"

    created_times: list[int] = []
    for pair in pairs:
        if not isinstance(pair, dict) or pair.get("chainId") != "solana":
            continue
        quote = pair.get("quoteToken") or {}
        if quote.get("address") != WRAPPED_SOL_MINT:
            continue
        created_ms = pair.get("pairCreatedAt")
        if isinstance(created_ms, (int, float)) and created_ms > 0:
            created_times.append(int(created_ms))
    if not created_times:
        return None, "no solana wSOL-quoted pair"
    return min(created_times), None


def fmt_utc(epoch_ms: int | None) -> str:
    if epoch_ms is None:
        return "-"
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def drift_minutes(jupiter_ms: int | None, dex_ms: int | None) -> float | None:
    if jupiter_ms is None or dex_ms is None:
        return None
    return (jupiter_ms - dex_ms) / 60_000.0


def main() -> None:
    import sqlite3

    if not API_KEY:
        print("ERROR: JUPITER_API_KEY not found in .env")
        return

    con = sqlite3.connect(DB_PATH)
    mints = [r[0] for r in con.execute(SQL)]
    con.close()
    print(f"Strategy B CLOSED distinct mints (recent 50): {len(mints)}")

    results: dict[str, dict] = {}
    client = httpx.Client(timeout=20)
    try:
        for i, mint in enumerate(mints):
            # Alternate Jupiter / DexScreener to spread load.
            if i % 2 == 0:
                jup_token, jup_err = fetch_jupiter(client, mint)
                time.sleep(RATE_LIMIT_S)
                dex_created, dex_err = fetch_dexscreener(client, mint)
            else:
                dex_created, dex_err = fetch_dexscreener(client, mint)
                time.sleep(RATE_LIMIT_S)
                jup_token, jup_err = fetch_jupiter(client, mint)
            time.sleep(RATE_LIMIT_S)

            fp = (jup_token or {}).get("firstPool") or {}
            grad_pool = (jup_token or {}).get("graduatedPool") or {}
            jup_first_ms = to_epoch_ms(fp.get("createdAt"))
            jup_grad_ms = to_epoch_ms((jup_token or {}).get("graduatedAt"))

            drift_first = drift_minutes(jup_first_ms, dex_created)
            drift_grad = drift_minutes(jup_grad_ms, dex_created)

            print(
                f"  [{i+1}/{len(mints)}] {mint[:12]}… "
                f"jup_first={fmt_utc(jup_first_ms)} "
                f"jup_grad={fmt_utc(jup_grad_ms)} "
                f"dex={fmt_utc(dex_created)} "
                f"drift_first={drift_first if drift_first is None else round(drift_first, 1)}m"
                + (f" drift_grad={round(drift_grad, 1)}m" if drift_grad is not None else "")
                + (f" ERR_jup={jup_err}" if jup_err else "")
                + (f" ERR_dex={dex_err}" if dex_err else "")
            )

            results[mint] = {
                "mint_address": mint,
                "jupiter": {
                    "found": jup_token is not None,
                    "error": jup_err,
                    "firstPoolCreatedAt": fp.get("createdAt"),
                    "firstPoolCreatedAtMs": jup_first_ms,
                    "graduatedAt": (jup_token or {}).get("graduatedAt"),
                    "graduatedAtMs": jup_grad_ms,
                    "graduatedPool": grad_pool if grad_pool else None,
                    "launchpad": (jup_token or {}).get("launchpad"),
                },
                "dexscreener": {
                    "error": dex_err,
                    "pairCreatedAtMs": dex_created,
                },
                "drift_first_minutes": drift_first,
                "drift_graduated_minutes": drift_grad,
            }
    finally:
        client.close()

    # ---- Stats ----
    def dkey(k: str) -> list[float]:
        return [v[k] for v in results.values() if v[k] is not None]

    df = dkey("drift_first_minutes")
    dg = dkey("drift_graduated_minutes")

    def stats(vals: list[float]) -> dict:
        if not vals:
            return {"n": 0, "mean": None, "median": None, "max": None,
                    "gt_10": 0, "gt_22": 0}
        return {
            "n": len(vals),
            "mean": round(statistics.mean(vals), 2),
            "median": round(statistics.median(vals), 2),
            "max": round(max(vals), 2),
            "gt_10": sum(1 for v in vals if v > 10),
            "gt_22": sum(1 for v in vals if v > 22),
        }

    s_first = stats(df)
    s_grad = stats(dg)

    # ---- Report ----
    lines = []
    lines.append("=== AGE DRIFT: firstPool.createdAt vs DexScreener pairCreatedAt ===")
    lines.append(f"Mints checked: {len(results)}")
    lines.append(f"Drift computed: {s_first['n']}/{len(results)} "
                 f"(missing: {len(results) - s_first['n']})")
    lines.append(f"Mean drift: {s_first['mean']} min (positive = Jupiter older)")
    lines.append(f"Median drift: {s_first['median']} min")
    lines.append(f"Max drift: {s_first['max']} min")
    lines.append(f"Drift > 10 min: {s_first['gt_10']}/{s_first['n']} "
                 f"({100 * s_first['gt_10'] / max(1, s_first['n']):.1f}%)")
    lines.append(f"Drift > 22 min (would flip gate outcome): "
                 f"{s_first['gt_22']}/{s_first['n']} "
                 f"({100 * s_first['gt_22'] / max(1, s_first['n']):.1f}%)")
    lines.append("")
    lines.append("=== AGE DRIFT: graduatedAt vs DexScreener pairCreatedAt ===")
    lines.append(f"Mints with graduatedAt: {s_grad['n']}/{len(results)}")
    lines.append(f"Mean drift: {s_grad['mean']} min")
    lines.append(f"Median drift: {s_grad['median']} min")
    lines.append(f"Max drift: {s_grad['max']} min")
    lines.append(f"Drift > 10 min: {s_grad['gt_10']}/{s_grad['n']} "
                 f"({100 * s_grad['gt_10'] / max(1, s_grad['n']):.1f}%)")
    lines.append(f"Drift > 22 min: {s_grad['gt_22']}/{s_grad['n']} "
                 f"({100 * s_grad['gt_22'] / max(1, s_grad['n']):.1f}%)")
    lines.append("")
    lines.append("=== RECOMMENDATION ===")
    lines.append(recommendation(s_first, s_grad))
    lines.append("")
    lines.append("=== PER-MINT DETAIL (sorted by drift descending) ===")
    lines.append("mint | jup_firstPool | jup_graduatedAt | dex_pairCreated "
                 "| drift_first | drift_graduated | launchpad")
    ordered = sorted(results.items(), key=lambda kv: kv[1]["drift_first_minutes"]
                     if kv[1]["drift_first_minutes"] is not None else -1e9,
                     reverse=True)
    for mint, v in ordered:
        j = v["jupiter"]
        lines.append(
            f"{mint} | {fmt_utc(j['firstPoolCreatedAtMs'])} | "
            f"{fmt_utc(j['graduatedAtMs'])} | {fmt_utc(v['dexscreener']['pairCreatedAtMs'])} | "
            f"{fmt_ms(v['drift_first_minutes'])} | {fmt_ms(v['drift_graduated_minutes'])} | "
            f"{j['launchpad'] or '-'}"
        )

    report = "\n".join(lines) + "\n"
    print("\n" + report)

    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "task": "MT-548",
        "method": "Jupiter GET /tokens/v2/search?query={mint} (x-api-key) vs "
                  "DexScreener GET /latest/dex/search?q={mint}; 250ms spacing, "
                  "alternating order",
        "db_path": str(DB_PATH),
        "mints_checked": len(results),
        "drift_firstPool_stats": s_first,
        "drift_graduated_stats": s_grad,
        "results": results,
    }
    RESULTS_PATH.write_text(json.dumps(summary, indent=2))
    REPORT_PATH.write_text(report)
    print(f"Saved {len(results)} per-mint results to {RESULTS_PATH}")
    print(f"Saved report to {REPORT_PATH}")


def fmt_ms(v: float | None) -> str:
    return "-" if v is None else f"{v:.1f}"


def recommendation(s_first: dict, s_grad: dict) -> str:
    if s_first["n"] == 0:
        return "Use fallback chain because no drift could be computed (all lookups failed)."
    if s_first["gt_22"] == 0 and s_first["gt_10"] == 0:
        return (f"Use firstPool.createdAt because drift is negligible "
                f"(max {s_first['max']} min, none > 10 min).")
    if s_grad["n"] >= s_first["n"] / 2 and s_grad["max"] is not None \
            and s_grad["max"] < s_first["max"]:
        return (f"Use graduatedAt as primary with firstPool.createdAt fallback "
                f"because graduated drift (max {s_grad['max']} min) tracks "
                f"DexScreener better than firstPool (max {s_first['max']} min).")
    if s_first["max"] > 22:
        return (f"Use fallback chain (graduatedAt if present, else DexScreener "
                f"pairCreatedAt via search) because firstPool.createdAt drifts "
                f"> 22 min in {s_first['gt_22']}/{s_first['n']} cases and would "
                f"flip Strategy B's age-gate outcome.")
    return (f"Use firstPool.createdAt with graduatedAt as cross-check; drift "
            f"is modest (max {s_first['max']} min) but {s_first['gt_10']} cases "
            f"exceed 10 min.")


if __name__ == "__main__":
    main()

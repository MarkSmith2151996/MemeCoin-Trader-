"""Phase 6 — standalone GeckoTerminal Profile B collection script.

Run locally on Mac:
    python scripts/run_phase6_collection.py

Polls GeckoTerminal for 2 hours, applies Profile B filters, stores candidates
in a local SQLite database, takes follow-up snapshots, and writes a report.

Dependencies: stdlib + requests (no other third-party imports).
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is required. Install it with: pip install requests")
    sys.exit(1)

BASE_URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"
POOL_URL = "https://api.geckoterminal.com/api/v2/networks/solana/pools/{address}"

TOTAL_CYCLES = 24
CYCLE_INTERVAL_S = 300
PAGE_DELAY_S = 2
SNAPSHOT_DELAY_S = 2
MAX_POOL_AGE_MIN = 240
MIN_POOL_AGE_MIN = 15

DB_PATH = Path("./phase6_candidates.db")
REPORT_PATH = Path("./phase6_report.md")


def _load_api_key() -> str | None:
    key = os.environ.get("COINGECKO_API_KEY")
    if key:
        return key
    env_path = Path(".env")
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("COINGECKO_API_KEY="):
                raw = line.split("=", 1)[1].strip()
                if raw.startswith(('"', "'")) and raw[0] == raw[-1]:
                    raw = raw[1:-1]
                if raw:
                    return raw
    return None


def _build_headers() -> dict[str, str]:
    key = _load_api_key()
    headers: dict[str, str] = {}
    if key:
        headers["x-cg-pro-api-key"] = key
    headers["Accept"] = "application/json"
    return headers


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _parse_gecko_datetime(raw: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _init_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""
        CREATE TABLE IF NOT EXISTS gt_candidates (
            pool_address TEXT PRIMARY KEY,
            mint_address TEXT,
            name TEXT,
            dex_id TEXT,
            discovered_at TEXT NOT NULL,
            age_at_discovery_min REAL,
            liquidity_usd REAL,
            volume_h1_usd REAL,
            price_change_h1_pct REAL,
            buys_h1 INTEGER,
            sells_h1 INTEGER,
            buy_sell_ratio REAL,
            market_cap_usd REAL,
            price_usd REAL
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS gt_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pool_address TEXT NOT NULL,
            snapshot_at TEXT NOT NULL,
            minutes_since_discovery REAL,
            liquidity_usd REAL,
            volume_h1_usd REAL,
            price_change_h1_pct REAL,
            price_usd REAL,
            buys_h1 INTEGER,
            sells_h1 INTEGER
        )
    """)
    db.commit()
    return db


def _pool_age_minutes(pool: dict) -> float | None:
    created_raw = pool.get("attributes", {}).get("pool_created_at")
    if not created_raw:
        return None
    created = _parse_gecko_datetime(str(created_raw))
    if created is None:
        return None
    return (datetime.now(timezone.utc) - created).total_seconds() / 60.0


def _dex_id(pool: dict) -> str:
    dex_data = pool.get("relationships", {}).get("dex", {}).get("data", {})
    if isinstance(dex_data, dict):
        return str(dex_data.get("id", ""))
    return ""


def _evaluate_pool(pool: dict) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    age = _pool_age_minutes(pool)
    if age is None:
        reasons.append("age_unknown")
        return False, reasons
    if age < MIN_POOL_AGE_MIN:
        reasons.append(f"age_too_young:{age:.1f}min")
        return False, reasons
    if age > MAX_POOL_AGE_MIN:
        reasons.append(f"age_too_old:{age:.1f}min")
        return False, reasons

    dex = _dex_id(pool).lower()
    if "pumpswap" not in dex and "raydium" not in dex:
        reasons.append(f"dex_not_target:{dex}")
        return False, reasons

    attrs = pool.get("attributes", {})
    reserve = _safe_float(attrs.get("reserve_in_usd"))
    if reserve < 50000:
        reasons.append(f"liquidity_below_50k:${reserve:.0f}")
        return False, reasons

    vol_h1 = _safe_float(attrs.get("volume_usd", {}).get("h1") if isinstance(attrs.get("volume_usd"), dict) else None)
    if vol_h1 < 10000:
        reasons.append(f"volume_h1_below_10k:${vol_h1:.0f}")
        return False, reasons

    price_change = _safe_float(attrs.get("price_change_percentage", {}).get("h1") if isinstance(attrs.get("price_change_percentage"), dict) else None)
    if price_change < 20.0:
        reasons.append(f"price_change_h1_below_20:{price_change:.1f}%")
        return False, reasons

    txn = attrs.get("transactions", {})
    if isinstance(txn, dict):
        h1 = txn.get("h1", {})
        if isinstance(h1, dict):
            buys = _safe_int(h1.get("buys"))
            sells = _safe_int(h1.get("sells"))
        else:
            buys = 0
            sells = 0
    else:
        buys = 0
        sells = 0

    if buys < 100:
        reasons.append(f"buys_h1_below_100:{buys}")
        return False, reasons

    ratio = buys / max(sells, 1)
    if ratio < 0.8 or ratio > 5.0:
        reasons.append(f"buy_sell_ratio_outside_range:{ratio:.2f}")
        return False, reasons

    return True, reasons


def _extract_candidate_row(pool: dict) -> dict[str, Any]:
    attrs = pool.get("attributes", {})
    txn_h1 = {}
    raw_txn = attrs.get("transactions", {})
    if isinstance(raw_txn, dict):
        h1 = raw_txn.get("h1", {})
        if isinstance(h1, dict):
            txn_h1 = h1
    buys = _safe_int(txn_h1.get("buys"))
    sells = _safe_int(txn_h1.get("sells"))
    price = _safe_float(attrs.get("base_token_price_usd"))
    mc = _safe_float(attrs.get("market_cap_usd"))

    relationships = pool.get("relationships", {})
    mint_data = relationships.get("base_token", {}).get("data", {})
    mint_address = ""
    if isinstance(mint_data, dict):
        mint_address = str(mint_data.get("id", ""))

    return {
        "pool_address": pool.get("id", ""),
        "mint_address": mint_address,
        "name": str(attrs.get("name", "")),
        "dex_id": _dex_id(pool),
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "age_at_discovery_min": _pool_age_minutes(pool),
        "liquidity_usd": _safe_float(attrs.get("reserve_in_usd")),
        "volume_h1_usd": _safe_float(attrs.get("volume_usd", {}).get("h1") if isinstance(attrs.get("volume_usd"), dict) else None),
        "price_change_h1_pct": _safe_float(attrs.get("price_change_percentage", {}).get("h1") if isinstance(attrs.get("price_change_percentage"), dict) else None),
        "buys_h1": buys,
        "sells_h1": sells,
        "buy_sell_ratio": buys / max(sells, 1),
        "market_cap_usd": mc,
        "price_usd": price,
    }


def _insert_candidate(db: sqlite3.Connection, row: dict[str, Any]) -> bool:
    result = db.execute(
        """INSERT OR IGNORE INTO gt_candidates
           (pool_address, mint_address, name, dex_id, discovered_at,
            age_at_discovery_min, liquidity_usd, volume_h1_usd,
            price_change_h1_pct, buys_h1, sells_h1, buy_sell_ratio,
            market_cap_usd, price_usd)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            row["pool_address"], row["mint_address"], row["name"],
            row["dex_id"], row["discovered_at"],
            row["age_at_discovery_min"], row["liquidity_usd"],
            row["volume_h1_usd"], row["price_change_h1_pct"],
            row["buys_h1"], row["sells_h1"], row["buy_sell_ratio"],
            row["market_cap_usd"], row["price_usd"],
        ),
    )
    db.commit()
    return result.rowcount == 1


def _fetch_json(url: str, headers: dict[str, str], session: requests.Session) -> dict | None:
    for attempt in range(2):
        try:
            response = session.get(url, headers=headers, timeout=15)
        except requests.RequestException as error:
            print(f"  Request error for {url}: {error}")
            return None

        if response.status_code == 429 and attempt == 0:
            print(f"  Rate limited for {url}; retrying in 10s")
            time.sleep(10)
            continue
        if response.status_code == 429:
            print(f"  Rate limited again for {url}; skipping")
            return None
        if not response.ok:
            print(f"  HTTP {response.status_code} for {url}; skipping")
            return None
        try:
            return response.json()
        except ValueError:
            print(f"  Invalid JSON for {url}; skipping")
            return None
    return None


def _fetch_pool_detail(address: str, headers: dict[str, str], session: requests.Session) -> dict | None:
    url = POOL_URL.format(address=address)
    data = _fetch_json(url, headers, session)
    if data and isinstance(data, dict):
        return data.get("data")
    return None


def _get_candidates_due_snapshots(db: sqlite3.Connection, now: datetime) -> list[tuple[str, float]]:
    due: list[tuple[str, float]] = []
    rows = db.execute(
        "SELECT pool_address, discovered_at FROM gt_candidates"
    ).fetchall()
    for pool_addr, disc_raw in rows:
        disc = _parse_gecko_datetime(disc_raw)
        if disc is None:
            continue
        elapsed = (now - disc).total_seconds() / 60.0
        for target_min in (15, 30, 60, 120):
            if elapsed >= target_min:
                existing = db.execute(
                    "SELECT 1 FROM gt_snapshots WHERE pool_address=? AND minutes_since_discovery=?",
                    (pool_addr, float(target_min)),
                ).fetchone()
                if existing is None:
                    due.append((pool_addr, float(target_min)))
    return due


def _insert_snapshot(db: sqlite3.Connection, pool_address: str, snapshot_min: float, pool_data: dict) -> None:
    attrs = pool_data.get("attributes", {})
    txn_h1 = {}
    raw_txn = attrs.get("transactions", {})
    if isinstance(raw_txn, dict):
        h1 = raw_txn.get("h1", {})
        if isinstance(h1, dict):
            txn_h1 = h1
    db.execute(
        """INSERT INTO gt_snapshots
           (pool_address, snapshot_at, minutes_since_discovery,
            liquidity_usd, volume_h1_usd, price_change_h1_pct,
            price_usd, buys_h1, sells_h1)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            pool_address,
            datetime.now(timezone.utc).isoformat(),
            snapshot_min,
            _safe_float(attrs.get("reserve_in_usd")),
            _safe_float(attrs.get("volume_usd", {}).get("h1") if isinstance(attrs.get("volume_usd"), dict) else None),
            _safe_float(attrs.get("price_change_percentage", {}).get("h1") if isinstance(attrs.get("price_change_percentage"), dict) else None),
            _safe_float(attrs.get("base_token_price_usd")),
            _safe_int(txn_h1.get("buys")) if isinstance(txn_h1, dict) else 0,
            _safe_int(txn_h1.get("sells")) if isinstance(txn_h1, dict) else 0,
        ),
    )
    db.commit()


def _run_snapshots(db: sqlite3.Connection, headers: dict[str, str], session: requests.Session) -> int:
    now = datetime.now(timezone.utc)
    due = _get_candidates_due_snapshots(db, now)
    taken = 0
    for pool_addr, snapshot_min in due:
        data = _fetch_pool_detail(pool_addr, headers, session)
        if data:
            _insert_snapshot(db, pool_addr, snapshot_min, data)
            taken += 1
        time.sleep(SNAPSHOT_DELAY_S)
    return taken


def _write_report(
    total_cycles: int,
    total_pages: int,
    total_pools: int,
    candidates: list[dict[str, Any]],
    rejection_counts: dict[str, int],
    snapshot_counts: dict[str, int],
    start_time: datetime,
) -> None:
    elapsed_h = (datetime.now(timezone.utc) - start_time).total_seconds() / 3600.0
    rate = len(candidates) / elapsed_h if elapsed_h > 0 else 0.0

    lines: list[str] = [
        "# Phase 6 Collection Report",
        "",
        f"- Run start: {start_time.isoformat()}",
        f"- Run end: {datetime.now(timezone.utc).isoformat()}",
        f"- Cycles completed: {total_cycles}/{TOTAL_CYCLES}",
        f"- Total pages scanned: {total_pages}",
        f"- Total pools evaluated: {total_pools}",
        f"- Profile B candidates: {len(candidates)}",
        f"- Candidates per hour: {rate:.1f}",
        "",
        "## Filter Rejection Breakdown",
        "",
    ]

    sorted_rejections = sorted(rejection_counts.items(), key=lambda x: -x[1])
    for reason, count in sorted_rejections:
        pct = (count / max(total_pools, 1)) * 100
        lines.append(f"- {reason}: {count} ({pct:.1f}%)")
    lines.append("")

    if candidates:
        lines.extend([
            "## Candidates",
            "",
            "| Pool Address | Name | DEX | Age (min) | Liquidity | Volume 1h | Price Δ 1h | Buy/Sell |",
            "|---|---|---|---|---|---|---|---|",
        ])
        for c in candidates:
            addr_short = c["pool_address"][:8] + ".." if len(c["pool_address"]) > 10 else c["pool_address"]
            lines.append(
                f"| {addr_short} | {c['name'] or '?'} | {c['dex_id']} "
                f"| {c['age_at_discovery_min']:.1f} | ${c['liquidity_usd']:,.0f} "
                f"| ${c['volume_h1_usd']:,.0f} | {c['price_change_h1_pct']:+.1f}% "
                f"| {c['buy_sell_ratio']:.2f} |"
            )
        lines.append("")

    if snapshot_counts:
        lines.extend([
            "## Snapshot Trajectories",
            "",
        ])
        for addr, count in sorted(snapshot_counts.items(), key=lambda x: -x[1]):
            addr_short = addr[:8] + ".." if len(addr) > 10 else addr
            lines.append(f"- {addr_short}: {count} snapshot(s)")
        lines.append("")

    conclusion = (
        "**Conclusion:** Candidate pool is sufficient for trading."
        if len(candidates) >= 3
        else (
            "**Conclusion:** Low candidate volume — consider relaxing filters "
            "or extending collection duration."
        )
    )
    lines.append("## Conclusion")
    lines.append("")
    lines.append(conclusion)
    lines.append("")

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    headers = _build_headers()
    db = _init_db()
    session = requests.Session()

    total_pools = 0
    total_pages = 0
    all_candidates: list[dict[str, Any]] = []
    rejection_counts: dict[str, int] = {}
    snapshot_counts: dict[str, int] = {}
    start_time = datetime.now(timezone.utc)

    print(f"[Phase 6] Starting {TOTAL_CYCLES} cycles × {CYCLE_INTERVAL_S}s = {TOTAL_CYCLES * CYCLE_INTERVAL_S // 3600}h collection")
    print(f"[Phase 6] Database: {DB_PATH.resolve()}")
    print(f"[Phase 6] Report:   {REPORT_PATH.resolve()}")
    print()

    for cycle in range(1, TOTAL_CYCLES + 1):
        cycle_start = time.monotonic()
        cycle_pools = 0
        cycle_pages = 0
        cycle_candidates = 0
        page = 1

        while True:
            url = f"{BASE_URL}?page={page}&include=dex"
            data = _fetch_json(url, headers, session)

            if data is None:
                print(f"  [Cycle {cycle}] page {page}: request failed, ending pagination")
                break

            pools = data.get("data", [])
            if not isinstance(pools, list) or len(pools) == 0:
                break

            oldest_age: float | None = None
            for pool in pools:
                age = _pool_age_minutes(pool)
                if age is not None:
                    oldest_age = max(oldest_age or age, age)

                total_pools += 1
                cycle_pools += 1

                passes, reasons = _evaluate_pool(pool)
                if passes:
                    row = _extract_candidate_row(pool)
                    if _insert_candidate(db, row):
                        all_candidates.append(row)
                        cycle_candidates += 1
                else:
                    for r in reasons:
                        key = r.split(":")[0]
                        rejection_counts[key] = rejection_counts.get(key, 0) + 1

            cycle_pages += 1
            total_pages += 1

            if oldest_age is not None and oldest_age > MAX_POOL_AGE_MIN:
                print(f"  [Cycle {cycle}] page {page}: oldest pool > {MAX_POOL_AGE_MIN}min, stopping pagination")
                break

            page += 1
            time.sleep(PAGE_DELAY_S)

        snapshots_taken = _run_snapshots(db, headers, session)
        if snapshots_taken > 0:
            print(f"  [Cycle {cycle}] took {snapshots_taken} follow-up snapshot(s)")

        for c in all_candidates:
            addr = c["pool_address"]
            count = db.execute(
                "SELECT COUNT(*) FROM gt_snapshots WHERE pool_address=?",
                (addr,),
            ).fetchone()[0]
            if count > 0:
                snapshot_counts[addr] = count

        elapsed_cycle = time.monotonic() - cycle_start
        print(
            f"[Cycle {cycle}/{TOTAL_CYCLES}] "
            f"Scanned {cycle_pages} pages, {cycle_pools} pools, "
            f"{len(all_candidates)} candidates found so far "
            f"({elapsed_cycle:.0f}s)"
        )

        if cycle < TOTAL_CYCLES:
            sleep_for = max(0, CYCLE_INTERVAL_S - elapsed_cycle)
            if sleep_for > 0:
                print(f"  Waiting {sleep_for:.0f}s until next cycle...")
                time.sleep(sleep_for)

    print()
    print(f"[Phase 6] Collection complete. Writing report...")
    _write_report(
        total_cycles=TOTAL_CYCLES,
        total_pages=total_pages,
        total_pools=total_pools,
        candidates=all_candidates,
        rejection_counts=rejection_counts,
        snapshot_counts=snapshot_counts,
        start_time=start_time,
    )
    print(f"[Phase 6] Report written to {REPORT_PATH.resolve()}")
    print(f"[Phase 6] Database at {DB_PATH.resolve()}")

    db.close()


if __name__ == "__main__":
    main()

"""Continuous DexScreener observation loop via browser-pc service.

Collects Profile B candidates every 3 minutes, stores them in SQLite,
takes scheduled follow-up snapshots, and writes a Markdown report on exit.

Usage:
    python scripts/run_dexscreener_collection.py

Dependencies: stdlib + requests
"""

from __future__ import annotations

import logging
import re
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BROWSER_PC_URL = "http://172.21.32.1:8099"
CAPTURE_URL = (
    "https://dexscreener.com/new-pairs/solana?"
    "rankBy=trendingScoreH6&order=desc"
    "&dexIds=pumpswap,raydium"
    "&minLiq=50000&minMarketCap=100000&maxMarketCap=10000000"
    "&minAge=1&maxAge=4"
    "&min24HTxns=500&min24HBuys=300&min24HVol=500000"
    "&min1HChg=20&profile=0"
)
CYCLE_INTERVAL_S = 180
CAPTURE_WAIT_S = 10
BROWSER_PC_TIMEOUT = 45
SNAPSHOT_WINDOWS_MIN = [15, 30, 60, 120, 240]

DB_PATH = Path("./dex_candidates.db")
LOG_PATH = Path("./dex_collection.log")
REPORT_PATH = Path("./dex_report.md")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_PATH), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("dex_collector")


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

CANDIDATES_DDL = """
CREATE TABLE IF NOT EXISTS dex_candidates (
    pool_address TEXT PRIMARY KEY,
    name TEXT,
    pair TEXT,
    dex_id TEXT,
    first_seen_at TEXT NOT NULL,
    age_at_discovery_min REAL,
    price_usd_at_discovery REAL,
    liquidity_usd_at_discovery REAL,
    volume_usd_at_discovery REAL,
    change_1h_pct_at_discovery REAL,
    buys_at_discovery INTEGER,
    sells_at_discovery INTEGER,
    traders_at_discovery INTEGER,
    market_cap_at_discovery REAL,
    last_seen_at TEXT,
    snapshot_count INTEGER DEFAULT 0
)
"""

SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS dex_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pool_address TEXT NOT NULL,
    snapshot_at TEXT NOT NULL,
    minutes_since_discovery REAL,
    price_usd REAL,
    liquidity_usd REAL,
    volume_usd REAL,
    change_1h_pct REAL,
    buys INTEGER,
    sells INTEGER,
    traders INTEGER,
    market_cap REAL
)
"""


def _init_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(CANDIDATES_DDL)
    db.execute(SNAPSHOTS_DDL)
    db.commit()
    return db


# ---------------------------------------------------------------------------
# Value parsing helpers
# ---------------------------------------------------------------------------

def _parse_price(text: str) -> float | None:
    text = text.strip().replace(",", "")
    if text.startswith("$"):
        text = text[1:]
    suffixes = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    suffix = text[-1].upper() if text else ""
    if suffix in suffixes and len(text) > 1:
        try:
            return float(text[:-1]) * suffixes[suffix]
        except (ValueError, TypeError):
            return None
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def _parse_int(text: str) -> int | None:
    text = text.strip().replace(",", "")
    try:
        return int(float(text))
    except (ValueError, TypeError):
        return None


def _parse_pct(text: str) -> float | None:
    text = text.strip().replace("%", "").replace(",", "")
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def _parse_age_min(text: str) -> float | None:
    text = text.strip().lower()
    total = 0.0
    m = re.findall(r"(\d+(?:\.\d+)?)\s*([smhd])", text)
    if not m:
        return None
    units = {"s": 1 / 60, "m": 1, "h": 60, "d": 1440}
    for val_str, unit in m:
        total += float(val_str) * units[unit]
    return total


# ---------------------------------------------------------------------------
# Page-text parser for DexScreener UI rows
# ---------------------------------------------------------------------------

TOKEN_LINE_SKIP = {"/", "sol", "?", ""}


def _is_price_line(text: str) -> bool:
    t = text.strip()
    if not t:
        return False
    if t.startswith("$"):
        rest = t[1:].replace(",", "")
        try:
            float(rest)
            return True
        except ValueError:
            pass
    return False


def _token_block_end(lines: list[str], start: int) -> int:
    for i in range(start, len(lines)):
        if _is_price_line(lines[i]):
            return i
    return len(lines)


def parse_candidates_from_text(page_text: str) -> list[dict[str, Any]]:
    """Parse page_text into candidate dicts using \\n#N\\n row markers."""
    candidates: list[dict[str, Any]] = []
    blocks = re.split(r"\n#\d+\n|\n#\d+ |^#\d+\n|^#\d+ ", page_text)
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        lines_raw = block.split("\n")
        lines = [l.strip() for l in lines_raw if l.strip()]
        if len(lines) < 6:
            continue

        price_idx = _token_block_end(lines, 0)
        if price_idx >= len(lines) - 7:
            continue

        token_lines = lines[:price_idx]
        name = token_lines[-1] if token_lines else ""
        symbol = ""
        for tl in token_lines:
            if tl.lower() not in TOKEN_LINE_SKIP and tl != name:
                symbol = tl
                break
        if not symbol:
            symbol = name

        pair = f"{symbol}/SOL"

        price = _parse_price(lines[price_idx]) if price_idx < len(lines) else None
        age_str = lines[price_idx + 1] if price_idx + 1 < len(lines) else ""
        buys = _parse_int(lines[price_idx + 2]) if price_idx + 2 < len(lines) else 0
        sells = _parse_int(lines[price_idx + 3]) if price_idx + 3 < len(lines) else 0
        volume = _parse_price(lines[price_idx + 4]) if price_idx + 4 < len(lines) else None
        traders = _parse_int(lines[price_idx + 5]) if price_idx + 5 < len(lines) else 0
        change_1h = _parse_pct(lines[price_idx + 7]) if price_idx + 7 < len(lines) else None
        liquidity = _parse_price(lines[price_idx + 11]) if price_idx + 11 < len(lines) else None
        mcap = _parse_price(lines[price_idx + 12]) if price_idx + 12 < len(lines) else None

        candidates.append({
            "pool_address": _make_pool_address(symbol),
            "name": name,
            "pair": pair,
            "dex_id": "pumpswap/raydium",
            "price_usd": price,
            "age_min": _parse_age_min(age_str) if age_str else None,
            "buys": buys or 0,
            "sells": sells or 0,
            "volume_usd": volume,
            "traders": traders or 0,
            "change_1h_pct": change_1h,
            "liquidity_usd": liquidity,
            "market_cap_usd": mcap,
        })
    return candidates


def _make_pool_address(symbol: str) -> str:
    safe = re.sub(r"[^a-z0-9]", "_", symbol.lower().strip())
    return f"ui_{safe}" if safe else f"ui_unknown_{int(time.time())}"


# ---------------------------------------------------------------------------
# Browser-pc capture
# ---------------------------------------------------------------------------

def capture_page(session: requests.Session) -> dict | None:
    """POST /capture to browser-pc, return response dict or None."""
    payload = {"url": CAPTURE_URL, "wait_seconds": CAPTURE_WAIT_S}
    try:
        resp = session.post(
            f"{BROWSER_PC_URL}/capture",
            json=payload,
            timeout=BROWSER_PC_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.ConnectionError:
        log.warning("browser-pc connection refused — is the service running?")
    except requests.Timeout:
        log.warning("browser-pc timed out after %ss", BROWSER_PC_TIMEOUT)
    except requests.HTTPError as e:
        log.warning("browser-pc HTTP error: %s", e)
    except Exception as e:
        log.warning("browser-pc request failed: %s", e)
    return None


# ---------------------------------------------------------------------------
# Candidate extraction from capture response
# ---------------------------------------------------------------------------

def extract_candidates(data: dict) -> list[dict[str, Any]] | None:
    """Extract candidates from the browser-pc response.

    Priority:
    1. ``candidates`` key (future MT-458 structured parser)
    2. ``page_text`` with #N row markers
    3. ``rows`` from HTML table extraction
    Returns None if no structured data is available.
    """
    if "candidates" in data and isinstance(data["candidates"], list):
        raw = data["candidates"]
        if raw and isinstance(raw[0], dict):
            log.info("Using structured candidates array from MT-458 parser")
            return raw

    page_text = data.get("page_text", "")
    if page_text and re.search(r"\n#\d+\n", page_text):
        parsed = parse_candidates_from_text(page_text)
        if parsed:
            log.info("Parsed %d candidates from page text", len(parsed))
            return parsed

    rows = data.get("rows", [])
    if rows and isinstance(rows, list) and len(rows) > 0:
        parsed = parse_candidates_from_rows(rows)
        if parsed:
            log.info("Parsed %d candidates from HTML rows", len(parsed))
            return parsed

    return None


def parse_candidates_from_rows(rows: list[list[str]]) -> list[dict[str, Any]]:
    """Attempt to parse candidates from the ``rows`` field of the response.

    Expects cells in DexScreener grid order:
    [#, TOKEN, NAME?, PRICE, AGE, BUYS, SELLS, VOLUME, TRADERS, 5M, 1H, 6H, 24H, LIQUIDITY, MCAP]
    """
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 10:
            continue

        symbol = ""
        name = ""
        first_cell = str(row[0]).strip() if row else ""
        second_cell = str(row[1]).strip() if len(row) > 1 else ""

        if re.match(r"^#?\d*$", first_cell) or first_cell == "":
            offset = 1
        else:
            offset = 0

        token_cell = str(row[offset]).strip() if offset < len(row) else ""
        if "/" in token_cell:
            parts = token_cell.split("/")
            symbol = parts[0].strip()
        else:
            symbol = token_cell

        name_cell = str(row[offset + 1]).strip() if offset + 1 < len(row) else ""
        if name_cell and name_cell != symbol:
            name = name_cell

        price_str = str(row[offset + 2]).strip() if offset + 2 < len(row) else ""
        age_str = str(row[offset + 3]).strip() if offset + 3 < len(row) else ""
        buys_str = str(row[offset + 4]).strip() if offset + 4 < len(row) else ""
        sells_str = str(row[offset + 5]).strip() if offset + 5 < len(row) else ""
        vol_str = str(row[offset + 6]).strip() if offset + 6 < len(row) else ""
        traders_str = str(row[offset + 7]).strip() if offset + 7 < len(row) else ""
        change_1h_str = str(row[offset + 9]).strip() if offset + 9 < len(row) else ""
        liq_str = str(row[offset + 11]).strip() if offset + 11 < len(row) else ""
        mcap_str = str(row[offset + 12]).strip() if offset + 12 < len(row) else ""

        if not symbol or not price_str:
            continue

        candidates.append({
            "pool_address": _make_pool_address(symbol),
            "name": name or symbol,
            "pair": f"{symbol}/SOL",
            "dex_id": "pumpswap/raydium",
            "price_usd": _parse_price(price_str),
            "age_min": _parse_age_min(age_str) if age_str else None,
            "buys": _parse_int(buys_str) or 0,
            "sells": _parse_int(sells_str) or 0,
            "volume_usd": _parse_price(vol_str),
            "traders": _parse_int(traders_str) or 0,
            "change_1h_pct": _parse_pct(change_1h_str),
            "liquidity_usd": _parse_price(liq_str),
            "market_cap_usd": _parse_price(mcap_str),
        })
    return candidates


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def upsert_candidate(db: sqlite3.Connection, cand: dict[str, Any]) -> bool:
    """Insert or update a candidate row. Returns True if new, False if updated."""
    now = datetime.now(timezone.utc).isoformat()
    existing = db.execute(
        "SELECT 1 FROM dex_candidates WHERE pool_address=?",
        (cand["pool_address"],),
    ).fetchone()

    if existing:
        db.execute(
            """UPDATE dex_candidates SET
               last_seen_at=?, snapshot_count=snapshot_count+1
               WHERE pool_address=?""",
            (now, cand["pool_address"]),
        )
        db.commit()
        return False

    db.execute(
        """INSERT INTO dex_candidates
           (pool_address, name, pair, dex_id, first_seen_at,
            age_at_discovery_min, price_usd_at_discovery,
            liquidity_usd_at_discovery, volume_usd_at_discovery,
            change_1h_pct_at_discovery, buys_at_discovery, sells_at_discovery,
            traders_at_discovery, market_cap_at_discovery, last_seen_at,
            snapshot_count)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
        (
            cand["pool_address"],
            cand.get("name", ""),
            cand.get("pair", ""),
            cand.get("dex_id", ""),
            now,
            cand.get("age_min"),
            cand.get("price_usd"),
            cand.get("liquidity_usd"),
            cand.get("volume_usd"),
            cand.get("change_1h_pct"),
            cand.get("buys", 0),
            cand.get("sells", 0),
            cand.get("traders", 0),
            cand.get("market_cap_usd"),
            now,
        ),
    )
    db.commit()
    return True


def get_due_snapshots(db: sqlite3.Connection) -> list[tuple[str, float]]:
    """Find candidates needing follow-up snapshots at the next window."""
    now = datetime.now(timezone.utc)
    due: list[tuple[str, float]] = []
    rows = db.execute(
        "SELECT pool_address, first_seen_at FROM dex_candidates"
    ).fetchall()

    for pool_addr, seen_raw in rows:
        try:
            seen = datetime.fromisoformat(seen_raw)
            if seen.tzinfo is None:
                seen = seen.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        elapsed_min = (now - seen).total_seconds() / 60.0
        for target_min in SNAPSHOT_WINDOWS_MIN:
            if elapsed_min >= target_min:
                existing = db.execute(
                    "SELECT 1 FROM dex_snapshots WHERE pool_address=? AND minutes_since_discovery=?",
                    (pool_addr, float(target_min)),
                ).fetchone()
                if existing is None:
                    due.append((pool_addr, float(target_min)))

    return due


def insert_snapshot(db: sqlite3.Connection, pool_address: str, window_min: float, cand: dict[str, Any]) -> None:
    db.execute(
        """INSERT INTO dex_snapshots
           (pool_address, snapshot_at, minutes_since_discovery,
            price_usd, liquidity_usd, volume_usd, change_1h_pct,
            buys, sells, traders, market_cap)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            pool_address,
            datetime.now(timezone.utc).isoformat(),
            window_min,
            cand.get("price_usd"),
            cand.get("liquidity_usd"),
            cand.get("volume_usd"),
            cand.get("change_1h_pct"),
            cand.get("buys", 0),
            cand.get("sells", 0),
            cand.get("traders", 0),
            cand.get("market_cap_usd"),
        ),
    )
    db.commit()


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def write_report(
    total_cycles: int,
    total_candidates_discovered: int,
    start_time: datetime,
    end_time: datetime,
    snapshot_counts: dict[str, int],
    db: sqlite3.Connection,
) -> None:
    elapsed_h = (end_time - start_time).total_seconds() / 3600.0
    rate = total_candidates_discovered / elapsed_h if elapsed_h > 0 else 0.0

    all_candidates = db.execute(
        "SELECT pool_address, name, pair, first_seen_at, age_at_discovery_min, "
        "liquidity_usd_at_discovery, volume_usd_at_discovery, "
        "change_1h_pct_at_discovery, market_cap_at_discovery, snapshot_count "
        "FROM dex_candidates ORDER BY first_seen_at"
    ).fetchall()

    lines: list[str] = [
        "# DexScreener Collection Report",
        "",
        f"- Run start: {start_time.isoformat()}",
        f"- Run end: {end_time.isoformat()}",
        f"- Total cycles: {total_cycles}",
        f"- Total unique candidates: {total_candidates_discovered}",
        f"- Candidates per hour: {rate:.1f}",
        "",
        "## Candidates",
        "",
        "| Pool | Name | Pair | Age (min) | Liq | Vol | Δ1h % | MCap | Snapshots |",
        "|------|------|------|-----------|-----|-----|-------|------|-----------|",
    ]

    for row in all_candidates:
        addr, name, pair, first_seen, age, liq, vol, chg, mcap, snap_count = row
        addr_short = str(addr)[:12] + ".." if len(str(addr)) > 14 else str(addr)
        lines.append(
            f"| {addr_short} | {name or '?'} | {pair or '?'} "
            f"| {_f(age)} | {_fd(liq)} | {_fd(vol)} | {_fp(chg)} "
            f"| {_fd(mcap)} | {snap_count} |"
        )
    lines.append("")

    lines.extend([
        "## Snapshot Trajectories",
        "",
    ])
    for pool_addr, snap_rows in snapshot_counts.items():
        addr_short = pool_addr[:12] + ".." if len(pool_addr) > 14 else pool_addr
        lines.append(f"- {addr_short}: {snap_rows} snapshot(s)")
        sub = db.execute(
            "SELECT minutes_since_discovery, price_usd FROM dex_snapshots "
            "WHERE pool_address=? ORDER BY minutes_since_discovery",
            (pool_addr,),
        ).fetchall()
        for mins, price in sub:
            lines.append(f"  - +{int(mins)}min: ${_f(price)}")
    lines.append("")

    conclusion = (
        "**Conclusion:** Viable candidate stream — sufficient density for observation."
        if total_candidates_discovered >= 3
        else "**Conclusion:** Low candidate volume — consider relaxing filters or increasing poll frequency."
    )
    lines.extend(["## Conclusion", "", conclusion, ""])

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("Report written to %s", REPORT_PATH.resolve())


def _f(v: object) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.4g}"
    return str(v)


def _fd(v: object) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        if abs(v) >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        if abs(v) >= 1_000:
            return f"${v / 1_000:.0f}K"
        return f"${v:.2f}"
    return str(v)


def _fp(v: object) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:+.1f}%"
    return str(v)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    db = _init_db()
    session = requests.Session()

    total_cycles = 0
    total_candidates = 0
    start_time = datetime.now(timezone.utc)
    snapshot_counts: dict[str, int] = {}

    log.info("=" * 60)
    log.info("DexScreener Collection Loop — Starting")
    log.info("Browser-pc: %s", BROWSER_PC_URL)
    log.info("Database:   %s", DB_PATH.resolve())
    log.info("Log:        %s", LOG_PATH.resolve())
    log.info("Report:     %s", REPORT_PATH.resolve())
    log.info("Cycle:      every %ds", CYCLE_INTERVAL_S)
    log.info("=" * 60)

    try:
        while True:
            cycle_start = time.monotonic()
            total_cycles += 1
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

            data = capture_page(session)

            if data is None:
                log.warning("[Cycle %d] %s — capture failed, retrying in 30s", total_cycles, now_str)
                time.sleep(30)
                continue

            if data.get("cloudflare_detected"):
                log.warning("[Cycle %d] %s — Cloudflare detected, skipping storage", total_cycles, now_str)

            candidates = extract_candidates(data)

            if candidates is None:
                page_text = data.get("page_text", "")
                text_snippet = page_text[:120].replace("\n", " ") if page_text else "empty"
                log.info(
                    "[Cycle %d] %s — structured parser not available "
                    "(no candidates key, no parseable rows/page_text). "
                    "raw_text=%s...",
                    total_cycles, now_str, text_snippet,
                )
            else:
                new_count = 0
                for cand in candidates:
                    is_new = upsert_candidate(db, cand)
                    if is_new:
                        total_candidates += 1
                        new_count += 1
                        log.info(
                            "[Cycle %d] %s — NEW candidate: %s | $%s | liq=$%s | vol=$%s",
                            total_cycles, now_str,
                            cand.get("pair", "?"),
                            _f(cand.get("price_usd")),
                            _f(cand.get("liquidity_usd")),
                            _f(cand.get("volume_usd")),
                        )

                # Update snapshot_counts for the report
                for c in db.execute(
                    "SELECT pool_address, COUNT(*) FROM dex_snapshots GROUP BY pool_address"
                ).fetchall():
                    snapshot_counts[c[0]] = c[1]

                known = len(candidates) - new_count
                log.info(
                    "[Cycle %d] %s — %d candidates on board (%d new, %d known)",
                    total_cycles, now_str, len(candidates), new_count, known,
                )

            # Check for due follow-up snapshots
            due = get_due_snapshots(db)
            snapshots_taken = 0
            if due and candidates:
                lookup_map = {c["pool_address"]: c for c in candidates}
                for pool_addr, window_min in due:
                    cand = lookup_map.get(pool_addr)
                    if cand is not None:
                        insert_snapshot(db, pool_addr, window_min, cand)
                        snapshots_taken += 1
                        log.info(
                            "  Snapshot +%dmin for %s: $%s",
                            int(window_min), pool_addr[:12],
                            _f(cand.get("price_usd")),
                        )
                    # If not in current cycle's candidates, skip — will
                    # capture when the token reappears on the board

            if snapshots_taken:
                log.info("  Took %d follow-up snapshot(s)", snapshots_taken)

            elapsed_cycle = time.monotonic() - cycle_start
            sleep_for = max(0, CYCLE_INTERVAL_S - elapsed_cycle)

            if sleep_for > 0:
                log.info("  Next cycle in %ds", int(sleep_for))
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        end_time = datetime.now(timezone.utc)
        elapsed_total = (end_time - start_time).total_seconds()
        log.info("")
        log.info("=" * 60)
        log.info("KeyboardInterrupt — writing report")
        log.info("Total runtime: %ds (%d cycles)", int(elapsed_total), total_cycles)
        log.info("Total candidates discovered: %d", total_candidates)
        log.info("=" * 60)

        write_report(
            total_cycles=total_cycles,
            total_candidates_discovered=total_candidates,
            start_time=start_time,
            end_time=end_time,
            snapshot_counts=snapshot_counts,
            db=db,
        )

    finally:
        db.close()
        session.close()


if __name__ == "__main__":
    main()

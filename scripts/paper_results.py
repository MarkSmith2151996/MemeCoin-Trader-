"""Human-readable paper trading summary."""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.execution.price_provider import DexScreenerPriceProvider
from src.monitoring.dashboard import resolve_db_path


def _ticker(mint: str) -> str:
    return mint[:6].upper()


def _fmt_pnl(pnl_pct: float) -> str:
    sign = "+" if pnl_pct >= 0 else ""
    return f"{sign}{pnl_pct:.1f}%"


def _fmt_age(iso_str: str) -> str:
    try:
        opened = datetime.fromisoformat(iso_str)
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=UTC)
        minutes = int((datetime.now(UTC) - opened).total_seconds() / 60)
        if minutes < 1:
            return "<1min"
        return f"{minutes}min"
    except (ValueError, TypeError):
        return "?"


async def get_open_positions(db_path: Path) -> list[dict]:
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            """SELECT mint_address, entry_price_sol, token_amount, opened_at
               FROM positions WHERE status != 'CLOSED' ORDER BY opened_at DESC"""
        )
        rows = await cursor.fetchall()
    return [
        {"mint": r[0], "entry_price": r[1], "token_amount": r[2], "opened_at": r[3]}
        for r in rows
    ]


async def get_closed_today(db_path: Path) -> list[dict]:
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            """SELECT p.mint_address, p.entry_price_sol, p.close_price_sol,
                      p.realized_pnl_sol, p.amount_sol
               FROM positions p
               WHERE p.status = 'CLOSED'
                 AND p.closed_at >= datetime('now', '-1 day')
               ORDER BY p.closed_at DESC"""
        )
        rows = await cursor.fetchall()

    results = []
    async with aiosqlite.connect(db_path) as db:
        for r in rows:
            mint, entry, close_price, realized_pnl, amount_sol = r
            reason = "unknown"
            cursor = await db.execute(
                """SELECT metadata_json FROM trades
                   WHERE mint_address = ? AND side = 'SELL'
                   ORDER BY executed_at DESC LIMIT 1""",
                (mint,),
            )
            trade_row = await cursor.fetchone()
            if trade_row:
                try:
                    meta = json.loads(trade_row[0])
                    reason = meta.get("close_reason", "unknown")
                except (json.JSONDecodeError, TypeError):
                    pass
            results.append({
                "mint": mint,
                "entry_price": entry,
                "close_price": close_price,
                "realized_pnl": realized_pnl or 0.0,
                "reason": reason,
            })
    return results


async def main() -> None:
    db_path = resolve_db_path(None)
    now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
    print(f"=== Paper Trading Results — {now_str} UTC ===\n")

    if not db_path.exists():
        print("No database found yet.\n")
        _print_summary(0, 0, 0, 0.0, 0.0)
        return

    open_positions = await get_open_positions(db_path)
    closed_today = await get_closed_today(db_path)
    price_provider = DexScreenerPriceProvider()

    print(f"OPEN POSITIONS ({len(open_positions)})")
    open_unrealized = 0.0
    for pos in open_positions:
        mint = pos["mint"]
        entry = pos["entry_price"]
        age = _fmt_age(pos["opened_at"])
        current_price = await price_provider.get_current_price(mint)

        if current_price and entry > 0:
            pnl_pct = ((current_price - entry) / entry) * 100
            pnl_str = _fmt_pnl(pnl_pct)
            price_str = f"{current_price:.8f} SOL"
            open_unrealized += (current_price - entry) * pos["token_amount"]
        else:
            pnl_str = "N/A"
            price_str = "N/A"

        print(f"  {_ticker(mint):6}  entry={entry:.8f} SOL  current={price_str}  PnL={pnl_str}  age={age}")
    print()

    print(f"CLOSED TODAY ({len(closed_today)})")
    wins = 0
    realized_total = 0.0
    for pos in closed_today:
        mint = pos["mint"]
        entry = pos["entry_price"]
        close_price = pos["close_price"]
        realized_pnl = pos["realized_pnl"]
        reason = pos["reason"]
        realized_total += realized_pnl

        if realized_pnl > 0:
            wins += 1

        if entry and close_price and entry > 0 and close_price > 0:
            pnl_pct = ((close_price - entry) / entry) * 100
            pnl_str = _fmt_pnl(pnl_pct)
        else:
            pnl_str = "N/A"

        print(f"  {_ticker(mint):6}  entry={entry:.8f}  close={close_price:.8f}  PnL={pnl_str}  reason={reason}")
    print()

    _print_summary(len(open_positions), len(closed_today), wins, realized_total, open_unrealized)


def _print_summary(
    open_count: int,
    closed_count: int,
    wins: int,
    realized_total: float,
    unrealized_total: float,
) -> None:
    win_rate_str = f"{wins}/{closed_count} ({int(wins / closed_count * 100) if closed_count else 0}%)"
    print("SUMMARY")
    print(f"  Open positions:     {open_count}")
    print(f"  Closed today:       {closed_count}")
    print(f"  Win rate:           {win_rate_str}")
    print(f"  Realized PnL:       {realized_total:+.6f} SOL")
    print(f"  Unrealized PnL:     {unrealized_total:+.6f} SOL (live marks)")


if __name__ == "__main__":
    asyncio.run(main())

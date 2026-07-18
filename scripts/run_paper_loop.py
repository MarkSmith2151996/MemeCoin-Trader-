"""Three-layer paper trading loop with split cycles.

Scan cycle (every 3 min):
  1. browser-pc  → scan Profile B DexScreener URL → coin names
  2. DexScreener search API → name → mint address
  3. JupiterClient.get_quote() → entry price
  4. Record paper entry (max 3 open positions, 0.01 SOL each)

Monitor cycle (every 30s):
  5. Re-mark and close open positions (trailing stop / hard stop / time stop)

Run: python scripts/run_paper_loop.py
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from pathlib import Path

import httpx
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.core.config import load_settings
from src.core.database import init_db, record_trade
from src.core.models import Side, Trade
from src.execution.price_provider import DexScreenerPriceProvider
from src.execution.paper import PaperExecutionAdapter
from src.strategy.position_manager import PositionManager
from src.monitoring.alerts import send_imessage
from src.risk.rugcheck import RugCheckClient

BROWSER_PC_URL = "http://localhost:8099"
CAPTURE_URL = (
    "https://dexscreener.com/new-pairs/solana?"
    "rankBy=trendingScoreH6&order=desc"
    "&dexIds=pumpswap,raydium"
    "&minLiq=50000&minMarketCap=100000&maxMarketCap=10000000"
    "&minAge=0&maxAge=1"
    "&min24HTxns=500&min24HBuys=300&min24HVol=500000"
    "&min1HChg=20&profile=0"
)
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"

SCAN_INTERVAL_S = 180
MONITOR_INTERVAL_S = 10
MAX_OPEN_POSITIONS = 3
PAPER_SIZE_SOL = 0.01
TRAILING_STOP_PCT = 0.08
HARD_STOP_PCT = 0.20
TIME_STOP_MINUTES = 30
RUGCHECK_ENABLED = True

DB_PATH = Path("data/trades.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("paper_loop")


def scan_candidates() -> list[str]:
    """Call browser-pc, return list of coin names from Profile B URL."""
    try:
        resp = requests.post(
            f"{BROWSER_PC_URL}/capture",
            json={"url": CAPTURE_URL, "wait": 4},
            timeout=45,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("browser-pc scan failed: %s", exc)
        return []

    names: list[str] = []
    if "candidates" in data and isinstance(data["candidates"], list):
        for row in data["candidates"]:
            name = row.get("name") or row.get("symbol") or row.get("token")
            if name and isinstance(name, str):
                names.append(name.strip())
        if names:
            log.info("browser-pc: %d candidates (structured)", len(names))
            return names

    page_text = data.get("page_text", "")
    tokens = re.findall(r"#\d+\n([^\n]+)", page_text)
    names = [t.strip() for t in tokens if t.strip()]
    log.info("browser-pc: %d candidates (text fallback)", len(names))
    return names


async def resolve_mint(name: str, client: httpx.AsyncClient) -> str | None:
    """Search DexScreener for the coin name, return Solana mint address or None."""
    try:
        resp = await client.get(
            DEXSCREENER_SEARCH_URL,
            params={"q": name},
            timeout=10.0,
        )
        resp.raise_for_status()
        pairs = resp.json().get("pairs") or []
    except Exception as exc:
        log.debug("DexScreener search failed for %s: %s", name, exc)
        return None

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        if pair.get("chainId") != "solana":
            continue
        quote = pair.get("quoteToken", {})
        if quote.get("address") != WRAPPED_SOL_MINT:
            continue
        mint = (pair.get("baseToken") or {}).get("address")
        if mint and isinstance(mint, str):
            log.info("RESOLVED %s → %s", name, mint)
            return mint
    return None


async def try_enter(
    mint: str,
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
) -> bool:
    """Price via DexScreener and record a paper entry. Returns True if entry recorded."""
    existing = await manager.get_position(mint, mode="paper")
    if existing is not None:
        log.warning("SKIP %s — position already open", mint)
        return False

    if RUGCHECK_ENABLED:
        result = await _rugcheck.fetch_report(mint)
        if result.provider_status != "ok":
            log.warning("SKIP %s — RugCheck unavailable for %s — skipping", mint, mint)
            return False
        if result.mint_authority_revoked is not True:
            log.warning("SKIP %s — mint authority not revoked", mint)
            return False
        if result.freeze_authority_revoked is not True:
            log.warning("SKIP %s — freeze authority not revoked", mint)
            return False
        if result.is_honeypot is True:
            log.warning("SKIP %s — flagged as honeypot", mint)
            return False
        if result.is_honeypot is None:
            log.warning("SKIP %s — honeypot status unknown, allowing through", mint)
        if result.liquidity_locked is False:
            log.warning("SKIP %s — liquidity not locked", mint)
        elif result.liquidity_locked is None:
            log.warning("SKIP %s — liquidity lock status unknown (soft warn)", mint)
        if result.top_holder_pct is not None and result.top_holder_pct >= 80:
            log.warning("SKIP %s — top 10 holder concentration %.1f%% >= 80%%", mint, result.top_holder_pct)
            return False

    price = await mark_provider.get_current_price(mint)
    if price is None or price <= 0:
        log.warning("SKIP %s — no valid DexScreener price", mint)
        return False

    try:
        trade = await adapter.execute_swap(mint, Side.BUY, PAPER_SIZE_SOL)
    except Exception as exc:
        log.warning("SKIP %s — execute_swap failed: %s", mint, exc)
        return False

    if trade is None:
        log.warning("SKIP %s — execute_swap returned None", mint)
        return False

    try:
        await record_trade(db_path, trade)
    except Exception as exc:
        log.warning("SKIP %s — record_trade failed: %s", mint, exc)
        return False

    try:
        from src.core.models import Signal, SignalSource, SignalType

        dummy_signal = Signal(
            source=SignalSource.MANUAL,
            type=SignalType.NEW_POOL,
            mint_address=mint,
            confidence=1.0,
        )
        await manager.open_position(trade, dummy_signal)
    except Exception as exc:
        log.warning("SKIP %s — open_position failed: %s", mint, exc)
        return False

    log.info("ENTRY: mint=%s price=%.8f SOL", mint, price)
    send_imessage(
        f"\U0001f7e2 [STRATEGY A] ENTERED {mint[:8]}\n"
        f"Price: {price:.8f} SOL\n"
        f"Size: {PAPER_SIZE_SOL} SOL"
    )
    return True


async def monitor_positions(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
) -> None:
    """Re-mark open positions and close any that hit stop or time limit."""
    from datetime import UTC, datetime

    positions = await manager.get_all_open(mode="paper")
    for pos in positions:
        current_price = await mark_provider.get_current_price(pos.mint_address)
        if current_price is None:
            age_min = (datetime.now(UTC) - pos.opened_at).total_seconds() / 60
            if age_min >= TIME_STOP_MINUTES:
                log.warning("CLOSE [price_unavailable]: mint=%s age=%.1fmin — force closing stale position", pos.mint_address[:16], age_min)
                peak = peak_prices.get(pos.mint_address)
                await manager.close_position(pos.mint_address, 0.0, mode="paper", peak_price_sol=peak)
                peak_prices.pop(pos.mint_address, None)
            else:
                log.warning("SKIP mark: mint=%s — DexScreener returned None (age=%.1fmin)", pos.mint_address[:16], age_min)
            continue

        age_min = (datetime.now(UTC) - pos.opened_at).total_seconds() / 60
        entry = pos.entry_price_sol if pos.entry_price_sol > 0 else current_price

        prev_peak = peak_prices.get(pos.mint_address, entry)
        peak = max(prev_peak, current_price)
        peak_prices[pos.mint_address] = peak

        pct_from_entry = (current_price - entry) / entry
        if pct_from_entry <= -0.75:
            log.warning("CLOSE [rug_detected]: mint=%s entry=%.8f current=%.8f drop=%.1f%%",
                        pos.mint_address[:16], entry, current_price, pct_from_entry * 100)
            close_reason = "rug_detected"
            close_price = current_price
        else:
            close_reason = None
            close_price = current_price
        if entry:
            drop_from_entry = (entry - current_price) / entry
            if drop_from_entry >= HARD_STOP_PCT:
                close_reason = "hard_stop"
                close_price = current_price
            elif (peak - current_price) / peak >= TRAILING_STOP_PCT:
                close_reason = "trailing_stop"
                close_price = current_price
        if age_min >= TIME_STOP_MINUTES and close_reason is None:
            close_reason = "time_stop"

        if close_reason:
            peak = peak_prices.get(pos.mint_address)
            peak_prices.pop(pos.mint_address, None)
            trade = await _adapter_close(pos, current_price, close_reason, db_path)
            await manager.close_position(pos.mint_address, current_price, mode="paper", peak_price_sol=peak)
            pnl_pct = ((current_price - pos.entry_price_sol) / pos.entry_price_sol) * 100 if pos.entry_price_sol else 0.0
            peak_pnl_pct = ((peak - pos.entry_price_sol) / pos.entry_price_sol) * 100 if pos.entry_price_sol else 0.0
            log.info(
                "CLOSE [%s]: mint=%s entry=%.8f peak=%.8f close=%.8f",
                close_reason, pos.mint_address[:16], pos.entry_price_sol, peak, current_price,
            )
            send_imessage(
                f"\U0001f534 [STRATEGY A] CLOSED {pos.mint_address[:8]}\n"
                f"Entry: {pos.entry_price_sol:.8f} \u2192 Close: {current_price:.8f}\n"
                f"PnL: {pnl_pct:+.1f}%  Peak: {peak_pnl_pct:+.1f}%\n"
                f"Reason: {close_reason}"
            )


async def _adapter_close(pos, close_price: float, reason: str, db_path: Path) -> Trade:
    """Record a paper sell trade for a closing position."""
    import uuid
    from datetime import UTC, datetime

    token_remaining = pos.token_amount
    sol_out = token_remaining * close_price
    trade = Trade(
        id=str(uuid.uuid4()),
        mint_address=pos.mint_address,
        side=Side.SELL,
        amount_sol=sol_out,
        token_amount=token_remaining,
        price_sol=close_price,
        slippage_bps=300,
        mode="paper",
        status="simulated",
        metadata={"close_reason": reason},
    )
    await record_trade(db_path, trade)
    return trade


_rugcheck = RugCheckClient()

seen_mints: set[str] = set()
peak_prices: dict[str, float] = {}  # mint -> highest price seen


async def scan_loop(
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
) -> None:
    """Discover and enter new candidates every 3 minutes."""
    global seen_mints
    async with httpx.AsyncClient() as http:
        while True:
            cycle_start = time.monotonic()
            log.info("--- Scan cycle ---")
            open_positions = await manager.get_all_open(mode="paper")
            slots_available = MAX_OPEN_POSITIONS - len(open_positions)
            log.info("Open positions: %d / %d", len(open_positions), MAX_OPEN_POSITIONS)

            if slots_available > 0:
                names = scan_candidates()
                log.info("Candidates from browser-pc: %s", names)
                entered = 0
                for name in names:
                    if entered >= slots_available:
                        break
                    mint = await resolve_mint(name, http)
                    if mint is None or mint in seen_mints:
                        if mint in seen_mints:
                            log.debug("SKIP %s — already seen this session", name)
                        continue
                    seen_mints.add(mint)
                    ok = await try_enter(mint, mark_provider, adapter, manager, db_path)
                    if ok:
                        entered += 1

            elapsed = time.monotonic() - cycle_start
            await asyncio.sleep(max(0.0, SCAN_INTERVAL_S - elapsed))


async def monitor_loop(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
) -> None:
    """Check open positions for stops every 30 seconds."""
    while True:
        cycle_start = time.monotonic()
        await monitor_positions(manager, mark_provider, db_path)
        elapsed = time.monotonic() - cycle_start
        await asyncio.sleep(max(0.0, MONITOR_INTERVAL_S - elapsed))


async def main() -> None:
    settings = load_settings()
    db_path = DB_PATH
    await init_db(db_path)

    mark_provider = DexScreenerPriceProvider()
    adapter = PaperExecutionAdapter(price_provider=mark_provider)
    manager = PositionManager(db_path, settings)

    log.info("Paper loop started. Scan every %ds, monitor every %ds.", SCAN_INTERVAL_S, MONITOR_INTERVAL_S)
    await asyncio.gather(
        scan_loop(mark_provider, adapter, manager, db_path),
        monitor_loop(manager, mark_provider, db_path),
    )


if __name__ == "__main__":
    asyncio.run(main())

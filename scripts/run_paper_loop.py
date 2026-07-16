"""Three-layer paper trading loop.

Every 3 minutes:
  1. browser-pc  → scan Profile B DexScreener URL → coin names
  2. DexScreener search API → name → mint address
  3. JupiterClient.get_quote() → entry price
  4. Record paper entry (max 3 open positions, 0.01 SOL each)
  5. Re-mark and close open positions (trailing stop / time stop)

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
from src.core.models import Side, Signal, SignalSource, SignalType, Trade
from src.chain.jupiter import JupiterClient
from src.execution.price_provider import DexScreenerPriceProvider
from src.execution.paper import PaperExecutionAdapter
from src.strategy.position_manager import PositionManager

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
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"

CYCLE_INTERVAL_S = 180
MAX_OPEN_POSITIONS = 3
PAPER_SIZE_SOL = 0.01
TRAILING_STOP_PCT = 0.08
HARD_STOP_PCT = 0.20
TIME_STOP_MINUTES = 30

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
            return mint
    return None


async def try_enter(
    mint: str,
    jupiter: JupiterClient,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
) -> bool:
    """Quote via Jupiter and record a paper entry. Returns True if entry recorded."""
    existing = await manager.get_position(mint, mode="paper")
    if existing is not None:
        return False

    try:
        quote = await jupiter.get_quote(mint, Side.BUY, PAPER_SIZE_SOL)
    except Exception as exc:
        log.debug("Jupiter quote failed for %s: %s", mint, exc)
        return False

    if quote.price_sol is None or quote.price_sol <= 0:
        log.debug("No valid Jupiter price for %s", mint)
        return False

    trade = await adapter.execute_swap(mint, Side.BUY, PAPER_SIZE_SOL)
    if trade is None:
        return False

    await record_trade(db_path, trade)
    dummy_signal = Signal(
        source=SignalSource.MANUAL,
        type=SignalType.NEW_POOL,
        mint_address=mint,
        confidence=1.0,
    )
    position = await manager.open_position(trade, dummy_signal)
    log.info(
        "ENTRY: mint=%s price=%.8f SOL tokens=%.2f",
        mint, quote.price_sol, quote.estimated_out_amount,
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
            continue

        age_min = (datetime.now(UTC) - pos.opened_at).total_seconds() / 60
        entry = pos.entry_price_sol if pos.entry_price_sol > 0 else None
        peak = getattr(pos, "highest_price_sol", entry) or entry

        close_reason = None
        close_price = current_price
        if entry:
            drop_from_entry = (entry - current_price) / entry
            if drop_from_entry >= HARD_STOP_PCT:
                close_reason = "hard_stop"
                close_price = current_price
            elif peak and (peak - current_price) / peak >= TRAILING_STOP_PCT:
                close_reason = "trailing_stop"
                close_price = current_price
        if age_min >= TIME_STOP_MINUTES and close_reason is None:
            close_reason = "time_stop"

        if close_reason:
            trade = await _adapter_close(pos, current_price, close_reason, db_path)
            await manager.close_position(pos.mint_address, current_price, mode="paper")
            log.info(
                "CLOSE [%s]: mint=%s entry=%.8f close=%.8f",
                close_reason, pos.mint_address[:16], pos.entry_price_sol, current_price,
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


async def main() -> None:
    settings = load_settings()
    db_path = DB_PATH
    await init_db(db_path)

    jupiter = JupiterClient()
    mark_provider = DexScreenerPriceProvider()
    adapter = PaperExecutionAdapter(price_provider=mark_provider)
    manager = PositionManager(db_path, settings)

    seen_mints: set[str] = set()
    log.info("Paper loop started. Cycle every %ds. Max positions: %d", CYCLE_INTERVAL_S, MAX_OPEN_POSITIONS)

    async with httpx.AsyncClient() as http:
        while True:
            cycle_start = time.monotonic()
            log.info("--- Cycle start ---")

            await monitor_positions(manager, mark_provider, db_path)

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
                    if mint is None:
                        log.debug("Could not resolve mint for: %s", name)
                        continue
                    if mint in seen_mints:
                        log.debug("Already seen mint: %s", mint)
                        continue
                    seen_mints.add(mint)
                    ok = await try_enter(mint, jupiter, adapter, manager, db_path)
                    if ok:
                        entered += 1

            elapsed = time.monotonic() - cycle_start
            sleep_s = max(0.0, CYCLE_INTERVAL_S - elapsed)
            log.info("Cycle done in %.1fs. Sleeping %.1fs.", elapsed, sleep_s)
            await asyncio.sleep(sleep_s)


if __name__ == "__main__":
    asyncio.run(main())

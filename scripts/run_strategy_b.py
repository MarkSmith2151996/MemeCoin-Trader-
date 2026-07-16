"""Strategy B: Grok X social-hype validated PumpFun paper trading loop.

SCAN (every 60s):
  1. Fetch fresh PumpFun launches from frontend-api-v3
  2. For unseen mints, query Grok x_search for X mention count
  3. Paper enter if mentions >= MIN_MENTIONS and slots available

MONITOR (every 30s):
  4. Re-mark open positions and close on take-profit / hard-stop / time-stop

Run: python scripts/run_strategy_b.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.core.config import load_settings
from src.core.database import init_db, record_trade
from src.core.models import Side, Trade
from src.execution.price_provider import DexScreenerPriceProvider
from src.execution.paper import PaperExecutionAdapter
from src.signals.grok_xsearch import count_unique_mentions
from src.strategy.position_manager import PositionManager

PUMPFUN_COINS_URL = "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&includeNsfw=true"

PAPER_SIZE_SOL = 0.05
MIN_MENTIONS = 10
MENTION_WINDOW_MINUTES = 5
MAX_OPEN = 5
SCAN_INTERVAL = 60
MONITOR_INTERVAL = 30
TAKE_PROFIT_MULT = 2.0
HARD_STOP_MULT = 0.5
TIME_STOP_MINUTES = 10

DB_PATH = Path("data/trades.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/strategy_b.log"),
    ],
)
log = logging.getLogger("strategy_b")

seen_mints: set[str] = set()


async def fetch_coins(client: httpx.AsyncClient) -> list[dict]:
    """Fetch the latest PumpFun coin list."""
    try:
        resp = await client.get(PUMPFUN_COINS_URL, timeout=15.0)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("PumpFun fetch failed: %s", exc)
        return []

    coins = data.get("coins") if isinstance(data, dict) else data if isinstance(data, list) else []
    if not isinstance(coins, list):
        return []
    return [c for c in coins if isinstance(c, dict)]


async def try_enter(
    mint: str,
    ticker: str,
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
) -> bool:
    """Price via DexScreener and record a paper entry."""
    existing = await manager.get_position(mint, mode="paper")
    if existing is not None:
        return False

    price = await mark_provider.get_current_price(mint)
    if price is None or price <= 0:
        log.warning("SKIP %s ticker=%s — no valid DexScreener price", mint[:16], ticker)
        return False

    try:
        trade = await adapter.execute_swap(mint, Side.BUY, PAPER_SIZE_SOL)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s — execute_swap failed: %s", mint[:16], ticker, exc)
        return False

    if trade is None:
        log.warning("SKIP %s ticker=%s — execute_swap returned None", mint[:16], ticker)
        return False

    try:
        await record_trade(db_path, trade)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s — record_trade failed: %s", mint[:16], ticker, exc)
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
        log.warning("SKIP %s ticker=%s — open_position failed: %s", mint[:16], ticker, exc)
        return False

    log.info("ENTRY mint=%s ticker=%s price=%.8f SOL", mint[:16], ticker, price)
    return True


async def monitor_positions(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
) -> None:
    """Check open positions for take-profit, hard-stop, or time-stop."""
    positions = await manager.get_all_open(mode="paper")
    for pos in positions:
        current_price = await mark_provider.get_current_price(pos.mint_address)
        if current_price is None:
            continue

        age_min = (datetime.now(UTC) - pos.opened_at).total_seconds() / 60
        entry = pos.entry_price_sol if pos.entry_price_sol > 0 else current_price

        close_reason = None
        close_price = current_price

        if entry:
            if current_price >= entry * TAKE_PROFIT_MULT:
                close_reason = "take_profit"
                close_price = entry * TAKE_PROFIT_MULT
            elif current_price <= entry * HARD_STOP_MULT:
                close_reason = "hard_stop"

        if age_min >= TIME_STOP_MINUTES and close_reason is None:
            close_reason = "time_stop"

        if close_reason:
            trade = await _adapter_close(pos, close_price, close_reason, db_path)
            await manager.close_position(pos.mint_address, close_price, mode="paper")
            log.info(
                "CLOSE [%s]: mint=%s entry=%.8f close=%.8f",
                close_reason, pos.mint_address[:16], pos.entry_price_sol, current_price,
            )


async def _adapter_close(pos, close_price: float, reason: str, db_path: Path) -> Trade:
    """Record a paper sell trade for a closing position."""
    import uuid

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


async def scan_loop(
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
) -> None:
    """Discover new PumpFun launches and paper-enter those with enough X hype."""
    global seen_mints
    async with httpx.AsyncClient() as http:
        while True:
            cycle_start = time.monotonic()
            log.info("--- Strategy B Scan ---")
            open_positions = await manager.get_all_open(mode="paper")
            log.info("Open positions: %d / %d", len(open_positions), MAX_OPEN)

            if len(open_positions) < MAX_OPEN:
                coins = await fetch_coins(http)
                log.info("PumpFun coins returned: %d", len(coins))
                for coin in coins:
                    mint = coin.get("mint")
                    if not mint or not isinstance(mint, str):
                        continue
                    if mint in seen_mints:
                        continue
                    seen_mints.add(mint)

                    ticker = coin.get("symbol") or coin.get("ticker") or coin.get("name") or mint[:8]
                    mentions = await count_unique_mentions(ticker, mint, minutes=MENTION_WINDOW_MINUTES)
                    if mentions >= MIN_MENTIONS:
                        ok = await try_enter(mint, ticker, mark_provider, adapter, manager, db_path)
                        if ok:
                            log.info("ENTRY mint=%s ticker=%s mentions=%d", mint[:16], ticker, mentions)
                        slots_used = len(await manager.get_all_open(mode="paper"))
                        if slots_used >= MAX_OPEN:
                            break
                    else:
                        log.info("SKIP mint=%s ticker=%s mentions=%d", mint[:16], ticker, mentions)

            elapsed = time.monotonic() - cycle_start
            await asyncio.sleep(max(0.0, SCAN_INTERVAL - elapsed))


async def monitor_loop(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
) -> None:
    """Check open positions for exits every MONITOR_INTERVAL seconds."""
    while True:
        cycle_start = time.monotonic()
        await monitor_positions(manager, mark_provider, db_path)
        elapsed = time.monotonic() - cycle_start
        await asyncio.sleep(max(0.0, MONITOR_INTERVAL - elapsed))


async def main() -> None:
    settings = load_settings()
    db_path = DB_PATH
    await init_db(db_path)

    mark_provider = DexScreenerPriceProvider()
    adapter = PaperExecutionAdapter(price_provider=mark_provider)
    manager = PositionManager(db_path, settings)

    log.info(
        "Strategy B started. Scan every %ds, monitor every %ds.",
        SCAN_INTERVAL, MONITOR_INTERVAL,
    )
    await asyncio.gather(
        scan_loop(mark_provider, adapter, manager, db_path),
        monitor_loop(manager, mark_provider, db_path),
    )


if __name__ == "__main__":
    asyncio.run(main())

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
from src.risk.rugcheck import RugCheckClient
from src.signals.grok_xsearch import get_mentions_with_timestamps
from src.strategy.position_manager import PositionManager

PUMPFUN_COINS_URL = "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&includeNsfw=true"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"

PAPER_SIZE_SOL = 0.05
MIN_MENTIONS = 3
MENTION_WINDOW_MINUTES = 5
MAX_OPEN = 5
SCAN_INTERVAL = 60
MONITOR_INTERVAL = 30
TAKE_PROFIT_MULT = 2.0
HARD_STOP_MULT = 0.70
TIME_STOP_MINUTES = 10

# Screening filters
MAX_AGE_MINUTES = 15
MIN_MCAP_USD = 5_000
MAX_MCAP_USD = 50_000
MIN_TRANSACTIONS = 12
MIN_BUY_SELL_RATIO = 0.6
MIN_VOLUME_USD = 500

# Security gates
MAX_DEV_HOLDINGS_PCT = 10.0
MAX_TOP10_HOLDER_PCT = 30.0
MAX_MCAP_RUGCHECK = 50_000

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
_rugcheck = RugCheckClient(timeout_s=5.0)


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


def _extract_creator_pct(report) -> float | None:
    """Extract creator/dev holding percentage from RugCheck raw payload."""
    raw = report.raw if hasattr(report, "raw") else {}
    creators = raw.get("creators")
    if isinstance(creators, list):
        for c in creators:
            if isinstance(c, dict) and c.get("isCreator"):
                pct = c.get("pct") or c.get("percentage") or c.get("share")
                if pct is not None:
                    try:
                        return float(pct)
                    except (TypeError, ValueError):
                        pass
    return None


async def screen_coin(
    coin: dict,
    http: httpx.AsyncClient,
    rugcheck_client: RugCheckClient,
) -> tuple[bool, str]:
    """Screen a PumpFun coin through age, mcap, DexScreener, and RugCheck gates.

    Returns (pass: bool, diagnostic: str) where diagnostic can be used as:
      SCREEN TICKER (mint): <diagnostic>
    """
    mint = coin.get("mint", "")
    now = datetime.now(UTC)

    # 2a — Age check from PumpFun payload
    created_ts = coin.get("created_timestamp")
    if not isinstance(created_ts, (int, float)) or created_ts <= 0:
        return False, "no created_timestamp"
    age_min = (now.timestamp() - created_ts / 1000) / 60
    if age_min > MAX_AGE_MINUTES:
        return False, f"age={age_min:.1f}m > {MAX_AGE_MINUTES}m"

    # 2a — Market cap check from PumpFun payload
    mcap = coin.get("usd_market_cap")
    if not isinstance(mcap, (int, float)) or mcap <= 0:
        return False, f"age={age_min:.1f}m no usd_market_cap"
    if mcap < MIN_MCAP_USD:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} < ${MIN_MCAP_USD}"
    if mcap > MAX_MCAP_USD:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} > ${MAX_MCAP_USD}"

    # 2b — DexScreener enrichment
    txns = None
    vol = None
    bs_ratio = None
    ds_pair_found = False
    try:
        resp = await http.get(
            "https://api.dexscreener.com/latest/dex/search",
            params={"q": mint},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs") or []
        pair = None
        for p in pairs:
            if not isinstance(p, dict):
                continue
            if p.get("chainId") != "solana":
                continue
            qt = p.get("quoteToken") or {}
            if qt.get("address") == WRAPPED_SOL_MINT or qt.get("symbol") in ("WSOL", "SOL"):
                pair = p
                break

        if pair:
            ds_pair_found = True
            h1 = (pair.get("txns") or {}).get("h1") or {}
            buys = int(h1.get("buys", 0))
            sells = int(h1.get("sells", 0))
            txns = buys + sells
            vol = float((pair.get("volume") or {}).get("h1", 0))
            bs_ratio = buys / max(sells, 1)

            if txns < MIN_TRANSACTIONS:
                return False, (
                    f"age={age_min:.1f}m mcap=${mcap:.0f} txns={txns} vol=${vol:.0f} "
                    f"buys/sells={bs_ratio:.1f} → FAIL txns<{MIN_TRANSACTIONS}"
                )
            if vol < MIN_VOLUME_USD:
                return False, (
                    f"age={age_min:.1f}m mcap=${mcap:.0f} txns={txns} vol=${vol:.0f} "
                    f"buys/sells={bs_ratio:.1f} → FAIL vol<${MIN_VOLUME_USD}"
                )
            if bs_ratio < MIN_BUY_SELL_RATIO:
                return False, (
                    f"age={age_min:.1f}m mcap=${mcap:.0f} txns={txns} vol=${vol:.0f} "
                    f"buys/sells={bs_ratio:.1f} → FAIL buys/sells<{MIN_BUY_SELL_RATIO}"
                )
        else:
            log.warning("DexScreener: no Solana/wSOL pair for %s — skipping txn/volume checks", mint[:8])
    except Exception as exc:
        log.warning("DexScreener search failed for %s: %s", mint[:8], exc)

    # 2c — RugCheck security gates
    try:
        report = await rugcheck_client.fetch_report(mint)
    except Exception as exc:
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} → FAIL RugCheck error: {exc}"
        )

    if report.provider_status in ("timeout", "provider_error", "http_429"):
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} → FAIL RugCheck {report.provider_status}"
        )

    if report.found:
        if report.mint_authority_revoked is False:
            return False, (
                f"age={age_min:.1f}m mcap=${mcap:.0f} → FAIL mint authority not revoked"
            )
        if report.freeze_authority_revoked is False:
            return False, (
                f"age={age_min:.1f}m mcap=${mcap:.0f} → FAIL freeze authority not revoked"
            )
        if report.top_holder_pct is not None and report.top_holder_pct >= MAX_TOP10_HOLDER_PCT:
            return False, (
                f"age={age_min:.1f}m mcap=${mcap:.0f} → FAIL top10 holders={report.top_holder_pct:.1f}% "
                f">= {MAX_TOP10_HOLDER_PCT}%"
            )

        creator_pct = _extract_creator_pct(report)
        if creator_pct is not None and creator_pct > MAX_DEV_HOLDINGS_PCT:
            return False, (
                f"age={age_min:.1f}m mcap=${mcap:.0f} → FAIL dev holds {creator_pct:.1f}% "
                f"> {MAX_DEV_HOLDINGS_PCT}%"
            )
        elif creator_pct is None:
            log.warning("RugCheck: no creator holdings for %s — allowing through", mint[:8])
    else:
        log.warning("RugCheck: no report for %s — allowing through", mint[:8])

    # Build PASS diagnostic
    txn_str = f"txns={txns}" if txns is not None else "txns=N/A"
    vol_str = f"vol=${vol:.0f}" if vol is not None else "vol=N/A"
    bs_str = f"buys/sells={bs_ratio:.1f}" if bs_ratio is not None else "buys/sells=N/A"
    return True, f"age={age_min:.1f}m mcap=${mcap:.0f} {txn_str} {vol_str} {bs_str} → PASS"


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

                    # Screening filters
                    passed, reason = await screen_coin(coin, http, _rugcheck)
                    log.info("SCREEN %s (%s): %s", ticker, mint[:8], reason)
                    if not passed:
                        log.info("FILTERED %s (%s): %s", ticker, mint[:8], reason)
                        continue

                    # Grok mention check (0-5min temporal bucket)
                    launched_at = datetime.fromtimestamp(coin["created_timestamp"] / 1000, tz=UTC)
                    mention_data = await get_mentions_with_timestamps(ticker, mint, launched_at, hours=1)
                    early_mentions = mention_data.get("mentions_0_5min", 0)
                    if early_mentions < MIN_MENTIONS:
                        log.info("SKIP %s — %d early mentions (need %d)", ticker, early_mentions, MIN_MENTIONS)
                        continue

                    ok = await try_enter(mint, ticker, mark_provider, adapter, manager, db_path)
                    if ok:
                        log.info("ENTRY mint=%s ticker=%s mentions=%d", mint[:16], ticker, early_mentions)
                    slots_used = len(await manager.get_all_open(mode="paper"))
                    if slots_used >= MAX_OPEN:
                        break

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

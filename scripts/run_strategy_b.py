"""Strategy B: DexScreener-backed Grok social-hype validated paper trading loop.

Replaces PumpFun API polling with DexScreener token-profiles + boosts + search
endpoints to discover fresh Solana pairs under 15 minutes old.

SCAN (every 60s):
  1. Poll DexScreener endpoints for fresh Solana pairs
  2. Screen through age/mcap/txns/vol/ratio/RugCheck gates
  3. Grok mention check via 0-5min temporal bucket
  4. Paper enter if mentions >= MIN_MENTIONS and slots available

MONITOR (every 30s):
  5. Re-mark open positions and close on take-profit / hard-stop / time-stop

Run:
    python3 scripts/run_strategy_b.py          # normal loop
    timeout 120 python3 scripts/run_strategy_b.py --test  # 2-minute test
"""

from __future__ import annotations

import argparse
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

DEX_PROFILES = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_BOOSTS_LATEST = "https://api.dexscreener.com/token-boosts/latest/v1"
DEX_SEARCH = "https://api.dexscreener.com/latest/dex/search"
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

MAX_AGE_MINUTES = 15
MIN_MCAP_USD = 5_000
MAX_MCAP_USD = 50_000
MIN_TRANSACTIONS = 12
MIN_BUY_SELL_RATIO = 0.6
MIN_VOLUME_USD = 500

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


# ── DexScreener data source ──────────────────────────────────────────

def _dexscreener_to_coin(pair: dict) -> dict | None:
    """Transform a DexScreener pair dict into PumpFun-style coin fields."""
    if not isinstance(pair, dict):
        return None
    if pair.get("chainId") != "solana":
        return None
    base = pair.get("baseToken") or {}
    mint = base.get("address")
    if not mint:
        return None
    created_ms = pair.get("pairCreatedAt")
    if not isinstance(created_ms, (int, float)) or created_ms <= 0:
        return None
    symbol = base.get("symbol", "?")
    return {
        "mint": mint,
        "created_timestamp": int(created_ms),
        "usd_market_cap": pair.get("marketCap") or pair.get("fdv") or 0,
        "symbol": symbol,
        "ticker": symbol,
        "name": base.get("name", symbol),
    }


async def _collect_mint_candidates(http: httpx.AsyncClient) -> set[str]:
    """Collect unique Solana mint addresses from token-profiles, boosts, and search."""
    mints: set[str] = set()

    # Source 1: token-profiles
    try:
        resp = await http.get(DEX_PROFILES, timeout=10.0)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            for p in data:
                if p.get("chainId") == "solana":
                    m = p.get("tokenAddress") or p.get("address")
                    if m:
                        mints.add(m)
    except Exception as exc:
        log.warning("DexScreener profiles fetch failed: %s", exc)

    # Source 2: token-boosts
    try:
        resp = await http.get(DEX_BOOSTS_LATEST, timeout=10.0)
        resp.raise_for_status()
        boosts = resp.json()
        if isinstance(boosts, list):
            for b in boosts:
                if b.get("chainId") == "solana":
                    m = b.get("tokenAddress")
                    if m:
                        mints.add(m)
    except Exception as exc:
        log.warning("DexScreener boosts fetch failed: %s", exc)

    # Source 3: search with broad terms for fresh pairs
    try:
        search_terms = ["pump", "raydium", "cat", "dog", "ai", "pepe", "moon", "solana"]
        for q in search_terms[:4]:
            resp = await http.get(
                DEX_SEARCH,
                params={"q": q},
                timeout=8.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                for p in data.get("pairs", [])[:20]:
                    if p.get("chainId") == "solana":
                        addr = (p.get("baseToken") or {}).get("address")
                        if addr:
                            mints.add(addr)
    except Exception as exc:
        log.warning("DexScreener search fetch failed: %s", exc)

    log.info("Collected %d unique candidate mints", len(mints))
    return mints


async def _enrich_mint(mint: str, http: httpx.AsyncClient) -> dict | None:
    """Fetch single mint's pair data via DexScreener search and return coin dict."""
    try:
        resp = await http.get(
            "https://api.dexscreener.com/latest/dex/search",
            params={"q": mint},
            timeout=8.0,
        )
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs") or []
        for p in pairs:
            if not isinstance(p, dict) or p.get("chainId") != "solana":
                continue
            c = _dexscreener_to_coin(p)
            if c:
                return c
    except Exception as exc:
        log.warning("Enrich failed for %s: %s", mint[:8], exc)
    return None


async def fetch_coins(http: httpx.AsyncClient) -> list[dict]:
    """Collect fresh coins from DexScreener, enriching each via search API."""
    candidate_mints = await _collect_mint_candidates(http)

    enriched: list[dict] = []
    for mint in candidate_mints:
        coin = await _enrich_mint(mint, http)
        if coin:
            enriched.append(coin)
        await asyncio.sleep(0.1)

    enriched.sort(key=lambda c: c.get("created_timestamp", 0), reverse=True)
    log.info("DexScreener: %d candidates enriched to %d coins", len(candidate_mints), len(enriched))
    return enriched


# ── Screening ────────────────────────────────────────────────────────

def _extract_creator_pct(report) -> float | None:
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
    mint = coin.get("mint", "")
    now = datetime.now(UTC)

    created_ts = coin.get("created_timestamp")
    if not isinstance(created_ts, (int, float)) or created_ts <= 0:
        return False, "no created_timestamp"
    age_min = (now.timestamp() - created_ts / 1000) / 60
    if age_min > MAX_AGE_MINUTES:
        return False, f"age={age_min:.1f}m > {MAX_AGE_MINUTES}m"

    mcap = coin.get("usd_market_cap")
    if not isinstance(mcap, (int, float)) or mcap <= 0:
        return False, f"age={age_min:.1f}m no usd_market_cap"
    if mcap < MIN_MCAP_USD:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} < ${MIN_MCAP_USD}"
    if mcap > MAX_MCAP_USD:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} > ${MAX_MCAP_USD}"

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
                    f"buys/sells={bs_ratio:.1f} \u2192 FAIL txns<{MIN_TRANSACTIONS}"
                )
            if vol < MIN_VOLUME_USD:
                return False, (
                    f"age={age_min:.1f}m mcap=${mcap:.0f} txns={txns} vol=${vol:.0f} "
                    f"buys/sells={bs_ratio:.1f} \u2192 FAIL vol<${MIN_VOLUME_USD}"
                )
            if bs_ratio < MIN_BUY_SELL_RATIO:
                return False, (
                    f"age={age_min:.1f}m mcap=${mcap:.0f} txns={txns} vol=${vol:.0f} "
                    f"buys/sells={bs_ratio:.1f} \u2192 FAIL buys/sells<{MIN_BUY_SELL_RATIO}"
                )
        else:
            log.warning("DexScreener: no Solana/wSOL pair for %s", mint[:8])
    except Exception as exc:
        log.warning("DexScreener search failed for %s: %s", mint[:8], exc)

    try:
        report = await rugcheck_client.fetch_report(mint)
    except Exception as exc:
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL RugCheck error: {exc}"
        )

    if report.provider_status in ("timeout", "provider_error", "http_429"):
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL RugCheck {report.provider_status}"
        )

    if report.found:
        if report.mint_authority_revoked is False:
            return False, (
                f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL mint authority not revoked"
            )
        if report.freeze_authority_revoked is False:
            return False, (
                f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL freeze authority not revoked"
            )
        if report.top_holder_pct is not None and report.top_holder_pct >= MAX_TOP10_HOLDER_PCT:
            return False, (
                f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL top10 holders={report.top_holder_pct:.1f}% "
                f">= {MAX_TOP10_HOLDER_PCT}%"
            )

        creator_pct = _extract_creator_pct(report)
        if creator_pct is not None and creator_pct > MAX_DEV_HOLDINGS_PCT:
            return False, (
                f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL dev holds {creator_pct:.1f}% "
                f"> {MAX_DEV_HOLDINGS_PCT}%"
            )
        elif creator_pct is None:
            log.warning("RugCheck: no creator holdings for %s", mint[:8])
    else:
        log.warning("RugCheck: no report for %s", mint[:8])

    txn_str = f"txns={txns}" if txns is not None else "txns=N/A"
    vol_str = f"vol=${vol:.0f}" if vol is not None else "vol=N/A"
    bs_str = f"buys/sells={bs_ratio:.1f}" if bs_ratio is not None else "buys/sells=N/A"
    return True, f"age={age_min:.1f}m mcap=${mcap:.0f} {txn_str} {vol_str} {bs_str} \u2192 PASS"


# ── Entry ────────────────────────────────────────────────────────────

async def try_enter(
    mint: str,
    ticker: str,
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
) -> bool:
    existing = await manager.get_position(mint, mode="paper")
    if existing is not None:
        return False

    price = await mark_provider.get_current_price(mint)
    if price is None or price <= 0:
        log.warning("SKIP %s ticker=%s \u2014 no valid DexScreener price", mint[:16], ticker)
        return False

    try:
        trade = await adapter.execute_swap(mint, Side.BUY, PAPER_SIZE_SOL)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s \u2014 execute_swap failed: %s", mint[:16], ticker, exc)
        return False

    if trade is None:
        log.warning("SKIP %s ticker=%s \u2014 execute_swap returned None", mint[:16], ticker)
        return False

    try:
        await record_trade(db_path, trade)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s \u2014 record_trade failed: %s", mint[:16], ticker, exc)
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
        log.warning("SKIP %s ticker=%s \u2014 open_position failed: %s", mint[:16], ticker, exc)
        return False

    log.info("ENTRY mint=%s ticker=%s price=%.8f SOL", mint[:16], ticker, price)
    return True


# ── Monitoring ───────────────────────────────────────────────────────

async def monitor_positions(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
) -> None:
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


# ── Loops ────────────────────────────────────────────────────────────

async def scan_loop(
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
    test_mode: bool = False,
) -> None:
    global seen_mints
    async with httpx.AsyncClient() as http:
        while True:
            cycle_start = time.monotonic()
            log.info("--- Strategy B Scan ---")
            open_positions = await manager.get_all_open(mode="paper")
            log.info("Open positions: %d / %d", len(open_positions), MAX_OPEN)

            pipe_stats = {
                "total_pairs": 0,
                "age_pass": 0,
                "mcap_pass": 0,
                "txn_pass": 0,
                "grok_reached": 0,
            }

            if len(open_positions) < MAX_OPEN:
                coins = await fetch_coins(http)
                pipe_stats["total_pairs"] = len(coins)
                log.info("DexScreener coins returned: %d", len(coins))

                for coin in coins:
                    mint = coin.get("mint")
                    if not mint or not isinstance(mint, str):
                        continue
                    if mint in seen_mints:
                        continue
                    seen_mints.add(mint)

                    ticker = coin.get("symbol") or coin.get("ticker") or coin.get("name") or mint[:8]

                    now = datetime.now(UTC)
                    created_ts = coin.get("created_timestamp")
                    age_min = None
                    if isinstance(created_ts, (int, float)) and created_ts > 0:
                        age_min = (now.timestamp() - created_ts / 1000) / 60
                    if age_min is not None and age_min <= MAX_AGE_MINUTES:
                        pipe_stats["age_pass"] += 1

                    mcap = coin.get("usd_market_cap")
                    if isinstance(mcap, (int, float)) and MIN_MCAP_USD <= mcap <= MAX_MCAP_USD:
                        pipe_stats["mcap_pass"] += 1

                    passed, reason = await screen_coin(coin, http, _rugcheck)
                    log.info("SCREEN %s (%s): %s", ticker, mint[:8], reason)
                    if not passed:
                        log.info("FILTERED %s (%s): %s", ticker, mint[:8], reason)
                        continue

                    pipe_stats["txn_pass"] += 1

                    launched_at = datetime.fromtimestamp(
                        coin["created_timestamp"] / 1000, tz=UTC,
                    )
                    mention_data = await get_mentions_with_timestamps(
                        ticker, mint, launched_at, hours=1,
                    )
                    pipe_stats["grok_reached"] += 1
                    early_mentions = mention_data.get("mentions_0_5min", 0)
                    if early_mentions < MIN_MENTIONS:
                        log.info(
                            "SKIP %s \u2014 %d early mentions (need %d)",
                            ticker, early_mentions, MIN_MENTIONS,
                        )
                        continue

                    ok = await try_enter(mint, ticker, mark_provider, adapter, manager, db_path)
                    if ok:
                        log.info("ENTRY mint=%s ticker=%s mentions=%d", mint[:16], ticker, early_mentions)
                    slots_used = len(await manager.get_all_open(mode="paper"))
                    if slots_used >= MAX_OPEN:
                        break

            log.info(
                "Pipe: total=%d age_pass=%d mcap_pass=%d txn_pass=%d grok_reached=%d",
                pipe_stats["total_pairs"], pipe_stats["age_pass"],
                pipe_stats["mcap_pass"], pipe_stats["txn_pass"],
                pipe_stats["grok_reached"],
            )
            print(
                f"Pipe: {pipe_stats['total_pairs']} pairs "
                f"\u2192 {pipe_stats['age_pass']} age "
                f"\u2192 {pipe_stats['mcap_pass']} mcap "
                f"\u2192 {pipe_stats['txn_pass']} txn/vol/rug "
                f"\u2192 {pipe_stats['grok_reached']} grok check",
            )
            if pipe_stats["grok_reached"] == 0 and pipe_stats["total_pairs"] > 0:
                print(
                    "  NOTE: Zero coins reached Grok check. "
                    "DexScreener API isn't surfacing sufficiently fresh+qualified pairs. "
                    "browser-pc PumpFun fallback may be needed for higher throughput."
                )

            elapsed = time.monotonic() - cycle_start
            if test_mode:
                log.info("Test mode: single cycle complete")
                return
            await asyncio.sleep(max(0.0, SCAN_INTERVAL - elapsed))


async def monitor_loop(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
) -> None:
    while True:
        cycle_start = time.monotonic()
        await monitor_positions(manager, mark_provider, db_path)
        elapsed = time.monotonic() - cycle_start
        await asyncio.sleep(max(0.0, MONITOR_INTERVAL - elapsed))


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run one cycle and exit (2-minute test)")
    args = parser.parse_args()

    settings = load_settings()
    db_path = DB_PATH
    await init_db(db_path)

    mark_provider = DexScreenerPriceProvider()
    adapter = PaperExecutionAdapter(price_provider=mark_provider)
    manager = PositionManager(db_path, settings)

    log.info(
        "Strategy B started (DexScreener source). Scan every %ds, monitor every %ds.",
        SCAN_INTERVAL, MONITOR_INTERVAL,
    )
    await asyncio.gather(
        scan_loop(mark_provider, adapter, manager, db_path, test_mode=args.test),
        monitor_loop(manager, mark_provider, db_path),
    )


if __name__ == "__main__":
    asyncio.run(main())

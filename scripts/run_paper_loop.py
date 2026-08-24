"""Three-layer paper trading loop with split cycles.

Scan cycle (every 3 min):
  1. browser-pc  → scan Profile B + Profile C DexScreener URLs → coin names
  2. DexScreener search API → name → mint address
  3. JupiterClient.get_quote() → entry price
  4. Record paper entry (max 4 open positions, 0.05 SOL each)

Monitor cycle (every 30s):
  5. Re-mark and close open positions (trailing stop / hard stop / time stop)

Run: python scripts/run_paper_loop.py
"""

# ── Position sizing (MT-522/MT-523) ─────────────────────────────────
# Entry size = POSITION_SIZE_SOL (0.05 SOL since MT-523; was 0.01 SOL) * size_multiplier.
# size_multiplier is always 1.0 in practice:
#   - Saturday halving: * 0.5 when utc_now.weekday() == 5 (-> 0.025 SOL).
#   - Whale conviction sizing (2x/4x/6x in src/signals/whale_tracker.py)
#     is DISABLED since MT-521: the get_whale_signal call block and
#     load_tracked_wallets loading block are commented out, so the
#     multiplier passed to try_enter() never changes from 1.0.
# Sizing is NOT driven by conviction score, liquidity tiers, or gate scores.

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
from src.monitoring.position_snapshots import snapshot_loop
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
# MT-527: Profile C — relaxed second query to widen the candidate pool.
# Same quality floor (mcap 100K-10M, dexIds, buy-side balance at B's 5:3
# txn:buy ratio), but looser age/liq/vol/txn/price-change bounds:
#   30 min - 2 h old, $25K+ liq, $200K+ 24h vol, 200+ 24h txns, +10% 1h.
CAPTURE_URL_C = (
    "https://dexscreener.com/new-pairs/solana?"
    "rankBy=trendingScoreH6&order=desc"
    "&dexIds=pumpswap,raydium"
    "&minLiq=25000&minMarketCap=100000&maxMarketCap=10000000"
    "&minAge=0.5&maxAge=2"
    "&min24HTxns=200&min24HBuys=120&min24HVol=200000"
    "&min1HChg=10&profile=0"
)
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"

SCAN_INTERVAL_S = 180
MONITOR_INTERVAL_S = 10
FAST_MONITOR_INTERVAL_S = 5
FAST_POLL_DROP_PCT = 0.05
CONFIRMATION_DELAY_S = 45
MAX_OPEN_POSITIONS = 4
POSITION_SIZE_SOL = 0.05
MAX_TOP10_HOLDER_PCT = 80.0
TRAILING_STOP_PCT = 3.0
TRAILING_ARM_PCT = 2.0
TAKE_PROFIT_PCT = 60.0
HARD_STOP_PCT = 8.0
TIME_STOP_MINUTES = 30
ENTRY_CONFIRM_WINDOW_S = 90
EARLY_EXIT_GREEN_PCT = 2.0
BLOCKED_UTC_HOURS = frozenset({0, 7, 19, 20})
SATURDAY_SIZE_MULTIPLIER = 0.5
REPEAT_LOSER_COOLDOWN_MINUTES = 120
RUGCHECK_ENABLED = True

DB_PATH = Path("data/trades.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("paper_loop")
logging.getLogger("httpx").setLevel(logging.WARNING)

try:
    from src.signals.whale_tracker import get_whale_signal, load_tracked_wallets  # noqa: F401 — kept for MT-521 re-enable
except ImportError:
    get_whale_signal = None
    load_tracked_wallets = None
    log.warning("whale_tracker sizing unavailable — whale conviction sizing disabled")


def scan_candidates(url: str = CAPTURE_URL) -> list[str]:
    """Call browser-pc, return list of coin names from a Profile DexScreener URL."""
    try:
        resp = requests.post(
            f"{BROWSER_PC_URL}/capture",
            json={"url": url, "wait": 4},
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


async def fetch_entry_metadata(
    mint: str,
    client: httpx.AsyncClient | None = None,
    ticker: str | None = None,
) -> dict[str, object]:
    """Fetch current pair metadata from DexScreener search API for entry logging.

    Returns a dict of entry_* fields, or {} when the API call fails or no
    matching Solana pair is found. Never raises — callers must not block entry
    on metadata capture.
    """
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=10.0) as own_client:
                return await _search_pair_metadata(mint, own_client, ticker=ticker)
        return await _search_pair_metadata(mint, client, ticker=ticker)
    except Exception as exc:
        log.debug("DexScreener search failed for entry metadata %s: %s", mint[:16], exc)
        return {}


async def _search_pair_metadata(
    mint: str,
    client: httpx.AsyncClient,
    *,
    ticker: str | None = None,
) -> dict[str, object]:
    """Search DexScreener for the mint and build the entry metadata dict."""
    resp = await client.get(
        DEXSCREENER_SEARCH_URL,
        params={"q": ticker or mint},
    )
    resp.raise_for_status()
    pairs = resp.json().get("pairs") or []

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        if pair.get("chainId") != "solana":
            continue
        base = pair.get("baseToken") or {}
        if base.get("address") != mint:
            continue
        txns_h24 = (pair.get("txns") or {}).get("h24") or {}
        txns_h1 = (pair.get("txns") or {}).get("h1") or {}
        created_ms = pair.get("pairCreatedAt")
        age_hours = None
        if isinstance(created_ms, (int, float)) and created_ms > 0:
            age_hours = max(0.0, (time.time() * 1000 - created_ms) / 3_600_000)
        dexscreener = {
            "mcap": pair.get("marketCap"),
            "volume": pair.get("volume") or {},
            "txns": pair.get("txns") or {},
            "liquidity": pair.get("liquidity") or {},
            "fdv": pair.get("fdv"),
            "age_hours": age_hours,
            "price_usd": pair.get("priceUsd"),
            "price_change": pair.get("priceChange") or {},
        }
        return {
            "quote_provider": "paper",
            "dexscreener": dexscreener,
            "entry_mcap": pair.get("marketCap"),
            "entry_volume_24h": (pair.get("volume") or {}).get("h24"),
            "entry_volume_1h": (pair.get("volume") or {}).get("h1"),
            "entry_txns_24h": txns_h24.get("buys", 0) + txns_h24.get("sells", 0),
            "entry_txns_1h": txns_h1.get("buys", 0) + txns_h1.get("sells", 0),
            "entry_liquidity_usd": (pair.get("liquidity") or {}).get("usd"),
            "entry_age_hours": age_hours,
            "entry_price_change_1h": (pair.get("priceChange") or {}).get("h1"),
            "entry_price_change_5m": (pair.get("priceChange") or {}).get("m5"),
            "entry_fdv": pair.get("fdv"),
        }
    return {}


async def try_enter(
    mint: str,
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
    size_multiplier: float = 1.0,
    ticker: str | None = None,
    profile: str = "B",
) -> bool:
    """Price via DexScreener and record a paper entry. Returns True if entry recorded."""
    from datetime import UTC, datetime

    from src.core.database import has_recent_losing_close, record_entry_skip

    open_positions = await manager.get_all_open(mode="paper")
    if len(open_positions) + len(pending_entries) >= MAX_OPEN_POSITIONS:
        log.warning(
            "SKIP %s — position cap reached (%d open + %d pending)",
            mint, len(open_positions), len(pending_entries),
        )
        return False

    utc_now = datetime.now(UTC)
    if utc_now.hour in BLOCKED_UTC_HOURS:
        log.warning("SKIP %s — time_gate: UTC hour %d blocked", mint, utc_now.hour)
        try:
            await record_entry_skip(
                db_path, strategy="A", mint_address=mint, ticker=ticker,
                gate="time_gate", reason=f"utc_hour={utc_now.hour}", profile=profile,
            )
        except Exception as exc:
            log.debug("candidate_log write failed (non-fatal): %s", exc)
        return False

    if await has_recent_losing_close(db_path, mint, cooldown_minutes=REPEAT_LOSER_COOLDOWN_MINUTES):
        log.warning("SKIP %s — repeat_loser: mint closed at a loss within the %dh cooldown",
                    mint, REPEAT_LOSER_COOLDOWN_MINUTES // 60)
        try:
            await record_entry_skip(
                db_path, strategy="A", mint_address=mint, ticker=ticker,
                gate="repeat_loser",
                reason=f"losing close within {REPEAT_LOSER_COOLDOWN_MINUTES // 60}h cooldown",
                profile=profile,
            )
        except Exception as exc:
            log.debug("candidate_log write failed (non-fatal): %s", exc)
        return False

    if utc_now.weekday() == 5:
        log.info("Saturday — halving position size for %s", mint)
        size_multiplier *= SATURDAY_SIZE_MULTIPLIER

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
        if result.top_holder_pct is not None and result.top_holder_pct >= MAX_TOP10_HOLDER_PCT:
            log.warning(
                "SKIP %s — top 10 holder concentration %.1f%% >= %.0f%%",
                mint, result.top_holder_pct, MAX_TOP10_HOLDER_PCT,
            )
            return False

    price = await mark_provider.get_current_price(mint)
    if price is None or price <= 0:
        log.warning("SKIP %s — no valid DexScreener price", mint)
        return False

    size_sol = POSITION_SIZE_SOL * size_multiplier
    try:
        trade = await adapter.execute_swap(mint, Side.BUY, size_sol)
    except Exception as exc:
        log.warning("SKIP %s — execute_swap failed: %s", mint, exc)
        return False

    if trade is None:
        log.warning("SKIP %s — execute_swap returned None", mint)
        return False

    try:
        entry_metadata = await fetch_entry_metadata(mint, ticker=ticker)
        merged = dict(trade.metadata or {})
        merged["entry_profile"] = profile
        if entry_metadata:
            merged.update(entry_metadata)
        trade.metadata = merged
    except Exception as exc:
        log.warning("ENTRY METADATA SKIP — mint=%s metadata fetch failed: %s", mint[:16], exc)

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

    log.info("ENTRY: mint=%s price=%.8f SOL size=%.4f SOL profile=%s", mint, price, size_sol, profile)
    send_imessage(
        f"\U0001f7e2 [STRATEGY A] ENTERED {mint[:8]}\n"
        f"Price: {price:.8f} SOL\n"
        f"Size: {size_sol} SOL\n"
        f"Profile: {profile}"
    )
    return True


async def monitor_positions(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
) -> bool:
    """Re-mark open positions and close any that hit stop or time limit.

    Returns True when any still-open position is trading below 95% of its
    entry (danger zone) — the caller polls at FAST_MONITOR_INTERVAL_S then.
    """
    from datetime import UTC, datetime

    danger = False
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
            pct_from_entry = (current_price - entry) / entry
            if drop_from_entry >= HARD_STOP_PCT / 100:
                close_reason = "hard_stop"
                close_price = entry * (1.0 - HARD_STOP_PCT / 100)
            elif pct_from_entry >= TAKE_PROFIT_PCT / 100:
                close_reason = "take_profit"
                close_price = current_price
            elif (
                peak > entry * (1.0 + TRAILING_ARM_PCT / 100)
                and (peak - current_price) / peak >= TRAILING_STOP_PCT / 100
            ):
                close_reason = "trailing_stop"
                close_price = current_price
        if (
            close_reason is None
            and age_min * 60 >= ENTRY_CONFIRM_WINDOW_S
            and peak <= entry * (1.0 + EARLY_EXIT_GREEN_PCT / 100)
        ):
            close_reason = "early_exit_no_green"
            close_price = current_price
        if age_min >= TIME_STOP_MINUTES and close_reason is None:
            close_reason = "time_stop"

        if close_reason:
            peak = peak_prices.get(pos.mint_address)
            peak_prices.pop(pos.mint_address, None)
            trade = await _adapter_close(pos, close_price, close_reason, db_path)
            await manager.close_position(pos.mint_address, close_price, mode="paper", peak_price_sol=peak)
            pnl_pct = ((close_price - pos.entry_price_sol) / pos.entry_price_sol) * 100 if pos.entry_price_sol else 0.0
            peak_pnl_pct = ((peak - pos.entry_price_sol) / pos.entry_price_sol) * 100 if pos.entry_price_sol else 0.0
            log.info(
                "CLOSE [%s]: mint=%s entry=%.8f peak=%.8f close=%.8f",
                close_reason, pos.mint_address[:16], pos.entry_price_sol, peak, close_price,
            )
            send_imessage(
                f"\U0001f534 [STRATEGY A] CLOSED {pos.mint_address[:8]}\n"
                f"Entry: {pos.entry_price_sol:.8f} \u2192 Close: {close_price:.8f}\n"
                f"PnL: {pnl_pct:+.1f}%  Peak: {peak_pnl_pct:+.1f}%\n"
                f"Reason: {close_reason}"
            )
        elif current_price < entry * (1.0 - FAST_POLL_DROP_PCT):
            danger = True

    return danger


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
pending_entries: dict[str, dict] = {}  # mint -> {"price": screen_price, "time": t, "ticker": name, "size_multiplier": float}
pending_confirmation_tasks: set[asyncio.Task[None]] = set()


async def confirm_pending_entry(
    mint: str,
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
    *,
    confirmation_delay_s: float = CONFIRMATION_DELAY_S,
) -> None:
    """Confirm a pending candidate independently of the scan interval."""
    await asyncio.sleep(confirmation_delay_s)
    pend = pending_entries.pop(mint, None)
    if pend is None:
        return

    age = time.time() - pend["time"]
    current_price = await mark_provider.get_current_price(mint)
    if current_price is None or current_price < pend["price"]:
        log.info(
            "SKIP [confirmation_fail]: mint=%s ticker=%s age=%.0fs screen=%.8f current=%s",
            mint[:16], pend["ticker"], age, pend["price"],
            current_price if current_price is not None else "N/A",
        )
        return

    log.info(
        "CONFIRM: mint=%s ticker=%s age=%.0fs screen=%.8f current=%.8f profile=%s",
        mint[:16], pend["ticker"], age, pend["price"], current_price,
        pend.get("profile", "B"),
    )
    ok = await try_enter(
        mint, mark_provider, adapter, manager, db_path,
        pend.get("size_multiplier", 1.0), ticker=pend["ticker"],
        profile=pend.get("profile", "B"),
    )
    if ok:
        log.info("ENTRY [confirmed]: mint=%s ticker=%s", mint[:16], pend["ticker"])
    else:
        log.warning(
            "SKIP [confirmation_entry_fail]: mint=%s ticker=%s - try_enter failed",
            mint[:16], pend["ticker"],
        )


def schedule_pending_confirmation(
    mint: str,
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
) -> None:
    """Keep the confirmation task alive until it consumes its pending entry."""
    task = asyncio.create_task(
        confirm_pending_entry(mint, mark_provider, adapter, manager, db_path),
    )
    pending_confirmation_tasks.add(task)
    task.add_done_callback(pending_confirmation_tasks.discard)


async def scan_loop(
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
    tracked_wallets: list | None = None,
) -> None:
    """Discover and enter new candidates every 3 minutes."""
    global seen_mints
    if tracked_wallets is None:
        tracked_wallets = []
    async with httpx.AsyncClient() as http:
        while True:
            cycle_start = time.monotonic()

            log.info("--- Scan cycle ---")
            log.info("whale tracker disabled — re-enable when Helius integration is built into entry pipeline.")
            open_positions = await manager.get_all_open(mode="paper")
            slots_available = MAX_OPEN_POSITIONS - len(open_positions) - len(pending_entries)
            log.info("Open positions: %d / %d", len(open_positions), MAX_OPEN_POSITIONS)

            if slots_available > 0:
                scheduled = 0
                profile_urls: list[tuple[str, str]] = [
                    ("B", CAPTURE_URL),
                    ("C", CAPTURE_URL_C),
                ]
                for profile, capture_url in profile_urls:
                    names = scan_candidates(capture_url)
                    log.info("Candidates from browser-pc (Profile %s): %s", profile, names)
                    for name in names:
                        if scheduled >= slots_available:
                            break
                        mint = await resolve_mint(name, http)
                        if mint is None or mint in seen_mints:
                            if mint in seen_mints:
                                log.debug("SKIP %s — already seen this session (Profile %s)", name, profile)
                            continue
                        seen_mints.add(mint)

                        size_multiplier = 1.0
                        # MT-521: whale tracker disabled — ~12 Helius /v0/addresses/{addr}/transactions
                        # calls per 3-minute cycle (~1M credits/day), zero decisions influenced across
                        # 2,200+ trades. Code kept intact for re-enable when Helius integration is
                        # built into the entry pipeline:
                        # if get_whale_signal is not None and mint is not None:
                        #     try:
                        #         whale_data = await get_whale_signal(mint, tracked_wallets, http)
                        #         whale_count = whale_data.get("whale_count", 0)
                        #         size_multiplier = whale_data.get("size_multiplier", 1.0)
                        #         if whale_count > 0:
                        #             log.info("🐋 WHALE SIGNAL: %d whale(s) in %s — size multiplier: %.1fx", whale_count, name, size_multiplier)
                        #     except Exception as e:
                        #         log.debug("Whale check failed (non-fatal): %s", e)

                        screen_price = await mark_provider.get_current_price(mint)
                        if screen_price is None or screen_price <= 0:
                            log.warning("SKIP %s — no valid DexScreener price for pending", name)
                            continue
                        pending_entries[mint] = {
                            "price": screen_price,
                            "time": time.time(),
                            "ticker": name,
                            "size_multiplier": size_multiplier,
                            "profile": profile,
                        }
                        log.info(
                            "PENDING: mint=%s ticker=%s price=%.8f SOL profile=%s — will confirm in 45s",
                            mint[:16], name, screen_price, profile,
                        )
                        schedule_pending_confirmation(mint, mark_provider, adapter, manager, db_path)
                        scheduled += 1

            elapsed = time.monotonic() - cycle_start
            await asyncio.sleep(max(0.0, SCAN_INTERVAL_S - elapsed))


async def monitor_loop(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
) -> None:
    """Check open positions for stops every 30 seconds.

    Polls at FAST_MONITOR_INTERVAL_S while any open position trades below
    95% of its entry so the hard stop is caught near its -10% trigger.
    """
    while True:
        cycle_start = time.monotonic()
        danger = await monitor_positions(manager, mark_provider, db_path)
        elapsed = time.monotonic() - cycle_start
        interval = FAST_MONITOR_INTERVAL_S if danger else MONITOR_INTERVAL_S
        await asyncio.sleep(max(0.0, interval - elapsed))


async def main() -> None:
    settings = load_settings()
    db_path = DB_PATH
    await init_db(db_path)

    mark_provider = DexScreenerPriceProvider()
    adapter = PaperExecutionAdapter(price_provider=mark_provider)
    manager = PositionManager(db_path, settings)

    # MT-521: tracked wallet loading disabled alongside the whale tracker (no Helius calls
    # in load_tracked_wallets itself, but no reason to load 50 wallets while unused).
    # tracked_wallets: list = []
    # if load_tracked_wallets is not None:
    #     try:
    #         tracked_wallets = load_tracked_wallets()
    #         log.info("Loaded %d tracked whale wallets", len(tracked_wallets))
    #     except Exception:
    #         log.warning("Failed to load tracked wallets — whale sizing disabled")
    tracked_wallets: list = []

    log.info("Paper loop started. Scan every %ds, monitor every %ds.", SCAN_INTERVAL_S, MONITOR_INTERVAL_S)
    await asyncio.gather(
        scan_loop(mark_provider, adapter, manager, db_path, tracked_wallets),
        monitor_loop(manager, mark_provider, db_path),
        snapshot_loop(manager, mark_provider, db_path),
    )


if __name__ == "__main__":
    asyncio.run(main())

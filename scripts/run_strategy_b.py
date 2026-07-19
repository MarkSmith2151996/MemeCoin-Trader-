"""Strategy B: browser-pc backed Grok social-hype validated paper trading loop.

Uses browser-pc to scan DexScreener new-pairs page for fresh Solana pairs
under 15 minutes old across all DEXs.

SCAN (every 60s):
  1. browser-pc captures DexScreener new-pairs URL → rows
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
from src.monitoring.alerts import send_imessage
from src.risk.rugcheck import RugCheckClient
from src.signals.grok_xsearch import get_mentions_with_timestamps, count_influencer_mentions
from src.strategy.position_manager import PositionManager

WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"

BROWSER_PC_URL = "http://localhost:8099"
STRATEGY_B_DEXSCREENER_URL = (
    "https://dexscreener.com/new-pairs/solana"
    "?rankBy=trendingScoreH6&order=desc"
    "&minLiq=1000&minMarketCap=5000&maxMarketCap=50000&maxAge=0.25"
)
BROWSER_PC_WAIT_SECONDS = 8

PAPER_SIZE_SOL = 0.05
MIN_MENTIONS = 3
MENTION_WINDOW_MINUTES = 5
MAX_OPEN = 5
SCAN_INTERVAL = 60
MONITOR_INTERVAL = 30
TAKE_PROFIT_MULT = 2.0
HARD_STOP_MULT = 0.70
TIME_STOP_MINUTES = 10

# Mode flags
REQUIRE_MENTIONS = False      # Set False to skip Grok entirely (on-chain only)
USE_INFLUENCER_MENTIONS = False  # Set True to use influencer-weighted mentions instead of raw count

MAX_AGE_MINUTES = 15
MIN_MCAP_USD = 5_000
MAX_MCAP_USD = 50_000
MIN_BUY_SELL_RATIO = 0.6
MIN_VOLUME_USD = 500

MAX_DEV_HOLDINGS_PCT = 10.0
MAX_TOP10_HOLDER_PCT = 30.0
MAX_MCAP_RUGCHECK = 50_000

# Rug signal filters
MIN_VOLUME_TO_MCAP_RATIO = 0.005
MAX_VOLUME_TO_MCAP_RATIO = 50.0
MIN_FEES_SOL_PER_15K_MCAP = 0.3

# Paper-mode holder concentration tiers (warn_pct, hard_reject_pct)
HOLDER_TIERS = [
    (2, 30.0, 80.0),    # 0-2 min: warn at 30%, hard reject at 80%
    (5, 30.0, 65.0),    # 2-5 min: warn at 30%, hard reject at 65%
    (10, 30.0, 50.0),   # 5-10 min: warn at 30%, hard reject at 50%
    (999, 30.0, 40.0),  # 10-15 min: hard reject at 40%
]

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

try:
    from src.signals.whale_tracker import get_whale_signal, load_tracked_wallets
except ImportError:
    get_whale_signal = None
    load_tracked_wallets = None
    log.warning("whale_tracker sizing unavailable — whale conviction sizing disabled")

seen_mints: set[str] = set()
_rugcheck = RugCheckClient(timeout_s=5.0)


# ── Gate helpers ────────────────────────────────────────────────────

def _age_adjusted_min_txns(age_min: float) -> int:
    """Age-aware minimum transaction threshold for paper mode."""
    if age_min < 1.0:
        return 3
    if age_min < 3.0:
        return 5
    if age_min < 5.0:
        return 8
    if age_min < 10.0:
        return 12
    return 16  # 10-15 minutes


def _age_holder_tier(age_min: float) -> tuple[float, float]:
    """Return (warn_pct, hard_reject_pct) for given age in minutes."""
    for max_age, warn, hard in HOLDER_TIERS:
        if age_min < max_age:
            return warn, hard
    return 30.0, 40.0


# ── browser-pc data source ──────────────────────────────────────────

def _parse_usd_string(s: str) -> float:
    """Parse a USD string like '$12.4K' or '$1.5M' to float."""
    if not isinstance(s, str):
        return 0.0
    s = s.replace("$", "").replace(",", "").strip().upper()
    if not s:
        return 0.0
    multiplier = 1.0
    if s.endswith("K"):
        multiplier = 1_000
        s = s[:-1]
    elif s.endswith("M"):
        multiplier = 1_000_000
        s = s[:-1]
    elif s.endswith("B"):
        multiplier = 1_000_000_000
        s = s[:-1]
    try:
        return float(s) * multiplier
    except (TypeError, ValueError):
        return 0.0


def _parse_age_minutes(s: str) -> float:
    """Parse an age string like '3m', '15m', '1h', '30s' to minutes."""
    if not isinstance(s, str):
        return 999.0
    s = s.strip().lower()
    if not s:
        return 999.0
    if s.endswith("h"):
        try:
            return float(s[:-1]) * 60
        except (TypeError, ValueError):
            return 999.0
    if s.endswith("m"):
        try:
            return float(s[:-1])
        except (TypeError, ValueError):
            return 999.0
    if s.endswith("s"):
        try:
            return float(s[:-1]) / 60
        except (TypeError, ValueError):
            return 999.0
    try:
        return float(s)
    except (TypeError, ValueError):
        return 999.0


def parse_row(row: dict) -> dict:
    """Map a browser-pc row to a coin dict for screen_coin()."""
    ticker = row.get("name") or row.get("symbol") or "?"
    # market_cap_usd is a float from browser-pc; mcap is a string from older format
    if row.get("market_cap_usd") is not None:
        mcap = float(row["market_cap_usd"])
    else:
        mcap = _parse_usd_string(row.get("mcap", "0"))
    # age: prefer age_minutes float, fallback to parsing age string
    if row.get("age_minutes") is not None:
        age_min = float(row["age_minutes"])
    else:
        age_min = _parse_age_minutes(row.get("age", "0"))
    now = datetime.now(UTC)
    created_ts = int((now.timestamp() - age_min * 60) * 1000)
    buys = int(row.get("buys", 0) or 0)
    sells = int(row.get("sells", 0) or 0)
    volume = float(row.get("volume_usd", 0) or 0)
    txns = buys + sells
    bs_ratio = buys / max(sells, 1)
    return {
        "ticker": ticker,
        "usd_market_cap": mcap,
        "created_timestamp": max(created_ts, 0),
        "volume": volume,
        "txns": txns,
        "buy_sell_ratio": bs_ratio,
        "liquidity": float(row.get("liquidity_usd", 0) or 0),
    }


async def fetch_candidates(http: httpx.AsyncClient) -> list[dict]:
    """Fetch fresh coins via browser-pc + DexScreener new-pairs URL."""
    try:
        resp = await http.post(
            f"{BROWSER_PC_URL}/capture",
            json={"url": STRATEGY_B_DEXSCREENER_URL, "wait": BROWSER_PC_WAIT_SECONDS},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("candidates", data.get("rows", []))
        log.info("browser-pc returned %d rows", len(rows))
        return rows
    except Exception as e:
        log.warning("browser-pc error: %s", e)
        return []


async def resolve_mint(name: str, http: httpx.AsyncClient) -> str | None:
    """Search DexScreener for the coin name, return Solana mint address or None."""
    try:
        resp = await http.get(
            "https://api.dexscreener.com/latest/dex/search",
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
            log.info("RESOLVED %s \u2192 %s", name, mint)
            return mint
    return None


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
) -> tuple[bool, str, dict]:
    """Screen a coin through all gates.

    Returns (passed, reason, gates) where gates is a dict mapping
    gate names to bool.  In paper mode low_fees is a warning only --
    it does not block passage.
    """
    mint = coin.get("mint", "")
    now = datetime.now(UTC)
    gates = {
        "age_pass": False,
        "mcap_pass": False,
        "txn_pass": False,
        "volume_pass": False,
        "vol_mcap_pass": False,
        "low_fees_pass": True,
        "low_fees_warn": False,
        "buy_sell_pass": False,
        "rugcheck_pass": False,
        "holder_pass": False,
        "creator_pass": True,
    }

    created_ts = coin.get("created_timestamp")
    if not isinstance(created_ts, (int, float)) or created_ts <= 0:
        return False, "no created_timestamp", gates
    age_min = (now.timestamp() - created_ts / 1000) / 60
    if age_min > MAX_AGE_MINUTES:
        return False, f"age={age_min:.1f}m > {MAX_AGE_MINUTES}m", gates
    gates["age_pass"] = True

    mcap = coin.get("usd_market_cap")
    if not isinstance(mcap, (int, float)) or mcap <= 0:
        return False, f"age={age_min:.1f}m no usd_market_cap", gates
    if mcap < MIN_MCAP_USD:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} < ${MIN_MCAP_USD}", gates
    if mcap > MAX_MCAP_USD:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} > ${MAX_MCAP_USD}", gates
    gates["mcap_pass"] = True

    txns = None
    vol = None
    bs_ratio = None
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
            h1 = (pair.get("txns") or {}).get("h1") or {}
            buys = int(h1.get("buys", 0))
            sells = int(h1.get("sells", 0))
            txns = buys + sells
            vol = float((pair.get("volume") or {}).get("h1", 0))
            bs_ratio = buys / max(sells, 1)

            min_txns = _age_adjusted_min_txns(age_min)
            if txns >= min_txns:
                gates["txn_pass"] = True

            if vol >= MIN_VOLUME_USD:
                gates["volume_pass"] = True

            if mcap > 0 and vol > 0:
                vol_ratio = vol / mcap
                if MIN_VOLUME_TO_MCAP_RATIO <= vol_ratio <= MAX_VOLUME_TO_MCAP_RATIO:
                    gates["vol_mcap_pass"] = True

            estimated_fees = txns * 0.001
            expected_min_fees = (mcap / 15000) * MIN_FEES_SOL_PER_15K_MCAP
            if estimated_fees < expected_min_fees:
                gates["low_fees_pass"] = False
                gates["low_fees_warn"] = True

            if bs_ratio >= MIN_BUY_SELL_RATIO:
                gates["buy_sell_pass"] = True
        else:
            log.warning("DexScreener: no Solana/wSOL pair for %s", mint[:8])
    except Exception as exc:
        log.warning("DexScreener search failed for %s: %s", mint[:8], exc)

    try:
        report = await rugcheck_client.fetch_report(mint)
    except Exception as exc:
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL RugCheck error: {exc}"
        ), gates

    if report.provider_status in ("timeout", "provider_error", "http_429"):
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL RugCheck {report.provider_status}"
        ), gates

    if report.found:
        if report.mint_authority_revoked is not False and report.freeze_authority_revoked is not False:
            gates["rugcheck_pass"] = True

        if report.freeze_authority_revoked is False:
            gates["rugcheck_pass"] = False

        if report.mint_authority_revoked is False:
            gates["rugcheck_pass"] = False

        warn_holder, hard_holder = _age_holder_tier(age_min)
        if report.top_holder_pct is not None:
            if report.top_holder_pct < hard_holder:
                gates["holder_pass"] = True

        creator_pct = _extract_creator_pct(report)
        if creator_pct is not None and creator_pct > MAX_DEV_HOLDINGS_PCT:
            gates["creator_pass"] = False
        elif creator_pct is None:
            log.warning("RugCheck: no creator holdings for %s", mint[:8])
    else:
        log.warning("RugCheck: no report for %s", mint[:8])

    # Build reason string
    fail_reasons = []
    if not gates["txn_pass"] and txns is not None:
        min_txns = _age_adjusted_min_txns(age_min)
        fail_reasons.append(f"txns={txns}<{min_txns}")
    if not gates["volume_pass"] and vol is not None:
        fail_reasons.append(f"vol=${vol:.0f}<${MIN_VOLUME_USD}")
    if not gates["vol_mcap_pass"] and vol is not None and mcap > 0 and vol > 0:
        vol_ratio = vol / mcap
        label = "dead_volume" if vol_ratio < MIN_VOLUME_TO_MCAP_RATIO else "wash_trading"
        fail_reasons.append(label)
    if gates["low_fees_warn"] and txns is not None:
        estimated_fees = txns * 0.001
        fail_reasons.append(f"low_fees_warn({estimated_fees:.3f}SOL)")
    if not gates["buy_sell_pass"] and bs_ratio is not None:
        fail_reasons.append(f"buys/sells={bs_ratio:.1f}<{MIN_BUY_SELL_RATIO}")
    if not gates["holder_pass"] and report.found and report.top_holder_pct is not None:
        _, hard_holder = _age_holder_tier(age_min)
        fail_reasons.append(f"top10={report.top_holder_pct:.1f}%>={hard_holder}%")
    if not gates["creator_pass"]:
        fail_reasons.append("dev_holdings")
    if not gates["rugcheck_pass"] and report.found:
        if report.mint_authority_revoked is False:
            fail_reasons.append("mint_authority")
        if report.freeze_authority_revoked is False:
            fail_reasons.append("freeze_authority")

    # Overall pass = all critical gates + not rug-failed
    all_pass = (
        gates["age_pass"]
        and gates["mcap_pass"]
        and gates["txn_pass"]
        and gates["volume_pass"]
        and gates["vol_mcap_pass"]
        and gates["buy_sell_pass"]
        and gates["rugcheck_pass"]
        and gates["holder_pass"]
        and gates["creator_pass"]
    )

    txn_str = f"txns={txns}" if txns is not None else "txns=N/A"
    vol_str = f"vol=${vol:.0f}" if vol is not None else "vol=N/A"
    bs_str = f"buys/sells={bs_ratio:.1f}" if bs_ratio is not None else "buys/sells=N/A"
    extra = ""
    if mcap > 0 and vol is not None and vol > 0:
        extra += f"vol/mcap={vol / mcap:.3f} "
    if txns is not None and txns > 0:
        extra += f"est_fees={txns * 0.001:.3f}SOL "
    flags = " ".join(fail_reasons)
    if all_pass:
        reason_str = f"age={age_min:.1f}m mcap=${mcap:.0f} {txn_str} {vol_str} {extra}{bs_str} \u2192 PASS"
    else:
        reason_str = f"age={age_min:.1f}m mcap=${mcap:.0f} {txn_str} {vol_str} {extra}{bs_str} \u2192 FAIL {flags}"
    return all_pass, reason_str, gates


# ── Entry ────────────────────────────────────────────────────────────

async def try_enter(
    mint: str,
    ticker: str,
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
    size_multiplier: float = 1.0,
) -> bool:
    existing = await manager.get_position(mint, mode="paper")
    if existing is not None:
        return False

    price = await mark_provider.get_current_price(mint)
    if price is None or price <= 0:
        log.warning("SKIP %s ticker=%s \u2014 no valid DexScreener price", mint[:16], ticker)
        return False

    size_sol = PAPER_SIZE_SOL * size_multiplier
    try:
        trade = await adapter.execute_swap(mint, Side.BUY, size_sol)
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

    log.info("ENTRY mint=%s ticker=%s price=%.8f SOL size=%.4f SOL", mint[:16], ticker, price, size_sol)
    send_imessage(
        f"\U0001f7e2 [STRATEGY B] ENTERED {ticker}\n"
        f"Price: {price:.8f} SOL\n"
        f"Size: {size_sol} SOL"
    )
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
            pnl_pct = ((current_price - pos.entry_price_sol) / pos.entry_price_sol) * 100 if pos.entry_price_sol else 0.0
            log.info(
                "CLOSE [%s]: mint=%s entry=%.8f close=%.8f",
                close_reason, pos.mint_address[:16], pos.entry_price_sol, current_price,
            )
            send_imessage(
                f"\U0001f534 [STRATEGY B] CLOSED {pos.mint_address[:8]}\n"
                f"Entry: {pos.entry_price_sol:.8f} \u2192 Close: {current_price:.8f}\n"
                f"PnL: {pnl_pct:+.1f}%\n"
                f"Reason: {close_reason}"
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
    tracked_wallets: list | None = None,
    test_mode: bool = False,
) -> None:
    global seen_mints
    if tracked_wallets is None:
        tracked_wallets = []
    async with httpx.AsyncClient() as http:
        while True:
            cycle_start = time.monotonic()
            log.info("--- Strategy B Scan ---")
            open_positions = await manager.get_all_open(mode="paper")
            log.info("Open positions: %d / %d", len(open_positions), MAX_OPEN)

            detailed = {
                "total": 0,
                "age_pass": 0,
                "mcap_pass": 0,
                "txn_pass": 0,
                "volume_pass": 0,
                "vol_mcap_pass": 0,
                "low_fees_warn_or_pass": 0,
                "buy_sell_pass": 0,
                "rugcheck_pass": 0,
                "holder_pass": 0,
                "full_screen_pass": 0,
                "entry_attempts": 0,
                "entered": 0,
            }
            main_blocker_count: dict[str, int] = {}

            if len(open_positions) < MAX_OPEN:
                rows = await fetch_candidates(http)
                detailed["total"] = len(rows)

                for row in rows:
                    coin = parse_row(row)
                    ticker = coin["ticker"]

                    mint = await resolve_mint(ticker, http)
                    if not mint or mint in seen_mints:
                        continue
                    seen_mints.add(mint)
                    coin["mint"] = mint

                    passed, reason, gates = await screen_coin(coin, http, _rugcheck)
                    log.info("SCREEN %s (%s): %s", ticker, mint[:8], reason)

                    # Aggregate per-gate diagnostics
                    for gk in ("age_pass", "mcap_pass", "txn_pass", "volume_pass",
                               "vol_mcap_pass", "buy_sell_pass", "rugcheck_pass",
                               "holder_pass"):
                        if gates.get(gk):
                            detailed[gk] += 1
                    if gates.get("low_fees_pass") or gates.get("low_fees_warn"):
                        detailed["low_fees_warn_or_pass"] += 1

                    if not passed:
                        # Identify the main blocker from the reason string
                        blockers = ["txn_pass", "volume_pass", "vol_mcap_pass",
                                    "low_fees_pass", "buy_sell_pass", "rugcheck_pass",
                                    "holder_pass", "creator_pass"]
                        for bk in blockers:
                            if not gates.get(bk, True):
                                main_blocker_count[bk] = main_blocker_count.get(bk, 0) + 1
                                break
                        continue

                    detailed["full_screen_pass"] += 1

                    if REQUIRE_MENTIONS:
                        launched_at = datetime.fromtimestamp(
                            coin["created_timestamp"] / 1000, tz=UTC,
                        )
                        if USE_INFLUENCER_MENTIONS:
                            infl_data = await count_influencer_mentions(
                                ticker, mint, launched_at, window_minutes=15,
                            )
                            pipe_stats["grok_reached"] += 1
                            infl_count = infl_data["total"]
                            if infl_count < 1:
                                log.info(
                                    "SKIP %s \u2014 %d influencer mentions (need >= 1)",
                                    ticker, infl_count,
                                )
                                continue
                            log.info(
                                "PASS %s \u2014 %d influencer mentions (accounts: %s)",
                                ticker, infl_count, infl_data["accounts_mentioned"],
                            )
                        else:
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
                            log.info(
                                "PASS %s \u2014 %d early mentions", ticker, early_mentions,
                            )
                    else:
                        log.info(
                            "MENTIONS SKIPPED (on-chain only mode) \u2014 proceeding to entry check for %s",
                            ticker,
                        )

                    size_multiplier = 1.0
                    if get_whale_signal is not None:
                        try:
                            whale_data = await get_whale_signal(mint, tracked_wallets, http)
                            whale_count = whale_data.get("whale_count", 0)
                            size_multiplier = whale_data.get("size_multiplier", 1.0)
                            if whale_count > 0:
                                log.info("🐋 WHALE SIGNAL: %d whale(s) in %s \u2014 size multiplier: %.1fx", whale_count, ticker, size_multiplier)
                        except Exception as e:
                            log.debug("Whale check failed (non-fatal): %s", e)

                    detailed["entry_attempts"] += 1
                    ok = await try_enter(mint, ticker, mark_provider, adapter, manager, db_path, size_multiplier)
                    if ok:
                        detailed["entered"] += 1
                        log.info("ENTRY mint=%s ticker=%s", mint[:16], ticker)
                    slots_used = len(await manager.get_all_open(mode="paper"))
                    if slots_used >= MAX_OPEN:
                        break

            main_blocker = max(main_blocker_count, key=main_blocker_count.get) if main_blocker_count else "none"
            log.info(
                "Gates: total=%d age=%d mcap=%d txns=%d vol=%d vol/mcap=%d low_fees~=%d "
                "b/s=%d rugcheck=%d holder=%d full_pass=%d entry_attempts=%d entered=%d "
                "main_blocker=%s",
                detailed["total"], detailed["age_pass"], detailed["mcap_pass"],
                detailed["txn_pass"], detailed["volume_pass"], detailed["vol_mcap_pass"],
                detailed["low_fees_warn_or_pass"], detailed["buy_sell_pass"],
                detailed["rugcheck_pass"], detailed["holder_pass"],
                detailed["full_screen_pass"], detailed["entry_attempts"],
                detailed["entered"], main_blocker,
            )
            print(
                f"Gates: {detailed['total']} pairs \u2192 "
                f"{detailed['age_pass']} age \u2192 "
                f"{detailed['mcap_pass']} mcap \u2192 "
                f"{detailed['txn_pass']} txns \u2192 "
                f"{detailed['volume_pass']} vol \u2192 "
                f"{detailed['vol_mcap_pass']} vol/mcap \u2192 "
                f"{detailed['low_fees_warn_or_pass']} low_fees~ \u2192 "
                f"{detailed['buy_sell_pass']} b/s \u2192 "
                f"{detailed['rugcheck_pass']} rugcheck \u2192 "
                f"{detailed['holder_pass']} holder \u2192 "
                f"{detailed['full_screen_pass']} full_pass \u2192 "
                f"{detailed['entry_attempts']} entry_attempts \u2192 "
                f"{detailed['entered']} entered "
                f"(blocker: {main_blocker})",
            )
            if detailed["full_screen_pass"] == 0 and detailed["total"] > 0:
                print(
                    "  NOTE: Zero coins passed full screen. "
                    "browser-pc isn't surfacing sufficiently qualified candidates."
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

    tracked_wallets: list = []
    if load_tracked_wallets is not None:
        try:
            tracked_wallets = load_tracked_wallets()
            log.info("Loaded %d tracked whale wallets", len(tracked_wallets))
        except Exception:
            log.warning("Failed to load tracked wallets — whale sizing disabled")

    if not REQUIRE_MENTIONS:
        mode_label = "ON-CHAIN ONLY (Grok disabled)"
    elif USE_INFLUENCER_MENTIONS:
        mode_label = "INFLUENCER MENTIONS >= 1 in first 15min"
    else:
        mode_label = f"RAW MENTIONS >= {MIN_MENTIONS} in first {MENTION_WINDOW_MINUTES}min"

    log.info(
        "Strategy B started \u2014 mode: %s (browser-pc, all Solana DEXs, 0-15min). Scan every %ds, monitor every %ds.",
        mode_label, SCAN_INTERVAL, MONITOR_INTERVAL,
    )
    await asyncio.gather(
        scan_loop(mark_provider, adapter, manager, db_path, tracked_wallets=tracked_wallets, test_mode=args.test),
        monitor_loop(manager, mark_provider, db_path),
    )


if __name__ == "__main__":
    asyncio.run(main())

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

# ── Position sizing (MT-522/MT-524) ─────────────────────────────────
# Entry size = PAPER_SIZE_SOL (0.05 SOL) * size_multiplier.
# size_multiplier is always 1.0 in practice:
#   - Saturday halving: * 0.5 when utc_now.weekday() == 5 (-> 0.025 SOL).
#   - Whale conviction sizing: DISABLED since MT-524 — the get_whale_signal
#     call block and load_tracked_wallets loading block are commented out
#     (WHALE_SIZE_MULTIPLIERS 1.0/2.0/4.0/6.0x in src/signals/whale_tracker.py:319),
#     so the multiplier passed to try_enter() never changes from 1.0.
# Sizing is NOT driven by conviction score, liquidity tiers, or gate scores.

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.chain.jupiter import LAMPORTS_PER_SOL
from src.chain.jupiter_quote import JupiterQuoteV2, JupiterV2QuoteClient
from src.core.config import load_settings
from src.core.database import (
    init_db,
    mark_strategy_candidate_entered,
    record_jupiter_quote,
    record_strategy_candidate,
    record_trade,
)
from src.core.models import Side, Trade
from src.execution.price_provider import DexScreenerPriceProvider
from src.execution.paper import PaperExecutionAdapter
from src.monitoring.alerts import send_imessage
from src.monitoring.position_snapshots import snapshot_loop
from src.risk.rugcheck import RugCheckClient
from src.signals.grok_xsearch import get_mentions_with_timestamps, count_influencer_mentions
from src.strategy.position_manager import PositionManager
from src.strategy.gate_tuner import GateThresholds, GateTuner

WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"

BROWSER_PC_URL = "http://localhost:8099"
# Browser rows are only discovery hints. DexScreener's URL age filters are
# client-side and can be stale, so API pairCreatedAt is authoritative.
STRATEGY_B_DEXSCREENER_URL = "https://dexscreener.com/new-pairs/solana"
BROWSER_PC_WAIT_SECONDS = 8
# API-side age filtering follows the widened Strategy B gate, rather than the
# unreliable client-side maxAge query parameter.
# MT-537: auto-tuner paused, so these constants ARE the live gate values.
# Frozen manually after the tuner oscillated (mcap dropped to $1,250 garbage tier).
MAX_AGE_MINUTES = 22
MIN_MCAP_USD = 5_000
MIN_VOLUME_USD = 500
MIN_TXNS = 3
SOURCE_MAX_AGE_MINUTES = MAX_AGE_MINUTES
MAX_SOURCE_ROWS = 30

PAPER_SIZE_SOL = 0.05
MIN_MENTIONS = 3
MENTION_WINDOW_MINUTES = 5
MAX_OPEN = 5
SCAN_INTERVAL = 60
MONITOR_INTERVAL = 30
FAST_MONITOR_INTERVAL_S = 5
FAST_POLL_DROP_PCT = 0.05
TRAILING_STOP_PCT = 4.0
TRAILING_ARM_PCT = 2.0
TAKE_PROFIT_PCT = 80.0
HARD_STOP_PCT = 10.0
TIME_STOP_MINUTES = 10
ENTRY_CONFIRM_WINDOW_S = 90
EARLY_EXIT_GREEN_PCT = 0.01
# MT-537: UTC 21 added to the MT-516 blocked list (20:00-21:59 dead zone).
BLOCKED_UTC_HOURS = frozenset({0, 7, 19, 20, 21})
SATURDAY_SIZE_MULTIPLIER = 0.5

# Mode flags
REQUIRE_MENTIONS = False      # Set False to skip Grok entirely (on-chain only)
USE_INFLUENCER_MENTIONS = False  # Set True to use influencer-weighted mentions instead of raw count

GATES = GateThresholds(
    max_age_minutes=MAX_AGE_MINUTES,
    min_mcap_usd=MIN_MCAP_USD,
    min_volume_usd=MIN_VOLUME_USD,
    min_buy_sell_ratio=0.5,
)
MAX_MCAP_USD = 50_000

MAX_DEV_HOLDINGS_PCT = 10.0
MAX_TOP10_HOLDER_PCT = 100.0
MAX_MCAP_RUGCHECK = 50_000

# Rug signal filters
MIN_VOLUME_TO_MCAP_RATIO = 0.005
MAX_VOLUME_TO_MCAP_RATIO = 50.0
MIN_FEES_SOL_PER_15K_MCAP = 0.3

# Paper-mode holder concentration tiers (warn_pct, hard_reject_pct)
HOLDER_TIERS = [
    (2, 30.0, MAX_TOP10_HOLDER_PCT),
    (5, 30.0, MAX_TOP10_HOLDER_PCT),
    (10, 30.0, MAX_TOP10_HOLDER_PCT),
    (999, 30.0, MAX_TOP10_HOLDER_PCT),
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
logging.getLogger("httpx").setLevel(logging.WARNING)

try:
    from src.signals.whale_tracker import get_whale_signal, load_tracked_wallets  # noqa: F401 — kept for MT-524 re-enable
except ImportError:
    get_whale_signal = None
    load_tracked_wallets = None
    log.warning("whale_tracker sizing unavailable — whale conviction sizing disabled")

seen_mints: dict[str, float] = {}
SEEN_MINTS_TTL = 3600  # 1 hour in seconds
_rugcheck = RugCheckClient(timeout_s=5.0)
peak_prices: dict[str, float] = {}  # mint -> highest price seen


# ── Shadow Jupiter quote telemetry (MT-538) ───────────────────────────
# Every paper BUY/SELL fires a read-only Jupiter V2 quote in the background
# (fire-and-forget) to measure real-world slippage vs DexScreener paper
# prices. Never blocks the main loop, never touches a wallet.

_shadow_client = JupiterV2QuoteClient()
_shadow_tasks: set[asyncio.Task] = set()


def _fire_shadow_task(coro) -> None:
    """Schedule a shadow quote coroutine without blocking the caller."""
    task = asyncio.create_task(coro)
    _shadow_tasks.add(task)
    task.add_done_callback(_shadow_tasks.discard)


async def _drain_shadow_tasks() -> None:
    """Await any in-flight shadow quotes (used at shutdown / test-mode exit)."""
    if not _shadow_tasks:
        return
    await asyncio.gather(*list(_shadow_tasks), return_exceptions=True)


async def _shadow_quote_and_record(
    mint: str,
    side: Side,
    amount_lamports: int,
    dex_price_sol: float,
    position_id: str,
    db_path: Path,
    client: JupiterV2QuoteClient | None = None,
) -> JupiterQuoteV2 | None:
    """Quote one paper trade against Jupiter and persist the comparison.

    Failures log a warning and return None — paper trading is never affected.
    """
    client = client or _shadow_client
    try:
        quote = await client.get_quote(mint, side, amount_lamports)
    except Exception as exc:
        log.warning("SHADOW quote error mint=%s side=%s: %s", mint[:16], side.value, exc)
        return None
    if quote is None:
        return None

    jup_price = quote.price_sol
    slip_pct = (
        ((jup_price - dex_price_sol) / dex_price_sol) * 100
        if jup_price is not None and dex_price_sol
        else None
    )
    jup_str = f"{jup_price:.8f}" if jup_price is not None else "N/A"
    slip_str = f"{slip_pct:+.2f}%" if slip_pct is not None else "N/A"
    log.info(
        "SHADOW: %s mint=%s paper=%.8f jup=%s slip=%s",
        side.value, mint[:16], dex_price_sol, jup_str, slip_str,
    )

    try:
        await record_jupiter_quote(
            db_path,
            position_id=position_id,
            side=side.value.lower(),
            mint_address=mint,
            dex_price_sol=dex_price_sol,
            jup_output_amount=quote.out_amount,
            jup_price_sol=jup_price,
            price_impact_pct=quote.price_impact_pct,
            slippage_vs_paper_pct=slip_pct,
            route_info=json.dumps(list(quote.route_plan)),
            quoted_at=quote.quoted_at,
        )
    except Exception as exc:
        log.warning("SHADOW record failed mint=%s side=%s: %s", mint[:16], side.value, exc)
    return quote


async def _shadow_buy_quote(
    mint: str,
    size_sol: float,
    dex_price_sol: float,
    position_id: str,
    db_path: Path,
) -> None:
    """Fire the shadow BUY quote for the same SOL size the paper trade used."""
    await _shadow_quote_and_record(
        mint=mint,
        side=Side.BUY,
        amount_lamports=int(size_sol * LAMPORTS_PER_SOL),
        dex_price_sol=dex_price_sol,
        position_id=position_id,
        db_path=db_path,
    )


async def _shadow_sell_quote(
    pos,
    close_price: float,
    db_path: Path,
) -> None:
    """Fire the shadow SELL quote for the reverse swap of the paper close."""
    decimals = await _shadow_client.get_token_decimals(pos.mint_address)
    amount_lamports = int(pos.token_amount * (10**decimals))
    await _shadow_quote_and_record(
        mint=pos.mint_address,
        side=Side.SELL,
        amount_lamports=amount_lamports,
        dex_price_sol=close_price,
        position_id=pos.id,
        db_path=db_path,
    )


# ── Gate helpers ────────────────────────────────────────────────────

def _age_adjusted_min_txns(age_min: float) -> int:
    """Age-aware minimum transaction threshold for paper mode."""
    age_minimum: int
    if age_min < 1.0:
        age_minimum = 3
    elif age_min < 3.0:
        age_minimum = 5
    elif age_min < 5.0:
        age_minimum = 8
    elif age_min < 10.0:
        age_minimum = 12
    else:
        age_minimum = 16
    return max(MIN_TXNS, age_minimum)


def _age_holder_tier(age_min: float) -> tuple[float, float]:
    """Return (warn_pct, hard_reject_pct) for given age in minutes."""
    for max_age, warn, hard in HOLDER_TIERS:
        if age_min < max_age:
            return warn, hard
    return 30.0, MAX_TOP10_HOLDER_PCT


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


async def _search_fresh_pair(ticker: str, http: httpx.AsyncClient) -> dict | None:
    """Resolve a browser hint through search and return an API-aged Solana pair."""
    try:
        response = await http.get(
            "https://api.dexscreener.com/latest/dex/search",
            params={"q": ticker},
            timeout=10.0,
        )
        response.raise_for_status()
        pairs = response.json().get("pairs") or []
    except Exception as exc:
        log.debug("DexScreener search failed for %s: %s", ticker, exc)
        return None

    now_ms = time.time() * 1000
    choices: list[tuple[float, dict]] = []
    for pair in pairs:
        if not isinstance(pair, dict) or pair.get("chainId") != "solana":
            continue
        quote = pair.get("quoteToken") or {}
        if quote.get("address") != WRAPPED_SOL_MINT:
            continue
        created_ms = pair.get("pairCreatedAt")
        if not isinstance(created_ms, (int, float)) or created_ms <= 0:
            continue
        age_minutes = max(0.0, (now_ms - created_ms) / 60_000)
        if age_minutes <= SOURCE_MAX_AGE_MINUTES:
            choices.append((age_minutes, pair))
    if not choices:
        return None

    age_minutes, pair = min(choices, key=lambda item: item[0])
    base = pair.get("baseToken") or {}
    mint = base.get("address")
    if not isinstance(mint, str) or not mint:
        return None
    txns = (pair.get("txns") or {}).get("h1") or {}
    buys = int(txns.get("buys") or 0)
    sells = int(txns.get("sells") or 0)
    return {
        "mint": mint,
        "ticker": base.get("symbol") or ticker,
        "usd_market_cap": float(pair.get("marketCap") or pair.get("fdv") or 0),
        "created_timestamp": int(pair["pairCreatedAt"]),
        "volume": float((pair.get("volume") or {}).get("h1") or 0),
        "txns": buys + sells,
        "buy_sell_ratio": buys / max(sells, 1),
        "liquidity": float((pair.get("liquidity") or {}).get("usd") or 0),
        "pair": pair,
        "source_age_minutes": age_minutes,
    }


async def fetch_candidates(http: httpx.AsyncClient) -> list[dict]:
    """Return only candidates whose API pair timestamps pass the live age gate."""
    try:
        resp = await http.post(
            f"{BROWSER_PC_URL}/capture",
            json={"url": STRATEGY_B_DEXSCREENER_URL, "wait": BROWSER_PC_WAIT_SECONDS},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("candidates", data.get("rows", []))
        tickers = []
        for row in rows:
            ticker = row.get("name") or row.get("symbol") or row.get("token")
            if isinstance(ticker, str) and ticker.strip() and ticker not in tickers:
                tickers.append(ticker.strip())
        candidates = await asyncio.gather(
            *(_search_fresh_pair(ticker, http) for ticker in tickers[:MAX_SOURCE_ROWS]),
        )
        fresh = [candidate for candidate in candidates if candidate is not None]
        log.info(
            "browser-pc discovery rows=%d; DexScreener API fresh candidates=%d (all <= %.0fm)",
            len(rows), len(fresh), SOURCE_MAX_AGE_MINUTES,
        )
        return fresh
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
    if age_min > GATES.max_age_minutes:
        return False, f"age={age_min:.1f}m > {GATES.max_age_minutes:.0f}m", gates
    gates["age_pass"] = True

    mcap = coin.get("usd_market_cap")
    if not isinstance(mcap, (int, float)) or mcap <= 0:
        return False, f"age={age_min:.1f}m no usd_market_cap", gates
    if mcap < GATES.min_mcap_usd:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} < ${GATES.min_mcap_usd:.0f}", gates
    if mcap > MAX_MCAP_USD:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} > ${MAX_MCAP_USD}", gates
    gates["mcap_pass"] = True

    txns = None
    vol = None
    bs_ratio = None
    try:
        pair = coin.get("pair")
        if isinstance(pair, dict):
            h1 = (pair.get("txns") or {}).get("h1") or {}
            buys = int(h1.get("buys", 0))
            sells = int(h1.get("sells", 0))
            txns = buys + sells
            vol = float((pair.get("volume") or {}).get("h1", 0))
            bs_ratio = buys / max(sells, 1)

            min_txns = _age_adjusted_min_txns(age_min)
            if txns >= min_txns:
                gates["txn_pass"] = True

            if vol >= GATES.min_volume_usd:
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

            if bs_ratio >= GATES.min_buy_sell_ratio:
                gates["buy_sell_pass"] = True
        else:
            log.warning("DexScreener: no API pair attached for %s", mint[:8])
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
            if report.top_holder_pct <= hard_holder:
                gates["holder_pass"] = True

        creator_pct = _extract_creator_pct(report)
        if creator_pct is not None and creator_pct > MAX_DEV_HOLDINGS_PCT:
            gates["creator_pass"] = False
        elif creator_pct is None:
            log.warning("RugCheck: no creator holdings for %s", mint[:8])
    else:
        log.warning("RugCheck: no report for %s", mint[:8])
    coin["rugcheck_report"] = report

    # Build reason string
    fail_reasons = []
    if not gates["txn_pass"] and txns is not None:
        min_txns = _age_adjusted_min_txns(age_min)
        fail_reasons.append(f"txns={txns}<{min_txns}")
    if not gates["volume_pass"] and vol is not None:
        fail_reasons.append(f"vol=${vol:.0f}<${GATES.min_volume_usd:.0f}")
    if not gates["vol_mcap_pass"] and vol is not None and mcap > 0 and vol > 0:
        vol_ratio = vol / mcap
        label = "dead_volume" if vol_ratio < MIN_VOLUME_TO_MCAP_RATIO else "wash_trading"
        fail_reasons.append(label)
    if gates["low_fees_warn"] and txns is not None:
        estimated_fees = txns * 0.001
        fail_reasons.append(f"low_fees_warn({estimated_fees:.3f}SOL)")
    if not gates["buy_sell_pass"] and bs_ratio is not None:
        fail_reasons.append(f"buys/sells={bs_ratio:.1f}<{GATES.min_buy_sell_ratio:.1f}")
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


def _pair_metadata(pair: dict) -> dict[str, object]:
    """Store the DexScreener fields used to make this entry decision."""
    created_ms = pair.get("pairCreatedAt")
    age_minutes = None
    if isinstance(created_ms, (int, float)) and created_ms > 0:
        age_minutes = max(0.0, (time.time() * 1000 - created_ms) / 60_000)
    return {
        "dexscreener": {
            "mcap": pair.get("marketCap"), "volume": pair.get("volume") or {},
            "txns": pair.get("txns") or {}, "liquidity": pair.get("liquidity") or {},
            "fdv": pair.get("fdv"), "age_minutes": age_minutes,
            "price_usd": pair.get("priceUsd"), "price_change": pair.get("priceChange") or {},
        },
    }


async def log_candidate(db_path: Path, coin: dict, gates: dict[str, bool], reason: str) -> int:
    """Persist every API-aged candidate before dedupe or entry filtering."""
    pair = coin.get("pair") or {}
    h1 = (pair.get("txns") or {}).get("h1") or {}
    report = coin.get("rugcheck_report")
    passed = [name.removesuffix("_pass") for name, value in gates.items() if name.endswith("_pass") and value]
    values = {
        "age": coin.get("source_age_minutes"), "mcap": coin.get("usd_market_cap"),
        "txns": int(h1.get("buys") or 0) + int(h1.get("sells") or 0),
        "volume": (pair.get("volume") or {}).get("h1"),
        "buy_sell": coin.get("buy_sell_ratio"),
        "rugcheck": getattr(report, "provider_status", "skip"),
        "holder": getattr(report, "top_holder_pct", None),
        "creator": _extract_creator_pct(report) if report is not None else None,
    }
    sequence = (
        ("age_pass", "age"), ("mcap_pass", "mcap"), ("txn_pass", "txns"),
        ("volume_pass", "volume"), ("vol_mcap_pass", "volume"),
        ("buy_sell_pass", "buy_sell"), ("rugcheck_pass", "rugcheck"),
        ("holder_pass", "holder"), ("creator_pass", "creator"),
    )
    failed_gate, value_name = next(
        ((gate, value) for gate, value in sequence if not gates.get(gate, False)), (None, None),
    )
    failed = {} if failed_gate is None else {
        "gate": failed_gate.removesuffix("_pass"), "value": values[value_name], "reason": reason,
    }
    return await record_strategy_candidate(
        db_path, strategy="B", mint_address=coin["mint"], ticker=coin.get("ticker"),
        age_minutes=coin.get("source_age_minutes"), mcap_usd=coin.get("usd_market_cap"),
        volume_usd=(pair.get("volume") or {}).get("h1"), txns_buys=int(h1.get("buys") or 0),
        txns_sells=int(h1.get("sells") or 0), buy_sell_ratio=coin.get("buy_sell_ratio"),
        liquidity_usd=(pair.get("liquidity") or {}).get("usd"), fdv=pair.get("fdv"),
        price_usd=float(pair["priceUsd"]) if pair.get("priceUsd") else None,
        price_change_5m=(pair.get("priceChange") or {}).get("m5"),
        price_change_1h=(pair.get("priceChange") or {}).get("h1"),
        rugcheck_result="pass" if report is not None and gates.get("rugcheck_pass") else "fail" if report is not None else "skip",
        dev_holdings_pct=values["creator"], top10_holder_pct=values["holder"],
        gates_passed=passed, gates_failed=failed,
    )


# ── Entry ────────────────────────────────────────────────────────────

async def try_enter(
    mint: str,
    ticker: str,
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
    size_multiplier: float = 1.0,
    pair: dict | None = None,
) -> str | None:
    from src.core.database import has_losing_close, record_entry_skip

    if len(await manager.get_all_open(mode="paper")) >= MAX_OPEN:
        log.warning("SKIP %s ticker=%s — strategy capacity reached", mint[:16], ticker)
        return None

    utc_now = datetime.now(UTC)
    if utc_now.hour in BLOCKED_UTC_HOURS:
        log.warning(
            "SKIP %s ticker=%s — time_gate: UTC hour %d blocked",
            mint[:16], ticker, utc_now.hour,
        )
        try:
            await record_entry_skip(
                db_path, strategy="B", mint_address=mint, ticker=ticker,
                gate="time_gate", reason=f"utc_hour={utc_now.hour}",
            )
        except Exception as exc:
            log.debug("candidate_log write failed (non-fatal): %s", exc)
        return None

    if await has_losing_close(db_path, mint):
        log.warning(
            "SKIP %s ticker=%s — repeat_loser: mint previously closed at a loss",
            mint[:16], ticker,
        )
        try:
            await record_entry_skip(
                db_path, strategy="B", mint_address=mint, ticker=ticker,
                gate="repeat_loser", reason="previous close had negative PnL",
            )
        except Exception as exc:
            log.debug("candidate_log write failed (non-fatal): %s", exc)
        return None

    if utc_now.weekday() == 5:
        log.info("Saturday — halving position size for %s (%s)", mint[:16], ticker)
        size_multiplier *= SATURDAY_SIZE_MULTIPLIER

    existing = await manager.get_position(mint, mode="paper")
    if existing is not None:
        return None

    price = await mark_provider.get_current_price(mint)
    if price is None or price <= 0:
        log.warning("SKIP %s ticker=%s \u2014 no valid DexScreener price", mint[:16], ticker)
        return None

    size_sol = PAPER_SIZE_SOL * size_multiplier
    try:
        trade = await adapter.execute_swap(mint, Side.BUY, size_sol)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s \u2014 execute_swap failed: %s", mint[:16], ticker, exc)
        return None

    if trade is None:
        log.warning("SKIP %s ticker=%s \u2014 execute_swap returned None", mint[:16], ticker)
        return None

    if isinstance(pair, dict):
        metadata = dict(trade.metadata or {})
        metadata.update(_pair_metadata(pair))
        trade.metadata = metadata

    try:
        await record_trade(db_path, trade)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s \u2014 record_trade failed: %s", mint[:16], ticker, exc)
        return None

    try:
        from src.core.models import Signal, SignalSource, SignalType

        dummy_signal = Signal(
            source=SignalSource.MANUAL,
            type=SignalType.NEW_POOL,
            mint_address=mint,
            confidence=1.0,
        )
        position = await manager.open_position(trade, dummy_signal)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s \u2014 open_position failed: %s", mint[:16], ticker, exc)
        return None

    log.info("ENTRY mint=%s ticker=%s price=%.8f SOL size=%.4f SOL", mint[:16], ticker, price, size_sol)
    send_imessage(
        f"\U0001f7e2 [STRATEGY B] ENTERED {ticker}\n"
        f"Price: {price:.8f} SOL\n"
        f"Size: {size_sol} SOL"
    )
    _fire_shadow_task(_shadow_buy_quote(mint, size_sol, price, position.id, db_path))
    return position.id


# ── Monitoring ───────────────────────────────────────────────────────

async def monitor_positions(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
    gate_tuner: GateTuner | None = None,
) -> bool:
    """Re-mark open positions and close on stops; True if any position is in
    the danger zone (below 95% of entry) so the caller polls at 5s."""
    danger = False
    positions = await manager.get_all_open(mode="paper")
    for pos in positions:
        current_price = await mark_provider.get_current_price(pos.mint_address)
        if current_price is None:
            continue

        age_min = (datetime.now(UTC) - pos.opened_at).total_seconds() / 60
        entry = pos.entry_price_sol if pos.entry_price_sol > 0 else current_price

        prev_peak = peak_prices.get(pos.mint_address, entry)
        peak = max(prev_peak, current_price)
        peak_prices[pos.mint_address] = peak

        close_reason = None
        close_price = current_price

        if entry:
            if current_price >= entry * (1 + TAKE_PROFIT_PCT / 100):
                close_reason = "take_profit"
                close_price = entry * (1 + TAKE_PROFIT_PCT / 100)
            elif current_price <= entry * (1 - HARD_STOP_PCT / 100):
                close_reason = "hard_stop"
                close_price = entry * (1 - HARD_STOP_PCT / 100)
            elif (
                peak > entry * (1 + TRAILING_ARM_PCT / 100)
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

        if age_min >= TIME_STOP_MINUTES and close_reason is None:
            close_reason = "time_stop"

        if close_reason:
            peak = peak_prices.get(pos.mint_address)
            peak_prices.pop(pos.mint_address, None)
            trade = await _adapter_close(pos, close_price, close_reason, db_path)
            await manager.close_position(pos.mint_address, close_price, mode="paper", peak_price_sol=peak)
            # AUTO-TUNER PAUSED — oscillating, not converging. See MT-537.
            # if gate_tuner is not None and await gate_tuner.maybe_tune():
            #     log.info("Auto-tuned Strategy B gates: %s", json.dumps(gate_tuner.thresholds.as_dict()))
            pnl_pct = ((close_price - pos.entry_price_sol) / pos.entry_price_sol) * 100 if pos.entry_price_sol else 0.0
            log.info(
                "CLOSE [%s]: mint=%s entry=%.8f close=%.8f",
                close_reason, pos.mint_address[:16], pos.entry_price_sol, close_price,
            )
            send_imessage(
                f"\U0001f534 [STRATEGY B] CLOSED {pos.mint_address[:8]}\n"
                f"Entry: {pos.entry_price_sol:.8f} \u2192 Close: {close_price:.8f}\n"
                f"PnL: {pnl_pct:+.1f}%\n"
                f"Reason: {close_reason}"
            )
        elif current_price < entry * (1.0 - FAST_POLL_DROP_PCT):
            danger = True

    return danger


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
    _fire_shadow_task(_shadow_sell_quote(pos, close_price, db_path))
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
            now_ts = time.time()
            expired = [m for m, t in seen_mints.items() if now_ts - t > SEEN_MINTS_TTL]
            for m in expired:
                del seen_mints[m]
            if expired:
                log.info("Expired %d stale seen_mints entries", len(expired))
            try:
                cycle_start = time.monotonic()
                log.info("--- Strategy B Scan ---")
                log.info("whale tracker disabled — re-enable when Helius integration is built into entry pipeline.")
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

                # Candidate telemetry is collected even at capacity; only entries are capped.
                candidates = await fetch_candidates(http)
                detailed["total"] = len(candidates)
                if candidates:

                    for coin in candidates:
                        ticker = coin["ticker"]
                        mint = coin["mint"]

                        passed, reason, gates = await screen_coin(coin, http, _rugcheck)
                        log.info("SCREEN %s (%s): %s", ticker, mint[:8], reason)
                        candidate_id = await log_candidate(db_path, coin, gates, reason)

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

                        if mint in seen_mints:
                            log.info("SKIP %s — already evaluated for entry this hour", ticker)
                            continue
                        seen_mints[mint] = time.time()

                        detailed["full_screen_pass"] += 1

                        if len(await manager.get_all_open(mode="paper")) >= MAX_OPEN:
                            log.info("SKIP %s — strategy capacity reached", ticker)
                            continue

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
                        # MT-524: whale conviction sizing disabled project-wide — the
                        # signal proved useless for trade decisions while burning Helius
                        # credits. Code kept intact for re-enable when Helius integration
                        # is built into the entry pipeline (WHALE_SIZE_MULTIPLIERS in
                        # src/signals/whale_tracker.py:319).
                        # if get_whale_signal is not None:
                        #     try:
                        #         whale_data = await get_whale_signal(mint, tracked_wallets, http)
                        #         whale_count = whale_data.get("whale_count", 0)
                        #         size_multiplier = whale_data.get("size_multiplier", 1.0)
                        #         if whale_count > 0:
                        #             log.info("🐋 WHALE SIGNAL: %d whale(s) in %s — size multiplier: %.1fx", whale_count, ticker, size_multiplier)
                        #     except Exception as e:
                        #         log.debug("Whale check failed (non-fatal): %s", e)

                        detailed["entry_attempts"] += 1
                        position_id = await try_enter(
                            mint, ticker, mark_provider, adapter, manager, db_path, size_multiplier,
                            pair=coin.get("pair"),
                        )
                        if position_id:
                            await mark_strategy_candidate_entered(db_path, candidate_id, position_id)
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
            except Exception as exc:
                log.error("CRASH in scan_loop cycle: %s", exc, exc_info=True)
                await asyncio.sleep(SCAN_INTERVAL)
                continue


async def monitor_loop(
    manager: PositionManager,
    mark_provider: DexScreenerPriceProvider,
    db_path: Path,
    gate_tuner: GateTuner,
) -> None:
    while True:
        cycle_start = time.monotonic()
        danger = await monitor_positions(manager, mark_provider, db_path, gate_tuner)
        elapsed = time.monotonic() - cycle_start
        interval = FAST_MONITOR_INTERVAL_S if danger else MONITOR_INTERVAL
        await asyncio.sleep(max(0.0, interval - elapsed))


async def record_manual_freeze(db_path: Path) -> None:
    """MT-537: persist the frozen gate thresholds as a manual_freeze row.

    Idempotent — only inserts when no manual_freeze row exists for strategy B,
    so restarts never duplicate the record. The bot's /gates report reads it.
    """
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM gate_config WHERE strategy = 'B' AND reason = 'manual_freeze' LIMIT 1",
        )
        exists = await cursor.fetchone()
        await cursor.close()
        if exists is None:
            await db.execute(
                """INSERT INTO gate_config
                   (strategy, updated_at, config_json, reason, sample_size, metrics_json)
                   VALUES ('B', ?, ?, 'manual_freeze', 0, '{}')""",
                (datetime.now(UTC).isoformat(), json.dumps(GATES.as_dict())),
            )
            await db.commit()
            log.info("Recorded manual_freeze gate config: %s", json.dumps(GATES.as_dict()))


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run one cycle and exit (2-minute test)")
    args = parser.parse_args()

    settings = load_settings()
    db_path = DB_PATH
    await init_db(db_path)
    gate_tuner = GateTuner(db_path, GATES)
    await gate_tuner.ensure_initial_config()
    await record_manual_freeze(db_path)

    mark_provider = DexScreenerPriceProvider()
    adapter = PaperExecutionAdapter(price_provider=mark_provider)
    manager = PositionManager(db_path, settings, strategy="B")

    tracked_wallets: list = []
    # MT-524: tracked wallet loading disabled alongside the whale tracker (no Helius calls
    # in load_tracked_wallets itself, but no reason to load 50 wallets while unused).
    # if load_tracked_wallets is not None:
    #     try:
    #         tracked_wallets = load_tracked_wallets()
    #         log.info("Loaded %d tracked whale wallets", len(tracked_wallets))
    #     except Exception:
    #         log.warning("Failed to load tracked wallets — whale sizing disabled")

    if not REQUIRE_MENTIONS:
        mode_label = "ON-CHAIN ONLY (Grok disabled)"
    elif USE_INFLUENCER_MENTIONS:
        mode_label = "INFLUENCER MENTIONS >= 1 in first 15min"
    else:
        mode_label = f"RAW MENTIONS >= {MIN_MENTIONS} in first {MENTION_WINDOW_MINUTES}min"

    log.info(
        "Strategy B started: mode=%s API-aged candidates<=%.0fmin scan=%ds monitor=%ds.",
        mode_label, SOURCE_MAX_AGE_MINUTES, SCAN_INTERVAL, MONITOR_INTERVAL,
    )
    if args.test:
        await scan_loop(mark_provider, adapter, manager, db_path, tracked_wallets=tracked_wallets, test_mode=True)
        await _drain_shadow_tasks()
        await _shadow_client.close()
        return
    await asyncio.gather(
        scan_loop(mark_provider, adapter, manager, db_path, tracked_wallets=tracked_wallets),
        monitor_loop(manager, mark_provider, db_path, gate_tuner),
        snapshot_loop(manager, mark_provider, db_path),
    )
    await _drain_shadow_tasks()
    await _shadow_client.close()


if __name__ == "__main__":
    asyncio.run(main())

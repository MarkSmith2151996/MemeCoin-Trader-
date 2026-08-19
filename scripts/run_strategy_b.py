"""Strategy B: Jupiter-discovered Grok social-hype validated paper trading loop.

Uses Jupiter Tokens V2 to scan fresh Solana tokens under 22 minutes old.

SCAN (every 1s by default, STRATEGY_B_SCAN_INTERVAL in .env):
  1. Jupiter Tokens V2 discovers fresh, organic, and trending tokens
     (/toporganicscore/5m, /recent, /toptrending/5m — MT-588)
  2. Screen through age/mcap/txns/vol/ratio/pool-depth/RugCheck/score gates
  3. Grok mention check via 0-5min temporal bucket
  4. Paper enter if mentions >= MIN_MENTIONS and slots available

MONITOR (every 30s):
  5. Re-mark open positions and close on take-profit / hard-stop / time-stop

MT-588: Jupiter Developer tier (10 RPS) is active. The scan cadence drops to
1s (three discovery endpoints per cycle ≈ 3 req/s), a third discovery endpoint
(/toptrending/5m) is added, slippage is tiered by pool SOL depth, the $5K mcap
floor is replaced by a pool-depth floor (30 SOL bonding curve / 50 SOL
graduated), graduated tokens get a lower score threshold than bonding-curve
tokens, and priority fees are queried dynamically from the RPC
(getRecentPrioritizationFees, 75th percentile, 30s cache).

MT-590: pool-depth floors lowered to 10 SOL bonding curve / 25 SOL graduated
and slippage tiers relaxed to >20 SOL / 5-20 SOL / <5 SOL skip. 66.7% of
tokens entered Aug 15-18 had <50 SOL depth and 39.9% <30 SOL, so the MT-588
floors were rejecting most of the previously-tradeable funnel. The MT-553
Wednesday weekday block is lifted (funnel and gates changed since the MT-552
sweep it was based on).

MT-593: walk-forward validated gates (MT-592) applied to the live loop:
pool floors 5 SOL bonding / 5 SOL graduated (tuner: ~4.5-4.7 SOL across all
3 iterations), creator-holdings gate tightened to >0% (selected in all 3
iterations; missing data passes), mcap floor re-enforced at $5,100 (2 of 3
iterations), Wednesday re-blocked (paper: -0.72 SOL / 23.7% WR), and the
bonding-curve strength threshold lowered 55 -> 40 (score gate useful in 1 of
3 iterations; 937 FAIL:score was over-filtering).

MT-560: scan cadence reduced from 60s to 2s. The old 60s interval was legacy
from the Chrome/DexScreener era, where every cycle needed an 8-second
browser-pc capture. Jupiter tokens/v2 free tier allows ~1 RPS; two discovery
endpoints per cycle at a 2s cadence stayed at ~1 req/s. Discovery lag and
end-to-end pipeline latency are logged per candidate/trade (DISCOVERY_LAG /
LATENCY).

MT-566: the MT-560 per-mint screening cooldown is removed. RugCheck results
are now cached per token (10-min TTL), so gate re-evaluation runs every cycle
(~0ms API-free) and a token enters on the first cycle where it becomes
eligible — the 45s cooldown wait was showing up as ~46s of `gates` latency in
the LATENCY telemetry. candidate_log inserts and SCREEN log lines remain
throttled per mint (see SCREEN_LOG_COOLDOWN_S) to keep DB/log volume bounded
at the faster cadence.

Run:
    python3 scripts/run_strategy_b.py          # normal loop
    timeout 120 python3 scripts/run_strategy_b.py --test  # 2-minute test
"""

# ── Position sizing (MT-522/MT-524) ─────────────────────────────────
# Entry size = PAPER_SIZE_SOL (0.05 SOL) * size_multiplier.
# size_multiplier is always 1.0 in practice:
#   - Saturday halving: * 0.5 when utc_now.weekday() == 5 (-> 0.025 SOL).
#   - Whale conviction sizing: DISABLED since MT-524 — the get_whale_signal
#     call block and load_tracked_wallets loading block are commented out,
#     so the multiplier passed to try_enter() never changes from 1.0.
# Sizing is NOT driven by conviction score, liquidity tiers, or gate scores.

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import statistics
import sys
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
# MT-560: load .env at import time so env-backed constants below (e.g.
# STRATEGY_B_SCAN_INTERVAL) resolve before main() runs. load_dotenv() never
# overrides already-set environment variables and is idempotent, so the
# call inside main() stays harmless.
from dotenv import load_dotenv

from src.chain.jupiter import LAMPORTS_PER_SOL
from src.chain.jupiter_quote import JupiterQuoteV2, JupiterV2QuoteClient
from src.chain.priority_fee import PriorityFeeProvider
from src.core.config import load_settings
from src.core.database import (
    init_db,
    mark_strategy_candidate_entered,
    record_discovery_lag,
    record_jupiter_quote,
    record_strategy_candidate,
    record_trade,
)
from src.core.models import Side, Trade
from src.execution.base import ExecutionAdapter
from src.execution.paper import PaperExecutionAdapter
from src.execution.price_provider import DexScreenerPriceProvider
from src.monitoring.alerts import send_imessage
from src.monitoring.position_snapshots import snapshot_loop
from src.risk.rugcheck import RugCheckClient, RugCheckResult
from src.signals.grok_xsearch import count_influencer_mentions, get_mentions_with_timestamps
from src.strategy.gate_tuner import GateThresholds, GateTuner
from src.strategy.position_manager import PositionManager

load_dotenv()

WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"

BROWSER_PC_URL = "http://localhost:8099"
# DEPRECATED: replaced by Jupiter API (MT-550).
# Browser rows are only discovery hints. DexScreener's URL age filters are
# client-side and can be stale, so API pairCreatedAt is authoritative.
STRATEGY_B_DEXSCREENER_URL = "https://dexscreener.com/new-pairs/solana"
BROWSER_PC_WAIT_SECONDS = 8
# API-side age filtering follows the widened Strategy B gate, rather than the
# unreliable client-side maxAge query parameter.
# MT-537: auto-tuner paused, so these constants ARE the live gate values.
# Frozen manually after the tuner oscillated (mcap dropped to $1,250 garbage tier).
MAX_AGE_MINUTES = 22
# MT-593: walk-forward validated mcap floor — 2 of 3 iterations found
# mcap >= ~$5.1K (iter1 $5,105 / iter3 $5,117); tightened from $5,000.
MIN_MCAP_USD = 5_100
MIN_VOLUME_USD = 500
MIN_TXNS = 3
SOURCE_MAX_AGE_MINUTES = MAX_AGE_MINUTES
MAX_SOURCE_ROWS = 30

JUPITER_API_BASE = "https://api.jup.ag/tokens/v2"
JUPITER_API_KEY = os.environ.get("JUPITER_API_KEY", "")
JUPITER_HEADERS = {"x-api-key": JUPITER_API_KEY}

# ── MT-588: pool depth / graduation / slippage / fees ─────────────────
# Jupiter Developer tier (10 RPS) is active — 3 discovery endpoints per 1s
# cycle is ~3 req/s, well within limits.
# Pool-depth floor (replaces the old $5K mcap floor, MT-588): bonding-curve
# pools must hold >= 5 SOL, PumpSwap/Raydium pools >= 5 SOL. Tokens with no
# pool liquidity data are skipped outright. MT-590: floors lowered from 30/50
# SOL — 66.7% of tokens entered Aug 15-18 had <50 SOL depth and 39.9% <30 SOL,
# so the MT-588 floors were rejecting most of the previously-tradeable funnel.
# MT-593: walk-forward tuner found ~4.5-4.7 SOL optimal across all 3
# iterations, so both floors drop to 5 SOL (bonding + graduated).
POOL_MIN_SOL_BONDING = 5.0
POOL_MIN_SOL_GRADUATED = 5.0
# SOL/USD price lookup cache for converting Jupiter's USD liquidity to SOL
# depth. One extra Jupiter call every SOL_PRICE_CACHE_TTL_S, never per cycle.
SOL_PRICE_CACHE_TTL_S = 60.0
# Graduated tokens (PumpSwap/Raydium) trade at 0.25% DEX fees vs 1.25% on the
# bonding curve — still-on-curve tokens must clear a higher strength score to
# enter (the stronger signal justifies the higher fee).
# MT-593: the walk-forward tuner found the score gate useful in only 1 of 3
# iterations, and the 55 threshold was filtering too aggressively (937
# FAIL:score in recent logs) — bonding-curve threshold lowered back to 40,
# equal to the graduated threshold.
MIN_SCORE_GRADUATED = 40.0
MIN_SCORE_BONDING_CURVE = 40.0
# Tighter slippage tiers by pool SOL depth (entry path, MT-588; MT-590:
# thresholds relaxed to 20/5 SOL to match the pre-MT-588 tradeable funnel):
#   >20 SOL depth  -> 1% max slippage (100 bps)
#   5-20 SOL depth -> 3% max slippage (300 bps)
#   <5 SOL depth   -> skipped as too thin
SLIPPAGE_BPS_THICK_POOL = 100
SLIPPAGE_BPS_MID_POOL = 300
THICK_POOL_MIN_SOL = 20.0
MID_POOL_MIN_SOL = 5.0

PAPER_SIZE_SOL = 0.05
MIN_MENTIONS = 3
MENTION_WINDOW_MINUTES = 5
MAX_OPEN = 5
# MT-560: 60s was legacy from the Chrome/DexScreener era (8s browser-pc
# capture per cycle). MT-588: with the Jupiter Developer tier (10 RPS) active
# and three discovery endpoints per cycle, the default cadence drops to 1s
# (~3 req/s). The cycle sleep is `max(0.0, SCAN_INTERVAL - elapsed)` — if
# screening takes longer than the interval the next cycle starts immediately
# rather than queuing up, which is the rate-limit protection. Tune via
# STRATEGY_B_SCAN_INTERVAL in .env.
SCAN_INTERVAL = int(os.environ.get("STRATEGY_B_SCAN_INTERVAL", "1"))
MONITOR_INTERVAL = 30
FAST_MONITOR_INTERVAL_S = 5
FAST_POLL_DROP_PCT = 0.05
# MT-566: per-mint throttle for candidate_log inserts and SCREEN log lines
# (replaces the MT-560 screening cooldown). Gate evaluation itself runs every
# cycle with the RugCheck cache; only persistence/logging is throttled so the
# DB and log volume stay bounded at the fast cadence (2s at MT-560, 1s since MT-588).
SCREEN_LOG_COOLDOWN_S = 45
# MT-560: cycle summary cadence — the full "Gates:" line would flood the
# log 30x/min. Logged at INFO every GATES_LOG_EVERY cycles (~60s), DEBUG in
# between; the console print follows the same throttle.
GATES_LOG_EVERY = 30
# MT-553: optimized exit params from the MT-552 sweep (2% trail / 150% TP / 8%
# hard stop) — +8.75 SOL vs +6.18 SOL baseline, PF 4.17 -> 7.63.
TRAILING_STOP_PCT = 2.0
TRAILING_ARM_PCT = 2.0
TAKE_PROFIT_PCT = 150.0
HARD_STOP_PCT = 8.0
TIME_STOP_MINUTES = 10
ENTRY_CONFIRM_WINDOW_S = 90
EARLY_EXIT_GREEN_PCT = 0.01
# MT-537: UTC 21 added to the MT-516 blocked list (20:00-21:59 dead zone).
BLOCKED_UTC_HOURS = frozenset({0, 7, 19, 20, 21})
# MT-593: Wednesday (weekday 2) re-blocked. MT-590 lifted it, but paper data
# since then shows Wednesday at -0.72 SOL / 23.7% win rate — the walk-forward
# and paper cohorts both say the block belongs back in.
BLOCKED_WEEKDAYS = frozenset({2})
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

# MT-593: creator-holdings gate tightened from >10% to >0% — the walk-forward
# tuner selected creator_holdings_max=0.0 in all 3 iterations (skip any token
# where the creator still holds). Missing data passes (no block on unknown).
MAX_DEV_HOLDINGS_PCT = 0.0
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

# MT-589: single-writer logging. The watchdog starts this script with
# `> /tmp/strategy_b.log 2>&1`, so an extra FileHandler on the same path made
# every line appear twice. Only a StreamHandler is configured here — the
# shell redirect owns the file. Handlers are cleared first so re-initializing
# (e.g. under pytest or a supervisor that imports this module twice) can
# never attach a duplicate handler.
_root_logger = logging.getLogger()
_root_logger.handlers.clear()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("strategy_b")
logging.getLogger("httpx").setLevel(logging.WARNING)

try:
    from src.signals.whale_tracker import (  # noqa: F401 — kept for MT-524 re-enable
        get_whale_signal,
        load_tracked_wallets,
    )
except ImportError:
    get_whale_signal = None
    load_tracked_wallets = None
    log.warning("whale_tracker sizing unavailable — whale conviction sizing disabled")

# MT-584: in-memory dedup of evaluated mints. A mint is marked once it has
# been screened (passed or failed) and skipped on subsequent cycles until
# the TTL expires. Expired entries are cleaned every cycle in scan_loop.
seen_mints: dict[str, float] = {}
SEEN_MINTS_TTL = 3600  # 1 hour in seconds
# MT-584: dedup-skip log lines are INFO once per mint (so the behavior is
# verifiable in the log) and DEBUG afterwards (so the fast cadence does not
# flood the log with up to ~130 skip lines per cycle).
_dedup_skip_logged: set[str] = set()
_rugcheck = RugCheckClient(timeout_s=5.0)
# MT-566: per-token RugCheck cache. First sight of a mint fetches the report
# (~400ms); every re-evaluation within the TTL uses the cached copy, skipping
# the API call entirely. Only provider_status == "ok" reports are cached —
# transient errors (timeout/429/provider) retry on the next cycle.
RUGCHECK_CACHE_TTL_S = 600  # 10 minutes
_rugcheck_cache: dict[str, tuple[float, RugCheckResult]] = {}
_rugcheck_cache_hits = 0
_rugcheck_cache_misses = 0
peak_prices: dict[str, float] = {}  # mint -> highest price seen

# ── MT-560 latency telemetry state ────────────────────────────────────
# Discovery lag: first wall-clock sighting of each mint in a Jupiter API
# response, compared against the on-chain createdAt later.
first_seen_epoch: dict[str, float] = {}  # mint -> epoch seconds of first API sighting
first_seen_mono: dict[str, float] = {}   # mint -> time.monotonic() of first API sighting
_lag_logged: set[str] = set()            # mints whose DISCOVERY_LAG was already logged
_lag_window: deque[float] = deque(maxlen=100)  # rolling discovery-lag seconds
# MT-563: per-source lag samples (pump/raydium/pumpswap/unknown) so the
# periodic DISCOVERY_LAG_REPORT can break the last 100 samples down by source.
_lag_by_source: dict[str, deque[float]] = {
    "pump": deque(maxlen=100),
    "raydium": deque(maxlen=100),
    "pumpswap": deque(maxlen=100),
    "unknown": deque(maxlen=100),
}
_lag_summary_every = 100                # log DISCOVERY_LAG_REPORT every N samples

# MT-566: per-mint throttle state for candidate_log inserts + SCREEN log lines
# (replaces the MT-560 screening cooldown). Gate evaluation runs every cycle.
_last_candidate_log: dict[str, float] = {}

# Cycle counter for log throttling (GATES_LOG_EVERY).
cycle_number = 0

# Pipe stats for the Grok mention lane (MT-560: initialized to fix a latent
# KeyError when REQUIRE_MENTIONS is enabled).
pipe_stats: dict[str, int] = {}


# ── Shadow Jupiter quote telemetry (MT-538) ───────────────────────────
# Every paper BUY/SELL fires a read-only Jupiter V2 quote in the background
# (fire-and-forget) to measure real-world slippage vs DexScreener paper
# prices. Never blocks the main loop, never touches a wallet.

_shadow_client = JupiterV2QuoteClient()
_shadow_tasks: set[asyncio.Task] = set()

# ── MT-588: dynamic priority fee + SOL price state ────────────────────
# The priority fee provider queries the connected RPC
# (getRecentPrioritizationFees, 75th percentile) and refreshes its cache at
# most every 30s — module-level state so the fee persists across cycles.
# In live mode the fee is passed to the swap client; in paper mode it is
# telemetry only (logged on each refresh).
_priority_fee_provider = PriorityFeeProvider()
# Cached Jupiter SOL/USD price (monotonic, usd) for pool-depth conversion.
_sol_price_cache: tuple[float, float] | None = None


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


# DEPRECATED: replaced by Jupiter API (MT-550).
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


# DEPRECATED: replaced by Jupiter API (MT-550).
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


# DEPRECATED: replaced by Jupiter API (MT-550).
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


def _synthesize_pair_dict(token: dict) -> dict:
    """Map Jupiter token fields to the DexScreener pair shape used downstream."""
    s1h = token.get("stats1h", {})
    s5m = token.get("stats5m", {})
    fp = token.get("firstPool", {})
    return {
        "chainId": "solana",
        "pairCreatedAt": (
            int(datetime.fromisoformat(fp["createdAt"]).timestamp() * 1000)
            if fp.get("createdAt") else 0
        ),
        "baseToken": {"address": token["id"], "symbol": token.get("symbol", "")},
        "txns": {
            "h1": {
                "buys": int(s1h.get("numBuys", 0) or 0),
                "sells": int(s1h.get("numSells", 0) or 0),
            },
        },
        "volume": {"h1": (s1h.get("buyVolume", 0) or 0) + (s1h.get("sellVolume", 0) or 0)},
        "liquidity": {"usd": float(token.get("liquidity", 0) or 0)},
        "marketCap": token.get("mcap"),
        "fdv": token.get("fdv"),
        "priceUsd": str(token.get("usdPrice", "0")),
        "priceChange": {
            "m5": s5m.get("priceChange", 0),
            "h1": s1h.get("priceChange", 0),
        },
    }


def classify_token_source(token: dict) -> str:
    """Best-effort launch-source label for a Jupiter token (MT-563).

    pump.fun mints always end in the literal 'pump' suffix, and a pump.fun
    bonding-curve firstPool id equals the mint itself. PumpSwap pool ids also
    end in 'pump' while Raydium pool ids do not, so a pump.fun-suffixed mint
    whose first pool no longer matches the mint (and lacks the suffix) has
    migrated to Raydium — the classic pump.fun graduation target. Jupiter's
    launchpad label covers the rest; anything else (e.g. 'met-dbc' Meteora
    launches, Moonshot) is 'unknown' for this pump/raydium/pumpswap taxonomy.
    """
    mint = str(token.get("id") or "").lower()
    launchpad = str(token.get("launchpad") or "").lower()
    pool_id = str((token.get("firstPool") or {}).get("id") or "").lower()

    if mint.endswith("pump"):
        if pool_id and pool_id != mint and not pool_id.endswith("pump"):
            return "raydium"
        return "pump"
    if launchpad == "pump.fun":
        return "pump"
    if pool_id.endswith("pump"):
        return "pumpswap"
    return "unknown"


def _token_graduation(token: dict | None) -> str:
    """Return "bonding" or "graduated" for a raw Jupiter token record (MT-588).

    A pump.fun mint still on the bonding curve has a firstPool whose id equals
    the mint itself (the curve pool IS the mint). Any other firstPool id means
    the token graduated to PumpSwap (pool id still ends in "pump") or Raydium
    (no "pump" suffix). Non-pump mints (Meteora/Moonshot launches, etc.) never
    trade on the pump.fun bonding curve, so they count as graduated. Tokens
    with no usable data fall back to "graduated" (normal threshold) rather
    than being rejected on this gate.
    """
    if not isinstance(token, dict):
        return "graduated"
    mint = str(token.get("id") or "").lower()
    pool_id = str((token.get("firstPool") or {}).get("id") or "").lower()
    if not mint.endswith("pump"):
        return "graduated"
    if pool_id and pool_id != mint:
        return "graduated"
    return "bonding"


def _candidate_strength_score(coin: dict, age_min: float) -> float:
    """0-100 composite signal strength used by the graduated gate (MT-588).

    Components (all capped):
      - buy/sell ratio vs 2.0                        -> 0-40 pts
      - 1h vol/mcap ratio vs 0.05                    -> 0-30 pts
      - 1h txns vs 4x the age-adjusted minimum       -> 0-15 pts
      - 1h volume vs 10x the minimum volume gate     -> 0-15 pts
    """
    pair = coin.get("pair") or {}
    h1 = (pair.get("txns") or {}).get("h1") or {}
    buys = int(h1.get("buys") or 0)
    sells = int(h1.get("sells") or 0)
    txns = buys + sells
    vol = float((pair.get("volume") or {}).get("h1") or 0)
    mcap = float(coin.get("usd_market_cap") or 0)
    bs_ratio = buys / max(sells, 1)
    vol_ratio = vol / mcap if mcap > 0 else 0.0
    min_txns = max(_age_adjusted_min_txns(age_min), 1)
    score = 0.0
    score += min(bs_ratio / 2.0, 1.0) * 40.0
    score += min(vol_ratio / 0.05, 1.0) * 30.0
    score += min(txns / (4.0 * min_txns), 1.0) * 15.0
    score += min(vol / (10.0 * max(GATES.min_volume_usd, 1.0)), 1.0) * 15.0
    return round(score, 1)


def _slippage_bps_for_pool(pool_sol: float | None) -> int | None:
    """Tiered max entry slippage by pool SOL depth (MT-588, retuned MT-590).

    >20 SOL -> 1% (100 bps); 5-20 SOL -> 3% (300 bps); <5 SOL or unknown
    depth -> None (too thin, skip).
    """
    if pool_sol is None:
        return None
    if pool_sol > THICK_POOL_MIN_SOL:
        return SLIPPAGE_BPS_THICK_POOL
    if pool_sol >= MID_POOL_MIN_SOL:
        return SLIPPAGE_BPS_MID_POOL
    return None


async def _get_sol_price_usd(http: httpx.AsyncClient) -> float | None:
    """Cached Jupiter SOL/USD price for pool-depth conversion (MT-588).

    Refreshed at most every SOL_PRICE_CACHE_TTL_S via one /search lookup of
    the SOL mint. On refresh failure the last known price is returned (stale
    within 10 minutes) so a hiccup does not reject every pool-depth gate in a
    cycle; with no cached value at all, returns None.
    """
    global _sol_price_cache
    now = time.monotonic()
    if _sol_price_cache is not None and now - _sol_price_cache[0] < SOL_PRICE_CACHE_TTL_S:
        return _sol_price_cache[1]
    try:
        response = await http.get(
            f"{JUPITER_API_BASE}/search",
            params={"query": WRAPPED_SOL_MINT},
            headers=JUPITER_HEADERS,
            timeout=10.0,
        )
        response.raise_for_status()
        payload = response.json()
        for token in payload if isinstance(payload, list) else []:
            if str(token.get("id", "")).lower() == WRAPPED_SOL_MINT.lower():
                price = float(token.get("usdPrice") or 0)
                if price > 0:
                    _sol_price_cache = (time.monotonic(), price)
                    return price
        log.warning("SOL price lookup: SOL mint not found in Jupiter /search response")
    except Exception as exc:
        log.warning("SOL price lookup failed: %s", exc)
    if _sol_price_cache is not None and now - _sol_price_cache[0] < 600.0:
        return _sol_price_cache[1]
    return None


async def fetch_candidates_jupiter(http: httpx.AsyncClient) -> list[dict]:
    """Discover fresh Solana tokens from Jupiter Tokens V2.

    MT-588: three discovery endpoints per cycle — /toporganicscore/5m,
    /recent, and /toptrending/5m — deduplicated by mint address before
    screening. 250ms spacing between the three calls (Developer tier, 10 RPS).
    """
    tokens_by_mint: dict[str, dict] = {}
    endpoints = (
        ("/toporganicscore/5m", {"limit": 100}),
        ("/recent", {"limit": 30}),
        ("/toptrending/5m", {"limit": 100}),
    )

    for index, (path, params) in enumerate(endpoints):
        try:
            response = await http.get(
                f"{JUPITER_API_BASE}{path}",
                params=params,
                headers=JUPITER_HEADERS,
                timeout=15.0,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                log.warning("Jupiter %s returned a non-list response", path)
                continue
            log.debug("Jupiter %s fetched %d tokens", path, len(payload))
            for token in payload:
                mint = token.get("id") if isinstance(token, dict) else None
                if isinstance(mint, str) and mint:
                    tokens_by_mint[mint] = token
        except Exception as exc:
            log.warning("Jupiter %s error: %s", path, exc)
        if index < len(endpoints) - 1:
            await asyncio.sleep(0.25)

    now_ms = time.time() * 1000
    now_epoch = time.time()
    now_mono = time.monotonic()
    candidates = []
    for token in tokens_by_mint.values():
        first_pool = token.get("firstPool") or {}
        created_at = first_pool.get("createdAt")
        if not isinstance(created_at, str) or not created_at:
            continue
        try:
            created_timestamp = int(datetime.fromisoformat(created_at).timestamp() * 1000)
        except ValueError:
            log.debug("Jupiter invalid firstPool.createdAt mint=%s", token["id"][:16])
            continue
        age_minutes = max(0.0, (now_ms - created_timestamp) / 60_000)
        if age_minutes > SOURCE_MAX_AGE_MINUTES:
            continue

        stats_1h = token.get("stats1h") or {}
        buys = stats_1h.get("numBuys", 0) or 0
        sells = stats_1h.get("numSells", 0) or 0
        # MT-560: record the first time we see this mint in any API response.
        # Used for DISCOVERY_LAG (how old a token is when we first learn of it).
        first_seen_epoch.setdefault(token["id"], now_epoch)
        first_seen_mono.setdefault(token["id"], now_mono)
        candidates.append({
            "mint": token["id"],
            "ticker": token.get("symbol", ""),
            "token_source": classify_token_source(token),
            # MT-588: raw Jupiter token record for graduation / pool-depth
            # gates (bonding curve vs PumpSwap/Raydium, USD liquidity).
            "token": token,
            "usd_market_cap": token.get("mcap") or token.get("fdv") or 0,
            "created_timestamp": created_timestamp,
            "volume": (stats_1h.get("buyVolume", 0) or 0) + (stats_1h.get("sellVolume", 0) or 0),
            "txns": buys + sells,
            "buy_sell_ratio": buys / max(sells, 1),
            "liquidity": float(token.get("liquidity", 0) or 0),
            "source_age_minutes": age_minutes,
            "pair": _synthesize_pair_dict(token),
        })

    log.debug(
        "Jupiter discovery tokens=%d (all <= %.0fm)",
        len(candidates), SOURCE_MAX_AGE_MINUTES,
    )
    return candidates


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


async def _fetch_rugcheck_cached(
    rugcheck_client: RugCheckClient,
    mint: str,
) -> tuple[RugCheckResult, bool]:
    """Return (report, from_cache).

    MT-566: RugCheck reports are cached per token for RUGCHECK_CACHE_TTL_S.
    A fresh cache entry skips the API call entirely; only provider_status ==
    "ok" results are cached so transient errors retry on the next cycle.
    """
    global _rugcheck_cache_hits, _rugcheck_cache_misses
    cached = _rugcheck_cache.get(mint)
    if cached is not None and time.monotonic() - cached[0] < RUGCHECK_CACHE_TTL_S:
        _rugcheck_cache_hits += 1
        return cached[1], True
    _rugcheck_cache_misses += 1
    report = await rugcheck_client.fetch_report(mint)
    if report.provider_status == "ok":
        _rugcheck_cache[mint] = (time.monotonic(), report)
    else:
        _rugcheck_cache.pop(mint, None)
    return report, False


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
        # MT-588: pool-depth floor (replaces the $5K mcap floor) and the
        # graduated-token strength-score gate.
        "liquidity_pass": False,
        "score_pass": False,
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
    # MT-593: walk-forward validated mcap floor re-enforced at $5,100 (2 of 3
    # iterations found mcap >= ~$5.1K; the tuner grid extended below the 10th
    # percentile specifically to find this cut). The upper cap stays — an
    # over-cap mcap is still rejected.
    if mcap < MIN_MCAP_USD:
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} < ${MIN_MCAP_USD:.0f} floor"
        ), gates
    if mcap > MAX_MCAP_USD:
        return False, f"age={age_min:.1f}m mcap=${mcap:.0f} > ${MAX_MCAP_USD:.0f}", gates
    gates["mcap_pass"] = True

    # MT-588: pool-depth floor. Jupiter reports liquidity in USD; convert to
    # SOL with the cached SOL/USD price. Tokens with no liquidity data are
    # skipped outright, per the MT-588 spec.
    graduation = _token_graduation(coin.get("token"))
    on_bonding_curve = graduation == "bonding"
    pool_min_sol = POOL_MIN_SOL_BONDING if on_bonding_curve else POOL_MIN_SOL_GRADUATED
    liquidity_usd = float(coin.get("liquidity") or 0)
    sol_price_usd = await _get_sol_price_usd(http)
    pool_sol = liquidity_usd / sol_price_usd if liquidity_usd > 0 and sol_price_usd else None
    coin["pool_sol"] = pool_sol
    if pool_sol is None:
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL no_pool_liquidity"
            f" (liquidity_usd={liquidity_usd:.0f} sol_price={sol_price_usd})"
        ), gates
    if pool_sol < pool_min_sol:
        log.info("SKIP %s pool_depth=%.1f SOL below minimum", mint, pool_sol)
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL "
            f"pool_depth={pool_sol:.1f}SOL below minimum {pool_min_sol:.0f}SOL "
            f"(graduation={graduation})"
        ), gates
    gates["liquidity_pass"] = True

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

    # MT-588: graduated-token preference. Bonding-curve pools charge 1.25%
    # DEX fees vs 0.25% on PumpSwap/Raydium, so a still-on-curve token must be
    # a stronger signal (higher composite score) to justify the higher fee.
    # Graduated tokens use the normal (lower) threshold.
    strength_score = _candidate_strength_score(coin, age_min)
    coin["strength_score"] = strength_score
    score_threshold = MIN_SCORE_BONDING_CURVE if on_bonding_curve else MIN_SCORE_GRADUATED
    if strength_score >= score_threshold:
        gates["score_pass"] = True
    log.info(
        "GRADUATION mint=%s ticker=%s graduation=%s bonding_curve=%s "
        "score=%.1f threshold=%.1f score_pass=%s",
        mint[:16], coin.get("ticker", "?"), graduation, on_bonding_curve,
        strength_score, score_threshold, gates["score_pass"],
    )

    t_rug = time.monotonic()
    try:
        report, rug_from_cache = await _fetch_rugcheck_cached(rugcheck_client, mint)
    except Exception as exc:
        return False, (
            f"age={age_min:.1f}m mcap=${mcap:.0f} \u2192 FAIL RugCheck error: {exc}"
        ), gates
    if log.isEnabledFor(logging.DEBUG):
        rug_ms = int((time.monotonic() - t_rug) * 1000)
        log.debug(
            "GATE_TIMING mint=%s rugcheck=%dms cached=%s cache_hits=%d cache_misses=%d",
            mint[:16], rug_ms, rug_from_cache, _rugcheck_cache_hits, _rugcheck_cache_misses,
        )

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
    if not gates["score_pass"]:
        label = "curve" if on_bonding_curve else "grad"
        fail_reasons.append(
            f"score={strength_score:.1f}<{score_threshold:.0f}({label})",
        )
    if not gates["holder_pass"] and report.found and report.top_holder_pct is not None:
        _, hard_holder = _age_holder_tier(age_min)
        fail_reasons.append(f"top10={report.top_holder_pct:.1f}%>={hard_holder}%")
    if not gates["creator_pass"]:
        fail_reasons.append("creator_holdings>0")
    if not gates["rugcheck_pass"] and report.found:
        if report.mint_authority_revoked is False:
            fail_reasons.append("mint_authority")
        if report.freeze_authority_revoked is False:
            fail_reasons.append("freeze_authority")

    # Overall pass = all critical gates + not rug-failed
    all_pass = (
        gates["age_pass"]
        and gates["mcap_pass"]
        and gates["liquidity_pass"]
        and gates["score_pass"]
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
        "liquidity": coin.get("pool_sol"), "score": coin.get("strength_score"),
        "txns": int(h1.get("buys") or 0) + int(h1.get("sells") or 0),
        "volume": (pair.get("volume") or {}).get("h1"),
        "buy_sell": coin.get("buy_sell_ratio"),
        "rugcheck": getattr(report, "provider_status", "skip"),
        "holder": getattr(report, "top_holder_pct", None),
        "creator": _extract_creator_pct(report) if report is not None else None,
    }
    sequence = (
        ("age_pass", "age"), ("mcap_pass", "mcap"),
        ("liquidity_pass", "liquidity"), ("score_pass", "score"),
        ("txn_pass", "txns"), ("volume_pass", "volume"),
        ("vol_mcap_pass", "volume"),
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
    timing: dict | None = None,
    pool_sol: float | None = None,
) -> str | None:
    from src.core.database import has_losing_close, record_entry_skip

    # MT-560: pipeline timing. scan_loop seeds t_detect/t_gate_pass; this
    # function stamps the remaining steps and logs one LATENCY line per entry.
    timing = timing if timing is not None else {}
    t0 = timing.setdefault("t_detect", time.monotonic())
    t_gate = timing.setdefault("t_gate_pass", time.monotonic())

    if len(await manager.get_all_open(mode="paper")) >= MAX_OPEN:
        log.warning("SKIP %s ticker=%s — strategy capacity reached", mint[:16], ticker)
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=capacity", mint[:16])
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
        log.debug(
            "DEBUG ENTRY_EVAL mint=%s result=rejected reason=time_gate_utc_hour=%d",
            mint[:16], utc_now.hour,
        )
        return None

    if utc_now.weekday() in BLOCKED_WEEKDAYS:
        log.warning("SKIP: Wednesday blocked — mint=%s ticker=%s", mint[:16], ticker)
        try:
            await record_entry_skip(
                db_path, strategy="B", mint_address=mint, ticker=ticker,
                gate="time_gate", reason=f"weekday={utc_now.weekday()} (Wednesday)",
            )
        except Exception as exc:
            log.debug("candidate_log write failed (non-fatal): %s", exc)
        log.debug(
            "DEBUG ENTRY_EVAL mint=%s result=rejected reason=weekday_blocked=%d",
            mint[:16], utc_now.weekday(),
        )
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
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=repeat_loser", mint[:16])
        return None

    if utc_now.weekday() == 5:
        log.info("Saturday — halving position size for %s (%s)", mint[:16], ticker)
        size_multiplier *= SATURDAY_SIZE_MULTIPLIER

    existing = await manager.get_position(mint, mode="paper")
    if existing is not None:
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=already_open", mint[:16])
        return None

    price = await mark_provider.get_current_price(mint)
    timing["t_quote"] = time.monotonic()
    if price is None or price <= 0:
        log.warning("SKIP %s ticker=%s \u2014 no valid DexScreener price", mint[:16], ticker)
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=no_price", mint[:16])
        return None

    # MT-588: tiered slippage by pool SOL depth (MT-590: thin tier relaxed to
    # <5 SOL). A depth below the thin tier is too thin to trade — skip instead
    # of entering with loose slippage. screen_coin already passed the
    # pool-depth floor (10/25 SOL), so this only rejects tokens whose depth
    # went stale or vanished.
    slippage_bps = _slippage_bps_for_pool(pool_sol)
    if slippage_bps is None:
        log.warning(
            "SKIP %s ticker=%s pool_depth=%s SOL below minimum (too thin for tiered slippage)",
            mint[:16], ticker, f"{pool_sol:.1f}" if pool_sol is not None else "N/A",
        )
        log.debug(
            "DEBUG ENTRY_EVAL mint=%s result=rejected reason=pool_depth=%s SOL below thin tier",
            mint[:16], f"{pool_sol:.1f}" if pool_sol is not None else "N/A",
        )
        return None
    log.info(
        "SLIPPAGE mint=%s ticker=%s pool_sol=%s slippage_bps=%d (tiered)",
        mint[:16], ticker, f"{pool_sol:.1f}" if pool_sol is not None else "N/A",
        slippage_bps,
    )
    log.debug(
        "DEBUG ENTRY_EVAL mint=%s result=accepted reason=all_gates_passed pool_sol=%s slippage_bps=%d",
        mint[:16], f"{pool_sol:.1f}" if pool_sol is not None else "N/A", slippage_bps,
    )

    size_sol = PAPER_SIZE_SOL * size_multiplier
    timing["t_signed"] = time.monotonic()
    timing["t_sent"] = time.monotonic()
    try:
        trade = await adapter.execute_swap(mint, Side.BUY, size_sol, slippage_bps)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s \u2014 execute_swap failed: %s", mint[:16], ticker, exc)
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=execute_swap_failed", mint[:16])
        return None
    t_swap_end = time.monotonic()

    if trade is None:
        log.warning("SKIP %s ticker=%s \u2014 execute_swap returned None", mint[:16], ticker)
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=execute_swap_none", mint[:16])
        return None

    # MT-593: a simulated swap that filled 0 tokens (price lookup failed at
    # swap time) must not open a position — a 0-token OPEN position can never
    # be closed by _adapter_close's sol_out<=0 guard and would hold a capacity
    # slot forever (5 such zombies blocked all entries on 2026-08-19).
    if trade.token_amount is None or trade.token_amount <= 0:
        log.warning(
            "SKIP %s ticker=%s \u2014 execute_swap filled 0 tokens (price=%s)",
            mint[:16], ticker, trade.price_sol,
        )
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=zero_token_fill", mint[:16])
        return None

    if isinstance(pair, dict):
        metadata = dict(trade.metadata or {})
        metadata.update(_pair_metadata(pair))
        trade.metadata = metadata

    try:
        await record_trade(db_path, trade)
    except Exception as exc:
        log.warning("SKIP %s ticker=%s \u2014 record_trade failed: %s", mint[:16], ticker, exc)
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=record_trade_failed", mint[:16])
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
        log.debug("DEBUG ENTRY_EVAL mint=%s result=rejected reason=open_position_failed", mint[:16])
        return None

    timing["t_confirmed"] = time.monotonic()

    # MT-560: per-trade end-to-end latency. Deltas are wall-clock between the
    # consecutive pipeline steps. In paper mode sign/send are simulated (~0ms)
    # and the swap block is the paper record; in live mode the Jupiter swap
    # client's internal quote/sign/send/confirm phases are NOT split here (the
    # swap integration is deliberately untouched), so `send` absorbs the whole
    # adapter.execute_swap duration (quote + sign + submit + confirm poll).
    t_quote = timing.get("t_quote", t_gate)
    t_signed = timing.get("t_signed", t_quote)
    t_sent = timing.get("t_sent", t_signed)
    t_confirmed = timing["t_confirmed"]
    log.info(
        "LATENCY mint=%s detect=%dms gates=%dms quote=%dms sign=%dms send=%dms confirm=%dms total=%dms",
        mint[:16],
        0,
        int((t_gate - t0) * 1000),
        int((t_quote - t_gate) * 1000),
        int((t_signed - t_quote) * 1000),
        int((t_sent - t_signed) * 1000) + int((t_swap_end - t_sent) * 1000),
        int((t_confirmed - t_swap_end) * 1000),
        int((t_confirmed - t0) * 1000),
    )

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
    adapter: ExecutionAdapter | None = None,
) -> bool:
    """Re-mark open positions and close on stops; True if any position is in
    the danger zone (below 95% of entry) so the caller polls at 5s."""
    danger = False
    positions = await manager.get_all_open(mode="paper")
    for pos in positions:
        # MT-593: zombie positions — a 0-token fill can never produce a
        # positive sol_out, so _adapter_close refuses to close it and the
        # slot is held forever. Close it outright (nothing to sell) to free
        # the capacity slot.
        if pos.token_amount is None or pos.token_amount <= 0:
            log.warning(
                "ZOMBIE CLOSE mint=%s entry_trade=%s token_amount=%s \u2014 closing 0-token position to free slot",
                pos.mint_address[:16], pos.entry_trade_id[:8], pos.token_amount,
            )
            await manager.close_position(pos.mint_address, mode="paper")
            continue
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
            trade = await _adapter_close(pos, close_price, close_reason, db_path, adapter)
            if trade is None:
                log.error(
                    "CLOSE FAILED [%s]: mint=%s — position left open",
                    close_reason, pos.mint_address[:16],
                )
                continue
            closed = await manager.close_position(pos.mint_address, close_price, mode="paper", peak_price_sol=peak)
            # AUTO-TUNER PAUSED — oscillating, not converging. See MT-537.
            # if gate_tuner is not None and await gate_tuner.maybe_tune():
            #     log.info("Auto-tuned Strategy B gates: %s", json.dumps(gate_tuner.thresholds.as_dict()))
            pnl_pct = ((close_price - pos.entry_price_sol) / pos.entry_price_sol) * 100 if pos.entry_price_sol else 0.0
            # MT-584: adjusted_pnl_sol = raw realized PnL minus estimated
            # round-trip execution costs (priority fee + dex fee + slippage).
            adjusted_pnl = closed.adjusted_pnl_sol if closed is not None else None
            log.info(
                "CLOSE [%s]: mint=%s entry=%.8f close=%.8f raw_pnl=%+.6f adjusted_pnl=%s SOL",
                close_reason, pos.mint_address[:16], pos.entry_price_sol, close_price,
                closed.realized_pnl_sol if closed is not None else 0.0,
                f"{adjusted_pnl:+.6f}" if adjusted_pnl is not None else "n/a",
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


async def _adapter_close(
    pos,
    close_price: float,
    reason: str,
    db_path: Path,
    adapter: ExecutionAdapter | None = None,
) -> Trade | None:
    import uuid

    # MT-544: in live mode the close is a real Jupiter sell; paper/shadow mode
    # keeps the existing simulated record path unchanged. A failed live sell
    # returns None so the position stays open for retry.
    if adapter is not None and adapter.mode == "live":
        try:
            live_trade = await adapter.sell(pos.mint_address, pos.token_amount)
        except Exception as exc:
            log.error("LIVE SELL mint=%s reason=%s failed: %s", pos.mint_address[:16], reason, exc)
            return None
        if live_trade.metadata is None:
            live_trade.metadata = {}
        live_trade.metadata["close_reason"] = reason
        await record_trade(db_path, live_trade)
        _fire_shadow_task(_shadow_sell_quote(pos, close_price, db_path))
        return live_trade

    token_remaining = pos.token_amount
    sol_out = token_remaining * close_price
    if sol_out <= 0:
        log.warning("SKIP close %s: sol_out=0", pos.mint_address)
        return None
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

def _first_failed_gate(gates: dict[str, bool]) -> str:
    """First failing gate name from a screen gates dict (MT-563)."""
    for gate in (
        "age_pass", "mcap_pass", "liquidity_pass", "score_pass",
        "txn_pass", "volume_pass", "vol_mcap_pass",
        "buy_sell_pass", "rugcheck_pass", "holder_pass", "creator_pass",
    ):
        if not gates.get(gate, True):
            return gate.removesuffix("_pass")
    return "none"


def _log_lag_report() -> None:
    """Log the MT-563 DISCOVERY_LAG_REPORT summary block (last 100 samples)."""
    stats = sorted(_lag_window)
    if not stats:
        return
    n = len(stats)
    buckets = (
        ("<5s", lambda s: s < 5),
        ("5-10s", lambda s: 5 <= s < 10),
        ("10-20s", lambda s: 10 <= s < 20),
        ("20-30s", lambda s: 20 <= s < 30),
        ("30-60s", lambda s: 30 <= s < 60),
        ("60s+", lambda s: s >= 60),
    )
    lines = [f"DISCOVERY_LAG_REPORT n={n}"]
    for label, predicate in buckets:
        count = sum(1 for s in stats if predicate(s))
        lines.append(f"  {label}: {count:2d} ({100.0 * count / n:3.0f}%)")
    median = statistics.median(stats)
    p95 = stats[min(n - 1, int(0.95 * n) - 1)]
    lines.append(
        f"  min={stats[0]:.0f}s avg={sum(stats) / n:.0f}s "
        f"median={median:.0f}s p95={p95:.0f}s max={stats[-1]:.0f}s",
    )
    by_source = " ".join(
        f"{source}={statistics.median(sorted(vals)):.0f}s"
        for source, vals in sorted(_lag_by_source.items())
        if vals
    )
    lines.append(f"  By source: {by_source}")
    for line in lines:
        log.info("%s", line)


async def scan_loop(
    mark_provider: DexScreenerPriceProvider,
    adapter: PaperExecutionAdapter,
    manager: PositionManager,
    db_path: Path,
    tracked_wallets: list | None = None,
    test_mode: bool = False,
) -> None:
    global seen_mints, cycle_number
    if tracked_wallets is None:
        tracked_wallets = []
    async with httpx.AsyncClient() as http:
        while True:
            now_ts = time.time()
            expired = [m for m, t in seen_mints.items() if now_ts - t > SEEN_MINTS_TTL]
            for m in expired:
                del seen_mints[m]
            # MT-566: prune stale per-mint persistence-throttle entries (1h TTL
            # matches seen_mints; candidates are only eligible while < 22m old).
            for m, t in list(_last_candidate_log.items()):
                if now_ts - t > SEEN_MINTS_TTL:
                    del _last_candidate_log[m]
            if expired:
                log.info("Expired %d stale seen_mints entries", len(expired))
            try:
                cycle_start = time.monotonic()
                cycle_number += 1
                log.debug("--- Strategy B Scan (cycle %d) ---", cycle_number)
                log.debug("whale tracker disabled — re-enable when Helius integration is built into entry pipeline.")
                open_positions = await manager.get_all_open(mode="paper")
                log.debug("Open positions: %d / %d", len(open_positions), MAX_OPEN)

                detailed = {
                    "total": 0,
                    "age_pass": 0,
                    "mcap_pass": 0,
                    "liquidity_pass": 0,
                    "score_pass": 0,
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
                candidates = await fetch_candidates_jupiter(http)
                detailed["total"] = len(candidates)
                if candidates:

                    for coin in candidates:
                        ticker = coin["ticker"]
                        mint = coin["mint"]

                        # MT-584: dedup — a mint evaluated this hour (passed
                        # or failed screening) is skipped on subsequent cycles
                        # until the TTL expires. INFO once per mint so the
                        # dedup is verifiable, DEBUG thereafter to avoid
                        # flooding the log at the fast cadence.
                        if mint in seen_mints:
                            if mint not in _dedup_skip_logged:
                                _dedup_skip_logged.add(mint)
                                log.info("skipping %s (%s), already evaluated", mint[:8], ticker)
                            else:
                                log.debug("skipping %s (%s), already evaluated", mint[:8], ticker)
                            continue

                        # MT-566: no screening cooldown — RugCheck is cached per
                        # token, so gate re-evaluation runs every cycle (~0ms)
                        # and a token enters on the cycle where it becomes
                        # eligible (fixes the ~46s `gates` latency from the
                        # MT-560 cooldown). candidate_log inserts and the
                        # SCREEN log line stay throttled per mint so DB/log
                        # volume stays bounded at the fast cadence; a passing
                        # mint always persists because its candidate row is
                        # required for entry marking.
                        passed, reason, gates = await screen_coin(coin, http, _rugcheck)
                        # MT-584: mark evaluated (pass or fail) so the dedup
                        # check above skips it on subsequent cycles.
                        seen_mints[mint] = now_ts
                        last_persisted = _last_candidate_log.get(mint, 0.0)
                        if now_ts - last_persisted >= SCREEN_LOG_COOLDOWN_S or passed:
                            _last_candidate_log[mint] = now_ts
                            log.info("SCREEN %s (%s): %s", ticker, mint[:8], reason)
                            candidate_id = await log_candidate(db_path, coin, gates, reason)
                        else:
                            candidate_id = None
                            log.debug(
                                "SCREEN %s (%s): %s (persist throttled)",
                                ticker, mint[:8], reason,
                            )

                        # MT-563: discovery-lag telemetry — how old is this token
                        # (per its firstPool.createdAt) when we first saw it in a
                        # Jupiter API response? Recorded once per mint per process
                        # for every screened candidate, passed or rejected, with
                        # the first failing gate as the "why" on rejection.
                        if mint not in _lag_logged:
                            _lag_logged.add(mint)
                            created_ms = coin.get("created_timestamp")
                            detected_epoch = first_seen_epoch.get(mint, now_ts)
                            if isinstance(created_ms, (int, float)) and created_ms > 0:
                                lag_s = max(0.0, detected_epoch - created_ms / 1000)
                                source = coin.get("token_source", "unknown")
                                _lag_window.append(lag_s)
                                _lag_by_source.setdefault(source, deque(maxlen=100)).append(lag_s)
                                try:
                                    await record_discovery_lag(
                                        db_path,
                                        mint_address=mint,
                                        token_source=source,
                                        created_at=datetime.fromtimestamp(
                                            created_ms / 1000, tz=UTC,
                                        ).isoformat(),
                                        detected_at=datetime.fromtimestamp(
                                            detected_epoch, tz=UTC,
                                        ).isoformat(),
                                        lag_seconds=lag_s,
                                        passed_gates=passed,
                                    )
                                except Exception as exc:
                                    log.warning("discovery_lag persist failed mint=%s: %s", mint[:8], exc)
                                log.info(
                                    "DISCOVERY_LAG mint=%s source=%s created=%d detected=%d lag=%.0fs gates=%s",
                                    mint, source, int(created_ms / 1000), int(detected_epoch), lag_s,
                                    "PASS" if passed else f"FAIL:{_first_failed_gate(gates)}",
                                )
                                if len(_lag_window) % _lag_summary_every == 0:
                                    _log_lag_report()

                        # Aggregate per-gate diagnostics
                        for gk in ("age_pass", "mcap_pass", "liquidity_pass", "score_pass",
                                   "txn_pass", "volume_pass", "vol_mcap_pass",
                                   "buy_sell_pass", "rugcheck_pass", "holder_pass"):
                            if gates.get(gk):
                                detailed[gk] += 1
                        if gates.get("low_fees_pass") or gates.get("low_fees_warn"):
                            detailed["low_fees_warn_or_pass"] += 1

                        if not passed:
                            # Identify the main blocker from the reason string
                            blockers = ["liquidity_pass", "score_pass",
                                        "txn_pass", "volume_pass", "vol_mcap_pass",
                                        "low_fees_pass", "buy_sell_pass", "rugcheck_pass",
                                        "holder_pass", "creator_pass"]
                            for bk in blockers:
                                if not gates.get(bk, True):
                                    main_blocker_count[bk] = main_blocker_count.get(bk, 0) + 1
                                    break
                            continue

                        detailed["full_screen_pass"] += 1

                        # MT-560: per-trade pipeline timing baseline. t_detect is
                        # the first API sighting; t_gate_pass now. try_enter stamps
                        # quote/send/confirm and logs the LATENCY line on entry.
                        timing = {
                            "t_detect": first_seen_mono.get(mint, time.monotonic()),
                            "t_gate_pass": time.monotonic(),
                        }

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
                            pair=coin.get("pair"), timing=timing,
                            pool_sol=coin.get("pool_sol"),
                        )
                        if position_id:
                            await mark_strategy_candidate_entered(db_path, candidate_id, position_id)
                            detailed["entered"] += 1
                            log.info("ENTRY mint=%s ticker=%s", mint[:16], ticker)
                        slots_used = len(await manager.get_all_open(mode="paper"))
                        if slots_used >= MAX_OPEN:
                            break

                main_blocker = max(main_blocker_count, key=main_blocker_count.get) if main_blocker_count else "none"
                # MT-560: the per-cycle Gates line would flood the
                # log 30x/min — full line every GATES_LOG_EVERY cycles, DEBUG
                # between. The console print follows the same throttle.
                gates_summary = (
                    "Gates: total=%d age=%d mcap=%d pool=%d score=%d txns=%d vol=%d "
                    "vol/mcap=%d low_fees~=%d "
                    "b/s=%d rugcheck=%d holder=%d full_pass=%d "
                    "entry_attempts=%d entered=%d main_blocker=%s",
                    detailed["total"], detailed["age_pass"], detailed["mcap_pass"],
                    detailed["liquidity_pass"], detailed["score_pass"],
                    detailed["txn_pass"], detailed["volume_pass"], detailed["vol_mcap_pass"],
                    detailed["low_fees_warn_or_pass"], detailed["buy_sell_pass"],
                    detailed["rugcheck_pass"], detailed["holder_pass"],
                    detailed["full_screen_pass"],
                    detailed["entry_attempts"], detailed["entered"], main_blocker,
                )
                if cycle_number % GATES_LOG_EVERY == 0:
                    log.info(*gates_summary)
                else:
                    log.debug(*gates_summary)
                # MT-589: periodic visibility of the dynamic priority fee
                # (75th percentile of getRecentPrioritizationFees, 30s cache).
                # Logged every 100 cycles (~100s); also drives the Jito tip.
                if cycle_number % 100 == 0:
                    _dynamic_fee = _priority_fee_provider.cached_fee_lamports
                    log.info(
                        "dynamic_priority_fee: %s microlamports",
                        _dynamic_fee if _dynamic_fee is not None else "N/A (static fallback)",
                    )
                if cycle_number % GATES_LOG_EVERY == 0:
                    print(
                        f"Gates: {detailed['total']} pairs \u2192 "
                        f"{detailed['age_pass']} age \u2192 "
                        f"{detailed['mcap_pass']} mcap \u2192 "
                        f"{detailed['liquidity_pass']} pool_depth \u2192 "
                        f"{detailed['score_pass']} score \u2192 "
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
                        "Jupiter did not surface sufficiently qualified candidates."
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
    adapter: ExecutionAdapter | None = None,
) -> None:
    while True:
        cycle_start = time.monotonic()
        danger = await monitor_positions(manager, mark_provider, db_path, gate_tuner, adapter)
        elapsed = time.monotonic() - cycle_start
        interval = FAST_MONITOR_INTERVAL_S if danger else MONITOR_INTERVAL
        await asyncio.sleep(max(0.0, interval - elapsed))


async def priority_fee_loop(interval_s: float = 30.0) -> None:
    """MT-588: keep the dynamic priority fee warm and visible in the log.

    Refreshes the 75th-percentile fee from the RPC every 30s (matching the
    provider's own cache TTL) so the fee value is logged periodically even in
    paper mode, where swaps never query it. Read-only RPC call — safe.
    """
    await _priority_fee_provider.refresh()
    while True:
        await asyncio.sleep(interval_s)
        await _priority_fee_provider.refresh()


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
    from dotenv import load_dotenv

    load_dotenv()
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
    # MT-544: EXECUTION_MODE selects the execution adapter. paper = current
    # behavior (default), shadow = paper + Jupiter quotes (MT-538), live =
    # real Jupiter swaps via src/execution/live.py.
    execution_mode = os.environ.get("EXECUTION_MODE", "paper").strip().lower()
    if execution_mode == "live":
        from src.chain.jupiter_swap import JupiterSwapClient
        from src.execution.live import LiveExecutionAdapter

        # MT-588: the live swap client uses the dynamic RPC priority fee
        # (75th percentile of getRecentPrioritizationFees, 30s cache) instead
        # of a static constant. Falls back to Jupiter's priority level when
        # the lookup is unavailable.
        client = JupiterSwapClient(
            priority_fee_callback=_priority_fee_provider.get_fee_lamports,
        )
        adapter = LiveExecutionAdapter(reference_price_provider=mark_provider, client=client)
        log.info("Strategy B execution adapter: LIVE (Jupiter swap, dynamic priority fee)")
    else:
        adapter = PaperExecutionAdapter(price_provider=mark_provider)
        log.info("Strategy B execution adapter: PAPER (EXECUTION_MODE=%s)", execution_mode)
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
        await _priority_fee_provider.close()
        return
    await asyncio.gather(
        scan_loop(mark_provider, adapter, manager, db_path, tracked_wallets=tracked_wallets),
        monitor_loop(manager, mark_provider, db_path, gate_tuner, adapter),
        snapshot_loop(manager, mark_provider, db_path),
        priority_fee_loop(),
    )
    await _drain_shadow_tasks()
    await _shadow_client.close()
    await _priority_fee_provider.close()


if __name__ == "__main__":
    asyncio.run(main())

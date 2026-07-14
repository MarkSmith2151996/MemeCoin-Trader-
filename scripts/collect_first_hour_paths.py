"""Bounded first-hour candidate path collection.

Discovers up to 20 fresh Solana meme coin candidates via DexScreener
token-profiles API, captures initial and follow-up snapshots at ~1min,
~5min, ~15min, ~30min, ~60min, then produces artifact CSVs and a report.

Detached — no runtime, config, risk-policy, or trading-path changes.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import httpx

OUTPUT_DIR = Path("/mnt/c/Users/Big A/custodian-shared/memecoin-trader/pattern-collection")

DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"

MAX_CANDIDATES = 20
SNAPSHOT_DELAY_S = 0.25
HTTP_TIMEOUT_S = 10.0

# Target elapsed times for snapshot rounds (seconds from discovery)
SNAPSHOT_SCHEDULE = [0, 60, 300, 900, 1800, 3600]  # 0, 1m, 5m, 15m, 30m, 60m
ROUND_LABELS = ["discovery", "1min", "5min", "15min", "30min", "60min"]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class Candidate:
    """One tracked candidate with all its snapshots."""

    def __init__(self, mint_address: str, discovery_ts: float) -> None:
        self.mint_address = mint_address
        self.discovery_ts = discovery_ts
        self.pair_address: str | None = None
        self.dex_id: str | None = None
        self.symbol: str | None = None
        self.name: str | None = None
        self.snapshots: dict[int, dict] = {}  # round_index -> snapshot dict

    def record_snapshot(self, round_idx: int, data: dict) -> None:
        self.snapshots[round_idx] = data
        # Fill in static fields from first successful snapshot
        if self.pair_address is None:
            self.pair_address = data.get("pair_address")
            self.dex_id = data.get("dex_id")
            self.symbol = data.get("symbol")
            self.name = data.get("name")


# ---------------------------------------------------------------------------
# Snapshot fetch helpers
# ---------------------------------------------------------------------------

def _safe_float(v: object) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) and f >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _safe_str(v: object) -> str | None:
    return str(v).strip() if isinstance(v, str) and v.strip() else None


def _extract_nested(d: object, *keys: str) -> object:
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _price_change_field(pc: dict | None, period: str) -> float | None:
    """Extract a price-change percentage from the priceChange sub-dict."""
    if not isinstance(pc, dict):
        return None
    val = pc.get(period)
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def discover_candidates(profiles: list[dict]) -> list[dict]:
    """Extract Solana token addresses from the profiles API response."""
    seen: set[str] = set()
    candidates: list[dict] = []
    for entry in profiles:
        if not isinstance(entry, dict):
            continue
        chain = entry.get("chainId")
        if chain != "solana":
            continue
        addr = _safe_str(entry.get("tokenAddress"))
        if addr is None or addr in seen:
            continue
        seen.add(addr)
        # Try to extract pair address from the URL
        url = _safe_str(entry.get("url"))
        pair_addr = None
        if url and "/solana/" in url:
            pair_addr = url.rsplit("/solana/", 1)[-1].strip()
            if not pair_addr:
                pair_addr = None
        candidates.append({"mint_address": addr, "pair_url": pair_addr})
        if len(candidates) >= MAX_CANDIDATES:
            break
    return candidates


async def fetch_snapshot(
    client: httpx.AsyncClient,
    mint_address: str,
    caller_ts: float,
) -> dict:
    """Fetch a DexScreener token snapshot. Returns a dict with all fields
    populated (None for missing data)."""
    blank = {
        "mint_address": mint_address,
        "caller_timestamp": caller_ts,
    }
    try:
        resp = await client.get(DEXSCREENER_TOKEN_URL.format(mint=mint_address))
        resp.raise_for_status()
        payload = resp.json()
    except (httpx.HTTPError, ValueError):
        return {**blank, "provider_status": "http_error"}

    if not isinstance(payload, dict):
        return {**blank, "provider_status": "malformed_response"}

    pairs = payload.get("pairs")
    if not isinstance(pairs, list):
        return {**blank, "provider_status": "no_pairs"}

    solana_pairs = [
        p for p in pairs
        if isinstance(p, dict) and p.get("chainId") == "solana"
    ]
    if not solana_pairs:
        return {**blank, "provider_status": "no_solana_pairs"}

    # Pick the pair with highest liquidity
    best = max(
        solana_pairs,
        key=lambda p: _safe_float(_extract_nested(p, "liquidity", "usd")) or 0,
    )
    volume = best.get("volume") if isinstance(best.get("volume"), dict) else {}
    txns = best.get("txns") if isinstance(best.get("txns"), dict) else {}
    liquidity = best.get("liquidity") if isinstance(best.get("liquidity"), dict) else {}
    price_change = best.get("priceChange") if isinstance(best.get("priceChange"), dict) else {}
    base_token = best.get("baseToken") if isinstance(best.get("baseToken"), dict) else {}

    price_native = best.get("priceNative")
    price_sol = _safe_float(price_native)

    pair_created = best.get("pairCreatedAt")
    pair_created_ts = None
    if pair_created is not None:
        try:
            pair_created_ts = float(pair_created) / 1000.0
        except (TypeError, ValueError):
            pass

    return {
        "mint_address": mint_address,
        "caller_timestamp": caller_ts,
        "provider_status": "ok",
        "pair_address": _safe_str(best.get("pairAddress")),
        "dex_id": _safe_str(best.get("dexId")),
        "symbol": _safe_str(base_token.get("symbol")),
        "name": _safe_str(base_token.get("name")),
        "price_sol": price_sol,
        "price_usd": _safe_float(best.get("priceUsd")),
        "fdv_usd": _safe_float(best.get("fdv")),
        "liquidity_usd": _safe_float(liquidity.get("usd")),
        "volume_m5": _safe_float(volume.get("m5")),
        "volume_h1": _safe_float(volume.get("h1")),
        "volume_h24": _safe_float(volume.get("h24")),
        "buys_m5": _safe_float(_extract_nested(txns, "m5", "buys")),
        "sells_m5": _safe_float(_extract_nested(txns, "m5", "sells")),
        "buys_h1": _safe_float(_extract_nested(txns, "h1", "buys")),
        "sells_h1": _safe_float(_extract_nested(txns, "h1", "sells")),
        "buys_h24": _safe_float(_extract_nested(txns, "h24", "buys")),
        "sells_h24": _safe_float(_extract_nested(txns, "h24", "sells")),
        "price_change_m5_pct": _price_change_field(price_change, "m5"),
        "price_change_h1_pct": _price_change_field(price_change, "h1"),
        "price_change_h6_pct": _price_change_field(price_change, "h6"),
        "price_change_h24_pct": _price_change_field(price_change, "h24"),
        "pair_created_at_s": pair_created_ts,
    }


def compute_buy_sell_ratio(buys: float | None, sells: float | None) -> float | None:
    if buys is None or sells is None:
        return None
    if sells <= 0:
        return None  # undefined
    if buys <= 0:
        return 0.0
    return round(buys / sells, 4)


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_long_csv(candidates: list[Candidate], path: Path) -> int:
    """Write one row per candidate per observation round."""
    columns = [
        "mint_address", "symbol", "name", "pair_address", "dex_id",
        "round_label", "round_index", "target_elapsed_s", "actual_elapsed_s",
        "price_sol", "price_usd", "fdv_usd", "liquidity_usd",
        "volume_m5", "volume_h1", "volume_h24",
        "buys_m5", "sells_m5", "buys_h1", "sells_h1", "buys_h24", "sells_h24",
        "buy_sell_ratio_m5", "buy_sell_ratio_h1",
        "price_change_m5_pct", "price_change_h1_pct",
        "price_change_h6_pct", "price_change_h24_pct",
        "pair_age_at_discovery_s", "provider_status",
    ]
    lines = [",".join(columns)]
    row_count = 0
    for cand in candidates:
        discovery_ts = cand.discovery_ts
        pair_created = None
        discovery_data = cand.snapshots.get(0, {})
        pair_created = discovery_data.get("pair_created_at_s")
        pair_age_s = None
        if pair_created is not None:
            pair_age_s = round(discovery_ts - pair_created, 1)

        for rnd in sorted(cand.snapshots.keys()):
            snap = cand.snapshots[rnd]
            actual_elapsed = round(snap.get("caller_timestamp", discovery_ts) - discovery_ts, 1)
            target_elapsed = SNAPSHOT_SCHEDULE[rnd] if rnd < len(SNAPSHOT_SCHEDULE) else 0
            buys_m5 = snap.get("buys_m5")
            sells_m5 = snap.get("sells_m5")
            buys_h1 = snap.get("buys_h1")
            sells_h1 = snap.get("sells_h1")
            vals = [
                cand.mint_address,
                cand.symbol or "",
                cand.name or "",
                cand.pair_address or "",
                cand.dex_id or "",
                ROUND_LABELS[rnd] if rnd < len(ROUND_LABELS) else f"round_{rnd}",
                str(rnd),
                str(target_elapsed),
                str(actual_elapsed),
                _csv_val(snap.get("price_sol")),
                _csv_val(snap.get("price_usd")),
                _csv_val(snap.get("fdv_usd")),
                _csv_val(snap.get("liquidity_usd")),
                _csv_val(snap.get("volume_m5")),
                _csv_val(snap.get("volume_h1")),
                _csv_val(snap.get("volume_h24")),
                _csv_val(buys_m5),
                _csv_val(sells_m5),
                _csv_val(buys_h1),
                _csv_val(sells_h1),
                _csv_val(snap.get("buys_h24")),
                _csv_val(snap.get("sells_h24")),
                _csv_val(compute_buy_sell_ratio(buys_m5, sells_m5)),
                _csv_val(compute_buy_sell_ratio(buys_h1, sells_h1)),
                _csv_val(snap.get("price_change_m5_pct")),
                _csv_val(snap.get("price_change_h1_pct")),
                _csv_val(snap.get("price_change_h6_pct")),
                _csv_val(snap.get("price_change_h24_pct")),
                _csv_val(pair_age_s),
                snap.get("provider_status", ""),
            ]
            escaped = []
            for v in vals:
                if v is None:
                    escaped.append("")
                elif isinstance(v, str) and ("," in v or '"' in v):
                    escaped.append(f'"{v.replace(chr(34), chr(34)+chr(34))}"')
                else:
                    escaped.append(str(v))
            lines.append(",".join(escaped))
            row_count += 1

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Long CSV: {path}  ({row_count} data rows)", flush=True)
    return row_count


def write_summary_csv(candidates: list[Candidate], path: Path) -> int:
    """One row per candidate with path metrics."""
    columns = [
        "mint_address", "symbol", "name", "pair_address", "dex_id",
        "price_sol_at_discovery", "price_usd_at_discovery",
        "liquidity_usd_at_discovery", "fdv_usd_at_discovery",
        "pair_age_s_at_discovery",
        "return_1min_pct", "return_5min_pct", "return_15min_pct",
        "return_30min_pct", "return_60min_pct",
        "max_observed_gain_pct", "max_observed_drawdown_pct",
        "time_to_peak_s", "final_return_pct",
        "reached_25pct", "reached_50pct", "reached_100pct",
        "became_unavailable", "snapshot_count",
        "outcome_label",
    ]
    lines = [",".join(columns)]
    row_count = 0
    for cand in candidates:
        discovery = cand.snapshots.get(0, {})
        disc_price = discovery.get("price_sol")
        disc_liquidity = discovery.get("liquidity_usd")

        # Compute returns at each horizon
        returns: dict[int, float | None] = {}
        all_prices: list[tuple[float, float]] = []  # (elapsed_s, price_sol)
        for rnd in sorted(cand.snapshots.keys()):
            snap = cand.snapshots[rnd]
            price = snap.get("price_sol")
            elapsed = snap.get("caller_timestamp", cand.discovery_ts) - cand.discovery_ts
            if price is not None and math.isfinite(price) and price > 0:
                all_prices.append((elapsed, price))

        # Compute return for each horizon
        final_return = None
        for horizon_s, rnd in zip(SNAPSHOT_SCHEDULE, sorted(cand.snapshots.keys())):
            snap = cand.snapshots.get(rnd)
            if snap is None:
                continue
            price = snap.get("price_sol")
            if disc_price is not None and disc_price > 0 and price is not None and price > 0:
                ret = (price / disc_price - 1.0) * 100.0
                returns[horizon_s] = round(ret, 2)
                if horizon_s == 3600:
                    final_return = round(ret, 2)

        # Max gain/drawdown from all priced snapshots
        max_gain_pct = None
        max_drawdown_pct = None
        time_to_peak_s = None
        if disc_price is not None and disc_price > 0 and all_prices:
            max_ratio = 0.0
            min_ratio = float("inf")
            peak_elapsed = 0.0
            for elapsed, price in all_prices:
                ratio = price / disc_price
                pct = (ratio - 1.0) * 100.0
                if ratio > max_ratio:
                    max_ratio = ratio
                    max_gain_pct = round(pct, 2)
                    peak_elapsed = elapsed
                if ratio < min_ratio:
                    min_ratio = ratio
                    max_drawdown_pct = round(pct, 2)
            time_to_peak_s = round(peak_elapsed, 1) if peak_elapsed > 0 else None

        # Milestone checks
        reached_25 = _check_milestone(all_prices, disc_price, 1.25)
        reached_50 = _check_milestone(all_prices, disc_price, 1.50)
        reached_100 = _check_milestone(all_prices, disc_price, 2.0)

        # Unavailable check
        became_unavailable = any(
            cand.snapshots.get(rnd, {}).get("provider_status") != "ok"
            for rnd in sorted(cand.snapshots.keys())
        )

        # Simple outcome label
        outcome = _label_outcome(
            final_return, max_gain_pct, max_drawdown_pct, disc_price, cand.snapshots,
        )

        vals = [
            cand.mint_address,
            cand.symbol or "",
            cand.name or "",
            cand.pair_address or "",
            cand.dex_id or "",
            _csv_val(disc_price),
            _csv_val(discovery.get("price_usd")),
            _csv_val(discovery.get("liquidity_usd")),
            _csv_val(discovery.get("fdv_usd")),
            _csv_val(discovery.get("pair_age_at_discovery_s")),
            _csv_val(returns.get(60)),
            _csv_val(returns.get(300)),
            _csv_val(returns.get(900)),
            _csv_val(returns.get(1800)),
            _csv_val(returns.get(3600)),
            _csv_val(max_gain_pct),
            _csv_val(max_drawdown_pct),
            _csv_val(time_to_peak_s),
            _csv_val(final_return),
            "1" if reached_25 else "0",
            "1" if reached_50 else "0",
            "1" if reached_100 else "0",
            "1" if became_unavailable else "0",
            str(len(cand.snapshots)),
            outcome,
        ]
        escaped = []
        for v in vals:
            if v is None:
                escaped.append("")
            elif isinstance(v, str) and ("," in v or '"' in v):
                escaped.append(f'"{v.replace(chr(34), chr(34)+chr(34))}"')
            else:
                escaped.append(str(v))
        lines.append(",".join(escaped))
        row_count += 1

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Summary CSV: {path}  ({row_count} data rows)", flush=True)
    return row_count


def _csv_val(v: object) -> str | None:
    if v is None:
        return None
    if isinstance(v, float):
        return f"{v:.8g}"
    return str(v)


def _check_milestone(
    prices: list[tuple[float, float]],
    disc_price: float | None,
    multiplier: float,
) -> bool:
    if disc_price is None or disc_price <= 0:
        return False
    target = disc_price * multiplier
    for _elapsed, price in prices:
        if price >= target:
            return True
    return False


def _label_outcome(
    final_return: float | None,
    max_gain: float | None,
    max_dd: float | None,
    disc_price: float | None,
    snapshots: dict[int, dict],
) -> str:
    """Simple deterministic outcome label for one candidate."""
    if disc_price is None or disc_price <= 0:
        return "unavailable"
    # Check if any snapshot after discovery returned data
    any_post_discovery_ok = any(
        s.get("provider_status") == "ok" for rnd, s in snapshots.items() if rnd > 0
    )
    if not any_post_discovery_ok:
        return "unavailable"

    if final_return is not None and final_return >= 50:
        return "strong_winner"
    if final_return is not None and final_return >= 15:
        return "moderate_winner"
    if max_gain is not None and max_gain >= 100:
        return "strong_winner"
    if max_gain is not None and max_gain >= 25:
        return "moderate_winner"
    if final_return is not None and final_return <= -50:
        return "loser"
    if max_dd is not None and max_dd <= -80:
        return "loser"
    if final_return is not None and abs(final_return) < 15:
        return "flat"
    return "loser"


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def write_report(candidates: list[Candidate], path: Path) -> None:
    from datetime import UTC, datetime

    lines: list[str] = []

    def p(text: str = "") -> None:
        lines.append(text)

    priced_at_discovery = sum(
        1 for c in candidates
        if c.snapshots.get(0, {}).get("provider_status") == "ok"
    )

    # Count outcomes
    outcomes: dict[str, int] = {}
    max_snapshots = 0
    total_priced_rows = 0
    for cand in candidates:
        lbl = _label_outcome(
            None, None, None,
            cand.snapshots.get(0, {}).get("price_sol"),
            cand.snapshots,
        )
        outcomes[lbl] = outcomes.get(lbl, 0) + 1
        max_snapshots = max(max_snapshots, len(cand.snapshots))
        # Count priced snapshots
        for snap in cand.snapshots.values():
            if snap.get("provider_status") == "ok" and snap.get("price_sol") is not None:
                total_priced_rows += 1

    p("# First-Hour Candidate Path Collection Report")
    p()
    p(f"**MT-447** — generated {datetime.now(UTC).isoformat()}")
    p()
    p("## Summary")
    p()
    p(f"- Candidates discovered: **{len(candidates)}**")
    p(f"- Priced at discovery: **{priced_at_discovery}**")
    p(f"- Total snapshot rows collected: **{sum(len(c.snapshots) for c in candidates)}**")
    p(f"- Priced snapshot rows: **{total_priced_rows}**")
    p(f"- Max snapshots per candidate: **{max_snapshots}**")
    p()

    if len(candidates) == 0:
        p("**No candidates discovered.** The DexScreener token-profiles API returned zero Solana tokens at query time.")
        p()
        p("## Discovery Source Details")
        p()
        p("Provider: `https://api.dexscreener.com/token-profiles/latest/v1`")
        p("Filter: `chainId == 'solana'`")
        p("Limit: 20")
        p("No filters applied beyond chain and availability.")
        p()
        path.write_text("\n".join(lines), encoding="utf-8")
        print(f"  Report: {path}  ({len(lines)} lines)", flush=True)
        return

    p("## Outcome Distribution")
    p()
    p("| Outcome | Count | % |")
    p("|---------|-------|---|")
    for lbl, cnt in sorted(outcomes.items(), key=lambda x: -x[1]):
        p(f"| {lbl} | {cnt} | {cnt/len(candidates)*100:.1f}% |")
    p()

    p("## Candidate Details")
    p()
    p("| Mint | Symbol | Dex | Disc. liq $ | Disc. FDV $ | Gain peak % | DD max % | Final return % | Outcome |")
    p("|------|--------|-----|-------------|------------|------------|---------|--------------|---------|")
    for cand in candidates:
        disc = cand.snapshots.get(0, {})
        disc_price = disc.get("price_sol")
        all_prices = []
        for rnd in sorted(cand.snapshots.keys()):
            snap = cand.snapshots[rnd]
            p2 = snap.get("price_sol")
            elapsed = snap.get("caller_timestamp", cand.discovery_ts) - cand.discovery_ts
            if p2 is not None and p2 > 0:
                all_prices.append((elapsed, p2))
        max_gain = None
        max_dd = None
        final_return = None
        if disc_price and disc_price > 0 and all_prices:
            max_r, min_r = 0.0, float("inf")
            for _e, p2 in all_prices:
                ratio = p2 / disc_price
                if ratio > max_r:
                    max_r = ratio
                if ratio < min_r:
                    min_r = ratio
            max_gain = round((max_r - 1) * 100, 1)
            max_dd = round((min_r - 1) * 100, 1)
        sixty = cand.snapshots.get(5, {})
        sp = sixty.get("price_sol")
        if disc_price and disc_price > 0 and sp and sp > 0:
            final_return = round((sp / disc_price - 1) * 100, 1)

        lbl = _label_outcome(
            final_return, max_gain, max_dd, disc_price, cand.snapshots,
        )
        p(f"| `{cand.mint_address[:12]}...` | {cand.symbol or '?'} | {cand.dex_id or '?'} | "
          f"{_fmt(disc.get('liquidity_usd'))} | {_fmt(disc.get('fdv_usd'))} | "
          f"{_fmt(max_gain)} | {_fmt(max_dd)} | {_fmt(final_return)} | {lbl} |")
    p()

    p("## Data Coverage and Provider Failures")
    p()
    p(f"Each candidate was targeted for {len(SNAPSHOT_SCHEDULE)} snapshot rounds "
      f"(discovery + {len(SNAPSHOT_SCHEDULE)-1} follow-ups over 60 minutes).")
    p()
    failure_counts: dict[str, int] = {}
    for cand in candidates:
        for snap in cand.snapshots.values():
            status = snap.get("provider_status", "missing")
            failure_counts[status] = failure_counts.get(status, 0) + 1
    p("| Provider status | Rows | % |")
    p("|---------------|------|---|")
    total_cells = sum(failure_counts.values())
    for status, cnt in sorted(failure_counts.items(), key=lambda x: -x[1]):
        p(f"| {status} | {cnt} | {cnt/total_cells*100:.1f}% |")
    p()

    # Coverage per round
    p("### Timing Coverage")
    p()
    p("| Round | Target elapsed | Actual range | Priced rows | Coverage % |")
    p("|-------|---------------|-------------|-----------|----------|")
    for rnd_idx, (target, label) in enumerate(zip(SNAPSHOT_SCHEDULE, ROUND_LABELS)):
        rows_in_round = [c for c in candidates if rnd_idx in c.snapshots]
        priced_in_round = sum(
            1 for c in rows_in_round
            if c.snapshots[rnd_idx].get("provider_status") == "ok"
        )
        elapsed_vals = [
            c.snapshots[rnd_idx].get("caller_timestamp", c.discovery_ts) - c.discovery_ts
            for c in rows_in_round
            if rnd_idx in c.snapshots
        ]
        min_elapsed = round(min(elapsed_vals), 1) if elapsed_vals else "-"
        max_elapsed = round(max(elapsed_vals), 1) if elapsed_vals else "-"
        cov_pct = len(rows_in_round) / max(len(candidates), 1) * 100
        p(f"| {label} | {target}s | {min_elapsed}–{max_elapsed}s | {priced_in_round}/{len(rows_in_round)} | {cov_pct:.0f}% |")
    p()

    p("## Observations")
    p()
    p("This is a single bounded crawl batch. The following observations are "
      "descriptive, not predictive trading rules.")
    p()
    p("- **Discovery source:** DexScreener token-profiles API gives recently created "
      "tokens, not necessarily all Solana new pairs. Some discovered tokens may "
      "already be minutes old by the time they appear in profiles.")
    p("- **Price volatility:** Meme coin prices can gap to zero between snapshots. "
      "A 1-minute polling interval may miss short-lived price spikes.")
    p("- **Liquidity data:** DexScreener reports liquidity USD from the best pair; "
      "this may differ from actual trade-able depth.")
    p("- **Provider availability:** DexScreener may return `no_pairs` or HTTP errors "
      "for any snapshot. Degraded states are recorded rather than substituted.")
    p("- **Outcome labels** use these rules:")
    p("  - `strong_winner`: +100% peak gain or +50% final return")
    p("  - `moderate_winner`: +25% peak gain or +15% final return")
    p("  - `flat`: final return within +/-15% of entry")
    p("  - `loser`: <= -50% final return or <= -80% drawdown")
    p("  - `unavailable`: no priced data at or after discovery")
    p()
    p("## Limitations")
    p()
    p("1. **Small sample.** This batch is at most 20 candidates. Patterns require "
      "multiple batches to become meaningful.")
    p("2. **Timing jitter.** API latency, rate limiting, and sequential fetching "
      "add jitter to snapshot times. Actual elapsed times are recorded in the CSV.")
    p("3. **No on-chain verification.** DexScreener data may lag or omit pairs "
      "that exist on-chain but aren't indexed.")
    p("4. **Survivorship within the hour.** A coin that becomes unavailable in the "
      "first hour may relist or regain DexScreener coverage later.")
    p("5. **No trade simulation.** Price path collection does not model slippage, "
      "fees, or execution feasibility.")
    p()

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Report: {path}  ({len(lines)} lines)", flush=True)


def _fmt(v: object) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        if abs(v) >= 1000:
            return f"{v:,.1f}"
        return f"{v:.4g}"
    return str(v)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("=== MT-447 First-Hour Path Collection ===", flush=True)
    start_wall = time.time()

    # Step 1: Discover candidates
    print("\n--- Discovery phase ---", flush=True)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
        try:
            profiles_resp = await client.get(DEXSCREENER_PROFILES_URL)
            profiles_resp.raise_for_status()
            profiles_data = profiles_resp.json()
        except (httpx.HTTPError, ValueError) as e:
            print(f"  Profiles API error: {e}", flush=True)
            profiles_data = []

    if not isinstance(profiles_data, list) or len(profiles_data) == 0:
        print("  No profiles returned. Writing empty report.", flush=True)
        candidates: list[Candidate] = []
        write_report(candidates, OUTPUT_DIR / "first_hour_collection_report.md")
        # Still write empty CSVs for consistency
        write_long_csv(candidates, OUTPUT_DIR / "first_hour_candidate_snapshots.csv")
        write_summary_csv(candidates, OUTPUT_DIR / "first_hour_candidate_summary.csv")
        print(f"\nTotal wall time: {time.time() - start_wall:.0f}s", flush=True)
        return

    raw_candidates = discover_candidates(profiles_data)
    print(f"  Discovered {len(raw_candidates)} Solana candidates", flush=True)

    # Step 2: Create Candidate objects and do initial snapshot
    candidates = []
    for rc in raw_candidates:
        cand = Candidate(rc["mint_address"], time.time())
        candidates.append(cand)

    print("\n--- Snapshot rounds ---", flush=True)

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
        for rnd_idx, (target_elapsed, label) in enumerate(
            zip(SNAPSHOT_SCHEDULE, ROUND_LABELS)
        ):
            if rnd_idx > 0:
                sleep_time = target_elapsed - (time.time() - start_wall)
                if sleep_time > 0:
                    print(f"  Waiting {sleep_time:.0f}s to {label} round...", flush=True)
                    await asyncio.sleep(sleep_time)
                else:
                    print(f"  Skipping wait (already past {label} target)", flush=True)

            actual_ts = time.time()
            print(f"  Round {rnd_idx} ({label}): fetching {len(candidates)} candidates...", flush=True)

            for cand in candidates:
                snap = await fetch_snapshot(client, cand.mint_address, actual_ts)
                cand.record_snapshot(rnd_idx, snap)
                await asyncio.sleep(SNAPSHOT_DELAY_S)

            # Summary for this round
            priced = sum(
                1 for c in candidates
                if rnd_idx in c.snapshots and c.snapshots[rnd_idx].get("provider_status") == "ok"
            )
            elapsed_actual = time.time() - start_wall
            print(f"    -> {priced}/{len(candidates)} priced at {elapsed_actual:.0f}s elapsed", flush=True)

    # Step 3: Write artifacts
    print("\n--- Writing artifacts ---", flush=True)
    long_path = OUTPUT_DIR / "first_hour_candidate_snapshots.csv"
    summary_path = OUTPUT_DIR / "first_hour_candidate_summary.csv"
    report_path = OUTPUT_DIR / "first_hour_collection_report.md"

    write_long_csv(candidates, long_path)
    write_summary_csv(candidates, summary_path)
    write_report(candidates, report_path)

    total_wall = time.time() - start_wall
    print(f"\nTotal wall time: {total_wall:.0f}s", flush=True)
    print("=== Collection complete ===", flush=True)


if __name__ == "__main__":
    asyncio.run(main())

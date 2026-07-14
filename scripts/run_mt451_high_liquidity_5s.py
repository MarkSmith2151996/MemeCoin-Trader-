"""Detached paper-only MT-451 high-liquidity 5-second paper comparison.

Uses DexScreener API fallback (token-profiles + token-boosts) per user approval.
Board: Solana, age <=1h, liquidity >$50K, market cap >$7K, volume >$1K, Trending 6H.
Exit params: MT-444 tuned (hard stop -20%, standard trail 8%, tightened +25%/5%).
Marks: every 5 seconds.
"""

from __future__ import annotations

import asyncio
import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from src.cli import _is_valid_price, force_paper_settings
from src.core.config import load_settings
from src.core.database import init_db, record_trade
from src.core.models import Side
from src.execution.paper import PaperExecutionAdapter
from src.execution.price_provider import DexScreenerPriceProvider
from src.monitoring.dashboard import resolve_db_path
from src.risk.funding_provider import HeliusFundingProvider
from src.risk.liquidity import LiquidityProbe
from src.risk.paper_momentum import evaluate_paper_new_pairs_momentum_evidence
from src.risk.rugcheck import RugCheckClient
from src.risk.scorer import DiscoveryRiskScorer, ReadOnlyHolderLookup
from src.strategy.momentum_trailing import MomentumTrailState, evaluate_momentum_trail
from src.strategy.position_manager import PositionManager

DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_BOOSTS_TOP_URL = "https://api.dexscreener.com/token-boosts/top/v1"
DEXSCREENER_BOOSTS_LATEST_URL = "https://api.dexscreener.com/token-boosts/latest/v1"

REPORT_DIR = Path("/mnt/c/Users/Big A/custodian-shared/memecoin-trader/paper-trade-reports")
REPORT_PATH = REPORT_DIR / "exact_rendered_high_liquidity_5s_batch.md"
CSV_PATH = REPORT_DIR / "exact_rendered_high_liquidity_5s_trade_log.csv"
CAPTURE_PATH = REPORT_DIR / "exact_rendered_board_capture.json"
HOLD_SECONDS = 30 * 60
MARK_INTERVAL_SECONDS = 5
AMOUNT_SOL = 0.01
MAX_ENTRIES = 3

BOARD_FILTERS = {
    "min_liquidity_usd": 50000,
    "min_market_cap_usd": 7000,
    "min_volume_h24_usd": 1000,
    "max_age_seconds": 3600,
}


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


async def fetch_source(client: httpx.AsyncClient, url: str, label: str) -> list[dict]:
    try:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def extract_mint_address(entry: dict) -> str | None:
    for key in ("tokenAddress", "address", "mintAddress"):
        val = entry.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


async def enrich_mint(client: httpx.AsyncClient, mint_address: str, token_url: str) -> dict | None:
    try:
        r = await client.get(token_url.format(mint_address=mint_address))
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    pairs = data.get("pairs")
    if not isinstance(pairs, list):
        return None
    solana_pairs = [p for p in pairs if isinstance(p, dict) and p.get("chainId") == "solana"]
    if not solana_pairs:
        return None
    best = max(solana_pairs, key=lambda p: _safe_float(p.get("liquidity"), {}).get("usd", 0) if isinstance(p.get("liquidity"), dict) else _safe_float(p.get("liquidity"), 0))
    return best


def pair_creation_age_seconds(pair: dict) -> float | None:
    raw = pair.get("pairCreatedAt")
    if raw is None:
        return None
    try:
        created = datetime.fromtimestamp(raw / 1000, tz=UTC)
        return (datetime.now(UTC) - created).total_seconds()
    except (TypeError, OSError):
        return None


def passes_board_filter(pair: dict) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    chain = pair.get("chainId")
    if chain != "solana":
        reasons.append(f"chain={chain}")
        return False, reasons
    liquidity = _safe_float(pair.get("liquidity"), {}).get("usd", 0) if isinstance(pair.get("liquidity"), dict) else _safe_float(pair.get("liquidity"), 0)
    if liquidity < BOARD_FILTERS["min_liquidity_usd"]:
        reasons.append(f"liquidity=${liquidity:.0f}<${BOARD_FILTERS['min_liquidity_usd']}")
    fdv = _safe_float(pair.get("fdv"), 0)
    if fdv < BOARD_FILTERS["min_market_cap_usd"]:
        reasons.append(f"fdv=${fdv:.0f}<${BOARD_FILTERS['min_market_cap_usd']}")
    volume = _safe_float(pair.get("volume"), {}).get("h24", 0) if isinstance(pair.get("volume"), dict) else _safe_float(pair.get("volume"), 0)
    if volume < BOARD_FILTERS["min_volume_h24_usd"]:
        reasons.append(f"vol=${volume:.0f}<${BOARD_FILTERS['min_volume_h24_usd']}")
    age = pair_creation_age_seconds(pair)
    if age is not None and age > BOARD_FILTERS["max_age_seconds"]:
        reasons.append(f"age={age:.0f}s>{BOARD_FILTERS['max_age_seconds']}s")
    elif age is None:
        reasons.append("age_unknown")
    else:
        pass
    return len(reasons) == 0, reasons


async def main() -> None:
    settings = force_paper_settings(load_settings())
    db_path = resolve_db_path(None)
    await init_db(db_path)
    manager = PositionManager(db_path, settings)
    provider = DexScreenerPriceProvider()
    token_url = provider._token_url
    all_candidates: list[dict] = []
    seen_mints: set[str] = set()
    source_stats: dict[str, int] = {}

    async with httpx.AsyncClient(timeout=10.0) as client:
        profiles_data = await fetch_source(client, DEXSCREENER_PROFILES_URL, "token-profiles")
        source_stats["token-profiles"] = len(profiles_data)
        boosts_top_data = await fetch_source(client, DEXSCREENER_BOOSTS_TOP_URL, "token-boosts-top")
        source_stats["token-boosts-top"] = len(boosts_top_data)
        boosts_latest_data = await fetch_source(client, DEXSCREENER_BOOSTS_LATEST_URL, "token-boosts-latest")
        source_stats["token-boosts-latest"] = len(boosts_latest_data)

        raw_mint_sources: dict[str, list[str]] = {}
        for entry in profiles_data:
            m = extract_mint_address(entry)
            if m:
                raw_mint_sources.setdefault(m, []).append("token-profiles")
        for entry in boosts_top_data:
            m = extract_mint_address(entry)
            if m:
                raw_mint_sources.setdefault(m, []).append("token-boosts-top")
        for entry in boosts_latest_data:
            m = extract_mint_address(entry)
            if m:
                raw_mint_sources.setdefault(m, []).append("token-boosts-latest")

        unique_mints = list(raw_mint_sources.keys())
        source_stats["unique_mints"] = len(unique_mints)

        for mint in unique_mints:
            if mint in seen_mints:
                continue
            seen_mints.add(mint)
            pair = await enrich_mint(client, mint, token_url)
            if pair is None:
                continue
            qualifies, reasons = passes_board_filter(pair)
            all_candidates.append({
                "mint": mint,
                "pair": pair,
                "qualifies": qualifies,
                "reasons": reasons,
                "sources": raw_mint_sources.get(mint, []),
                "age_seconds": pair_creation_age_seconds(pair),
            })

    qualifying = [c for c in all_candidates if c["qualifies"]]
    source_stats["total_pairs_found"] = len(all_candidates)
    source_stats["board_qualifying"] = len(qualifying)
    source_stats["filter_breakdown"] = {}
    for c in all_candidates:
        if not c["qualifies"]:
            for r in c["reasons"]:
                key = r.split("=")[0] if "=" in r else r
                source_stats["filter_breakdown"][key] = source_stats["filter_breakdown"].get(key, 0) + 1

    if len(qualifying) == 0:
        _write_empty_report(source_stats, all_candidates)
        return

    records: list[dict] = []
    blocked: dict[str, int] = {}
    active: list[dict] = []
    pass_count = quote_count = 0

    async with httpx.AsyncClient(timeout=5.0) as liquidity_client:
        scorer = DiscoveryRiskScorer(
            settings.risk,
            holder_lookup=ReadOnlyHolderLookup(timeout_s=5.0),
            rugcheck_client=RugCheckClient(timeout_s=5.0),
            funding_provider=HeliusFundingProvider(api_key=""),
            liquidity_probe=LiquidityProbe(timeout_s=5.0, client=liquidity_client),
            holder_policy_mode="strict",
            enable_holder_lookup=True,
            enable_funding_analysis=False,
        )
        for idx, candidate in enumerate(qualifying):
            mint = candidate["mint"]
            if len(active) >= MAX_ENTRIES:
                break
            pair_data = candidate["pair"]
            base_token = pair_data.get("baseToken")
            symbol = (base_token.get("symbol") if isinstance(base_token, dict) else None) or ""
            name = (base_token.get("name") if isinstance(base_token, dict) else None) or ""
            pair_addr = pair_data.get("pairAddress") or ""
            pair_created = pair_data.get("pairCreatedAt")

            evidence = evaluate_paper_new_pairs_momentum_evidence(
                None,
                None,
                top10_holder_pct=None,
                ui_age_minutes=None,
                ui_max_age_minutes=None,
            )
            if not evidence.eligible:
                for label in evidence.reason_labels:
                    if label.startswith("paper_momentum_blocked_"):
                        blocked[label] = blocked.get(label, 0) + 1
                continue
            pass_count += 1

            quote = await provider.get_price_with_diagnostic(mint)
            if not _is_valid_price(quote.price_sol):
                blocked[f"quote_{quote.reason}"] = blocked.get(f"quote_{quote.reason}", 0) + 1
                continue
            quote_count += 1

            if await manager.get_position(mint, mode="paper") is not None:
                continue

            execution = PaperExecutionAdapter(price_lookup={mint: quote.price_sol})
            try:
                trade = await execution.execute_swap(mint, Side.BUY, AMOUNT_SOL, slippage_bps=settings.position.default_slippage_bps)
            finally:
                await execution.close()
            await record_trade(db_path, trade)
            signal = None
            await manager.open_position(trade, signal)
            now = datetime.now(UTC)
            entry = float(quote.price_sol)
            record = {
                "mint": mint,
                "pair": pair_addr,
                "symbol": symbol,
                "name": name,
                "entry": entry,
                "opened": now,
                "state": MomentumTrailState(entry, entry),
                "marks": [],
                "liquidity_usd": _safe_float(pair_data.get("liquidity"), {}).get("usd", 0) if isinstance(pair_data.get("liquidity"), dict) else 0,
                "fdv_usd": _safe_float(pair_data.get("fdv"), 0),
                "volume_h24_usd": _safe_float(pair_data.get("volume"), {}).get("h24", 0) if isinstance(pair_data.get("volume"), dict) else 0,
                "age_seconds": candidate["age_seconds"],
                "sources": candidate["sources"],
            }
            record["marks"].append((now, entry, "entry"))
            active.append(record)
            records.append(record)

    if not active:
        _write_empty_report(source_stats, all_candidates, blocked=blocked, pass_count=pass_count, quote_count=quote_count)
        return

    started = datetime.now(UTC)
    try:
        while active and (datetime.now(UTC) - started).total_seconds() < HOLD_SECONDS:
            await asyncio.sleep(MARK_INTERVAL_SECONDS)
            for record in list(active):
                now = datetime.now(UTC)
                result = await provider.get_price_with_diagnostic(str(record["mint"]))
                if not _is_valid_price(result.price_sol):
                    record["marks"].append((now, None, result.reason))
                    continue
                price = float(result.price_sol)
                decision = evaluate_momentum_trail(record["state"], price)
                record["state"] = decision.state
                record["marks"].append((now, price, decision.exit_reason or "mark"))
                if decision.exit_reason:
                    closed = await manager.close_position(str(record["mint"]), exit_price_sol=price, mode="paper")
                    record["exit_reason"] = decision.exit_reason
                    record["close"] = price
                    record["pnl"] = closed.realized_pnl_sol if closed else None
                    active.remove(record)
    except Exception:
        import traceback
        traceback.print_exc()

    for record in list(active):
        now = datetime.now(UTC)
        result = await provider.get_price_with_diagnostic(str(record["mint"]))
        if _is_valid_price(result.price_sol):
            price = float(result.price_sol)
            record["marks"].append((now, price, "max_hold"))
            closed = await manager.close_position(str(record["mint"]), exit_price_sol=price, mode="paper")
            record["exit_reason"] = "max_hold"
            record["close"] = price
            record["pnl"] = closed.realized_pnl_sol if closed else None
        else:
            record["marks"].append((now, None, f"final_mark_unavailable:{result.reason}"))
            record["exit_reason"] = "final_mark_unavailable"
        active.remove(record)

    _write_results(source_stats, all_candidates, records, blocked, pass_count, quote_count)
    _write_capture(all_candidates)


def _write_empty_report(source_stats: dict, all_candidates: list, blocked: dict | None = None, pass_count: int = 0, quote_count: int = 0) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    blockers = blocked or {}
    nearest = sorted(
        [c for c in all_candidates if not c["qualifies"]],
        key=lambda c: (_safe_float(c["pair"].get("liquidity"), {}).get("usd", 0) if isinstance(c["pair"].get("liquidity"), dict) else 0),
        reverse=True,
    )[:5]
    lines = [
        "# MT-451: Exact Rendered High-Liquidity 5-Second Paper Batch",
        "",
        f"- Run timestamp: {datetime.now(UTC).isoformat()}",
        f"- Source method: DexScreener API fallback (token-profiles + token-boosts top/latest)",
        f"- Sources: token-profiles ({source_stats.get('token-profiles', 0)}), token-boosts-top ({source_stats.get('token-boosts-top', 0)}), token-boosts-latest ({source_stats.get('token-boosts-latest', 0)})",
        f"- Unique mints found: {source_stats.get('unique_mints', 0)}",
        f"- Pairs with Solana data: {source_stats.get('total_pairs_found', 0)}",
        f"- Board filter qualifying: {source_stats.get('board_qualifying', 0)}",
        f"- Paper gate passes/quotes/entries: {pass_count}/{quote_count}/0",
        f"- Blockers: {dict(blockers) if blockers else 'none'}",
    ]
    if source_stats.get("filter_breakdown"):
        lines.append(f"- Filter breakdown: {source_stats['filter_breakdown']}")
    lines.extend([
        "",
        "## Board Filter Applied",
        f"- Chain: Solana",
        f"- Max age: {BOARD_FILTERS['max_age_seconds']}s ({BOARD_FILTERS['max_age_seconds']/60:.0f}m)",
        f"- Min liquidity: ${BOARD_FILTERS['min_liquidity_usd']:,}",
        f"- Min market cap (FDV): ${BOARD_FILTERS['min_market_cap_usd']:,}",
        f"- Min 24h volume: ${BOARD_FILTERS['min_volume_h24_usd']:,}",
        "",
        "## Result: No qualifying candidates available",
        "",
    ])
    if len(all_candidates) == 0:
        lines.append("All mint sources returned no resolvable pairs.")
    else:
        lines.append(f"Searched {len(all_candidates)} resolvable Solana pairs. None met the combined board filter.")
    if nearest:
        lines.extend(["", "## Nearest Misses"])
        for c in nearest:
            p = c["pair"]
            liq = _safe_float(p.get("liquidity"), {}).get("usd", 0) if isinstance(p.get("liquidity"), dict) else 0
            fdv = _safe_float(p.get("fdv"), 0)
            vol = _safe_float(p.get("volume"), {}).get("h24", 0) if isinstance(p.get("volume"), dict) else 0
            age = c["age_seconds"]
            sym = (p.get("baseToken") or {}).get("symbol") or c["mint"][:8]
            lines.append(f"- {sym} (age={age:.0f}s, liq=${liq:.0f}, fdv=${fdv:.0f}, vol=${vol:.0f}) reasons: {c['reasons']}")
    lines.append("")
    lines.append("## Comparison to MT-444")
    lines.append("No trades available — no candidates qualified. Cannot compare PnL.")
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    CSV_PATH.write_text("timestamp_utc,mint,pair,event,price_sol,entry_change_pct,peak_price_sol\n", encoding="utf-8")
    _write_capture(all_candidates)


def _write_results(source_stats: dict, all_candidates: list, records: list, blocked: dict, pass_count: int, quote_count: int) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    with CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp_utc", "mint", "pair", "event", "price_sol", "entry_change_pct", "peak_price_sol"])
        for record in records:
            for timestamp, price, event in record["marks"]:
                entry = record["entry"]
                change = None if price is None else ((price - entry) / entry) * 100
                writer.writerow([timestamp.isoformat(), record["mint"], record["pair"], event, price, change, record["state"].peak_price_sol])

    closed = [r for r in records if r.get("close") is not None]
    pnl = sum(r.get("pnl") or 0.0 for r in closed)
    wins = sum(1 for r in closed if (r.get("pnl") or 0.0) > 0)
    losses = sum(1 for r in closed if (r.get("pnl") or 0.0) < 0)

    lines = [
        "# MT-451: Exact Rendered High-Liquidity 5-Second Paper Batch",
        "",
        f"- Run timestamp: {datetime.now(UTC).isoformat()}",
        f"- Source method: DexScreener API fallback (token-profiles + token-boosts top/latest)",
        f"- Sources: token-profiles ({source_stats.get('token-profiles', 0)}), token-boosts-top ({source_stats.get('token-boosts-top', 0)}), token-boosts-latest ({source_stats.get('token-boosts-latest', 0)})",
        f"- Unique mints found: {source_stats.get('unique_mints', 0)}",
        f"- Pairs with Solana data: {source_stats.get('total_pairs_found', 0)}",
        f"- Board filter qualifying: {source_stats.get('board_qualifying', 0)}",
        "- Board filter: Solana, age <=1h, liquidity >$50K, market cap >$7K, volume >$1K",
        "- MT-444 exit parameters: hard stop -20%; activation +10%; standard trail 8%; tightened trail +25%/5%; max hold 30m.",
        f"- Mark interval: 5s",
        f"- Paper gate passes/quotes/entries: {pass_count}/{quote_count}/{len(records)}",
        f"- Blockers: {blocked}",
        "",
        "## Trades",
    ]
    for record in records:
        entry = record["entry"]
        hold = (record["marks"][-1][0] - record["opened"]).total_seconds() if record["marks"] else 0.0
        pct = ((record["close"] - entry) / entry * 100) if record.get("close") is not None else None
        lines.append(f"- mint `{record['mint']}`; pair `{record['pair']}`; symbol `{record.get('symbol', '')}`; entry {entry:.12f} SOL; peak {record['state'].peak_price_sol:.12f} SOL; exit {record.get('close')}; reason {record.get('exit_reason')}; hold {hold:.0f}s; PnL {record.get('pnl')}; result {pct}%.")
        lines.append(f"  - Liquidity: ${record.get('liquidity_usd', 0):.0f}; FDV: ${record.get('fdv_usd', 0):.0f}; Volume 24h: ${record.get('volume_h24_usd', 0):.0f}; Age: {record.get('age_seconds', 0):.0f}s; Sources: {record.get('sources', [])}")
        for timestamp, price, event in record["marks"]:
            change = "unavailable" if price is None else f"{((price - entry) / entry) * 100:+.4f}%"
            lines.append(f"  - {timestamp.isoformat()} mark={price} change={change} event={event}")

    lines.extend([
        "",
        "## Summary",
        f"- Total trades: {len(records)}",
        f"- Wins/losses/open: {wins}/{losses}/{len(records) - len(closed)}",
        f"- Net realized PnL: {pnl:+.9f} SOL",
        "",
        "## Comparison to MT-444",
    ])
    if records:
        lines.append(f"- MT-451 5s marks vs MT-444 15s marks")
        lines.append(f"- MT-444 net PnL: -0.001048 SOL (3 trades, 1 win / 2 losses)")
        lines.append(f"- MT-451 net PnL: {pnl:+.9f} SOL ({len(records)} trades, {wins} win(s) / {losses} loss(es))")
        if len(records) > 0 and len(closed) == len(records):
            diff = pnl - (-0.001048)
            lines.append(f"- Delta: {diff:+.9f} SOL ({'better' if diff > 0 else 'worse'} than MT-444)")
    else:
        lines.append("No trades executed. Cannot compare with MT-444.")

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_capture(all_candidates: list) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for c in all_candidates:
        p = c["pair"]
        base = p.get("baseToken") or {}
        liq = _safe_float(p.get("liquidity"), {}).get("usd", 0) if isinstance(p.get("liquidity"), dict) else 0
        vol = _safe_float(p.get("volume"), {}).get("h24", 0) if isinstance(p.get("volume"), dict) else 0
        fdv = _safe_float(p.get("fdv"), 0)
        rows.append({
            "mint_address": c["mint"],
            "pair_address": p.get("pairAddress", ""),
            "symbol": base.get("symbol"),
            "name": base.get("name"),
            "age_seconds": c["age_seconds"],
            "liquidity_usd": liq,
            "volume_h24_usd": vol,
            "fdv_usd": fdv,
            "qualifies": c["qualifies"],
            "filter_reasons": c["reasons"],
            "sources": c["sources"],
            "chain_id": p.get("chainId"),
            "dex_id": p.get("dexId"),
            "url": f"https://dexscreener.com/solana/{p.get('pairAddress', '')}",
        })
    CAPTURE_PATH.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())

"""Detached paper-only MT-438 momentum trailing-exit collector."""

from __future__ import annotations

import asyncio
import csv
from datetime import UTC, datetime
from pathlib import Path

import httpx

from src.cli import _is_valid_price, _payload_float, force_paper_settings
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
from src.signals.dexscreener_new_pairs import DEXSCREENER_NEW_PAIRS_UI_URL, load_new_pairs_ui_rows, resolve_new_pairs_ui_rows
from src.strategy.momentum_trailing import MomentumTrailState, evaluate_momentum_trail
from src.strategy.position_manager import PositionManager


CAPTURE_PATH = Path("/mnt/c/Users/Big A/custodian-shared/memecoin-trader/paper-trade-reports/mt436_new_pairs_ui_capture.json")
REPORT_PATH = Path("/mnt/c/Users/Big A/custodian-shared/memecoin-trader/paper-trade-reports/momentum_gate_lifecycle_batch.md")
CSV_PATH = Path("/mnt/c/Users/Big A/custodian-shared/memecoin-trader/paper-trade-reports/momentum_gate_trade_log.csv")
HOLD_SECONDS = 30 * 60
MARK_INTERVAL_SECONDS = 60
AMOUNT_SOL = 0.01
MAX_ENTRIES = 3


async def main() -> None:
    rows = load_new_pairs_ui_rows(CAPTURE_PATH)
    signals = await resolve_new_pairs_ui_rows(rows, max_age_minutes=60.0)
    settings = force_paper_settings(load_settings())
    db_path = resolve_db_path(None)
    await init_db(db_path)
    manager = PositionManager(db_path, settings)
    provider = DexScreenerPriceProvider()
    records: list[dict[str, object]] = []
    blocked: dict[str, int] = {}
    active: list[dict[str, object]] = []
    seen: set[str] = set()
    fallback_count = passes = quotes = repeats = 0

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
        for signal in signals:
            mint = signal.mint_address.strip()
            if not mint or mint in seen:
                repeats += 1
                continue
            seen.add(mint)
            assessment = await scorer.assess_signal(signal)
            evidence = evaluate_paper_new_pairs_momentum_evidence(
                signal,
                assessment,
                top10_holder_pct=assessment.token.top10_holder_pct if assessment.token else None,
                ui_age_minutes=_payload_float(signal.payload, "ui_age_minutes"),
                ui_max_age_minutes=_payload_float(signal.payload, "ui_max_age_minutes"),
            )
            if "paper_momentum_age_ui_observed_fresh" in evidence.reason_labels:
                fallback_count += 1
            if not evidence.eligible:
                for label in evidence.reason_labels:
                    if label.startswith("paper_momentum_blocked_"):
                        blocked[label] = blocked.get(label, 0) + 1
                continue
            passes += 1
            quote = await provider.get_price_with_diagnostic(mint)
            if not _is_valid_price(quote.price_sol):
                blocked[f"quote_{quote.reason}"] = blocked.get(f"quote_{quote.reason}", 0) + 1
                continue
            quotes += 1
            if len(active) >= MAX_ENTRIES or await manager.get_position(mint, mode="paper") is not None:
                continue
            execution = PaperExecutionAdapter(price_lookup={mint: quote.price_sol})
            try:
                trade = await execution.execute_swap(mint, Side.BUY, AMOUNT_SOL, slippage_bps=settings.position.default_slippage_bps)
            finally:
                await execution.close()
            await record_trade(db_path, trade)
            await manager.open_position(trade, signal)
            now = datetime.now(UTC)
            entry = float(quote.price_sol)
            record = {"mint": mint, "pair": signal.payload.get("pair_address"), "entry": entry, "opened": now, "state": MomentumTrailState(entry, entry), "marks": []}
            record["marks"].append((now, entry, "entry"))
            active.append(record)
            records.append(record)

    started = datetime.now(UTC)
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

    _write_outputs(rows, signals, repeats, fallback_count, passes, quotes, blocked, records)


def _write_outputs(rows, signals, repeats, fallback_count, passes, quotes, blocked, records) -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp_utc", "mint", "pair", "event", "price_sol", "entry_change_pct", "peak_price_sol"])
        for record in records:
            for timestamp, price, event in record["marks"]:
                entry = record["entry"]
                change = None if price is None else ((price - entry) / entry) * 100
                writer.writerow([timestamp.isoformat(), record["mint"], record["pair"], event, price, change, record["state"].peak_price_sol])
    closed = [record for record in records if record.get("close") is not None]
    pnl = sum(record.get("pnl") or 0.0 for record in closed)
    wins = sum(1 for record in closed if (record.get("pnl") or 0.0) > 0)
    losses = sum(1 for record in closed if (record.get("pnl") or 0.0) < 0)
    lines = [
        "# Momentum Trailing-Exit Paper Batch",
        "",
        f"- Run timestamp: {datetime.now(UTC).isoformat()}",
        f"- Source: `{DEXSCREENER_NEW_PAIRS_UI_URL}`",
        "- Filter state: Solana, Last hour, Newest/pair-age ascending, age <=60m, liquidity >$1K, market cap $5K-$100K, no 1H/24H volume minimum.",
        f"- Candidates rendered/resolved/novel/repeated: {len(rows)}/{len(signals)}/{len(signals) - repeats}/{repeats}",
        f"- UI-age fallbacks/gate passes/quotes/entries: {fallback_count}/{passes}/{quotes}/{len(records)}",
        f"- Blockers: {blocked}",
        "",
        "## Trades",
    ]
    for record in records:
        entry = record["entry"]
        hold = (record["marks"][-1][0] - record["opened"]).total_seconds() if record["marks"] else 0.0
        pct = ((record["close"] - entry) / entry * 100) if record.get("close") is not None else None
        lines.append(f"- mint `{record['mint']}`; pair `{record['pair']}`; entry {entry:.12f} SOL; peak {record['state'].peak_price_sol:.12f} SOL; exit {record.get('close')}; reason {record.get('exit_reason')}; hold {hold:.0f}s; PnL {record.get('pnl')}; result {pct}%." )
        for timestamp, price, event in record["marks"]:
            change = "unavailable" if price is None else f"{((price - entry) / entry) * 100:+.4f}%"
            lines.append(f"  - {timestamp.isoformat()} mark={price} change={change} event={event}")
    lines.extend(["", "## Summary", f"- Total trades: {len(records)}", f"- Wins/losses/open: {wins}/{losses}/{len(records) - len(closed)}", f"- Net realized PnL: {pnl:+.9f} SOL"])
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())

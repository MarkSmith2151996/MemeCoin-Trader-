#!/usr/bin/env python3
"""Read-only Strategy B integrity and robustness sweep.

The sweep replays only the current entry filters and exit thresholds against
persisted closed Strategy B positions. It never updates the trading database.
Run with: python3 scripts/sanity_sweep.py
"""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "trades.db"
REPORT_PATH = ROOT / "data" / "sanity_sweep_report.md"

MIN_MCAP_USD = 5_000.0
BLOCKED_UTC_HOURS = frozenset({0, 7, 19, 20, 21})
BLOCKED_WEEKDAYS = frozenset({2})
TRAILING_STOP_PCT = 2.0
TRAILING_ARM_PCT = 2.0
TAKE_PROFIT_PCT = 150.0
HARD_STOP_PCT = 8.0


@dataclass(frozen=True)
class TradeRecord:
    """One closed Strategy B position with its linked entry evidence."""

    position_id: str
    mint_address: str
    amount_sol: float
    entry_price_sol: float
    close_price_sol: float | None
    actual_pnl_sol: float
    opened_at: datetime
    closed_at: datetime
    scan_time: datetime | None
    mcap_usd: float | None
    snapshots: tuple[tuple[datetime, float], ...]


@dataclass(frozen=True)
class ReplayResult:
    """PnL from the current exit parameters for one persisted mark path."""

    pnl_sol: float
    reason: str


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse persisted ISO timestamps as timezone-aware UTC datetimes."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def fmt_sol(value: float) -> str:
    return f"{value:+.4f} SOL"


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds / 60:.1f}m"


class Report:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def add(self, text: str = "") -> None:
        self.lines.append(text)

    def table(self, headers: list[str], rows: list[list[str]]) -> None:
        self.add("| " + " | ".join(headers) + " |")
        self.add("| " + " | ".join("---" for _ in headers) + " |")
        for row in rows:
            self.add("| " + " | ".join(row) + " |")
        self.add()


def load_trades(db: sqlite3.Connection) -> list[TradeRecord]:
    """Load closed Strategy B positions, one linked candidate, and mark paths."""
    position_rows = db.execute(
        """
        WITH ranked_candidates AS (
            SELECT
                position_id,
                scan_time,
                mcap_usd,
                ROW_NUMBER() OVER (
                    PARTITION BY position_id
                    ORDER BY entered DESC, scan_time DESC, id DESC
                ) AS candidate_rank
            FROM candidate_log
            WHERE strategy = 'B' AND position_id IS NOT NULL
        )
        SELECT
            p.id,
            p.mint_address,
            p.amount_sol,
            p.entry_price_sol,
            p.close_price_sol,
            p.realized_pnl_sol,
            p.opened_at,
            p.closed_at,
            c.scan_time,
            c.mcap_usd
        FROM positions AS p
        LEFT JOIN ranked_candidates AS c
            ON c.position_id = p.id AND c.candidate_rank = 1
        WHERE p.strategy = 'B' AND p.status = 'CLOSED'
        ORDER BY p.closed_at, p.id
        """,
    ).fetchall()
    snapshots_by_position: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    snapshot_rows = db.execute(
        """
        SELECT s.position_id, s.observed_at, s.price_sol
        FROM price_snapshots AS s
        INNER JOIN positions AS p ON p.id = s.position_id
        WHERE p.strategy = 'B'
          AND p.status = 'CLOSED'
          AND s.price_sol > 0
        ORDER BY s.position_id, s.observed_at
        """,
    ).fetchall()
    for position_id, observed_at, price_sol in snapshot_rows:
        observed = parse_timestamp(observed_at)
        if observed is not None:
            snapshots_by_position[str(position_id)].append((observed, float(price_sol)))

    records = []
    for row in position_rows:
        opened_at = parse_timestamp(row[6])
        closed_at = parse_timestamp(row[7])
        amount = float(row[2] or 0.0)
        entry = float(row[3] or 0.0)
        if opened_at is None or closed_at is None or amount <= 0 or entry <= 0:
            continue
        records.append(
            TradeRecord(
                position_id=str(row[0]),
                mint_address=str(row[1]),
                amount_sol=amount,
                entry_price_sol=entry,
                close_price_sol=float(row[4]) if row[4] and row[4] > 0 else None,
                actual_pnl_sol=float(row[5] or 0.0),
                opened_at=opened_at,
                closed_at=closed_at,
                scan_time=parse_timestamp(row[8]),
                mcap_usd=float(row[9]) if row[9] is not None else None,
                snapshots=tuple(snapshots_by_position[str(row[0])]),
            ),
        )
    return records


def survives_current_entry_filters(trade: TradeRecord) -> bool:
    """Apply gates at the actual entry timestamp using its logged candidate mcap."""
    return (
        trade.mcap_usd is not None
        and trade.mcap_usd >= MIN_MCAP_USD
        and trade.opened_at.hour not in BLOCKED_UTC_HOURS
        and trade.opened_at.weekday() not in BLOCKED_WEEKDAYS
    )


def replay_exit(trade: TradeRecord) -> ReplayResult | None:
    """Replay the 2% trail / 150% TP / 8% hard stop against recorded marks.

    A trade without snapshots is not replayable. If snapshots do not trigger a
    parameterized exit, the recorded close is appended as the terminal mark so
    its residual value is not fabricated.
    """
    if not trade.snapshots or trade.close_price_sol is None:
        return None

    points = [
        (observed_at, price)
        for observed_at, price in trade.snapshots
        if observed_at >= trade.opened_at and price > 0
    ]
    if not points:
        return None
    points.append((trade.closed_at, trade.close_price_sol))
    points.sort(key=lambda point: point[0])

    peak = trade.entry_price_sol
    for _, price in points:
        peak = max(peak, price)
        if price >= trade.entry_price_sol * (1 + TAKE_PROFIT_PCT / 100):
            return ReplayResult(trade.amount_sol * (TAKE_PROFIT_PCT / 100), "take_profit")
        if price <= trade.entry_price_sol * (1 - HARD_STOP_PCT / 100):
            return ReplayResult(-trade.amount_sol * (HARD_STOP_PCT / 100), "hard_stop")
        if (
            peak > trade.entry_price_sol * (1 + TRAILING_ARM_PCT / 100)
            and (peak - price) / peak >= TRAILING_STOP_PCT / 100
        ):
            return ReplayResult(
                trade.amount_sol * (price / trade.entry_price_sol - 1),
                "trailing_stop",
            )

    return ReplayResult(
        trade.amount_sol * (trade.close_price_sol / trade.entry_price_sol - 1),
        "recorded_close_fallback",
    )


def slippage_adjusted_pnl(trade: TradeRecord, replay: ReplayResult, round_trip_pct: float) -> float:
    """Apply half the requested round-trip cost to each side of a replay."""
    per_leg_pct = round_trip_pct / 2
    entry_cost = trade.amount_sol * per_leg_pct
    exit_value = max(0.0, trade.amount_sol + replay.pnl_sol)
    exit_cost = exit_value * per_leg_pct
    return replay.pnl_sol - entry_cost - exit_cost


def drawdown_summary(
    trades: list[TradeRecord],
) -> tuple[float, datetime | None, datetime | None, datetime | None]:
    """Return max drawdown and its peak, trough, and recovery timestamps."""
    equity = 0.0
    peak_equity = 0.0
    peak_time: datetime | None = None
    max_drawdown = 0.0
    trough_time: datetime | None = None
    drawdown_peak_time: datetime | None = None
    recovery_time: datetime | None = None
    tracking_recovery = False

    for trade in trades:
        equity += trade.actual_pnl_sol
        if equity > peak_equity:
            peak_equity = equity
            peak_time = trade.closed_at
        drawdown = peak_equity - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            trough_time = trade.closed_at
            drawdown_peak_time = peak_time
            recovery_time = None
            tracking_recovery = True
        if tracking_recovery and equity >= peak_equity:
            recovery_time = trade.closed_at
            tracking_recovery = False
    return max_drawdown, drawdown_peak_time, trough_time, recovery_time


def worst_losing_streak(trades: list[TradeRecord]) -> tuple[int, float]:
    """Return the longest run of strictly negative closed-trade PnL."""
    current_count = 0
    current_loss = 0.0
    worst_count = 0
    worst_loss = 0.0
    for trade in trades:
        if trade.actual_pnl_sol < 0:
            current_count += 1
            current_loss += trade.actual_pnl_sol
            if current_count > worst_count or (
                current_count == worst_count and current_loss < worst_loss
            ):
                worst_count = current_count
                worst_loss = current_loss
        else:
            current_count = 0
            current_loss = 0.0
    return worst_count, worst_loss


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")

    db = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        trades = load_trades(db)
    finally:
        db.close()

    report = Report()
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    report.add("# Strategy B Sanity Sweep")
    report.add()
    report.add(f"Generated: {generated_at}")
    report.add(f"Database: `{DB_PATH}` (read-only)")
    report.add()

    total_pnl = sum(trade.actual_pnl_sol for trade in trades)
    with_candidate = [trade for trade in trades if trade.mcap_usd is not None]
    survivors = [trade for trade in with_candidate if survives_current_entry_filters(trade)]
    report.add("## 1. Entry Filter Replay")
    report.add(
        "Current gates: mcap >= $5,000; blocked UTC hours 0, 7, 19, 20, 21; Wednesday blocked."
    )
    report.table(
        ["Cohort", "Trades", "PnL (SOL)", "Win rate"],
        [
            [
                "All closed Strategy B",
                str(len(trades)),
                fmt_sol(total_pnl),
                (
                    f"{sum(t.actual_pnl_sol > 0 for t in trades) / len(trades):.1%}"
                    if trades
                    else "n/a"
                ),
            ],
            [
                "Candidate-log linked",
                str(len(with_candidate)),
                fmt_sol(sum(t.actual_pnl_sol for t in with_candidate)),
                (
                    f"{sum(t.actual_pnl_sol > 0 for t in with_candidate) / len(with_candidate):.1%}"
                    if with_candidate
                    else "n/a"
                ),
            ],
            [
                "Would survive current gates",
                str(len(survivors)),
                fmt_sol(sum(t.actual_pnl_sol for t in survivors)),
                (
                    f"{sum(t.actual_pnl_sol > 0 for t in survivors) / len(survivors):.1%}"
                    if survivors
                    else "n/a"
                ),
            ],
        ],
    )
    report.add(
        f"- Missing candidate-log mcap: {len(trades) - len(with_candidate)} trades "
        "(excluded from survivor cohort)."
    )
    report.add()

    replayed = [(trade, replay_exit(trade)) for trade in survivors]
    replayed = [(trade, result) for trade, result in replayed if result is not None]
    replay_pnl = sum(result.pnl_sol for _, result in replayed)
    actual_replay_pnl = sum(trade.actual_pnl_sol for trade, _ in replayed)
    reasons: dict[str, int] = defaultdict(int)
    for _, result in replayed:
        reasons[result.reason] += 1
    report.add("## 2. Exit Parameter Replay")
    report.add(
        "Replay uses ordered position snapshots with the current 2% trail (armed at +2%), "
        "150% TP, and 8% hard stop. A non-triggering path uses its recorded close "
        "as a terminal fallback."
    )
    report.table(
        ["Replayable survivors", "Actual PnL", "Replayed PnL", "Difference"],
        [
            [
                str(len(replayed)),
                fmt_sol(actual_replay_pnl),
                fmt_sol(replay_pnl),
                fmt_sol(replay_pnl - actual_replay_pnl),
            ],
        ],
    )
    if reasons:
        report.add(
            "- Replay exits: "
            + ", ".join(f"{reason}={count}" for reason, count in sorted(reasons.items()))
        )
    else:
        report.add("- No snapshot-backed survivor paths were available.")
    report.add()

    report.add("## 3. Slippage Stress Test")
    report.add("Round-trip slippage is split equally between entry and exit value.")
    slippage_rows = []
    for pct in (0.005, 0.01, 0.02, 0.03, 0.05):
        adjusted = [slippage_adjusted_pnl(trade, replay, pct) for trade, replay in replayed]
        slippage_rows.append(
            [
                f"{pct:.1%}",
                str(len(adjusted)),
                fmt_sol(sum(adjusted)),
                (
                    f"{sum(value > 0 for value in adjusted) / len(adjusted):.1%}"
                    if adjusted
                    else "n/a"
                ),
            ],
        )
    report.table(["Round-trip slippage", "Trades", "PnL (SOL)", "Win rate"], slippage_rows)

    delays = [
        (trade.opened_at - trade.scan_time).total_seconds()
        for trade in trades
        if trade.scan_time is not None
    ]
    negative_delays = [delay for delay in delays if delay < 0]
    under_two_seconds = sum(0 <= delay < 2 for delay in delays)
    pnl_groups: dict[float, int] = defaultdict(int)
    for trade in trades:
        pnl_groups[round(trade.actual_pnl_sol, 8)] += 1
    repeated_pnls = sorted(
        ((pnl, count) for pnl, count in pnl_groups.items() if count >= 3),
        key=lambda item: (-item[1], item[0]),
    )[:10]
    report.add("## 4. Integrity Checks")
    report.table(
        ["Metric", "Value"],
        [
            ["Scan-to-entry rows", str(len(delays))],
            ["Average delay", fmt_duration(sum(delays) / len(delays) if delays else None)],
            ["Minimum delay", fmt_duration(min(delays) if delays else None)],
            ["Maximum delay", fmt_duration(max(delays) if delays else None)],
            ["Delay under 2s", f"{under_two_seconds / len(delays):.1%}" if delays else "n/a"],
            ["Negative delays (look-ahead)", str(len(negative_delays))],
        ],
    )
    if repeated_pnls:
        report.add("Repeated actual PnL values (rounded to 8 decimals; three or more occurrences):")
        report.table(
            ["PnL (SOL)", "Trades", "Share of all closed trades"],
            [
                [fmt_sol(pnl), str(count), f"{count / len(trades):.1%}"]
                for pnl, count in repeated_pnls
            ],
        )
    else:
        report.add("- No actual PnL value repeats three or more times at 8-decimal precision.")
        report.add()

    max_drawdown, peak_time, trough_time, recovery_time = drawdown_summary(trades)
    streak_count, streak_loss = worst_losing_streak(trades)
    recovery_duration = (
        (recovery_time - trough_time).total_seconds()
        if recovery_time is not None and trough_time is not None
        else None
    )
    report.add("## 5. Drawdown Analysis")
    report.table(
        ["Metric", "Value"],
        [
            ["Max drawdown", fmt_sol(-max_drawdown)],
            ["Peak before drawdown", peak_time.isoformat() if peak_time else "n/a"],
            ["Drawdown trough", trough_time.isoformat() if trough_time else "n/a"],
            [
                "Time to recover",
                fmt_duration(recovery_duration) if recovery_time else "not recovered",
            ],
            ["Worst losing streak", f"{streak_count} trades / {fmt_sol(streak_loss)}"],
        ],
    )

    weekly: dict[str, dict[str, float | int]] = defaultdict(lambda: {"trades": 0, "pnl": 0.0})
    for trade in trades:
        iso_year, iso_week, _ = trade.closed_at.isocalendar()
        week = f"{iso_year}-W{iso_week:02d}"
        weekly[week]["trades"] += 1
        weekly[week]["pnl"] += trade.actual_pnl_sol
    report.add("## 6. Weekly PnL")
    report.table(
        ["ISO week", "Trades", "PnL (SOL)", "Flag"],
        [
            [
                week,
                str(stats["trades"]),
                fmt_sol(float(stats["pnl"])),
                "RED" if float(stats["pnl"]) < 0 else "",
            ]
            for week, stats in sorted(weekly.items())
        ],
    )

    winners = sorted(
        (trade.actual_pnl_sol for trade in trades if trade.actual_pnl_sol > 0),
        reverse=True,
    )
    top_five_removed = total_pnl - sum(winners[:5])
    top_ten_removed = total_pnl - sum(winners[:10])
    top_trade_count = math.ceil(len(trades) * 0.10)
    top_ten_pct_pnl = sum(
        sorted((trade.actual_pnl_sol for trade in trades), reverse=True)[:top_trade_count],
    )
    concentration_pct = top_ten_pct_pnl / total_pnl if total_pnl else 0.0
    report.add("## 7. Winner Concentration")
    report.table(
        ["Scenario", "PnL (SOL)", "Profitable?"],
        [
            ["All closed trades", fmt_sol(total_pnl), "YES" if total_pnl > 0 else "NO"],
            [
                "Remove top 5 winners",
                fmt_sol(top_five_removed),
                "YES" if top_five_removed > 0 else "NO",
            ],
            [
                "Remove top 10 winners",
                fmt_sol(top_ten_removed),
                "YES" if top_ten_removed > 0 else "NO",
            ],
        ],
    )
    report.add(
        f"- Top 10% of trades ({top_trade_count} rows) contribute {fmt_sol(top_ten_pct_pnl)} "
        f"({concentration_pct:.1%} of total PnL)."
    )
    report.add()
    report.add("## Scope")
    report.add(
        "- This sweep is read-only; it does not change Strategy B runtime logic, the live adapter, "
        "safety controls, or shadow-mode code."
    )

    output = "\n".join(report.lines) + "\n"
    print(output)
    REPORT_PATH.write_text(output, encoding="utf-8")
    print(f"Report saved to {REPORT_PATH}")


if __name__ == "__main__":
    main()

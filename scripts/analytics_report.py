"""MT-550: Comprehensive Strategy B analytics report.

Queries data/trades.db (read-only) and prints a full performance report for
the 762 closed Strategy B trades, covering overall summary, mcap-tier PnL,
day-of-week / UTC-hour win rates, exit-type breakdown, daily PnL timeline,
slippage-adjusted PnL by tier, gate-config effectiveness, and shadow-mode
Jupiter quote data (if present).

Read-only: never writes to the DB, does not touch runtime code.

Run:
    python3 scripts/analytics_report.py

Results printed to stdout and saved to data/analytics_report.md.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "data" / "trades.db"
REPORT_PATH = REPO_ROOT / "data" / "analytics_report.md"

BLOCKED_UTC_HOURS = frozenset({0, 7, 19, 20, 21})
LOW_WINRATE_FLAG = 0.25

MCAP_TIERS = [
    ("<$5K", None, 5000),
    ("$5K-10K", 5000, 10000),
    ("$10K-20K", 10000, 20000),
    ("$20K-50K", 20000, 50000),
    ("$50K+", 50000, None),
]

SLIPPAGE_PCT_BY_TIER = [
    (10000, 0.08),
    (20000, 0.05),
    (None, 0.03),
]

DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def slippage_pct(mcap_usd: float | None) -> float:
    if mcap_usd is None:
        return 0.08
    for limit, pct in SLIPPAGE_PCT_BY_TIER:
        if limit is None or mcap_usd < limit:
            return pct
    return 0.03


def tier_label(mcap_usd: float | None) -> str:
    if mcap_usd is None:
        return "<$5K"
    for label, lo, hi in MCAP_TIERS:
        if (lo is None or mcap_usd >= lo) and (hi is None or mcap_usd < hi):
            return label
    return "<$5K"


def fmt_sol(value: float) -> str:
    return f"{value:+.4f}"


class Report:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def add(self, text: str = "") -> None:
        self.lines.append(text)

    def table(self, header: list[str], rows: list[list[str]], widths: list[int]) -> None:
        self.add("| " + " | ".join(h.ljust(w) for h, w in zip(header, widths)) + " |")
        self.add("|-" + "-|-".join("-" * w for w in widths) + "-|")
        for row in rows:
            self.add("| " + " | ".join(str(c).ljust(w) for c, w in zip(row, widths)) + " |")
        self.add()


def load_positions(db: sqlite3.Connection) -> list[dict]:
    rows = db.execute(
        """SELECT id, mint_address, amount_sol, entry_price_sol, realized_pnl_sol,
                  opened_at, closed_at, close_price_sol, strategy
           FROM positions
           WHERE strategy = 'B' AND status = 'CLOSED'
           ORDER BY closed_at"""
    ).fetchall()
    return [dict(r) for r in rows]


def load_candidate_mcaps(db: sqlite3.Connection) -> dict[str, float | None]:
    rows = db.execute(
        "SELECT position_id, mcap_usd FROM candidate_log WHERE position_id IS NOT NULL"
    ).fetchall()
    return {r["position_id"]: r["mcap_usd"] for r in rows}


def load_sell_reasons(db: sqlite3.Connection) -> dict[str, dict]:
    """Map mint -> list of (executed_at, close_reason) from SELL trades."""
    sells: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    rows = db.execute(
        """SELECT mint_address, executed_at,
                  json_extract(metadata_json, '$.metadata.close_reason') AS reason
           FROM trades WHERE side = 'SELL'"""
    ).fetchall()
    for r in rows:
        ts = parse_ts(r["executed_at"])
        if ts is not None:
            sells[r["mint_address"]].append((ts, r["reason"] or "unknown"))
    return sells


def match_exit_reason(
    pos: dict, sells: dict[str, list[tuple[datetime, str]]]
) -> str:
    closed_at = parse_ts(pos["closed_at"])
    if closed_at is None:
        return "unknown"
    cands = sells.get(pos["mint_address"], [])
    if not cands:
        return "unknown"
    best = min(cands, key=lambda c: abs((c[0] - closed_at).total_seconds()))
    if abs((best[0] - closed_at).total_seconds()) > 60:
        return "unknown"
    return best[1]


def load_gate_history(db: sqlite3.Connection) -> list[dict]:
    rows = db.execute(
        """SELECT id, updated_at, config_json, reason
           FROM gate_config WHERE strategy = 'B' ORDER BY updated_at"""
    ).fetchall()
    out = []
    for r in rows:
        try:
            cfg = json.loads(r["config_json"])
        except (json.JSONDecodeError, TypeError):
            cfg = {}
        out.append({"id": r["id"], "updated_at": parse_ts(r["updated_at"]), "config": cfg, "reason": r["reason"]})
    return out


def load_jupiter_quotes(db: sqlite3.Connection) -> list[dict]:
    rows = db.execute(
        """SELECT mint_address, dex_price_sol, jup_price_sol, slippage_vs_paper_pct,
                  quoted_at FROM jupiter_quotes"""
    ).fetchall()
    return [dict(r) for r in rows]


def main() -> None:
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    report = Report()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    positions = load_positions(db)
    mcap_by_pos = load_candidate_mcaps(db)
    sells = load_sell_reasons(db)
    gates = load_gate_history(db)
    quotes = load_jupiter_quotes(db)

    for p in positions:
        p["mcap_usd"] = mcap_by_pos.get(p["id"])
        p["exit_reason"] = match_exit_reason(p, sells)
        p["opened_dt"] = parse_ts(p["opened_at"])
        p["closed_dt"] = parse_ts(p["closed_at"])

    open_count = db.execute(
        "SELECT COUNT(*) FROM positions WHERE strategy = 'B' AND status = 'OPEN'"
    ).fetchone()[0]

    # ---------- 1. Overall summary ----------
    total = len(positions)
    pnls = [p["realized_pnl_sol"] for p in positions]
    winners = [x for x in pnls if x > 0]
    losers = [x for x in pnls if x < 0]
    flat = [x for x in pnls if x == 0]
    gross_win = sum(winners)
    gross_loss = abs(sum(losers))
    total_pnl = sum(pnls)
    win_rate = len(winners) / total if total else 0.0
    profit_factor = gross_win / gross_loss if gross_loss > 0 else float("inf")

    report.add("# Strategy B Analytics Report")
    report.add(f"\n_Generated: {now_str} — data from {DB_PATH}_")
    report.add("\n## 1. Overall Summary")
    report.add(f"- Total closed trades: **{total}**")
    report.add(f"- Win rate: **{win_rate:.1%}** ({len(winners)}W / {len(losers)}L / {len(flat)} flat)")
    report.add(f"- Total PnL: **{fmt_sol(total_pnl)} SOL**")
    report.add(f"- Average PnL per trade: {fmt_sol(total_pnl / total if total else 0.0)} SOL")
    report.add(f"- Average win: {fmt_sol(gross_win / len(winners) if winners else 0.0)} SOL (n={len(winners)})")
    report.add(f"- Average loss: {fmt_sol(-gross_loss / len(losers) if losers else 0.0)} SOL (n={len(losers)})")
    report.add(f"- Profit factor: **{profit_factor:.3f}**")
    report.add(f"- Open positions: **{open_count}**")
    report.add()

    # ---------- 2. PnL by mcap tier ----------
    report.add("## 2. PnL by Mcap Tier")
    tier_stats = {label: {"n": 0, "pnl": 0.0, "wins": 0, "unknown_mcap": False} for label, _, _ in MCAP_TIERS}
    for p in positions:
        mcap = p["mcap_usd"]
        label = tier_label(mcap)
        st = tier_stats[label]
        st["n"] += 1
        st["pnl"] += p["realized_pnl_sol"]
        if p["realized_pnl_sol"] > 0:
            st["wins"] += 1
        if mcap is None:
            st["unknown_mcap"] = True
    header = ["Tier", "Trades", "PnL (SOL)", "Win rate", "Avg PnL"]
    widths = [10, 8, 12, 10, 10]
    rows = []
    for label, _, _ in MCAP_TIERS:
        st = tier_stats[label]
        note = " (mcap unknown)" if st["unknown_mcap"] else ""
        rows.append([
            label,
            str(st["n"]),
            fmt_sol(st["pnl"]),
            f"{(st['wins'] / st['n'] * 100) if st['n'] else 0:.1f}%",
            fmt_sol(st["pnl"] / st["n"] if st["n"] else 0.0) + note,
        ])
    report.table(header, rows, widths)
    report.add("> Note: `<$5K` tier is now blocked by gate config (manual_freeze on 2026-08-13, min_mcap_usd=5000).")
    report.add()

    # ---------- 3. Win rate by day of week ----------
    report.add("## 3. Win Rate by Day of Week")
    dow = {i: {"n": 0, "pnl": 0.0, "wins": 0} for i in range(7)}
    for p in positions:
        if p["opened_dt"] is None:
            continue
        d = p["opened_dt"].weekday()
        dow[d]["n"] += 1
        dow[d]["pnl"] += p["realized_pnl_sol"]
        if p["realized_pnl_sol"] > 0:
            dow[d]["wins"] += 1
    header = ["Day", "Trades", "PnL (SOL)", "Win rate", "Flag"]
    widths = [6, 8, 12, 10, 20]
    rows = []
    for i in range(7):
        st = dow[i]
        wr = st["wins"] / st["n"] if st["n"] else 0.0
        flag = "**<25%**" if st["n"] and wr < LOW_WINRATE_FLAG else ""
        rows.append([
            DAY_NAMES[i], str(st["n"]), fmt_sol(st["pnl"]),
            f"{wr:.1%}" if st["n"] else "—", flag,
        ])
    report.table(header, rows, widths)
    report.add()

    # ---------- 4. Win rate by UTC hour ----------
    report.add("## 4. Win Rate by UTC Hour")
    hours = {i: {"n": 0, "pnl": 0.0, "wins": 0} for i in range(24)}
    for p in positions:
        if p["opened_dt"] is None:
            continue
        h = p["opened_dt"].hour
        hours[h]["n"] += 1
        hours[h]["pnl"] += p["realized_pnl_sol"]
        if p["realized_pnl_sol"] > 0:
            hours[h]["wins"] += 1
    header = ["UTC hour", "Trades", "PnL (SOL)", "Win rate", "Blocked?", "Flag"]
    widths = [10, 8, 12, 10, 10, 20]
    rows = []
    for h in range(24):
        st = hours[h]
        wr = st["wins"] / st["n"] if st["n"] else 0.0
        blocked = "YES" if h in BLOCKED_UTC_HOURS else ""
        flag = ""
        if st["n"] and h not in BLOCKED_UTC_HOURS and wr < LOW_WINRATE_FLAG:
            flag = "**<25% unblocked**"
        rows.append([str(h), str(st["n"]), fmt_sol(st["pnl"]), f"{wr:.1%}" if st["n"] else "—", blocked, flag])
    report.table(header, rows, widths)
    report.add(f"> Blocked UTC hours: {sorted(BLOCKED_UTC_HOURS)}")
    report.add()

    # ---------- 5. Exit type breakdown ----------
    report.add("## 5. Exit Type Breakdown")
    exits = defaultdict(lambda: {"n": 0, "pnl": 0.0, "wins": 0})
    for p in positions:
        e = exits[p["exit_reason"]]
        e["n"] += 1
        e["pnl"] += p["realized_pnl_sol"]
        if p["realized_pnl_sol"] > 0:
            e["wins"] += 1
    header = ["Exit type", "Trades", "PnL (SOL)", "Avg PnL", "Win rate"]
    widths = [18, 8, 12, 10, 10]
    rows = []
    for name, e in sorted(exits.items(), key=lambda kv: -kv[1]["n"]):
        rows.append([
            name, str(e["n"]), fmt_sol(e["pnl"]),
            fmt_sol(e["pnl"] / e["n"]), f"{e['wins'] / e['n']:.1%}",
        ])
    report.table(header, rows, widths)
    report.add()

    # ---------- 6. Daily PnL timeline ----------
    report.add("## 6. Daily PnL Timeline")
    daily: dict[str, dict] = defaultdict(lambda: {"n": 0, "pnl": 0.0})
    for p in positions:
        if p["closed_dt"] is None:
            continue
        key = p["closed_dt"].strftime("%Y-%m-%d")
        daily[key]["n"] += 1
        daily[key]["pnl"] += p["realized_pnl_sol"]
    header = ["Date", "Trades", "PnL (SOL)", "Cumulative"]
    widths = [12, 8, 12, 12]
    rows = []
    cumulative = 0.0
    for key in sorted(daily):
        cumulative += daily[key]["pnl"]
        rows.append([key, str(daily[key]["n"]), fmt_sol(daily[key]["pnl"]), fmt_sol(cumulative)])
    report.table(header, rows, widths)
    if daily:
        best = max(daily.items(), key=lambda kv: kv[1]["pnl"])
        worst = min(daily.items(), key=lambda kv: kv[1]["pnl"])
        report.add(f"- Best day: **{best[0]}** ({fmt_sol(best[1]['pnl'])} SOL, {best[1]['n']} trades)")
        report.add(f"- Worst day: **{worst[0]}** ({fmt_sol(worst[1]['pnl'])} SOL, {worst[1]['n']} trades)")
    report.add()

    # ---------- 7. Slippage estimate by tier ----------
    report.add("## 7. Slippage-Adjusted PnL by Tier")
    slip_stats = {label: {"n": 0, "paper_pnl": 0.0, "realistic_pnl": 0.0, "slip_cost": 0.0} for label, _, _ in MCAP_TIERS}
    for p in positions:
        mcap = p["mcap_usd"]
        label = tier_label(mcap)
        pct = slippage_pct(mcap)
        amount = p["amount_sol"] or 0.0
        entry_cost = amount * pct
        exit_value = amount + p["realized_pnl_sol"]
        exit_cost = exit_value * pct if exit_value > 0 else 0.0
        st = slip_stats[label]
        st["n"] += 1
        st["paper_pnl"] += p["realized_pnl_sol"]
        st["slip_cost"] += entry_cost + exit_cost
        st["realistic_pnl"] += p["realized_pnl_sol"] - entry_cost - exit_cost
    header = ["Tier", "Slippage", "Trades", "Paper PnL", "Slip cost", "Realistic PnL"]
    widths = [10, 10, 8, 12, 12, 14]
    rows = []
    tier_pct = {"<$5K": 0.08, "$5K-10K": 0.08, "$10K-20K": 0.05, "$20K-50K": 0.03, "$50K+": 0.03}
    for label, _, _ in MCAP_TIERS:
        st = slip_stats[label]
        rows.append([
            label,
            f"{tier_pct[label]:.0%}",
            str(st["n"]), fmt_sol(st["paper_pnl"]), f"-{st['slip_cost']:.4f}", fmt_sol(st["realistic_pnl"]),
        ])
    report.table(header, rows, widths)
    report.add("> Slippage model: round-trip cost = entry slippage (amount_sol × pct) + exit slippage (exit value × pct); "
               "`<$10K`=8%, `$10-20K`=5%, `$20K+`=3%.")
    report.add()

    # ---------- 8. Gate effectiveness ----------
    report.add("## 8. Gate Effectiveness")
    if gates:
        for idx, g in enumerate(gates):
            cfg = g["config"]
            cutoff = g["updated_at"]
            if idx == 0:
                cohort = [p for p in positions if p["opened_dt"] is not None and p["opened_dt"] < cutoff]
                label = f"Before gate #{g['id']} ({cutoff:%Y-%m-%d %H:%M})"
            else:
                prev_cutoff = gates[idx - 1]["updated_at"]
                cohort = [
                    p for p in positions
                    if p["opened_dt"] is not None and prev_cutoff <= p["opened_dt"] < cutoff
                ]
                label = f"Under gate #{g['id']} ({cutoff:%Y-%m-%d %H:%M})"
            n = len(cohort)
            pnl = sum(p["realized_pnl_sol"] for p in cohort)
            wins = sum(1 for p in cohort if p["realized_pnl_sol"] > 0)
            wr = f"{wins / n:.1%}" if n else "—"
            cfg_summary = ", ".join(
                f"{k}={v}" for k, v in sorted(cfg.items())
            )
            if g["reason"] == "manual_freeze":
                change = "manual_freeze"
            elif idx == 0:
                change = "INITIAL"
            else:
                change = "auto_tuned"
            report.add(f"### Gate #{g['id']} — {cutoff:%Y-%m-%d %H:%M} UTC — {change}")
            report.add(f"- Config: `{cfg_summary}`")
            report.add(f"- Cohort: {n} trades, PnL {fmt_sol(pnl)} SOL, win rate {wr}")
            report.add()
        # trades after the last gate
        last_cutoff = gates[-1]["updated_at"]
        cohort = [p for p in positions if p["opened_dt"] is not None and p["opened_dt"] >= last_cutoff]
        n = len(cohort)
        pnl = sum(p["realized_pnl_sol"] for p in cohort)
        wins = sum(1 for p in cohort if p["realized_pnl_sol"] > 0)
        wr = f"{wins / n:.1%}" if n else "—"
        report.add(f"### After last gate #{gates[-1]['id']} ({last_cutoff:%Y-%m-%d %H:%M} UTC)")
        report.add(f"- Cohort: {n} trades, PnL {fmt_sol(pnl)} SOL, win rate {wr}")
        report.add()
    else:
        report.add("- No gate_config history found.")
        report.add()

    # ---------- 9. Shadow mode data ----------
    report.add("## 9. Shadow Mode Data (Jupiter Quotes)")
    if not quotes:
        report.add("- No rows in `jupiter_quotes` — shadow mode is not capturing data yet (0 quotes).")
    else:
        q_by_mcap: dict[str, list[float]] = defaultdict(list)
        pos_mcap = {p["id"]: p["mcap_usd"] for p in positions}
        header = ["Tier", "Quotes", "Avg slippage vs paper"]
        widths = [10, 8, 22]
        rows = []
        for q in quotes:
            tier = tier_label(pos_mcap.get(q["position_id"]) if "position_id" in q.keys() else None)
            slip = q["slippage_vs_paper_pct"]
            if slip is not None:
                q_by_mcap[tier].append(float(slip))
        for label, _, _ in MCAP_TIERS:
            vals = q_by_mcap.get(label, [])
            avg = sum(vals) / len(vals) if vals else None
            rows.append([label, str(len(vals)), f"{avg:.2f}%" if avg is not None else "n/a"])
        report.table(header, rows, widths)
        report.add()

    output = "\n".join(report.lines) + "\n"
    print(output)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(output)
    print(f"Report saved to {REPORT_PATH}")
    db.close()


if __name__ == "__main__":
    main()

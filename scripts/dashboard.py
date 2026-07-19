"""Live monitoring dashboard for both strategies.

Serves a single HTML page on 0.0.0.0:8100 with process status,
open positions, closed trades, pipeline stats, and tracker activity.
"""

from __future__ import annotations

import http.server
import json
import logging
import os
import re
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

HOST = "0.0.0.0"
PORT = 8100

DB_PATH = Path("data/trades.db")
STRATEGY_A_LOG = Path("/home/dev/paper_loop.log")
STRATEGY_B_LOG = Path("/home/dev/strategy_b.log")
WHALE_LOG = Path("/tmp/whale_tracker.log")
INFLUENCER_LOG = Path("/tmp/influencer_tracker.log")
NARRATIVE_LOG = Path("/tmp/narrative_tracker.log")

log = logging.getLogger("dashboard")


def process_running(name_substr: str) -> bool:
    try:
        result = subprocess.run(
            ["pgrep", "-f", name_substr],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def browser_pc_alive() -> bool:
    try:
        import httpx
        r = httpx.get("http://127.0.0.1:8099/health", timeout=3)
        return r.status_code < 500
    except Exception:
        return False


def last_log_seconds(log_path: Path) -> float | None:
    if not log_path.exists():
        return None
    try:
        mtime = log_path.stat().st_mtime
        return time.time() - mtime
    except OSError:
        return None


def safe_price(val: float | None) -> str:
    if val is None:
        return "—"
    if val == 0:
        return "$0"
    if val < 0.0001:
        return f"${val:.2e}"
    if val < 1:
        return f"${val:.6f}"
    return f"${val:.4f}"


def pct_str(val: float) -> str:
    if val >= 0:
        return f"+{val:.1f}%"
    return f"{val:.1f}%"


def age_str(ts_iso: str) -> str:
    try:
        ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - ts
        mins = int(delta.total_seconds() / 60)
        secs = int(delta.total_seconds()) % 60
        if mins > 0:
            return f"{mins}min"
        return f"{secs}s"
    except Exception:
        return "?"


def sol_str(val: float) -> str:
    if abs(val) < 0.001:
        return f"{val:.6f}"
    return f"{val:.4f}"


def get_db():
    if not DB_PATH.exists():
        return None
    try:
        return sqlite3.connect(str(DB_PATH))
    except Exception:
        return None


def get_open_positions(db) -> list[dict]:
    rows = []
    try:
        cursor = db.execute(
            "SELECT mint_address, entry_price_sol, amount_sol, token_amount, "
            "opened_at, partial_exits_json FROM positions WHERE status='open' "
            "ORDER BY opened_at DESC"
        )
        for row in cursor.fetchall():
            mint, entry, amount, tokens, opened_at, pej = row
            try:
                pej_data = json.loads(pej)
                mode = pej_data.get("mode", "paper")
            except Exception:
                mode = "paper"
            rows.append({
                "mint": mint,
                "mint_short": mint[:10] + "…",
                "entry": entry,
                "amount": amount,
                "tokens": tokens,
                "opened_at": opened_at,
                "age": age_str(opened_at),
                "mode": mode,
            })
    except Exception as exc:
        log.warning("get_open_positions: %s", exc)
    return rows


def get_today_closed(db) -> list[dict]:
    rows = []
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cursor = db.execute(
            "SELECT mint_address, entry_price_sol, realized_pnl_sol, "
            "close_price_sol, peak_price_sol, closed_at, opened_at, "
            "partial_exits_json FROM positions WHERE status='CLOSED' "
            "AND date(closed_at) = ? ORDER BY closed_at DESC",
            (today,),
        )
        for row in cursor.fetchall():
            mint, entry, pnl, close_price, peak_price, closed_at, opened_at, pej = row
            pnl_pct = ((close_price or entry) / entry - 1) * 100 if close_price and entry else 0
            peak_pct = ((peak_price or entry) / entry - 1) * 100 if peak_price and entry else 0
            left_on_table = peak_pct - pnl_pct if peak_price else 0
            close_reason = "?"
            if pej:
                try:
                    pej_data = json.loads(pej)
                    close_reason = pej_data.get("close_reason", "?")
                except Exception:
                    pass
            try:
                pej_data = json.loads(pej)
                mode = pej_data.get("mode", "paper")
            except Exception:
                mode = "paper"
            rows.append({
                "mint": mint,
                "mint_short": mint[:8] + "…",
                "entry": entry,
                "pnl_sol": pnl,
                "pnl_pct": pnl_pct,
                "peak_pct": peak_pct,
                "left_on_table": left_on_table,
                "close_reason": close_reason,
                "closed_at": closed_at,
                "age": age_str(opened_at),
            })
    except Exception as exc:
        log.warning("get_today_closed: %s", exc)
    return rows


def parse_strategy_a_log() -> dict:
    result = {"candidates": "?", "rugcheck_pass": "?", "entered": "?", "log_line": ""}
    if not STRATEGY_A_LOG.exists():
        return result
    try:
        lines = STRATEGY_A_LOG.read_text().splitlines()
        last_50 = lines[-50:]
        text = "\n".join(last_50)
        candidates = len(re.findall(r"RESOLVED\s+\S+\s+→", text))
        rugcheck = len(re.findall(r"RugCheck\s+OK|rugcheck.*pass", text, re.I))
        entered = len(re.findall(r"ENTER\b", text))
        scan_line = ""
        for line in reversed(last_50):
            if "candidates" in line.lower() or "browser-pc:" in line:
                scan_line = line.strip()
                break
        result.update({
            "candidates": str(candidates) if candidates > 0 else "0",
            "rugcheck_pass": str(rugcheck),
            "entered": str(entered),
            "log_line": scan_line,
        })
    except Exception as exc:
        log.warning("parse_strategy_a_log: %s", exc)
    return result


def parse_strategy_b_log() -> dict:
    result = {"total": "?", "age_pass": "?", "mcap_pass": "?", "txn_pass": "?", "entered": "?", "log_line": ""}
    if not STRATEGY_B_LOG.exists():
        return result
    try:
        lines = STRATEGY_B_LOG.read_text().splitlines()
        last_50 = lines[-50:]
        text = "\n".join(last_50)
        pipe_match = re.search(r"Pipe:\s*total=(\d+)\s*age_pass=(\d+)\s*mcap_pass=(\d+)\s*txn_pass=(\d+)", text)
        if pipe_match:
            result.update({
                "total": pipe_match.group(1),
                "age_pass": pipe_match.group(2),
                "mcap_pass": pipe_match.group(3),
                "txn_pass": pipe_match.group(4),
            })
        entered = len(re.findall(r"ENTER\b", text))
        grok_reached = len(re.findall(r"grok_reached=(\d+)", text))
        result["entered"] = str(entered)
        pipe_line = ""
        for line in reversed(last_50):
            if line.startswith("Pipe:"):
                pipe_line = line.strip()
                break
        result["log_line"] = pipe_line
    except Exception as exc:
        log.warning("parse_strategy_b_log: %s", exc)
    return result


def parse_tracker_log(log_path: Path, label: str) -> str:
    if not log_path.exists():
        return f"{label}: file not found"
    try:
        lines = log_path.read_text().splitlines()
        last_5 = lines[-5:] if len(lines) >= 5 else lines
        summary_line = ""
        for line in reversed(last_5):
            if "No whale activity" in line or "No" in line and "cycle" in line:
                summary_line = line.strip()
                break
            if any(x in line for x in ["NEW:", "keyword", "match", "keywords:", "last cycle", "cycle"]):
                summary_line = line.strip()
                break
        if not summary_line:
            ts_line = last_5[-1] if last_5 else ""
            summary_line = ts_line.strip() if ts_line else "(empty)"
        ts_match = re.search(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", summary_line)
        if ts_match:
            logged_ts = ts_match.group(1)
            try:
                lt = datetime.strptime(logged_ts, "%Y-%m-%d %H:%M:%S")
                seconds_ago = int((datetime.now() - lt.replace(tzinfo=None)).total_seconds())
            except Exception:
                seconds_ago = "?"
            summary_line = re.sub(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}\s*", "", summary_line)
            summary_line = re.sub(r"\[\w+\]\s*", "", summary_line)
            return f"{label}: {seconds_ago}s ago — {summary_line.strip()}"
        return f"{label}: {summary_line.strip()}"
    except Exception as exc:
        return f"{label}: error ({exc})"


def build_html() -> str:
    db = get_db()

    strat_a_alive = process_running("run_paper_loop")
    strat_b_alive = process_running("run_strategy_b")
    browser_pc = browser_pc_alive()
    watchdog_alive = process_running("watchdog_memecoin")

    a_secs = last_log_seconds(STRATEGY_A_LOG)
    b_secs = last_log_seconds(STRATEGY_B_LOG)

    open_positions = get_open_positions(db) if db else []
    today_closed = get_today_closed(db) if db else []

    total_trades = len(today_closed)
    wins = sum(1 for t in today_closed if t["pnl_sol"] > 0)
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    total_pnl = sum(t["pnl_sol"] for t in today_closed)

    strat_a_stats = parse_strategy_a_log()
    strat_b_stats = parse_strategy_b_log()

    whale_info = parse_tracker_log(WHALE_LOG, "WHALE TRACKER")
    influencer_info = parse_tracker_log(INFLUENCER_LOG, "INFLUENCER TRACKER")
    narrative_info = parse_tracker_log(NARRATIVE_LOG, "NARRATIVE TRACKER")

    def status_dot(alive: bool) -> str:
        return '<span class="dot on">&#9679;</span>' if alive else '<span class="dot off">&#9679;</span>'

    a_dot = status_dot(strat_a_alive)
    b_dot = status_dot(strat_b_alive)
    bp_dot = status_dot(browser_pc)
    wd_dot = status_dot(watchdog_alive)

    a_last = f"{int(a_secs)}s ago" if a_secs is not None else "N/A"
    b_last = f"{int(b_secs)}s ago" if b_secs is not None else "N/A"

    positions_rows = ""
    for p in open_positions:
        current_price = p["entry"]
        pnl_pct = 0
        try:
            if db:
                cur = db.execute(
                    "SELECT price_sol FROM trades WHERE mint_address=? AND side='SELL' ORDER BY executed_at DESC LIMIT 1",
                    (p["mint"],),
                )
                row = cur.fetchone()
                if row and row[0]:
                    current_price = row[0]
                    pnl_pct = (current_price / p["entry"] - 1) * 100
        except Exception:
            pass
        positions_rows += f"""
          <tr>
            <td>{p['mint_short']}</td>
            <td>{p['mode']}</td>
            <td>{safe_price(p['entry'])}</td>
            <td>{safe_price(current_price)}</td>
            <td class="{'green' if pnl_pct >= 0 else 'red'}">{pct_str(pnl_pct)}</td>
            <td>{p['age']}</td>
          </tr>"""

    closed_rows = ""
    for t in today_closed:
        color_class = "green" if t["pnl_sol"] >= 0 else "red"
        left_class = "red" if t["left_on_table"] < 0 else ""
        closed_rows += f"""
          <tr>
            <td>{t['mint_short']}</td>
            <td class="{color_class}">{pct_str(t['pnl_pct'])}</td>
            <td>{pct_str(t['peak_pct'])}</td>
            <td class="{left_class}">{pct_str(t['left_on_table'])}</td>
            <td>{t['close_reason']}</td>
            <td>{t['age']}</td>
          </tr>"""

    tracker_lines = f"""
          <p>{whale_info}</p>
          <p>{influencer_info}</p>
          <p>{narrative_info}</p>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="10">
<title>Memecoin Trader — Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0d1117; color: #c9d1d9; font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', 'Consolas', monospace; padding: 20px; }}
  h1 {{ font-size: 18px; margin-bottom: 12px; color: #f0f6fc; }}
  h2 {{ font-size: 14px; margin: 16px 0 8px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }}
  .header {{ display: flex; gap: 16px; flex-wrap: wrap; align-items: center; padding: 12px 16px; background: #161b22; border: 1px solid #30363d; border-radius: 6px; margin-bottom: 12px; }}
  .header-item {{ font-size: 13px; }}
  .header-item .label {{ color: #8b949e; }}
  .header .last-scan {{ font-size: 12px; color: #8b949e; margin-left: auto; }}
  .dot {{ font-size: 14px; margin-right: 4px; }}
  .dot.on {{ color: #3fb950; }}
  .dot.off {{ color: #f85149; }}
  .green {{ color: #3fb950; }}
  .red {{ color: #f85149; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 8px; font-size: 12px; }}
  th {{ text-align: left; padding: 6px 8px; border-bottom: 1px solid #30363d; color: #8b949e; font-weight: normal; font-size: 11px; text-transform: uppercase; }}
  td {{ padding: 4px 8px; border-bottom: 1px solid #21262d; }}
  .summary-row {{ display: flex; gap: 24px; padding: 8px 12px; background: #161b22; border: 1px solid #30363d; border-radius: 6px; margin-bottom: 8px; font-size: 13px; }}
  .summary-item {{ }}
  .summary-item .value {{ font-size: 18px; font-weight: bold; }}
  .summary-item .label {{ font-size: 10px; color: #8b949e; text-transform: uppercase; }}
  .pipeline {{ display: flex; gap: 24px; }}
  .pipeline-box {{ flex: 1; padding: 10px 14px; background: #161b22; border: 1px solid #30363d; border-radius: 6px; }}
  .pipeline-box h3 {{ font-size: 12px; color: #8b949e; margin-bottom: 6px; }}
  .pipeline-stats {{ list-style: none; font-size: 12px; }}
  .pipeline-stats li {{ padding: 2px 0; }}
  .pipeline-stats li .num {{ color: #58a6ff; }}
  .tracker {{ padding: 8px 12px; background: #161b22; border: 1px solid #30363d; border-radius: 6px; font-size: 12px; }}
  .tracker p {{ padding: 2px 0; }}
  .empty {{ color: #484f58; font-style: italic; padding: 8px 0; }}
  .footer {{ margin-top: 16px; font-size: 10px; color: #484f58; text-align: center; }}
</style>
</head>
<body>
<h1>&#x1f4b0; Memecoin Trader</h1>

<div class="header">
  <div class="header-item">{a_dot} <span class="label">Strategy A</span></div>
  <div class="header-item">{b_dot} <span class="label">Strategy B</span></div>
  <div class="header-item">{bp_dot} <span class="label">browser-pc</span></div>
  <div class="header-item">{wd_dot} <span class="label">Watchdog</span></div>
  <div class="last-scan">A: {a_last} &middot; B: {b_last}</div>
</div>

<h2>Positions ({len(open_positions)} open)</h2>
<table>
  <tr><th>Mint</th><th>Mode</th><th>Entry</th><th>Current</th><th>PnL</th><th>Age</th></tr>
  {positions_rows if positions_rows else '<tr><td colspan="6" class="empty">No open positions</td></tr>'}
</table>

<h2>Today&#x27;s Closed</h2>
<div class="summary-row">
  <div class="summary-item"><div class="value">{total_trades}</div><div class="label">Trades</div></div>
  <div class="summary-item"><div class="value">{win_rate:.0f}%</div><div class="label">Win Rate</div></div>
  <div class="summary-item"><div class="value {'green' if total_pnl >= 0 else 'red'}">{sol_str(total_pnl)}</div><div class="label">PnL (SOL)</div></div>
</div>
<table>
  <tr><th>Mint</th><th>PnL</th><th>Peak</th><th>Left</th><th>Exit</th><th>Age</th></tr>
  {closed_rows if closed_rows else '<tr><td colspan="6" class="empty">No closed trades today</td></tr>'}
</table>

<div class="pipeline">
  <div class="pipeline-box">
    <h3>Strategy A — Last Scan</h3>
    <ul class="pipeline-stats">
      <li>Candidates: <span class="num">{strat_a_stats['candidates']}</span></li>
      <li>RugCheck pass: <span class="num">{strat_a_stats['rugcheck_pass']}</span></li>
      <li>Entered: <span class="num">{strat_a_stats['entered']}</span></li>
    </ul>
    <div style="font-size:10px;color:#484f58;margin-top:4px;word-break:break-all;">{strat_a_stats['log_line'][:120]}</div>
  </div>
  <div class="pipeline-box">
    <h3>Strategy B — Last Scan</h3>
    <ul class="pipeline-stats">
      <li>Candidates: <span class="num">{strat_b_stats['total']}</span></li>
      <li>Age pass: <span class="num">{strat_b_stats['age_pass']}</span></li>
      <li>MCap pass: <span class="num">{strat_b_stats['mcap_pass']}</span></li>
      <li>Txn pass: <span class="num">{strat_b_stats['txn_pass']}</span></li>
      <li>Entered: <span class="num">{strat_b_stats['entered']}</span></li>
    </ul>
    <div style="font-size:10px;color:#484f58;margin-top:4px;word-break:break-all;">{strat_b_stats['log_line'][:120]}</div>
  </div>
</div>

<h2>Tracker Activity</h2>
<div class="tracker">
  {tracker_lines}
</div>

<div class="footer">Auto-refresh every 10s &middot; {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</div>
</body>
</html>"""
    return html


class DashboardHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            html = build_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        log.info(format, *args)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = http.server.HTTPServer((HOST, PORT), DashboardHandler)
    log.info("Dashboard listening on http://%s:%d", HOST, PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        server.shutdown()


if __name__ == "__main__":
    main()

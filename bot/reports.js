"use strict";
const path = require("node:path");
const db = require("./db");
const sys = require("./system");
const lib = require("./lib");

const DAY_MS = 24 * 3600 * 1000;
const TUNER_START_CLOSES = 50;

const SCRIPT_DIR = path.join(__dirname, "..", "scripts");

function tickerOrShort(mint) {
  const t = db.tickerForMint(mint);
  if (t) return t.toUpperCase().slice(0, 12);
  return mint ? `${mint.slice(0, 6)}..` : "unknown";
}

function strategyBreakdown(positions) {
  const byStrategy = { A: [], B: [] };
  for (const p of positions) {
    const key = (p.strategy || "A").toUpperCase();
    (byStrategy[key] || (byStrategy[key] = [])).push(p);
  }
  const out = {};
  for (const [key, rows] of Object.entries(byStrategy)) {
    const pnl = rows.reduce((s, r) => s + (r.realized_pnl_sol || 0), 0);
    const wins = rows.filter((r) => (r.realized_pnl_sol || 0) > 0).length;
    let best = null;
    let worst = null;
    for (const r of rows) {
      const v = r.realized_pnl_sol || 0;
      if (best == null || v > best.pnl) best = { pnl: v, ticker: tickerOrShort(r.mint_address) };
      if (worst == null || v < worst.pnl) worst = { pnl: v, ticker: tickerOrShort(r.mint_address) };
    }
    out[key] = {
      closed: rows.length,
      pnl,
      winRate: rows.length ? wins / rows.length : 0,
      best,
      worst,
    };
  }
  return out;
}

/** Per-strategy + combined PnL for the current ET day. */
function pnlReport(dayStartUtc) {
  const dayEnd = dayStartUtc + DAY_MS;
  const dayClosed = db.closedInWindow(dayStartUtc, dayEnd);
  const allClosed = db.closedPositions();
  const br = strategyBreakdown(dayClosed);
  const a = br.A || { closed: 0, pnl: 0, winRate: 0, best: null, worst: null };
  const b = br.B || { closed: 0, pnl: 0, winRate: 0, best: null, worst: null };
  const todayPnl = dayClosed.reduce((s, r) => s + (r.realized_pnl_sol || 0), 0);
  const allTimePnl = allClosed.reduce((s, r) => s + (r.realized_pnl_sol || 0), 0);
  const openCount = db.openPositionCount();

  const lines = [];
  lines.push(`*Today's PnL — ${lib.fmtEtDate(dayStartUtc)}*`);
  lines.push("");
  for (const [label, s] of [
    ["Strategy A", a],
    ["Strategy B", b],
  ]) {
    lines.push(`*${label}*`);
    lines.push(`Closed: ${s.closed} trade${s.closed === 1 ? "" : "s"} | PnL: ${lib.fmtSolShort(s.pnl)}`);
    lines.push(`Win rate: ${lib.pct(s.winRate)}`);
    if (s.best && s.worst) {
      lines.push(`Best: ${lib.fmtSol(s.best.pnl)} (${s.best.ticker}) | Worst: ${lib.fmtSol(s.worst.pnl)} (${s.worst.ticker})`);
    }
    lines.push("");
  }
  lines.push(`*Combined*`);
  lines.push(`Today: ${lib.fmtSolShort(todayPnl)} | All-time: ${lib.fmtSolShort(allTimePnl)}`);
  lines.push(`Open positions: ${openCount}`);
  return lines.join("\n");
}

/** Loop + browser-pc health, last trade time, recent errors. */
function checkReport() {
  const snap = sys.healthSnapshot();
  const lines = [];
  lines.push(`*Health Check — ${lib.fmtEtTime(Date.now())}*`);
  lines.push("");
  lines.push(`Strategy A: ${snap.A.alive ? `ALIVE (pid ${snap.A.pid})` : "DEAD"}`);
  lines.push(`Strategy B: ${snap.B.alive ? `ALIVE (pid ${snap.B.pid})` : "DEAD"}`);
  lines.push(`browser-pc: ${snap.browserPc.ok ? "ok" : `DOWN (${snap.browserPc.detail})`}`);
  lines.push("");
  const last = db.lastTradeTimeMs();
  lines.push(`Last trade: ${last ? lib.fmtEtTime(last) : "never"}`);
  lines.push("");
  lines.push(`*Errors in last hour*`);
  const sources = [
    ["Strategy A log", sys.LOOP_CMD.A.log],
    ["Strategy B log", "/tmp/strategy_b.log"],
    ["browser-pc log", "/tmp/browser_service.log"],
  ];
  let total = 0;
  for (const [label, file] of sources) {
    const e = sys.scanLogErrors(file, 3600000);
    total += e.count;
    lines.push(`${label}: ${e.count}`);
  }
  if (total === 0) lines.push("None. All clear.");
  return lines.join("\n");
}

/** Gate funnel from candidate_log + auto-tuner state. */
function statusReport(dayStartUtc) {
  const lines = [];
  lines.push(`*Gate Funnel — ${lib.fmtEtDate(dayStartUtc)}*`);
  lines.push("");
  let totalCandidates = 0;
  let totalEntered = 0;
  const blockers = new Map();
  for (const strategy of ["A", "B"]) {
    const f = db.funnelForStrategy(strategy, dayStartUtc);
    totalCandidates += f.candidates;
    totalEntered += f.entered;
    for (const [g, c] of f.blockers) blockers.set(g, (blockers.get(g) || 0) + c);
    lines.push(
      `${strategy}: ${f.candidates} candidates | ${f.entered} entered | ${f.candidates - f.entered} rejected` +
        (f.mainBlocker ? ` (blocker: ${f.mainBlocker})` : "")
    );
  }
  lines.push(`Total: ${totalCandidates} scanned | ${totalEntered} entered`);
  lines.push("");
  lines.push(`*Auto-tuner (Strategy B)*`);
  const closedB = db.closedPositions().filter((p) => (p.strategy || "A").toUpperCase() === "B").length;
  lines.push(`Closed trades: ${closedB} / ${TUNER_START_CLOSES} (tuning starts after ${TUNER_START_CLOSES})`);
  const cfg = db.latestGateConfig("B");
  if (cfg) {
    lines.push(`Last gate config: id ${cfg.id} (${cfg.reason || "n/a"}${cfg.sample_size ? `, sample ${cfg.sample_size}` : ""})`);
    try {
      const j = JSON.parse(cfg.config_json || "{}");
      const parts = [];
      if (j.max_age_minutes != null) parts.push(`age<=${j.max_age_minutes}m`);
      if (j.min_mcap_usd != null) parts.push(`mcap>=${lib.fmtUsd(j.min_mcap_usd)}`);
      if (j.min_volume_usd != null) parts.push(`vol>=${lib.fmtUsd(j.min_volume_usd)}`);
      if (j.min_buy_sell_ratio != null) parts.push(`b/s>=${j.min_buy_sell_ratio}`);
      if (parts.length) lines.push(`Thresholds: ${parts.join("  ")}`);
    } catch {
      /* malformed config_json */
    }
  } else {
    lines.push("No gate_config row yet.");
  }
  return lines.join("\n");
}

/** Full day summary (midnight wire + "today" command). */
function summaryReport(dayStartUtc) {
  const dayEnd = dayStartUtc + DAY_MS;
  const dayClosed = db.closedInWindow(dayStartUtc, dayEnd);
  const allClosed = db.closedPositions();
  const br = strategyBreakdown(dayClosed);
  const a = br.A || { closed: 0, pnl: 0, winRate: 0, best: null, worst: null };
  const b = br.B || { closed: 0, pnl: 0, winRate: 0, best: null, worst: null };
  const todayPnl = dayClosed.reduce((s, r) => s + (r.realized_pnl_sol || 0), 0);
  const allTimePnl = allClosed.reduce((s, r) => s + (r.realized_pnl_sol || 0), 0);
  const openCount = db.openPositionCount();

  const lines = [];
  lines.push(`*Daily Summary — ${lib.fmtEtDate(dayStartUtc)}*`);
  lines.push("");
  for (const [label, s] of [
    ["Strategy A", a],
    ["Strategy B", b],
  ]) {
    lines.push(`*${label}*`);
    lines.push(`Closed: ${s.closed} | PnL: ${lib.fmtSolShort(s.pnl)} | Win rate: ${lib.pct(s.winRate)}`);
    if (s.best && s.worst) {
      lines.push(`Best: ${lib.fmtSol(s.best.pnl)} (${s.best.ticker}) | Worst: ${lib.fmtSol(s.worst.pnl)} (${s.worst.ticker})`);
    }
    lines.push("");
  }
  lines.push(`*Combined*`);
  lines.push(`Today: ${lib.fmtSolShort(todayPnl)} | All-time: ${lib.fmtSolShort(allTimePnl)} | Open: ${openCount}`);
  lines.push("");
  lines.push(`*System*`);
  const snap = sys.healthSnapshot();
  lines.push(`Strategy A: ${snap.A.alive ? "ALIVE" : "DOWN"}`);
  lines.push(`Strategy B: ${snap.B.alive ? "ALIVE" : "DOWN"}`);
  lines.push(`browser-pc: ${snap.browserPc.ok ? "ok" : "DOWN"}`);
  lines.push(`Helius: ${heliusLine()}`);
  lines.push("");
  lines.push(`*Auto-tuner*`);
  const cfgChanges = db.gateConfigsSince(dayStartUtc);
  lines.push(`Gate changes today: ${cfgChanges.length}`);
  const cfg = db.latestGateConfig("B");
  if (cfg) {
    try {
      const j = JSON.parse(cfg.config_json || "{}");
      const parts = [];
      if (j.max_age_minutes != null) parts.push(`age<=${j.max_age_minutes}m`);
      if (j.min_mcap_usd != null) parts.push(`mcap>=${lib.fmtUsd(j.min_mcap_usd)}`);
      if (j.min_volume_usd != null) parts.push(`vol>=$` + j.min_volume_usd);
      if (j.min_buy_sell_ratio != null) parts.push(`b/s>=${j.min_buy_sell_ratio}`);
      lines.push(`Current: ${parts.join("  ") || cfg.config_json}`);
    } catch {
      lines.push(`Current: ${cfg.config_json}`);
    }
  }
  lines.push("");
  lines.push(`*Funnel (today)*`);
  let candidates = 0;
  let entered = 0;
  const blockers = new Map();
  for (const s of ["A", "B"]) {
    const f = db.funnelForStrategy(s, dayStartUtc);
    candidates += f.candidates;
    entered += f.entered;
    for (const [g, c] of f.blockers) blockers.set(g, (blockers.get(g) || 0) + c);
  }
  lines.push(`Candidates scanned: ${candidates}`);
  lines.push(`Entered: ${entered} | Rejected: ${candidates - entered}`);
  if (blockers.size) {
    const top = [...blockers.entries()].sort((x, y) => y[1] - x[1]).slice(0, 3);
    lines.push(`Top blockers: ${top.map(([g, c]) => `${g} (${c})`).join(", ")}`);
  }
  return lines.join("\n");
}

function heliusLine() {
  const u = sys.heliusUsage();
  if (u.error && !u.exhausted) return `n/a (${u.error})`;
  if (u.exhausted) return "MAX USAGE REACHED — credits exhausted";
  if (u.used != null) {
    const pctUsed = u.limit ? ((u.used / u.limit) * 100).toFixed(1) : "?";
    const over = u.rateLimited ? " — OVER LIMIT" : "";
    return `${u.used.toLocaleString("en-US")} / ${u.limit ? u.limit.toLocaleString("en-US") : "?"} credits (${pctUsed}%)${over}`;
  }
  return "ok (usage endpoint unavailable)";
}

/** Last 5 closed trades: ticker, strategy, PnL, exit reason. */
function last5Report() {
  const rows = db.closedPositions().slice(0, 5);
  if (!rows.length) return "*Last 5 Closed Trades*\n\nNo closed trades yet.";
  const lines = ["*Last 5 Closed Trades*", ""];
  rows.forEach((p, i) => {
    const reason = db.closeReasonForMint(p.mint_address) || "n/a";
    const strategy = (p.strategy || "A").toUpperCase();
    lines.push(`${i + 1}. ${tickerOrShort(p.mint_address)} (${strategy})  ${lib.fmtSol(p.realized_pnl_sol)}  ${reason}`);
  });
  return lines.join("\n");
}

/** Current gate thresholds for both strategies. */
function gatesReport() {
  const lines = ["*Gate Thresholds*", ""];
  const aConsts = lib.parseScriptConstants(path.join(SCRIPT_DIR, "run_paper_loop.py"));
  const bConsts = lib.parseScriptConstants(path.join(SCRIPT_DIR, "run_strategy_b.py"));

  const aHolder = aConsts.HOLDER_MAX_PCT || 80;
  lines.push("*Strategy A (run_paper_loop.py)*");
  lines.push(`Top-10 holder max: ${aHolder}%`);
  const aTrail = lib.pyNum(aConsts.TRAILING_STOP_PCT);
  const aHard = lib.pyNum(aConsts.HARD_STOP_PCT);
  const aTime = lib.pyNum(aConsts.TIME_STOP_MINUTES);
  const aSize = lib.pyNum(aConsts.PAPER_SIZE_SOL);
  const aMaxOpen = lib.pyNum(aConsts.MAX_OPEN_POSITIONS);
  if (aTrail != null) lines.push(`Trailing stop: ${(aTrail * 100).toFixed(0)}%`);
  if (aHard != null) lines.push(`Hard stop: ${(aHard * 100).toFixed(0)}%`);
  if (aTime != null) lines.push(`Time stop: ${aTime}m`);
  if (aSize != null) lines.push(`Position size: ${aSize} SOL`);
  if (aMaxOpen != null) lines.push(`Max open: ${aMaxOpen}`);
  lines.push("");

  lines.push("*Strategy B (run_strategy_b.py)*");
  const cfg = db.latestGateConfig("B");
  if (cfg) {
    lines.push(`Tuner config id ${cfg.id} (${cfg.reason || "n/a"}):`);
    try {
      const j = JSON.parse(cfg.config_json || "{}");
      if (j.max_age_minutes != null) lines.push(`  max age: ${j.max_age_minutes}m`);
      if (j.min_mcap_usd != null) lines.push(`  min mcap: ${lib.fmtUsd(j.min_mcap_usd)}`);
      if (j.min_volume_usd != null) lines.push(`  min volume: ${lib.fmtUsd(j.min_volume_usd)}`);
      if (j.min_buy_sell_ratio != null) lines.push(`  min buy/sell: ${j.min_buy_sell_ratio}`);
    } catch {
      lines.push(`  ${cfg.config_json}`);
    }
  } else {
    lines.push("  (no gate_config row yet)");
  }
  const bMaxMcap = lib.pyNum(bConsts.MAX_MCAP_USD);
  const bDev = lib.pyNum(bConsts.MAX_DEV_HOLDINGS_PCT);
  const bHolder = lib.pyNum(bConsts.MAX_TOP10_HOLDER_PCT);
  const bTp = lib.pyNum(bConsts.TAKE_PROFIT_MULT);
  const bHardStop = lib.pyNum(bConsts.HARD_STOP_MULT);
  const bTime = lib.pyNum(bConsts.TIME_STOP_MINUTES);
  const bSize = lib.pyNum(bConsts.PAPER_SIZE_SOL);
  const bMaxOpen = lib.pyNum(bConsts.MAX_OPEN);
  const bMentions = lib.pyNum(bConsts.MIN_MENTIONS);
  if (bMaxMcap != null) lines.push(`Max mcap: ${lib.fmtUsd(bMaxMcap)}`);
  if (bDev != null) lines.push(`Max dev holdings: ${bDev}%`);
  if (bHolder != null) lines.push(`Top-10 holder max: ${bHolder}%`);
  if (bTp != null) lines.push(`Take profit: ${bTp}x`);
  if (bHardStop != null) lines.push(`Hard stop: ${bHardStop}x`);
  if (bTime != null) lines.push(`Time stop: ${bTime}m`);
  if (bSize != null) lines.push(`Position size: ${bSize} SOL`);
  if (bMaxOpen != null) lines.push(`Max open: ${bMaxOpen}`);
  if (bMentions != null) lines.push(`Min Grok mentions: ${bMentions}`);
  const tiers = lib.parseHolderTiers(bConsts.HOLDER_TIERS || "", bConsts);
  if (tiers.length) {
    const uniq = [...new Set(tiers.map((t) => `${t.warn}/${t.reject}`))];
    lines.push(`Holder tiers (warn/reject): ${uniq.join(", ")}`);
  }
  return lines.join("\n");
}

function helpReport() {
  return [
    "*Available commands*",
    "",
    "pnl — today's PnL by strategy",
    "check — loop health, last trade, recent errors",
    "status — gate funnel + auto-tuner state",
    "today — full day summary",
    "last 5 — last 5 closed trades",
    "gates — current gate thresholds",
    "kill A / start A — stop/start Strategy A",
    "kill B / start B — stop/start Strategy B",
  ].join("\n");
}

module.exports = {
  pnlReport,
  checkReport,
  statusReport,
  summaryReport,
  last5Report,
  gatesReport,
  helpReport,
  TUNER_START_CLOSES,
};

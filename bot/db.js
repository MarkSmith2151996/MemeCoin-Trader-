"use strict";
const { DatabaseSync } = require("node:sqlite");

let _dbPath = null;

function initDb(path) {
  _dbPath = path;
}

function openDb() {
  if (!_dbPath) throw new Error("db path not configured");
  return new DatabaseSync(_dbPath, { readOnly: true });
}

/** Run a SELECT returning all rows (fresh read-only connection per call). */
function all(sql, params = []) {
  const db = openDb();
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

/** Run a SELECT returning one row. */
function get(sql, params = []) {
  const db = openDb();
  try {
    return db.prepare(sql).get(...params);
  } finally {
    db.close();
  }
}

/** All closed positions sorted by close time (newest first). */
function closedPositions() {
  return all(
    "SELECT id, mint_address, strategy, status, realized_pnl_sol, opened_at, closed_at, close_price_sol, peak_price_sol " +
      "FROM positions WHERE status = 'CLOSED'"
  )
    .map((r) => ({ ...r }))
    .sort((a, b) => (parseIsoSafe(b.closed_at) || 0) - (parseIsoSafe(a.closed_at) || 0));
}

function openPositionCount() {
  const r = get("SELECT COUNT(*) AS n FROM positions WHERE status = 'OPEN'");
  return r ? r.n : 0;
}

/** Closed positions whose close time falls inside [startUtc, endUtc). */
function closedInWindow(startUtc, endUtc) {
  const rows = closedPositions();
  const start = startUtc == null ? -Infinity : startUtc;
  const end = endUtc == null ? Infinity : endUtc;
  return rows.filter((r) => {
    const t = parseIsoSafe(r.closed_at);
    return t != null && t >= start && t < end;
  });
}

/** Last SELL trade for a mint, with close_reason from metadata. */
function closeReasonForMint(mint) {
  const rows = all(
    "SELECT executed_at, metadata_json FROM trades WHERE mint_address = ? AND side = 'SELL' ORDER BY executed_at DESC LIMIT 20",
    [mint]
  );
  for (const r of rows) {
    try {
      const md = JSON.parse(r.metadata_json || "{}");
      const reason = md && md.metadata && md.metadata.close_reason;
      if (reason) return reason;
    } catch {
      /* malformed metadata — keep scanning */
    }
  }
  return null;
}

/** Latest ticker recorded for a mint (from candidate_log). */
function tickerForMint(mint) {
  const r = get(
    "SELECT ticker FROM candidate_log WHERE mint_address = ? AND ticker IS NOT NULL AND ticker != '' ORDER BY scan_time DESC LIMIT 1",
    [mint]
  );
  return r && r.ticker ? r.ticker : null;
}

function lastTradeTimeMs() {
  const r = get("SELECT executed_at FROM trades ORDER BY executed_at DESC LIMIT 1");
  return r ? parseIsoSafe(r.executed_at) : null;
}

/** Candidate funnel rows for one strategy since startUtc. */
function funnelForStrategy(strategy, startUtc) {
  const rows = all(
    "SELECT scan_time, strategy, mint_address, entered, gates_failed FROM candidate_log WHERE strategy = ?",
    [strategy]
  );
  const start = startUtc == null ? -Infinity : startUtc;
  const inWindow = rows.filter((r) => (parseIsoSafe(r.scan_time) || 0) >= start);
  const failedGates = new Map(); // gate name -> count
  for (const r of inWindow) {
    try {
      const f = JSON.parse(r.gates_failed || "{}");
      if (f && f.gate) failedGates.set(f.gate, (failedGates.get(f.gate) || 0) + 1);
    } catch {
      /* malformed gates_failed — ignore */
    }
  }
  const top = [...failedGates.entries()].sort((a, b) => b[1] - a[1]);
  return {
    candidates: inWindow.length,
    entered: inWindow.filter((r) => r.entered === 1 || r.entered === true).length,
    mainBlocker: top.length ? top[0][0] : null,
    blockers: top.slice(0, 5),
  };
}

/** Latest gate_config row per strategy. */
function latestGateConfig(strategy) {
  const rows = all(
    "SELECT id, strategy, updated_at, config_json, reason, sample_size, metrics_json " +
      "FROM gate_config WHERE strategy = ? ORDER BY id DESC LIMIT 1",
    [strategy]
  );
  return rows[0] || null;
}

/** All gate_config rows created on/after startUtc (for gate-change alerts). */
function gateConfigsSince(startUtc) {
  const rows = all("SELECT id, strategy, updated_at, config_json, reason, sample_size FROM gate_config ORDER BY id");
  return rows.filter((r) => (parseIsoSafe(r.updated_at) || 0) >= startUtc);
}

function maxGateConfigId() {
  const r = get("SELECT MAX(id) AS m FROM gate_config");
  return r && r.m != null ? r.m : 0;
}

function parseIsoSafe(s) {
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : null;
}

module.exports = {
  initDb,
  all,
  get,
  closedPositions,
  openPositionCount,
  closedInWindow,
  closeReasonForMint,
  tickerForMint,
  lastTradeTimeMs,
  funnelForStrategy,
  latestGateConfig,
  gateConfigsSince,
  maxGateConfigId,
};

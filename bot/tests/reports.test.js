"use strict";
const { test } = require("node:test");
const assert = require("node:assert");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { DatabaseSync } = require("node:sqlite");

const lib = require("../lib");
const db = require("../db");
const reports = require("../reports");

/* ---------------- lib ---------------- */

test("etDayStartUtc returns ET midnight in UTC (EDT summer)", () => {
  const now = Date.UTC(2026, 7, 5, 20, 30, 0); // Aug 5 2026 20:30 UTC = 16:30 EDT
  const start = lib.etDayStartUtc(0);
  const startIso = new Date(start).toISOString();
  assert.ok(start <= now && start > now - 24 * 3600 * 1000, `start ${startIso} must be within the last 24h of ${new Date(now).toISOString()}`);
});

test("etDayStartUtc dayOffset lands exactly 24h later", () => {
  const a = lib.etDayStartUtc(0);
  const b = lib.etDayStartUtc(1);
  assert.strictEqual(b - a, 24 * 3600 * 1000);
});

test("msUntilNextEtTime is positive and within a day", () => {
  const ms = lib.msUntilNextEtTime(0, 0);
  assert.ok(ms > 0 && ms <= 26 * 3600 * 1000);
});

test("formatters", () => {
  assert.strictEqual(lib.fmtSol(0.0123), "+0.0123 SOL");
  assert.strictEqual(lib.fmtSol(-0.0499), "-0.0499 SOL");
  assert.strictEqual(lib.fmtSolShort(0.05), "+0.05 SOL");
  assert.strictEqual(lib.pct(0.333), "33%");
  assert.strictEqual(lib.pct(0), "0%");
});

test("python literal parsers", () => {
  assert.strictEqual(lib.pyNum("2_000"), 2000);
  assert.strictEqual(lib.pyNum("0.04"), 0.04);
  assert.strictEqual(lib.pyNum("x"), null);
  assert.strictEqual(lib.pyStr('"abc"'), "abc");
  assert.strictEqual(lib.pyStr("'abc'"), "abc");
  const tiers = lib.parseHolderTiers(
    "[\n    (2, 30.0, MAX_TOP10_HOLDER_PCT),\n    (999, 30.0, 100.0),\n]",
    { MAX_TOP10_HOLDER_PCT: "100.0" }
  );
  assert.strictEqual(tiers.length, 2);
  assert.deepStrictEqual(tiers[0], { age: 2, warn: 30.0, reject: 100.0 });
});

test("parseScriptConstants extracts scalars and skips comments", () => {
  const tmp = path.join(os.tmpdir(), `mt510-consts-${Date.now()}.py`);
  fs.writeFileSync(
    tmp,
    [
      "A = 1",
      'S = "hello"',
      "B = 2.5  # trailing comment",
      "# C = 3",
      "D = [1, 2]",
    ].join("\n")
  );
  const c = lib.parseScriptConstants(tmp);
  assert.strictEqual(c.A, "1");
  assert.strictEqual(c.S, '"hello"');
  assert.strictEqual(c.B, "2.5");
  assert.strictEqual(c.C, undefined);
  fs.unlinkSync(tmp);
});

test("digitsOnly", () => {
  assert.strictEqual(lib.digitsOnly("+1 (734) 555-0100"), "17345550100");
  assert.strictEqual(lib.digitsOnly(""), "");
});

/* ---------------- reports against a fixture DB ---------------- */

function makeFixtureDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "mt510-"));
  const p = path.join(dir, "trades.db");
  const c = new DatabaseSync(p);
  c.exec(`
    CREATE TABLE positions (
      id TEXT PRIMARY KEY, mint_address TEXT, strategy TEXT DEFAULT 'A',
      status TEXT, realized_pnl_sol REAL, opened_at TEXT, closed_at TEXT,
      close_price_sol REAL, peak_price_sol REAL
    );
    CREATE TABLE trades (
      id TEXT PRIMARY KEY, mint_address TEXT, side TEXT, executed_at TEXT, metadata_json TEXT
    );
    CREATE TABLE candidate_log (
      id INTEGER PRIMARY KEY, scan_time TEXT, strategy TEXT, mint_address TEXT,
      ticker TEXT, entered BOOLEAN DEFAULT 0, gates_failed TEXT
    );
    CREATE TABLE gate_config (
      id INTEGER PRIMARY KEY, strategy TEXT, updated_at TEXT, config_json TEXT,
      reason TEXT, sample_size INTEGER, metrics_json TEXT
    );
    CREATE TABLE daily_stats (
      date TEXT PRIMARY KEY, strategy_a_trades INTEGER, strategy_a_pnl_sol REAL,
      strategy_a_win_rate REAL, strategy_b_trades INTEGER, strategy_b_pnl_sol REAL,
      strategy_b_win_rate REAL, total_pnl_sol REAL, cumulative_pnl_sol REAL,
      max_drawdown_sol REAL, sharpe_ratio REAL
    );
  `);
  c.close();
  return p;
}

function seedFixture(p) {
  const c = new DatabaseSync(p);
  const now = Date.now();
  const dayStart = lib.etDayStartUtc(0);
  const iso = (ms) => new Date(ms).toISOString();
  const ins = c.prepare(
    "INSERT INTO positions (id, mint_address, strategy, status, realized_pnl_sol, opened_at, closed_at) VALUES (?,?,?,?,?,?,?)"
  );
  ins.run("p1", "MINT_TICKER_A", "A", "CLOSED", 0.01, iso(dayStart + 1000), iso(dayStart + 2000));
  ins.run("p2", "MINT_B", "B", "CLOSED", -0.05, iso(dayStart + 1000), iso(dayStart + 2000));
  ins.run("p3", "MINT_B2", "B", "CLOSED", 0.03, iso(dayStart + 3000), iso(dayStart + 4000));
  ins.run("p4", "MINT_OLD", "A", "CLOSED", -0.02, iso(dayStart - 2 * 86400000), iso(dayStart - 2 * 86400000 + 60000));
  ins.run("p5", "MINT_OPEN", "B", "OPEN", 0.0, iso(now - 300000), null);

  const tr = c.prepare("INSERT INTO trades (id, mint_address, side, executed_at, metadata_json) VALUES (?,?,?,?,?)");
  tr.run("t1", "MINT_TICKER_A", "BUY", iso(dayStart + 1000), "{}");
  tr.run(
    "t2",
    "MINT_TICKER_A",
    "SELL",
    iso(dayStart + 2000),
    JSON.stringify({ metadata: { close_reason: "take_profit" } })
  );
  tr.run("t3", "MINT_B", "SELL", iso(dayStart + 2000), JSON.stringify({ metadata: { close_reason: "hard_stop" } }));
  tr.run("t4", "MINT_B2", "SELL", iso(dayStart + 4000), JSON.stringify({ metadata: { close_reason: "trailing_stop" } }));

  const cl = c.prepare(
    "INSERT INTO candidate_log (id, scan_time, strategy, mint_address, ticker, entered, gates_failed) VALUES (?,?,?,?,?,?,?)"
  );
  cl.run(1, iso(dayStart + 100), "B", "MINT_B", "WIZCAT", 1, "{}");
  cl.run(2, iso(dayStart + 200), "B", "MINT_X", "NOPE", 0, JSON.stringify({ gate: "volume", value: 5 }));
  cl.run(3, iso(dayStart + 300), "B", "MINT_Y", "ZERO", 0, JSON.stringify({ gate: "txn", value: 1 }));
  cl.run(4, iso(dayStart + 400), "B", "MINT_Z", "MAYBE", 0, JSON.stringify({ gate: "volume", value: 8 }));

  const gc = c.prepare(
    "INSERT INTO gate_config (id, strategy, updated_at, config_json, reason, sample_size) VALUES (?,?,?,?,?,?)"
  );
  gc.run(1, "B", iso(dayStart - 86400000), JSON.stringify({ max_age_minutes: 30, min_mcap_usd: 2000, min_volume_usd: 200 }), "initial", 0);
  c.close();
  return { dayStart };
}

test("pnlReport splits strategies and filters to today", () => {
  const p = makeFixtureDb();
  const { dayStart } = seedFixture(p);
  db.initDb(p);
  const out = reports.pnlReport(dayStart);
  assert.match(out, /\*Strategy B\*/);
  assert.doesNotMatch(out, /Strategy A/);
  assert.match(out, /Closed: 2 trades/); // B today
  assert.match(out, /-0\.02 SOL/); // B today = -0.05 + 0.03
  assert.match(out, /Win rate: 50%/); // B: 1 win of 2
  assert.match(out, /All-time: -0\.02 SOL/); // B only
  assert.match(out, /Open positions: 1/);
  fs.rmSync(path.dirname(p), { recursive: true, force: true });
});

test("last5Report shows ticker, strategy, pnl, exit reason", () => {
  const p = makeFixtureDb();
  seedFixture(p);
  db.initDb(p);
  const out = reports.last5Report();
  assert.match(out, /\*Last 5 Closed Trades\*/);
  assert.match(out, /WIZCAT \(B\)/); // ticker from candidate_log, newest first
  assert.match(out, /take_profit/);
  assert.match(out, /hard_stop/);
  assert.match(out, /trailing_stop/);
  fs.rmSync(path.dirname(p), { recursive: true, force: true });
});

test("statusReport shows funnel and tuner state", () => {
  const p = makeFixtureDb();
  const { dayStart } = seedFixture(p);
  db.initDb(p);
  const out = reports.statusReport(dayStart);
  assert.match(out, /B: 4 candidates \| 1 entered \| 3 rejected \(blocker: volume\)/);
  assert.match(out, /Total: 4 scanned \| 1 entered/);
  assert.match(out, /Closed trades: 2 \/ 50/);
  assert.match(out, /Thresholds: age<=30m  mcap>\=\$2,000  vol>\=\$200/);
  fs.rmSync(path.dirname(p), { recursive: true, force: true });
});

test("summaryReport performance section falls back without daily_stats rows", () => {
  const p = makeFixtureDb();
  const { dayStart } = seedFixture(p);
  db.initDb(p);
  const out = reports.summaryReport(dayStart);
  assert.match(out, /\*Performance\*/);
  assert.match(out, /Cumulative PnL: -0\.03 SOL/); // all-time from positions
  assert.match(out, /Drawdown from peak: n\/a/);
  assert.match(out, /7-day Sharpe: n\/a/);
  assert.match(out, /Streak: n\/a/);
  fs.rmSync(path.dirname(p), { recursive: true, force: true });
});

test("summaryReport performance section reads daily_stats rows", () => {
  const p = makeFixtureDb();
  seedFixture(p);
  const c = new DatabaseSync(p);
  const ins = c.prepare(
    "INSERT INTO daily_stats (date, strategy_a_trades, strategy_a_pnl_sol, strategy_a_win_rate, strategy_b_trades, strategy_b_pnl_sol, strategy_b_win_rate, total_pnl_sol, cumulative_pnl_sol, max_drawdown_sol, sharpe_ratio) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
  );
  ins.run("2026-08-08", 1, 0.1, 1.0, 0, 0, 0, 0.1, 0.1, 0, null);
  ins.run("2026-08-09", 0, 0, 0, 1, -0.04, 0, -0.04, 0.06, 0.04, 1.2345);
  ins.run("2026-08-10", 0, 0, 0, 1, 0.02, 1.0, 0.02, 0.08, 0.04, 0.5);
  c.close();
  db.initDb(p);
  const out = reports.summaryReport(lib.etDayStartUtc(0));
  assert.match(out, /\*Performance\*/);
  assert.match(out, /Cumulative PnL: \+0\.08 SOL/);
  assert.match(out, /Drawdown from peak: -0\.0200 SOL/);
  assert.match(out, /7-day Sharpe: 0\.50/);
  assert.match(out, /Streak: 1 green day/);
  fs.rmSync(path.dirname(p), { recursive: true, force: true });
});

test("streakLabel ignores an untraded today row at the midnight wire", () => {
  const todayStr = reports.etDateString(Date.now());
  const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
  const rows = [
    { date: yesterday, strategy_a_trades: 1, strategy_b_trades: 0, total_pnl_sol: 0.02 },
    { date: todayStr, strategy_a_trades: 0, strategy_b_trades: 0, total_pnl_sol: 0 },
  ];
  assert.strictEqual(reports.streakLabel(rows), "1 green day");
  const tradedToday = [
    { date: yesterday, strategy_a_trades: 1, strategy_b_trades: 0, total_pnl_sol: 0.02 },
    { date: todayStr, strategy_a_trades: 1, strategy_b_trades: 0, total_pnl_sol: 0 },
  ];
  assert.strictEqual(reports.streakLabel(tradedToday), "0 (flat)");
});

test("gatesReport parses the active strategy script", () => {
  const p = makeFixtureDb();
  seedFixture(p);
  db.initDb(p);
  const out = reports.gatesReport();
  assert.doesNotMatch(out, /Strategy A|run_paper_loop\.py/);
  assert.match(out, /\*Strategy B \(run_strategy_b\.py\)\*/);
  assert.match(out, /Tuner config id 1/);
  assert.match(out, /min mcap: \$2,000/);
  assert.match(out, /Take profit: 80%/);
  assert.match(out, /Hard stop: 10%/);
  assert.match(out, /Holder tiers/);
  fs.rmSync(path.dirname(p), { recursive: true, force: true });
});

test("helpReport lists Strategy B commands", () => {
  const out = reports.helpReport();
  for (const c of ["pnl", "check", "status", "today", "last 5", "gates", "kill B / start B"]) {
    assert.match(out, new RegExp(c.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  }
  assert.doesNotMatch(out, /Strategy A|kill A|start A/);
});

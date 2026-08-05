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
  assert.match(out, /\*Strategy A\*/);
  assert.match(out, /\*Strategy B\*/);
  assert.match(out, /Closed: 1 trade/); // A today
  assert.match(out, /\+0\.01 SOL/); // A best today
  assert.match(out, /-0\.02 SOL/); // B today = -0.05 + 0.03
  assert.match(out, /Win rate: 50%/); // B: 1 win of 2
  assert.match(out, /All-time: -0\.03 SOL/); // +0.01 -0.05 +0.03 -0.02
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

test("gatesReport parses both strategy scripts", () => {
  const p = makeFixtureDb();
  seedFixture(p);
  db.initDb(p);
  const out = reports.gatesReport();
  assert.match(out, /\*Strategy A \(run_paper_loop\.py\)\*/);
  assert.match(out, /Trailing stop: 4%/);
  assert.match(out, /Hard stop: 10%/);
  assert.match(out, /\*Strategy B \(run_strategy_b\.py\)\*/);
  assert.match(out, /Tuner config id 1/);
  assert.match(out, /min mcap: \$2,000/);
  assert.match(out, /Take profit: 2x/);
  assert.match(out, /Holder tiers/);
  fs.rmSync(path.dirname(p), { recursive: true, force: true });
});

test("helpReport lists all commands", () => {
  const out = reports.helpReport();
  for (const c of ["pnl", "check", "status", "today", "last 5", "gates", "kill A / start A", "kill B / start B"]) {
    assert.match(out, new RegExp(c.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  }
});

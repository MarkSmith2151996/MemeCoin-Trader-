"use strict";
/*
 * Telegram trading bot for the memecoin paper trader (MT-513).
 *
 * Setup (after creating the bot with @BotFather):
 *   1. Create the bot with @BotFather and copy the API token.
 *   2. Get your chat id (message @userinfobot once and read "Id:").
 *   3. Fill telegram_token and telegram_chat_id in bot/config.json.
 *   4. cd bot && npm install
 *
 * Then run as a background service (watchdog-managed, like the trading loops):
 *   cd bot && NODE_NO_WARNINGS=1 nohup node bot.js >> /tmp/telegram_bot.log 2>&1 &
 *   (the cron watchdog /home/dev/watchdog_memecoin.sh does this automatically)
 *
 * Uses long polling (no webhook — simpler from WSL) and only responds to
 * messages from the configured chat id. With blank telegram_token and/or
 * telegram_chat_id the bot starts and idles until the config is filled.
 */
const path = require("node:path");
const fs = require("node:fs");
const { spawn, spawnSync } = require("node:child_process");
const TelegramBot = require("node-telegram-bot-api");

const lib = require("./lib");
const db = require("./db");
const sys = require("./system");
const reports = require("./reports");

const CONFIG_PATH = process.env.BOT_CONFIG || path.join(__dirname, "config.json");
const DEFAULT_DB_PATH = "/home/dev/projects/memecoin-trader/data/trades.db";

let config;
try {
  config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
} catch (e) {
  console.error(`cannot read config ${CONFIG_PATH}: ${e.message}`);
  process.exit(1);
}

const DB_PATH = config.db_path || DEFAULT_DB_PATH;
const PROJECT_ROOT = path.dirname(path.dirname(DB_PATH));

const TOKEN = String(config.telegram_token || "").trim();
const rawChatId = String(config.telegram_chat_id == null ? "" : config.telegram_chat_id).trim();
const CHAT_ID = rawChatId && Number.isInteger(Number(rawChatId)) ? Number(rawChatId) : null;

db.initDb(DB_PATH);

const HEALTH_INTERVAL_MS = Math.max(0, Number(config.health_check_interval_seconds || 60) * 1000);
const RE_ALERT_MS = 30 * 60 * 1000;

let bot = null;
let shuttingDown = false;

/* ---------------- Telegram connection ---------------- */

function initTelegram() {
  bot = new TelegramBot(TOKEN, { polling: true });
  bot.on("message", handleMessage);
  bot.on("polling_error", (e) => console.error(`[${stamp()}] polling error: ${e.message}`));
  bot.on("error", (e) => console.error(`[${stamp()}] api error: ${e.message}`));
}

/* ---------------- messages / commands ---------------- */

function parseCommand(text) {
  return String(text || "").trim().toLowerCase().replace(/^\/+/, "").replace(/\s+/g, " ");
}

async function sendText(chatId, text) {
  if (!bot) return;
  try {
    await bot.sendMessage(chatId, text, { parse_mode: "Markdown" });
  } catch (e) {
    try {
      await bot.sendMessage(chatId, text);
    } catch (e2) {
      console.error(`[${stamp()}] send failed: ${e2.message}`);
    }
  }
}

async function handleCommand(chatId, text) {
  const cmd = parseCommand(text);
  let reply;
  const dayStart = lib.etDayStartUtc(0);
  switch (true) {
    case cmd === "pnl" || cmd === "pnl today":
      reply = reports.pnlReport(dayStart);
      break;
    case cmd === "check" || cmd === "health":
      reply = reports.checkReport();
      break;
    case cmd === "status":
      reply = reports.statusReport(dayStart);
      break;
    case cmd === "today" || cmd === "summary":
      reply = reports.summaryReport(dayStart);
      break;
    case cmd === "last 5" || cmd === "last5":
      reply = reports.last5Report();
      break;
    case cmd === "gates":
      reply = reports.gatesReport();
      break;
    case /^kill b$/.test(cmd): {
      const r = sys.killLoop("B");
      await waitMs(1500);
      const alive = sys.processAlive(sys.LOOP_CMD.B.pattern) != null;
      reply =
        "*Strategy B*\n" +
        `${r.ok ? "Kill signal sent." : `Failed: ${r.message}`}\n` +
        `Process: ${alive ? "STILL RUNNING" : "stopped"}.\n` +
        'Note: the external watchdog restarts it within ~3 min unless you send "start B" first.';
      break;
    }
    case cmd === "kill switch" || cmd === "killswitch": {
      reply = await runKillSwitch(PROJECT_ROOT);
      break;
    }
    case /^start b$/.test(cmd): {
      const r = sys.startLoop("B", PROJECT_ROOT);
      await waitMs(2500);
      const pid = sys.processAlive(sys.LOOP_CMD.B.pattern);
      reply =
        "*Strategy B*\n" +
        (r.ok ? "Launch requested." : `Failed: ${r.message}`) +
        `\nProcess: ${pid != null ? `ALIVE (pid ${pid})` : "not up yet — check again in a minute."}`;
      break;
    }
    default:
      reply = reports.helpReport();
  }
  await sendText(chatId, reply);
}

async function handleMessage(msg) {
  if (!msg || !msg.text || !CHAT_ID) return;
  if (Number(msg.chat.id) !== CHAT_ID) return;
  const text = String(msg.text).trim();
  if (!text) return;
  console.log(`[${stamp()}] command from owner: "${text.slice(0, 60)}"`);
  try {
    await handleCommand(msg.chat.id, text);
  } catch (e) {
    console.error(`[${stamp()}] command failed: ${e.message}`);
    await sendText(msg.chat.id, `Command error: ${e.message}`);
  }
}

/* ---------------- auto-alerts ---------------- */

const healthState = {
  B: { ok: null, lastAlertAt: 0 },
  browserPc: { ok: null, lastAlertAt: 0 },
};

async function healthWatch() {
  let snap;
  try {
    snap = sys.healthSnapshot();
  } catch (e) {
    console.error(`[${stamp()}] health check failed: ${e.message}`);
    return;
  }
  console.log(
    `[${stamp()}] hb B=${snap.B.alive ? "up" : "down"} browser=${snap.browserPc.ok ? "ok" : "down"}`
  );
  if (!bot || !CHAT_ID) return;
  const now = Date.now();
  const items = [
    ["Strategy B", "B", snap.B.alive, `process run_strategy_b.py not found`],
    ["browser-pc", "browserPc", snap.browserPc.ok, `health endpoint down (${snap.browserPc.detail})`],
  ];
  for (const [label, key, ok, downDetail] of items) {
    const st = healthState[key];
    const alive = !!ok;
    if (!alive) {
      const shouldAlert = st.ok !== false || now - st.lastAlertAt > RE_ALERT_MS;
      if (shouldAlert) {
        st.lastAlertAt = now;
        await sendText(CHAT_ID, `*ALERT — ${label} DOWN*\n${downDetail}\nChecked at ${lib.fmtEtTime(now)}.`);
        console.log(`[${stamp()}] alerted: ${label} down`);
      }
    } else if (st.ok === false) {
      await sendText(CHAT_ID, `*${label} recovered* at ${lib.fmtEtTime(now)}.`);
      console.log(`[${stamp()}] alerted: ${label} recovered`);
    }
    st.ok = alive;
  }
}

let lastGateId = null;

function gateWatch() {
  if (!bot || !CHAT_ID) return;
  let current;
  try {
    current = db.maxGateConfigId();
  } catch (e) {
    console.error(`[${stamp()}] gate watch failed: ${e.message}`);
    return;
  }
  if (lastGateId == null) {
    lastGateId = current;
    return;
  }
  if (current <= lastGateId) return;
  const newRows = db.gateConfigsSince(0).filter((r) => r.id > lastGateId);
  for (const row of newRows) {
    const msg = gateChangeMessage(row);
    sendText(CHAT_ID, msg);
    console.log(`[${stamp()}] gate change alert: id ${row.id}`);
  }
  lastGateId = current;
}

function gateChangeMessage(row) {
  const strategy = row.strategy || "B";
  const lines = [`*Auto-tuner gate change — Strategy ${strategy}*`, `id ${row.id} at ${lib.fmtEtTime(Date.parse(row.updated_at))}`];
  try {
    const cur = JSON.parse(row.config_json || "{}");
    const prev = db
      .all("SELECT config_json FROM gate_config WHERE strategy = ? AND id < ? ORDER BY id DESC LIMIT 1", [strategy, row.id])
      .map((r) => JSON.parse(r.config_json || "{}"))[0];
    const parts = [];
    for (const [k, v] of Object.entries(cur)) {
      const oldV = prev ? prev[k] : undefined;
      if (oldV === undefined) parts.push(`${k}: ${v}`);
      else if (oldV !== v) parts.push(`${k}: ${oldV} -> ${v}`);
    }
    if (parts.length) lines.push(`Changed: ${parts.join(", ")}`);
    else lines.push(`Config: ${row.config_json}`);
  } catch {
    lines.push(`Config: ${row.config_json}`);
  }
  if (row.reason) lines.push(`Why: ${row.reason}`);
  if (row.sample_size) lines.push(`Sample: ${row.sample_size} closed trades`);
  return lines.join("\n");
}

/** Recompute daily_stats (MT-526) before the midnight summary sends. */
function refreshDailyStats() {
  try {
    const script = path.join(PROJECT_ROOT, "scripts", "run_daily_stats.py");
    const res = spawnSync("python3", [script, "--today"], { timeout: 30000, encoding: "utf8" });
    if (res.status !== 0) {
      const detail = String(res.stderr || "").trim().split("\n").slice(-3).join(" ");
      console.error(`[${stamp()}] daily stats refresh failed (${res.status}): ${detail}`);
    } else {
      console.log(`[${stamp()}] daily stats refreshed`);
    }
  } catch (e) {
    console.error(`[${stamp()}] daily stats refresh error: ${e.message}`);
  }
}

async function dailySummaryTick() {
  if (!bot || !CHAT_ID) return;
  refreshDailyStats();
  const dayStart = lib.etDayStartUtc(0);
  try {
    const text = reports.summaryReport(dayStart);
    await sendText(CHAT_ID, text);
    console.log(`[${stamp()}] midnight summary sent`);
  } catch (e) {
    console.error(`[${stamp()}] summary failed: ${e.message}`);
  }
}

function scheduleDailySummary() {
  const ms = lib.msUntilNextEtTime(config.daily_summary_hour, config.daily_summary_minute);
  console.log(`[${stamp()}] next daily summary in ${Math.round(ms / 60000)} min`);
  setTimeout(async () => {
    await dailySummaryTick();
    scheduleDailySummary();
  }, ms);
}

/* ---------------- helpers ---------------- */

/** Run scripts/kill_switch.py asynchronously and resolve with its output. */
function runKillSwitch(projectRoot) {
  const script = path.join(projectRoot, "scripts", "kill_switch.py");
  return new Promise((resolve) => {
    const child = spawn("python3", [script], { cwd: projectRoot, timeout: 240000 });
    let out = "";
    let err = "";
    let settled = false;
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    const finish = () => {
      if (settled) return;
      settled = true;
      const body = (out.trim() || err.trim() || "(no output)").split("\n").slice(0, 40).join("\n");
      resolve(`*Kill switch*\n${body}`);
    };
    child.on("close", finish);
    child.on("error", (e) => {
      out = `Failed to launch: ${e.message}`;
      finish();
    });
  });
}

function waitMs(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function stamp() {
  return lib.fmtEtTime(Date.now());
}

async function main() {
  if (!TOKEN) {
    console.warn(
      `[${stamp()}] WARNING: telegram_token is blank in ${CONFIG_PATH} — bot idles without connecting. Fill it in and restart.`
    );
  }
  if (!CHAT_ID) {
    console.warn(
      `[${stamp()}] WARNING: telegram_chat_id is blank in ${CONFIG_PATH} — bot ignores all messages. Fill it in and restart.`
    );
  }
  console.log(`[${stamp()}] starting telegram bot (db=${DB_PATH}${TOKEN ? "" : ", token blank"})`);
  if (TOKEN) initTelegram();

  if (HEALTH_INTERVAL_MS > 0) {
    setInterval(healthWatch, HEALTH_INTERVAL_MS);
    setTimeout(healthWatch, HEALTH_INTERVAL_MS);
    setInterval(gateWatch, HEALTH_INTERVAL_MS);
    setTimeout(gateWatch, HEALTH_INTERVAL_MS);
  }
  scheduleDailySummary();

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[${stamp()}] shutting down`);
  if (bot) {
    try {
      bot.stopPolling();
    } catch {
      /* ignore */
    }
  }
  setTimeout(() => process.exit(0), 800);
}

main().catch((e) => {
  console.error(`[${stamp()}] fatal: ${e.message}`);
  process.exit(1);
});

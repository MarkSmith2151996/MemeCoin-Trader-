"use strict";
/*
 * WhatsApp trading bot for the memecoin paper trader (MT-510).
 *
 * First run (QR scan):
 *   cd bot && npm install
 *   node /home/dev/projects/memecoin-trader/bot/bot.js
 *   Scan the QR with WhatsApp > Linked Devices. Credentials are stored in
 *   bot/auth/ and reused on later starts. (Run with the absolute path so the
 *   watchdog's "node bot/bot.js" process check recognizes this instance and
 *   does not spawn a duplicate while you are scanning.)
 *
 * Then run as a background service (watchdog-managed, like the trading loops):
 *   cd bot && NODE_NO_WARNINGS=1 nohup node bot.js >> /tmp/whatsapp_bot.log 2>&1 &
 *   (the cron watchdog /home/dev/watchdog_memecoin.sh does this automatically)
 *
 * Only responds to the owner number in bot/config.json.
 */
const path = require("node:path");
const fs = require("node:fs");
const pino = require("pino");
const qrcode = require("qrcode-terminal");
const makeWASocket = require("@whiskeysockets/baileys").default;
const {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason,
} = require("@whiskeysockets/baileys");

const lib = require("./lib");
const db = require("./db");
const sys = require("./system");
const reports = require("./reports");

const CONFIG_PATH = process.env.BOT_CONFIG || path.join(__dirname, "config.json");
const AUTH_DIR = process.env.BOT_AUTH_DIR || path.join(__dirname, "auth");
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

const ownerDigits = lib.digitsOnly(config.owner_number || "");
const OWNER_JID = ownerDigits ? `${ownerDigits}@s.whatsapp.net` : null;

db.initDb(DB_PATH);

const HEALTH_INTERVAL_MS = Math.max(0, Number(config.health_check_interval_seconds || 60) * 1000);
const RE_ALERT_MS = 30 * 60 * 1000;
const DAY_MS = 24 * 3600 * 1000;

const logger = pino({ level: "warn" });

let sock = null;
let connectionState = "idle";
let lastOpenAt = 0;
let reconnectTimer = null;
let backoffMs = 5000;
let shuttingDown = false;

/* ---------------- WhatsApp connection ---------------- */

async function startSocket() {
  if (shuttingDown) return;
  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version } = await fetchLatestBaileysVersion();
    if (sock) {
      try {
        sock.end(0);
      } catch {
        /* ignore */
      }
    }
    sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger,
      browser: ["memecoin-trader-bot", "Chrome", "122.0.0.1"],
      markOnlineOnConnect: true,
    });
    sock.ev.on("creds.update", saveCreds);
    sock.ev.on("connection.update", handleConnectionUpdate);
    sock.ev.on("messages.upsert", handleMessages);
  } catch (e) {
    console.error(`[${stamp()}] socket start failed: ${e.message}`);
    scheduleReconnect();
  }
}

function handleConnectionUpdate(update) {
  if (update.qr) {
    console.log(`[${stamp()}] QR generated — scan within ~60s (Linked Devices > Link a device)`);
    qrcode.generate(update.qr, { small: true });
  }
  if (update.connection === "connecting") {
    connectionState = "connecting";
    console.log(`[${stamp()}] connecting...`);
  }
  if (update.connection === "open") {
    connectionState = "open";
    lastOpenAt = Date.now();
    backoffMs = 5000;
    console.log(`[${stamp()}] connected as ${sock.user ? sock.user.id : "unknown"}`);
  }
  if (update.connection === "close") {
    const code = update.lastDisconnect && update.lastDisconnect.error;
    const reason = code ? code.output && code.output.statusCode : null;
    connectionState = "close";
    console.log(
      `[${stamp()}] connection closed (code=${reason ?? "n/a"})${reason === DisconnectReason.loggedOut ? " — logged out, remove bot/auth to re-pair" : ""}`
    );
    scheduleReconnect();
  }
}

function scheduleReconnect() {
  if (shuttingDown || reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    startSocket();
  }, backoffMs);
  backoffMs = Math.min(backoffMs * 2, 60000);
}

/* ---------------- messages / commands ---------------- */

function extractText(msg) {
  const m = msg.message || {};
  return (
    m.conversation ||
    (m.extendedTextMessage && m.extendedTextMessage.text) ||
    (m.imageMessage && m.imageMessage.caption) ||
    (m.videoMessage && m.videoMessage.caption) ||
    null
  );
}

function isOwner(jid) {
  if (!OWNER_JID || !jid) return false;
  return lib.digitsOnly(jid.split("@")[0]) === ownerDigits;
}

async function sendText(jid, text) {
  try {
    await sock.sendMessage(jid, { text });
  } catch (e) {
    console.error(`[${stamp()}] send failed: ${e.message}`);
  }
}

function parseCommand(text) {
  return String(text || "").trim().toLowerCase().replace(/\s+/g, " ");
}

async function handleCommand(jid, text) {
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
    case /^kill a$/.test(cmd):
    case /^kill b$/.test(cmd): {
      const strategy = cmd.endsWith("a") ? "A" : "B";
      const r = sys.killLoop(strategy);
      await waitMs(1500);
      const alive = sys.processAlive(sys.LOOP_CMD[strategy].pattern) != null;
      reply =
        `*Strategy ${strategy}*\n` +
        `${r.ok ? "Kill signal sent." : `Failed: ${r.message}`}\n` +
        `Process: ${alive ? "STILL RUNNING" : "stopped"}.\n` +
        `Note: the external watchdog restarts it within ~3 min unless you send "start ${strategy}" first.`;
      break;
    }
    case /^start a$/.test(cmd):
    case /^start b$/.test(cmd): {
      const strategy = cmd.endsWith("a") ? "A" : "B";
      const r = sys.startLoop(strategy, PROJECT_ROOT);
      await waitMs(2500);
      const pid = sys.processAlive(sys.LOOP_CMD[strategy].pattern);
      reply =
        `*Strategy ${strategy}*\n` +
        (r.ok ? "Launch requested." : `Failed: ${r.message}`) +
        `\nProcess: ${pid != null ? `ALIVE (pid ${pid})` : "not up yet — check again in a minute."}`;
      break;
    }
    default:
      reply = reports.helpReport();
  }
  await sendText(jid, reply);
}

async function handleMessages(upsert) {
  if (upsert.type !== "notify") return;
  for (const msg of upsert.messages) {
    if (!msg || msg.key.fromMe) continue;
    const jid = msg.key.remoteJid;
    if (!isOwner(jid)) continue;
    const text = extractText(msg);
    if (!text) continue;
    console.log(`[${stamp()}] command from owner: "${text.trim().slice(0, 60)}"`);
    try {
      await handleCommand(jid, text);
    } catch (e) {
      console.error(`[${stamp()}] command failed: ${e.message}`);
      await sendText(jid, `Command error: ${e.message}`);
    }
  }
}

/* ---------------- auto-alerts ---------------- */

const healthState = {
  A: { ok: null, lastAlertAt: 0 },
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
    `[${stamp()}] hb conn=${connectionState} A=${snap.A.alive ? "up" : "down"} B=${snap.B.alive ? "up" : "down"} browser=${snap.browserPc.ok ? "ok" : "down"}`
  );
  if (!OWNER_JID) return;
  const now = Date.now();
  const items = [
    ["Strategy A", "A", snap.A.alive, `process run_paper_loop.py not found`],
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
        await sendText(
          OWNER_JID,
          `*ALERT — ${label} DOWN*\n${downDetail}\nChecked at ${lib.fmtEtTime(now)}.`
        );
        console.log(`[${stamp()}] alerted: ${label} down`);
      }
    } else if (st.ok === false) {
      await sendText(OWNER_JID, `*${label} recovered* at ${lib.fmtEtTime(now)}.`);
      console.log(`[${stamp()}] alerted: ${label} recovered`);
    }
    st.ok = alive;
  }
}

let lastGateId = null;

function gateWatch() {
  if (!OWNER_JID) return;
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
    sendText(OWNER_JID, msg);
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

async function dailySummaryTick() {
  if (!OWNER_JID) return;
  const dayStart = lib.etDayStartUtc(0);
  try {
    const text = reports.summaryReport(dayStart);
    await sendText(OWNER_JID, text);
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

function waitMs(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function stamp() {
  return lib.fmtEtTime(Date.now());
}

async function main() {
  if (!OWNER_JID) {
    console.warn(
      `[${stamp()}] WARNING: owner_number is blank in ${CONFIG_PATH} — bot ignores all messages. Fill it in and restart.`
    );
  }
  console.log(`[${stamp()}] starting whatsapp bot (db=${DB_PATH}, auth=${AUTH_DIR})`);
  await startSocket();

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
  if (sock) {
    try {
      sock.end(0);
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
